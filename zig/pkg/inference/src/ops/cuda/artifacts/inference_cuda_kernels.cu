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
__device__ __forceinline__ float termite_tq_decode_polar4_scalar(unsigned char code);
__device__ __forceinline__ float termite_tq_decode_polar4_at(const unsigned char* encoded, unsigned int value_index);
__device__ __forceinline__ unsigned int termite_tq_physical_token(
    unsigned int logical_token,
    const unsigned int* block_table,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int physical_token_capacity
);
__device__ __forceinline__ float termite_tq_f16_value(const unsigned char* row, unsigned int value_index);

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

// Keep dense encoder activations in F32 through the graph, then stage only
// the operand consumed by the FP16 tensor-core GEMM. This gives FP16 GGUF
// models tensor-core throughput without changing residual/norm accumulation.
extern "C" __global__ void termite_f32_to_f16(
    unsigned short* dst,
    const float* input,
    unsigned int count
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count) return;
    __half value = __float2half_rn(input[idx]);
    dst[idx] = __half_as_ushort(value);
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

// Compatibility route for native FP16 encoder weights when cuBLASLt cannot
// produce or execute an algorithm for a newly introduced dense shape. This is
// intentionally simple and universally shaped: qualified tensor-core plans
// remain the fast path, while an unsupported heuristic degrades to correct
// device-resident execution instead of failing the request.
extern "C" __global__ void termite_linear_f16_weight_f32_tiled(
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
        acc += input[row * in_dim + i] * __half2float(__ushort_as_half(weight[col * in_dim + i]));
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

// Dense tensor-core epilogue used by encoder/span-head MLPs. The GEMM output
// is already resident in F32; apply bias and ReLU in-place so large prefill
// shapes do not make a second full-tensor allocation and memory pass.
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

// Strided gate/up activation multiply over the output of a concatenated
// gate|up GEMM. `fused` is row-major [rows, 2*f]: within each row, columns
// [0, f) hold the gate projection and columns [f, 2*f) hold the up
// projection. dst is contiguous [rows, f] with
// dst[r][c] = act(fused[r][c]) * fused[r][f + c]. Activation ids match
// termite_activation_multiply_f32 (0/1 gelu tanh, 2 silu, 3 relu,
// 4 quick gelu, else relu^2).
extern "C" __global__ void termite_activation_multiply_fused_gate_up_f32(
    float* dst,
    const float* fused,
    unsigned int rows,
    unsigned int f,
    unsigned int activation
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * f;
    if (i >= count) return;
    unsigned int row = i / f;
    unsigned int col = i - row * f;
    const float* fused_row = fused + (size_t)row * (2u * f);
    dst[i] = termite_decoder_activation_f32(fused_row[col], activation) * fused_row[f + col];
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

extern "C" __global__ void termite_embedding_lookup_f16_weight_f32(
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
    __half value = reinterpret_cast<const __half*>(weight)[(unsigned long long)id * dim + col];
    dst[idx] = __half2float(value) * scale;
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

extern "C" __global__ void termite_embedding_lookup_i32_f16_weight_f32(
    float* dst,
    const unsigned short* weight,
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
    __half value = reinterpret_cast<const __half*>(weight)[(unsigned long long)((unsigned int)id) * dim + col];
    dst[idx] = __half2float(value) * scale;
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

// Shape-specialized BERT encoder prefill attention for the BGE-M3/XLM-R hot
// path: full attention over head-major [batch, heads, 256, 64] tensors.
//
// The generic block kernel above synchronizes a 128-thread block seven times
// for *every* key. At sequence 256 that serializes thousands of barriers per
// layer. This schedule gives eight warps independent key streams, reduces each
// 64-wide QK dot with warp shuffles, then synchronizes only at the softmax
// phase boundaries. It is deliberately narrow and selected only when all
// semantics match exactly (non-causal, unmasked, unbiased, head-major S=256
// and D=64); other attention routes retain the generic implementation.
extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64(
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

// Eight-query tile of the same S=256, D=64 BERT attention.  Each warp owns a
// query row, which removes the cross-warp hand-offs in the single-query
// schedule and gives BERT's medium-batch prefill enough independent work to
// fill the GPU.  The reduction tree deliberately mirrors the single-query
// generic kernel so encoder embeddings remain bit-stable.
extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64_q8(
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
    (void)mask;
    (void)bias;
    (void)causal;
    (void)has_mask;
    (void)bias_mode;
    (void)head_major;

    const unsigned int tile_id = blockIdx.x;
    const unsigned int tiles_per_head = 32u;
    const unsigned int total_tiles = batch * num_heads * tiles_per_head;
    if (tile_id >= total_tiles || seq_len != 256u || head_dim != 64u) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int tile = tile_id % tiles_per_head;
    const unsigned int tmp = tile_id / tiles_per_head;
    const unsigned int head = tmp % num_heads;
    const unsigned int b = tmp / num_heads;
    const unsigned int qi = tile * 8u + warp;
    const unsigned int base = ((b * num_heads + head) * 256u) * 64u;
    const unsigned int q_base = base + qi * 64u;
    const float q0 = q[q_base + lane];
    const float q1 = q[q_base + lane + 32u];

    __shared__ float scores[8][256];
    // One [32, 64] tile is reused for K and V.  At B8 the eight query warps
    // otherwise repeatedly fetch the same 256-byte rows from global memory.
    __shared__ float kv_tile[32][64];
    float max_score = -3.402823466e+38f;
    for (unsigned int key_start = 0u; key_start < 256u; key_start += 32u) {
        for (unsigned int element = tid; element < 32u * 64u; element += blockDim.x) {
            kv_tile[element / 64u][element % 64u] = k[base + key_start * 64u + element];
        }
        __syncthreads();
        #pragma unroll
        for (unsigned int key = 0u; key < 32u; ++key) {
            const float dot_lo = q0 * kv_tile[key][lane];
            const float dot_hi = q1 * kv_tile[key][lane + 32u];
            float dot = __fadd_rn(dot_lo, dot_hi);
            #pragma unroll
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                dot = __fadd_rn(dot, __shfl_down_sync(0xffffffffu, dot, offset));
            }
            if (lane == 0u) {
                const float score = dot * 0.125f;
                scores[warp][key_start + key] = score;
                max_score = fmaxf(max_score, score);
            }
        }
        __syncthreads();
    }
    max_score = __shfl_sync(0xffffffffu, max_score, 0);
    __syncwarp();

    // Reconstruct the generic 128-lane denominator reduction exactly.  Each
    // lane owns four of its partial sums, then the warp performs the remaining
    // 32-way tail of that same reduction tree.
    const unsigned int score_base = lane;
    const float e0 = expf(scores[warp][score_base] - max_score);
    const float e1 = expf(scores[warp][score_base + 32u] - max_score);
    const float e2 = expf(scores[warp][score_base + 64u] - max_score);
    const float e3 = expf(scores[warp][score_base + 96u] - max_score);
    const float e4 = expf(scores[warp][score_base + 128u] - max_score);
    const float e5 = expf(scores[warp][score_base + 160u] - max_score);
    const float e6 = expf(scores[warp][score_base + 192u] - max_score);
    const float e7 = expf(scores[warp][score_base + 224u] - max_score);
    scores[warp][score_base] = e0;
    scores[warp][score_base + 32u] = e1;
    scores[warp][score_base + 64u] = e2;
    scores[warp][score_base + 96u] = e3;
    scores[warp][score_base + 128u] = e4;
    scores[warp][score_base + 160u] = e5;
    scores[warp][score_base + 192u] = e6;
    scores[warp][score_base + 224u] = e7;
    float denom = __fadd_rn(
        __fadd_rn(__fadd_rn(e0, e4), __fadd_rn(e2, e6)),
        __fadd_rn(__fadd_rn(e1, e5), __fadd_rn(e3, e7))
    );
    #pragma unroll
    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        denom = __fadd_rn(denom, __shfl_down_sync(0xffffffffu, denom, offset));
    }
    denom = __shfl_sync(0xffffffffu, denom, 0);
    __syncwarp();

    float acc0 = 0.0f;
    float acc1 = 0.0f;
    for (unsigned int key_start = 0u; key_start < 256u; key_start += 32u) {
        for (unsigned int element = tid; element < 32u * 64u; element += blockDim.x) {
            kv_tile[element / 64u][element % 64u] = v[base + key_start * 64u + element];
        }
        __syncthreads();
        #pragma unroll
        for (unsigned int key = 0u; key < 32u; ++key) {
            const float weight = scores[warp][key_start + key];
            acc0 += weight * kv_tile[key][lane];
            acc1 += weight * kv_tile[key][lane + 32u];
        }
        __syncthreads();
    }
    dst[q_base + lane] = acc0 / denom;
    dst[q_base + lane + 32u] = acc1 / denom;
}

// Sixteen-query version of the exact q8 schedule above. It preserves every
// per-row arithmetic and reduction order, but doubles query rows per launch
// block to reduce grid traffic and improve occupancy on encoder prefill.
extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64_q16(
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
    (void)mask;
    (void)bias;
    (void)causal;
    (void)has_mask;
    (void)bias_mode;
    (void)head_major;

    const unsigned long long tile_id = blockIdx.x;
    const unsigned int tiles_per_head = 16u;
    const unsigned long long total_tiles = static_cast<unsigned long long>(batch) * num_heads * tiles_per_head;
    if (tile_id >= total_tiles || seq_len != 256u || head_dim != 64u || blockDim.x != 512u) return;

    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int tile = static_cast<unsigned int>(tile_id % tiles_per_head);
    const unsigned long long tmp = tile_id / tiles_per_head;
    const unsigned int head = static_cast<unsigned int>(tmp % num_heads);
    const unsigned int b = static_cast<unsigned int>(tmp / num_heads);
    const unsigned int qi = tile * 16u + warp;
    const size_t base = (static_cast<size_t>(b) * num_heads + head) * 256u * 64u;
    const size_t q_base = base + qi * 64u;
    const float q0 = q[q_base + lane];
    const float q1 = q[q_base + lane + 32u];

    __shared__ float scores[16][256];
    __shared__ float kv_tile[32][64];
    float max_score = -3.402823466e+38f;
    for (unsigned int key_start = 0u; key_start < 256u; key_start += 32u) {
        for (unsigned int element = tid; element < 32u * 64u; element += blockDim.x) {
            kv_tile[element / 64u][element % 64u] = k[base + key_start * 64u + element];
        }
        __syncthreads();
        #pragma unroll
        for (unsigned int key = 0u; key < 32u; ++key) {
            const float dot_lo = q0 * kv_tile[key][lane];
            const float dot_hi = q1 * kv_tile[key][lane + 32u];
            float dot = __fadd_rn(dot_lo, dot_hi);
            #pragma unroll
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                dot = __fadd_rn(dot, __shfl_down_sync(0xffffffffu, dot, offset));
            }
            if (lane == 0u) {
                const float score = dot * 0.125f;
                scores[warp][key_start + key] = score;
                max_score = fmaxf(max_score, score);
            }
        }
        __syncthreads();
    }
    max_score = __shfl_sync(0xffffffffu, max_score, 0);
    __syncwarp();

    const unsigned int score_base = lane;
    const float e0 = expf(scores[warp][score_base] - max_score);
    const float e1 = expf(scores[warp][score_base + 32u] - max_score);
    const float e2 = expf(scores[warp][score_base + 64u] - max_score);
    const float e3 = expf(scores[warp][score_base + 96u] - max_score);
    const float e4 = expf(scores[warp][score_base + 128u] - max_score);
    const float e5 = expf(scores[warp][score_base + 160u] - max_score);
    const float e6 = expf(scores[warp][score_base + 192u] - max_score);
    const float e7 = expf(scores[warp][score_base + 224u] - max_score);
    scores[warp][score_base] = e0;
    scores[warp][score_base + 32u] = e1;
    scores[warp][score_base + 64u] = e2;
    scores[warp][score_base + 96u] = e3;
    scores[warp][score_base + 128u] = e4;
    scores[warp][score_base + 160u] = e5;
    scores[warp][score_base + 192u] = e6;
    scores[warp][score_base + 224u] = e7;
    float denom = __fadd_rn(
        __fadd_rn(__fadd_rn(e0, e4), __fadd_rn(e2, e6)),
        __fadd_rn(__fadd_rn(e1, e5), __fadd_rn(e3, e7))
    );
    #pragma unroll
    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
        denom = __fadd_rn(denom, __shfl_down_sync(0xffffffffu, denom, offset));
    }
    denom = __shfl_sync(0xffffffffu, denom, 0);
    __syncwarp();

    float acc0 = 0.0f;
    float acc1 = 0.0f;
    for (unsigned int key_start = 0u; key_start < 256u; key_start += 32u) {
        for (unsigned int element = tid; element < 32u * 64u; element += blockDim.x) {
            kv_tile[element / 64u][element % 64u] = v[base + key_start * 64u + element];
        }
        __syncthreads();
        #pragma unroll
        for (unsigned int key = 0u; key < 32u; ++key) {
            const float weight = scores[warp][key_start + key];
            acc0 += weight * kv_tile[key][lane];
            acc1 += weight * kv_tile[key][lane + 32u];
        }
        __syncthreads();
    }
    dst[q_base + lane] = acc0 / denom;
    dst[q_base + lane + 32u] = acc1 / denom;
}

// Opt-in WMMA candidate for full BERT/XLM-R prefill attention. A block owns
// 16 query rows for one [batch, head] pair and streams all 256 keys in four
// 64-key tiles. QK stays FP32 to preserve encoder embedding quality. The
// dominant P*V term uses f16 tensor cores and then adds the two FP32 residual
// terms, so value/probability narrowing does not change the result. Online
// softmax and output stay f32. This is separate from the exact q8 path above.
extern "C" __global__ void termite_attention_f32_bert_prefill_s256_hd64_mma(
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

// quant-kernel-codegen:begin generated CUDA attention kernels (do not edit; run: zig build quant-kernel-codegen -- --write)
// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_split_kv_hd256_f32_stage1_v1 plan_id=cuda/attention/decode_1x/hd256/gqa16/split8/min512/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_scalars_hd256_f32_v1(
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }
    if (kv_seq_len >= split_kv_min_tokens) return;
    const unsigned int block = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        block >= num_heads || head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    const unsigned int lane = threadIdx.x;
    const unsigned int head = block;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            const float score = dot * scale;
            const float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        const unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void antfly_gqa_attention_decode_split_kv_hd256_f32_stage1_v1(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    const unsigned int splits = 8u;
    const unsigned int split = blockIdx.x % splits;
    const unsigned int head_block = blockIdx.x / splits;
    const unsigned int head = head_block % num_heads;
    const unsigned int b = head_block / num_heads;
    if (b >= batch) return;
    const unsigned int heads_per_kv = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_kv;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    unsigned int split_begin = (kv_seq_len * split) / splits;
    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;
    if (split_begin < key_start) split_begin = key_start;
    if (split_end > key_end) split_end = key_end;
    if (split_begin > split_end) split_begin = split_end;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    __shared__ float warp_sums[8];
    __shared__ float head_max;
    __shared__ float head_denom;
    __shared__ float head_alpha;
    __shared__ float head_beta;
    float q_values[1];
    float acc[1];
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        q_values[item] = q[q_base + d];
        acc[item] = 0.0f;
    }
    if (tid == 0u) {
        head_max = -3.402823466e+38f;
        head_denom = 0.0f;
        head_alpha = 0.0f;
        head_beta = 0.0f;
    }
    __syncthreads();

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int key_pos = kv_position_offset + ki;
        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        const bool future_blocked = key_pos > query_pos && !future_allowed;
        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        const bool valid = !(future_blocked || past_blocked);
        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;

        float dot = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float key_value = valid ? k[kv_base + d] : 0.0f;
            dot += q_values[item] * key_value;
        }
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        }
        if (lane == 0u) warp_sums[warp] = dot;
        __syncthreads();

        float block_dot = (warp == 0u && lane < 8u) ? warp_sums[lane] : 0.0f;
        if (warp == 0u) {
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
            }
            if (lane == 0u) {
                float score = valid ? block_dot * scale : -3.402823466e+38f;
                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
                const float next_max = fmaxf(head_max, score);
                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
                const float beta = valid ? expf(score - next_max) : 0.0f;
                head_denom = head_denom * alpha + beta;
                head_max = next_max;
                head_alpha = alpha;
                head_beta = beta;
            }
        }
        __syncthreads();

#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float value = valid ? v[kv_base + d] : 0.0f;
            acc[item] = acc[item] * head_alpha + head_beta * value;
        }
    }

    const unsigned int partial = (b * num_heads + head) * splits + split;
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        partial_values[(size_t)partial * head_dim + d] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = head_max;
        partial_denom[partial] = head_denom;
    }
}

extern "C" __global__ void antfly_gqa_attention_decode_split_kv_hd256_f32_stage2_v1(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int batch,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || head_dim != 256u || blockDim.x != 256u || num_heads > 32u) return;
    const unsigned int head_block = blockIdx.x;
    if (head_block >= batch * num_heads) return;
    const unsigned int splits = 8u;
    __shared__ float merged_denom;
    __shared__ float merge_alpha[8];
    __shared__ float merge_beta[8];
    if (threadIdx.x == 0u) {
        float merged_max = -3.402823466e+38f;
        float denom = 0.0f;
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            const float local_denom = partial_denom[partial];
            if (local_denom > 0.0f) {
                const float next_max = fmaxf(merged_max, partial_max[partial]);
                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;
                const float beta = expf(partial_max[partial] - next_max);
                denom = denom * alpha + local_denom * beta;
                merged_max = next_max;
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        merged_denom = denom;
    }
    __syncthreads();
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = threadIdx.x + item * blockDim.x;
        float numerator = 0.0f;
#pragma unroll
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];
        }
        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;
    }
}

// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_split_kv_hd512_f32_stage1_v1 plan_id=cuda/attention/decode_1x/hd512/gqa16/split8/min512/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_scalars_hd512_f32_v1(
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }
    if (kv_seq_len >= split_kv_min_tokens) return;
    const unsigned int block = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        block >= num_heads || head_dim != 512u || blockDim.x != 512u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    const unsigned int lane = threadIdx.x;
    const unsigned int head = block;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            const float score = dot * scale;
            const float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        const unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void antfly_gqa_attention_decode_split_kv_hd512_f32_stage1_v1(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        head_dim != 512u || blockDim.x != 512u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    const unsigned int splits = 8u;
    const unsigned int split = blockIdx.x % splits;
    const unsigned int head_block = blockIdx.x / splits;
    const unsigned int head = head_block % num_heads;
    const unsigned int b = head_block / num_heads;
    if (b >= batch) return;
    const unsigned int heads_per_kv = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_kv;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    unsigned int split_begin = (kv_seq_len * split) / splits;
    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;
    if (split_begin < key_start) split_begin = key_start;
    if (split_end > key_end) split_end = key_end;
    if (split_begin > split_end) split_begin = split_end;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    __shared__ float warp_sums[16];
    __shared__ float head_max;
    __shared__ float head_denom;
    __shared__ float head_alpha;
    __shared__ float head_beta;
    float q_values[1];
    float acc[1];
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        q_values[item] = q[q_base + d];
        acc[item] = 0.0f;
    }
    if (tid == 0u) {
        head_max = -3.402823466e+38f;
        head_denom = 0.0f;
        head_alpha = 0.0f;
        head_beta = 0.0f;
    }
    __syncthreads();

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int key_pos = kv_position_offset + ki;
        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        const bool future_blocked = key_pos > query_pos && !future_allowed;
        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        const bool valid = !(future_blocked || past_blocked);
        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;

        float dot = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float key_value = valid ? k[kv_base + d] : 0.0f;
            dot += q_values[item] * key_value;
        }
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        }
        if (lane == 0u) warp_sums[warp] = dot;
        __syncthreads();

        float block_dot = (warp == 0u && lane < 16u) ? warp_sums[lane] : 0.0f;
        if (warp == 0u) {
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
            }
            if (lane == 0u) {
                float score = valid ? block_dot * scale : -3.402823466e+38f;
                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
                const float next_max = fmaxf(head_max, score);
                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
                const float beta = valid ? expf(score - next_max) : 0.0f;
                head_denom = head_denom * alpha + beta;
                head_max = next_max;
                head_alpha = alpha;
                head_beta = beta;
            }
        }
        __syncthreads();

#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float value = valid ? v[kv_base + d] : 0.0f;
            acc[item] = acc[item] * head_alpha + head_beta * value;
        }
    }

    const unsigned int partial = (b * num_heads + head) * splits + split;
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        partial_values[(size_t)partial * head_dim + d] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = head_max;
        partial_denom[partial] = head_denom;
    }
}

extern "C" __global__ void antfly_gqa_attention_decode_split_kv_hd512_f32_stage2_v1(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int batch,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || head_dim != 512u || blockDim.x != 512u || num_heads > 32u) return;
    const unsigned int head_block = blockIdx.x;
    if (head_block >= batch * num_heads) return;
    const unsigned int splits = 8u;
    __shared__ float merged_denom;
    __shared__ float merge_alpha[8];
    __shared__ float merge_beta[8];
    if (threadIdx.x == 0u) {
        float merged_max = -3.402823466e+38f;
        float denom = 0.0f;
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            const float local_denom = partial_denom[partial];
            if (local_denom > 0.0f) {
                const float next_max = fmaxf(merged_max, partial_max[partial]);
                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;
                const float beta = expf(partial_max[partial] - next_max);
                denom = denom * alpha + local_denom * beta;
                merged_max = next_max;
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        merged_denom = denom;
    }
    __syncthreads();
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = threadIdx.x + item * blockDim.x;
        float numerator = 0.0f;
#pragma unroll
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];
        }
        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;
    }
}

// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_split2_kv_hd256_f32_stage1_v1 plan_id=cuda/attention/decode_1x/hd256/gqa16/split2/min512/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_scalars_split2_hd256_f32_v1(
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }
    if (kv_seq_len >= split_kv_min_tokens) return;
    const unsigned int block = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        block >= num_heads || head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    const unsigned int lane = threadIdx.x;
    const unsigned int head = block;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            const float score = dot * scale;
            const float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        const unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void antfly_gqa_attention_decode_split2_kv_hd256_f32_stage1_v1(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    const unsigned int splits = 2u;
    const unsigned int split = blockIdx.x % splits;
    const unsigned int head_block = blockIdx.x / splits;
    const unsigned int head = head_block % num_heads;
    const unsigned int b = head_block / num_heads;
    if (b >= batch) return;
    const unsigned int heads_per_kv = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_kv;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    unsigned int split_begin = (kv_seq_len * split) / splits;
    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;
    if (split_begin < key_start) split_begin = key_start;
    if (split_end > key_end) split_end = key_end;
    if (split_begin > split_end) split_begin = split_end;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    __shared__ float warp_sums[8];
    __shared__ float head_max;
    __shared__ float head_denom;
    __shared__ float head_alpha;
    __shared__ float head_beta;
    float q_values[1];
    float acc[1];
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        q_values[item] = q[q_base + d];
        acc[item] = 0.0f;
    }
    if (tid == 0u) {
        head_max = -3.402823466e+38f;
        head_denom = 0.0f;
        head_alpha = 0.0f;
        head_beta = 0.0f;
    }
    __syncthreads();

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int key_pos = kv_position_offset + ki;
        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        const bool future_blocked = key_pos > query_pos && !future_allowed;
        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        const bool valid = !(future_blocked || past_blocked);
        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;

        float dot = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float key_value = valid ? k[kv_base + d] : 0.0f;
            dot += q_values[item] * key_value;
        }
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        }
        if (lane == 0u) warp_sums[warp] = dot;
        __syncthreads();

        float block_dot = (warp == 0u && lane < 8u) ? warp_sums[lane] : 0.0f;
        if (warp == 0u) {
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
            }
            if (lane == 0u) {
                float score = valid ? block_dot * scale : -3.402823466e+38f;
                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
                const float next_max = fmaxf(head_max, score);
                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
                const float beta = valid ? expf(score - next_max) : 0.0f;
                head_denom = head_denom * alpha + beta;
                head_max = next_max;
                head_alpha = alpha;
                head_beta = beta;
            }
        }
        __syncthreads();

#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float value = valid ? v[kv_base + d] : 0.0f;
            acc[item] = acc[item] * head_alpha + head_beta * value;
        }
    }

    const unsigned int partial = (b * num_heads + head) * splits + split;
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        partial_values[(size_t)partial * head_dim + d] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = head_max;
        partial_denom[partial] = head_denom;
    }
}

extern "C" __global__ void antfly_gqa_attention_decode_split2_kv_hd256_f32_stage2_v1(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int batch,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || head_dim != 256u || blockDim.x != 256u || num_heads > 32u) return;
    const unsigned int head_block = blockIdx.x;
    if (head_block >= batch * num_heads) return;
    const unsigned int splits = 2u;
    __shared__ float merged_denom;
    __shared__ float merge_alpha[2];
    __shared__ float merge_beta[2];
    if (threadIdx.x == 0u) {
        float merged_max = -3.402823466e+38f;
        float denom = 0.0f;
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            const float local_denom = partial_denom[partial];
            if (local_denom > 0.0f) {
                const float next_max = fmaxf(merged_max, partial_max[partial]);
                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;
                const float beta = expf(partial_max[partial] - next_max);
                denom = denom * alpha + local_denom * beta;
                merged_max = next_max;
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        merged_denom = denom;
    }
    __syncthreads();
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = threadIdx.x + item * blockDim.x;
        float numerator = 0.0f;
#pragma unroll
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];
        }
        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;
    }
}

// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_split2_kv_hd512_f32_stage1_v1 plan_id=cuda/attention/decode_1x/hd512/gqa16/split2/min512/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_scalars_split2_hd512_f32_v1(
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }
    if (kv_seq_len >= split_kv_min_tokens) return;
    const unsigned int block = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        block >= num_heads || head_dim != 512u || blockDim.x != 512u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    const unsigned int lane = threadIdx.x;
    const unsigned int head = block;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            const float score = dot * scale;
            const float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        const unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void antfly_gqa_attention_decode_split2_kv_hd512_f32_stage1_v1(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        head_dim != 512u || blockDim.x != 512u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    const unsigned int splits = 2u;
    const unsigned int split = blockIdx.x % splits;
    const unsigned int head_block = blockIdx.x / splits;
    const unsigned int head = head_block % num_heads;
    const unsigned int b = head_block / num_heads;
    if (b >= batch) return;
    const unsigned int heads_per_kv = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_kv;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    unsigned int split_begin = (kv_seq_len * split) / splits;
    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;
    if (split_begin < key_start) split_begin = key_start;
    if (split_end > key_end) split_end = key_end;
    if (split_begin > split_end) split_begin = split_end;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    __shared__ float warp_sums[16];
    __shared__ float head_max;
    __shared__ float head_denom;
    __shared__ float head_alpha;
    __shared__ float head_beta;
    float q_values[1];
    float acc[1];
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        q_values[item] = q[q_base + d];
        acc[item] = 0.0f;
    }
    if (tid == 0u) {
        head_max = -3.402823466e+38f;
        head_denom = 0.0f;
        head_alpha = 0.0f;
        head_beta = 0.0f;
    }
    __syncthreads();

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int key_pos = kv_position_offset + ki;
        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        const bool future_blocked = key_pos > query_pos && !future_allowed;
        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        const bool valid = !(future_blocked || past_blocked);
        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;

        float dot = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float key_value = valid ? k[kv_base + d] : 0.0f;
            dot += q_values[item] * key_value;
        }
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        }
        if (lane == 0u) warp_sums[warp] = dot;
        __syncthreads();

        float block_dot = (warp == 0u && lane < 16u) ? warp_sums[lane] : 0.0f;
        if (warp == 0u) {
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
            }
            if (lane == 0u) {
                float score = valid ? block_dot * scale : -3.402823466e+38f;
                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
                const float next_max = fmaxf(head_max, score);
                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
                const float beta = valid ? expf(score - next_max) : 0.0f;
                head_denom = head_denom * alpha + beta;
                head_max = next_max;
                head_alpha = alpha;
                head_beta = beta;
            }
        }
        __syncthreads();

#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float value = valid ? v[kv_base + d] : 0.0f;
            acc[item] = acc[item] * head_alpha + head_beta * value;
        }
    }

    const unsigned int partial = (b * num_heads + head) * splits + split;
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        partial_values[(size_t)partial * head_dim + d] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = head_max;
        partial_denom[partial] = head_denom;
    }
}

extern "C" __global__ void antfly_gqa_attention_decode_split2_kv_hd512_f32_stage2_v1(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int batch,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || head_dim != 512u || blockDim.x != 512u || num_heads > 32u) return;
    const unsigned int head_block = blockIdx.x;
    if (head_block >= batch * num_heads) return;
    const unsigned int splits = 2u;
    __shared__ float merged_denom;
    __shared__ float merge_alpha[2];
    __shared__ float merge_beta[2];
    if (threadIdx.x == 0u) {
        float merged_max = -3.402823466e+38f;
        float denom = 0.0f;
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            const float local_denom = partial_denom[partial];
            if (local_denom > 0.0f) {
                const float next_max = fmaxf(merged_max, partial_max[partial]);
                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;
                const float beta = expf(partial_max[partial] - next_max);
                denom = denom * alpha + local_denom * beta;
                merged_max = next_max;
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        merged_denom = denom;
    }
    __syncthreads();
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = threadIdx.x + item * blockDim.x;
        float numerator = 0.0f;
#pragma unroll
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];
        }
        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;
    }
}

// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_split4_kv_hd256_f32_stage1_v1 plan_id=cuda/attention/decode_1x/hd256/gqa16/split4/min512/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_scalars_split4_hd256_f32_v1(
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }
    if (kv_seq_len >= split_kv_min_tokens) return;
    const unsigned int block = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        block >= num_heads || head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    const unsigned int lane = threadIdx.x;
    const unsigned int head = block;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            const float score = dot * scale;
            const float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        const unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void antfly_gqa_attention_decode_split4_kv_hd256_f32_stage1_v1(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        head_dim != 256u || blockDim.x != 256u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    const unsigned int splits = 4u;
    const unsigned int split = blockIdx.x % splits;
    const unsigned int head_block = blockIdx.x / splits;
    const unsigned int head = head_block % num_heads;
    const unsigned int b = head_block / num_heads;
    if (b >= batch) return;
    const unsigned int heads_per_kv = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_kv;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    unsigned int split_begin = (kv_seq_len * split) / splits;
    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;
    if (split_begin < key_start) split_begin = key_start;
    if (split_end > key_end) split_end = key_end;
    if (split_begin > split_end) split_begin = split_end;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    __shared__ float warp_sums[8];
    __shared__ float head_max;
    __shared__ float head_denom;
    __shared__ float head_alpha;
    __shared__ float head_beta;
    float q_values[1];
    float acc[1];
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        q_values[item] = q[q_base + d];
        acc[item] = 0.0f;
    }
    if (tid == 0u) {
        head_max = -3.402823466e+38f;
        head_denom = 0.0f;
        head_alpha = 0.0f;
        head_beta = 0.0f;
    }
    __syncthreads();

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int key_pos = kv_position_offset + ki;
        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        const bool future_blocked = key_pos > query_pos && !future_allowed;
        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        const bool valid = !(future_blocked || past_blocked);
        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;

        float dot = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float key_value = valid ? k[kv_base + d] : 0.0f;
            dot += q_values[item] * key_value;
        }
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        }
        if (lane == 0u) warp_sums[warp] = dot;
        __syncthreads();

        float block_dot = (warp == 0u && lane < 8u) ? warp_sums[lane] : 0.0f;
        if (warp == 0u) {
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
            }
            if (lane == 0u) {
                float score = valid ? block_dot * scale : -3.402823466e+38f;
                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
                const float next_max = fmaxf(head_max, score);
                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
                const float beta = valid ? expf(score - next_max) : 0.0f;
                head_denom = head_denom * alpha + beta;
                head_max = next_max;
                head_alpha = alpha;
                head_beta = beta;
            }
        }
        __syncthreads();

#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float value = valid ? v[kv_base + d] : 0.0f;
            acc[item] = acc[item] * head_alpha + head_beta * value;
        }
    }

    const unsigned int partial = (b * num_heads + head) * splits + split;
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        partial_values[(size_t)partial * head_dim + d] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = head_max;
        partial_denom[partial] = head_denom;
    }
}

extern "C" __global__ void antfly_gqa_attention_decode_split4_kv_hd256_f32_stage2_v1(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int batch,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || head_dim != 256u || blockDim.x != 256u || num_heads > 32u) return;
    const unsigned int head_block = blockIdx.x;
    if (head_block >= batch * num_heads) return;
    const unsigned int splits = 4u;
    __shared__ float merged_denom;
    __shared__ float merge_alpha[4];
    __shared__ float merge_beta[4];
    if (threadIdx.x == 0u) {
        float merged_max = -3.402823466e+38f;
        float denom = 0.0f;
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            const float local_denom = partial_denom[partial];
            if (local_denom > 0.0f) {
                const float next_max = fmaxf(merged_max, partial_max[partial]);
                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;
                const float beta = expf(partial_max[partial] - next_max);
                denom = denom * alpha + local_denom * beta;
                merged_max = next_max;
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        merged_denom = denom;
    }
    __syncthreads();
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = threadIdx.x + item * blockDim.x;
        float numerator = 0.0f;
#pragma unroll
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];
        }
        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;
    }
}

// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_split4_kv_hd512_f32_stage1_v1 plan_id=cuda/attention/decode_1x/hd512/gqa16/split4/min512/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_scalars_split4_hd512_f32_v1(
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
    }
    if (kv_seq_len >= split_kv_min_tokens) return;
    const unsigned int block = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        block >= num_heads || head_dim != 512u || blockDim.x != 512u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float warp_sums[16];
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    const unsigned int lane = threadIdx.x;
    const unsigned int head = block;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
    }
    __syncthreads();

    float acc = 0.0f;
    for (unsigned int ki = key_start; ki < key_end; ++ki) {
        float partial = 0.0f;
        if (lane < head_dim) {
            const unsigned int k_base = ki * kv_hidden + kv_head * head_dim;
            partial = q[q_base + lane] * k[k_base + lane];
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u) {
            const float score = dot * scale;
            const float next_max = fmaxf(shared_max_score, score);
            shared_alpha = expf(shared_max_score - next_max);
            shared_beta = expf(score - next_max);
            shared_denom = shared_denom * shared_alpha + shared_beta;
            shared_max_score = next_max;
        }
        __syncthreads();
        if (lane < head_dim) {
            const unsigned int v_idx = ki * kv_hidden + kv_head * head_dim + lane;
            acc = acc * shared_alpha + shared_beta * v[v_idx];
        }
        __syncthreads();
    }

    if (lane < head_dim) {
        const unsigned int out_idx = head * head_dim + lane;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }

    (void)attn_or_mask;
    (void)bias;
    (void)total_sequence_len;
}

extern "C" __global__ void antfly_gqa_attention_decode_split4_kv_hd512_f32_stage1_v1(
    float* partial_values,
    float* partial_max,
    float* partial_denom,
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
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || q_seq_len != 1u || mask_len != 0u || bias_mode != 0u ||
        head_dim != 512u || blockDim.x != 512u ||
        num_kv_heads == 0u || num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    const unsigned int splits = 4u;
    const unsigned int split = blockIdx.x % splits;
    const unsigned int head_block = blockIdx.x / splits;
    const unsigned int head = head_block % num_heads;
    const unsigned int b = head_block / num_heads;
    if (b >= batch) return;
    const unsigned int heads_per_kv = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_kv;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    unsigned int split_begin = (kv_seq_len * split) / splits;
    unsigned int split_end = (kv_seq_len * (split + 1u)) / splits;
    if (split_begin < key_start) split_begin = key_start;
    if (split_end > key_end) split_end = key_end;
    if (split_begin > split_end) split_begin = split_end;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int q_hidden = num_heads * head_dim;
    const unsigned int kv_hidden = num_kv_heads * head_dim;
    const unsigned int q_base = (b * q_seq_len) * q_hidden + head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    __shared__ float warp_sums[16];
    __shared__ float head_max;
    __shared__ float head_denom;
    __shared__ float head_alpha;
    __shared__ float head_beta;
    float q_values[1];
    float acc[1];
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        q_values[item] = q[q_base + d];
        acc[item] = 0.0f;
    }
    if (tid == 0u) {
        head_max = -3.402823466e+38f;
        head_denom = 0.0f;
        head_alpha = 0.0f;
        head_beta = 0.0f;
    }
    __syncthreads();

    for (unsigned int ki = split_begin; ki < split_end; ++ki) {
        const unsigned int key_pos = kv_position_offset + ki;
        const unsigned int mask_idx = query_pos * total_sequence_len + key_pos;
        const bool future_allowed = attn_or_mask != 0 && mask_idx < mask_len && attn_or_mask[mask_idx] != 0u;
        const bool future_blocked = key_pos > query_pos && !future_allowed;
        const bool past_blocked = key_pos > query_pos || (sliding_window != 0u && (query_pos - key_pos) >= sliding_window);
        const bool valid = !(future_blocked || past_blocked);
        const unsigned int kv_base = (b * kv_seq_len + ki) * kv_hidden + kv_head * head_dim;

        float dot = 0.0f;
#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float key_value = valid ? k[kv_base + d] : 0.0f;
            dot += q_values[item] * key_value;
        }
        for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
            dot += __shfl_down_sync(0xffffffffu, dot, offset);
        }
        if (lane == 0u) warp_sums[warp] = dot;
        __syncthreads();

        float block_dot = (warp == 0u && lane < 16u) ? warp_sums[lane] : 0.0f;
        if (warp == 0u) {
            for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {
                block_dot += __shfl_down_sync(0xffffffffu, block_dot, offset);
            }
            if (lane == 0u) {
                float score = valid ? block_dot * scale : -3.402823466e+38f;
                if (valid && bias_mode == 1u) score += bias[head * kv_seq_len + ki];
                if (valid && bias_mode == 2u) score += bias[(b * num_heads + head) * kv_seq_len + ki];
                const float next_max = fmaxf(head_max, score);
                const float alpha = head_denom > 0.0f ? expf(head_max - next_max) : 0.0f;
                const float beta = valid ? expf(score - next_max) : 0.0f;
                head_denom = head_denom * alpha + beta;
                head_max = next_max;
                head_alpha = alpha;
                head_beta = beta;
            }
        }
        __syncthreads();

#pragma unroll
        for (unsigned int item = 0u; item < 1u; ++item) {
            const unsigned int d = tid + item * blockDim.x;
            const float value = valid ? v[kv_base + d] : 0.0f;
            acc[item] = acc[item] * head_alpha + head_beta * value;
        }
    }

    const unsigned int partial = (b * num_heads + head) * splits + split;
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = tid + item * blockDim.x;
        partial_values[(size_t)partial * head_dim + d] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = head_max;
        partial_denom[partial] = head_denom;
    }
}

extern "C" __global__ void antfly_gqa_attention_decode_split4_kv_hd512_f32_stage2_v1(
    float* dst,
    const float* partial_values,
    const float* partial_max,
    const float* partial_denom,
    unsigned int batch,
    unsigned int num_heads,
    unsigned int head_dim,
    unsigned int kv_seq_len,
    unsigned int split_kv_min_tokens,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) kv_seq_len = decode_scalars[2];
    if (kv_seq_len < split_kv_min_tokens) return;
    if (batch != 1u || head_dim != 512u || blockDim.x != 512u || num_heads > 32u) return;
    const unsigned int head_block = blockIdx.x;
    if (head_block >= batch * num_heads) return;
    const unsigned int splits = 4u;
    __shared__ float merged_denom;
    __shared__ float merge_alpha[4];
    __shared__ float merge_beta[4];
    if (threadIdx.x == 0u) {
        float merged_max = -3.402823466e+38f;
        float denom = 0.0f;
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            const float local_denom = partial_denom[partial];
            if (local_denom > 0.0f) {
                const float next_max = fmaxf(merged_max, partial_max[partial]);
                const float alpha = denom > 0.0f ? expf(merged_max - next_max) : 0.0f;
                const float beta = expf(partial_max[partial] - next_max);
                denom = denom * alpha + local_denom * beta;
                merged_max = next_max;
                merge_alpha[split] = alpha;
                merge_beta[split] = beta;
            } else {
                merge_alpha[split] = 1.0f;
                merge_beta[split] = 0.0f;
            }
        }
        merged_denom = denom;
    }
    __syncthreads();
#pragma unroll
    for (unsigned int item = 0u; item < 1u; ++item) {
        const unsigned int d = threadIdx.x + item * blockDim.x;
        float numerator = 0.0f;
#pragma unroll
        for (unsigned int split = 0u; split < splits; ++split) {
            const unsigned int partial = head_block * splits + split;
            numerator = numerator * merge_alpha[split] + partial_values[(size_t)partial * head_dim + d] * merge_beta[split];
        }
        dst[(size_t)head_block * head_dim + d] = merged_denom > 0.0f ? numerator / merged_denom : 0.0f;
    }
}

// Production generated attention route from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1 plan_id=cuda/attention/decode_1x/hd256/gqa16/score-prework/max4096/consumers-serial+tiled64-max512/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_hd256_f32_v1(
    float* scores,
    const float* q,
    const unsigned char* k,
    const unsigned int* block_table,
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
    unsigned int key_row_bytes,
    unsigned int base_key_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    unsigned int chunk_size,
    unsigned int chunk_count,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (batch != 1u || q_seq_len != 1u || head_dim != 256u || blockDim.x != 256u ||
        score_capacity == 0u || score_capacity > 4096u || chunk_size == 0u ||
        chunk_count != 128u || key_row_bytes == 0u || base_key_row_bytes != key_row_bytes ||
        (format != 0u && format != 2u) || num_kv_heads == 0u || num_heads == 0u ||
        (num_heads % num_kv_heads) != 0u || (num_heads / num_kv_heads) > 16u ||
        num_heads > 32u) return;
    const unsigned int block = blockIdx.x;
    const unsigned int head = block / chunk_count;
    const unsigned int chunk = block - head * chunk_count;
    if (head >= num_heads) return;
    __shared__ float warp_sums[8];
    const unsigned int lane = threadIdx.x;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int visible_count = key_end - key_start;
    if (visible_count > score_capacity) return;
    unsigned int chunk_begin = key_start + chunk * chunk_size;
    unsigned int chunk_end = chunk_begin + chunk_size;
    if (chunk_end > key_end) chunk_end = key_end;
    if (chunk_begin >= chunk_end) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    for (unsigned int ki = chunk_begin; ki < chunk_end; ++ki) {
        const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        const bool valid = physical_token != 0xffffffffu;
        const unsigned char* k_row = valid ? k + (size_t)physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            const unsigned int value_index = kv_head * head_dim + lane;
            const float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u && valid) scores[(size_t)head * score_capacity + (ki - key_start)] = dot * scale;
    }
    (void)total_sequence_len;
}


extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_serial_hd256_f32_v1(
    float* dst,
    const float* scores,
    const unsigned char* v,
    const unsigned int* block_table,
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
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    const unsigned int head = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != 256u || blockDim.x != 256u || value_row_bytes == 0u ||
        (value_format != 0u && value_format != 2u) || score_capacity == 0u || score_capacity > 4096u || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[4096];
    __shared__ float shared_beta[4096];
    const unsigned int lane = threadIdx.x;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    if (key_end - key_start > score_capacity) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            if (valid) {
                const float score = scores[(size_t)head * score_capacity + (ki - key_start)];
                const float next_max = fmaxf(shared_max_score, score);
                shared_alpha[recurrence_index] = expf(shared_max_score - next_max);
                shared_beta[recurrence_index] = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];
                shared_max_score = next_max;
            } else {
                shared_alpha[recurrence_index] = 1.0f;
                shared_beta[recurrence_index] = 0.0f;
            }
        }
    }
    __syncthreads();
    float acc = 0.0f;
    if (lane < head_dim) {
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            acc *= shared_alpha[recurrence_index];
            if (valid) {
                const unsigned char* v_row = v + (size_t)physical_token * value_row_bytes;
                const unsigned int value_index = kv_head * head_dim + lane;
                const float value = value_format == 0u
                    ? reinterpret_cast<const float*>(v_row)[value_index]
                    : termite_tq_f16_value(v_row, value_index);
                acc += shared_beta[recurrence_index] * value;
            }
        }
    }
    if (lane < head_dim) dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    (void)total_sequence_len;
}


extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd256_f32_v1(
    float* dst,
    const float* scores,
    const unsigned char* v,
    const unsigned int* block_table,
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
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    const unsigned int head = blockIdx.x;
    const unsigned int output_tile = blockIdx.y;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != 256u || blockDim.x != 64u || output_tile >= head_dim / 64u ||
        value_row_bytes == 0u || (value_format != 0u && value_format != 2u) ||
        score_capacity == 0u || score_capacity > 512u || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[512];
    __shared__ float shared_beta[512];
    const unsigned int tile_lane = threadIdx.x;
    const unsigned int lane = output_tile * 64u + tile_lane;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    if (key_end - key_start > score_capacity) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    if (tile_lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            if (valid) {
                const float score = scores[(size_t)head * score_capacity + (ki - key_start)];
                const float next_max = fmaxf(shared_max_score, score);
                shared_alpha[recurrence_index] = expf(shared_max_score - next_max);
                shared_beta[recurrence_index] = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];
                shared_max_score = next_max;
            } else {
                shared_alpha[recurrence_index] = 1.0f;
                shared_beta[recurrence_index] = 0.0f;
            }
        }
    }
    __syncthreads();
    float acc = 0.0f;
    if (lane < head_dim) {
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            acc *= shared_alpha[recurrence_index];
            if (valid) {
                const unsigned char* v_row = v + (size_t)physical_token * value_row_bytes;
                const unsigned int value_index = kv_head * head_dim + lane;
                const float value = value_format == 0u
                    ? reinterpret_cast<const float*>(v_row)[value_index]
                    : termite_tq_f16_value(v_row, value_index);
                acc += shared_beta[recurrence_index] * value;
            }
        }
    }
    if (lane < head_dim) dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    (void)total_sequence_len;
}

// Production generated attention route from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_turboquant_score_prework_hd512_f32_v1 plan_id=cuda/attention/decode_1x/hd512/gqa16/score-prework/max4096/consumers-serial+tiled64-max4096/f32/device_scalars
extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_hd512_f32_v1(
    float* scores,
    const float* q,
    const unsigned char* k,
    const unsigned int* block_table,
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
    unsigned int key_row_bytes,
    unsigned int base_key_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    unsigned int chunk_size,
    unsigned int chunk_count,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    if (batch != 1u || q_seq_len != 1u || head_dim != 512u || blockDim.x != 512u ||
        score_capacity == 0u || score_capacity > 4096u || chunk_size == 0u ||
        chunk_count != 128u || key_row_bytes == 0u || base_key_row_bytes != key_row_bytes ||
        (format != 0u && format != 2u) || num_kv_heads == 0u || num_heads == 0u ||
        (num_heads % num_kv_heads) != 0u || (num_heads / num_kv_heads) > 16u ||
        num_heads > 32u) return;
    const unsigned int block = blockIdx.x;
    const unsigned int head = block / chunk_count;
    const unsigned int chunk = block - head * chunk_count;
    if (head >= num_heads) return;
    __shared__ float warp_sums[16];
    const unsigned int lane = threadIdx.x;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned int visible_count = key_end - key_start;
    if (visible_count > score_capacity) return;
    unsigned int chunk_begin = key_start + chunk * chunk_size;
    unsigned int chunk_end = chunk_begin + chunk_size;
    if (chunk_end > key_end) chunk_end = key_end;
    if (chunk_begin >= chunk_end) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    const unsigned int q_base = head * head_dim;
    const float scale_input = (float)head_dim;
    float scale;
    asm volatile ("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));
    for (unsigned int ki = chunk_begin; ki < chunk_end; ++ki) {
        const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
        const bool valid = physical_token != 0xffffffffu;
        const unsigned char* k_row = valid ? k + (size_t)physical_token * key_row_bytes : k;
        float partial = 0.0f;
        if (valid && lane < head_dim) {
            const unsigned int value_index = kv_head * head_dim + lane;
            const float key_value = format == 0u
                ? termite_tq_decode_polar4_at(k_row, value_index)
                : termite_tq_f16_value(k_row, value_index);
            partial = q[q_base + lane] * key_value;
        }
        const float dot = termite_block_reduce_sum_f32(partial, warp_sums);
        if (lane == 0u && valid) scores[(size_t)head * score_capacity + (ki - key_start)] = dot * scale;
    }
    (void)total_sequence_len;
}


extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_serial_hd512_f32_v1(
    float* dst,
    const float* scores,
    const unsigned char* v,
    const unsigned int* block_table,
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
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    const unsigned int head = blockIdx.x;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != 512u || blockDim.x != 512u || value_row_bytes == 0u ||
        (value_format != 0u && value_format != 2u) || score_capacity == 0u || score_capacity > 4096u || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[4096];
    __shared__ float shared_beta[4096];
    const unsigned int lane = threadIdx.x;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    if (key_end - key_start > score_capacity) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    if (lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            if (valid) {
                const float score = scores[(size_t)head * score_capacity + (ki - key_start)];
                const float next_max = fmaxf(shared_max_score, score);
                shared_alpha[recurrence_index] = expf(shared_max_score - next_max);
                shared_beta[recurrence_index] = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];
                shared_max_score = next_max;
            } else {
                shared_alpha[recurrence_index] = 1.0f;
                shared_beta[recurrence_index] = 0.0f;
            }
        }
    }
    __syncthreads();
    float acc = 0.0f;
    if (lane < head_dim) {
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            acc *= shared_alpha[recurrence_index];
            if (valid) {
                const unsigned char* v_row = v + (size_t)physical_token * value_row_bytes;
                const unsigned int value_index = kv_head * head_dim + lane;
                const float value = value_format == 0u
                    ? reinterpret_cast<const float*>(v_row)[value_index]
                    : termite_tq_f16_value(v_row, value_index);
                acc += shared_beta[recurrence_index] * value;
            }
        }
    }
    if (lane < head_dim) dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    (void)total_sequence_len;
}


extern "C" __global__ void antfly_gqa_attention_decode_turboquant_score_prework_tiled64_hd512_f32_v1(
    float* dst,
    const float* scores,
    const unsigned char* v,
    const unsigned int* block_table,
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
    unsigned int value_row_bytes,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int value_format,
    unsigned int physical_token_capacity,
    unsigned int score_capacity,
    const unsigned int* decode_scalars
) {
    if (decode_scalars != 0) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    const unsigned int head = blockIdx.x;
    const unsigned int output_tile = blockIdx.y;
    if (batch != 1u || q_seq_len != 1u || head >= num_heads ||
        head_dim != 512u || blockDim.x != 64u || output_tile >= head_dim / 64u ||
        value_row_bytes == 0u || (value_format != 0u && value_format != 2u) ||
        score_capacity == 0u || score_capacity > 4096u || num_kv_heads == 0u ||
        num_heads == 0u || (num_heads % num_kv_heads) != 0u ||
        (num_heads / num_kv_heads) > 16u || num_heads > 32u) return;
    __shared__ float shared_max_score;
    __shared__ float shared_denom;
    __shared__ float shared_alpha[4096];
    __shared__ float shared_beta[4096];
    const unsigned int tile_lane = threadIdx.x;
    const unsigned int lane = output_tile * 64u + tile_lane;
    const unsigned int query_pos = query_position_offset;
    unsigned int key_start = 0u;
    unsigned int key_end = 0u;
    if (kv_seq_len != 0u && query_pos >= kv_position_offset) {
        const unsigned int visible = query_pos - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (sliding_window != 0u) {
            const unsigned int window_start_abs = (query_pos + 1u > sliding_window) ? (query_pos + 1u - sliding_window) : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    if (key_start > key_end) key_start = key_end;
    if (key_end - key_start > score_capacity) return;
    const unsigned int heads_per_group = num_heads / num_kv_heads;
    const unsigned int kv_head = head / heads_per_group;
    if (tile_lane == 0u) {
        shared_max_score = -3.402823466e+38f;
        shared_denom = 0.0f;
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            if (valid) {
                const float score = scores[(size_t)head * score_capacity + (ki - key_start)];
                const float next_max = fmaxf(shared_max_score, score);
                shared_alpha[recurrence_index] = expf(shared_max_score - next_max);
                shared_beta[recurrence_index] = expf(score - next_max);
                shared_denom = shared_denom * shared_alpha[recurrence_index] + shared_beta[recurrence_index];
                shared_max_score = next_max;
            } else {
                shared_alpha[recurrence_index] = 1.0f;
                shared_beta[recurrence_index] = 0.0f;
            }
        }
    }
    __syncthreads();
    float acc = 0.0f;
    if (lane < head_dim) {
        for (unsigned int ki = key_start; ki < key_end; ++ki) {
            const unsigned int physical_token = termite_tq_physical_token(ki, block_table, block_count, page_size_tokens, physical_token_capacity);
            const bool valid = physical_token != 0xffffffffu;
            const unsigned int recurrence_index = ki - key_start;
            acc *= shared_alpha[recurrence_index];
            if (valid) {
                const unsigned char* v_row = v + (size_t)physical_token * value_row_bytes;
                const unsigned int value_index = kv_head * head_dim + lane;
                const float value = value_format == 0u
                    ? reinterpret_cast<const float*>(v_row)[value_index]
                    : termite_tq_f16_value(v_row, value_index);
                acc += shared_beta[recurrence_index] * value;
            }
        }
    }
    if (lane < head_dim) dst[head * head_dim + lane] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    (void)total_sequence_len;
}

// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_splitk_online_sm89_hd256_swa512_f16_f32_v1 plan_id=cuda/attention/decode_1x/splitk-online/sm89/hd256/gqa8/split64/t128/page16/swa512/f32q-f16kv-f32o
#define ANTFLY_SPLITK_ONLINE_NAMESPACE antfly_splitk_online_decode_sm89_hd256_swa512_f16_f32_v1
#define ANTFLY_SPLITK_ONLINE_KERNEL antfly_gqa_attention_decode_splitk_online_sm89_hd256_swa512_f16_f32_v1
#define ANTFLY_SPLITK_ONLINE_HEAD_DIM 256
#define ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW 512
#define ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS 512
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

// Typed, default-off SM89 q=1 paged-GQA split-K online-softmax template.
//
// One CTA owns one (query head, chronological KV split). It publishes a stable
// online-softmax partial into a persistent, route-exclusive workspace. The
// last arriving CTA for each head observes all partials after `__threadfence`,
// merges them in fixed chronological split order, writes F32 output, and
// re-arms the head-local completion counter. The same stream-ordering contract
// works for ordinary launches and CUDA graph replay without host-side memset.

#include <cuda_fp16.h>
#include <float.h>
#include <stddef.h>
#include <stdint.h>

#ifndef ANTFLY_SPLITK_ONLINE_NAMESPACE
#error "ANTFLY_SPLITK_ONLINE_NAMESPACE must name a unique renderer-owned namespace"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_KERNEL
#error "ANTFLY_SPLITK_ONLINE_KERNEL must name the exported production entry point"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_HEAD_DIM
#error "ANTFLY_SPLITK_ONLINE_HEAD_DIM must be 256 or 512"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW
#error "ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW must be 512 for HD256 or 0 for HD512"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS
#error "ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS must match the qualified policy"
#endif

namespace ANTFLY_SPLITK_ONLINE_NAMESPACE {

constexpr unsigned kHeadDim = ANTFLY_SPLITK_ONLINE_HEAD_DIM;
constexpr unsigned kSlidingWindow = ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW;
constexpr unsigned kMaxVisibleTokens = ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS;
constexpr unsigned kHeads = 8u;
constexpr unsigned kKvHeads = 1u;
constexpr unsigned kSplits = 64u;
constexpr unsigned kThreads = 128u;
constexpr unsigned kPageTokens = 16u;
constexpr unsigned kF16Format = 2u;
constexpr unsigned kInvalidToken = 0xffffffffu;
constexpr unsigned kMaxItems = kHeadDim / kThreads;

__device__ __forceinline__ float warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float block_reduce_sum_f32(
    float value,
    float* warp_sums
) {
    const unsigned lane = threadIdx.x & 31u;
    const unsigned warp = threadIdx.x >> 5u;
    value = warp_reduce_sum_f32(value);
    if (lane == 0u) warp_sums[warp] = value;
    __syncthreads();
    value = (warp == 0u && lane < kThreads / 32u)
        ? warp_sums[lane]
        : 0.0f;
    if (warp == 0u) value = warp_reduce_sum_f32(value);
    return value;
}

__device__ __forceinline__ unsigned physical_token(
    unsigned logical_token,
    const unsigned* block_table,
    unsigned block_count,
    unsigned physical_token_capacity
) {
    if (block_table == nullptr) {
        return logical_token < physical_token_capacity
            ? logical_token
            : kInvalidToken;
    }
    const unsigned block_index = logical_token / kPageTokens;
    if (block_index >= block_count) return kInvalidToken;
    const unsigned token_offset = logical_token - block_index * kPageTokens;
    const unsigned long long physical =
        (unsigned long long)block_table[block_index] * kPageTokens +
        token_offset;
    if (physical >= (unsigned long long)physical_token_capacity) {
        return kInvalidToken;
    }
    return (unsigned)physical;
}

__device__ __forceinline__ float f16_value(
    const unsigned char* row,
    unsigned index
) {
    return __half2float(reinterpret_cast<const __half*>(row)[index]);
}

}  // namespace ANTFLY_SPLITK_ONLINE_NAMESPACE

extern "C" __global__ void ANTFLY_SPLITK_ONLINE_KERNEL(
    float* dst,
    unsigned* completion_counters,
    // Volatile is part of the cross-CTA publication contract: after each
    // producer's __threadfence and atomic counter increment, the last CTA must
    // reload globally visible partials rather than satisfy reads from stale L1.
    volatile float* partial_values,
    volatile float* partial_max,
    volatile float* partial_denom,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned key_format,
    unsigned value_format,
    unsigned physical_token_capacity,
    unsigned score_capacity,
    const unsigned* decode_scalars
) {
    using namespace ANTFLY_SPLITK_ONLINE_NAMESPACE;
    if (decode_scalars != nullptr) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    constexpr unsigned kRowBytes = kHeadDim * sizeof(__half);
    if (dst == nullptr || completion_counters == nullptr ||
        partial_values == nullptr || partial_max == nullptr ||
        partial_denom == nullptr || q == nullptr || k == nullptr ||
        v == nullptr || batch != 1u ||
        q_seq_len != 1u || num_heads != kHeads ||
        num_kv_heads != kKvHeads || head_dim != kHeadDim ||
        blockDim.x != kThreads || sliding_window != kSlidingWindow ||
        key_row_bytes != kRowBytes || base_key_row_bytes != kRowBytes ||
        value_row_bytes != kRowBytes ||
        ((block_table == nullptr) != (block_count == 0u)) ||
        page_size_tokens != kPageTokens || key_format != kF16Format ||
        value_format != kF16Format || physical_token_capacity == 0u ||
        physical_token_capacity % kPageTokens != 0u ||
        score_capacity != kMaxVisibleTokens) {
        return;
    }

    const unsigned block = blockIdx.x;
    const unsigned head = block / kSplits;
    const unsigned split = block - head * kSplits;
    if (head >= kHeads) return;

    unsigned key_start = 0u;
    unsigned key_end = 0u;
    if (kv_seq_len != 0u && query_position_offset >= kv_position_offset) {
        const unsigned visible = query_position_offset - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (kSlidingWindow != 0u) {
            const unsigned window_start_abs =
                query_position_offset + 1u > kSlidingWindow
                ? query_position_offset + 1u - kSlidingWindow
                : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned visible_count = key_end - key_start;
    // Host admission rejects dynamic replay scalars outside the qualified
    // range before graph launch. Still let every CTA publish an empty partial
    // if an invariant is ever violated: the last-CTA protocol then writes a
    // deterministic zero row and re-arms the counter instead of leaving stale
    // output or a partially advanced completion counter.
    const bool scalar_contract_valid =
        kv_seq_len != 0u && kv_seq_len <= 4096u &&
        total_sequence_len != 0u &&
        total_sequence_len <= 4096u &&
        query_position_offset != 0xffffffffu &&
        query_position_offset + 1u == total_sequence_len &&
        kv_position_offset <= total_sequence_len &&
        kv_seq_len == total_sequence_len - kv_position_offset &&
        (block_table != nullptr || kv_seq_len <= physical_token_capacity) &&
        (block_table == nullptr ||
            block_count >= (kv_seq_len + kPageTokens - 1u) / kPageTokens);
    const bool visible_valid = scalar_contract_valid &&
        visible_count != 0u && visible_count <= kMaxVisibleTokens;
    const unsigned split_begin = visible_valid
        ? key_start +
            (unsigned)(((unsigned long long)visible_count * split) / kSplits)
        : 0u;
    const unsigned split_end = visible_valid
        ? key_start +
            (unsigned)(((unsigned long long)visible_count * (split + 1u)) / kSplits)
        : 0u;
    const unsigned tid = threadIdx.x;

    float q_items[kMaxItems];
    float acc[kMaxItems];
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        q_items[item] = q[head * kHeadDim + dimension];
        acc[item] = 0.0f;
    }

    __shared__ float warp_sums[kThreads / 32u];
    __shared__ float running_max;
    __shared__ float running_denom;
    __shared__ float alpha;
    __shared__ float beta;
    __shared__ float scale_input;
    if (tid == 0u) {
        running_max = -FLT_MAX;
        running_denom = 0.0f;
        scale_input = (float)kHeadDim;
    }
    __syncthreads();
    float scale;
    asm volatile("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    for (unsigned logical_token = split_begin;
         logical_token < split_end;
         ++logical_token) {
        const unsigned token = physical_token(
            logical_token, block_table, block_count, physical_token_capacity);
        const bool valid = token != kInvalidToken;
        const unsigned char* k_row = valid
            ? k + (size_t)token * key_row_bytes
            : k;
        float dot_item = 0.0f;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            if (valid) {
                dot_item += q_items[item] * f16_value(k_row, dimension);
            }
        }
        const float dot = block_reduce_sum_f32(dot_item, warp_sums);
        if (tid == 0u) {
            if (valid) {
                const float score = dot * scale;
                const float next_max = fmaxf(running_max, score);
                alpha = running_denom > 0.0f
                    ? expf(running_max - next_max)
                    : 0.0f;
                beta = expf(score - next_max);
                running_denom = running_denom * alpha + beta;
                running_max = next_max;
            } else {
                alpha = 1.0f;
                beta = 0.0f;
            }
        }
        __syncthreads();
        const unsigned char* v_row = valid
            ? v + (size_t)token * value_row_bytes
            : v;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            const float value = valid ? f16_value(v_row, dimension) : 0.0f;
            acc[item] = acc[item] * alpha + beta * value;
        }
    }

    const size_t partial = (size_t)head * kSplits + split;
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        partial_values[partial * kHeadDim + dimension] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = running_max;
        partial_denom[partial] = running_denom;
    }
    __threadfence();
    __syncthreads();

    __shared__ unsigned is_last_split;
    if (tid == 0u) {
        is_last_split = atomicAdd(&completion_counters[head], 1u) == kSplits - 1u;
    }
    __syncthreads();
    if (!is_last_split) return;

    __shared__ float merge_alpha[kSplits];
    __shared__ float merge_beta[kSplits];
    __shared__ float final_denom;
    if (tid == 0u) {
        float merge_max = -FLT_MAX;
        float merge_denom = 0.0f;
        for (unsigned merge_split = 0u;
             merge_split < kSplits;
             ++merge_split) {
            const size_t merge_partial = (size_t)head * kSplits + merge_split;
            const float split_denom = partial_denom[merge_partial];
            if (split_denom > 0.0f) {
                const float split_max = partial_max[merge_partial];
                const float next_max = fmaxf(merge_max, split_max);
                const float merge_a = merge_denom > 0.0f
                    ? expf(merge_max - next_max)
                    : 0.0f;
                const float merge_b = expf(split_max - next_max);
                merge_alpha[merge_split] = merge_a;
                merge_beta[merge_split] = merge_b;
                merge_denom = merge_denom * merge_a + split_denom * merge_b;
                merge_max = next_max;
            } else {
                merge_alpha[merge_split] = 1.0f;
                merge_beta[merge_split] = 0.0f;
            }
        }
        final_denom = merge_denom;
    }
    __syncthreads();

    float merge_acc[kMaxItems];
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) merge_acc[item] = 0.0f;
    for (unsigned merge_split = 0u;
         merge_split < kSplits;
         ++merge_split) {
        const size_t merge_partial = (size_t)head * kSplits + merge_split;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            merge_acc[item] = merge_acc[item] * merge_alpha[merge_split] +
                merge_beta[merge_split] *
                    partial_values[merge_partial * kHeadDim + dimension];
        }
    }
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        dst[head * kHeadDim + dimension] = final_denom > 0.0f
            ? merge_acc[item] / final_denom
            : 0.0f;
    }
    if (tid == 0u) atomicExch(&completion_counters[head], 0u);
    asm volatile("" : : "r"(total_sequence_len));
}
#undef ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS
#undef ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW
#undef ANTFLY_SPLITK_ONLINE_HEAD_DIM
#undef ANTFLY_SPLITK_ONLINE_KERNEL
#undef ANTFLY_SPLITK_ONLINE_NAMESPACE

// Opt-in generated attention candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1 plan_id=cuda/attention/decode_1x/splitk-online/sm89/hd512/gqa8/split64/t128/page16/swa0/f32q-f16kv-f32o
#define ANTFLY_SPLITK_ONLINE_NAMESPACE antfly_splitk_online_decode_sm89_hd512_global_f16_f32_v1
#define ANTFLY_SPLITK_ONLINE_KERNEL antfly_gqa_attention_decode_splitk_online_sm89_hd512_global_f16_f32_v1
#define ANTFLY_SPLITK_ONLINE_HEAD_DIM 512
#define ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW 0
#define ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS 4096
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

// Typed, default-off SM89 q=1 paged-GQA split-K online-softmax template.
//
// One CTA owns one (query head, chronological KV split). It publishes a stable
// online-softmax partial into a persistent, route-exclusive workspace. The
// last arriving CTA for each head observes all partials after `__threadfence`,
// merges them in fixed chronological split order, writes F32 output, and
// re-arms the head-local completion counter. The same stream-ordering contract
// works for ordinary launches and CUDA graph replay without host-side memset.

#include <cuda_fp16.h>
#include <float.h>
#include <stddef.h>
#include <stdint.h>

#ifndef ANTFLY_SPLITK_ONLINE_NAMESPACE
#error "ANTFLY_SPLITK_ONLINE_NAMESPACE must name a unique renderer-owned namespace"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_KERNEL
#error "ANTFLY_SPLITK_ONLINE_KERNEL must name the exported production entry point"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_HEAD_DIM
#error "ANTFLY_SPLITK_ONLINE_HEAD_DIM must be 256 or 512"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW
#error "ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW must be 512 for HD256 or 0 for HD512"
#endif
#ifndef ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS
#error "ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS must match the qualified policy"
#endif

namespace ANTFLY_SPLITK_ONLINE_NAMESPACE {

constexpr unsigned kHeadDim = ANTFLY_SPLITK_ONLINE_HEAD_DIM;
constexpr unsigned kSlidingWindow = ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW;
constexpr unsigned kMaxVisibleTokens = ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS;
constexpr unsigned kHeads = 8u;
constexpr unsigned kKvHeads = 1u;
constexpr unsigned kSplits = 64u;
constexpr unsigned kThreads = 128u;
constexpr unsigned kPageTokens = 16u;
constexpr unsigned kF16Format = 2u;
constexpr unsigned kInvalidToken = 0xffffffffu;
constexpr unsigned kMaxItems = kHeadDim / kThreads;

__device__ __forceinline__ float warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

__device__ __forceinline__ float block_reduce_sum_f32(
    float value,
    float* warp_sums
) {
    const unsigned lane = threadIdx.x & 31u;
    const unsigned warp = threadIdx.x >> 5u;
    value = warp_reduce_sum_f32(value);
    if (lane == 0u) warp_sums[warp] = value;
    __syncthreads();
    value = (warp == 0u && lane < kThreads / 32u)
        ? warp_sums[lane]
        : 0.0f;
    if (warp == 0u) value = warp_reduce_sum_f32(value);
    return value;
}

__device__ __forceinline__ unsigned physical_token(
    unsigned logical_token,
    const unsigned* block_table,
    unsigned block_count,
    unsigned physical_token_capacity
) {
    if (block_table == nullptr) {
        return logical_token < physical_token_capacity
            ? logical_token
            : kInvalidToken;
    }
    const unsigned block_index = logical_token / kPageTokens;
    if (block_index >= block_count) return kInvalidToken;
    const unsigned token_offset = logical_token - block_index * kPageTokens;
    const unsigned long long physical =
        (unsigned long long)block_table[block_index] * kPageTokens +
        token_offset;
    if (physical >= (unsigned long long)physical_token_capacity) {
        return kInvalidToken;
    }
    return (unsigned)physical;
}

__device__ __forceinline__ float f16_value(
    const unsigned char* row,
    unsigned index
) {
    return __half2float(reinterpret_cast<const __half*>(row)[index]);
}

}  // namespace ANTFLY_SPLITK_ONLINE_NAMESPACE

extern "C" __global__ void ANTFLY_SPLITK_ONLINE_KERNEL(
    float* dst,
    unsigned* completion_counters,
    // Volatile is part of the cross-CTA publication contract: after each
    // producer's __threadfence and atomic counter increment, the last CTA must
    // reload globally visible partials rather than satisfy reads from stale L1.
    volatile float* partial_values,
    volatile float* partial_max,
    volatile float* partial_denom,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned key_format,
    unsigned value_format,
    unsigned physical_token_capacity,
    unsigned score_capacity,
    const unsigned* decode_scalars
) {
    using namespace ANTFLY_SPLITK_ONLINE_NAMESPACE;
    if (decode_scalars != nullptr) {
        kv_position_offset = decode_scalars[4];
        query_position_offset = decode_scalars[1];
        kv_seq_len = decode_scalars[2];
        total_sequence_len = decode_scalars[3];
    }
    constexpr unsigned kRowBytes = kHeadDim * sizeof(__half);
    if (dst == nullptr || completion_counters == nullptr ||
        partial_values == nullptr || partial_max == nullptr ||
        partial_denom == nullptr || q == nullptr || k == nullptr ||
        v == nullptr || batch != 1u ||
        q_seq_len != 1u || num_heads != kHeads ||
        num_kv_heads != kKvHeads || head_dim != kHeadDim ||
        blockDim.x != kThreads || sliding_window != kSlidingWindow ||
        key_row_bytes != kRowBytes || base_key_row_bytes != kRowBytes ||
        value_row_bytes != kRowBytes ||
        ((block_table == nullptr) != (block_count == 0u)) ||
        page_size_tokens != kPageTokens || key_format != kF16Format ||
        value_format != kF16Format || physical_token_capacity == 0u ||
        physical_token_capacity % kPageTokens != 0u ||
        score_capacity != kMaxVisibleTokens) {
        return;
    }

    const unsigned block = blockIdx.x;
    const unsigned head = block / kSplits;
    const unsigned split = block - head * kSplits;
    if (head >= kHeads) return;

    unsigned key_start = 0u;
    unsigned key_end = 0u;
    if (kv_seq_len != 0u && query_position_offset >= kv_position_offset) {
        const unsigned visible = query_position_offset - kv_position_offset + 1u;
        key_end = visible < kv_seq_len ? visible : kv_seq_len;
        if (kSlidingWindow != 0u) {
            const unsigned window_start_abs =
                query_position_offset + 1u > kSlidingWindow
                ? query_position_offset + 1u - kSlidingWindow
                : 0u;
            if (window_start_abs > kv_position_offset) {
                key_start = window_start_abs - kv_position_offset;
                if (key_start > key_end) key_start = key_end;
            }
        }
    }
    const unsigned visible_count = key_end - key_start;
    // Host admission rejects dynamic replay scalars outside the qualified
    // range before graph launch. Still let every CTA publish an empty partial
    // if an invariant is ever violated: the last-CTA protocol then writes a
    // deterministic zero row and re-arms the counter instead of leaving stale
    // output or a partially advanced completion counter.
    const bool scalar_contract_valid =
        kv_seq_len != 0u && kv_seq_len <= 4096u &&
        total_sequence_len != 0u &&
        total_sequence_len <= 4096u &&
        query_position_offset != 0xffffffffu &&
        query_position_offset + 1u == total_sequence_len &&
        kv_position_offset <= total_sequence_len &&
        kv_seq_len == total_sequence_len - kv_position_offset &&
        (block_table != nullptr || kv_seq_len <= physical_token_capacity) &&
        (block_table == nullptr ||
            block_count >= (kv_seq_len + kPageTokens - 1u) / kPageTokens);
    const bool visible_valid = scalar_contract_valid &&
        visible_count != 0u && visible_count <= kMaxVisibleTokens;
    const unsigned split_begin = visible_valid
        ? key_start +
            (unsigned)(((unsigned long long)visible_count * split) / kSplits)
        : 0u;
    const unsigned split_end = visible_valid
        ? key_start +
            (unsigned)(((unsigned long long)visible_count * (split + 1u)) / kSplits)
        : 0u;
    const unsigned tid = threadIdx.x;

    float q_items[kMaxItems];
    float acc[kMaxItems];
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        q_items[item] = q[head * kHeadDim + dimension];
        acc[item] = 0.0f;
    }

    __shared__ float warp_sums[kThreads / 32u];
    __shared__ float running_max;
    __shared__ float running_denom;
    __shared__ float alpha;
    __shared__ float beta;
    __shared__ float scale_input;
    if (tid == 0u) {
        running_max = -FLT_MAX;
        running_denom = 0.0f;
        scale_input = (float)kHeadDim;
    }
    __syncthreads();
    float scale;
    asm volatile("rsqrt.approx.f32 %0, %1;" : "=f"(scale) : "f"(scale_input));

    for (unsigned logical_token = split_begin;
         logical_token < split_end;
         ++logical_token) {
        const unsigned token = physical_token(
            logical_token, block_table, block_count, physical_token_capacity);
        const bool valid = token != kInvalidToken;
        const unsigned char* k_row = valid
            ? k + (size_t)token * key_row_bytes
            : k;
        float dot_item = 0.0f;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            if (valid) {
                dot_item += q_items[item] * f16_value(k_row, dimension);
            }
        }
        const float dot = block_reduce_sum_f32(dot_item, warp_sums);
        if (tid == 0u) {
            if (valid) {
                const float score = dot * scale;
                const float next_max = fmaxf(running_max, score);
                alpha = running_denom > 0.0f
                    ? expf(running_max - next_max)
                    : 0.0f;
                beta = expf(score - next_max);
                running_denom = running_denom * alpha + beta;
                running_max = next_max;
            } else {
                alpha = 1.0f;
                beta = 0.0f;
            }
        }
        __syncthreads();
        const unsigned char* v_row = valid
            ? v + (size_t)token * value_row_bytes
            : v;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            const float value = valid ? f16_value(v_row, dimension) : 0.0f;
            acc[item] = acc[item] * alpha + beta * value;
        }
    }

    const size_t partial = (size_t)head * kSplits + split;
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        partial_values[partial * kHeadDim + dimension] = acc[item];
    }
    if (tid == 0u) {
        partial_max[partial] = running_max;
        partial_denom[partial] = running_denom;
    }
    __threadfence();
    __syncthreads();

    __shared__ unsigned is_last_split;
    if (tid == 0u) {
        is_last_split = atomicAdd(&completion_counters[head], 1u) == kSplits - 1u;
    }
    __syncthreads();
    if (!is_last_split) return;

    __shared__ float merge_alpha[kSplits];
    __shared__ float merge_beta[kSplits];
    __shared__ float final_denom;
    if (tid == 0u) {
        float merge_max = -FLT_MAX;
        float merge_denom = 0.0f;
        for (unsigned merge_split = 0u;
             merge_split < kSplits;
             ++merge_split) {
            const size_t merge_partial = (size_t)head * kSplits + merge_split;
            const float split_denom = partial_denom[merge_partial];
            if (split_denom > 0.0f) {
                const float split_max = partial_max[merge_partial];
                const float next_max = fmaxf(merge_max, split_max);
                const float merge_a = merge_denom > 0.0f
                    ? expf(merge_max - next_max)
                    : 0.0f;
                const float merge_b = expf(split_max - next_max);
                merge_alpha[merge_split] = merge_a;
                merge_beta[merge_split] = merge_b;
                merge_denom = merge_denom * merge_a + split_denom * merge_b;
                merge_max = next_max;
            } else {
                merge_alpha[merge_split] = 1.0f;
                merge_beta[merge_split] = 0.0f;
            }
        }
        final_denom = merge_denom;
    }
    __syncthreads();

    float merge_acc[kMaxItems];
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) merge_acc[item] = 0.0f;
    for (unsigned merge_split = 0u;
         merge_split < kSplits;
         ++merge_split) {
        const size_t merge_partial = (size_t)head * kSplits + merge_split;
#pragma unroll
        for (unsigned item = 0u; item < kMaxItems; ++item) {
            const unsigned dimension = tid + item * kThreads;
            merge_acc[item] = merge_acc[item] * merge_alpha[merge_split] +
                merge_beta[merge_split] *
                    partial_values[merge_partial * kHeadDim + dimension];
        }
    }
#pragma unroll
    for (unsigned item = 0u; item < kMaxItems; ++item) {
        const unsigned dimension = tid + item * kThreads;
        dst[head * kHeadDim + dimension] = final_denom > 0.0f
            ? merge_acc[item] / final_denom
            : 0.0f;
    }
    if (tid == 0u) atomicExch(&completion_counters[head], 0u);
    asm volatile("" : : "r"(total_sequence_len));
}
#undef ANTFLY_SPLITK_ONLINE_MAX_VISIBLE_TOKENS
#undef ANTFLY_SPLITK_ONLINE_SLIDING_WINDOW
#undef ANTFLY_SPLITK_ONLINE_HEAD_DIM
#undef ANTFLY_SPLITK_ONLINE_KERNEL
#undef ANTFLY_SPLITK_ONLINE_NAMESPACE

// Production generated attention route from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1 plan_id=cuda/attention/prefill_flash/sm89/hd256/gqa8/page16/q512-or-q3/swa512/f32q-f16kv-hmma
#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_sm89_hd256_swa512_f32_v1
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_sm89_hd256_swa512_f32_v1
#define ANTFLY_FLASH_HEAD_DIM 256
#define ANTFLY_FLASH_SLIDING_WINDOW 512
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

// Typed, default-off SM89 paged-GQA flash-prefill template.
//
// The typed CUDA renderer supplies a unique namespace, one production entry
// point, head dimension, and attention policy before expanding this template.
// The standalone qualification wrapper uses the same template and additionally
// requests a scalar reference entry point, keeping qualification and runtime
// code byte-identical below the exported ABI.
//
// Candidate algorithm:
//   * one CTA owns one query head and a 16-query tile;
//   * WMMA m16n16k16 computes F16 QK^T into F32 score accumulators;
//   * causal/SWA masking and online softmax are evaluated per query row;
//   * normalized tile probabilities are rounded to F16 and WMMA computes PV;
//   * the output numerator and softmax denominator remain F32 across tiles.
//
// Supported contracts are deliberately narrow:
//   * SM89, batch=1, query heads=8, KV heads=1;
//   * q_seq_len=512 or the production tail q_seq_len=3;
//   * HD256 with sliding_window=512, or HD512 with global attention;
//   * contiguous F32 Q and paged F16 K/V, F32 output;
//   * 16-token pages, either null/identity or explicit page tables.
//
// The exported entry point uses the production 28-argument paged-GQA ABI. The
// candidate never calls a fallback. Optional/required routing is host-owned.

#include <cuda_fp16.h>
#include <mma.h>
#include <stdint.h>
#include <stddef.h>
#include <math.h>

#ifndef ANTFLY_FLASH_NAMESPACE
#error "ANTFLY_FLASH_NAMESPACE must name a unique renderer-owned namespace"
#endif
#ifndef ANTFLY_FLASH_KERNEL
#error "ANTFLY_FLASH_KERNEL must name the exported production entry point"
#endif
#ifndef ANTFLY_FLASH_HEAD_DIM
#error "ANTFLY_FLASH_HEAD_DIM must be 256 or 512"
#endif
#ifndef ANTFLY_FLASH_SLIDING_WINDOW
#error "ANTFLY_FLASH_SLIDING_WINDOW must be 512 for HD256 or 0 for HD512"
#endif

namespace ANTFLY_FLASH_NAMESPACE {

using namespace nvcuda;

constexpr unsigned kHeads = 8u;
constexpr unsigned kKvHeads = 1u;
constexpr unsigned kPageTokens = 16u;
constexpr unsigned kQueryTile = 16u;
constexpr unsigned kKeyTile = 16u;
constexpr unsigned kThreads = 256u;
constexpr unsigned kWarps = kThreads / 32u;
constexpr unsigned kInvalidToken = 0xffffffffu;
constexpr float kNegativeInfinity = -3.402823466e+38F;

enum PrototypeStatus : unsigned {
    kStatusOk = 0u,
    kStatusNullPointer = 1u,
    kStatusMisalignedPointer = 2u,
    kStatusLaunchGeometry = 3u,
    kStatusUnsupportedShape = 4u,
    kStatusUnsupportedPolicy = 5u,
    kStatusInvalidStride = 6u,
    kStatusPositionOverflow = 7u,
    kStatusInvalidPageContract = 8u,
    kStatusMappedPageOutOfBounds = 9u,
    kStatusAddressOverflow = 10u,
};

__device__ __forceinline__ bool pointer_aligned(const void* ptr, uintptr_t alignment) {
    return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1u)) == 0u;
}

__device__ __forceinline__ unsigned ceil_div_u32(unsigned value, unsigned divisor) {
    return value / divisor + (value % divisor != 0u ? 1u : 0u);
}

__device__ __forceinline__ unsigned physical_token(
    unsigned logical_token,
    const unsigned* block_table,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned physical_token_capacity
) {
    unsigned long long physical = static_cast<unsigned long long>(logical_token);
    if (block_table != nullptr) {
        const unsigned logical_block = logical_token / page_size_tokens;
        if (logical_block >= block_count) return kInvalidToken;
        const unsigned token_offset = logical_token - logical_block * page_size_tokens;
        physical = static_cast<unsigned long long>(block_table[logical_block]) *
                static_cast<unsigned long long>(page_size_tokens) +
            static_cast<unsigned long long>(token_offset);
    }
    if (physical > static_cast<unsigned long long>(0xffffffffu) ||
        physical >= static_cast<unsigned long long>(physical_token_capacity)) {
        return kInvalidToken;
    }
    return static_cast<unsigned>(physical);
}

struct VisibleRange {
    unsigned begin;
    unsigned end;
};

__device__ __forceinline__ VisibleRange visible_range(
    unsigned query_pos,
    unsigned kv_seq_len,
    unsigned kv_position_offset,
    unsigned sliding_window
) {
    VisibleRange result{0u, 0u};
    if (query_pos < kv_position_offset) return result;
    const unsigned visible = query_pos - kv_position_offset + 1u;
    result.end = visible < kv_seq_len ? visible : kv_seq_len;
    if (sliding_window != 0u) {
        const unsigned window_start_abs = query_pos + 1u > sliding_window
            ? query_pos + 1u - sliding_window
            : 0u;
        if (window_start_abs > kv_position_offset) {
            result.begin = window_start_abs - kv_position_offset;
            if (result.begin > result.end) result.begin = result.end;
        }
    }
    return result;
}

template <int HeadDim, int SlidingWindow, bool Flash>
__device__ bool validate_launch(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned mask_len,
    unsigned bias_mode,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned format,
    unsigned value_format,
    unsigned physical_token_capacity,
    const unsigned* decode_scalars,
    unsigned* shared_error
) {
    if (threadIdx.x == 0u) {
        unsigned error = kStatusOk;
        if (dst == nullptr || q == nullptr || k == nullptr || v == nullptr) {
            error = kStatusNullPointer;
        } else if (!pointer_aligned(dst, alignof(float)) ||
            !pointer_aligned(q, alignof(float)) ||
            !pointer_aligned(k, alignof(half)) ||
            !pointer_aligned(v, alignof(half)) ||
            (block_table != nullptr && !pointer_aligned(block_table, alignof(unsigned)))) {
            error = kStatusMisalignedPointer;
        } else {
            const unsigned expected_grid_y = Flash
                ? ceil_div_u32(q_seq_len, kQueryTile)
                : q_seq_len;
            const unsigned expected_threads = Flash ? kThreads : static_cast<unsigned>(HeadDim);
            if (blockDim.x != expected_threads || blockDim.y != 1u || blockDim.z != 1u ||
                gridDim.x != kHeads || gridDim.y != expected_grid_y || gridDim.z != 1u) {
                error = kStatusLaunchGeometry;
            } else if (batch != 1u || num_heads != kHeads || num_kv_heads != kKvHeads ||
                head_dim != static_cast<unsigned>(HeadDim) ||
                (q_seq_len != 512u && q_seq_len != 3u) ||
                kv_seq_len == 0u || kv_seq_len > 4096u) {
                error = kStatusUnsupportedShape;
            } else {
                const bool supported_prefix = q_seq_len == 512u
                    ? (query_position_offset == 0u || query_position_offset == 512u ||
                          query_position_offset == 1024u || query_position_offset == 1536u)
                    : query_position_offset == 2048u;
                if (!supported_prefix || kv_position_offset != 0u ||
                    kv_seq_len != query_position_offset + q_seq_len ||
                    total_sequence_len != kv_seq_len) {
                    error = kStatusUnsupportedShape;
                }
            }
            if (error == kStatusOk && sliding_window != static_cast<unsigned>(SlidingWindow)) {
                error = kStatusUnsupportedPolicy;
            } else if (error == kStatusOk && (mask_len != 0u || bias_mode != 0u || format != 2u ||
                value_format != 2u || decode_scalars != nullptr)) {
                error = kStatusUnsupportedPolicy;
            } else if (error == kStatusOk && (key_row_bytes != static_cast<unsigned>(HeadDim) * sizeof(half) ||
                base_key_row_bytes != key_row_bytes || value_row_bytes != key_row_bytes)) {
                error = kStatusInvalidStride;
            } else if (error == kStatusOk && (query_position_offset > 0xffffffffu - q_seq_len ||
                kv_position_offset > 0xffffffffu - kv_seq_len ||
                query_position_offset + q_seq_len > total_sequence_len ||
                kv_position_offset + kv_seq_len > total_sequence_len)) {
                error = kStatusPositionOverflow;
            } else if (error == kStatusOk) {
                const bool table_present = block_table != nullptr;
                const bool count_present = block_count != 0u;
                const unsigned required_blocks = ceil_div_u32(kv_seq_len, kPageTokens);
                if (page_size_tokens != kPageTokens || table_present != count_present ||
                    physical_token_capacity == 0u ||
                    physical_token_capacity % kPageTokens != 0u ||
                    (!table_present && physical_token_capacity < kv_seq_len) ||
                    (table_present && block_count < required_blocks)) {
                    error = kStatusInvalidPageContract;
                } else {
                    const unsigned long long q_elements =
                        static_cast<unsigned long long>(q_seq_len) * kHeads * HeadDim;
                    const unsigned long long kv_elements =
                        static_cast<unsigned long long>(physical_token_capacity) * HeadDim;
                    const unsigned long long output_elements = q_elements;
                    if (q_elements > 0x7fffffffffffffffull / sizeof(float) ||
                        kv_elements > 0x7fffffffffffffffull / sizeof(half) ||
                        output_elements > 0x7fffffffffffffffull / sizeof(float)) {
                        error = kStatusAddressOverflow;
                    }
                }
            }
        }
        *shared_error = error;
    }
    __syncthreads();
    if (*shared_error != kStatusOk) {
        return false;
    }

    if (block_table != nullptr) {
        const unsigned required_blocks = ceil_div_u32(kv_seq_len, kPageTokens);
        for (unsigned logical_block = threadIdx.x;
             logical_block < required_blocks;
             logical_block += blockDim.x) {
            const unsigned long long physical_begin =
                static_cast<unsigned long long>(block_table[logical_block]) * kPageTokens;
            const unsigned long long physical_end = physical_begin + (kPageTokens - 1u);
            if (physical_begin > 0xffffffffull || physical_end > 0xffffffffull ||
                physical_end >= static_cast<unsigned long long>(physical_token_capacity)) {
                atomicCAS(shared_error, kStatusOk, kStatusMappedPageOutOfBounds);
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return false;
        }
    }
    return true;
}

template <int HeadDim>
__device__ __forceinline__ size_t flash_shared_bytes() {
    // Keep this layout mirrored by the standalone Zig harness.
    return
        2u * kQueryTile * HeadDim * sizeof(half) + // Q tile + K/V tile
        kQueryTile * kKeyTile * sizeof(float) +   // scores
        kQueryTile * kKeyTile * sizeof(half) +    // probabilities
        kWarps * kQueryTile * 16u * sizeof(float) + // per-warp WMMA scratch
        3u * kQueryTile * sizeof(unsigned) +       // physical, begin, end
        4u * kQueryTile * sizeof(float) +          // max, denom, alpha, beta
        sizeof(unsigned);                          // shared error
}

template <int HeadDim, int SlidingWindow>
__device__ void flash_prefill_body(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned mask_len,
    unsigned bias_mode,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned format,
    unsigned value_format,
    unsigned physical_token_capacity,
    const unsigned* decode_scalars,
    unsigned char* storage
) {
    half* q_shared = reinterpret_cast<half*>(storage);
    half* kv_shared = q_shared + kQueryTile * HeadDim;
    float* scores = reinterpret_cast<float*>(kv_shared + kKeyTile * HeadDim);
    half* probabilities = reinterpret_cast<half*>(scores + kQueryTile * kKeyTile);
    float* warp_scratch = reinterpret_cast<float*>(probabilities + kQueryTile * kKeyTile);
    unsigned* physical = reinterpret_cast<unsigned*>(warp_scratch + kWarps * kQueryTile * 16u);
    unsigned* row_begin = physical + kKeyTile;
    unsigned* row_end = row_begin + kQueryTile;
    float* running_max = reinterpret_cast<float*>(row_end + kQueryTile);
    float* running_denom = running_max + kQueryTile;
    float* row_alpha = running_denom + kQueryTile;
    float* row_beta = row_alpha + kQueryTile;
    unsigned* shared_error = reinterpret_cast<unsigned*>(row_beta + kQueryTile);

    if (!validate_launch<HeadDim, SlidingWindow, true>(
            dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len,
            num_heads, num_kv_heads, head_dim, query_position_offset,
            kv_position_offset, sliding_window, total_sequence_len, mask_len,
            bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes,
            block_count, page_size_tokens, format, value_format,
            physical_token_capacity, decode_scalars, shared_error)) {
        return;
    }

    const unsigned tid = threadIdx.x;
    const unsigned warp = tid >> 5u;
    const unsigned lane = tid & 31u;
    const unsigned head = blockIdx.x;
    const unsigned tile_query_begin = blockIdx.y * kQueryTile;
    const unsigned valid_rows = q_seq_len - tile_query_begin < kQueryTile
        ? q_seq_len - tile_query_begin
        : kQueryTile;

    // validate_launch has already proved the fixed 256-thread geometry.  Keep
    // the stride compile-time-visible so nvcc can unroll the cooperative
    // loads/reductions instead of emitting a runtime %ntid.x loop.
    for (unsigned index = tid; index < kQueryTile * HeadDim; index += kThreads) {
        const unsigned row = index / HeadDim;
        const unsigned column = index - row * HeadDim;
        q_shared[index] = row < valid_rows
            ? __float2half_rn(q[(tile_query_begin + row) * kHeads * HeadDim +
                  head * HeadDim + column])
            : __float2half_rn(0.0f);
    }
    if (tid < kQueryTile) {
        if (tid < valid_rows) {
            const unsigned query_pos = query_position_offset + tile_query_begin + tid;
            const VisibleRange range = visible_range(
                query_pos, kv_seq_len, kv_position_offset, SlidingWindow);
            row_begin[tid] = range.begin;
            row_end[tid] = range.end;
        } else {
            row_begin[tid] = 0u;
            row_end[tid] = 0u;
        }
        running_max[tid] = kNegativeInfinity;
        running_denom[tid] = 0.0f;
        row_alpha[tid] = 1.0f;
        row_beta[tid] = 0.0f;
    }
    __syncthreads();

    unsigned union_begin = kv_seq_len;
    unsigned union_end = 0u;
    if (tid == 0u) {
        for (unsigned row = 0u; row < valid_rows; ++row) {
            if (row_begin[row] < row_end[row]) {
                if (row_begin[row] < union_begin) union_begin = row_begin[row];
                if (row_end[row] > union_end) union_end = row_end[row];
            }
        }
        if (union_end == 0u) union_begin = 0u;
        row_alpha[0] = static_cast<float>(union_begin);
        row_beta[0] = static_cast<float>(union_end);
    }
    __syncthreads();
    // Both values are <= UINT32_MAX and exact in F32 for the supported model
    // context.  Reuse two metadata slots only until the first score tile.
    union_begin = static_cast<unsigned>(row_alpha[0]);
    union_end = static_cast<unsigned>(row_beta[0]);
    const unsigned first_key_tile = (union_begin / kKeyTile) * kKeyTile;
    const unsigned final_key_tile = ceil_div_u32(union_end, kKeyTile) * kKeyTile;

    constexpr unsigned kOutputTiles = HeadDim / 16u;
    constexpr unsigned kFragmentsPerWarp = kOutputTiles / kWarps;
    static_assert(HeadDim == 256 || HeadDim == 512, "unsupported head dimension");
    static_assert(kOutputTiles % kWarps == 0, "output tiles must divide warps");
    wmma::fragment<wmma::accumulator, 16, 16, 16, float>
        output_fragments[kFragmentsPerWarp];
#pragma unroll
    for (unsigned fragment_index = 0u;
         fragment_index < kFragmentsPerWarp;
         ++fragment_index) {
        wmma::fill_fragment(output_fragments[fragment_index], 0.0f);
    }

    for (unsigned key_tile_begin = first_key_tile;
         key_tile_begin < final_key_tile;
         key_tile_begin += kKeyTile) {
        if (tid < kKeyTile) {
            const unsigned logical_token = key_tile_begin + tid;
            physical[tid] = logical_token < kv_seq_len
                ? physical_token(logical_token, block_table, block_count,
                    page_size_tokens, physical_token_capacity)
                : kInvalidToken;
            if (logical_token < kv_seq_len && physical[tid] == kInvalidToken) {
                atomicCAS(shared_error, kStatusOk, kStatusMappedPageOutOfBounds);
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return;
        }

        for (unsigned index = tid; index < kKeyTile * HeadDim; index += kThreads) {
            const unsigned key_row = index / HeadDim;
            const unsigned column = index - key_row * HeadDim;
            const unsigned token = physical[key_row];
            kv_shared[index] = token != kInvalidToken
                ? reinterpret_cast<const half*>(
                      k + static_cast<size_t>(token) * key_row_bytes)[column]
                : __float2half_rn(0.0f);
        }
        __syncthreads();

        // Split the head dimension across all eight warps.  Each warp writes
        // one F32 partial score tile into its otherwise-idle PV scratch; the
        // CTA then reduces the eight tiles before softmax.  This avoids a
        // single-warp QK bottleneck while retaining a fixed F32 merge order.
        {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> score_fragment;
            wmma::fill_fragment(score_fragment, 0.0f);
#pragma unroll
            for (unsigned dimension = warp * 16u;
                 dimension < HeadDim;
                 dimension += kWarps * 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major>
                    query_fragment;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major>
                    key_fragment;
                wmma::load_matrix_sync(query_fragment, q_shared + dimension, HeadDim);
                // K rows [key, dimension] are exactly a column-major K^T tile.
                wmma::load_matrix_sync(key_fragment, kv_shared + dimension, HeadDim);
                wmma::mma_sync(
                    score_fragment, query_fragment, key_fragment, score_fragment);
            }
            wmma::store_matrix_sync(
                warp_scratch + warp * kQueryTile * kKeyTile,
                score_fragment,
                kKeyTile,
                wmma::mem_row_major);
        }
        __syncthreads();
        for (unsigned index = tid; index < kQueryTile * kKeyTile; index += kThreads) {
            float score = 0.0f;
#pragma unroll
            for (unsigned source_warp = 0u; source_warp < kWarps; ++source_warp) {
                score += warp_scratch[source_warp * kQueryTile * kKeyTile + index];
            }
            scores[index] = score;
        }
        __syncthreads();

        if (tid < kQueryTile) {
            const unsigned row = tid;
            const float scale = rsqrtf(static_cast<float>(HeadDim));
            float tile_max = kNegativeInfinity;
            bool tile_has_value = false;
#pragma unroll
            for (unsigned key_column = 0u; key_column < kKeyTile; ++key_column) {
                const unsigned logical_token = key_tile_begin + key_column;
                const bool visible = row < valid_rows && logical_token < kv_seq_len &&
                    logical_token >= row_begin[row] && logical_token < row_end[row] &&
                    physical[key_column] != kInvalidToken;
                if (visible) {
                    tile_max = fmaxf(
                        tile_max,
                        scores[row * kKeyTile + key_column] * scale);
                    tile_has_value = true;
                }
            }
            float tile_denom = 0.0f;
#pragma unroll
            for (unsigned key_column = 0u; key_column < kKeyTile; ++key_column) {
                const unsigned logical_token = key_tile_begin + key_column;
                const bool visible = tile_has_value && row < valid_rows &&
                    logical_token < kv_seq_len && logical_token >= row_begin[row] &&
                    logical_token < row_end[row] && physical[key_column] != kInvalidToken;
                const float probability = visible
                    ? expf(scores[row * kKeyTile + key_column] * scale - tile_max)
                    : 0.0f;
                probabilities[row * kKeyTile + key_column] = __float2half_rn(probability);
                tile_denom += probability;
            }

            if (tile_has_value) {
                const float old_max = running_max[row];
                const float old_denom = running_denom[row];
                const float next_max = fmaxf(old_max, tile_max);
                const float alpha = old_denom > 0.0f ? expf(old_max - next_max) : 0.0f;
                const float beta = expf(tile_max - next_max);
                running_max[row] = next_max;
                running_denom[row] = old_denom * alpha + tile_denom * beta;
                row_alpha[row] = alpha;
                row_beta[row] = beta;
            } else {
                row_alpha[row] = 1.0f;
                row_beta[row] = 0.0f;
            }
        }
        __syncthreads();

        // Reuse the K tile allocation for V only after score consumers finish.
        for (unsigned index = tid; index < kKeyTile * HeadDim; index += kThreads) {
            const unsigned key_row = index / HeadDim;
            const unsigned column = index - key_row * HeadDim;
            const unsigned token = physical[key_row];
            kv_shared[index] = token != kInvalidToken
                ? reinterpret_cast<const half*>(
                      v + static_cast<size_t>(token) * value_row_bytes)[column]
                : __float2half_rn(0.0f);
        }
        __syncthreads();

        wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major>
            probability_fragment;
        wmma::load_matrix_sync(probability_fragment, probabilities, kKeyTile);
        float* const scratch = warp_scratch + warp * kQueryTile * 16u;
#pragma unroll
        for (unsigned fragment_index = 0u;
             fragment_index < kFragmentsPerWarp;
             ++fragment_index) {
            const unsigned output_tile = warp + fragment_index * kWarps;
            wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major>
                value_fragment;
            wmma::fragment<wmma::accumulator, 16, 16, 16, float>
                tile_output_fragment;
            wmma::load_matrix_sync(
                value_fragment, kv_shared + output_tile * 16u, HeadDim);
            wmma::fill_fragment(tile_output_fragment, 0.0f);
            wmma::mma_sync(
                tile_output_fragment,
                probability_fragment,
                value_fragment,
                tile_output_fragment);

            // Accumulator-fragment lane layouts are intentionally opaque.
            // Round-trip through warp-private shared storage to apply exact
            // row-wise online-softmax merge factors without layout assumptions.
            wmma::store_matrix_sync(
                scratch,
                output_fragments[fragment_index],
                16,
                wmma::mem_row_major);
            __syncwarp();
            for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                scratch[index] *= row_alpha[index / 16u];
            }
            __syncwarp();
            wmma::load_matrix_sync(
                output_fragments[fragment_index],
                scratch,
                16,
                wmma::mem_row_major);

            wmma::store_matrix_sync(
                scratch, tile_output_fragment, 16, wmma::mem_row_major);
            __syncwarp();
            for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                scratch[index] *= row_beta[index / 16u];
            }
            __syncwarp();
            wmma::load_matrix_sync(
                tile_output_fragment, scratch, 16, wmma::mem_row_major);
#pragma unroll
            for (unsigned element = 0u;
                 element < output_fragments[fragment_index].num_elements;
                 ++element) {
                output_fragments[fragment_index].x[element] +=
                    tile_output_fragment.x[element];
            }
        }
        __syncthreads();
    }

#pragma unroll
    for (unsigned fragment_index = 0u;
         fragment_index < kFragmentsPerWarp;
         ++fragment_index) {
        const unsigned output_tile = warp + fragment_index * kWarps;
        float* const scratch = warp_scratch + warp * kQueryTile * 16u;
        wmma::store_matrix_sync(
            scratch,
            output_fragments[fragment_index],
            16,
            wmma::mem_row_major);
        __syncwarp();
        for (unsigned index = lane; index < 16u * 16u; index += 32u) {
            const unsigned row = index / 16u;
            const unsigned column = index - row * 16u;
            if (row < valid_rows) {
                const float denominator = running_denom[row];
                const size_t output_index =
                    static_cast<size_t>(tile_query_begin + row) * kHeads * HeadDim +
                    head * HeadDim + output_tile * 16u + column;
                dst[output_index] = denominator > 0.0f
                    ? scratch[index] / denominator
                    : 0.0f;
            }
        }
        __syncwarp();
    }
}

template <int HeadDim, int SlidingWindow>
__device__ void reference_prefill_body(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned mask_len,
    unsigned bias_mode,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned format,
    unsigned value_format,
    unsigned physical_token_capacity,
    const unsigned* decode_scalars,
    unsigned* shared_error,
    float* warp_sums,
    unsigned* shared_physical,
    float* shared_max,
    float* shared_denom,
    float* shared_alpha,
    float* shared_beta
) {
    if (!validate_launch<HeadDim, SlidingWindow, false>(
            dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len,
            num_heads, num_kv_heads, head_dim, query_position_offset,
            kv_position_offset, sliding_window, total_sequence_len, mask_len,
            bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes,
            block_count, page_size_tokens, format, value_format,
            physical_token_capacity, decode_scalars, shared_error)) {
        return;
    }

    const unsigned tid = threadIdx.x;
    const unsigned lane = tid & 31u;
    const unsigned warp = tid >> 5u;
    constexpr unsigned kReferenceWarps = HeadDim / 32u;
    const unsigned head = blockIdx.x;
    const unsigned query_index = blockIdx.y;
    const unsigned query_pos = query_position_offset + query_index;
    const VisibleRange range = visible_range(
        query_pos, kv_seq_len, kv_position_offset, SlidingWindow);
    const size_t q_offset =
        static_cast<size_t>(query_index) * kHeads * HeadDim + head * HeadDim;
    const float query_value = q[q_offset + tid];
    const float scale = rsqrtf(static_cast<float>(HeadDim));
    float output_accumulator = 0.0f;
    if (tid == 0u) {
        *shared_max = kNegativeInfinity;
        *shared_denom = 0.0f;
    }
    __syncthreads();

    for (unsigned logical_token = range.begin;
         logical_token < range.end;
         ++logical_token) {
        if (tid == 0u) {
            *shared_physical = physical_token(
                logical_token, block_table, block_count,
                page_size_tokens, physical_token_capacity);
            if (*shared_physical == kInvalidToken) {
                *shared_error = kStatusMappedPageOutOfBounds;
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return;
        }
        const half* const key_row = reinterpret_cast<const half*>(
            k + static_cast<size_t>(*shared_physical) * key_row_bytes);
        const half* const value_row = reinterpret_cast<const half*>(
            v + static_cast<size_t>(*shared_physical) * value_row_bytes);
        float partial = query_value * __half2float(key_row[tid]);
        for (unsigned offset = 16u; offset > 0u; offset >>= 1u) {
            partial += __shfl_down_sync(0xffffffffu, partial, offset);
        }
        if (lane == 0u) warp_sums[warp] = partial;
        __syncthreads();
        float dot = warp == 0u && lane < kReferenceWarps
            ? warp_sums[lane]
            : 0.0f;
        if (warp == 0u) {
            for (unsigned offset = 16u; offset > 0u; offset >>= 1u) {
                dot += __shfl_down_sync(0xffffffffu, dot, offset);
            }
            if (lane == 0u) {
                const float score = dot * scale;
                const float next_max = fmaxf(*shared_max, score);
                *shared_alpha = *shared_denom > 0.0f
                    ? expf(*shared_max - next_max)
                    : 0.0f;
                *shared_beta = expf(score - next_max);
                *shared_denom = *shared_denom * *shared_alpha + *shared_beta;
                *shared_max = next_max;
            }
        }
        __syncthreads();
        output_accumulator = output_accumulator * *shared_alpha +
            *shared_beta * __half2float(value_row[tid]);
        __syncthreads();
    }

    dst[q_offset + tid] = *shared_denom > 0.0f
        ? output_accumulator / *shared_denom
        : 0.0f;
}

} // namespace ANTFLY_FLASH_NAMESPACE

#define ANTFLY_FLASH_ARGUMENTS \
    float* dst, \
    const float* q, \
    const unsigned char* k, \
    const unsigned char* v, \
    const unsigned* block_table, \
    const unsigned char* attn_or_mask, \
    const float* bias, \
    unsigned batch, \
    unsigned q_seq_len, \
    unsigned kv_seq_len, \
    unsigned num_heads, \
    unsigned num_kv_heads, \
    unsigned head_dim, \
    unsigned query_position_offset, \
    unsigned kv_position_offset, \
    unsigned sliding_window, \
    unsigned total_sequence_len, \
    unsigned mask_len, \
    unsigned bias_mode, \
    unsigned key_row_bytes, \
    unsigned base_key_row_bytes, \
    unsigned value_row_bytes, \
    unsigned block_count, \
    unsigned page_size_tokens, \
    unsigned format, \
    unsigned value_format, \
    unsigned physical_token_capacity, \
    const unsigned* decode_scalars

#define ANTFLY_FORWARD_ARGUMENTS \
    dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len, \
    num_heads, num_kv_heads, head_dim, query_position_offset, \
    kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, \
    key_row_bytes, base_key_row_bytes, value_row_bytes, block_count, \
    page_size_tokens, format, value_format, physical_token_capacity, decode_scalars

extern "C" __global__ void
ANTFLY_FLASH_KERNEL(
    ANTFLY_FLASH_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    extern __shared__ __align__(16) unsigned char storage[];
    ANTFLY_FLASH_NAMESPACE::flash_prefill_body<
        ANTFLY_FLASH_HEAD_DIM,
        ANTFLY_FLASH_SLIDING_WINDOW>(ANTFLY_FORWARD_ARGUMENTS, storage);
}

#ifdef ANTFLY_FLASH_REFERENCE_KERNEL
extern "C" __global__ void
ANTFLY_FLASH_REFERENCE_KERNEL(
    ANTFLY_FLASH_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    __shared__ unsigned shared_error;
    __shared__ float warp_sums[16];
    __shared__ unsigned shared_physical;
    __shared__ float shared_max;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    ANTFLY_FLASH_NAMESPACE::reference_prefill_body<
        ANTFLY_FLASH_HEAD_DIM,
        ANTFLY_FLASH_SLIDING_WINDOW>(
        ANTFLY_FORWARD_ARGUMENTS,
        &shared_error,
        warp_sums,
        &shared_physical,
        &shared_max,
        &shared_denom,
        &shared_alpha,
        &shared_beta);
}
#endif

#undef ANTFLY_FORWARD_ARGUMENTS
#undef ANTFLY_FLASH_ARGUMENTS
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE

// Production generated attention route from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1 plan_id=cuda/attention/prefill_flash/sm89/hd512/gqa8/page16/q512-or-q3/swa0/f32q-f16kv-hmma
#define ANTFLY_FLASH_NAMESPACE antfly_flash_prefill_sm89_hd512_global_f32_v1
#define ANTFLY_FLASH_KERNEL antfly_gqa_attention_prefill_flash_sm89_hd512_global_f32_v1
#define ANTFLY_FLASH_HEAD_DIM 512
#define ANTFLY_FLASH_SLIDING_WINDOW 0
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

// Typed, default-off SM89 paged-GQA flash-prefill template.
//
// The typed CUDA renderer supplies a unique namespace, one production entry
// point, head dimension, and attention policy before expanding this template.
// The standalone qualification wrapper uses the same template and additionally
// requests a scalar reference entry point, keeping qualification and runtime
// code byte-identical below the exported ABI.
//
// Candidate algorithm:
//   * one CTA owns one query head and a 16-query tile;
//   * WMMA m16n16k16 computes F16 QK^T into F32 score accumulators;
//   * causal/SWA masking and online softmax are evaluated per query row;
//   * normalized tile probabilities are rounded to F16 and WMMA computes PV;
//   * the output numerator and softmax denominator remain F32 across tiles.
//
// Supported contracts are deliberately narrow:
//   * SM89, batch=1, query heads=8, KV heads=1;
//   * q_seq_len=512 or the production tail q_seq_len=3;
//   * HD256 with sliding_window=512, or HD512 with global attention;
//   * contiguous F32 Q and paged F16 K/V, F32 output;
//   * 16-token pages, either null/identity or explicit page tables.
//
// The exported entry point uses the production 28-argument paged-GQA ABI. The
// candidate never calls a fallback. Optional/required routing is host-owned.

#include <cuda_fp16.h>
#include <mma.h>
#include <stdint.h>
#include <stddef.h>
#include <math.h>

#ifndef ANTFLY_FLASH_NAMESPACE
#error "ANTFLY_FLASH_NAMESPACE must name a unique renderer-owned namespace"
#endif
#ifndef ANTFLY_FLASH_KERNEL
#error "ANTFLY_FLASH_KERNEL must name the exported production entry point"
#endif
#ifndef ANTFLY_FLASH_HEAD_DIM
#error "ANTFLY_FLASH_HEAD_DIM must be 256 or 512"
#endif
#ifndef ANTFLY_FLASH_SLIDING_WINDOW
#error "ANTFLY_FLASH_SLIDING_WINDOW must be 512 for HD256 or 0 for HD512"
#endif

namespace ANTFLY_FLASH_NAMESPACE {

using namespace nvcuda;

constexpr unsigned kHeads = 8u;
constexpr unsigned kKvHeads = 1u;
constexpr unsigned kPageTokens = 16u;
constexpr unsigned kQueryTile = 16u;
constexpr unsigned kKeyTile = 16u;
constexpr unsigned kThreads = 256u;
constexpr unsigned kWarps = kThreads / 32u;
constexpr unsigned kInvalidToken = 0xffffffffu;
constexpr float kNegativeInfinity = -3.402823466e+38F;

enum PrototypeStatus : unsigned {
    kStatusOk = 0u,
    kStatusNullPointer = 1u,
    kStatusMisalignedPointer = 2u,
    kStatusLaunchGeometry = 3u,
    kStatusUnsupportedShape = 4u,
    kStatusUnsupportedPolicy = 5u,
    kStatusInvalidStride = 6u,
    kStatusPositionOverflow = 7u,
    kStatusInvalidPageContract = 8u,
    kStatusMappedPageOutOfBounds = 9u,
    kStatusAddressOverflow = 10u,
};

__device__ __forceinline__ bool pointer_aligned(const void* ptr, uintptr_t alignment) {
    return (reinterpret_cast<uintptr_t>(ptr) & (alignment - 1u)) == 0u;
}

__device__ __forceinline__ unsigned ceil_div_u32(unsigned value, unsigned divisor) {
    return value / divisor + (value % divisor != 0u ? 1u : 0u);
}

__device__ __forceinline__ unsigned physical_token(
    unsigned logical_token,
    const unsigned* block_table,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned physical_token_capacity
) {
    unsigned long long physical = static_cast<unsigned long long>(logical_token);
    if (block_table != nullptr) {
        const unsigned logical_block = logical_token / page_size_tokens;
        if (logical_block >= block_count) return kInvalidToken;
        const unsigned token_offset = logical_token - logical_block * page_size_tokens;
        physical = static_cast<unsigned long long>(block_table[logical_block]) *
                static_cast<unsigned long long>(page_size_tokens) +
            static_cast<unsigned long long>(token_offset);
    }
    if (physical > static_cast<unsigned long long>(0xffffffffu) ||
        physical >= static_cast<unsigned long long>(physical_token_capacity)) {
        return kInvalidToken;
    }
    return static_cast<unsigned>(physical);
}

struct VisibleRange {
    unsigned begin;
    unsigned end;
};

__device__ __forceinline__ VisibleRange visible_range(
    unsigned query_pos,
    unsigned kv_seq_len,
    unsigned kv_position_offset,
    unsigned sliding_window
) {
    VisibleRange result{0u, 0u};
    if (query_pos < kv_position_offset) return result;
    const unsigned visible = query_pos - kv_position_offset + 1u;
    result.end = visible < kv_seq_len ? visible : kv_seq_len;
    if (sliding_window != 0u) {
        const unsigned window_start_abs = query_pos + 1u > sliding_window
            ? query_pos + 1u - sliding_window
            : 0u;
        if (window_start_abs > kv_position_offset) {
            result.begin = window_start_abs - kv_position_offset;
            if (result.begin > result.end) result.begin = result.end;
        }
    }
    return result;
}

template <int HeadDim, int SlidingWindow, bool Flash>
__device__ bool validate_launch(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned mask_len,
    unsigned bias_mode,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned format,
    unsigned value_format,
    unsigned physical_token_capacity,
    const unsigned* decode_scalars,
    unsigned* shared_error
) {
    if (threadIdx.x == 0u) {
        unsigned error = kStatusOk;
        if (dst == nullptr || q == nullptr || k == nullptr || v == nullptr) {
            error = kStatusNullPointer;
        } else if (!pointer_aligned(dst, alignof(float)) ||
            !pointer_aligned(q, alignof(float)) ||
            !pointer_aligned(k, alignof(half)) ||
            !pointer_aligned(v, alignof(half)) ||
            (block_table != nullptr && !pointer_aligned(block_table, alignof(unsigned)))) {
            error = kStatusMisalignedPointer;
        } else {
            const unsigned expected_grid_y = Flash
                ? ceil_div_u32(q_seq_len, kQueryTile)
                : q_seq_len;
            const unsigned expected_threads = Flash ? kThreads : static_cast<unsigned>(HeadDim);
            if (blockDim.x != expected_threads || blockDim.y != 1u || blockDim.z != 1u ||
                gridDim.x != kHeads || gridDim.y != expected_grid_y || gridDim.z != 1u) {
                error = kStatusLaunchGeometry;
            } else if (batch != 1u || num_heads != kHeads || num_kv_heads != kKvHeads ||
                head_dim != static_cast<unsigned>(HeadDim) ||
                (q_seq_len != 512u && q_seq_len != 3u) ||
                kv_seq_len == 0u || kv_seq_len > 4096u) {
                error = kStatusUnsupportedShape;
            } else {
                const bool supported_prefix = q_seq_len == 512u
                    ? (query_position_offset == 0u || query_position_offset == 512u ||
                          query_position_offset == 1024u || query_position_offset == 1536u)
                    : query_position_offset == 2048u;
                if (!supported_prefix || kv_position_offset != 0u ||
                    kv_seq_len != query_position_offset + q_seq_len ||
                    total_sequence_len != kv_seq_len) {
                    error = kStatusUnsupportedShape;
                }
            }
            if (error == kStatusOk && sliding_window != static_cast<unsigned>(SlidingWindow)) {
                error = kStatusUnsupportedPolicy;
            } else if (error == kStatusOk && (mask_len != 0u || bias_mode != 0u || format != 2u ||
                value_format != 2u || decode_scalars != nullptr)) {
                error = kStatusUnsupportedPolicy;
            } else if (error == kStatusOk && (key_row_bytes != static_cast<unsigned>(HeadDim) * sizeof(half) ||
                base_key_row_bytes != key_row_bytes || value_row_bytes != key_row_bytes)) {
                error = kStatusInvalidStride;
            } else if (error == kStatusOk && (query_position_offset > 0xffffffffu - q_seq_len ||
                kv_position_offset > 0xffffffffu - kv_seq_len ||
                query_position_offset + q_seq_len > total_sequence_len ||
                kv_position_offset + kv_seq_len > total_sequence_len)) {
                error = kStatusPositionOverflow;
            } else if (error == kStatusOk) {
                const bool table_present = block_table != nullptr;
                const bool count_present = block_count != 0u;
                const unsigned required_blocks = ceil_div_u32(kv_seq_len, kPageTokens);
                if (page_size_tokens != kPageTokens || table_present != count_present ||
                    physical_token_capacity == 0u ||
                    physical_token_capacity % kPageTokens != 0u ||
                    (!table_present && physical_token_capacity < kv_seq_len) ||
                    (table_present && block_count < required_blocks)) {
                    error = kStatusInvalidPageContract;
                } else {
                    const unsigned long long q_elements =
                        static_cast<unsigned long long>(q_seq_len) * kHeads * HeadDim;
                    const unsigned long long kv_elements =
                        static_cast<unsigned long long>(physical_token_capacity) * HeadDim;
                    const unsigned long long output_elements = q_elements;
                    if (q_elements > 0x7fffffffffffffffull / sizeof(float) ||
                        kv_elements > 0x7fffffffffffffffull / sizeof(half) ||
                        output_elements > 0x7fffffffffffffffull / sizeof(float)) {
                        error = kStatusAddressOverflow;
                    }
                }
            }
        }
        *shared_error = error;
    }
    __syncthreads();
    if (*shared_error != kStatusOk) {
        return false;
    }

    if (block_table != nullptr) {
        const unsigned required_blocks = ceil_div_u32(kv_seq_len, kPageTokens);
        for (unsigned logical_block = threadIdx.x;
             logical_block < required_blocks;
             logical_block += blockDim.x) {
            const unsigned long long physical_begin =
                static_cast<unsigned long long>(block_table[logical_block]) * kPageTokens;
            const unsigned long long physical_end = physical_begin + (kPageTokens - 1u);
            if (physical_begin > 0xffffffffull || physical_end > 0xffffffffull ||
                physical_end >= static_cast<unsigned long long>(physical_token_capacity)) {
                atomicCAS(shared_error, kStatusOk, kStatusMappedPageOutOfBounds);
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return false;
        }
    }
    return true;
}

template <int HeadDim>
__device__ __forceinline__ size_t flash_shared_bytes() {
    // Keep this layout mirrored by the standalone Zig harness.
    return
        2u * kQueryTile * HeadDim * sizeof(half) + // Q tile + K/V tile
        kQueryTile * kKeyTile * sizeof(float) +   // scores
        kQueryTile * kKeyTile * sizeof(half) +    // probabilities
        kWarps * kQueryTile * 16u * sizeof(float) + // per-warp WMMA scratch
        3u * kQueryTile * sizeof(unsigned) +       // physical, begin, end
        4u * kQueryTile * sizeof(float) +          // max, denom, alpha, beta
        sizeof(unsigned);                          // shared error
}

template <int HeadDim, int SlidingWindow>
__device__ void flash_prefill_body(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned mask_len,
    unsigned bias_mode,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned format,
    unsigned value_format,
    unsigned physical_token_capacity,
    const unsigned* decode_scalars,
    unsigned char* storage
) {
    half* q_shared = reinterpret_cast<half*>(storage);
    half* kv_shared = q_shared + kQueryTile * HeadDim;
    float* scores = reinterpret_cast<float*>(kv_shared + kKeyTile * HeadDim);
    half* probabilities = reinterpret_cast<half*>(scores + kQueryTile * kKeyTile);
    float* warp_scratch = reinterpret_cast<float*>(probabilities + kQueryTile * kKeyTile);
    unsigned* physical = reinterpret_cast<unsigned*>(warp_scratch + kWarps * kQueryTile * 16u);
    unsigned* row_begin = physical + kKeyTile;
    unsigned* row_end = row_begin + kQueryTile;
    float* running_max = reinterpret_cast<float*>(row_end + kQueryTile);
    float* running_denom = running_max + kQueryTile;
    float* row_alpha = running_denom + kQueryTile;
    float* row_beta = row_alpha + kQueryTile;
    unsigned* shared_error = reinterpret_cast<unsigned*>(row_beta + kQueryTile);

    if (!validate_launch<HeadDim, SlidingWindow, true>(
            dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len,
            num_heads, num_kv_heads, head_dim, query_position_offset,
            kv_position_offset, sliding_window, total_sequence_len, mask_len,
            bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes,
            block_count, page_size_tokens, format, value_format,
            physical_token_capacity, decode_scalars, shared_error)) {
        return;
    }

    const unsigned tid = threadIdx.x;
    const unsigned warp = tid >> 5u;
    const unsigned lane = tid & 31u;
    const unsigned head = blockIdx.x;
    const unsigned tile_query_begin = blockIdx.y * kQueryTile;
    const unsigned valid_rows = q_seq_len - tile_query_begin < kQueryTile
        ? q_seq_len - tile_query_begin
        : kQueryTile;

    // validate_launch has already proved the fixed 256-thread geometry.  Keep
    // the stride compile-time-visible so nvcc can unroll the cooperative
    // loads/reductions instead of emitting a runtime %ntid.x loop.
    for (unsigned index = tid; index < kQueryTile * HeadDim; index += kThreads) {
        const unsigned row = index / HeadDim;
        const unsigned column = index - row * HeadDim;
        q_shared[index] = row < valid_rows
            ? __float2half_rn(q[(tile_query_begin + row) * kHeads * HeadDim +
                  head * HeadDim + column])
            : __float2half_rn(0.0f);
    }
    if (tid < kQueryTile) {
        if (tid < valid_rows) {
            const unsigned query_pos = query_position_offset + tile_query_begin + tid;
            const VisibleRange range = visible_range(
                query_pos, kv_seq_len, kv_position_offset, SlidingWindow);
            row_begin[tid] = range.begin;
            row_end[tid] = range.end;
        } else {
            row_begin[tid] = 0u;
            row_end[tid] = 0u;
        }
        running_max[tid] = kNegativeInfinity;
        running_denom[tid] = 0.0f;
        row_alpha[tid] = 1.0f;
        row_beta[tid] = 0.0f;
    }
    __syncthreads();

    unsigned union_begin = kv_seq_len;
    unsigned union_end = 0u;
    if (tid == 0u) {
        for (unsigned row = 0u; row < valid_rows; ++row) {
            if (row_begin[row] < row_end[row]) {
                if (row_begin[row] < union_begin) union_begin = row_begin[row];
                if (row_end[row] > union_end) union_end = row_end[row];
            }
        }
        if (union_end == 0u) union_begin = 0u;
        row_alpha[0] = static_cast<float>(union_begin);
        row_beta[0] = static_cast<float>(union_end);
    }
    __syncthreads();
    // Both values are <= UINT32_MAX and exact in F32 for the supported model
    // context.  Reuse two metadata slots only until the first score tile.
    union_begin = static_cast<unsigned>(row_alpha[0]);
    union_end = static_cast<unsigned>(row_beta[0]);
    const unsigned first_key_tile = (union_begin / kKeyTile) * kKeyTile;
    const unsigned final_key_tile = ceil_div_u32(union_end, kKeyTile) * kKeyTile;

    constexpr unsigned kOutputTiles = HeadDim / 16u;
    constexpr unsigned kFragmentsPerWarp = kOutputTiles / kWarps;
    static_assert(HeadDim == 256 || HeadDim == 512, "unsupported head dimension");
    static_assert(kOutputTiles % kWarps == 0, "output tiles must divide warps");
    wmma::fragment<wmma::accumulator, 16, 16, 16, float>
        output_fragments[kFragmentsPerWarp];
#pragma unroll
    for (unsigned fragment_index = 0u;
         fragment_index < kFragmentsPerWarp;
         ++fragment_index) {
        wmma::fill_fragment(output_fragments[fragment_index], 0.0f);
    }

    for (unsigned key_tile_begin = first_key_tile;
         key_tile_begin < final_key_tile;
         key_tile_begin += kKeyTile) {
        if (tid < kKeyTile) {
            const unsigned logical_token = key_tile_begin + tid;
            physical[tid] = logical_token < kv_seq_len
                ? physical_token(logical_token, block_table, block_count,
                    page_size_tokens, physical_token_capacity)
                : kInvalidToken;
            if (logical_token < kv_seq_len && physical[tid] == kInvalidToken) {
                atomicCAS(shared_error, kStatusOk, kStatusMappedPageOutOfBounds);
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return;
        }

        for (unsigned index = tid; index < kKeyTile * HeadDim; index += kThreads) {
            const unsigned key_row = index / HeadDim;
            const unsigned column = index - key_row * HeadDim;
            const unsigned token = physical[key_row];
            kv_shared[index] = token != kInvalidToken
                ? reinterpret_cast<const half*>(
                      k + static_cast<size_t>(token) * key_row_bytes)[column]
                : __float2half_rn(0.0f);
        }
        __syncthreads();

        // Split the head dimension across all eight warps.  Each warp writes
        // one F32 partial score tile into its otherwise-idle PV scratch; the
        // CTA then reduces the eight tiles before softmax.  This avoids a
        // single-warp QK bottleneck while retaining a fixed F32 merge order.
        {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> score_fragment;
            wmma::fill_fragment(score_fragment, 0.0f);
#pragma unroll
            for (unsigned dimension = warp * 16u;
                 dimension < HeadDim;
                 dimension += kWarps * 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major>
                    query_fragment;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::col_major>
                    key_fragment;
                wmma::load_matrix_sync(query_fragment, q_shared + dimension, HeadDim);
                // K rows [key, dimension] are exactly a column-major K^T tile.
                wmma::load_matrix_sync(key_fragment, kv_shared + dimension, HeadDim);
                wmma::mma_sync(
                    score_fragment, query_fragment, key_fragment, score_fragment);
            }
            wmma::store_matrix_sync(
                warp_scratch + warp * kQueryTile * kKeyTile,
                score_fragment,
                kKeyTile,
                wmma::mem_row_major);
        }
        __syncthreads();
        for (unsigned index = tid; index < kQueryTile * kKeyTile; index += kThreads) {
            float score = 0.0f;
#pragma unroll
            for (unsigned source_warp = 0u; source_warp < kWarps; ++source_warp) {
                score += warp_scratch[source_warp * kQueryTile * kKeyTile + index];
            }
            scores[index] = score;
        }
        __syncthreads();

        if (tid < kQueryTile) {
            const unsigned row = tid;
            const float scale = rsqrtf(static_cast<float>(HeadDim));
            float tile_max = kNegativeInfinity;
            bool tile_has_value = false;
#pragma unroll
            for (unsigned key_column = 0u; key_column < kKeyTile; ++key_column) {
                const unsigned logical_token = key_tile_begin + key_column;
                const bool visible = row < valid_rows && logical_token < kv_seq_len &&
                    logical_token >= row_begin[row] && logical_token < row_end[row] &&
                    physical[key_column] != kInvalidToken;
                if (visible) {
                    tile_max = fmaxf(
                        tile_max,
                        scores[row * kKeyTile + key_column] * scale);
                    tile_has_value = true;
                }
            }
            float tile_denom = 0.0f;
#pragma unroll
            for (unsigned key_column = 0u; key_column < kKeyTile; ++key_column) {
                const unsigned logical_token = key_tile_begin + key_column;
                const bool visible = tile_has_value && row < valid_rows &&
                    logical_token < kv_seq_len && logical_token >= row_begin[row] &&
                    logical_token < row_end[row] && physical[key_column] != kInvalidToken;
                const float probability = visible
                    ? expf(scores[row * kKeyTile + key_column] * scale - tile_max)
                    : 0.0f;
                probabilities[row * kKeyTile + key_column] = __float2half_rn(probability);
                tile_denom += probability;
            }

            if (tile_has_value) {
                const float old_max = running_max[row];
                const float old_denom = running_denom[row];
                const float next_max = fmaxf(old_max, tile_max);
                const float alpha = old_denom > 0.0f ? expf(old_max - next_max) : 0.0f;
                const float beta = expf(tile_max - next_max);
                running_max[row] = next_max;
                running_denom[row] = old_denom * alpha + tile_denom * beta;
                row_alpha[row] = alpha;
                row_beta[row] = beta;
            } else {
                row_alpha[row] = 1.0f;
                row_beta[row] = 0.0f;
            }
        }
        __syncthreads();

        // Reuse the K tile allocation for V only after score consumers finish.
        for (unsigned index = tid; index < kKeyTile * HeadDim; index += kThreads) {
            const unsigned key_row = index / HeadDim;
            const unsigned column = index - key_row * HeadDim;
            const unsigned token = physical[key_row];
            kv_shared[index] = token != kInvalidToken
                ? reinterpret_cast<const half*>(
                      v + static_cast<size_t>(token) * value_row_bytes)[column]
                : __float2half_rn(0.0f);
        }
        __syncthreads();

        wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major>
            probability_fragment;
        wmma::load_matrix_sync(probability_fragment, probabilities, kKeyTile);
        float* const scratch = warp_scratch + warp * kQueryTile * 16u;
#pragma unroll
        for (unsigned fragment_index = 0u;
             fragment_index < kFragmentsPerWarp;
             ++fragment_index) {
            const unsigned output_tile = warp + fragment_index * kWarps;
            wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major>
                value_fragment;
            wmma::fragment<wmma::accumulator, 16, 16, 16, float>
                tile_output_fragment;
            wmma::load_matrix_sync(
                value_fragment, kv_shared + output_tile * 16u, HeadDim);
            wmma::fill_fragment(tile_output_fragment, 0.0f);
            wmma::mma_sync(
                tile_output_fragment,
                probability_fragment,
                value_fragment,
                tile_output_fragment);

            // Accumulator-fragment lane layouts are intentionally opaque.
            // Round-trip through warp-private shared storage to apply exact
            // row-wise online-softmax merge factors without layout assumptions.
            wmma::store_matrix_sync(
                scratch,
                output_fragments[fragment_index],
                16,
                wmma::mem_row_major);
            __syncwarp();
            for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                scratch[index] *= row_alpha[index / 16u];
            }
            __syncwarp();
            wmma::load_matrix_sync(
                output_fragments[fragment_index],
                scratch,
                16,
                wmma::mem_row_major);

            wmma::store_matrix_sync(
                scratch, tile_output_fragment, 16, wmma::mem_row_major);
            __syncwarp();
            for (unsigned index = lane; index < 16u * 16u; index += 32u) {
                scratch[index] *= row_beta[index / 16u];
            }
            __syncwarp();
            wmma::load_matrix_sync(
                tile_output_fragment, scratch, 16, wmma::mem_row_major);
#pragma unroll
            for (unsigned element = 0u;
                 element < output_fragments[fragment_index].num_elements;
                 ++element) {
                output_fragments[fragment_index].x[element] +=
                    tile_output_fragment.x[element];
            }
        }
        __syncthreads();
    }

#pragma unroll
    for (unsigned fragment_index = 0u;
         fragment_index < kFragmentsPerWarp;
         ++fragment_index) {
        const unsigned output_tile = warp + fragment_index * kWarps;
        float* const scratch = warp_scratch + warp * kQueryTile * 16u;
        wmma::store_matrix_sync(
            scratch,
            output_fragments[fragment_index],
            16,
            wmma::mem_row_major);
        __syncwarp();
        for (unsigned index = lane; index < 16u * 16u; index += 32u) {
            const unsigned row = index / 16u;
            const unsigned column = index - row * 16u;
            if (row < valid_rows) {
                const float denominator = running_denom[row];
                const size_t output_index =
                    static_cast<size_t>(tile_query_begin + row) * kHeads * HeadDim +
                    head * HeadDim + output_tile * 16u + column;
                dst[output_index] = denominator > 0.0f
                    ? scratch[index] / denominator
                    : 0.0f;
            }
        }
        __syncwarp();
    }
}

template <int HeadDim, int SlidingWindow>
__device__ void reference_prefill_body(
    float* dst,
    const float* q,
    const unsigned char* k,
    const unsigned char* v,
    const unsigned* block_table,
    unsigned batch,
    unsigned q_seq_len,
    unsigned kv_seq_len,
    unsigned num_heads,
    unsigned num_kv_heads,
    unsigned head_dim,
    unsigned query_position_offset,
    unsigned kv_position_offset,
    unsigned sliding_window,
    unsigned total_sequence_len,
    unsigned mask_len,
    unsigned bias_mode,
    unsigned key_row_bytes,
    unsigned base_key_row_bytes,
    unsigned value_row_bytes,
    unsigned block_count,
    unsigned page_size_tokens,
    unsigned format,
    unsigned value_format,
    unsigned physical_token_capacity,
    const unsigned* decode_scalars,
    unsigned* shared_error,
    float* warp_sums,
    unsigned* shared_physical,
    float* shared_max,
    float* shared_denom,
    float* shared_alpha,
    float* shared_beta
) {
    if (!validate_launch<HeadDim, SlidingWindow, false>(
            dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len,
            num_heads, num_kv_heads, head_dim, query_position_offset,
            kv_position_offset, sliding_window, total_sequence_len, mask_len,
            bias_mode, key_row_bytes, base_key_row_bytes, value_row_bytes,
            block_count, page_size_tokens, format, value_format,
            physical_token_capacity, decode_scalars, shared_error)) {
        return;
    }

    const unsigned tid = threadIdx.x;
    const unsigned lane = tid & 31u;
    const unsigned warp = tid >> 5u;
    constexpr unsigned kReferenceWarps = HeadDim / 32u;
    const unsigned head = blockIdx.x;
    const unsigned query_index = blockIdx.y;
    const unsigned query_pos = query_position_offset + query_index;
    const VisibleRange range = visible_range(
        query_pos, kv_seq_len, kv_position_offset, SlidingWindow);
    const size_t q_offset =
        static_cast<size_t>(query_index) * kHeads * HeadDim + head * HeadDim;
    const float query_value = q[q_offset + tid];
    const float scale = rsqrtf(static_cast<float>(HeadDim));
    float output_accumulator = 0.0f;
    if (tid == 0u) {
        *shared_max = kNegativeInfinity;
        *shared_denom = 0.0f;
    }
    __syncthreads();

    for (unsigned logical_token = range.begin;
         logical_token < range.end;
         ++logical_token) {
        if (tid == 0u) {
            *shared_physical = physical_token(
                logical_token, block_table, block_count,
                page_size_tokens, physical_token_capacity);
            if (*shared_physical == kInvalidToken) {
                *shared_error = kStatusMappedPageOutOfBounds;
            }
        }
        __syncthreads();
        if (*shared_error != kStatusOk) {
            return;
        }
        const half* const key_row = reinterpret_cast<const half*>(
            k + static_cast<size_t>(*shared_physical) * key_row_bytes);
        const half* const value_row = reinterpret_cast<const half*>(
            v + static_cast<size_t>(*shared_physical) * value_row_bytes);
        float partial = query_value * __half2float(key_row[tid]);
        for (unsigned offset = 16u; offset > 0u; offset >>= 1u) {
            partial += __shfl_down_sync(0xffffffffu, partial, offset);
        }
        if (lane == 0u) warp_sums[warp] = partial;
        __syncthreads();
        float dot = warp == 0u && lane < kReferenceWarps
            ? warp_sums[lane]
            : 0.0f;
        if (warp == 0u) {
            for (unsigned offset = 16u; offset > 0u; offset >>= 1u) {
                dot += __shfl_down_sync(0xffffffffu, dot, offset);
            }
            if (lane == 0u) {
                const float score = dot * scale;
                const float next_max = fmaxf(*shared_max, score);
                *shared_alpha = *shared_denom > 0.0f
                    ? expf(*shared_max - next_max)
                    : 0.0f;
                *shared_beta = expf(score - next_max);
                *shared_denom = *shared_denom * *shared_alpha + *shared_beta;
                *shared_max = next_max;
            }
        }
        __syncthreads();
        output_accumulator = output_accumulator * *shared_alpha +
            *shared_beta * __half2float(value_row[tid]);
        __syncthreads();
    }

    dst[q_offset + tid] = *shared_denom > 0.0f
        ? output_accumulator / *shared_denom
        : 0.0f;
}

} // namespace ANTFLY_FLASH_NAMESPACE

#define ANTFLY_FLASH_ARGUMENTS \
    float* dst, \
    const float* q, \
    const unsigned char* k, \
    const unsigned char* v, \
    const unsigned* block_table, \
    const unsigned char* attn_or_mask, \
    const float* bias, \
    unsigned batch, \
    unsigned q_seq_len, \
    unsigned kv_seq_len, \
    unsigned num_heads, \
    unsigned num_kv_heads, \
    unsigned head_dim, \
    unsigned query_position_offset, \
    unsigned kv_position_offset, \
    unsigned sliding_window, \
    unsigned total_sequence_len, \
    unsigned mask_len, \
    unsigned bias_mode, \
    unsigned key_row_bytes, \
    unsigned base_key_row_bytes, \
    unsigned value_row_bytes, \
    unsigned block_count, \
    unsigned page_size_tokens, \
    unsigned format, \
    unsigned value_format, \
    unsigned physical_token_capacity, \
    const unsigned* decode_scalars

#define ANTFLY_FORWARD_ARGUMENTS \
    dst, q, k, v, block_table, batch, q_seq_len, kv_seq_len, \
    num_heads, num_kv_heads, head_dim, query_position_offset, \
    kv_position_offset, sliding_window, total_sequence_len, mask_len, bias_mode, \
    key_row_bytes, base_key_row_bytes, value_row_bytes, block_count, \
    page_size_tokens, format, value_format, physical_token_capacity, decode_scalars

extern "C" __global__ void
ANTFLY_FLASH_KERNEL(
    ANTFLY_FLASH_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    extern __shared__ __align__(16) unsigned char storage[];
    ANTFLY_FLASH_NAMESPACE::flash_prefill_body<
        ANTFLY_FLASH_HEAD_DIM,
        ANTFLY_FLASH_SLIDING_WINDOW>(ANTFLY_FORWARD_ARGUMENTS, storage);
}

#ifdef ANTFLY_FLASH_REFERENCE_KERNEL
extern "C" __global__ void
ANTFLY_FLASH_REFERENCE_KERNEL(
    ANTFLY_FLASH_ARGUMENTS
) {
    (void)attn_or_mask;
    (void)bias;
    __shared__ unsigned shared_error;
    __shared__ float warp_sums[16];
    __shared__ unsigned shared_physical;
    __shared__ float shared_max;
    __shared__ float shared_denom;
    __shared__ float shared_alpha;
    __shared__ float shared_beta;
    ANTFLY_FLASH_NAMESPACE::reference_prefill_body<
        ANTFLY_FLASH_HEAD_DIM,
        ANTFLY_FLASH_SLIDING_WINDOW>(
        ANTFLY_FORWARD_ARGUMENTS,
        &shared_error,
        warp_sums,
        &shared_physical,
        &shared_max,
        &shared_denom,
        &shared_alpha,
        &shared_beta);
}
#endif

#undef ANTFLY_FORWARD_ARGUMENTS
#undef ANTFLY_FLASH_ARGUMENTS
#undef ANTFLY_FLASH_SLIDING_WINDOW
#undef ANTFLY_FLASH_HEAD_DIM
#undef ANTFLY_FLASH_KERNEL
#undef ANTFLY_FLASH_NAMESPACE
// quant-kernel-codegen:end generated CUDA attention kernels

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
    unsigned long long physical = (unsigned long long)logical_token;
    if (block_table != 0 && block_count != 0u && page_size_tokens != 0u) {
        unsigned int block_index = logical_token / page_size_tokens;
        if (block_index >= block_count) return 0xffffffffu;
        unsigned int token_offset = logical_token - block_index * page_size_tokens;
        const unsigned long long physical_block = (unsigned long long)block_table[block_index];
        physical = physical_block * (unsigned long long)page_size_tokens +
            (unsigned long long)token_offset;
    }
    if (physical > 0xffffffffull ||
        physical >= (unsigned long long)physical_token_capacity) return 0xffffffffu;
    return (unsigned int)physical;
}

// Validate the two supported page-addressing contracts before a raw-F16 GQA
// prefill kernel starts writing output:
//   identity: (block_table == null, block_count == 0)
//   explicit: (block_table != null, block_count != 0)
// Device KV allocations contain complete pages, so both modes retain that
// invariant. Identity translation additionally needs every logical token to
// fit directly in the physical allocation; explicit translation is bounded
// by the number of logical blocks and termite_tq_physical_token validates each
// mapped physical token.
__device__ __forceinline__ bool termite_tq_f16_prefill_page_layout_valid(
    unsigned int kv_seq_len,
    const unsigned int* block_table,
    unsigned int block_count,
    unsigned int page_size_tokens,
    unsigned int physical_token_capacity
) {
    if (kv_seq_len == 0u || page_size_tokens == 0u) return false;
    const bool table_present = block_table != 0;
    const bool count_present = block_count != 0u;
    if (table_present != count_present) return false;
    if (physical_token_capacity == 0u ||
        physical_token_capacity % page_size_tokens != 0u) return false;
    if (!table_present) return physical_token_capacity >= kv_seq_len;
    if (physical_token_capacity < page_size_tokens) return false;
    const unsigned int required_blocks = kv_seq_len / page_size_tokens +
        (kv_seq_len % page_size_tokens != 0u ? 1u : 0u);
    return block_count >= required_blocks;
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

// Exact-F16 paged GQA prefill candidate. One CTA owns a 16-query tile for a
// single query head. Keys are visited once in chronological order and reused
// by all 16 queries before advancing. The block width exactly matches the head
// dimension, preserving the established fast kernel's F32 block-reduction
// topology while avoiding both BF16 staging and K/V tile regrouping.
// Grid: (num_heads, ceil(q_seq_len / 16)); blockDim.x = head_dim (256 or 512).
extern "C" __global__ void termite_gqa_attention_prefill_tiled_f16_exact_f32(
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

// Raw-F16 warp-tiled GQA prefill candidate. Eight warps own two queries each.
// For every 32-key span, lanes compute QK scores independently in parallel,
// then replay those scores and raw F16 values in chronological key order so
// online-softmax/PV semantics never depend on a BF16 staging tile.
// Grid: (num_heads, ceil(q_seq_len / 16)); blockDim.x = 256.
extern "C" __global__ void termite_gqa_attention_prefill_tiled_f16_warp_f32(
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

// q4_0_hmma packed device layout (mirrors q8_0_hmma: scales | quants).
//
// Host pack contract (to be implemented as packQ4_0TensorCore in
// cuda_compute.zig): input is the raw GGUF Q4_0 byte stream for an
// [out_dim, in_dim] weight, 18 bytes per 32-value block laid out row-major
// by output column (block_index = col * row_blocks + block with
// row_blocks = in_dim / 32; the engine additionally requires
// in_dim % 256 == 0 for tensor-core packing eligibility). Raw block bytes:
//   src[0..2)  f16 little-endian scale d
//   src[2..18) 16 quant bytes; byte j holds element j in the low nibble and
//              element j + 16 in the high nibble; value = (nibble - 8) * d
// Packed output (block_count = out_dim * row_blocks):
//   out[0                     .. block_count * 2)   scales: block_index * 2
//                                                   is the f16 LE scale
//                                                   (raw src[0..2) copied)
//   out[block_count * 2       .. block_count * 18)  quants: block_count * 2
//                                                   + block_index * 16 is
//                                                   the 16 raw quant bytes
//                                                   (raw src[2..18) copied,
//                                                   nibble order unchanged)
// Pack loop: for block in 0..block_count:
//   copy raw[block*18 .. block*18+2)  -> out[block*2 .. block*2+2)
//   copy raw[block*18+2 .. block*18+18) -> out[block_count*2 + block*16 ..)
// Total packed size = block_count * 18 bytes.
__device__ __forceinline__ half termite_q4_0_tc_value_at(
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
    unsigned char packed_q = packed[block_count * 2u + block_index * 16u + (lane & 15u)];
    int q = lane < 16u ? (int)(packed_q & 0x0fu) : (int)(packed_q >> 4u);
    return __float2half_rn((float)(q - 8) * __half2float(d));
}

static constexpr unsigned int TERMITE_QTC_FMT_Q8_0 = 0u;
static constexpr unsigned int TERMITE_QTC_FMT_Q4_K = 1u;
static constexpr unsigned int TERMITE_QTC_FMT_Q4_0 = 2u;

// Float-returning Q4_0 dequant for the bf16 tensor-core mirror. Identical math
// to termite_q4_0_tc_value_at but the result stays in float so the caller
// rounds once to the WMMA element type (maximizes precision).
__device__ __forceinline__ float termite_q4_0_tc_value_at_f32(
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
    unsigned char packed_q = packed[block_count * 2u + block_index * 16u + (lane & 15u)];
    int q = lane < 16u ? (int)(packed_q & 0x0fu) : (int)(packed_q >> 4u);
    return (float)(q - 8) * __half2float(d);
}

// WMMA element policy for termite_qtc_hmma_tile. The half specialization
// reproduces the original tile code exactly (from_float == __float2half_rn,
// load_b == the FMT dequant ternary), so the emitted code for the existing f16
// Q8_0/Q4_K/Q4_0 tc_hmma kernels is byte-for-byte unchanged. The bf16
// specialization exists because Gemma activations exceed f16's 65504 range;
// __nv_bfloat16 shares f32's exponent range, so bf16*bf16 -> f32 WMMA stays
// bit-close to the exact f32 mirror. bf16 is wired for Q4_0 only.
template <typename WmmaElem>
struct termite_qtc_tile_ops;

template <>
struct termite_qtc_tile_ops<half> {
    static __device__ __forceinline__ half from_float(float x) {
        return __float2half_rn(x);
    }
    template <unsigned int FMT>
    static __device__ __forceinline__ half load_b(
        const unsigned char* packed_weight,
        unsigned int out_dim,
        unsigned int col,
        unsigned int row_blocks,
        unsigned int k_abs
    ) {
        return FMT == TERMITE_QTC_FMT_Q4_K
            ? termite_q4_k_tc_value_at(packed_weight, out_dim, col, row_blocks, k_abs)
            : (FMT == TERMITE_QTC_FMT_Q4_0
                ? termite_q4_0_tc_value_at(packed_weight, out_dim, col, row_blocks, k_abs)
                : termite_q8_0_tc_value_at(packed_weight, out_dim, col, row_blocks, k_abs));
    }
};

template <>
struct termite_qtc_tile_ops<__nv_bfloat16> {
    static __device__ __forceinline__ __nv_bfloat16 from_float(float x) {
        return __float2bfloat16_rn(x);
    }
    // bf16 tensor-core path is Q4_0-only; dequantize in float, round once.
    template <unsigned int FMT>
    static __device__ __forceinline__ __nv_bfloat16 load_b(
        const unsigned char* packed_weight,
        unsigned int out_dim,
        unsigned int col,
        unsigned int row_blocks,
        unsigned int k_abs
    ) {
        return __float2bfloat16_rn(
            termite_q4_0_tc_value_at_f32(packed_weight, out_dim, col, row_blocks, k_abs));
    }
};

template <unsigned int MODE, unsigned int FMT, typename WmmaElem = half>
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
    unsigned int row_blocks = FMT == TERMITE_QTC_FMT_Q4_K ? (in_dim / 256u) : (in_dim / 32u);

    __shared__ WmmaElem a_tile[TERMITE_QTC_M * TERMITE_QTC_K];
    __shared__ WmmaElem b_tile[TERMITE_QTC_K * TERMITE_QTC_N];
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
            a_tile[i] = termite_qtc_tile_ops<WmmaElem>::from_float(x);
        }
        for (unsigned int i = tid; i < TERMITE_QTC_K * TERMITE_QTC_N; i += TERMITE_QTC_THREADS) {
            unsigned int local_k = i / TERMITE_QTC_N;
            unsigned int local_col = i - local_k * TERMITE_QTC_N;
            unsigned int col = col_base + local_col;
            unsigned int k_abs = k_base + local_k;
            WmmaElem w = termite_qtc_tile_ops<WmmaElem>::from_float(0.0f);
            if (col < out_dim && k_abs < in_dim) {
                w = termite_qtc_tile_ops<WmmaElem>::template load_b<FMT>(packed_weight, out_dim, col, row_blocks, k_abs);
            }
            b_tile[i] = w;
        }
        __syncthreads();

        wmma::fragment<wmma::matrix_a, 16, 16, 16, WmmaElem, wmma::row_major> a_frag;
        wmma::fragment<wmma::matrix_b, 16, 16, 16, WmmaElem, wmma::row_major> b_frag;
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
    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<2u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<3u, TERMITE_QTC_FMT_Q8_0>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_k_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<2u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<3u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<4u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<5u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
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
    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_K>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_0_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q4_0>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_0_bias_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_0>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_0_bias_gelu_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<2u, TERMITE_QTC_FMT_Q4_0>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
}

extern "C" __global__ void termite_linear_q4_0_bias_add_f32_tc_hmma(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    termite_qtc_hmma_tile<3u, TERMITE_QTC_FMT_Q4_0>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
}

// BF16 tensor-core mirror of the Q4_0 tc_hmma linear set. Same signatures and
// launch geometry as the f16 kernels above (block=256, grid.x=ceil(out_dim/32),
// grid.y=ceil(rows/64), no dynamic smem); the only difference is the WMMA
// element type. Consumes the identical q4_0_hmma packed layout. bf16 has f32's
// exponent range, so these stay numerically close to the exact f32 mirror even
// when Gemma activations exceed f16's 65504 limit.
extern "C" __global__ void termite_linear_q4_0_f32_tc_hmma_bf16(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
#if __CUDA_ARCH__ >= 800
    termite_qtc_hmma_tile<0u, TERMITE_QTC_FMT_Q4_0, __nv_bfloat16>(dst, input, packed_weight, nullptr, nullptr, rows, in_dim, out_dim);
#endif
}

extern "C" __global__ void termite_linear_q4_0_bias_f32_tc_hmma_bf16(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
#if __CUDA_ARCH__ >= 800
    termite_qtc_hmma_tile<1u, TERMITE_QTC_FMT_Q4_0, __nv_bfloat16>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
#endif
}

extern "C" __global__ void termite_linear_q4_0_bias_gelu_f32_tc_hmma_bf16(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
#if __CUDA_ARCH__ >= 800
    termite_qtc_hmma_tile<2u, TERMITE_QTC_FMT_Q4_0, __nv_bfloat16>(dst, input, packed_weight, bias, nullptr, rows, in_dim, out_dim);
#endif
}

extern "C" __global__ void termite_linear_q4_0_bias_add_f32_tc_hmma_bf16(
    float* dst,
    const float* input,
    const unsigned char* packed_weight,
    const float* bias,
    const float* residual,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
#if __CUDA_ARCH__ >= 800
    termite_qtc_hmma_tile<3u, TERMITE_QTC_FMT_Q4_0, __nv_bfloat16>(dst, input, packed_weight, bias, residual, rows, in_dim, out_dim);
#endif
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

// Fused DeBERTa-v3 disentangled attention for encoder prefill.  The legacy
// elementwise implementation above computes the same score independently for
// every output channel (head_dim times).  This kernel assigns one block to a
// (batch, head, query) tuple, forms each content/relative score exactly once,
// keeps the normalized probabilities in shared memory, then cooperatively
// applies V.  GLiNER2's 64-wide heads and <=512-token encoder inputs fit the
// fixed shared-memory tile; other shapes retain the general implementation.
extern "C" __global__ void termite_deberta_attention_fused_f32(
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

// FP16-storage DeBERTa-v3 encoder prefill attention.  Unlike the F32 fused
// fallback above, each warp owns a query and performs an online softmax while
// accumulating two FP32 value lanes.  This avoids the score/probability
// workspace entirely, keeps the relative-position gathers local to the warp,
// and gives the common H=12, D=64 encoder shape eight independent queries per
// CTA.  The output remains F32 so residuals and layer norms retain the graph's
// established numerical contract.
//
// The three DeBERTa score terms are deliberately not expressed as a fake GEMM:
// the relative tensors are Toeplitz gathers (their vector depends on both q
// and k).  They are therefore evaluated with coalesced half loads and FP32
// accumulation.  Tensor-core score/PV candidates belong in a separate
// materialized-score schedule; this streaming route is the latency baseline.
extern "C" __global__ void termite_deberta_attention_stream_f16(
    float* dst,
    const unsigned short* q,
    const unsigned short* k,
    const unsigned short* v,
    const unsigned short* q_r,
    const unsigned short* k_r,
    const long long* mask,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
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

// Generated tensor-core schedule for DeBERTa-v3 encoder prefill.  A CTA owns
// sixteen query rows of one [batch, head] matrix and streams 32-key blocks.
// The two disentangled relative-position terms are not ordinary GEMMs: each
// needs a diagonal from a (M x (M+N-1)) tile.  We form those compact tiles in
// shared memory with WMMA, gather the diagonals locally, then use an online
// softmax and WMMA P*V.  No head packing, score/probability workspace, or
// cuBLASLt attention launch is required.
//
// Inputs remain graph-layout F32, preserving the encoder ABI.  Conversion to
// FP16 happens only while staging the current tile; every MMA accumulator,
// softmax state, and output value is F32.  The fixed D=64 / S<=256 envelope
// matches DeBERTa-base and leaves the general fused-F32 implementation as the
// correctness fallback for all other shapes.
extern "C" __global__ __launch_bounds__(256) void termite_deberta_attention_tc_f16_m16n32(
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
    constexpr unsigned int kThreads = 256u;
    constexpr unsigned int kQueryTile = 16u;
    constexpr unsigned int kKeyTile = 32u;
    constexpr unsigned int kHeadDim = 64u;
    constexpr unsigned int kRelTile = kQueryTile + kKeyTile - 1u;
    constexpr unsigned int kRelPitch = 48u; // WMMA requires a 16-column tail.
    constexpr float kNegInf = -3.402823466e+38f;
    if (seq_len == 0u || seq_len > 256u || head_dim != kHeadDim || blockDim.x != kThreads) return;

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

    __shared__ __align__(16) half q_tile[kQueryTile * kHeadDim];
    // K is transposed so it is directly consumable as a row-major WMMA B.
    __shared__ __align__(16) half k_tile[kHeadDim * kKeyTile];
    // The p2c term uses K as a WMMA A operand, so retain its native [N, D]
    // layout as well.  Both layouts are CTA-local and replace the old global
    // head-packing buffer.
    __shared__ __align__(16) half k_rows[kKeyTile * kHeadDim];
    __shared__ __align__(16) half v_tile[kKeyTile * kHeadDim];
    // Relative tensors are [D, R] matrices. R is padded to 48 to retain
    // WMMA's 16-wide fragment contract without special tail code.
    __shared__ __align__(16) half qr_tile[kHeadDim * kRelPitch];
    __shared__ __align__(16) half kr_tile[kHeadDim * kRelPitch];
    __shared__ __align__(16) float scores[kQueryTile * kKeyTile];
    __shared__ __align__(16) float c2p_scores[kQueryTile * kRelPitch];
    __shared__ __align__(16) float p2c_scores[kKeyTile * kRelPitch];
    __shared__ __align__(16) half probabilities[kQueryTile * kKeyTile];
    __shared__ __align__(16) float output[kQueryTile * kHeadDim];
    __shared__ float running_max[kQueryTile];
    __shared__ float running_sum[kQueryTile];
    __shared__ float row_alpha[kQueryTile];

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int row = index / kHeadDim;
        const unsigned int d = index - row * kHeadDim;
        const unsigned int qi = query_start + row;
        q_tile[index] = qi < seq_len ? __float2half_rn(q[(b * seq_len + qi) * hidden + head_off + d]) : __float2half_rn(0.0f);
        output[index] = 0.0f;
    }
    if (tid < kQueryTile) {
        running_max[tid] = kNegInf;
        running_sum[tid] = 0.0f;
    }
    __syncthreads();

    for (unsigned int key_start = 0u; key_start < seq_len; key_start += kKeyTile) {
        // The relative window spans r=(query_start - key_start) +
        // [-(N-1), M-1], exactly the values needed by this score tile.
        const int rel_low = (int)query_start + (int)seq_len - 1 - ((int)key_start + (int)kKeyTile - 1);
        for (unsigned int index = tid; index < kHeadDim * kKeyTile; index += kThreads) {
            const unsigned int d = index / kKeyTile;
            const unsigned int n = index - d * kKeyTile;
            const unsigned int ki = key_start + n;
            const float kval = ki < seq_len ? k[(b * seq_len + ki) * hidden + head_off + d] : 0.0f;
            k_tile[index] = __float2half_rn(kval);
            k_rows[n * kHeadDim + d] = __float2half_rn(kval);
            v_tile[n * kHeadDim + d] = __float2half_rn(ki < seq_len ? v[(b * seq_len + ki) * hidden + head_off + d] : 0.0f);
        }
        for (unsigned int index = tid; index < kHeadDim * kRelPitch; index += kThreads) {
            const unsigned int d = index / kRelPitch;
            const unsigned int r = index - d * kRelPitch;
            const int rel = rel_low + (int)r;
            const float qrv = r < kRelTile && rel >= 0 && rel < (int)(seq_len * 2u - 1u) ? q_r[(unsigned int)rel * hidden + head_off + d] : 0.0f;
            const float krv = r < kRelTile && rel >= 0 && rel < (int)(seq_len * 2u - 1u) ? k_r[(unsigned int)rel * hidden + head_off + d] : 0.0f;
            qr_tile[index] = __float2half_rn(qrv);
            kr_tile[index] = __float2half_rn(krv);
        }
        __syncthreads();

        // Q*K^T: two 16x16 fragments, one per warp.
        if (warp < 2u) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::fill_fragment(acc, 0.0f);
            #pragma unroll
            for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
                wmma::load_matrix_sync(a_frag, q_tile + d0, kHeadDim);
                wmma::load_matrix_sync(b_frag, k_tile + d0 * kKeyTile + warp * 16u, kKeyTile);
                wmma::mma_sync(acc, a_frag, b_frag, acc);
            }
            wmma::store_matrix_sync(scores + warp * 16u, acc, kKeyTile, wmma::mem_row_major);
        }

        // Q*Kr^T: three compact relative columns. Warp 2 handles chunks 0/2
        // and warp 3 handles chunk 1, keeping all relative gathers local.
        if (warp == 2u || warp == 3u) {
            const unsigned int first_chunk = warp == 2u ? 0u : 1u;
            for (unsigned int chunk = first_chunk; chunk < 3u; chunk += 2u) {
                wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
                wmma::fill_fragment(acc, 0.0f);
                #pragma unroll
                for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                    wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                    wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
                    wmma::load_matrix_sync(a_frag, q_tile + d0, kHeadDim);
                    wmma::load_matrix_sync(b_frag, kr_tile + d0 * kRelPitch + chunk * 16u, kRelPitch);
                    wmma::mma_sync(acc, a_frag, b_frag, acc);
                }
                wmma::store_matrix_sync(c2p_scores + chunk * 16u, acc, kRelPitch, wmma::mem_row_major);
            }
        }

        // K*Qr^T: six compact relative fragments.  The result is indexed by
        // key row so the p2c diagonal is a simple shared-memory gather.
        if (warp >= 4u) {
            for (unsigned int work = warp - 4u; work < 6u; work += 4u) {
                const unsigned int key_chunk = work / 3u;
                const unsigned int rel_chunk = work - key_chunk * 3u;
                wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
                wmma::fill_fragment(acc, 0.0f);
                #pragma unroll
                for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                    wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                    wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
                    wmma::load_matrix_sync(a_frag, k_rows + (key_chunk * 16u) * kHeadDim + d0, kHeadDim);
                    wmma::load_matrix_sync(b_frag, qr_tile + d0 * kRelPitch + rel_chunk * 16u, kRelPitch);
                    wmma::mma_sync(acc, a_frag, b_frag, acc);
                }
                wmma::store_matrix_sync(p2c_scores + (key_chunk * 16u) * kRelPitch + rel_chunk * 16u, acc, kRelPitch, wmma::mem_row_major);
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
            if (qi < seq_len && ki < seq_len && mask[b * seq_len + ki] != 0ll) {
                const unsigned int rel = m + kKeyTile - 1u - lane;
                score = (scores[m * kKeyTile + lane] + c2p_scores[m * kRelPitch + rel] + p2c_scores[lane * kRelPitch + rel]) * scale;
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
                wmma::load_matrix_sync(v_frag, v_tile + (key_chunk * 16u) * kHeadDim + warp * 16u, kHeadDim);
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
            dst[(b * seq_len + qi) * hidden + head_off + d] = denom > 0.0f ? output[index] / denom : 0.0f;
        }
    }
}

// Larger-query generated tensor-core DeBERTa prefill schedule.  The M16xN32
// variant above minimizes per-CTA shared memory, but it rereads every K/V row
// for sixteen query tiles at S=256.  This M32xN16 schedule keeps the same
// tensor-core work while halving CTA count and K/V rereads.  Its 42 KiB static
// shared footprint remains below the portable 48 KiB limit, so it needs no
// occupancy or shared-memory carveout API special case on SM80+.
extern "C" __global__ __launch_bounds__(256) void termite_deberta_attention_tc_f16_m32n16(
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
    constexpr unsigned int kThreads = 256u;
    constexpr unsigned int kQueryTile = 32u;
    constexpr unsigned int kKeyTile = 16u;
    constexpr unsigned int kHeadDim = 64u;
    constexpr unsigned int kRelTile = kQueryTile + kKeyTile - 1u;
    constexpr unsigned int kRelPitch = 48u;
    constexpr float kNegInf = -3.402823466e+38f;
    if (seq_len == 0u || seq_len > 256u || head_dim != kHeadDim || blockDim.x != kThreads) return;

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

    __shared__ __align__(16) half q_tile[kQueryTile * kHeadDim];
    __shared__ __align__(16) half k_tile[kHeadDim * kKeyTile];
    __shared__ __align__(16) half k_rows[kKeyTile * kHeadDim];
    __shared__ __align__(16) half v_tile[kKeyTile * kHeadDim];
    __shared__ __align__(16) half qr_tile[kHeadDim * kRelPitch];
    __shared__ __align__(16) half kr_tile[kHeadDim * kRelPitch];
    __shared__ __align__(16) float scores[kQueryTile * kKeyTile];
    __shared__ __align__(16) float c2p_scores[kQueryTile * kRelPitch];
    __shared__ __align__(16) float p2c_scores[kKeyTile * kRelPitch];
    __shared__ __align__(16) half probabilities[kQueryTile * kKeyTile];
    __shared__ __align__(16) float output[kQueryTile * kHeadDim];
    __shared__ float running_max[kQueryTile];
    __shared__ float running_sum[kQueryTile];
    __shared__ float row_alpha[kQueryTile];

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int row = index / kHeadDim;
        const unsigned int d = index - row * kHeadDim;
        const unsigned int qi = query_start + row;
        q_tile[index] = qi < seq_len ? __float2half_rn(q[(b * seq_len + qi) * hidden + head_off + d]) : __float2half_rn(0.0f);
        output[index] = 0.0f;
    }
    if (tid < kQueryTile) {
        running_max[tid] = kNegInf;
        running_sum[tid] = 0.0f;
    }
    __syncthreads();

    for (unsigned int key_start = 0u; key_start < seq_len; key_start += kKeyTile) {
        const int rel_low = (int)query_start + (int)seq_len - 1 - ((int)key_start + (int)kKeyTile - 1);
        for (unsigned int index = tid; index < kHeadDim * kKeyTile; index += kThreads) {
            const unsigned int d = index / kKeyTile;
            const unsigned int n = index - d * kKeyTile;
            const unsigned int ki = key_start + n;
            const float kval = ki < seq_len ? k[(b * seq_len + ki) * hidden + head_off + d] : 0.0f;
            k_tile[index] = __float2half_rn(kval);
            k_rows[n * kHeadDim + d] = __float2half_rn(kval);
            v_tile[n * kHeadDim + d] = __float2half_rn(ki < seq_len ? v[(b * seq_len + ki) * hidden + head_off + d] : 0.0f);
        }
        for (unsigned int index = tid; index < kHeadDim * kRelPitch; index += kThreads) {
            const unsigned int d = index / kRelPitch;
            const unsigned int r = index - d * kRelPitch;
            const int rel = rel_low + (int)r;
            const float qrv = r < kRelTile && rel >= 0 && rel < (int)(seq_len * 2u - 1u) ? q_r[(unsigned int)rel * hidden + head_off + d] : 0.0f;
            const float krv = r < kRelTile && rel >= 0 && rel < (int)(seq_len * 2u - 1u) ? k_r[(unsigned int)rel * hidden + head_off + d] : 0.0f;
            qr_tile[index] = __float2half_rn(qrv);
            kr_tile[index] = __float2half_rn(krv);
        }
        __syncthreads();

        // Q*K^T has two M fragments.  The remaining warps produce the six
        // M32xR48 content-to-position fragments in parallel.
        if (warp < 2u) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::fill_fragment(acc, 0.0f);
            #pragma unroll
            for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
                wmma::load_matrix_sync(a_frag, q_tile + warp * 16u * kHeadDim + d0, kHeadDim);
                wmma::load_matrix_sync(b_frag, k_tile + d0 * kKeyTile, kKeyTile);
                wmma::mma_sync(acc, a_frag, b_frag, acc);
            }
            wmma::store_matrix_sync(scores + warp * 16u * kKeyTile, acc, kKeyTile, wmma::mem_row_major);
        }
        if (warp >= 2u) {
            const unsigned int work = warp - 2u;
            const unsigned int query_chunk = work / 3u;
            const unsigned int rel_chunk = work - query_chunk * 3u;
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::fill_fragment(acc, 0.0f);
            #pragma unroll
            for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
                wmma::load_matrix_sync(a_frag, q_tile + query_chunk * 16u * kHeadDim + d0, kHeadDim);
                wmma::load_matrix_sync(b_frag, kr_tile + d0 * kRelPitch + rel_chunk * 16u, kRelPitch);
                wmma::mma_sync(acc, a_frag, b_frag, acc);
            }
            wmma::store_matrix_sync(c2p_scores + query_chunk * 16u * kRelPitch + rel_chunk * 16u, acc, kRelPitch, wmma::mem_row_major);
        }
        __syncthreads();

        // K*Qr^T is M16xR48. It reuses warps after the first score phase.
        if (warp < 3u) {
            wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
            wmma::fill_fragment(acc, 0.0f);
            #pragma unroll
            for (unsigned int d0 = 0u; d0 < kHeadDim; d0 += 16u) {
                wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> a_frag;
                wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> b_frag;
                wmma::load_matrix_sync(a_frag, k_rows + d0, kHeadDim);
                wmma::load_matrix_sync(b_frag, qr_tile + d0 * kRelPitch + warp * 16u, kRelPitch);
                wmma::mma_sync(acc, a_frag, b_frag, acc);
            }
            wmma::store_matrix_sync(p2c_scores + warp * 16u, acc, kRelPitch, wmma::mem_row_major);
        }
        __syncthreads();

        // Two independent 16-lane reductions let every warp update four
        // query rows while all lanes remain useful on the N=16 score tile.
        #pragma unroll
        for (unsigned int group = 0u; group < 2u; ++group) {
            const unsigned int lane_group = lane >> 4u;
            const unsigned int m = warp * 4u + group * 2u + lane_group;
            const unsigned int qi = query_start + m;
            const unsigned int ki = key_start + (lane & 15u);
            const unsigned int group_mask = lane_group == 0u ? 0x0000ffffu : 0xffff0000u;
            float score = kNegInf;
            if (qi < seq_len && ki < seq_len && mask[b * seq_len + ki] != 0ll) {
                const unsigned int rel = m + kKeyTile - 1u - (lane & 15u);
                score = (scores[m * kKeyTile + (lane & 15u)] + c2p_scores[m * kRelPitch + rel] + p2c_scores[(lane & 15u) * kRelPitch + rel]) * scale;
            }
            scores[m * kKeyTile + (lane & 15u)] = score;
            float tile_max = score;
            #pragma unroll
            for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) tile_max = fmaxf(tile_max, __shfl_down_sync(group_mask, tile_max, offset));
            const unsigned int leader = lane_group * 16u;
            tile_max = __shfl_sync(group_mask, tile_max, leader);
            const float old_max = running_max[m];
            const float next_max = fmaxf(old_max, tile_max);
            const float alpha = old_max > kNegInf * 0.5f ? expf(old_max - next_max) : 0.0f;
            const float beta = score > kNegInf * 0.5f ? expf(score - next_max) : 0.0f;
            float tile_sum = beta;
            #pragma unroll
            for (unsigned int offset = 8u; offset > 0u; offset >>= 1u) tile_sum += __shfl_down_sync(group_mask, tile_sum, offset);
            tile_sum = __shfl_sync(group_mask, tile_sum, leader);
            probabilities[m * kKeyTile + (lane & 15u)] = __float2half_rn(beta);
            if ((lane & 15u) == 0u) {
                running_max[m] = next_max;
                running_sum[m] = running_sum[m] * alpha + tile_sum;
                row_alpha[m] = alpha;
            }
        }
        __syncthreads();

        for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) output[index] *= row_alpha[index / kHeadDim];
        __syncthreads();

        // Eight warps cover the two M fragments and four output-D fragments
        // of the M32xD64 P*V matrix in a single tensor-core pass.
        const unsigned int output_query_chunk = warp >> 2u;
        const unsigned int output_d_chunk = warp & 3u;
        wmma::fragment<wmma::accumulator, 16, 16, 16, float> acc;
        wmma::load_matrix_sync(acc, output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, kHeadDim, wmma::mem_row_major);
        wmma::fragment<wmma::matrix_a, 16, 16, 16, half, wmma::row_major> p_frag;
        wmma::fragment<wmma::matrix_b, 16, 16, 16, half, wmma::row_major> v_frag;
        wmma::load_matrix_sync(p_frag, probabilities + output_query_chunk * 16u * kKeyTile, kKeyTile);
        wmma::load_matrix_sync(v_frag, v_tile + output_d_chunk * 16u, kHeadDim);
        wmma::mma_sync(acc, p_frag, v_frag, acc);
        wmma::store_matrix_sync(output + output_query_chunk * 16u * kHeadDim + output_d_chunk * 16u, acc, kHeadDim, wmma::mem_row_major);
        __syncthreads();
    }

    for (unsigned int index = tid; index < kQueryTile * kHeadDim; index += kThreads) {
        const unsigned int m = index / kHeadDim;
        const unsigned int d = index - m * kHeadDim;
        const unsigned int qi = query_start + m;
        if (qi < seq_len) {
            const float denom = running_sum[m];
            dst[(b * seq_len + qi) * hidden + head_off + d] = denom > 0.0f ? output[index] / denom : 0.0f;
        }
    }
}

// Pack graph-layout [B, S, H, D] tensors into the head-major layout consumed
// by strided-batched tensor-core GEMMs. Relative projections are intentionally
// replicated over B: this gives cuBLASLt regular, contiguous strided batches
// without pointer-array setup or a broadcast-specific algorithm assumption.
extern "C" __global__ void termite_deberta_pack_heads_f16(
    unsigned short* q_out,
    unsigned short* k_out,
    unsigned short* v_out,
    unsigned short* q_r_out,
    unsigned short* k_r_out,
    const float* q,
    const float* k,
    const float* v,
    const float* q_r,
    const float* k_r,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    const unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned int hidden = num_heads * head_dim;
    const unsigned int token_count = batch * seq_len * hidden;
    const unsigned int rel_len = seq_len * 2u - 1u;
    const unsigned int rel_count = batch * num_heads * rel_len * head_dim;
    if (idx < token_count) {
        const unsigned int d = idx % head_dim;
        const unsigned int tmp = idx / head_dim;
        const unsigned int h = tmp % num_heads;
        const unsigned int t = (tmp / num_heads) % seq_len;
        const unsigned int b = tmp / (num_heads * seq_len);
        const unsigned int packed = ((b * num_heads + h) * seq_len + t) * head_dim + d;
        q_out[packed] = __half_as_ushort(__float2half_rn(q[idx]));
        k_out[packed] = __half_as_ushort(__float2half_rn(k[idx]));
        // P*V uses V as the right/weight operand of the row-major GEMM, so
        // store it as [head_dim, seq_len] rather than the Q/K [seq_len,
        // head_dim] layout above. This transpose is folded into packing.
        const unsigned int v_packed = ((b * num_heads + h) * head_dim + d) * seq_len + t;
        v_out[v_packed] = __half_as_ushort(__float2half_rn(v[idx]));
    }
    if (idx < rel_count) {
        const unsigned int d = idx % head_dim;
        const unsigned int tmp = idx / head_dim;
        const unsigned int rel = tmp % rel_len;
        const unsigned int matrix = tmp / rel_len;
        const unsigned int h = matrix % num_heads;
        const unsigned int src = (rel * hidden) + h * head_dim + d;
        q_r_out[idx] = __half_as_ushort(__float2half_rn(q_r[src]));
        k_r_out[idx] = __half_as_ushort(__float2half_rn(k_r[src]));
    }
}

// Adds DeBERTa's two relative-position score terms to tensor-core materialized
// GEMMs and normalizes each row in-place. Input/output `content_scores` is
// [B*H, S, S]; c2p/p2c are [B*H, S, 2S-1].
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

extern "C" __global__ void termite_deberta_unpack_heads_f32(
    float* dst,
    const float* packed,
    unsigned int batch,
    unsigned int seq_len,
    unsigned int num_heads,
    unsigned int head_dim
) {
    const unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned int hidden = num_heads * head_dim;
    const unsigned int count = batch * seq_len * hidden;
    if (idx >= count) return;
    const unsigned int d = idx % head_dim;
    const unsigned int tmp = idx / head_dim;
    const unsigned int h = tmp % num_heads;
    const unsigned int t = (tmp / num_heads) % seq_len;
    const unsigned int b = tmp / (num_heads * seq_len);
    const unsigned int src = ((b * num_heads + h) * seq_len + t) * head_dim + d;
    dst[idx] = packed[src];
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

// Runtime-wired generated quant kernels from graph/quant_kernel_compiler.zig.
// Kernel bodies must match src/ops/cuda/generated/quant_kernel_q4_0_mmv.cu and
// src/ops/cuda/generated/quant_kernel_q4_0_mm.cu byte-for-byte modulo the
// uint8_t/uint16_t -> unsigned char/unsigned short spellings; the compiler test
// "promoted CUDA kernel bodies stay in sync with the production bundle"
// enforces this, so update this copy whenever the generated source changes.
// kernel_id=antfly_q4_0_mmv_f32_v1 plan_id=cuda/q4_0/rows_1/none/mmv
// kernel_id=antfly_q4_0_mm_f32_v1 plan_id=cuda/q4_0/rows_9_64/none/mm

static __device__ __forceinline__ float antfly_half_le_to_float(const unsigned char *p) {
    const unsigned short bits = (unsigned short)p[0] | ((unsigned short)p[1] << 8);
    return __half2float(__ushort_as_half(bits));
}

static __device__ __forceinline__ float antfly_warp_reduce_sum(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

extern "C" __global__ void antfly_q4_0_mmv_f32_v1(
    const float *input,
    const unsigned char *weight_q4_0,
    float *output,
    int rows,
    int in_dim,
    int out_dim
) {
    const int col0 = blockIdx.x << 2;
    if (rows != 1 || col0 >= out_dim) return;
    if (blockDim.x != 256) return;
    if ((in_dim & 31) != 0) return;

    const int row_blocks = in_dim >> 5;
    const int half_bytes = in_dim >> 1;
    float acc[4] = {0.0f, 0.0f, 0.0f, 0.0f};
    for (int byte_idx = threadIdx.x; byte_idx < half_bytes; byte_idx += 256) {
        const int block_idx = byte_idx >> 4;
        const int offset = byte_idx & 15;
        const int base = block_idx << 5;
        const float x_lo = input[base + offset];
        const float x_hi = input[base + offset + 16];
#pragma unroll
        for (int c = 0; c < 4; ++c) {
            if (col0 + c >= out_dim) continue;
            const unsigned char *block = weight_q4_0 + ((size_t)(col0 + c) * row_blocks + block_idx) * 18;
            const float d = antfly_half_le_to_float(block);
            const int packed = (int)block[2 + offset];
            acc[c] += d * (x_lo * (float)((packed & 15) - 8) + x_hi * (float)((packed >> 4) - 8));
        }
    }

    __shared__ float partial[4][8];
    const int lane = threadIdx.x & 31;
    const int warp = threadIdx.x >> 5;
#pragma unroll
    for (int c = 0; c < 4; ++c) {
        const float total = antfly_warp_reduce_sum(acc[c]);
        if (lane == 0) partial[c][warp] = total;
    }
    __syncthreads();
    if (threadIdx.x < 4) {
        float total = 0.0f;
#pragma unroll
        for (int w = 0; w < 8; ++w) total += partial[threadIdx.x][w];
        if (col0 + threadIdx.x < out_dim) output[col0 + threadIdx.x] = total;
    }
}

extern "C" __global__ void antfly_q4_0_mm_f32_v1(
    const float *input,
    const unsigned char *weight_q4_0,
    float *output,
    int rows,
    int in_dim,
    int out_dim
) {
    const int col0 = blockIdx.x << 2;
    const int row0 = blockIdx.y << 3;
    if (rows < 9 || rows > 64) return;
    if (col0 >= out_dim || row0 >= rows) return;
    if (blockDim.x != 256) return;
    if ((in_dim & 31) != 0) return;

    const int row_blocks = in_dim >> 5;
    const int half_bytes = in_dim >> 1;
    float acc[4][8];
#pragma unroll
    for (int c = 0; c < 4; ++c) {
#pragma unroll
        for (int r = 0; r < 8; ++r) acc[c][r] = 0.0f;
    }

    for (int byte_idx = threadIdx.x; byte_idx < half_bytes; byte_idx += 256) {
        const int block_idx = byte_idx >> 4;
        const int offset = byte_idx & 15;
        const int base = block_idx << 5;
        float x_lo[8];
        float x_hi[8];
#pragma unroll
        for (int r = 0; r < 8; ++r) {
            const int row = row0 + r;
            x_lo[r] = row < rows ? input[(size_t)row * in_dim + base + offset] : 0.0f;
            x_hi[r] = row < rows ? input[(size_t)row * in_dim + base + offset + 16] : 0.0f;
        }
#pragma unroll
        for (int c = 0; c < 4; ++c) {
            if (col0 + c >= out_dim) continue;
            const unsigned char *block = weight_q4_0 + ((size_t)(col0 + c) * row_blocks + block_idx) * 18;
            const float d = antfly_half_le_to_float(block);
            const int packed = (int)block[2 + offset];
            const float w_lo = d * (float)((packed & 15) - 8);
            const float w_hi = d * (float)((packed >> 4) - 8);
#pragma unroll
            for (int r = 0; r < 8; ++r) acc[c][r] += w_lo * x_lo[r] + w_hi * x_hi[r];
        }
    }

    __shared__ float partial[4][8][8];
    const int lane = threadIdx.x & 31;
    const int warp = threadIdx.x >> 5;
#pragma unroll
    for (int c = 0; c < 4; ++c) {
#pragma unroll
        for (int r = 0; r < 8; ++r) {
            const float total = antfly_warp_reduce_sum(acc[c][r]);
            if (lane == 0) partial[c][r][warp] = total;
        }
    }
    __syncthreads();
    if (threadIdx.x < 32) {
        const int c = threadIdx.x >> 3;
        const int r = threadIdx.x & 7;
        float total = 0.0f;
#pragma unroll
        for (int w = 0; w < 8; ++w) total += partial[c][r][w];
        const int col = col0 + c;
        const int row = row0 + r;
        if (col < out_dim && row < rows) output[(size_t)row * out_dim + col] = total;
    }
}

// Runtime-wired generated quant kernel from graph/quant_kernel_compiler.zig.
// Kernel body must match src/ops/cuda/generated/quant_kernel_q4_0_pair_mmv.cu
// byte-for-byte modulo the uint8_t -> unsigned char spelling; enforced by the
// compiler sync test, so update this copy whenever the generated source changes.
// kernel_id=antfly_q4_0_pair_mmv_f32_v1 plan_id=cuda/q4_0/rows_1/pair/mmv

extern "C" __global__ void antfly_q4_0_pair_mmv_f32_v1(
    const float *input,
    const unsigned char *weight_a_q4_0,
    const unsigned char *weight_b_q4_0,
    float *output_a,
    float *output_b,
    int rows,
    int in_dim,
    int out_dim
) {
    const int col0 = blockIdx.x << 2;
    if (rows != 1 || col0 >= out_dim) return;
    if (blockDim.x != 256) return;
    if ((in_dim & 31) != 0) return;

    const int row_blocks = in_dim >> 5;
    const int half_bytes = in_dim >> 1;
    float acc[2][4];
#pragma unroll
    for (int w = 0; w < 2; ++w) {
#pragma unroll
        for (int c = 0; c < 4; ++c) acc[w][c] = 0.0f;
    }

    for (int byte_idx = threadIdx.x; byte_idx < half_bytes; byte_idx += 256) {
        const int block_idx = byte_idx >> 4;
        const int offset = byte_idx & 15;
        const int base = block_idx << 5;
        const float x_lo = input[base + offset];
        const float x_hi = input[base + offset + 16];
#pragma unroll
        for (int w = 0; w < 2; ++w) {
            const unsigned char *weight = w == 0 ? weight_a_q4_0 : weight_b_q4_0;
#pragma unroll
            for (int c = 0; c < 4; ++c) {
                if (col0 + c >= out_dim) continue;
                const unsigned char *block = weight + ((size_t)(col0 + c) * row_blocks + block_idx) * 18;
                const float d = antfly_half_le_to_float(block);
                const int packed = (int)block[2 + offset];
                acc[w][c] += d * (x_lo * (float)((packed & 15) - 8) + x_hi * (float)((packed >> 4) - 8));
            }
        }
    }

    __shared__ float partial[2][4][8];
    const int lane = threadIdx.x & 31;
    const int warp = threadIdx.x >> 5;
#pragma unroll
    for (int w = 0; w < 2; ++w) {
#pragma unroll
        for (int c = 0; c < 4; ++c) {
            const float total = antfly_warp_reduce_sum(acc[w][c]);
            if (lane == 0) partial[w][c][warp] = total;
        }
    }
    __syncthreads();
    if (threadIdx.x < 8) {
        const int w = threadIdx.x >> 2;
        const int c = threadIdx.x & 3;
        float total = 0.0f;
#pragma unroll
        for (int i = 0; i < 8; ++i) total += partial[w][c][i];
        if (col0 + c < out_dim) {
            float *output = w == 0 ? output_a : output_b;
            output[col0 + c] = total;
        }
    }
}

// Runtime-wired generated quant kernel from graph/quant_kernel_compiler.zig.
// Kernel body must match src/ops/cuda/generated/quant_kernel_q4_0_pair_activation_q8_1.cu
// byte-for-byte; update this copy whenever the candidate source changes.
// kernel_id=antfly_q4_0_pair_activation_q8_1_mmv_v1 plan_id=cuda/q4_0/rows_1/pair_activation/mmv

static __device__ __forceinline__ float antfly_half_bits_to_float(unsigned short bits) {
    return __half2float(__ushort_as_half(bits));
}

static __device__ __forceinline__ float antfly_warp_reduce_sum_f32(float value) {
    value += __shfl_down_sync(0xffffffffu, value, 16);
    value += __shfl_down_sync(0xffffffffu, value, 8);
    value += __shfl_down_sync(0xffffffffu, value, 4);
    value += __shfl_down_sync(0xffffffffu, value, 2);
    value += __shfl_down_sync(0xffffffffu, value, 1);
    return value;
}

static __device__ __forceinline__ float antfly_warp_reduce_max_f32(float value) {
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 16));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 8));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 4));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 2));
    value = fmaxf(value, __shfl_down_sync(0xffffffffu, value, 1));
    return __shfl_sync(0xffffffffu, value, 0);
}

static __device__ __forceinline__ float antfly_decoder_activation_f32(float x, unsigned int activation) {
    if (activation <= 1u) {
        const float inner = 0.7978845608028654f * (x + 0.044715f * x * x * x);
        return 0.5f * x * (1.0f + tanhf(inner));
    }
    if (activation == 2u) return x / (1.0f + __expf(-x));
    if (activation == 3u) return fmaxf(x, 0.0f);
    if (activation == 4u) return x / (1.0f + __expf(-1.702f * x));
    const float r = fmaxf(x, 0.0f);
    return r * r;
}

// q4_0 payload bytes live at bp+2; bp is always 2-byte aligned (18-byte
// blocks), so every 4-byte word can be assembled from two aligned u16 loads.
static __device__ __forceinline__ unsigned int antfly_q4_0_word_u16(const unsigned char *payload) {
    const unsigned short *halves = (const unsigned short *)payload;
    return (unsigned int)halves[0] | ((unsigned int)halves[1] << 16);
}

static __device__ __forceinline__ float antfly_q4_0_q8_dot16(
    const unsigned char *q4_bp,
    float q8_d,
    unsigned int iqs,
    int q8_low0,
    int q8_high0,
    int q8_low1,
    int q8_high1
) {
    const float q4_d = antfly_half_bits_to_float(((const unsigned short *)q4_bp)[0]);
    const unsigned int base0 = iqs * 4u;
    const unsigned int word0 = antfly_q4_0_word_u16(q4_bp + 2u + base0);
    const unsigned int word1 = antfly_q4_0_word_u16(q4_bp + 2u + base0 + 4u);
    const unsigned int low0 = __vadd4(word0 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int high0 = __vadd4((word0 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int low1 = __vadd4(word1 & 0x0f0f0f0fu, 0xf8f8f8f8u);
    const unsigned int high1 = __vadd4((word1 >> 4) & 0x0f0f0f0fu, 0xf8f8f8f8u);
    int sumi = __dp4a((int)low0, q8_low0, 0);
    sumi = __dp4a((int)high0, q8_high0, sumi);
    sumi = __dp4a((int)low1, q8_low1, sumi);
    sumi = __dp4a((int)high1, q8_high1, sumi);
    return q4_d * q8_d * (float)sumi;
}

extern "C" __global__ void antfly_q4_0_pair_activation_q8_1_mmv_v1(
    unsigned char *dst_q8,
    const unsigned char *q8_input,
    const unsigned char *weight_gate,
    const unsigned char *weight_up,
    unsigned int activation,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int out_row_blocks = out_dim >> 5;
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;

    const unsigned int out_block = blockIdx.x % out_row_blocks;
    const unsigned int row = blockIdx.x / out_row_blocks;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / 5u;
    const unsigned int group_warp = warp - group * 5u;
    if (blockDim.x != 640u || row >= rows) return;

    __shared__ float gate_partial[4][4][5];
    __shared__ float up_partial[4][4][5];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            const unsigned int local_tid = group_warp * 32u + lane;
            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            const unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 80u) {
                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
                const unsigned int q8_base0 = iqs * 4u;
                const unsigned int q8_base1 = q8_base0 + 4u;
                const int q8_low0 = *(const int *)(q8_values + q8_base0);
                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
                const int q8_low1 = *(const int *)(q8_values + q8_base1);
                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    const unsigned int col = col_tile + c;
                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                    gate_acc[c] += antfly_q4_0_q8_dot16(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += antfly_q4_0_q8_dot16(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            const unsigned int out_group = tid >> 2u;
            const unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < 5u; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            const unsigned short d_bits = __half_as_ushort(__float2half(d));
            bp[0] = (unsigned char)(d_bits & 0xffu);
            bp[1] = (unsigned char)(d_bits >> 8);
            bp[2] = 0u;
            bp[3] = 0u;
        }
    }
}

// Runtime-wired generated quant kernel from graph/quant_kernel_compiler.zig.
// Kernel body must match src/ops/cuda/generated/quant_kernel_q4_0_down_q8_1.cu
// byte-for-byte; update this copy whenever the candidate source changes.
// kernel_id=antfly_q4_0_down_q8_1_mmv_v1 plan_id=cuda/q4_0/rows_1/gated_down/mmv

extern "C" __global__ void antfly_q4_0_down_q8_1_mmv_v1(
    float *dst,
    const unsigned char *q8_input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / col_tiles;
    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    if (blockDim.x != 256u || row >= rows) return;

    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += 128u) {
        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int *)(q8_values + q8_base0);
        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int *)(q8_values + q8_base1);
        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < 4u) {
        float y = 0.0f;
        #pragma unroll
        for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[tid][w];
        const unsigned int col = col_tile + tid;
        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    }
}

static __device__ __forceinline__ float antfly_q6_k_sub_scale_f32(
    const unsigned char *block,
    unsigned int sub
) {
    const signed char *scales = (const signed char *)(block + 192u);
    const unsigned short d_bits = (unsigned short)block[208] | ((unsigned short)block[209] << 8);
    return antfly_half_bits_to_float(d_bits) * (float)scales[sub];
}

// quant-kernel-codegen:begin generated CUDA runtime-wired dev matmul candidates (do not edit; run: zig build quant-kernel-codegen -- --write)
// Runtime-owned llama.cpp CUDA block_q8_1 support; generated, never hand-edited.
// One of two 16-value contributions for a Q4_0/Q8_1 block. Keeping Q4
// nibbles unsigned removes four packed-byte centering instructions; each
// contribution applies half of the block-wide -8*sum correction.
static __device__ __forceinline__ float antfly_q4_0_ggml_q8_1_dot16(
    const unsigned char *q4_bp,
    float q8_d,
    float q8_sum,
    unsigned int iqs,
    int q8_low0,
    int q8_high0,
    int q8_low1,
    int q8_high1
) {
    const float q4_d = antfly_half_bits_to_float(((const unsigned short *)q4_bp)[0]);
    const unsigned int base0 = iqs * 4u;
    const unsigned int word0 = antfly_q4_0_word_u16(q4_bp + 2u + base0);
    const unsigned int word1 = antfly_q4_0_word_u16(q4_bp + 2u + base0 + 4u);
    const unsigned int low0 = word0 & 0x0f0f0f0fu;
    const unsigned int high0 = (word0 >> 4) & 0x0f0f0f0fu;
    const unsigned int low1 = word1 & 0x0f0f0f0fu;
    const unsigned int high1 = (word1 >> 4) & 0x0f0f0f0fu;
    int sumi = __dp4a((int)low0, q8_low0, 0);
    sumi = __dp4a((int)high0, q8_high0, sumi);
    sumi = __dp4a((int)low1, q8_low1, sumi);
    sumi = __dp4a((int)high1, q8_high1, sumi);
    return q4_d * (q8_d * (float)sumi - 4.0f * q8_sum);
}

extern "C" __global__ void antfly_quantize_f32_ggml_q8_1_rows_v1(
    unsigned char *dst_q8,
    const float *input,
    unsigned int rows,
    unsigned int dim
) {
    if (blockDim.x != 32u || rows == 0u || dim == 0u || (dim & 31u) != 0u) return;
    const unsigned int blocks_per_row = dim >> 5;
    const unsigned int block = blockIdx.x;
    const unsigned int row = block / blocks_per_row;
    const unsigned int value_block = block - row * blocks_per_row;
    if (row >= rows) return;
    const unsigned int lane = threadIdx.x;
    const float x = input[(size_t)row * dim + value_block * 32u + lane];
    const float amax = antfly_warp_reduce_max_f32(fabsf(x));
    const float sum = antfly_warp_reduce_sum_f32(x);
    const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
    int q = d > 0.0f ? __float2int_rn(x / d) : 0;
    q = max(-127, min(127, q));
    unsigned char *bp = dst_q8 + (size_t)block * 36u;
    bp[4u + lane] = (unsigned char)(signed char)q;
    if (lane == 0u) {
        const unsigned short d_bits = __half_as_ushort(__float2half(d));
        const unsigned short sum_bits = __half_as_ushort(__float2half(sum));
        bp[0] = (unsigned char)(d_bits & 0xffu);
        bp[1] = (unsigned char)(d_bits >> 8);
        bp[2] = (unsigned char)(sum_bits & 0xffu);
        bp[3] = (unsigned char)(sum_bits >> 8);
    }
}
// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1 plan_id=cuda/q4_0/rows_1/pair_activation/mmv
extern "C" __global__ void antfly_q4_0_pair_activation_q8_1_e2b_6144_mmv_v1(
    unsigned char *dst_q8,
    const unsigned char *q8_input,
    const unsigned char *weight_gate,
    const unsigned char *weight_up,
    unsigned int activation,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int out_row_blocks = out_dim >> 5;
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;

    const unsigned int out_block = blockIdx.x % out_row_blocks;
    const unsigned int row = blockIdx.x / out_row_blocks;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / 3u;
    const unsigned int group_warp = warp - group * 3u;
    if (blockDim.x != 384u || row >= rows) return;

    __shared__ float gate_partial[4][4][3];
    __shared__ float up_partial[4][4][3];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            const unsigned int local_tid = group_warp * 32u + lane;
            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            const unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 48u) {
                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
                const unsigned int q8_base0 = iqs * 4u;
                const unsigned int q8_base1 = q8_base0 + 4u;
                const int q8_low0 = *(const int *)(q8_values + q8_base0);
                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
                const int q8_low1 = *(const int *)(q8_values + q8_base1);
                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    const unsigned int col = col_tile + c;
                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                    gate_acc[c] += antfly_q4_0_q8_dot16(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += antfly_q4_0_q8_dot16(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            const unsigned int out_group = tid >> 2u;
            const unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < 3u; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            const unsigned short d_bits = __half_as_ushort(__float2half(d));
            bp[0] = (unsigned char)(d_bits & 0xffu);
            bp[1] = (unsigned char)(d_bits >> 8);
            bp[2] = 0u;
            bp[3] = 0u;
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1 plan_id=cuda/q4_0/rows_1/pair_activation/mmv
extern "C" __global__ void antfly_q4_0_pair_activation_q8_1_e2b_12288_mmv_v1(
    unsigned char *dst_q8,
    const unsigned char *q8_input,
    const unsigned char *weight_gate,
    const unsigned char *weight_up,
    unsigned int activation,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int out_row_blocks = out_dim >> 5;
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;

    const unsigned int out_block = blockIdx.x % out_row_blocks;
    const unsigned int row = blockIdx.x / out_row_blocks;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / 3u;
    const unsigned int group_warp = warp - group * 3u;
    if (blockDim.x != 384u || row >= rows) return;

    __shared__ float gate_partial[4][4][3];
    __shared__ float up_partial[4][4][3];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            const unsigned int local_tid = group_warp * 32u + lane;
            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            const unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 48u) {
                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
                const unsigned int q8_base0 = iqs * 4u;
                const unsigned int q8_base1 = q8_base0 + 4u;
                const int q8_low0 = *(const int *)(q8_values + q8_base0);
                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
                const int q8_low1 = *(const int *)(q8_values + q8_base1);
                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    const unsigned int col = col_tile + c;
                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                    gate_acc[c] += antfly_q4_0_q8_dot16(gate_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += antfly_q4_0_q8_dot16(up_bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            const unsigned int out_group = tid >> 2u;
            const unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < 3u; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            const unsigned short d_bits = __half_as_ushort(__float2half(d));
            bp[0] = (unsigned char)(d_bits & 0xffu);
            bp[1] = (unsigned char)(d_bits >> 8);
            bp[2] = 0u;
            bp[3] = 0u;
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_down_q8_1_e2b_6144_mmv_v1 plan_id=cuda/q4_0/rows_1/gated_down/mmv
extern "C" __global__ void antfly_q4_0_down_q8_1_e2b_6144_mmv_v1(
    float *dst,
    const unsigned char *q8_input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / col_tiles;
    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    if (blockDim.x != 128u || row >= rows) return;

    __shared__ float warp_partial[4][4];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += 64u) {
        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int *)(q8_values + q8_base0);
        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int *)(q8_values + q8_base1);
        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < 4u) {
        float y = 0.0f;
        #pragma unroll
        for (unsigned int w = 0u; w < 4u; ++w) y += warp_partial[tid][w];
        const unsigned int col = col_tile + tid;
        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_down_q8_1_e2b_12288_mmv_v1 plan_id=cuda/q4_0/rows_1/gated_down/mmv
extern "C" __global__ void antfly_q4_0_down_q8_1_e2b_12288_mmv_v1(
    float *dst,
    const unsigned char *q8_input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / col_tiles;
    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    if (blockDim.x != 256u || row >= rows) return;

    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += 128u) {
        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int *)(q8_values + q8_base0);
        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int *)(q8_values + q8_base1);
        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < 4u) {
        float y = 0.0f;
        #pragma unroll
        for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[tid][w];
        const unsigned int col = col_tile + tid;
        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1 plan_id=cuda/q4_0/rows_1/pair_activation/mmv
extern "C" __global__ void antfly_q4_0_pair_activation_ggml_q8_1_e2b_6144_mmv_v1(
    unsigned char *dst_q8,
    const unsigned char *q8_input,
    const unsigned char *weight_gate,
    const unsigned char *weight_up,
    unsigned int activation,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int out_row_blocks = out_dim >> 5;
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;

    const unsigned int out_block = blockIdx.x % out_row_blocks;
    const unsigned int row = blockIdx.x / out_row_blocks;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / 3u;
    const unsigned int group_warp = warp - group * 3u;
    if (blockDim.x != 384u || row >= rows) return;

    __shared__ float gate_partial[4][4][3];
    __shared__ float up_partial[4][4][3];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            const unsigned int local_tid = group_warp * 32u + lane;
            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            const unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 48u) {
                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
                const float q8_sum = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[1]);
                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
                const unsigned int q8_base0 = iqs * 4u;
                const unsigned int q8_base1 = q8_base0 + 4u;
                const int q8_low0 = *(const int *)(q8_values + q8_base0);
                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
                const int q8_low1 = *(const int *)(q8_values + q8_base1);
                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    const unsigned int col = col_tile + c;
                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                    gate_acc[c] += antfly_q4_0_ggml_q8_1_dot16(gate_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += antfly_q4_0_ggml_q8_1_dot16(up_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            const unsigned int out_group = tid >> 2u;
            const unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < 3u; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float sum = antfly_warp_reduce_sum_f32(x);
        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            const unsigned short d_bits = __half_as_ushort(__float2half(d));
            bp[0] = (unsigned char)(d_bits & 0xffu);
            bp[1] = (unsigned char)(d_bits >> 8);
            const unsigned short sum_bits = __half_as_ushort(__float2half(sum));
            bp[2] = (unsigned char)(sum_bits & 0xffu);
            bp[3] = (unsigned char)(sum_bits >> 8);
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_pair_activation_ggml_q8_1_e2b_12288_mmv_v1 plan_id=cuda/q4_0/rows_1/pair_activation/mmv
extern "C" __global__ void antfly_q4_0_pair_activation_ggml_q8_1_e2b_12288_mmv_v1(
    unsigned char *dst_q8,
    const unsigned char *q8_input,
    const unsigned char *weight_gate,
    const unsigned char *weight_up,
    unsigned int activation,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    if (rows == 0u || (in_dim & 31u) != 0u || (out_dim & 31u) != 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int out_row_blocks = out_dim >> 5;
    const unsigned int group_cols = 4u;
    const unsigned int groups_per_wave = 4u;
    const unsigned int waves = 2u;

    const unsigned int out_block = blockIdx.x % out_row_blocks;
    const unsigned int row = blockIdx.x / out_row_blocks;
    const unsigned int col_block = out_block * 32u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int group = warp / 3u;
    const unsigned int group_warp = warp - group * 3u;
    if (blockDim.x != 384u || row >= rows) return;

    __shared__ float gate_partial[4][4][3];
    __shared__ float up_partial[4][4][3];
    __shared__ float activated[32];

    #pragma unroll
    for (unsigned int wave = 0u; wave < waves; ++wave) {
        if (group < groups_per_wave) {
            const unsigned int local_tid = group_warp * 32u + lane;
            const unsigned int col_tile = col_block + (wave * groups_per_wave + group) * group_cols;
            float gate_acc[4];
            float up_acc[4];
            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                gate_acc[c] = 0.0f;
                up_acc[c] = 0.0f;
            }

            const unsigned int iqs = (local_tid & 1u) * 2u;
            for (unsigned int block = local_tid >> 1u; block < row_blocks; block += 48u) {
                const unsigned char *q8_bp = q8_input + (row * row_blocks + block) * 36u;
                const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
                const float q8_sum = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[1]);
                const signed char *q8_values = (const signed char *)(q8_bp + 4u);
                const unsigned int q8_base0 = iqs * 4u;
                const unsigned int q8_base1 = q8_base0 + 4u;
                const int q8_low0 = *(const int *)(q8_values + q8_base0);
                const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
                const int q8_low1 = *(const int *)(q8_values + q8_base1);
                const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

                #pragma unroll
                for (unsigned int c = 0u; c < group_cols; ++c) {
                    const unsigned int col = col_tile + c;
                    const unsigned char *gate_bp = weight_gate + ((size_t)col * row_blocks + block) * 18u;
                    const unsigned char *up_bp = weight_up + ((size_t)col * row_blocks + block) * 18u;
                    gate_acc[c] += antfly_q4_0_ggml_q8_1_dot16(gate_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                    up_acc[c] += antfly_q4_0_ggml_q8_1_dot16(up_bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
                }
            }

            #pragma unroll
            for (unsigned int c = 0u; c < group_cols; ++c) {
                const float gate_sum = antfly_warp_reduce_sum_f32(gate_acc[c]);
                const float up_sum = antfly_warp_reduce_sum_f32(up_acc[c]);
                if (lane == 0u) {
                    gate_partial[group][c][group_warp] = gate_sum;
                    up_partial[group][c][group_warp] = up_sum;
                }
            }
        }
        __syncthreads();
        if (tid < 16u) {
            const unsigned int out_group = tid >> 2u;
            const unsigned int c = tid & 3u;
            float gate_y = 0.0f;
            float up_y = 0.0f;
            #pragma unroll
            for (unsigned int w = 0u; w < 3u; ++w) {
                gate_y += gate_partial[out_group][c][w];
                up_y += up_partial[out_group][c][w];
            }
            activated[wave * 16u + out_group * group_cols + c] = antfly_decoder_activation_f32(gate_y, activation) * up_y;
        }
        __syncthreads();
    }

    if (warp == 0u) {
        const float x = activated[lane];
        const float sum = antfly_warp_reduce_sum_f32(x);
        const float amax = antfly_warp_reduce_max_f32(fabsf(x));
        const float d = amax > 0.0f ? amax / 127.0f : 0.0f;
        int q = 0;
        if (d > 0.0f) {
            q = __float2int_rn(x / d);
            q = max(-127, min(127, q));
        }
        unsigned char *bp = dst_q8 + ((size_t)row * out_row_blocks + out_block) * 36u;
        bp[4u + lane] = (unsigned char)(signed char)q;
        if (lane == 0u) {
            const unsigned short d_bits = __half_as_ushort(__float2half(d));
            bp[0] = (unsigned char)(d_bits & 0xffu);
            bp[1] = (unsigned char)(d_bits >> 8);
            const unsigned short sum_bits = __half_as_ushort(__float2half(sum));
            bp[2] = (unsigned char)(sum_bits & 0xffu);
            bp[3] = (unsigned char)(sum_bits >> 8);
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_down_ggml_q8_1_e2b_6144_mmv_v1 plan_id=cuda/q4_0/rows_1/gated_down/mmv
extern "C" __global__ void antfly_q4_0_down_ggml_q8_1_e2b_6144_mmv_v1(
    float *dst,
    const unsigned char *q8_input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 1u;
    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / col_tiles;
    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    if (blockDim.x != 128u || row >= rows) return;

    __shared__ float warp_partial[1][4];
    float acc[1];
    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += 64u) {
        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
        const float q8_sum = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[1]);
        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int *)(q8_values + q8_base0);
        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int *)(q8_values + q8_base1);
        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += antfly_q4_0_ggml_q8_1_dot16(bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < 1u) {
        float y = 0.0f;
        #pragma unroll
        for (unsigned int w = 0u; w < 4u; ++w) y += warp_partial[tid][w];
        const unsigned int col = col_tile + tid;
        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_down_ggml_q8_1_e2b_12288_mmv_v1 plan_id=cuda/q4_0/rows_1/gated_down/mmv
extern "C" __global__ void antfly_q4_0_down_ggml_q8_1_e2b_12288_mmv_v1(
    float *dst,
    const unsigned char *q8_input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 1u;
    if (rows == 0u || (in_dim & 31u) != 0u || out_dim == 0u) return;
    const unsigned int row_blocks = in_dim >> 5;
    const unsigned int col_tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / col_tiles;
    const unsigned int col_tile = (blockIdx.x % col_tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    if (blockDim.x != 128u || row >= rows) return;

    __shared__ float warp_partial[1][4];
    float acc[1];
    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    for (unsigned int block = tid >> 1u; block < row_blocks; block += 64u) {
        const unsigned char *q8_bp = q8_input + ((size_t)row * row_blocks + block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[0]);
        const float q8_sum = antfly_half_bits_to_float(((const unsigned short *)q8_bp)[1]);
        const signed char *q8_values = (const signed char *)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int *)(q8_values + q8_base0);
        const int q8_high0 = *(const int *)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int *)(q8_values + q8_base1);
        const int q8_high1 = *(const int *)(q8_values + q8_base1 + 16u);

        #pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char *bp = weight + ((size_t)col * row_blocks + block) * 18u;
                acc[c] += antfly_q4_0_ggml_q8_1_dot16(bp, q8_d, q8_sum, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid < 1u) {
        float y = 0.0f;
        #pragma unroll
        for (unsigned int w = 0u; w < 4u; ++w) y += warp_partial[tid][w];
        const unsigned int col = col_tile + tid;
        if (col < out_dim) dst[(size_t)row * out_dim + col] = y;
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_pair_activation_f32_e2b_6144_exact_v1 plan_id=cuda/q4_0/rows_1/pair_activation/mmv
extern "C" __global__ void antfly_q4_0_pair_activation_f32_e2b_6144_exact_v1(
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
    const unsigned int tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / tiles;
    const unsigned int col_tile = (blockIdx.x - row * tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int row_blocks = in_dim / 32u;
    if (rows != 1u || in_dim != 1536u || out_dim != 6144u || blockDim.x != 128u || row >= rows) return;

    __shared__ float gate_partial[4][4];
    __shared__ float up_partial[4][4];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        const float x = input[row * in_dim + i];
        const unsigned int block = i / 32u;
        const unsigned int value_lane = i - block * 32u;
        const unsigned int q_offset = 2u + (value_lane & 15u);
        const unsigned int high_nibble = value_lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += x * termite_q4_0_value_nibble(gate_bp, q_offset, high_nibble);
                up_acc[c] += x * termite_q4_0_value_nibble(up_bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        const float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        const float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 4u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 4u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1 plan_id=cuda/q4_0/rows_1/pair_activation/mmv
extern "C" __global__ void antfly_q4_0_pair_activation_f32_e2b_12288_exact_v1(
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
    const unsigned int tiles = (out_dim + cols - 1u) / cols;
    const unsigned int row = blockIdx.x / tiles;
    const unsigned int col_tile = (blockIdx.x - row * tiles) * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int row_blocks = in_dim / 32u;
    if (rows != 1u || in_dim != 1536u || out_dim != 12288u || blockDim.x != 128u || row >= rows) return;

    __shared__ float gate_partial[4][4];
    __shared__ float up_partial[4][4];
    float gate_acc[4];
    float up_acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        gate_acc[c] = 0.0f;
        up_acc[c] = 0.0f;
    }

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        const float x = input[row * in_dim + i];
        const unsigned int block = i / 32u;
        const unsigned int value_lane = i - block * 32u;
        const unsigned int q_offset = 2u + (value_lane & 15u);
        const unsigned int high_nibble = value_lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* gate_bp = weight_gate + (col * row_blocks + block) * 18u;
                const unsigned char* up_bp = weight_up + (col * row_blocks + block) * 18u;
                gate_acc[c] += x * termite_q4_0_value_nibble(gate_bp, q_offset, high_nibble);
                up_acc[c] += x * termite_q4_0_value_nibble(up_bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        const float gate_sum = termite_warp_reduce_sum(gate_acc[c]);
        const float up_sum = termite_warp_reduce_sum(up_acc[c]);
        if (lane == 0u && warp < 4u) {
            gate_partial[c][warp] = gate_sum;
            up_partial[c][warp] = up_sum;
        }
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                float gate_y = 0.0f;
                float up_y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 4u; ++w) {
                    gate_y += gate_partial[c][w];
                    up_y += up_partial[c][w];
                }
                dst[row * out_dim + col] = termite_decoder_activation_f32(gate_y, activation) * up_y;
            }
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_down_f32_e2b_6144_exact_v1 plan_id=cuda/q4_0/rows_1/gated_down/mmv
extern "C" __global__ void antfly_q4_0_down_f32_e2b_6144_exact_v1(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    const unsigned int col_tile = blockIdx.x * cols;
    const unsigned int row = 0u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int row_blocks = in_dim / 32u;
    if (rows != 1u || in_dim != 6144u || out_dim != 1536u || blockDim.x != 256u) return;

    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        const float x = input[row * in_dim + i];
        const unsigned int block = i / 32u;
        const unsigned int value_lane = i - block * 32u;
        const unsigned int q_offset = 2u + (value_lane & 15u);
        const unsigned int high_nibble = value_lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        const float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 8u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[c][w];
                dst[row * out_dim + col] = y;
            }
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_down_f32_e2b_12288_exact_v1 plan_id=cuda/q4_0/rows_1/gated_down/mmv
extern "C" __global__ void antfly_q4_0_down_f32_e2b_12288_exact_v1(
    float* dst,
    const float* input,
    const unsigned char* weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    const unsigned int cols = 4u;
    const unsigned int col_tile = blockIdx.x * cols;
    const unsigned int row = 0u;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    const unsigned int row_blocks = in_dim / 32u;
    if (rows != 1u || in_dim != 12288u || out_dim != 1536u || blockDim.x != 256u) return;

    __shared__ float warp_partial[4][8];
    float acc[4];
    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) acc[c] = 0.0f;

    for (unsigned int i = tid; i < in_dim; i += blockDim.x) {
        const float x = input[row * in_dim + i];
        const unsigned int block = i / 32u;
        const unsigned int value_lane = i - block * 32u;
        const unsigned int q_offset = 2u + (value_lane & 15u);
        const unsigned int high_nibble = value_lane >> 4u;
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                const unsigned char* bp = weight + (col * row_blocks + block) * 18u;
                acc[c] += x * termite_q4_0_value_nibble(bp, q_offset, high_nibble);
            }
        }
    }

    #pragma unroll
    for (unsigned int c = 0u; c < 4u; ++c) {
        const float sum = termite_warp_reduce_sum(acc[c]);
        if (lane == 0u && warp < 8u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid == 0u) {
        #pragma unroll
        for (unsigned int c = 0u; c < 4u; ++c) {
            const unsigned int col = col_tile + c;
            if (col < out_dim) {
                float y = 0.0f;
                #pragma unroll
                for (unsigned int w = 0u; w < 8u; ++w) y += warp_partial[c][w];
                dst[row * out_dim + col] = y;
            }
        }
    }
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1 plan_id=cuda/q4_0/rows_1/argmax/mmv
extern "C" __global__ void antfly_q4_0_q8_1_argmax_rows_stage1_tile8_v1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    const unsigned int cols = 8u;
    const unsigned int row_blocks = 48u;
    if (rows != 1u || in_dim != 1536u || out_dim != 262144u || blockDim.x != 96u) return;

    const unsigned int global_tile = blockIdx.x;
    const unsigned int col_tile = global_tile * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float warp_partial[8][3];
    float acc[8];
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    const unsigned int iqs = (tid & 1u) * 2u;
    const unsigned int block = tid >> 1u;
    if (block < row_blocks) {
        const unsigned char* q8_bp = q8_input + block * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        const unsigned int q8_base0 = iqs * 4u;
        const unsigned int q8_base1 = q8_base0 + 4u;
        const int q8_low0 = *(const int*)(q8_values + q8_base0);
        const int q8_high0 = *(const int*)(q8_values + q8_base0 + 16u);
        const int q8_low1 = *(const int*)(q8_values + q8_base1);
        const int q8_high1 = *(const int*)(q8_values + q8_base1 + 16u);
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned int col = col_tile + c;
            const unsigned char* bp = weight + ((size_t)col * row_blocks + block) * 18u;
            acc[c] = antfly_q4_0_q8_dot16(bp, q8_d, iqs, q8_low0, q8_high0, q8_low1, q8_high1);
        }
    }

#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid != 0u) return;

    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const unsigned int col = col_tile + c;
        float value = 0.0f;
#pragma unroll
        for (unsigned int w = 0u; w < 3u; ++w) value += warp_partial[c][w];
        bool suppressed = false;
        for (unsigned int j = 0u; j < suppress_count; ++j) {
            const int token_id = suppress_token_ids[j];
            if (token_id >= 0 && (unsigned int)token_id == col) {
                suppressed = true;
                break;
            }
        }
        if (!suppressed && (value > best_value || (value == best_value && col < best_index))) {
            best_value = value;
            best_index = col;
        }
    }
    partial_values[global_tile] = best_value;
    partial_indices[global_tile] = best_index;
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1 plan_id=cuda/q6_k/rows_1/argmax/mmv
static __device__ __forceinline__ int antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(
    const unsigned char* ql,
    const unsigned char* qh,
    unsigned int nibble_shift,
    unsigned int qh_shift,
    unsigned int offset
) {
    const unsigned int q0 = ((unsigned int)(ql[offset + 0u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 0u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q1 = ((unsigned int)(ql[offset + 1u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 1u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q2 = ((unsigned int)(ql[offset + 2u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 2u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q3 = ((unsigned int)(ql[offset + 3u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 3u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int p0 = (q0 - 32u) & 0xffu;
    const unsigned int p1 = (q1 - 32u) & 0xffu;
    const unsigned int p2 = (q2 - 32u) & 0xffu;
    const unsigned int p3 = (q3 - 32u) & 0xffu;
    return (int)(p0 | (p1 << 8u) | (p2 << 16u) | (p3 << 24u));
}

static __device__ __forceinline__ int antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_q8_1_dot16_sub(
    const unsigned char* block,
    unsigned int sub,
    int q8_pack0,
    int q8_pack1,
    int q8_pack2,
    int q8_pack3
) {
    const unsigned int half = sub >> 3u;
    const unsigned int group = (sub & 7u) >> 1u;
    const unsigned int l_base = (sub & 1u) * 16u;
    const unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    const unsigned int qh_off = half * 32u;
    const unsigned int qh_shift = group << 1u;
    const unsigned int nibble_shift = (group >> 1u) << 2u;
    const unsigned char* ql = block + ql_off + l_base;
    const unsigned char* qh = block + 128u + qh_off + l_base;
    int sumi = 0;
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 0u), q8_pack0, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 4u), q8_pack1, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 8u), q8_pack2, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 12u), q8_pack3, sumi);
    return sumi;
}
extern "C" __global__ void antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    const unsigned int cols = 8u;
    const unsigned int row_blocks = 10u;
    const unsigned int task_threads = 160u;
    if (rows != 1u || in_dim != 2560u || out_dim != 262144u || suppress_count != 0u || blockDim.x != 160u) return;

    const unsigned int global_tile = blockIdx.x;
    const unsigned int col_tile = global_tile * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float warp_partial[8][5];
    float acc[8];
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    if (tid < task_threads) {
        const unsigned int block = tid >> 4u;
        const unsigned int sub = tid & 15u;
        const unsigned int q8_sub_block = sub >> 1u;
        const unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = q8_input + (block * 8u + q8_sub_block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        const int q8_pack0 = *(const int*)(q8_values + q8_lane_base + 0u);
        const int q8_pack1 = *(const int*)(q8_values + q8_lane_base + 4u);
        const int q8_pack2 = *(const int*)(q8_values + q8_lane_base + 8u);
        const int q8_pack3 = *(const int*)(q8_values + q8_lane_base + 12u);
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;
            const int sumi = antfly_q6_k_q8_1_argmax_rows1_k2560_tile8_v1_q6_k_q8_1_dot16_sub(bp, sub, q8_pack0, q8_pack1, q8_pack2, q8_pack3);
            acc[c] = (q8_d * antfly_q6_k_sub_scale_f32(bp, sub)) * (float)sumi;
        }
    }

#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid != 0u) return;

    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        float value = 0.0f;
#pragma unroll
        for (unsigned int w = 0u; w < 5u; ++w) value += warp_partial[c][w];
        const unsigned int col = col_tile + c;
        if (value > best_value || (value == best_value && col < best_index)) {
            best_value = value;
            best_index = col;
        }
    }
    partial_values[global_tile] = best_value;
    partial_indices[global_tile] = best_index;
}

// Opt-in runtime-wired generated CUDA matmul candidate from graph/quant_kernel_compiler.zig.
// kernel_id=antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1 plan_id=cuda/q6_k/rows_1/argmax/mmv
static __device__ __forceinline__ int antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_pack4(
    const unsigned char* ql,
    const unsigned char* qh,
    unsigned int nibble_shift,
    unsigned int qh_shift,
    unsigned int offset
) {
    const unsigned int q0 = ((unsigned int)(ql[offset + 0u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 0u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q1 = ((unsigned int)(ql[offset + 1u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 1u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q2 = ((unsigned int)(ql[offset + 2u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 2u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int q3 = ((unsigned int)(ql[offset + 3u] >> nibble_shift) & 0x0fu) | (((unsigned int)(qh[offset + 3u] >> qh_shift) & 0x03u) << 4u);
    const unsigned int p0 = (q0 - 32u) & 0xffu;
    const unsigned int p1 = (q1 - 32u) & 0xffu;
    const unsigned int p2 = (q2 - 32u) & 0xffu;
    const unsigned int p3 = (q3 - 32u) & 0xffu;
    return (int)(p0 | (p1 << 8u) | (p2 << 16u) | (p3 << 24u));
}

static __device__ __forceinline__ int antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_q8_1_dot16_sub(
    const unsigned char* block,
    unsigned int sub,
    int q8_pack0,
    int q8_pack1,
    int q8_pack2,
    int q8_pack3
) {
    const unsigned int half = sub >> 3u;
    const unsigned int group = (sub & 7u) >> 1u;
    const unsigned int l_base = (sub & 1u) * 16u;
    const unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    const unsigned int qh_off = half * 32u;
    const unsigned int qh_shift = group << 1u;
    const unsigned int nibble_shift = (group >> 1u) << 2u;
    const unsigned char* ql = block + ql_off + l_base;
    const unsigned char* qh = block + 128u + qh_off + l_base;
    int sumi = 0;
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 0u), q8_pack0, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 4u), q8_pack1, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 8u), q8_pack2, sumi);
    sumi = __dp4a(antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_pack4(ql, qh, nibble_shift, qh_shift, 12u), q8_pack3, sumi);
    return sumi;
}
extern "C" __global__ void antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1(
    float* partial_values,
    unsigned int* partial_indices,
    const unsigned char* q8_input,
    const unsigned char* weight,
    const int* suppress_token_ids,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim,
    unsigned int suppress_count
) {
    (void)suppress_token_ids;
    const unsigned int cols = 8u;
    const unsigned int row_blocks = 15u;
    const unsigned int task_threads = 240u;
    if (rows != 1u || in_dim != 3840u || out_dim != 262144u || suppress_count != 0u || blockDim.x != 256u) return;

    const unsigned int global_tile = blockIdx.x;
    const unsigned int col_tile = global_tile * cols;
    const unsigned int tid = threadIdx.x;
    const unsigned int lane = tid & 31u;
    const unsigned int warp = tid >> 5u;
    __shared__ float warp_partial[8][8];
    float acc[8];
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) acc[c] = 0.0f;

    if (tid < task_threads) {
        const unsigned int block = tid >> 4u;
        const unsigned int sub = tid & 15u;
        const unsigned int q8_sub_block = sub >> 1u;
        const unsigned int q8_lane_base = (sub & 1u) * 16u;
        const unsigned char* q8_bp = q8_input + (block * 8u + q8_sub_block) * 36u;
        const float q8_d = antfly_half_bits_to_float(((const unsigned short*)q8_bp)[0]);
        const signed char* q8_values = (const signed char*)(q8_bp + 4u);
        const int q8_pack0 = *(const int*)(q8_values + q8_lane_base + 0u);
        const int q8_pack1 = *(const int*)(q8_values + q8_lane_base + 4u);
        const int q8_pack2 = *(const int*)(q8_values + q8_lane_base + 8u);
        const int q8_pack3 = *(const int*)(q8_values + q8_lane_base + 12u);
#pragma unroll
        for (unsigned int c = 0u; c < cols; ++c) {
            const unsigned char* bp = weight + ((col_tile + c) * row_blocks + block) * 210u;
            const int sumi = antfly_q6_k_q8_1_argmax_rows1_k3840_tile8_v1_q6_k_q8_1_dot16_sub(bp, sub, q8_pack0, q8_pack1, q8_pack2, q8_pack3);
            acc[c] = (q8_d * antfly_q6_k_sub_scale_f32(bp, sub)) * (float)sumi;
        }
    }

#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        const float sum = antfly_warp_reduce_sum_f32(acc[c]);
        if (lane == 0u) warp_partial[c][warp] = sum;
    }
    __syncthreads();
    if (tid != 0u) return;

    float best_value = -3.402823466e+38f;
    unsigned int best_index = 0xffffffffu;
#pragma unroll
    for (unsigned int c = 0u; c < cols; ++c) {
        float value = 0.0f;
#pragma unroll
        for (unsigned int w = 0u; w < 8u; ++w) value += warp_partial[c][w];
        const unsigned int col = col_tile + c;
        if (value > best_value || (value == best_value && col < best_index)) {
            best_value = value;
            best_index = col;
        }
    }
    partial_values[global_tile] = best_value;
    partial_indices[global_tile] = best_index;
}
// quant-kernel-codegen:end generated CUDA runtime-wired dev matmul candidates
