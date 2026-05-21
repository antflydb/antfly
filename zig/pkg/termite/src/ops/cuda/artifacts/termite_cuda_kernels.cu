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

extern "C" __global__ void termite_fill_f32(float* dst, unsigned int n, float value) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) dst[i] = value;
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
    float sumsq = 0.0f;
    const unsigned int base = row * dim;
    for (unsigned int i = 0; i < dim; ++i) {
        float x = input[base + i];
        sumsq += x * x;
    }
    float scale = rsqrtf(sumsq / (float)dim + eps);
    for (unsigned int i = 0; i < dim; ++i) {
        dst[base + i] = input[base + i] * scale * weight[i];
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
    } else if (op == 8u) {
        out = x < y ? 1.0f : 0.0f;
    } else if (op == 9u) {
        out = x / y;
    } else if (op == 10u) {
        out = expf(x);
    } else if (op == 11u) {
        out = logf(x);
    } else if (op == 12u) {
        out = sqrtf(x);
    } else if (op == 13u) {
        out = rsqrtf(x);
    } else if (op == 14u) {
        out = fabsf(x);
    } else if (op == 15u) {
        out = sinf(x);
    } else if (op == 16u) {
        out = cosf(x);
    } else if (op == 17u) {
        out = erff(x);
    } else if (op == 18u) {
        out = x - y;
    } else {
        out = tanhf(x);
    }
    dst[i] = out;
}

extern "C" __global__ void termite_softmax_lastdim_f32(
    float* dst,
    const float* input,
    unsigned int rows,
    unsigned int dim,
    unsigned int log_mode
) {
    __shared__ float scratch[256];
    unsigned int tid = threadIdx.x;
    unsigned int row = blockIdx.x;
    if (row >= rows || dim == 0u) return;

    unsigned int base = row * dim;
    float local_max = -3.402823466e+38f;
    for (unsigned int j = tid; j < dim; j += 256u) {
        float v = input[base + j];
        local_max = fmaxf(local_max, v);
    }
    scratch[tid] = local_max;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) scratch[tid] = fmaxf(scratch[tid], scratch[tid + stride]);
        __syncthreads();
    }

    float max_val = scratch[0];
    float local_sum = 0.0f;
    for (unsigned int j = tid; j < dim; j += 256u) {
        local_sum += expf(input[base + j] - max_val);
    }
    scratch[tid] = local_sum;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) scratch[tid] += scratch[tid + stride];
        __syncthreads();
    }

    float log_sum = max_val + logf(scratch[0]);
    for (unsigned int j = tid; j < dim; j += 256u) {
        float z = input[base + j] - log_sum;
        dst[base + j] = log_mode != 0u ? z : expf(z);
    }
}

extern "C" __global__ void termite_reduce_lastdim_f32(
    float* dst,
    const float* input,
    unsigned int rows,
    unsigned int dim,
    unsigned int op
) {
    __shared__ float scratch[256];
    unsigned int tid = threadIdx.x;
    unsigned int row = blockIdx.x;
    if (row >= rows || dim == 0u) return;

    unsigned int base = row * dim;
    float local = op == 1u ? -3.402823466e+38f : 0.0f;
    for (unsigned int j = tid; j < dim; j += 256u) {
        float v = input[base + j];
        if (op == 1u) {
            local = fmaxf(local, v);
        } else {
            local += v;
        }
    }
    scratch[tid] = local;
    __syncthreads();
    for (unsigned int stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            if (op == 1u) {
                scratch[tid] = fmaxf(scratch[tid], scratch[tid + stride]);
            } else {
                scratch[tid] += scratch[tid + stride];
            }
        }
        __syncthreads();
    }
    if (tid == 0u) {
        float out = scratch[0];
        if (op == 2u) out /= (float)dim;
        dst[row] = out;
    }
}

extern "C" __global__ void termite_broadcast_in_dim_f32(
    float* dst,
    const float* input,
    unsigned int out_count,
    unsigned int out_rank,
    unsigned int in_rank,
    const unsigned int* target_shape,
    const unsigned int* input_shape,
    const unsigned int* axes
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= out_count) return;

    unsigned int coords[8];
    unsigned int tmp = idx;
    for (int d = (int)out_rank - 1; d >= 0; --d) {
        const unsigned int size = target_shape[d];
        coords[d] = size == 0u ? 0u : tmp % size;
        tmp = size == 0u ? 0u : tmp / size;
    }

    unsigned int in_offset = 0u;
    for (unsigned int j = 0u; j < in_rank; ++j) {
        const unsigned int axis = axes[j];
        const unsigned int dim = input_shape[j];
        unsigned int coord = coords[axis];
        if (dim == 1u) coord = 0u;
        in_offset = in_offset * dim + coord;
    }
    dst[idx] = input[in_offset];
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

extern "C" __global__ void termite_scatter_add_rows_f32(
    float* dst,
    const float* input,
    const unsigned int* row_ids,
    unsigned int out_rows,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * dim;
    if (idx >= count) return;
    unsigned int row = idx / dim;
    unsigned int col = idx - row * dim;
    unsigned int out_row = row_ids[row];
    if (out_row >= out_rows) return;
    atomicAdd(&dst[out_row * dim + col], input[idx]);
}

extern "C" __global__ void termite_transpose2d_f32(
    float* dst,
    const float* input,
    unsigned int rows,
    unsigned int cols
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int count = rows * cols;
    if (idx >= count) return;
    unsigned int row = idx / cols;
    unsigned int col = idx - row * cols;
    dst[col * rows + row] = input[idx];
}

extern "C" __global__ void termite_transpose_nd_f32(
    float* dst,
    const float* input,
    unsigned int count,
    unsigned int rank,
    const unsigned int* input_shape,
    const unsigned int* input_strides,
    const unsigned int* output_strides,
    const unsigned int* perm
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count || rank > 8u) return;
    unsigned int rem = idx;
    unsigned int src_idx = 0u;
    for (unsigned int dst_axis = 0u; dst_axis < rank; ++dst_axis) {
        unsigned int stride = output_strides[dst_axis];
        unsigned int coord = stride == 0u ? 0u : rem / stride;
        if (stride != 0u) rem -= coord * stride;
        unsigned int src_axis = perm[dst_axis];
        if (src_axis >= rank || coord >= input_shape[src_axis]) return;
        src_idx += coord * input_strides[src_axis];
    }
    dst[idx] = input[src_idx];
}

extern "C" __global__ void termite_rope_f32(
    float* dst,
    const float* input,
    unsigned int count,
    unsigned int seq_len,
    unsigned int head_dim,
    unsigned int rope_dim,
    float theta,
    float freq_scale,
    unsigned int position_offset,
    unsigned int consecutive_pairs
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if (idx >= count || seq_len == 0u || head_dim == 0u || rope_dim == 0u) return;
    unsigned int dim = idx % head_dim;
    if (dim >= rope_dim) {
        dst[idx] = input[idx];
        return;
    }
    unsigned int half = rope_dim / 2u;
    unsigned int head_half = head_dim / 2u;
    unsigned int j;
    unsigned int idx0;
    unsigned int idx1;
    bool write_second;
    unsigned int base = idx - dim;
    if (consecutive_pairs != 0u) {
        if (dim >= half * 2u) {
            dst[idx] = input[idx];
            return;
        }
        j = dim / 2u;
        idx0 = base + 2u * j;
        idx1 = idx0 + 1u;
        write_second = (dim & 1u) != 0u;
    } else {
        if (dim < half) {
            j = dim;
            idx0 = base + j;
            idx1 = base + head_half + j;
            write_second = false;
        } else if (dim >= head_half && dim < head_half + half) {
            j = dim - head_half;
            idx0 = base + j;
            idx1 = base + head_half + j;
            write_second = true;
        } else {
            dst[idx] = input[idx];
            return;
        }
    }

    unsigned int total_chunks = count / head_dim;
    unsigned int chunks_per_position = total_chunks / seq_len;
    if (chunks_per_position == 0u) return;
    unsigned int chunk = idx / head_dim;
    unsigned int pos = position_offset + ((chunk / chunks_per_position) % seq_len);
    float freq = powf(theta, -((float)(2u * j) / (float)rope_dim));
    float angle = (float)pos * freq_scale * freq;
    float cos_val = cosf(angle);
    float sin_val = sinf(angle);
    float x0 = input[idx0];
    float x1 = input[idx1];
    dst[idx] = write_second ? (x0 * sin_val + x1 * cos_val) : (x0 * cos_val - x1 * sin_val);
}

extern "C" __global__ void termite_batched_matmul_f32(
    float* dst,
    const float* lhs,
    const float* rhs,
    unsigned int batches,
    unsigned int m,
    unsigned int k,
    unsigned int n
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = batches * m * n;
    if (idx >= total) return;
    unsigned int col = idx % n;
    unsigned int row = (idx / n) % m;
    unsigned int batch = idx / (m * n);
    unsigned int lhs_base = (batch * m + row) * k;
    unsigned int rhs_base = batch * k * n + col;
    float acc = 0.0f;
    for (unsigned int kk = 0u; kk < k; ++kk) {
        acc += lhs[lhs_base + kk] * rhs[rhs_base + kk * n];
    }
    dst[idx] = acc;
}

extern "C" __global__ void termite_argmax_lastdim_f32(
    float* dst,
    const float* input,
    unsigned int rows,
    unsigned int dim
) {
    unsigned int row = blockIdx.x * blockDim.x + threadIdx.x;
    if (row >= rows || dim == 0u) return;
    unsigned int base = row * dim;
    float max_val = input[base];
    unsigned int max_idx = 0u;
    for (unsigned int col = 1u; col < dim; ++col) {
        float value = input[base + col];
        if (value > max_val) {
            max_val = value;
            max_idx = col;
        }
    }
    dst[row] = (float)max_idx;
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

    float sum = 0.0f;
    unsigned int n = 0;
    unsigned int token_base = b * seq_len;
    unsigned int hidden_base = token_base * hidden_size + d;
    for (unsigned int t = 0; t < seq_len; ++t) {
        if (words_mask[token_base + t] == wanted) {
            sum += hidden[hidden_base + t * hidden_size];
            ++n;
        }
    }
    dst[idx] = n == 0 ? 0.0f : sum / (float)n;
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
    unsigned int total_sequence_len,
    unsigned int query_position_offset,
    unsigned int kv_position_offset,
    unsigned int num_heads,
    unsigned int num_kv_heads,
    unsigned int head_dim,
    unsigned int sliding_window,
    unsigned int has_attn_or_mask,
    unsigned int bias_mode
) {
    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int q_hidden = num_heads * head_dim;
    unsigned int total = batch * q_seq_len * q_hidden;
    if (idx >= total || num_kv_heads == 0u) return;

    unsigned int d = idx % head_dim;
    unsigned int tmp = idx / head_dim;
    unsigned int head = tmp % num_heads;
    tmp /= num_heads;
    unsigned int qi = tmp % q_seq_len;
    unsigned int b = tmp / q_seq_len;
    unsigned int heads_per_group = num_heads / num_kv_heads;
    if (heads_per_group == 0u || head >= heads_per_group * num_kv_heads) return;
    unsigned int kv_head = head / heads_per_group;
    unsigned int kv_hidden = num_kv_heads * head_dim;
    unsigned int query_pos = query_position_offset + qi;
    float scale = rsqrtf((float)head_dim);

    float max_score = -3.402823466e+38f;
    for (unsigned int ki = 0; ki < kv_seq_len; ++ki) {
        unsigned int key_pos = kv_position_offset + ki;
        bool valid = true;
        if (key_pos > query_pos) {
            valid = false;
            if (has_attn_or_mask && query_pos < total_sequence_len && key_pos < total_sequence_len) {
                valid = attn_or_mask[query_pos * total_sequence_len + key_pos] != 0u;
            }
        } else if (sliding_window != 0u && query_pos - key_pos >= sliding_window) {
            valid = false;
        }
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
        bool valid = true;
        if (key_pos > query_pos) {
            valid = false;
            if (has_attn_or_mask && query_pos < total_sequence_len && key_pos < total_sequence_len) {
                valid = attn_or_mask[query_pos * total_sequence_len + key_pos] != 0u;
            }
        } else if (sliding_window != 0u && query_pos - key_pos >= sliding_window) {
            valid = false;
        }
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

__device__ float termite_half_to_float(unsigned short h) {
    unsigned int sign = (h >> 15) & 1u;
    unsigned int exp = (h >> 10) & 0x1fu;
    unsigned int mant = h & 0x3ffu;
    float value;
    if (exp == 0u) {
        value = mant == 0u ? 0.0f : ldexpf((float)mant, -24);
    } else if (exp == 31u) {
        value = mant == 0u ? 3.402823466e+38f : 0.0f;
    } else {
        value = ldexpf(1.0f + (float)mant / 1024.0f, (int)exp - 15);
    }
    return sign ? -value : value;
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
        unsigned short h = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
        float d = termite_half_to_float(h);
        for (unsigned int i = 0; i < 32u; ++i) {
            unsigned char packed = bp[2u + i / 2u];
            int q = (i & 1u) == 0u ? (int)(packed & 0x0fu) : (int)(packed >> 4);
            q -= 8;
            acc += input[row * in_dim + block * 32u + i] * ((float)q * d);
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

__device__ float termite_q5k_value(const unsigned char* bp, unsigned int value_index) {
    unsigned short dh = (unsigned short)bp[0] | ((unsigned short)bp[1] << 8);
    unsigned short dminh = (unsigned short)bp[2] | ((unsigned short)bp[3] << 8);
    float d = termite_half_to_float(dh);
    float dmin = termite_half_to_float(dminh);
    const unsigned char* scales = bp + 4u;
    const unsigned char* qh = bp + 16u;
    const unsigned char* ql = bp + 48u;
    unsigned int sub = value_index / 32u;
    unsigned int chunk = value_index / 64u;
    unsigned int lane = value_index & 31u;
    unsigned char packed = ql[chunk * 32u + lane];
    unsigned int low = (sub & 1u) == 0u ? (unsigned int)(packed & 0x0fu) : (unsigned int)(packed >> 4u);
    unsigned int high = ((unsigned int)qh[lane] >> sub) & 1u;
    unsigned int q = low + high * 16u;
    float scale = d * (float)termite_q4k_scale(scales, sub);
    float minv = dmin * (float)termite_q4k_min(scales, sub);
    return scale * (float)q - minv;
}

__device__ float termite_q6k_value(const unsigned char* bp, unsigned int value_index) {
    const unsigned char* ql = bp;
    const unsigned char* qh = bp + 128u;
    const unsigned char* scales = bp + 192u;
    unsigned short dh = (unsigned short)bp[208] | ((unsigned short)bp[209] << 8);
    float d = termite_half_to_float(dh);
    unsigned int sub = value_index / 16u;
    unsigned int i = value_index & 15u;
    unsigned int half = sub / 8u;
    unsigned int group = (sub % 8u) / 2u;
    unsigned int l_base = (sub % 2u) * 16u;
    unsigned int ql_off = half * 64u + (group & 1u) * 32u;
    unsigned int qh_off = half * 32u;
    unsigned int qh_shift = group * 2u;
    unsigned int nibble_shift = (group / 2u) * 4u;
    unsigned int l = l_base + i;
    unsigned int low4 = ((unsigned int)ql[ql_off + l] >> nibble_shift) & 0x0fu;
    unsigned int high2 = ((unsigned int)qh[qh_off + l] >> qh_shift) & 0x03u;
    int q = (int)(low4 | (high2 << 4u)) - 32;
    int s = (int)((signed char)scales[sub]);
    return d * (float)s * (float)q;
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
                if (MODE == 4u && y < 0.0f) y = 0.0f;
                dst[idx] = y;
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
    termite_q4k_tile_rows_cols<2u, 4u, 1u>(dst, input, weight, bias, rows, in_dim, out_dim);
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
    termite_q4k_tile_rows_cols<2u, 4u, 4u>(dst, input, weight, bias, rows, in_dim, out_dim);
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

extern "C" __global__ void termite_linear_q5_k_f32(
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
        const unsigned char* bp = weight + (col * row_blocks + block) * 176u;
        for (unsigned int i = 0; i < 256u; ++i) {
            acc += input[row * in_dim + block * 256u + i] * termite_q5k_value(bp, i);
        }
    }
    dst[idx] = acc;
}

extern "C" __global__ void termite_linear_q5_k_bias_f32(
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
        const unsigned char* bp = weight + (col * row_blocks + block) * 176u;
        for (unsigned int i = 0; i < 256u; ++i) {
            acc += input[row * in_dim + block * 256u + i] * termite_q5k_value(bp, i);
        }
    }
    dst[idx] = acc;
}

extern "C" __global__ void termite_linear_q6_k_f32(
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
        const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
        for (unsigned int i = 0; i < 256u; ++i) {
            acc += input[row * in_dim + block * 256u + i] * termite_q6k_value(bp, i);
        }
    }
    dst[idx] = acc;
}

extern "C" __global__ void termite_linear_q6_k_bias_f32(
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
        const unsigned char* bp = weight + (col * row_blocks + block) * 210u;
        for (unsigned int i = 0; i < 256u; ++i) {
            acc += input[row * in_dim + block * 256u + i] * termite_q6k_value(bp, i);
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

extern "C" __global__ void termite_embedding_lookup_q5_k_f32(
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
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 176ull;
    dst[idx] = termite_q5k_value(bp, value_index);
}

extern "C" __global__ void termite_embedding_lookup_q6_k_f32(
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
    const unsigned char* bp = weight + (src_row * row_blocks + block) * 210ull;
    dst[idx] = termite_q6k_value(bp, value_index);
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
