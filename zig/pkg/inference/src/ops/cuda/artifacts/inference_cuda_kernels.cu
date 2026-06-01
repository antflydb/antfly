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
    } else {
        out = tanhf(x);
    }
    dst[i] = out;
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
