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

#include <cuda_fp16.h>

extern "C" __global__ void termite_fill_f32(float *out, unsigned int n, float value) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < n) out[i] = value;
}

extern "C" __global__ void termite_linear_f32(
    float *out,
    const float *input,
    const float *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (i >= total) return;
    unsigned int row = i / out_dim;
    unsigned int col = i % out_dim;
    float acc = 0.0f;
    for (unsigned int k = 0; k < in_dim; ++k) {
        acc += input[row * in_dim + k] * weight[col * in_dim + k];
    }
    out[i] = acc;
}

extern "C" __global__ void termite_linear_bias_f32(
    float *out,
    const float *input,
    const float *weight,
    const float *bias,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (i >= total) return;
    unsigned int row = i / out_dim;
    unsigned int col = i % out_dim;
    float acc = bias[col];
    for (unsigned int k = 0; k < in_dim; ++k) {
        acc += input[row * in_dim + k] * weight[col * in_dim + k];
    }
    out[i] = acc;
}

extern "C" __global__ void termite_rms_norm_f32(
    float *out,
    const float *input,
    const float *weight,
    unsigned int total_rows,
    unsigned int dim,
    float eps
) {
    unsigned int row = blockIdx.x;
    if (row >= total_rows) return;
    float sum = 0.0f;
    for (unsigned int i = 0; i < dim; ++i) {
        float x = input[row * dim + i];
        sum += x * x;
    }
    float scale = 1.0f / sqrtf(sum / (float)dim + eps);
    for (unsigned int i = 0; i < dim; ++i) {
        out[row * dim + i] = input[row * dim + i] * scale * weight[i];
    }
}

extern "C" __global__ void termite_elementwise_f32(
    float *out,
    const float *a,
    const float *b,
    unsigned int n,
    unsigned int op
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= n) return;
    float x = a[i];
    if (op == 2) {
        out[i] = x / (1.0f + expf(-x));
    } else {
        float y = b[i];
        out[i] = (op == 1) ? (x * y) : (x + y);
    }
}

extern "C" __global__ void termite_linear_q8_0_f32(
    float *out,
    const float *input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (i >= total) return;

    unsigned int row = i / out_dim;
    unsigned int col = i % out_dim;
    unsigned int row_blocks = in_dim / 32;
    float acc = 0.0f;
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char *qblock = weight + (col * row_blocks + block) * 34;
        unsigned short scale_bits = (unsigned short)qblock[0] | ((unsigned short)qblock[1] << 8);
        float scale = __half2float(__ushort_as_half(scale_bits));
        const signed char *qs = reinterpret_cast<const signed char *>(qblock + 2);
        const float *x = input + row * in_dim + block * 32;
        float block_acc = 0.0f;
        for (unsigned int k = 0; k < 32; ++k) {
            block_acc += x[k] * (float)qs[k];
        }
        acc += scale * block_acc;
    }
    out[i] = acc;
}

extern "C" __global__ void termite_linear_q4_0_f32(
    float *out,
    const float *input,
    const unsigned char *weight,
    unsigned int rows,
    unsigned int in_dim,
    unsigned int out_dim
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    unsigned int total = rows * out_dim;
    if (i >= total) return;

    unsigned int row = i / out_dim;
    unsigned int col = i % out_dim;
    unsigned int row_blocks = in_dim / 32;
    float acc = 0.0f;
    for (unsigned int block = 0; block < row_blocks; ++block) {
        const unsigned char *qblock = weight + (col * row_blocks + block) * 18;
        unsigned short scale_bits = (unsigned short)qblock[0] | ((unsigned short)qblock[1] << 8);
        float scale = __half2float(__ushort_as_half(scale_bits));
        const unsigned char *qs = qblock + 2;
        const float *x = input + row * in_dim + block * 32;
        float block_acc = 0.0f;
        for (unsigned int k = 0; k < 16; ++k) {
            block_acc += x[k] * (float)((int)(qs[k] & 0x0f) - 8);
        }
        for (unsigned int k = 0; k < 16; ++k) {
            block_acc += x[16 + k] * (float)((int)(qs[k] >> 4) - 8);
        }
        acc += scale * block_acc;
    }
    out[i] = acc;
}
