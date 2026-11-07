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

// native-program:entry termite_rms_norm_add_weighted_embedding_i32_q6_k_f32
extern "C" __global__ void termite_rms_norm_add_weighted_embedding_i32_q6_k_f32(ANTFLY_CUDA_RMS_NORM_ADD_WEIGHTED_EMBEDDING_I32_Q6_K_F32_PARAMS) {
    unsigned int group_row = blockIdx.x;
    unsigned int rows = total * num_groups;
    if (group_row >= rows) return;
    unsigned int tid = threadIdx.x;
    unsigned int token_row = group_row / num_groups;
    unsigned int group = group_row - token_row * num_groups;
    unsigned int base = token_row * embedding_dim + group * group_dim;
    __shared__ float partial[256];
    float norm_scale = antfly_row_rms_tree_inverse(group_dim, eps, partial,
        [&](unsigned i) { return input[base + i]; });
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
// native-program:entry termite_gliner_qkv_rope_f32
extern "C" __global__ void termite_gliner_qkv_rope_f32(
    float* q, float* k, float* v, const float* packed,
    unsigned int batch, unsigned int sequence, unsigned int heads,
    unsigned int dimension, float theta
) {
    const unsigned int index = blockIdx.x * blockDim.x + threadIdx.x;
    const unsigned int half = dimension / 2u;
    const unsigned int count = batch * sequence * heads * half;
    if (index >= count) return;
    const unsigned int chunk = index / half, d = index % half;
    const unsigned int row = chunk / heads, head = chunk % heads;
    const unsigned int hidden = heads * dimension;
    const unsigned int source = row * 3u * hidden + head * dimension + d;
    const unsigned int dest = chunk * dimension + d;
    const float frequency = 1.0f / powf(theta, (2.0f * (float)d) / (float)dimension);
    const float angle = (float)(row % sequence) * frequency;
    const float s = sinf(angle), c = cosf(angle);
    const float q0 = packed[source], q1 = packed[source + half];
    const float k0 = packed[source + hidden], k1 = packed[source + hidden + half];
    q[dest] = q0 * c - q1 * s;
    q[dest + half] = q0 * s + q1 * c;
    k[dest] = k0 * c - k1 * s;
    k[dest + half] = k0 * s + k1 * c;
    v[dest] = packed[source + 2u * hidden];
    v[dest + half] = packed[source + 2u * hidden + half];
}
