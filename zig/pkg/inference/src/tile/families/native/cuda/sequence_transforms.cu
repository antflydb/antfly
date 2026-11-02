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

// native-program:entry termite_gliner_gather_concat_relu_f32
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
// native-program:entry termite_gliner_word_embeddings_f32
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
// native-program:entry termite_gliner_gru_combine_f32
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
// native-program:entry termite_laya_action_features_f32
extern "C" __global__ void termite_laya_action_features_f32(
    float* dst, const float* hidden, const float* logits, const long long* markers,
    unsigned int batch, unsigned int seq, unsigned int count, unsigned int dim
) {
    const unsigned int row = blockIdx.x;
    if (row >= batch || count < 2u || count > 20u) return;
    for (unsigned int d = threadIdx.x; d < dim; d += blockDim.x)
        dst[row * (dim + 4u) + d] = hidden[row * seq * dim + d];
    if (threadIdx.x != 0u) return;
    float values[20];
    float maximum = -INFINITY;
    unsigned int valid = 0;
    for (unsigned int i = 0; i < count; ++i) {
        bool present = markers[row * count + i] >= 0;
        values[i] = present ? logits[row * count + i] : -1.0e4f;
        maximum = fmaxf(maximum, values[i]);
        valid += present;
    }
    float sum = 0.0f;
    for (unsigned int i = 0; i < count; ++i) sum += expf(values[i] - maximum);
    float first = 0.0f, second = 0.0f, entropy = 0.0f;
    for (unsigned int i = 0; i < count; ++i) {
        float p = expf(values[i] - maximum) / sum;
        entropy -= p * logf(fmaxf(p, 1.0e-9f));
        if (p > first) { second = first; first = p; }
        else second = fmaxf(second, p);
    }
    dst[row * (dim + 4u) + dim] = first;
    dst[row * (dim + 4u) + dim + 1u] = first - second;
    dst[row * (dim + 4u) + dim + 2u] = entropy / logf((float)valid);
    dst[row * (dim + 4u) + dim + 3u] = (float)valid / 255.0f;
}
// native-program:entry termite_laya_packed_geglu_f32
extern "C" __global__ void termite_laya_packed_geglu_f32(
    float* dst, const float* src, unsigned int rows, unsigned int width
) {
    unsigned int i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i >= rows * width) return;
    unsigned int row = i / width, col = i % width;
    float x = src[(size_t)row * 2u * width + col];
    float gelu = isfinite(x) ? 0.5f * x * (1.0f + erff(x * 0.7071067811865476f)) : 0.0f;
    dst[i] = gelu * src[(size_t)row * 2u * width + width + col];
}
