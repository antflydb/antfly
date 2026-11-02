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

// native-program:entry termite_deberta_embeddings_f32
// native-program:indent 11
kernel void termite_deberta_embeddings_f32(device const float *weight [[buffer(0)]], device const float *gamma [[buffer(1)]], device const float *beta [[buffer(2)]], device const uint *ids [[buffer(3)]], device const uint *mask [[buffer(4)]], device float *output [[buffer(5)]], constant termite_metal_deberta_embeddings_f32_params &p [[buffer(6)]], uint tid [[thread_index_in_threadgroup]], uint3 tg [[threadgroup_position_in_grid]], uint3 threads_per_tg [[threads_per_threadgroup]]) {
    threadgroup float sums[256]; threadgroup float sqs[256];
    uint row = tg.x; uint width = threads_per_tg.x; if (row >= p.total || p.dim == 0u || width == 0u || tid >= 256u) return;
    uint token = ids[row]; float sum = 0.0f; float sq = 0.0f;
    if (token < p.rows) { for (uint d = tid; d < p.dim; d += width) { float v = weight[token * p.dim + d]; sum += v; sq += v * v; } }
    sums[tid] = sum; sqs[tid] = sq; threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint stride = width >> 1; stride > 0u; stride >>= 1) { if (tid < stride && tid + stride < 256u) { sums[tid] += sums[tid + stride]; sqs[tid] += sqs[tid + stride]; } threadgroup_barrier(mem_flags::mem_threadgroup); }
    float mean = sums[0] / float(p.dim); float var = max(sqs[0] / float(p.dim) - mean * mean, 0.0f); float inv_std = rsqrt(var + p.eps); float m = (mask[row] == 0u || token >= p.rows) ? 0.0f : 1.0f;
    for (uint d = tid; d < p.dim; d += width) { float v = token < p.rows ? weight[token * p.dim + d] : 0.0f; output[row * p.dim + d] = ((v - mean) * inv_std * gamma[d] + beta[d]) * m; }
}
// native-program:entry termite_gliner_boundary_ffn_bias_gelu_exact
// native-program:indent 11
kernel void termite_gliner_boundary_ffn_bias_gelu_exact(device const float *product [[buffer(0)]], device const float *bias [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_apply_activation_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint total=p.rows*p.dim;if(gid>=total||p.dim==0u)return;float x=product[gid]+bias[gid%p.dim];output[gid]=0.5f*x*(1.0f+termite_erf_approx(x*0.7071067811865476f));
}
// native-program:entry termite_gliner_word_embeddings_f32
// native-program:indent 11
kernel void termite_gliner_word_embeddings_f32(device const float *hidden [[buffer(0)]], device const float *words_mask [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_gliner_word_embeddings_f32_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    uint total = p.batch * p.num_words * p.hidden_size; if (gid >= total || p.seq_len == 0u || p.hidden_size == 0u) return;
    uint d = gid % p.hidden_size; uint tmp = gid / p.hidden_size; uint word = tmp % p.num_words; uint b = tmp / p.num_words;
    float wanted = float(word + 1u); uint token_base = b * p.seq_len; uint hidden_base = token_base * p.hidden_size + d;
    for (uint t = 0u; t < p.seq_len; ++t) { if (words_mask[token_base + t] == wanted) { output[gid] = hidden[hidden_base + t * p.hidden_size]; return; } }
    output[gid] = 0.0f;
}
// native-program:entry termite_gliner_gru_combine_f32
// native-program:indent 11
kernel void termite_gliner_gru_combine_f32(device const float *label_embeddings [[buffer(0)]], device const float *gi [[buffer(1)]], device const float *gh [[buffer(2)]], device float *output [[buffer(3)]], constant termite_metal_gliner_gru_combine_f32_params &p [[buffer(4)]], uint gid [[thread_position_in_grid]]) {
    uint count = p.rows * p.dim; if (gid >= count || p.dim == 0u) return;
    uint row = gid / p.dim; uint col = gid - row * p.dim; uint gate_base = row * p.dim * 3u + col;
    float r = 1.0f / (1.0f + exp(-(gi[gate_base] + gh[gate_base])));
    float z = 1.0f / (1.0f + exp(-(gi[gate_base + p.dim] + gh[gate_base + p.dim])));
    float n = tanh(gi[gate_base + p.dim * 2u] + r * gh[gate_base + p.dim * 2u]);
    float h0 = label_embeddings[gid]; float h1 = (1.0f - z) * n + z * h0; output[gid] = h1 + h0;
}
// native-program:entry termite_embed_absolute_position
// native-program:indent 11
kernel void termite_embed_absolute_position(device const float *token_embedding [[buffer(0)]], device const float *position_embedding [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_embed_absolute_position_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
    if (gid >= p.hidden_size) return;
    uint token_base = p.token_id * p.hidden_size;
    uint position_base = p.position_id * p.hidden_size;
    output[gid] = token_embedding[token_base + gid] + position_embedding[position_base + gid];
}
