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

// native-program:entry termite_training_accumulate_f32
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
// native-program:entry termite_training_adamw_f32
extern "C" __global__ void termite_training_adamw_f32(ANTFLY_CUDA_TRAIN_ADAMW_PARAMS) {
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
// native-program:entry termite_training_sum_squares_f32
extern "C" __global__ void termite_training_sum_squares_f32(
    float* output,
    const float* input,
    unsigned int count
) {
    __shared__ float partial[256];
    // Training uses this sum to derive the global clipping scale.  A
    // multi-block atomic reduction makes the add order scheduler-dependent;
    // tiny differences then compound across AdamW steps.  One block owns the
    // whole tensor and reduces fixed per-thread grid-stride sums in a fixed
    // tree.  Successive tensor launches share the same stream, so the final
    // output accumulation is ordered as well.
    float value = 0.0f;
    for (unsigned int i = threadIdx.x; i < count; i += blockDim.x) {
        float x = input[i];
        value += x * x;
    }
    partial[threadIdx.x] = value;
    __syncthreads();
    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {
        if (threadIdx.x < stride) partial[threadIdx.x] += partial[threadIdx.x + stride];
        __syncthreads();
    }
    if (threadIdx.x == 0u) output[0] += partial[0];
}
// native-program:entry termite_masked_bce_accumulate_f32
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
// native-program:entry termite_masked_bce_finalize_f32
extern "C" __global__ void termite_masked_bce_finalize_f32(
    float* accum,
    float eps,
    unsigned int mean_reduction
) {
    if (blockIdx.x == 0 && threadIdx.x == 0 && mean_reduction) {
        accum[0] /= accum[1] + eps;
    }
}
// native-program:entry termite_masked_bce_backward_f32
extern "C" __global__ void termite_masked_bce_backward_f32(ANTFLY_CUDA_MASKED_BCE_BACKWARD_F32_PARAMS) {
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
// native-program:entry termite_layer_norm_backward_f32
extern "C" __global__ void termite_layer_norm_backward_f32(ANTFLY_CUDA_LAYER_NORM_BACKWARD_PARAMS) {
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
// native-program:entry termite_deberta_attention_backward_scores_f32
extern "C" __global__ void termite_deberta_attention_backward_scores_f32(ANTFLY_CUDA_DEBERTA_ATTENTION_BACKWARD_SCORES_F32_PARAMS) {
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

{{relative_softmax}}

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
// native-program:entry termite_deberta_attention_backward_f32
extern "C" __global__ void termite_deberta_attention_backward_f32(ANTFLY_CUDA_DEBERTA_ATTENTION_BACKWARD_F32_PARAMS) {
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
