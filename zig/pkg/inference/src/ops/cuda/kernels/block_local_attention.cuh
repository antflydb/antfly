// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
// Included directly in the two native 128-thread kernels; no include guard.
// Local is static. Full/local ranges, sentinels, bias and finite checks retain their original contracts.

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

    const unsigned int begin = Local && qi > radius ? qi - radius : 0u;
    const unsigned int end = Local && radius < seq_len - 1u - qi ? qi + radius + 1u : seq_len;
    float max_score = Local ? -INFINITY : -3.402823466e+38f;
    for (unsigned int ki = begin; ki < end; ++ki) {
        bool valid = Local ? mask[b * seq_len + ki] != 0ll : !(causal && ki > qi) && !(has_mask && mask[b * seq_len + ki] == 0ll);
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
            float score = valid ? scratch[0] * scale : (Local ? -INFINITY : -3.402823466e+38f);
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
    for (unsigned int ki = begin + tid; ki < end; ki += blockDim.x) {
        float e = Local && !isfinite(shared_max) ? 0.0f : expf(scores[ki] - shared_max);
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
        for (unsigned int ki = begin; ki < end; ++ki) {
            unsigned int v_idx = head_major ? ((b * num_heads + head) * seq_len + ki) * head_dim + d : (b * seq_len + ki) * hidden + head * head_dim + d;
            acc += scores[ki] * v[v_idx];
        }
        unsigned int out_idx = head_major ? ((b * num_heads + head) * seq_len + qi) * head_dim + d : (b * seq_len + qi) * hidden + head * head_dim + d;
        dst[out_idx] = shared_denom > 0.0f ? acc / shared_denom : 0.0f;
    }
