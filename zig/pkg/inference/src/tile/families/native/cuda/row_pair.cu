// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

// native-program:entry termite_linear_f16_rowpair_f32
#include <cuda_fp16.h>
// One warp reduces two rows. Shared by plain/bias/GELU and three-output
// projection entrypoints; static epilogue, no duplicated dot/reduction body.
template<bool Gelu>
__device__ void antfly_f16_rowpair_impl(float* out, const float* input,
    const __half* weight, const float* bias, unsigned inputs, unsigned outputs) {
    const unsigned lane = threadIdx.x % 32;
    const unsigned row = blockIdx.x * 8 + (threadIdx.x / 32) * 2;
    float a = 0, b = 0;
    if ((inputs & 1) == 0) {
        float a1 = 0, b1 = 0;
        for (unsigned c = lane * 2; c < inputs; c += 64) {
            const float x = __half2float(__float2half_rn(input[c]));
            const float y = __half2float(__float2half_rn(input[c + 1]));
            if (row < outputs) {
                const float2 w = __half22float2(*reinterpret_cast<const __half2*>(weight + size_t(row) * inputs + c));
                a = fmaf(x, w.x, a); a1 = fmaf(y, w.y, a1);
            }
            if (row + 1 < outputs) {
                const float2 w = __half22float2(*reinterpret_cast<const __half2*>(weight + size_t(row + 1) * inputs + c));
                b = fmaf(x, w.x, b); b1 = fmaf(y, w.y, b1);
            }
        }
        a += a1; b += b1;
    } else {
        for (unsigned c = lane; c < inputs; c += 32) {
            const float x = __half2float(__float2half_rn(input[c]));
            if (row < outputs) a = fmaf(x, __half2float(weight[size_t(row) * inputs + c]), a);
            if (row + 1 < outputs) b = fmaf(x, __half2float(weight[size_t(row + 1) * inputs + c]), b);
        }
    }
    for (unsigned offset = 16; offset; offset >>= 1) {
        a += __shfl_down_sync(0xffffffff, a, offset);
        b += __shfl_down_sync(0xffffffff, b, offset);
    }
    if (lane == 0) {
        if (row < outputs) {
            const float x = a + (bias ? bias[row] : 0);
            out[row] = Gelu ? termite_decoder_activation_f32(x, 0u) : x;
        }
        if (row + 1 < outputs) {
            const float x = b + (bias ? bias[row + 1] : 0);
            out[row + 1] = Gelu ? termite_decoder_activation_f32(x, 0u) : x;
        }
    }
}
extern "C" __global__ void termite_linear_f16_rowpair_f32(float* dst,
    const float* input, const __half* weight, const float* bias, unsigned inputs, unsigned outputs) {
    antfly_f16_rowpair_impl<false>(dst, input, weight, bias, inputs, outputs);
}
// native-program:entry termite_linear_f16_rowpair_gelu_f32
extern "C" __global__ void termite_linear_f16_rowpair_gelu_f32(float* dst,
    const float* input, const __half* weight, const float* bias, unsigned inputs, unsigned outputs) {
    antfly_f16_rowpair_impl<true>(dst, input, weight, bias, inputs, outputs);
}
// native-program:entry termite_linear_f16_rowpair_qkv_f32
extern "C" __global__ void termite_linear_f16_rowpair_qkv_f32(float* q, float* k, float* v,
    const float* input, const __half* qw, const __half* kw, const __half* vw,
    const float* qb, const float* kb, const float* vb, unsigned inputs, unsigned q_outputs, unsigned kv_outputs) {
    float* out = blockIdx.y == 0 ? q : (blockIdx.y == 1 ? k : v);
    const __half* weight = blockIdx.y == 0 ? qw : (blockIdx.y == 1 ? kw : vw);
    const float* bias = blockIdx.y == 0 ? qb : (blockIdx.y == 1 ? kb : vb);
    const unsigned outputs = blockIdx.y == 0 ? q_outputs : kv_outputs;
    antfly_f16_rowpair_impl<false>(out, input, weight, bias, inputs, outputs);
}
// native-program:entry termite_add_bias_gelu_rows_f32
extern "C" __global__ void termite_add_bias_gelu_rows_f32(float* dst,
    const float* bias, unsigned rows, unsigned outputs) {
    const unsigned i = blockIdx.x * blockDim.x + threadIdx.x;
    if (i < rows * outputs) dst[i] = termite_decoder_activation_f32(dst[i] + bias[i % outputs], 0u);
}
