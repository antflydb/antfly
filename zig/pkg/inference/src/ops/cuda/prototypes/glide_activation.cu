// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
#include <cuda_fp16.h>

// Preserve F32 SiLU arithmetic, rounding only at the next projection's F16
// input boundary. This avoids materializing and rereading an F32 activation.
extern "C" __global__ void glide_silu_up_f16(half *out, const float *gate,
                                            const float *up, unsigned count) {
    unsigned i=blockIdx.x*blockDim.x+threadIdx.x;
    if(i<count) {
        float x=gate[i];
        out[i]=__float2half_rn((x/(1.f+expf(-x)))*up[i]);
    }
}
