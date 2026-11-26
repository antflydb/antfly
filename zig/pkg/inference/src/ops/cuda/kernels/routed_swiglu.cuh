// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <cuda_bf16.h>

namespace antfly_routed_swiglu {

__device__ inline float sigmoid(float x) {
    float result;
    asm("{ .reg .f32 t; mul.rn.f32 t, %1, 0fBFB8AA3B; "
        "ex2.approx.ftz.f32 t, t; add.rn.f32 t, t, 0f3F800000; "
        "rcp.approx.ftz.f32 %0, t; }" : "=f"(result) : "f"(x));
    return result;
}

__device__ inline float softcap(float x, float cap) {
    const float scaled = __fdiv_rn(x, cap);
    float value;
    asm("tanh.approx.f32 %0, %1;" : "=f"(value) : "f"(scaled));
    return cap * value;
}

__device__ inline __nv_bfloat16 weighted(__nv_bfloat16 gate_word, __nv_bfloat16 up_word,
                                        float route_weight, float gate_cap, float up_cap) {
    const float gate = __bfloat162float(gate_word);
    const float up = __bfloat162float(up_word);
    const float value = ((softcap(gate, gate_cap) * sigmoid(gate)) * softcap(up, up_cap)) * route_weight;
    return __float2bfloat16_rn(value);
}

} // namespace antfly_routed_swiglu
