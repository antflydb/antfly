// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

#pragma once

#include <stdint.h>

namespace antfly_mxfp8 {

__device__ inline float e8m0(uint32_t exponent) {
    if (exponent == 255u) return __uint_as_float(0x7fc00000u);
    return __uint_as_float(exponent == 0u ? 0x00400000u : exponent << 23);
}

__device__ inline unsigned block_exponent(float maximum) {
    const float raw = maximum * (1.0f / 448.0f);
    const unsigned rounded = ((__float_as_uint(raw) + 0x007fffffu) & 0x7f800000u) >> 23;
    return raw <= 0x1p-127f ? 0u : min(rounded, 254u);
}

__device__ inline float inverse_scale(unsigned exponent) {
    return exponent == 0u ? 0x1p127f : __frcp_rn(e8m0(exponent));
}

} // namespace antfly_mxfp8
