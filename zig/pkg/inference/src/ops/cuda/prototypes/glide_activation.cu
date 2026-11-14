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
