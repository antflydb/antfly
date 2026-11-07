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

// SM120 owns each query/key row in one warp and each channel in one lane.
// Keep the reduction tree common to forward, saved delta and both gradients.
#pragma once

__device__ __forceinline__ float boundary_sum32(float value) {
    #pragma unroll
    for (int step = 16; step; step >>= 1)
        value += __shfl_down_sync(0xffffffff, value, step);
    return __shfl_sync(0xffffffff, value, 0);
}

__device__ __forceinline__ int boundary_row_index(
    int batch, int token, int head, int sequence, int heads) {
    return (batch * sequence + token) * heads * 32 + head * 32 + (threadIdx.x & 31);
}

__device__ __forceinline__ int boundary_packed_index(
    int batch, int token, int head, int sequence, int heads) {
    return (batch * sequence + token) * heads * 96 + head * 32 + (threadIdx.x & 31);
}
