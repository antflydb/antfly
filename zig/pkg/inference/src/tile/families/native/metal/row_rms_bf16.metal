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

// native-program:entry antfly_row_rms_to_bf16
struct RowRmsBf16Params { uint width, input_f32; float epsilon; };

inline float antfly_row_rms_input(device const uchar *source, ulong index, bool f32) {
    return f32 ? reinterpret_cast<device const float *>(source)[index]
               : float(reinterpret_cast<device const bfloat *>(source)[index]);
}

inline void antfly_row_rms_round(
    device const uchar *source, device const bfloat *gamma, device bfloat *output,
    RowRmsBf16Params p, uint tid, threadgroup float *partial
) {
    float square = 0.0f;
    for (uint i = tid; i < p.width; i += 128u) {
        const float value = antfly_row_rms_input(source, i, p.input_f32);
        square = fma(value, value, square);
    }
    const float scale = rsqrt(antfly_residual_mix_reduce(square, tid, partial) / float(p.width) + p.epsilon);
    for (uint i = tid; i < p.width; i += 128u) {
        const float value = antfly_row_rms_input(source, i, p.input_f32);
        output[i] = bfloat((value * scale) * float(gamma[i]));
    }
}

kernel void antfly_row_rms_to_bf16(
    device const uchar *source [[buffer(0)]],
    device const bfloat *gamma [[buffer(1)]],
    device bfloat *output [[buffer(2)]],
    constant RowRmsBf16Params &p [[buffer(3)]],
    uint row [[threadgroup_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]]
) {
    const ulong base = ulong(row) * p.width;
    threadgroup float partial[4];
    antfly_row_rms_round(source + base * (p.input_f32 ? 4u : 2u), gamma, output + base, p, tid, partial);
}

// native-program:entry antfly_indexed_row_rms_to_bf16
struct IndexedRowRmsBf16Params { uint width; float epsilon; uint ids[256]; };

kernel void antfly_indexed_row_rms_to_bf16(
    device const uchar *source [[buffer(0)]],
    device const bfloat *gamma [[buffer(1)]],
    device bfloat *output [[buffer(2)]],
    constant IndexedRowRmsBf16Params &p [[buffer(3)]],
    uint row [[threadgroup_position_in_grid]],
    uint tid [[thread_index_in_threadgroup]]
) {
    const RowRmsBf16Params norm{p.width, 0u, p.epsilon};
    const ulong first = ulong(p.ids[row]) * p.width * 2u;
    threadgroup float partial[4];
    antfly_row_rms_round(source + first, gamma, output + ulong(row) * p.width, norm, tid, partial);
}
