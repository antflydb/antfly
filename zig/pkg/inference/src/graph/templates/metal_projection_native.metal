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

struct antfly_projection_plain {
    static float finish(float value, device const float*, uint) { return value; }
};
struct antfly_projection_bias {
    static float finish(float value, device const float* bias, uint col) { return value + bias[col]; }
};
struct antfly_projection_bias_gelu {
    static float finish(float value, device const float* bias, uint col) { return antfly_qk_gelu(value + bias[col]); }
};
struct antfly_projection_relu {
    static float finish(float value, device const float*, uint) { return max(value, 0.0f); }
};

template<typename Decoder, typename Epilogue, uint Threads, uint Reduction>
__attribute__((always_inline)) inline void antfly_projection_column(
    device const float* input, device const uchar* weight, device const float* bias,
    device float* output, int rows, int in_dim, int out_dim, uint3 thread_pos,
    uint3 group_pos, ushort lane_id, ushort simdgroup_id, threadgroup float* partial) {
    uint tid = thread_pos.x; int col = int(group_pos.x); int row = int(group_pos.y);
    if (row >= rows || rows < 2 || rows > 8 || col >= out_dim || (in_dim & (Decoder::values - 1)) != 0) return;
    float acc = 0.0f; int block_count = in_dim >> Decoder::shift;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) {
        device const uchar* block = weight + ((col * block_count + block_idx) * Decoder::bytes);
        int base = block_idx << Decoder::shift;
        for (int lane = int(tid); lane < Decoder::values; lane += Threads)
            acc += input[row * in_dim + base + lane] * Decoder::decode(block, lane);
    }
    if (Reduction == 0u) {
        acc = simd_sum(acc);
        if (tid == 0) output[row * out_dim + col] = Epilogue::finish(acc, bias, col);
    } else if (Reduction == 1u) {
        partial[tid] = acc; threadgroup_barrier(mem_flags::mem_threadgroup);
        for (uint stride = Threads / 2u; stride > 0u; stride >>= 1) {
            if (tid < stride) partial[tid] += partial[tid + stride];
            threadgroup_barrier(mem_flags::mem_threadgroup);
        }
        if (tid == 0) output[row * out_dim + col] = Epilogue::finish(partial[0], bias, col);
    } else {
        acc = simd_sum(acc); if (lane_id == 0u) partial[simdgroup_id] = acc;
        if (simdgroup_id == 0u && lane_id >= Threads / 32u) partial[lane_id] = 0.0f;
        threadgroup_barrier(mem_flags::mem_threadgroup);
        float total = simd_sum(partial[lane_id]);
        if (lane_id == 0u && simdgroup_id == 0u) output[row * out_dim + col] = Epilogue::finish(total, bias, col);
    }
}

template<typename Decoder, typename Epilogue>
__attribute__((always_inline)) inline void antfly_projection_two_columns(
    device const float* input, device const uchar* weight, device const float* bias,
    device float* output, int rows, int in_dim, int out_dim, uint3 thread_pos, uint3 group_pos) {
    uint tid = thread_pos.x; int col0 = int(group_pos.x << 1); int col1 = col0 + 1; int row = int(group_pos.y);
    if (row >= rows || rows < 2 || rows > 8 || col0 >= out_dim || (in_dim & (Decoder::values - 1)) != 0) return;
    float acc0 = 0.0f; float acc1 = 0.0f; int block_count = in_dim >> Decoder::shift;
    device const float* row_input = input + row * in_dim;
    device const uchar* col0_weight = weight + col0 * block_count * Decoder::bytes;
    bool has_col1 = col1 < out_dim;
    device const uchar* col1_weight = has_col1 ? weight + col1 * block_count * Decoder::bytes : col0_weight;
    for (int block_idx = 0; block_idx < block_count; ++block_idx) {
        device const uchar* block0 = col0_weight + block_idx * Decoder::bytes;
        device const uchar* block1 = col1_weight + block_idx * Decoder::bytes;
        int base = block_idx << Decoder::shift;
        for (int lane = int(tid); lane < Decoder::values; lane += 32) {
            float x = row_input[base + lane]; acc0 += x * Decoder::decode(block0, lane);
            if (has_col1) acc1 += x * Decoder::decode(block1, lane);
        }
    }
    acc0 = simd_sum(acc0); acc1 = simd_sum(acc1);
    if (tid == 0) {
        output[row * out_dim + col0] = Epilogue::finish(acc0, bias, col0);
        if (has_col1) output[row * out_dim + col1] = Epilogue::finish(acc1, bias, col1);
    }
}

template<typename Decoder, typename Epilogue, uint NSG, uint NC, uint NR>
__attribute__((always_inline)) inline void antfly_projection_k_tile(
    device const float* input, device const uchar* weight, device const float* bias,
    device float* output, int rows, int in_dim, int out_dim, ushort lane_id,
    ushort simdgroup_id, uint3 group_pos) {
    const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NC;
    const uint first_r = group_pos.y * NR;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return;
    const uint block_count = uint(in_dim) >> 8;
    const uint sub = uint(lane_id) >> Decoder::sub_shift;
    const uint j0 = (uint(lane_id) & Decoder::sub_mask) << 3;
    float acc[NC][NR]; for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] = 0.0f;
    for (uint b = 0u; b < block_count; ++b) {
        uint input_off = first_r * uint(in_dim) + b * 256u + sub * Decoder::sub_values + j0;
        float x[NR][8]; for (uint rr = 0u; rr < NR; ++rr) for (uint i = 0u; i < 8u; ++i)
            x[rr][i] = first_r + rr < uint(rows) ? input[input_off + rr * uint(in_dim) + i] : 0.0f;
        for (uint c = 0u; c < NC; ++c) {
            uint col = first_o + c; if (col >= uint(out_dim)) continue;
            device const uchar* block = weight + (col * block_count + b) * Decoder::bytes;
            typename Decoder::Scale scale = Decoder::scale(block, sub);
            for (uint i = 0u; i < 8u; ++i) {
                float w = Decoder::value(block, scale, sub, j0, i);
                for (uint rr = 0u; rr < NR; ++rr) acc[c][rr] += x[rr][i] * w;
            }
        }
    }
    for (uint c = 0u; c < NC; ++c) for (uint rr = 0u; rr < NR; ++rr) {
        float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr;
        if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = Epilogue::finish(total, bias, col);
    }
}

template<typename Decoder, uint Rows, uint Threads>
__attribute__((always_inline)) inline void antfly_projection_matrix(
    device const float* input, device const uchar* weight, device float* output,
    int rows, int in_dim, int out_dim, uint3 thread_pos, uint3 group_pos,
    ushort simdgroup_id, threadgroup uchar* shared) {
    using Scalar = typename Decoder::Scalar;
    using Matrix = typename Decoder::Matrix;
    const uint tid = thread_pos.x; const uint first_o = group_pos.x * 64u; const uint first_r = group_pos.y * Rows;
    if (rows < 2 || in_dim <= 0 || (uint(in_dim) & 255u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return;
    threadgroup Scalar* weight_tile = (threadgroup Scalar*)shared;
    threadgroup Scalar* input_tile = weight_tile + 2048;
    Matrix mw[8]; Matrix mx; simdgroup_float8x8 acc[8];
    for (uint i = 0u; i < 8u; ++i) acc[i] = make_filled_simdgroup_matrix<float, 8>(0.0f);
    const uint block_count = uint(in_dim) >> 8; const uint sg_row = uint(simdgroup_id) * 8u;
    for (uint k0 = 0u; k0 < uint(in_dim); k0 += 32u) {
        for (uint index = tid; index < 2048u; index += Threads) {
            uint k = index >> 6; uint col = index & 63u; uint global_col = first_o + col; uint global_k = k0 + k;
            Scalar value = Scalar(0.0f);
            if (global_col < uint(out_dim)) {
                device const uchar* block = weight + (global_col * block_count + (global_k >> 8)) * Decoder::bytes;
                value = Scalar(Decoder::decode(block, int(global_k & 255u)));
            }
            weight_tile[k * 64u + col] = value;
        }
        for (uint index = tid; index < Rows * 32u; index += Threads) {
            uint row = index >> 5; uint k = index & 31u; uint global_row = first_r + row;
            input_tile[row * 32u + k] = global_row < uint(rows) ? Scalar(input[global_row * uint(in_dim) + k0 + k]) : Scalar(0.0f);
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
        for (uint k = 0u; k < 32u; k += 8u) {
            for (uint i = 0u; i < 8u; ++i) simdgroup_load(mw[i], weight_tile + k * 64u + i * 8u, 64u);
            simdgroup_load(mx, input_tile + sg_row * 32u + k, 32u);
            for (uint i = 0u; i < 8u; ++i) simdgroup_multiply_accumulate(acc[i], mx, mw[i], acc[i]);
        }
        threadgroup_barrier(mem_flags::mem_threadgroup);
    }
    threadgroup float* result_tile = (threadgroup float*)shared;
    for (uint i = 0u; i < 8u; ++i) simdgroup_store(acc[i], result_tile + sg_row * 64u + i * 8u, 64u);
    threadgroup_barrier(mem_flags::mem_threadgroup);
    for (uint index = tid; index < Rows * 64u; index += Threads) {
        uint row = index >> 6; uint col = index & 63u; uint global_row = first_r + row;
        if (global_row < uint(rows) && first_o + col < uint(out_dim)) output[global_row * uint(out_dim) + first_o + col] = result_tile[index];
    }
}

template<uint NSG, uint NR0>
__attribute__((always_inline)) inline void antfly_projection_q4_packed_tile(
    device const float* input, device const uchar* weight, device float* output,
    int rows, int in_dim, int out_dim, ushort lane_id, ushort simdgroup_id, uint3 group_pos) {
    const uint NQ16 = 16u; const uint first_o = (group_pos.x * NSG + uint(simdgroup_id)) * NR0; const uint first_r = group_pos.y * 2u;
    if (rows != 2 || in_dim <= 0 || (uint(in_dim) & 31u) != 0u || out_dim <= 0 || first_r >= uint(rows) || first_o >= uint(out_dim)) return;
    const uint block_count = uint(in_dim) >> 5; const uint ix = uint(lane_id) >> 1; const uint il = (uint(lane_id) & 1u) * 8u;
    float acc[NR0][2]; for (uint c = 0u; c < NR0; ++c) for (uint rr = 0u; rr < 2u; ++rr) acc[c][rr] = 0.0f;
    for (uint b = ix; b < block_count; b += NQ16) {
        ushort qs_cache[NR0][4]; float dscale[NR0];
        for (uint c = 0u; c < NR0; ++c) {
            uint col = first_o + c;
            if (col >= uint(out_dim)) { dscale[c] = 0.0f; for (uint q = 0u; q < 4u; ++q) qs_cache[c][q] = 0u; continue; }
            uint off = (col * block_count + b) * 18u;
            ushort bits = (ushort(weight[off + 1u]) << 8) | ushort(weight[off]); dscale[c] = float(as_type<half>(bits));
            const device ushort* qs = reinterpret_cast<const device ushort*>(weight + off + 2u) + (il >> 1);
            for (uint q = 0u; q < 4u; ++q) qs_cache[c][q] = qs[q];
        }
        for (uint rr = 0u; rr < 2u; ++rr) {
            uint in_off = (first_r + rr) * uint(in_dim) + b * 32u + il; float yl[16]; float sumy = 0.0f;
            for (uint i = 0u; i < 8u; i += 2u) {
                float x0 = input[in_off + i]; float x1 = input[in_off + i + 1u]; float x16 = input[in_off + 16u + i]; float x17 = input[in_off + 16u + i + 1u];
                sumy += x0 + x1 + x16 + x17; yl[i] = x0; yl[i + 1u] = x1 / 256.0f; yl[i + 8u] = x16 / 16.0f; yl[i + 9u] = x17 / 4096.0f;
            }
            for (uint c = 0u; c < NR0; ++c) {
                float qacc = 0.0f;
                for (uint i = 0u; i < 8u; i += 2u) {
                    uint packed = uint(qs_cache[c][i >> 1]); qacc += yl[i] * float(packed & 0x000Fu); qacc += yl[i + 1u] * float(packed & 0x0F00u);
                    qacc += yl[i + 8u] * float(packed & 0x00F0u); qacc += yl[i + 9u] * float(packed & 0xF000u);
                }
                acc[c][rr] += dscale[c] * (qacc - 8.0f * sumy);
            }
        }
    }
    for (uint c = 0u; c < NR0; ++c) for (uint rr = 0u; rr < 2u; ++rr) {
        float total = simd_sum(acc[c][rr]); uint col = first_o + c; uint row = first_r + rr;
        if (lane_id == 0u && col < uint(out_dim) && row < uint(rows)) output[row * uint(out_dim) + col] = total;
    }
}
