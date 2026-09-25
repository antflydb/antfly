// Copyright 2026 Antfly, Inc.
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

// Tiled matrix multiplication with B transposed and BitNet I2_S dequantization.
// C[M,N] = quant8(A[M,K]) @ dequant(B_i2_s[N,K])^T
//
// I2_S packs 128 ternary values into 32 bytes. Each byte stores four 2-bit
// lanes in the order used by BitNet GGUF:
//   bits 7..6 -> k = group
//   bits 5..4 -> k = 32 + group
//   bits 3..2 -> k = 64 + group
//   bits 1..0 -> k = 96 + group
//
// BitNet W1.58A8 execution quantizes activations per row using absmax/127.

struct Params {
    M: u32,
    N: u32,
    K: u32,
    _pad: u32,
};

@group(0) @binding(0) var<storage, read> A: array<f32>;
@group(0) @binding(1) var<storage, read> B_raw: array<u32>;
@group(0) @binding(2) var<storage, read_write> C: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

const TILE_M: u32 = 16;
const TILE_N: u32 = 16;
const TILE_K: u32 = 128;

var<workgroup> tile_a: array<f32, 2048>; // [TILE_M][TILE_K]
var<workgroup> tile_b: array<f32, 2048>; // [TILE_K][TILE_N]
var<workgroup> row_scales: array<f32, 16>;

fn read_byte(byte_offset: u32) -> u32 {
    let word_idx = byte_offset / 4u;
    let byte_in_word = byte_offset % 4u;
    return (B_raw[word_idx] >> (byte_in_word * 8u)) & 0xFFu;
}

fn i2_s_value(code: u32) -> f32 {
    if (code == 0u) {
        return -1.0;
    }
    if (code == 2u) {
        return 1.0;
    }
    return 0.0;
}

fn quantize_activation(value: f32, scale: f32) -> f32 {
    return clamp(round(value / scale), -127.0, 127.0) * scale;
}

fn row_activation_scale(row: u32) -> f32 {
    var abs_max = 0.0;
    for (var k: u32 = 0u; k < params.K; k++) {
        abs_max = max(abs_max, abs(A[row * params.K + k]));
    }
    if (abs_max == 0.0) {
        return 1.0;
    }
    return abs_max / 127.0;
}

fn dequant_i2_s_block(b_row: u32, block_idx: u32, in_block: u32) -> f32 {
    let blocks_per_row = params.K / 128u;
    let block_byte = (b_row * blocks_per_row + block_idx) * 32u;
    let group = in_block % 32u;
    let lane = in_block / 32u;
    let packed = read_byte(block_byte + group);
    var code: u32;
    if (lane == 0u) {
        code = (packed >> 6u) & 0x03u;
    } else if (lane == 1u) {
        code = (packed >> 4u) & 0x03u;
    } else if (lane == 2u) {
        code = (packed >> 2u) & 0x03u;
    } else {
        code = packed & 0x03u;
    }
    return i2_s_value(code);
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_i2_s(
    @builtin(global_invocation_id) gid: vec3<u32>,
    @builtin(local_invocation_id) lid: vec3<u32>,
) {
    let row = gid.y;
    let col = gid.x;
    let local_row = lid.y;
    let local_col = lid.x;
    let tid = local_row * TILE_N + local_col;

    let row_base = gid.y - local_row;
    let col_base = gid.x - local_col;
    let num_tiles = (params.K + TILE_K - 1u) / TILE_K;

    if (local_col == 0u) {
        if (row < params.M) {
            row_scales[local_row] = row_activation_scale(row);
        } else {
            row_scales[local_row] = 1.0;
        }
    }
    workgroupBarrier();

    var sum = 0.0;

    for (var t: u32 = 0u; t < num_tiles; t++) {
        let k_base = t * TILE_K;

        for (var stage: u32 = 0u; stage < 8u; stage++) {
            let idx = tid + stage * 256u;
            let a_m = idx / TILE_K;
            let a_k = idx % TILE_K;
            let g_row = row_base + a_m;
            let g_k = k_base + a_k;
            if (g_row < params.M && g_k < params.K) {
                tile_a[idx] = quantize_activation(A[g_row * params.K + g_k], row_scales[a_m]);
            } else {
                tile_a[idx] = 0.0;
            }
        }

        for (var stage: u32 = 0u; stage < 8u; stage++) {
            let idx = tid + stage * 256u;
            let b_k = idx / TILE_N;
            let b_n = idx % TILE_N;
            let g_col = col_base + b_n;
            let g_k = k_base + b_k;
            if (g_col < params.N && g_k < params.K) {
                tile_b[idx] = dequant_i2_s_block(g_col, t, b_k);
            } else {
                tile_b[idx] = 0.0;
            }
        }

        workgroupBarrier();

        for (var i: u32 = 0u; i < TILE_K; i++) {
            sum += tile_a[local_row * TILE_K + i] * tile_b[i * TILE_N + local_col];
        }

        workgroupBarrier();
    }

    if (row < params.M && col < params.N) {
        C[row * params.N + col] = sum;
    }
}
