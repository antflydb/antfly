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

// Tiled matrix multiplication with B transposed and Q4_1 dequantization.
// C[M,N] = A[M,K] @ dequant(B_q4_1)^T
//
// B is quantized in Q4_1 format: blocks of 32 values.
// Each block = 2 bytes (f16 scale) + 2 bytes (f16 min)
// + 16 bytes (32 x 4-bit nibbles) = 20 bytes.
// B has shape [N, K] row-major (before transpose), so each of the N rows
// has K values packed into K/32 blocks.

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
const TILE_K: u32 = 32;

var<workgroup> tile_a: array<f32, 512>;
var<workgroup> tile_b: array<f32, 512>;

fn read_f16_at(byte_offset: u32) -> f32 {
    let word_idx = byte_offset / 4u;
    let byte_off = byte_offset % 4u;
    let w0 = B_raw[word_idx];
    var bits: u32;
    if (byte_off == 0u) {
        bits = w0 & 0xFFFFu;
    } else if (byte_off == 2u) {
        bits = (w0 >> 16u) & 0xFFFFu;
    } else {
        let w1 = B_raw[word_idx + 1u];
        bits = ((w0 >> (byte_off * 8u)) | (w1 << ((4u - byte_off) * 8u))) & 0xFFFFu;
    }
    return unpack2x16float(bits).x;
}

fn dequant_q4_1_block(b_row: u32, block_idx: u32, in_block: u32) -> f32 {
    let blocks_per_row = params.K / 32u;
    let block_byte = (b_row * blocks_per_row + block_idx) * 20u;
    let scale = read_f16_at(block_byte);
    let min_val = read_f16_at(block_byte + 2u);

    let nibble_byte_offset = block_byte + 4u + (in_block % 16u);
    let nibble_word_idx = nibble_byte_offset / 4u;
    let nibble_byte_in_word = nibble_byte_offset % 4u;
    let nibble_word = B_raw[nibble_word_idx];
    let nibble_byte_val = (nibble_word >> (nibble_byte_in_word * 8u)) & 0xFFu;

    var nibble: u32;
    if (in_block < 16u) {
        nibble = nibble_byte_val & 0xFu;
    } else {
        nibble = (nibble_byte_val >> 4u) & 0xFu;
    }

    return scale * f32(nibble) + min_val;
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_q4_1(
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

    var sum: f32 = 0.0;

    for (var t: u32 = 0u; t < num_tiles; t++) {
        let k_base = t * TILE_K;

        for (var stage: u32 = 0u; stage < 2u; stage++) {
            let idx = tid + stage * 256u;
            let a_m = idx / TILE_K;
            let a_k = idx % TILE_K;
            let g_row = row_base + a_m;
            let g_k = k_base + a_k;
            if (g_row < params.M && g_k < params.K) {
                tile_a[idx] = A[g_row * params.K + g_k];
            } else {
                tile_a[idx] = 0.0;
            }
        }

        let block_idx = t;
        for (var stage: u32 = 0u; stage < 2u; stage++) {
            let idx = tid + stage * 256u;
            let b_k = idx / TILE_N;
            let b_n = idx % TILE_N;
            let g_col = col_base + b_n;
            let g_k = k_base + b_k;
            if (g_col < params.N && g_k < params.K) {
                tile_b[idx] = dequant_q4_1_block(g_col, block_idx, b_k);
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
