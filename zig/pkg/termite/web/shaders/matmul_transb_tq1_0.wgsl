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

// Tiled matrix multiplication with B transposed and TQ1_0 dequantization.
// C[M,N] = A[M,K] @ dequant(B_tq1_0)^T
//
// TQ1_0 layout: blocks of 256 values, 54 bytes per block:
//   [0:48]   qs - 48 bytes; first 32 bytes hold base-3 packed quintuples for
//             elements 0..159; next 16 bytes hold base-3 packed quintuples for
//             elements 160..239
//   [48:52]  qh - 4 bytes; base-3 packed for elements 240..255 (4 levels)
//   [52:54]  d  - f16 scale
// Decoding for an output index in_block (0..255):
//   range 1 (0..159):    n = in_block / 32,        m = in_block % 32,        src = qs[m]
//   range 2 (160..239):  n = (in_block-160)/16,   m = (in_block-160)%16,    src = qs[32+m]
//   range 3 (240..255):  n = (in_block-240)/4,    m = (in_block-240)%4,     src = qh[m]
// q'  = (src * pow3[n]) mod 256
// xi  = (q' * 3) >> 8        // 0, 1, or 2
// out = (xi - 1) * d         // -d, 0, or d (ternary)

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

fn read_byte(byte_offset: u32) -> u32 {
    let word_idx = byte_offset / 4u;
    let byte_in_word = byte_offset % 4u;
    return (B_raw[word_idx] >> (byte_in_word * 8u)) & 0xFFu;
}

fn read_f16(byte_offset: u32) -> f32 {
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

fn pow3(n: u32) -> u32 {
    var v: u32;
    switch (n) {
        case 0u: { v = 1u; }
        case 1u: { v = 3u; }
        case 2u: { v = 9u; }
        case 3u: { v = 27u; }
        default: { v = 81u; }
    }
    return v;
}

fn dequant_tq1_0(b_row: u32, k_abs: u32) -> f32 {
    let blocks_per_row = params.K / 256u;
    let block_idx = k_abs / 256u;
    let in_block = k_abs % 256u;
    let block_byte = (b_row * blocks_per_row + block_idx) * 54u;

    let d = read_f16(block_byte + 52u);

    var n: u32;
    var src_byte_offset: u32;
    if (in_block < 160u) {
        n = in_block / 32u;
        let m = in_block % 32u;
        src_byte_offset = block_byte + m;
    } else if (in_block < 240u) {
        let rel = in_block - 160u;
        n = rel / 16u;
        let m = rel % 16u;
        src_byte_offset = block_byte + 32u + m;
    } else {
        let rel = in_block - 240u;
        n = rel / 4u;
        let m = rel % 4u;
        src_byte_offset = block_byte + 48u + m;
    }

    let src = read_byte(src_byte_offset);
    let q = (src * pow3(n)) & 0xFFu;
    let xi = (q * 3u) >> 8u;
    return (f32(i32(xi) - 1)) * d;
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_tq1_0(
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

        for (var stage: u32 = 0u; stage < 2u; stage++) {
            let idx = tid + stage * 256u;
            let b_k = idx / TILE_N;
            let b_n = idx % TILE_N;
            let g_col = col_base + b_n;
            let g_k = k_base + b_k;
            if (g_col < params.N && g_k < params.K) {
                tile_b[idx] = dequant_tq1_0(g_col, g_k);
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
