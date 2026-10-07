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

// Tiled matrix multiplication with B transposed and Q6_K dequantization.
// C[M,N] = A[M,K] @ dequant(B_q6k)^T
//
// B is quantized in Q6_K format: blocks of 256 values.
// Each block = 210 bytes:
//   [0:128]   ql  — 128 bytes, low 4-bit nibbles (256 values packed as pairs)
//   [128:192] qh  — 64 bytes, high 2-bit pairs
//   [192:208] scales — 16 signed int8 scales (one per 16-value sub-block)
//   [208:210] d   — f16 global scale
//
// 16 sub-blocks of 16 values each.
// value = d * scales[sub] * ((ql_low4 | (qh_high2 << 4)) - 32)
//
// Uses 16x16x32 tiling with TILE_K=32. Each 256-value Q6_K block spans 8
// K-tiles (256/32). The dequant function maps (b_row, k_abs) to the correct
// block, sub-block, and element.

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

var<workgroup> tile_a: array<f32, 512>;  // [TILE_M][TILE_K]
var<workgroup> tile_b: array<f32, 512>;  // [TILE_K][TILE_N]

// Read a single byte from B_raw at arbitrary byte offset.
fn read_byte(byte_offset: u32) -> u32 {
    let word_idx = byte_offset / 4u;
    let byte_in_word = byte_offset % 4u;
    return (B_raw[word_idx] >> (byte_in_word * 8u)) & 0xFFu;
}

// Read f16 value from 2 bytes at byte_offset (may span a u32 boundary).
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

fn dequant_q6_k(b_row: u32, k_abs: u32) -> f32 {
    let blocks_per_row = params.K / 256u;
    let block_idx = k_abs / 256u;
    let in_block = k_abs % 256u;            // 0..255
    let block_byte = (b_row * blocks_per_row + block_idx) * 210u;

    // Determine sub-block (0..15) and position within sub-block (0..15)
    let sub = in_block / 16u;
    let elem = in_block % 16u;

    // Read global scale d (f16 at offset 208)
    let d = read_f16(block_byte + 208u);

    // Read signed int8 scale for this sub-block (at offset 192 + sub)
    let sc_byte = read_byte(block_byte + 192u + sub);
    var sc_signed: f32;
    if (sc_byte >= 128u) {
        sc_signed = f32(sc_byte) - 256.0;
    } else {
        sc_signed = f32(sc_byte);
    }
    let scale = d * sc_signed;

    // ql layout: 128 bytes at offset 0. Two halves of 64 bytes each.
    // half = sub / 8, group = (sub % 8) / 2
    // nibble_shift = (group / 2) * 4 — selects high or low nibble
    // ql_off = half * 64 + (group & 1) * 32
    // l = (sub % 2) * 16 + elem
    let half = sub / 8u;
    let group = (sub % 8u) / 2u;
    let nibble_shift = (group / 2u) * 4u;
    let ql_off = half * 64u + (group & 1u) * 32u;
    let l = (sub % 2u) * 16u + elem;
    let ql_byte = read_byte(block_byte + ql_off + l);
    let low4 = (ql_byte >> nibble_shift) & 0x0Fu;

    // qh layout: 64 bytes at offset 128.
    // qh_off = half * 32, qh_shift = group * 2
    let qh_off = half * 32u;
    let qh_shift = group * 2u;
    let qh_byte = read_byte(block_byte + 128u + qh_off + l);
    let high2 = (qh_byte >> qh_shift) & 0x03u;

    // Combine: 6-bit value = low4 | (high2 << 4), range [0, 63], centered at 32
    let q = i32(low4 | (high2 << 4u)) - 32;
    return scale * f32(q);
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_q6_k(
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

        // Load tile_a: A[row_base+m, k_base+k]
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

        // Load tile_b: dequant B[col_base+n, k_base+k]
        for (var stage: u32 = 0u; stage < 2u; stage++) {
            let idx = tid + stage * 256u;
            let b_k = idx / TILE_N;
            let b_n = idx % TILE_N;
            let g_col = col_base + b_n;
            let g_k = k_base + b_k;
            if (g_col < params.N && g_k < params.K) {
                tile_b[idx] = dequant_q6_k(g_col, g_k);
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
