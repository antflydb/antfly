// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

// Tiled matrix multiplication with B transposed and Q8_0 dequantization.
// C[M,N] = A[M,K] @ dequant(B_q8)^T
//
// B is quantized in Q8_0 format: blocks of 32 values.
// Each block = 2 bytes (f16 scale) + 32 bytes (32 x int8) = 34 bytes.
// B has shape [N, K] row-major (before transpose), so each of the N rows
// has K values packed into K/32 blocks.
//
// Uses 16x16x32 tiling: TILE_K=32 matches the Q8_0 block size exactly,
// so each K-tile step processes one complete quant block per B row.
// Dequantization happens cooperatively when loading tile_b into shared memory.

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
const TILE_K: u32 = 32;  // = Q8_0 block size

var<workgroup> tile_a: array<f32, 512>;  // [TILE_M][TILE_K]
var<workgroup> tile_b: array<f32, 512>;  // [TILE_K][TILE_N]

fn dequant_q8_0_block(b_row: u32, block_idx: u32, in_block: u32) -> f32 {
    let blocks_per_row = params.K / 32u;
    let block_byte = (b_row * blocks_per_row + block_idx) * 34u;

    // Read scale (f16, 2 bytes)
    let scale_word_idx = block_byte / 4u;
    let scale_byte_off = block_byte % 4u;
    let w0 = B_raw[scale_word_idx];
    var scale_bits: u32;
    if (scale_byte_off == 0u) {
        scale_bits = w0 & 0xFFFFu;
    } else if (scale_byte_off == 2u) {
        scale_bits = (w0 >> 16u) & 0xFFFFu;
    } else {
        let w1 = B_raw[scale_word_idx + 1u];
        scale_bits = ((w0 >> (scale_byte_off * 8u)) | (w1 << ((4u - scale_byte_off) * 8u))) & 0xFFFFu;
    }
    let scale = unpack2x16float(scale_bits).x;

    // Read int8 value
    let val_byte_offset = block_byte + 2u + in_block;
    let val_word_idx = val_byte_offset / 4u;
    let val_byte_in_word = val_byte_offset % 4u;
    let val_word = B_raw[val_word_idx];
    let byte_val = (val_word >> (val_byte_in_word * 8u)) & 0xFFu;

    // Sign-extend from 8-bit
    var signed_val: f32;
    if (byte_val >= 128u) {
        signed_val = f32(byte_val) - 256.0;
    } else {
        signed_val = f32(byte_val);
    }

    return scale * signed_val;
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_q8_0(
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

        // Load tile_a: A[row_base+m, k_base+k] → tile_a[m*32+k]
        for (var pass: u32 = 0u; pass < 2u; pass++) {
            let idx = tid + pass * 256u;
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

        // Load tile_b: dequant B[col_base+n, k_base+k] → tile_b[k*16+n]
        let block_idx = t;
        for (var pass: u32 = 0u; pass < 2u; pass++) {
            let idx = tid + pass * 256u;
            let b_k = idx / TILE_N;
            let b_n = idx % TILE_N;
            let g_col = col_base + b_n;
            let g_k = k_base + b_k;
            if (g_col < params.N && g_k < params.K) {
                tile_b[idx] = dequant_q8_0_block(g_col, block_idx, b_k);
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
