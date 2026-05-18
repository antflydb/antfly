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

// Tiled matrix multiplication with B transposed and Q8_K dequantization.
// C[M,N] = A[M,K] @ dequant(B_q8_k)^T

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

fn read_f32(byte_offset: u32) -> f32 {
    return bitcast<f32>(B_raw[byte_offset / 4u]);
}

fn signed_i8(byte_value: u32) -> f32 {
    if (byte_value >= 128u) {
        return f32(byte_value) - 256.0;
    }
    return f32(byte_value);
}

fn dequant_q8_k(b_row: u32, k_abs: u32) -> f32 {
    let blocks_per_row = params.K / 256u;
    let block_idx = k_abs / 256u;
    let in_block = k_abs % 256u;
    let block_byte = (b_row * blocks_per_row + block_idx) * 292u;
    let d = read_f32(block_byte);
    let q = signed_i8(read_byte(block_byte + 4u + in_block));
    return d * q;
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_q8_k(
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

    for (var t = 0u; t < num_tiles; t++) {
        let k_base = t * TILE_K;
        for (var pass = 0u; pass < 2u; pass++) {
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

        for (var pass = 0u; pass < 2u; pass++) {
            let idx = tid + pass * 256u;
            let b_k = idx / TILE_N;
            let b_n = idx % TILE_N;
            let g_col = col_base + b_n;
            let g_k = k_base + b_k;
            if (g_col < params.N && g_k < params.K) {
                tile_b[idx] = dequant_q8_k(g_col, g_k);
            } else {
                tile_b[idx] = 0.0;
            }
        }

        workgroupBarrier();
        for (var i = 0u; i < TILE_K; i++) {
            sum += tile_a[local_row * TILE_K + i] * tile_b[i * TILE_N + local_col];
        }
        workgroupBarrier();
    }

    if (row < params.M && col < params.N) {
        C[row * params.N + col] = sum;
    }
}
