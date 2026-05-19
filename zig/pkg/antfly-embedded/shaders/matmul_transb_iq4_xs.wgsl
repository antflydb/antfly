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

// Tiled matrix multiplication with B transposed and IQ4_XS dequantization.
// C[M,N] = A[M,K] @ dequant(B_iq4_xs)^T
//
// B is quantized in IQ4_XS format: blocks of 256 values.
// Each block = 2 bytes f16 d + 2 bytes high scale bits
// + 4 bytes low scale nibbles + 128 bytes q4 table indices.

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

fn read_u16(byte_offset: u32) -> u32 {
    return read_byte(byte_offset) | (read_byte(byte_offset + 1u) << 8u);
}

fn read_f16(byte_offset: u32) -> f32 {
    return unpack2x16float(read_u16(byte_offset)).x;
}

fn iq4_nl_value(nibble: u32) -> f32 {
    var value: f32;
    switch (nibble) {
        case 0u: { value = -127.0; }
        case 1u: { value = -104.0; }
        case 2u: { value = -83.0; }
        case 3u: { value = -65.0; }
        case 4u: { value = -49.0; }
        case 5u: { value = -35.0; }
        case 6u: { value = -22.0; }
        case 7u: { value = -10.0; }
        case 8u: { value = 1.0; }
        case 9u: { value = 13.0; }
        case 10u: { value = 25.0; }
        case 11u: { value = 38.0; }
        case 12u: { value = 53.0; }
        case 13u: { value = 69.0; }
        case 14u: { value = 89.0; }
        default: { value = 113.0; }
    }
    return value;
}

fn dequant_iq4_xs(b_row: u32, k_abs: u32) -> f32 {
    let blocks_per_row = params.K / 256u;
    let block_idx = k_abs / 256u;
    let in_block = k_abs % 256u;
    let block_byte = (b_row * blocks_per_row + block_idx) * 136u;
    let sub = in_block / 32u;
    let elem = in_block % 32u;

    let d = read_f16(block_byte);
    let scales_h = read_u16(block_byte + 2u);
    let scales_l = read_byte(block_byte + 4u + sub / 2u);
    let low = (scales_l >> (4u * (sub % 2u))) & 0x0Fu;
    let high = (scales_h >> (2u * sub)) & 0x03u;
    let dl = d * (f32(low | (high << 4u)) - 32.0);

    let packed = read_byte(block_byte + 8u + sub * 16u + (elem % 16u));
    let nibble = select((packed >> 4u) & 0x0Fu, packed & 0x0Fu, elem < 16u);
    return dl * iq4_nl_value(nibble);
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_iq4_xs(
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

        for (var pass: u32 = 0u; pass < 2u; pass++) {
            let idx = tid + pass * 256u;
            let b_k = idx / TILE_N;
            let b_n = idx % TILE_N;
            let g_col = col_base + b_n;
            let g_k = k_base + b_k;
            if (g_col < params.N && g_k < params.K) {
                tile_b[idx] = dequant_iq4_xs(g_col, g_k);
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
