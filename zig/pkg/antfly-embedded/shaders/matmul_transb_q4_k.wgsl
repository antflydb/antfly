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

// Tiled matrix multiplication with B transposed and Q4_K dequantization.
// C[M,N] = A[M,K] @ dequant(B_q4k)^T
//
// B is quantized in Q4_K format: blocks of 256 values.
// Each block = 144 bytes:
//   [0:2]    d      - f16 scale
//   [2:4]    dmin   - f16 minimum scale
//   [4:16]   scales - 12 bytes, packed 6-bit scale+min per sub-block
//   [16:144] qs     - 128 bytes, low/high nibbles
//
// 8 sub-blocks of 32 values each.
// value = d * sc * q4 - dmin * mn

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

fn unpack_scale_min(block_byte: u32, sub: u32) -> vec2<f32> {
    let scales_off = block_byte + 4u;
    if (sub < 4u) {
        let sc = f32(read_byte(scales_off + sub) & 63u);
        let mn = f32(read_byte(scales_off + sub + 4u) & 63u);
        return vec2<f32>(sc, mn);
    } else {
        let sc_low = read_byte(scales_off + sub + 4u) & 0x0Fu;
        let sc_high = (read_byte(scales_off + sub - 4u) >> 6u) << 4u;
        let mn_low = read_byte(scales_off + sub + 4u) >> 4u;
        let mn_high = (read_byte(scales_off + sub) >> 6u) << 4u;
        return vec2<f32>(f32(sc_low | sc_high), f32(mn_low | mn_high));
    }
}

fn dequant_q4_k(b_row: u32, k_abs: u32) -> f32 {
    let blocks_per_row = params.K / 256u;
    let block_idx = k_abs / 256u;
    let in_block = k_abs % 256u;
    let block_byte = (b_row * blocks_per_row + block_idx) * 144u;

    let sub = in_block / 32u;
    let elem = in_block % 32u;

    let d = read_f16(block_byte);
    let dmin = read_f16(block_byte + 2u);
    let sc_mn = unpack_scale_min(block_byte, sub);
    let dsc = d * sc_mn.x;
    let dmn = dmin * sc_mn.y;

    let q_byte = read_byte(block_byte + 16u + (sub / 2u) * 32u + elem);
    var q: u32;
    if ((sub % 2u) == 0u) {
        q = q_byte & 0x0Fu;
    } else {
        q = q_byte >> 4u;
    }

    return dsc * f32(q) - dmn;
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_q4_k(
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
                tile_b[idx] = dequant_q4_k(g_col, g_k);
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
