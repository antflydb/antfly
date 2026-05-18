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

// Tiled matrix multiplication with B transposed and Q5_K dequantization.
// C[M,N] = A[M,K] @ dequant(B_q5k)^T
//
// B is quantized in Q5_K format: blocks of 256 values.
// Each block = 176 bytes:
//   [0:2]    d      — f16 scale
//   [2:4]    dmin   — f16 minimum scale
//   [4:16]   scales — 12 bytes, packed 6-bit scale+min per sub-block
//   [16:48]  qh     — 32 bytes, high bits (1 bit per value, 256 bits)
//   [48:176] ql     — 128 bytes, low 4-bit nibbles
//
// 8 sub-blocks of 32 values each.
// value = d * sc * (ql_low4 | (qh_bit << 4)) - dmin * mn
//
// Uses 16x16x32 tiling. Each 256-value block spans 8 K-tiles.

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

// Unpack 6-bit scale and min for sub-block `sub` from the 12-byte scales array.
// Port of unpackQ4KScaleMins from quant_codec.zig.
//
// Layout of 12-byte scales array (indices 0..11):
//   bytes 0-3: low 6 bits = scale for sub-blocks 0-3
//   bytes 4-7: low 6 bits = min for sub-blocks 0-3
//   bytes 8-11: packed high bits for sub-blocks 4-7
//     For sub 4-7: scale = (scales[sub+4] & 0x0F) | ((scales[sub-4] >> 6) << 4)
//                  min   = (scales[sub+4] >> 4)    | ((scales[sub]   >> 6) << 4)
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

fn dequant_q5_k(b_row: u32, k_abs: u32) -> f32 {
    let blocks_per_row = params.K / 256u;
    let block_idx = k_abs / 256u;
    let in_block = k_abs % 256u;            // 0..255
    let block_byte = (b_row * blocks_per_row + block_idx) * 176u;

    // Sub-block (0..7), element within sub-block (0..31)
    let sub = in_block / 32u;
    let elem = in_block % 32u;

    // Read d and dmin (f16)
    let d = read_f16(block_byte);
    let dmin = read_f16(block_byte + 2u);

    // Unpack 6-bit scale and min for this sub-block
    let sc_mn = unpack_scale_min(block_byte, sub);
    let dsc = d * sc_mn.x;
    let dmn = dmin * sc_mn.y;

    // ql: 128 bytes at offset 48. 4 chunks of 32 bytes each.
    // chunk = sub / 2, is_high = sub % 2
    // For is_high=0: low nibble. For is_high=1: high nibble.
    let chunk = sub / 2u;
    let is_high = sub % 2u;
    let ql_off = block_byte + 48u + chunk * 32u + elem;
    let ql_byte = read_byte(ql_off);
    var low: u32;
    if (is_high == 0u) {
        low = ql_byte & 0x0Fu;
    } else {
        low = ql_byte >> 4u;
    }

    // qh: 32 bytes at offset 16. One bit per value.
    // Bit `sub` of byte qh[elem] gives the high bit.
    let qh_byte = read_byte(block_byte + 16u + elem);
    let high_bit = (qh_byte >> sub) & 1u;

    // 5-bit value = low4 + high_bit * 16
    let q = low + high_bit * 16u;
    return dsc * f32(q) - dmn;
}

@compute @workgroup_size(TILE_M, TILE_N)
fn matmul_transb_q5_k(
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
                tile_b[idx] = dequant_q5_k(g_col, g_k);
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
