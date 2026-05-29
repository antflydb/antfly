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

// Fused GQA cached attention with turbo3-packed keys and f32 values.
//
// Q layout: [batch*q_len, num_heads*head_dim] f32
// K layout: [batch*kv_len, ceil(num_kv_heads*head_dim*3/8) + ceil(num_kv_heads*32/8)]
// packed 3-bit codes followed by one 32-bit residual sketch per KV head.
// V layout: [batch*kv_len, num_kv_heads*head_dim] f32
// Output: [batch*q_len, num_heads*head_dim] f32

struct Params {
    q_len: u32,
    kv_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    scale: f32,
};

@group(0) @binding(0) var<storage, read> Q: array<f32>;
@group(0) @binding(1) var<storage, read> K_turbo3_words: array<u32>;
@group(0) @binding(2) var<storage, read> V: array<f32>;
@group(0) @binding(3) var<storage, read_write> out: array<f32>;
@group(0) @binding(4) var<uniform> params: Params;

const WG: u32 = 256u;
const MAX_KV: u32 = 2048u;
const TURBO3_RESIDUAL_BITS_PER_HEAD: u32 = 32u;
const TURBO3_RESIDUAL_SCALE: f32 = 0.125;

var<workgroup> scores: array<f32, MAX_KV>;
var<workgroup> reduce_buf: array<f32, WG>;

fn decode_turbo3_code(code: u32) -> f32 {
    return f32(code & 0x07u) / 3.5 - 1.0;
}

fn load_turbo3_byte(byte_index: u32) -> u32 {
    let word = K_turbo3_words[byte_index >> 2u];
    let shift = (byte_index & 3u) * 8u;
    return (word >> shift) & 0xffu;
}

fn load_turbo3_code(row_byte_base: u32, value_index: u32) -> u32 {
    let bit_offset = value_index * 3u;
    let byte_index = row_byte_base + (bit_offset >> 3u);
    let shift = bit_offset & 7u;
    var bits = load_turbo3_byte(byte_index) >> shift;
    if (shift > 5u) {
        bits |= load_turbo3_byte(byte_index + 1u) << (8u - shift);
    }
    return bits & 0x07u;
}

fn load_residual_sign(residual_byte_base: u32, kv_h: u32, projection: u32) -> f32 {
    let bit_index = kv_h * TURBO3_RESIDUAL_BITS_PER_HEAD + projection;
    let bit = (load_turbo3_byte(residual_byte_base + (bit_index >> 3u)) >> (bit_index & 7u)) & 1u;
    return select(-1.0, 1.0, bit != 0u);
}

fn mul32_hi(a: u32, b: u32) -> u32 {
    let a_lo = a & 0xffffu;
    let a_hi = a >> 16u;
    let b_lo = b & 0xffffu;
    let b_hi = b >> 16u;
    let p0 = a_lo * b_lo;
    let p1 = a_lo * b_hi;
    let p2 = a_hi * b_lo;
    let p3 = a_hi * b_hi;
    let carry = ((p0 >> 16u) + (p1 & 0xffffu) + (p2 & 0xffffu)) >> 16u;
    return p3 + (p1 >> 16u) + (p2 >> 16u) + carry;
}

fn mul_u64_const(x: vec2<u32>, c_lo: u32, c_hi: u32) -> vec2<u32> {
    let lo = x.x * c_lo;
    let hi = mul32_hi(x.x, c_lo) + x.x * c_hi + x.y * c_lo;
    return vec2<u32>(lo, hi);
}

fn shr_u64(x: vec2<u32>, n: u32) -> vec2<u32> {
    if (n == 0u) {
        return x;
    }
    if (n < 32u) {
        return vec2<u32>((x.x >> n) | (x.y << (32u - n)), x.y >> n);
    }
    if (n == 32u) {
        return vec2<u32>(x.y, 0u);
    }
    return vec2<u32>(x.y >> (n - 32u), 0u);
}

fn xor_u64(a: vec2<u32>, b: vec2<u32>) -> vec2<u32> {
    return vec2<u32>(a.x ^ b.x, a.y ^ b.y);
}

fn random_sign(head: u32, projection: u32, dim: u32) -> f32 {
    var x = mul_u64_const(vec2<u32>(head + 1u, 0u), 0x7f4a7c15u, 0x9e3779b9u);
    x = xor_u64(x, mul_u64_const(vec2<u32>(projection + 1u, 0u), 0x1ce4e5b9u, 0xbf58476du));
    x = xor_u64(x, mul_u64_const(vec2<u32>(dim + 1u, 0u), 0x133111ebu, 0x94d049bbu));
    x = xor_u64(x, shr_u64(x, 30u));
    x = mul_u64_const(x, 0x1ce4e5b9u, 0xbf58476du);
    x = xor_u64(x, shr_u64(x, 27u));
    x = mul_u64_const(x, 0x133111ebu, 0x94d049bbu);
    x = xor_u64(x, shr_u64(x, 31u));
    return select(-1.0, 1.0, (x.x & 1u) == 0u);
}

@compute @workgroup_size(WG)
fn gqa_cached_attention_turbo3(
    @builtin(workgroup_id) wg_id: vec3<u32>,
    @builtin(local_invocation_index) tid: u32,
) {
    let h = wg_id.x;
    let b = wg_id.y;
    let qi = wg_id.z;

    let Q_LEN = params.q_len;
    let KV_LEN = params.kv_len;
    let H_q = params.num_heads * params.head_dim;
    let H_kv = params.num_kv_heads * params.head_dim;
    let BASE_K_ROW_BYTES = (H_kv * 3u + 7u) >> 3u;
    let RESIDUAL_K_ROW_BYTES = (params.num_kv_heads * TURBO3_RESIDUAL_BITS_PER_HEAD + 7u) >> 3u;
    let K_ROW_BYTES = BASE_K_ROW_BYTES + RESIDUAL_K_ROW_BYTES;
    let D = params.head_dim;
    let heads_per_group = params.num_heads / params.num_kv_heads;
    let kv_h = h / heads_per_group;
    let kv_head_off = kv_h * D;

    let abs_pos = KV_LEN - Q_LEN + qi;
    let q_base = (b * Q_LEN + qi) * H_q + h * D;

    var projected_query: array<f32, 32>;
    for (var projection: u32 = 0u; projection < TURBO3_RESIDUAL_BITS_PER_HEAD; projection++) {
        var projection_acc: f32 = 0.0;
        for (var d: u32 = 0u; d < D; d++) {
            projection_acc += random_sign(kv_h, projection, d) * Q[q_base + d];
        }
        projected_query[projection] = projection_acc;
    }

    var local_max: f32 = -3.402823466e+38;

    for (var ki = tid; ki < KV_LEN; ki += WG) {
        if (ki > abs_pos) {
            scores[ki] = -3.402823466e+38;
            continue;
        }

        let k_row_byte_base = (b * KV_LEN + ki) * K_ROW_BYTES;
        let residual_byte_base = k_row_byte_base + BASE_K_ROW_BYTES;
        var base_dot: f32 = 0.0;
        for (var d: u32 = 0u; d < D; d++) {
            let value_index = kv_head_off + d;
            let k_value = decode_turbo3_code(load_turbo3_code(k_row_byte_base, value_index));
            base_dot += Q[q_base + d] * k_value;
        }

        var residual_dot: f32 = 0.0;
        for (var projection: u32 = 0u; projection < TURBO3_RESIDUAL_BITS_PER_HEAD; projection++) {
            residual_dot += load_residual_sign(residual_byte_base, kv_h, projection) * projected_query[projection];
        }
        residual_dot /= f32(TURBO3_RESIDUAL_BITS_PER_HEAD);

        var dot = base_dot + TURBO3_RESIDUAL_SCALE * residual_dot;
        dot *= params.scale;

        scores[ki] = dot;
        local_max = max(local_max, dot);
    }

    reduce_buf[tid] = local_max;
    workgroupBarrier();

    for (var s = WG >> 1u; s > 0u; s >>= 1u) {
        if (tid < s) {
            reduce_buf[tid] = max(reduce_buf[tid], reduce_buf[tid + s]);
        }
        workgroupBarrier();
    }
    let row_max = reduce_buf[0];

    var local_sum: f32 = 0.0;
    for (var ki = tid; ki < KV_LEN; ki += WG) {
        let e = exp(scores[ki] - row_max);
        scores[ki] = e;
        local_sum += e;
    }

    reduce_buf[tid] = local_sum;
    workgroupBarrier();

    for (var s = WG >> 1u; s > 0u; s >>= 1u) {
        if (tid < s) {
            reduce_buf[tid] += reduce_buf[tid + s];
        }
        workgroupBarrier();
    }
    let inv_sum = 1.0 / max(reduce_buf[0], 1e-12);

    for (var ki = tid; ki < KV_LEN; ki += WG) {
        scores[ki] *= inv_sum;
    }
    workgroupBarrier();

    let out_base = (b * Q_LEN + qi) * H_q + h * D;
    for (var d = tid; d < D; d += WG) {
        var acc: f32 = 0.0;
        for (var vi: u32 = 0u; vi < KV_LEN; vi++) {
            acc += scores[vi] * V[(b * KV_LEN + vi) * H_kv + kv_head_off + d];
        }
        out[out_base + d] = acc;
    }
}
