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

// Fused GQA cached attention with polar4-packed keys and f32 values.
//
// Q layout: [batch*q_len, num_heads*head_dim] f32
// K layout: [batch*kv_len, (num_kv_heads*head_dim)/2] packed polar4 bytes
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
@group(0) @binding(1) var<storage, read> K_polar4_words: array<u32>;
@group(0) @binding(2) var<storage, read> V: array<f32>;
@group(0) @binding(3) var<storage, read_write> out: array<f32>;
@group(0) @binding(4) var<uniform> params: Params;

const WG: u32 = 256u;
const MAX_KV: u32 = 2048u;

var<workgroup> scores: array<f32, MAX_KV>;
var<workgroup> reduce_buf: array<f32, WG>;

fn decode_polar4_code(code: u32) -> f32 {
    return f32(code & 0x0fu) / 7.5 - 1.0;
}

fn load_polar4_byte(byte_index: u32) -> u32 {
    let word = K_polar4_words[byte_index >> 2u];
    let shift = (byte_index & 3u) * 8u;
    return (word >> shift) & 0xffu;
}

fn load_polar4_value(byte_index: u32, high_nibble: bool) -> f32 {
    let packed_byte = load_polar4_byte(byte_index);
    let code = select(packed_byte & 0x0fu, (packed_byte >> 4u) & 0x0fu, high_nibble);
    return decode_polar4_code(code);
}

@compute @workgroup_size(WG)
fn gqa_cached_attention_polar4(
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
    let K_ROW_BYTES = H_kv >> 1u;
    let D = params.head_dim;
    let heads_per_group = params.num_heads / params.num_kv_heads;
    let kv_h = h / heads_per_group;
    let kv_head_off = kv_h * D;

    let abs_pos = KV_LEN - Q_LEN + qi;
    let q_base = (b * Q_LEN + qi) * H_q + h * D;

    var local_max: f32 = -3.402823466e+38;

    for (var ki = tid; ki < KV_LEN; ki += WG) {
        if (ki > abs_pos) {
            scores[ki] = -3.402823466e+38;
            continue;
        }

        var dot: f32 = 0.0;
        let k_row_byte_base = (b * KV_LEN + ki) * K_ROW_BYTES;
        for (var d: u32 = 0u; d < D; d++) {
            let value_index = kv_head_off + d;
            let k_byte_index = k_row_byte_base + (value_index >> 1u);
            let k_value = load_polar4_value(k_byte_index, (value_index & 1u) != 0u);
            dot += Q[q_base + d] * k_value;
        }
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
