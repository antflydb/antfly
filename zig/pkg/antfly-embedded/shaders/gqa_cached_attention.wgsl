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

// Fused causal GQA attention with asymmetric Q/KV lengths.
// out = softmax(Q @ K^T / sqrt(head_dim) + causal_mask) @ V
//
// Used with GPU-resident KV cache: Q has q_len positions (often 1 for decode),
// K/V have kv_len positions (full cached history).
//
// Query at relative position qi (0..q_len) has absolute position
// (kv_len - q_len + qi). Causal mask: can attend to K positions 0..abs_pos.
//
// Q layout: [batch*q_len, num_heads*head_dim]
// K/V layout: [batch*kv_len, num_kv_heads*head_dim]
// Output: [batch*q_len, num_heads*head_dim]
//
// Dispatch: (num_heads, batch, q_len) workgroups.
// Max kv_len: 2048 (limited by workgroup shared memory).

struct Params {
    q_len: u32,
    kv_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    scale: f32,
};

@group(0) @binding(0) var<storage, read> Q: array<f32>;
@group(0) @binding(1) var<storage, read> K: array<f32>;
@group(0) @binding(2) var<storage, read> V: array<f32>;
@group(0) @binding(3) var<storage, read_write> out: array<f32>;
@group(0) @binding(4) var<uniform> params: Params;

const WG: u32 = 256u;
const MAX_KV: u32 = 2048u;

var<workgroup> scores: array<f32, MAX_KV>;
var<workgroup> reduce_buf: array<f32, WG>;

@compute @workgroup_size(WG)
fn gqa_cached_attention(
    @builtin(workgroup_id) wg_id: vec3<u32>,
    @builtin(local_invocation_index) tid: u32,
) {
    let h = wg_id.x;      // Q head index
    let b = wg_id.y;      // batch index
    let qi = wg_id.z;     // query position (0..q_len)

    let Q_LEN = params.q_len;
    let KV_LEN = params.kv_len;
    let H_q = params.num_heads * params.head_dim;
    let H_kv = params.num_kv_heads * params.head_dim;
    let D = params.head_dim;
    let heads_per_group = params.num_heads / params.num_kv_heads;
    let kv_h = h / heads_per_group;
    let kv_head_off = kv_h * D;

    // Absolute position of this query in the full KV sequence
    let abs_pos = KV_LEN - Q_LEN + qi;

    let q_base = (b * Q_LEN + qi) * H_q + h * D;

    // ---- Step 1: scores[ki] = dot(Q[qi,:], K[ki,:]) * scale, causal mask ----
    var local_max: f32 = -3.402823466e+38;

    for (var ki = tid; ki < KV_LEN; ki += WG) {
        if (ki > abs_pos) {
            scores[ki] = -3.402823466e+38;
            continue;
        }

        var dot: f32 = 0.0;
        let k_base = (b * KV_LEN + ki) * H_kv + kv_head_off;
        for (var d: u32 = 0u; d < D; d++) {
            dot += Q[q_base + d] * K[k_base + d];
        }
        dot *= params.scale;

        scores[ki] = dot;
        local_max = max(local_max, dot);
    }

    // ---- Step 2: parallel max reduction ----
    reduce_buf[tid] = local_max;
    workgroupBarrier();

    for (var s = WG >> 1u; s > 0u; s >>= 1u) {
        if (tid < s) {
            reduce_buf[tid] = max(reduce_buf[tid], reduce_buf[tid + s]);
        }
        workgroupBarrier();
    }
    let row_max = reduce_buf[0];

    // ---- Step 3: exp(score - max) and partial sum ----
    var local_sum: f32 = 0.0;
    for (var ki = tid; ki < KV_LEN; ki += WG) {
        let e = exp(scores[ki] - row_max);
        scores[ki] = e;
        local_sum += e;
    }

    // ---- Step 4: parallel sum reduction ----
    reduce_buf[tid] = local_sum;
    workgroupBarrier();

    for (var s = WG >> 1u; s > 0u; s >>= 1u) {
        if (tid < s) {
            reduce_buf[tid] += reduce_buf[tid + s];
        }
        workgroupBarrier();
    }
    let inv_sum = 1.0 / max(reduce_buf[0], 1e-12);

    // ---- Step 5: normalize softmax scores ----
    for (var ki = tid; ki < KV_LEN; ki += WG) {
        scores[ki] *= inv_sum;
    }
    workgroupBarrier();

    // ---- Step 6: output[d] = sum_ki scores[ki] * V[ki, d] ----
    let out_base = (b * Q_LEN + qi) * H_q + h * D;
    for (var d = tid; d < D; d += WG) {
        var acc: f32 = 0.0;
        for (var vi: u32 = 0u; vi < KV_LEN; vi++) {
            acc += scores[vi] * V[(b * KV_LEN + vi) * H_kv + kv_head_off + d];
        }
        out[out_base + d] = acc;
    }
}
