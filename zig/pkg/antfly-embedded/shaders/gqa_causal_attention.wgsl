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

// Fused causal GQA (grouped-query) attention.
// out = softmax(Q @ K^T / sqrt(head_dim) + causal_mask) @ V
//
// GQA: num_heads Q heads share num_kv_heads K/V heads.
//   kv_h = h / heads_per_group
// Q layout: [batch*seq_len, num_heads*head_dim]      stride = num_heads*head_dim
// K/V layout: [batch*seq_len, num_kv_heads*head_dim]  stride = num_kv_heads*head_dim
// Output layout: same as Q.
//
// Each workgroup computes one output row for one (batch, head, query_position).
// Dispatch: (num_heads, batch, seq_len) workgroups.
// Max seq_len: 512 (limited by workgroup shared memory).

struct Params {
    seq_len: u32,
    num_heads: u32,
    num_kv_heads: u32,
    head_dim: u32,
    scale: f32,       // 1.0 / sqrt(head_dim), precomputed on host
    _pad: u32,
};

@group(0) @binding(0) var<storage, read> Q: array<f32>;
@group(0) @binding(1) var<storage, read> K: array<f32>;
@group(0) @binding(2) var<storage, read> V: array<f32>;
@group(0) @binding(3) var<storage, read_write> out: array<f32>;
@group(0) @binding(4) var<uniform> params: Params;

const WG: u32 = 256u;
const MAX_SEQ: u32 = 512u;

var<workgroup> scores: array<f32, MAX_SEQ>;
var<workgroup> reduce_buf: array<f32, WG>;

@compute @workgroup_size(WG)
fn gqa_causal_attention(
    @builtin(workgroup_id) wg_id: vec3<u32>,
    @builtin(local_invocation_index) tid: u32,
) {
    let h = wg_id.x;      // Q head index
    let b = wg_id.y;      // batch index
    let qi = wg_id.z;     // query position

    let S = params.seq_len;
    let H_q = params.num_heads * params.head_dim;
    let H_kv = params.num_kv_heads * params.head_dim;
    let D = params.head_dim;
    let heads_per_group = params.num_heads / params.num_kv_heads;
    let kv_h = h / heads_per_group;

    let q_base = (b * S + qi) * H_q + h * D;
    let kv_head_off = kv_h * D;

    // ---- Step 1: scores[ki] = dot(Q[qi,:], K[ki,:]) * scale, causal mask ----
    var local_max: f32 = -3.402823466e+38;

    for (var ki = tid; ki < S; ki += WG) {
        if (ki > qi) {
            scores[ki] = -3.402823466e+38;
            continue;
        }

        var dot: f32 = 0.0;
        let k_base = (b * S + ki) * H_kv + kv_head_off;
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
    for (var ki = tid; ki < S; ki += WG) {
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
    for (var ki = tid; ki < S; ki += WG) {
        scores[ki] *= inv_sum;
    }
    workgroupBarrier();

    // ---- Step 6: output[d] = sum_ki scores[ki] * V[ki, d] ----
    let out_base = (b * S + qi) * H_q + h * D;
    for (var d = tid; d < D; d += WG) {
        var acc: f32 = 0.0;
        for (var vi: u32 = 0u; vi < S; vi++) {
            acc += scores[vi] * V[(b * S + vi) * H_kv + kv_head_off + d];
        }
        out[out_base + d] = acc;
    }
}
