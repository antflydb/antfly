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

// Fused cross-attention for encoder-decoder models (Whisper, Florence-2).
// out = softmax(Q @ K^T / sqrt(head_dim) + enc_mask) @ V
//
// Asymmetric: Q from decoder [batch, dec_seq, H], K/V from encoder [batch, enc_seq, H].
// Score matrix is [dec_seq, enc_seq] per head.
// Encoder mask: [batch * enc_seq] as u32 (0 = masked/pad, 1 = attend).
//
// Each workgroup computes one output row for one (batch, head, query_position).
// Dispatch: (num_heads, batch, dec_seq) workgroups.
//
// Max enc_seq: 512 (limited by workgroup shared memory).

struct Params {
    dec_seq: u32,
    enc_seq: u32,
    num_heads: u32,
    head_dim: u32,
    scale: f32,       // 1.0 / sqrt(head_dim), precomputed on host
    _pad: u32,        // align to 24 bytes (6 x u32)
};

@group(0) @binding(0) var<storage, read> Q: array<f32>;
@group(0) @binding(1) var<storage, read> K: array<f32>;
@group(0) @binding(2) var<storage, read> V: array<f32>;
@group(0) @binding(3) var<storage, read> enc_mask: array<u32>;
@group(0) @binding(4) var<storage, read_write> out: array<f32>;
@group(0) @binding(5) var<uniform> params: Params;

const WG: u32 = 256u;
const MAX_SEQ: u32 = 512u;

var<workgroup> scores: array<f32, MAX_SEQ>;
var<workgroup> reduce_buf: array<f32, WG>;

@compute @workgroup_size(WG)
fn cross_attention(
    @builtin(workgroup_id) wg_id: vec3<u32>,
    @builtin(local_invocation_index) tid: u32,
) {
    let h = wg_id.x;      // head index
    let b = wg_id.y;      // batch index
    let qi = wg_id.z;     // query (decoder) position

    let DS = params.dec_seq;
    let ES = params.enc_seq;
    let H = params.num_heads;
    let D = params.head_dim;
    let q_stride = H * D;  // stride for Q/out (decoder layout)
    let k_stride = H * D;  // stride for K/V (encoder layout)

    let q_base = (b * DS + qi) * q_stride + h * D;

    // ---- Step 1: scores[ki] = dot(Q[qi,:], K[ki,:]) * scale + enc_mask ----
    var local_max: f32 = -3.402823466e+38;

    for (var ki = tid; ki < ES; ki += WG) {
        var dot: f32 = 0.0;
        let k_base = (b * ES + ki) * k_stride + h * D;
        for (var d: u32 = 0u; d < D; d++) {
            dot += Q[q_base + d] * K[k_base + d];
        }
        dot *= params.scale;

        // Apply encoder padding mask
        if (enc_mask[b * ES + ki] == 0u) {
            dot = -3.402823466e+38;
        }

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
    for (var ki = tid; ki < ES; ki += WG) {
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
    for (var ki = tid; ki < ES; ki += WG) {
        scores[ki] *= inv_sum;
    }
    workgroupBarrier();

    // ---- Step 6: output[d] = sum_ki scores[ki] * V[ki, d] ----
    let out_base = (b * DS + qi) * q_stride + h * D;
    for (var d = tid; d < D; d += WG) {
        var acc: f32 = 0.0;
        for (var vi: u32 = 0u; vi < ES; vi++) {
            acc += scores[vi] * V[(b * ES + vi) * k_stride + h * D + d];
        }
        out[out_base + d] = acc;
    }
}
