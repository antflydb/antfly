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

// LayerNorm GPU shader.
// out[i] = (x[i] - mean) / sqrt(var + eps) * gamma[i] + beta[i]
// Dispatch: one workgroup per row, 256 threads per workgroup.

struct Params {
    total_rows: u32,
    dim: u32,
    eps_bits: u32,  // f32 bitcasted to u32
    _pad: u32,
}

@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read> gamma: array<f32>;
@group(0) @binding(2) var<storage, read> beta: array<f32>;
@group(0) @binding(3) var<storage, read_write> output: array<f32>;
@group(0) @binding(4) var<uniform> params: Params;

var<workgroup> shared_sum: array<f32, 256>;
var<workgroup> shared_sq: array<f32, 256>;

@compute @workgroup_size(256)
fn layer_norm(
    @builtin(workgroup_id) wg: vec3<u32>,
    @builtin(local_invocation_id) lid: vec3<u32>,
) {
    let row = wg.x;
    if (row >= params.total_rows) { return; }

    let tid = lid.x;
    let dim = params.dim;
    let eps = bitcast<f32>(params.eps_bits);
    let row_start = row * dim;

    // Compute partial sum and sum of squares
    var local_sum: f32 = 0.0;
    var local_sq: f32 = 0.0;
    for (var i = tid; i < dim; i += 256u) {
        let val = input[row_start + i];
        local_sum += val;
        local_sq += val * val;
    }
    shared_sum[tid] = local_sum;
    shared_sq[tid] = local_sq;
    workgroupBarrier();

    // Tree reduction for both
    for (var stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            shared_sum[tid] += shared_sum[tid + stride];
            shared_sq[tid] += shared_sq[tid + stride];
        }
        workgroupBarrier();
    }

    let mean = shared_sum[0] / f32(dim);
    let variance = shared_sq[0] / f32(dim) - mean * mean;
    let inv_std = inverseSqrt(variance + eps);

    // Apply normalization, scale, and shift
    for (var i = tid; i < dim; i += 256u) {
        output[row_start + i] = (input[row_start + i] - mean) * inv_std * gamma[i] + beta[i];
    }
}
