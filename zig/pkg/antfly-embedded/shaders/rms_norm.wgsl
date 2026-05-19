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

// RMSNorm GPU shader.
// out[i] = x[i] / sqrt(mean(x^2) + eps) * weight[i]
// Dispatch: one workgroup per row, 256 threads per workgroup.

struct Params {
    total_rows: u32,
    dim: u32,
    eps_bits: u32,  // f32 bitcasted to u32
    _pad: u32,
}

@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read> weight: array<f32>;
@group(0) @binding(2) var<storage, read_write> output: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

var<workgroup> shared_sum: array<f32, 256>;

@compute @workgroup_size(256)
fn rms_norm(
    @builtin(workgroup_id) wg: vec3<u32>,
    @builtin(local_invocation_id) lid: vec3<u32>,
) {
    let row = wg.x;
    if (row >= params.total_rows) { return; }

    let tid = lid.x;
    let dim = params.dim;
    let eps = bitcast<f32>(params.eps_bits);
    let row_start = row * dim;

    // Each thread computes partial sum of squares
    var sum_sq: f32 = 0.0;
    for (var i = tid; i < dim; i += 256u) {
        let val = input[row_start + i];
        sum_sq += val * val;
    }
    shared_sum[tid] = sum_sq;
    workgroupBarrier();

    // Tree reduction
    for (var stride = 128u; stride > 0u; stride >>= 1u) {
        if (tid < stride) {
            shared_sum[tid] += shared_sum[tid + stride];
        }
        workgroupBarrier();
    }

    let inv_rms = inverseSqrt(shared_sum[0] / f32(dim) + eps);

    // Apply normalization and weight
    for (var i = tid; i < dim; i += 256u) {
        output[row_start + i] = input[row_start + i] * inv_rms * weight[i];
    }
}
