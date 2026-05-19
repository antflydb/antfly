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

struct Params {
    rows: u32,
    dim: u32,
    _pad0: u32,
    _pad1: u32,
};

@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read> unused: array<f32>;
@group(0) @binding(2) var<storage, read_write> output: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

@compute @workgroup_size(1)
fn softmax(@builtin(global_invocation_id) gid: vec3<u32>) {
    let row = gid.x;
    if (row >= params.rows) { return; }
    let base = row * params.dim;
    var max_val = -3.4028234663852886e38;
    for (var j: u32 = 0u; j < params.dim; j = j + 1u) {
        max_val = max(max_val, input[base + j]);
    }
    var sum = 0.0;
    for (var j: u32 = 0u; j < params.dim; j = j + 1u) {
        let v = exp(input[base + j] - max_val);
        output[base + j] = v;
        sum = sum + v;
    }
    let inv_sum = 1.0 / sum;
    for (var j: u32 = 0u; j < params.dim; j = j + 1u) {
        output[base + j] = output[base + j] * inv_sum;
    }
}

@compute @workgroup_size(1)
fn log_softmax(@builtin(global_invocation_id) gid: vec3<u32>) {
    let row = gid.x;
    if (row >= params.rows) { return; }
    let base = row * params.dim;
    var max_val = -3.4028234663852886e38;
    for (var j: u32 = 0u; j < params.dim; j = j + 1u) {
        max_val = max(max_val, input[base + j]);
    }
    var sum = 0.0;
    for (var j: u32 = 0u; j < params.dim; j = j + 1u) {
        sum = sum + exp(input[base + j] - max_val);
    }
    let lse = log(sum);
    for (var j: u32 = 0u; j < params.dim; j = j + 1u) {
        output[base + j] = (input[base + j] - max_val) - lse;
    }
}
