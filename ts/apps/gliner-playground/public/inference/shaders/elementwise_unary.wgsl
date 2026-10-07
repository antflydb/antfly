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
    len: u32,
    _pad0: u32,
    _pad1: u32,
    _pad2: u32,
};

@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read> unused: array<f32>;
@group(0) @binding(2) var<storage, read_write> output: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

fn erf_approx(x: f32) -> f32 {
    let sign = select(-1.0, 1.0, x >= 0.0);
    let ax = abs(x);
    let t = 1.0 / (1.0 + 0.3275911 * ax);
    let a1 = 0.254829592;
    let a2 = -0.284496736;
    let a3 = 1.421413741;
    let a4 = -1.453152027;
    let a5 = 1.061405429;
    let poly = (((((a5 * t + a4) * t) + a3) * t + a2) * t + a1) * t;
    return sign * (1.0 - poly * exp(-ax * ax));
}

fn tanh_approx(x: f32) -> f32 {
    let e2x = exp(2.0 * x);
    return (e2x - 1.0) / (e2x + 1.0);
}

@compute @workgroup_size(256)
fn neg(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = -input[i];
}

@compute @workgroup_size(256)
fn sqrt_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = sqrt(input[i]);
}

@compute @workgroup_size(256)
fn rsqrt(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = inverseSqrt(input[i]);
}

@compute @workgroup_size(256)
fn exp_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = exp(input[i]);
}

@compute @workgroup_size(256)
fn log_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = log(input[i]);
}

@compute @workgroup_size(256)
fn sin_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = sin(input[i]);
}

@compute @workgroup_size(256)
fn cos_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = cos(input[i]);
}

@compute @workgroup_size(256)
fn tanh_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = tanh_approx(input[i]);
}

@compute @workgroup_size(256)
fn abs_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = abs(input[i]);
}

@compute @workgroup_size(256)
fn erf_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    output[i] = erf_approx(input[i]);
}
