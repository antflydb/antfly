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
    a_len: u32,
    b_len: u32,
    _pad0: u32,
};

@group(0) @binding(0) var<storage, read> A: array<f32>;
@group(0) @binding(1) var<storage, read> B: array<f32>;
@group(0) @binding(2) var<storage, read_write> Out: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

fn a_at(i: u32) -> f32 {
    return A[i % params.a_len];
}

fn b_at(i: u32) -> f32 {
    return B[i % params.b_len];
}

@compute @workgroup_size(256)
fn add_broadcast(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    Out[i] = a_at(i) + b_at(i);
}

@compute @workgroup_size(256)
fn mul(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    Out[i] = a_at(i) * b_at(i);
}

@compute @workgroup_size(256)
fn sub(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    Out[i] = a_at(i) - b_at(i);
}

@compute @workgroup_size(256)
fn div(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    Out[i] = a_at(i) / b_at(i);
}

@compute @workgroup_size(256)
fn less_than(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    Out[i] = select(0.0, 1.0, a_at(i) < b_at(i));
}
