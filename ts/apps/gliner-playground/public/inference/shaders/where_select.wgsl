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
    true_len: u32,
    false_len: u32,
    _pad0: u32,
};

@group(0) @binding(0) var<storage, read> Cond: array<f32>;
@group(0) @binding(1) var<storage, read> TrueValues: array<f32>;
@group(0) @binding(2) var<storage, read> FalseValues: array<f32>;
@group(0) @binding(3) var<storage, read_write> Out: array<f32>;
@group(0) @binding(4) var<uniform> params: Params;

@compute @workgroup_size(256)
fn where_select(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= params.len) { return; }
    let tv = TrueValues[i % params.true_len];
    let fv = FalseValues[i % params.false_len];
    Out[i] = select(fv, tv, Cond[i] != 0.0);
}
