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
    out_len: u32,
    out_rank: u32,
    in_rank: u32,
    _pad0: u32,
    target_shape0: vec4<u32>,
    target_shape1: vec4<u32>,
    input_shape0: vec4<u32>,
    input_shape1: vec4<u32>,
    axes0: vec4<u32>,
    axes1: vec4<u32>,
    out_strides0: vec4<u32>,
    out_strides1: vec4<u32>,
    in_strides0: vec4<u32>,
    in_strides1: vec4<u32>,
};

@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read> unused: array<f32>;
@group(0) @binding(2) var<storage, read_write> output: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

fn get4(lo: vec4<u32>, hi: vec4<u32>, idx: u32) -> u32 {
    switch idx {
        case 0u: { return lo.x; }
        case 1u: { return lo.y; }
        case 2u: { return lo.z; }
        case 3u: { return lo.w; }
        case 4u: { return hi.x; }
        case 5u: { return hi.y; }
        case 6u: { return hi.z; }
        default: { return hi.w; }
    }
}

fn target_shape(idx: u32) -> u32 {
    return get4(params.target_shape0, params.target_shape1, idx);
}

fn input_shape(idx: u32) -> u32 {
    return get4(params.input_shape0, params.input_shape1, idx);
}

fn axis(idx: u32) -> u32 {
    return get4(params.axes0, params.axes1, idx);
}

fn out_stride(idx: u32) -> u32 {
    return get4(params.out_strides0, params.out_strides1, idx);
}

fn in_stride(idx: u32) -> u32 {
    return get4(params.in_strides0, params.in_strides1, idx);
}

@compute @workgroup_size(256)
fn broadcast_in_dim(@builtin(global_invocation_id) gid: vec3<u32>) {
    let flat_out = gid.x;
    if (flat_out >= params.out_len) { return; }

    var remaining = flat_out;
    var flat_in = 0u;
    for (var out_d = 0u; out_d < params.out_rank; out_d = out_d + 1u) {
        let stride = out_stride(out_d);
        let coord = remaining / stride;
        remaining = remaining % stride;

        for (var in_d = 0u; in_d < params.in_rank; in_d = in_d + 1u) {
            if (axis(in_d) == out_d) {
                if (input_shape(in_d) > 1u) {
                    flat_in = flat_in + coord * in_stride(in_d);
                }
            }
        }
        _ = target_shape(out_d);
    }

    output[flat_out] = input[flat_in];
}
