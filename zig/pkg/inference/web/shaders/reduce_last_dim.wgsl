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
    reduce_count: u32,
    in_rank: u32,
    out_rank: u32,
    input_shape0: vec4<u32>,
    input_shape1: vec4<u32>,
    out_shape0: vec4<u32>,
    out_shape1: vec4<u32>,
    reduced0: vec4<u32>,
    reduced1: vec4<u32>,
    in_strides0: vec4<u32>,
    in_strides1: vec4<u32>,
    out_strides0: vec4<u32>,
    out_strides1: vec4<u32>,
    kept_axes0: vec4<u32>,
    kept_axes1: vec4<u32>,
    reduced_axes0: vec4<u32>,
    reduced_axes1: vec4<u32>,
};

@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read> unused: array<f32>;
@group(0) @binding(2) var<storage, read_write> output: array<f32>;
@group(0) @binding(3) var<uniform> params: Params;

var<workgroup> reduce_buf: array<f32, 256>;

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

fn input_shape(idx: u32) -> u32 {
    return get4(params.input_shape0, params.input_shape1, idx);
}

fn out_shape(idx: u32) -> u32 {
    return get4(params.out_shape0, params.out_shape1, idx);
}

fn is_reduced(idx: u32) -> bool {
    return get4(params.reduced0, params.reduced1, idx) != 0u;
}

fn in_stride(idx: u32) -> u32 {
    return get4(params.in_strides0, params.in_strides1, idx);
}

fn out_stride(idx: u32) -> u32 {
    return get4(params.out_strides0, params.out_strides1, idx);
}

fn kept_axis(idx: u32) -> u32 {
    return get4(params.kept_axes0, params.kept_axes1, idx);
}

fn reduced_axis(idx: u32) -> u32 {
    return get4(params.reduced_axes0, params.reduced_axes1, idx);
}

fn output_base(flat_out: u32) -> u32 {
    var remaining = flat_out;
    var base = 0u;
    for (var out_d = 0u; out_d < params.out_rank; out_d = out_d + 1u) {
        let stride = out_stride(out_d);
        let coord = remaining / stride;
        remaining = remaining % stride;
        base = base + coord * in_stride(kept_axis(out_d));
        _ = out_shape(out_d);
    }
    return base;
}

fn reduced_offset(flat_reduce: u32) -> u32 {
    var remaining = flat_reduce;
    var offset = 0u;
    var reduce_d = 0u;
    for (var in_d = 0u; in_d < params.in_rank; in_d = in_d + 1u) {
        if (is_reduced(in_d)) {
            let dim = input_shape(in_d);
            let stride = in_stride(in_d);
            let coord = remaining % dim;
            remaining = remaining / dim;
            offset = offset + coord * stride;
            _ = reduced_axis(reduce_d);
            reduce_d = reduce_d + 1u;
        }
    }
    return offset;
}

fn reduce_sum_output(flat_out: u32, local_id: u32) -> f32 {
    var acc = 0.0;
    let base = output_base(flat_out);
    var j = local_id;
    while (j < params.reduce_count) {
        acc = acc + input[base + reduced_offset(j)];
        j = j + 256u;
    }
    reduce_buf[local_id] = acc;
    workgroupBarrier();

    var stride = 128u;
    while (stride > 0u) {
        if (local_id < stride) {
            reduce_buf[local_id] = reduce_buf[local_id] + reduce_buf[local_id + stride];
        }
        workgroupBarrier();
        stride = stride / 2u;
    }
    return reduce_buf[0];
}

fn reduce_max_output(flat_out: u32, local_id: u32) -> f32 {
    var acc = -3.4028234663852886e38;
    let base = output_base(flat_out);
    var j = local_id;
    while (j < params.reduce_count) {
        acc = max(acc, input[base + reduced_offset(j)]);
        j = j + 256u;
    }
    reduce_buf[local_id] = acc;
    workgroupBarrier();

    var stride = 128u;
    while (stride > 0u) {
        if (local_id < stride) {
            reduce_buf[local_id] = max(reduce_buf[local_id], reduce_buf[local_id + stride]);
        }
        workgroupBarrier();
        stride = stride / 2u;
    }
    return reduce_buf[0];
}

@compute @workgroup_size(256)
fn reduce_sum(@builtin(workgroup_id) wg: vec3<u32>, @builtin(local_invocation_id) lid: vec3<u32>) {
    let flat_out = wg.x;
    if (flat_out >= params.out_len) { return; }
    let v = reduce_sum_output(flat_out, lid.x);
    if (lid.x == 0u) {
        output[flat_out] = v;
    }
}

@compute @workgroup_size(256)
fn reduce_mean(@builtin(workgroup_id) wg: vec3<u32>, @builtin(local_invocation_id) lid: vec3<u32>) {
    let flat_out = wg.x;
    if (flat_out >= params.out_len) { return; }
    let v = reduce_sum_output(flat_out, lid.x);
    if (lid.x == 0u) {
        output[flat_out] = v / f32(params.reduce_count);
    }
}

@compute @workgroup_size(256)
fn reduce_max(@builtin(workgroup_id) wg: vec3<u32>, @builtin(local_invocation_id) lid: vec3<u32>) {
    let flat_out = wg.x;
    if (flat_out >= params.out_len) { return; }
    let v = reduce_max_output(flat_out, lid.x);
    if (lid.x == 0u) {
        output[flat_out] = v;
    }
}
