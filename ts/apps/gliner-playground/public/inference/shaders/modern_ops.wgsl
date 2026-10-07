// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Apache-2.0
// Dense activation operations. FP32 activations; packed FP16 weights are handled
// by matmul_transb_f16. No optional shader-f16 device feature is required.
struct Params { len: u32, mode: u32, dim: u32, stride: u32, offset: u32, seq: u32, theta: f32, pad: u32 };
@group(0) @binding(0) var<storage, read> input: array<f32>;
@group(0) @binding(1) var<storage, read> indices: array<u32>;
@group(0) @binding(2) var<storage, read_write> output: array<f32>;
@group(0) @binding(3) var<uniform> p: Params;

fn erf_approx(x: f32) -> f32 {
    let a = abs(x);
    let t = 1.0 / (1.0 + 0.3275911 * a);
    let poly = (((((1.061405429 * t - 1.453152027) * t) + 1.421413741) * t - 0.284496736) * t + 0.254829592) * t;
    return select(-1.0, 1.0, x >= 0.0) * (1.0 - poly * exp(-a * a));
}
@compute @workgroup_size(256)
fn modern_op(@builtin(global_invocation_id) gid: vec3<u32>) {
    let i = gid.x;
    if (i >= p.len) { return; }
    switch p.mode {
        case 0u: { // RoPE, full head dimension, position offset zero.
            let d = i % p.dim;
            let half = p.dim / 2u;
            let interleaved = p.offset != 0u;
            let pair = select(d % half, d / 2u, interleaved);
            let other = select(select(d + half, d - half, d >= half), d ^ 1u, interleaved);
            let negative = select(d < half, d % 2u == 0u, interleaved);
            let pos = (i / p.dim) / (p.len / (p.seq * p.dim));
            let angle = f32(pos) / pow(p.theta, 2.0 * f32(pair) / f32(p.dim));
            let rotated = input[i - d + other] * select(1.0, -1.0, negative);
            output[i] = input[i] * cos(angle) + rotated * sin(angle);
        }
        case 1u: { let x = input[i]; output[i] = 0.5 * x * (1.0 + erf_approx(x * 0.7071067811865475)); }
        case 2u: { output[i] = max(input[i], 0.0); }
        case 3u: { output[i] = input[(i / p.dim) * p.stride + p.offset + i % p.dim]; }
        case 4u: { output[i] = input[indices[i / p.dim] * p.dim + i % p.dim]; }
        default: { output[i] = input[i]; }
    }
}
