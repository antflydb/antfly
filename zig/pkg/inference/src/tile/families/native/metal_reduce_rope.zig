// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

const program = @import("../../native_program.zig");

pub const source_0 =
    program.metal(
        \\kernel void termite_apply_rope(device const float *input [[buffer(0)]], device const uint *positions [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_rope_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.total_chunks * p.head_dim;
        \\    if (gid >= total) return;
        \\    uint chunk = gid / p.head_dim;
        \\    uint d = gid - chunk * p.head_dim;
        \\    uint base = chunk * p.head_dim;
        \\    if (p.rope_dim < 2u) { output[gid] = input[gid]; return; }
        \\    uint idx0 = 0u; uint idx1 = 0u; uint j = 0u;
        \\    if (!termite_rope_pair_indices(d, p.head_dim, p.rope_dim, p.consecutive_pairs, idx0, idx1, j)) { output[gid] = input[gid]; return; }
        \\    float pos = float(positions[chunk]);
        \\    float freq = 1.0f / pow(p.theta, float(2u * j) / float(p.rope_dim));
        \\    float angle = pos * p.freq_scale * freq;
        \\    float cos_val = cos(angle);
        \\    float sin_val = sin(angle);
        \\    float x0 = input[base + idx0];
        \\    float x1 = input[base + idx1];
        \\    if (d == idx0) output[gid] = x0 * cos_val - x1 * sin_val;
        \\    else if (d == idx1) output[gid] = x0 * sin_val + x1 * cos_val;
        \\    else output[gid] = input[gid];
        \\}
        \\
    );

pub const source_1 =
    program.metal(
        \\kernel void termite_apply_rope_pair(device const float *first_input [[buffer(0)]], device const float *second_input [[buffer(1)]], device const uint *positions [[buffer(2)]], device float *first_output [[buffer(3)]], device float *second_output [[buffer(4)]], constant termite_metal_rope_params &p [[buffer(5)]], uint gid [[thread_position_in_grid]]) {
        \\    uint total = p.total_chunks * p.head_dim; if (gid >= total) return; uint chunk = gid / p.head_dim; uint d = gid - chunk * p.head_dim; uint base = chunk * p.head_dim;
        \\    if (p.rope_dim < 2u) { first_output[gid] = first_input[gid]; second_output[gid] = second_input[gid]; return; }
        \\    uint idx0 = 0u; uint idx1 = 0u; uint j = 0u;
        \\    if (!termite_rope_pair_indices(d, p.head_dim, p.rope_dim, p.consecutive_pairs, idx0, idx1, j)) { first_output[gid] = first_input[gid]; second_output[gid] = second_input[gid]; return; }
        \\    float pos = float(positions[chunk]); float freq = 1.0f / pow(p.theta, float(2u * j) / float(p.rope_dim)); float angle = pos * p.freq_scale * freq; float cos_val = cos(angle); float sin_val = sin(angle);
        \\    float first_x0 = first_input[base + idx0]; float first_x1 = first_input[base + idx1]; float second_x0 = second_input[base + idx0]; float second_x1 = second_input[base + idx1];
        \\    if (d == idx0) { first_output[gid] = first_x0 * cos_val - first_x1 * sin_val; second_output[gid] = second_x0 * cos_val - second_x1 * sin_val; }
        \\    else if (d == idx1) { first_output[gid] = first_x0 * sin_val + first_x1 * cos_val; second_output[gid] = second_x0 * sin_val + second_x1 * cos_val; }
        \\    else { first_output[gid] = first_input[gid]; second_output[gid] = second_input[gid]; }
        \\}
        \\
    );
