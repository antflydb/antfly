// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Qualified native program instances and specialization exports.

pub const source_0 =
    \\           "kernel void termite_embedding_lookup(device const float *weight [[buffer(0)]], device const uint *ids [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_embedding_lookup_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {\n"
    \\           "    uint total = p.total * p.dim;\n"
    \\           "    if (gid >= total) return;\n"
    \\           "    uint row = gid / p.dim;\n"
    \\           "    uint col = gid - row * p.dim;\n"
    \\           "    uint idx = ids[row];\n"
    \\           "    output[gid] = weight[idx * p.dim + col];\n"
    \\           "}\n"
    \\
;

pub const source_1 =
    \\           TERMITE_MSL_ABI_DENSE_EMBEDDING_16(termite_embedding_lookup_bf16) "\n"
    \\           "    uint total = p.total * p.dim;\n"
    \\           "    if (gid >= total) return;\n"
    \\           "    uint row = gid / p.dim;\n"
    \\           "    uint col = gid - row * p.dim;\n"
    \\           "    uint idx = ids[row];\n"
    \\           "    output[gid] = termite_embedding_bf16_to_f32(weight[idx * p.dim + col]);\n"
    \\           "}\n"
    \\           TERMITE_MSL_ABI_DENSE_EMBEDDING_16(termite_embedding_lookup_f16) "\n"
    \\           "    uint total = p.total * p.dim;\n"
    \\           "    if (gid >= total) return;\n"
    \\           "    uint row = gid / p.dim;\n"
    \\           "    uint col = gid - row * p.dim;\n"
    \\           "    uint idx = ids[row];\n"
    \\           "    output[gid] = float(as_type<half>(weight[idx * p.dim + col]));\n"
    \\           "}\n"
    \\
;
