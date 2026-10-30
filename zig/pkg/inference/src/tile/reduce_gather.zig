// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
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

//! Embedding lookups: one thread per output element gathers
//! `weight[ids[row] * dim + col]`. Kernels differ only in the weight format
//! (f32, bf16, f16 bits) and, on CUDA, the id type and a scale.

const fmt = @import("std").fmt.comptimePrint;

pub const Format = enum { f32, bf16, f16 };

fn metalLookup(comptime name: []const u8, comptime format: Format) []const u8 {
    const at = "weight[idx * p.dim + col]";
    return fmt(
        \\kernel void {s}(device const {s} *weight [[buffer(0)]], device const uint *ids [[buffer(1)]], device float *output [[buffer(2)]], constant termite_metal_embedding_lookup_params &p [[buffer(3)]], uint gid [[thread_position_in_grid]]) {{
        \\    uint total = p.total * p.dim;
        \\    if (gid >= total) return;
        \\    uint row = gid / p.dim;
        \\    uint col = gid - row * p.dim;
        \\    uint idx = ids[row];
        \\    output[gid] = {s};
        \\}}
        \\
    , .{ name, if (format == .f32) "float" else "ushort", switch (format) {
        .f32 => at,
        .bf16 => "termite_embedding_bf16_to_f32(" ++ at ++ ")",
        .f16 => "float(as_type<half>(" ++ at ++ "))",
    } });
}

/// Metal: f32, bf16 and f16 weights with uint ids.
pub fn metalRegion() []const u8 {
    return metalLookup("termite_embedding_lookup", .f32) ++
        metalLookup("termite_embedding_lookup_bf16", .bf16) ++
        metalLookup("termite_embedding_lookup_f16", .f16);
}

fn cudaLookup(comptime format: Format, comptime i32_ids: bool) []const u8 {
    const id_ty = if (i32_ids) "int" else "long long";
    const name = "termite_embedding_lookup" ++ (if (i32_ids) "_i32" else "") ++ switch (format) {
        .f32 => "_f32",
        .bf16 => "_bf16_weight_f32",
        .f16 => "_f16_weight_f32",
    };
    const at = fmt("[(unsigned long long){s} * dim + col]", .{if (i32_ids) "((unsigned int)id)" else "id"});
    return fmt(
        \\extern "C" __global__ void {s}(
        \\    float* dst,
        \\    const {s}* weight,
        \\    const {s}* ids,
        \\    unsigned int total,
        \\    unsigned int dim,
        \\    float scale
        \\) {{
        \\    unsigned int idx = blockIdx.x * blockDim.x + threadIdx.x;
        \\    unsigned int count = total * dim;
        \\    if (idx >= count) return;
        \\    unsigned int row = idx / dim;
        \\    unsigned int col = idx - row * dim;
        \\    {s} id = ids[row];
        \\    {s}
        \\}}
        \\
    , .{ name, if (format == .f32) "float" else "unsigned short", id_ty, id_ty, switch (format) {
        .f32 => "dst[idx] = weight" ++ at ++ " * scale;",
        .bf16 => "dst[idx] = termite_bf16_to_f32(weight" ++ at ++ ") * scale;",
        .f16 => "__half value = reinterpret_cast<const __half*>(weight)" ++ at ++ ";\n    dst[idx] = __half2float(value) * scale;",
    } });
}

/// CUDA: the three weight formats with i64 ids, then with i32 ids.
pub fn cudaRegion() []const u8 {
    var out: []const u8 = "";
    for ([_]bool{ false, true }, 0..) |i32_ids, i| for ([_]Format{ .f32, .bf16, .f16 }, 0..) |format, j| {
        out = out ++ (if (i + j == 0) "" else "\n") ++ cudaLookup(format, i32_ids);
    };
    return out;
}
