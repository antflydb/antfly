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

//! Metal per-row reductions: last-dim reduce / softmax with one 256-thread
//! threadgroup per row, the inclusive/exclusive scan, and the gated RMS norm
//! of the Qwen3.5 / LightOnOCR-3 linear-attention output.

const fmt = @import("std").fmt.comptimePrint;

// ---------------------------------------------------------------- rows

const barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);";
const row_args = "threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {\n";

/// Signature, row guard, block size and row base.
fn rowHead(comptime name: []const u8, comptime buffers: []const u8, comptime params: []const u8, comptime slot: u32) []const u8 {
    return fmt("kernel void {s}({s}, constant {s} &p [[buffer({d})]], ", .{ name, buffers, params, slot }) ++ row_args ++
        \\    if (row >= p.rows || p.dim == 0u) return;
        \\    const uint threads = 256u;
        \\    const uint base = row * p.dim;
        \\
    ;
}

fn cols(comptime indent: []const u8, comptime body: []const u8) []const u8 {
    return indent ++ "for (uint col = tid; col < p.dim; col += threads) " ++ body ++ "\n";
}

/// Tree over `shmem`: store `value`, then halve with `update` while tid < stride.
fn tree(comptime value: []const u8, comptime update: []const u8) []const u8 {
    return fmt(
        \\    shmem[tid] = {s};
        \\    {s}
        \\    for (uint stride = threads >> 1u; stride > 0u; stride >>= 1u) {{
        \\        if (tid < stride) {s}
        \\        {s}
        \\    }}
        \\
    , .{ value, barrier, update, barrier });
}

const add = "shmem[tid] += shmem[tid + stride];";
const max = "shmem[tid] = max(shmem[tid], shmem[tid + stride]);";
const flt_lowest = "-3.4028234663852886e+38f";

const softmax_rows = rowHead("termite_apply_softmax_rows", "device const float *input [[buffer(0)]], device float *output [[buffer(1)]]", "termite_metal_apply_softmax_params", 2) ++
    "    float local_max = " ++ flt_lowest ++ ";\n" ++
    cols("    ", "local_max = max(local_max, input[base + col]);") ++
    tree("local_max", max) ++
    "    const float row_max = shmem[0]; " ++ barrier ++ "\n" ++
    "    float local_sum = 0.0f;\n" ++
    cols("    ", "local_sum += exp(input[base + col] - row_max);") ++
    tree("local_sum", add) ++
    \\    const float row_sum = max(shmem[0], 1.0e-30f);
    \\    const float log_sum = log(row_sum);
    \\
++ cols("    ", "{") ++
    \\        const float shifted = input[base + col] - row_max;
    \\        output[base + col] = p.log_softmax != 0u ? shifted - log_sum : exp(shifted) / row_sum;
    \\    }
    \\}
    \\
;

/// `p.kind`: 0 sum, 1 max, 2 mean.
const reduce_last_dim_rows = rowHead("termite_reduce_last_dim_rows", "device const float *input [[buffer(0)]], device float *output [[buffer(1)]]", "termite_metal_reduce_last_dim_params", 2) ++
    "    float local = p.kind == 1u ? " ++ flt_lowest ++ " : 0.0f;\n" ++
    "    if (p.kind == 1u) {\n" ++ cols("        ", "local = max(local, input[base + col]);") ++
    "    } else {\n" ++ cols("        ", "local += input[base + col];") ++ "    }\n" ++
    tree("local", "{\n            if (p.kind == 1u) " ++ max ++ "\n            else " ++ add ++ "\n        }") ++
    "    if (tid == 0u) output[row] = p.kind == 2u ? shmem[0] / float(p.dim) : shmem[0];\n}\n";

const multiply_reduce_last_dim_rows = rowHead("termite_multiply_reduce_last_dim_rows", "device const float *lhs [[buffer(0)]], device const float *rhs [[buffer(1)]], device float *output [[buffer(2)]]", "termite_metal_reduce_last_dim_params", 3) ++
    "    float local = 0.0f;\n" ++
    cols("    ", "local += lhs[base + col] * rhs[base + col];") ++
    tree("local", add) ++
    "    if (tid == 0u) output[row] = shmem[0];\n}\n";

pub fn rowsRegion() []const u8 {
    return softmax_rows ++ reduce_last_dim_rows ++ multiply_reduce_last_dim_rows;
}

// ---------------------------------------------------------------- scan

/// Cumulative sum along one axis: lanes scan contiguous chunks, then a
/// Hillis-Steele prefix over the 256 chunk totals.
fn cumulativeSum(comptime name: []const u8, comptime T: []const u8, comptime zero: []const u8) []const u8 {
    return fmt(
        \\kernel void {0s}(device const {1s} *input [[buffer(0)]], device {1s} *output [[buffer(1)]], constant termite_scan_params &p [[buffer(2)]], uint tid [[thread_index_in_threadgroup]], uint row [[threadgroup_position_in_grid]]) {{
        \\    // Each lane scans a contiguous chunk, then a workgroup prefix propagates
        \\    // chunk totals. O(N + 256 log 256) work, constant scratch, one dispatch.
        \\    threadgroup {1s} totals[256];
        \\    uint chunk = p.width / 256u + uint(p.width % 256u != 0u);
        \\    ulong start = ulong(tid) * chunk, end = min(start + chunk, ulong(p.width));
        \\    uint base = (row / p.inner) * p.width * p.inner + row % p.inner;
        \\    {1s} sum = {2s};
        \\    for (ulong j = start; j < end; ++j) {{ uint pos = p.reverse != 0u ? p.width - 1u - uint(j) : uint(j); sum += input[base + pos * p.inner]; }}
        \\    totals[tid] = sum; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    for (uint step = 1u; step < 256u; step <<= 1u) {{
        \\        {1s} prior = tid >= step ? totals[tid - step] : {2s};
        \\        threadgroup_barrier(mem_flags::mem_threadgroup);
        \\        totals[tid] += prior; threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    }}
        \\    sum = tid == 0u ? {2s} : totals[tid - 1u];
        \\    for (ulong j = start; j < end; ++j) {{
        \\        uint pos = p.reverse != 0u ? p.width - 1u - uint(j) : uint(j), index = base + pos * p.inner;
        \\        {1s} value = input[index]; if (p.exclusive != 0u) output[index] = sum;
        \\        sum += value; if (p.exclusive == 0u) output[index] = sum;
        \\    }}
        \\}}
        \\
    , .{ name, T, zero });
}

pub fn scanRegion() []const u8 {
    return cumulativeSum("termite_cumulative_sum_f32", "float", "0.0f") ++ cumulativeSum("termite_cumulative_sum_i64", "ulong", "0u");
}

// ---------------------------------------------------------------- gated norm

/// `core * rsqrt(mean(core^2) + 1e-6) * weight * silu(z)` per head of p.dim,
/// either one thread per head (`heads` = bound on gid) or one SIMD-group per
/// head (gid / 32).
fn gatedNorm(comptime name: []const u8, comptime params: []const u8, comptime heads: []const u8, comptime simd: bool) []const u8 {
    const lanes = if (simd) "for(uint d=uint(lane);d<p.dim;d+=32)" else "for(uint d=0;d<p.dim;d++)";
    return fmt("kernel void {s}(device const float *core [[buffer(11)]], device const float *z [[buffer(12)]], device const float *weight [[buffer(13)]], device float *output [[buffer(14)]], constant {s} &p [[buffer(15)]], uint gid [[thread_position_in_grid]]{s}) {{\n", .{ name, params, if (simd) ", ushort lane [[thread_index_in_simdgroup]]" else "" }) ++
        (if (simd) "    uint h=gid/32; if(h>=" ++ heads ++ ")return; uint base=h*p.dim;\n" else "    if(gid>=" ++ heads ++ ")return;\n    uint base=gid*p.dim;\n") ++
        "    float sum=0;\n" ++
        "    " ++ lanes ++ "{float v=core[base+d];sum+=v*v;}\n" ++
        "    float scale=1.0f/sqrt(" ++ (if (simd) "simd_sum(sum)" else "sum") ++ "/float(p.dim)+1e-6f);\n" ++
        "    " ++ lanes ++ (if (simd) "" else " ") ++ "{float gate=z[base+d];output[base+d]=core[base+d]*scale*weight[d]*(gate/(1.0f+exp(-gate)));}\n}\n";
}

pub fn qwen35NormRegion() []const u8 {
    return gatedNorm("termite_qwen35_norm", "termite_qwen35_params", "p.seq*p.value_heads", false) ++
        gatedNorm("termite_qwen35_norm_parallel", "termite_qwen35_params", "p.value_heads", true);
}

pub fn lightonocr3NormRegion() []const u8 {
    return gatedNorm("termite_lightonocr3_prefill_norm_parallel", "CoreParams", "p.seq*p.value_heads", true);
}

// ---------------------------------------------------------------- cached norm

/// LightOnOCR-3 row norm instances of `termite_lightonocr3_norm_cached_body`
/// for the hidden sizes it caches in registers.
pub fn lightonocr3CachedRegion() []const u8 {
    var out: []const u8 = "";
    for ([_]u32{ 1024, 2560 }) |h| out = out ++ fmt("kernel void termite_lightonocr3_norm_cached{0d}(device const float4 *x [[buffer(0)]], device const float4 *w [[buffer(1)]], device float4 *y [[buffer(2)]], constant termite_metal_apply_row_norm_params &p [[buffer(3)]], threadgroup float *partial [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint sg [[simdgroup_index_in_threadgroup]], uint lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]) {{ termite_lightonocr3_norm_cached_body<{0d}>(x,w,y,p,partial,tid,sg,lane,row); }}\n", .{h});
    return out;
}
