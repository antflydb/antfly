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

//! GPU lowerings of the model tiles (GEMM, LayerNorm, attention, im2col,
//! embed) for Metal and CUDA, rendered at comptime from the same
//! descriptions the CPU lowering compiles.
//!
//! Launch contract (buffers in binding order, then a params struct;
//! Metal passes it as `constant&`, CUDA by value):
//!
//! | kernel     | buffers                                  | params                                 | grid                         | threads            |
//! |------------|------------------------------------------|----------------------------------------|------------------------------|--------------------|
//! | GEMM       | x, w, [bias], [gate], [residual], out            | m, n, k                                | ceil(n/tile_n), ceil(m/tile_m) | `Gemm.threads()` |
//! | LayerNorm  | x, gamma, beta, out                      | rows, dim                              | rows                         | `threads`          |
//! | attention  | q, k, v, out                             | tq, tk, heads, ld_q/k/v/o, causal, q_offset, scale | ceil(tq/queries_per_block), heads | 32 * queries_per_block |
//! | im2col     | x, out                                   | channels, t_in, t_out                  | ceil(t_out*channels*kernel / threads) | `threads`   |
//! | embed      | ids, table, pos, out                     | rows, dim, pos_offset                  | ceil(rows*dim / threads)     | `threads`          |
//!
//! `residual` may alias `out`. SIMD-group / warp width is 32.

const std = @import("std");
const fexpr = @import("fexpr.zig");
const gemm = @import("gemm.zig");
const norm = @import("norm.zig");
const attention = @import("attention.zig");
const layout = @import("layout.zig");
const vmath = @import("vmath.zig");
const gpu = @import("gpu.zig");

const fmt = std.fmt.comptimePrint;

/// `opencl` is a test dialect: PoCL runs it on the CPU so the GPU schedules
/// can be executed and checked without a GPU (scripts/tile/run_whisper_opencl.py).
/// PoCL has no sub-groups, so its warp sum goes through local memory and
/// attention runs one query per work-group.
pub const Lang = enum { metal, cuda, opencl };

const L = struct {
    lang: Lang,
    fn msl(comptime l: L) bool {
        return l.lang == .metal;
    }
    fn pick(comptime l: L, comptime metal: []const u8, comptime cuda: []const u8, comptime opencl: []const u8) []const u8 {
        return switch (l.lang) {
            .metal => metal,
            .cuda => cuda,
            .opencl => opencl,
        };
    }
    fn func(comptime l: L) []const u8 {
        return l.pick("inline", "static __device__ __forceinline__", "inline");
    }
    fn uint(comptime l: L) []const u8 {
        return l.pick("uint", "unsigned int", "uint");
    }
    fn exp(comptime l: L) []const u8 {
        return l.pick("exp", "expf", "exp");
    }
    fn fma(comptime l: L) []const u8 {
        return l.pick("fma", "fmaf", "fma");
    }
    fn rsqrt(comptime l: L) []const u8 {
        return l.pick("rsqrt", "rsqrtf", "rsqrt");
    }
    fn fmax(comptime l: L) []const u8 {
        return l.pick("max", "fmaxf", "fmax");
    }
    fn barrier(comptime l: L) []const u8 {
        return l.pick("threadgroup_barrier(mem_flags::mem_threadgroup);", "__syncthreads();", "barrier(CLK_LOCAL_MEM_FENCE);");
    }
    fn shared(comptime l: L) []const u8 {
        return l.pick("threadgroup float", "__shared__ float", "__local float");
    }
    /// Sum over the 32 lanes of the caller's SIMD-group / warp.
    fn warpSum(comptime l: L, comptime x: []const u8) []const u8 {
        return l.pick("simd_sum(" ++ x ++ ")", "antfly_tile_warp_sum(" ++ x ++ ")", "antfly_tile_warp_sum(" ++ x ++ ", wsum, tid)");
    }
    fn negInf(comptime l: L) []const u8 {
        return l.pick("(-INFINITY)", "(__int_as_float(0xff800000))", "(-INFINITY)");
    }
    /// Read-only buffer parameter.
    fn in(comptime l: L, comptime ty: []const u8, comptime name: []const u8, comptime slot: usize) []const u8 {
        return switch (l.lang) {
            .metal => fmt("device const {s} *{s} [[buffer({d})]]", .{ ty, name, slot }),
            .cuda => fmt("const {s} *__restrict__ {s}", .{ cudaTy(ty), name }),
            .opencl => fmt("__global const {s} *restrict {s}", .{ ty, name }),
        };
    }
    /// Read-only buffer that may alias an output.
    fn inAliased(comptime l: L, comptime ty: []const u8, comptime name: []const u8, comptime slot: usize) []const u8 {
        return switch (l.lang) {
            .metal => fmt("device const {s} *{s} [[buffer({d})]]", .{ ty, name, slot }),
            .cuda => fmt("const {s} *{s}", .{ cudaTy(ty), name }),
            .opencl => fmt("__global const {s} *{s}", .{ ty, name }),
        };
    }
    fn out(comptime l: L, comptime name: []const u8, comptime slot: usize) []const u8 {
        return switch (l.lang) {
            .metal => fmt("device float *{s} [[buffer({d})]]", .{ name, slot }),
            .cuda => fmt("float *{s}", .{name}),
            .opencl => fmt("__global float *{s}", .{name}),
        };
    }
    fn params(comptime l: L, comptime ty: []const u8, comptime slot: usize) []const u8 {
        return switch (l.lang) {
            .metal => fmt("constant {s} &p [[buffer({d})]]", .{ ty, slot }),
            .cuda, .opencl => fmt("const {s} p", .{ty}),
        };
    }
    fn kernel(comptime l: L, comptime name: []const u8, comptime threads: u32, comptime args: []const []const u8, comptime builtins: []const []const u8) []const u8 {
        var out_s: []const u8 = switch (l.lang) {
            .metal => "kernel void " ++ name ++ "(",
            .cuda => fmt("extern \"C\" __global__ void __launch_bounds__({d}) {s}(", .{ threads, name }),
            .opencl => fmt("__kernel __attribute__((reqd_work_group_size({d}, 1, 1))) void {s}(", .{ threads, name }),
        };
        var first = true;
        const none: []const []const u8 = &.{};
        for (args ++ (if (l.msl()) builtins else none)) |a| {
            out_s = out_s ++ (if (first) "" else ", ") ++ a;
            first = false;
        }
        return out_s ++ ") {\n";
    }
    /// Thread/group indices the Metal signature gets as attributes.
    fn ids(comptime l: L, comptime cuda: []const u8, comptime opencl: []const u8) []const u8 {
        return l.pick("", cuda, opencl);
    }
};

fn cudaTy(comptime ty: []const u8) []const u8 {
    return if (std.mem.eql(u8, ty, "uint")) "unsigned int" else ty;
}

fn structDecl(comptime l: L, comptime name: []const u8, comptime fields: []const [2][]const u8) []const u8 {
    var s: []const u8 = if (l.lang == .opencl) "typedef struct {" else "struct " ++ name ++ " {";
    for (fields) |f| s = s ++ " " ++ (if (std.mem.eql(u8, f[0], "uint")) l.uint() else f[0]) ++ " " ++ f[1] ++ ";";
    return s ++ (if (l.lang == .opencl) " } " ++ name ++ ";\n" else " };\n");
}

/// Emit once per source module. Native GEMM schedules may use these same
/// expression helpers at their stores without adopting the portable GEMM loop.
pub fn epilogueHelpers(comptime e: fexpr.FExpr, comptime lang: Lang) []const u8 {
    const l: L = .{ .lang = lang };
    var s: []const u8 = "";
    if (e.uses(.silu)) s = s ++ l.func() ++ " float antfly_tile_silu(float x) { return " ++ fexpr.acc.siluOf().renderNative(.{ .exp = l.exp() }, .{ .value = "x", .bias = "", .gate = "", .residual = "" }, null) ++ "; }\n";
    if (e.uses(.round_f16)) s = s ++ l.pick(
        "inline float antfly_tile_round_f16(float x) { return float(half(x)); }\n",
        "#include <cuda_fp16.h>\nstatic __device__ __forceinline__ float antfly_tile_round_f16(float x) { return __half2float(__float2half_rn(x)); }\n",
        "#pragma OPENCL EXTENSION cl_khr_fp16 : enable\ninline float antfly_tile_round_f16(float x) { return convert_float(convert_half_rte(x)); }\n",
    );
    if (e.uses(.round_bf16)) s = s ++ l.func() ++ " float antfly_tile_round_bf16(float x) { " ++ l.uint() ++ " u = " ++ l.pick("as_type<uint>(x)", "__float_as_uint(x)", "as_uint(x)") ++ "; u = ((u & 0x7fffffffu) > 0x7f800000u) ? ((u & 0xffff0000u) | 0x00400000u) : ((u + 0x7fffu + ((u >> 16) & 1u)) & 0xffff0000u); return " ++ l.pick("as_type<float>(u)", "__uint_as_float(u)", "as_float(u)") ++ "; }\n";
    return s;
}

/// Shared declarations: params structs and math helpers.
pub fn preamble(comptime lang: Lang) []const u8 {
    const l: L = .{ .lang = lang };
    var s: []const u8 = l.pick("#include <metal_stdlib>\nusing namespace metal;\n\n", "", "");
    s = s ++ structDecl(l, "antfly_tile_gemm_params", &.{ .{ "uint", "m" }, .{ "uint", "n" }, .{ "uint", "k" } });
    s = s ++ structDecl(l, "antfly_tile_norm_params", &.{ .{ "uint", "rows" }, .{ "uint", "dim" } });
    s = s ++ structDecl(l, "antfly_tile_attention_params", &.{ .{ "uint", "tq" }, .{ "uint", "tk" }, .{ "uint", "heads" }, .{ "uint", "ld_q" }, .{ "uint", "ld_k" }, .{ "uint", "ld_v" }, .{ "uint", "ld_o" }, .{ "uint", "causal" }, .{ "uint", "q_offset" }, .{ "float", "scale" } });
    s = s ++ structDecl(l, "antfly_tile_im2col_params", &.{ .{ "uint", "channels" }, .{ "uint", "t_in" }, .{ "uint", "t_out" } });
    s = s ++ structDecl(l, "antfly_tile_embed_params", &.{ .{ "uint", "rows" }, .{ "uint", "dim" }, .{ "uint", "pos_offset" } });
    s = s ++ "\n" ++ vmath.cHelpers(l.func(), l.exp(), l.pick("fabs", "fabsf", "fabs"));
    if (l.lang == .opencl) s = s ++
        \\inline float antfly_tile_warp_sum(float x, __local float *scratch, uint tid) {
        \\    barrier(CLK_LOCAL_MEM_FENCE);
        \\    scratch[tid] = x;
        \\    barrier(CLK_LOCAL_MEM_FENCE);
        \\    const uint base = tid & ~31u;
        \\    float s = 0.0f;
        \\    for (uint j = 0u; j < 32u; ++j) s += scratch[base + j];
        \\    return s;
        \\}
        \\
    ;
    if (l.lang == .cuda) s = s ++
        \\static __device__ __forceinline__ float antfly_tile_warp_sum(float x) {
        \\    for (unsigned int off = 16u; off > 0u; off >>= 1u) x += __shfl_xor_sync(0xffffffffu, x, off);
        \\    return x;
        \\}
        \\
    ;
    return s ++ "\n";
}

pub fn renderGemm(comptime g: gemm.Gemm, comptime lang: Lang) []const u8 {
    comptime {
        @setEvalBranchQuota(100_000);
        const l: L = .{ .lang = lang };
        const u = l.uint();
        const threads = g.threads();
        const cols_per = g.tile_n / g.reg_n;
        var args: []const []const u8 = &.{ l.in("float", "x", 0), l.in("float", "w", 1) };
        var slot: usize = 2;
        if (g.usesBias()) {
            args = args ++ .{l.in("float", "bias", slot)};
            slot += 1;
        }
        if (g.usesGate()) {
            args = args ++ .{l.inAliased("float", "gate", slot)};
            slot += 1;
        }
        if (g.usesResidual()) {
            args = args ++ .{l.inAliased("float", "residual", slot)};
            slot += 1;
        }
        args = args ++ .{ l.out("out", slot), l.params("antfly_tile_gemm_params", slot + 1) };
        var s: []const u8 = l.kernel(g.name, threads, args, &.{ "uint3 tg_pos [[threadgroup_position_in_grid]]", "uint tid [[thread_index_in_threadgroup]]" });
        s = s ++ l.ids(
            "    const unsigned int tid = threadIdx.x;\n    const uint3 tg_pos = make_uint3(blockIdx.x, blockIdx.y, 0u);\n",
            "    const uint tid = (uint)get_local_id(0);\n    const uint3 tg_pos = (uint3)((uint)get_group_id(0), (uint)get_group_id(1), 0u);\n",
        );
        s = s ++ fmt(
            \\    {[sh]s} xs[{[tk]d} * {[tm]d}];
            \\    {[sh]s} ws[{[tk]d} * {[tn]d}];
            \\    const {[u]s} row0 = tg_pos.y * {[tm]d}u;
            \\    const {[u]s} col0 = tg_pos.x * {[tn]d}u;
            \\    const {[u]s} tr = tid / {[cp]d}u;
            \\    const {[u]s} tc = tid % {[cp]d}u;
            \\    float acc[{[rm]d}][{[rn]d}];
            \\    for ({[u]s} i = 0u; i < {[rm]d}u; ++i) for ({[u]s} j = 0u; j < {[rn]d}u; ++j) acc[i][j] = 0.0f;
            \\    for ({[u]s} k0 = 0u; k0 < p.k; k0 += {[tk]d}u) {{
            \\        for ({[u]s} idx = tid; idx < {[tm]d}u * {[tk]d}u; idx += {[threads]d}u) {{
            \\            const {[u]s} r = idx / {[tk]d}u; const {[u]s} kk = idx % {[tk]d}u;
            \\            const {[u]s} gr = row0 + r; const {[u]s} gk = k0 + kk;
            \\            xs[kk * {[tm]d}u + r] = (gr < p.m && gk < p.k) ? x[gr * p.k + gk] : 0.0f;
            \\        }}
            \\        for ({[u]s} idx = tid; idx < {[tn]d}u * {[tk]d}u; idx += {[threads]d}u) {{
            \\            const {[u]s} c = idx / {[tk]d}u; const {[u]s} kk = idx % {[tk]d}u;
            \\            const {[u]s} gc = col0 + c; const {[u]s} gk = k0 + kk;
            \\            ws[kk * {[tn]d}u + c] = (gc < p.n && gk < p.k) ? w[gc * p.k + gk] : 0.0f;
            \\        }}
            \\        {[barrier]s}
            \\        for ({[u]s} kk = 0u; kk < {[tk]d}u; ++kk) {{
            \\            float a[{[rm]d}]; float b[{[rn]d}];
            \\            for ({[u]s} i = 0u; i < {[rm]d}u; ++i) a[i] = xs[kk * {[tm]d}u + tr * {[rm]d}u + i];
            \\            for ({[u]s} j = 0u; j < {[rn]d}u; ++j) b[j] = ws[kk * {[tn]d}u + tc * {[rn]d}u + j];
            \\            for ({[u]s} i = 0u; i < {[rm]d}u; ++i) for ({[u]s} j = 0u; j < {[rn]d}u; ++j) acc[i][j] = {[fma]s}(a[i], b[j], acc[i][j]);
            \\        }}
            \\        {[barrier]s}
            \\    }}
            \\    for ({[u]s} i = 0u; i < {[rm]d}u; ++i) for ({[u]s} j = 0u; j < {[rn]d}u; ++j) {{
            \\        const {[u]s} gr = row0 + tr * {[rm]d}u + i;
            \\        const {[u]s} gc = col0 + tc * {[rn]d}u + j;
            \\        if (gr >= p.m || gc >= p.n) continue;
            \\        const float v = acc[i][j];
            \\        out[gr * p.n + gc] = {[epilogue]s};
            \\    }}
            \\}}
            \\
        , .{
            .sh = l.shared(),
            .u = u,
            .tm = g.tile_m,
            .tn = g.tile_n,
            .tk = g.tile_k,
            .rm = g.reg_m,
            .rn = g.reg_n,
            .cp = cols_per,
            .threads = threads,
            .barrier = l.barrier(),
            .fma = l.fma(),
            .epilogue = g.epilogue.renderInputs(.{}, .{ .value = "v", .bias = "bias[gc]", .gate = "gate[gr * p.n + gc]", .residual = "residual[gr * p.n + gc]" }),
        });
        return s;
    }
}

/// Block-wide sum of `v` into `name` (all threads get the result).
fn blockSum(comptime l: L, comptime v: []const u8, comptime name: []const u8) []const u8 {
    return fmt(
        \\    {{
        \\        const float part = {[ws]s};
        \\        if (lane == 0u) red[sg] = part;
        \\        {[barrier]s}
        \\        float total = 0.0f;
        \\        for ({[u]s} i = 0u; i < sg_count; ++i) total += red[i];
        \\        {[name]s} = total;
        \\        {[barrier]s}
        \\    }}
        \\
    , .{ .ws = l.warpSum(v), .barrier = l.barrier(), .u = l.uint(), .name = name });
}

/// Uncentered RMS reduction: one sum of squares, no mean/variance pass.
pub fn renderRmsNorm(comptime rn: norm.RmsNorm, comptime lang: Lang) []const u8 {
    comptime {
        const l: L = .{ .lang = lang };
        const u = l.uint();
        if (rn.threads == 0 or rn.threads > 1024 or rn.threads % 32 != 0) @compileError("RMS threads must be a nonzero multiple of 32 up to 1024");
        var s = l.kernel(rn.name, rn.threads, &.{ l.inAliased("float", "x", 0), l.in("float", "gamma", 1), l.out("out", 2), l.params("antfly_tile_norm_params", 3) }, &.{
            "uint row [[threadgroup_position_in_grid]]",    "uint tid [[thread_index_in_threadgroup]]",
            "uint lane [[thread_index_in_simdgroup]]",      "uint sg [[simdgroup_index_in_threadgroup]]",
            "uint sg_count [[simdgroups_per_threadgroup]]",
        });
        s = s ++ l.ids(
            "    const unsigned int row = blockIdx.x; const unsigned int tid = threadIdx.x;\n    const unsigned int lane = tid & 31u; const unsigned int sg = tid >> 5u; const unsigned int sg_count = (blockDim.x + 31u) >> 5u;\n",
            fmt("    const uint row = get_group_id(0); const uint tid = get_local_id(0);\n    const uint lane = tid & 31u; const uint sg = tid >> 5u; const uint sg_count = (get_local_size(0) + 31u) >> 5u;\n    __local float wsum[{d}];\n", .{rn.threads}),
        );
        s = s ++ fmt("    {s} red[32];\n    if (row >= p.rows) return;\n    const {s} base = row * p.dim;\n    float q = 0.0f;\n    for ({s} c = tid; c < p.dim; c += {d}u) {{ const float v = x[base + c]; q += v * v; }}\n    float sq = 0.0f;\n", .{ l.shared(), u, u, rn.threads });
        s = s ++ blockSum(l, "q", "sq");
        s = s ++ fmt("    const float inv = {s}(sq / (float)p.dim + {e}f);\n    for ({s} c = tid; c < p.dim; c += {d}u) out[base + c] = (x[base + c] * inv) * gamma[c];\n}}\n", .{ l.rsqrt(), rn.eps, u, rn.threads });
        return s;
    }
}

/// One thread per output column; selected tokens accumulate in row order.
/// Buffers x, mask (positive means included), out, params {rows, dim}.
pub fn renderMaskedMean(comptime m: norm.MaskedMean, comptime lang: Lang) []const u8 {
    comptime {
        const l: L = .{ .lang = lang };
        var s = l.kernel(m.name, m.threads, &.{ l.in("float", "x", 0), l.in("int", "mask", 1), l.out("out", 2), l.params("antfly_tile_norm_params", 3) }, &.{"uint gid [[thread_position_in_grid]]"});
        s = s ++ globalId(l);
        s = s ++ fmt("    if (gid >= p.dim) return;\n    float sum = 0.0f, count = 0.0f;\n    for ({s} r = 0; r < p.rows; ++r) {{ if (mask[r] > 0) {{ sum += x[r * p.dim + gid]; count += 1.0f; }} }}\n    out[gid] = count > 0.0f ? sum / count : 0.0f;\n}}\n", .{l.uint()});
        return s;
    }
}

/// Typed strided copy/gather. Host validates all views/indices before launch;
/// GPU offsets use 64 bits to avoid overflowing flattened tensor addresses.
/// Buffers: src, [ids], dst, params. Grid ceil(rows*cols/threads).
pub fn renderCopy(comptime c: layout.Copy, comptime lang: Lang) []const u8 {
    comptime {
        const l: L = .{ .lang = lang };
        const ty = switch (c.element) {
            .f32 => "float",
            .f16 => l.pick("half", "__half", "half"),
            .u32 => l.uint(),
            .i32 => "int",
        };
        const address = l.pick("ulong", "unsigned long long", "ulong");
        const params_name = c.name ++ "_params";
        var s: []const u8 = if (c.element == .f16) l.pick("", "#include <cuda_fp16.h>\n", "#pragma OPENCL EXTENSION cl_khr_fp16 : enable\n") else "";
        s = s ++ structDecl(l, params_name, &.{ .{ address, "rows" }, .{ address, "cols" }, .{ address, "src_offset" }, .{ address, "src_row_stride" }, .{ address, "src_col_stride" }, .{ address, "dst_offset" }, .{ address, "dst_row_stride" }, .{ address, "dst_col_stride" } });
        var args: []const []const u8 = &.{l.in(ty, "src", 0)};
        if (c.gather) args = args ++ .{l.in("uint", "ids", 1)};
        const slot = if (c.gather) 2 else 1;
        const output = l.pick(fmt("device {s} *dst [[buffer({d})]]", .{ ty, slot }), fmt("{s} *dst", .{ty}), fmt("__global {s} *dst", .{ty}));
        s = s ++ l.kernel(c.name, c.threads, args ++ .{ output, l.params(params_name, slot + 1) }, &.{ "uint group [[threadgroup_position_in_grid]]", "uint tid [[thread_index_in_threadgroup]]" });
        s = s ++ l.pick(fmt("    const ulong gid = (ulong)group * {d}ul + tid;\n", .{c.threads}), "", "");
        s = s ++ l.ids("    const unsigned long long gid = (unsigned long long)blockIdx.x * blockDim.x + threadIdx.x;\n", "    const ulong gid = get_global_id(0);\n");
        s = s ++ fmt("    if (gid >= p.rows * p.cols) return;\n    const {s} row = gid / p.cols, col = gid % p.cols;\n    const {s} sr = {s};\n    dst[p.dst_offset + row * p.dst_row_stride + col * p.dst_col_stride] = src[p.src_offset + sr * p.src_row_stride + col * p.src_col_stride];\n}}\n", .{ address, address, if (c.gather) "ids[row]" else "row" });
        return s;
    }
}

pub fn renderLayerNorm(comptime ln: norm.LayerNorm, comptime lang: Lang) []const u8 {
    comptime {
        const l: L = .{ .lang = lang };
        const u = l.uint();
        var s: []const u8 = l.kernel(ln.name, ln.threads, &.{ l.in("float", "x", 0), l.in("float", "gamma", 1), l.in("float", "beta", 2), l.out("out", 3), l.params("antfly_tile_norm_params", 4) }, &.{
            "uint row [[threadgroup_position_in_grid]]",
            "uint tid [[thread_index_in_threadgroup]]",
            "uint lane [[thread_index_in_simdgroup]]",
            "uint sg [[simdgroup_index_in_threadgroup]]",
            "uint sg_count [[simdgroups_per_threadgroup]]",
        });
        s = s ++ l.ids(
            "    const unsigned int row = blockIdx.x; const unsigned int tid = threadIdx.x;\n    const unsigned int lane = tid & 31u; const unsigned int sg = tid >> 5u; const unsigned int sg_count = (blockDim.x + 31u) >> 5u;\n",
            fmt("    const uint row = (uint)get_group_id(0); const uint tid = (uint)get_local_id(0);\n    const uint lane = tid & 31u; const uint sg = tid >> 5u; const uint sg_count = ((uint)get_local_size(0) + 31u) >> 5u;\n    __local float wsum[{d}];\n", .{ln.threads}),
        );
        s = s ++ fmt("    {s} red[32];\n    if (row >= p.rows) return;\n", .{l.shared()});
        s = s ++ fmt("    const {s} base = row * p.dim;\n    float s = 0.0f;\n    for ({s} c = tid; c < p.dim; c += {d}u) s += x[base + c];\n    float sum = 0.0f;\n", .{ u, u, ln.threads });
        s = s ++ blockSum(l, "s", "sum");
        s = s ++ fmt("    const float mean = sum / (float)p.dim;\n    float q = 0.0f;\n    for ({s} c = tid; c < p.dim; c += {d}u) {{ const float d = x[base + c] - mean; q += d * d; }}\n    float sq = 0.0f;\n", .{ u, ln.threads });
        s = s ++ blockSum(l, "q", "sq");
        s = s ++ fmt("    const float inv = {s}(sq / (float)p.dim + {e}f);\n", .{ l.rsqrt(), ln.eps });
        s = s ++ fmt("    for ({s} c = tid; c < p.dim; c += {d}u) out[base + c] = (x[base + c] - mean) * inv * gamma[c] + beta[c];\n}}\n", .{ u, ln.threads });
        return s;
    }
}

pub fn renderAttention(comptime att: attention.Attention, comptime lang: Lang) []const u8 {
    comptime {
        const l: L = .{ .lang = lang };
        const u = l.uint();
        if (att.head_dim % 32 != 0) @compileError("attention head_dim must be a multiple of 32");
        const per_lane = att.head_dim / 32;
        const qpb = if (lang == .opencl) 1 else att.queries_per_block;
        var s: []const u8 = l.kernel(att.name, 32 * qpb, &.{ l.in("float", "q", 0), l.in("float", "k", 1), l.in("float", "v", 2), l.out("out", 3), l.params("antfly_tile_attention_params", 4) }, &.{
            "uint3 tg_pos [[threadgroup_position_in_grid]]",
            "uint lane [[thread_index_in_simdgroup]]",
            "uint sg [[simdgroup_index_in_threadgroup]]",
        });
        s = s ++ l.ids(
            "    const uint3 tg_pos = make_uint3(blockIdx.x, blockIdx.y, 0u);\n    const unsigned int lane = threadIdx.x & 31u; const unsigned int sg = threadIdx.x >> 5u;\n",
            "    const uint3 tg_pos = (uint3)((uint)get_group_id(0), (uint)get_group_id(1), 0u);\n    const uint tid = (uint)get_local_id(0); const uint lane = tid & 31u; const uint sg = tid >> 5u;\n    __local float wsum[32];\n",
        );
        s = s ++ fmt(
            \\    // One SIMD-group per (query, head); lane owns elements lane + 32 * t.
            \\    const {[u]s} h = tg_pos.y;
            \\    const {[u]s} i = tg_pos.x * {[qpb]d}u + sg;
            \\    if (i >= p.tq || h >= p.heads) return;
            \\    float qv[{[e]d}]; float o[{[e]d}];
            \\    for ({[u]s} t = 0u; t < {[e]d}u; ++t) {{ qv[t] = q[i * p.ld_q + h * {[d]d}u + lane + 32u * t] * p.scale; o[t] = 0.0f; }}
            \\    const {[u]s} limit = (p.causal != 0u && p.q_offset + i + 1u < p.tk) ? p.q_offset + i + 1u : p.tk;
            \\    float m = {[ninf]s}; float l = 0.0f;
            \\    for ({[u]s} j = 0u; j < limit; ++j) {{
            \\        float part = 0.0f;
            \\        for ({[u]s} t = 0u; t < {[e]d}u; ++t) part += qv[t] * k[j * p.ld_k + h * {[d]d}u + lane + 32u * t];
            \\        const float s = {[ws]s};
            \\        const float m_new = {[max]s}(m, s);
            \\        const float corr = {[exp]s}(m - m_new);
            \\        const float pr = {[exp]s}(s - m_new);
            \\        l = l * corr + pr;
            \\        for ({[u]s} t = 0u; t < {[e]d}u; ++t) o[t] = o[t] * corr + pr * v[j * p.ld_v + h * {[d]d}u + lane + 32u * t];
            \\        m = m_new;
            \\    }}
            \\    const float inv = l > 0.0f ? 1.0f / l : 0.0f;
            \\    for ({[u]s} t = 0u; t < {[e]d}u; ++t) out[i * p.ld_o + h * {[d]d}u + lane + 32u * t] = o[t] * inv;
            \\}}
            \\
        , .{ .u = u, .qpb = qpb, .e = per_lane, .d = att.head_dim, .ninf = l.negInf(), .ws = l.warpSum("part"), .max = l.fmax(), .exp = l.exp() });
        return s;
    }
}

fn globalId(comptime l: L) []const u8 {
    return l.ids("    const unsigned int gid = blockIdx.x * blockDim.x + threadIdx.x;\n", "    const uint gid = (uint)get_global_id(0);\n");
}

pub fn renderIm2col(comptime c: layout.Im2col, comptime lang: Lang) []const u8 {
    comptime {
        const l: L = .{ .lang = lang };
        const u = l.uint();
        var s: []const u8 = l.kernel(c.name, c.threads, &.{ l.in("float", "x", 0), l.out("out", 1), l.params("antfly_tile_im2col_params", 2) }, &.{"uint gid [[thread_position_in_grid]]"});
        s = s ++ globalId(l);
        const src = switch (c.layout) {
            .channel_major => "x[ch * p.t_in + (" ++ u ++ ")src]",
            .time_major => "x[(" ++ u ++ ")src * p.channels + ch]",
        };
        s = s ++ fmt(
            \\    const {[u]s} width = p.channels * {[k]d}u;
            \\    if (gid >= p.t_out * width) return;
            \\    const {[u]s} t = gid / width; const {[u]s} rem = gid % width;
            \\    const {[u]s} ch = rem / {[k]d}u; const {[u]s} j = rem % {[k]d}u;
            \\    const int src = (int)(t * {[stride]d}u + j) - {[pad]d};
            \\    out[gid] = (src < 0 || src >= (int)p.t_in) ? 0.0f : {[src]s};
            \\}}
            \\
        , .{ .u = u, .k = c.kernel, .stride = c.stride, .pad = c.pad, .src = src });
        return s;
    }
}

pub fn renderEmbed(comptime e: layout.Embed, comptime lang: Lang) []const u8 {
    comptime {
        const l: L = .{ .lang = lang };
        const u = l.uint();
        var s: []const u8 = l.kernel(e.name, e.threads, &.{ l.in("uint", "ids", 0), l.in("float", "table", 1), l.in("float", "pos", 2), l.out("out", 3), l.params("antfly_tile_embed_params", 4) }, &.{"uint gid [[thread_position_in_grid]]"});
        s = s ++ globalId(l);
        s = s ++ fmt(
            \\    if (gid >= p.rows * p.dim) return;
            \\    const {[u]s} t = gid / p.dim; const {[u]s} c = gid % p.dim;
            \\    out[gid] = table[ids[t] * p.dim + c] + pos[(p.pos_offset + t) * p.dim + c];
            \\}}
            \\
        , .{ .u = u });
        return s;
    }
}

/// One source file with every kernel in `kernels` (a struct of tile
/// descriptions, e.g. `whisper_model.kernels`). RowReduce kernels are
/// rendered by `gpu.zig` into their own files.
pub fn renderModule(comptime kernels: type, comptime lang: Lang, comptime title: []const u8) []const u8 {
    comptime {
        @setEvalBranchQuota(2_000_000);
        var s: []const u8 = gpu.license_header ++ "// Generated by `zig build tile-kernel-codegen -- --write` from " ++ title ++ ". Do not edit.\n\n" ++ preamble(lang);
        var has_silu = false;
        var has_f16 = false;
        var has_bf16 = false;
        for (std.meta.declarations(kernels)) |decl_name| {
            const k = @field(kernels, decl_name);
            if (@TypeOf(k) == gemm.Gemm) {
                has_silu = has_silu or k.epilogue.uses(.silu);
                has_f16 = has_f16 or k.epilogue.uses(.round_f16);
                has_bf16 = has_bf16 or k.epilogue.uses(.round_bf16);
            }
        }
        if (has_silu) s = s ++ epilogueHelpers(fexpr.acc.siluOf(), lang);
        if (has_f16) s = s ++ epilogueHelpers(fexpr.acc.roundF16(), lang);
        if (has_bf16) s = s ++ epilogueHelpers(fexpr.acc.roundBf16(), lang);
        for (std.meta.declarations(kernels)) |decl_name| {
            const k = @field(kernels, decl_name);
            const T = @TypeOf(k);
            s = s ++ "\n" ++ (if (T == gemm.Gemm)
                renderGemm(k, lang)
            else if (T == norm.LayerNorm)
                renderLayerNorm(k, lang)
            else if (T == norm.RmsNorm)
                renderRmsNorm(k, lang)
            else if (T == norm.MaskedMean)
                renderMaskedMean(k, lang)
            else if (T == layout.Copy)
                renderCopy(k, lang)
            else if (T == attention.Attention)
                renderAttention(k, lang)
            else if (T == layout.Im2col)
                renderIm2col(k, lang)
            else if (T == layout.Embed)
                renderEmbed(k, lang)
            else
                "");
        }
        return std.mem.trimEnd(u8, s, "\n") ++ "\n";
    }
}

/// Compatibility adapters for existing native schedules. Binding positions and
/// attributes are data; the schedule's body remains target-owned.
pub const NativeBuffer = struct { name: []const u8, writable: bool = false, element: []const u8 = "float" };
pub fn nativeMetalExport(comptime name: []const u8, comptime buffers: []const NativeBuffer, comptime params_type: []const u8, comptime attributes: []const []const u8, comptime body: []const u8) []const u8 {
    const l: L = .{ .lang = .metal };
    var args: []const []const u8 = &.{};
    for (buffers, 0..) |buffer, slot| args = args ++ .{if (buffer.writable) fmt("device {s} *{s} [[buffer({d})]]", .{ buffer.element, buffer.name, slot }) else l.in(buffer.element, buffer.name, slot)};
    args = args ++ .{l.params(params_type, buffers.len)};
    return l.kernel(name, 0, args, attributes) ++ body ++ "}\n";
}

pub const RotaryPlane = struct { input: []const u8, output: []const u8, prefix: []const u8 = "" };
pub fn nativeMetalRotary(comptime name: []const u8, comptime planes: []const RotaryPlane) []const u8 {
    if (planes.len == 0) @compileError("rotary program requires an output plane");
    var buffers: []const NativeBuffer = &.{};
    var copies: []const u8 = "";
    var loads: []const u8 = "";
    var lower: []const u8 = "";
    var upper: []const u8 = "";
    for (planes) |p| {
        buffers = buffers ++ [_]NativeBuffer{.{ .name = p.input }};
        copies = copies ++ p.output ++ "[gid] = " ++ p.input ++ "[gid]; ";
        const x0 = p.prefix ++ "x0";
        const x1 = p.prefix ++ "x1";
        loads = loads ++ "    float " ++ x0 ++ " = " ++ p.input ++ "[base + idx0]; float " ++ x1 ++ " = " ++ p.input ++ "[base + idx1];\n";
        const bindings = .{ .value = x0, .other = x1, .cosine = "cos_val", .sine = "sin_val" };
        lower = lower ++ p.output ++ "[gid] = " ++ @import("native_math.zig").rotary(false).renderNative(.{}, bindings, null) ++ "; ";
        upper = upper ++ p.output ++ "[gid] = " ++ @import("native_math.zig").rotary(true).renderNative(.{}, bindings, null) ++ "; ";
    }
    buffers = buffers ++ [_]NativeBuffer{.{ .name = "positions", .element = "uint" }};
    for (planes) |p| buffers = buffers ++ [_]NativeBuffer{.{ .name = p.output, .writable = true }};
    const body = "    uint total = p.total_chunks * p.head_dim; if (gid >= total) return; uint chunk = gid / p.head_dim; uint d = gid - chunk * p.head_dim; uint base = chunk * p.head_dim;\n" ++
        "    if (p.rope_dim < 2u) { " ++ copies ++ "return; }\n    uint idx0 = 0u; uint idx1 = 0u; uint j = 0u;\n" ++
        "    if (!termite_rope_pair_indices(d, p.head_dim, p.rope_dim, p.consecutive_pairs, idx0, idx1, j)) { " ++ copies ++ "return; }\n" ++
        "    float pos = float(positions[chunk]); float freq = 1.0f / pow(p.theta, float(2u * j) / float(p.rope_dim)); float angle = pos * p.freq_scale * freq; float cos_val = cos(angle); float sin_val = sin(angle);\n" ++ loads ++
        "    if (d == idx0) { " ++ lower ++ "} else if (d == idx1) { " ++ upper ++ "} else { " ++ copies ++ "}\n";
    return nativeMetalExport(name, buffers, "termite_metal_rope_params", &.{"uint gid [[thread_position_in_grid]]"}, body);
}
