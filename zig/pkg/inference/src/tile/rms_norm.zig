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

//! The production RMSNorm family as tile descriptions.
//!
//! Every kernel computes inv = rsqrt(sum(x^2) / dim + eps) per row and then
//! one or more outputs from an `NExpr` over x, inv, weights, a residual and
//! a scale. Kernels differ in schedule (one thread, one thread per row,
//! block tree, float4 + SIMD-group sums), where sum(x^2) comes from, the
//! epilogue, and their ABI (names, buffer order, params). One table holds
//! both backends; one renderer emits Metal (into `metal_kernels.m`) and CUDA
//! (into `inference_cuda_kernels.cu`) with each kernel's ABI unchanged, so
//! host dispatch is untouched.

const std = @import("std");

const fmt = std.fmt.comptimePrint;

/// Epilogue expression. Rendering adds parentheses only where C precedence
/// needs them, so `x * inv * w + r` and `x * (inv * w)` keep their order.
pub const NExpr = union(enum) {
    x,
    inv,
    mean,
    weight: u8,
    residual,
    scale,
    /// The value materialized by `Norm.staged`.
    staged,
    /// Element of a row-major buffer at this row and column.
    load: []const u8,
    /// Element of a per-column vector.
    col: []const u8,
    add: [2]*const NExpr,
    sub: [2]*const NExpr,
    mul: [2]*const NExpr,

    fn box(comptime e: NExpr) *const NExpr {
        return &struct {
            const value = e;
        }.value;
    }
    pub fn times(comptime a: NExpr, comptime b: NExpr) NExpr {
        return .{ .mul = .{ box(a), box(b) } };
    }
    pub fn plus(comptime a: NExpr, comptime b: NExpr) NExpr {
        return .{ .add = .{ box(a), box(b) } };
    }
    pub fn minus(comptime a: NExpr, comptime b: NExpr) NExpr {
        return .{ .sub = .{ box(a), box(b) } };
    }

    pub const Names = struct {
        x: []const u8,
        inv: []const u8 = "inv",
        weights: [3][]const u8 = .{ "", "", "" },
        residual: []const u8 = "",
        scale: []const u8 = "",
        /// Row offset prefix for `.load`, e.g. "base + ".
        base: []const u8 = "",
    };

    pub fn render(comptime e: NExpr, comptime n: Names) []const u8 {
        return renderPrec(e, n, 0, false);
    }

    fn renderPrec(comptime e: NExpr, comptime n: Names, comptime parent: u8, comptime right: bool) []const u8 {
        const prec: u8 = switch (e) {
            .add, .sub => 1,
            .mul => 2,
            else => 3,
        };
        const body = switch (e) {
            .x => n.x,
            .inv => n.inv,
            .weight => |k| n.weights[k],
            .residual => n.residual,
            .scale => n.scale,
            .mean => "mean",
            .staged => "staged",
            .load => |name| name ++ "[" ++ n.base ++ "i]",
            .col => |name| name ++ "[i]",
            .add => |xs| renderPrec(xs[0].*, n, 1, false) ++ " + " ++ renderPrec(xs[1].*, n, 1, true),
            .sub => |xs| renderPrec(xs[0].*, n, 1, false) ++ " - " ++ renderPrec(xs[1].*, n, 1, true),
            .mul => |xs| renderPrec(xs[0].*, n, 2, false) ++ " * " ++ renderPrec(xs[1].*, n, 2, true),
        };
        return if (prec < parent or (prec == parent and right)) "(" ++ body ++ ")" else body;
    }
};

const x: NExpr = .x;
const inv: NExpr = .inv;
const residual: NExpr = .residual;
const scale: NExpr = .scale;
fn weight(comptime k: u8) NExpr {
    return .{ .weight = k };
}

pub const Backend = enum { metal, cuda };

pub const Schedule = enum {
    /// Thread 0 does the whole (single) row.
    serial_single,
    /// One thread per row.
    serial_rows,
    /// One block per row, shared-memory tree over the block.
    tree,
    /// One block per row, float4 loads, SIMD-group sums (Metal).
    simd4,
    /// One SIMD-group per row: row = gid / 32 (Metal).
    simd_row,
};

pub const Stats = enum {
    /// inv = rsqrt(sum(x^2) / dim + eps).
    rms,
    /// mean, then inv = rsqrt(sum((x - mean)^2) / dim + eps).
    mean_var_two_pass,
    /// sum and sum of squares in one pass; var = max(sq / dim - mean^2, 0).
    mean_var_one_pass,
};

pub const Rows = enum {
    /// A single row; no row index.
    single,
    /// Row = block index, not bounds-checked (params have no row count).
    grid,
    /// Row = block index, checked against the row count.
    grid_checked,
};

pub const Role = union(enum) {
    input,
    input_half,
    weight: u8,
    residual,
    output: u8,
    /// bf16 copy of output k (CUDA).
    output_bf16: u8,
    /// Per-row partial sums of squares, one per 8 elements.
    sumsq,
    /// Receives inv only.
    inv_out,
    /// One-element scale buffer (CUDA).
    scalar,
    /// Metal: the `constant` params struct. CUDA: scalar arguments.
    params,
    /// Any other read-only float buffer, referenced by `.load`/`.col`.
    data,
    /// Receives x during the first pass; later passes read it back.
    stash,
};

pub const Binding = struct { name: []const u8, role: Role };

pub const Norm = struct {
    name: []const u8,
    backend: Backend,
    schedule: Schedule,
    rows: Rows = .single,
    stats: Stats = .rms,
    /// The normalized value; defaults to the `.input` (or `.input_half`) element.
    x: ?NExpr = null,
    /// Tree schedule: guard threads and strides at 256 (shared memory holds
    /// two 256-float halves).
    tree_cap: bool = false,
    /// Metal params struct.
    params_type: []const u8 = "",
    /// Params as written in the body: Metal fields, or CUDA argument names.
    p_rows: []const u8 = "p.rows",
    p_dim: []const u8 = "p.hidden_size",
    p_eps: []const u8 = "p.eps",
    p_scale: []const u8 = "p.scale",
    bindings: []const Binding,
    /// One expression per `.output` binding, in output index order.
    outputs: []const NExpr = &.{},
    /// simd4: clamp with `p.lo`/`p.hi` when `p.flags` bits 0/1 are set.
    clamp_flags: bool = false,
    /// simd4: shared memory is a local array, not a `threadgroup(0)` buffer.
    local_shared: bool = false,
    /// simd4: materialize this value in the output buffer (volatile store,
    /// device barrier, reload) before evaluating `outputs` over `.staged`,
    /// so the rounding matches the unfused chain.
    staged: ?NExpr = null,
    /// Emitted before the kernel (params structs only this kernel uses).
    decl: ?[]const u8 = null,

    fn binding(comptime k: Norm, comptime role: std.meta.Tag(Role), comptime index: u8) ?[]const u8 {
        for (k.bindings) |b| {
            if (b.role != role) continue;
            switch (b.role) {
                .weight, .output, .output_bf16 => |i| if (i == index) return b.name,
                else => return b.name,
            }
        }
        return null;
    }

    fn need(comptime k: Norm, comptime role: std.meta.Tag(Role), comptime index: u8) []const u8 {
        return k.binding(role, index) orelse @compileError(k.name ++ ": no binding for " ++ @tagName(role));
    }

    fn inputName(comptime k: Norm) []const u8 {
        return k.binding(.input_half, 0) orelse k.need(.input, 0);
    }
};

const Dialect = struct {
    shared: []const u8,
    barrier: []const u8,
    rsqrt: []const u8,

    fn of(comptime b: Backend) Dialect {
        return switch (b) {
            .metal => .{ .shared = "", .barrier = "threadgroup_barrier(mem_flags::mem_threadgroup);", .rsqrt = "rsqrt" },
            .cuda => .{ .shared = "    __shared__ float shmem[256];\n", .barrier = "__syncthreads();", .rsqrt = "rsqrtf" },
        };
    }
};

fn signature(comptime k: Norm) []const u8 {
    var args: []const []const u8 = &.{};
    for (k.bindings, 0..) |b, slot| switch (k.backend) {
        .metal => args = args ++ .{switch (b.role) {
            .input, .weight, .residual, .sumsq, .data => fmt("device const float *{s} [[buffer({d})]]", .{ b.name, slot }),
            .input_half => fmt("device const half *{s} [[buffer({d})]]", .{ b.name, slot }),
            .output, .inv_out, .stash => fmt("device float *{s} [[buffer({d})]]", .{ b.name, slot }),
            .params => fmt("constant {s} &p [[buffer({d})]]", .{ k.params_type, slot }),
            .output_bf16, .scalar => @compileError(k.name ++ ": CUDA-only binding"),
        }},
        .cuda => args = args ++ switch (b.role) {
            .input, .weight, .residual, .sumsq, .scalar, .data => .{"const float* " ++ b.name},
            .output, .inv_out, .stash => .{"float* " ++ b.name},
            .output_bf16 => .{"unsigned short* " ++ b.name},
            .params => (if (k.rows == .grid_checked) .{"unsigned int " ++ k.p_rows} else .{}) ++ .{ "unsigned int " ++ k.p_dim, "float " ++ k.p_eps },
            .input_half => @compileError(k.name ++ ": Metal-only binding"),
        },
    };
    var s: []const u8 = switch (k.backend) {
        .metal => "kernel void " ++ k.name ++ "(",
        .cuda => "extern \"C\" __global__ void " ++ k.name ++ "(",
    };
    for (args, 0..) |a, i| s = s ++ (if (i == 0) "" else ", ") ++ a;
    if (k.backend == .cuda) {
        if (k.schedule != .tree) @compileError(k.name ++ ": CUDA renders the tree schedule only");
        return s ++ ") {\n    const unsigned int row = blockIdx.x; const unsigned int tid = threadIdx.x; const unsigned int tpg = blockDim.x; (void)row;\n";
    }
    s = s ++ switch (k.schedule) {
        .serial_single, .serial_rows => ", uint gid [[thread_position_in_grid]]",
        .simd_row => ", uint gid [[thread_position_in_grid]], ushort lane [[thread_index_in_simdgroup]]",
        .tree => ", threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]" ++
            (if (k.rows == .single) "" else ", uint row [[threadgroup_position_in_grid]]"),
        .simd4 => (if (k.local_shared) "" else ", threadgroup float *shmem [[threadgroup(0)]]") ++
            ", uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint simd_group [[simdgroup_index_in_threadgroup]], uint simd_lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]",
    };
    return s ++ ") {\n";
}

/// Names for scalar loops: x is the per-iteration local `v`.
fn scalarNames(comptime k: Norm, comptime base: []const u8) NExpr.Names {
    var n: NExpr.Names = .{ .x = "v", .base = base, .scale = if (k.binding(.scalar, 0)) |s| s ++ "[0]" else k.p_scale };
    for (k.bindings) |b| switch (b.role) {
        .weight => |w| n.weights[w] = b.name ++ "[i]",
        .residual => n.residual = b.name ++ "[" ++ base ++ "i]",
        else => {},
    };
    return n;
}

/// `const float v = <x>;` for one loop iteration. With a stash, the first
/// pass computes and stores x and later passes read it back.
fn loadX(comptime k: Norm, comptime base: []const u8, comptime first_pass: bool) []const u8 {
    if (k.binding(.stash, 0)) |stash| {
        if (!first_pass) return fmt("const float v = {s}[{s}i];", .{ stash, base });
        return fmt("const float v = {s}; {s}[{s}i] = v;", .{ xSource(k, base), stash, base });
    }
    return "const float v = " ++ xSource(k, base) ++ ";";
}

fn xSource(comptime k: Norm, comptime base: []const u8) []const u8 {
    if (k.x) |e| return e.render(scalarNames(k, base));
    if (k.binding(.input_half, 0)) |h| return "float(" ++ h ++ "[" ++ base ++ "i])";
    return k.need(.input, 0) ++ "[" ++ base ++ "i]";
}

/// Sum of squares into `sum`, for the scalar schedules.
fn scalarSum(comptime k: Norm, comptime first: []const u8, comptime step: []const u8, comptime base: []const u8) []const u8 {
    if (k.binding(.sumsq, 0)) |sumsq| return fmt(
        \\    const unsigned int partial_count = ({0s} + 7u) / 8u;
        \\    float sum = 0.0f;
        \\    for (unsigned int i = {1s}; i < partial_count; {2s}) sum += {3s}[{4s}i];
        \\
    , .{ k.p_dim, first, step, sumsq, if (k.rows == .single) "" else "row * partial_count + " });
    return fmt(
        \\    float sum = 0.0f;
        \\    for (unsigned int i = {0s}; i < {1s}; {2s}) {{ {3s} sum += v * v; }}
        \\
    , .{ first, k.p_dim, step, loadX(k, base, true) });
}

/// Mean and centered variance (two passes) into `mean` and `inv`.
fn twoPassStats(comptime k: Norm, comptime first: []const u8, comptime step: []const u8, comptime base: []const u8, comptime reduce: []const u8, comptime rsqrt: []const u8) []const u8 {
    return fmt(
        \\    float sum = 0.0f;
        \\    for (unsigned int i = {0s}; i < {1s}; {2s}) {{ {3s} sum += v; }}
        \\    const float mean = {6s}(sum) / float({1s});
        \\    float sq = 0.0f;
        \\    for (unsigned int i = {0s}; i < {1s}; {2s}) {{ {4s} const float d = v - mean; sq += d * d; }}
        \\    const float inv = {7s}({6s}(sq) / float({1s}) + {5s});
        \\
    , .{ first, k.p_dim, step, loadX(k, base, true), loadX(k, base, false), k.p_eps, reduce, rsqrt });
}

fn scalarOutputs(comptime k: Norm, comptime first: []const u8, comptime step: []const u8, comptime base: []const u8) []const u8 {
    const names = scalarNames(k, base);
    var body: []const u8 = "";
    for (k.outputs, 0..) |e, o| {
        const out = k.need(.output, @intCast(o));
        if (k.binding(.output_bf16, @intCast(o))) |half_out| {
            body = body ++ fmt(" const float y{0d} = {1s}; {2s}[{3s}i] = y{0d}; {4s}[{3s}i] = termite_f32_to_bf16(y{0d});", .{ o, e.render(names), out, base, half_out });
        } else body = body ++ fmt(" {s}[{s}i] = {s};", .{ out, base, e.render(names) });
    }
    return fmt("    for (unsigned int i = {s}; i < {s}; {s}) {{ {s}{s} }}\n", .{ first, k.p_dim, step, loadX(k, base, false), body });
}

pub fn render(comptime k: Norm) []const u8 {
    comptime {
        @setEvalBranchQuota(200_000);
        const d = Dialect.of(k.backend);
        var s: []const u8 = (k.decl orelse "") ++ signature(k);
        switch (k.schedule) {
            .serial_single, .serial_rows => {
                const base = if (k.schedule == .serial_single) "" else "base + ";
                s = s ++ (if (k.schedule == .serial_single) "    if (gid != 0u) return;\n" else if (k.rows == .grid) "" else fmt("    if (gid >= {s}) return;\n", .{k.p_rows}));
                s = s ++ fmt("    if ({s} == 0u) return;\n", .{k.p_dim});
                if (k.schedule == .serial_rows) s = s ++ fmt("    const unsigned int base = gid * {s};\n", .{k.p_dim});
                switch (k.stats) {
                    .rms => {
                        s = s ++ scalarSum(k, "0u", "++i", base);
                        s = s ++ fmt("    const float inv = {s}(sum / float({s}) + {s});\n", .{ d.rsqrt, k.p_dim, k.p_eps });
                    },
                    .mean_var_two_pass => s = s ++ twoPassStats(k, "0u", "++i", base, "", d.rsqrt),
                    .mean_var_one_pass => @compileError(k.name ++ ": one-pass stats need the tree schedule"),
                }
                s = s ++ scalarOutputs(k, "0u", "++i", base);
            },
            .simd_row => {
                if (k.backend != .metal or k.stats != .mean_var_two_pass) @compileError(k.name ++ ": simd_row renders Metal two-pass stats");
                s = s ++ fmt("    const uint row = gid / 32u;\n    if ({s} == 0u) return;\n    const uint base = row * {s};\n", .{ k.p_dim, k.p_dim });
                s = s ++ twoPassStats(k, "lane", "i += 32u", "base + ", "simd_sum", "rsqrt");
                s = s ++ scalarOutputs(k, "lane", "i += 32u", "base + ");
            },
            .tree => {
                const base = if (k.rows == .single) "" else "base + ";
                s = s ++ (if (k.backend == .cuda) (if (k.stats == .mean_var_one_pass) "    __shared__ float shmem[512];\n" else d.shared) else "");
                s = s ++ (if (k.rows == .grid_checked) fmt("    if (row >= {s}) return;\n", .{k.p_rows}) else "");
                s = s ++ (if (k.tree_cap) fmt("    if ({s} == 0u || tpg == 0u || tid >= 256u) return;\n", .{k.p_dim}) else fmt("    if ({s} == 0u) return;\n", .{k.p_dim}));
                if (k.rows != .single) s = s ++ fmt("    const unsigned int base = row * {s};\n", .{k.p_dim});
                if (k.stats == .mean_var_one_pass) {
                    s = s ++ treeOnePass(k, base, d);
                } else {
                    if (k.stats != .rms) @compileError(k.name ++ ": tree renders rms or one-pass stats");
                    s = s ++ scalarSum(k, "tid", "i += tpg", base);
                    s = s ++ fmt(
                        \\    shmem[tid] = sum;
                        \\    {0s}
                        \\    for (unsigned int stride = tpg >> 1; stride > 0u; stride >>= 1u) {{ if (tid < stride) shmem[tid] += shmem[tid + stride]; {0s} }}
                        \\    const float inv = {1s}(shmem[0] / float({2s}) + {3s});
                        \\
                    , .{ d.barrier, d.rsqrt, k.p_dim, k.p_eps });
                }
                if (k.binding(.inv_out, 0)) |inv_out| {
                    s = s ++ fmt("    if (tid == 0u) {s}[0] = inv;\n", .{inv_out});
                } else s = s ++ scalarOutputs(k, "tid", "i += tpg", base);
            },
            .simd4 => s = s ++ renderSimd4(k),
        }
        return s ++ "}\n";
    }
}

/// Sum and sum of squares over the block; shared memory holds two 256-float halves.
fn treeOnePass(comptime k: Norm, comptime base: []const u8, comptime d: Dialect) []const u8 {
    const cap = if (k.tree_cap) " && tid + stride < 256u" else "";
    return fmt(
        \\    float sum = 0.0f;
        \\    float sq = 0.0f;
        \\    for (unsigned int i = tid; i < {0s}; i += tpg) {{ {1s} sum += v; sq += v * v; }}
        \\    shmem[tid] = sum; shmem[256u + tid] = sq;
        \\    {2s}
        \\    for (unsigned int stride = tpg >> 1; stride > 0u; stride >>= 1u) {{ if (tid < stride{3s}) {{ shmem[tid] += shmem[tid + stride]; shmem[256u + tid] += shmem[256u + tid + stride]; }} {2s} }}
        \\    const float mean = shmem[0] / float({0s});
        \\    const float var = {4s}(shmem[256u] / float({0s}) - mean * mean, 0.0f);
        \\    const float inv = {5s}(var + {6s});
        \\
    , .{ k.p_dim, loadX(k, base, true), d.barrier, cap, if (k.backend == .cuda) "fmaxf" else "max", d.rsqrt, k.p_eps });
}

fn float4At(comptime buf: []const u8, comptime idx: []const u8) []const u8 {
    return fmt("float4({0s}[{1s}], {0s}[{1s} + 1u], {0s}[{1s} + 2u], {0s}[{1s} + 3u])", .{ buf, idx });
}

fn store4(comptime buf: []const u8, comptime value: []const u8) []const u8 {
    return fmt("{0s}[i] = {1s}.x; {0s}[i + 1u] = {1s}.y; {0s}[i + 2u] = {1s}.z; {0s}[i + 3u] = {1s}.w;", .{ buf, value });
}

fn renderSimd4(comptime k: Norm) []const u8 {
    if (k.backend != .metal) @compileError(k.name ++ ": simd4 is Metal-only");
    const input = k.inputName();
    var s: []const u8 = if (k.local_shared) "    threadgroup float shmem[32];\n" else "";
    s = s ++ fmt(
        \\    if (row >= {0s} || {1s} == 0u || ({1s} & 3u) != 0u) return;
        \\    if (simd_group == 0u) shmem[simd_lane] = 0.0f;
        \\    const uint base = row * {1s};
        \\    const uint vectors = {1s} >> 2u;
        \\    float sum = 0.0f;
        \\    for (uint v = tid; v < vectors; v += tpg) {{ const uint i = base + (v << 2u); const float4 x = {2s}; sum += dot(x, x); }}
        \\    sum = simd_sum(sum);
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (simd_lane == 0u) shmem[simd_group] = sum;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    sum = simd_sum(shmem[simd_lane]);
        \\    const float inv = rsqrt(sum / float({1s}) + {3s});
        \\
    , .{ k.p_rows, k.p_dim, float4At(input, "i"), k.p_eps });
    var names: NExpr.Names = .{ .x = "x", .scale = k.p_scale };
    var weight_loads: []const u8 = "";
    for (k.bindings) |b| switch (b.role) {
        .weight => |w| {
            names.weights[w] = fmt("w{d}", .{w});
            weight_loads = weight_loads ++ fmt(" const float4 w{d} = {s};", .{ w, float4At(b.name, "j") });
        },
        .residual => names.residual = "r",
        else => {},
    };
    const head = "    for (uint v = tid; v < vectors; v += tpg) { const uint j = v << 2u; const uint i = base + j;";
    const residual_load = if (k.binding(.residual, 0)) |r| fmt(" const float4 r = {s};", .{float4At(r, "i")}) else "";
    if (k.staged) |first| {
        // Store the staged value, then finish from the reloaded copy.
        const out = k.need(.output, 0);
        s = s ++ fmt("    device volatile float *rounded = {s};\n", .{out});
        s = s ++ fmt("{s} const float4 x = {s};{s} const float4 staged = {s}; {s} }}\n", .{ head, float4At(input, "i"), weight_loads, first.render(names), store4("rounded", "staged") });
        s = s ++ "    threadgroup_barrier(mem_flags::mem_device);\n";
        return s ++ fmt("{s} const float4 staged = {s};{s} const float4 y = {s}; {s} }}\n", .{ head, float4At("rounded", "i"), residual_load, k.outputs[0].render(names), store4(out, "y") });
    }
    var body: []const u8 = "";
    for (k.outputs, 0..) |e, o| {
        const y = fmt("y{d}", .{o});
        body = body ++ fmt(" float4 {s} = {s};", .{ y, e.render(names) });
        if (k.clamp_flags) body = body ++ fmt(" if ((p.flags & 1u) != 0u) {0s} = max({0s}, float4(p.lo)); if ((p.flags & 2u) != 0u) {0s} = min({0s}, float4(p.hi));", .{y});
        body = body ++ " " ++ store4(k.need(.output, @intCast(o)), y);
    }
    return s ++ fmt("{s} const float4 x = {s};{s}{s}{s} }}\n", .{ head, float4At(input, "i"), weight_loads, residual_load, body });
}

// ---------------------------------------------------------------- kernels

fn bind(comptime name: []const u8, comptime role: Role) Binding {
    return .{ .name = name, .role = role };
}
const p_struct = bind("p", .params);
const in_w_out: []const Binding = &.{ bind("input", .input), bind("weight", .{ .weight = 0 }), bind("output", .{ .output = 0 }), p_struct };
const in_w_res_out: []const Binding = &.{ bind("input", .input), bind("weight", .{ .weight = 0 }), bind("residual", .residual), bind("output", .{ .output = 0 }), p_struct };
const in_w_res_out_sumsq: []const Binding = &.{ bind("input", .input), bind("weight", .{ .weight = 0 }), bind("residual", .residual), bind("output", .{ .output = 0 }), bind("sumsq", .sumsq), p_struct };

const normed = x.times(inv).times(weight(0));
const normed_add = normed.plus(residual);
const normed_add_scale = normed_add.times(scale);
const normed4 = x.times(inv.times(weight(0)));

const layer_params = "termite_metal_apply_layer_norm_params";
const layer_scale_params = "termite_metal_apply_layer_norm_scale_params";
const row_params = "termite_metal_apply_row_norm_params";
const row_scale_params = "termite_metal_apply_row_norm_scale_params";

/// CUDA kernels: one block per row, `rows`/`dim`/`eps` scalar arguments.
fn cuda(comptime name: []const u8, comptime bindings: []const Binding, comptime outputs: []const NExpr) Norm {
    return .{ .name = name, .backend = .cuda, .schedule = .tree, .rows = .grid_checked, .p_rows = "rows", .p_dim = "dim", .p_eps = "eps", .bindings = bindings, .outputs = outputs };
}
const c_params = bind("", .params);

pub const kernels = [_]Norm{
    // ---- Metal
    .{ .name = "termite_apply_rms_norm_1x", .backend = .metal, .schedule = .serial_single, .params_type = layer_params, .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_apply_rms_norm_1x_reduce", .backend = .metal, .schedule = .tree, .params_type = layer_params, .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_compute_rms_inv_scale_1x", .backend = .metal, .schedule = .tree, .params_type = layer_params, .bindings = &.{ bind("input", .input), bind("inv_scale", .inv_out), p_struct } },
    .{ .name = "termite_apply_rms_norm_rows", .backend = .metal, .schedule = .serial_rows, .params_type = row_params, .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_apply_rms_norm_rows_reduce", .backend = .metal, .schedule = .tree, .rows = .grid_checked, .params_type = row_params, .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_apply_rms_norm_rows_simd4_reduce", .backend = .metal, .schedule = .simd4, .rows = .grid_checked, .params_type = row_params, .bindings = in_w_out, .outputs = &.{normed4} },
    .{
        .name = "antfly_norm_clamp",
        .backend = .metal,
        .schedule = .simd4,
        .rows = .grid_checked,
        .params_type = "antfly_norm_clamp_params",
        .p_dim = "p.dim",
        .bindings = in_w_out,
        .outputs = &.{normed4},
        .clamp_flags = true,
        .local_shared = true,
        .decl = "struct antfly_norm_clamp_params {uint rows,dim,flags,reserved;float eps,lo,hi,padding;};\n",
    },
    .{
        .name = "termite_apply_rms_norm_rows_triple_simd4_reduce",
        .backend = .metal,
        .schedule = .simd4,
        .rows = .grid_checked,
        .params_type = row_params,
        .bindings = &.{ bind("input", .input), bind("first_weight", .{ .weight = 0 }), bind("second_weight", .{ .weight = 1 }), bind("third_weight", .{ .weight = 2 }), bind("first_output", .{ .output = 0 }), bind("second_output", .{ .output = 1 }), bind("third_output", .{ .output = 2 }), p_struct },
        .outputs = &.{ normed4, x.times(inv.times(weight(1))), x.times(inv.times(weight(2))) },
    },
    .{ .name = "termite_apply_rms_norm_add_1x", .backend = .metal, .schedule = .tree, .params_type = layer_params, .bindings = in_w_res_out, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_rows_canonical", .backend = .metal, .schedule = .tree, .rows = .grid, .params_type = layer_params, .bindings = in_w_res_out, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_1x_sumsq", .backend = .metal, .schedule = .tree, .params_type = layer_params, .bindings = in_w_res_out_sumsq, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_rows_sumsq", .backend = .metal, .schedule = .tree, .rows = .grid, .params_type = layer_params, .bindings = in_w_res_out_sumsq, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_scale_1x_sumsq", .backend = .metal, .schedule = .tree, .params_type = layer_scale_params, .bindings = in_w_res_out_sumsq, .outputs = &.{normed_add_scale} },
    .{ .name = "termite_apply_rms_norm_add_1x_f16_input", .backend = .metal, .schedule = .tree, .params_type = layer_params, .bindings = &.{ bind("input", .input_half), bind("weight", .{ .weight = 0 }), bind("residual", .residual), bind("output", .{ .output = 0 }), p_struct }, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_scale_1x", .backend = .metal, .schedule = .tree, .params_type = layer_scale_params, .bindings = in_w_res_out, .outputs = &.{normed_add_scale} },
    .{ .name = "termite_apply_rms_norm_add_scale_rows", .backend = .metal, .schedule = .tree, .rows = .grid_checked, .params_type = row_scale_params, .bindings = in_w_res_out, .outputs = &.{normed_add_scale} },
    // Normalized values round through the output buffer before the residual
    // add, matching the unfused chain. `* p.scale` replaces the previous
    // `if (p.scale != 1.0f)`: multiplying by 1.0 is exact.
    .{ .name = "termite_apply_rms_norm_add_scale_simd4", .backend = .metal, .schedule = .simd4, .rows = .grid_checked, .params_type = row_scale_params, .bindings = in_w_res_out, .staged = normed4, .outputs = &.{(NExpr{ .staged = {} }).plus(residual).times(scale)} },

    // ---- CUDA (argument order is the host launch ABI in ops/cuda/kernels.zig)
    cuda("termite_rms_norm_f32", &.{ bind("dst", .{ .output = 0 }), bind("input", .input), bind("weight", .{ .weight = 0 }), c_params }, &.{normed}),
    cuda("termite_rms_norm_f32_bf16", &.{ bind("dst", .{ .output = 0 }), bind("dst_bf16", .{ .output_bf16 = 0 }), bind("input", .input), bind("weight", .{ .weight = 0 }), c_params }, &.{normed}),
    cuda("termite_rms_norm_add_f32", &.{ bind("dst", .{ .output = 0 }), bind("input", .input), bind("weight", .{ .weight = 0 }), bind("residual", .residual), c_params }, &.{normed_add}),
    cuda("termite_rms_norm_add_f32_bf16", &.{ bind("dst", .{ .output = 0 }), bind("dst_bf16", .{ .output_bf16 = 0 }), bind("input", .input), bind("weight", .{ .weight = 0 }), bind("residual", .residual), c_params }, &.{normed_add}),
    cuda("termite_rms_norm_add_mul_scalar_f32", &.{ bind("dst", .{ .output = 0 }), bind("input", .input), bind("weight", .{ .weight = 0 }), bind("residual", .residual), bind("scalar", .scalar), c_params }, &.{normed.times(scale).plus(residual)}),
    cuda("termite_rms_norm_add_output_scale_f32", &.{ bind("dst", .{ .output = 0 }), bind("input", .input), bind("weight", .{ .weight = 0 }), bind("residual", .residual), bind("scalar", .scalar), c_params }, &.{normed_add_scale}),
    cuda("termite_rms_norm_bare_f32", &.{ bind("dst", .{ .output = 0 }), bind("input", .input), c_params }, &.{x.times(inv)}),
    // Gemma 4 A4B branches three RMSNorms from the same attention residual.
    cuda("termite_gemma4_a4b_rms_norm_triple_f32", &.{ bind("first", .{ .output = 0 }), bind("second", .{ .output = 1 }), bind("third", .{ .output = 2 }), bind("input", .input), bind("first_weight", .{ .weight = 0 }), bind("second_weight", .{ .weight = 1 }), bind("third_weight", .{ .weight = 2 }), c_params }, &.{ normed, x.times(inv).times(weight(1)), x.times(inv).times(weight(2)) }),
};

// ---- LayerNorm: ((x - mean) * inv) * gamma + beta

const ln_out = x.minus(.mean).times(inv).times(weight(0)).plus(weight(1));
const ln_gamma_beta = .{ bind("gamma", .{ .weight = 0 }), bind("beta", .{ .weight = 1 }) };
const ln_add: NExpr = (NExpr{ .load = "a" }).plus(.{ .load = "b" });
const ln_bias_add: NExpr = (NExpr{ .load = "projected" }).plus(.{ .col = "bias" }).plus(.{ .load = "residual" });
const ln_bias_add_bindings: []const Binding = &(.{ bind("projected", .data), bind("bias", .data), bind("residual", .data) } ++ ln_gamma_beta ++ .{ bind("output", .{ .output = 0 }), p_struct });

fn metalLayerNorm(comptime name: []const u8, comptime schedule: Schedule, comptime bindings: []const Binding, comptime input: ?NExpr) Norm {
    const tree = schedule == .tree;
    return .{
        .name = name,
        .backend = .metal,
        .schedule = schedule,
        .rows = .grid,
        .stats = if (tree) .mean_var_one_pass else .mean_var_two_pass,
        .tree_cap = tree,
        .params_type = layer_params,
        .x = input,
        .bindings = bindings,
        .outputs = &.{ln_out},
    };
}

pub const layer_norm_kernels = [_]Norm{
    metalLayerNorm("termite_apply_layer_norm_1x", .serial_rows, &(.{bind("input", .input)} ++ ln_gamma_beta ++ .{ bind("output", .{ .output = 0 }), p_struct }), null),
    metalLayerNorm("termite_apply_layer_norm_rows", .tree, &(.{bind("input", .input)} ++ ln_gamma_beta ++ .{ bind("output", .{ .output = 0 }), p_struct }), null),
    metalLayerNorm("termite_apply_add_layer_norm_1x", .serial_rows, &(.{ bind("a", .data), bind("b", .data) } ++ ln_gamma_beta ++ .{ bind("output", .{ .output = 0 }), p_struct }), ln_add),
    metalLayerNorm("termite_apply_add_layer_norm_sum_1x", .serial_rows, &(.{ bind("a", .data), bind("b", .data) } ++ ln_gamma_beta ++ .{ bind("sum_out", .stash), bind("output", .{ .output = 0 }), p_struct }), ln_add),
    metalLayerNorm("termite_apply_add_layer_norm_rows", .tree, &(.{ bind("a", .data), bind("b", .data) } ++ ln_gamma_beta ++ .{ bind("output", .{ .output = 0 }), p_struct }), ln_add),
    metalLayerNorm("termite_apply_add_layer_norm_sum_rows", .tree, &(.{ bind("a", .data), bind("b", .data) } ++ ln_gamma_beta ++ .{ bind("sum_out", .stash), bind("output", .{ .output = 0 }), p_struct }), ln_add),
    metalLayerNorm("termite_apply_bias_add_layer_norm_1x", .serial_rows, ln_bias_add_bindings, ln_bias_add),
    metalLayerNorm("termite_apply_bias_add_layer_norm_rows", .tree, ln_bias_add_bindings, ln_bias_add),
    metalLayerNorm("termite_gliner_boundary_ffn_bias_add_layer_norm", .simd_row, ln_bias_add_bindings, ln_bias_add),
    .{ .name = "termite_layer_norm_f32", .backend = .cuda, .schedule = .tree, .rows = .grid_checked, .stats = .mean_var_one_pass, .p_rows = "rows", .p_dim = "dim", .p_eps = "eps", .bindings = &(.{ bind("dst", .{ .output = 0 }), bind("input", .input) } ++ ln_gamma_beta ++ .{c_params}), .outputs = &.{ln_out} },
    .{ .name = "termite_add_layer_norm_f32", .backend = .cuda, .schedule = .tree, .rows = .grid_checked, .stats = .mean_var_one_pass, .p_rows = "rows", .p_dim = "dim", .p_eps = "eps", .x = ln_add, .bindings = &(.{ bind("dst", .{ .output = 0 }), bind("a", .data), bind("b", .data) } ++ ln_gamma_beta ++ .{c_params}), .outputs = &.{ln_out} },
};

fn renderAll(comptime table: []const Norm, comptime backend: Backend) []const u8 {
    var out: []const u8 = "";
    for (table) |k| if (k.backend == backend) {
        out = out ++ render(k);
    };
    return out;
}

/// The region body for metal_kernels.m: each source line as a C string.
pub fn metalRegion(comptime table: []const Norm) []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        const src = renderAll(table, .metal);
        var out: []const u8 = "";
        var start: usize = 0;
        for (src, 0..) |ch, i| {
            if (ch != '\n') continue;
            out = out ++ "           \"" ++ escape(src[start..i]) ++ "\\n\"\n";
            start = i + 1;
        }
        return out;
    }
}

pub fn escape(comptime s: []const u8) []const u8 {
    var out: []const u8 = "";
    for (s) |ch| out = out ++ switch (ch) {
        '\\' => "\\\\",
        '"' => "\\\"",
        else => &[_]u8{ch},
    };
    return out;
}

pub fn cudaRegion(comptime table: []const Norm) []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        return renderAll(table, .cuda);
    }
}
