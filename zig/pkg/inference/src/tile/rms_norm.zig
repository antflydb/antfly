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
//! a scale. What differs between the hand-written kernels is the schedule
//! (one thread, one thread per row, threadgroup tree, float4 + SIMD-group
//! sums), where sum(x^2) comes from, the epilogue, and each kernel's ABI.
//!
//! - Metal: `metal_kernels` render into the marker region of
//!   `src/backends/metal_kernels.m`, keeping every kernel's name, buffer
//!   slots and params struct, so host dispatch is unchanged.
//! - CUDA: `cuda_kernels` render byte-identical to the previous hand-written
//!   kernels in `inference_cuda_kernels.cu`, so the checked-in PTX, fatbin
//!   and cubin stay valid without a CUDA toolkit. Normalizing their
//!   spelling is a follow-up that needs an artifact regeneration.

const std = @import("std");

const fmt = std.fmt.comptimePrint;

/// Epilogue expression. Rendering adds parentheses only where C precedence
/// needs them, so `x * inv * w + r` and `x * (inv * w)` keep their order.
pub const NExpr = union(enum) {
    x,
    inv,
    weight: u8,
    residual,
    scale,
    /// A named intermediate (`Let`).
    ref: []const u8,
    add: [2]*const NExpr,
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

    pub const Names = struct {
        x: []const u8,
        inv: []const u8,
        weights: [3][]const u8 = .{ "", "", "" },
        residual: []const u8 = "",
        scale: []const u8 = "",
    };

    pub fn render(comptime e: NExpr, comptime n: Names) []const u8 {
        return renderPrec(e, n, 0, false);
    }

    fn renderPrec(comptime e: NExpr, comptime n: Names, comptime parent: u8, comptime right: bool) []const u8 {
        const prec: u8 = switch (e) {
            .add => 1,
            .mul => 2,
            else => 3,
        };
        const body = switch (e) {
            .x => n.x,
            .inv => n.inv,
            .weight => |k| n.weights[k],
            .residual => n.residual,
            .scale => n.scale,
            .ref => |name| name,
            .add => |xs| renderPrec(xs[0].*, n, 1, false) ++ " + " ++ renderPrec(xs[1].*, n, 1, true),
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

// ---------------------------------------------------------------- Metal

pub const Schedule = enum {
    /// Thread 0 does the whole (single) row.
    serial_single,
    /// One thread per row.
    serial_rows,
    /// One threadgroup per row, shared-memory tree over `tpg` threads.
    tree,
    /// One threadgroup per row, float4 loads, SIMD-group sums.
    simd4,
};

pub const Rows = enum {
    /// A single row; no row index.
    single,
    /// Row = threadgroup index, not bounds-checked (params have no `rows`).
    grid,
    /// Row = threadgroup index, checked against `p.rows`.
    grid_checked,
};

pub const Role = union(enum) {
    input,
    input_half,
    weight: u8,
    residual,
    output: u8,
    /// Per-row partial sums of squares, one per 8 elements.
    sumsq,
    /// Receives inv only.
    inv_out,
    params,
};

pub const Binding = struct { name: []const u8, role: Role };

pub const MetalNorm = struct {
    name: []const u8,
    schedule: Schedule,
    rows: Rows = .single,
    params_type: []const u8,
    dim: []const u8 = "hidden_size",
    bindings: []const Binding,
    /// One expression per `.output` binding, in output index order.
    outputs: []const NExpr = &.{},
    /// simd4: clamp with `p.lo`/`p.hi` when `p.flags` bits 0/1 are set.
    clamp_flags: bool = false,
    /// simd4: shared memory is a local array, not a `threadgroup(0)` buffer.
    local_shared: bool = false,
    /// simd4: materialize this value in the output buffer (volatile store,
    /// device barrier, reload) before evaluating `outputs` with `.ref("staged")`,
    /// so the rounding matches the unfused chain.
    staged: ?NExpr = null,
    /// Emitted before the kernel (params structs only this kernel uses).
    decl: ?[]const u8 = null,

    fn binding(comptime k: MetalNorm, comptime role: std.meta.Tag(Role), comptime index: u8) []const u8 {
        for (k.bindings) |b| {
            if (b.role != role) continue;
            switch (b.role) {
                .weight, .output => |i| if (i == index) return b.name,
                else => return b.name,
            }
        }
        @compileError(k.name ++ ": no binding for role " ++ @tagName(role));
    }

    fn has(comptime k: MetalNorm, comptime role: std.meta.Tag(Role)) bool {
        for (k.bindings) |b| if (b.role == role) return true;
        return false;
    }

    fn inputName(comptime k: MetalNorm) []const u8 {
        return if (k.has(.input_half)) k.binding(.input_half, 0) else k.binding(.input, 0);
    }
};

fn metalSignature(comptime k: MetalNorm) []const u8 {
    var args: []const u8 = "";
    for (k.bindings, 0..) |b, slot| {
        const arg = switch (b.role) {
            .input, .weight, .residual, .sumsq => fmt("device const float *{s} [[buffer({d})]]", .{ b.name, slot }),
            .input_half => fmt("device const half *{s} [[buffer({d})]]", .{ b.name, slot }),
            .output, .inv_out => fmt("device float *{s} [[buffer({d})]]", .{ b.name, slot }),
            .params => fmt("constant {s} &p [[buffer({d})]]", .{ k.params_type, slot }),
        };
        args = args ++ (if (slot == 0) "" else ", ") ++ arg;
    }
    const builtins = switch (k.schedule) {
        .serial_single, .serial_rows => ", uint gid [[thread_position_in_grid]]",
        .tree => ", threadgroup float *shmem [[threadgroup(0)]], uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]]" ++
            (if (k.rows == .single) "" else ", uint row [[threadgroup_position_in_grid]]"),
        .simd4 => (if (k.local_shared) "" else ", threadgroup float *shmem [[threadgroup(0)]]") ++
            ", uint tid [[thread_index_in_threadgroup]], uint tpg [[threads_per_threadgroup]], uint simd_group [[simdgroup_index_in_threadgroup]], uint simd_lane [[thread_index_in_simdgroup]], uint row [[threadgroup_position_in_grid]]",
    };
    return "kernel void " ++ k.name ++ "(" ++ args ++ builtins ++ ") {\n";
}

fn scalarNames(comptime k: MetalNorm, comptime base: []const u8) NExpr.Names {
    const load = if (k.has(.input_half)) "float(" ++ k.inputName() ++ "[" ++ base ++ "i])" else k.inputName() ++ "[" ++ base ++ "i]";
    var n: NExpr.Names = .{ .x = load, .inv = "inv", .scale = "p.scale" };
    for (k.bindings) |b| switch (b.role) {
        .weight => |w| n.weights[w] = b.name ++ "[i]",
        .residual => n.residual = b.name ++ "[" ++ base ++ "i]",
        else => {},
    };
    return n;
}

fn float4At(comptime buf: []const u8, comptime idx: []const u8) []const u8 {
    return fmt("float4({0s}[{1s}], {0s}[{1s} + 1u], {0s}[{1s} + 2u], {0s}[{1s} + 3u])", .{ buf, idx });
}

fn store4(comptime buf: []const u8, comptime value: []const u8) []const u8 {
    return fmt("{0s}[i] = {1s}.x; {0s}[i + 1u] = {1s}.y; {0s}[i + 2u] = {1s}.z; {0s}[i + 3u] = {1s}.w;", .{ buf, value });
}

/// Sum of squares into `sum`, for the scalar schedules.
fn scalarSum(comptime k: MetalNorm, comptime first: []const u8, comptime step: []const u8, comptime base: []const u8) []const u8 {
    if (k.has(.sumsq)) return fmt(
        \\    const uint partial_count = (p.{0s} + 7u) / 8u;
        \\    float sum = 0.0f;
        \\    for (uint i = {1s}; i < partial_count; {2s}) sum += {3s}[{4s}i];
        \\
    , .{ k.dim, first, step, k.binding(.sumsq, 0), if (k.rows == .single) "" else "row * partial_count + " });
    const names = scalarNames(k, base);
    return fmt(
        \\    float sum = 0.0f;
        \\    for (uint i = {0s}; i < p.{1s}; {2s}) {{ const float v = {3s}; sum += v * v; }}
        \\
    , .{ first, k.dim, step, names.x });
}

fn scalarOutputs(comptime k: MetalNorm, comptime first: []const u8, comptime step: []const u8, comptime base: []const u8) []const u8 {
    const names = scalarNames(k, base);
    var body: []const u8 = "";
    for (k.outputs, 0..) |e, o| body = body ++ fmt(" {s}[{s}i] = {s};", .{ k.binding(.output, @intCast(o)), base, e.render(names) });
    return fmt("    for (uint i = {s}; i < p.{s}; {s}) {{{s} }}\n", .{ first, k.dim, step, body });
}

pub fn renderMetal(comptime k: MetalNorm) []const u8 {
    comptime {
        @setEvalBranchQuota(200_000);
        var s: []const u8 = (k.decl orelse "") ++ metalSignature(k);
        switch (k.schedule) {
            .serial_single, .serial_rows => {
                const base = if (k.schedule == .serial_single) "" else "base + ";
                s = s ++ (if (k.schedule == .serial_single) "    if (gid != 0u) return;\n" else "    if (gid >= p.rows) return;\n");
                s = s ++ fmt("    if (p.{s} == 0u) return;\n", .{k.dim});
                if (k.schedule == .serial_rows) s = s ++ fmt("    const uint base = gid * p.{s};\n", .{k.dim});
                s = s ++ scalarSum(k, "0u", "++i", base);
                s = s ++ fmt("    const float inv = rsqrt(sum / float(p.{s}) + p.eps);\n", .{k.dim});
                s = s ++ scalarOutputs(k, "0u", "++i", base);
            },
            .tree => {
                const base = if (k.rows == .single) "" else "base + ";
                s = s ++ (if (k.rows == .grid_checked) "    if (row >= p.rows) return;\n" else "");
                s = s ++ fmt("    if (p.{s} == 0u) return;\n", .{k.dim});
                if (k.rows != .single) s = s ++ fmt("    const uint base = row * p.{s};\n", .{k.dim});
                s = s ++ scalarSum(k, "tid", "i += tpg", base);
                s = s ++
                    \\    shmem[tid] = sum;
                    \\    threadgroup_barrier(mem_flags::mem_threadgroup);
                    \\    for (uint stride = tpg >> 1; stride > 0u; stride >>= 1u) { if (tid < stride) shmem[tid] += shmem[tid + stride]; threadgroup_barrier(mem_flags::mem_threadgroup); }
                    \\
                ;
                s = s ++ fmt("    const float inv = rsqrt(shmem[0] / float(p.{s}) + p.eps);\n", .{k.dim});
                if (k.has(.inv_out)) {
                    s = s ++ fmt("    if (tid == 0u) {s}[0] = inv;\n", .{k.binding(.inv_out, 0)});
                } else s = s ++ scalarOutputs(k, "tid", "i += tpg", base);
            },
            .simd4 => s = s ++ renderSimd4(k),
        }
        return s ++ "}\n";
    }
}

fn renderSimd4(comptime k: MetalNorm) []const u8 {
    const input = k.inputName();
    var s: []const u8 = if (k.local_shared) "    threadgroup float shmem[32];\n" else "";
    s = s ++ fmt(
        \\    if (row >= p.rows || p.{0s} == 0u || (p.{0s} & 3u) != 0u) return;
        \\    if (simd_group == 0u) shmem[simd_lane] = 0.0f;
        \\    const uint base = row * p.{0s};
        \\    const uint vectors = p.{0s} >> 2u;
        \\    float sum = 0.0f;
        \\    for (uint v = tid; v < vectors; v += tpg) {{ const uint i = base + (v << 2u); const float4 x = {1s}; sum += dot(x, x); }}
        \\    sum = simd_sum(sum);
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    if (simd_lane == 0u) shmem[simd_group] = sum;
        \\    threadgroup_barrier(mem_flags::mem_threadgroup);
        \\    sum = simd_sum(shmem[simd_lane]);
        \\    const float inv = rsqrt(sum / float(p.{0s}) + p.eps);
        \\
    , .{ k.dim, float4At(input, "i") });
    var names: NExpr.Names = .{ .x = "x", .inv = "inv", .scale = "p.scale" };
    var loads: []const u8 = "";
    for (k.bindings) |b| switch (b.role) {
        .weight => |w| {
            names.weights[w] = fmt("w{d}", .{w});
            loads = loads ++ fmt(" const float4 w{d} = {s};", .{ w, float4At(b.name, "j") });
        },
        .residual => {
            names.residual = "r";
            loads = loads ++ fmt(" const float4 r = {s};", .{float4At(b.name, "i")});
        },
        else => {},
    };
    const head = "    for (uint v = tid; v < vectors; v += tpg) { const uint j = v << 2u; const uint i = base + j;";
    if (k.staged) |first| {
        // Store the staged value, then finish from the reloaded copy.
        const out = k.binding(.output, 0);
        var first_names = names;
        first_names.residual = "";
        var first_loads: []const u8 = "";
        for (k.bindings) |b| if (b.role == .weight) {
            first_loads = first_loads ++ fmt(" const float4 w{d} = {s};", .{ b.role.weight, float4At(b.name, "j") });
        };
        s = s ++ fmt("    device volatile float *rounded = {s};\n", .{out});
        s = s ++ fmt("{s} const float4 x = {s};{s} const float4 staged = {s}; {s} }}\n", .{ head, float4At(input, "i"), first_loads, first.render(first_names), store4("rounded", "staged") });
        s = s ++ "    threadgroup_barrier(mem_flags::mem_device);\n";
        s = s ++ fmt("{s} const float4 staged = {s}; const float4 r = {s}; const float4 y = {s}; {s} }}\n", .{ head, float4At("rounded", "i"), float4At(k.binding(.residual, 0), "i"), k.outputs[0].render(names), store4(out, "y") });
        return s;
    }
    var body: []const u8 = "";
    for (k.outputs, 0..) |e, o| {
        const y = fmt("y{d}", .{o});
        body = body ++ fmt(" float4 {s} = {s};", .{ y, e.render(names) });
        if (k.clamp_flags) body = body ++ fmt(" if ((p.flags & 1u) != 0u) {0s} = max({0s}, float4(p.lo)); if ((p.flags & 2u) != 0u) {0s} = min({0s}, float4(p.hi));", .{y});
        body = body ++ " " ++ store4(k.binding(.output, @intCast(o)), y);
    }
    return s ++ fmt("{s} const float4 x = {s};{s}{s} }}\n", .{ head, float4At(input, "i"), loads, body });
}

const in_w_out: []const Binding = &.{ .{ .name = "input", .role = .input }, .{ .name = "weight", .role = .{ .weight = 0 } }, .{ .name = "output", .role = .{ .output = 0 } }, .{ .name = "p", .role = .params } };
const in_w_res_out: []const Binding = &.{ .{ .name = "input", .role = .input }, .{ .name = "weight", .role = .{ .weight = 0 } }, .{ .name = "residual", .role = .residual }, .{ .name = "output", .role = .{ .output = 0 } }, .{ .name = "p", .role = .params } };
const in_w_res_out_sumsq: []const Binding = &.{ .{ .name = "input", .role = .input }, .{ .name = "weight", .role = .{ .weight = 0 } }, .{ .name = "residual", .role = .residual }, .{ .name = "output", .role = .{ .output = 0 } }, .{ .name = "sumsq", .role = .sumsq }, .{ .name = "p", .role = .params } };

const normed = x.times(inv).times(weight(0));
const normed_add = normed.plus(residual);
const normed_add_scale = normed_add.times(scale);
const normed4 = x.times(inv.times(weight(0)));

pub const metal_kernels = [_]MetalNorm{
    .{ .name = "termite_apply_rms_norm_1x", .schedule = .serial_single, .params_type = "termite_metal_apply_layer_norm_params", .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_apply_rms_norm_1x_reduce", .schedule = .tree, .params_type = "termite_metal_apply_layer_norm_params", .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_compute_rms_inv_scale_1x", .schedule = .tree, .params_type = "termite_metal_apply_layer_norm_params", .bindings = &.{ .{ .name = "input", .role = .input }, .{ .name = "inv_scale", .role = .inv_out }, .{ .name = "p", .role = .params } } },
    .{ .name = "termite_apply_rms_norm_rows", .schedule = .serial_rows, .params_type = "termite_metal_apply_row_norm_params", .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_apply_rms_norm_rows_reduce", .schedule = .tree, .rows = .grid_checked, .params_type = "termite_metal_apply_row_norm_params", .bindings = in_w_out, .outputs = &.{normed} },
    .{ .name = "termite_apply_rms_norm_rows_simd4_reduce", .schedule = .simd4, .rows = .grid_checked, .params_type = "termite_metal_apply_row_norm_params", .bindings = in_w_out, .outputs = &.{normed4} },
    .{
        .name = "antfly_norm_clamp",
        .schedule = .simd4,
        .rows = .grid_checked,
        .params_type = "antfly_norm_clamp_params",
        .dim = "dim",
        .bindings = in_w_out,
        .outputs = &.{normed4},
        .clamp_flags = true,
        .local_shared = true,
        .decl = "struct antfly_norm_clamp_params {uint rows,dim,flags,reserved;float eps,lo,hi,padding;};\n",
    },
    .{
        .name = "termite_apply_rms_norm_rows_triple_simd4_reduce",
        .schedule = .simd4,
        .rows = .grid_checked,
        .params_type = "termite_metal_apply_row_norm_params",
        .bindings = &.{
            .{ .name = "input", .role = .input },
            .{ .name = "first_weight", .role = .{ .weight = 0 } },
            .{ .name = "second_weight", .role = .{ .weight = 1 } },
            .{ .name = "third_weight", .role = .{ .weight = 2 } },
            .{ .name = "first_output", .role = .{ .output = 0 } },
            .{ .name = "second_output", .role = .{ .output = 1 } },
            .{ .name = "third_output", .role = .{ .output = 2 } },
            .{ .name = "p", .role = .params },
        },
        .outputs = &.{ normed4, x.times(inv.times(weight(1))), x.times(inv.times(weight(2))) },
    },
    .{ .name = "termite_apply_rms_norm_add_1x", .schedule = .tree, .params_type = "termite_metal_apply_layer_norm_params", .bindings = in_w_res_out, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_rows_canonical", .schedule = .tree, .rows = .grid, .params_type = "termite_metal_apply_layer_norm_params", .bindings = in_w_res_out, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_1x_sumsq", .schedule = .tree, .params_type = "termite_metal_apply_layer_norm_params", .bindings = in_w_res_out_sumsq, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_rows_sumsq", .schedule = .tree, .rows = .grid, .params_type = "termite_metal_apply_layer_norm_params", .bindings = in_w_res_out_sumsq, .outputs = &.{normed_add} },
    .{ .name = "termite_apply_rms_norm_add_scale_1x_sumsq", .schedule = .tree, .params_type = "termite_metal_apply_layer_norm_scale_params", .bindings = in_w_res_out_sumsq, .outputs = &.{normed_add_scale} },
    .{
        .name = "termite_apply_rms_norm_add_1x_f16_input",
        .schedule = .tree,
        .params_type = "termite_metal_apply_layer_norm_params",
        .bindings = &.{ .{ .name = "input", .role = .input_half }, .{ .name = "weight", .role = .{ .weight = 0 } }, .{ .name = "residual", .role = .residual }, .{ .name = "output", .role = .{ .output = 0 } }, .{ .name = "p", .role = .params } },
        .outputs = &.{normed_add},
    },
    .{ .name = "termite_apply_rms_norm_add_scale_1x", .schedule = .tree, .params_type = "termite_metal_apply_layer_norm_scale_params", .bindings = in_w_res_out, .outputs = &.{normed_add_scale} },
    .{ .name = "termite_apply_rms_norm_add_scale_rows", .schedule = .tree, .rows = .grid_checked, .params_type = "termite_metal_apply_row_norm_scale_params", .bindings = in_w_res_out, .outputs = &.{normed_add_scale} },
    // Normalized values round through the output buffer before the residual
    // add, matching the unfused chain. `* p.scale` replaces the previous
    // `if (p.scale != 1.0f)`: multiplying by 1.0 is exact.
    .{ .name = "termite_apply_rms_norm_add_scale_simd4", .schedule = .simd4, .rows = .grid_checked, .params_type = "termite_metal_apply_row_norm_scale_params", .bindings = in_w_res_out, .staged = normed4, .outputs = &.{(NExpr{ .ref = "staged" }).plus(residual).times(scale)} },
};

/// The region body for metal_kernels.m: each source line as a C string.
pub fn metalRegion() []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var src: []const u8 = "";
        for (metal_kernels) |k| src = src ++ renderMetal(k);
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

fn escape(comptime s: []const u8) []const u8 {
    var out: []const u8 = "";
    for (s) |ch| out = out ++ switch (ch) {
        '\\' => "\\\\",
        '"' => "\\\"",
        else => &[_]u8{ch},
    };
    return out;
}

// ---------------------------------------------------------------- CUDA

pub const CudaNorm = struct {
    name: []const u8,
    /// Output buffers, in argument order before `input`.
    outputs: []const []const u8 = &.{"dst"},
    /// Also write a bf16 copy to `dst_bf16`.
    bf16: bool = false,
    /// Weight arguments after `input`.
    weights: []const []const u8 = &.{"weight"},
    residual: bool = false,
    /// `const float* scalar` argument, read once as `output_scale`.
    scalar: bool = false,
    /// Name of the 1/rms variable.
    inv_name: []const u8 = "scale",
    /// Index elements through `unsigned int idx = base + i`.
    idx: bool = true,
    /// `const`-qualified locals.
    const_locals: bool = false,
    /// A shared intermediate (`const float normalized = ...`) before the stores.
    let: ?struct { name: []const u8, expr: NExpr } = null,
    /// One per output.
    exprs: []const NExpr,
    comment: []const u8 = "",
};

pub fn renderCuda(comptime k: CudaNorm) []const u8 {
    comptime {
        @setEvalBranchQuota(200_000);
        const c = if (k.const_locals) "const " else "";
        var args: []const []const u8 = &.{};
        for (k.outputs) |o| args = args ++ .{"float* " ++ o};
        if (k.bf16) args = args ++ .{"unsigned short* dst_bf16"};
        args = args ++ .{"const float* input"};
        for (k.weights) |w| args = args ++ .{"const float* " ++ w};
        if (k.residual) args = args ++ .{"const float* residual"};
        if (k.scalar) args = args ++ .{"const float* scalar"};
        args = args ++ .{ "unsigned int rows", "unsigned int dim", "float eps" };
        var s: []const u8 = k.comment ++ "extern \"C\" __global__ void " ++ k.name ++ "(\n";
        for (args, 0..) |a, i| s = s ++ "    " ++ a ++ (if (i + 1 < args.len) ",\n" else "\n");
        s = s ++ fmt(
            \\) {{
            \\    {0s}unsigned int row = blockIdx.x;
            \\    if (row >= rows) return;
            \\    {0s}unsigned int tid = threadIdx.x;
            \\    const unsigned int base = row * dim;
            \\    __shared__ float partial[256];
            \\    float sumsq = 0.0f;
            \\    for (unsigned int i = tid; i < dim; i += blockDim.x) {{
            \\        {0s}float x = input[base + i];
            \\        sumsq += x * x;
            \\    }}
            \\    partial[tid] = sumsq;
            \\    __syncthreads();
            \\    for (unsigned int stride = blockDim.x >> 1; stride > 0u; stride >>= 1u) {{
            \\        if (tid < stride) partial[tid] += partial[tid + stride];
            \\        __syncthreads();
            \\    }}
            \\    {0s}float {1s} = rsqrtf(partial[0] / (float)dim + eps);
            \\
        , .{ c, k.inv_name });
        if (k.scalar) s = s ++ "    float output_scale = scalar[0];\n";
        const at = if (k.idx) "idx" else "base + i";
        var names: NExpr.Names = .{ .x = "input[" ++ at ++ "]", .inv = k.inv_name, .residual = "residual[" ++ at ++ "]", .scale = "output_scale" };
        for (k.weights, 0..) |w, i| names.weights[i] = w ++ "[i]";
        s = s ++ "    for (unsigned int i = tid; i < dim; i += blockDim.x) {\n";
        if (k.idx) s = s ++ fmt("        {s}unsigned int idx = base + i;\n", .{c});
        if (k.let) |l| s = s ++ fmt("        {s}float {s} = {s};\n", .{ c, l.name, l.expr.render(names) });
        for (k.outputs, k.exprs) |o, e| {
            if (k.bf16) {
                s = s ++ fmt("        float value = {s};\n        {s}[{s}] = value;\n        dst_bf16[{s}] = termite_f32_to_bf16(value);\n", .{ e.render(names), o, at, at });
            } else s = s ++ fmt("        {s}[{s}] = {s};\n", .{ o, at, e.render(names) });
        }
        return s ++ "    }\n}\n";
    }
}

pub const cuda_kernels = [_]CudaNorm{
    .{ .name = "termite_rms_norm_f32", .idx = false, .exprs = &.{normed} },
    .{ .name = "termite_rms_norm_f32_bf16", .bf16 = true, .exprs = &.{normed} },
    .{ .name = "termite_rms_norm_add_f32", .residual = true, .inv_name = "norm_scale", .exprs = &.{normed_add} },
    .{ .name = "termite_rms_norm_add_f32_bf16", .bf16 = true, .residual = true, .inv_name = "norm_scale", .exprs = &.{normed_add} },
    .{ .name = "termite_rms_norm_add_mul_scalar_f32", .residual = true, .scalar = true, .inv_name = "norm_scale", .exprs = &.{normed.times(scale).plus(residual)} },
    .{ .name = "termite_rms_norm_add_output_scale_f32", .residual = true, .scalar = true, .inv_name = "norm_scale", .exprs = &.{normed_add_scale} },
    .{ .name = "termite_rms_norm_bare_f32", .weights = &.{}, .idx = false, .exprs = &.{x.times(inv)} },
    .{
        .name = "termite_gemma4_a4b_rms_norm_triple_f32",
        .outputs = &.{ "first", "second", "third" },
        .weights = &.{ "first_weight", "second_weight", "third_weight" },
        .const_locals = true,
        .let = .{ .name = "normalized", .expr = x.times(inv) },
        .exprs = &.{ (NExpr{ .ref = "normalized" }).times(weight(0)), (NExpr{ .ref = "normalized" }).times(weight(1)), (NExpr{ .ref = "normalized" }).times(weight(2)) },
        .comment =
        \\// Gemma 4 A4B branches three RMSNorms from the same attention residual. Share
        \\// the denominator reduction while preserving the ordinary RMSNorm reduction
        \\// order, then materialize the three independently weighted outputs.
        \\
        ,
    },
};

pub fn cudaRegion() []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        var out: []const u8 = "";
        for (cuda_kernels, 0..) |k, i| out = out ++ (if (i == 0) "" else "\n") ++ renderCuda(k);
        return out;
    }
}
