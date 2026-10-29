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

//! The production argmax family as tile descriptions.
//!
//! Every kernel reduces (value, index) candidates with the same rule: a
//! larger value wins, ties go to the lower index. Kernels differ in where
//! candidates come from (a row of logits, or partial results of an earlier
//! pass), how threads cover them (one thread, a block tree, warp shuffles),
//! what the empty / "no candidate" / NaN cases produce, and their ABI. One
//! renderer emits Metal and CUDA with each kernel's ABI unchanged.

const std = @import("std");

const rms_norm = @import("rms_norm.zig");
const fmt = std.fmt.comptimePrint;

pub const Backend = enum { metal, cuda };

pub const Schedule = enum {
    /// One thread scans every candidate.
    serial,
    /// Strided per-thread scan, then a shared-memory tree over the block.
    tree,
    /// Strided per-thread scan, warp shuffles, then lane 0s via shared
    /// memory (CUDA).
    warp,
};

pub const Source = enum {
    /// A row of values; the candidate index is the position.
    values,
    /// `partial_values` / `partial_ids` arrays from an earlier pass.
    pairs,
};

pub const Cover = enum {
    /// Everything; output element 0.
    all,
    /// Block b covers values [b * 1024, (b + 1) * 1024) and writes partial b.
    chunk1024,
    /// `chunk1024` per batch row y = threadgroup y (logits and partials offset).
    chunk1024_batch,
    /// Serial over the partials of row `gid` (output gid).
    per_gid,
    /// The last of `rows` rows.
    last_row,
    /// Block = row (row_start + blockIdx.x), checked against row_count.
    row_window,
    /// Block = row, checked against `rows`; output per row.
    row,
};

pub const Arg = struct { name: []const u8, ty: []const u8 };
pub const Suppress = struct { list: []const u8, count: []const u8 };

pub const Argmax = struct {
    name: []const u8,
    backend: Backend,
    schedule: Schedule,
    source: Source = .values,
    cover: Cover = .all,
    /// Kernel arguments in ABI order (CUDA), or buffer order (Metal).
    args: []const Arg,
    /// Candidate count: a parameter expression (e.g. "out_dim", "p.out_dim").
    count: []const u8,
    /// Data buffers.
    values: []const u8 = "logits",
    ids: []const u8 = "partial_ids",
    /// Destination(s).
    out: []const u8 = "output",
    out_values: []const u8 = "partial_values",
    init_value: []const u8 = "-INFINITY",
    init_index: []const u8 = "0u",
    /// Serial over pairs: start from candidate 0 instead of the identity.
    seed_first: bool = false,
    /// Skip partial candidates whose index is 0xffffffff.
    skip_none: bool = false,
    /// Before the tree: a thread that saw no candidate reports index 0.
    none_to_zero: bool = false,
    /// Suppress list: `(list, count)`.
    suppress: ?Suppress = null,
    /// Output 0 when count is 0 (serial).
    empty_zero: bool = false,
    /// Tree over partials: the reduce_tg output rule (empty -> 0, NaN first
    /// partial -> its id, no candidate -> 0).
    tg_final: bool = false,
    /// Warp schedule: shared slots for this many warps.
    warps: u32 = 8,
};

fn cmp(comptime v: []const u8, comptime i: []const u8, comptime bv: []const u8, comptime bi: []const u8) []const u8 {
    return fmt("{0s} > {2s} || ({0s} == {2s} && {1s} < {3s})", .{ v, i, bv, bi });
}

fn signature(comptime k: Argmax) []const u8 {
    var s: []const u8 = "";
    switch (k.backend) {
        .metal => {
            s = "kernel void " ++ k.name ++ "(";
            for (k.args, 0..) |a, slot| {
                const space = if (std.mem.endsWith(u8, a.ty, "&")) "constant" else "device";
                s = s ++ fmt("{s}{s} {s}{s} [[buffer({d})]]", .{ if (slot == 0) "" else ", ", space, a.ty, a.name, slot });
            }
            s = s ++ switch (k.schedule) {
                .serial => ", uint gid [[thread_position_in_grid]]",
                .tree => ", threadgroup float *sh_vals [[threadgroup(0)]], threadgroup uint *sh_ids [[threadgroup(1)]], uint tid [[thread_index_in_threadgroup]], " ++
                    (if (k.cover == .chunk1024_batch) "ushort3 tg_shape [[threads_per_threadgroup]], uint3 group [[threadgroup_position_in_grid]]" else if (k.cover == .all) "uint tpg [[threads_per_threadgroup]]" else "uint tpg [[threads_per_threadgroup]], uint block [[threadgroup_position_in_grid]]"),
                .warp => @compileError(k.name ++ ": warp schedule is CUDA-only"),
            };
        },
        .cuda => {
            s = "extern \"C\" __global__ void " ++ k.name ++ "(";
            for (k.args, 0..) |a, i| s = s ++ (if (i == 0) "" else ", ") ++ a.ty ++ " " ++ a.name;
        },
    }
    s = s ++ ") {\n";
    if (k.backend == .cuda) s = s ++ "    const unsigned int tid = threadIdx.x; const unsigned int tpg = blockDim.x;\n";
    return s;
}

fn suppressed(comptime k: Argmax) []const u8 {
    const sp = k.suppress orelse return "";
    return switch (k.backend) {
        .metal => fmt(" if (termite_argmax_suppressed_token(i, {s}, {s})) continue;", .{ sp.list, sp.count }),
        .cuda => fmt(" if (antfly_tile_suppressed(i, {s}, {s})) continue;", .{ sp.list, sp.count }),
    };
}

/// `const float v = ...; const uint idx = ...;` for candidate i.
fn candidate(comptime k: Argmax, comptime base: []const u8) []const u8 {
    return switch (k.source) {
        .values => fmt(" const float v = {s}[{s}i]; const unsigned int idx = i;", .{ if (k.backend == .cuda and k.cover != .all) "row_ptr" else k.values, base }),
        .pairs => fmt(" const unsigned int idx = {s}[{s}i];{s} const float v = {s}[{s}i];", .{ k.ids, base, if (k.skip_none) " if (idx == 0xffffffffu) continue;" else "", k.out_values, base }),
    };
}

/// Per-thread strided scan into best / best_idx.
fn scan(comptime k: Argmax, comptime first: []const u8, comptime end: []const u8, comptime step: []const u8, comptime base: []const u8) []const u8 {
    return fmt(
        \\    float best = {0s}; unsigned int best_idx = {1s};
        \\    for (unsigned int i = {2s}; i < {3s}; {4s}) {{{5s}{6s} if ({7s}) {{ best = v; best_idx = idx; }} }}
        \\
    , .{ k.init_value, k.init_index, first, end, step, suppressed(k), candidate(k, base), cmp("v", "idx", "best", "best_idx") });
}

pub fn render(comptime k: Argmax) []const u8 {
    comptime {
        @setEvalBranchQuota(200_000);
        var s: []const u8 = signature(k);
        switch (k.schedule) {
            .serial => s = s ++ renderSerial(k),
            .tree => s = s ++ renderTree(k),
            .warp => s = s ++ renderWarp(k),
        }
        return s ++ "}\n";
    }
}

fn renderSerial(comptime k: Argmax) []const u8 {
    var s: []const u8 = switch (k.cover) {
        .all => "    if (gid != 0u) return;\n",
        .per_gid => fmt("    {0s} += gid;\n    {1s} += size_t(gid) * {3s};\n    {2s} += size_t(gid) * {3s};\n", .{ k.out, k.out_values, k.ids, k.count }),
        else => @compileError(k.name ++ ": serial covers all or per_gid"),
    };
    if (k.empty_zero) s = s ++ fmt("    if ({s} == 0u) {{ {s}[0] = 0u; return; }}\n", .{ k.count, k.out });
    if (k.seed_first) {
        s = s ++ fmt("    float best = {s}[0]; unsigned int best_idx = {s}[0];\n", .{ k.out_values, k.ids });
        s = s ++ fmt("    for (unsigned int i = 1u; i < {s}; ++i) {{{s} if ({s}) {{ best = v; best_idx = idx; }} }}\n", .{ k.count, candidate(k, ""), cmp("v", "idx", "best", "best_idx") });
    } else s = s ++ scan(k, "0u", k.count, "++i", "");
    return s ++ fmt("    {s}[0] = best_idx;\n", .{k.out});
}

fn renderTree(comptime k: Argmax) []const u8 {
    const cuda = k.backend == .cuda;
    const barrier = if (cuda) "__syncthreads();" else "threadgroup_barrier(mem_flags::mem_threadgroup);";
    var s: []const u8 = if (cuda) "    __shared__ float sh_vals[256];\n    __shared__ unsigned int sh_ids[256];\n" else "";
    var first: []const u8 = "tid";
    var end: []const u8 = k.count;
    switch (k.cover) {
        .all => {},
        .chunk1024, .chunk1024_batch => {
            if (k.cover == .chunk1024_batch) s = s ++ fmt(
                \\    const uint tpg = tg_shape.x;
                \\    const uint block = group.x;
                \\    {0s} += size_t(group.y) * {1s};
                \\    {2s} += size_t(group.y) * (({1s} + 1023u) / 1024u);
                \\    {3s} += size_t(group.y) * (({1s} + 1023u) / 1024u);
                \\
            , .{ k.values, k.count, k.out_values, k.ids });
            s = s ++ fmt("    const unsigned int start = block * 1024u;\n    const unsigned int end = min(start + 1024u, {s});\n", .{k.count});
            first = "start + tid";
            end = "end";
        },
        .last_row => {
            s = s ++ fmt("    const unsigned int row = rows == 0u ? 0u : rows - 1u;\n    const float* row_ptr = {s} + row * dim;\n", .{k.values});
        },
        .row_window => {
            s = s ++ fmt("    const unsigned int row_local = blockIdx.x;\n    if (row_local >= row_count) return;\n    const float* row_ptr = {s} + (row_start + row_local) * dim;\n", .{k.values});
        },
        else => @compileError(k.name ++ ": unsupported tree cover"),
    }
    s = s ++ scan(k, first, end, "i += tpg", "");
    if (k.none_to_zero) s = s ++ "    if (best_idx == 0xffffffffu) best_idx = 0u;\n";
    s = s ++ fmt(
        \\    sh_vals[tid] = best; sh_ids[tid] = best_idx;
        \\    {0s}
        \\    for (unsigned int stride = tpg >> 1u; stride > 0u; stride >>= 1u) {{
        \\        if (tid < stride) {{ const float other = sh_vals[tid + stride]; const unsigned int other_idx = sh_ids[tid + stride]; if ({1s}) {{ sh_vals[tid] = other; sh_ids[tid] = other_idx; }} }}
        \\        {0s}
        \\    }}
        \\
    , .{ barrier, cmp("other", "other_idx", "sh_vals[tid]", "sh_ids[tid]") });
    if (k.tg_final) return s ++ fmt("    if (tid == 0u) {{ if ({0s} == 0u) {1s}[0] = 0u; else if (isnan({2s}[0])) {1s}[0] = {3s}[0]; else {1s}[0] = sh_ids[0] == 0xffffffffu ? 0u : sh_ids[0]; }}\n", .{ k.count, k.out, k.out_values, k.ids });
    return s ++ switch (k.cover) {
        .chunk1024, .chunk1024_batch => fmt("    if (tid == 0u) {{ {s}[block] = sh_vals[0]; {s}[block] = sh_ids[0]; }}\n", .{ k.out_values, k.ids }),
        .row_window => fmt("    if (tid == 0u) {s}[row_local] = sh_ids[0];\n", .{k.out}),
        else => fmt("    if (tid == 0u) {s}[0] = sh_ids[0];\n", .{k.out}),
    };
}

fn renderWarp(comptime k: Argmax) []const u8 {
    if (k.backend != .cuda) @compileError(k.name ++ ": warp schedule is CUDA-only");
    const per_row = k.cover == .row;
    var s: []const u8 = fmt("    __shared__ float warp_vals[{0d}];\n    __shared__ unsigned int warp_ids[{0d}];\n    const unsigned int lane = tid & 31u; const unsigned int warp = tid >> 5;\n", .{k.warps});
    if (per_row) {
        s = s ++ "    const unsigned int row = blockIdx.x;\n";
        s = s ++ fmt("    float best = {s}; unsigned int best_idx = {s};\n", .{ k.init_value, k.init_index });
        s = s ++ fmt("    if (row < rows) {{ const unsigned int base = row * {0s}; for (unsigned int i = tid; i < {0s}; i += tpg) {{{1s} if ({2s}) {{ best = v; best_idx = idx; }} }} }}\n", .{ k.count, candidate(k, "base + "), cmp("v", "idx", "best", "best_idx") });
    } else s = s ++ scan(k, "tid", k.count, "i += tpg", "");
    s = s ++ fmt(
        \\    for (unsigned int offset = 16u; offset > 0u; offset >>= 1u) {{
        \\        const float other = __shfl_down_sync(0xffffffffu, best, offset); const unsigned int other_idx = __shfl_down_sync(0xffffffffu, best_idx, offset);
        \\        if (other_idx != 0xffffffffu && ({0s})) {{ best = other; best_idx = other_idx; }}
        \\    }}
        \\    if (lane == 0u) {{ warp_vals[warp] = best; warp_ids[warp] = best_idx; }}
        \\    __syncthreads();
        \\    if (tid == 0u{1s}) {{
        \\        float final_value = {2s}; unsigned int final_idx = 0xffffffffu; const unsigned int warp_count = tpg >> 5;
        \\        #pragma unroll
        \\        for (unsigned int w = 0u; w < {3d}u; ++w) {{ if (w >= warp_count) continue; const float other = warp_vals[w]; const unsigned int other_idx = warp_ids[w]; if (other_idx != 0xffffffffu && ({4s})) {{ final_value = other; final_idx = other_idx; }} }}
        \\        {5s}[{6s}] = final_idx == 0xffffffffu ? 0u : final_idx;
        \\    }}
        \\
    , .{ cmp("other", "other_idx", "best", "best_idx"), if (per_row) " && row < rows" else "", k.init_value, k.warps, cmp("other", "other_idx", "final_value", "final_idx"), k.out, if (per_row) "row" else "0" });
    return s;
}

// ---------------------------------------------------------------- kernels

fn arg(comptime name: []const u8, comptime ty: []const u8) Arg {
    return .{ .name = name, .ty = ty };
}

const flt_max_neg = "-3.402823466e+38f";
const none = "0xffffffffu";

const metal_partials_args = [_]Arg{ arg("logits", "const float *"), arg("partial_values", "float *"), arg("partial_ids", "uint *") };
const metal_reduce_args = [_]Arg{ arg("partial_values", "const float *"), arg("partial_ids", "const uint *"), arg("output", "uint *"), arg("partial_count", "uint &") };
const metal_suppress_args = [_]Arg{ arg("logits", "const float *"), arg("suppress_ids", "const int *"), arg("partial_values", "float *"), arg("partial_ids", "uint *"), arg("p", "termite_metal_argmax_suppress_params &") };
const metal_suppress: Suppress = .{ .list = "suppress_ids", .count = "p.suppress_count" };

fn cudaRowArgmax(comptime name: []const u8, comptime cover: Cover, comptime suppress: bool) Argmax {
    const head = [_]Arg{ arg("dst", "unsigned int*"), arg("input", "const float*") } ++ (if (suppress) [_]Arg{arg("suppress_token_ids", "const int*")} else [_]Arg{});
    const shape = if (cover == .last_row) [_]Arg{ arg("rows", "unsigned int"), arg("dim", "unsigned int") } else [_]Arg{ arg("row_start", "unsigned int"), arg("row_count", "unsigned int"), arg("dim", "unsigned int") };
    return .{
        .name = name,
        .backend = .cuda,
        .schedule = .tree,
        .cover = cover,
        .args = &(head ++ shape ++ (if (suppress) [_]Arg{arg("suppress_count", "unsigned int")} else [_]Arg{})),
        .count = "dim",
        .values = "input",
        .out = "dst",
        .init_value = flt_max_neg,
        .suppress = if (suppress) .{ .list = "suppress_token_ids", .count = "suppress_count" } else null,
    };
}

fn cudaPairs(comptime name: []const u8, comptime per_row: bool, comptime warps: u32) Argmax {
    return .{
        .name = name,
        .backend = .cuda,
        .schedule = .warp,
        .source = .pairs,
        .cover = if (per_row) .row else .all,
        .args = &([_]Arg{ arg("dst", "unsigned int*"), arg("partial_values", "const float*"), arg("partial_indices", "const unsigned int*") } ++ (if (per_row) [_]Arg{ arg("rows", "unsigned int"), arg("col_tiles", "unsigned int") } else [_]Arg{arg("count", "unsigned int")})),
        .count = if (per_row) "col_tiles" else "count",
        .ids = "partial_indices",
        .out = "dst",
        .init_value = flt_max_neg,
        .init_index = none,
        .skip_none = true,
        .warps = warps,
    };
}

pub const kernels = [_]Argmax{
    // ---- Metal
    .{ .name = "termite_argmax_logits_1x", .backend = .metal, .schedule = .serial, .args = &.{ arg("logits", "const float *"), arg("output", "uint *"), arg("out_dim", "uint &") }, .count = "out_dim", .empty_zero = true },
    .{ .name = "termite_argmax_logits_partials", .backend = .metal, .schedule = .tree, .cover = .chunk1024, .args = &(metal_partials_args ++ .{arg("out_dim", "uint &")}), .count = "out_dim" },
    .{ .name = "termite_argmax_logits_suppress_partials", .backend = .metal, .schedule = .tree, .cover = .chunk1024, .args = &metal_suppress_args, .count = "p.out_dim", .init_index = none, .none_to_zero = true, .suppress = metal_suppress },
    .{ .name = "termite_lighton_batch_argmax_partials", .backend = .metal, .schedule = .tree, .cover = .chunk1024_batch, .args = &metal_suppress_args, .count = "p.out_dim", .init_index = none, .none_to_zero = true, .suppress = metal_suppress },
    .{ .name = "termite_argmax_logits_reduce", .backend = .metal, .schedule = .serial, .source = .pairs, .args = &metal_reduce_args, .count = "partial_count", .empty_zero = true, .seed_first = true },
    .{ .name = "termite_lighton_batch_argmax_reduce", .backend = .metal, .schedule = .serial, .source = .pairs, .cover = .per_gid, .args = &metal_reduce_args, .count = "partial_count", .empty_zero = true, .seed_first = true },
    .{ .name = "termite_argmax_logits_reduce_tg", .backend = .metal, .schedule = .tree, .source = .pairs, .args = &metal_reduce_args, .count = "partial_count", .init_index = none, .tg_final = true },

    // ---- CUDA (argument order is the host launch ABI in ops/cuda/kernels.zig)
    cudaRowArgmax("termite_argmax_last_row_f32", .last_row, false),
    cudaRowArgmax("termite_argmax_rows_f32", .row_window, false),
    cudaRowArgmax("termite_argmax_rows_suppress_f32", .row_window, true),
    cudaRowArgmax("termite_argmax_last_row_suppress_f32", .last_row, true),
    cudaPairs("termite_argmax_reduce_rows_pairs_f32", true, 8),
    cudaPairs("termite_argmax_reduce_rows_pairs_f32_w16", true, 16),
    cudaPairs("termite_argmax_reduce_pairs_f32", false, 8),
};

fn renderAll(comptime backend: Backend) []const u8 {
    var out: []const u8 = "";
    for (kernels) |k| if (k.backend == backend) {
        out = out ++ render(k);
    };
    return out;
}

pub fn metalRegion() []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        const src = renderAll(.metal);
        var out: []const u8 = "";
        var start: usize = 0;
        for (src, 0..) |ch, i| {
            if (ch != '\n') continue;
            out = out ++ "           \"" ++ rms_norm.escape(src[start..i]) ++ "\\n\"\n";
            start = i + 1;
        }
        return out;
    }
}

pub fn cudaRegion() []const u8 {
    comptime {
        @setEvalBranchQuota(10_000_000);
        return
        \\static __device__ __forceinline__ bool antfly_tile_suppressed(unsigned int token, const int* ids, unsigned int count) {
        \\    for (unsigned int j = 0; j < count; ++j) { const int id = ids[j]; if (id >= 0 && (unsigned int)id == token) return true; }
        \\    return false;
        \\}
        \\
        ++ renderAll(.cuda);
    }
}
