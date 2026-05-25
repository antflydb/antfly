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

const std = @import("std");

pub const adapter_a_tensor_name = "adapter.a";
pub const adapter_b_tensor_name = "adapter.b";
pub const adapter_alpha_tensor_name = "adapter.alpha";

pub const Matrix = struct {
    rows: usize,
    cols: usize,
    data: []const f32,

    pub fn row(self: Matrix, idx: usize) []const f32 {
        const start = idx * self.cols;
        return self.data[start .. start + self.cols];
    }
};

pub const LayerTensorKind = enum {
    a,
    b,
};

pub const ParsedLayerTensor = struct {
    layer_name: []const u8,
    kind: LayerTensorKind,
};

pub fn normalizeTensorName(name: []const u8) []const u8 {
    var normalized = name;
    if (std.mem.startsWith(u8, normalized, "var:")) normalized = normalized[4..];
    while (normalized.len > 0 and normalized[0] == '/') normalized = normalized[1..];
    return normalized;
}

pub fn parseLayerTensorName(name: []const u8) ?ParsedLayerTensor {
    const normalized = normalizeTensorName(name);
    if (std.mem.endsWith(u8, normalized, "/lora_A")) {
        return .{ .layer_name = normalized[0 .. normalized.len - "/lora_A".len], .kind = .a };
    }
    if (std.mem.endsWith(u8, normalized, "/lora_A/weight")) {
        return .{ .layer_name = normalized[0 .. normalized.len - "/lora_A/weight".len], .kind = .a };
    }
    if (std.mem.endsWith(u8, normalized, ".lora_A")) {
        return .{ .layer_name = normalized[0 .. normalized.len - ".lora_A".len], .kind = .a };
    }
    if (std.mem.endsWith(u8, normalized, ".lora_A.weight")) {
        return .{ .layer_name = normalized[0 .. normalized.len - ".lora_A.weight".len], .kind = .a };
    }
    if (std.mem.endsWith(u8, normalized, "/lora_B")) {
        return .{ .layer_name = normalized[0 .. normalized.len - "/lora_B".len], .kind = .b };
    }
    if (std.mem.endsWith(u8, normalized, "/lora_B/weight")) {
        return .{ .layer_name = normalized[0 .. normalized.len - "/lora_B/weight".len], .kind = .b };
    }
    if (std.mem.endsWith(u8, normalized, ".lora_B")) {
        return .{ .layer_name = normalized[0 .. normalized.len - ".lora_B".len], .kind = .b };
    }
    if (std.mem.endsWith(u8, normalized, ".lora_B.weight")) {
        return .{ .layer_name = normalized[0 .. normalized.len - ".lora_B.weight".len], .kind = .b };
    }
    return null;
}

pub const ScalingMode = enum {
    /// Standard LoRA: scale = alpha / rank (Hu et al. 2021).
    standard,
    /// rsLoRA: scale = alpha / sqrt(rank) (Kalajdzievski 2023). Unlocks
    /// effective use of rank >= 32 where the standard scaling saturates.
    rs_lora,
};

pub fn effectiveScale(alpha: f32, rank: usize) f32 {
    return effectiveScaleMode(alpha, rank, .standard);
}

pub fn effectiveScaleMode(alpha: f32, rank: usize, mode: ScalingMode) f32 {
    if (rank == 0) return 0;
    const r_f: f32 = @floatFromInt(rank);
    return switch (mode) {
        .standard => alpha / r_f,
        .rs_lora => alpha / @sqrt(r_f),
    };
}

pub fn applyInPlace(hidden: []f32, adapter_a: Matrix, adapter_b: Matrix, alpha: f32) void {
    const rank = adapter_b.rows;
    if (rank == 0) return;
    const scale = effectiveScale(alpha, rank);
    if (scale == 0) return;

    var low_rank = std.heap.stackFallback(4096, std.heap.page_allocator);
    const alloc = low_rank.get();
    const tmp = alloc.alloc(f32, rank) catch return;
    defer alloc.free(tmp);
    @memset(tmp, 0);

    const n = @min(hidden.len, adapter_a.rows);
    for (0..n) |i| {
        const x = hidden[i];
        const row = adapter_a.row(i);
        for (row, 0..) |a, r| {
            tmp[r] += x * a;
        }
    }
    for (0..rank) |r| {
        const brow = adapter_b.row(r);
        const m = tmp[r] * scale;
        const hn = @min(hidden.len, brow.len);
        for (0..hn) |h| {
            hidden[h] += m * brow[h];
        }
    }
}

pub fn mergeInto(base: Matrix, adapter_a: Matrix, adapter_b: Matrix, alpha: f32, out: []f32) void {
    std.debug.assert(out.len == base.data.len);
    @memcpy(out, base.data);
    const rank = adapter_b.rows;
    if (rank == 0) return;
    const scale = effectiveScale(alpha, rank);
    if (scale == 0) return;

    for (0..adapter_a.rows) |i| {
        const a_row = adapter_a.row(i);
        const base_offset = i * base.cols;
        for (0..rank) |r| {
            const a = a_row[r];
            if (a == 0) continue;
            const b_row = adapter_b.row(r);
            for (0..@min(base.cols, b_row.len)) |j| {
                out[base_offset + j] += a * b_row[j] * scale;
            }
        }
    }
}

pub fn accumulateLinearLoRAGrads(
    grad_a: []f32,
    grad_b: []f32,
    input_rows: usize,
    input_cols: usize,
    inputs: []const f32,
    output_cols: usize,
    output_grads: []const f32,
    adapter_a: Matrix,
    adapter_b: Matrix,
    alpha: f32,
) void {
    std.debug.assert(adapter_a.rows == input_cols);
    std.debug.assert(adapter_b.cols == output_cols);
    std.debug.assert(adapter_a.cols == adapter_b.rows);
    std.debug.assert(inputs.len == input_rows * input_cols);
    std.debug.assert(output_grads.len == input_rows * output_cols);
    std.debug.assert(grad_a.len == adapter_a.rows * adapter_a.cols);
    std.debug.assert(grad_b.len == adapter_b.rows * adapter_b.cols);

    const rank = adapter_b.rows;
    if (rank == 0) return;
    const scale = effectiveScale(alpha, rank);
    if (scale == 0) return;

    var low_rank = std.heap.stackFallback(4096, std.heap.page_allocator);
    const alloc = low_rank.get();
    const tmp_rank = alloc.alloc(f32, rank) catch return;
    defer alloc.free(tmp_rank);
    const back_rank = alloc.alloc(f32, rank) catch return;
    defer alloc.free(back_rank);

    for (0..input_rows) |row_idx| {
        const x_row = inputs[row_idx * input_cols .. (row_idx + 1) * input_cols];
        const g_row = output_grads[row_idx * output_cols .. (row_idx + 1) * output_cols];

        @memset(tmp_rank, 0);
        for (0..input_cols) |i| {
            const x = x_row[i];
            const a_row = adapter_a.row(i);
            for (a_row, 0..) |a, r| tmp_rank[r] += x * a;
        }
        for (0..rank) |r| {
            const base = r * output_cols;
            const t = tmp_rank[r] * scale;
            for (0..output_cols) |j| {
                grad_b[base + j] += t * g_row[j];
            }
        }

        @memset(back_rank, 0);
        for (0..rank) |r| {
            const b_row = adapter_b.row(r);
            var acc: f32 = 0;
            for (0..output_cols) |j| acc += g_row[j] * b_row[j];
            back_rank[r] = acc * scale;
        }
        for (0..input_cols) |i| {
            const base = i * rank;
            const x = x_row[i];
            for (0..rank) |r| grad_a[base + r] += x * back_rank[r];
        }
    }
}

/// Transpose a [rows x cols] matrix into [cols x rows].
fn transpose2DLora(dst: []f32, src: []const f32, rows: usize, cols: usize) void {
    for (0..rows) |r| {
        for (0..cols) |c| {
            dst[c * rows + r] = src[r * cols + c];
        }
    }
}

/// Accumulates LoRA gradients using a compute backend when available
/// (for GPU acceleration), falling back to pure-Zig CPU otherwise.
///
/// Layout conversion: the CPU convention uses adapter_a [in_features, rank]
/// and adapter_b [rank, out_features], while backend vtables expect
/// lora_a [rank, in_features] and lora_b [out_features, rank].  This
/// function handles the transpositions and accumulates the results.
pub fn accumulateLinearLoRAGradsBackend(
    cb: anytype,
    grad_a: []f32,
    grad_b: []f32,
    input_rows: usize,
    input_cols: usize,
    inputs: []const f32,
    output_cols: usize,
    output_grads: []const f32,
    adapter_a: Matrix,
    adapter_b: Matrix,
    alpha: f32,
) void {
    const in_features = input_cols;
    const out_features = output_cols;
    const rank = adapter_b.rows;

    gpu_path: {
        const backend = cb orelse break :gpu_path;
        if (rank == 0) break :gpu_path;

        const scale = effectiveScale(alpha, rank);
        if (scale == 0) break :gpu_path;

        // Allocate transposed weight buffers and temporary gradient buffers.
        var sf = std.heap.stackFallback(8192, std.heap.page_allocator);
        const alloc = sf.get();

        const lora_a_t = alloc.alloc(f32, rank * in_features) catch break :gpu_path;
        defer alloc.free(lora_a_t);
        const lora_b_t = alloc.alloc(f32, out_features * rank) catch break :gpu_path;
        defer alloc.free(lora_b_t);
        const tmp_grad_a = alloc.alloc(f32, rank * in_features) catch break :gpu_path;
        defer alloc.free(tmp_grad_a);
        const tmp_grad_b = alloc.alloc(f32, out_features * rank) catch break :gpu_path;
        defer alloc.free(tmp_grad_b);

        // adapter_a is [in_features, rank] → lora_a_t is [rank, in_features]
        transpose2DLora(lora_a_t, adapter_a.data, in_features, rank);
        // adapter_b is [rank, out_features] → lora_b_t is [out_features, rank]
        transpose2DLora(lora_b_t, adapter_b.data, rank, out_features);

        @memset(tmp_grad_a, 0);
        @memset(tmp_grad_b, 0);

        const gpu_ran = backend.accumulateLoRAGrads(
            alloc,
            tmp_grad_a,
            tmp_grad_b,
            inputs,
            output_grads,
            lora_a_t,
            lora_b_t,
            input_rows,
            in_features,
            out_features,
            rank,
            scale,
        ) catch break :gpu_path;

        if (!gpu_ran) break :gpu_path;

        // tmp_grad_a is [rank, in_features] → accumulate into grad_a [in_features, rank]
        const tmp_a_transposed = alloc.alloc(f32, in_features * rank) catch break :gpu_path;
        defer alloc.free(tmp_a_transposed);
        transpose2DLora(tmp_a_transposed, tmp_grad_a, rank, in_features);
        for (grad_a, tmp_a_transposed) |*dst, src| dst.* += src;

        // tmp_grad_b is [out_features, rank] → accumulate into grad_b [rank, out_features]
        const tmp_b_transposed = alloc.alloc(f32, rank * out_features) catch break :gpu_path;
        defer alloc.free(tmp_b_transposed);
        transpose2DLora(tmp_b_transposed, tmp_grad_b, out_features, rank);
        for (grad_b, tmp_b_transposed) |*dst, src| dst.* += src;

        return;
    }

    // CPU fallback.
    accumulateLinearLoRAGrads(grad_a, grad_b, input_rows, input_cols, inputs, output_cols, output_grads, adapter_a, adapter_b, alpha);
}

// ────────────────────────────────────────────────────────────────────────────
// DoRA: Weight-Decomposed Low-Rank Adaptation (Liu et al., ICML 2024).
//
// Decomposes the effective weight W_eff = base + scale * B @ A as
//     W_eff = m * (V / ||V||_col)
// where V = base + scale * B @ A, ||V||_col_j = sqrt(sum_i V[i,j]^2), and
// the per-column magnitude vector `m` is trainable. Forward pass applies this
// to an input row x as y = x @ (m ⊙ (V / ||V||_col))^T.
//
// The caller owns the base weight, the LoRA A/B factors (same layout as
// standard LoRA above), and a `m` vector of length `base.cols`. This module
// provides pure-Zig reference helpers for apply/merge/grad-accumulation.
//
// Layout convention (matches the CPU functions above):
//   base       : Matrix { rows = in_features,  cols = out_features } row-major
//   adapter_a  : Matrix { rows = in_features,  cols = rank          } row-major
//   adapter_b  : Matrix { rows = rank,         cols = out_features  } row-major
//   magnitude  : []f32 of length out_features
// ────────────────────────────────────────────────────────────────────────────

pub const DoRAView = struct {
    base: Matrix,
    adapter_a: Matrix,
    adapter_b: Matrix,
    magnitude: []const f32,
    alpha: f32,
    scaling: ScalingMode = .standard,
};

/// Compute per-column L2 norms of V = base + scale * A @ B into `out_norms`.
/// out_norms.len must equal base.cols.
pub fn doraColumnNorms(view: DoRAView, out_norms: []f32) void {
    const out_features = view.base.cols;
    std.debug.assert(out_norms.len == out_features);
    const in_features = view.base.rows;
    const rank = view.adapter_b.rows;
    const scale = effectiveScaleMode(view.alpha, rank, view.scaling);

    @memset(out_norms, 0);
    for (0..in_features) |i| {
        const base_row = view.base.data[i * out_features .. (i + 1) * out_features];
        const a_row = view.adapter_a.row(i);
        for (0..out_features) |j| {
            var v = base_row[j];
            if (rank != 0 and scale != 0) {
                var delta: f32 = 0;
                for (0..rank) |r| {
                    delta += a_row[r] * view.adapter_b.row(r)[j];
                }
                v += scale * delta;
            }
            out_norms[j] += v * v;
        }
    }
    for (out_norms) |*n| n.* = @sqrt(n.* + 1e-12);
}

/// Apply the DoRA-composed weight to a single input row `x` (length in_features),
/// writing the output into `out` (length out_features).
/// `col_norms` must be precomputed from doraColumnNorms for the current V.
pub fn doraApplyRow(
    view: DoRAView,
    x: []const f32,
    col_norms: []const f32,
    out: []f32,
) void {
    const in_features = view.base.rows;
    const out_features = view.base.cols;
    std.debug.assert(x.len == in_features);
    std.debug.assert(out.len == out_features);
    std.debug.assert(col_norms.len == out_features);
    std.debug.assert(view.magnitude.len == out_features);

    const rank = view.adapter_b.rows;
    const scale = effectiveScaleMode(view.alpha, rank, view.scaling);

    @memset(out, 0);
    for (0..in_features) |i| {
        const xi = x[i];
        if (xi == 0) continue;
        const base_row = view.base.data[i * out_features .. (i + 1) * out_features];
        for (0..out_features) |j| {
            out[j] += xi * base_row[j];
        }
        if (rank != 0 and scale != 0) {
            const a_row = view.adapter_a.row(i);
            for (0..rank) |r| {
                const ar = a_row[r];
                if (ar == 0) continue;
                const b_row = view.adapter_b.row(r);
                const m = xi * ar * scale;
                for (0..out_features) |j| {
                    out[j] += m * b_row[j];
                }
            }
        }
    }

    for (0..out_features) |j| {
        out[j] = view.magnitude[j] * out[j] / col_norms[j];
    }
}

/// Merge a DoRA adapter into a dense weight buffer.
/// out.len must equal base.rows * base.cols. The merged weight is
///     out[i, j] = m[j] * (base[i, j] + scale * (A @ B)[i, j]) / ||V||_col_j
pub fn doraMergeInto(view: DoRAView, out: []f32) void {
    const in_features = view.base.rows;
    const out_features = view.base.cols;
    std.debug.assert(out.len == in_features * out_features);
    std.debug.assert(view.magnitude.len == out_features);

    var norms_buf = std.heap.stackFallback(4096, std.heap.page_allocator);
    const alloc = norms_buf.get();
    const col_norms = alloc.alloc(f32, out_features) catch return;
    defer alloc.free(col_norms);
    doraColumnNorms(view, col_norms);

    const rank = view.adapter_b.rows;
    const scale = effectiveScaleMode(view.alpha, rank, view.scaling);

    for (0..in_features) |i| {
        const base_row = view.base.data[i * out_features .. (i + 1) * out_features];
        const a_row = view.adapter_a.row(i);
        const dst = out[i * out_features .. (i + 1) * out_features];
        for (0..out_features) |j| {
            var v = base_row[j];
            if (rank != 0 and scale != 0) {
                var delta: f32 = 0;
                for (0..rank) |r| {
                    delta += a_row[r] * view.adapter_b.row(r)[j];
                }
                v += scale * delta;
            }
            dst[j] = view.magnitude[j] * v / col_norms[j];
        }
    }
}

/// Accumulate DoRA gradients w.r.t. A, B, and the magnitude vector `m`.
///
/// Shapes (row-major):
///   inputs        [input_rows, in_features]
///   output_grads  [input_rows, out_features]
///   grad_a        [in_features, rank]
///   grad_b        [rank, out_features]
///   grad_m        [out_features]
///   col_norms     [out_features] — precomputed via doraColumnNorms
///
/// This is the "freeze the base weight, train LoRA + magnitude" regime from
/// the paper. The base weight gradient is NOT computed.
///
/// Note on approximation: DoRA's exact backward requires backpropping through
/// the per-column norm, which couples the gradient of every row of V. The
/// faithful formula is implemented here in O(in_features * out_features * rank);
/// callers with tight budgets can use the simpler-but-biased approximation
/// that treats ||V||_col as a constant during backward (common in practice).
pub fn accumulateDoRAGrads(
    grad_a: []f32,
    grad_b: []f32,
    grad_m: []f32,
    input_rows: usize,
    inputs: []const f32,
    output_grads: []const f32,
    view: DoRAView,
    col_norms: []const f32,
) void {
    const in_features = view.base.rows;
    const out_features = view.base.cols;
    const rank = view.adapter_b.rows;

    std.debug.assert(inputs.len == input_rows * in_features);
    std.debug.assert(output_grads.len == input_rows * out_features);
    std.debug.assert(grad_a.len == in_features * rank);
    std.debug.assert(grad_b.len == rank * out_features);
    std.debug.assert(grad_m.len == out_features);
    std.debug.assert(col_norms.len == out_features);
    std.debug.assert(view.magnitude.len == out_features);

    if (rank == 0) return;
    const scale = effectiveScaleMode(view.alpha, rank, view.scaling);

    var stack = std.heap.stackFallback(16 * 1024, std.heap.page_allocator);
    const alloc = stack.get();
    const v_col = alloc.alloc(f32, in_features) catch return;
    defer alloc.free(v_col);
    const dL_dv_col = alloc.alloc(f32, in_features) catch return;
    defer alloc.free(dL_dv_col);

    // Sum over samples of output_grads[:, j] * pre_norm_output[:, j] — where
    // pre_norm_output[r, j] is the "V-proj" output for sample r, i.e. x_r · V[:, j].
    // We also need sum_r output_grads[r, j] * x_r[i] which equals the projection
    // of the column-j residual onto the input.
    //
    // Iterate per output column j:
    for (0..out_features) |j| {
        const m_j = view.magnitude[j];
        const n_j = col_norms[j];
        const inv_n = 1.0 / n_j;

        // Reconstruct V[:, j] (length in_features) into v_col.
        for (0..in_features) |i| {
            var v = view.base.data[i * out_features + j];
            const a_row = view.adapter_a.row(i);
            var delta: f32 = 0;
            for (0..rank) |r| delta += a_row[r] * view.adapter_b.row(r)[j];
            v += scale * delta;
            v_col[i] = v;
        }

        // s_proj = sum_r output_grads[r, j] * (x_r · V[:, j]) — used for the
        //         chain-rule contribution from d||V||/dV.
        // s_dot  = sum_r output_grads[r, j] * x_r   (vector in R^{in_features}).
        // Use dL_dv_col as the s_dot accumulator (reused for clarity).
        @memset(dL_dv_col, 0);
        var s_proj: f32 = 0;
        var acc_m: f32 = 0;
        for (0..input_rows) |r| {
            const x_r = inputs[r * in_features .. (r + 1) * in_features];
            const g_rj = output_grads[r * out_features + j];
            if (g_rj == 0) continue;
            // x·V[:,j]
            var dot: f32 = 0;
            for (0..in_features) |i| dot += x_r[i] * v_col[i];
            s_proj += g_rj * dot;
            acc_m += g_rj * dot * inv_n;
            // Accumulate g_rj * x_r into dL_dv_col (scaled later).
            for (0..in_features) |i| dL_dv_col[i] += g_rj * x_r[i];
        }

        // Magnitude gradient: dL/dm_j = sum_r g_rj * (x_r · V[:, j]) / n_j
        grad_m[j] += acc_m;

        // dL/dV[:, j] = (m_j / n_j) * dL_dv_col - (m_j / n_j^3) * s_proj * V[:, j]
        const k1 = m_j * inv_n;
        const k2 = m_j * inv_n * inv_n * inv_n * s_proj;
        for (0..in_features) |i| {
            dL_dv_col[i] = k1 * dL_dv_col[i] - k2 * v_col[i];
        }

        // Backprop dL/dV[:, j] into A and B (V = base + scale * A @ B).
        //   dL/dA[i, r] += scale * dL/dV[i, j] * B[r, j]
        //   dL/dB[r, j] += scale * sum_i dL/dV[i, j] * A[i, r]
        for (0..rank) |r| {
            const b_rj = view.adapter_b.row(r)[j];
            const a_col_r_base = r; // A is [in_features, rank], so A[i, r] is at i*rank + r
            var b_grad_accum: f32 = 0;
            for (0..in_features) |i| {
                const dv = dL_dv_col[i];
                grad_a[i * rank + a_col_r_base] += scale * dv * b_rj;
                b_grad_accum += dv * view.adapter_a.data[i * rank + a_col_r_base];
            }
            grad_b[r * out_features + j] += scale * b_grad_accum;
        }
    }
}

test "effective scale rs_lora" {
    try std.testing.expectEqual(@as(f32, 32.0 / 16.0), effectiveScaleMode(32.0, 16, .standard));
    try std.testing.expectApproxEqAbs(@as(f32, 32.0 / 4.0), effectiveScaleMode(32.0, 16, .rs_lora), 1e-5);
    try std.testing.expectEqual(@as(f32, 0.0), effectiveScaleMode(1.0, 0, .rs_lora));
}

test "dora merge reduces to plain LoRA when m == ||V||_col" {
    // If magnitude[j] == ||V||_col[j], DoRA output = V = base + scale * A @ B.
    const base = [_]f32{
        1, 2,
        3, 4,
    };
    const a = [_]f32{
        1,
        2,
    };
    const b = [_]f32{ 10, 20 };

    const view = DoRAView{
        .base = .{ .rows = 2, .cols = 2, .data = &base },
        .adapter_a = .{ .rows = 2, .cols = 1, .data = &a },
        .adapter_b = .{ .rows = 1, .cols = 2, .data = &b },
        .magnitude = undefined,
        .alpha = 1.0,
        .scaling = .standard,
    };
    var col_norms: [2]f32 = undefined;
    doraColumnNorms(view, &col_norms);

    var view_m = view;
    view_m.magnitude = &col_norms;
    var out: [4]f32 = undefined;
    doraMergeInto(view_m, &out);

    // Expected: [11, 22, 23, 44] — same as the vanilla LoRA merge test.
    try std.testing.expectApproxEqAbs(@as(f32, 11), out[0], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 22), out[1], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 23), out[2], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 44), out[3], 1e-4);
}

test "dora grad shapes and finite values" {
    const base = [_]f32{
        0.1, 0.2,
        0.3, 0.4,
    };
    const a = [_]f32{
        0.5,
        0.25,
    };
    const b = [_]f32{ 0.7, -0.3 };
    const magnitude = [_]f32{ 1.1, 0.9 };

    const view = DoRAView{
        .base = .{ .rows = 2, .cols = 2, .data = &base },
        .adapter_a = .{ .rows = 2, .cols = 1, .data = &a },
        .adapter_b = .{ .rows = 1, .cols = 2, .data = &b },
        .magnitude = &magnitude,
        .alpha = 2.0,
        .scaling = .standard,
    };
    var col_norms: [2]f32 = undefined;
    doraColumnNorms(view, &col_norms);

    const inputs = [_]f32{ 1.0, 0.5 }; // 1 sample, 2 features
    const output_grads = [_]f32{ 0.8, -0.2 };

    var grad_a: [2]f32 = .{ 0, 0 };
    var grad_b: [2]f32 = .{ 0, 0 };
    var grad_m: [2]f32 = .{ 0, 0 };
    accumulateDoRAGrads(&grad_a, &grad_b, &grad_m, 1, &inputs, &output_grads, view, &col_norms);

    for (grad_a) |g| try std.testing.expect(std.math.isFinite(g));
    for (grad_b) |g| try std.testing.expect(std.math.isFinite(g));
    for (grad_m) |g| try std.testing.expect(std.math.isFinite(g));
    try std.testing.expect(grad_m[0] != 0 or grad_m[1] != 0);
}

test "dora grad matches finite-difference on 2-dim toy" {
    // 2×2 base, rank=1, single input row. The loss is L = 0.5 * sum(out_j^2)
    // so dL/dout = out and we can compare analytic grads against a symmetric
    // FD estimate of the full forward pass (recomputing col_norms and out).
    var base = [_]f32{
        0.1, 0.2,
        0.3, 0.4,
    };
    var a_buf = [_]f32{
        0.5,
        0.25,
    }; // [in=2, rank=1]; A[i,r] at i*rank + r
    var b_buf = [_]f32{ 0.7, -0.3 }; // [rank=1, out=2]; B[r,j] at r*out + j
    var magnitude = [_]f32{ 1.1, 0.9 };
    const inputs = [_]f32{ 1.0, 0.5 };

    const make_view = struct {
        fn call(
            base_data: []const f32,
            a_data: []const f32,
            b_data: []const f32,
            mag: []const f32,
        ) DoRAView {
            return .{
                .base = .{ .rows = 2, .cols = 2, .data = base_data },
                .adapter_a = .{ .rows = 2, .cols = 1, .data = a_data },
                .adapter_b = .{ .rows = 1, .cols = 2, .data = b_data },
                .magnitude = mag,
                .alpha = 2.0,
                .scaling = .standard,
            };
        }
    }.call;

    // Forward pass at original parameters — produces d_output = out and
    // the corresponding col_norms that `accumulateDoRAGrads` expects.
    const view = make_view(&base, &a_buf, &b_buf, &magnitude);
    var col_norms: [2]f32 = undefined;
    doraColumnNorms(view, &col_norms);
    var out_baseline: [2]f32 = undefined;
    doraApplyRow(view, inputs[0..2], &col_norms, &out_baseline);

    // For L = 0.5 * ||out||^2, dL/dout = out.
    const d_output: [2]f32 = out_baseline;

    var grad_a: [2]f32 = .{ 0, 0 };
    var grad_b: [2]f32 = .{ 0, 0 };
    var grad_m: [2]f32 = .{ 0, 0 };
    accumulateDoRAGrads(&grad_a, &grad_b, &grad_m, 1, &inputs, &d_output, view, &col_norms);

    // Helper: compute L at current parameter state. Recomputes col_norms
    // and output so FD captures the full chain rule through ||V||_col.
    const loss_fn = struct {
        fn call(
            base_data: []const f32,
            a_data: []const f32,
            b_data: []const f32,
            mag: []const f32,
            x: []const f32,
        ) f32 {
            const v = DoRAView{
                .base = .{ .rows = 2, .cols = 2, .data = base_data },
                .adapter_a = .{ .rows = 2, .cols = 1, .data = a_data },
                .adapter_b = .{ .rows = 1, .cols = 2, .data = b_data },
                .magnitude = mag,
                .alpha = 2.0,
                .scaling = .standard,
            };
            var cn: [2]f32 = undefined;
            doraColumnNorms(v, &cn);
            var out: [2]f32 = undefined;
            doraApplyRow(v, x[0..2], &cn, &out);
            return 0.5 * (out[0] * out[0] + out[1] * out[1]);
        }
    }.call;

    const h: f32 = 1e-4;
    const tol: f32 = 1e-3;

    // Perturbation helper that returns (L_plus - L_minus)/(2h) for a slot.
    const ParamKind = enum { a, b, m };
    const Perturb = struct {
        kind: ParamKind,
        idx: usize,
        label: []const u8,
        analytic: f32,
    };

    const checks = [_]Perturb{
        .{ .kind = .a, .idx = 0, .label = "a[0]", .analytic = grad_a[0] },
        .{ .kind = .a, .idx = 1, .label = "a[1]", .analytic = grad_a[1] },
        .{ .kind = .b, .idx = 0, .label = "b[0]", .analytic = grad_b[0] },
        .{ .kind = .b, .idx = 1, .label = "b[1]", .analytic = grad_b[1] },
        .{ .kind = .m, .idx = 0, .label = "magnitude[0]", .analytic = grad_m[0] },
        .{ .kind = .m, .idx = 1, .label = "magnitude[1]", .analytic = grad_m[1] },
    };

    for (checks) |chk| {
        const slot: *f32 = switch (chk.kind) {
            .a => &a_buf[chk.idx],
            .b => &b_buf[chk.idx],
            .m => &magnitude[chk.idx],
        };
        const save = slot.*;
        slot.* = save + h;
        const lp = loss_fn(&base, &a_buf, &b_buf, &magnitude, &inputs);
        slot.* = save - h;
        const lm = loss_fn(&base, &a_buf, &b_buf, &magnitude, &inputs);
        slot.* = save;
        const fd = (lp - lm) / (2.0 * h);
        if (@abs(fd - chk.analytic) > tol) {
            std.debug.print(
                "DoRA FD mismatch at {s}: analytic={d:.6} fd={d:.6} diff={d:.6}\n",
                .{ chk.label, chk.analytic, fd, fd - chk.analytic },
            );
            return error.TestFailed;
        }
    }
}

test "parse layer tensor names" {
    const parsed_a = parseLayerTensorName("encoder.layer.0.attention.self.query.lora_A.weight").?;
    try std.testing.expectEqualStrings("encoder.layer.0.attention.self.query", parsed_a.layer_name);
    try std.testing.expectEqual(.a, parsed_a.kind);

    const parsed_b = parseLayerTensorName("/encoder.layer.0.attention.self.query/lora_B").?;
    try std.testing.expectEqualStrings("encoder.layer.0.attention.self.query", parsed_b.layer_name);
    try std.testing.expectEqual(.b, parsed_b.kind);
}

test "merge low rank delta into base" {
    const base = [_]f32{
        1, 2,
        3, 4,
    };
    const a = [_]f32{
        1,
        2,
    };
    const b = [_]f32{
        10, 20,
    };
    var out: [4]f32 = undefined;
    mergeInto(
        .{ .rows = 2, .cols = 2, .data = &base },
        .{ .rows = 2, .cols = 1, .data = &a },
        .{ .rows = 1, .cols = 2, .data = &b },
        1.0,
        &out,
    );
    try std.testing.expectEqualSlices(f32, &.{ 11, 22, 23, 44 }, &out);
}
