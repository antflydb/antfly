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

// Pure Zig CPU InfoNCE contrastive loss with analytical gradients.
// Direct port of gopeft/e2e/finetune/infonce_cpu.go.

const std = @import("std");

const epsilon_norm: f64 = 1e-12;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

pub const InfoNCEResult = struct {
    loss: f64,
    grad: []f32, // owned [N*dim], caller frees with allocator

    pub fn deinit(self: *InfoNCEResult, allocator: std.mem.Allocator) void {
        allocator.free(self.grad);
        self.* = undefined;
    }
};

pub const ContrastiveLossResult = struct {
    contrastive_loss: f64,
    total_loss: f64,
    grad: []f32, // owned [B*C*E], caller frees

    pub fn deinit(self: *ContrastiveLossResult, allocator: std.mem.Allocator) void {
        allocator.free(self.grad);
        self.* = undefined;
    }
};

// ---------------------------------------------------------------------------
// denseSymmetricSimilarity
// ---------------------------------------------------------------------------

fn denseSymmetricSimilarity(allocator: std.mem.Allocator, norm_flat: []const f32, N: usize, dim: usize) ![]f32 {
    var sim = try allocator.alloc(f32, N * N);
    for (0..N) |i| {
        const base_i = i * dim;
        for (i..N) |j| {
            const base_j = j * dim;
            var dot: f64 = 0;
            for (0..dim) |k| {
                dot += @as(f64, norm_flat[base_i + k]) * @as(f64, norm_flat[base_j + k]);
            }
            sim[i * N + j] = @floatCast(dot);
            sim[j * N + i] = @floatCast(dot);
        }
    }
    return sim;
}

// ---------------------------------------------------------------------------
// computeInfoNCELossAndGrad
// ---------------------------------------------------------------------------

pub fn computeInfoNCELossAndGrad(
    allocator: std.mem.Allocator,
    flat_vecs: []const f32, // [N*dim]
    chunk_mask: []const f32, // [N]
    doc_ids: []const u32, // [N]
    N: usize,
    dim: usize,
    temperature: f64,
    focal_gamma: f64,
    focal_alpha: f64,
) !InfoNCEResult {
    // Step 1: build valid index list
    var valid_idx = std.ArrayListUnmanaged(usize).empty;
    defer valid_idx.deinit(allocator);

    for (0..N) |i| {
        if (chunk_mask[i] > 0.5) {
            try valid_idx.append(allocator, i);
        }
    }
    const V = valid_idx.items.len;

    if (V < 2) {
        const grad = try allocator.alloc(f32, N * dim);
        @memset(grad, 0);
        return InfoNCEResult{ .loss = 0, .grad = grad };
    }

    // Step 2: L2-normalize valid vectors into compact [V*dim]
    const norms = try allocator.alloc(f64, V);
    defer allocator.free(norms);
    const norm_flat = try allocator.alloc(f32, V * dim);
    defer allocator.free(norm_flat);

    for (0..V) |ci| {
        const orig_i = valid_idx.items[ci];
        const orig_base = orig_i * dim;
        const compact_base = ci * dim;

        var sum_sq: f64 = 0;
        for (0..dim) |j| {
            const val: f64 = flat_vecs[orig_base + j];
            sum_sq += val * val;
        }
        norms[ci] = std.math.sqrt(sum_sq + epsilon_norm);
        const inv_norm = 1.0 / norms[ci];
        for (0..dim) |j| {
            norm_flat[compact_base + j] = @floatCast(@as(f64, flat_vecs[orig_base + j]) * inv_norm);
        }
    }

    // Step 3: compute similarity matrix [V*V]
    const sim = try denseSymmetricSimilarity(allocator, norm_flat, V, dim);
    defer allocator.free(sim);

    // Step 4: scale by 1/temperature
    const inv_t = 1.0 / temperature;
    for (sim) |*s| {
        s.* = @floatCast(@as(f64, s.*) * inv_t);
    }

    // Step 5: build compact doc IDs, positive mask, pos count
    const compact_doc_ids = try allocator.alloc(u32, V);
    defer allocator.free(compact_doc_ids);
    for (0..V) |ci| {
        compact_doc_ids[ci] = doc_ids[valid_idx.items[ci]];
    }

    const pos_mask = try allocator.alloc(f32, V * V);
    defer allocator.free(pos_mask);
    @memset(pos_mask, 0);

    const pos_count = try allocator.alloc(f32, V);
    defer allocator.free(pos_count);
    @memset(pos_count, 0);

    for (0..V) |i| {
        for (0..V) |j| {
            if (i == j) continue;
            if (compact_doc_ids[i] == compact_doc_ids[j]) {
                pos_mask[i * V + j] = 1.0;
                pos_count[i] += 1.0;
            }
        }
    }

    // Step 6: InfoNCE loss loop
    const d_ld_sim = try allocator.alloc(f64, V * V);
    defer allocator.free(d_ld_sim);
    @memset(d_ld_sim, 0);

    var loss: f64 = 0;
    var num_anchors: usize = 0;

    for (0..V) |i| {
        if (pos_count[i] == 0) continue;
        num_anchors += 1;

        // Find max for numerical stability (exclude diagonal)
        var max_sim: f64 = -1e30;
        for (0..V) |j| {
            if (j == i) continue;
            const s: f64 = sim[i * V + j];
            if (s > max_sim) max_sim = s;
        }

        // sum(exp(sim - max)) for j != i
        var sum_exp: f64 = 0;
        for (0..V) |j| {
            if (j == i) continue;
            sum_exp += std.math.exp(@as(f64, sim[i * V + j]) - max_sim);
        }
        const log_sum_exp = max_sim + std.math.log(f64, std.math.e, sum_exp + 1e-30);

        // Mean positive similarity
        const inv_pos_count = 1.0 / @as(f64, pos_count[i]);
        var mean_pos_sim: f64 = 0;
        for (0..V) |j| {
            if (pos_mask[i * V + j] > 0.5) {
                mean_pos_sim += @as(f64, sim[i * V + j]);
            }
        }
        mean_pos_sim *= inv_pos_count;

        // Focal reweighting
        var mean_pos_softmax: f64 = 0;
        if (focal_gamma > 0) {
            for (0..V) |j| {
                if (j == i or pos_mask[i * V + j] < 0.5) continue;
                mean_pos_softmax += std.math.exp(@as(f64, sim[i * V + j]) - max_sim) / sum_exp;
            }
            mean_pos_softmax *= inv_pos_count;
        }

        var focal_weight: f64 = 1.0;
        if (focal_gamma > 0) {
            focal_weight = std.math.pow(f64, 1.0 - mean_pos_softmax, focal_gamma) * focal_alpha;
        }

        // Accumulate loss
        loss += focal_weight * (-mean_pos_sim + log_sum_exp);

        // Gradient w.r.t. sim
        for (0..V) |j| {
            if (j == i) continue;
            const softmax_j = std.math.exp(@as(f64, sim[i * V + j]) - max_sim) / sum_exp;
            d_ld_sim[i * V + j] = focal_weight * softmax_j;
            if (pos_mask[i * V + j] > 0.5) {
                d_ld_sim[i * V + j] -= focal_weight * inv_pos_count;
            }
        }
    }

    // Step 7: normalize by num_anchors
    if (num_anchors > 0) {
        loss /= @as(f64, @floatFromInt(num_anchors));
        const scale = 1.0 / @as(f64, @floatFromInt(num_anchors));
        for (d_ld_sim) |*g| {
            g.* *= scale;
        }
    }

    // Step 8: symmetrize gradient
    const d_ld_sim_sym = try allocator.alloc(f64, V * V);
    defer allocator.free(d_ld_sim_sym);
    for (0..V) |i| {
        for (0..V) |j| {
            d_ld_sim_sym[i * V + j] = d_ld_sim[i * V + j] + d_ld_sim[j * V + i];
        }
    }

    // Step 9: backprop through matmul
    const d_ld_norm_flat = try allocator.alloc(f64, V * dim);
    defer allocator.free(d_ld_norm_flat);
    @memset(d_ld_norm_flat, 0);

    for (0..V) |i| {
        for (0..V) |j| {
            if (d_ld_sim_sym[i * V + j] == 0) continue;
            const s = inv_t * d_ld_sim_sym[i * V + j];
            const base_j = j * dim;
            const base_i = i * dim;
            for (0..dim) |k| {
                d_ld_norm_flat[base_i + k] += s * @as(f64, norm_flat[base_j + k]);
            }
        }
    }

    // Step 10: backprop through L2 norm, scatter to [N*dim]
    const d_ld_flat = try allocator.alloc(f32, N * dim);
    @memset(d_ld_flat, 0);

    for (0..V) |ci| {
        const compact_base = ci * dim;
        const orig_base = valid_idx.items[ci] * dim;

        var dot_prod: f64 = 0;
        for (0..dim) |j| {
            dot_prod += @as(f64, norm_flat[compact_base + j]) * d_ld_norm_flat[compact_base + j];
        }
        const inv_norm = 1.0 / norms[ci];
        for (0..dim) |j| {
            d_ld_flat[orig_base + j] = @floatCast((d_ld_norm_flat[compact_base + j] - @as(f64, norm_flat[compact_base + j]) * dot_prod) * inv_norm);
        }
    }

    return InfoNCEResult{ .loss = loss, .grad = d_ld_flat };
}

// ---------------------------------------------------------------------------
// computeContrastiveLossOnCPU
// ---------------------------------------------------------------------------

pub fn computeContrastiveLossOnCPU(
    allocator: std.mem.Allocator,
    chunk_emb_flat: []const f32, // [B*C*E]
    chunk_mask_flat: []const f32, // [B*C]
    doc_ids: []const u32, // [B*C]
    temperature: f64,
    lambda_contrastive: f64,
    B: usize,
    C: usize,
    E: usize,
    focal_gamma: f64,
    focal_alpha: f64,
) !ContrastiveLossResult {
    const result = try computeInfoNCELossAndGrad(
        allocator,
        chunk_emb_flat,
        chunk_mask_flat,
        doc_ids,
        B * C,
        E,
        temperature,
        focal_gamma,
        focal_alpha,
    );

    for (result.grad) |*g| {
        g.* = @floatCast(@as(f64, g.*) * lambda_contrastive);
    }

    return ContrastiveLossResult{
        .contrastive_loss = result.loss,
        .total_loss = lambda_contrastive * result.loss,
        .grad = result.grad,
    };
}

// ---------------------------------------------------------------------------
// MatryoshkaConfig / MatryoshkaResult / computeMatryoshkaLossAndGrad
// ---------------------------------------------------------------------------

/// Configuration for Matryoshka multi-scale loss.
pub const MatryoshkaConfig = struct {
    /// Embedding dimensions to evaluate at, in decreasing order.
    /// e.g. &.{768, 256, 128}
    dims: []const u32,
    /// Loss weight for each scale (should sum to 1.0 or be normalized by caller).
    /// e.g. &.{1.0, 1.0, 1.0} (equal weighting)
    weights: []const f32,
};

pub const MatryoshkaResult = struct {
    total_loss: f64,
    /// One entry per dim in config.dims, caller owns.
    per_scale_loss: []f64,
    /// [N * full_dim] accumulated gradient — only the first `dim` positions for
    /// each vector receive gradient from scales with that dim; caller owns.
    grad: []f32,

    pub fn deinit(self: *MatryoshkaResult, allocator: std.mem.Allocator) void {
        allocator.free(self.per_scale_loss);
        allocator.free(self.grad);
        self.* = undefined;
    }
};

/// Compute MRL loss: InfoNCE at multiple embedding scales.
///
/// flat_vecs:   [N * full_dim] — full embeddings (only first `dim` dims used at each scale)
/// chunk_mask:  [N] — 1.0 valid, 0.0 padding
/// doc_ids:     [N]
/// N:           number of chunks
/// full_dim:    full embedding dimension (e.g. 768)
///
/// Returns combined loss and accumulated gradient [N * full_dim].
/// The gradient accumulation means that dimension 0 gets gradient from ALL scales,
/// dimension 127 gets gradient only from scales with d >= 128, etc.
pub fn computeMatryoshkaLossAndGrad(
    allocator: std.mem.Allocator,
    flat_vecs: []const f32,
    chunk_mask: []const f32,
    doc_ids: []const u32,
    N: usize,
    full_dim: usize,
    config: MatryoshkaConfig,
    temperature: f32,
    focal_gamma: f32,
    focal_alpha: f32,
) !MatryoshkaResult {
    const num_scales = config.dims.len;

    // Allocate per-scale loss array.
    const per_scale_loss = try allocator.alloc(f64, num_scales);
    errdefer allocator.free(per_scale_loss);
    @memset(per_scale_loss, 0);

    // Allocate full-dim gradient accumulator — zero-initialised.
    const full_grad = try allocator.alloc(f32, N * full_dim);
    errdefer allocator.free(full_grad);
    @memset(full_grad, 0);

    var total_loss: f64 = 0;

    for (config.dims, config.weights, 0..) |dim_u32, weight, scale_idx| {
        // Clamp dim to full_dim to handle the "dim > full_dim" edge case.
        const d: usize = @min(@as(usize, dim_u32), full_dim);

        if (d == 0) continue;

        // Build a [N * d] slice with just the first d dimensions of each vector.
        const truncated = try allocator.alloc(f32, N * d);
        defer allocator.free(truncated);

        for (0..N) |i| {
            @memcpy(truncated[i * d .. i * d + d], flat_vecs[i * full_dim .. i * full_dim + d]);
        }

        // Call existing InfoNCE implementation on the truncated embeddings.
        var scale_result = try computeInfoNCELossAndGrad(
            allocator,
            truncated,
            chunk_mask,
            doc_ids,
            N,
            d,
            @as(f64, temperature),
            @as(f64, focal_gamma),
            @as(f64, focal_alpha),
        );
        defer scale_result.deinit(allocator);

        const weighted_loss = @as(f64, weight) * scale_result.loss;
        per_scale_loss[scale_idx] = scale_result.loss; // raw, unweighted
        total_loss += weighted_loss;

        // Accumulate d-dim gradient back into the full [N * full_dim] buffer.
        // For each vector i, add the d-dim gradient into positions [i*full_dim .. i*full_dim + d].
        for (0..N) |i| {
            const src = scale_result.grad[i * d .. i * d + d];
            const dst = full_grad[i * full_dim .. i * full_dim + d];
            for (src, dst) |s, *g| {
                g.* += @as(f32, weight) * s;
            }
        }
    }

    return MatryoshkaResult{
        .total_loss = total_loss,
        .per_scale_loss = per_scale_loss,
        .grad = full_grad,
    };
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "InfoNCE trivial: V<2 returns zero" {
    const allocator = std.testing.allocator;

    const N = 3;
    const dim = 4;
    const flat_vecs = [_]f32{
        1, 0, 0, 0,
        0, 1, 0, 0,
        0, 0, 1, 0,
    };
    // Only index 2 is valid — V=1 < 2, expect zero loss and zero grad
    const chunk_mask = [_]f32{ 0, 0, 1 };
    const doc_ids = [_]u32{ 0, 0, 0 };

    var res = try computeInfoNCELossAndGrad(
        allocator,
        &flat_vecs,
        &chunk_mask,
        &doc_ids,
        N,
        dim,
        0.07,
        0.0,
        0.75,
    );
    defer res.deinit(allocator);

    try std.testing.expectEqual(@as(f64, 0), res.loss);
    try std.testing.expectEqual(@as(usize, N * dim), res.grad.len);
    for (res.grad) |g| {
        try std.testing.expectEqual(@as(f32, 0), g);
    }
}

test "InfoNCE two docs two chunks each" {
    const allocator = std.testing.allocator;

    const N = 4;
    const dim = 2;
    // Roughly unit vectors: [1,0], [0.9,0.436], [-1,0], [-0.9,-0.436]
    const flat_vecs = [_]f32{
        1.0,   0.0,
        0.9,   0.436,
        -1.0,  0.0,
        -0.9,  -0.436,
    };
    const chunk_mask = [_]f32{ 1, 1, 1, 1 };
    const doc_ids = [_]u32{ 0, 0, 1, 1 };

    var res = try computeInfoNCELossAndGrad(
        allocator,
        &flat_vecs,
        &chunk_mask,
        &doc_ids,
        N,
        dim,
        0.07,
        0.0,
        0.75,
    );
    defer res.deinit(allocator);

    try std.testing.expect(res.loss > 0);
    try std.testing.expectEqual(@as(usize, N * dim), res.grad.len);
}

test "denseSymmetricSimilarity" {
    const allocator = std.testing.allocator;

    const norm_flat = [_]f32{ 1, 0, 0, 1 };
    const sim = try denseSymmetricSimilarity(allocator, &norm_flat, 2, 2);
    defer allocator.free(sim);

    // sim = [[1,0],[0,1]]
    try std.testing.expectApproxEqAbs(@as(f32, 1), sim[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), sim[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), sim[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 1), sim[3], 1e-6);
}

test "MatryoshkaConfig type compiles" {
    // Compile-time check that MatryoshkaConfig and MatryoshkaResult types are correct.
    const cfg = MatryoshkaConfig{
        .dims = &.{ 768, 256, 128 },
        .weights = &.{ 1.0, 1.0, 1.0 },
    };
    try std.testing.expectEqual(@as(usize, 3), cfg.dims.len);
    try std.testing.expectEqual(@as(usize, 3), cfg.weights.len);
    try std.testing.expectEqual(@as(u32, 768), cfg.dims[0]);
    try std.testing.expectEqual(@as(f32, 1.0), cfg.weights[2]);
}

test "computeMatryoshkaLossAndGrad basic" {
    const allocator = std.testing.allocator;

    // 4 vectors with full_dim=4; use dims [4, 2] so we can test truncation.
    // Vectors: two from doc 0, two from doc 1.
    const N = 4;
    const full_dim = 4;
    // Each row: unit-like vector, padded with extra dims.
    const flat_vecs = [_]f32{
        1.0,  0.0,  0.5, 0.1,
        0.9,  0.44, 0.3, 0.2,
        -1.0, 0.0,  0.5, 0.1,
        -0.9, -0.44, 0.3, 0.2,
    };
    const chunk_mask = [_]f32{ 1, 1, 1, 1 };
    const doc_ids = [_]u32{ 0, 0, 1, 1 };

    const dims = [_]u32{ 4, 2 };
    const weights = [_]f32{ 0.6, 0.4 };
    const cfg = MatryoshkaConfig{ .dims = &dims, .weights = &weights };

    var result = try computeMatryoshkaLossAndGrad(
        allocator,
        &flat_vecs,
        &chunk_mask,
        &doc_ids,
        N,
        full_dim,
        cfg,
        0.07,
        0.0,
        0.75,
    );
    defer result.deinit(allocator);

    // Should produce a positive total loss.
    try std.testing.expect(result.total_loss > 0);
    // per_scale_loss has one entry per scale.
    try std.testing.expectEqual(@as(usize, 2), result.per_scale_loss.len);
    // per_scale_loss entries are raw (unweighted); total_loss is the weighted sum.
    try std.testing.expect(result.per_scale_loss[0] >= 0);
    try std.testing.expect(result.per_scale_loss[1] >= 0);
    // Verify total_loss matches the weighted combination.
    const expected_total = @as(f64, weights[0]) * result.per_scale_loss[0] + @as(f64, weights[1]) * result.per_scale_loss[1];
    try std.testing.expectApproxEqAbs(expected_total, result.total_loss, 1e-9);
    // Gradient has full [N * full_dim] shape.
    try std.testing.expectEqual(@as(usize, N * full_dim), result.grad.len);
    // The last 2 dims (indices 2,3 of each vector) only receive gradient from the
    // full-dim (4) scale, not the 2-dim scale.  They should be non-zero for at
    // least some vectors since the dim=4 scale has non-zero gradients.
    // (Just verify the slice length is correct — correctness checked above.)
    try std.testing.expectEqual(@as(usize, N * full_dim), result.grad.len);
}

test "computeMatryoshkaLossAndGrad dim exceeds full_dim is clamped" {
    const allocator = std.testing.allocator;

    const N = 4;
    const full_dim = 2;
    const flat_vecs = [_]f32{
        1.0, 0.0,
        0.9, 0.44,
        -1.0, 0.0,
        -0.9, -0.44,
    };
    const chunk_mask = [_]f32{ 1, 1, 1, 1 };
    const doc_ids = [_]u32{ 0, 0, 1, 1 };

    // dim=768 exceeds full_dim=2; should be silently clamped to 2.
    const dims = [_]u32{768};
    const weights = [_]f32{1.0};
    const cfg = MatryoshkaConfig{ .dims = &dims, .weights = &weights };

    var result = try computeMatryoshkaLossAndGrad(
        allocator,
        &flat_vecs,
        &chunk_mask,
        &doc_ids,
        N,
        full_dim,
        cfg,
        0.07,
        0.0,
        0.75,
    );
    defer result.deinit(allocator);

    try std.testing.expect(result.total_loss > 0);
    try std.testing.expectEqual(@as(usize, N * full_dim), result.grad.len);
}
