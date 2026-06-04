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

// SPLADE sparse embedding head for the fused-chunker model.
//
// SPLADE produces vocabulary-space sparse vectors:
//   v[vocab] = max_pool_over_tokens( log(1 + relu(hidden @ W^T)) )
//
// These sparse vectors enable inverted-index retrieval.
// Weight key: SPLADE_WEIGHT_KEY [vocab_size, hidden_size]

const std = @import("std");

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

/// Safetensors weight key for the SPLADE projection matrix [vocab_size, hidden_size].
pub const SPLADE_WEIGHT_KEY = "fused_chunker_embedder/splade_head/proj/weight";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

pub const SpladePooling = enum { max, mean };

pub const SpladeConfig = struct {
    vocab_size: u32 = 50368,
    flops_weight: f32 = 3e-5, // regularisation strength
    temperature: f32 = 0.07,
    pooling: SpladePooling = .max,
};

// ---------------------------------------------------------------------------
// computeSpladeActivation
// ---------------------------------------------------------------------------

/// Compute a SPLADE sparse activation vector from flat hidden states.
///
/// hidden       : [total_tokens * hidden_size] f32, row-major
/// weight       : [vocab_size * hidden_size] f32, row-major  (W; no bias)
/// out_splade   : [vocab_size] f32, caller-allocated and pre-zeroed
///
/// Algorithm (max-pooling variant):
///   for each token t:
///     proj[v] = dot(hidden[t*H..(t+1)*H], weight[v*H..(v+1)*H])
///     activated[t, v] = log(1 + max(0, proj[t, v]))
///   out[v] = max over t of activated[t, v]
///
/// For mean pooling, replace max with sum/count.
pub fn computeSpladeActivation(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    weight: []const f32,
    out_splade: []f32,
    total_tokens: usize,
    hidden_size: usize,
    vocab_size: u32,
    pooling: SpladePooling,
) !void {
    const V: usize = @intCast(vocab_size);
    const H = hidden_size;

    std.debug.assert(hidden.len == total_tokens * H);
    std.debug.assert(weight.len == V * H);
    std.debug.assert(out_splade.len == V);

    @memset(out_splade, 0);

    switch (pooling) {
        .max => {
            for (0..total_tokens) |t| {
                const h_base = t * H;
                for (0..V) |v| {
                    const w_base = v * H;
                    var dot: f32 = 0;
                    for (0..H) |k| {
                        dot += hidden[h_base + k] * weight[w_base + k];
                    }
                    const act = std.math.log1p(@max(0.0, dot));
                    if (act > out_splade[v]) out_splade[v] = act;
                }
            }
        },
        .mean => {
            // Use f64 accumulator to reduce rounding error.
            const acc = try allocator.alloc(f64, V);
            defer allocator.free(acc);
            @memset(acc, 0);

            for (0..total_tokens) |t| {
                const h_base = t * H;
                for (0..V) |v| {
                    const w_base = v * H;
                    var dot: f32 = 0;
                    for (0..H) |k| {
                        dot += hidden[h_base + k] * weight[w_base + k];
                    }
                    acc[v] += std.math.log1p(@max(0.0, dot));
                }
            }

            if (total_tokens > 0) {
                const inv_count: f64 = 1.0 / @as(f64, @floatFromInt(total_tokens));
                for (0..V) |v| {
                    out_splade[v] = @floatCast(acc[v] * inv_count);
                }
            }
        },
    }
}

// ---------------------------------------------------------------------------
// computeSpladeFlopsLoss
// ---------------------------------------------------------------------------

/// FLOPS regularisation: encourages output sparsity.
///
///   L_flops = flops_weight * (1 / num_chunks) * sum_c( sum_v |splade_vecs[c, v]| )
///
/// splade_vecs : [num_chunks * vocab_size] f32, row-major
/// out_grad    : [num_chunks * vocab_size] f32, caller-allocated (will be overwritten)
///
/// Returns the scalar loss value.
pub fn computeSpladeFlopsLoss(
    splade_vecs: []const f32,
    out_grad: []f32,
    num_chunks: usize,
    vocab_size: u32,
    flops_weight: f32,
) f32 {
    const V: usize = @intCast(vocab_size);

    std.debug.assert(splade_vecs.len == num_chunks * V);
    std.debug.assert(out_grad.len == num_chunks * V);

    // Accumulate sum of absolute values.
    var total_abs: f64 = 0;
    for (splade_vecs) |val| {
        total_abs += @abs(@as(f64, val));
    }

    const inv_n: f64 = if (num_chunks > 0) 1.0 / @as(f64, @floatFromInt(num_chunks)) else 0.0;
    // Note: this uses mean L1 (not squared mean L1 as in the SPLADE paper).
    // L1 mean is a simpler but effective sparsity regularizer.
    const loss: f32 = @floatCast(flops_weight * total_abs * inv_n);

    // Gradient: d/dv |v| = sign(v), scaled by flops_weight / num_chunks.
    const grad_scale: f32 = @floatCast(flops_weight * inv_n);
    for (0..splade_vecs.len) |i| {
        const v = splade_vecs[i];
        out_grad[i] = if (v > 0) grad_scale else if (v < 0) -grad_scale else 0.0;
    }

    return loss;
}

// ---------------------------------------------------------------------------
// computeSpladeContrastiveLoss
// ---------------------------------------------------------------------------

pub const SpladeContrastiveResult = struct {
    loss: f64,
    grad: []f32, // [num_chunks * vocab_size], caller owns

    pub fn deinit(self: *SpladeContrastiveResult, allocator: std.mem.Allocator) void {
        allocator.free(self.grad);
        self.* = undefined;
    }
};

/// InfoNCE contrastive loss on sparse vocabulary-space vectors.
///
/// Similarity = dot product (no L2 normalisation — SPLADE vecs are inherently
/// non-negative sparse).
///
/// splade_vecs : [num_chunks * vocab_size] f32, row-major
/// chunk_mask  : [num_chunks] f32  (1.0 = valid, 0.0 = padding)
/// doc_ids     : [num_chunks] u32  (equal doc_id → positive pair)
///
/// Returns SpladeContrastiveResult with loss and gradient [num_chunks * vocab_size].
pub fn computeSpladeContrastiveLoss(
    allocator: std.mem.Allocator,
    splade_vecs: []const f32,
    chunk_mask: []const f32,
    doc_ids: []const u32,
    num_chunks: usize,
    vocab_size: u32,
    temperature: f32,
) !SpladeContrastiveResult {
    const V: usize = @intCast(vocab_size);
    const N = num_chunks;

    // Step 1: collect valid indices.
    var valid_idx = std.ArrayListUnmanaged(usize).empty;
    defer valid_idx.deinit(allocator);

    for (0..N) |i| {
        if (chunk_mask[i] > 0.5) {
            try valid_idx.append(allocator, i);
        }
    }
    const Vn = valid_idx.items.len; // number of valid chunks (renamed to avoid shadowing V)

    // Allocate output gradient (full [N*V], padded chunks stay zero).
    const grad = try allocator.alloc(f32, N * V);
    errdefer allocator.free(grad);
    @memset(grad, 0);

    if (Vn < 2) {
        return SpladeContrastiveResult{ .loss = 0, .grad = grad };
    }

    // Step 2: compute compact similarity matrix [Vn * Vn] using dot products.
    //         sim[i, j] = dot(splade_vecs[valid_idx[i]], splade_vecs[valid_idx[j]])
    const inv_t: f64 = 1.0 / @as(f64, temperature);

    const sim = try allocator.alloc(f64, Vn * Vn);
    defer allocator.free(sim);

    for (0..Vn) |ci| {
        const base_i = valid_idx.items[ci] * V;
        for (ci..Vn) |cj| {
            const base_j = valid_idx.items[cj] * V;
            var dot: f64 = 0;
            for (0..V) |k| {
                dot += @as(f64, splade_vecs[base_i + k]) * @as(f64, splade_vecs[base_j + k]);
            }
            // Scale by temperature here.
            const scaled = dot * inv_t;
            sim[ci * Vn + cj] = scaled;
            sim[cj * Vn + ci] = scaled;
        }
    }

    // Step 3: build compact doc_ids, positive mask, positive count.
    const compact_doc_ids = try allocator.alloc(u32, Vn);
    defer allocator.free(compact_doc_ids);
    for (0..Vn) |ci| {
        compact_doc_ids[ci] = doc_ids[valid_idx.items[ci]];
    }

    const pos_mask = try allocator.alloc(f32, Vn * Vn);
    defer allocator.free(pos_mask);
    @memset(pos_mask, 0);

    const pos_count = try allocator.alloc(f32, Vn);
    defer allocator.free(pos_count);
    @memset(pos_count, 0);

    for (0..Vn) |ci| {
        for (0..Vn) |cj| {
            if (ci == cj) continue;
            if (compact_doc_ids[ci] == compact_doc_ids[cj]) {
                pos_mask[ci * Vn + cj] = 1.0;
                pos_count[ci] += 1.0;
            }
        }
    }

    // Step 4: InfoNCE loss + gradient w.r.t. sim.
    const d_ld_sim = try allocator.alloc(f64, Vn * Vn);
    defer allocator.free(d_ld_sim);
    @memset(d_ld_sim, 0);

    var loss: f64 = 0;
    var num_anchors: usize = 0;

    for (0..Vn) |ci| {
        if (pos_count[ci] == 0) continue;
        num_anchors += 1;

        // Numerically stable log-sum-exp, excluding diagonal.
        var max_sim: f64 = -1e30;
        for (0..Vn) |cj| {
            if (cj == ci) continue;
            if (sim[ci * Vn + cj] > max_sim) max_sim = sim[ci * Vn + cj];
        }

        var sum_exp: f64 = 0;
        for (0..Vn) |cj| {
            if (cj == ci) continue;
            sum_exp += std.math.exp(sim[ci * Vn + cj] - max_sim);
        }
        const log_sum_exp = max_sim + std.math.log(f64, std.math.e, sum_exp + 1e-30);

        // Mean positive similarity.
        const inv_pos = 1.0 / @as(f64, pos_count[ci]);
        var mean_pos: f64 = 0;
        for (0..Vn) |cj| {
            if (pos_mask[ci * Vn + cj] > 0.5) {
                mean_pos += sim[ci * Vn + cj];
            }
        }
        mean_pos *= inv_pos;

        loss += -mean_pos + log_sum_exp;

        // Gradient w.r.t. scaled similarity.
        for (0..Vn) |cj| {
            if (cj == ci) continue;
            const softmax_j = std.math.exp(sim[ci * Vn + cj] - max_sim) / (sum_exp + 1e-30);
            d_ld_sim[ci * Vn + cj] = softmax_j;
            if (pos_mask[ci * Vn + cj] > 0.5) {
                d_ld_sim[ci * Vn + cj] -= inv_pos;
            }
        }
    }

    // Normalise by number of anchors.
    if (num_anchors > 0) {
        const scale = 1.0 / @as(f64, @floatFromInt(num_anchors));
        loss *= scale;
        for (d_ld_sim) |*g| g.* *= scale;
    }

    // Step 5: symmetrise gradient w.r.t. sim.
    const d_ld_sim_sym = try allocator.alloc(f64, Vn * Vn);
    defer allocator.free(d_ld_sim_sym);
    for (0..Vn) |ci| {
        for (0..Vn) |cj| {
            d_ld_sim_sym[ci * Vn + cj] = d_ld_sim[ci * Vn + cj] + d_ld_sim[cj * Vn + ci];
        }
    }

    // Step 6: backprop through dot product.
    //
    // sim[i,j] = inv_t * dot(x_i, x_j)
    // d_loss/d_x_i += inv_t * sum_j (d_ld_sim_sym[i,j] * x_j)
    //
    // This is a straightforward outer-product scatter:
    //   grad[valid_idx[i], :] += inv_t * sum_j d_ld_sim_sym[i,j] * splade_vecs[valid_idx[j], :]

    for (0..Vn) |ci| {
        const orig_i = valid_idx.items[ci];
        const grad_base = orig_i * V;
        for (0..Vn) |cj| {
            const g = d_ld_sim_sym[ci * Vn + cj] * inv_t;
            if (g == 0) continue;
            const src_base = valid_idx.items[cj] * V;
            for (0..V) |k| {
                grad[grad_base + k] += @floatCast(g * @as(f64, splade_vecs[src_base + k]));
            }
        }
    }

    return SpladeContrastiveResult{ .loss = loss, .grad = grad };
}

// ---------------------------------------------------------------------------
// SpladeForwardInfo + computeSpladeActivationWithInfo + backwardSpladeWeight
// ---------------------------------------------------------------------------

/// Intermediate values from the SPLADE max-pool forward pass needed for backprop
/// through the projection weight W.
pub const SpladeForwardInfo = struct {
    allocator: std.mem.Allocator,
    /// [vocab_size] — the sparse output (same as computeSpladeActivation output)
    splade_vec: []f32,
    /// [vocab_size] — which token index gave the max for each vocab dim (max-pool only)
    argmax_tokens: []u32,
    /// [vocab_size] — relu(h[t*] @ W[k,:]) at the argmax token (before log1p, after relu)
    pre_relu: []f32,

    pub fn deinit(self: *SpladeForwardInfo) void {
        self.allocator.free(self.splade_vec);
        self.allocator.free(self.argmax_tokens);
        self.allocator.free(self.pre_relu);
        self.* = undefined;
    }
};

/// Like computeSpladeActivation but also returns intermediate values for backprop.
/// Only supports max pooling (mean pooling doesn't have clean argmax backprop).
///
/// hidden       : [total_tokens * hidden_size] f32, row-major
/// weight       : [vocab_size * hidden_size] f32, row-major  (W; no bias)
///
/// Returns a SpladeForwardInfo (caller must call deinit).
pub fn computeSpladeActivationWithInfo(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    weight: []const f32,
    total_tokens: usize,
    hidden_size: usize,
    vocab_size: u32,
) !SpladeForwardInfo {
    const V: usize = @intCast(vocab_size);
    const H = hidden_size;

    std.debug.assert(hidden.len == total_tokens * H);
    std.debug.assert(weight.len == V * H);

    const splade_vec = try allocator.alloc(f32, V);
    errdefer allocator.free(splade_vec);
    @memset(splade_vec, 0);

    const argmax_tokens = try allocator.alloc(u32, V);
    errdefer allocator.free(argmax_tokens);
    @memset(argmax_tokens, 0);

    const pre_relu = try allocator.alloc(f32, V);
    errdefer allocator.free(pre_relu);
    @memset(pre_relu, 0);

    for (0..total_tokens) |t| {
        const h_base = t * H;
        for (0..V) |v| {
            const w_base = v * H;
            var dot: f32 = 0;
            for (0..H) |k| {
                dot += hidden[h_base + k] * weight[w_base + k];
            }
            const relu_val = @max(0.0, dot);
            const act = std.math.log1p(relu_val);
            if (act > splade_vec[v]) {
                splade_vec[v] = act;
                argmax_tokens[v] = @intCast(t);
                pre_relu[v] = relu_val;
            }
        }
    }

    return SpladeForwardInfo{
        .allocator = allocator,
        .splade_vec = splade_vec,
        .argmax_tokens = argmax_tokens,
        .pre_relu = pre_relu,
    };
}

/// Compute gradient of loss w.r.t. the SPLADE projection weight W.
///
/// Math: for each vocab dim k:
///   scale[k] = splade_grad[k] * (pre_relu[k] > 0 ? 1/(1+pre_relu[k]) : 0)
///   dW[k,:] += scale[k] * hidden[argmax_tokens[k], :]
///
/// splade_grad : [vocab_size]          — dL/dv from SPLADE loss
/// info        : from computeSpladeActivationWithInfo
/// hidden      : [total_tokens * hidden_size] — same hidden states used in forward
/// hidden_size : H (must match the H used in computeSpladeActivationWithInfo)
/// dW          : [vocab_size * hidden_size]   — gradient accumulator (ADD to, not overwrite)
pub fn backwardSpladeWeight(
    splade_grad: []const f32,
    info: *const SpladeForwardInfo,
    hidden: []const f32,
    hidden_size: usize,
    dW: []f32,
) void {
    const V = info.splade_vec.len;
    const H = hidden_size;

    std.debug.assert(splade_grad.len == V);
    std.debug.assert(dW.len == V * H);

    for (0..V) |v| {
        if (info.pre_relu[v] <= 0.0) continue;
        const scale = splade_grad[v] / (1.0 + info.pre_relu[v]);
        if (scale == 0.0) continue;
        const t = info.argmax_tokens[v];
        const h_base = @as(usize, t) * H;
        const w_base = v * H;
        for (0..H) |j| {
            dW[w_base + j] += scale * hidden[h_base + j];
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

test "computeSpladeActivation — single token, max pooling" {
    const allocator = std.testing.allocator;

    // 1 token, hidden_size=4, vocab_size=3
    // hidden = [1, 0, 0, 0]
    // weight row 0 = [2, 0, 0, 0]  → proj = 2 → log1p(2) ≈ 1.0986
    // weight row 1 = [-1, 0, 0, 0] → proj = -1 → relu → 0 → log1p(0) = 0
    // weight row 2 = [0.5, 0, 0, 0]→ proj = 0.5 → log1p(0.5) ≈ 0.4055
    const hidden = [_]f32{ 1, 0, 0, 0 };
    const weight = [_]f32{
        2,    0, 0, 0, // vocab 0
        -1,   0, 0, 0, // vocab 1
        0.5,  0, 0, 0, // vocab 2
    };

    var out = [_]f32{ 0, 0, 0 };
    try computeSpladeActivation(
        allocator,
        &hidden,
        &weight,
        &out,
        1, // total_tokens
        4, // hidden_size
        3, // vocab_size
        .max,
    );

    try std.testing.expectApproxEqAbs(std.math.log1p(@as(f32, 2.0)), out[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), out[1], 1e-5);
    try std.testing.expectApproxEqAbs(std.math.log1p(@as(f32, 0.5)), out[2], 1e-5);
}

test "computeSpladeActivation — multi-token max pooling selects maximum" {
    const allocator = std.testing.allocator;

    // 2 tokens, hidden_size=2, vocab_size=2
    // token 0: h=[1,0]  weight row 0=[1,0] → proj=1 → act=log1p(1)≈0.693
    //                   weight row 1=[0,1] → proj=0 → act=0
    // token 1: h=[0,2]  weight row 0=[1,0] → proj=0 → act=0
    //                   weight row 1=[0,1] → proj=2 → act=log1p(2)≈1.099
    // max pool: out[0]=log1p(1), out[1]=log1p(2)
    const hidden = [_]f32{ 1, 0, 0, 2 };
    const weight = [_]f32{ 1, 0, 0, 1 };

    var out = [_]f32{ 0, 0 };
    try computeSpladeActivation(allocator, &hidden, &weight, &out, 2, 2, 2, .max);

    try std.testing.expectApproxEqAbs(std.math.log1p(@as(f32, 1.0)), out[0], 1e-5);
    try std.testing.expectApproxEqAbs(std.math.log1p(@as(f32, 2.0)), out[1], 1e-5);
}

test "computeSpladeActivation — mean pooling" {
    const allocator = std.testing.allocator;

    // 2 tokens, hidden_size=2, vocab_size=1
    // weight row 0 = [1,0]
    // token 0: h=[2,0] → proj=2 → act=log1p(2)
    // token 1: h=[4,0] → proj=4 → act=log1p(4)
    // mean: (log1p(2)+log1p(4))/2
    const hidden = [_]f32{ 2, 0, 4, 0 };
    const weight = [_]f32{ 1, 0 };
    var out = [_]f32{0};
    try computeSpladeActivation(allocator, &hidden, &weight, &out, 2, 2, 1, .mean);
    const expected = (std.math.log1p(@as(f32, 2.0)) + std.math.log1p(@as(f32, 4.0))) / 2.0;
    try std.testing.expectApproxEqAbs(expected, out[0], 1e-5);
}

test "computeSpladeFlopsLoss — basic" {
    // 2 chunks, vocab_size=3
    // values: [1, -2, 0,  3, 0, -1]
    // sum |v| = 1+2+0+3+0+1 = 7; mean over 2 chunks = 3.5; loss = 1e-3 * 3.5 = 3.5e-3
    const vecs = [_]f32{ 1, -2, 0, 3, 0, -1 };
    var grad = @as([6]f32, @splat(0));
    const loss = computeSpladeFlopsLoss(&vecs, &grad, 2, 3, 1e-3);

    const expected_loss: f32 = 1e-3 * 3.5;
    try std.testing.expectApproxEqAbs(expected_loss, loss, 1e-6);

    // Gradient signs.
    const scale: f32 = 1e-3 / 2.0;
    try std.testing.expectApproxEqAbs(scale, grad[0], 1e-7);   // +1
    try std.testing.expectApproxEqAbs(-scale, grad[1], 1e-7);  // -2
    try std.testing.expectApproxEqAbs(@as(f32, 0), grad[2], 1e-7); // 0
    try std.testing.expectApproxEqAbs(scale, grad[3], 1e-7);   // +3
    try std.testing.expectApproxEqAbs(@as(f32, 0), grad[4], 1e-7); // 0
    try std.testing.expectApproxEqAbs(-scale, grad[5], 1e-7);  // -1
}

test "computeSpladeContrastiveLoss — trivial V<2 returns zero" {
    const allocator = std.testing.allocator;

    const N = 3;
    const V = 4;
    const vecs = @as([(N * V)]f32, @splat(0));
    const mask = [_]f32{ 0, 0, 1 }; // only 1 valid → zero loss
    const doc_ids = [_]u32{ 0, 0, 0 };

    var res = try computeSpladeContrastiveLoss(allocator, &vecs, &mask, &doc_ids, N, V, 0.07);
    defer res.deinit(allocator);

    try std.testing.expectEqual(@as(f64, 0), res.loss);
    for (res.grad) |g| try std.testing.expectEqual(@as(f32, 0), g);
}

test "computeSpladeContrastiveLoss — two docs two chunks each" {
    const allocator = std.testing.allocator;

    // 4 chunks, vocab_size=3
    // Chunks 0,1 are from doc 0; chunks 2,3 from doc 1.
    // Positives should attract (low similarity between doc groups → non-trivial loss).
    const V = 3;
    const N = 4;
    const vecs = [_]f32{
        1.0, 0.5, 0.0, // chunk 0 (doc 0)
        0.9, 0.6, 0.1, // chunk 1 (doc 0)
        0.0, 0.1, 1.0, // chunk 2 (doc 1)
        0.1, 0.0, 0.8, // chunk 3 (doc 1)
    };
    const mask = [_]f32{ 1, 1, 1, 1 };
    const doc_ids = [_]u32{ 0, 0, 1, 1 };

    var res = try computeSpladeContrastiveLoss(allocator, &vecs, &mask, &doc_ids, N, V, 0.07);
    defer res.deinit(allocator);

    try std.testing.expect(res.loss > 0);
    try std.testing.expectEqual(@as(usize, N * V), res.grad.len);
}

test "computeSpladeContrastiveLoss — no positives yields zero loss" {
    const allocator = std.testing.allocator;

    // Each chunk from a distinct doc → no positive pairs → loss = 0.
    const V = 2;
    const N = 3;
    const vecs = [_]f32{
        1.0, 0.0,
        0.0, 1.0,
        0.5, 0.5,
    };
    const mask = [_]f32{ 1, 1, 1 };
    const doc_ids = [_]u32{ 0, 1, 2 };

    var res = try computeSpladeContrastiveLoss(allocator, &vecs, &mask, &doc_ids, N, V, 0.07);
    defer res.deinit(allocator);

    try std.testing.expectEqual(@as(f64, 0), res.loss);
    for (res.grad) |g| try std.testing.expectEqual(@as(f32, 0), g);
}

test "computeSpladeActivationWithInfo — matches computeSpladeActivation output" {
    const allocator = std.testing.allocator;

    // 2 tokens, hidden_size=2, vocab_size=2
    // token 0: h=[1,0]  weight row 0=[1,0] → proj=1 → act=log1p(1)
    //                   weight row 1=[0,1] → proj=0 → act=0
    // token 1: h=[0,2]  weight row 0=[1,0] → proj=0 → act=0
    //                   weight row 1=[0,1] → proj=2 → act=log1p(2)
    // argmax: vocab 0 → token 0 (pre_relu=1), vocab 1 → token 1 (pre_relu=2)
    const hidden = [_]f32{ 1, 0, 0, 2 };
    const weight = [_]f32{ 1, 0, 0, 1 };

    var info = try computeSpladeActivationWithInfo(allocator, &hidden, &weight, 2, 2, 2);
    defer info.deinit();

    try std.testing.expectApproxEqAbs(std.math.log1p(@as(f32, 1.0)), info.splade_vec[0], 1e-5);
    try std.testing.expectApproxEqAbs(std.math.log1p(@as(f32, 2.0)), info.splade_vec[1], 1e-5);
    try std.testing.expectEqual(@as(u32, 0), info.argmax_tokens[0]);
    try std.testing.expectEqual(@as(u32, 1), info.argmax_tokens[1]);
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), info.pre_relu[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), info.pre_relu[1], 1e-5);
}

test "backwardSpladeWeight — gradient accumulates correctly" {
    const allocator = std.testing.allocator;

    // 1 token, hidden_size=2, vocab_size=2
    // hidden=[3,4], weight row 0=[1,0] → proj=3 → pre_relu=3, act=log1p(3)
    //              weight row 1=[0,1] → proj=4 → pre_relu=4, act=log1p(4)
    // splade_grad = [1.0, 1.0]
    // dW[0,:] += 1.0 / (1+3) * [3,4] = [0.75, 1.0]
    // dW[1,:] += 1.0 / (1+4) * [3,4] = [0.6,  0.8]
    const hidden = [_]f32{ 3, 4 };
    const weight = [_]f32{ 1, 0, 0, 1 };

    var info = try computeSpladeActivationWithInfo(allocator, &hidden, &weight, 1, 2, 2);
    defer info.deinit();

    const splade_grad = [_]f32{ 1.0, 1.0 };
    var dW = [_]f32{ 0, 0, 0, 0 };
    backwardSpladeWeight(&splade_grad, &info, &hidden, 2, &dW);

    try std.testing.expectApproxEqAbs(@as(f32, 3.0 / 4.0), dW[0], 1e-5); // dW[0,0]
    try std.testing.expectApproxEqAbs(@as(f32, 4.0 / 4.0), dW[1], 1e-5); // dW[0,1]
    try std.testing.expectApproxEqAbs(@as(f32, 3.0 / 5.0), dW[2], 1e-5); // dW[1,0]
    try std.testing.expectApproxEqAbs(@as(f32, 4.0 / 5.0), dW[3], 1e-5); // dW[1,1]
}

test "backwardSpladeWeight — zero pre_relu skipped" {
    const allocator = std.testing.allocator;

    // vocab 1 has negative projection → pre_relu=0, should be skipped
    // hidden=[1,0], weight row 0=[1,0] → proj=1, pre_relu=1
    //              weight row 1=[-1,0] → proj=-1, pre_relu=0
    const hidden = [_]f32{ 1, 0 };
    const weight = [_]f32{ 1, 0, -1, 0 };

    var info = try computeSpladeActivationWithInfo(allocator, &hidden, &weight, 1, 2, 2);
    defer info.deinit();

    try std.testing.expectApproxEqAbs(@as(f32, 0.0), info.pre_relu[1], 1e-5);

    const splade_grad = [_]f32{ 1.0, 99.0 }; // vocab 1 grad should have zero effect
    var dW = [_]f32{ 0, 0, 0, 0 };
    backwardSpladeWeight(&splade_grad, &info, &hidden, 2, &dW);

    // vocab 0: scale = 1/(1+1) = 0.5; dW[0,:] = 0.5 * [1,0] = [0.5, 0]
    try std.testing.expectApproxEqAbs(@as(f32, 0.5), dW[0], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), dW[1], 1e-5);
    // vocab 1 skipped: dW[1,:] = [0, 0]
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), dW[2], 1e-5);
    try std.testing.expectApproxEqAbs(@as(f32, 0.0), dW[3], 1e-5);
}
