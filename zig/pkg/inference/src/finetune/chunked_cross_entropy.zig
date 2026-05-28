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

// Chunked cross-entropy loss with fused forward+backward in a single pass.
//
// Computes loss and writes gradients directly to logits chunk-by-chunk, avoiding
// the O(batch * vocab) peak memory of materializing full softmax probabilities.

const std = @import("std");

pub const Reduction = enum { mean, sum, none };

pub const ChunkedCEConfig = struct {
    chunk_size: usize = 1024,
    label_smoothing: f32 = 0.0,
    ignore_index: i32 = -100,
    reduction: Reduction = .mean,
};

pub const ChunkedCEResult = struct {
    loss: f32,
    valid_tokens: usize,
    grad_h: []f32,
    grad_w: []f32,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *ChunkedCEResult) void {
        self.allocator.free(self.grad_h);
        if (self.grad_w.len > 0) self.allocator.free(self.grad_w);
        self.* = undefined;
    }
};

pub fn chunkedCrossEntropy(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    w_vocab: []const f32,
    targets: []const i32,
    total_tokens: usize,
    hidden_size: usize,
    vocab: usize,
    config: ChunkedCEConfig,
    compute_w_grad: bool,
) !ChunkedCEResult {
    std.debug.assert(hidden.len == total_tokens * hidden_size);
    std.debug.assert(w_vocab.len == vocab * hidden_size);
    std.debug.assert(targets.len == total_tokens);

    const d = hidden_size;
    const V = vocab;

    const grad_h = try allocator.alloc(f32, total_tokens * d);
    errdefer allocator.free(grad_h);
    @memset(grad_h, 0.0);

    var grad_w: []f32 = &[_]f32{};
    if (compute_w_grad) {
        grad_w = try allocator.alloc(f32, V * d);
        @memset(grad_w, 0.0);
    }
    errdefer if (compute_w_grad) allocator.free(grad_w);

    // Count valid tokens in a single pass so we can scale per-token grads inside the loop.
    var valid_tokens: usize = 0;
    for (targets) |t| {
        if (t != config.ignore_index) valid_tokens += 1;
    }

    if (valid_tokens == 0) {
        return .{
            .loss = 0.0,
            .valid_tokens = 0,
            .grad_h = grad_h,
            .grad_w = grad_w,
            .allocator = allocator,
        };
    }

    const chunk_cap = if (config.chunk_size == 0) total_tokens else config.chunk_size;
    const scratch = try allocator.alloc(f32, chunk_cap * V);
    defer allocator.free(scratch);

    const eps = config.label_smoothing;
    const one_minus_eps = 1.0 - eps;
    const eps_over_v: f32 = if (V > 0) eps / @as(f32, @floatFromInt(V)) else 0.0;

    const grad_scale: f32 = switch (config.reduction) {
        .mean => 1.0 / @as(f32, @floatFromInt(valid_tokens)),
        .sum, .none => 1.0,
    };

    var total_loss: f32 = 0.0;

    var start: usize = 0;
    while (start < total_tokens) : (start += chunk_cap) {
        const end = @min(start + chunk_cap, total_tokens);
        const chunk_len = end - start;

        // logits[i, j] = sum_k H[(start+i), k] * W[j, k]
        // Zero the rows we're about to fill (we only touch chunk_len * V entries).
        @memset(scratch[0 .. chunk_len * V], 0.0);
        {
            var i: usize = 0;
            while (i < chunk_len) : (i += 1) {
                const h_row = hidden[(start + i) * d .. (start + i + 1) * d];
                const out_row = scratch[i * V .. (i + 1) * V];
                var j: usize = 0;
                while (j < V) : (j += 1) {
                    const w_row = w_vocab[j * d .. (j + 1) * d];
                    var acc: f32 = 0.0;
                    var k: usize = 0;
                    while (k < d) : (k += 1) {
                        acc += h_row[k] * w_row[k];
                    }
                    out_row[j] = acc;
                }
            }
        }

        // Forward + in-place conversion of each row into its (scaled) gradient w.r.t. logits.
        {
            var i: usize = 0;
            while (i < chunk_len) : (i += 1) {
                const row = scratch[i * V .. (i + 1) * V];
                const t = targets[start + i];
                if (t == config.ignore_index) {
                    @memset(row, 0.0);
                    continue;
                }
                std.debug.assert(t >= 0 and @as(usize, @intCast(t)) < V);
                const t_idx: usize = @intCast(t);

                // max for numerical stability.
                var m: f32 = row[0];
                {
                    var j: usize = 1;
                    while (j < V) : (j += 1) {
                        if (row[j] > m) m = row[j];
                    }
                }

                // exp(logits - m) in place, sum Z.
                var Z: f32 = 0.0;
                {
                    var j: usize = 0;
                    while (j < V) : (j += 1) {
                        const e = std.math.exp(row[j] - m);
                        row[j] = e;
                        Z += e;
                    }
                }
                const log_Z = m + std.math.log(f32, std.math.e, Z);

                // Loss: -(1-eps)*lp_t - eps * mean_j lp_j
                // lp_j = logits_orig_j - log_Z = (m + log(e_j)) - log_Z, but logits_orig_j = m + log(e_j)
                // We no longer have logits_orig; recompute from row (row = exp(logits-m)).
                // lp_j = log(row[j]) + m - log_Z = log(row[j]) - log(Z)
                // So lp_t = log(row[t]/Z), and sum lp_j = sum(log(row[j])) - V*log(Z).
                const p_t = @max(row[t_idx] / Z, 1e-30);
                const lp_t = std.math.log(f32, std.math.e, p_t);

                var token_loss: f32 = -one_minus_eps * lp_t;
                if (eps > 0.0) {
                    var sum_lp: f32 = 0.0;
                    var j: usize = 0;
                    while (j < V) : (j += 1) {
                        // Guard: row[j] = exp(logit_j - m) can underflow to 0 for
                        // logits far below the max, producing -inf from log(0).
                        // Clamp to a tiny positive value to keep the sum finite.
                        sum_lp += std.math.log(f32, std.math.e, @max(row[j], 1e-30));
                    }
                    // sum_lp currently = sum(log(row[j])) = sum(logits_j - m) = sum_logits - V*m
                    // lp_j = logits_j - log_Z = (log(row[j]) + m) - log_Z
                    // mean lp = (sum_lp + V*m)/V - log_Z = sum_lp/V + m - log_Z
                    const V_f: f32 = @floatFromInt(V);
                    const mean_lp = sum_lp / V_f + m - log_Z;
                    token_loss += -eps * mean_lp;
                }
                total_loss += token_loss;

                // Gradient: softmax - (1-eps)*onehot - eps/V, then scale by 1/valid_tokens if mean.
                var j: usize = 0;
                while (j < V) : (j += 1) {
                    const sm = row[j] / Z;
                    var g = sm - eps_over_v;
                    if (j == t_idx) g -= one_minus_eps;
                    row[j] = g * grad_scale;
                }
            }
        }

        // grad_h[start+i, :] = scratch[i, :] @ W      (W is [V, d] row-major)
        {
            var i: usize = 0;
            while (i < chunk_len) : (i += 1) {
                const gh_row = grad_h[(start + i) * d .. (start + i + 1) * d];
                const gl_row = scratch[i * V .. (i + 1) * V];
                var j: usize = 0;
                while (j < V) : (j += 1) {
                    const gj = gl_row[j];
                    if (gj == 0.0) continue;
                    const w_row = w_vocab[j * d .. (j + 1) * d];
                    var k: usize = 0;
                    while (k < d) : (k += 1) {
                        gh_row[k] += gj * w_row[k];
                    }
                }
            }
        }

        // grad_w[j, :] += sum_i scratch[i, j] * H[start+i, :]
        if (compute_w_grad) {
            var j: usize = 0;
            while (j < V) : (j += 1) {
                const gw_row = grad_w[j * d .. (j + 1) * d];
                var i: usize = 0;
                while (i < chunk_len) : (i += 1) {
                    const gij = scratch[i * V + j];
                    if (gij == 0.0) continue;
                    const h_row = hidden[(start + i) * d .. (start + i + 1) * d];
                    var k: usize = 0;
                    while (k < d) : (k += 1) {
                        gw_row[k] += gij * h_row[k];
                    }
                }
            }
        }
    }

    const final_loss: f32 = switch (config.reduction) {
        .mean => total_loss / @as(f32, @floatFromInt(valid_tokens)),
        .sum, .none => total_loss,
    };

    return .{
        .loss = final_loss,
        .valid_tokens = valid_tokens,
        .grad_h = grad_h,
        .grad_w = grad_w,
        .allocator = allocator,
    };
}

// ---------- Dense reference for tests ----------

const RefResult = struct {
    loss: f32,
    valid_tokens: usize,
    grad_h: []f32,
    grad_w: []f32,
    allocator: std.mem.Allocator,

    fn deinit(self: *RefResult) void {
        self.allocator.free(self.grad_h);
        self.allocator.free(self.grad_w);
        self.* = undefined;
    }
};

fn denseCrossEntropy(
    allocator: std.mem.Allocator,
    hidden: []const f32,
    w_vocab: []const f32,
    targets: []const i32,
    total_tokens: usize,
    hidden_size: usize,
    vocab: usize,
    config: ChunkedCEConfig,
) !RefResult {
    const d = hidden_size;
    const V = vocab;

    const grad_h = try allocator.alloc(f32, total_tokens * d);
    @memset(grad_h, 0.0);
    const grad_w = try allocator.alloc(f32, V * d);
    @memset(grad_w, 0.0);

    // Full logits [N, V]
    const logits = try allocator.alloc(f32, total_tokens * V);
    defer allocator.free(logits);
    @memset(logits, 0.0);
    {
        var i: usize = 0;
        while (i < total_tokens) : (i += 1) {
            var j: usize = 0;
            while (j < V) : (j += 1) {
                var acc: f32 = 0.0;
                var k: usize = 0;
                while (k < d) : (k += 1) {
                    acc += hidden[i * d + k] * w_vocab[j * d + k];
                }
                logits[i * V + j] = acc;
            }
        }
    }

    var valid: usize = 0;
    for (targets) |t| if (t != config.ignore_index) {
        valid += 1;
    };

    const eps = config.label_smoothing;
    const one_minus_eps = 1.0 - eps;
    const eps_over_v: f32 = if (V > 0) eps / @as(f32, @floatFromInt(V)) else 0.0;
    const grad_scale: f32 = switch (config.reduction) {
        .mean => if (valid == 0) 0.0 else 1.0 / @as(f32, @floatFromInt(valid)),
        .sum, .none => 1.0,
    };

    var total_loss: f32 = 0.0;
    const grad_logits = try allocator.alloc(f32, total_tokens * V);
    defer allocator.free(grad_logits);
    @memset(grad_logits, 0.0);

    {
        var i: usize = 0;
        while (i < total_tokens) : (i += 1) {
            const t = targets[i];
            if (t == config.ignore_index) continue;
            const t_idx: usize = @intCast(t);
            const row = logits[i * V .. (i + 1) * V];

            var m: f32 = row[0];
            {
                var j: usize = 1;
                while (j < V) : (j += 1) if (row[j] > m) {
                    m = row[j];
                };
            }
            var Z: f32 = 0.0;
            {
                var j: usize = 0;
                while (j < V) : (j += 1) Z += std.math.exp(row[j] - m);
            }
            const log_Z = m + std.math.log(f32, std.math.e, Z);
            const lp_t = row[t_idx] - log_Z;

            var token_loss: f32 = -one_minus_eps * lp_t;
            if (eps > 0.0) {
                var sum_lp: f32 = 0.0;
                var j: usize = 0;
                while (j < V) : (j += 1) sum_lp += (row[j] - log_Z);
                token_loss += -eps * sum_lp / @as(f32, @floatFromInt(V));
            }
            total_loss += token_loss;

            var j: usize = 0;
            while (j < V) : (j += 1) {
                const sm = std.math.exp(row[j] - log_Z);
                var g = sm - eps_over_v;
                if (j == t_idx) g -= one_minus_eps;
                grad_logits[i * V + j] = g * grad_scale;
            }
        }
    }

    // grad_h = grad_logits @ W ; grad_w = grad_logits^T @ H
    {
        var i: usize = 0;
        while (i < total_tokens) : (i += 1) {
            var k: usize = 0;
            while (k < d) : (k += 1) {
                var acc: f32 = 0.0;
                var j: usize = 0;
                while (j < V) : (j += 1) acc += grad_logits[i * V + j] * w_vocab[j * d + k];
                grad_h[i * d + k] = acc;
            }
        }
    }
    {
        var j: usize = 0;
        while (j < V) : (j += 1) {
            var k: usize = 0;
            while (k < d) : (k += 1) {
                var acc: f32 = 0.0;
                var i: usize = 0;
                while (i < total_tokens) : (i += 1) acc += grad_logits[i * V + j] * hidden[i * d + k];
                grad_w[j * d + k] = acc;
            }
        }
    }

    const final_loss: f32 = switch (config.reduction) {
        .mean => if (valid == 0) 0.0 else total_loss / @as(f32, @floatFromInt(valid)),
        .sum, .none => total_loss,
    };

    return .{
        .loss = final_loss,
        .valid_tokens = valid,
        .grad_h = grad_h,
        .grad_w = grad_w,
        .allocator = allocator,
    };
}

fn fillDeterministic(buf: []f32, seed: u64) void {
    var rng = std.Random.DefaultPrng.init(seed);
    var r = rng.random();
    for (buf) |*x| x.* = (r.float(f32) - 0.5) * 2.0;
}

fn maxAbsDiff(a: []const f32, b: []const f32) f32 {
    std.debug.assert(a.len == b.len);
    var m: f32 = 0.0;
    for (a, b) |x, y| {
        const d = @abs(x - y);
        if (d > m) m = d;
    }
    return m;
}

// ---------- Tests ----------

test "chunked CE matches dense reference (no smoothing, mean)" {
    const allocator = std.testing.allocator;
    const N: usize = 8;
    const D: usize = 4;
    const V: usize = 16;

    const h = try allocator.alloc(f32, N * D);
    defer allocator.free(h);
    const w = try allocator.alloc(f32, V * D);
    defer allocator.free(w);
    const targets = try allocator.alloc(i32, N);
    defer allocator.free(targets);

    fillDeterministic(h, 1);
    fillDeterministic(w, 2);
    for (targets, 0..) |*t, i| t.* = @intCast(i % V);

    const cfg = ChunkedCEConfig{ .chunk_size = 3, .reduction = .mean };
    var got = try chunkedCrossEntropy(allocator, h, w, targets, N, D, V, cfg, true);
    defer got.deinit();
    var ref = try denseCrossEntropy(allocator, h, w, targets, N, D, V, cfg);
    defer ref.deinit();

    try std.testing.expectApproxEqAbs(ref.loss, got.loss, 1e-5);
    try std.testing.expect(maxAbsDiff(ref.grad_h, got.grad_h) < 1e-5);
    try std.testing.expect(maxAbsDiff(ref.grad_w, got.grad_w) < 1e-5);
    try std.testing.expectEqual(ref.valid_tokens, got.valid_tokens);
}

test "ignore_index masks tokens (all ignored -> zero loss and grads)" {
    const allocator = std.testing.allocator;
    const N: usize = 5;
    const D: usize = 3;
    const V: usize = 7;

    const h = try allocator.alloc(f32, N * D);
    defer allocator.free(h);
    const w = try allocator.alloc(f32, V * D);
    defer allocator.free(w);
    const targets = try allocator.alloc(i32, N);
    defer allocator.free(targets);

    fillDeterministic(h, 11);
    fillDeterministic(w, 22);
    for (targets) |*t| t.* = -100;

    const cfg = ChunkedCEConfig{ .chunk_size = 2, .ignore_index = -100 };
    var got = try chunkedCrossEntropy(allocator, h, w, targets, N, D, V, cfg, true);
    defer got.deinit();

    try std.testing.expectEqual(@as(f32, 0.0), got.loss);
    try std.testing.expectEqual(@as(usize, 0), got.valid_tokens);
    for (got.grad_h) |g| try std.testing.expectEqual(@as(f32, 0.0), g);
    for (got.grad_w) |g| try std.testing.expectEqual(@as(f32, 0.0), g);
}

test "ignore_index partial mask matches dense reference" {
    const allocator = std.testing.allocator;
    const N: usize = 6;
    const D: usize = 4;
    const V: usize = 10;

    const h = try allocator.alloc(f32, N * D);
    defer allocator.free(h);
    const w = try allocator.alloc(f32, V * D);
    defer allocator.free(w);
    const targets = try allocator.alloc(i32, N);
    defer allocator.free(targets);

    fillDeterministic(h, 33);
    fillDeterministic(w, 44);
    targets[0] = -100;
    targets[1] = 2;
    targets[2] = -100;
    targets[3] = 7;
    targets[4] = 0;
    targets[5] = 9;

    const cfg = ChunkedCEConfig{ .chunk_size = 4 };
    var got = try chunkedCrossEntropy(allocator, h, w, targets, N, D, V, cfg, true);
    defer got.deinit();
    var ref = try denseCrossEntropy(allocator, h, w, targets, N, D, V, cfg);
    defer ref.deinit();

    try std.testing.expectApproxEqAbs(ref.loss, got.loss, 1e-5);
    try std.testing.expect(maxAbsDiff(ref.grad_h, got.grad_h) < 1e-5);
    try std.testing.expect(maxAbsDiff(ref.grad_w, got.grad_w) < 1e-5);
}

test "label smoothing eps=0.1 hand-computed single token" {
    const allocator = std.testing.allocator;
    // Single token, V=4, pick logits s.t. loss is tractable.
    // Use D=1, h=[1], W=[[l0],[l1],[l2],[l3]] => logits = [l0,l1,l2,l3].
    const N: usize = 1;
    const D: usize = 1;
    const V: usize = 4;

    const h = [_]f32{1.0};
    const w = [_]f32{ 1.0, 2.0, 3.0, 0.0 }; // logits = [1,2,3,0]
    const targets = [_]i32{2};

    // log_Z = log(e + e^2 + e^3 + 1)
    const Z: f64 = std.math.exp(@as(f64, 1.0)) + std.math.exp(@as(f64, 2.0)) + std.math.exp(@as(f64, 3.0)) + 1.0;
    const log_Z: f64 = std.math.log(f64, std.math.e, Z);
    const lp = [_]f64{ 1.0 - log_Z, 2.0 - log_Z, 3.0 - log_Z, 0.0 - log_Z };
    const eps: f64 = 0.1;
    const mean_lp: f64 = (lp[0] + lp[1] + lp[2] + lp[3]) / 4.0;
    const expected_loss: f64 = -(1.0 - eps) * lp[2] - eps * mean_lp;

    const cfg = ChunkedCEConfig{ .chunk_size = 8, .label_smoothing = 0.1, .reduction = .mean };
    var got = try chunkedCrossEntropy(allocator, &h, &w, &targets, N, D, V, cfg, true);
    defer got.deinit();

    try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected_loss)), got.loss, 1e-5);

    // Also compare against dense ref for the gradient.
    var ref = try denseCrossEntropy(allocator, &h, &w, &targets, N, D, V, cfg);
    defer ref.deinit();
    try std.testing.expect(maxAbsDiff(ref.grad_h, got.grad_h) < 1e-5);
    try std.testing.expect(maxAbsDiff(ref.grad_w, got.grad_w) < 1e-5);
}

test "compute_w_grad=false leaves grad_w empty" {
    const allocator = std.testing.allocator;
    const N: usize = 4;
    const D: usize = 3;
    const V: usize = 5;

    const h = try allocator.alloc(f32, N * D);
    defer allocator.free(h);
    const w = try allocator.alloc(f32, V * D);
    defer allocator.free(w);
    const targets = try allocator.alloc(i32, N);
    defer allocator.free(targets);
    fillDeterministic(h, 5);
    fillDeterministic(w, 6);
    for (targets, 0..) |*t, i| t.* = @intCast(i % V);

    const cfg = ChunkedCEConfig{ .chunk_size = 2 };
    var got = try chunkedCrossEntropy(allocator, h, w, targets, N, D, V, cfg, false);
    defer got.deinit();

    try std.testing.expectEqual(@as(usize, 0), got.grad_w.len);
    // grad_h should still match ref.
    var ref = try denseCrossEntropy(allocator, h, w, targets, N, D, V, cfg);
    defer ref.deinit();
    try std.testing.expect(maxAbsDiff(ref.grad_h, got.grad_h) < 1e-5);
}

test "chunk_size larger than total_tokens (single chunk path)" {
    const allocator = std.testing.allocator;
    const N: usize = 3;
    const D: usize = 4;
    const V: usize = 6;

    const h = try allocator.alloc(f32, N * D);
    defer allocator.free(h);
    const w = try allocator.alloc(f32, V * D);
    defer allocator.free(w);
    const targets = try allocator.alloc(i32, N);
    defer allocator.free(targets);
    fillDeterministic(h, 100);
    fillDeterministic(w, 200);
    targets[0] = 3;
    targets[1] = 0;
    targets[2] = 5;

    const cfg = ChunkedCEConfig{ .chunk_size = 1024 };
    var got = try chunkedCrossEntropy(allocator, h, w, targets, N, D, V, cfg, true);
    defer got.deinit();
    var ref = try denseCrossEntropy(allocator, h, w, targets, N, D, V, cfg);
    defer ref.deinit();

    try std.testing.expectApproxEqAbs(ref.loss, got.loss, 1e-5);
    try std.testing.expect(maxAbsDiff(ref.grad_h, got.grad_h) < 1e-5);
    try std.testing.expect(maxAbsDiff(ref.grad_w, got.grad_w) < 1e-5);
}

test "reduction sum == mean * valid_tokens" {
    const allocator = std.testing.allocator;
    const N: usize = 7;
    const D: usize = 3;
    const V: usize = 9;

    const h = try allocator.alloc(f32, N * D);
    defer allocator.free(h);
    const w = try allocator.alloc(f32, V * D);
    defer allocator.free(w);
    const targets = try allocator.alloc(i32, N);
    defer allocator.free(targets);
    fillDeterministic(h, 7);
    fillDeterministic(w, 8);
    targets[0] = 1;
    targets[1] = -100;
    targets[2] = 4;
    targets[3] = 0;
    targets[4] = 8;
    targets[5] = -100;
    targets[6] = 3;

    const cfg_mean = ChunkedCEConfig{ .chunk_size = 3, .reduction = .mean };
    const cfg_sum = ChunkedCEConfig{ .chunk_size = 3, .reduction = .sum };

    var g_mean = try chunkedCrossEntropy(allocator, h, w, targets, N, D, V, cfg_mean, true);
    defer g_mean.deinit();
    var g_sum = try chunkedCrossEntropy(allocator, h, w, targets, N, D, V, cfg_sum, true);
    defer g_sum.deinit();

    const vt: f32 = @floatFromInt(g_mean.valid_tokens);
    try std.testing.expectApproxEqAbs(g_mean.loss * vt, g_sum.loss, 1e-4);

    // Grads: sum grads == mean grads * valid_tokens
    try std.testing.expectEqual(g_mean.grad_h.len, g_sum.grad_h.len);
    for (g_mean.grad_h, g_sum.grad_h) |m, s| {
        try std.testing.expectApproxEqAbs(m * vt, s, 1e-5);
    }
    for (g_mean.grad_w, g_sum.grad_w) |m, s| {
        try std.testing.expectApproxEqAbs(m * vt, s, 1e-5);
    }
}
