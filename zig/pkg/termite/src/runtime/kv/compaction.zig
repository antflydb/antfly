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

// Attention Matching KV cache compaction.
//
// Compresses a sequence's KV cache by selecting a subset of keys based on
// attention mass and fitting new values via OLS (ordinary least squares) to
// preserve attention output. Based on "Fast KV Compaction via Attention
// Matching" (Zweiger et al., arXiv 2602.16284).
//
// The algorithm per KV-head:
// 1. Score each key by total attention mass using reference queries
// 2. Select the top-M keys (M = ceil(N * target_ratio))
// 3. Fit new values via OLS so softmax(Q * K_hat^T) * V_hat ≈ softmax(Q * K^T) * V

const std = @import("std");
const native = @import("../../backends/native.zig");
const activations = @import("../../backends/activations.zig");
const linalg = @import("linalg.zig");
const manager_mod = @import("manager.zig");
const block_mod = @import("block.zig");
const storage_runtime_mod = @import("storage_runtime.zig");

const VEC_LEN = 8;
const F32xN = @Vector(VEC_LEN, f32);

pub const CompactionConfig = struct {
    /// Fraction of tokens to retain. 0.02 = 50x, 0.1 = 10x, 0.5 = 2x.
    target_ratio: f32 = 0.1,
    /// Process input in contiguous chunks of this size.
    chunk_size: usize = 512,
    /// Number of reference queries sampled from cached K values.
    num_ref_queries: usize = 64,
};

pub const CompactionResult = struct {
    allocator: std.mem.Allocator,
    k_hat: []f32, // [M * head_dim]
    v_hat: []f32, // [M * head_dim]
    retained_count: usize, // M

    pub fn deinit(self: *CompactionResult) void {
        self.allocator.free(self.k_hat);
        self.allocator.free(self.v_hat);
    }
};

pub const CompactedSequence = struct {
    allocator: std.mem.Allocator,
    k_per_layer: [][]f32, // [num_layers][M * num_kv_heads * head_dim]
    v_per_layer: [][]f32, // [num_layers][M * num_kv_heads * head_dim]
    retained_count: usize, // M

    pub fn deinit(self: *CompactedSequence) void {
        for (self.k_per_layer) |layer| self.allocator.free(layer);
        for (self.v_per_layer) |layer| self.allocator.free(layer);
        self.allocator.free(self.k_per_layer);
        self.allocator.free(self.v_per_layer);
    }
};

pub const GatheredLayerKv = struct {
    k: []const f32,
    v: []const f32,
};

/// Compact one KV-head's cache using Attention Matching.
///
/// K: [n_tokens x head_dim], V: [n_tokens x head_dim], Q_ref: [n_ref x head_dim]
/// All row-major f32 slices.
pub fn compactLayerHead(
    allocator: std.mem.Allocator,
    K: []const f32,
    V: []const f32,
    Q_ref: []const f32,
    n_tokens: usize,
    n_ref: usize,
    head_dim: usize,
    config: CompactionConfig,
) !CompactionResult {
    const M = @max(1, @as(usize, @intFromFloat(@ceil(@as(f32, @floatFromInt(n_tokens)) * config.target_ratio))));

    // Degenerate: keep everything.
    if (M >= n_tokens) {
        const k_hat = try allocator.alloc(f32, n_tokens * head_dim);
        errdefer allocator.free(k_hat);
        const v_hat = try allocator.alloc(f32, n_tokens * head_dim);
        @memcpy(k_hat, K[0 .. n_tokens * head_dim]);
        @memcpy(v_hat, V[0 .. n_tokens * head_dim]);
        return .{ .allocator = allocator, .k_hat = k_hat, .v_hat = v_hat, .retained_count = n_tokens };
    }

    // 1. Compute attention scores: S = Q_ref * K^T  [n_ref x n_tokens]
    const scores = try allocator.alloc(f32, n_ref * n_tokens);
    defer allocator.free(scores);
    native.sgemmTransBSync(n_ref, n_tokens, head_dim, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))), Q_ref, K, 0.0, scores);

    // Apply softmax per row.
    activations.softmax(scores, n_tokens);

    // 2. Column-sum for attention mass per key (SIMD accumulation).
    const mass = try allocator.alloc(f32, n_tokens);
    defer allocator.free(mass);
    columnSum(scores, n_ref, n_tokens, mass);

    // 3. Select top-M key indices.
    const indices = try allocator.alloc(usize, n_tokens);
    defer allocator.free(indices);
    for (0..n_tokens) |i| indices[i] = i;
    partialSort(indices, mass, M);

    // Sort the selected indices so output order is deterministic.
    std.mem.sort(usize, indices[0..M], {}, std.sort.asc(usize));

    // Copy selected keys.
    const k_hat = try allocator.alloc(f32, M * head_dim);
    errdefer allocator.free(k_hat);
    for (0..M) |m| {
        const src_start = indices[m] * head_dim;
        @memcpy(k_hat[m * head_dim ..][0..head_dim], K[src_start..][0..head_dim]);
    }

    // 4. Compute target output: Y = S * V  [n_ref x head_dim]
    //    (S already has softmax applied from step 1)
    const Y = try allocator.alloc(f32, n_ref * head_dim);
    defer allocator.free(Y);
    native.sgemmSync(n_ref, head_dim, n_tokens, 1.0, scores, V, 0.0, Y);

    // 5. Compute compressed attention weights: A_hat = softmax(Q_ref * K_hat^T)  [n_ref x M]
    const A_hat = try allocator.alloc(f32, n_ref * M);
    defer allocator.free(A_hat);
    native.sgemmTransBSync(n_ref, M, head_dim, 1.0 / @sqrt(@as(f32, @floatFromInt(head_dim))), Q_ref, k_hat, 0.0, A_hat);
    activations.softmax(A_hat, M);

    // 6. OLS solve: V_hat = (A_hat^T * A_hat)^{-1} * A_hat^T * Y
    //    Normal equations: (A^T A) V_hat = A^T Y
    const AtA = try allocator.alloc(f32, M * M);
    defer allocator.free(AtA);
    linalg.matmulAtA(A_hat, n_ref, M, AtA);

    // Add small regularization for numerical stability.
    for (0..M) |i| AtA[i * M + i] += 1e-6;

    // A^T * Y -> [M x head_dim], stored in v_hat (the result).
    const v_hat = try allocator.alloc(f32, M * head_dim);
    errdefer allocator.free(v_hat);
    linalg.matmulAtB(A_hat, Y, n_ref, M, head_dim, v_hat);

    // Solve in-place: AtA * X = v_hat -> X overwrites v_hat.
    try linalg.choleskySolve(AtA, v_hat, M, head_dim);

    return .{ .allocator = allocator, .k_hat = k_hat, .v_hat = v_hat, .retained_count = M };
}

/// Compact a full sequence's KV cache across all layers and heads.
pub fn compactSequence(
    allocator: std.mem.Allocator,
    kv_manager: *manager_mod.KvManager,
    sequence_id: manager_mod.SequenceId,
    pool_id: block_mod.KvPoolId,
    config: CompactionConfig,
) !CompactedSequence {
    const pool = kv_manager.getPool(pool_id) orelse return error.InvalidPoolId;
    const num_layers: usize = pool.config.num_layers_packed;
    const num_kv_heads: usize = pool.config.num_kv_heads;
    const head_dim: usize = pool.config.head_dim;
    const token_count = kv_manager.tokenCount(sequence_id) orelse return error.InvalidSequenceId;

    if (token_count == 0) return error.EmptySequence;

    // Gather reference queries from layer 0's K values.
    const ref_count = @min(config.num_ref_queries, token_count);
    const layer0_kv = try kv_manager.gatherLayerKv(allocator, sequence_id, 0, token_count);
    defer allocator.free(layer0_kv.k);
    defer allocator.free(layer0_kv.v);

    // Extract reference queries: first ref_count tokens, all heads concatenated.
    // We use K values as proxy queries (Approach A from design doc).
    const ref_queries = layer0_kv.k[0 .. ref_count * num_kv_heads * head_dim];

    // Compute retained count from first head to ensure consistency.
    const M_per_head = @max(1, @as(usize, @intFromFloat(@ceil(@as(f32, @floatFromInt(token_count)) * config.target_ratio))));
    const M = @min(M_per_head, token_count);

    const k_per_layer = try allocator.alloc([]f32, num_layers);
    const v_per_layer = try allocator.alloc([]f32, num_layers);
    var layers_completed: usize = 0;
    errdefer {
        for (k_per_layer[0..layers_completed]) |s| allocator.free(s);
        for (v_per_layer[0..layers_completed]) |s| allocator.free(s);
        allocator.free(k_per_layer);
        allocator.free(v_per_layer);
    }

    for (0..num_layers) |layer| {
        // Gather this layer's full KV. Reuse layer 0 data we already have for ref queries.
        const layer_kv = if (layer == 0)
            layer0_kv
        else
            try kv_manager.gatherLayerKv(allocator, sequence_id, layer, token_count);
        defer if (layer != 0) {
            allocator.free(layer_kv.k);
            allocator.free(layer_kv.v);
        };

        const kv_width = num_kv_heads * head_dim;

        // Allocate interleaved output for this layer.
        const layer_k = try allocator.alloc(f32, M * kv_width);
        errdefer allocator.free(layer_k);
        const layer_v = try allocator.alloc(f32, M * kv_width);
        errdefer allocator.free(layer_v);

        // Compact each KV-head independently.
        for (0..num_kv_heads) |h| {
            // Extract per-head K, V, Q_ref as strided slices -> contiguous copies.
            const head_k = try extractHead(allocator, layer_kv.k, token_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_k);
            const head_v = try extractHead(allocator, layer_kv.v, token_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_v);
            const head_q = try extractHead(allocator, ref_queries, ref_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_q);

            var result = try compactLayerHead(allocator, head_k, head_v, head_q, token_count, ref_count, head_dim, config);
            defer result.deinit();

            // Interleave back into the flat per-layer layout: [token, head, dim].
            for (0..result.retained_count) |m| {
                const dst_base = m * kv_width + h * head_dim;
                @memcpy(layer_k[dst_base..][0..head_dim], result.k_hat[m * head_dim ..][0..head_dim]);
                @memcpy(layer_v[dst_base..][0..head_dim], result.v_hat[m * head_dim ..][0..head_dim]);
            }
        }

        k_per_layer[layer] = layer_k;
        v_per_layer[layer] = layer_v;
        layers_completed = layer + 1;
    }

    return .{
        .allocator = allocator,
        .k_per_layer = k_per_layer,
        .v_per_layer = v_per_layer,
        .retained_count = M,
    };
}

pub fn compactStorageSequence(
    allocator: std.mem.Allocator,
    kv_storage: *storage_runtime_mod.KvStorageRuntime,
    sequence_id: manager_mod.SequenceId,
    config: CompactionConfig,
) !CompactedSequence {
    const pool = kv_storage.getPool(kv_storage.poolId()) orelse return error.InvalidPoolId;
    const num_layers: usize = pool.config.num_layers_packed;
    const num_kv_heads: usize = pool.config.num_kv_heads;
    const head_dim: usize = pool.config.head_dim;
    const token_count = kv_storage.tokenCount(sequence_id) orelse return error.InvalidSequenceId;

    if (token_count == 0) return error.EmptySequence;

    const ref_count = @min(config.num_ref_queries, token_count);
    const layer0_kv = try kv_storage.gatherLayerKv(allocator, sequence_id, 0, token_count);
    defer allocator.free(layer0_kv.k);
    defer allocator.free(layer0_kv.v);

    const ref_queries = layer0_kv.k[0 .. ref_count * num_kv_heads * head_dim];
    const M_per_head = @max(1, @as(usize, @intFromFloat(@ceil(@as(f32, @floatFromInt(token_count)) * config.target_ratio))));
    const M = @min(M_per_head, token_count);

    const k_per_layer = try allocator.alloc([]f32, num_layers);
    const v_per_layer = try allocator.alloc([]f32, num_layers);
    var layers_completed: usize = 0;
    errdefer {
        for (k_per_layer[0..layers_completed]) |s| allocator.free(s);
        for (v_per_layer[0..layers_completed]) |s| allocator.free(s);
        allocator.free(k_per_layer);
        allocator.free(v_per_layer);
    }

    for (0..num_layers) |layer| {
        const layer_kv = if (layer == 0)
            layer0_kv
        else
            try kv_storage.gatherLayerKv(allocator, sequence_id, layer, token_count);
        defer if (layer != 0) {
            allocator.free(layer_kv.k);
            allocator.free(layer_kv.v);
        };

        const kv_width = num_kv_heads * head_dim;
        const layer_k = try allocator.alloc(f32, M * kv_width);
        errdefer allocator.free(layer_k);
        const layer_v = try allocator.alloc(f32, M * kv_width);
        errdefer allocator.free(layer_v);

        for (0..num_kv_heads) |h| {
            const head_k = try extractHead(allocator, layer_kv.k, token_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_k);
            const head_v = try extractHead(allocator, layer_kv.v, token_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_v);
            const head_q = try extractHead(allocator, ref_queries, ref_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_q);

            var result = try compactLayerHead(allocator, head_k, head_v, head_q, token_count, ref_count, head_dim, config);
            defer result.deinit();

            for (0..result.retained_count) |m| {
                const dst_base = m * kv_width + h * head_dim;
                @memcpy(layer_k[dst_base..][0..head_dim], result.k_hat[m * head_dim ..][0..head_dim]);
                @memcpy(layer_v[dst_base..][0..head_dim], result.v_hat[m * head_dim ..][0..head_dim]);
            }
        }

        k_per_layer[layer] = layer_k;
        v_per_layer[layer] = layer_v;
        layers_completed = layer + 1;
    }

    return .{
        .allocator = allocator,
        .k_per_layer = k_per_layer,
        .v_per_layer = v_per_layer,
        .retained_count = M,
    };
}

pub fn compactGatheredSequence(
    allocator: std.mem.Allocator,
    gathered_layers: []const GatheredLayerKv,
    token_count: usize,
    num_kv_heads: usize,
    head_dim: usize,
    config: CompactionConfig,
) !CompactedSequence {
    if (gathered_layers.len == 0) return error.EmptySequence;
    if (token_count == 0) return error.EmptySequence;

    const ref_count = @min(config.num_ref_queries, token_count);
    const layer0_k = gathered_layers[0].k;
    const layer0_v = gathered_layers[0].v;
    _ = layer0_v;
    const ref_queries = layer0_k[0 .. ref_count * num_kv_heads * head_dim];
    const M_per_head = @max(1, @as(usize, @intFromFloat(@ceil(@as(f32, @floatFromInt(token_count)) * config.target_ratio))));
    const M = @min(M_per_head, token_count);

    const k_per_layer = try allocator.alloc([]f32, gathered_layers.len);
    const v_per_layer = try allocator.alloc([]f32, gathered_layers.len);
    var layers_completed: usize = 0;
    errdefer {
        for (k_per_layer[0..layers_completed]) |s| allocator.free(s);
        for (v_per_layer[0..layers_completed]) |s| allocator.free(s);
        allocator.free(k_per_layer);
        allocator.free(v_per_layer);
    }

    for (gathered_layers, 0..) |layer_kv, layer| {
        const kv_width = num_kv_heads * head_dim;
        const layer_k = try allocator.alloc(f32, M * kv_width);
        errdefer allocator.free(layer_k);
        const layer_v = try allocator.alloc(f32, M * kv_width);
        errdefer allocator.free(layer_v);

        for (0..num_kv_heads) |h| {
            const head_k = try extractHead(allocator, layer_kv.k, token_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_k);
            const head_v = try extractHead(allocator, layer_kv.v, token_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_v);
            const head_q = try extractHead(allocator, ref_queries, ref_count, num_kv_heads, head_dim, h);
            defer allocator.free(head_q);

            var result = try compactLayerHead(allocator, head_k, head_v, head_q, token_count, ref_count, head_dim, config);
            defer result.deinit();

            for (0..result.retained_count) |m| {
                const dst_base = m * kv_width + h * head_dim;
                @memcpy(layer_k[dst_base..][0..head_dim], result.k_hat[m * head_dim ..][0..head_dim]);
                @memcpy(layer_v[dst_base..][0..head_dim], result.v_hat[m * head_dim ..][0..head_dim]);
            }
        }

        k_per_layer[layer] = layer_k;
        v_per_layer[layer] = layer_v;
        layers_completed = layer + 1;
    }

    return .{
        .allocator = allocator,
        .k_per_layer = k_per_layer,
        .v_per_layer = v_per_layer,
        .retained_count = M,
    };
}

// --- Internal helpers ---

/// Extract one head's data from an interleaved [n_tokens x num_kv_heads x head_dim] layout
/// into a contiguous [n_tokens x head_dim] slice.
fn extractHead(allocator: std.mem.Allocator, data: []const f32, n_tokens: usize, num_kv_heads: usize, head_dim: usize, head_idx: usize) ![]f32 {
    const out = try allocator.alloc(f32, n_tokens * head_dim);
    const stride = num_kv_heads * head_dim;
    for (0..n_tokens) |t| {
        const src_start = t * stride + head_idx * head_dim;
        @memcpy(out[t * head_dim ..][0..head_dim], data[src_start..][0..head_dim]);
    }
    return out;
}

/// SIMD column-sum: given matrix [rows x cols], compute sum of each column.
fn columnSum(matrix: []const f32, rows: usize, cols: usize, out: []f32) void {
    @memset(out[0..cols], 0);
    for (0..rows) |r| {
        const row = matrix[r * cols ..][0..cols];
        var c: usize = 0;
        while (c + VEC_LEN <= cols) : (c += VEC_LEN) {
            const src: F32xN = row[c..][0..VEC_LEN].*;
            const dst: F32xN = out[c..][0..VEC_LEN].*;
            out[c..][0..VEC_LEN].* = dst + src;
        }
        while (c < cols) : (c += 1) {
            out[c] += row[c];
        }
    }
}

/// Partial sort: rearrange indices[0..len] so that the top-M elements by
/// descending mass are in indices[0..M] (in unspecified order).
/// Uses quickselect partitioning.
fn partialSort(indices: []usize, mass: []const f32, M: usize) void {
    if (M >= indices.len) return;
    var lo: usize = 0;
    var hi: usize = indices.len;

    while (lo < hi) {
        const pivot_idx = lo + (hi - lo) / 2;
        const pivot_mass = mass[indices[pivot_idx]];

        // Move pivot to end.
        std.mem.swap(usize, &indices[pivot_idx], &indices[hi - 1]);
        var store: usize = lo;
        for (lo..hi - 1) |i| {
            if (mass[indices[i]] > pivot_mass) {
                std.mem.swap(usize, &indices[store], &indices[i]);
                store += 1;
            }
        }
        std.mem.swap(usize, &indices[store], &indices[hi - 1]);

        if (store == M) return;
        if (store < M) lo = store + 1 else hi = store;
    }
}

// --- Tests ---

test "compactLayerHead preserves output at ratio 1.0" {
    const allocator = std.testing.allocator;
    const n = 8;
    const d = 4;
    const r = 4;

    var K: [n * d]f32 = undefined;
    var V: [n * d]f32 = undefined;
    var Q: [r * d]f32 = undefined;
    // Simple pattern: K[i] = [i, i, i, i], V[i] = [i*10, ...], Q[j] = [j, ...]
    for (0..n) |i| {
        const f: f32 = @floatFromInt(i);
        for (0..d) |dd| {
            K[i * d + dd] = f + @as(f32, @floatFromInt(dd)) * 0.1;
            V[i * d + dd] = f * 10.0 + @as(f32, @floatFromInt(dd));
        }
    }
    for (0..r) |i| {
        const f: f32 = @floatFromInt(i);
        for (0..d) |dd| Q[i * d + dd] = f + @as(f32, @floatFromInt(dd)) * 0.1;
    }

    var result = try compactLayerHead(allocator, &K, &V, &Q, n, r, d, .{ .target_ratio = 1.0 });
    defer result.deinit();

    try std.testing.expectEqual(n, result.retained_count);
    try std.testing.expectEqualSlices(f32, &K, result.k_hat);
    try std.testing.expectEqualSlices(f32, &V, result.v_hat);
}

test "compactLayerHead reduces token count at ratio 0.5" {
    const allocator = std.testing.allocator;
    const n = 16;
    const d = 4;
    const r = 8;

    // Create keys where half dominate attention (large magnitude).
    var K: [n * d]f32 = undefined;
    var V: [n * d]f32 = undefined;
    var Q: [r * d]f32 = undefined;
    for (0..n) |i| {
        const scale: f32 = if (i < 8) 10.0 else 0.01; // first 8 are dominant
        for (0..d) |dd| {
            K[i * d + dd] = scale * (@as(f32, @floatFromInt(i)) + @as(f32, @floatFromInt(dd)) * 0.1);
            V[i * d + dd] = @as(f32, @floatFromInt(i)) + @as(f32, @floatFromInt(dd)) * 0.01;
        }
    }
    for (0..r) |i| {
        for (0..d) |dd| Q[i * d + dd] = 5.0 + @as(f32, @floatFromInt(i)) + @as(f32, @floatFromInt(dd)) * 0.1;
    }

    var result = try compactLayerHead(allocator, &K, &V, &Q, n, r, d, .{ .target_ratio = 0.5 });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 8), result.retained_count);

    // Verify the compacted output approximates the original.
    // Y_orig = softmax(Q * K^T / sqrt(d)) * V  [r x d]
    // Y_hat  = softmax(Q * K_hat^T / sqrt(d)) * V_hat  [r x d]
    const scale = 1.0 / @sqrt(@as(f32, d));

    var scores_orig: [r * n]f32 = undefined;
    native.sgemmTransBSync(r, n, d, scale, &Q, &K, 0.0, &scores_orig);
    activations.softmax(&scores_orig, n);
    var Y_orig: [r * d]f32 = undefined;
    native.sgemmSync(r, d, n, 1.0, &scores_orig, &V, 0.0, &Y_orig);

    var scores_hat: [r * 8]f32 = undefined;
    native.sgemmTransBSync(r, 8, d, scale, &Q, result.k_hat, 0.0, &scores_hat);
    activations.softmax(&scores_hat, 8);
    var Y_hat: [r * d]f32 = undefined;
    native.sgemmSync(r, d, 8, 1.0, &scores_hat, result.v_hat, 0.0, &Y_hat);

    // Check approximate equality.
    for (0..r * d) |i| {
        try std.testing.expectApproxEqAbs(Y_orig[i], Y_hat[i], 0.5);
    }
}

test "columnSum SIMD accumulation" {
    // 3 rows, 10 columns (tests both SIMD and scalar remainder).
    const matrix = [_]f32{
        1,  2,  3,  4,  5,  6,  7,  8,  9,  10,
        10, 20, 30, 40, 50, 60, 70, 80, 90, 100,
        1,  1,  1,  1,  1,  1,  1,  1,  1,  1,
    };
    var out: [10]f32 = undefined;
    columnSum(&matrix, 3, 10, &out);
    try std.testing.expectApproxEqAbs(@as(f32, 12.0), out[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 23.0), out[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 111.0), out[9], 1e-6);
}

test "partialSort selects top-M by descending mass" {
    const mass = [_]f32{ 1.0, 5.0, 3.0, 4.0, 2.0 };
    var indices = [_]usize{ 0, 1, 2, 3, 4 };
    partialSort(&indices, &mass, 2);

    // Top-2 by mass should be indices 1 (mass=5) and 3 (mass=4).
    var top_set = [_]bool{ false, false, false, false, false };
    for (indices[0..2]) |idx| top_set[idx] = true;
    try std.testing.expect(top_set[1]); // mass=5
    try std.testing.expect(top_set[3]); // mass=4
}

test "extractHead deinterleaves correctly" {
    // 2 tokens, 3 heads, head_dim=2 -> interleaved as [t0h0d0, t0h0d1, t0h1d0, t0h1d1, t0h2d0, t0h2d1, t1h0d0, ...]
    const data = [_]f32{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const allocator = std.testing.allocator;

    // Extract head 1: should get [t0h1d0, t0h1d1, t1h1d0, t1h1d1] = [3, 4, 9, 10]
    const head1 = try extractHead(allocator, &data, 2, 3, 2, 1);
    defer allocator.free(head1);
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), head1[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), head1[1], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 9.0), head1[2], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 10.0), head1[3], 1e-6);
}

test "compactLayerHead single token" {
    const allocator = std.testing.allocator;
    // Single token can't be reduced below 1.
    var K = [_]f32{ 1, 2, 3, 4 };
    var V = [_]f32{ 5, 6, 7, 8 };
    var Q = [_]f32{ 1, 2, 3, 4 };

    var result = try compactLayerHead(allocator, &K, &V, &Q, 1, 1, 4, .{ .target_ratio = 0.1 });
    defer result.deinit();

    try std.testing.expectEqual(@as(usize, 1), result.retained_count);
    try std.testing.expectEqualSlices(f32, &K, result.k_hat);
}
