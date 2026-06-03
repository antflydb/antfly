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

// Fused chunker loss configuration, boundary head graph, and boundary metrics.

const std = @import("std");
const ml = @import("ml");
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const Builder = ml.graph.Builder;

// ----------------------------------------------------------------------------
// FusedLossConfig
// ----------------------------------------------------------------------------

pub const FusedLossConfig = struct {
    lambda_chunk: f32 = 1.0,
    lambda_embed: f32 = 0.5,
    lambda_coherence: f32 = 0.2,
    focal_gamma: f32 = 2.0,
    focal_alpha: f32 = 0.75,
    temperature: f32 = 0.07,
    num_hard_negatives: u32 = 7,
    coherence_margin: f32 = 0.2,
    pos_weight: f32 = 5.0,
    use_focal: bool = false,

    // MRL (Matryoshka Representation Learning)
    use_mrl: bool = false,
    mrl_dims: []const u32 = &.{ 768, 256, 128 },
    mrl_weights: []const f32 = &.{ 1.0, 1.0, 1.0 },

    // SPLADE sparse embeddings
    enable_splade: bool = false,
    lambda_splade: f32 = 0.15,
    lambda_flops: f32 = 3e-5,
    splade_focus_epoch: u32 = 4, // epoch when SPLADE activates
};

/// Returns a config tuned for boundary-focused training:
/// lambda_embed=0.1, lambda_coherence=0.0 (ignore coherence term).
pub fn boundaryFocusConfig() FusedLossConfig {
    return .{
        .lambda_embed = 0.1,
        .lambda_coherence = 0.0,
    };
}

// ----------------------------------------------------------------------------
// BoundaryHeadGraph
// ----------------------------------------------------------------------------

pub const BoundaryHeadGraph = struct {
    graph: Graph,
    feature_id: NodeId,
    target_id: NodeId,
    w1_id: NodeId,
    b1_id: NodeId,
    w2_id: NodeId,
    b2_id: NodeId,
    logits_id: NodeId,
    loss_id: NodeId,
    total: usize,
    hidden_dim: usize,
    mlp_dim: usize,

    /// Build the 2-layer MLP boundary head graph.
    ///
    /// Architecture:
    ///   features [total, hidden_dim]
    ///     -> linear(w1[mlp_dim, hidden_dim] + b1[mlp_dim])
    ///     -> gelu
    ///     -> linear(w2[2, mlp_dim] + b2[2])
    ///     -> crossEntropyLoss(logits, targets)
    pub fn init(allocator: std.mem.Allocator, total: usize, hidden_dim: usize, mlp_dim: usize) !BoundaryHeadGraph {
        var graph = Graph.init(allocator);
        errdefer graph.deinit();
        var b = Builder.init(&graph);

        const features = try b.parameter("features", Shape.init(.f32, &.{ @as(i64, @intCast(total)), @as(i64, @intCast(hidden_dim)) }));
        const targets = try b.parameter("targets", Shape.init(.f32, &.{ @as(i64, @intCast(total)), 2 }));
        const w1 = try b.parameter("w1", Shape.init(.f32, &.{ @as(i64, @intCast(mlp_dim)), @as(i64, @intCast(hidden_dim)) }));
        const b1 = try b.parameter("b1", Shape.init(.f32, &.{@as(i64, @intCast(mlp_dim))}));
        const w2 = try b.parameter("w2", Shape.init(.f32, &.{ 2, @as(i64, @intCast(mlp_dim)) }));
        const b2 = try b.parameter("b2", Shape.init(.f32, &.{2}));

        const dense = try b.linear(features, w1, b1, @intCast(total), @intCast(hidden_dim), @intCast(mlp_dim));
        const hidden = try b.gelu(dense);
        const logits = try b.linear(hidden, w2, b2, @intCast(total), @intCast(mlp_dim), 2);
        const loss = try b.crossEntropyLoss(logits, targets);
        try graph.markOutput(loss);

        return .{
            .graph = graph,
            .feature_id = features,
            .target_id = targets,
            .w1_id = w1,
            .b1_id = b1,
            .w2_id = w2,
            .b2_id = b2,
            .logits_id = logits,
            .loss_id = loss,
            .total = total,
            .hidden_dim = hidden_dim,
            .mlp_dim = mlp_dim,
        };
    }

    pub fn deinit(self: *BoundaryHeadGraph) void {
        self.graph.deinit();
        self.* = undefined;
    }
};

// ----------------------------------------------------------------------------
// BoundaryHeadWeights
// ----------------------------------------------------------------------------

pub const BoundaryHeadWeights = struct {
    allocator: std.mem.Allocator,
    w1: []f32, // [mlp_dim, hidden_dim]
    b1: []f32, // [mlp_dim]
    w2: []f32, // [2, mlp_dim]
    b2: []f32, // [2]
    hidden_dim: usize,
    mlp_dim: usize,

    /// Initialise weights with small deterministic values using the same
    /// scheme as `LinearHead.initDeterministic` in graph_bridge.zig:
    ///   angle = (row+1)*(col+5)
    ///   w[idx] = (sin(angle*0.11) + cos(angle*0.07)) * 0.05
    /// All biases are zero.
    pub fn initDeterministic(allocator: std.mem.Allocator, hidden_dim: usize, mlp_dim: usize) !BoundaryHeadWeights {
        const w1 = try allocator.alloc(f32, mlp_dim * hidden_dim);
        errdefer allocator.free(w1);
        const b1 = try allocator.alloc(f32, mlp_dim);
        errdefer allocator.free(b1);
        const w2 = try allocator.alloc(f32, 2 * mlp_dim);
        errdefer allocator.free(w2);
        const b2 = try allocator.alloc(f32, 2);
        errdefer allocator.free(b2);

        // w1: [mlp_dim, hidden_dim]
        for (0..mlp_dim) |row| {
            for (0..hidden_dim) |col| {
                const idx = row * hidden_dim + col;
                const angle = @as(f32, @floatFromInt((row + 1) * (col + 5)));
                w1[idx] = (@sin(angle * 0.11) + @cos(angle * 0.07)) * 0.05;
            }
        }
        @memset(b1, 0);

        // w2: [2, mlp_dim]
        for (0..2) |row| {
            for (0..mlp_dim) |col| {
                const idx = row * mlp_dim + col;
                const angle = @as(f32, @floatFromInt((row + 1) * (col + 5)));
                w2[idx] = (@sin(angle * 0.11) + @cos(angle * 0.07)) * 0.05;
            }
        }
        @memset(b2, 0);

        return .{
            .allocator = allocator,
            .w1 = w1,
            .b1 = b1,
            .w2 = w2,
            .b2 = b2,
            .hidden_dim = hidden_dim,
            .mlp_dim = mlp_dim,
        };
    }

    pub fn deinit(self: *BoundaryHeadWeights) void {
        self.allocator.free(self.w1);
        self.allocator.free(self.b1);
        self.allocator.free(self.w2);
        self.allocator.free(self.b2);
        self.* = undefined;
    }
};

// ----------------------------------------------------------------------------
// BoundaryMetrics
// ----------------------------------------------------------------------------

pub const BoundaryMetrics = struct {
    tp: u64,
    fp: u64,
    fn_: u64,

    pub fn precision(self: BoundaryMetrics) f32 {
        const denom = self.tp + self.fp;
        if (denom == 0) return 0.0;
        return @as(f32, @floatFromInt(self.tp)) / @as(f32, @floatFromInt(denom));
    }

    pub fn recall(self: BoundaryMetrics) f32 {
        const denom = self.tp + self.fn_;
        if (denom == 0) return 0.0;
        return @as(f32, @floatFromInt(self.tp)) / @as(f32, @floatFromInt(denom));
    }

    pub fn f1(self: BoundaryMetrics) f32 {
        const p = self.precision();
        const r = self.recall();
        const denom = p + r;
        if (denom == 0.0) return 0.0;
        return 2.0 * p * r / denom;
    }
};

/// Compute boundary detection metrics from flat logits and labels.
///
/// logits: flat [total * 2] f32, row-major — two class scores per token.
/// labels: flat [total] f32, values 0 or 1.
/// mask:   flat [total] f32 (1=valid, 0=pad); pass null to count all tokens.
pub fn computeBoundaryMetrics(logits: []const f32, labels: []const f32, mask: ?[]const f32) BoundaryMetrics {
    var tp: u64 = 0;
    var fp: u64 = 0;
    var fn_: u64 = 0;

    const total = labels.len;
    for (0..total) |i| {
        if (mask) |m| {
            if (m[i] <= 0.5) continue;
        }
        const predicted: u1 = if (logits[i * 2 + 1] > logits[i * 2 + 0]) 1 else 0;
        const true_label: bool = labels[i] > 0.5;

        if (predicted == 1 and true_label) {
            tp += 1;
        } else if (predicted == 1 and !true_label) {
            fp += 1;
        } else if (predicted == 0 and true_label) {
            fn_ += 1;
        }
    }

    return .{ .tp = tp, .fp = fp, .fn_ = fn_ };
}

// ----------------------------------------------------------------------------
// Tests
// ----------------------------------------------------------------------------

test "FusedLossConfig defaults" {
    const cfg = FusedLossConfig{};
    try std.testing.expectApproxEqAbs(cfg.lambda_chunk, 1.0, 1e-6);
    try std.testing.expectApproxEqAbs(cfg.lambda_embed, 0.5, 1e-6);
    try std.testing.expectApproxEqAbs(cfg.lambda_coherence, 0.2, 1e-6);
    try std.testing.expectApproxEqAbs(cfg.focal_gamma, 2.0, 1e-6);
    try std.testing.expectApproxEqAbs(cfg.focal_alpha, 0.75, 1e-6);
    try std.testing.expectApproxEqAbs(cfg.temperature, 0.07, 1e-6);
    try std.testing.expectEqual(cfg.num_hard_negatives, 7);
    try std.testing.expectApproxEqAbs(cfg.coherence_margin, 0.2, 1e-6);
    try std.testing.expectApproxEqAbs(cfg.pos_weight, 5.0, 1e-6);
    try std.testing.expect(!cfg.use_focal);
}

test "computeBoundaryMetrics basic" {
    // 4 tokens total; mask marks tokens 0 and 2 as valid, 1 and 3 as padding.
    //
    // Token 0: logits=[0.1, 0.9] → predicted=1, label=1 → TP
    // Token 1: masked out (ignored)
    // Token 2: logits=[0.8, 0.2] → predicted=0, label=1 → FN
    // Token 3: masked out (ignored)
    const logits = [_]f32{
        0.1, 0.9, // token 0
        0.3, 0.7, // token 1 (masked)
        0.8, 0.2, // token 2
        0.4, 0.6, // token 3 (masked)
    };
    const labels = [_]f32{ 1.0, 0.0, 1.0, 0.0 };
    const mask = [_]f32{ 1.0, 0.0, 1.0, 0.0 };

    const m = computeBoundaryMetrics(&logits, &labels, &mask);
    try std.testing.expectEqual(m.tp, 1);
    try std.testing.expectEqual(m.fp, 0);
    try std.testing.expectEqual(m.fn_, 1);

    try std.testing.expectApproxEqAbs(m.precision(), 1.0, 1e-6);
    try std.testing.expectApproxEqAbs(m.recall(), 0.5, 1e-6);
    // f1 = 2*1*0.5/(1+0.5) = 1/1.5 ≈ 0.6667
    try std.testing.expectApproxEqAbs(m.f1(), 2.0 / 3.0, 1e-5);
}
