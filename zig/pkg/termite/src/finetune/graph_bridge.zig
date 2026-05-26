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
const ml = @import("ml");
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const Builder = ml.graph.Builder;
const optimizers = ml.graph.optimizers;
const ops_mod = @import("../ops/ops.zig");
const ComputeBackend = ops_mod.ComputeBackend;
const CT = ops_mod.CT;
const training = @import("../graph/training.zig");

pub const LinearClassifierGraph = struct {
    graph: Graph,
    feature_id: NodeId,
    target_id: NodeId,
    weight_id: NodeId,
    bias_id: NodeId,
    logits_id: NodeId,
    loss_id: NodeId,
    rows: usize,
    input_dim: usize,
    num_labels: usize,

    pub fn init(allocator: std.mem.Allocator, rows: usize, input_dim: usize, num_labels: usize) !LinearClassifierGraph {
        var graph = Graph.init(allocator);
        errdefer graph.deinit();
        var b = Builder.init(&graph);

        const features = try b.parameter("features", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(input_dim)) }));
        const targets = try b.parameter("targets", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(num_labels)) }));
        const weight = try b.parameter("weight", Shape.init(.f32, &.{ @as(i64, @intCast(num_labels)), @as(i64, @intCast(input_dim)) }));
        const bias = try b.parameter("bias", Shape.init(.f32, &.{@as(i64, @intCast(num_labels))}));
        const logits = try b.linear(features, weight, bias, @intCast(rows), @intCast(input_dim), @intCast(num_labels));
        const loss = if (num_labels == 1)
            try b.mseLoss(logits, targets)
        else
            try b.crossEntropyLoss(logits, targets);
        try graph.markOutput(loss);

        return .{
            .graph = graph,
            .feature_id = features,
            .target_id = targets,
            .weight_id = weight,
            .bias_id = bias,
            .logits_id = logits,
            .loss_id = loss,
            .rows = rows,
            .input_dim = input_dim,
            .num_labels = num_labels,
        };
    }

    pub fn deinit(self: *LinearClassifierGraph) void {
        self.graph.deinit();
        self.* = undefined;
    }
};

pub const MlpClassifierGraph = struct {
    graph: Graph,
    feature_id: NodeId,
    target_id: NodeId,
    dense_weight_id: NodeId,
    dense_bias_id: NodeId,
    out_weight_id: NodeId,
    out_bias_id: NodeId,
    hidden_id: NodeId,
    logits_id: NodeId,
    loss_id: NodeId,
    rows: usize,
    input_dim: usize,
    hidden_dim: usize,
    num_labels: usize,

    pub fn init(allocator: std.mem.Allocator, rows: usize, input_dim: usize, hidden_dim: usize, num_labels: usize) !MlpClassifierGraph {
        var graph = Graph.init(allocator);
        errdefer graph.deinit();
        var b = Builder.init(&graph);

        const features = try b.parameter("features", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(input_dim)) }));
        const targets = try b.parameter("targets", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(num_labels)) }));
        const dense_weight = try b.parameter("dense_weight", Shape.init(.f32, &.{ @as(i64, @intCast(hidden_dim)), @as(i64, @intCast(input_dim)) }));
        const dense_bias = try b.parameter("dense_bias", Shape.init(.f32, &.{@as(i64, @intCast(hidden_dim))}));
        const out_weight = try b.parameter("out_weight", Shape.init(.f32, &.{ @as(i64, @intCast(num_labels)), @as(i64, @intCast(hidden_dim)) }));
        const out_bias = try b.parameter("out_bias", Shape.init(.f32, &.{@as(i64, @intCast(num_labels))}));

        const dense = try b.linear(features, dense_weight, dense_bias, @intCast(rows), @intCast(input_dim), @intCast(hidden_dim));
        const hidden = try b.tanhOp(dense);
        const logits = try b.linear(hidden, out_weight, out_bias, @intCast(rows), @intCast(hidden_dim), @intCast(num_labels));
        const loss = try b.crossEntropyLoss(logits, targets);
        try graph.markOutput(loss);

        return .{
            .graph = graph,
            .feature_id = features,
            .target_id = targets,
            .dense_weight_id = dense_weight,
            .dense_bias_id = dense_bias,
            .out_weight_id = out_weight,
            .out_bias_id = out_bias,
            .hidden_id = hidden,
            .logits_id = logits,
            .loss_id = loss,
            .rows = rows,
            .input_dim = input_dim,
            .hidden_dim = hidden_dim,
            .num_labels = num_labels,
        };
    }

    pub fn deinit(self: *MlpClassifierGraph) void {
        self.graph.deinit();
        self.* = undefined;
    }
};

pub const LinearHead = struct {
    allocator: std.mem.Allocator,
    weight: []f32,
    bias: []f32,
    num_labels: usize,
    input_dim: usize,

    pub fn initDeterministic(allocator: std.mem.Allocator, input_dim: usize, num_labels: usize) !LinearHead {
        const weight = try allocator.alloc(f32, input_dim * num_labels);
        errdefer allocator.free(weight);
        const bias = try allocator.alloc(f32, num_labels);
        errdefer allocator.free(bias);

        for (0..num_labels) |row| {
            for (0..input_dim) |col| {
                const idx = row * input_dim + col;
                const angle = @as(f32, @floatFromInt((row + 1) * (col + 5)));
                weight[idx] = (@sin(angle * 0.11) + @cos(angle * 0.07)) * 0.05;
            }
        }
        @memset(bias, 0);

        return .{
            .allocator = allocator,
            .weight = weight,
            .bias = bias,
            .num_labels = num_labels,
            .input_dim = input_dim,
        };
    }

    pub fn deinit(self: *LinearHead) void {
        self.allocator.free(self.weight);
        self.allocator.free(self.bias);
        self.* = undefined;
    }
};

pub const MlpHead = struct {
    allocator: std.mem.Allocator,
    dense_weight: []f32,
    dense_bias: []f32,
    out_weight: []f32,
    out_bias: []f32,
    input_dim: usize,
    hidden_dim: usize,
    num_labels: usize,

    pub fn deinit(self: *MlpHead) void {
        self.allocator.free(self.dense_weight);
        self.allocator.free(self.dense_bias);
        self.allocator.free(self.out_weight);
        self.allocator.free(self.out_bias);
        self.* = undefined;
    }
};

pub const ClassificationBatch = struct {
    features: []const f32,
    labels: []const usize,
    rows: usize,
    input_dim: usize,
    num_labels: usize,
};

pub const RegressionBatch = struct {
    features: []const f32,
    targets: []const f32,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
};

pub const LinearTrainSummary = struct {
    loss_before: f32,
    loss_after: f32,
    weight_grad_l2: f64,
    bias_grad_l2: f64,
    weight_l2_before: f64,
    weight_l2_after: f64,
    bias_l2_before: f64,
    bias_l2_after: f64,
};

pub const MlpTrainSummary = struct {
    loss_before: f32,
    loss_after: f32,
    dense_weight_grad_l2: f64,
    dense_bias_grad_l2: f64,
    out_weight_grad_l2: f64,
    out_bias_grad_l2: f64,
    dense_weight_l2_before: f64,
    dense_weight_l2_after: f64,
    out_weight_l2_before: f64,
    out_weight_l2_after: f64,
};

pub const LoRALinearGraph = struct {
    graph: Graph,
    feature_id: NodeId,
    target_id: NodeId,
    weight_id: NodeId,
    lora_a_id: NodeId,
    lora_b_id: NodeId,
    output_id: NodeId,
    loss_id: NodeId,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    alpha: f32,

    pub fn init(allocator: std.mem.Allocator, rows: usize, input_dim: usize, output_dim: usize, rank: usize, alpha: f32) !LoRALinearGraph {
        var graph = Graph.init(allocator);
        errdefer graph.deinit();
        var b = Builder.init(&graph);

        const features = try b.parameter("features", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(input_dim)) }));
        const weight = try b.parameter("weight", Shape.init(.f32, &.{ @as(i64, @intCast(output_dim)), @as(i64, @intCast(input_dim)) }));
        const lora_a = try b.parameter("lora_a", Shape.init(.f32, &.{ @as(i64, @intCast(rank)), @as(i64, @intCast(input_dim)) }));
        const lora_b = try b.parameter("lora_b", Shape.init(.f32, &.{ @as(i64, @intCast(output_dim)), @as(i64, @intCast(rank)) }));
        const target = try b.parameter("target", Shape.init(.f32, &.{ @as(i64, @intCast(rows)), @as(i64, @intCast(output_dim)) }));
        const base_output = try b.linearNoBias(features, weight, @intCast(rows), @intCast(input_dim), @intCast(output_dim));
        const lora_hidden = try b.linearNoBias(features, lora_a, @intCast(rows), @intCast(input_dim), @intCast(rank));
        const lora_out = try b.linearNoBias(lora_hidden, lora_b, @intCast(rows), @intCast(rank), @intCast(output_dim));
        const scale_const = try b.scalarConst(.f32, alpha / @as(f32, @floatFromInt(rank)));
        const scaled_lora = try b.mul(lora_out, scale_const);
        const output = try b.add(base_output, scaled_lora);
        const loss = try b.mseLoss(output, target);
        try graph.markOutput(loss);

        return .{
            .graph = graph,
            .feature_id = features,
            .target_id = target,
            .weight_id = weight,
            .lora_a_id = lora_a,
            .lora_b_id = lora_b,
            .output_id = output,
            .loss_id = loss,
            .rows = rows,
            .input_dim = input_dim,
            .output_dim = output_dim,
            .rank = rank,
            .alpha = alpha,
        };
    }

    pub fn deinit(self: *LoRALinearGraph) void {
        self.graph.deinit();
        self.* = undefined;
    }
};

pub const LoRALinearTrainSummary = struct {
    loss_before: f32,
    loss_after: f32,
    lora_a_grad_l2: f64,
    lora_b_grad_l2: f64,
    lora_a_l2_before: f64,
    lora_a_l2_after: f64,
    lora_b_l2_before: f64,
    lora_b_l2_after: f64,
};

pub fn trainLinearClassifierOneStep(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const LinearClassifierGraph,
    head: *LinearHead,
    batch: ClassificationBatch,
    optimizer: optimizers.Optimizer,
    optimizer_state: *optimizers.OptimizerState,
    learning_rate: f32,
) !LinearTrainSummary {
    if (batch.rows != graph_bundle.rows or batch.input_dim != graph_bundle.input_dim or batch.num_labels != graph_bundle.num_labels) {
        return error.ShapeMismatch;
    }
    if (batch.features.len != batch.rows * batch.input_dim) return error.ShapeMismatch;
    if (batch.labels.len != batch.rows) return error.ShapeMismatch;
    if (head.input_dim != batch.input_dim or head.num_labels != batch.num_labels) return error.ShapeMismatch;

    const loss_before = try evaluateLoss(allocator, cb, graph_bundle, head, batch);
    const weight_l2_before = l2Norm(head.weight);
    const bias_l2_before = l2Norm(head.bias);

    const targets = try buildOneHotTargets(allocator, batch.labels, batch.num_labels);
    defer allocator.free(targets);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, batch.features, &.{ @intCast(batch.rows), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, targets, &.{ @intCast(batch.rows), @intCast(batch.num_labels) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.weight_id, head.weight, &.{ @intCast(batch.num_labels), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.bias_id, head.bias, &.{@intCast(batch.num_labels)});

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{ "weight", "bias" },
    });
    defer result.deinit();

    const weight_grad = result.gradients.get("weight") orelse return error.MissingWeightGradient;
    const bias_grad = result.gradients.get("bias") orelse return error.MissingBiasGradient;
    optimizer_state.step_count += 1;
    try optimizers.step(optimizer, optimizer_state, learning_rate, "weight", head.weight, weight_grad);
    try optimizers.step(optimizer, optimizer_state, learning_rate, "bias", head.bias, bias_grad);

    const loss_after = try evaluateLoss(allocator, cb, graph_bundle, head, batch);

    return .{
        .loss_before = loss_before,
        .loss_after = loss_after,
        .weight_grad_l2 = l2Norm(weight_grad),
        .bias_grad_l2 = l2Norm(bias_grad),
        .weight_l2_before = weight_l2_before,
        .weight_l2_after = l2Norm(head.weight),
        .bias_l2_before = bias_l2_before,
        .bias_l2_after = l2Norm(head.bias),
    };
}

pub fn evaluateLoss(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const LinearClassifierGraph,
    head: *const LinearHead,
    batch: ClassificationBatch,
) !f32 {
    const targets = try buildOneHotTargets(allocator, batch.labels, batch.num_labels);
    defer allocator.free(targets);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, batch.features, &.{ @intCast(batch.rows), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, targets, &.{ @intCast(batch.rows), @intCast(batch.num_labels) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.weight_id, head.weight, &.{ @intCast(batch.num_labels), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.bias_id, head.bias, &.{@intCast(batch.num_labels)});

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{},
    });
    defer result.deinit();
    return result.loss;
}

pub fn trainMlpClassifierOneStep(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const MlpClassifierGraph,
    head: *MlpHead,
    batch: ClassificationBatch,
    optimizer: optimizers.Optimizer,
    optimizer_state: *optimizers.OptimizerState,
    learning_rate: f32,
) !MlpTrainSummary {
    if (batch.rows != graph_bundle.rows or batch.input_dim != graph_bundle.input_dim or batch.num_labels != graph_bundle.num_labels) {
        return error.ShapeMismatch;
    }
    if (batch.features.len != batch.rows * batch.input_dim) return error.ShapeMismatch;
    if (batch.labels.len != batch.rows) return error.ShapeMismatch;
    if (head.input_dim != batch.input_dim or head.hidden_dim != graph_bundle.hidden_dim or head.num_labels != batch.num_labels) {
        return error.ShapeMismatch;
    }

    const loss_before = try evaluateMlpLoss(allocator, cb, graph_bundle, head, batch);
    const dense_weight_l2_before = l2Norm(head.dense_weight);
    const out_weight_l2_before = l2Norm(head.out_weight);

    const targets = try buildOneHotTargets(allocator, batch.labels, batch.num_labels);
    defer allocator.free(targets);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, batch.features, &.{ @intCast(batch.rows), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, targets, &.{ @intCast(batch.rows), @intCast(batch.num_labels) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.dense_weight_id, head.dense_weight, &.{ @intCast(head.hidden_dim), @intCast(head.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.dense_bias_id, head.dense_bias, &.{@intCast(head.hidden_dim)});
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.out_weight_id, head.out_weight, &.{ @intCast(head.num_labels), @intCast(head.hidden_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.out_bias_id, head.out_bias, &.{@intCast(head.num_labels)});

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{ "dense_weight", "dense_bias", "out_weight", "out_bias" },
    });
    defer result.deinit();

    const dense_weight_grad = result.gradients.get("dense_weight") orelse return error.MissingDenseWeightGradient;
    const dense_bias_grad = result.gradients.get("dense_bias") orelse return error.MissingDenseBiasGradient;
    const out_weight_grad = result.gradients.get("out_weight") orelse return error.MissingOutWeightGradient;
    const out_bias_grad = result.gradients.get("out_bias") orelse return error.MissingOutBiasGradient;
    optimizer_state.step_count += 1;
    try optimizers.step(optimizer, optimizer_state, learning_rate, "dense_weight", head.dense_weight, dense_weight_grad);
    try optimizers.step(optimizer, optimizer_state, learning_rate, "dense_bias", head.dense_bias, dense_bias_grad);
    try optimizers.step(optimizer, optimizer_state, learning_rate, "out_weight", head.out_weight, out_weight_grad);
    try optimizers.step(optimizer, optimizer_state, learning_rate, "out_bias", head.out_bias, out_bias_grad);

    const loss_after = try evaluateMlpLoss(allocator, cb, graph_bundle, head, batch);
    return .{
        .loss_before = loss_before,
        .loss_after = loss_after,
        .dense_weight_grad_l2 = l2Norm(dense_weight_grad),
        .dense_bias_grad_l2 = l2Norm(dense_bias_grad),
        .out_weight_grad_l2 = l2Norm(out_weight_grad),
        .out_bias_grad_l2 = l2Norm(out_bias_grad),
        .dense_weight_l2_before = dense_weight_l2_before,
        .dense_weight_l2_after = l2Norm(head.dense_weight),
        .out_weight_l2_before = out_weight_l2_before,
        .out_weight_l2_after = l2Norm(head.out_weight),
    };
}

pub fn trainLinearRegressorOneStep(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const LinearClassifierGraph,
    head: *LinearHead,
    batch: RegressionBatch,
    optimizer: optimizers.Optimizer,
    optimizer_state: *optimizers.OptimizerState,
    learning_rate: f32,
) !LinearTrainSummary {
    if (batch.rows != graph_bundle.rows or batch.input_dim != graph_bundle.input_dim or batch.output_dim != graph_bundle.num_labels) {
        return error.ShapeMismatch;
    }
    if (batch.features.len != batch.rows * batch.input_dim) return error.ShapeMismatch;
    if (batch.targets.len != batch.rows * batch.output_dim) return error.ShapeMismatch;
    if (head.input_dim != batch.input_dim or head.num_labels != batch.output_dim) return error.ShapeMismatch;

    const loss_before = try evaluateRegressionLoss(allocator, cb, graph_bundle, head, batch);
    const weight_l2_before = l2Norm(head.weight);
    const bias_l2_before = l2Norm(head.bias);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, batch.features, &.{ @intCast(batch.rows), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, batch.targets, &.{ @intCast(batch.rows), @intCast(batch.output_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.weight_id, head.weight, &.{ @intCast(batch.output_dim), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.bias_id, head.bias, &.{@intCast(batch.output_dim)});

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{ "weight", "bias" },
    });
    defer result.deinit();

    const weight_grad = result.gradients.get("weight") orelse return error.MissingWeightGradient;
    const bias_grad = result.gradients.get("bias") orelse return error.MissingBiasGradient;

    optimizer_state.step_count += 1;
    try optimizers.step(optimizer, optimizer_state, learning_rate, "weight", head.weight, weight_grad);
    try optimizers.step(optimizer, optimizer_state, learning_rate, "bias", head.bias, bias_grad);

    const loss_after = try evaluateRegressionLoss(allocator, cb, graph_bundle, head, batch);

    return .{
        .loss_before = loss_before,
        .loss_after = loss_after,
        .weight_grad_l2 = l2Norm(weight_grad),
        .bias_grad_l2 = l2Norm(bias_grad),
        .weight_l2_before = weight_l2_before,
        .weight_l2_after = l2Norm(head.weight),
        .bias_l2_before = bias_l2_before,
        .bias_l2_after = l2Norm(head.bias),
    };
}

pub fn evaluateRegressionLoss(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const LinearClassifierGraph,
    head: *const LinearHead,
    batch: RegressionBatch,
) !f32 {
    if (batch.rows != graph_bundle.rows or batch.input_dim != graph_bundle.input_dim or batch.output_dim != graph_bundle.num_labels) {
        return error.ShapeMismatch;
    }
    if (batch.features.len != batch.rows * batch.input_dim) return error.ShapeMismatch;
    if (batch.targets.len != batch.rows * batch.output_dim) return error.ShapeMismatch;
    if (head.input_dim != batch.input_dim or head.num_labels != batch.output_dim) return error.ShapeMismatch;

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, batch.features, &.{ @intCast(batch.rows), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, batch.targets, &.{ @intCast(batch.rows), @intCast(batch.output_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.weight_id, head.weight, &.{ @intCast(batch.output_dim), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.bias_id, head.bias, &.{@intCast(batch.output_dim)});

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{},
    });
    defer result.deinit();
    return result.loss;
}

pub fn evaluateMlpLoss(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const MlpClassifierGraph,
    head: *const MlpHead,
    batch: ClassificationBatch,
) !f32 {
    const targets = try buildOneHotTargets(allocator, batch.labels, batch.num_labels);
    defer allocator.free(targets);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, batch.features, &.{ @intCast(batch.rows), @intCast(batch.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, targets, &.{ @intCast(batch.rows), @intCast(batch.num_labels) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.dense_weight_id, head.dense_weight, &.{ @intCast(head.hidden_dim), @intCast(head.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.dense_bias_id, head.dense_bias, &.{@intCast(head.hidden_dim)});
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.out_weight_id, head.out_weight, &.{ @intCast(head.num_labels), @intCast(head.hidden_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.out_bias_id, head.out_bias, &.{@intCast(head.num_labels)});

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{},
    });
    defer result.deinit();
    return result.loss;
}

pub fn trainLoRALinearOneStep(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const LoRALinearGraph,
    optimizer: optimizers.Optimizer,
    optimizer_state: *optimizers.OptimizerState,
    base_weight_native: []const f32,
    adapter_a_native: []f32,
    adapter_b_native: []f32,
    features: []const f32,
    target: []const f32,
    learning_rate: f32,
) !LoRALinearTrainSummary {
    if (features.len != graph_bundle.rows * graph_bundle.input_dim) return error.ShapeMismatch;
    if (target.len != graph_bundle.rows * graph_bundle.output_dim) return error.ShapeMismatch;
    if (base_weight_native.len != graph_bundle.input_dim * graph_bundle.output_dim) return error.ShapeMismatch;
    if (adapter_a_native.len != graph_bundle.input_dim * graph_bundle.rank) return error.ShapeMismatch;
    if (adapter_b_native.len != graph_bundle.rank * graph_bundle.output_dim) return error.ShapeMismatch;

    const base_weight = try allocator.alloc(f32, graph_bundle.output_dim * graph_bundle.input_dim);
    defer allocator.free(base_weight);
    transpose2DF32(base_weight, base_weight_native, graph_bundle.input_dim, graph_bundle.output_dim);

    const lora_a = try allocator.alloc(f32, graph_bundle.rank * graph_bundle.input_dim);
    defer allocator.free(lora_a);
    transpose2DF32(lora_a, adapter_a_native, graph_bundle.input_dim, graph_bundle.rank);

    const lora_b = try allocator.alloc(f32, graph_bundle.output_dim * graph_bundle.rank);
    defer allocator.free(lora_b);
    transpose2DF32(lora_b, adapter_b_native, graph_bundle.rank, graph_bundle.output_dim);

    const loss_before = try evaluateLoRALinearLoss(allocator, cb, graph_bundle, base_weight, lora_a, lora_b, features, target);
    const lora_a_l2_before = l2Norm(lora_a);
    const lora_b_l2_before = l2Norm(lora_b);

    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, features, &.{ @intCast(graph_bundle.rows), @intCast(graph_bundle.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, target, &.{ @intCast(graph_bundle.rows), @intCast(graph_bundle.output_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.weight_id, base_weight, &.{ @intCast(graph_bundle.output_dim), @intCast(graph_bundle.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.lora_a_id, lora_a, &.{ @intCast(graph_bundle.rank), @intCast(graph_bundle.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.lora_b_id, lora_b, &.{ @intCast(graph_bundle.output_dim), @intCast(graph_bundle.rank) });

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{ "lora_a", "lora_b" },
    });
    defer result.deinit();

    const lora_a_grad = result.gradients.get("lora_a") orelse return error.MissingLoRAAGradient;
    const lora_b_grad = result.gradients.get("lora_b") orelse return error.MissingLoRABGradient;
    optimizer_state.step_count += 1;
    try optimizers.step(optimizer, optimizer_state, learning_rate, "lora_a", lora_a, lora_a_grad);
    try optimizers.step(optimizer, optimizer_state, learning_rate, "lora_b", lora_b, lora_b_grad);

    transpose2DF32(adapter_a_native, lora_a, graph_bundle.rank, graph_bundle.input_dim);
    transpose2DF32(adapter_b_native, lora_b, graph_bundle.output_dim, graph_bundle.rank);

    const loss_after = try evaluateLoRALinearLoss(allocator, cb, graph_bundle, base_weight, lora_a, lora_b, features, target);
    return .{
        .loss_before = loss_before,
        .loss_after = loss_after,
        .lora_a_grad_l2 = l2Norm(lora_a_grad),
        .lora_b_grad_l2 = l2Norm(lora_b_grad),
        .lora_a_l2_before = lora_a_l2_before,
        .lora_a_l2_after = l2Norm(lora_a),
        .lora_b_l2_before = lora_b_l2_before,
        .lora_b_l2_after = l2Norm(lora_b),
    };
}

pub fn trainLoRALinearFromOutputGradOneStep(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const LoRALinearGraph,
    optimizer: optimizers.Optimizer,
    optimizer_state: *optimizers.OptimizerState,
    base_weight_native: []const f32,
    adapter_a_native: []f32,
    adapter_b_native: []f32,
    features: []const f32,
    output_grad: []const f32,
    learning_rate: f32,
) !LoRALinearTrainSummary {
    if (output_grad.len != graph_bundle.rows * graph_bundle.output_dim) return error.ShapeMismatch;

    const current_output = try allocator.alloc(f32, output_grad.len);
    defer allocator.free(current_output);
    computeLoRALinearOutputNative(
        current_output,
        graph_bundle.rows,
        graph_bundle.input_dim,
        graph_bundle.output_dim,
        graph_bundle.rank,
        graph_bundle.alpha / @as(f32, @floatFromInt(graph_bundle.rank)),
        base_weight_native,
        adapter_a_native,
        adapter_b_native,
        features,
    );

    const targets = try allocator.alloc(f32, output_grad.len);
    defer allocator.free(targets);
    buildMseTargetsFromOutputGrad(targets, current_output, output_grad, graph_bundle.rows * graph_bundle.output_dim);

    return trainLoRALinearOneStep(
        allocator,
        cb,
        graph_bundle,
        optimizer,
        optimizer_state,
        base_weight_native,
        adapter_a_native,
        adapter_b_native,
        features,
        targets,
        learning_rate,
    );
}

pub fn evaluateLoRALinearLoss(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    graph_bundle: *const LoRALinearGraph,
    base_weight: []const f32,
    lora_a: []const f32,
    lora_b: []const f32,
    features: []const f32,
    target: []const f32,
) !f32 {
    var rt = std.AutoHashMapUnmanaged(NodeId, CT){};
    defer {
        var it = rt.iterator();
        while (it.next()) |entry| cb.free(entry.value_ptr.*);
        rt.deinit(allocator);
    }
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.feature_id, features, &.{ @intCast(graph_bundle.rows), @intCast(graph_bundle.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.target_id, target, &.{ @intCast(graph_bundle.rows), @intCast(graph_bundle.output_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.weight_id, base_weight, &.{ @intCast(graph_bundle.output_dim), @intCast(graph_bundle.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.lora_a_id, lora_a, &.{ @intCast(graph_bundle.rank), @intCast(graph_bundle.input_dim) });
    try putRuntimeInput(allocator, cb, &rt, graph_bundle.lora_b_id, lora_b, &.{ @intCast(graph_bundle.output_dim), @intCast(graph_bundle.rank) });

    var result = try training.trainStep(allocator, &graph_bundle.graph, graph_bundle.loss_id, cb, rt, .{
        .trainable_params = &.{},
    });
    defer result.deinit();
    return result.loss;
}

fn buildOneHotTargets(allocator: std.mem.Allocator, labels: []const usize, num_labels: usize) ![]f32 {
    const targets = try allocator.alloc(f32, labels.len * num_labels);
    @memset(targets, 0);
    for (labels, 0..) |label, row| {
        if (label >= num_labels) return error.InvalidLabel;
        targets[row * num_labels + label] = 1.0;
    }
    return targets;
}

fn putRuntimeInput(
    allocator: std.mem.Allocator,
    cb: *const ComputeBackend,
    rt: *std.AutoHashMapUnmanaged(NodeId, CT),
    node_id: NodeId,
    values: []const f32,
    shape: []const i32,
) !void {
    const ct = try cb.fromFloat32Shape(values, shape);
    errdefer cb.free(ct);
    try rt.put(allocator, node_id, ct);
}

fn transpose2DF32(dst: []f32, src: []const f32, src_rows: usize, src_cols: usize) void {
    std.debug.assert(dst.len == src.len);
    for (0..src_rows) |row| {
        for (0..src_cols) |col| {
            dst[col * src_rows + row] = src[row * src_cols + col];
        }
    }
}

fn computeLoRALinearOutputNative(
    dst: []f32,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    scale: f32,
    base_weight_native: []const f32,
    adapter_a_native: []const f32,
    adapter_b_native: []const f32,
    features: []const f32,
) void {
    std.debug.assert(dst.len == rows * output_dim);
    std.debug.assert(base_weight_native.len == input_dim * output_dim);
    std.debug.assert(adapter_a_native.len == input_dim * rank);
    std.debug.assert(adapter_b_native.len == rank * output_dim);
    std.debug.assert(features.len == rows * input_dim);

    for (0..rows) |row| {
        const feature_row = features[row * input_dim .. (row + 1) * input_dim];
        for (0..output_dim) |out_idx| {
            var base_sum: f32 = 0.0;
            for (0..input_dim) |in_idx| {
                base_sum += feature_row[in_idx] * base_weight_native[in_idx * output_dim + out_idx];
            }
            dst[row * output_dim + out_idx] = base_sum;
            var lora_sum: f32 = 0.0;
            for (0..rank) |rank_idx| {
                var rank_sum: f32 = 0.0;
                for (0..input_dim) |in_idx| {
                    rank_sum += feature_row[in_idx] * adapter_a_native[in_idx * rank + rank_idx];
                }
                lora_sum += rank_sum * adapter_b_native[rank_idx * output_dim + out_idx];
            }
            dst[row * output_dim + out_idx] += scale * lora_sum;
        }
    }
}

fn buildMseTargetsFromOutputGrad(dst: []f32, current_output: []const f32, output_grad: []const f32, element_count: usize) void {
    std.debug.assert(dst.len == current_output.len);
    std.debug.assert(dst.len == output_grad.len);
    const scale = @as(f32, @floatFromInt(element_count)) * 0.5;
    for (dst, current_output, output_grad) |*target, current, grad| {
        target.* = current - (grad * scale);
    }
}

fn l2Norm(values: []const f32) f64 {
    var total: f64 = 0;
    for (values) |value| {
        const widened: f64 = value;
        total += widened * widened;
    }
    return @sqrt(total);
}

// ── PJRT LoRA training step ─────────────────────────────────────────

const build_options = @import("build_options");

/// A pre-compiled PJRT training step for one LoRA linear layer.
/// Inputs (in order): features, target, base_weight, lora_a, lora_b.
/// Outputs (in order): loss, grad_lora_a, grad_lora_b.
pub const LoRAPjrtTrainStep = struct {
    session: training.PjrtTrainSession,
    rows: usize,
    input_dim: usize,
    output_dim: usize,
    rank: usize,
    alpha: f32,
    // Original NodeIds for the 5 inputs (for session.execute() key lookup).
    feature_node_id: NodeId,
    target_node_id: NodeId,
    weight_node_id: NodeId,
    lora_a_node_id: NodeId,
    lora_b_node_id: NodeId,

    pub fn deinit(self: *LoRAPjrtTrainStep) void {
        self.session.deinit();
    }
};

/// Compile a LoRA linear layer's gradient computation for PJRT.
/// Call this once before the training loop; reuse for every step.
pub fn compileLoRALinearPjrtStep(
    allocator: std.mem.Allocator,
    graph_bundle: *const LoRALinearGraph,
    pjrt_client: anytype,
) !LoRAPjrtTrainStep {
    if (comptime !build_options.enable_pjrt) return error.PjrtNotCompiled;

    const runtime_inputs = [_]NodeId{
        graph_bundle.feature_id,
        graph_bundle.target_id,
        graph_bundle.weight_id,
        graph_bundle.lora_a_id,
        graph_bundle.lora_b_id,
    };

    const session = try training.compilePjrtTrainSession(
        allocator,
        &graph_bundle.graph,
        graph_bundle.loss_id,
        pjrt_client,
        &runtime_inputs,
        .{ .trainable_params = &.{ "lora_a", "lora_b" } },
    );

    return .{
        .session = session,
        .rows = graph_bundle.rows,
        .input_dim = graph_bundle.input_dim,
        .output_dim = graph_bundle.output_dim,
        .rank = graph_bundle.rank,
        .alpha = graph_bundle.alpha,
        .feature_node_id = graph_bundle.feature_id,
        .target_node_id = graph_bundle.target_id,
        .weight_node_id = graph_bundle.weight_id,
        .lora_a_node_id = graph_bundle.lora_a_id,
        .lora_b_node_id = graph_bundle.lora_b_id,
    };
}

/// Compute LoRA gradients for one step using a pre-compiled PJRT executable.
/// Takes output_grad (same convention as accumulateLinearLoRAGradsBackend).
/// Returns grad_a [input_dim × rank] and grad_b [rank × output_dim] in native layout.
/// Caller owns the returned slices.
pub fn computeLoRALinearGradsWithPjrt(
    allocator: std.mem.Allocator,
    compiled: *const LoRAPjrtTrainStep,
    base_weight_native: []const f32,
    adapter_a_native: []const f32,
    adapter_b_native: []const f32,
    features: []const f32,
    output_grad: []const f32,
) !struct { grad_a: []f32, grad_b: []f32 } {
    if (comptime !build_options.enable_pjrt) return error.PjrtNotCompiled;

    // Convert native layout → graph layout (transpose like trainLoRALinearOneStep does).
    const base_weight = try allocator.alloc(f32, compiled.output_dim * compiled.input_dim);
    defer allocator.free(base_weight);
    transpose2DF32(base_weight, base_weight_native, compiled.input_dim, compiled.output_dim);

    const lora_a = try allocator.alloc(f32, compiled.rank * compiled.input_dim);
    defer allocator.free(lora_a);
    transpose2DF32(lora_a, adapter_a_native, compiled.input_dim, compiled.rank);

    const lora_b = try allocator.alloc(f32, compiled.output_dim * compiled.rank);
    defer allocator.free(lora_b);
    transpose2DF32(lora_b, adapter_b_native, compiled.rank, compiled.output_dim);

    // Compute current LoRA output and build MSE targets from output_grad.
    const current_output = try allocator.alloc(f32, compiled.rows * compiled.output_dim);
    defer allocator.free(current_output);
    computeLoRALinearOutputNative(
        current_output,
        compiled.rows,
        compiled.input_dim,
        compiled.output_dim,
        compiled.rank,
        compiled.alpha / @as(f32, @floatFromInt(compiled.rank)),
        base_weight_native,
        adapter_a_native,
        adapter_b_native,
        features,
    );
    const targets = try allocator.alloc(f32, output_grad.len);
    defer allocator.free(targets);
    buildMseTargetsFromOutputGrad(targets, current_output, output_grad, compiled.rows * compiled.output_dim);

    // Build runtime inputs map.
    var rt = std.AutoHashMapUnmanaged(NodeId, training.PjrtRuntimeInput){};
    defer rt.deinit(allocator);

    const rows_i32: i32 = @intCast(compiled.rows);
    const in_i32: i32 = @intCast(compiled.input_dim);
    const out_i32: i32 = @intCast(compiled.output_dim);
    const rank_i32: i32 = @intCast(compiled.rank);

    const feature_shape = [_]i32{ rows_i32, in_i32 };
    const target_shape = [_]i32{ rows_i32, out_i32 };
    const weight_shape = [_]i32{ out_i32, in_i32 };
    const lora_a_shape = [_]i32{ rank_i32, in_i32 };
    const lora_b_shape = [_]i32{ out_i32, rank_i32 };

    try rt.put(allocator, compiled.feature_node_id, .{ .data = features, .shape = &feature_shape });
    try rt.put(allocator, compiled.target_node_id, .{ .data = targets, .shape = &target_shape });
    try rt.put(allocator, compiled.weight_node_id, .{ .data = base_weight, .shape = &weight_shape });
    try rt.put(allocator, compiled.lora_a_node_id, .{ .data = lora_a, .shape = &lora_a_shape });
    try rt.put(allocator, compiled.lora_b_node_id, .{ .data = lora_b, .shape = &lora_b_shape });

    // Execute on PJRT.
    var result = try compiled.session.execute(allocator, rt);
    defer result.deinit();

    // Extract gradients (graph layout: lora_a=[rank,input_dim], lora_b=[output_dim,rank]).
    const grad_a_graph = result.gradients.get("lora_a") orelse return error.MissingLoRAAGradient;
    const grad_b_graph = result.gradients.get("lora_b") orelse return error.MissingLoRABGradient;

    // Convert back to native layout.
    const grad_a_native = try allocator.alloc(f32, compiled.input_dim * compiled.rank);
    errdefer allocator.free(grad_a_native);
    transpose2DF32(grad_a_native, grad_a_graph, compiled.rank, compiled.input_dim);

    const grad_b_native = try allocator.alloc(f32, compiled.rank * compiled.output_dim);
    errdefer allocator.free(grad_b_native);
    transpose2DF32(grad_b_native, grad_b_graph, compiled.output_dim, compiled.rank);

    return .{ .grad_a = grad_a_native, .grad_b = grad_b_native };
}

test "graph bridge linear classifier one step updates head" {
    const allocator = std.testing.allocator;
    const native_compute = @import("../ops/native_compute.zig");
    var ws = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native_compute.NativeCompute.init(allocator, &ws, null);
    var cb = compute.computeBackend();

    var graph_bundle = try LinearClassifierGraph.init(allocator, 4, 3, 2);
    defer graph_bundle.deinit();
    var head = try LinearHead.initDeterministic(allocator, 3, 2);
    defer head.deinit();
    var opt_state = optimizers.OptimizerState.init(allocator);
    defer opt_state.deinit();
    const weight_before = try allocator.dupe(f32, head.weight);
    defer allocator.free(weight_before);
    const bias_before = try allocator.dupe(f32, head.bias);
    defer allocator.free(bias_before);

    const batch: ClassificationBatch = .{
        .features = &.{
            1.0, 0.0, 0.0,
            0.8, 0.1, 0.0,
            0.0, 1.0, 1.0,
            0.1, 0.9, 0.8,
        },
        .labels = &.{ 0, 0, 1, 1 },
        .rows = 4,
        .input_dim = 3,
        .num_labels = 2,
    };

    const summary = try trainLinearClassifierOneStep(
        allocator,
        &cb,
        &graph_bundle,
        &head,
        batch,
        .{ .adam = .{} },
        &opt_state,
        0.05,
    );
    try std.testing.expect(summary.weight_grad_l2 > 0);
    try std.testing.expect(summary.bias_grad_l2 > 0);
    try std.testing.expect(!std.mem.eql(f32, weight_before, head.weight));
    try std.testing.expect(!std.mem.eql(f32, bias_before, head.bias));
}

test "graph bridge mlp classifier one step updates head" {
    const allocator = std.testing.allocator;
    const native_compute = @import("../ops/native_compute.zig");
    var ws = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native_compute.NativeCompute.init(allocator, &ws, null);
    var cb = compute.computeBackend();

    var graph_bundle = try MlpClassifierGraph.init(allocator, 4, 3, 5, 2);
    defer graph_bundle.deinit();
    var head = MlpHead{
        .allocator = allocator,
        .dense_weight = try allocator.dupe(f32, &([_]f32{ 0.1, 0.2, 0.3 } ** 5)),
        .dense_bias = try allocator.dupe(f32, &([_]f32{0.0} ** 5)),
        .out_weight = try allocator.dupe(f32, &([_]f32{ 0.2, -0.1, 0.05, 0.03, 0.07, -0.2, 0.1, -0.05, 0.02, -0.03 })),
        .out_bias = try allocator.dupe(f32, &([_]f32{ 0.0, 0.0 })),
        .input_dim = 3,
        .hidden_dim = 5,
        .num_labels = 2,
    };
    defer head.deinit();
    var opt_state = optimizers.OptimizerState.init(allocator);
    defer opt_state.deinit();
    const dense_before = try allocator.dupe(f32, head.dense_weight);
    defer allocator.free(dense_before);
    const out_before = try allocator.dupe(f32, head.out_weight);
    defer allocator.free(out_before);

    const batch: ClassificationBatch = .{
        .features = &.{
            1.0, 0.0, 0.0,
            0.8, 0.1, 0.0,
            0.0, 1.0, 1.0,
            0.1, 0.9, 0.8,
        },
        .labels = &.{ 0, 0, 1, 1 },
        .rows = 4,
        .input_dim = 3,
        .num_labels = 2,
    };

    const summary = try trainMlpClassifierOneStep(
        allocator,
        &cb,
        &graph_bundle,
        &head,
        batch,
        .{ .adam = .{} },
        &opt_state,
        0.05,
    );
    try std.testing.expect(summary.dense_weight_grad_l2 > 0);
    try std.testing.expect(summary.out_weight_grad_l2 > 0);
    try std.testing.expect(!std.mem.eql(f32, dense_before, head.dense_weight));
    try std.testing.expect(!std.mem.eql(f32, out_before, head.out_weight));
}

test "graph bridge lora linear one step updates adapters" {
    const allocator = std.testing.allocator;
    const native_compute = @import("../ops/native_compute.zig");
    var ws = native_compute.WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute = native_compute.NativeCompute.init(allocator, &ws, null);
    var cb = compute.computeBackend();

    var graph_bundle = try LoRALinearGraph.init(allocator, 3, 4, 4, 2, 4.0);
    defer graph_bundle.deinit();
    var opt_state = optimizers.OptimizerState.init(allocator);
    defer opt_state.deinit();

    const base_weight = [_]f32{
        0.1,  0.2,   0.3,  0.4,
        0.2,  0.1,   0.0,  -0.1,
        0.05, -0.05, 0.15, 0.25,
        -0.1, 0.0,   0.1,  0.2,
    };
    var adapter_a = [_]f32{
        0.01,  -0.02,
        0.02,  0.03,
        -0.01, 0.04,
        0.05,  -0.03,
    };
    var adapter_b = [_]f32{
        0.0, 0.0, 0.0, 0.0,
        0.0, 0.0, 0.0, 0.0,
    };
    const b_before = adapter_b;
    const features = [_]f32{
        1.0,  0.0, 0.5,  -0.2,
        0.2,  0.8, -0.1, 0.3,
        -0.4, 0.1, 0.7,  0.9,
    };
    const target = [_]f32{
        0.3,  -0.2, 0.1,  0.4,
        0.0,  0.5,  -0.3, 0.2,
        -0.1, 0.2,  0.6,  -0.4,
    };

    const summary = try trainLoRALinearOneStep(
        allocator,
        &cb,
        &graph_bundle,
        .{ .adam = .{} },
        &opt_state,
        &base_weight,
        &adapter_a,
        &adapter_b,
        &features,
        &target,
        0.05,
    );
    try std.testing.expect(summary.lora_b_grad_l2 > 0);
    try std.testing.expect(!std.mem.eql(f32, &b_before, &adapter_b));
}
