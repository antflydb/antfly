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

// Distributed training: data-parallel training across multiple devices.
//
// Orchestrates N independent TrainingLoops, one per device. Each device
// runs forward + backward on its own data shard, then gradients are
// averaged across all devices before the optimizer step. This is the
// standard "data-parallel" pattern (like PyTorch DDP / Horovod).
//
// In this first version, gradient averaging is simulated on the CPU
// (f32 arithmetic) rather than using device-native communication
// primitives. On Apple Silicon with unified memory this is essentially
// free; for networked multi-device setups, a future extension would
// wire in the collective_ops all-reduce.
//
// Usage:
//   var dist = DistributedTrainingLoop.init(allocator, .{ .num_devices = 2 });
//   defer dist.deinit();
//   // materialize weights into each device loop ...
//   const avg_loss = try dist.step(&graph, loss_node, &backends);

const std = @import("std");
const ml = @import("ml");
const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const Shape = ml.graph.Shape;
const optimizers = ml.graph.optimizers;
const ops_mod = @import("../ops/ops.zig");
const contracts = @import("backend_contracts.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const training = @import("training.zig");
const training_loop = @import("training_loop.zig");
const TrainingLoop = training_loop.TrainingLoop;
const TrainingWeightStore = training_loop.TrainingWeightStore;
const TrainingConfig = training_loop.TrainingConfig;

// ─── DistributedConfig ───────────────────────────────────────────────────────

pub const DistributedConfig = struct {
    num_devices: u32 = 2,
    optimizer: optimizers.Optimizer = .{ .adam = .{} },
    lr_schedule: optimizers.LearningRateSchedule = .{ .constant = 0.001 },
    grad_clip: optimizers.GradientClipConfig = .{ .none = {} },
    trainable_params: ?[]const []const u8 = null,
    checkpoint_config: ?ml.graph.checkpoint.CheckpointConfig = null,
    emit_checkpoint_analysis: bool = false,
};

// ─── DistributedTrainingLoop ─────────────────────────────────────────────────

pub const DistributedTrainingLoop = struct {
    config: DistributedConfig,
    device_loops: []TrainingLoop,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, config: DistributedConfig) !DistributedTrainingLoop {
        const device_loops = try allocator.alloc(TrainingLoop, config.num_devices);
        errdefer allocator.free(device_loops);

        const training_config = TrainingConfig{
            .optimizer = config.optimizer,
            .lr_schedule = config.lr_schedule,
            .grad_clip = config.grad_clip,
            .trainable_params = config.trainable_params,
            .checkpoint_config = config.checkpoint_config,
            .emit_checkpoint_analysis = config.emit_checkpoint_analysis,
        };

        for (device_loops) |*loop| {
            loop.* = TrainingLoop.init(allocator, training_config);
        }

        return .{
            .config = config,
            .device_loops = device_loops,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *DistributedTrainingLoop) void {
        for (self.device_loops) |*loop| {
            loop.deinit();
        }
        self.allocator.free(self.device_loops);
    }

    /// Run one distributed training step:
    /// 1. Each device: forward + backward on its data shard
    /// 2. All-reduce gradients (average across devices)
    /// 3. Each device: optimizer step with averaged gradients
    /// Returns the average loss across devices.
    pub fn step(
        self: *DistributedTrainingLoop,
        graph: *const Graph,
        loss_node: NodeId,
        backends: []const *const ComputeBackend,
    ) !f32 {
        const num_devices = self.device_loops.len;
        std.debug.assert(backends.len == num_devices);

        // Step 1: Run forward + backward on each device, collecting raw
        // gradients and losses.
        var device_results = try self.allocator.alloc(training.TrainStepResult, num_devices);
        defer {
            for (device_results) |*r| r.deinit();
            self.allocator.free(device_results);
        }

        var total_loss: f32 = 0.0;

        for (self.device_loops, backends, 0..) |*loop, cb, i| {
            // Upload current weights as runtime inputs.
            var rt = try loop.weight_store.buildRuntimeInputs(graph, cb, self.allocator);
            defer {
                var it = rt.iterator();
                while (it.next()) |entry| {
                    cb.free(entry.value_ptr.*);
                }
                rt.deinit(self.allocator);
            }

            // Forward + backward.
            device_results[i] = try training.trainStep(
                self.allocator,
                graph,
                loss_node,
                cb,
                rt,
                .{
                    .trainable_params = self.config.trainable_params,
                    .checkpoint_config = self.config.checkpoint_config,
                    .emit_checkpoint_analysis = self.config.emit_checkpoint_analysis,
                },
            );

            total_loss += device_results[i].loss;
        }

        const avg_loss = total_loss / @as(f32, @floatFromInt(num_devices));

        // Step 2: Average gradients across devices.
        // Collect the union of all parameter names from device 0 (all
        // devices should have the same set of parameters).
        var name_list = std.ArrayListUnmanaged([]const u8).empty;
        defer name_list.deinit(self.allocator);
        {
            var it = device_results[0].gradients.iterator();
            while (it.next()) |entry| {
                try name_list.append(self.allocator, entry.key_ptr.*);
            }
        }

        // For each parameter, average its gradient across devices.
        const inv_n: f32 = 1.0 / @as(f32, @floatFromInt(num_devices));

        for (name_list.items) |name| {
            // Start with device 0's gradient.
            const grad0 = device_results[0].gradients.get(name) orelse continue;
            const len = grad0.len;

            // Accumulate from devices 1..N.
            for (1..num_devices) |d| {
                const grad_d = device_results[d].gradients.get(name) orelse continue;
                std.debug.assert(grad_d.len == len);
                for (0..len) |j| {
                    grad0[j] += grad_d[j];
                }
            }

            // Scale by 1/N to get the average.
            for (0..len) |j| {
                grad0[j] *= inv_n;
            }
        }

        // Step 3: Apply averaged gradients on each device's loop.
        for (self.device_loops) |*loop| {
            loop.optimizer_state.step_count += 1;
            const current_lr = loop.config.lr_schedule.lr(loop.optimizer_state.step_count);

            for (name_list.items) |name| {
                const avg_grad = device_results[0].gradients.get(name) orelse continue;

                if (loop.weight_store.getWeight(name)) |param| {
                    try optimizers.step(
                        loop.config.optimizer,
                        &loop.optimizer_state,
                        current_lr,
                        name,
                        param,
                        avg_grad,
                    );
                }
            }
        }

        return avg_loss;
    }

    /// Sync weights across all devices by copying from device 0 to all others.
    /// Call this after initial weight materialization so all devices start
    /// with identical parameters.
    pub fn syncWeights(self: *DistributedTrainingLoop) !void {
        if (self.device_loops.len <= 1) return;

        const source = &self.device_loops[0].weight_store;
        var it = source.trainable_weights.iterator();
        while (it.next()) |entry| {
            const name = entry.key_ptr.*;
            const data = entry.value_ptr.*;
            for (self.device_loops[1..]) |*loop| {
                try loop.weight_store.setWeight(name, data);
            }
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const Builder = ml.graph.Builder;
const native_mod = @import("../ops/native_compute.zig");
const NativeCompute = native_mod.NativeCompute;
const WeightStore = native_mod.WeightStore;

/// Helper: build a simple graph  y = Wx + b, loss = sum(y)
fn buildTestGraph(allocator: std.mem.Allocator) !struct { graph: Graph, loss: NodeId, x: NodeId, w: NodeId, bias: NodeId } {
    var g = Graph.init(allocator);
    var bld = Builder.init(&g);

    const x = try bld.parameter("x", Shape.init(.f32, &.{ 2, 3 }));
    const w = try bld.parameter("w", Shape.init(.f32, &.{ 4, 3 }));
    const bias = try bld.parameter("bias", Shape.init(.f32, &.{4}));
    const y = try bld.linear(x, w, bias, 2, 3, 4);
    const loss = try bld.reduceSum(y, &.{ 0, 1 });
    try g.markOutput(loss);

    return .{ .graph = g, .loss = loss, .x = x, .w = w, .bias = bias };
}

test "2-device gradient averaging" {
    const allocator = std.testing.allocator;

    // Build graph.
    var built = try buildTestGraph(allocator);
    defer built.graph.deinit();

    // Set up 2 native backends.
    var ws_a = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var ws_b = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute_a = NativeCompute.init(allocator, &ws_a, null);
    var compute_b = NativeCompute.init(allocator, &ws_b, null);
    var cb_a = compute_a.computeBackend();
    var cb_b = compute_b.computeBackend();

    // Create distributed training loop with SGD (simplest optimizer).
    var dist = try DistributedTrainingLoop.init(allocator, .{
        .num_devices = 2,
        .optimizer = .{ .sgd = .{} },
        .lr_schedule = .{ .constant = 0.01 },
    });
    defer dist.deinit();

    // Materialize identical weights on both devices.
    const x_data = &[_]f32{ 1, 1, 1, 1, 1, 1 };
    const w_data = &[_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 };
    const bias_data = &[_]f32{ 0.01, 0.02, 0.03, 0.04 };

    for (dist.device_loops) |*loop| {
        _ = try loop.weight_store.materializeFromSlice("x", x_data);
        _ = try loop.weight_store.materializeFromSlice("w", w_data);
        _ = try loop.weight_store.materializeFromSlice("bias", bias_data);
    }

    // Run one distributed step.
    const backends = &[_]*const ComputeBackend{ &cb_a, &cb_b };
    const loss = try dist.step(&built.graph, built.loss, backends);

    // Loss should be positive.
    try std.testing.expect(loss > 0.0);

    // After the step, weights on both devices should be identical (same
    // averaged gradients applied with same optimizer).
    const w0 = dist.device_loops[0].weight_store.getWeight("w").?;
    const w1 = dist.device_loops[1].weight_store.getWeight("w").?;
    try std.testing.expectEqual(w0.len, w1.len);
    for (w0, w1) |a, b| {
        try std.testing.expectApproxEqAbs(a, b, 1e-6);
    }

    const b0 = dist.device_loops[0].weight_store.getWeight("bias").?;
    const b1 = dist.device_loops[1].weight_store.getWeight("bias").?;
    for (b0, b1) |a, b| {
        try std.testing.expectApproxEqAbs(a, b, 1e-6);
    }
}

test "distributed loss decreases" {
    const allocator = std.testing.allocator;

    // Build graph.
    var built = try buildTestGraph(allocator);
    defer built.graph.deinit();

    // Set up 2 native backends.
    var ws_a = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var ws_b = WeightStore{ .allocator = allocator, .resident_weights = .{}, .lazy_weights = .{} };
    var compute_a = NativeCompute.init(allocator, &ws_a, null);
    var compute_b = NativeCompute.init(allocator, &ws_b, null);
    var cb_a = compute_a.computeBackend();
    var cb_b = compute_b.computeBackend();

    // Create distributed training loop with SGD.
    var dist = try DistributedTrainingLoop.init(allocator, .{
        .num_devices = 2,
        .optimizer = .{ .sgd = .{} },
        .lr_schedule = .{ .constant = 0.01 },
    });
    defer dist.deinit();

    // Materialize identical weights on both devices.
    const x_data = &[_]f32{ 1, 1, 1, 1, 1, 1 };
    const w_data = &[_]f32{ 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.1, 1.2 };
    const bias_data = &[_]f32{ 0.01, 0.02, 0.03, 0.04 };

    for (dist.device_loops) |*loop| {
        _ = try loop.weight_store.materializeFromSlice("x", x_data);
        _ = try loop.weight_store.materializeFromSlice("w", w_data);
        _ = try loop.weight_store.materializeFromSlice("bias", bias_data);
    }

    const backends = &[_]*const ComputeBackend{ &cb_a, &cb_b };

    // Train several steps — loss should decrease.
    var prev_loss: f32 = std.math.inf(f32);
    for (0..5) |_| {
        const loss = try dist.step(&built.graph, built.loss, backends);
        try std.testing.expect(loss < prev_loss);
        prev_loss = loss;
    }
}
