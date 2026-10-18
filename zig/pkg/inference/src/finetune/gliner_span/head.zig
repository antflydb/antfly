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

//! Frozen-feature GLiNER span head optimization. The graph differentiates the
//! selected final affine layer; the shared transactional trainer owns AdamW,
//! accumulation, resume, clipping and publication of every update.
const std = @import("std");
const ml = @import("ml").graph;
const ops = @import("../../ops/ops.zig");
const interpreter = @import("../../graph/interpreter.zig");
pub const controller = @import("../seeded_gradient_trainer.zig");
const A = std.mem.Allocator;

pub const Layout = struct { rows: u32, features: u32, outputs: u32, columns: u32 = 0 };
pub const Example = struct {
    features: []const f32,
    /// Optional frozen count-aware label projections [columns, outputs].
    projection: []const f32 = &.{},
    targets: []const f32,
    /// Invalid span cells never contribute to the objective or its VJP.
    mask: []const bool = &.{},
};
pub const Objective = enum { binary, exclusive };
pub const Loss = struct { value: f32, gradient: []f32 };

pub fn loss(a: A, objective: Objective, logits: []const f32, target: []const f32, mask: []const bool, positive_weight: f32) !Loss {
    if (logits.len == 0 or logits.len != target.len or (mask.len != 0 and mask.len != logits.len) or !std.math.isFinite(positive_weight) or positive_weight <= 0)
        return error.InvalidSpanTrainingTarget;
    const gradient = try a.alloc(f32, logits.len);
    errdefer a.free(gradient);
    @memset(gradient, 0);
    var value: f64 = 0;
    var count: usize = 0;
    if (objective == .exclusive) {
        var maximum: f32 = -std.math.inf(f32);
        var total_target: f64 = 0;
        for (logits, target, 0..) |z, t, i| {
            if (mask.len != 0 and !mask[i]) continue;
            if (!std.math.isFinite(z) or !std.math.isFinite(t) or t < 0 or t > 1) return error.InvalidSpanTrainingTarget;
            maximum = @max(maximum, z);
            total_target += t;
            count += 1;
        }
        if (count == 0 or @abs(total_target - 1) > 1e-5) return error.InvalidSpanTrainingTarget;
        var sum: f64 = 0;
        for (logits, 0..) |z, i| if (mask.len == 0 or mask[i]) {
            sum += @exp(@as(f64, z - maximum));
        };
        const normalizer = @as(f64, maximum) + @log(sum);
        for (logits, target, 0..) |z, t, i| {
            if (mask.len != 0 and !mask[i]) continue;
            gradient[i] = @floatCast(@exp(@as(f64, z) - normalizer) - t);
            value -= @as(f64, t) * (@as(f64, z) - normalizer);
        }
    } else {
        for (logits, target, 0..) |z, t, i| {
            if (mask.len != 0 and !mask[i]) continue;
            if (!std.math.isFinite(z) or !std.math.isFinite(t) or t < 0 or t > 1) return error.InvalidSpanTrainingTarget;
            const x: f64 = z;
            const y: f64 = t;
            const weight: f64 = 1 + (@as(f64, positive_weight) - 1) * y;
            const softplus = @max(-x, 0) + @log(1 + @exp(-@abs(x)));
            value += (1 - y) * x + weight * softplus;
            const sigmoid = if (x >= 0) 1 / (1 + @exp(-x)) else @exp(x) / (1 + @exp(x));
            gradient[i] = @floatCast(weight * sigmoid - @as(f64, positive_weight) * y);
            count += 1;
        }
        if (count == 0) return error.InvalidSpanTrainingTarget;
        value /= @floatFromInt(count);
        for (gradient) |*g| g.* /= @floatFromInt(count);
    }
    if (!std.math.isFinite(value)) return error.NonFiniteTrainingUpdate;
    return .{ .value = @floatCast(value), .gradient = gradient };
}

pub const Program = struct {
    graph: ml.Graph,
    gradients: ml.autodiff.GradientResult,
    x: ml.NodeId,
    projection: ml.NodeId,
    seed: ml.NodeId,
    output: ml.NodeId,
    wrt: [2]ml.NodeId,
    layout: Layout,

    pub fn init(a: A, prefix: []const u8, l: Layout) !Program {
        if (l.rows == 0 or l.features == 0 or l.outputs == 0) return error.InvalidSpanTrainingLayout;
        var graph = ml.Graph.init(a);
        errdefer graph.deinit();
        var b = ml.Builder.init(&graph);
        const x = try b.parameter("__span_features", ml.Shape.init(.f32, &.{ l.rows, l.features }));
        const w_name = try std.fmt.allocPrint(a, "{s}.weight", .{prefix});
        defer a.free(w_name);
        const bias_name = try std.fmt.allocPrint(a, "{s}.bias", .{prefix});
        defer a.free(bias_name);
        const w = try b.parameter(w_name, ml.Shape.init(.f32, &.{ l.outputs, l.features }));
        const bias = try b.parameter(bias_name, ml.Shape.init(.f32, &.{l.outputs}));
        const affine = try b.linear(x, w, bias, l.rows, l.features, l.outputs);
        var projection: ml.NodeId = ml.null_node;
        const output = if (l.columns == 0) affine else blk: {
            projection = try b.parameter("__span_projection", ml.Shape.init(.f32, &.{ l.columns, l.outputs }));
            break :blk try b.linearNoBias(affine, projection, l.rows, l.outputs, l.columns);
        };
        try graph.markOutput(output);
        const seed = try b.parameter("__span_cotangent", graph.node(output).output_shape);
        const wrt = [2]ml.NodeId{ w, bias };
        var gradients = try ml.autodiff.gradientWithSeeds(a, &graph, &.{.{ .output = output, .cotangent = seed }}, &wrt, .{ .require_all_gradients = true });
        errdefer gradients.deinit();
        gradients.graph.outputs.clearRetainingCapacity();
        for (gradients.param_grads) |id| try gradients.graph.markOutput(id);
        return .{ .graph = graph, .gradients = gradients, .x = x, .projection = projection, .seed = seed, .output = output, .wrt = wrt, .layout = l };
    }
    pub fn deinit(self: *Program) void {
        self.gradients.deinit();
        self.graph.deinit();
    }

    fn inputs(self: *Program, a: A, cb: *const ops.ComputeBackend, binding: controller.Bindings, example: Example) ![]interpreter.RuntimeInput {
        const l = self.layout;
        if (example.features.len != @as(usize, l.rows) * l.features or example.projection.len != @as(usize, l.columns) * l.outputs) return error.InvalidSpanTrainingLayout;
        const count: usize = if (l.columns == 0) 1 else 2;
        const runtime = try a.alloc(interpreter.RuntimeInput, binding.inputs.len + count);
        errdefer a.free(runtime);
        var built: usize = binding.inputs.len;
        errdefer for (runtime[binding.inputs.len..built]) |input| cb.free(input.value);
        @memcpy(runtime[0..binding.inputs.len], binding.inputs);
        runtime[binding.inputs.len] = .{ .node_id = self.x, .value = try cb.fromFloat32Shape(example.features, &.{ @intCast(l.rows), @intCast(l.features) }) };
        built += 1;
        if (l.columns != 0) runtime[binding.inputs.len + 1] = .{ .node_id = self.projection, .value = try cb.fromFloat32Shape(example.projection, &.{ @intCast(l.columns), @intCast(l.outputs) }) };
        return runtime;
    }

    pub fn predict(self: *Program, a: A, trainer: *controller.Trainer, example: Example) ![]f32 {
        const cb = trainer.owner.compute_backend;
        var binding = try trainer.bind(&self.graph, cb.execution_control);
        defer binding.deinit();
        const runtime = try self.inputs(a, cb, binding, example);
        defer a.free(runtime);
        defer for (runtime[binding.inputs.len..]) |input| cb.free(input.value);
        var result = try interpreter.execute(a, &self.graph, cb, .{ .runtime_inputs = runtime });
        defer result.deinit(cb);
        return cb.toFloat32(result.outputs[0], a);
    }

    pub fn step(self: *Program, a: A, trainer: *controller.Trainer, example: Example, objective: Objective, positive_weight: f32) !controller.Result {
        const cb = trainer.owner.compute_backend;
        var binding = try trainer.bind(&self.graph, cb.execution_control);
        var bound = true;
        defer if (bound) binding.deinit();
        const runtime = try self.inputs(a, cb, binding, example);
        defer a.free(runtime);
        defer for (runtime[binding.inputs.len..]) |input| cb.free(input.value);
        const logits = logits: {
            var result = try interpreter.execute(a, &self.graph, cb, .{ .runtime_inputs = runtime });
            defer result.deinit(cb);
            break :logits try cb.toFloat32(result.outputs[0], a);
        };
        defer a.free(logits);
        const measured = try loss(a, objective, logits, example.targets, example.mask, positive_weight);
        defer a.free(measured.gradient);
        const seed = try cb.fromFloat32Shape(measured.gradient, &.{ @intCast(self.layout.rows), @intCast(if (self.layout.columns == 0) self.layout.outputs else self.layout.columns) });
        defer cb.free(seed);
        const mapped = try a.alloc(interpreter.RuntimeInput, runtime.len + 1);
        defer a.free(mapped);
        for (runtime, mapped[0..runtime.len]) |input, *out| out.* = .{ .node_id = self.gradients.id_map[input.node_id], .value = input.value };
        mapped[runtime.len] = .{ .node_id = self.gradients.id_map[self.seed], .value = seed };
        var result = try interpreter.execute(a, &self.gradients.graph, cb, .{ .runtime_inputs = mapped });
        defer result.deinit(cb);
        var values: [2][]f32 = undefined;
        var built: usize = 0;
        defer for (values[0..built]) |value| a.free(value);
        var gradients: [2]controller.Gradient = undefined;
        for (self.wrt, result.outputs, &values, &gradients) |id, output, *value, *gradient| {
            value.* = try cb.toFloat32(output, a);
            built += 1;
            gradient.* = .{ .name = self.graph.parameterName(self.graph.node(id)), .values = value.* };
        }
        binding.deinit();
        bound = false;
        return trainer.submit(trainer.identity(), measured.value, &gradients, cb.execution_control);
    }
};

pub fn testLossGradients() !void {
    const a = std.testing.allocator;
    for ([_]Objective{ .binary, .exclusive }) |objective| {
        var logits = [_]f32{ -3, 1, 2, 1000 };
        const target = [_]f32{ 0, 1, 0, 0 };
        const mask = [_]bool{ true, true, true, false };
        const value = try loss(a, objective, &logits, &target, &mask, 2);
        defer a.free(value.gradient);
        try std.testing.expectEqual(@as(f32, 0), value.gradient[3]);
        for (0..3) |i| {
            const z = logits[i];
            logits[i] = z + 0.01;
            const plus = try loss(a, objective, &logits, &target, &mask, 2);
            defer a.free(plus.gradient);
            logits[i] = z - 0.01;
            const minus = try loss(a, objective, &logits, &target, &mask, 2);
            defer a.free(minus.gradient);
            logits[i] = z;
            try std.testing.expectApproxEqAbs(value.gradient[i], (plus.value - minus.value) / 0.02, 1e-4);
        }
    }
}

pub fn testUpdateAndResume() !void {
    const a = std.testing.allocator;
    const native = @import("../../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const parameters = [_]controller.Parameter{
        .{ .name = "probe.weight", .values = &.{ 0.2, -0.3 }, .dimensions = &.{ 1, 2 }, .group = 0 },
        .{ .name = "probe.bias", .values = &.{0.1}, .dimensions = &.{1}, .group = 0 },
    };
    const config = controller.Config{ .groups = &.{.{ .schedule = .{ .constant = 0.01 }, .optimizer = .{ .weight_decay = 0 } }} };
    var trainer = try controller.Trainer.init(a, &cb, &parameters, config);
    defer trainer.deinit();
    var program = try Program.init(a, "probe", .{ .rows = 2, .features = 2, .outputs = 1, .columns = 2 });
    defer program.deinit();
    const example = Example{ .features = &.{ 1, 2, -2, 1 }, .projection = &.{ 1, -1 }, .targets = &.{ 1, 0, 0, 1 } };
    const before = try program.predict(a, &trainer, example);
    defer a.free(before);
    const initial = try loss(a, .binary, before, example.targets, &.{}, 1);
    defer a.free(initial.gradient);
    for (0..3) |_| _ = try program.step(a, &trainer, example, .binary, 1);
    var temp = std.testing.tmpDir(.{});
    defer temp.cleanup();
    const filename = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/span.safetensors", .{temp.sub_path});
    defer a.free(filename);
    try trainer.save(filename, @splat(73), null);
    var restored = try controller.Trainer.initRestored(a, &cb, &parameters, config, filename, @splat(73), null);
    defer restored.deinit();
    _ = try program.step(a, &trainer, example, .binary, 1);
    _ = try program.step(a, &restored, example, .binary, 1);
    try std.testing.expectEqual(try trainer.stateFingerprint(@splat(73), null), try restored.stateFingerprint(@splat(73), null));
    const after = try program.predict(a, &trainer, example);
    defer a.free(after);
    const final = try loss(a, .binary, after, example.targets, &.{}, 1);
    defer a.free(final.gradient);
    try std.testing.expect(final.value < initial.value);
}

pub fn testDenseContractions() !void {
    const a = std.testing.allocator;
    const native = @import("../../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const M = 3;
    const N = 5;
    const K = 7;
    const left = [_]f32{ 1, -2, 0.5, 3, -4, 2, 1, -1, 2, 0.25, -3, 4, -2, 1, 2, -1, 0.75, 4, 1, -2, 3 };
    var right: [K * N]f32 = undefined;
    for (&right, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i % 11)) - 5)) / 4;
    for ([_]u8{ 0, 1 }) |lc| for ([_]u8{ 0, 1 }) |rc| {
        var lhs: [M * K]f32 = undefined;
        var rhs: [K * N]f32 = undefined;
        for (0..M) |i| for (0..K) |k| {
            lhs[if (lc == 1) i * K + k else k * M + i] = left[i * K + k];
        };
        for (0..K) |k| for (0..N) |j| {
            rhs[if (rc == 0) k * N + j else j * K + k] = right[k * N + j];
        };
        const lshape = [_]i64{ if (lc == 1) M else K, if (lc == 1) K else M };
        const rshape = [_]i64{ if (rc == 0) K else N, if (rc == 0) N else K };
        const x = try cb.fromFloat32Shape(&lhs, &.{ @intCast(lshape[0]), @intCast(lshape[1]) });
        defer cb.free(x);
        const y = try cb.fromFloat32Shape(&rhs, &.{ @intCast(rshape[0]), @intCast(rshape[1]) });
        defer cb.free(y);
        const z = try cb.primDotGeneral(x, y, &lshape, &rshape, &.{lc}, &.{rc}, &.{}, &.{});
        defer cb.free(z);
        const values = try cb.toFloat32(z, a);
        defer a.free(values);
        for (0..M) |i| for (0..N) |j| {
            var expected: f64 = 0;
            for (0..K) |k| expected += @as(f64, left[i * K + k]) * @as(f64, right[k * N + j]);
            try std.testing.expectApproxEqAbs(@as(f32, @floatCast(expected)), values[i * N + j], 1e-5);
        };
    };
}
