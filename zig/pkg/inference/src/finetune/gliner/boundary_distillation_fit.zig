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

//! Fits an Antenna GLiNER neck in closed form before feature distillation.
//!
//! A neck that starts random (or as the identity, which is just as
//! uninformative about the teacher's space) makes a constant trunk output the
//! fastest early descent, and the trunk collapses. Fitting the neck first, as
//! the ridge regression from the student's routed states onto the teacher's,
//! gives the trunk an informative target from its first update. The fit runs
//! the frozen student's encoder forward (on the job's GPU when it trains on
//! Metal) over the first rows of the training set, with an identity neck so the
//! routed outputs are the trunk's states.
const std = @import("std");
const model = @import("../../models/gliner_boundary.zig");
const native = @import("../../ops/native_compute.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const data = @import("boundary_dataset.zig");
const encoder_graph = @import("boundary_encoder_graph.zig");
const backend_mod = @import("boundary_training_backend.zig");
const ops = @import("../../ops/ops.zig");
const ml = @import("ml").graph;
const interpreter = @import("../../graph/interpreter.zig");
const distillation = @import("boundary_distillation.zig");
const Control = @import("../../execution_control.zig").InferenceExecutionControl;
const Allocator = std.mem.Allocator;

pub const Options = struct {
    /// Rows from the start of the training set; zero disables the fit.
    rows: u32 = 0,
    /// Ridge penalty relative to the row count: (X'X + ridge * n * I) W = X'Y.
    ridge: f32 = 1e-2,
};

pub const Fitted = struct {
    allocator: Allocator,
    /// [hidden_out, hidden_in], as `gliner_neck.weight`.
    weight: []f32,
    bias: []f32,
    rows: usize,
    /// Fraction of the teacher states' variance the fitted map explains.
    r2: f64,

    pub fn deinit(self: *Fitted) void {
        self.allocator.free(self.weight);
        self.allocator.free(self.bias);
        self.* = undefined;
    }
};

/// Normal equations over [x, 1] -> y in f64.
pub const Accumulator = struct {
    allocator: Allocator,
    hidden: usize,
    xtx: []f64, // (h+1)^2
    xty: []f64, // (h+1) x h
    y_sum: []f64,
    y_squares: f64 = 0,
    rows: usize = 0,

    pub fn init(a: Allocator, hidden: usize) !Accumulator {
        const n = hidden + 1;
        const xtx = try a.alloc(f64, n * n);
        errdefer a.free(xtx);
        const xty = try a.alloc(f64, n * hidden);
        errdefer a.free(xty);
        const y_sum = try a.alloc(f64, hidden);
        @memset(xtx, 0);
        @memset(xty, 0);
        @memset(y_sum, 0);
        return .{ .allocator = a, .hidden = hidden, .xtx = xtx, .xty = xty, .y_sum = y_sum };
    }

    pub fn deinit(self: *Accumulator) void {
        self.allocator.free(self.xtx);
        self.allocator.free(self.xty);
        self.allocator.free(self.y_sum);
        self.* = undefined;
    }

    pub fn add(self: *Accumulator, groups: []const distillation.Group) !void {
        const h = self.hidden;
        const n = h + 1;
        var x = try self.allocator.alloc(f64, n);
        defer self.allocator.free(x);
        for (groups) |group| {
            if (group.student.len != group.valid.len * h or group.teacher.len != group.student.len) return error.BoundaryDistillationShapeMismatch;
            for (group.valid, 0..) |valid, row| {
                if (!valid) continue;
                for (x[0..h], group.student[row * h ..][0..h]) |*out, value| out.* = value;
                x[h] = 1;
                const y = group.teacher[row * h ..][0..h];
                for (0..n) |i| {
                    const xi = x[i];
                    // Upper triangle only; mirrored before the solve.
                    for (i..n) |j| self.xtx[i * n + j] += xi * x[j];
                    for (y, 0..) |value, j| self.xty[i * h + j] += xi * value;
                }
                for (y, self.y_sum) |value, *sum| {
                    sum.* += value;
                    self.y_squares += @as(f64, value) * value;
                }
                self.rows += 1;
            }
        }
    }

    pub fn solve(self: *const Accumulator, a: Allocator, ridge: f32) !Fitted {
        const h = self.hidden;
        const n = h + 1;
        if (self.rows < 2 or !std.math.isFinite(ridge) or ridge <= 0) return error.InvalidBoundaryNeckFit;
        const matrix = try a.alloc(f64, n * n);
        defer a.free(matrix);
        for (0..n) |i| for (0..n) |j| {
            matrix[i * n + j] = if (j >= i) self.xtx[i * n + j] else self.xtx[j * n + i];
        };
        const penalty = @as(f64, ridge) * @as(f64, @floatFromInt(self.rows));
        for (0..n) |i| matrix[i * n + i] += penalty;
        const solution = try a.dupe(f64, self.xty);
        defer a.free(solution);
        try choleskySolve(matrix, solution, n, h);
        // R^2 from the unregularized normal equations:
        // SSE = sum y^2 - 2 tr(W' X'Y) + tr(W' X'X W).
        var cross: f64 = 0;
        for (solution, self.xty) |w, b| cross += w * b;
        var quadratic: f64 = 0;
        for (0..h) |k| for (0..n) |i| {
            var row: f64 = 0;
            for (0..n) |j| row += (if (j >= i) self.xtx[i * n + j] else self.xtx[j * n + i]) * solution[j * h + k];
            quadratic += solution[i * h + k] * row;
        };
        const sse = self.y_squares - 2 * cross + quadratic;
        var total: f64 = self.y_squares;
        for (self.y_sum) |sum| total -= sum * sum / @as(f64, @floatFromInt(self.rows));
        const weight = try a.alloc(f32, h * h);
        errdefer a.free(weight);
        const bias = try a.alloc(f32, h);
        for (0..h) |out| {
            for (0..h) |in| weight[out * h + in] = @floatCast(solution[in * h + out]);
            bias[out] = @floatCast(solution[h * h + out]);
        }
        for (weight) |value| if (!std.math.isFinite(value)) return error.InvalidBoundaryNeckFit;
        return .{ .allocator = a, .weight = weight, .bias = bias, .rows = self.rows, .r2 = if (total > 0) 1 - sse / total else 0 };
    }
};

fn choleskySolve(matrix: []f64, rhs: []f64, n: usize, columns: usize) !void {
    for (0..n) |j| {
        var diagonal = matrix[j * n + j];
        for (0..j) |k| diagonal -= matrix[j * n + k] * matrix[j * n + k];
        if (!(diagonal > 0)) return error.InvalidBoundaryNeckFit;
        const pivot = @sqrt(diagonal);
        matrix[j * n + j] = pivot;
        for (j + 1..n) |i| {
            var value = matrix[i * n + j];
            for (0..j) |k| value -= matrix[i * n + k] * matrix[j * n + k];
            matrix[i * n + j] = value / pivot;
        }
    }
    for (0..columns) |c| {
        for (0..n) |i| {
            var value = rhs[i * columns + c];
            for (0..i) |k| value -= matrix[i * n + k] * rhs[k * columns + c];
            rhs[i * columns + c] = value / matrix[i * n + i];
        }
        var i = n;
        while (i > 0) {
            i -= 1;
            var value = rhs[i * columns + c];
            for (i + 1..n) |k| value -= matrix[k * n + i] * rhs[k * columns + c];
            rhs[i * columns + c] = value / matrix[i * n + i];
        }
    }
}

/// The neck must be the identity with a zero bias, so the routed outputs the
/// fit reads are the trunk's own states.
pub fn requireIdentity(weight: []const f32, bias: []const f32, hidden: usize) !void {
    if (weight.len != hidden * hidden or bias.len != hidden) return error.InvalidBoundaryNeckFit;
    for (0..hidden) |out| for (0..hidden) |in| {
        if (weight[out * hidden + in] != @as(f32, if (out == in) 1 else 0)) return error.BoundaryNeckFitNeedsIdentity;
    };
    for (bias) |value| if (value != 0) return error.BoundaryNeckFitNeedsIdentity;
}

pub const Source = struct {
    store: *native.WeightStore,
    config: model.Config,
    dataset: *const data.Dataset,
    tokenizer: @import("inference_tokenizer").Tokenizer,
    processor: processor.Options,
    batch_size: u32,
    /// Where the forward passes run; a Metal job fits on the GPU.
    execution: @import("../seeded_gradient_trainer.zig").Execution = .native,
    encoder_limits: encoder_graph.Limits = .{},
};

/// Uploaded weights, reused across batches (graphs differ per layout; names do not).
const WeightCache = struct {
    map: std.StringHashMapUnmanaged(ops.CT) = .empty,

    fn get(self: *WeightCache, a: Allocator, cb: *const ops.ComputeBackend, store: *native.WeightStore, name: []const u8, shape: ml.Shape) !ops.CT {
        if (self.map.get(name)) |value| return value;
        const entry = store.resident_weights.get(name) orelse return error.MissingBoundaryNeckFitWeight;
        const values = entry.tensor.asFloat32();
        if (values.len != shape.numElements().?) return error.BoundaryDistillationShapeMismatch;
        var dims: [8]i32 = undefined;
        for (shape.dims[0..shape.rank_], dims[0..shape.rank_]) |size, *out| out.* = @intCast(size);
        const value = try cb.fromFloat32Shape(values, dims[0..shape.rank_]);
        errdefer cb.free(value);
        // Names borrow from a batch's graph; the cache outlives it.
        const key = try a.dupe(u8, name);
        errdefer a.free(key);
        try self.map.put(a, key, value);
        return value;
    }

    pub fn deinit(self: *WeightCache, a: Allocator, cb: *const ops.ComputeBackend) void {
        var it = self.map.iterator();
        while (it.next()) |entry| {
            cb.free(entry.value_ptr.*);
            a.free(entry.key_ptr.*);
        }
        self.map.deinit(a);
    }
};

/// Runs the frozen student's encoder (with its identity neck) forward over the
/// first `options.rows` training rows and returns the fitted neck. Weights
/// resolve from `source.store`; no training graph, targets or optimizer run.
pub fn fit(a: Allocator, source: Source, teacher: distillation.Teacher, options: Options, control: ?Control) !Fitted {
    if (source.config.neck != .linear or options.rows == 0 or source.batch_size == 0) return error.InvalidBoundaryNeckFit;
    const hidden = source.config.encoder.hidden_size;
    var accumulator = try Accumulator.init(a, hidden);
    defer accumulator.deinit();
    var native_compute: ?native.NativeCompute = null;
    defer if (native_compute) |*value| value.deinit();
    var metal_compute: ?backend_mod.ScratchMetal = null;
    defer if (metal_compute) |*value| value.deinit();
    const cb = if (source.execution == .resident_metal) blk: {
        metal_compute = try backend_mod.ScratchMetal.init(a);
        break :blk metal_compute.?.computeBackend();
    } else blk: {
        native_compute = native.NativeCompute.init(a, source.store, null);
        break :blk native_compute.?.computeBackend();
    };
    var weights = WeightCache{};
    defer weights.deinit(a, &cb);
    const rows = @min(@as(usize, options.rows), source.dataset.index.len);
    var start: usize = 0;
    while (start < rows) : (start += source.batch_size) {
        if (control) |c| try c.check();
        const count = @min(@as(usize, source.batch_size), rows - start);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const samples = try scratch.alloc(data.Sample, count);
        var made: usize = 0;
        defer for (samples[0..made]) |*sample| sample.deinit();
        const items = try scratch.alloc(processor.Item, count);
        for (samples, items, 0..) |*sample, *item, offset| {
            sample.* = try source.dataset.sample(start + offset, control, null);
            made += 1;
            item.* = .{ .text = sample.row.text, .schema = &sample.schema };
        }
        var processor_options = source.processor;
        processor_options.control = control;
        var prepared = try processor.prepare(a, source.tokenizer, items, processor_options);
        defer prepared.deinit();
        var states = try teacher.encode(teacher.ptr, a, items, &prepared, control);
        defer states.deinit();
        const layout = try encoder_graph.layoutFromPrepared(&source.config, &prepared, source.encoder_limits);
        var graph = ml.Graph.init(a);
        defer graph.deinit();
        var builder = ml.Builder.init(&graph);
        var built = try encoder_graph.buildWithProfile(&builder, &source.config, layout, .eval, .materialized_v1, source.encoder_limits);
        defer built.deinit();
        const routed = [_]struct { distillation.Route, ml.NodeId, []const f32, []const bool }{
            .{ .text, built.nodes.text, states.text, prepared.text_word_mask },
            .{ .queries, built.nodes.queries, states.queries, prepared.query_marker_mask },
            .{ .classifications, built.nodes.classifications, states.classifications, prepared.cls_marker_mask },
            .{ .parents, built.nodes.parents, states.parents, prepared.parent_marker_mask },
        };
        for (routed) |entry| if (entry[1] != ml.null_node) try graph.markOutput(entry[1]);
        var inputs = std.ArrayListUnmanaged(interpreter.RuntimeInput).empty;
        var owned = std.ArrayListUnmanaged(ops.CT).empty;
        defer {
            for (owned.items) |value| cb.free(value);
            owned.deinit(a);
            inputs.deinit(a);
        }
        var bound = try encoder_graph.bindPrepared(a, &built, &source.config, &prepared, .{ .seed = 0, .micro_batch = start });
        defer bound.deinit();
        for (bound.bindings) |binding| {
            var dims: [8]i32 = undefined;
            for (binding.shape.dims[0..binding.shape.rank_], dims[0..binding.shape.rank_]) |size, *out| out.* = @intCast(size);
            const value = switch (binding.values) {
                .f32 => |values| try cb.fromFloat32Shape(values, dims[0..binding.shape.rank_]),
                .i32 => |values| (try cb.fromInt32Shape(values, dims[0..binding.shape.rank_])) orelse return error.UnsupportedBoundaryNeckFitBackend,
            };
            owned.append(a, value) catch |err| {
                cb.free(value);
                return err;
            };
            try inputs.append(a, .{ .node_id = binding.node, .value = value });
        }
        for (graph.parameters.items) |id| {
            const node = graph.node(id);
            const name = graph.parameterName(node);
            if (std.mem.startsWith(u8, name, "__")) continue;
            try inputs.append(a, .{ .node_id = id, .value = try weights.get(a, &cb, source.store, name, node.output_shape) });
        }
        var result = try interpreter.execute(a, &graph, &cb, .{ .runtime_inputs = inputs.items, .strict_integer_constants = true });
        defer result.deinit(&cb);
        var groups = std.ArrayListUnmanaged(distillation.Group).empty;
        var output: usize = 0;
        for (routed) |entry| {
            if (entry[1] == ml.null_node) continue;
            const values = try cb.toFloat32(result.outputs[output], scratch);
            output += 1;
            try groups.append(scratch, .{ .route = entry[0], .student = values, .teacher = entry[2], .valid = entry[3] });
        }
        try accumulator.add(groups.items);
    }
    return accumulator.solve(a, options.ridge);
}

test "neck fit recovers an exact affine map and reports its explained variance" {
    const a = std.testing.allocator;
    const hidden = 3;
    var accumulator = try Accumulator.init(a, hidden);
    defer accumulator.deinit();
    // y = M x + c for well-spread x.
    const m = [_]f32{ 2, 0, -1, 0.5, 1, 0, 0, -3, 1 };
    const c = [_]f32{ 1, -2, 0.5 };
    var students: [40 * hidden]f32 = undefined;
    var teachers: [40 * hidden]f32 = undefined;
    for (0..40) |row| {
        // Distinct frequencies keep the three inputs linearly independent.
        const frequencies = [_]f32{ 1.7, 2.9, 0.37 };
        for (0..hidden) |i| students[row * hidden + i] = @sin(@as(f32, @floatFromInt(row + 1)) * frequencies[i]) * 2;
        for (0..hidden) |out| {
            var value = c[out];
            for (0..hidden) |in| value += m[out * hidden + in] * students[row * hidden + in];
            teachers[row * hidden + out] = value;
        }
    }
    var valid: [40]bool = @splat(true);
    valid[7] = false;
    try accumulator.add(&.{.{ .route = .text, .student = &students, .teacher = &teachers, .valid = &valid }});
    try std.testing.expectEqual(@as(usize, 39), accumulator.rows);
    var fitted = try accumulator.solve(a, 1e-9);
    defer fitted.deinit();
    for (fitted.weight, m) |got, want| try std.testing.expectApproxEqAbs(want, got, 1e-4);
    for (fitted.bias, c) |got, want| try std.testing.expectApproxEqAbs(want, got, 1e-4);
    try std.testing.expectApproxEqAbs(@as(f64, 1), fitted.r2, 1e-6);
    // A heavy ridge shrinks the map and explains less.
    var shrunk = try accumulator.solve(a, 10);
    defer shrunk.deinit();
    try std.testing.expect(shrunk.r2 < 0.9 and shrunk.r2 > 0);
    try std.testing.expectError(error.InvalidBoundaryNeckFit, accumulator.solve(a, 0));
}

test "neck fit requires an identity starting neck" {
    try requireIdentity(&.{ 1, 0, 0, 1 }, &.{ 0, 0 }, 2);
    try std.testing.expectError(error.BoundaryNeckFitNeedsIdentity, requireIdentity(&.{ 1, 0.5, 0, 1 }, &.{ 0, 0 }, 2));
    try std.testing.expectError(error.BoundaryNeckFitNeedsIdentity, requireIdentity(&.{ 1, 0, 0, 1 }, &.{ 0, 1 }, 2));
}
