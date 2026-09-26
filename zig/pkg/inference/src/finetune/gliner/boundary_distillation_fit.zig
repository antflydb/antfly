// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Fits an Antenna GLiNER neck in closed form before feature distillation.
//!
//! A neck that starts random (or as the identity, which is just as
//! uninformative about the teacher's space) makes a constant trunk output the
//! fastest early descent, and the trunk collapses. Fitting the neck first, as
//! the ridge regression from the student's routed states onto the teacher's,
//! gives the trunk an informative target from its first update. The fit runs
//! the frozen student forward on the CPU over the first rows of the training
//! set, with an identity neck so the routed outputs are the trunk's states.
const std = @import("std");
const model = @import("../../models/gliner_boundary.zig");
const native = @import("../../ops/native_compute.zig");
const processor = @import("../../pipelines/gliner_boundary_processor.zig");
const schema_mod = @import("../../pipelines/extraction_schema.zig");
const targets = @import("boundary_targets.zig");
const data = @import("boundary_dataset.zig");
const step = @import("boundary_train_step.zig");
const distillation = @import("boundary_distillation.zig");
const regex = @import("../../pipelines/extraction_regex.zig");
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
    capacities: step.Capacities = .{},
    limits: step.Limits = .{},
    regex: regex.ContextOptions = .{},
};

/// Runs the frozen student over the first `options.rows` training rows and
/// returns the fitted neck. Every weight resolves from `source.store`.
pub fn fit(a: Allocator, source: Source, teacher: distillation.Teacher, options: Options, control: ?Control) !Fitted {
    if (source.config.neck != .linear or options.rows == 0 or source.batch_size == 0) return error.InvalidBoundaryNeckFit;
    const hidden = source.config.encoder.hidden_size;
    var accumulator = try Accumulator.init(a, hidden);
    defer accumulator.deinit();
    var compute = native.NativeCompute.init(a, source.store, null);
    defer compute.deinit();
    const cb = compute.computeBackend();
    const Collect = struct {
        fn observe(raw: *anyopaque, width: usize, groups: []const distillation.Group) !void {
            const self: *Accumulator = @ptrCast(@alignCast(raw));
            if (width != self.hidden) return error.BoundaryDistillationShapeMismatch;
            try self.add(groups);
        }
    };
    const rows = @min(@as(usize, options.rows), source.dataset.index.len);
    var start: usize = 0;
    var microbatch: u64 = 0;
    while (start < rows) : (start += source.batch_size) {
        if (control) |c| try c.check();
        const count = @min(@as(usize, source.batch_size), rows - start);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const samples = try scratch.alloc(data.Sample, count);
        var made: usize = 0;
        defer for (samples[0..made]) |*sample| sample.deinit();
        const schemas = try scratch.alloc(*const schema_mod.CompiledSchema, count);
        const annotations = try scratch.alloc(targets.Annotations, count);
        const items = try scratch.alloc(processor.Item, count);
        for (samples, schemas, annotations, items, 0..) |*sample, *compiled, *annotation, *item, offset| {
            sample.* = try source.dataset.sample(start + offset, control, null);
            made += 1;
            compiled.* = &sample.schema;
            annotation.* = sample.annotations;
            item.* = .{ .text = sample.row.text, .schema = &sample.schema };
        }
        var options_with_control = source.processor;
        options_with_control.control = control;
        var prepared = try processor.prepare(a, source.tokenizer, items, options_with_control);
        defer prepared.deinit();
        var states = try teacher.encode(teacher.ptr, a, items, &prepared, control);
        defer states.deinit();
        // Target compilation validates declared extraction validators, as in
        // the trainer's plans.
        var validators = regex.Context.init(a, source.regex);
        defer validators.deinit();
        validators.options.compile_options.control = control;
        validators.options.match_options.control = control;
        for (schemas) |compiled| {
            for (compiled.schema.entities) |entity| for (entity.validators) |validator| try regex.Context.validateCompile(&validators, validator);
            for (compiled.schema.structures) |structure| for (structure.fields) |field| for (field.validators) |validator| try regex.Context.validateCompile(&validators, validator);
        }
        var limits = source.limits;
        limits.targets.regex_context = &validators;
        limits.targets.validate_value_fn = regex.Context.validateValue;
        var plan = try step.buildWithObjectives(a, source.config, &prepared, schemas, source.capacities, .training, .materialized_v1, .retained_v1, .{}, .{ .heads = false, .distillation = true }, limits);
        defer plan.deinit();
        try plan.finalize(&.{}, .{ .allow_no_gradients = true });
        var result = try plan.run(&cb, &.{}, &prepared, schemas, annotations, .{
            .identity = .{ .binding = @splat(0), .optimizer_step = 0, .microbatch = microbatch },
            .replay = .{ .seed = 0, .micro_batch = microbatch },
            .progress = .{ .optimizer_step = 0, .total_optimizer_steps = 1 },
            .distillation = .{ .text = states.text, .queries = states.queries, .classifications = states.classifications, .parents = states.parents, .observer = .{ .ptr = &accumulator, .observe = Collect.observe } },
        }, control);
        result.deinit(&cb);
        microbatch += 1;
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
