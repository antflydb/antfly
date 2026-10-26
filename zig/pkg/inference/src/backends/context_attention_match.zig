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

//! Native FP64 residual OMP / active NNLS / ridge QR with a bounded Metal
//! frontend. Only derived P/Y matrices and validation metrics are read back;
//! source KV and captured queries remain immutable device buffers. Fits are
//! private staging until the rounded, assembled whole head has passed v2.
const std = @import("std");
const shared = @import("metal_generated_kernels.zig");
const solver = @import("../runtime/kv/attention_match.zig");
const execution = @import("../execution_control.zig");
pub const Ref = shared.Ref;
pub const Buffer = shared.Buffer;
pub const acceptance_version: u32 = 2;
pub const recipe = "native-fp64-residual-omp-active-nnls-ridge-qr-whole-head-v2";
pub const validation_batch_rows: usize = 64;

fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.ContextStagingBudgetExceeded;
}
fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.ContextStagingBudgetExceeded;
}
fn rounded(n: usize) !usize {
    const page = std.heap.pageSize();
    return (try add(n, page - 1)) & ~(page - 1);
}

pub const FitRequest = struct {
    source_rows: u32 = 512,
    retained_rows: u32,
    head_dim: u32,
    kv_heads: u32 = 1,
    kv_head: u32 = 0,
    training_rows: u32,
    scale: f32,
    softcap: f32 = 0,
    /// Joint host + device staging, excluding inputs and the live cache.
    staging_budget_bytes: usize,
    max_sweeps: usize = 4096,
    nnls_tolerance: f64 = 1e-8,
    ridge: f64 = 1e-8,

    fn validate(self: FitRequest) !void {
        if (self.source_rows < 2 or self.source_rows > 512 or self.retained_rows == 0 or self.retained_rows >= self.source_rows or
            self.head_dim == 0 or self.head_dim > 512 or self.head_dim % 2 != 0 or self.kv_heads == 0 or self.kv_heads > 65535 or self.kv_head >= self.kv_heads or
            self.training_rows == 0 or self.training_rows > 4096 or self.max_sweeps == 0 or self.max_sweeps > 4096)
            return error.InvalidCompactionShape;
        if (@as(u64, self.source_rows) * self.kv_heads * self.head_dim > std.math.maxInt(u32)) return error.InvalidCompactionShape;
        if (!std.math.isFinite(self.scale) or self.scale <= 0 or !std.math.isFinite(self.softcap) or self.softcap < 0 or
            !std.math.isFinite(self.ridge) or self.ridge <= 0 or !std.math.isFinite(self.nnls_tolerance) or self.nnls_tolerance <= 0)
            return error.InvalidCompactionOptions;
    }
};

pub const FitAllocation = struct {
    derived_device_bytes: usize,
    derived_host_bytes: usize,
    solver_bytes: usize,
    candidate_bytes: usize,
    physical_bytes: usize,
};

pub fn fitAllocation(request: FitRequest) !FitAllocation {
    try request.validate();
    const elements = try mul(request.training_rows, try add(request.source_rows, request.head_dim));
    const device = try rounded(try mul(elements, 4));
    const host = try rounded(try mul(elements, 8));
    // Page slack for each independent native result/scratch allocation. The
    // owner allocator also measures actual malloc_size throughout the solve.
    const scratch = try add(try solver.matrixWorkspaceBytes(request.source_rows, request.retained_rows, request.training_rows, request.head_dim), 8 * std.heap.pageSize());
    const kv = try rounded(try mul(try mul(request.retained_rows, request.head_dim), 2));
    const row_buffer = try rounded(try mul(request.retained_rows, 4));
    const candidate = try add(try mul(kv, 2), try mul(row_buffer, 2)); // bias and gather indices
    return .{ .derived_device_bytes = device, .derived_host_bytes = host, .solver_bytes = scratch, .candidate_bytes = candidate, .physical_bytes = try add(try add(device, host), try add(scratch, candidate)) };
}

/// This is deliberately NOT an accepted Candidate. It carries no report bit
/// that could accidentally authorize a block-level or live publication.
pub const StagedFit = struct {
    rows: u32,
    head_dim: u32,
    keys: Buffer,
    values: Buffer,
    bias: Buffer,
    metrics: solver.Metrics,
    peak_staging_bytes: usize,

    pub fn deinit(self: *StagedFit) void {
        self.keys.deinit();
        self.values.deinit();
        self.bias.deinit();
        self.* = undefined;
    }
};

pub const HeadRequest = struct {
    source_rows: u32,
    candidate_rows: u32,
    head_dim: u32,
    validation_rows: u32,
    /// Every physical consumer contributes one equal contiguous stratum.
    validation_group_rows: u32,
    scale: f32,
    softcap: f32 = 0,
    max_mass_error: f64 = 0.05,
    max_output_rmse: f64 = 0.10,
    max_consumer_output_rmse: f64 = 0.10,
    staging_budget_bytes: usize,

    fn validate(self: HeadRequest) !void {
        if (self.source_rows == 0 or self.source_rows > 131072 or self.candidate_rows == 0 or self.candidate_rows > self.source_rows or
            self.head_dim == 0 or self.head_dim > 512 or self.head_dim % 2 != 0 or self.validation_rows == 0 or self.validation_rows > 4096 or
            self.validation_group_rows == 0 or self.validation_rows % self.validation_group_rows != 0)
            return error.InvalidCompactionShape;
        if (!std.math.isFinite(self.scale) or self.scale <= 0 or !std.math.isFinite(self.softcap) or self.softcap < 0)
            return error.InvalidCompactionOptions;
        for ([_]f64{ self.max_mass_error, self.max_output_rmse, self.max_consumer_output_rmse }) |x|
            if (!std.math.isFinite(x) or x < 0) return error.InvalidCompactionOptions;
    }
};

const QueryMetrics = extern struct {
    mass_relative: f32,
    error_squared: f32,
    target_squared: f32,
    invalid: u32,
};
comptime {
    std.debug.assert(@sizeOf(QueryMetrics) == 16);
}

pub const HeadMetrics = struct {
    version: u32 = acceptance_version,
    queries: u32 = 0,
    consumer_groups: u32 = 0,
    max_mass_error: f64 = 0,
    output_rmse: f64 = 0,
    max_consumer_output_rmse: f64 = 0,
};

pub fn validationAllocation(request: HeadRequest) !usize {
    try request.validate();
    const batch: usize = @min(validation_batch_rows, request.validation_rows);
    const probabilities = try rounded(try mul(try mul(batch, try add(request.source_rows, request.candidate_rows)), 4));
    const reports = try rounded(try mul(batch, @sizeOf(QueryMetrics)));
    // Include a dense f32 bias expansion for the packed candidate. Exact rows
    // have zero bias; the caller owns this one-head buffer during validation.
    return add(try add(probabilities, reports), try rounded(try mul(request.candidate_rows, 4)));
}

pub const Kernels = struct {
    core: shared.Kernels,
    execution_control: ?execution.InferenceExecutionControl = null,
    derived_readback_bytes: u64 = 0,
    status_readback_bytes: u64 = 0,
    last_metrics: ?HeadMetrics = null,

    pub fn initTracked(tracker: ?*shared.BufferAllocationTracker) !Kernels {
        return .{ .core = try shared.Kernels.initSourceTracked(@embedFile("context_attention_match.metal"), "context_attention_match", tracker) };
    }
    pub fn initBorrowedTracked(source: *const shared.Execution, tracker: ?*shared.BufferAllocationTracker) !Kernels {
        return .{ .core = try shared.Kernels.initBorrowedTracked(source, tracker) };
    }
    pub fn deinit(self: *Kernels) void {
        self.core.deinit();
    }
    fn check(self: *const Kernels) !void {
        if (self.core.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.execution_control) |control| try control.check();
    }
    fn checkNative(raw: ?*anyopaque) !void {
        const self: *Kernels = @ptrCast(@alignCast(raw.?));
        try self.check();
    }
    fn run(self: *Kernels, params: shared.Params, inputs: []const Ref, output: Ref, groups: usize) !void {
        try self.check();
        for (inputs) |ref| if (ref.offset % 4 != 0 or ref.bytes == 0) return error.MetalBufferRange;
        if (output.offset % 4 != 0 or output.bytes == 0) return error.MetalBufferRange;
        _ = try self.core.runThreadgroupsPipeline(self.core.pipeline, params, inputs, output, .{ groups, 1, 1 }, .{ 128, 1, 1 });
        try self.check();
    }

    pub fn fit(self: *Kernels, allocator: std.mem.Allocator, request: FitRequest, keys: Ref, values: Ref, training: Ref) !StagedFit {
        try self.check();
        const sizes = try fitAllocation(request);
        if (sizes.physical_bytes > request.staging_budget_bytes) return error.ContextStagingBudgetExceeded;
        const n: usize = request.source_rows;
        const m: usize = request.retained_rows;
        const q: usize = request.training_rows;
        const dim: usize = request.head_dim;
        const source_bytes = try mul(try mul(try mul(n, request.kv_heads), dim), 2);
        if (keys.bytes < source_bytes or values.bytes < source_bytes or training.bytes < try mul(try mul(q, dim), 4)) return error.MetalBufferRange;
        const count = try mul(q, try add(n, dim));
        const matrices = try allocator.alloc(f64, count);
        defer allocator.free(matrices);
        {
            var derived = try self.core.buffer(try mul(count, 4));
            defer derived.deinit();
            try self.run(.{ .op = 0, .n = request.source_rows, .dim = request.head_dim, .groups = request.kv_heads, .position = request.kv_head, .stride = request.training_rows, .scale = request.scale, .reserved = request.softcap }, &.{ keys, values, training }, derived.ref(), q);
            for (matrices, derived.floats()) |*dst, src| dst.* = src;
            self.derived_readback_bytes +|= count * 4;
        }
        var fitted = solver.fitMatrices(allocator, .{ .probabilities = matrices[0 .. q * n], .targets = matrices[q * n ..], .rows = n, .dim = dim, .query_rows = q }, .{
            .target_rows = m,
            .selection = .residual_omp,
            .mass_solver = .active_set,
            .ridge = request.ridge,
            .max_sweeps = request.max_sweeps,
            .nnls_tolerance = request.nnls_tolerance,
            .max_workspace_bytes = sizes.solver_bytes,
            .control = .{ .context = self, .check_fn = checkNative },
        }) catch |err| switch (err) {
            error.NonFiniteAttention, error.InvalidAttentionProbabilities => return error.NonFiniteCompactionInput,
            else => return err,
        };
        defer fitted.deinit();
        var candidate_keys = try self.core.buffer(m * dim * 2);
        errdefer candidate_keys.deinit();
        var candidate_values = try self.core.buffer(m * dim * 2);
        errdefer candidate_values.deinit();
        var candidate_bias = try self.core.buffer(m * 4);
        errdefer candidate_bias.deinit();
        var indices = try self.core.buffer(m * 4);
        defer indices.deinit();
        const gathered = std.mem.bytesAsSlice(u32, @as([]align(4) u8, @alignCast(indices.bytes[0 .. m * 4])));
        for (gathered, fitted.source_indices) |*dst, src| dst.* = @intCast(src);
        const half_values = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(candidate_values.bytes[0 .. m * dim * 2])));
        for (half_values, fitted.values) |*dst, src| {
            dst.* = @floatCast(src);
            if (!std.math.isFinite(dst.*)) return error.CompactionQualityRejected;
        }
        for (candidate_bias.floats(), fitted.bias) |*dst, src| {
            dst.* = @floatCast(src);
            if (!std.math.isFinite(dst.*)) return error.CompactionQualityRejected;
        }
        try self.run(.{ .op = 1, .n = @intCast(m * dim), .dim = request.head_dim, .groups = request.kv_heads, .position = request.kv_head, .stride = request.retained_rows }, &.{ keys, indices.ref() }, candidate_keys.ref(), (m * dim + 127) / 128);
        return .{ .rows = request.retained_rows, .head_dim = request.head_dim, .keys = candidate_keys, .values = candidate_values, .bias = candidate_bias, .metrics = fitted.metrics, .peak_staging_bytes = sizes.physical_bytes };
    }

    /// Copy only into private caller-owned staging, never a live generation.
    pub fn stage(self: *Kernels, fit_result: *const StagedFit, keys: Ref, values: Ref, bias: Ref) !void {
        if (fit_result.rows == 0 or fit_result.rows >= 512 or fit_result.head_dim == 0 or fit_result.head_dim > 512 or fit_result.head_dim % 2 != 0)
            return error.InvalidCompactionShape;
        const bytes = try mul(try mul(fit_result.rows, fit_result.head_dim), 2);
        if (fit_result.keys.logical_bytes < bytes or fit_result.values.logical_bytes < bytes or fit_result.bias.logical_bytes < try mul(fit_result.rows, 4))
            return error.MetalBufferRange;
        if (keys.bytes < bytes or values.bytes < bytes or bias.bytes < try mul(fit_result.rows, 4)) return error.MetalBufferRange;
        const inputs = [_]Ref{ fit_result.keys.ref(), fit_result.values.ref(), fit_result.bias.ref() };
        const outputs = [_]Ref{ keys, values, bias };
        for (outputs, 0..) |dst, i| {
            for (inputs) |src| if (dst.handle == src.handle) return error.AliasedContextStaging;
            for (outputs[0..i]) |other| if (dst.handle == other.handle) return error.AliasedContextStaging;
        }
        try self.run(.{ .op = 2, .n = fit_result.rows, .dim = fit_result.head_dim }, &.{ inputs[0], inputs[1], inputs[2], values, bias }, keys, (bytes / 2 + 127) / 128);
    }

    pub fn copyBias(self: *Kernels, source: Ref, destination: Ref, rows: u32) !void {
        if (rows == 0 or source.bytes < try mul(rows, 4) or destination.bytes < try mul(rows, 4)) return error.MetalBufferRange;
        if (source.handle == destination.handle) return error.AliasedContextStaging;
        try self.run(.{ .op = 4, .n = rows }, &.{source}, destination, (@as(usize, rows) + 127) / 128);
    }

    pub fn validateHead(self: *Kernels, request: HeadRequest, source_keys: Ref, source_values: Ref, queries: Ref, keys: Ref, values: Ref, dense_bias: Ref) !HeadMetrics {
        self.last_metrics = null;
        try self.check();
        const size = try validationAllocation(request);
        if (size > request.staging_budget_bytes) return error.ContextStagingBudgetExceeded;
        const dim: usize = request.head_dim;
        if (source_keys.bytes < try mul(try mul(request.source_rows, dim), 2) or source_values.bytes < try mul(try mul(request.source_rows, dim), 2) or
            keys.bytes < try mul(try mul(request.candidate_rows, dim), 2) or values.bytes < try mul(try mul(request.candidate_rows, dim), 2) or
            dense_bias.bytes < try mul(request.candidate_rows, 4) or queries.bytes < try mul(try mul(request.validation_rows, dim), 4)) return error.MetalBufferRange;
        const batch: usize = @min(validation_batch_rows, request.validation_rows);
        var probabilities = try self.core.buffer(try mul(try mul(batch, try add(request.source_rows, request.candidate_rows)), 4));
        defer probabilities.deinit();
        var reports = try self.core.buffer(batch * @sizeOf(QueryMetrics));
        defer reports.deinit();
        var result = HeadMetrics{ .queries = request.validation_rows, .consumer_groups = request.validation_rows / request.validation_group_rows };
        var error_norm: f64 = 0;
        var target_norm: f64 = 0;
        var group_error: f64 = 0;
        var group_target: f64 = 0;
        var offset: usize = 0;
        while (offset < request.validation_rows) {
            const take: usize = @min(batch, request.validation_rows - offset);
            try self.run(.{ .op = 3, .n = request.source_rows, .dim = request.head_dim, .groups = request.candidate_rows, .position = @intCast(take), .scale = request.scale, .reserved = request.softcap }, &.{ source_keys, source_values, try queries.slice(offset * dim * 4, take * dim * 4), keys, values, dense_bias, probabilities.ref() }, reports.ref(), take);
            const rows = std.mem.bytesAsSlice(QueryMetrics, @as([]align(@alignOf(QueryMetrics)) u8, @alignCast(reports.bytes[0 .. take * @sizeOf(QueryMetrics)])));
            self.status_readback_bytes +|= take * @sizeOf(QueryMetrics);
            for (rows, 0..) |row, i| {
                if (row.invalid != 0 or !std.math.isFinite(row.mass_relative) or !std.math.isFinite(row.error_squared) or !std.math.isFinite(row.target_squared) or row.error_squared < 0 or row.target_squared < 0)
                    return error.NonFiniteCompactionInput;
                result.max_mass_error = @max(result.max_mass_error, row.mass_relative);
                const err = @sqrt(@as(f64, row.error_squared));
                const target = @sqrt(@as(f64, row.target_squared));
                error_norm = std.math.hypot(error_norm, err);
                target_norm = std.math.hypot(target_norm, target);
                group_error = std.math.hypot(group_error, err);
                group_target = std.math.hypot(group_target, target);
                if ((offset + i + 1) % request.validation_group_rows == 0) {
                    result.max_consumer_output_rmse = @max(result.max_consumer_output_rmse, group_error / @max(group_target, 1e-12));
                    group_error = 0;
                    group_target = 0;
                }
            }
            offset += take;
        }
        result.output_rmse = error_norm / @max(target_norm, 1e-12);
        self.last_metrics = result;
        try self.check();
        if (result.max_mass_error > request.max_mass_error or result.output_rmse > request.max_output_rmse or result.max_consumer_output_rmse > request.max_consumer_output_rmse)
            return error.CompactionQualityRejected;
        return result;
    }
};

test "context native v2 admission includes host solve and bounded whole-head validation" {
    const fit = FitRequest{ .retained_rows = 128, .head_dim = 512, .training_rows = 768, .scale = 0.044194174, .staging_budget_bytes = 0 };
    const sizes = try fitAllocation(fit);
    try std.testing.expect(sizes.physical_bytes > sizes.derived_device_bytes + sizes.solver_bytes);
    const head = HeadRequest{ .source_rows = 8192, .candidate_rows = 4096, .head_dim = 512, .validation_rows = 256, .validation_group_rows = 32, .scale = 0.044194174, .staging_budget_bytes = 0 };
    const first = try validationAllocation(head);
    var more = head;
    more.validation_rows = 4096;
    try std.testing.expectEqual(first, try validationAllocation(more));
    more.validation_group_rows = 3;
    try std.testing.expectError(error.InvalidCompactionShape, validationAllocation(more));
    var excessive_stride = fit;
    excessive_stride.kv_heads = 65535;
    try std.testing.expectError(error.InvalidCompactionShape, fitAllocation(excessive_stride));
}

fn halves(buffer: Buffer) []f16 {
    return std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(buffer.bytes[0..buffer.logical_bytes])));
}

test "context native Metal v2 fits private matrices validates rounded full head and rejects corruption" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var tracker = shared.BufferAllocationTracker.init(null, 0);
    const before = tracker.snapshot();
    {
        var kernels = try Kernels.initTracked(&tracker);
        defer kernels.deinit();
        var stop = std.atomic.Value(bool).init(false);
        kernels.core.cancelled = &stop;
        var keys = try kernels.core.buffer(6 * 2 * 2);
        defer keys.deinit();
        var values = try kernels.core.buffer(keys.logical_bytes);
        defer values.deinit();
        const original_keys = [_]f16{ 0, 0, 0, 0, 0, 0, 0, 0, 1, -1, 0.5, 0.25 };
        const original_values = [_]f16{ 2, 2, 4, 4, 6, 6, 8, 8, 3, 3, 7, 7 };
        @memcpy(halves(keys), &original_keys);
        @memcpy(halves(values), &original_values);
        var training = try kernels.core.buffer(4 * 2 * 4);
        defer training.deinit();
        @memcpy(training.floats(), &[_]f32{ -1, 0, 0, 1, 1, -1, 2, 0.5 });
        var request = FitRequest{ .source_rows = 4, .retained_rows = 2, .head_dim = 2, .training_rows = 4, .scale = 0.5, .softcap = 0.7, .staging_budget_bytes = 64 * 1024 * 1024 };
        const sizes = try fitAllocation(request);
        request.staging_budget_bytes = sizes.physical_bytes - 1;
        try std.testing.expectError(error.ContextStagingBudgetExceeded, kernels.fit(std.testing.failing_allocator, request, keys.ref(), values.ref(), training.ref()));
        request.staging_budget_bytes += 1;
        stop.store(true, .release);
        try std.testing.expectError(error.Cancelled, kernels.fit(std.testing.failing_allocator, request, keys.ref(), values.ref(), training.ref()));
        stop.store(false, .release);
        var staged = try kernels.fit(tracker.allocator(), request, keys.ref(), values.ref(), training.ref());
        defer staged.deinit();
        try std.testing.expect(staged.metrics.selection_steps == 2);
        try std.testing.expectEqual(@as(u64, 4 * (4 + 2) * 4), kernels.derived_readback_bytes);
        var compact_keys = try kernels.core.buffer(4 * 2 * 2);
        defer compact_keys.deinit();
        var compact_values = try kernels.core.buffer(compact_keys.logical_bytes);
        defer compact_values.deinit();
        var bias = try kernels.core.buffer(4 * 4);
        defer bias.deinit();
        @memset(bias.bytes, 0);
        try kernels.stage(&staged, try compact_keys.ref().slice(0, 2 * 2 * 2), try compact_values.ref().slice(0, 2 * 2 * 2), try bias.ref().slice(0, 2 * 4));
        @memcpy(halves(compact_keys)[4..], original_keys[8..]);
        @memcpy(halves(compact_values)[4..], original_values[8..]);
        // Two physical consumer groups cross both a 64-query batch boundary
        // and the final partial batch. They must not be pooled for acceptance.
        var held = try kernels.core.buffer(130 * 2 * 4);
        defer held.deinit();
        var held64: [130 * 2]f64 = undefined;
        for (held.floats(), &held64, 0..) |*dst, *wide, i| {
            dst.* = @as(f32, @floatFromInt(i % 11)) * 0.1 - 0.5;
            wide.* = dst.*;
        }
        var head_request = HeadRequest{ .source_rows = 6, .candidate_rows = 4, .head_dim = 2, .validation_rows = 130, .validation_group_rows = 65, .scale = 0.5, .softcap = 0.7, .staging_budget_bytes = 64 * 1024 * 1024 };
        const actual = try kernels.validateHead(head_request, keys.ref(), values.ref(), held.ref(), compact_keys.ref(), compact_values.ref(), bias.ref());
        try std.testing.expectEqual(@as(u32, 2), actual.consumer_groups);
        try std.testing.expectEqual(@as(u64, 130 * 16), kernels.status_readback_bytes);
        var k64: [12]f64 = undefined;
        var v64: [12]f64 = undefined;
        var ck64: [8]f64 = undefined;
        var cv64: [8]f64 = undefined;
        var b64: [4]f64 = undefined;
        for (&k64, original_keys) |*dst, src| dst.* = src;
        for (&v64, original_values) |*dst, src| dst.* = src;
        for (&ck64, halves(compact_keys)) |*dst, src| dst.* = src;
        for (&cv64, halves(compact_values)) |*dst, src| dst.* = src;
        for (&b64, bias.floats()) |*dst, src| dst.* = src;
        const source = solver.Head{ .keys = &k64, .values = &v64, .rows = 6, .dim = 2, .scale = 0.5, .softcap = 0.7 };
        const compact = solver.Head{ .keys = &ck64, .values = &cv64, .bias = &b64, .rows = 4, .dim = 2, .scale = 0.5, .softcap = 0.7 };
        const expected = try solver.validationMetrics(std.testing.allocator, source, compact, &held64, .{ .target_rows = 4 });
        try std.testing.expectApproxEqAbs(expected.max_relative_mass_error, actual.max_mass_error, 2e-6);
        try std.testing.expectApproxEqAbs(expected.relative_output_rmse, actual.output_rmse, 2e-6);
        bias.floats()[0] += 1;
        try std.testing.expectError(error.CompactionQualityRejected, kernels.validateHead(head_request, keys.ref(), values.ref(), held.ref(), compact_keys.ref(), compact_values.ref(), bias.ref()));
        try std.testing.expect(kernels.last_metrics.?.max_mass_error > 0.05);
        head_request.staging_budget_bytes = (try validationAllocation(head_request)) - 1;
        try std.testing.expectError(error.ContextStagingBudgetExceeded, kernels.validateHead(head_request, keys.ref(), values.ref(), held.ref(), compact_keys.ref(), compact_values.ref(), bias.ref()));
        try std.testing.expect(kernels.last_metrics == null);
        try std.testing.expectEqualSlices(f16, &original_keys, halves(keys));
        try std.testing.expectEqualSlices(f16, &original_values, halves(values));
    }
    const after = tracker.snapshot();
    try std.testing.expectEqual(before.logical_live_bytes, after.logical_live_bytes);
    try std.testing.expectEqual(before.physical_live_bytes, after.physical_live_bytes);
    try std.testing.expectEqual(before.shared_backing_live_bytes, after.shared_backing_live_bytes);
}

test "context native Metal v2 rejects a weak consumer hidden by pooled output energy" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.initTracked(null);
    defer kernels.deinit();
    var keys = try kernels.core.buffer(2 * 2 * 2);
    defer keys.deinit();
    var values = try kernels.core.buffer(keys.logical_bytes);
    defer values.deinit();
    var candidate = try kernels.core.buffer(keys.logical_bytes);
    defer candidate.deinit();
    var bias = try kernels.core.buffer(2 * 4);
    defer bias.deinit();
    var queries = try kernels.core.buffer(2 * 2 * 4);
    defer queries.deinit();
    @memcpy(halves(keys), &[_]f16{ 1, 0, -1, 0 });
    @memcpy(halves(values), &[_]f16{ 10000, 10000, 1, 1 });
    @memcpy(halves(candidate), &[_]f16{ 10000, 10000, 2, 2 });
    @memset(bias.bytes, 0);
    @memcpy(queries.floats(), &[_]f32{ 20, 0, -20, 0 });
    const request = HeadRequest{ .source_rows = 2, .candidate_rows = 2, .head_dim = 2, .validation_rows = 2, .validation_group_rows = 1, .scale = 1, .staging_budget_bytes = 1024 * 1024 };
    try std.testing.expectError(error.CompactionQualityRejected, kernels.validateHead(request, keys.ref(), values.ref(), queries.ref(), keys.ref(), candidate.ref(), bias.ref()));
    const metrics = kernels.last_metrics.?;
    try std.testing.expect(metrics.max_mass_error < 1e-6);
    try std.testing.expect(metrics.output_rmse < 0.001);
    try std.testing.expect(metrics.max_consumer_output_rmse > 0.99);
}
