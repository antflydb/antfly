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

//! GPU-resident asymmetric MRL contrastive objective for EG2 training.

const std = @import("std");
const cuda_compute = @import("../ops/cuda/cuda_compute.zig");
const eg2_module = @import("../ops/cuda/embedding_gemma2.zig");
const buffer_mod = @import("../ops/cuda/buffer.zig");

const CudaCompute = cuda_compute.CudaCompute;
const DeviceBuffer = buffer_mod.DeviceBuffer;

pub const Request = struct {
    embeddings: DeviceBuffer,
    query_count: u32,
    candidate_count: u32,
    valid: []const u8,
    targets: []const u32,
    dimensions: []const u32,
    temperature: f32,
    loss_scale: f32 = 1,

    pub fn validate(self: Request) !void {
        const rows = std.math.add(u32, self.query_count, self.candidate_count) catch return error.InvalidEmbeddingGemma2Objective;
        if (self.query_count == 0 or self.query_count > 64 or self.candidate_count == 0 or self.candidate_count > 384 or
            rows > 384 or self.dimensions.len == 0 or self.dimensions.len > 4 or !std.math.isFinite(self.temperature) or self.temperature <= 0 or
            !std.math.isFinite(@as(f32, 1) / self.temperature) or
            !std.math.isFinite(self.loss_scale) or self.loss_scale <= 0) return error.InvalidEmbeddingGemma2Objective;
        if (self.valid.len != @as(usize, self.query_count) * self.candidate_count or self.targets.len != self.query_count)
            return error.InvalidEmbeddingGemma2Objective;
        var previous: u32 = 0;
        for (self.dimensions) |dimension| {
            if ((dimension != 128 and dimension != 256 and dimension != 512 and dimension != 768) or dimension <= previous) return error.InvalidEmbeddingGemma2Objective;
            previous = dimension;
        }
        for (self.targets, 0..) |target, query| {
            if (target >= self.candidate_count or self.valid[query * self.candidate_count + target] == 0)
                return error.InvalidEmbeddingGemma2Objective;
        }
        const embedding_elements = std.math.mul(usize, rows, 768) catch return error.InvalidEmbeddingGemma2Objective;
        const embedding_bytes = std.math.mul(usize, embedding_elements, @sizeOf(f32)) catch return error.InvalidEmbeddingGemma2Objective;
        if (self.embeddings.ptr == 0 or self.embeddings.len < embedding_bytes) return error.InvalidEmbeddingGemma2Objective;
    }
};

pub const Result = struct {
    compute: *CudaCompute,
    embedding_gradients: DeviceBuffer,
    losses: DeviceBuffer,
    loss_count: usize,

    pub fn deinit(self: *Result) void {
        self.compute.synchronizeEmbeddingGemma2Training() catch {};
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.losses);
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.embedding_gradients);
        self.* = undefined;
    }
};

pub const ResidentObjective = struct {
    compute: *CudaCompute,
    embeddings: DeviceBuffer,
    query_count: u32,
    candidate_count: u32,
    temperature: f32,
    valid: []u8,
    targets: []u32,
    evaluated: ?Result = null,
    captured_queries: u32 = 0,
    captured_candidates: u32 = 0,

    pub fn init(compute: *CudaCompute, query_count: u32, candidate_count: u32, temperature: f32, validity: []const u8, target_indexes: []const i32) !ResidentObjective {
        const rows = std.math.add(u32, query_count, candidate_count) catch return error.InvalidEmbeddingGemma2Objective;
        const valid = try std.heap.page_allocator.dupe(u8, validity);
        errdefer std.heap.page_allocator.free(valid);
        const targets = try std.heap.page_allocator.alloc(u32, target_indexes.len);
        errdefer std.heap.page_allocator.free(targets);
        for (target_indexes, 0..) |target, index| {
            if (target < 0) return error.InvalidEmbeddingGemma2Objective;
            targets[index] = @intCast(target);
        }
        var embeddings = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(try mul(rows, 768), @sizeOf(f32)));
        errdefer compute.releaseEmbeddingGemma2TrainingBuffer(&embeddings);
        const candidate = Request{ .embeddings = embeddings, .query_count = query_count, .candidate_count = candidate_count, .valid = valid, .targets = targets, .dimensions = &.{ 128, 256, 512, 768 }, .temperature = temperature };
        try candidate.validate();
        return .{ .compute = compute, .embeddings = embeddings, .query_count = query_count, .candidate_count = candidate_count, .temperature = temperature, .valid = valid, .targets = targets };
    }

    pub fn captureQueries(self: *ResidentObjective, start: u32, row_count: u32, source: DeviceBuffer) !void {
        if (self.evaluated != null or start != self.captured_queries or row_count == 0 or start + row_count > self.query_count)
            return error.InvalidEmbeddingGemma2ObjectiveCapture;
        try self.capture(start, row_count, source);
        self.captured_queries += row_count;
    }

    pub fn captureCandidates(self: *ResidentObjective, start: u32, row_count: u32, source: DeviceBuffer) !void {
        if (self.evaluated != null or start != self.captured_candidates or row_count == 0 or start + row_count > self.candidate_count)
            return error.InvalidEmbeddingGemma2ObjectiveCapture;
        try self.capture(self.query_count + start, row_count, source);
        self.captured_candidates += row_count;
    }

    fn capture(self: *ResidentObjective, destination_row: u32, row_count: u32, source: DeviceBuffer) !void {
        const byte_len = try mul(try mul(row_count, 768), @sizeOf(f32));
        if (source.ptr == 0 or source.len < byte_len) return error.InvalidEmbeddingGemma2ObjectiveCapture;
        const byte_offset = try mul(try mul(destination_row, 768), @sizeOf(f32));
        const destination = DeviceBuffer{ .ptr = self.embeddings.ptr + byte_offset, .len = byte_len };
        try destination.copyFromDevice(self.compute.embeddingGemma2TrainingContext(), source, byte_len);
    }

    /// Returns the mean weighted loss. This is the sole objective readback;
    /// embeddings, coefficients and gradients remain resident.
    pub fn evaluate(self: *ResidentObjective) !f32 {
        if (self.evaluated != null or self.captured_queries != self.query_count or self.captured_candidates != self.candidate_count)
            return error.InvalidEmbeddingGemma2ObjectiveCapture;
        var result = try run(self.compute, .{ .embeddings = self.embeddings, .query_count = self.query_count, .candidate_count = self.candidate_count, .valid = self.valid, .targets = self.targets, .dimensions = &.{ 128, 256, 512, 768 }, .temperature = self.temperature });
        errdefer result.deinit();
        const host_losses = try std.heap.page_allocator.alloc(f32, result.loss_count);
        defer std.heap.page_allocator.free(host_losses);
        try result.losses.copyToHost(self.compute.embeddingGemma2TrainingContext(), std.mem.sliceAsBytes(host_losses));
        try self.compute.synchronizeEmbeddingGemma2Training();
        var total: f64 = 0;
        for (host_losses) |loss| {
            if (!std.math.isFinite(loss)) return error.NonFiniteEmbeddingGemma2Loss;
            total += loss;
        }
        self.evaluated = result;
        return @floatCast(total / @as(f64, @floatFromInt(self.query_count)));
    }

    pub fn queryGradient(self: *const ResidentObjective, start: u32, rows: u32) !DeviceBuffer {
        if (start + rows > self.query_count) return error.InvalidEmbeddingGemma2ObjectiveGradient;
        return self.gradientView(start, rows);
    }

    pub fn candidateGradient(self: *const ResidentObjective, start: u32, rows: u32) !DeviceBuffer {
        if (start + rows > self.candidate_count) return error.InvalidEmbeddingGemma2ObjectiveGradient;
        return self.gradientView(self.query_count + start, rows);
    }

    fn gradientView(self: *const ResidentObjective, start: u32, rows: u32) !DeviceBuffer {
        const result = self.evaluated orelse return error.EmbeddingGemma2ObjectiveNotEvaluated;
        const offset = try mul(try mul(start, 768), @sizeOf(f32));
        const len = try mul(try mul(rows, 768), @sizeOf(f32));
        return .{ .ptr = result.embedding_gradients.ptr + offset, .len = len };
    }

    pub fn deinit(self: *ResidentObjective) void {
        if (self.evaluated) |*result| result.deinit();
        self.compute.synchronizeEmbeddingGemma2Training() catch {};
        self.compute.releaseEmbeddingGemma2TrainingBuffer(&self.embeddings);
        std.heap.page_allocator.free(self.targets);
        std.heap.page_allocator.free(self.valid);
        self.* = undefined;
    }
};

pub fn run(compute: *CudaCompute, request: Request) !Result {
    try request.validate();
    const module = try compute.embeddingGemma2Module();
    const rows: usize = @as(usize, request.query_count) + request.candidate_count;
    const full_elements = try mul(rows, 768);
    const largest_width: usize = request.dimensions[request.dimensions.len - 1];
    const normalized_elements = try mul(rows, largest_width);
    const coefficient_elements = try mul(request.query_count, request.candidate_count);
    const loss_count = try mul(request.query_count, request.dimensions.len);

    var normalized = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(normalized_elements, @sizeOf(f32)));
    defer compute.releaseEmbeddingGemma2TrainingBuffer(&normalized);
    var coefficients = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(coefficient_elements, @sizeOf(f32)));
    defer compute.releaseEmbeddingGemma2TrainingBuffer(&coefficients);
    var logits = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(coefficient_elements, @sizeOf(f32)));
    defer compute.releaseEmbeddingGemma2TrainingBuffer(&logits);
    var normalized_gradients = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(normalized_elements, @sizeOf(f32)));
    defer compute.releaseEmbeddingGemma2TrainingBuffer(&normalized_gradients);
    var valid = try compute.allocEmbeddingGemma2TrainingBuffer(request.valid.len);
    defer compute.releaseEmbeddingGemma2TrainingBuffer(&valid);
    var targets = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(request.targets.len, @sizeOf(u32)));
    defer compute.releaseEmbeddingGemma2TrainingBuffer(&targets);
    var result = Result{
        .compute = compute,
        .embedding_gradients = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(full_elements, @sizeOf(f32))),
        .losses = .{},
        .loss_count = loss_count,
    };
    errdefer result.deinit();
    result.losses = try compute.allocEmbeddingGemma2TrainingBuffer(try mul(loss_count, @sizeOf(f32)));

    // The first queued operation establishes the failure boundary. Every
    // subsequent error drains before deferred buffers can return to the cache.
    var queued = false;
    errdefer if (queued) compute.synchronizeEmbeddingGemma2Training() catch {};
    try valid.copyFromHost(compute.embeddingGemma2TrainingContext(), request.valid);
    queued = true;
    try targets.copyFromHost(compute.embeddingGemma2TrainingContext(), std.mem.sliceAsBytes(request.targets));
    try compute.fillEmbeddingGemma2TrainingF32(result.embedding_gradients, full_elements, 0);

    const objective_weight = 1.0 / @as(f32, @floatFromInt(request.dimensions.len));
    for (request.dimensions, 0..) |dimension, dimension_index| {
        const width: usize = dimension;
        try module.launchPrefixNormalize(compute.embeddingGemma2TrainingContext(), normalized, request.embeddings, .{ .rows = @intCast(rows), .width = dimension, .input_stride = 768 });
        const query_bytes = try mul(try mul(request.query_count, width), @sizeOf(f32));
        const candidate_elements = try mul(request.candidate_count, width);
        const candidate_bytes = try mul(candidate_elements, @sizeOf(f32));
        const query_normalized = DeviceBuffer{ .ptr = normalized.ptr, .len = query_bytes };
        const candidate_normalized = DeviceBuffer{ .ptr = normalized.ptr + query_bytes, .len = candidate_bytes };
        const query_gradient = DeviceBuffer{ .ptr = normalized_gradients.ptr, .len = query_bytes };
        const candidate_gradient = DeviceBuffer{ .ptr = normalized_gradients.ptr + query_bytes, .len = candidate_bytes };
        const loss_offset = try mul(try mul(dimension_index, request.query_count), @sizeOf(f32));
        const losses = DeviceBuffer{ .ptr = result.losses.ptr + loss_offset, .len = @as(usize, request.query_count) * @sizeOf(f32) };
        // Match eager cross_entropy's FP32 tensor boundaries: strict GEMM
        // logits, log_softmax coefficients scaled before the two GEMM VJPs.
        const cublas = try compute.embeddingGemma2TrainingBlas();
        const ctx = compute.embeddingGemma2TrainingContext();
        try cublas.linear(ctx.stream, @ptrFromInt(logits.ptr), @ptrFromInt(query_normalized.ptr), @ptrFromInt(candidate_normalized.ptr), request.query_count, width, request.candidate_count, false);
        module.noteCublasF32Matmul();
        try module.launchContrastiveCoefficients(
            ctx,
            coefficients,
            losses,
            logits,
            valid,
            targets,
            .{ .queries = request.query_count, .candidates = request.candidate_count, .width = dimension, .temperature = request.temperature, .objective_weight = objective_weight },
            request.loss_scale,
        );
        try cublas.matrix(ctx.stream, @ptrFromInt(query_gradient.ptr), @ptrFromInt(coefficients.ptr), @ptrFromInt(candidate_normalized.ptr), request.query_count, request.candidate_count, width, false, false, false);
        module.noteCublasF32Matmul();
        try cublas.matrix(ctx.stream, @ptrFromInt(candidate_gradient.ptr), @ptrFromInt(coefficients.ptr), @ptrFromInt(query_normalized.ptr), request.candidate_count, request.query_count, width, true, false, false);
        module.noteCublasF32Matmul();
        try module.launchPrefixNormalizeVjp(compute.embeddingGemma2TrainingContext(), result.embedding_gradients, normalized_gradients, request.embeddings, .{ .rows = @intCast(rows), .width = dimension, .input_stride = 768 }, 1);
    }
    return result;
}

fn mul(a: anytype, b: anytype) !usize {
    return std.math.mul(usize, @intCast(a), @intCast(b)) catch error.InvalidEmbeddingGemma2Objective;
}

test "EmbeddingGemma2 objective rejects invalid positives and MRL dimensions" {
    const buffer = DeviceBuffer{ .ptr = 1, .len = 3 * 768 * 4 };
    try (Request{ .embeddings = buffer, .query_count = 1, .candidate_count = 2, .valid = &.{ 1, 0 }, .targets = &.{0}, .dimensions = &.{ 128, 256, 512, 768 }, .temperature = 0.05 }).validate();
    try std.testing.expectError(error.InvalidEmbeddingGemma2Objective, (Request{ .embeddings = buffer, .query_count = 1, .candidate_count = 2, .valid = &.{ 1, 0 }, .targets = &.{1}, .dimensions = &.{768}, .temperature = 0.05 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2Objective, (Request{ .embeddings = buffer, .query_count = 1, .candidate_count = 2, .valid = &.{ 1, 0 }, .targets = &.{0}, .dimensions = &.{ 256, 128 }, .temperature = 0.05 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2Objective, (Request{ .embeddings = buffer, .query_count = 1, .candidate_count = 2, .valid = &.{ 1, 0 }, .targets = &.{0}, .dimensions = &.{127}, .temperature = 0.05 }).validate());
    try std.testing.expectError(error.InvalidEmbeddingGemma2Objective, (Request{ .embeddings = buffer, .query_count = 1, .candidate_count = 2, .valid = &.{ 1, 0 }, .targets = &.{0}, .dimensions = &.{768}, .temperature = 1e-40 }).validate());
}
