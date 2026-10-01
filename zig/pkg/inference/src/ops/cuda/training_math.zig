// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Explicit CUDA 12.8 activation arithmetic for resident training. The small
//! checked-in PTX module needs only the existing CUDA driver at runtime.
const std = @import("std");
const driver = @import("driver.zig");
const Context = @import("context.zig").CudaContext;
const Buffer = @import("buffer.zig").DeviceBuffer;
const Scan = @import("ml").graph.node.PrefixScanAttrs;
const SpanFeatures = @import("ml").graph.node.FrozenSpanFeaturesAttrs;
const reduction = @import("reduction_plan.zig");
const ptx = @embedFile("artifacts/gliner25_training_math.ptx");

pub fn artifactHash() [32]u8 {
    var hash: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(ptx, &hash, .{});
    return hash;
}

pub const Module = struct {
    module: driver.CUmodule,
    gelu: driver.CUfunction,
    silu: driver.CUfunction,
    sigmoid: driver.CUfunction,
    scan_outer: driver.CUfunction,
    scan_inner: driver.CUfunction,
    scan_vector: driver.CUfunction,
    scan_vector_sums: driver.CUfunction,
    reduce_part: driver.CUfunction,
    reduce_finish: driver.CUfunction,
    span_features: driver.CUfunction,
    scatter: driver.CUfunction,
    embedding_scatter: driver.CUfunction,
    adamw: driver.CUfunction,
    norm_chunks: driver.CUfunction,
    norm_finish: driver.CUfunction,
    norm_total: driver.CUfunction,
    consistency_prepare: driver.CUfunction,
    consistency_boundary: driver.CUfunction,
    consistency_pair: driver.CUfunction,
    elementwise_vjp: driver.CUfunction,
    listwise_prepare: driver.CUfunction,
    listwise_vjp: driver.CUfunction,
    log_softmax_warp: driver.CUfunction,
    log_softmax_block: driver.CUfunction,
    record_prepare: driver.CUfunction,
    record_vjp: driver.CUfunction,
    record_mask: driver.CUfunction,
    shared_memory_per_block: u32,
    multiprocessors: u32,
    max_threads_per_multiprocessor: u32,

    pub fn init(ctx: *Context) !Module {
        try ctx.makeCurrent();
        if (try ctx.streamCaptureActive()) return error.ResidentTrainingExternalFrame;
        var module: driver.CUmodule = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleLoadDataEx(&module, ptx.ptr, 0, null, null));
        errdefer _ = ctx.driver.fns.cuModuleUnload(module);
        var gelu: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&gelu, module, "termite_gliner25_gelu_f32_cuda128"));
        var silu: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&silu, module, "termite_gliner25_silu_f32_cuda128"));
        const get_attribute = ctx.driver.fns.cuDeviceGetAttribute orelse return error.CudaSymbolMissing;
        var multiprocessors: c_int = 0;
        try ctx.driver.check(get_attribute(&multiprocessors, 16, ctx.device)); // MULTIPROCESSOR_COUNT
        if (multiprocessors <= 0 or multiprocessors > 1024) return error.CudaKernelUnavailable;
        var max_threads: c_int = 0;
        try ctx.driver.check(get_attribute(&max_threads, 39, ctx.device)); // MAX_THREADS_PER_MULTIPROCESSOR
        if (max_threads < 512 or max_threads > 4096) return error.CudaKernelUnavailable;
        var shared_memory: c_int = 0;
        try ctx.driver.check(get_attribute(&shared_memory, 8, ctx.device));
        if (shared_memory < 4096) return error.CudaKernelUnavailable;
        return .{ .norm_chunks = try function(ctx, module, "termite_gliner25_norm_chunks_v1"), .norm_finish = try function(ctx, module, "termite_gliner25_norm_finish_v1"), .norm_total = try function(ctx, module, "termite_gliner25_norm_total_v1"), .consistency_prepare = try function(ctx, module, "termite_gliner25_consistency_prepare_v1"), .consistency_boundary = try function(ctx, module, "termite_gliner25_consistency_boundary_v1"), .consistency_pair = try function(ctx, module, "termite_gliner25_consistency_pair_v1"), .adamw = try function(ctx, module, "termite_gliner25_adamw_pytorch_v1"), .log_softmax_warp = try function(ctx, module, "termite_gliner25_log_softmax_warp_f32"), .log_softmax_block = try function(ctx, module, "termite_gliner25_log_softmax_block_f32"), .record_prepare = try function(ctx, module, "termite_gliner25_record_target_exp_v1"), .record_vjp = try function(ctx, module, "termite_gliner25_record_logp_vjp_v1"), .record_mask = try function(ctx, module, "termite_gliner25_record_mask_vjp_v1"), .shared_memory_per_block = @intCast(shared_memory), .module = module, .gelu = gelu, .silu = silu, .sigmoid = try function(ctx, module, "termite_gliner25_sigmoid_f32_cuda128"), .scan_outer = try function(ctx, module, "termite_gliner25_scan_outer_v1"), .scan_inner = try function(ctx, module, "termite_gliner25_scan_inner_v1"), .scan_vector = try function(ctx, module, "termite_gliner25_scan_vector_v1"), .scan_vector_sums = try function(ctx, module, "termite_gliner25_scan_vector_sums_v1"), .reduce_part = try function(ctx, module, "termite_gliner25_reduce_part_v1"), .reduce_finish = try function(ctx, module, "termite_gliner25_reduce_finish_v1"), .span_features = try function(ctx, module, "termite_gliner25_frozen_span_features_v1"), .scatter = try function(ctx, module, "termite_gliner25_scatter_gather_v1"), .embedding_scatter = try function(ctx, module, "termite_gliner25_scatter_embedding_v1"), .elementwise_vjp = try function(ctx, module, "termite_gliner25_elementwise_vjp_v1"), .listwise_prepare = try function(ctx, module, "termite_gliner25_listwise_prepare_v1"), .listwise_vjp = try function(ctx, module, "termite_gliner25_listwise_vjp_v1"), .multiprocessors = @intCast(multiprocessors), .max_threads_per_multiprocessor = @intCast(max_threads) };
    }

    pub fn deinit(self: *Module, ctx: *Context) void {
        ctx.makeCurrent() catch {};
        if (self.module != null) _ = ctx.driver.fns.cuModuleUnload(self.module);
        self.module = null;
        self.gelu = null;
        self.silu = null;
        self.sigmoid = null;
        self.scan_outer = null;
        self.scan_inner = null;
        self.scan_vector = null;
        self.scan_vector_sums = null;
        self.reduce_part = null;
        self.reduce_finish = null;
        self.span_features = null;
        self.scatter = null;
        self.embedding_scatter = null;
        self.adamw = null;
        self.norm_chunks = null;
        self.norm_finish = null;
        self.norm_total = null;
        self.consistency_prepare = null;
        self.consistency_boundary = null;
        self.consistency_pair = null;
        self.elementwise_vjp = null;
        self.listwise_prepare = null;
        self.listwise_vjp = null;
        self.log_softmax_warp = null;
        self.log_softmax_block = null;
        self.record_prepare = null;
        self.record_vjp = null;
        self.record_mask = null;
    }

    pub fn reductionDevice(self: *const Module) reduction.Device {
        return .{ .multiprocessors = self.multiprocessors, .max_threads_per_multiprocessor = self.max_threads_per_multiprocessor };
    }

    pub fn launchReduction(self: *const Module, ctx: *Context, plan: reduction.Plan, output: Buffer, input: Buffer, scratch: Buffer) !void {
        var config = plan.config;
        const input_bytes = try std.math.mul(usize, try std.math.mul(usize, config.inputs, config.outputs), 4);
        const output_bytes = try std.math.mul(usize, config.outputs, 4);
        if (input.ptr == 0 or input.ptr % 16 != 0 or input.len < input_bytes or output.ptr == 0 or output.len < output_bytes or
            scratch.len < plan.scratch_bytes or (plan.scratch_bytes != 0 and scratch.ptr == 0)) return error.InvalidCudaState;
        var out = output.ptr;
        var in = input.ptr;
        var staging = scratch.ptr;
        var params = [_]?*anyopaque{ @ptrCast(&out), @ptrCast(&in), @ptrCast(&staging), @ptrCast(&config) };
        try ctx.makeCurrent();
        const part = self.reduce_part orelse return error.CudaKernelUnavailable;
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(part, plan.grid, config.ctas, 1, plan.block[0], plan.block[1], 1, 0, ctx.stream, &params, null));
        ctx.noteKernelLaunch();
        if (config.ctas > 1) {
            var finish = [_]?*anyopaque{ @ptrCast(&out), @ptrCast(&staging), @ptrCast(&config) };
            try launch(ctx, self.reduce_finish, plan.grid, plan.block, &finish);
        }
    }

    pub fn launchGelu(self: *const Module, ctx: *Context, output: Buffer, input: Buffer, upstream: Buffer, count: usize, backward: bool) !void {
        return launchActivation(self.gelu, ctx, output, input, upstream, count, backward);
    }

    pub fn launchSpanFeatures(self: *const Module, ctx: *Context, attrs: SpanFeatures, output: Buffer, lengths: Buffer, counts: Buffer) !void {
        var rows = try attrs.rows();
        var capacity = attrs.capacity;
        const row_bytes = try std.math.mul(usize, rows, 4);
        if (output.ptr == 0 or output.len < try std.math.mul(usize, row_bytes, 3) or
            lengths.ptr == 0 or lengths.len < row_bytes or
            counts.ptr == 0 or counts.len < try std.math.mul(usize, attrs.batch, 4)) return error.InvalidCudaState;
        var out = output.ptr;
        var lens = lengths.ptr;
        var ns = counts.ptr;
        var params = [_]?*anyopaque{ @ptrCast(&out), @ptrCast(&lens), @ptrCast(&ns), @ptrCast(&rows), @ptrCast(&capacity) };
        try ctx.makeCurrent();
        try launch(ctx, self.span_features, (rows + 255) / 256, .{ 256, 1 }, &params);
    }

    /// Backward consumes the saved forward output, not the original logits.
    pub fn launchSigmoid(self: *const Module, ctx: *Context, output: Buffer, input: Buffer, upstream: Buffer, count: usize, backward: bool) !void {
        return launchActivation(self.sigmoid, ctx, output, input, upstream, count, backward);
    }

    pub fn launchSilu(self: *const Module, ctx: *Context, output: Buffer, input: Buffer, upstream: Buffer, count: usize, backward: bool) !void {
        return launchActivation(self.silu, ctx, output, input, upstream, count, backward);
    }

    pub fn launchAdamW(self: *const Module, ctx: *Context, buffers: [4]Buffer, count: usize, step: u32, rate: f64, config: @import("ml").graph.optimizers.AdamWConfig64) !void {
        if (count == 0 or count > std.math.maxInt(u32) or step == 0 or step > 16777216) return error.InvalidCudaState;
        const bytes = try std.math.mul(usize, count, 4);
        for (buffers) |buffer| if (buffer.ptr == 0 or buffer.len < bytes) return error.InvalidCudaState;
        if (!std.math.isFinite(rate) or rate < 0 or !std.math.isFinite(config.beta1) or config.beta1 < 0 or config.beta1 >= 1 or !std.math.isFinite(config.beta2) or config.beta2 < 0 or config.beta2 >= 1 or !std.math.isFinite(config.eps) or config.eps <= 0 or !std.math.isFinite(config.weight_decay) or config.weight_decay < 0) return error.InvalidOptimizerState;
        var pointers = [_]driver.CUdeviceptr{ buffers[0].ptr, buffers[1].ptr, buffers[2].ptr, buffers[3].ptr };
        var n: u32 = @intCast(count);
        var adam_step = step;
        var scalars = [_]f64{ rate, config.beta1, config.beta2, config.eps, config.weight_decay };
        var params: [11]?*anyopaque = undefined;
        for (&pointers, 0..) |*value, i| params[i] = @ptrCast(value);
        params[4] = @ptrCast(&n);
        for (&scalars, 0..) |*value, i| params[5 + i] = @ptrCast(value);
        params[10] = @ptrCast(&adam_step);
        try ctx.makeCurrent();
        try launch(ctx, self.adamw, @intCast((@as(u64, n) + 255) / 256), .{ 256, 1 }, &params);
    }

    pub fn launchScatter(self: *const Module, ctx: *Context, output: Buffer, values: Buffer, rows: Buffer, offsets: Buffer, order: Buffer, group_count: usize, width: usize, value_rows: usize, output_rows: usize, reduction_profile: @import("ml").graph.node.ScatterReduction, padding_index: ?u32) !void {
        const elements = try std.math.mul(usize, group_count, width);
        const value_count = try std.math.mul(usize, value_rows, width);
        const output_count = try std.math.mul(usize, output_rows, width);
        if (group_count == 0 or width == 0 or group_count > @min(value_rows, output_rows) or
            elements > std.math.maxInt(u32) or value_count > std.math.maxInt(u32) or output_count > std.math.maxInt(u32)) return error.InvalidCudaState;
        const buffers = [_]Buffer{ output, values, rows, offsets, order };
        const counts = [_]usize{ output_count, value_count, group_count, try std.math.add(usize, group_count, 1), value_rows };
        for (buffers, counts) |buffer, count| if (buffer.ptr == 0 or buffer.len < try std.math.mul(usize, count, 4)) return error.InvalidCudaState;
        var ptrs = [_]driver.CUdeviceptr{ output.ptr, values.ptr, rows.ptr, offsets.ptr, order.ptr };
        var groups_u32: u32 = @intCast(group_count);
        var width_u32: u32 = @intCast(width);
        var params: [9]?*anyopaque = undefined;
        for (&ptrs, 0..) |*ptr, i| params[i] = @ptrCast(ptr);
        params[5] = @ptrCast(&groups_u32);
        params[6] = @ptrCast(&width_u32);
        const embedding = reduction_profile == .pytorch_embedding_v1;
        if ((!embedding and reduction_profile != .pytorch_gather_v1) or (padding_index != null and !embedding)) return error.InvalidCudaState;
        if (padding_index) |index| if (index >= output_rows or index > std.math.maxInt(i32)) return error.InvalidCudaState;
        var value_rows_u32: u32 = @intCast(value_rows);
        var padding_i32: i32 = if (padding_index) |index| @intCast(index) else -1;
        params[7] = @ptrCast(&value_rows_u32);
        params[8] = @ptrCast(&padding_i32);
        try ctx.makeCurrent();
        const per_block: u32 = if (embedding) 256 else 8;
        try launch(ctx, if (embedding) self.embedding_scatter else self.scatter, @intCast((@as(u64, elements) + per_block - 1) / per_block), .{ 256, 1 }, &params);
    }

    pub const NormPart = enum { chunks, finish, total };

    pub fn launchNorm(self: *const Module, ctx: *Context, comptime part: NormPart, output: Buffer, input: Buffer, n: usize) !void {
        if (n == 0 or n > std.math.maxInt(i32) or (part == .total and n > 16384)) return error.InvalidCudaState;
        const out_count = if (part == .chunks) (n + 65535) / 65536 else 1;
        if (input.ptr == 0 or input.len < try std.math.mul(usize, n, 4) or
            output.ptr == 0 or output.len < try std.math.mul(usize, out_count, 4)) return error.InvalidCudaState;
        var out = output.ptr;
        var in = input.ptr;
        var count: u32 = @intCast(n);
        var params = [_]?*anyopaque{ @ptrCast(&out), @ptrCast(&in), @ptrCast(&count) };
        const block: u32 = if (part == .total) blk: {
            const width: u32 = @intCast(if (n > 128) n / 4 else n);
            break :blk @min(512, @as(u32, 1) << @intCast(31 - @clz(width)));
        } else 512;
        const func = switch (part) {
            .chunks => self.norm_chunks,
            .finish => self.norm_finish,
            .total => self.norm_total,
        };
        try ctx.makeCurrent();
        try launch(ctx, func, @intCast(out_count), .{ block, 1 }, &params);
    }

    pub fn launchConsistencyPrepare(self: *const Module, ctx: *Context, buffers: [4]Buffer, n: usize) !void {
        if (n == 0 or n > std.math.maxInt(i32)) return error.InvalidCudaState;
        var ptrs: [4]driver.CUdeviceptr = undefined;
        var params: [5]?*anyopaque = undefined;
        for (buffers, 0..) |buffer, i| {
            if (buffer.ptr == 0 or buffer.len < try std.math.mul(usize, n, 4)) return error.InvalidCudaState;
            ptrs[i] = buffer.ptr;
            params[i] = @ptrCast(&ptrs[i]);
        }
        var count: u32 = @intCast(n);
        params[4] = @ptrCast(&count);
        try ctx.makeCurrent();
        try launch(ctx, self.consistency_prepare, @intCast((n + 255) / 256), .{ 256, 1 }, &params);
    }

    pub fn launchConsistencyBoundary(self: *const Module, ctx: *Context, buffers: [5]Buffer, m: usize, kept: usize, weight: f32) !void {
        if (m == 0 or m > std.math.maxInt(i32) or kept > m or !std.math.isFinite(weight) or weight < 0) return error.InvalidCudaState;
        var ptrs: [5]driver.CUdeviceptr = undefined;
        var params: [8]?*anyopaque = undefined;
        for (buffers, 0..) |buffer, i| {
            if (buffer.ptr == 0 or buffer.len < try std.math.mul(usize, m, 4)) return error.InvalidCudaState;
            ptrs[i] = buffer.ptr;
            params[i] = @ptrCast(&ptrs[i]);
        }
        var count: u32 = @intCast(m);
        var valid_count: u32 = @intCast(kept);
        var seed = weight;
        params[5] = @ptrCast(&count);
        params[6] = @ptrCast(&valid_count);
        params[7] = @ptrCast(&seed);
        try ctx.makeCurrent();
        try launch(ctx, self.consistency_boundary, @intCast((m + 255) / 256), .{ 256, 1 }, &params);
    }

    pub fn launchConsistencyPair(self: *const Module, ctx: *Context, buffers: [7]Buffer, n: usize, m: usize) !void {
        if (n == 0 or m == 0 or @max(n, m) > std.math.maxInt(i32)) return error.InvalidCudaState;
        const counts = [_]usize{ n, n, m, m, n, n, n };
        var ptrs: [7]driver.CUdeviceptr = undefined;
        var params: [8]?*anyopaque = undefined;
        for (buffers, counts, 0..) |buffer, count, i| {
            if (buffer.ptr == 0 or buffer.len < try std.math.mul(usize, count, 4)) return error.InvalidCudaState;
            ptrs[i] = buffer.ptr;
            params[i] = @ptrCast(&ptrs[i]);
        }
        var count: u32 = @intCast(n);
        params[7] = @ptrCast(&count);
        try ctx.makeCurrent();
        try launch(ctx, self.consistency_pair, @intCast((n + 255) / 256), .{ 256, 1 }, &params);
    }

    pub fn launchListwisePrepare(self: *const Module, ctx: *Context, buffers: [6]Buffer, rows: usize, cols: usize, queries: usize, candidate_major: bool) !void {
        const n = try std.math.mul(usize, rows, cols);
        if (n == 0 or n > std.math.maxInt(i32) or queries == 0 or rows % queries != 0) return error.InvalidCudaState;
        const counts = [_]usize{ n, n, n, n, try std.math.mul(usize, rows, 2), rows };
        var ptrs: [6]driver.CUdeviceptr = undefined;
        var params: [10]?*anyopaque = undefined;
        for (buffers, counts, 0..) |buffer, count, i| {
            if (buffer.ptr == 0 or buffer.len < try std.math.mul(usize, count, 4)) return error.InvalidCudaState;
            ptrs[i] = buffer.ptr;
            params[i] = @ptrCast(&ptrs[i]);
        }
        var scalars = [_]u32{ @intCast(rows), @intCast(cols), @intCast(queries), @intFromBool(candidate_major) };
        for (&scalars, 0..) |*value, i| params[6 + i] = @ptrCast(value);
        try ctx.makeCurrent();
        try launch(ctx, self.listwise_prepare, @intCast((n + 255) / 256), .{ 256, 1 }, &params);
    }

    pub fn launchListwiseVjp(self: *const Module, ctx: *Context, buffers: [7]Buffer, rows: usize, cols: usize, queries: usize, candidate_major: bool, canonical_output: bool) !void {
        const n = try std.math.mul(usize, rows, cols);
        if (n == 0 or n > std.math.maxInt(i32) or queries == 0 or rows % queries != 0) return error.InvalidCudaState;
        const counts = [_]usize{ n, n, n, try std.math.mul(usize, rows, 2), rows, rows, rows };
        var ptrs: [7]driver.CUdeviceptr = undefined;
        var params: [12]?*anyopaque = undefined;
        for (buffers, counts, 0..) |buffer, count, i| {
            if (buffer.ptr == 0 or buffer.len < try std.math.mul(usize, count, 4)) return error.InvalidCudaState;
            ptrs[i] = buffer.ptr;
            params[i] = @ptrCast(&ptrs[i]);
        }
        var scalars = [_]u32{ @intCast(rows), @intCast(cols), @intCast(queries), @intFromBool(candidate_major), @intFromBool(canonical_output) };
        for (&scalars, 0..) |*value, i| params[7 + i] = @ptrCast(value);
        try ctx.makeCurrent();
        try launch(ctx, self.listwise_vjp, @intCast((n + 255) / 256), .{ 256, 1 }, &params);
    }

    pub fn launchLogSoftmax(self: *const Module, ctx: *Context, output: Buffer, input: Buffer, logp: Buffer, rows: usize, width: usize, backward: bool) !void {
        const n = try std.math.mul(usize, rows, width);
        if (n == 0 or n > std.math.maxInt(i32)) return error.InvalidCudaState;
        const bytes = try std.math.mul(usize, n, 4);
        for ([_]Buffer{ output, input, logp }) |buffer| if (buffer.ptr == 0 or buffer.len < bytes) return error.InvalidCudaState;
        var pointers = [_]driver.CUdeviceptr{ output.ptr, input.ptr, logp.ptr };
        var scalars: [3]u32 = undefined;
        var block: [2]u32 = undefined;
        var grid: u32 = undefined;
        var kernel: driver.CUfunction = undefined;
        if (width <= @as(usize, if (backward) 1024 else 2048)) {
            const padded = try std.math.ceilPowerOfTwo(u32, @intCast(width));
            const lanes: u32 = @min(padded, 32);
            const batches: u32 = if (padded <= 128) 2 else 1;
            block = .{ lanes, 128 / lanes };
            grid = @intCast((rows + block[1] * batches - 1) / (block[1] * batches));
            scalars = .{ @intCast(rows), @intCast(width), @intFromBool(backward) };
            kernel = self.log_softmax_warp;
        } else {
            block = .{ if (backward) @max(32, try std.math.ceilPowerOfTwo(u32, @intCast(@min(width / 4, 1024) / 2))) else 1024, 1 };
            grid = @intCast(rows);
            const shared_rows = (self.shared_memory_per_block - block[0] / 32 * 4) / 4;
            const warp_reduce = width % 4 == 0 and width < shared_rows and output.ptr % 16 == 0 and input.ptr % 16 == 0 and logp.ptr % 16 == 0;
            scalars = .{ @intCast(width), @intFromBool(backward), @intFromBool(warp_reduce) };
            kernel = self.log_softmax_block;
        }
        var params: [6]?*anyopaque = undefined;
        for (&pointers, 0..) |*value, i| params[i] = @ptrCast(value);
        for (&scalars, 3..) |*value, i| params[i] = @ptrCast(value);
        try ctx.makeCurrent();
        try launch(ctx, kernel, grid, block, &params);
    }

    pub const RecordPart = enum { prepare, vjp, mask };
    pub fn launchRecord(self: *const Module, ctx: *Context, comptime part: RecordPart, buffers: []const Buffer, rows: usize, width: usize) !void {
        const n = try std.math.mul(usize, rows, width);
        if (n == 0 or n > std.math.maxInt(i32)) return error.InvalidCudaState;
        const buffer_count = switch (part) {
            .prepare => 4,
            .vjp => 7,
            .mask => 3,
        };
        const counts: [buffer_count]usize = switch (part) {
            .prepare => .{ n, n, n, rows },
            .vjp => .{ n, rows, n, n, rows, rows, rows },
            .mask => .{ n, n, rows },
        };
        if (buffers.len != counts.len) return error.InvalidCudaState;
        var pointers: [counts.len]driver.CUdeviceptr = undefined;
        var params: [counts.len + 2]?*anyopaque = undefined;
        for (buffers, counts, 0..) |buffer, count, i| {
            if (buffer.ptr == 0 or buffer.len < try std.math.mul(usize, count, 4)) return error.InvalidCudaState;
            pointers[i] = buffer.ptr;
            params[i] = @ptrCast(&pointers[i]);
        }
        var scalars = [_]u32{ @intCast(n), @intCast(width) };
        for (&scalars, counts.len..) |*value, i| params[i] = @ptrCast(value);
        try ctx.makeCurrent();
        try launch(ctx, switch (part) {
            .prepare => self.record_prepare,
            .vjp => self.record_vjp,
            .mask => self.record_mask,
        }, @intCast((n + 255) / 256), .{ 256, 1 }, &params);
    }

    pub fn launchElementwiseVjp(self: *const Module, ctx: *Context, output: Buffer, logits: Buffer, targets: Buffer, seeds: Buffer, count: usize, settings: @import("../elementwise_loss_math.zig").Settings) !void {
        if (count == 0 or count > std.math.maxInt(i32)) return error.InvalidCudaState;
        const bytes = try std.math.mul(usize, count, 4);
        for ([_]Buffer{ output, logits, targets, seeds }) |b| if (b.ptr == 0 or b.len < bytes) return error.InvalidCudaState;
        var ptrs = [_]driver.CUdeviceptr{ output.ptr, logits.ptr, targets.ptr, seeds.ptr };
        var n: u32 = @intCast(count);
        var kind: u32 = @intFromEnum(settings.kind);
        var scalars = [_]f32{ settings.gamma_positive, settings.gamma_negative, settings.clip, settings.negative_weight, settings.positive_backward_power, settings.negative_backward_power };
        var params: [12]?*anyopaque = undefined;
        for (&ptrs, 0..) |*ptr, i| params[i] = @ptrCast(ptr);
        params[4] = @ptrCast(&n);
        params[5] = @ptrCast(&kind);
        for (&scalars, 6..) |*scalar, i| params[i] = @ptrCast(scalar);
        try ctx.makeCurrent();
        try launch(ctx, self.elementwise_vjp, (n + 255) / 256, .{ 256, 1 }, &params);
    }

    fn function(ctx: *Context, module: driver.CUmodule, name: [:0]const u8) !driver.CUfunction {
        var result: driver.CUfunction = null;
        try ctx.driver.check(ctx.driver.fns.cuModuleGetFunction(&result, module, name.ptr));
        return result;
    }

    fn launch(ctx: *Context, kernel: driver.CUfunction, grid: u32, block: [2]u32, params: []?*anyopaque) !void {
        const f = kernel orelse return error.CudaKernelUnavailable;
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(f, grid, 1, 1, block[0], block[1], 1, 0, ctx.stream, params.ptr, null));
        ctx.noteKernelLaunch();
    }

    pub fn launchScan(self: *const Module, ctx: *Context, attrs: Scan, output: Buffer, input: Buffer, scratch: Buffer) !void {
        const count = try attrs.elements();
        const bytes = try std.math.mul(usize, count, 4);
        for ([_]Buffer{ output, input }) |buffer| if (buffer.len < bytes or buffer.ptr == 0) return error.InvalidCudaState;
        const scratch_bytes = try attrs.scratchBytes();
        if (scratch.len < scratch_bytes or (scratch_bytes != 0 and scratch.ptr == 0)) return error.InvalidCudaState;
        try ctx.makeCurrent();
        var out = output.ptr;
        var in = input.ptr;
        var b = attrs.batch;
        var width = attrs.width;
        var channels = attrs.channels;
        var reverse: u32 = @intFromBool(attrs.reverse);
        if (attrs.singleVector()) {
            if (self.multiprocessors == 0 or self.multiprocessors > 1024) return error.InvalidCudaState;
            const tiles: u32 = (width + 8191) / 8192;
            const grid = @min(tiles, self.multiprocessors);
            var iterations = (tiles + self.multiprocessors - 1) / self.multiprocessors;
            var sums = scratch.ptr;
            var sum_params = [_]?*anyopaque{ @ptrCast(&sums), @ptrCast(&in), @ptrCast(&width), @ptrCast(&iterations), @ptrCast(&reverse) };
            try launch(ctx, self.scan_vector_sums, grid, .{ 512, 1 }, &sum_params);
            var params = [_]?*anyopaque{ @ptrCast(&out), @ptrCast(&in), @ptrCast(&sums), @ptrCast(&width), @ptrCast(&iterations), @ptrCast(&reverse) };
            try launch(ctx, self.scan_vector, grid, .{ 512, 1 }, &params);
        } else if (attrs.reference == .inner) {
            const log_width: i32 = @intCast(std.math.log2_int_ceil(u32, width));
            const log_rows: i32 = @intCast(std.math.log2_int_ceil(u32, b));
            var log_x: u32 = @intCast(std.math.clamp(@divTrunc(9 + log_width - log_rows, 2), 4, 9));
            const tx: u32 = @as(u32, 1) << @intCast(log_x);
            const ty = 512 / tx;
            var params = [_]?*anyopaque{ @ptrCast(&out), @ptrCast(&in), @ptrCast(&b), @ptrCast(&width), @ptrCast(&log_x), @ptrCast(&reverse) };
            try launch(ctx, self.scan_inner, (b + ty - 1) / ty, .{ tx, ty }, &params);
        } else {
            var params = [_]?*anyopaque{ @ptrCast(&out), @ptrCast(&in), @ptrCast(&b), @ptrCast(&width), @ptrCast(&channels), @ptrCast(&reverse) };
            try launch(ctx, self.scan_outer, (b * channels + 255) / 256, .{ 256, 1 }, &params);
        }
    }

    fn launchActivation(kernel: driver.CUfunction, ctx: *Context, output: Buffer, input: Buffer, upstream: Buffer, count: usize, backward: bool) !void {
        const f = kernel orelse return error.CudaKernelUnavailable;
        const n = std.math.cast(u32, count) orelse return error.InvalidCudaState;
        const bytes = std.math.mul(usize, count, 4) catch return error.InvalidCudaState;
        for ([_]Buffer{ output, input, if (backward) upstream else input }) |buffer| {
            if (buffer.len < bytes or (bytes != 0 and buffer.ptr == 0)) return error.InvalidCudaState;
        }
        if (n == 0) return;
        var out_ptr = output.ptr;
        var in_ptr = input.ptr;
        var dy_ptr = if (backward) upstream.ptr else input.ptr;
        var count_u32 = n;
        var backward_u32: u32 = @intFromBool(backward);
        var parameters = [_]?*anyopaque{ @ptrCast(&out_ptr), @ptrCast(&in_ptr), @ptrCast(&dy_ptr), @ptrCast(&count_u32), @ptrCast(&backward_u32) };
        try ctx.makeCurrent();
        try ctx.driver.check(ctx.driver.fns.cuLaunchKernel(f, @intCast((@as(u64, n) + 255) / 256), 1, 1, 256, 1, 1, 0, ctx.stream, &parameters, null));
        ctx.noteKernelLaunch();
    }
};
