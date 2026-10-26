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

//! Segmented f16 KV / FP32 attention correctness kernel. No production route
//! selects this kernel until per-model qualification and storage integration.
const std = @import("std");
const shared = @import("metal_generated_kernels.zig");
const flash = @import("context_flash.zig");
const mr = @import("metal_runtime.zig");
const views = @import("../runtime/kv/context_view.zig");
extern fn setenv(name: [*:0]const u8, value: [*:0]const u8, overwrite: c_int) c_int;
extern fn unsetenv(name: [*:0]const u8) c_int;
pub const Buffer = shared.Buffer;
pub const Ref = shared.Ref;

pub const split_decode_rows_per_split: u32 = 32;
pub const split_decode_max_splits: u32 = 32;
pub const split_decode_scratch_alignment: usize = @alignOf(f32);

pub const SplitDecode = struct {
    scratch: Ref,
    split_count: u32,
};

pub const DescriptorStorage = struct {
    spans: Ref,
    heads: Ref,
};

pub const SplitDecodePlan = struct {
    split_count: u32,
    /// Page-rounded allocation length. This is the exact physical allocation
    /// charged by Buffer.init, not just the portion addressed by the kernels.
    scratch_bytes: usize,
};

pub fn splitDecodeScratchBytes(query_heads: u32, head_dim: u32, split_count: u32) !usize {
    if (query_heads == 0 or head_dim == 0 or split_count == 0 or split_count > split_decode_max_splits)
        return error.InvalidContextAttention;
    var elements = try std.math.mul(usize, query_heads, split_count);
    elements = try std.math.mul(usize, elements, try std.math.add(usize, head_dim, 2));
    const raw_bytes = try std.math.mul(usize, elements, @sizeOf(f32));
    const page = std.heap.pageSize();
    return (try std.math.add(usize, raw_bytes, page - 1)) & ~(page - 1);
}

pub fn splitDecodePlan(max_kv_rows: u32, query_heads: u32, head_dim: u32) !SplitDecodePlan {
    if (max_kv_rows == 0) return error.InvalidContextAttention;
    const needed = 1 + (max_kv_rows - 1) / split_decode_rows_per_split;
    const split_count = @min(needed, split_decode_max_splits);
    return .{
        .split_count = split_count,
        .scratch_bytes = try splitDecodeScratchBytes(query_heads, head_dim, split_count),
    };
}

pub const Request = struct {
    view: views.View,
    current_epoch: u64,
    query_start: u32,
    query_rows: u32,
    query_heads: u32,
    sliding_window: u32 = 0,
    /// Rolling global attention protects the exact prefix in every query.
    keep_prefix_tokens: u32 = 0,
    scale: f32,
    logit_softcap: f32 = 0,
    /// Opt-in decode-only route. The scratch allocation and fixed split count
    /// must be admitted and owned by the caller before dispatch.
    split_decode: ?SplitDecode = null,
    /// Qualification-only tiled ABI2 prefill route. Its pipelines must be
    /// created by the owning session before any request reaches this method.
    flash_prefill: bool = false,
    /// Required for framed dispatch so descriptor bytes stay immutable through
    /// the decoder owner's terminal wait.
    descriptors: ?DescriptorStorage = null,
};

pub const Kernels = struct {
    core: shared.Kernels,
    status: Buffer,
    flash_prefill: ?flash.Kernels = null,
    owns_flash_prefill: bool = true,
    status_readback_bytes: u64 = 0,
    standalone_dispatches: u64 = 0,
    framed_dispatches: u64 = 0,

    pub fn init() !Kernels {
        return initTracked(false, null);
    }

    pub fn initFlash() !Kernels {
        return initTracked(true, null);
    }

    pub fn initTracked(enable_flash_prefill: bool, tracker: ?*shared.BufferAllocationTracker) !Kernels {
        var core = try shared.Kernels.initSourceTracked(@embedFile("context_attention.metal"), "context_attention", tracker);
        errdefer core.deinit();
        core.batching_enabled = true;
        var status = try core.buffer(4);
        errdefer status.deinit();
        return .{
            .core = core,
            .status = status,
            .flash_prefill = if (enable_flash_prefill) try flash.Kernels.init() else null,
        };
    }
    pub fn deinit(self: *Kernels) void {
        self.core.cancelBatch();
        if (self.owns_flash_prefill) if (self.flash_prefill) |*kernels| kernels.deinit();
        self.status.deinit();
        self.core.deinit();
    }

    pub fn initBorrowedTracked(execution: *const shared.Execution, flash_execution: ?*const flash.Execution, tracker: ?*shared.BufferAllocationTracker) !Kernels {
        var core = try shared.Kernels.initBorrowedTracked(execution, tracker);
        errdefer core.deinit();
        core.batching_enabled = true;
        return .{
            .core = core,
            .status = try core.buffer(4),
            .flash_prefill = if (flash_execution) |value| value.* else null,
            .owns_flash_prefill = false,
        };
    }

    /// Input buffers are immutable until the synchronous dispatch completes.
    /// Descriptor upload is bounded metadata; KV stays on the device. An
    /// external producer must finish its frame before transferring buffers
    /// to this standalone runtime, whose queue is independently owned.
    pub fn attention(self: *Kernels, request: Request, queries: Ref, keys: Ref, values: Ref, bias: Ref, output: Ref) !void {
        try self.attentionImpl(null, request, queries, keys, values, bias, output);
        self.standalone_dispatches +|= 1;
    }

    pub fn attentionOnActiveRuntime(self: *Kernels, owner_runtime: *mr.RawMetalDecodeRuntime, request: Request, queries: Ref, keys: Ref, values: Ref, bias: Ref, output: Ref) !void {
        try self.attentionImpl(owner_runtime, request, queries, keys, values, bias, output);
        self.framed_dispatches +|= 1;
    }

    fn attentionImpl(self: *Kernels, owner_runtime: ?*mr.RawMetalDecodeRuntime, request: Request, queries: Ref, keys: Ref, values: Ref, bias: Ref, output: Ref) !void {
        for ([_]Ref{ queries, keys, values, bias, output }) |ref| {
            if (ref.offset % 4 != 0) return error.MetalBufferRange;
        }
        for ([_]Ref{ queries, keys, values, bias }) |input|
            if (input.handle == output.handle) return error.AliasedContextStaging;
        const view = request.view;
        _ = try view.validate();
        if (request.current_epoch != view.epoch) return error.StaleContextView;
        if (request.query_rows == 0 or request.query_heads == 0 or request.query_heads % view.kv_heads != 0 or
            !std.math.isFinite(request.scale) or request.scale <= 0 or
            !std.math.isFinite(request.logit_softcap) or request.logit_softcap < 0) return error.InvalidContextAttention;
        const end = std.math.add(u32, request.query_start, request.query_rows) catch return error.InvalidContextAttention;
        if (end > view.logical_tokens) return error.InvalidContextAttention;
        if (request.keep_prefix_tokens > 0 and (request.sliding_window == 0 or
            !view.coversExact(0, @min(request.keep_prefix_tokens, end)))) return error.MissingContextHistory;
        if (request.sliding_window == 0) {
            // Global attention must represent every original position. A
            // compacted interval may represent many rows, but gaps silently
            // turn exact/global attention into an unrequested rolling policy.
            for (0..view.kv_heads) |head| {
                var covered: u64 = 0;
                for (view.headSpans(head)) |span| {
                    if (span.logical_start != covered) return error.MissingContextHistory;
                    covered += span.logical_count;
                }
                if (covered < end) return error.MissingContextHistory;
            }
        }
        const required_start = if (request.sliding_window > 0) request.query_start -| (request.sliding_window - 1) else request.query_start;
        if (!view.coversExact(required_start, end)) return error.MissingContextHistory;
        // No candidate summary can be used to recompute a query inside its
        // own source block; its causal subdivisions have been discarded.
        for (view.spans) |span| if (span.kind == .compacted and
            request.query_start < @as(u64, span.logical_start) + span.logical_count) return error.InvalidContextAttention;
        if (request.sliding_window > 0) for (view.spans) |span| {
            if (span.kind == .compacted) return error.InvalidContextAttention;
        };
        const query_elements = try std.math.mul(usize, try std.math.mul(usize, request.query_rows, request.query_heads), view.head_dim);
        const kv_elements = std.math.cast(usize, view.kvElements()) orelse return error.InvalidContextAttention;
        const bias_elements = std.math.cast(usize, view.biasElements()) orelse return error.InvalidContextAttention;
        const bias_bytes = try std.math.mul(usize, bias_elements, 4);
        if (queries.bytes < try std.math.mul(usize, query_elements, 4) or
            output.bytes < try std.math.mul(usize, query_elements, 4) or
            keys.bytes < try std.math.mul(usize, kv_elements, 2) or
            values.bytes < try std.math.mul(usize, kv_elements, 2) or bias.bytes < bias_bytes)
            return error.MetalBufferRange;
        // MSL indexes rows/elements with uint; do not silently wrap offsets.
        if (kv_elements > std.math.maxInt(u32) or query_elements > std.math.maxInt(u32) or bias_elements > std.math.maxInt(u32)) return error.InvalidContextAttention;
        if (request.flash_prefill and request.split_decode != null) return error.InvalidContextAttention;
        if (request.split_decode) |split| {
            if (request.query_rows != 1 or split.split_count == 0 or split.split_count > split_decode_max_splits or
                split.scratch.offset % split_decode_scratch_alignment != 0)
                return error.InvalidContextAttention;
            var scratch_elements = try std.math.mul(usize, request.query_heads, split.split_count);
            scratch_elements = try std.math.mul(usize, scratch_elements, try std.math.add(usize, view.head_dim, 2));
            if (scratch_elements > std.math.maxInt(u32) or
                split.scratch.bytes < try splitDecodeScratchBytes(request.query_heads, view.head_dim, split.split_count))
                return error.InvalidContextAttention;
            for ([_]Ref{ queries, keys, values, bias, output, self.status.ref() }) |ref|
                if (split.scratch.handle == ref.handle) return error.AliasedContextStaging;
        }
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        var owned_descriptors: ?Buffer = null;
        defer if (owned_descriptors) |*buffer| buffer.deinit();
        var owned_heads: ?Buffer = null;
        defer if (owned_heads) |*buffer| buffer.deinit();
        const descriptor_refs = if (request.descriptors) |provided| blk: {
            if (provided.spans.bytes < view.spans.len * @sizeOf(views.Span) or
                provided.heads.bytes < view.heads.len * @sizeOf(views.Head)) return error.MetalBufferRange;
            break :blk provided;
        } else blk: {
            if (owner_runtime != null) return error.ContextDescriptorArenaRequired;
            owned_descriptors = try self.core.buffer(view.spans.len * @sizeOf(views.Span));
            @memcpy(owned_descriptors.?.bytes[0..owned_descriptors.?.logical_bytes], std.mem.sliceAsBytes(view.spans));
            if (view.version == views.abi_version) {
                owned_heads = try self.core.buffer(view.heads.len * @sizeOf(views.Head));
                @memcpy(owned_heads.?.bytes[0..owned_heads.?.logical_bytes], std.mem.sliceAsBytes(view.heads));
            }
            break :blk DescriptorStorage{
                .spans = owned_descriptors.?.ref(),
                .heads = if (owned_heads) |buffer| buffer.ref() else self.core.dummy.ref(),
            };
        };
        @memset(self.status.bytes[0..4], 0);
        const base_params = shared.Params{
            .op = view.version,
            .n = 0,
            .dim = view.head_dim,
            .groups = request.query_heads,
            .stride = view.kv_heads,
            .index = @intCast(view.spans.len),
            .position = request.query_start,
            .mode = request.sliding_window,
            .scale = request.scale,
            .epsilon = request.logit_softcap,
            .theta = @bitCast(request.keep_prefix_tokens),
        };
        if (request.flash_prefill) {
            const kernels = if (self.flash_prefill) |*value| value else return error.ContextFlashPrefillUnavailable;
            var tiled = base_params;
            tiled.n = request.query_rows;
            const inputs = [_]Ref{ queries, keys, values, bias, descriptor_refs.spans, self.status.ref(), descriptor_refs.heads };
            if (owner_runtime) |runtime|
                try kernels.dispatchOnActiveRuntime(&self.core, runtime, tiled, &inputs, output)
            else
                _ = try kernels.dispatch(&self.core, tiled, &inputs, output);
        } else if (request.split_decode) |split| {
            var stage = base_params;
            stage.op = view.version + 2;
            stage.n = try std.math.mul(u32, try std.math.mul(u32, request.query_heads, split.split_count), 32);
            stage.reserved = @bitCast(split.split_count);
            var reduce = base_params;
            reduce.op = view.version + 4;
            reduce.n = try std.math.mul(u32, request.query_heads, 32);
            reduce.reserved = @bitCast(split.split_count);
            const stage_inputs = [_]Ref{ queries, keys, values, bias, descriptor_refs.spans, self.status.ref(), descriptor_refs.heads };
            const reduce_inputs = [_]Ref{ split.scratch, self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), self.status.ref(), self.core.dummy.ref() };
            if (owner_runtime) |runtime| {
                try self.core.runContextSplitPairOnActiveRuntime(runtime, stage, &stage_inputs, split.scratch, reduce, &reduce_inputs, output);
            } else _ = try self.core.runPair(stage, &stage_inputs, split.scratch, reduce, &reduce_inputs, output, .attention);
        } else {
            var serial = base_params;
            serial.n = try std.math.mul(u32, try std.math.mul(u32, request.query_rows, request.query_heads), 32);
            const inputs = [_]Ref{ queries, keys, values, bias, descriptor_refs.spans, self.status.ref(), descriptor_refs.heads };
            if (owner_runtime) |runtime|
                try self.core.runOnActiveRuntime(runtime, serial, &inputs, output)
            else
                try self.core.run(serial, &inputs, output);
        }
        if (owner_runtime != null) return;
        // A bounded status read prevents NaN/overflow from silently becoming
        // a zero attention output. KV and queries never return to the host.
        self.status_readback_bytes +|= 4;
        if (std.mem.bytesToValue(u32, self.status.bytes[0..4]) != 0) return error.NonFiniteContextAttention;
    }
};

test "context split decode plan is bounded and reports physical allocation bytes" {
    const page = std.heap.pageSize();
    const one = try splitDecodePlan(1, 8, 512);
    try std.testing.expectEqual(@as(u32, 1), one.split_count);
    const one_raw = 8 * (512 + 2) * @sizeOf(f32);
    try std.testing.expectEqual((one_raw + page - 1) & ~(page - 1), one.scratch_bytes);
    const long = try splitDecodePlan(8192, 16, 512);
    try std.testing.expectEqual(split_decode_max_splits, long.split_count);
    const raw = 16 * 32 * (512 + 2) * @sizeOf(f32);
    try std.testing.expectEqual((raw + page - 1) & ~(page - 1), long.scratch_bytes);
    try std.testing.expectError(error.InvalidContextAttention, splitDecodePlan(0, 8, 512));
    try std.testing.expectError(error.InvalidContextAttention, splitDecodeScratchBytes(8, 512, split_decode_max_splits + 1));
}

test "context Metal segmented attention preserves compacted mass against appended tokens" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    const dim = 32;
    const rows = 257;
    var keys = try Buffer.init(kernels.core.runtime, rows * dim * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, rows * dim * 2);
    defer values.deinit();
    var queries = try Buffer.init(kernels.core.runtime, dim * 4);
    defer queries.deinit();
    var bias = try Buffer.init(kernels.core.runtime, 256 * 4);
    defer bias.deinit();
    var output = try Buffer.init(kernels.core.runtime, dim * 4);
    defer output.deinit();
    @memset(keys.bytes[0..keys.logical_bytes], 0);
    const value_halves = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(values.bytes[0..values.logical_bytes])));
    @memset(value_halves[0 .. 256 * dim], 1);
    @memset(value_halves[256 * dim ..], 0);
    @memset(queries.floats(), 1);
    @memset(bias.floats(), @log(@as(f32, 2)));
    const spans = [_]views.Span{
        .{ .physical_start = 0, .row_count = 256, .logical_start = 0, .logical_count = 512, .kind = .compacted },
        .{ .physical_start = 256, .row_count = 1, .logical_start = 512, .logical_count = 1 },
    };
    const request = Request{
        .view = .{ .epoch = 1, .logical_tokens = 513, .capacity_rows = rows, .bias_rows = 256, .kv_heads = 1, .head_dim = dim, .spans = &spans },
        .current_epoch = 1,
        .query_start = 512,
        .query_rows = 1,
        .query_heads = 1,
        .scale = 1,
    };
    try kernels.attention(request, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref());
    for (output.floats()) |value| try std.testing.expectApproxEqAbs(@as(f32, 512.0 / 513.0), value, 1e-5);
    var stale = request;
    stale.current_epoch = 2;
    try std.testing.expectError(error.StaleContextView, kernels.attention(stale, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref()));
    queries.floats()[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteContextAttention, kernels.attention(request, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref()));
    queries.floats()[0] = 1;
    value_halves[0] = std.math.inf(f16);
    try std.testing.expectError(error.NonFiniteContextAttention, kernels.attention(request, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref()));
}

test "context Metal wrapped sliding attention masks absolute positions" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    const dim = 256;
    var keys = try Buffer.init(kernels.core.runtime, 4 * dim * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 4 * dim * 2);
    defer values.deinit();
    var queries = try Buffer.init(kernels.core.runtime, 4 * dim * 4);
    defer queries.deinit();
    var output = try Buffer.init(kernels.core.runtime, 4 * dim * 4);
    defer output.deinit();
    @memset(keys.bytes[0..keys.logical_bytes], 0);
    @memset(queries.floats(), 1);
    const halves = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(values.bytes[0..values.logical_bytes])));
    // Physical rows 0..3 represent logical positions 102,103,100,101.
    for ([_]f16{ 2, 3, 0, 1 }, 0..) |v, i| @memset(halves[i * dim ..][0..dim], v);
    const spans = [_]views.Span{
        .{ .physical_start = 2, .row_count = 2, .logical_start = 100, .logical_count = 2 },
        .{ .physical_start = 0, .row_count = 2, .logical_start = 102, .logical_count = 2 },
    };
    try kernels.attention(.{
        .view = .{ .epoch = 1, .logical_tokens = 104, .capacity_rows = 4, .kv_heads = 1, .head_dim = dim, .spans = &spans },
        .current_epoch = 1,
        .query_start = 102,
        .query_rows = 2,
        .query_heads = 2,
        .sliding_window = 2,
        .scale = 1,
    }, queries.ref(), keys.ref(), values.ref(), kernels.core.dummy.ref(), output.ref());
    for (output.floats()[0 .. 2 * dim]) |v| try std.testing.expectApproxEqAbs(@as(f32, 1.5), v, 1e-6);
    for (output.floats()[2 * dim ..]) |v| try std.testing.expectApproxEqAbs(@as(f32, 2.5), v, 1e-6);
    // Exercise the opt-in route on a wrapped decode row. Two splits force one
    // valid SWA row into each partial and GQA maps both query heads to one KV.
    const scratch_bytes = try splitDecodeScratchBytes(2, dim, 2);
    var scratch = try Buffer.init(kernels.core.runtime, scratch_bytes);
    defer scratch.deinit();
    const decode_queries = try queries.ref().slice(2 * dim * 4, 2 * dim * 4);
    const decode_output = try output.ref().slice(0, 2 * dim * 4);
    const split_request = Request{
        .view = .{ .epoch = 1, .logical_tokens = 104, .capacity_rows = 4, .kv_heads = 1, .head_dim = dim, .spans = &spans },
        .current_epoch = 1,
        .query_start = 103,
        .query_rows = 1,
        .query_heads = 2,
        .sliding_window = 2,
        .scale = 1,
        .split_decode = .{ .scratch = scratch.ref(), .split_count = 2 },
    };
    try kernels.attention(split_request, decode_queries, keys.ref(), values.ref(), kernels.core.dummy.ref(), decode_output);
    for (output.floats()[0 .. 2 * dim]) |v| try std.testing.expectApproxEqAbs(@as(f32, 2.5), v, 1e-6);
    halves[0] = std.math.inf(f16);
    try std.testing.expectError(error.NonFiniteContextAttention, kernels.attention(split_request, decode_queries, keys.ref(), values.ref(), kernels.core.dummy.ref(), decode_output));
    halves[0] = 2;
    // The host-visible status word is reset before each paired dispatch; a
    // nonfinite failure must not poison the next use of the shared buffers.
    try kernels.attention(split_request, decode_queries, keys.ref(), values.ref(), kernels.core.dummy.ref(), decode_output);
    for (output.floats()[0 .. 2 * dim]) |v| try std.testing.expectApproxEqAbs(@as(f32, 2.5), v, 1e-6);
    try std.testing.expectEqual(@as(u64, 3), kernels.core.pair_metrics.dispatches);
    try std.testing.expectEqual(@as(u64, 0), kernels.core.pair_metrics.failures);
    var split_prefill = split_request;
    split_prefill.query_start = 102;
    split_prefill.query_rows = 2;
    try std.testing.expectError(error.InvalidContextAttention, kernels.attention(split_prefill, queries.ref(), keys.ref(), values.ref(), kernels.core.dummy.ref(), output.ref()));
    try std.testing.expectError(error.MissingContextHistory, kernels.attention(.{
        .view = .{ .epoch = 1, .logical_tokens = 104, .capacity_rows = 4, .kv_heads = 1, .head_dim = dim, .spans = &spans },
        .current_epoch = 1,
        .query_start = 102,
        .query_rows = 2,
        .query_heads = 2,
        .scale = 1,
    }, queries.ref(), keys.ref(), values.ref(), kernels.core.dummy.ref(), output.ref()));
}

test "context Metal segmented GQA matches independent full history at Gemma head widths" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    for ([_]usize{ 256, 512 }) |dim| {
        const kv_heads = 2;
        const query_heads = 4;
        const rows = 257;
        var keys = try Buffer.init(kernels.core.runtime, rows * kv_heads * dim * 2);
        defer keys.deinit();
        var values = try Buffer.init(kernels.core.runtime, rows * kv_heads * dim * 2);
        defer values.deinit();
        var queries = try Buffer.init(kernels.core.runtime, query_heads * dim * 4);
        defer queries.deinit();
        var bias = try Buffer.init(kernels.core.runtime, 256 * kv_heads * 4);
        defer bias.deinit();
        var output = try Buffer.init(kernels.core.runtime, query_heads * dim * 4);
        defer output.deinit();
        const plan = try splitDecodePlan(rows, query_heads, @intCast(dim));
        var scratch = try Buffer.init(kernels.core.runtime, plan.scratch_bytes);
        defer scratch.deinit();
        const ks = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(keys.bytes[0..keys.logical_bytes])));
        const vs = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(values.bytes[0..values.logical_bytes])));
        for (0..rows) |r| for (0..kv_heads) |h| for (0..dim) |d| {
            ks[(r * kv_heads + h) * dim + d] = @floatCast((@as(f32, @floatFromInt((r * 31 + h * 7 + d * 3) % 23)) - 11) / 32);
            vs[(r * kv_heads + h) * dim + d] = @floatCast((@as(f32, @floatFromInt((r * 13 + h * 11 + d * 5) % 19)) - 9) / 16);
        };
        for (queries.floats(), 0..) |*q, i| q.* = (@as(f32, @floatFromInt(i % 17)) - 8) / 8;
        @memset(bias.floats(), @log(@as(f32, 2)));
        const spans = [_]views.Span{
            .{ .physical_start = 0, .row_count = 256, .logical_start = 0, .logical_count = 512, .kind = .compacted },
            .{ .physical_start = 256, .row_count = 1, .logical_start = 512, .logical_count = 1 },
        };
        const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(dim)));
        var request = Request{
            .view = .{ .epoch = 1, .logical_tokens = 513, .capacity_rows = rows, .bias_rows = 256, .kv_heads = kv_heads, .head_dim = @intCast(dim), .spans = &spans },
            .current_epoch = 1,
            .query_start = 512,
            .query_rows = 1,
            .query_heads = query_heads,
            .scale = scale,
            .split_decode = .{ .scratch = scratch.ref(), .split_count = plan.split_count },
        };
        const full_keys = try alloc.alloc(f32, 513 * dim);
        defer alloc.free(full_keys);
        const full_values = try alloc.alloc(f32, 513 * dim);
        defer alloc.free(full_values);
        const expected = try alloc.alloc(f64, dim);
        defer alloc.free(expected);
        for ([_]f32{ 0, 0.15 }) |softcap| {
            request.logit_softcap = softcap;
            for ([_]bool{ false, true }) |use_split| {
                request.split_decode = if (use_split) .{ .scratch = scratch.ref(), .split_count = plan.split_count } else null;
                try kernels.attention(request, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref());
                for (0..query_heads) |qh| {
                    const kh = qh / (query_heads / kv_heads);
                    for (0..513) |r| for (0..dim) |d| {
                        const src = if (r == 512) 256 else r / 2;
                        full_keys[r * dim + d] = ks[(src * kv_heads + kh) * dim + d];
                        full_values[r * dim + d] = vs[(src * kv_heads + kh) * dim + d];
                    };
                    // Duplicate each original history row, then append one
                    // exact row. The oracle consumes neither descriptors nor
                    // mass bias and independently checks both routes.
                    fullHistoryReference(queries.floats()[qh * dim ..][0..dim], full_keys, full_values, scale, softcap, expected);
                    for (expected, output.floats()[qh * dim ..][0..dim]) |truth, got|
                        try std.testing.expectApproxEqAbs(truth, @as(f64, got), 3e-5);
                }
            }
        }
    }
}

test "context Metal packed unequal heads match full history with GQA and causal append" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const alloc = std.testing.allocator;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    for ([_]usize{ 256, 512 }) |dim| {
        const counts = [_]usize{ 128, 384, 512 };
        const starts = [_]usize{ 0, 130, 516 };
        const total_rows = 1030;
        var keys = try Buffer.init(kernels.core.runtime, total_rows * dim * 2);
        defer keys.deinit();
        var values = try Buffer.init(kernels.core.runtime, total_rows * dim * 2);
        defer values.deinit();
        var queries = try Buffer.init(kernels.core.runtime, 2 * 6 * dim * 4);
        defer queries.deinit();
        var bias = try Buffer.init(kernels.core.runtime, 512 * 4);
        defer bias.deinit();
        var output = try Buffer.init(kernels.core.runtime, 2 * 6 * dim * 4);
        defer output.deinit();
        const split_plan = try splitDecodePlan(514, 6, @intCast(dim));
        var split_scratch = try Buffer.init(kernels.core.runtime, split_plan.scratch_bytes);
        defer split_scratch.deinit();
        const ks = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(keys.bytes[0..keys.logical_bytes])));
        const vs = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(values.bytes[0..values.logical_bytes])));
        for (0..3) |h| for (0..counts[h] + 2) |r| for (0..dim) |d| {
            const index = (starts[h] + r) * dim + d;
            ks[index] = @floatCast((@as(f32, @floatFromInt((r * 31 + h * 7 + d * 3) % 23)) - 11) / 32);
            vs[index] = @floatCast((@as(f32, @floatFromInt((r * 13 + h * 11 + d * 5) % 19)) - 9) / 16);
        };
        for (queries.floats(), 0..) |*q, i| q.* = (@as(f32, @floatFromInt(i % 17)) - 8) / 8;
        @memset(bias.floats()[0..128], @log(@as(f32, 4)));
        @memset(bias.floats()[128..256], @log(@as(f32, 2)));
        @memset(bias.floats()[256..], 0);
        var spans = [_]views.Span{
            .{ .physical_start = 0, .row_count = 128, .logical_start = 0, .logical_count = 512, .kind = .compacted },
            .{ .physical_start = 128, .row_count = 2, .logical_start = 512, .logical_count = 2 },
            .{ .physical_start = 0, .row_count = 384, .logical_start = 0, .logical_count = 512, .kind = .compacted },
            .{ .physical_start = 384, .row_count = 2, .logical_start = 512, .logical_count = 2 },
            .{ .physical_start = 0, .row_count = 514, .logical_start = 0, .logical_count = 514 },
        };
        const heads = [_]views.Head{
            .{ .kv_start = 0, .capacity_rows = 130, .bias_start = 0, .bias_rows = 128, .span_start = 0, .span_count = 2 },
            .{ .kv_start = 130, .capacity_rows = 386, .bias_start = 128, .bias_rows = 384, .span_start = 2, .span_count = 2 },
            .{ .kv_start = 516, .capacity_rows = 514, .bias_start = 512, .bias_rows = 0, .span_start = 4, .span_count = 1 },
        };
        const scale = 1.0 / @sqrt(@as(f32, @floatFromInt(dim)));
        var request = Request{
            .view = .{ .version = views.abi_version, .epoch = 1, .logical_tokens = 514, .capacity_rows = total_rows, .bias_rows = 512, .kv_heads = 3, .head_dim = @intCast(dim), .spans = &spans, .heads = &heads },
            .current_epoch = 1,
            .query_start = 512,
            .query_rows = 2,
            .query_heads = 6,
            .scale = scale,
        };
        const full_keys = try alloc.alloc(f32, 514 * dim);
        defer alloc.free(full_keys);
        const full_values = try alloc.alloc(f32, 514 * dim);
        defer alloc.free(full_values);
        const expected = try alloc.alloc(f64, dim);
        defer alloc.free(expected);
        for ([_]f32{ 0, 0.15 }) |softcap| {
            request.logit_softcap = softcap;
            try kernels.attention(request, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref());
            for (0..2) |token| for (0..6) |qh| {
                const kh = qh / 2;
                const rows = 513 + token;
                for (0..rows) |r| for (0..dim) |d| {
                    const src = if (r >= 512) counts[kh] + r - 512 else switch (kh) {
                        0 => r / 4,
                        1 => if (r < 256) r / 2 else r - 128,
                        else => r,
                    };
                    full_keys[r * dim + d] = ks[(starts[kh] + src) * dim + d];
                    full_values[r * dim + d] = vs[(starts[kh] + src) * dim + d];
                };
                const offset = (token * 6 + qh) * dim;
                fullHistoryReference(queries.floats()[offset..][0..dim], full_keys[0 .. rows * dim], full_values[0 .. rows * dim], scale, softcap, expected);
                for (expected, output.floats()[offset..][0..dim]) |truth, got|
                    try std.testing.expectApproxEqAbs(truth, @as(f64, got), 2e-5);
            };
            // Decode each appended position independently through packed ABI2.
            // At position 512 the row for 513 remains present but causal-masked.
            for (0..2) |token| {
                var split_request = request;
                split_request.query_start = @intCast(512 + token);
                split_request.query_rows = 1;
                split_request.split_decode = .{ .scratch = split_scratch.ref(), .split_count = split_plan.split_count };
                const byte_offset = token * 6 * dim * @sizeOf(f32);
                const byte_count = 6 * dim * @sizeOf(f32);
                const split_queries = try queries.ref().slice(byte_offset, byte_count);
                const split_output = try output.ref().slice(byte_offset, byte_count);
                try kernels.attention(split_request, split_queries, keys.ref(), values.ref(), bias.ref(), split_output);
                for (0..6) |qh| {
                    const kh = qh / 2;
                    const rows = 513 + token;
                    for (0..rows) |r| for (0..dim) |d| {
                        const src = if (r >= 512) counts[kh] + r - 512 else switch (kh) {
                            0 => r / 4,
                            1 => if (r < 256) r / 2 else r - 128,
                            else => r,
                        };
                        full_keys[r * dim + d] = ks[(starts[kh] + src) * dim + d];
                        full_values[r * dim + d] = vs[(starts[kh] + src) * dim + d];
                    };
                    const element_offset = (token * 6 + qh) * dim;
                    fullHistoryReference(queries.floats()[element_offset..][0..dim], full_keys[0 .. rows * dim], full_values[0 .. rows * dim], scale, softcap, expected);
                    for (expected, output.floats()[element_offset..][0..dim]) |truth, got|
                        try std.testing.expectApproxEqAbs(truth, @as(f64, got), 3e-5);
                }
            }
        }
        // All invalid requests fail before a GPU dispatch/status readback.
        const readbacks = kernels.status_readback_bytes;
        spans[3].logical_start += 1;
        spans[3].row_count = 1;
        spans[3].logical_count = 1;
        try std.testing.expectError(error.MissingContextHistory, kernels.attention(request, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref()));
        spans[3].logical_start -= 1;
        spans[3].row_count = 2;
        spans[3].logical_count = 2;
        var invalid = request;
        invalid.view.version = 99;
        try std.testing.expectError(error.InvalidContextView, kernels.attention(invalid, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref()));
        var short_keys = keys.ref();
        short_keys.bytes -= 2;
        try std.testing.expectError(error.MetalBufferRange, kernels.attention(request, queries.ref(), short_keys, values.ref(), bias.ref(), output.ref()));
        invalid = request;
        invalid.sliding_window = 1;
        try std.testing.expectError(error.InvalidContextAttention, kernels.attention(invalid, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref()));
        invalid = request;
        invalid.query_start = 511;
        try std.testing.expectError(error.MissingContextHistory, kernels.attention(invalid, queries.ref(), keys.ref(), values.ref(), bias.ref(), output.ref()));
        try std.testing.expectEqual(readbacks, kernels.status_readback_bytes);
    }
}

// Independent FP64 full-history oracle for multiple appended rows and softcap.
// It consumes expanded original KV, never compact descriptors or mass biases.
fn fullHistoryReference(query: []const f32, keys: []const f32, values: []const f32, scale: f32, softcap: f32, output: []f64) void {
    const dim = query.len;
    const rows = keys.len / dim;
    std.debug.assert(dim > 0 and rows > 0 and values.len == keys.len and output.len == dim);
    var maximum: f64 = -std.math.inf(f64);
    for (0..rows) |r| {
        var dot: f64 = 0;
        for (query, keys[r * dim ..][0..dim]) |q, k| dot += @as(f64, q) * k;
        dot *= scale;
        if (softcap > 0) dot = softcap * std.math.tanh(dot / softcap);
        maximum = @max(maximum, dot);
    }
    @memset(output, 0);
    var denominator: f64 = 0;
    for (0..rows) |r| {
        var dot: f64 = 0;
        for (query, keys[r * dim ..][0..dim]) |q, k| dot += @as(f64, q) * k;
        dot *= scale;
        if (softcap > 0) dot = softcap * std.math.tanh(dot / softcap);
        const weight = @exp(dot - maximum);
        denominator += weight;
        for (output, values[r * dim ..][0..dim]) |*o, v| o.* += weight * v;
    }
    for (output) |*o| o.* /= denominator;
}

test "context Metal active split pair preserves reuse and downstream dependencies in all encoder modes" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!mr.metalDeviceAvailable()) return error.SkipZigTest;

    const enable_dag = "TERMITE_METAL_ENABLE_A4B_DAG_SCHEDULER";
    const disable_dag = "TERMITE_METAL_DISABLE_A4B_DAG_SCHEDULER";
    const original_enable = if (std.c.getenv(enable_dag)) |value|
        try std.testing.allocator.dupeSentinel(u8, std.mem.span(value), 0)
    else
        null;
    const original_disable = if (std.c.getenv(disable_dag)) |value|
        try std.testing.allocator.dupeSentinel(u8, std.mem.span(value), 0)
    else
        null;
    defer {
        if (original_enable) |value| {
            _ = setenv(enable_dag, value.ptr, 1);
            std.testing.allocator.free(value);
        } else _ = unsetenv(enable_dag);
        if (original_disable) |value| {
            _ = setenv(disable_dag, value.ptr, 1);
            std.testing.allocator.free(value);
        } else _ = unsetenv(disable_dag);
    }

    var kernels = try Kernels.init();
    defer kernels.deinit();
    const dim = 256;
    var keys = try Buffer.init(kernels.core.runtime, 4 * dim * @sizeOf(f16));
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 4 * dim * @sizeOf(f16));
    defer values.deinit();
    var queries = try Buffer.init(kernels.core.runtime, 2 * dim * @sizeOf(f32));
    defer queries.deinit();
    var first_output = try Buffer.init(kernels.core.runtime, 2 * dim * @sizeOf(f32));
    defer first_output.deinit();
    var second_output = try Buffer.init(kernels.core.runtime, 2 * dim * @sizeOf(f32));
    defer second_output.deinit();
    const scratch_bytes = try splitDecodeScratchBytes(2, dim, 2);
    var scratch = try Buffer.init(kernels.core.runtime, scratch_bytes);
    defer scratch.deinit();
    var span_storage = try Buffer.init(kernels.core.runtime, 2 * @sizeOf(views.Span));
    defer span_storage.deinit();
    var head_storage = try Buffer.init(kernels.core.runtime, @sizeOf(views.Head));
    defer head_storage.deinit();

    @memset(queries.floats(), 1);
    const key_halves = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(keys.bytes[0..keys.logical_bytes])));
    const value_halves = std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(values.bytes[0..values.logical_bytes])));
    for (0..4) |row| for (0..dim) |d| {
        const centered = @as(f32, @floatFromInt(@as(i32, @intCast(d % 11)) - 5));
        const key = switch (row) {
            0 => 0.03125 + centered / 512,
            1 => -0.015625 + centered / 384,
            else => 0.25,
        };
        const wave = @as(f32, @floatFromInt(@as(i32, @intCast(d % 17)) - 8)) / 8;
        const value = switch (row) {
            0 => wave,
            1 => 0.5 - wave * 0.75,
            else => 11,
        };
        key_halves[row * dim + d] = @floatCast(key);
        value_halves[row * dim + d] = @floatCast(value);
    };
    @memset(first_output.floats(), -7);
    @memset(second_output.floats(), 13);
    const reference_keys = try std.testing.allocator.alloc(f32, 2 * dim);
    defer std.testing.allocator.free(reference_keys);
    const reference_values = try std.testing.allocator.alloc(f32, 2 * dim);
    defer std.testing.allocator.free(reference_values);
    for (0..2 * dim) |i| {
        reference_keys[i] = @floatCast(key_halves[i]);
        reference_values[i] = @floatCast(value_halves[i]);
    }
    const first_expected = try std.testing.allocator.alloc(f64, dim);
    defer std.testing.allocator.free(first_expected);
    const first_expected_query = try std.testing.allocator.alloc(f32, dim);
    defer std.testing.allocator.free(first_expected_query);
    const second_expected = try std.testing.allocator.alloc(f64, dim);
    defer std.testing.allocator.free(second_expected);
    const stale_query = try std.testing.allocator.alloc(f32, dim);
    defer std.testing.allocator.free(stale_query);
    const stale_second = try std.testing.allocator.alloc(f64, dim);
    defer std.testing.allocator.free(stale_second);
    const scale = 1.0 / @sqrt(@as(f32, dim));
    fullHistoryReference(queries.floats()[0..dim], reference_keys, reference_values, scale, 0, first_expected);
    for (first_expected, first_expected_query) |expected, *query| query.* = @floatCast(expected);
    fullHistoryReference(first_expected_query, reference_keys, reference_values, scale, 0, second_expected);
    @memset(stale_query, -7);
    fullHistoryReference(stale_query, reference_keys, reference_values, scale, 0, stale_second);
    var stale_max_difference: f64 = 0;
    for (second_expected, stale_second) |expected, stale| stale_max_difference = @max(stale_max_difference, @abs(expected - stale));
    try std.testing.expect(stale_max_difference > 0.1);
    const spans = [_]views.Span{
        .{ .physical_start = 2, .row_count = 2, .logical_start = 100, .logical_count = 2 },
        .{ .physical_start = 0, .row_count = 2, .logical_start = 102, .logical_count = 2 },
    };
    const heads = [_]views.Head{
        .{ .kv_start = 0, .capacity_rows = 4, .bias_start = 0, .bias_rows = 0, .span_start = 0, .span_count = 2 },
    };
    @memcpy(span_storage.bytes[0..span_storage.logical_bytes], std.mem.sliceAsBytes(spans[0..]));
    @memcpy(head_storage.bytes[0..head_storage.logical_bytes], std.mem.sliceAsBytes(heads[0..]));
    const request = Request{
        .view = .{ .version = views.abi_version, .epoch = 1, .logical_tokens = 104, .capacity_rows = 4, .kv_heads = 1, .head_dim = dim, .spans = &spans, .heads = &heads },
        .current_epoch = 1,
        .query_start = 103,
        .query_rows = 1,
        .query_heads = 2,
        .sliding_window = 2,
        .scale = scale,
        .split_decode = .{ .scratch = scratch.ref(), .split_count = 2 },
        .descriptors = .{ .spans = span_storage.ref(), .heads = head_storage.ref() },
    };
    const cases = [_]struct { concurrent: bool, resource_barriers: bool }{
        .{ .concurrent = false, .resource_barriers = false },
        .{ .concurrent = true, .resource_barriers = false },
        .{ .concurrent = true, .resource_barriers = true },
    };
    for (cases) |case| {
        if (case.resource_barriers) {
            try std.testing.expectEqual(@as(c_int, 0), setenv(enable_dag, "1", 1));
            try std.testing.expectEqual(@as(c_int, 0), unsetenv(disable_dag));
        } else {
            try std.testing.expectEqual(@as(c_int, 0), unsetenv(enable_dag));
            try std.testing.expectEqual(@as(c_int, 0), unsetenv(disable_dag));
        }
        const owner = mr.termite_metal_decode_runtime_create() orelse return error.SkipZigTest;
        defer mr.termite_metal_decode_runtime_destroy(owner);
        if (mr.termite_metal_decode_runtime_ready(owner) == 0) return error.SkipZigTest;
        try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_set_concurrent_planned_dispatch(owner, @intFromBool(case.concurrent)));
        if (case.resource_barriers)
            try std.testing.expectEqual(@as(c_int, 0), mr.termite_metal_decode_runtime_configure_a4b_dag_scheduler(owner, 1));

        try mr.beginFrame(owner);
        errdefer mr.cancelFrame(owner) catch {};
        try mr.beginPlannedComputeScope(owner, 3, .attention);
        if (!case.concurrent) try mr.pushPlannedComputeBarrierSuppression(owner);
        try kernels.attentionOnActiveRuntime(owner, request, queries.ref(), keys.ref(), values.ref(), kernels.core.dummy.ref(), first_output.ref());
        // The second pair reuses scratch and consumes the first pair's output,
        // exercising both reduce->stage WAR and final-output->stage RAW.
        try kernels.attentionOnActiveRuntime(owner, request, first_output.ref(), keys.ref(), values.ref(), kernels.core.dummy.ref(), second_output.ref());
        if (!case.concurrent) try mr.popPlannedComputeBarrierSuppression(owner);
        try mr.endPlannedComputeScope(owner);
        try mr.submitFrame(owner);
        try mr.waitFrame(owner);

        for (0..2) |head| {
            for (first_expected, first_output.floats()[head * dim ..][0..dim]) |expected, value|
                try std.testing.expectApproxEqAbs(expected, @as(f64, value), 3e-5);
            for (second_expected, second_output.floats()[head * dim ..][0..dim]) |expected, value|
                try std.testing.expectApproxEqAbs(expected, @as(f64, value), 3e-5);
        }
        try std.testing.expectEqual(@as(u32, 0), std.mem.bytesToValue(u32, kernels.status.bytes[0..4]));
        const snapshot = mr.runtimeMemorySnapshot(owner);
        try std.testing.expectEqual(@as(u64, 1), snapshot.last_frame_compute_encoder_count);
        if (case.concurrent)
            try std.testing.expect(snapshot.last_frame_planned_barrier_count >= 3)
        else
            try std.testing.expectEqual(@as(u64, 0), snapshot.last_frame_planned_barrier_count);
    }
}
