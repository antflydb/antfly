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

//! Exact global reference attention over leased original KV and a private tail.
//! The request sequence lock serializes acquisition with live owner mutation.
//! Calls drain GPU work synchronously; the caller must end calls before deinit.
//! This does not own live model state, SWA, sampling, grammar or user usage, and
//! is not a serving capability. The model branch must own its separate SWA.
const std = @import("std");
const shared = @import("metal_generated_kernels.zig");
const mr = @import("metal_runtime.zig");
const storage = @import("packed_context_storage.zig");
const views = @import("../runtime/kv/context_view.zig");
const execution = @import("../execution_control.zig");
const Buffer = shared.Buffer;
const Ref = shared.Ref;

fn rounded(bytes: usize) !usize {
    return (try std.math.add(usize, bytes, std.heap.pageSize() - 1)) & ~(std.heap.pageSize() - 1);
}

pub const Allocation = struct {
    tail_bytes: usize,
    temporary_bytes: usize,
    /// Conservative lease-control reservation. Once created, this belongs to
    /// the original generation's ownedBytes until that generation retires;
    /// closing the replay releases only temporary_bytes, not this reservation.
    owner_control_bytes: usize,
    physical_bytes: usize,
};

/// Includes the separately charged lease control, tail K/V, uploaded heads,
/// status page and kernel dummy page. The original KV is retained, never copied.
pub fn allocation(base: views.View, tail_capacity: u32, position_limit: u32) !Allocation {
    _ = try base.validate();
    if (base.version != views.abi_version or base.head_dim % 2 != 0 or base.logical_tokens == 0 or tail_capacity == 0 or
        tail_capacity > @import("../runtime/kv/reference_replay.zig").max_additional_tokens or
        @as(u64, base.logical_tokens) + tail_capacity > position_limit) return error.InvalidReferenceReplay;
    // Original packed plans are dense per head. Reject arbitrary sparse/wrapped
    // views and every compacted span instead of silently changing addressing.
    for (0..base.kv_heads) |head| {
        var end: u64 = 0;
        for (base.headSpans(head)) |span| {
            if (span.kind != .exact or span.logical_start != end or span.physical_start != end)
                return error.InvalidReferenceReplay;
            end += span.logical_count;
        }
        if (end != base.logical_tokens) return error.InvalidReferenceReplay;
    }
    const elements = @as(u64, tail_capacity) * base.kv_heads * base.head_dim;
    if (elements > std.math.maxInt(u32) or base.kvElements() > std.math.maxInt(u32)) return error.InvalidReferenceReplay;
    const bytes = try std.math.mul(usize, @intCast(elements), 2);
    const buffers = try std.math.add(usize, try std.math.mul(usize, try rounded(bytes), 2), try rounded(base.heads.len * @sizeOf(views.Head)));
    const temporary = try std.math.add(usize, buffers, try std.math.mul(usize, try rounded(4), 2));
    const control = try storage.replayControlBytes();
    return .{ .tail_bytes = bytes, .temporary_bytes = temporary, .owner_control_bytes = control, .physical_bytes = try std.math.add(usize, temporary, control) };
}

pub const Config = struct {
    tail_capacity: u32,
    position_limit: u32,
    physical_budget_bytes: usize,
    cancelled: ?*const std.atomic.Value(bool) = null,
    execution_control: ?execution.InferenceExecutionControl = null,
};

pub const Request = struct {
    query_start: u32,
    query_rows: u32,
    query_heads: u32,
    scale: f32,
    logit_softcap: f32 = 0,
    split_attention: ?SplitAttention = null,
};

pub const split_max_splits: u32 = 32;
pub const split_max_query_rows: u32 = 8;
pub const SplitConfig = struct { max_query_rows: u32, split_count: u32 };
pub const SplitAttention = struct { scratch: Ref, split_count: u32 };
pub const SplitPlan = struct { max_query_rows: u32, split_count: u32, scratch_bytes: usize };

pub fn splitScratchBytes(max_query_rows: u32, query_heads: u32, head_dim: u32, split_count: u32) !usize {
    if (max_query_rows == 0 or max_query_rows > split_max_query_rows or query_heads == 0 or
        head_dim == 0 or head_dim > 512 or split_count < 2 or split_count > split_max_splits)
        return error.InvalidReferenceReplay;
    var elements = try std.math.mul(usize, max_query_rows, query_heads);
    elements = try std.math.mul(usize, elements, split_count);
    elements = try std.math.mul(usize, elements, try std.math.add(usize, head_dim, 2));
    return rounded(try std.math.mul(usize, elements, @sizeOf(f32)));
}

pub fn splitPlan(max_query_rows: u32, query_heads: u32, head_dim: u32, split_count: u32) !SplitPlan {
    return .{ .max_query_rows = max_query_rows, .split_count = split_count, .scratch_bytes = try splitScratchBytes(max_query_rows, query_heads, head_dim, split_count) };
}

pub const Replay = struct {
    lease: storage.ReplayLease,
    core: shared.Kernels,
    keys: Buffer,
    values: Buffer,
    heads: Buffer,
    status: Buffer,
    config: Config,
    sizes: Allocation,
    tail_rows: u32 = 0,
    failed: bool = false,
    status_readback_bytes: u64 = 0,
    tail_write_bytes: u64 = 0,
    serial_attention_dispatches: u64 = 0,
    split_attention_dispatches: u64 = 0,
    mutex: std.atomic.Mutex = .unlocked,

    pub fn init(original: *storage.Generation, config: Config) !Replay {
        return initTracked(original, config, null);
    }
    pub fn initTracked(original: *storage.Generation, config: Config, tracker: ?*shared.BufferAllocationTracker) !Replay {
        return initWithExecution(original, config, tracker, null);
    }
    pub fn initBorrowedTracked(original: *storage.Generation, config: Config, tracker: ?*shared.BufferAllocationTracker, borrowed_execution: *const shared.Execution) !Replay {
        return initWithExecution(original, config, tracker, borrowed_execution);
    }
    fn initWithExecution(original: *storage.Generation, config: Config, tracker: ?*shared.BufferAllocationTracker, borrowed_execution: ?*const shared.Execution) !Replay {
        const source = try original.source();
        const sizes = try allocation(source.view, config.tail_capacity, config.position_limit);
        if (sizes.physical_bytes > config.physical_budget_bytes) return error.ContextReplayBudgetExceeded;
        if (config.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (config.execution_control) |control| try control.check();
        var protection = if (config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        // Acquire last: failed buffer allocations do not alter the live owner.
        var core = if (borrowed_execution) |value|
            try shared.Kernels.initBorrowedTracked(value, tracker)
        else
            try shared.Kernels.initSourceTracked(@embedFile("context_replay.metal"), "context_replay", tracker);
        errdefer core.deinit();
        core.cancelled = config.cancelled;
        core.batching_enabled = false;
        var keys = try core.buffer(sizes.tail_bytes);
        errdefer keys.deinit();
        var values = try core.buffer(sizes.tail_bytes);
        errdefer values.deinit();
        var heads = try core.buffer(source.view.heads.len * @sizeOf(views.Head));
        errdefer heads.deinit();
        @memcpy(heads.bytes[0..heads.logical_bytes], std.mem.sliceAsBytes(source.view.heads));
        var status = try core.buffer(4);
        errdefer status.deinit();
        const lease = try original.acquireReplay(try storage.replayControlBytes());
        return .{ .lease = lease, .core = core, .keys = keys, .values = values, .heads = heads, .status = status, .config = config, .sizes = sizes };
    }
    pub fn deinit(self: *Replay) void {
        self.keys.deinit();
        self.values.deinit();
        self.heads.deinit();
        self.status.deinit();
        self.core.deinit();
        self.lease.deinit();
        self.* = undefined;
    }
    fn check(self: *const Replay) !void {
        if (self.failed) return error.ContextSessionFailed;
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        if (self.config.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.config.execution_control) |control| try control.check();
    }
    fn dispatch(self: *Replay, params: shared.Params, inputs: []const Ref, out: Ref) !void {
        try self.core.run(params, inputs, out);
        self.status_readback_bytes +|= 4;
        if (std.mem.bytesToValue(u32, self.status.bytes[0..4]) != 0) return error.NonFiniteContextAttention;
        try self.check();
    }
    fn dispatchOnActiveRuntime(self: *Replay, owner_runtime: *mr.RawMetalDecodeRuntime, params: shared.Params, inputs: []const Ref, out: Ref) !void {
        try self.core.runOnActiveRuntime(owner_runtime, params, inputs, out);
    }
    /// Append f32 projected K/V into only the branch's f16 tail. Failure poisons
    /// this branch without advancing its visible length or mutating live KV.
    pub fn append(self: *Replay, start: u32, rows: u32, keys: Ref, values: Ref) !void {
        return self.appendImpl(null, start, rows, keys, values);
    }
    pub fn appendOnActiveRuntime(self: *Replay, owner_runtime: *mr.RawMetalDecodeRuntime, start: u32, rows: u32, keys: Ref, values: Ref) !void {
        return self.appendImpl(owner_runtime, start, rows, keys, values);
    }
    fn appendImpl(self: *Replay, owner_runtime: ?*mr.RawMetalDecodeRuntime, start: u32, rows: u32, keys: Ref, values: Ref) !void {
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        try self.check();
        const view = self.lease.source.view;
        if (rows == 0 or start != view.logical_tokens + self.tail_rows or rows > self.config.tail_capacity - self.tail_rows)
            return error.InvalidReferenceReplay;
        const elements = @as(usize, rows) * view.kv_heads * view.head_dim;
        for ([_]Ref{ keys, values }) |ref| {
            if (ref.offset % 4 != 0 or ref.bytes < elements * 4) return error.MetalBufferRange;
            for ([_]Ref{ self.keys.ref(), self.values.ref(), self.lease.source.keys, self.lease.source.values, self.status.ref(), self.heads.ref() }) |owned|
                if (ref.handle == owned.handle) return error.AliasedContextStaging;
        }
        var protection = if (self.config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        errdefer self.failed = true;
        const offset = @as(usize, self.tail_rows) * view.kv_heads * view.head_dim * 2;
        @memset(self.status.bytes[0..4], 0);
        for ([_]Ref{ keys, values }, [_]Ref{ self.keys.ref(), self.values.ref() }) |input, target| {
            const params = shared.Params{ .op = 0, .n = @intCast(elements) };
            const inputs = [_]Ref{ input, self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), self.status.ref() };
            if (owner_runtime) |runtime|
                try self.dispatchOnActiveRuntime(runtime, params, &inputs, try target.slice(offset, elements * 2))
            else
                try self.dispatch(params, &inputs, try target.slice(offset, elements * 2));
        }
        self.tail_rows += rows;
        self.tail_write_bytes +|= elements * 4;
    }
    pub fn attention(self: *Replay, request: Request, queries: Ref, output: Ref) !void {
        return self.attentionImpl(null, request, queries, output);
    }
    pub fn attentionOnActiveRuntime(self: *Replay, owner_runtime: *mr.RawMetalDecodeRuntime, request: Request, queries: Ref, output: Ref) !void {
        return self.attentionImpl(owner_runtime, request, queries, output);
    }
    fn attentionImpl(self: *Replay, owner_runtime: ?*mr.RawMetalDecodeRuntime, request: Request, queries: Ref, output: Ref) !void {
        if (!self.mutex.tryLock()) return error.ContextTransactionBusy;
        defer self.mutex.unlock();
        try self.check();
        const view = self.lease.source.view;
        if (request.query_rows == 0 or request.query_start < view.logical_tokens or
            @as(u64, request.query_start) + request.query_rows > @as(u64, view.logical_tokens) + self.tail_rows or
            request.query_heads == 0 or request.query_heads % view.kv_heads != 0 or
            !std.math.isFinite(request.scale) or request.scale <= 0 or
            !std.math.isFinite(request.logit_softcap) or request.logit_softcap < 0) return error.InvalidReferenceReplay;
        const vectors = try std.math.mul(u32, request.query_rows, request.query_heads);
        const elements = try std.math.mul(u32, vectors, view.head_dim);
        const threads = try std.math.mul(u32, vectors, 32);
        if (queries.offset % 4 != 0 or output.offset % 4 != 0 or queries.bytes < @as(u64, elements) * 4 or output.bytes < @as(u64, elements) * 4)
            return error.MetalBufferRange;
        const inputs = [_]Ref{ queries, self.lease.source.keys, self.lease.source.values, self.keys.ref(), self.values.ref(), self.status.ref(), self.heads.ref() };
        for (inputs) |input| if (input.handle == output.handle) return error.AliasedContextStaging;
        if (request.split_attention) |split| {
            if (request.query_rows > split_max_query_rows or split.split_count < 2 or
                split.split_count > split_max_splits or split.scratch.offset % @alignOf(f32) != 0 or
                split.scratch.bytes < try splitScratchBytes(request.query_rows, request.query_heads, view.head_dim, split.split_count))
                return error.InvalidReferenceReplay;
            for (inputs) |input| if (split.scratch.handle == input.handle) return error.AliasedContextStaging;
            if (split.scratch.handle == output.handle) return error.AliasedContextStaging;
        }
        var protection = if (self.config.execution_control) |control| try control.enterUninterruptible(.uninterruptible) else null;
        defer if (protection) |*guard| guard.deinit();
        errdefer self.failed = true;
        @memset(self.status.bytes[0..4], 0);
        const params = shared.Params{ .op = 1, .n = threads, .dim = view.head_dim, .groups = request.query_heads, .stride = view.kv_heads, .position = request.query_start, .index = view.logical_tokens, .scale = request.scale, .epsilon = request.logit_softcap };
        if (request.split_attention) |split| {
            var stage = params;
            stage.op = 2;
            stage.n = try std.math.mul(u32, threads, split.split_count);
            stage.mode = request.query_rows;
            stage.reserved = @bitCast(split.split_count);
            var reduce = params;
            reduce.op = 3;
            reduce.mode = request.query_rows;
            reduce.reserved = @bitCast(split.split_count);
            const reduce_inputs = [_]Ref{ split.scratch, self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), self.status.ref(), self.core.dummy.ref() };
            if (owner_runtime) |runtime| {
                try self.core.runReplaySplitPairOnActiveRuntime(runtime, stage, &inputs, split.scratch, reduce, &reduce_inputs, output);
            } else {
                _ = try self.core.runPair(stage, &inputs, split.scratch, reduce, &reduce_inputs, output, .attention);
                self.status_readback_bytes +|= 4;
                if (std.mem.bytesToValue(u32, self.status.bytes[0..4]) != 0) return error.NonFiniteContextAttention;
                try self.check();
            }
            self.split_attention_dispatches +|= 1;
        } else {
            if (owner_runtime) |runtime|
                try self.dispatchOnActiveRuntime(runtime, params, &inputs, output)
            else
                try self.dispatch(params, &inputs, output);
            self.serial_attention_dispatches +|= 1;
        }
    }
};

test "context replay allocation requires dense exact owners and bounded private capacity" {
    var plan = try storage.Plan.init(std.testing.allocator, .{ .head_dim = 256, .kv_heads = 2, .continuation_rows = 2, .max_retained_rows = 6 }, &.{.{ .logical_count = 4, .retained = &.{ 4, 4 } }});
    defer plan.deinit();
    const sizes = try allocation(plan.descriptor, 4, 8);
    try std.testing.expectEqual(@as(usize, 4096), sizes.tail_bytes);
    try std.testing.expect(sizes.physical_bytes > 2 * sizes.tail_bytes);
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(plan.descriptor, 0, 8));
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(plan.descriptor, 4, 7));
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(plan.descriptor, 8193, 10000));
    plan.spans[0].physical_start = 1;
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(plan.descriptor, 4, 8));
    plan.spans[0].physical_start = 0;
    var compressed = try storage.Plan.init(std.testing.allocator, .{ .head_dim = 512, .kv_heads = 1, .continuation_rows = 0, .max_retained_rows = 257 }, &.{ .{ .logical_count = 512, .retained = &.{256} }, .{ .logical_count = 1, .retained = &.{1} } });
    defer compressed.deinit();
    try std.testing.expectError(error.InvalidReferenceReplay, allocation(compressed.descriptor, 4, 517));
}

fn halves(buffer: Buffer) []f16 {
    return std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(buffer.bytes[0..buffer.logical_bytes])));
}

fn fillOracle(a: std.mem.Allocator, keys: []const f16, values: []const f16, queries: []const f32, expected: []f32, dim: usize, kv_heads: usize, request: Request) !void {
    const score_count = try std.math.add(usize, @intCast(request.query_start), @intCast(request.query_rows));
    const scores = try a.alloc(f64, score_count);
    defer a.free(scores);
    for (0..request.query_rows) |row| for (0..request.query_heads) |head| {
        const position = request.query_start + row;
        const query = queries[(row * request.query_heads + head) * dim ..][0..dim];
        const owner = head / (request.query_heads / kv_heads);
        var maximum: f64 = -std.math.inf(f64);
        for (0..position + 1) |token| {
            const key = keys[(token * kv_heads + owner) * dim ..][0..dim];
            var score: f64 = 0;
            for (query, key) |q, k| score += @as(f64, q) * @as(f64, k);
            score *= request.scale;
            if (request.logit_softcap > 0) score = request.logit_softcap * std.math.tanh(score / request.logit_softcap);
            scores[token] = score;
            maximum = @max(maximum, score);
        }
        var mass: f64 = 0;
        for (scores[0 .. position + 1]) |*score| {
            score.* = @exp(score.* - maximum);
            mass += score.*;
        }
        for (0..dim) |d| {
            var weighted: f64 = 0;
            for (scores[0 .. position + 1], 0..) |probability, token| weighted += probability / mass * @as(f64, values[(token * kv_heads + owner) * dim + d]);
            expected[(row * request.query_heads + head) * dim + d] = @floatCast(weighted);
        }
    };
}

fn compareOracle(keys: []const f16, values: []const f16, queries: []const f32, output: []const f32, dim: usize, kv_heads: usize, request: Request) !void {
    const elements = try std.math.mul(usize, try std.math.mul(usize, request.query_rows, request.query_heads), dim);
    try std.testing.expect(output.len >= elements);
    const expected = try std.testing.allocator.alloc(f32, elements);
    defer std.testing.allocator.free(expected);
    try fillOracle(std.testing.allocator, keys, values, queries, expected, dim, kv_heads, request);
    for (expected, output[0..elements]) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
}

test "context Metal replay shares original pages and matches causal FP64 attention after owner retirement" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    for ([_]u32{ 256, 512 }) |dim| {
        const base_rows = 5;
        const tail_rows = 3;
        const kv_heads = 2;
        const query_heads = 8;
        var copier = try storage.Copier.init();
        defer copier.deinit();
        const full_keys = try allocator.alloc(f16, (base_rows + tail_rows) * kv_heads * dim);
        defer allocator.free(full_keys);
        const full_values = try allocator.alloc(f16, full_keys.len);
        defer allocator.free(full_values);
        for (full_keys, full_values, 0..) |*key, *value, index| {
            key.* = @floatCast((@as(f32, @floatFromInt((index * 7) % 29)) - 14) / 16);
            value.* = @floatCast((@as(f32, @floatFromInt((index * 13) % 31)) - 15) / 8);
        }
        var plan = try storage.Plan.init(allocator, .{ .head_dim = dim, .kv_heads = kv_heads, .continuation_rows = 2, .max_retained_rows = 7 }, &.{.{ .logical_count = base_rows, .retained = &.{ base_rows, base_rows } }});
        var original = storage.Generation.init(&copier, plan, 1024 * 1024, 0) catch |err| {
            plan.deinit();
            return err;
        };
        var original_live = true;
        defer if (original_live) original.deinit();
        @memset(original.keys.bytes, 0);
        @memset(original.values.bytes, 0);
        for (original.plan.heads, 0..) |head, h| for (0..base_rows) |row| {
            @memcpy(halves(original.keys)[(head.kv_start + row) * dim ..][0..dim], full_keys[(row * kv_heads + h) * dim ..][0..dim]);
            @memcpy(halves(original.values)[(head.kv_start + row) * dim ..][0..dim], full_values[(row * kv_heads + h) * dim ..][0..dim]);
        };
        @memset(original.filled, true);
        try original.finish();
        const sizes = try allocation(original.plan.descriptor, tail_rows, base_rows + tail_rows);
        var config = Config{ .tail_capacity = tail_rows, .position_limit = base_rows + tail_rows, .physical_budget_bytes = sizes.physical_bytes };
        config.physical_budget_bytes -= 1;
        try std.testing.expectError(error.ContextReplayBudgetExceeded, Replay.init(&original, config));
        try std.testing.expect(!original.hasReplayReaders());
        config.physical_budget_bytes += 1;
        var replay = try Replay.init(&original, config);
        defer replay.deinit();
        var failed_branch = try Replay.init(&original, config);
        defer failed_branch.deinit();
        try std.testing.expectEqual(original.keys.handle, replay.lease.source.keys.handle);
        const key_hash = std.hash.Wyhash.hash(0, original.keys.bytes);
        const value_hash = std.hash.Wyhash.hash(0, original.values.bytes);
        var projected_keys = try Buffer.init(replay.core.runtime, tail_rows * kv_heads * dim * 4);
        defer projected_keys.deinit();
        var projected_values = try Buffer.init(replay.core.runtime, projected_keys.logical_bytes);
        defer projected_values.deinit();
        for (projected_keys.floats(), projected_values.floats(), 0..) |*key, *value, index| {
            key.* = full_keys[base_rows * kv_heads * dim + index];
            value.* = full_values[base_rows * kv_heads * dim + index];
        }
        try replay.append(base_rows, 2, projected_keys.ref(), projected_values.ref());
        try std.testing.expectEqual(@as(u32, 2), replay.tail_rows);
        try std.testing.expectError(error.InvalidReferenceReplay, replay.append(base_rows, 1, projected_keys.ref(), projected_values.ref()));
        // A nonfinite private branch cannot poison original KV or its sibling.
        const saved = projected_values.floats()[0];
        projected_values.floats()[0] = std.math.inf(f32);
        try std.testing.expectError(error.NonFiniteContextAttention, failed_branch.append(base_rows, 1, projected_keys.ref(), projected_values.ref()));
        try std.testing.expectEqual(@as(u32, 0), failed_branch.tail_rows);
        try std.testing.expectError(error.ContextSessionFailed, failed_branch.append(base_rows, 1, projected_keys.ref(), projected_values.ref()));
        projected_values.floats()[0] = saved;
        try std.testing.expectEqual(key_hash, std.hash.Wyhash.hash(0, original.keys.bytes));
        try std.testing.expectEqual(value_hash, std.hash.Wyhash.hash(0, original.values.bytes));
        original.deinit();
        original_live = false;
        var queries = try Buffer.init(replay.core.runtime, 2 * query_heads * dim * 4);
        defer queries.deinit();
        var output = try Buffer.init(replay.core.runtime, queries.logical_bytes);
        defer output.deinit();
        const split_plan = try splitPlan(2, query_heads, dim, 8);
        var split_scratch = try Buffer.init(replay.core.runtime, split_plan.scratch_bytes);
        defer split_scratch.deinit();
        for (queries.floats(), 0..) |*q, index| q.* = (@as(f32, @floatFromInt((index * 5) % 19)) - 9) / 8;
        var request = Request{ .query_start = base_rows, .query_rows = 2, .query_heads = query_heads, .scale = 1.0 / @sqrt(@as(f32, @floatFromInt(dim))) };
        try std.testing.expectError(error.AliasedContextStaging, replay.attention(request, queries.ref(), queries.ref()));
        for ([_]f32{ 0, 2 }) |cap| {
            request.logit_softcap = cap;
            try replay.attention(request, queries.ref(), output.ref());
            try compareOracle(full_keys, full_values, queries.floats(), output.floats(), dim, kv_heads, request);
            request.split_attention = .{ .scratch = split_scratch.ref(), .split_count = 8 };
            try replay.attention(request, queries.ref(), output.ref());
            try compareOracle(full_keys, full_values, queries.floats(), output.floats(), dim, kv_heads, request);
            request.split_attention = null;
        }
        try std.testing.expectEqual(@as(u64, 2), replay.serial_attention_dispatches);
        try std.testing.expectEqual(@as(u64, 2), replay.split_attention_dispatches);
        request.split_attention = .{ .scratch = output.ref(), .split_count = 8 };
        try std.testing.expectError(error.InvalidReferenceReplay, replay.attention(request, queries.ref(), output.ref()));
        // Isolate alias validation from scratch capacity: both refs cover the
        // full scratch allocation, which is also large enough for the output.
        request.split_attention = .{ .scratch = split_scratch.ref(), .split_count = 8 };
        try std.testing.expectError(error.AliasedContextStaging, replay.attention(request, queries.ref(), split_scratch.ref()));
        request.split_attention = null;
        // Two split pairs share scratch in one owner frame, and the second
        // consumes the first pair's output. This makes both scratch WAR and
        // downstream output RAW ordering observable instead of testing two
        // independent dispatches whose order would not affect the result.
        const vector_elements = query_heads * dim;
        var chained_first = try Buffer.init(replay.core.runtime, vector_elements * @sizeOf(f32));
        defer chained_first.deinit();
        var chained_second = try Buffer.init(replay.core.runtime, vector_elements * @sizeOf(f32));
        defer chained_second.deinit();
        @memset(chained_first.floats(), -1234);
        @memset(chained_second.floats(), 4321);
        const first_request = Request{ .query_start = base_rows, .query_rows = 1, .query_heads = query_heads, .scale = request.scale, .logit_softcap = 2, .split_attention = .{ .scratch = split_scratch.ref(), .split_count = 8 } };
        const second_request = Request{ .query_start = base_rows + 1, .query_rows = 1, .query_heads = query_heads, .scale = request.scale, .logit_softcap = 2, .split_attention = .{ .scratch = split_scratch.ref(), .split_count = 8 } };
        const expected_first = try allocator.alloc(f32, vector_elements);
        defer allocator.free(expected_first);
        const expected_second = try allocator.alloc(f32, vector_elements);
        defer allocator.free(expected_second);
        try fillOracle(allocator, full_keys, full_values, queries.floats()[0..vector_elements], expected_first, dim, kv_heads, first_request);
        try fillOracle(allocator, full_keys, full_values, expected_first, expected_second, dim, kv_heads, second_request);
        try mr.beginFrame(replay.core.runtime);
        var invalid_request = first_request;
        invalid_request.split_attention = .{ .scratch = split_scratch.ref(), .split_count = 8 };
        try std.testing.expectError(error.AliasedContextStaging, replay.attentionOnActiveRuntime(replay.core.runtime, invalid_request, try queries.ref().slice(0, vector_elements * @sizeOf(f32)), split_scratch.ref()));
        try replay.attentionOnActiveRuntime(replay.core.runtime, first_request, try queries.ref().slice(0, vector_elements * @sizeOf(f32)), chained_first.ref());
        try replay.attentionOnActiveRuntime(replay.core.runtime, second_request, chained_first.ref(), chained_second.ref());
        try mr.submitFrame(replay.core.runtime);
        try mr.waitFrame(replay.core.runtime);
        try std.testing.expectEqual(@as(u32, 0), std.mem.bytesToValue(u32, replay.status.bytes[0..4]));
        for (expected_first, chained_first.floats()) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        for (expected_second, chained_second.floats()) |want, got| try std.testing.expectApproxEqAbs(want, got, 3e-5);
        try std.testing.expectEqual(@as(u64, 4), replay.split_attention_dispatches);
        const offset = 2 * kv_heads * dim * 4;
        try replay.append(base_rows + 2, 1, try projected_keys.ref().slice(offset, kv_heads * dim * 4), try projected_values.ref().slice(offset, kv_heads * dim * 4));
        request.query_start = base_rows + 2;
        request.query_rows = 1;
        request.split_attention = .{ .scratch = split_scratch.ref(), .split_count = 8 };
        const unused_output_hash = std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(output.floats()[vector_elements..]));
        try replay.attention(request, queries.ref(), output.ref());
        try compareOracle(full_keys, full_values, queries.floats(), output.floats(), dim, kv_heads, request);
        try std.testing.expectEqual(unused_output_hash, std.hash.Wyhash.hash(0, std.mem.sliceAsBytes(output.floats()[vector_elements..])));
        try std.testing.expectEqual(@as(u64, 5), replay.split_attention_dispatches);
        try std.testing.expectEqual(@as(u64, tail_rows * kv_heads * dim * 4), replay.tail_write_bytes);
        try std.testing.expectError(error.InvalidReferenceReplay, replay.append(base_rows + tail_rows, 1, projected_keys.ref(), projected_values.ref()));
        var stop = std.atomic.Value(bool).init(true);
        replay.config.cancelled = &stop;
        try std.testing.expectError(error.Cancelled, replay.attention(request, queries.ref(), output.ref()));
        stop.store(false, .release);
        queries.floats()[0] = std.math.nan(f32);
        try std.testing.expectError(error.NonFiniteContextAttention, replay.attention(request, queries.ref(), output.ref()));
        // Failed dispatches never become route-proof successes.
        try std.testing.expectEqual(@as(u64, 5), replay.split_attention_dispatches);
    }
}

fn benchmarkEnabled() bool {
    const value = std.c.getenv("TERMITE_METAL_REPLAY_SPLIT_8K_BENCH") orelse return false;
    return std.mem.eql(u8, std.mem.span(value), "1");
}

fn benchmarkMaxSplitsValue(value: ?[]const u8) !u32 {
    const text = value orelse return 16;
    if (std.mem.eql(u8, text, "16")) return 16;
    if (std.mem.eql(u8, text, "32")) return 32;
    return error.InvalidReferenceReplay;
}

fn benchmarkMaxSplits() !u32 {
    const value = std.c.getenv("TERMITE_METAL_REPLAY_SPLIT_8K_MAX_SPLITS");
    return benchmarkMaxSplitsValue(if (value) |text| std.mem.span(text) else null);
}

test "context replay split benchmark max count selector is strict and defaults to 16" {
    try std.testing.expectEqual(@as(u32, 16), try benchmarkMaxSplitsValue(null));
    try std.testing.expectEqual(@as(u32, 16), try benchmarkMaxSplitsValue("16"));
    try std.testing.expectEqual(@as(u32, 32), try benchmarkMaxSplitsValue("32"));
    for ([_][]const u8{ "", "0", "8", "64", "016", " 32", "32 " }) |invalid|
        try std.testing.expectError(error.InvalidReferenceReplay, benchmarkMaxSplitsValue(invalid));
}

fn benchmarkNow() !u64 {
    var ts: std.c.timespec = undefined;
    if (std.c.clock_gettime(.MONOTONIC, &ts) != 0) return error.MonotonicClockUnavailable;
    return @as(u64, @intCast(ts.sec)) * 1_000_000_000 + @as(u64, @intCast(ts.nsec));
}

fn benchmarkAttention(replay: *Replay, request: Request, queries: Ref, output: Ref) !u64 {
    const start = try benchmarkNow();
    try mr.beginFrame(replay.core.runtime);
    replay.attentionOnActiveRuntime(replay.core.runtime, request, queries, output) catch |err| {
        _ = mr.cancelFrame(replay.core.runtime) catch {};
        return err;
    };
    mr.submitFrame(replay.core.runtime) catch |err| {
        _ = mr.cancelFrame(replay.core.runtime) catch {};
        return err;
    };
    try mr.waitFrame(replay.core.runtime);
    const elapsed = (try benchmarkNow()) - start;
    if (std.mem.bytesToValue(u32, replay.status.bytes[0..4]) != 0) return error.NonFiniteContextAttention;
    return elapsed;
}

fn outputError(reference: []const f32, candidate: []const f32) !struct { max_abs: f64, relative_rmse: f64 } {
    if (reference.len != candidate.len or reference.len == 0) return error.InvalidReferenceReplay;
    var max_abs: f64 = 0;
    var squared_error: f64 = 0;
    var squared_reference: f64 = 0;
    for (reference, candidate) |want, got| {
        if (!std.math.isFinite(want) or !std.math.isFinite(got)) return error.NonFiniteContextAttention;
        const delta = @as(f64, got) - @as(f64, want);
        max_abs = @max(max_abs, @abs(delta));
        squared_error += delta * delta;
        squared_reference += @as(f64, want) * @as(f64, want);
    }
    return .{ .max_abs = max_abs, .relative_rmse = @sqrt(squared_error / @max(squared_reference, 1e-30)) };
}

fn medianFour(values: *[4]u64) u64 {
    std.mem.sort(u64, values, {}, std.sort.asc(u64));
    return values[1] / 2 + values[2] / 2 + (values[1] % 2 + values[2] % 2) / 2;
}

test "context Metal replay 8K bounded split benchmark preserves serial and FP64 outputs" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!benchmarkEnabled()) return error.SkipZigTest;
    const a = std.testing.allocator;
    const base_rows: u32 = 8192;
    const tail_rows: u32 = 8;
    const kv_heads: u32 = 2;
    const query_heads: u32 = 8;
    const max_splits = try benchmarkMaxSplits();
    const candidate_splits = [_]u32{ 4, 8, 16, 32 };
    const split_counts = candidate_splits[0..if (max_splits == 32) candidate_splits.len else candidate_splits.len - 1];
    for ([_]u32{ 256, 512 }) |dim| {
        var copier = try storage.Copier.init();
        defer copier.deinit();
        var plan = try storage.Plan.init(a, .{ .head_dim = dim, .kv_heads = kv_heads, .continuation_rows = tail_rows, .max_retained_rows = base_rows + tail_rows }, &.{.{ .logical_count = base_rows, .retained = &.{ base_rows, base_rows } }});
        var original = storage.Generation.init(&copier, plan, std.math.maxInt(usize), 0) catch |err| {
            plan.deinit();
            return err;
        };
        defer original.deinit();
        for (original.plan.heads, 0..) |head, h| for (0..base_rows) |row| {
            for (0..dim) |d| {
                halves(original.keys)[(head.kv_start + row) * dim + d] = @floatCast((@as(f32, @floatFromInt((row * 13 + h * 7 + d * 3) % 37)) - 18) / 32);
                halves(original.values)[(head.kv_start + row) * dim + d] = @floatCast((@as(f32, @floatFromInt((row * 5 + h * 11 + d * 17) % 41)) - 20) / 16);
            }
        };
        @memset(original.filled, true);
        try original.finish();
        const allocation_size = try allocation(original.plan.descriptor, tail_rows, base_rows + tail_rows);
        var replay = try Replay.init(&original, .{ .tail_capacity = tail_rows, .position_limit = base_rows + tail_rows, .physical_budget_bytes = allocation_size.physical_bytes });
        defer replay.deinit();
        var projected_keys = try Buffer.init(replay.core.runtime, @as(usize, tail_rows) * kv_heads * dim * @sizeOf(f32));
        defer projected_keys.deinit();
        var projected_values = try Buffer.init(replay.core.runtime, projected_keys.logical_bytes);
        defer projected_values.deinit();
        for (projected_keys.floats(), projected_values.floats(), 0..) |*key, *value, index| {
            key.* = (@as(f32, @floatFromInt((index * 19) % 43)) - 21) / 32;
            value.* = (@as(f32, @floatFromInt((index * 23) % 47)) - 23) / 16;
        }
        try replay.append(base_rows, tail_rows, projected_keys.ref(), projected_values.ref());
        const full_keys = try a.alloc(f16, @as(usize, base_rows + tail_rows) * kv_heads * dim);
        defer a.free(full_keys);
        const full_values = try a.alloc(f16, full_keys.len);
        defer a.free(full_values);
        for (original.plan.heads, 0..) |head, h| for (0..base_rows) |row| {
            @memcpy(full_keys[(@as(usize, row) * kv_heads + h) * dim ..][0..dim], halves(original.keys)[(head.kv_start + row) * dim ..][0..dim]);
            @memcpy(full_values[(@as(usize, row) * kv_heads + h) * dim ..][0..dim], halves(original.values)[(head.kv_start + row) * dim ..][0..dim]);
        };
        for (0..tail_rows) |row| for (0..kv_heads) |head| {
            for (0..dim) |d| {
                const index = (row * kv_heads + head) * dim + d;
                full_keys[((base_rows + row) * kv_heads + head) * dim + d] = @floatCast(projected_keys.floats()[index]);
                full_values[((base_rows + row) * kv_heads + head) * dim + d] = @floatCast(projected_values.floats()[index]);
            }
        };
        var queries = try Buffer.init(replay.core.runtime, @as(usize, tail_rows) * query_heads * dim * @sizeOf(f32));
        defer queries.deinit();
        var serial_output = try Buffer.init(replay.core.runtime, queries.logical_bytes);
        defer serial_output.deinit();
        var split_output = try Buffer.init(replay.core.runtime, queries.logical_bytes);
        defer split_output.deinit();
        for (queries.floats(), 0..) |*q, index| q.* = (@as(f32, @floatFromInt((index * 29) % 53)) - 26) / 32;
        const scratch_size = try splitScratchBytes(tail_rows, query_heads, dim, max_splits);
        var scratch = try Buffer.init(replay.core.runtime, scratch_size);
        defer scratch.deinit();
        const scratch_physical_bytes = shared.termite_metal_buffer_allocated_bytes(scratch.handle);
        try std.testing.expectEqual(@as(u64, @intCast(scratch.bytes.len)), scratch_physical_bytes);
        for ([_]u32{ 1, 4, 8 }) |rows| {
            const serial_request = Request{ .query_start = base_rows, .query_rows = rows, .query_heads = query_heads, .scale = 1.0 / @sqrt(@as(f32, @floatFromInt(dim))), .logit_softcap = 50 };
            var oracle_checked = false;
            for (split_counts) |splits| {
                var split_request = serial_request;
                split_request.split_attention = .{ .scratch = scratch.ref(), .split_count = splits };
                const serial_before = replay.serial_attention_dispatches;
                const split_before = replay.split_attention_dispatches;
                // Warm both routes, then use a fixed ABBA order to remove a
                // systematic first-route thermal/order advantage.
                _ = try benchmarkAttention(&replay, serial_request, queries.ref(), serial_output.ref());
                _ = try benchmarkAttention(&replay, split_request, queries.ref(), split_output.ref());
                var serial_ns: [4]u64 = undefined;
                var split_ns: [4]u64 = undefined;
                serial_ns[0] = try benchmarkAttention(&replay, serial_request, queries.ref(), serial_output.ref());
                split_ns[0] = try benchmarkAttention(&replay, split_request, queries.ref(), split_output.ref());
                split_ns[1] = try benchmarkAttention(&replay, split_request, queries.ref(), split_output.ref());
                serial_ns[1] = try benchmarkAttention(&replay, serial_request, queries.ref(), serial_output.ref());
                split_ns[2] = try benchmarkAttention(&replay, split_request, queries.ref(), split_output.ref());
                serial_ns[2] = try benchmarkAttention(&replay, serial_request, queries.ref(), serial_output.ref());
                serial_ns[3] = try benchmarkAttention(&replay, serial_request, queries.ref(), serial_output.ref());
                split_ns[3] = try benchmarkAttention(&replay, split_request, queries.ref(), split_output.ref());
                const serial_dispatches = replay.serial_attention_dispatches - serial_before;
                const split_dispatches = replay.split_attention_dispatches - split_before;
                try std.testing.expectEqual(@as(u64, 5), serial_dispatches);
                try std.testing.expectEqual(@as(u64, 5), split_dispatches);
                const serial_median = medianFour(&serial_ns);
                const split_median = medianFour(&split_ns);
                const output_len = @as(usize, rows) * query_heads * dim;
                if (rows == 1 and !oracle_checked) {
                    const oracle_output = try a.alloc(f32, output_len);
                    defer a.free(oracle_output);
                    try fillOracle(a, full_keys, full_values, queries.floats(), oracle_output, dim, kv_heads, serial_request);
                    const oracle_error = try outputError(oracle_output, serial_output.floats()[0..output_len]);
                    try std.testing.expect(oracle_error.max_abs <= 0.0003);
                    try std.testing.expect(oracle_error.relative_rmse <= 0.0005);
                    oracle_checked = true;
                }
                const err = try outputError(serial_output.floats()[0..output_len], split_output.floats()[0..output_len]);
                try std.testing.expect(err.max_abs <= 0.0003);
                try std.testing.expect(err.relative_rmse <= 0.0005);
                std.debug.print("replay_split_8k: {{\"dim\":{d},\"rows\":{d},\"splits\":{d},\"serial_median_ns\":{d},\"split_median_ns\":{d},\"max_abs\":{d},\"relative_rmse\":{d},\"scratch_bytes\":{d},\"scratch_physical_bytes\":{d},\"serial_dispatches\":{d},\"split_dispatches\":{d}}}\n", .{ dim, rows, splits, serial_median, split_median, err.max_abs, err.relative_rmse, try splitScratchBytes(rows, query_heads, dim, splits), scratch_physical_bytes, serial_dispatches, split_dispatches });
            }
        }
    }
}
