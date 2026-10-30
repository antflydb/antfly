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

//! Bounded Metal P/Y acceleration for the common checkpoint compaction engine.
//! Metal's FP32 attention frontend feeds shared FP64 OMP/NNLS with bounded
//! Accelerate Householder solves. The native direct KKT and FP64 publication
//! validation remain unchanged.
//! No CPU attention fallback or CUDA context is used by this adapter.
const std = @import("std");
const shared = @import("../backends/metal_generated_kernels.zig");
const solver = @import("../runtime/kv/attention_match.zig");
const engine = @import("../runtime/kv/attention_match_engine.zig");
const fp64 = @import("metal_attention_match_lapack.zig");

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.CompactionWorkspaceLimit;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.CompactionWorkspaceLimit;
}
fn rounded(bytes: usize) !usize {
    const page = std.heap.pageSize();
    return (try add(bytes, page - 1)) & ~(page - 1);
}

pub const Fitter = struct {
    /// Largest query matrix accepted by one fitting dispatch. The production
    /// E2B global owner has forty physical consumer strata: 256 training rows
    /// per stratum produce 10,240 rows. Keep headroom for supported layouts;
    /// the solver's unchanged byte budget remains the authoritative limit.
    pub const max_query_rows: usize = 16384;
    const query_chunk_rows: usize = 16;

    kernels: shared.Kernels,
    max_device_bytes: usize,
    derived_readback_bytes: u64 = 0,
    dispatches: u64 = 0,
    accelerated_value_solves: u64 = 0,

    pub fn init(allocator: std.mem.Allocator, tracker: ?*shared.BufferAllocationTracker, max_device_bytes: usize) !Fitter {
        _ = allocator;
        if (max_device_bytes < 6 * std.heap.pageSize()) return error.CompactionWorkspaceLimit;
        return .{
            .kernels = try shared.Kernels.initSourceTracked(@embedFile("metal_attention_match_fit.metal"), "metal_attention_match_fit", tracker),
            .max_device_bytes = max_device_bytes,
        };
    }

    pub fn initWorker(allocator: std.mem.Allocator, tracker: ?*shared.BufferAllocationTracker, max_device_bytes: usize, workspace_limit: usize) !Fitter {
        if (workspace_limit == 0 or workspace_limit > max_device_bytes) return error.CompactionWorkspaceLimit;
        return init(allocator, tracker, workspace_limit);
    }

    pub fn deinit(self: *Fitter) void {
        self.kernels.deinit();
        self.* = undefined;
    }

    /// Save counters before deinit invalidates the fitter. Preparation retires
    /// workers before physical measurements and cannot inspect them afterward.
    pub fn deinitAndTakeValueSolves(self: *Fitter) u64 {
        const count = self.accelerated_value_solves;
        self.deinit();
        return count;
    }

    pub fn releaseWorkspace(_: *Fitter) void {
        // Every synchronous call frees its admitted buffers before returning.
    }

    pub fn backend(self: *Fitter) solver.Backend {
        return backendForContext(self);
    }

    fn backendForContext(context: ?*anyopaque) solver.Backend {
        return .{
            .context = context,
            .validation_batch = .rows_64,
            .workspace_fn = workspace,
            .attend_fn = attend,
            .summary_fn = summary,
            .values_fn = if (fp64.supported) solveValuesAccelerated else null,
            .select_fn = if (fp64.supported) fp64.select else null,
        };
    }

    fn solveValuesAccelerated(context: ?*anyopaque, allocator: std.mem.Allocator, a: []const f64, y: []const f64, q: usize, m: usize, d: usize, ridge: f64, control: solver.Control, out: []f64) !void {
        const self: *Fitter = @ptrCast(@alignCast(context orelse return error.InvalidCallbackContext));
        try fp64.values(null, allocator, a, y, q, m, d, ridge, control, out);
        self.accelerated_value_solves +|= 1;
    }

    pub fn headHostWorkspace(original_rows: usize, target_rows: usize, block_rows: usize, train_elements: usize, heldout_elements: usize, dim: usize, options: engine.Options) !engine.HeadWorkspace {
        var planned = options;
        planned.fit.backend = backendForContext(null);
        return engine.headWorkspaceBytes(original_rows, target_rows, block_rows, train_elements, heldout_elements, dim, planned);
    }

    pub const ParallelPlan = struct {
        workers: usize,
        resident_bytes: usize,
        per_worker_bytes: usize,
        admitted_bytes: usize,
    };

    /// Select independently owned Metal fitters under one aggregate host bound.
    /// The common engine accounts source + worst-case candidate in `resident`;
    /// each admitted worker then receives exactly the largest planned head
    /// workspace, never the whole request reservation.
    pub fn parallelPlan(source_bytes: usize, head_count: usize, head_workspace_bytes: usize, aggregate_limit: usize, maximum_workers: usize) !ParallelPlan {
        if (maximum_workers == 0 or maximum_workers > 4) return error.InvalidFitConfig;
        const resident = try engine.parallelResidentBytes(source_bytes, head_count);
        if (resident > aggregate_limit) return error.CompactionWorkspaceLimit;
        if (head_workspace_bytes == 0) return .{
            .workers = 1,
            .resident_bytes = resident,
            .per_worker_bytes = 0,
            .admitted_bytes = resident,
        };
        if (head_count == 0) return error.InvalidFitConfig;
        const capacity = (aggregate_limit - resident) / head_workspace_bytes;
        if (capacity == 0) return error.CompactionWorkspaceLimit;
        const workers = @min(@min(maximum_workers, head_count), capacity);
        const admitted = try engine.parallelHostBytes(source_bytes, head_count, head_workspace_bytes, workers);
        if (admitted > aggregate_limit) return error.CompactionWorkspaceLimit;
        return .{
            .workers = workers,
            .resident_bytes = resident,
            .per_worker_bytes = head_workspace_bytes,
            .admitted_bytes = admitted,
        };
    }

    fn workspace(_: ?*anyopaque, n: usize, _: usize, q: usize, d: usize) !usize {
        if (n == 0 or n > 131072 or q == 0 or q > max_query_rows or d == 0 or d > 1024) return error.InvalidAttentionShape;
        const kv = try rounded(try mul(try mul(n, d), 4));
        const batch_rows = @min(q, query_chunk_rows);
        const queries = try rounded(try mul(try mul(batch_rows, d), 4));
        const bias = try rounded(try mul(n, 4));
        const result = try rounded(try mul(try mul(batch_rows, try add(try add(n, d), 1)), 4));
        // Shared-buffer backing is physical host memory on Metal. Include it
        // in the engine's transient admission as well as the per-call cap.
        return add(try add(try add(try mul(kv, 2), queries), try add(bias, result)), std.heap.pageSize());
    }

    fn attend(raw: ?*anyopaque, _: std.mem.Allocator, head: solver.Head, queries: []const f64, probabilities: []f64, targets: ?[]f64, masses: ?[]f64, control: solver.Control) !void {
        return attendImpl(raw, head, queries, probabilities, targets, masses, control);
    }

    fn summary(raw: ?*anyopaque, _: std.mem.Allocator, head: solver.Head, queries: []const f64, targets: []f64, masses: []f64, control: solver.Control) !void {
        return attendImpl(raw, head, queries, null, targets, masses, control);
    }

    fn fill(out: []f32, input: []const f64, control: solver.Control) !void {
        if (out.len != input.len) return error.InvalidAttentionShape;
        for (input, out, 0..) |value, *destination, i| {
            if (i % 4096 == 0) try control.check();
            if (!std.math.isFinite(value) or @abs(value) > std.math.floatMax(f32)) return error.NonFiniteAttention;
            destination.* = @floatCast(value);
        }
    }

    fn attendImpl(raw: ?*anyopaque, head: solver.Head, queries: []const f64, probabilities: ?[]f64, targets: ?[]f64, masses: ?[]f64, control: solver.Control) !void {
        const self: *Fitter = @ptrCast(@alignCast(raw orelse return error.InvalidAttentionShape));
        try control.check();
        try head.validate();
        if (queries.len == 0 or queries.len % head.dim != 0) return error.InvalidAttentionShape;
        const q = queries.len / head.dim;
        if (probabilities) |out| if (out.len != try mul(q, head.rows)) return error.InvalidAttentionShape;
        if (targets) |out| if (out.len != queries.len) return error.InvalidAttentionShape;
        if (masses) |out| if (out.len != q) return error.InvalidAttentionShape;
        const bytes = try workspace(null, head.rows, 1, q, head.dim);
        if (bytes > self.max_device_bytes) return error.CompactionWorkspaceLimit;
        if (head.scale > std.math.floatMax(f32) or head.softcap > std.math.floatMax(f32)) return error.NonFiniteAttention;
        const scale: f32 = @floatCast(head.scale);
        const softcap: f32 = @floatCast(head.softcap);
        if (scale <= 0 or (head.softcap > 0 and softcap <= 0)) return error.NonFiniteAttention;
        var keys = try self.kernels.buffer(try mul(head.keys.len, 4));
        defer keys.deinit();
        var values = try self.kernels.buffer(try mul(head.values.len, 4));
        defer values.deinit();
        const batch_rows = @min(q, query_chunk_rows);
        var query = try self.kernels.buffer(try mul(try mul(batch_rows, head.dim), 4));
        defer query.deinit();
        var bias = try self.kernels.buffer(try mul(head.rows, 4));
        defer bias.deinit();
        var result = try self.kernels.buffer(try mul(try mul(batch_rows, try add(try add(head.rows, head.dim), 1)), 4));
        defer result.deinit();
        try fill(keys.floats(), head.keys, control);
        try fill(values.floats(), head.values, control);
        for (queries, 0..) |value, i| {
            if (i % 4096 == 0) try control.check();
            if (!std.math.isFinite(value) or @abs(value) > std.math.floatMax(f32)) return error.NonFiniteAttention;
        }
        if (head.bias) |input| try fill(bias.floats(), input, control) else @memset(bias.floats(), 0);
        // Fence each bounded query chunk before polling cancellation. The
        // device buffers cover only one chunk. Scatter each fenced result into
        // the complete caller-owned matrices used by the shared host solver.
        var offset: usize = 0;
        while (offset < q) {
            try control.check();
            const rows = @min(query_chunk_rows, q - offset);
            const query_elements = try mul(rows, head.dim);
            try fill(query.floats()[0..query_elements], queries[offset * head.dim ..][0..query_elements], control);
            _ = try self.kernels.runThreadgroupsPipeline(self.kernels.pipeline, .{
                .op = 0,
                .n = @intCast(head.rows),
                .dim = @intCast(head.dim),
                .groups = @intCast(rows),
                .scale = scale,
                .reserved = softcap,
            }, &.{ keys.ref(), values.ref(), query.ref(), bias.ref() }, result.ref(), .{ rows, 1, 1 }, .{ 128, 1, 1 });
            self.dispatches += 1;
            try control.check();
            const pn = try mul(rows, head.rows);
            const result_elements = try add(try add(pn, query_elements), rows);
            const output = result.floats()[0..result_elements];
            for (output, 0..) |value, i| {
                if (i % 4096 == 0) try control.check();
                if (!std.math.isFinite(value)) return error.NonFiniteAttention;
            }
            if (probabilities) |out| for (out[offset * head.rows ..][0..pn], output[0..pn]) |*destination, value| {
                destination.* = value;
            };
            if (targets) |out| for (out[offset * head.dim ..][0..query_elements], output[pn..][0..query_elements]) |*destination, value| {
                destination.* = value;
            };
            if (masses) |out| for (out[offset..][0..rows], output[pn + query_elements ..][0..rows]) |*destination, value| {
                destination.* = value;
            };
            self.derived_readback_bytes +|= try mul(result_elements, 4);
            offset += rows;
        }
    }
};

test "attention_match Metal checkpoint fitter matches FP64 with prior bias after softcap" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const head: solver.Head = .{
        .rows = 4,
        .dim = 2,
        .keys = &.{ 1, 0, 0, 1, -1, 0, 0, -1 },
        .values = &.{ 1, 2, 3, 4, 5, 6, 7, 8 },
        .bias = &.{ 0.25, -0.5, 1.25, -1.5 },
        .scale = 0.75,
        .softcap = 0.5,
    };
    const queries = [_]f64{ 1, 0.5, -1, 1, 2, -2 };
    var expected_p: [12]f64 = undefined;
    var expected_y: [6]f64 = undefined;
    var expected_z: [3]f64 = undefined;
    const config: solver.Config = .{ .target_rows = 2 };
    try solver.attendBatch(a, head, &queries, &expected_p, &expected_y, &expected_z, config);
    var fitter = try Fitter.init(a, null, 1024 * 1024);
    defer fitter.deinit();
    var p: [12]f64 = undefined;
    var y: [6]f64 = undefined;
    var z: [3]f64 = undefined;
    var accelerated = config;
    accelerated.backend = fitter.backend();
    try solver.attendBatch(a, head, &queries, &p, &y, &z, accelerated);
    for (expected_p, p) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
    for (expected_y, y) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
    for (expected_z, z) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
    try solver.attendSummary(a, head, &queries, &y, &z, accelerated);
    for (expected_y, y) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
    for (expected_z, z) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
    try std.testing.expectEqual(@as(u64, 2), fitter.dispatches);
    const before = fitter.dispatches;
    fitter.max_device_bytes = 1;
    try std.testing.expectError(error.CompactionWorkspaceLimit, solver.attendBatch(a, head, &queries, &p, &y, &z, accelerated));
    try std.testing.expectEqual(before, fitter.dispatches);
}

test "attention_match Metal checkpoint fitter includes shared backing in pure admission" {
    const bytes = try Fitter.workspace(null, 512, 128, 128, 512);
    try std.testing.expect(bytes > 512 * 512 * 8);
    // Gemma4 E2B's shared global owner contributes 40 consumer/GQA strata.
    // Every fifth study query is held out: 320 rows leave 256 for training.
    const e2b = try Fitter.workspace(null, 1024, 512, 40 * 256, 512);
    try std.testing.expect(e2b > bytes);
    try std.testing.expectEqual(try Fitter.workspace(null, 1024, 512, Fitter.query_chunk_rows, 512), e2b);
    const config: solver.Config = .{ .target_rows = 512, .backend = Fitter.backendForContext(null) };
    const solver_bytes = try solver.fitWorkspaceBytes(1024, 512, 40 * 256, 512, 40 * 64, config);
    try std.testing.expect(solver_bytes <= config.max_workspace_bytes);
    try std.testing.expectError(error.InvalidAttentionShape, Fitter.workspace(null, 1024, 512, Fitter.max_query_rows + 1, 512));
    try std.testing.expectError(error.InvalidAttentionShape, Fitter.workspace(null, 0, 1, 1, 512));
}

test "attention_match Metal parallel plan bounds independent workers under two GiB" {
    const aggregate = 2 * 1024 * 1024 * 1024;
    const source = 512 * 1024 * 1024;
    const workspace = 200 * 1024 * 1024;
    const plan = try Fitter.parallelPlan(source, 8, workspace, aggregate, 4);
    try std.testing.expectEqual(@as(usize, 4), plan.workers);
    try std.testing.expectEqual(workspace, plan.per_worker_bytes);
    try std.testing.expectEqual(
        try engine.parallelHostBytes(source, 8, workspace, 4),
        plan.admitted_bytes,
    );
    try std.testing.expect(plan.admitted_bytes <= aggregate);

    const one_worker_limit = try engine.parallelHostBytes(source, 8, workspace, 1);
    const serial = try Fitter.parallelPlan(source, 8, workspace, one_worker_limit, 4);
    try std.testing.expectEqual(@as(usize, 1), serial.workers);
    try std.testing.expectError(
        error.CompactionWorkspaceLimit,
        Fitter.parallelPlan(source, 8, workspace, one_worker_limit - 1, 4),
    );
    try std.testing.expectError(
        error.InvalidFitConfig,
        Fitter.parallelPlan(source, 8, workspace, aggregate, 5),
    );
    try std.testing.expectError(
        error.CompactionWorkspaceLimit,
        Fitter.parallelPlan(std.math.maxInt(usize), 8, workspace, aggregate, 4),
    );
}

test "attention_match Metal fitter bounds resident query chunks and preserves a ragged tail" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    const rows = 64;
    const dim = 64;
    const query_rows = 35;
    var keys: [rows * dim]f64 = undefined;
    var values: [rows * dim]f64 = undefined;
    var queries: [query_rows * dim]f64 = undefined;
    var bias: [rows]f64 = undefined;
    for (&keys, &values, 0..) |*k, *v, i| {
        k.* = (@as(f64, @floatFromInt(i % 11)) - 5) / 16;
        v.* = (@as(f64, @floatFromInt(i % 7)) - 3) / 8;
    }
    for (&queries, 0..) |*q, i| q.* = (@as(f64, @floatFromInt(i % 13)) - 6) / 8;
    for (&bias, 0..) |*b, i| b.* = if (i % 2 == 0) 0.25 else -0.5;
    const head: solver.Head = .{ .rows = rows, .dim = dim, .keys = &keys, .values = &values, .bias = &bias, .scale = 0.125, .softcap = 1.5 };
    var expected_p: [query_rows * rows]f64 = undefined;
    var expected_y: [query_rows * dim]f64 = undefined;
    var expected_z: [query_rows]f64 = undefined;
    var p: [query_rows * rows]f64 = undefined;
    var y: [query_rows * dim]f64 = undefined;
    var z: [query_rows]f64 = undefined;
    const budget = try Fitter.workspace(null, rows, 1, Fitter.query_chunk_rows, dim);
    var tracker = shared.BufferAllocationTracker.init(null, 0);
    {
        var fitter = try Fitter.init(a, &tracker, budget);
        defer fitter.deinit();
        const baseline = tracker.snapshot().shared_backing_live_bytes;
        var config: solver.Config = .{ .target_rows = 1 };
        try solver.attendBatch(a, head, &queries, &expected_p, &expected_y, &expected_z, config);
        config.backend = fitter.backend();
        try solver.attendBatch(a, head, &queries, &p, &y, &z, config);
        try std.testing.expectEqual(@as(u64, 3), fitter.dispatches);
        for (expected_p, p) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        for (expected_y, y) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        for (expected_z, z) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        try solver.attendSummary(a, head, &queries, &y, &z, config);
        try std.testing.expectEqual(@as(u64, 6), fitter.dispatches);
        for (expected_y, y) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        for (expected_z, z) |want, got| try std.testing.expectApproxEqAbs(want, got, 2e-5);
        try std.testing.expectEqual(@as(u64, 2 * query_rows * (rows + dim + 1) * 4), fitter.derived_readback_bytes);
        try std.testing.expect(tracker.snapshot().shared_backing_peak_bytes - baseline <= budget);
        try std.testing.expectEqual(baseline, tracker.snapshot().shared_backing_live_bytes);
    }
    try std.testing.expectEqual(@as(u64, 0), tracker.snapshot().shared_backing_live_bytes);
}

test "attention_match Metal fitter chunks queries and drains tracked buffers on cancellation" {
    if (!@import("build_options").enable_metal) return error.SkipZigTest;
    const a = std.testing.allocator;
    var tracker = shared.BufferAllocationTracker.init(null, 0);
    {
        var fitter = try Fitter.init(a, &tracker, 16 * 1024 * 1024);
        defer fitter.deinit();
        const baseline = tracker.snapshot().shared_backing_live_bytes;
        const head: solver.Head = .{
            .rows = 4,
            .dim = 2,
            .keys = &.{ 1, 0, 0, 1, -1, 0, 0, -1 },
            .values = &.{ 1, 2, 3, 4, 5, 6, 7, 8 },
            .scale = 0.75,
        };
        var queries: [34]f64 = undefined;
        for (&queries, 0..) |*value, i| value.* = @as(f64, @floatFromInt(i % 5)) - 2;
        var probabilities: [68]f64 = @splat(-123);
        var targets: [34]f64 = @splat(-123);
        var masses: [17]f64 = @splat(-123);
        const CancelAfterDispatch = struct {
            fn check(raw: ?*anyopaque) !void {
                const current: *Fitter = @ptrCast(@alignCast(raw.?));
                if (current.dispatches > 0) return error.Cancelled;
            }
        };
        var config: solver.Config = .{
            .target_rows = 2,
            .backend = fitter.backend(),
            .control = .{ .context = &fitter, .check_fn = CancelAfterDispatch.check },
        };
        try std.testing.expectError(error.Cancelled, solver.attendBatch(a, head, &queries, &probabilities, &targets, &masses, config));
        try std.testing.expectEqual(@as(u64, 1), fitter.dispatches);
        for (probabilities) |value| try std.testing.expectEqual(@as(f64, -123), value);
        try std.testing.expectEqual(baseline, tracker.snapshot().shared_backing_live_bytes);
        try std.testing.expect(tracker.snapshot().shared_backing_peak_bytes > baseline);
        config.control = .{};
        try solver.attendBatch(a, head, &queries, &probabilities, &targets, &masses, config);
        try std.testing.expectEqual(@as(u64, 3), fitter.dispatches);
        var reference_p: [68]f64 = undefined;
        var reference_y: [34]f64 = undefined;
        var reference_z: [17]f64 = undefined;
        config.backend = null;
        try solver.attendBatch(a, head, &queries, &reference_p, &reference_y, &reference_z, config);
        for (reference_p, probabilities) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
        for (reference_y, targets) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
        for (reference_z, masses) |expected, actual| try std.testing.expectApproxEqAbs(expected, actual, 2e-5);
        try std.testing.expectEqual(baseline, tracker.snapshot().shared_backing_live_bytes);
    }
    try std.testing.expectEqual(@as(u64, 0), tracker.snapshot().shared_backing_live_bytes);
}

test "attention_match Metal engine fits and roundtrips FP8 through the shared checkpoint contract" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const checkpoint = @import("../runtime/kv/attention_match_checkpoint.zig");
    const a = std.testing.allocator;
    const state: checkpoint.State = .{
        .identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) },
        .logical_length = 6,
        .tokens = &.{ 0, 0, 0, 0, 0, 0 },
        .logits = &.{0},
        .layer_donors = &.{0},
        .donors = &.{.{ .heads = &.{.{
            .dim = 2,
            .positions = &.{ 0, 1, 2, 3, 4, 5 },
            .coverage_ends = &.{ 1, 2, 3, 4, 5, 6 },
            .keys = &.{ 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 },
            .values = &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 },
            .bias = &.{ 0, 0, 0, 0, 0, 0 },
        }} }},
    };
    const original = try checkpoint.encodeWithEncoding(a, state, .{}, .f16, .{});
    defer a.free(original);
    const batches = [_]engine.ReferenceBatch{.{
        .consumer_layer = 0,
        .query_head = 0,
        .train = &.{ 0.25, 0.5, 1, 0, 0, 1, -0.5, 0.25 },
        .heldout = &.{ -1, 1, 0.5, -0.5 },
        .train_positions = &.{ 6, 7, 8, 9 },
        .heldout_positions = &.{ 10, 11 },
    }};
    const layout: engine.Layout = .{ .query_heads_by_layer = &.{1}, .scales_by_donor = &.{1}, .softcaps_by_donor = &.{0} };
    var fitter = try Fitter.init(a, null, 16 * 1024 * 1024);
    defer fitter.deinit();
    var stats: engine.Stats = .{};
    var result = try engine.prepare(a, state, &batches, layout, .{
        .stats = &stats,
        .retention_divisor = 2,
        .min_retention_divisor = 2,
        .prefix_rows = 1,
        .recent_rows = 1,
        .chunk_rows = 2,
        .storage_precision = .f16,
        .validation = .whole_head,
        .fp8_checkpoint = true,
        .fit = .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set, .backend = fitter.backend() },
    });
    defer result.deinit();
    const head = result.state.donors[0].heads[0];
    try std.testing.expectEqual(@as(usize, 4), head.positions.len);
    try std.testing.expectEqual(@as(usize, 1), stats.validated_heads);
    try std.testing.expectEqual(@as(usize, 1), stats.fp8_heads);
    try std.testing.expect(fitter.dispatches > 0);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2 }, head.values[0..2]);
    try std.testing.expectEqualSlices(f32, &.{ 11, 12 }, head.values[head.values.len - 2 ..]);
    const wire = try checkpoint.encodeWithEncoding(a, result.state, .{}, .fp8, .{});
    defer a.free(wire);
    var restored = try checkpoint.decode(a, wire, state.identity, .{});
    defer restored.deinit();
    try std.testing.expectEqualDeep(head.fp8, restored.state.donors[0].heads[0].fp8);
    try std.testing.expectEqualSlices(f32, head.values, restored.state.donors[0].heads[0].values);
    try std.testing.expectEqualSlices(f32, head.bias, restored.state.donors[0].heads[0].bias);
    const unchanged = try checkpoint.encodeWithEncoding(a, state, .{}, .f16, .{});
    defer a.free(unchanged);
    try std.testing.expectEqualSlices(u8, original, unchanged);
}

test "attention_match Metal fitter retirement preserves actual FP64 solver calls" {
    if (comptime @import("builtin").os.tag != .macos or !fp64.supported) return error.SkipZigTest;
    var fitter = try Fitter.init(std.testing.allocator, null, 8 * 1024 * 1024);
    var live = true;
    defer if (live) fitter.deinit();
    const backend = fitter.backend();
    var out: [2]f64 = undefined;
    try backend.values_fn.?(backend.context, std.testing.allocator, &.{ 1, 0, 0, 1, 1, 1 }, &.{ 2, 3, 5 }, 3, 2, 1, 0, .{}, &out);
    try std.testing.expectEqual(@as(u64, 1), fitter.accelerated_value_solves);
    const calls = fitter.deinitAndTakeValueSolves();
    live = false;
    try std.testing.expectEqual(@as(u64, 1), calls);
    try std.testing.expectApproxEqAbs(@as(f64, 2), out[0], 1e-12);
    try std.testing.expectApproxEqAbs(@as(f64, 3), out[1], 1e-12);
}
