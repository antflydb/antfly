// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Host reference orchestration for attention_match_v1. Query capture and device
//! execution remain separate adapters: this module never substitutes cached keys
//! for queries and never silently routes a device request through the CPU.
const std = @import("std");
const solver = @import("attention_match.zig");
const checkpoint = @import("attention_match_checkpoint.zig");

pub const ReferenceBatch = struct {
    consumer_layer: u32,
    query_head: u32,
    /// Post-RoPE queries from an isolated repeat-prefill/self-study run. These
    /// positions must follow the represented history and must be disjoint across
    /// training and held-out sets. Each consumer contributes equal sample counts.
    train: []const f32,
    heldout: []const f32,
    train_positions: []const u64,
    heldout_positions: []const u64,
};

pub const Layout = struct {
    query_heads_by_layer: []const u32,
    scales_by_donor: []const f64,
    softcaps_by_donor: []const f64,
};

pub const Options = struct {
    /// Optional diagnostics survive a rejected candidate; never authorize it.
    stats: ?*Stats = null,
    retention_divisor: usize = 4,
    /// Reject by default. Explicit fallback preserves the original head only
    /// for a rejected numerical fit; cancellation, OOM and corrupt input fail.
    rejected_head_policy: enum { reject, preserve_original } = .reject,
    /// Actual global device bytes, counting candidate bias/position/coverage
    /// metadata. Admission never treats the requested retention as achieved.
    min_global_savings_ratio: ?f64 = null,
    source_has_metadata: bool = false,
    /// Round fitted K/V to the serving precision BEFORE held-out validation.
    /// Portable checkpoints still represent these exact rounded values as f32.
    storage_precision: enum { f32, f16 } = .f32,
    recent_rows: usize = 2048,
    /// Preserve these initial logical positions exactly, including across cycles.
    prefix_rows: usize = 0,
    /// Whole-head validation includes the exact prefix/recent rows and applies
    /// the same thresholds after rounding. Chunk mode retains the strict oracle.
    validation: enum { chunk, whole_head } = .chunk,
    /// Bound concurrent whole-head conversion/validation, independently of fit workers.
    max_parallel_validations: usize = 2,
    chunk_rows: usize = 512,
    max_queries_per_consumer: usize = 256,
    /// Counts source + candidate host state, serialization, query conversion,
    /// replacement buffers and solver scratch. Device memory needs an additional
    /// reservation by the serving adapter and is not hidden in this number.
    max_transient_host_bytes: usize = 2 * 1024 * 1024 * 1024,
    checkpoint_limits: checkpoint.Limits = .{},
    fit: solver.Config = .{ .target_rows = 1 },
};

pub const Stats = struct {
    donor_layer: u32 = 0,
    head: usize = 0,
    chunk_start: usize = 0,
    attempted_chunks: usize = 0,
    completed_chunks: usize = 0,
    max_nnls_sweeps: usize = 0,
    last_fit: solver.Metrics = .{},
    validating_storage: bool = false,
    rounded_fit: solver.Metrics = .{},
    validating_head: bool = false,
    validated_heads: usize = 0,
    preserved_heads: usize = 0,
    convergence_fallbacks: usize = 0,
    source_global_bytes: usize = 0,
    candidate_global_bytes: usize = 0,
    actual_global_savings_ratio: f64 = 1,
    max_rejected_mass_error: f64 = 0,
    max_rejected_output_rmse: f64 = 0,
    max_head_mass_error: f64 = 0,
    max_head_output_rmse: f64 = 0,
};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.CompactionWorkspaceLimit;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.CompactionWorkspaceLimit;
}
fn budget(required: usize, maximum: usize) !void {
    if (required > maximum) return error.CompactionWorkspaceLimit;
}

pub const HeadWorkspace = struct {
    /// Additional bytes beyond the source and worst-case candidate states.
    bytes: usize,
    solver_bytes: usize,
};

/// Shared by preflight admission and compactHead. Includes the replacement
/// arrays even though the resident bound also reserves a full-size candidate.
pub fn headWorkspaceBytes(
    original_rows: usize,
    target_rows: usize,
    block_rows: usize,
    train_elements: usize,
    heldout_elements: usize,
    d: usize,
    options: Options,
) !HeadWorkspace {
    if (d == 0 or block_rows == 0 or target_rows == 0 or target_rows > original_rows or
        block_rows > original_rows or options.retention_divisor < 2 or options.retention_divisor > 4 or
        train_elements == 0 or heldout_elements == 0 or train_elements % d != 0 or heldout_elements % d != 0)
        return error.InvalidFitConfig;
    const query_bytes = try mul(try add(train_elements, heldout_elements), 8);
    const replacement_bytes = try mul(target_rows, try add(20, try mul(d, 8)));
    const conversion_bytes = try mul(try add(try mul(block_rows, try add(try mul(d, 4), 6)), try mul(d, 2)), 8);
    const scratch_max = try solver.fitWorkspaceBytes(block_rows, std.math.divCeil(usize, block_rows, options.retention_divisor) catch unreachable, train_elements / d, d, heldout_elements / d, options.fit);
    const head_validation_bytes = if (options.validation == .whole_head)
        try add(try mul(try mul(try add(original_rows, target_rows), try add(try mul(d, 2), 1)), 8), try solver.validationBytes(original_rows, target_rows, d, heldout_elements / d, options.fit))
    else
        0;
    return .{
        .bytes = try add(query_bytes, try add(replacement_bytes, @max(head_validation_bytes, try add(conversion_bytes, scratch_max)))),
        .solver_bytes = scratch_max,
    };
}

pub fn parallelResidentBytes(source_bytes: usize, head_count: usize) !usize {
    return add(try mul(source_bytes, 2), try mul(head_count, @sizeOf(HeadTask)));
}

/// The scheduler divides scratch equally, so every worker must accommodate the
/// largest eligible head. A validation concurrency gate does not shrink this
/// conservative bound.
pub fn parallelHostBytes(source_bytes: usize, head_count: usize, head_workspace_bytes: usize, workers: usize) !usize {
    if (workers == 0 or workers > 4) return error.InvalidFitConfig;
    return add(try parallelResidentBytes(source_bytes, head_count), try mul(workers, head_workspace_bytes));
}

fn validatePositions(positions: []const u64, logical_length: u64) !void {
    for (positions, 0..) |pos, i| {
        if (pos < logical_length or (i > 0 and pos <= positions[i - 1])) return error.InvalidCompactionQueries;
    }
}

fn validateQueries(source: checkpoint.State, batches: []const ReferenceBatch, layout: Layout, options: Options) !void {
    if (layout.query_heads_by_layer.len != source.layer_donors.len or
        layout.scales_by_donor.len != source.donors.len or layout.softcaps_by_donor.len != source.donors.len)
        return error.InvalidCompactionQueries;
    var expected: usize = 0;
    var train_count: ?usize = null;
    var heldout_count: ?usize = null;
    for (source.donors, layout.scales_by_donor, layout.softcaps_by_donor) |_, scale, cap| {
        if (!std.math.isFinite(scale) or scale <= 0 or !std.math.isFinite(cap) or cap < 0) return error.InvalidCompactionQueries;
    }
    for (source.layer_donors, layout.query_heads_by_layer, 0..) |donor_index, qh, layer| {
        const donor = source.donors[donor_index];
        if (qh == 0 or qh % donor.heads.len != 0) return error.InvalidCompactionQueries;
        if (donor.window != 0) continue;
        expected = try add(expected, qh);
        for (0..qh) |head| {
            var found = false;
            for (batches) |batch| {
                if (batch.consumer_layer != layer or batch.query_head != head) continue;
                if (found) return error.InvalidCompactionQueries;
                found = true;
                const nt = batch.train_positions.len;
                const nh = batch.heldout_positions.len;
                if (nt == 0 or nh == 0 or nt > options.max_queries_per_consumer or nh > options.max_queries_per_consumer or
                    batch.train.len != try mul(nt, donor.heads[0].dim) or batch.heldout.len != try mul(nh, donor.heads[0].dim))
                    return error.InvalidCompactionQueries;
                if (train_count) |count| {
                    if (count != nt) return error.UnbalancedCompactionQueries;
                } else train_count = nt;
                if (heldout_count) |count| {
                    if (count != nh) return error.UnbalancedCompactionQueries;
                } else heldout_count = nh;
                try validatePositions(batch.train_positions, source.logical_length);
                try validatePositions(batch.heldout_positions, source.logical_length);
                var j: usize = 0;
                for (batch.train_positions) |pos| {
                    while (j < nh and batch.heldout_positions[j] < pos) : (j += 1) {}
                    if (j < nh and batch.heldout_positions[j] == pos) return error.InvalidCompactionQueries;
                }
                for (batch.train) |x| if (!std.math.isFinite(x)) return error.InvalidCompactionQueries;
                for (batch.heldout) |x| if (!std.math.isFinite(x)) return error.InvalidCompactionQueries;
            }
            if (!found) return error.MissingCompactionConsumerQueries;
        }
    }
    if (batches.len != expected) return error.InvalidCompactionQueries;
}

fn belongs(batch: ReferenceBatch, source: checkpoint.State, layout: Layout, donor_index: usize, head_index: usize) bool {
    if (source.layer_donors[batch.consumer_layer] != donor_index) return false;
    const group = layout.query_heads_by_layer[batch.consumer_layer] / source.donors[donor_index].heads.len;
    return batch.query_head / group == head_index;
}

fn converted(allocator: std.mem.Allocator, values: []const f32) ![]f64 {
    const result = try allocator.alloc(f64, values.len);
    for (result, values) |*out, value| out.* = value;
    return result;
}

fn compactHead(allocator: std.mem.Allocator, source: checkpoint.State, donor_index: usize, head_index: usize, batches: []const ReferenceBatch, layout: Layout, options: Options, resident: usize, validation_gate: ?*HeadValidationGate) !?checkpoint.Head {
    const original = source.donors[donor_index].heads[head_index];
    const d = original.dim;
    const boundary = source.logical_length - @min(source.logical_length, options.recent_rows);
    if (boundary < original.compacted_until) return error.CompactionRecentWindowUnavailable;
    var old_rows: usize = 0;
    while (old_rows < original.positions.len and original.coverage_ends[old_rows] <= boundary) : (old_rows += 1) {}
    var prefix = @min(options.prefix_rows, @as(usize, @intCast(source.logical_length)));
    if (prefix > original.positions.len) return error.CompactionPrefixUnavailable;
    for (0..prefix) |i| {
        if (original.positions[i] != i or original.coverage_ends[i] != i + 1 or original.bias[i] != 0)
            return error.CompactionPrefixUnavailable;
    }
    // Already fitted history is immutable across cycles. Only newly eligible
    // exact rows enter the solver, avoiding repeated approximation of the same
    // vectors. Whole-head validation still includes the retained fitted rows.
    while (prefix < original.positions.len and original.coverage_ends[prefix] <= original.compacted_until) : (prefix += 1) {}
    if (old_rows <= prefix or old_rows - prefix < 2) return null;
    var target_rows: usize = original.positions.len - old_rows + prefix;
    var offset: usize = prefix;
    while (offset < old_rows) {
        const count = @min(options.chunk_rows, old_rows - offset);
        target_rows = try add(target_rows, std.math.divCeil(usize, count, options.retention_divisor) catch unreachable);
        offset += count;
    }
    if (target_rows == original.positions.len) return null;
    var train_elements: usize = 0;
    var heldout_elements: usize = 0;
    for (batches) |batch| if (belongs(batch, source, layout, donor_index, head_index)) {
        train_elements = try add(train_elements, batch.train.len);
        heldout_elements = try add(heldout_elements, batch.heldout.len);
    };
    const block_rows = @min(old_rows - prefix, options.chunk_rows);
    const workspace = try headWorkspaceBytes(original.positions.len, target_rows, block_rows, train_elements, heldout_elements, d, options);
    try budget(try add(resident, workspace.bytes), options.max_transient_host_bytes);
    try budget(workspace.solver_bytes, options.fit.max_workspace_bytes);

    const train = try allocator.alloc(f64, train_elements);
    defer allocator.free(train);
    const heldout = try allocator.alloc(f64, heldout_elements);
    defer allocator.free(heldout);
    var ti: usize = 0;
    var hi: usize = 0;
    for (batches) |batch| if (belongs(batch, source, layout, donor_index, head_index)) {
        for (batch.train) |x| {
            train[ti] = x;
            ti += 1;
        }
        for (batch.heldout) |x| {
            heldout[hi] = x;
            hi += 1;
        }
    };

    const positions = try allocator.alloc(u64, target_rows);
    errdefer allocator.free(positions);
    const ends = try allocator.alloc(u64, target_rows);
    errdefer allocator.free(ends);
    const keys = try allocator.alloc(f32, try mul(target_rows, d));
    errdefer allocator.free(keys);
    const values = try allocator.alloc(f32, keys.len);
    errdefer allocator.free(values);
    const biases = try allocator.alloc(f32, target_rows);
    errdefer allocator.free(biases);
    @memcpy(positions[0..prefix], original.positions[0..prefix]);
    @memcpy(ends[0..prefix], original.coverage_ends[0..prefix]);
    @memcpy(biases[0..prefix], original.bias[0..prefix]);
    @memcpy(keys[0 .. prefix * d], original.keys[0 .. prefix * d]);
    @memcpy(values[0 .. prefix * d], original.values[0 .. prefix * d]);
    offset = prefix;
    var out: usize = prefix;
    while (offset < old_rows) {
        try options.fit.control.check();
        const count = @min(options.chunk_rows, old_rows - offset);
        const k64 = try converted(allocator, original.keys[offset * d ..][0 .. count * d]);
        defer allocator.free(k64);
        const v64 = try converted(allocator, original.values[offset * d ..][0 .. count * d]);
        defer allocator.free(v64);
        const b64 = try converted(allocator, original.bias[offset..][0..count]);
        defer allocator.free(b64);
        const h: solver.Head = .{ .keys = k64, .values = v64, .bias = b64, .rows = count, .dim = d, .scale = layout.scales_by_donor[donor_index], .softcap = layout.softcaps_by_donor[donor_index] };
        var cfg = options.fit;
        // These intermediate blocks are private. Whole-head mode rejects the
        // assembled rounded candidate before it can leave this function.
        if (options.validation == .whole_head) {
            cfg.max_relative_mass_error = std.math.floatMax(f64);
            cfg.max_relative_output_rmse = std.math.floatMax(f64);
        }
        cfg.target_rows = std.math.divCeil(usize, count, options.retention_divisor) catch unreachable;
        if (options.stats) |stats| {
            stats.donor_layer = source.donors[donor_index].layer;
            stats.head = head_index;
            stats.chunk_start = offset;
            stats.attempted_chunks += 1;
            stats.validating_storage = false;
            stats.validating_head = false;
            stats.rounded_fit = .{};
            cfg.metrics = &stats.last_fit;
        }
        var fitted = try solver.fit(allocator, h, train, heldout, cfg);
        defer fitted.deinit();
        if (options.stats) |stats| stats.max_nnls_sweeps = @max(stats.max_nnls_sweeps, fitted.metrics.nnls_sweeps);
        for (fitted.source_indices, 0..) |idx, row| {
            positions[out + row] = original.positions[offset + idx];
            ends[out + row] = original.coverage_ends[offset + count - 1];
            biases[out + row] = @floatCast(fitted.bias[row]);
        }
        for (fitted.keys, fitted.values, 0..) |k, v, i| {
            keys[out * d + i] = if (options.storage_precision == .f16) @as(f16, @floatCast(k)) else @floatCast(k);
            values[out * d + i] = if (options.storage_precision == .f16) @as(f16, @floatCast(v)) else @floatCast(v);
        }
        // Check held-out fit again AFTER storage rounding, using the same
        // thresholds. This catches overflowing or unstable fitted f32 values.
        for (fitted.keys, fitted.values, 0..) |*k, *v, i| {
            k.* = keys[out * d + i];
            v.* = values[out * d + i];
        }
        for (fitted.bias, 0..) |*b, i| b.* = biases[out + i];
        var rounded_cfg = cfg;
        if (options.stats) |stats| {
            stats.validating_storage = true;
            rounded_cfg.metrics = &stats.rounded_fit;
        }
        try validateRounded(allocator, h, fitted.head(h), heldout, rounded_cfg);
        if (options.stats) |stats| stats.completed_chunks += 1;
        out += fitted.bias.len;
        offset += count;
    }
    const remaining = original.positions.len - old_rows;
    @memcpy(positions[out..], original.positions[old_rows..]);
    @memcpy(ends[out..], original.coverage_ends[old_rows..]);
    @memcpy(biases[out..], original.bias[old_rows..]);
    @memcpy(keys[out * d ..], original.keys[old_rows * d ..]);
    @memcpy(values[out * d ..], original.values[old_rows * d ..]);
    std.debug.assert(out + remaining == target_rows);
    const result: checkpoint.Head = .{ .dim = d, .compacted_until = original.coverage_ends[old_rows - 1], .positions = positions, .coverage_ends = ends, .keys = keys, .values = values, .bias = biases };
    if (options.validation == .whole_head) {
        // Bound simultaneous whole-head FP64 conversions at long contexts.
        // Chunk fitting still runs on all independently owned workers.
        if (validation_gate) |gate| try gate.enter(options.fit.control);
        defer if (validation_gate) |gate| gate.leave();
        if (options.stats) |stats| {
            stats.validating_head = true;
            stats.validating_storage = true;
        }
        const ok = try converted(allocator, original.keys);
        defer allocator.free(ok);
        const ov = try converted(allocator, original.values);
        defer allocator.free(ov);
        const ob = try converted(allocator, original.bias);
        defer allocator.free(ob);
        const ck = try converted(allocator, keys);
        defer allocator.free(ck);
        const cv = try converted(allocator, values);
        defer allocator.free(cv);
        const cb = try converted(allocator, biases);
        defer allocator.free(cb);
        const h: solver.Head = .{ .keys = ok, .values = ov, .bias = ob, .rows = original.positions.len, .dim = d, .scale = layout.scales_by_donor[donor_index], .softcap = layout.softcaps_by_donor[donor_index] };
        var compact = h;
        compact.keys = ck;
        compact.values = cv;
        compact.bias = cb;
        compact.rows = target_rows;
        var cfg = options.fit;
        if (options.stats) |stats| cfg.metrics = &stats.rounded_fit;
        try validateRounded(allocator, h, compact, heldout, cfg);
        if (options.stats) |stats| {
            stats.validated_heads += 1;
            stats.max_head_mass_error = @max(stats.max_head_mass_error, stats.rounded_fit.max_relative_mass_error);
            stats.max_head_output_rmse = @max(stats.max_head_output_rmse, stats.rounded_fit.relative_output_rmse);
        }
    }
    return result;
}

fn validateRounded(allocator: std.mem.Allocator, original: solver.Head, compact: solver.Head, queries: []const f64, cfg: solver.Config) !void {
    var metrics: solver.Metrics = .{};
    defer {
        if (cfg.metrics) |out| out.* = metrics;
    }
    try compact.validate();
    metrics = try solver.validationMetrics(allocator, original, compact, queries, cfg);
    if (!std.math.isFinite(metrics.relative_output_rmse) or
        metrics.relative_output_rmse > cfg.max_relative_output_rmse or
        metrics.max_relative_mass_error > cfg.max_relative_mass_error) return error.CompactionFitRejected;
}

/// Dense exact CUDA storage has no compaction metadata; staged candidates
/// carry bias/position/coverage arrays even for unchanged heads. Local windows
/// are deliberately excluded from the global-cache saving, but remain counted
/// in the complete source/candidate host and device admission budgets.
pub fn globalStorageBytes(state_value: checkpoint.State, precision: @FieldType(Options, "storage_precision"), staged: bool) !usize {
    var total: usize = 0;
    for (state_value.donors) |donor| {
        if (donor.window != 0) continue;
        for (donor.heads) |head| {
            const kv_bytes = try mul(head.keys.len, if (precision == .f16) 4 else 8);
            const metadata = if (staged or head.compacted_until != 0) try mul(head.positions.len, 20) else 0;
            total = try add(total, try add(kv_bytes, metadata));
        }
    }
    return total;
}

/// Prepare a complete candidate without mutating source. Commit is a separate
/// operation after device upload has succeeded. Chunking bounds QR work; every
/// compacted row records the coverage of its entire source chunk.
pub fn prepare(allocator: std.mem.Allocator, source: checkpoint.State, batches: []const ReferenceBatch, layout: Layout, options: Options) !checkpoint.Owned {
    return prepareWithBackends(allocator, source, batches, layout, options, &.{});
}

/// Parallel preparation uses a known thread-safe allocator. Backends must be
/// independently owned, remain alive through this call, and share an admitted
/// aggregate device budget. The source and queries are immutable throughout.
pub fn prepareParallel(source: checkpoint.State, batches: []const ReferenceBatch, layout: Layout, options: Options, backends: []const solver.Backend) !checkpoint.Owned {
    if (backends.len < 2 or backends.len > 4) return error.InvalidFitConfig;
    for (backends, 0..) |backend, i| if (backend.context) |context| {
        for (backends[0..i]) |previous| if (previous.context == context) return error.InvalidFitConfig;
    };
    return prepareWithBackends(std.heap.page_allocator, source, batches, layout, options, backends);
}

fn preparedHead(allocator: std.mem.Allocator, source: checkpoint.State, donor_index: usize, head_index: usize, batches: []const ReferenceBatch, layout: Layout, options: Options, resident: usize, validation_gate: ?*HeadValidationGate) !checkpoint.Head {
    try options.fit.control.check();
    const donor = source.donors[donor_index];
    const replacement = if (donor.window == 0)
        compactHead(allocator, source, donor_index, head_index, batches, layout, options, resident, validation_gate) catch |err| fallback: {
            if (options.rejected_head_policy != .preserve_original or
                (err != error.CompactionFitRejected and err != error.MassFitDidNotConverge)) return err;
            try options.fit.control.check();
            if (options.stats) |stats| {
                stats.preserved_heads += 1;
                if (err == error.MassFitDidNotConverge) stats.convergence_fallbacks += 1;
                const metrics = if (stats.validating_storage) stats.rounded_fit else stats.last_fit;
                stats.max_rejected_mass_error = @max(stats.max_rejected_mass_error, metrics.max_relative_mass_error);
                stats.max_rejected_output_rmse = @max(stats.max_rejected_output_rmse, metrics.relative_output_rmse);
            }
            break :fallback null;
        }
    else
        null;
    return replacement orelse try checkpoint.cloneHead(allocator, donor.heads[head_index]);
}

const HeadTask = struct {
    ordinal: usize,
    score: usize,
    fn less(_: void, a: HeadTask, b: HeadTask) bool {
        return if (a.score == b.score) a.ordinal < b.ordinal else a.score > b.score;
    }
};
const HeadValidationGate = struct {
    maximum: usize = 2,
    active: std.atomic.Value(usize) = .init(0),
    fn enter(self: *@This(), control: solver.Control) !void {
        while (true) {
            try control.check();
            const count = self.active.load(.acquire);
            if (count < self.maximum and self.active.cmpxchgWeak(count, count + 1, .acq_rel, .acquire) == null) {
                errdefer self.leave();
                try control.check();
                return;
            }
            std.Thread.yield() catch {};
        }
    }
    fn leave(self: *@This()) void {
        const before = self.active.fetchSub(1, .acq_rel);
        std.debug.assert(before > 0 and before <= self.maximum);
    }
};

const ParallelControl = struct {
    validation_gate: HeadValidationGate = .{},
    mutex: std.atomic.Mutex = .unlocked,
    control: solver.Control,
    failure: ?anyerror = null,
    next_head: usize = 0,
    tasks: []const HeadTask,

    fn lock(self: *@This()) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    fn check(raw: ?*anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw.?));
        self.lock();
        defer self.mutex.unlock();
        if (self.failure) |err| return err;
        // Arbitrary caller callbacks are never invoked concurrently.
        self.control.check() catch |err| {
            self.failure = err;
            return err;
        };
    }
    fn fail(self: *@This(), err: anyerror) void {
        self.lock();
        defer self.mutex.unlock();
        if (self.failure == null) self.failure = err;
    }
    fn next(self: *@This()) ?usize {
        self.lock();
        defer self.mutex.unlock();
        if (self.failure != null or self.next_head == self.tasks.len) return null;
        const result = self.tasks[self.next_head].ordinal;
        self.next_head += 1;
        return result;
    }
};
const HeadWorker = struct {
    source: checkpoint.State,
    candidate: checkpoint.State,
    batches: []const ReferenceBatch,
    layout: Layout,
    options: Options,
    resident: usize,
    shared: *ParallelControl,
    stats: Stats = .{},

    fn run(self: *@This()) void {
        self.options.stats = &self.stats;
        self.options.fit.control = .{ .context = self.shared, .check_fn = ParallelControl.check };
        // A caller metrics pointer is not worker-owned. compactHead uses its
        // private Stats instead; never let the solver publish into shared data.
        self.options.fit.metrics = null;
        while (self.shared.next()) |task| {
            var first: usize = 0;
            for (self.source.donors, 0..) |donor, di| {
                if (donor.window != 0) continue;
                if (task >= first + donor.heads.len) {
                    first += donor.heads.len;
                    continue;
                }
                const hi = task - first;
                const result = preparedHead(std.heap.page_allocator, self.source, di, hi, self.batches, self.layout, self.options, self.resident, &self.shared.validation_gate) catch |err| {
                    self.shared.fail(err);
                    return;
                };
                @constCast(self.candidate.donors[di].heads)[hi] = result;
                break;
            }
        }
    }
};
fn mergeStats(total: *Stats, part: Stats) void {
    total.donor_layer = part.donor_layer;
    total.head = part.head;
    total.chunk_start = part.chunk_start;
    total.last_fit = part.last_fit;
    total.rounded_fit = part.rounded_fit;
    total.validating_storage = part.validating_storage;
    total.validating_head = part.validating_head;
    inline for (.{ "attempted_chunks", "completed_chunks", "validated_heads", "preserved_heads", "convergence_fallbacks" }) |field|
        @field(total, field) += @field(part, field);
    inline for (.{ "max_nnls_sweeps", "max_rejected_mass_error", "max_rejected_output_rmse", "max_head_mass_error", "max_head_output_rmse" }) |field|
        @field(total, field) = @max(@field(total, field), @field(part, field));
}
fn prepareHeadsParallel(candidate: checkpoint.State, source: checkpoint.State, batches: []const ReferenceBatch, layout: Layout, options: Options, source_bytes: usize, backends: []const solver.Backend) !void {
    // Reserve source + worst-case candidate once. Divide the remaining host
    // workspace among workers so individually admitted jobs cannot oversubscribe
    // the process. No worker reads another worker's partially built candidate.
    var head_count: usize = 0;
    for (source.donors) |donor| if (donor.window == 0) {
        head_count = try add(head_count, donor.heads.len);
    };
    const resident = try parallelResidentBytes(source_bytes, head_count);
    try budget(resident, options.max_transient_host_bytes);
    const per_worker = (options.max_transient_host_bytes - resident) / backends.len;
    const tasks = try std.heap.page_allocator.alloc(HeadTask, head_count);
    defer std.heap.page_allocator.free(tasks);
    var ordinal: usize = 0;
    for (source.donors, 0..) |donor, di| {
        if (donor.window == 0) {
            for (donor.heads, 0..) |head, hi| {
                var query_elements: usize = 0;
                for (batches) |batch| if (belongs(batch, source, layout, di, hi)) {
                    query_elements = try add(query_elements, batch.train.len);
                };
                tasks[ordinal] = .{ .ordinal = ordinal, .score = try mul(head.positions.len, query_elements) };
                ordinal += 1;
            }
        } else for (donor.heads, 0..) |head, hi| {
            try options.fit.control.check();
            @constCast(candidate.donors[di].heads)[hi] = try checkpoint.cloneHead(std.heap.page_allocator, head);
        }
    }
    std.mem.sort(HeadTask, tasks, {}, HeadTask.less);
    var shared: ParallelControl = .{ .control = options.fit.control, .tasks = tasks, .validation_gate = .{ .maximum = options.max_parallel_validations } };
    var jobs: [4]HeadWorker = undefined;
    for (jobs[0..backends.len], backends) |*job, backend| {
        var worker_options = options;
        worker_options.max_transient_host_bytes = resident + per_worker;
        worker_options.fit.backend = backend;
        job.* = .{ .source = source, .candidate = candidate, .batches = batches, .layout = layout, .options = worker_options, .resident = resident, .shared = &shared };
    }
    {
        var threads: [3]std.Thread = undefined;
        var spawned: usize = 0;
        defer for (threads[0..spawned]) |thread| thread.join();
        for (jobs[1..backends.len]) |*job| {
            threads[spawned] = std.Thread.spawn(.{}, HeadWorker.run, .{job}) catch |err| {
                shared.fail(err);
                break;
            };
            spawned += 1;
        }
        jobs[0].run();
    }
    if (options.stats) |stats| for (jobs[0..backends.len]) |job| mergeStats(stats, job.stats);
    if (shared.failure) |err| return err;
}

fn prepareWithBackends(allocator: std.mem.Allocator, source: checkpoint.State, batches: []const ReferenceBatch, layout: Layout, options: Options, backends: []const solver.Backend) !checkpoint.Owned {
    if (options.stats) |stats| stats.* = .{};
    try options.fit.control.check();
    try source.validateWithControl(options.checkpoint_limits, options.fit.control);
    if (options.max_parallel_validations == 0 or options.max_parallel_validations > 4) return error.InvalidFitConfig;
    if (options.chunk_rows < 2 or options.retention_divisor < 2 or options.retention_divisor > 4 or options.max_queries_per_consumer == 0)
        return error.InvalidFitConfig;
    if (options.min_global_savings_ratio) |minimum| {
        if (!std.math.isFinite(minimum) or minimum < 1) return error.InvalidFitConfig;
    }
    try validateQueries(source, batches, layout, options);
    const source_bytes = try source.residentBytes();
    // Admit a conservative source + full-size candidate bound before allocating.
    // The actual candidate is assembled head-by-head; no full clone/encoding.
    if (source_bytes > options.checkpoint_limits.max_state_bytes) return error.KvCheckpointLimit;
    try budget(try mul(source_bytes, 2), options.max_transient_host_bytes);
    var candidate = try checkpoint.cloneMetadata(allocator, source);
    errdefer candidate.deinit();
    if (backends.len != 0) {
        try prepareHeadsParallel(candidate.state, source, batches, layout, options, source_bytes, backends);
    } else for (source.donors, 0..) |donor, donor_index| {
        for (donor.heads, 0..) |_, head_index| {
            const resident = try add(source_bytes, try candidate.state.residentBytes());
            @constCast(candidate.state.donors[donor_index].heads)[head_index] = try preparedHead(allocator, source, donor_index, head_index, batches, layout, options, resident, null);
        }
    }
    try candidate.state.validateWithControl(options.checkpoint_limits, options.fit.control);
    const source_global = try globalStorageBytes(source, options.storage_precision, options.source_has_metadata);
    const candidate_global = try globalStorageBytes(candidate.state, options.storage_precision, true);
    const ratio = if (candidate_global == 0) @as(f64, 1) else @as(f64, @floatFromInt(source_global)) / @as(f64, @floatFromInt(candidate_global));
    if (options.stats) |stats| {
        stats.source_global_bytes = source_global;
        stats.candidate_global_bytes = candidate_global;
        stats.actual_global_savings_ratio = ratio;
    }
    // Diagnostic callers may prepare an identity state without a saving gate.
    // An explicit minimum is a strict acceptance condition, never a hint.
    if (options.min_global_savings_ratio) |minimum| {
        if (ratio < minimum) return error.CompactionSavingsInsufficient;
    }
    try options.fit.control.check();
    return candidate;
}

const identity: checkpoint.Identity = .{ .weights = @splat(1), .tokenizer = @splat(2), .template = @splat(3), .layout = @splat(4) };
const global: checkpoint.Head = .{ .dim = 1, .positions = &.{ 0, 1, 2, 3 }, .coverage_ends = &.{ 1, 2, 3, 4 }, .keys = &.{ 0, 0, 0, 0 }, .values = &.{ 2, 4, 6, 8 }, .bias = &.{ 0, 0, 0, 0 } };
const local: checkpoint.Head = .{ .dim = 1, .positions = &.{ 2, 3 }, .coverage_ends = &.{ 3, 4 }, .keys = &.{ 1, 2 }, .values = &.{ 6, 8 }, .bias = &.{ 0, 0 } };
const state: checkpoint.State = .{ .identity = identity, .logical_length = 4, .tokens = &.{ 0, 1, 0, 1 }, .logits = &.{ 0.2, 0.8 }, .layer_donors = &.{ 0, 1, 0 }, .donors = &.{ .{ .layer = 0, .heads = &.{ global, global } }, .{ .layer = 1, .window = 2, .heads = &.{ local, local } } } };
const layout_fixture: Layout = .{ .query_heads_by_layer = &.{ 4, 4, 4 }, .scales_by_donor = &.{ 1, 1 }, .softcaps_by_donor = &.{ 0, 0 } };
fn batchesFixture() [8]ReferenceBatch {
    var batches: [8]ReferenceBatch = undefined;
    for (&batches, 0..) |*b, i| b.* = .{ .consumer_layer = if (i < 4) 0 else 2, .query_head = @intCast(i % 4), .train = &.{0}, .heldout = &.{1}, .train_positions = &.{4}, .heldout_positions = &.{5} };
    return batches;
}

test "attention_match engine compacts shared donors once and preserves exact recent and local rows" {
    const batches = batchesFixture();
    var result = try prepare(std.testing.allocator, state, &batches, layout_fixture, .{ .recent_rows = 1, .chunk_rows = 2 });
    defer result.deinit();
    try std.testing.expectEqual(@as(usize, 2), result.state.donors.len);
    try std.testing.expectEqualSlices(u32, state.layer_donors, result.state.layer_donors);
    try std.testing.expectEqualSlices(f32, local.values, result.state.donors[1].heads[0].values);
    const head = result.state.donors[0].heads[0];
    try std.testing.expectEqualSlices(u64, &.{ 0, 2, 3 }, head.positions);
    try std.testing.expectEqualSlices(u64, &.{ 2, 3, 4 }, head.coverage_ends);
    try std.testing.expectEqual(@as(f32, 8), head.values[2]);
    try std.testing.expectEqual(@as(f32, 0), head.bias[2]);
    try std.testing.expectEqual(@as(u64, 4), result.state.logical_length);
}

test "attention_match engine requires queries from every shared consumer and GQA head" {
    var batches = batchesFixture();
    try std.testing.expectError(error.MissingCompactionConsumerQueries, prepare(std.testing.allocator, state, batches[0..4], layout_fixture, .{ .recent_rows = 0 }));
    batches[1].query_head = 0;
    try std.testing.expectError(error.InvalidCompactionQueries, prepare(std.testing.allocator, state, &batches, layout_fixture, .{}));
    batches = batchesFixture();
    batches[0].heldout_positions = &.{4};
    try std.testing.expectError(error.InvalidCompactionQueries, prepare(std.testing.allocator, state, &batches, layout_fixture, .{}));
    try std.testing.expectEqual(@as(f32, 2), state.donors[0].heads[0].values[0]);
}

test "attention_match engine rejects enlarging exact recent window into fitted history" {
    const batches = batchesFixture();
    var candidate = try prepare(std.testing.allocator, state, &batches, layout_fixture, .{ .recent_rows = 1 });
    defer candidate.deinit();
    try std.testing.expectError(error.CompactionRecentWindowUnavailable, prepare(std.testing.allocator, candidate.state, &batches, layout_fixture, .{ .recent_rows = 3 }));
    try std.testing.expectEqual(@as(u64, 3), candidate.state.donors[0].heads[0].compacted_until);
}

fn allocationFailure(allocator: std.mem.Allocator) !void {
    const batches = batchesFixture();
    var result = try prepare(allocator, state, &batches, layout_fixture, .{ .recent_rows = 0, .chunk_rows = 2 });
    defer result.deinit();
}
test "attention_match engine failures preserve source and unwind partial candidates" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocationFailure, .{});
}

test "attention_match engine twenty actual checkpoint cycles preserve fitted history and original mass" {
    const a = std.testing.allocator;
    const tokens: [44]u32 = @splat(0);
    var current: ?checkpoint.Owned = null;
    defer if (current) |*owned| owned.deinit();
    for (0..20) |cycle| {
        const length: usize = 4 + cycle * 2;
        var positions: [44]u64 = undefined;
        var ends: [44]u64 = undefined;
        var keys: [44]f32 = @splat(0);
        var values: [44]f32 = @splat(3);
        var biases: [44]f32 = @splat(0);
        const retained: usize = if (current) |old| old.state.donors[0].heads[0].positions.len else 0;
        const appended: usize = if (current != null) 2 else length;
        const rows = retained + appended;
        if (current) |old| {
            const h = old.state.donors[0].heads[0];
            @memcpy(positions[0..retained], h.positions);
            @memcpy(ends[0..retained], h.coverage_ends);
            @memcpy(keys[0..retained], h.keys);
            @memcpy(values[0..retained], h.values);
            @memcpy(biases[0..retained], h.bias);
        }
        for (retained..rows) |row| {
            positions[row] = length - appended + row - retained;
            ends[row] = positions[row] + 1;
        }
        const h: checkpoint.Head = .{ .dim = 1, .compacted_until = if (current) |old| old.state.logical_length else 0, .positions = positions[0..rows], .coverage_ends = ends[0..rows], .keys = keys[0..rows], .values = values[0..rows], .bias = biases[0..rows] };
        const local_positions = [_]u64{ length - 2, length - 1 };
        const local_ends = [_]u64{ length - 1, length };
        const lh: checkpoint.Head = .{ .dim = 1, .positions = &local_positions, .coverage_ends = &local_ends, .keys = &.{ 0, 0 }, .values = &.{ 3, 3 }, .bias = &.{ 0, 0 } };
        var source = state;
        source.logical_length = length;
        source.tokens = tokens[0..length];
        source.donors = &.{ .{ .layer = 0, .heads = &.{ h, h } }, .{ .layer = 1, .window = 2, .heads = &.{ lh, lh } } };
        var batches = batchesFixture();
        const train_positions = [_]u64{length};
        const heldout_positions = [_]u64{length + 1};
        for (&batches) |*batch| {
            batch.train_positions = &train_positions;
            batch.heldout_positions = &heldout_positions;
        }
        var candidate = try prepare(a, source, &batches, layout_fixture, .{ .recent_rows = 0, .validation = .whole_head });
        defer candidate.deinit();
        const encoded = try checkpoint.encode(a, candidate.state, .{});
        defer a.free(encoded);
        var restored = try checkpoint.decode(a, encoded, identity, .{});
        errdefer restored.deinit();
        const head = restored.state.donors[0].heads[0];
        try std.testing.expectEqual(length, head.compacted_until);
        try std.testing.expectEqual(retained + 1, head.positions.len);
        try std.testing.expectEqualSlices(u64, h.positions[0..retained], head.positions[0..retained]);
        try std.testing.expectEqualSlices(u64, h.coverage_ends[0..retained], head.coverage_ends[0..retained]);
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(h.keys[0..retained]), std.mem.sliceAsBytes(head.keys[0..retained]));
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(h.values[0..retained]), std.mem.sliceAsBytes(head.values[0..retained]));
        try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(h.bias[0..retained]), std.mem.sliceAsBytes(head.bias[0..retained]));
        var mass: f64 = 0;
        var numerator: f64 = 0;
        for (head.bias, head.values) |bias, value| {
            const weight = @exp(@as(f64, bias));
            mass += weight;
            numerator += weight * value;
        }
        try std.testing.expectApproxEqAbs(@as(f64, @floatFromInt(length)), mass, 1e-5);
        try std.testing.expectApproxEqAbs(@as(f64, 3), numerator / mass, 1e-6);
        if (current) |*owned| owned.deinit();
        current = restored;
    }
}

test "attention_match fitting validates serving f16 rounding before publication" {
    var rounded_global = global;
    rounded_global.values = &.{ 1.0001, 1.0001, 1.0001, 1.0001 };
    var source = state;
    source.donors = &.{ .{ .layer = 0, .heads = &.{ rounded_global, rounded_global } }, .{ .layer = 1, .window = 2, .heads = &.{ local, local } } };
    const batches = batchesFixture();
    var stats: Stats = .{};
    var options: Options = .{ .stats = &stats, .recent_rows = 0, .chunk_rows = 4, .fit = .{ .target_rows = 1, .max_relative_output_rmse = 1e-6 } };
    var precise = try prepare(std.testing.allocator, source, &batches, layout_fixture, options);
    defer precise.deinit();
    options.storage_precision = .f16;
    try std.testing.expectError(error.CompactionFitRejected, prepare(std.testing.allocator, source, &batches, layout_fixture, options));
    try std.testing.expectEqual(@as(usize, 1), stats.attempted_chunks);
    try std.testing.expectEqual(@as(usize, 0), stats.completed_chunks);
    try std.testing.expectEqual(@as(u32, 0), stats.donor_layer);
    try std.testing.expectEqual(@as(usize, 0), stats.head);
    try std.testing.expect(stats.last_fit.nnls_sweeps > 0);
    try std.testing.expect(stats.validating_storage);
    try std.testing.expect(stats.last_fit.relative_output_rmse < 1e-6);
    try std.testing.expect(stats.rounded_fit.relative_output_rmse > 1e-6);
    options.validation = .whole_head;
    try std.testing.expectError(error.CompactionFitRejected, prepare(std.testing.allocator, source, &batches, layout_fixture, options));
    try std.testing.expect(stats.validating_head);
    try std.testing.expectEqual(@as(usize, 1), stats.completed_chunks);
    try std.testing.expectEqual(@as(usize, 0), stats.validated_heads);
    try std.testing.expect(stats.rounded_fit.relative_output_rmse > 1e-6);
    options.validation = .chunk;
    options.fit.max_relative_output_rmse = 0.01;
    var rounded = try prepare(std.testing.allocator, source, &batches, layout_fixture, options);
    defer rounded.deinit();
    try std.testing.expectEqual(@as(usize, 2), stats.attempted_chunks);
    try std.testing.expectEqual(@as(usize, 2), stats.completed_chunks);
    try std.testing.expectEqual(@as(f32, 1), rounded.state.donors[0].heads[0].values[0]);
    try std.testing.expectEqualSlices(f32, local.values, rounded.state.donors[1].heads[0].values);
    try std.testing.expectEqualSlices(f32, &.{ 1.0001, 1.0001, 1.0001, 1.0001 }, source.donors[0].heads[0].values);
}

test "attention_match fitting admits two resident states without serialized full cache clone" {
    const batches = batchesFixture();
    // No eligible old rows: this exercises the exact-copy path at the new
    // two-state bound, below the former three-state + encoding requirement.
    const bound = try state.residentBytes() * 2;
    var result = try prepare(std.testing.allocator, state, &batches, layout_fixture, .{ .max_transient_host_bytes = bound });
    defer result.deinit();
    try std.testing.expectEqualSlices(f32, global.values, result.state.donors[0].heads[0].values);
    try std.testing.expect(result.state.donors[0].heads[0].values.ptr != global.values.ptr);
    try std.testing.expectError(error.CompactionWorkspaceLimit, prepare(std.testing.allocator, state, &batches, layout_fixture, .{ .max_transient_host_bytes = bound - 1 }));
}

test "attention_match whole head validation preserves exact prefix and rejects unavailable prefix" {
    const batches = batchesFixture();
    var stats: Stats = .{};
    var result = try prepare(std.testing.allocator, state, &batches, layout_fixture, .{ .stats = &stats, .recent_rows = 1, .prefix_rows = 1, .validation = .whole_head, .fit = .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set } });
    defer result.deinit();
    const h = result.state.donors[0].heads[0];
    try std.testing.expectEqualSlices(u64, &.{ 0, 1, 3 }, h.positions);
    try std.testing.expectEqualSlices(u64, &.{ 1, 3, 4 }, h.coverage_ends);
    try std.testing.expectEqual(@as(f32, 2), h.values[0]);
    try std.testing.expectEqual(@as(f32, 0), h.bias[0]);
    try std.testing.expectEqual(@as(usize, 2), stats.validated_heads);
    try std.testing.expectError(error.CompactionPrefixUnavailable, prepare(std.testing.allocator, result.state, &batches, layout_fixture, .{ .recent_rows = 1, .prefix_rows = 2 }));
    var second = try prepare(std.testing.allocator, result.state, &batches, layout_fixture, .{ .recent_rows = 1, .prefix_rows = 1, .validation = .whole_head });
    defer second.deinit();
    try std.testing.expectEqualSlices(f32, h.values, second.state.donors[0].heads[0].values);
}

fn wholeHeadAllocationFailure(allocator: std.mem.Allocator) !void {
    const batches = batchesFixture();
    var result = try prepare(allocator, state, &batches, layout_fixture, .{ .recent_rows = 1, .prefix_rows = 1, .validation = .whole_head, .fit = .{ .target_rows = 1, .selection = .residual_omp, .mass_solver = .active_set } });
    defer result.deinit();
}
test "attention_match whole head validation unwinds allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, wholeHeadAllocationFailure, .{});
}

fn rejectedFixture() checkpoint.State {
    var source = state;
    // Return a static backing array; the source and every head remain borrowed.
    const donors = struct {
        const heads = [_]checkpoint.Head{ blk: {
            var h = global;
            h.keys = &.{ -1, -1, 1, 1 };
            break :blk h;
        }, global };
        const values = [_]checkpoint.Donor{ .{ .layer = 0, .heads = &heads }, .{ .layer = 1, .window = 2, .heads = &.{ local, local } } };
    };
    source.donors = &donors.values;
    return source;
}

test "attention_match explicit fallback preserves rejected head and reports actual storage savings" {
    const source = rejectedFixture();
    const batches = batchesFixture();
    var stats: Stats = .{};
    var options: Options = .{ .stats = &stats, .recent_rows = 0, .chunk_rows = 4, .validation = .whole_head };
    try std.testing.expectError(error.CompactionFitRejected, prepare(std.testing.allocator, source, &batches, layout_fixture, options));
    options.rejected_head_policy = .preserve_original;
    var candidate = try prepare(std.testing.allocator, source, &batches, layout_fixture, options);
    defer candidate.deinit();
    try std.testing.expectEqual(@as(usize, 1), stats.preserved_heads);
    try std.testing.expectEqual(@as(usize, 1), stats.validated_heads);
    const kept = candidate.state.donors[0].heads[0];
    try std.testing.expectEqualSlices(f32, source.donors[0].heads[0].keys, kept.keys);
    try std.testing.expectEqualSlices(f32, source.donors[0].heads[0].values, kept.values);
    try std.testing.expectEqualSlices(u64, source.donors[0].heads[0].coverage_ends, kept.coverage_ends);
    try std.testing.expect(kept.keys.ptr != source.donors[0].heads[0].keys.ptr);
    try std.testing.expect(stats.max_rejected_mass_error > 0.05);
    try std.testing.expectEqual(@as(usize, 1), candidate.state.donors[0].heads[1].positions.len);
    // Tiny dim=1 test heads are dominated by metadata. A nominal 4x request
    // must not be mistaken for a real device saving.
    try std.testing.expect(stats.actual_global_savings_ratio < 1);
    options.min_global_savings_ratio = 1;
    try std.testing.expectError(error.CompactionSavingsInsufficient, prepare(std.testing.allocator, source, &batches, layout_fixture, options));
    options.min_global_savings_ratio = std.math.nan(f64);
    try std.testing.expectError(error.InvalidFitConfig, prepare(std.testing.allocator, source, &batches, layout_fixture, options));
}

fn preservedAllocationFailure(allocator: std.mem.Allocator) !void {
    const source = rejectedFixture();
    const batches = batchesFixture();
    var result = try prepare(allocator, source, &batches, layout_fixture, .{ .recent_rows = 0, .chunk_rows = 4, .validation = .whole_head, .rejected_head_policy = .preserve_original });
    defer result.deinit();
}
test "attention_match fallback cannot swallow allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, preservedAllocationFailure, .{});
}

test "attention_match fallback cannot swallow cancellation or invalid source" {
    const batches = batchesFixture();
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.CompactionCancelled;
        }
    };
    try std.testing.expectError(error.CompactionCancelled, prepare(std.testing.allocator, rejectedFixture(), &batches, layout_fixture, .{ .rejected_head_policy = .preserve_original, .fit = .{ .target_rows = 1, .control = .{ .check_fn = Cancel.check } } }));
    var bad = rejectedFixture();
    bad.logical_length = 0;
    try std.testing.expectError(error.InvalidKvCheckpoint, prepare(std.testing.allocator, bad, &batches, layout_fixture, .{ .rejected_head_policy = .preserve_original }));
}

const ParallelTestBackend = struct {
    fn workspace(_: ?*anyopaque, _: usize, _: usize, _: usize, _: usize) !usize {
        return 0;
    }
    fn failWorkspace(_: ?*anyopaque, _: usize, _: usize, _: usize, _: usize) !usize {
        return error.InjectedWorkerFailure;
    }
    const backend: solver.Backend = .{ .workspace_fn = workspace };
};
test "attention_match planned host bound admits parallel fitting and rejects one byte less" {
    const batches = batchesFixture();
    const backends: [4]solver.Backend = @splat(ParallelTestBackend.backend);
    var options: Options = .{
        .recent_rows = 1,
        .chunk_rows = 2,
        .validation = .whole_head,
        .fit = .{ .target_rows = 1, .backend = ParallelTestBackend.backend },
    };
    // Two consumer layers, each with two query heads per KV head.
    const workspace = try headWorkspaceBytes(4, 3, 2, 4, 4, 1, options);
    const source_bytes = try state.residentBytes();
    var serial = try prepare(std.testing.allocator, state, &batches, layout_fixture, options);
    defer serial.deinit();
    const expected = try checkpoint.encode(std.testing.allocator, serial.state, .{});
    defer std.testing.allocator.free(expected);
    for (2..5) |workers| {
        const bound = try parallelHostBytes(source_bytes, 2, workspace.bytes, workers);
        options.max_transient_host_bytes = bound;
        var parallel = try prepareParallel(state, &batches, layout_fixture, options, backends[0..workers]);
        defer parallel.deinit();
        const actual = try checkpoint.encode(std.testing.allocator, parallel.state, .{});
        defer std.testing.allocator.free(actual);
        try std.testing.expectEqualSlices(u8, expected, actual);
        options.max_transient_host_bytes = bound - 1;
        try std.testing.expectError(error.CompactionWorkspaceLimit, prepareParallel(state, &batches, layout_fixture, options, backends[0..workers]));
    }
}

test "attention_match host planning rejects malformed geometry and overflow" {
    try std.testing.expectError(error.InvalidFitConfig, headWorkspaceBytes(4, 2, 2, 2, 2, 0, .{}));
    try std.testing.expectError(error.InvalidFitConfig, headWorkspaceBytes(4, 2, 2, 3, 2, 2, .{}));
    try std.testing.expectError(error.InvalidFitConfig, parallelHostBytes(1, 1, 1, 0));
    try std.testing.expectError(error.InvalidFitConfig, parallelHostBytes(1, 1, 1, 5));
    try std.testing.expectError(error.CompactionWorkspaceLimit, parallelHostBytes(std.math.maxInt(usize), 1, 1, 4));
    try std.testing.expectError(error.CompactionWorkspaceLimit, parallelHostBytes(1, 1, std.math.maxInt(usize), 4));
}

test "attention_match parallel heads match serial checkpoints and aggregate diagnostics" {
    const batches = batchesFixture();
    var serial_stats: Stats = .{};
    var parallel_stats: Stats = .{};
    var options: Options = .{ .recent_rows = 1, .chunk_rows = 2, .stats = &serial_stats };
    var serial = try prepare(std.testing.allocator, state, &batches, layout_fixture, options);
    defer serial.deinit();
    options.stats = &parallel_stats;
    const backends: [4]solver.Backend = @splat(ParallelTestBackend.backend);
    for (2..5) |workers| {
        var parallel = try prepareParallel(state, &batches, layout_fixture, options, backends[0..workers]);
        defer parallel.deinit();
        const actual = try checkpoint.encode(std.testing.allocator, parallel.state, .{});
        defer std.testing.allocator.free(actual);
        const expected = try checkpoint.encode(std.testing.allocator, serial.state, .{});
        defer std.testing.allocator.free(expected);
        try std.testing.expectEqualSlices(u8, expected, actual);
        try std.testing.expectEqual(serial_stats.completed_chunks, parallel_stats.completed_chunks);
        try std.testing.expectEqual(serial_stats.source_global_bytes, parallel_stats.source_global_bytes);
        try std.testing.expectEqual(serial_stats.candidate_global_bytes, parallel_stats.candidate_global_bytes);
    }
}
test "attention_match parallel cancellation worker failures and aggregate admission preserve source" {
    const batches = batchesFixture();
    const backends: [4]solver.Backend = @splat(ParallelTestBackend.backend);
    var options: Options = .{ .recent_rows = 1, .chunk_rows = 2, .rejected_head_policy = .preserve_original };
    try std.testing.expectError(error.InvalidFitConfig, prepareParallel(state, &batches, layout_fixture, options, backends[0..1]));
    options.max_transient_host_bytes = 2 * try state.residentBytes() + 4;
    try std.testing.expectError(error.CompactionWorkspaceLimit, prepareParallel(state, &batches, layout_fixture, options, &backends));
    options.max_transient_host_bytes = 1024 * 1024;
    const failing: [2]solver.Backend = @splat(.{ .workspace_fn = ParallelTestBackend.failWorkspace });
    try std.testing.expectError(error.InjectedWorkerFailure, prepareParallel(state, &batches, layout_fixture, options, &failing));
    const CancelParallel = struct {
        active: std.atomic.Value(bool) = .init(false),
        calls: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            if (self.active.swap(true, .acq_rel)) return error.ConcurrentControlCallback;
            defer self.active.store(false, .release);
            self.calls += 1;
            if (self.calls >= 10) return error.CompactionCancelled;
        }
    };
    var cancellation: CancelParallel = .{};
    options.fit.control = .{ .context = &cancellation, .check_fn = CancelParallel.check };
    try std.testing.expectError(error.CompactionCancelled, prepareParallel(state, &batches, layout_fixture, options, &backends));
    try std.testing.expectEqual(@as(usize, 10), cancellation.calls);
    try std.testing.expectEqualSlices(f32, &.{ 2, 4, 6, 8 }, state.donors[0].heads[0].values);
    try std.testing.expectEqualSlices(u64, &.{ 0, 1, 2, 3 }, state.donors[0].heads[0].positions);
}

test "attention_match parallel rejects aliased mutable backend contexts before execution" {
    const batches = batchesFixture();
    var contexts: [2]u8 = .{ 0, 0 };
    var backends = [_]solver.Backend{ ParallelTestBackend.backend, ParallelTestBackend.backend };
    backends[0].context = &contexts[0];
    backends[1].context = &contexts[0];
    try std.testing.expectError(error.InvalidFitConfig, prepareParallel(state, &batches, layout_fixture, .{ .recent_rows = 1 }, &backends));
    backends[1].context = &contexts[1];
    var candidate = try prepareParallel(state, &batches, layout_fixture, .{ .recent_rows = 1 }, &backends);
    defer candidate.deinit();
}

test "attention_match repeated fit preserves exact prefix fitted history and exact recent tail" {
    const h: checkpoint.Head = .{ .dim = 1, .compacted_until = 4, .positions = &.{ 0, 1, 4, 5, 6, 7 }, .coverage_ends = &.{ 1, 4, 5, 6, 7, 8 }, .keys = &.{ 0, 0, 0, 0, 0, 0 }, .values = &.{ 3, 3, 3, 3, 3, 3 }, .bias = &.{ 0, @log(@as(f32, 3)), 0, 0, 0, 0 } };
    const lh: checkpoint.Head = .{ .dim = 1, .positions = &.{ 6, 7 }, .coverage_ends = &.{ 7, 8 }, .keys = &.{ 0, 0 }, .values = &.{ 3, 3 }, .bias = &.{ 0, 0 } };
    var source = state;
    source.logical_length = 8;
    source.tokens = &.{ 0, 0, 0, 0, 0, 0, 0, 0 };
    source.donors = &.{ .{ .layer = 0, .heads = &.{ h, h } }, .{ .layer = 1, .window = 2, .heads = &.{ lh, lh } } };
    var batches = batchesFixture();
    for (&batches) |*b| {
        b.train_positions = &.{8};
        b.heldout_positions = &.{9};
    }
    var stats: Stats = .{};
    const options: Options = .{ .prefix_rows = 1, .recent_rows = 1, .validation = .whole_head, .stats = &stats };
    var result = try prepare(std.testing.allocator, source, &batches, layout_fixture, options);
    defer result.deinit();
    const actual = result.state.donors[0].heads[0];
    try std.testing.expectEqual(@as(u64, 7), actual.compacted_until);
    try std.testing.expectEqualSlices(u64, &.{ 0, 1, 4, 7 }, actual.positions);
    try std.testing.expectEqualSlices(u64, &.{ 1, 4, 7, 8 }, actual.coverage_ends);
    try std.testing.expectEqualSlices(u8, std.mem.sliceAsBytes(h.bias[0..2]), std.mem.sliceAsBytes(actual.bias[0..2]));
    try std.testing.expectEqual(@as(f32, 0), actual.bias[3]);
    var unchanged = try prepare(std.testing.allocator, result.state, &batches, layout_fixture, options);
    defer unchanged.deinit();
    try std.testing.expectEqual(@as(usize, 0), stats.attempted_chunks);
    const first = try checkpoint.encode(std.testing.allocator, result.state, .{});
    defer std.testing.allocator.free(first);
    const second = try checkpoint.encode(std.testing.allocator, unchanged.state, .{});
    defer std.testing.allocator.free(second);
    try std.testing.expectEqualSlices(u8, first, second);
}

test "attention_match whole-head validation gate cancellation preserves permits" {
    var gate: HeadValidationGate = .{};
    const Cancel = struct {
        calls: usize = 0,
        after: usize,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls >= self.after) return error.CompactionCancelled;
        }
    };
    var cancel: Cancel = .{ .after = 2 };
    try std.testing.expectError(error.CompactionCancelled, gate.enter(.{ .context = &cancel, .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 0), gate.active.load(.acquire));
    try gate.enter(.{});
    try gate.enter(.{});
    cancel = .{ .after = 3 };
    try std.testing.expectError(error.CompactionCancelled, gate.enter(.{ .context = &cancel, .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 2), gate.active.load(.acquire));
    gate.leave();
    try gate.enter(.{});
    gate.leave();
    gate.leave();
    try std.testing.expectEqual(@as(usize, 0), gate.active.load(.acquire));
}

test "attention_match one-permit validation gate releases cancellation reservation" {
    var gate: HeadValidationGate = .{ .maximum = 1 };
    try gate.enter(.{});
    const Cancel = struct {
        fn check(_: ?*anyopaque) !void {
            return error.CompactionCancelled;
        }
    };
    try std.testing.expectError(error.CompactionCancelled, gate.enter(.{ .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 1), gate.active.load(.acquire));
    gate.leave();
    try gate.enter(.{});
    gate.leave();
    try std.testing.expectEqual(@as(usize, 0), gate.active.load(.acquire));
}
test "attention_match validation concurrency rejects unbounded configurations" {
    const batches = batchesFixture();
    for ([_]usize{ 0, 5 }) |maximum| try std.testing.expectError(error.InvalidFitConfig, prepare(std.testing.allocator, state, &batches, layout_fixture, .{ .max_parallel_validations = maximum }));
}
