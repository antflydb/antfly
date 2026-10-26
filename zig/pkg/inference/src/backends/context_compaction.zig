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

//! Bounded, device-resident attention-matching fitter. Inputs are real
//! post-RoPE queries and immutable original f16 KV. Publication is deliberately
//! separate: a successful fit owns staging buffers, never live cache pointers.
const std = @import("std");
const builtin = @import("builtin");
const shared = @import("metal_generated_kernels.zig");
const mr = @import("metal_runtime.zig");
const reference = @import("../runtime/kv/attention_matching.zig");
pub const Buffer = shared.Buffer;
pub const Ref = shared.Ref;
pub const Options = reference.Options;

pub const Request = struct {
    source_rows: u32,
    head_dim: u32,
    kv_heads: u32,
    kv_head: u32,
    training_rows: u32,
    validation_rows: u32,
    /// Equal contiguous strata, one per GQA/shared-layer consumer. Zero keeps
    /// the single-consumer contract for standalone callers.
    validation_group_rows: u32 = 0,
    scale: f32,
    /// Physical allocation budget including page slack, workspace, candidate
    /// KV, and mass/status buffer. Inputs are charged by their external owner.
    staging_budget_bytes: usize,
    options: Options = .{},

    fn validate(self: Request) !void {
        if (self.source_rows < 2 or self.source_rows > 512 or self.source_rows % 2 != 0 or
            self.head_dim == 0 or self.head_dim > 512 or self.kv_heads == 0 or self.kv_heads > 65535 or
            self.kv_head >= self.kv_heads or self.training_rows == 0 or self.validation_rows == 0 or
            self.training_rows >= reference.max_query_rows or self.validation_rows > reference.max_query_rows - self.training_rows)
            return error.InvalidCompactionShape;
        if (self.validation_group_rows > self.validation_rows or
            (self.validation_group_rows != 0 and self.validation_rows % self.validation_group_rows != 0)) return error.InvalidCompactionShape;
        const o = self.options;
        // Exact retention bypasses the fitter and copies the original f16 KV.
        // The fixed-page session still requests half blocks; packed generations
        // can request any bounded lossy row count without changing the gates.
        const rows = o.retained_rows orelse self.source_rows / 2;
        if (rows == 0 or rows >= self.source_rows) return error.UnsupportedCompactionProfile;
        if (!std.math.isFinite(self.scale) or self.scale <= 0 or
            !std.math.isFinite(o.ridge) or o.ridge <= 0 or
            !std.math.isFinite(o.mass_tolerance) or o.mass_tolerance <= 0 or
            !std.math.isFinite(o.output_tolerance) or o.output_tolerance <= 0 or
            o.max_iterations == 0 or o.max_iterations > 4096) return error.InvalidCompactionOptions;
        const elements = @as(u64, self.source_rows) * self.kv_heads * self.head_dim;
        if (elements > std.math.maxInt(u32)) return error.InvalidCompactionShape;
    }
};

pub const Allocation = struct {
    workspace_bytes: usize,
    candidate_bytes: usize,
    status_mass_bytes: usize,
    physical_bytes: usize,
};

pub const ValidationAllocation = struct {
    workspace_bytes: usize,
    status_mass_bytes: usize,
    physical_bytes: usize,
};

fn compensatedAddF32(sum: *f32, correction: *f32, value: f32) void {
    const adjusted = value - correction.*;
    const next = sum.* + adjusted;
    correction.* = (next - sum.*) - adjusted;
    sum.* = next;
}

pub fn allocation(request: Request) !Allocation {
    try request.validate();
    const n: usize = request.source_rows;
    const m = request.options.retained_rows orelse n / 2;
    const q: usize = request.training_rows;
    const h: usize = request.validation_rows;
    const d: usize = request.head_dim;
    // Same consecutive offsets as Workspace in context_compaction.metal.
    const workspace = (q * n + q * d + n + m + m + q * m + q + m * m + m * d +
        1 + h * (n + m) + 2 * h * d + h) * 4;
    const candidate = m * d * 2;
    const status_mass = (8 + m) * 4;
    const page = std.heap.pageSize();
    const physical = std.mem.alignForward(usize, workspace, page) +
        2 * std.mem.alignForward(usize, candidate, page) + std.mem.alignForward(usize, status_mass, page);
    return .{ .workspace_bytes = workspace, .candidate_bytes = candidate, .status_mass_bytes = status_mass, .physical_bytes = physical };
}

/// Scratch charged by diagnostic revalidation of caller-owned stored bytes.
/// Candidate K/V/bias remain externally owned and are not included here.
pub fn validationAllocation(request: Request) !ValidationAllocation {
    const sizes = try allocation(request);
    const page = std.heap.pageSize();
    return .{
        .workspace_bytes = sizes.workspace_bytes,
        .status_mass_bytes = sizes.status_mass_bytes,
        .physical_bytes = std.mem.alignForward(usize, sizes.workspace_bytes, page) +
            std.mem.alignForward(usize, sizes.status_mass_bytes, page),
    };
}

pub const Report = extern struct {
    failure_flags: u32,
    iterations: u32,
    mass_relative_max_error: f32,
    output_relative_rmse: f32,
    training_mass_relative_max_error: f32,
    /// Bit 0: training mass; bit 1: held-out mass; bit 2: held-out output.
    /// Zero-valued error fields are meaningful only when their bit is set.
    evaluated: u32,
    max_consumer_output_relative_rmse: f32,
    validation_groups: u32,
};
comptime {
    std.debug.assert(@sizeOf(Report) == 32);
}

pub const Candidate = struct {
    rows: u32,
    head_dim: u32,
    keys: Buffer,
    values: Buffer,
    status_mass: Buffer,
    report: Report,
    peak_staging_bytes: usize,
    /// No other device data is read by the fitter. Tests may inspect candidate
    /// data after completion; serving transfers these refs to the cache owner.
    status_readback_bytes: usize = @sizeOf(Report),

    pub fn mass(self: Candidate) Ref {
        return self.status_mass.ref().slice(@sizeOf(Report), self.status_mass.logical_bytes - @sizeOf(Report)) catch unreachable;
    }
    pub fn deinit(self: *Candidate) void {
        self.keys.deinit();
        self.values.deinit();
        self.status_mass.deinit();
        self.* = undefined;
    }
};

/// Caller-owned stored representation. This type has no publication or packing
/// authority; it is accepted only as input to device revalidation.
pub const StoredCandidate = struct {
    rows: u32,
    keys: Ref,
    values: Ref,
    bias: Ref,
};

pub const StoredValidation = struct {
    report: Report,
    peak_staging_bytes: usize,
    status_readback_bytes: usize = @sizeOf(Report),
};

pub const Kernels = struct {
    core: shared.Kernels,
    status_readback_bytes: u64 = 0,
    last_report: ?Report = null,
    execution_control: ?@import("../execution_control.zig").InferenceExecutionControl = null,

    pub fn init() !Kernels {
        return initTracked(null);
    }
    pub fn initTracked(tracker: ?*shared.BufferAllocationTracker) !Kernels {
        var core = try shared.Kernels.initSourceTracked(@embedFile("context_compaction.metal"), "context_compaction", tracker);
        core.batching_enabled = true;
        return .{ .core = core };
    }
    pub fn deinit(self: *Kernels) void {
        self.core.deinit();
    }
    pub fn initBorrowedTracked(execution: *const shared.Execution, tracker: ?*shared.BufferAllocationTracker) !Kernels {
        var core = try shared.Kernels.initBorrowedTracked(execution, tracker);
        core.batching_enabled = true;
        return .{ .core = core };
    }

    /// The producer must finish its command buffer before this independently
    /// owned queue consumes inputs. All stages complete before returning, even
    /// on failure. Inputs are borrowed and never mutated or copied to the CPU.
    pub fn fit(self: *Kernels, request: Request, keys: Ref, values: Ref, training: Ref, validation: Ref) !Candidate {
        self.last_report = null;
        const sizes = try allocation(request);
        if (sizes.physical_bytes > request.staging_budget_bytes) return error.ContextStagingBudgetExceeded;
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        try self.checkCancellation();
        const source_bytes = @as(usize, request.source_rows) * request.kv_heads * request.head_dim * 2;
        const training_bytes = @as(usize, request.training_rows) * request.head_dim * 4;
        const validation_bytes = @as(usize, request.validation_rows) * request.head_dim * 4;
        if (keys.bytes < source_bytes or values.bytes < source_bytes or training.bytes < training_bytes or validation.bytes < validation_bytes)
            return error.MetalBufferRange;
        for ([_]Ref{ keys, values, training, validation }) |ref| {
            if (ref.offset % 4 != 0) return error.MetalBufferRange;
        }
        var workspace = try self.core.buffer(sizes.workspace_bytes);
        defer workspace.deinit();
        var candidate_keys = try self.core.buffer(sizes.candidate_bytes);
        errdefer candidate_keys.deinit();
        var candidate_values = try self.core.buffer(sizes.candidate_bytes);
        errdefer candidate_values.deinit();
        var status_mass = try self.core.buffer(sizes.status_mass_bytes);
        errdefer status_mass.deinit();
        @memset(status_mass.bytes[0..@sizeOf(Report)], 0);
        try self.core.beginBatch();
        // Registered after all buffer defers: finish/cancel GPU use before
        // freeing either successful staging allocations or scratch on errors.
        errdefer self.core.cancelBatch();
        const n = request.source_rows;
        const m: u32 = @intCast(request.options.retained_rows orelse n / 2);
        const q = request.training_rows;
        const h = request.validation_rows;
        const d = request.head_dim;
        const pursuit = request.options.key_selection == .matching_pursuit;
        const dispatches = [_]u32{ q * n * 32, q, q * d, if (pursuit) 32 else 1, 32, q, m * m, m * d, 32, d, m * d, h * (n + m) * 32, h, h * d, 1, q * d };
        const refs = [_]Ref{ keys, values, training, validation, workspace.ref(), candidate_keys.ref(), candidate_values.ref(), status_mass.ref() };
        for ([_]u32{ 0, 1, 2, 3, 4, 5, 15, 6, 7, 8, 9, 10, 11, 12, 13, 14 }) |op| {
            const threads = dispatches[op];
            try self.encode(.{
                .op = @intCast(op),
                .n = threads,
                .dim = d,
                .groups = n | (m << 16),
                .position = q,
                .stride = h,
                .mode = @as(u32, @intCast(request.options.max_iterations)) | (if (pursuit) @as(u32, 1 << 16) else 0) | (request.validation_group_rows << 17),
                .index = (request.kv_heads << 16) | request.kv_head,
                .epsilon = request.options.ridge,
                .scale = request.scale,
                .theta = request.options.mass_tolerance,
                .reserved = request.options.output_tolerance,
            }, refs);
        }
        try self.core.endBatch();
        try self.checkCancellation();
        // Exactly one fixed-size status readback, after GPU completion.
        const report = std.mem.bytesToValue(Report, status_mass.bytes[0..@sizeOf(Report)]);
        self.last_report = report;
        self.status_readback_bytes +|= @sizeOf(Report);
        if (report.failure_flags & 1 != 0) return error.NonFiniteCompactionInput;
        if (report.failure_flags & 2 != 0) return error.CompactionFitDiverged;
        if (report.failure_flags != 0) return error.CompactionQualityRejected;
        if (!std.math.isFinite(report.mass_relative_max_error) or !std.math.isFinite(report.output_relative_rmse) or
            !std.math.isFinite(report.max_consumer_output_relative_rmse) or
            report.max_consumer_output_relative_rmse > request.options.output_tolerance or
            report.validation_groups != (if (request.validation_group_rows == 0) @as(u32, 1) else request.validation_rows / request.validation_group_rows) or
            report.evaluated != 7 or report.iterations != request.options.max_iterations) return error.InvalidCompactionReport;
        return .{ .rows = m, .head_dim = d, .keys = candidate_keys, .values = candidate_values, .status_mass = status_mass, .report = report, .peak_staging_bytes = sizes.physical_bytes };
    }

    /// Revalidate arbitrary stored f16 K/V plus f32 bias against the original
    /// source and frozen training/held query partitions. This diagnostic seam
    /// never constructs Candidate, cannot call packHead, and confers no serving
    /// or publication authority.
    pub fn validateStored(self: *Kernels, request: Request, keys: Ref, values: Ref, training: Ref, validation: Ref, stored: StoredCandidate) !StoredValidation {
        self.last_report = null;
        try request.validate();
        const n = request.source_rows;
        const m: u32 = @intCast(request.options.retained_rows orelse n / 2);
        const q = request.training_rows;
        const h = request.validation_rows;
        const d = request.head_dim;
        if (stored.rows != m) return error.InvalidCompactionShape;
        const sizes = try validationAllocation(request);
        if (sizes.physical_bytes > request.staging_budget_bytes) return error.ContextStagingBudgetExceeded;
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        try self.checkCancellation();

        const source_bytes = @as(usize, n) * request.kv_heads * d * 2;
        const training_bytes = @as(usize, q) * d * 4;
        const validation_bytes = @as(usize, h) * d * 4;
        const candidate_bytes = @as(usize, m) * d * 2;
        const bias_bytes = @as(usize, m) * 4;
        if (keys.bytes < source_bytes or values.bytes < source_bytes or training.bytes < training_bytes or validation.bytes < validation_bytes or
            stored.keys.bytes < candidate_bytes or stored.values.bytes < candidate_bytes or stored.bias.bytes < bias_bytes)
            return error.MetalBufferRange;
        for ([_]Ref{ keys, values, training, validation, stored.keys, stored.values, stored.bias }) |ref| {
            if (ref.offset % 4 != 0) return error.MetalBufferRange;
        }
        for ([_]Ref{ stored.keys, stored.values, stored.bias }, 0..) |candidate_ref, i| {
            for (([_]Ref{ stored.keys, stored.values, stored.bias })[0..i]) |prior|
                if (candidate_ref.handle == prior.handle) return error.AliasedContextStaging;
            for ([_]Ref{ keys, values, training, validation }) |source|
                if (candidate_ref.handle == source.handle) return error.AliasedContextStaging;
        }

        var workspace = try self.core.buffer(sizes.workspace_bytes);
        defer workspace.deinit();
        var status_mass = try self.core.buffer(sizes.status_mass_bytes);
        defer status_mass.deinit();
        @memset(status_mass.bytes[0..@sizeOf(Report)], 0);
        try self.core.beginBatch();
        errdefer self.core.cancelBatch();
        const params = shared.Params{
            .op = 0,
            .dim = d,
            .groups = n | (m << 16),
            .position = q,
            .stride = h,
            .mode = request.validation_group_rows << 17,
            .index = (request.kv_heads << 16) | request.kv_head,
            .scale = request.scale,
            .theta = request.options.mass_tolerance,
            .reserved = request.options.output_tolerance,
        };
        // Copy caller-owned bias into the private report/bias allocation. No
        // candidate bytes are read by the CPU.
        var copy_params = params;
        copy_params.op = 20;
        copy_params.n = m;
        try self.encode(copy_params, .{ self.core.dummy.ref(), self.core.dummy.ref(), stored.bias, self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), self.core.dummy.ref(), status_mass.ref() });
        const refs = [_]Ref{ keys, values, training, validation, workspace.ref(), stored.keys, stored.values, status_mass.ref() };
        for ([_]struct { op: u32, threads: u32 }{
            .{ .op = 17, .threads = q * (n + m) * 32 },
            .{ .op = 18, .threads = q },
            .{ .op = 19, .threads = 1 },
            .{ .op = 11, .threads = h * (n + m) * 32 },
            .{ .op = 12, .threads = h },
            .{ .op = 13, .threads = h * d },
            .{ .op = 14, .threads = 1 },
        }) |dispatch| {
            var encoded = params;
            encoded.op = dispatch.op;
            encoded.n = dispatch.threads;
            try self.encode(encoded, refs);
        }
        try self.core.endBatch();
        try self.checkCancellation();
        const report = std.mem.bytesToValue(Report, status_mass.bytes[0..@sizeOf(Report)]);
        self.last_report = report;
        self.status_readback_bytes +|= @sizeOf(Report);
        if (report.failure_flags & 1 != 0) return error.NonFiniteCompactionInput;
        if (report.failure_flags != 0) return error.CompactionQualityRejected;
        if (!std.math.isFinite(report.training_mass_relative_max_error) or
            !std.math.isFinite(report.mass_relative_max_error) or
            !std.math.isFinite(report.output_relative_rmse) or
            !std.math.isFinite(report.max_consumer_output_relative_rmse) or
            report.training_mass_relative_max_error > request.options.mass_tolerance or
            report.mass_relative_max_error > request.options.mass_tolerance or
            report.output_relative_rmse > request.options.output_tolerance or
            report.max_consumer_output_relative_rmse > request.options.output_tolerance or
            report.validation_groups != (if (request.validation_group_rows == 0) @as(u32, 1) else h / request.validation_group_rows) or
            report.evaluated != 7 or report.iterations != 0)
            return error.InvalidCompactionReport;
        return .{ .report = report, .peak_staging_bytes = sizes.physical_bytes };
    }

    /// Test-only composition of genuine stored-payload validation and
    /// device copy. It exposes no independently callable copy capability: the
    /// exact refs copied here are first evaluated by validateStored, and the
    /// destination remains caller-owned unpublished staging. The caller must
    /// exclusively own and keep all supplied refs immutable until this
    /// synchronous operation returns; a sealed loader lease provides that
    /// lifetime in the later transaction layer.
    pub fn validateAndCopyStoredDiagnostic(
        self: *Kernels,
        request: Request,
        keys: Ref,
        values: Ref,
        training: Ref,
        validation: Ref,
        stored: StoredCandidate,
        destination_keys: Ref,
        destination_values: Ref,
        destination_bias: Ref,
    ) !StoredValidation {
        if (!builtin.is_test) return error.DiagnosticOnly;
        try self.validateStoredCopyDestination(
            request,
            stored,
            destination_keys,
            destination_values,
            destination_bias,
            &.{ keys, values, training, validation },
        );
        const result = try self.validateStored(request, keys, values, training, validation, stored);
        try self.copyStoredHeadDiagnostic(
            request,
            stored,
            destination_keys,
            destination_values,
            destination_bias,
            &.{ keys, values, training, validation },
        );
        return result;
    }

    fn validateStoredCopyDestination(
        self: *const Kernels,
        request: Request,
        stored: StoredCandidate,
        keys: Ref,
        values: Ref,
        bias: Ref,
        protected: []const Ref,
    ) !void {
        try request.validate();
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        const rows = request.options.retained_rows orelse request.source_rows / 2;
        if (stored.rows != rows) return error.InvalidCompactionShape;
        const candidate_elements = std.math.mul(usize, @as(usize, stored.rows), @as(usize, request.head_dim)) catch return error.InvalidCompactionShape;
        const candidate_bytes = std.math.mul(usize, candidate_elements, 2) catch return error.InvalidCompactionShape;
        const bias_bytes = std.math.mul(usize, @as(usize, stored.rows), 4) catch return error.InvalidCompactionShape;
        if (stored.keys.bytes < candidate_bytes or stored.values.bytes < candidate_bytes or stored.bias.bytes < bias_bytes or
            keys.bytes < candidate_bytes or values.bytes < candidate_bytes or bias.bytes < bias_bytes) return error.MetalBufferRange;
        const sources = [_]Ref{ stored.keys, stored.values, stored.bias };
        for ([_]Ref{ keys, values, bias }, 0..) |destination, i| {
            if (destination.offset % 4 != 0) return error.MetalBufferRange;
            for (sources) |source| if (destination.handle == source.handle) return error.AliasedContextStaging;
            for (protected) |source| if (destination.handle == source.handle) return error.AliasedContextStaging;
            for (([_]Ref{ keys, values, bias })[0..i]) |prior|
                if (destination.handle == prior.handle) return error.AliasedContextStaging;
        }
    }

    fn copyStoredHeadDiagnostic(
        self: *Kernels,
        request: Request,
        stored: StoredCandidate,
        keys: Ref,
        values: Ref,
        bias: Ref,
        protected: []const Ref,
    ) !void {
        // Recheck the identical immutable refs after validation. The first
        // preflight occurred before any GPU dispatch; this second check closes
        // the synchronous validate/copy seam without exposing a bearer token.
        try self.validateStoredCopyDestination(request, stored, keys, values, bias, protected);
        try self.checkCancellation();
        try self.encode(.{
            .op = 16,
            .n = stored.rows * request.head_dim,
            .dim = request.head_dim,
            .groups = stored.rows,
            .index = 1 << 16,
        }, .{ stored.keys, stored.values, stored.bias, self.core.dummy.ref(), bias, keys, values, self.core.dummy.ref() });
        try self.checkCancellation();
    }

    /// Copy a completed per-head candidate into reserved interleaved staging
    /// entirely on the GPU. The destination must not be a published cache.
    /// Shared-storage memcpy helpers are intentionally not used here.
    pub fn packHead(self: *Kernels, candidate: *const Candidate, kv_head: u32, kv_heads: u32, keys: Ref, values: Ref, mass: Ref) !void {
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        if (candidate.rows == 0 or candidate.rows >= 512 or candidate.head_dim == 0 or candidate.head_dim > 512 or
            kv_heads == 0 or kv_heads > 65535 or kv_head >= kv_heads) return error.InvalidCompactionShape;
        const candidate_elements = @as(usize, candidate.rows) * candidate.head_dim;
        if (candidate.keys.logical_bytes < candidate_elements * 2 or candidate.values.logical_bytes < candidate_elements * 2 or
            candidate.status_mass.logical_bytes < @sizeOf(Report) + @as(usize, candidate.rows) * 4) return error.MetalBufferRange;
        const elements = @as(u64, candidate.rows) * candidate.head_dim * kv_heads;
        if (elements > std.math.maxInt(u32)) return error.InvalidCompactionShape;
        if (keys.bytes < elements * 2 or values.bytes < elements * 2 or mass.bytes < @as(usize, candidate.rows) * kv_heads * 4)
            return error.MetalBufferRange;
        const refs = [_]Ref{ candidate.keys.ref(), candidate.values.ref(), candidate.mass(), self.core.dummy.ref(), mass, keys, values, self.core.dummy.ref() };
        for ([_]Ref{ keys, values, mass }, 0..) |dest, i| {
            if (dest.offset % 4 != 0) return error.MetalBufferRange;
            for ([_]Ref{ candidate.keys.ref(), candidate.values.ref(), candidate.mass() }) |source|
                if (dest.handle == source.handle) return error.AliasedContextStaging;
            for (([_]Ref{ keys, values, mass })[0..i]) |prior|
                if (dest.handle == prior.handle) return error.AliasedContextStaging;
        }
        try self.encode(.{ .op = 16, .n = candidate.rows * candidate.head_dim, .dim = candidate.head_dim, .groups = candidate.rows, .index = (kv_heads << 16) | kv_head }, refs);
        try self.checkCancellation();
    }

    fn checkCancellation(self: *const Kernels) !void {
        if (self.core.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        if (self.execution_control) |control| try control.check();
    }

    fn encode(self: *Kernels, params: shared.Params, refs: [8]Ref) !void {
        try self.checkCancellation();
        var handles: [8]?*anyopaque = undefined;
        var offsets: [8]usize = undefined;
        var lengths: [8]usize = undefined;
        for (refs, 0..) |ref, i| {
            handles[i] = ref.handle;
            offsets[i] = ref.offset;
            lengths[i] = ref.bytes;
        }
        // These opcodes have their own validated shapes; the Qwen-specific
        // opcode checks in shared.Kernels.run do not apply to this program.
        if (mr.termite_metal_generated_dispatch(self.core.runtime, self.core.pipeline, &handles, &offsets, &lengths, 8, &params, @sizeOf(shared.Params), params.n) != 0)
            return error.ContextMetalDispatchFailed;
    }
};

fn halves(buffer: Buffer) []f16 {
    return std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(buffer.bytes[0..buffer.logical_bytes])));
}

test "context Metal retention profiles charge their requested physical allocation" {
    var request = Request{ .source_rows = 512, .head_dim = 512, .kv_heads = 2, .kv_head = 0, .training_rows = 3072, .validation_rows = 1024, .scale = 1, .staging_budget_bytes = 0 };
    const legacy = try allocation(request);
    request.options.retained_rows = 256;
    try std.testing.expectEqualDeep(legacy, try allocation(request));
    var previous: usize = 0;
    for ([_]usize{ 1, 128, 256, 384, 448, 511 }) |retained| {
        request.options.retained_rows = retained;
        const size = try allocation(request);
        try std.testing.expectEqual(retained * 512 * 2, size.candidate_bytes);
        try std.testing.expect(size.physical_bytes > previous);
        previous = size.physical_bytes;
    }
    for ([_]usize{ 0, 512, std.math.maxInt(usize) }) |retained| {
        request.options.retained_rows = retained;
        try std.testing.expectError(error.UnsupportedCompactionProfile, allocation(request));
    }
}

test "stored candidate validation allocation excludes caller-owned payload" {
    const request = Request{ .source_rows = 512, .head_dim = 512, .kv_heads = 1, .kv_head = 0, .training_rows = 384, .validation_rows = 128, .scale = 1, .staging_budget_bytes = std.math.maxInt(usize), .options = .{ .retained_rows = 352 } };
    const fitted = try allocation(request);
    const validated = try validationAllocation(request);
    try std.testing.expectEqual(fitted.workspace_bytes, validated.workspace_bytes);
    try std.testing.expectEqual(fitted.status_mass_bytes, validated.status_mass_bytes);
    try std.testing.expect(validated.physical_bytes < fitted.physical_bytes);
}

test "compensated validation reduction preserves strict aggregate parity" {
    // Models the long, serial, nonnegative reduction used by the device
    // validation kernel. A plain f32 sum crosses the unchanged 1e-5 parity
    // tolerance; the compensated recurrence used by the Metal kernel does not.
    const terms = 300_000;
    const term: f32 = 1e-7;
    var naive: f32 = 1;
    var compensated: f32 = 1;
    var correction: f32 = 0;
    for (0..terms) |_| {
        naive += term;
        compensatedAddF32(&compensated, &correction, term);
    }
    const exact = 1.0 + @as(f64, @floatFromInt(terms)) * @as(f64, term);
    try std.testing.expect(@abs(@as(f64, naive) - exact) > 1e-5);
    try std.testing.expect(@abs(@as(f64, compensated) - exact) < 1e-6);
}

test "context Metal validates arbitrary stored bytes without candidate authority" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    var keys = try Buffer.init(kernels.core.runtime, 4 * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 4 * 2);
    defer values.deinit();
    var training = try Buffer.init(kernels.core.runtime, 3 * 4);
    defer training.deinit();
    var held = try Buffer.init(kernels.core.runtime, 2 * 4);
    defer held.deinit();
    var candidate_keys = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer candidate_keys.deinit();
    var candidate_values = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer candidate_values.deinit();
    var candidate_bias = try Buffer.init(kernels.core.runtime, 2 * 4);
    defer candidate_bias.deinit();
    @memset(halves(keys), 0);
    @memset(halves(values), 1);
    @memset(training.floats(), 0);
    @memset(held.floats(), 0);
    @memset(halves(candidate_keys), 0);
    @memset(halves(candidate_values), 1);
    @memset(candidate_bias.floats(), @log(@as(f32, 2)));
    const keys_before = halves(candidate_keys)[0..2].*;
    const values_before = halves(candidate_values)[0..2].*;
    const bias_before = candidate_bias.floats()[0..2].*;
    var request = Request{ .source_rows = 4, .head_dim = 1, .kv_heads = 1, .kv_head = 0, .training_rows = 3, .validation_rows = 2, .validation_group_rows = 1, .scale = 1, .staging_budget_bytes = 0, .options = .{ .retained_rows = 2 } };
    request.staging_budget_bytes = (try validationAllocation(request)).physical_bytes;
    const stored = StoredCandidate{ .rows = 2, .keys = candidate_keys.ref(), .values = candidate_values.ref(), .bias = candidate_bias.ref() };
    const result = try kernels.validateStored(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored);
    try std.testing.expectEqual(@as(u32, 7), result.report.evaluated);
    try std.testing.expectEqual(@as(u32, 0), result.report.iterations);
    try std.testing.expectEqual(@as(u32, 2), result.report.validation_groups);
    try std.testing.expectApproxEqAbs(@as(f32, 0), result.report.training_mass_relative_max_error, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), result.report.mass_relative_max_error, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), result.report.max_consumer_output_relative_rmse, 1e-6);
    try std.testing.expectEqualSlices(f16, &keys_before, halves(candidate_keys)[0..2]);
    try std.testing.expectEqualSlices(f16, &values_before, halves(candidate_values)[0..2]);
    try std.testing.expectEqualSlices(f32, &bias_before, candidate_bias.floats()[0..2]);

    @memset(halves(candidate_values), 0);
    try std.testing.expectError(error.CompactionQualityRejected, kernels.validateStored(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored));
    try std.testing.expectEqual(@as(u32, 7), kernels.last_report.?.evaluated);
    @memset(halves(candidate_values), 1);
    candidate_bias.floats()[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteCompactionInput, kernels.validateStored(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored));
    try std.testing.expectEqual(@as(u32, 0), kernels.last_report.?.evaluated);
    candidate_bias.floats()[0] = @log(@as(f32, 2));
    try std.testing.expectError(error.AliasedContextStaging, kernels.validateStored(request, keys.ref(), values.ref(), training.ref(), held.ref(), .{ .rows = 2, .keys = try keys.ref().slice(0, 4), .values = candidate_values.ref(), .bias = candidate_bias.ref() }));
    request.staging_budget_bytes = 0;
    try std.testing.expectError(error.ContextStagingBudgetExceeded, kernels.validateStored(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored));
}

test "stored validation isolates training mass failure from passing held metrics" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    var keys = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer values.deinit();
    var training = try Buffer.init(kernels.core.runtime, 4);
    defer training.deinit();
    var held = try Buffer.init(kernels.core.runtime, 4);
    defer held.deinit();
    var candidate_keys = try Buffer.init(kernels.core.runtime, 2);
    defer candidate_keys.deinit();
    var candidate_values = try Buffer.init(kernels.core.runtime, 2);
    defer candidate_values.deinit();
    var candidate_bias = try Buffer.init(kernels.core.runtime, 4);
    defer candidate_bias.deinit();
    @memcpy(halves(keys), &[_]f16{ -1, 1 });
    @memset(halves(values), 1);
    training.floats()[0] = 1;
    held.floats()[0] = 0;
    halves(candidate_keys)[0] = 0;
    halves(candidate_values)[0] = 1;
    candidate_bias.floats()[0] = @log(@as(f32, 2));
    var request = Request{ .source_rows = 2, .head_dim = 1, .kv_heads = 1, .kv_head = 0, .training_rows = 1, .validation_rows = 1, .validation_group_rows = 1, .scale = 1, .staging_budget_bytes = 0, .options = .{ .retained_rows = 1 } };
    request.staging_budget_bytes = (try validationAllocation(request)).physical_bytes;
    try std.testing.expectError(error.CompactionQualityRejected, kernels.validateStored(request, keys.ref(), values.ref(), training.ref(), held.ref(), .{ .rows = 1, .keys = candidate_keys.ref(), .values = candidate_values.ref(), .bias = candidate_bias.ref() }));
    const report = kernels.last_report.?;
    try std.testing.expectEqual(@as(u32, 7), report.evaluated);
    try std.testing.expect(report.training_mass_relative_max_error > request.options.mass_tolerance);
    try std.testing.expectApproxEqAbs(@as(f32, 0), report.mass_relative_max_error, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), report.output_relative_rmse, 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 0), report.max_consumer_output_relative_rmse, 1e-6);
}

test "context Metal stored diagnostic validates before copy and preserves destination authority" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    var keys = try Buffer.init(kernels.core.runtime, 4 * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 4 * 2);
    defer values.deinit();
    var training = try Buffer.init(kernels.core.runtime, 3 * 4);
    defer training.deinit();
    var held = try Buffer.init(kernels.core.runtime, 2 * 4);
    defer held.deinit();
    var stored_keys = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer stored_keys.deinit();
    var stored_values = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer stored_values.deinit();
    var stored_bias = try Buffer.init(kernels.core.runtime, 2 * 4);
    defer stored_bias.deinit();
    var destination_keys = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer destination_keys.deinit();
    var destination_values = try Buffer.init(kernels.core.runtime, 2 * 2);
    defer destination_values.deinit();
    var destination_bias = try Buffer.init(kernels.core.runtime, 2 * 4);
    defer destination_bias.deinit();
    @memset(halves(keys), 0);
    @memset(halves(values), 1);
    @memset(training.floats(), 0);
    @memset(held.floats(), 0);
    @memcpy(halves(stored_keys), &[_]f16{ 3, -2 });
    @memcpy(halves(stored_values), &[_]f16{ 1, 1 });
    @memset(stored_bias.floats(), @log(@as(f32, 2)));
    const stored = StoredCandidate{ .rows = 2, .keys = stored_keys.ref(), .values = stored_values.ref(), .bias = stored_bias.ref() };
    var request = Request{ .source_rows = 4, .head_dim = 1, .kv_heads = 1, .kv_head = 0, .training_rows = 3, .validation_rows = 2, .validation_group_rows = 1, .scale = 1, .staging_budget_bytes = 0, .options = .{ .retained_rows = 2 } };
    request.staging_budget_bytes = (try validationAllocation(request)).physical_bytes;

    @memset(halves(destination_keys), 17);
    @memset(halves(destination_values), 17);
    @memset(destination_bias.floats(), 17);
    const result = try kernels.validateAndCopyStoredDiagnostic(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored, destination_keys.ref(), destination_values.ref(), destination_bias.ref());
    try std.testing.expectEqual(@as(u32, 7), result.report.evaluated);
    try std.testing.expectEqualSlices(f16, halves(stored_keys), halves(destination_keys));
    try std.testing.expectEqualSlices(f16, halves(stored_values), halves(destination_values));
    try std.testing.expectEqualSlices(f32, stored_bias.floats(), destination_bias.floats());

    @memset(halves(destination_values), 17);
    @memset(halves(stored_values), 0);
    try std.testing.expectError(error.CompactionQualityRejected, kernels.validateAndCopyStoredDiagnostic(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored, destination_keys.ref(), destination_values.ref(), destination_bias.ref()));
    try std.testing.expectEqualSlices(f16, &.{ 17, 17 }, halves(destination_values));
    @memset(halves(stored_values), 1);
    const readbacks = kernels.status_readback_bytes;
    const report_before_invalid_destination = kernels.last_report;
    try std.testing.expectError(error.AliasedContextStaging, kernels.validateAndCopyStoredDiagnostic(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored, stored_keys.ref(), destination_values.ref(), destination_bias.ref()));
    try std.testing.expectEqual(readbacks, kernels.status_readback_bytes);
    try std.testing.expectEqualDeep(report_before_invalid_destination, kernels.last_report);
    try std.testing.expectError(error.MetalBufferRange, kernels.validateAndCopyStoredDiagnostic(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored, try destination_keys.ref().slice(0, 2), destination_values.ref(), destination_bias.ref()));
    try std.testing.expectEqual(readbacks, kernels.status_readback_bytes);
    try std.testing.expectEqualDeep(report_before_invalid_destination, kernels.last_report);
    var stop = std.atomic.Value(bool).init(true);
    kernels.core.cancelled = &stop;
    try std.testing.expectError(error.Cancelled, kernels.validateAndCopyStoredDiagnostic(request, keys.ref(), values.ref(), training.ref(), held.ref(), stored, destination_keys.ref(), destination_values.ref(), destination_bias.ref()));
}

test "context Metal fitter rejects consumer error hidden by aggregate energy" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    var keys = try Buffer.init(kernels.core.runtime, 8);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 8);
    defer values.deinit();
    var train = try Buffer.init(kernels.core.runtime, 12);
    defer train.deinit();
    var held = try Buffer.init(kernels.core.runtime, 8);
    defer held.deinit();
    @memcpy(halves(keys), &[_]f16{ -1, -1, -1, 1 });
    @memcpy(halves(values), &[_]f16{ 0, 0, 1, -1 });
    @memcpy(train.floats(), &[_]f32{ -0.5, 0.5, 1 });
    @memcpy(held.floats(), &[_]f32{ -1, 0 });
    var request = Request{ .source_rows = 4, .head_dim = 1, .kv_heads = 1, .kv_head = 0, .training_rows = 3, .validation_rows = 2, .scale = 1, .staging_budget_bytes = 1024 * 1024 };
    var aggregate = try kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref());
    defer aggregate.deinit();
    try std.testing.expect(aggregate.report.output_relative_rmse < 0.01);
    try std.testing.expectEqual(@as(u32, 1), aggregate.report.validation_groups);
    request.validation_group_rows = 1;
    try std.testing.expectError(error.CompactionQualityRejected, kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref()));
    const rejected = kernels.last_report.?;
    try std.testing.expectEqual(@as(u32, 7), rejected.evaluated);
    try std.testing.expectEqual(@as(u32, 2), rejected.validation_groups);
    try std.testing.expect(rejected.output_relative_rmse < 0.01);
    try std.testing.expect(rejected.max_consumer_output_relative_rmse > request.options.output_tolerance);
    request.validation_group_rows = 3;
    try std.testing.expectError(error.InvalidCompactionShape, allocation(request));
}

test "context Metal matching pursuit recovers minority mass with unchanged validation" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    var keys = try Buffer.init(kernels.core.runtime, 8 * 4 * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 8 * 4 * 2);
    defer values.deinit();
    var queries = try Buffer.init(kernels.core.runtime, 101 * 4 * 4);
    defer queries.deinit();
    var held = try Buffer.init(kernels.core.runtime, 2 * 4 * 4);
    defer held.deinit();
    @memset(halves(keys), 0);
    @memset(halves(values), 0);
    for (0..6) |row| {
        halves(keys)[row * 4] = 4;
        @memset(halves(values)[row * 4 ..][0..4], 1);
    }
    halves(keys)[6 * 4 + 1] = 8;
    @memset(halves(values)[6 * 4 ..][0..4], -2);
    halves(keys)[7 * 4] = -8;
    halves(keys)[7 * 4 + 1] = -8;
    @memset(queries.floats(), 0);
    for (0..100) |row| queries.floats()[row * 4] = 4;
    queries.floats()[100 * 4 + 1] = 4;
    @memcpy(held.floats(), &[_]f32{ 3, 0, 0, 0, 0, 3, 0, 0 });
    var request = Request{ .source_rows = 8, .head_dim = 4, .kv_heads = 1, .kv_head = 0, .training_rows = 101, .validation_rows = 2, .scale = 1, .staging_budget_bytes = 0 };
    request.staging_budget_bytes = (try allocation(request)).physical_bytes;
    try std.testing.expectError(error.CompactionQualityRejected, kernels.fit(request, keys.ref(), values.ref(), queries.ref(), held.ref()));
    request.options.key_selection = .matching_pursuit;
    var candidate = try kernels.fit(request, keys.ref(), values.ref(), queries.ref(), held.ref());
    defer candidate.deinit();
    var found_rare = false;
    for (0..candidate.rows) |row| if (halves(candidate.keys)[row * 4 + 1] == 8) {
        found_rare = true;
    };
    try std.testing.expect(found_rare);
    try std.testing.expect(candidate.report.mass_relative_max_error < 0.01);
    try std.testing.expect(candidate.report.output_relative_rmse < 0.01);
}

test "context device fitter allocation includes simultaneous candidate and page slack" {
    const request = Request{ .source_rows = 512, .head_dim = 512, .kv_heads = 4, .kv_head = 3, .training_rows = 384, .validation_rows = 128, .scale = 1, .staging_budget_bytes = 0 };
    const sizes = try allocation(request);
    try std.testing.expect(sizes.physical_bytes >= sizes.workspace_bytes + 2 * sizes.candidate_bytes + sizes.status_mass_bytes);
    try std.testing.expect(sizes.physical_bytes < 8 * 1024 * 1024);
    var invalid = request;
    invalid.source_rows = 514;
    try std.testing.expectError(error.InvalidCompactionShape, allocation(invalid));
    invalid = request;
    invalid.training_rows = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidCompactionShape, allocation(invalid));
    var largest = request;
    largest.training_rows = reference.max_training_rows;
    largest.validation_rows = reference.max_validation_rows;
    try std.testing.expect((try allocation(largest)).physical_bytes < 32 * 1024 * 1024);
    largest.validation_rows += 1;
    try std.testing.expectError(error.InvalidCompactionShape, allocation(largest));
}

test "context Metal fitter covers every row in the maximum calibration envelope" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    var keys = try Buffer.init(kernels.core.runtime, 8 * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 8 * 2);
    defer values.deinit();
    var train = try Buffer.init(kernels.core.runtime, reference.max_training_rows * 4);
    defer train.deinit();
    var held = try Buffer.init(kernels.core.runtime, reference.max_validation_rows * 4);
    defer held.deinit();
    @memset(halves(keys), 0);
    @memset(halves(values), 1);
    @memset(train.floats(), 0);
    @memset(held.floats(), 1);
    const request = Request{ .source_rows = 8, .head_dim = 1, .kv_heads = 1, .kv_head = 0, .training_rows = reference.max_training_rows, .validation_rows = reference.max_validation_rows, .scale = 1, .staging_budget_bytes = 1024 * 1024, .options = .{ .key_selection = .matching_pursuit } };
    var candidate = try kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref());
    candidate.deinit();
    // The last query must be processed, not silently truncated to the old cap.
    train.floats()[reference.max_training_rows - 1] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteCompactionInput, kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref()));
    try std.testing.expectEqual(@as(u32, 0), kernels.last_report.?.evaluated);
    train.floats()[reference.max_training_rows - 1] = 0;
    held.floats()[reference.max_validation_rows - 1] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteCompactionInput, kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref()));
    try std.testing.expectEqual(@as(u32, 1), kernels.last_report.?.evaluated);
}

test "context Metal fitter preserves unequal mass and values and rejects failed candidates" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    var keys = try Buffer.init(kernels.core.runtime, 4 * 2 * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 4 * 2 * 2);
    defer values.deinit();
    var train = try Buffer.init(kernels.core.runtime, 3 * 4);
    defer train.deinit();
    var held = try Buffer.init(kernels.core.runtime, 3 * 4);
    defer held.deinit();
    @memcpy(halves(keys), &[_]f16{ 10, -1, 20, -1, 30, -1, 40, 1 });
    @memcpy(halves(values), &[_]f16{ 10, -1, 20, -2, 30, -3, 40, 4 });
    @memcpy(train.floats(), &[_]f32{ -0.5, 0.5, 1 });
    @memcpy(held.floats(), &[_]f32{ -1.5, 0, 2 });
    const request = Request{ .source_rows = 4, .head_dim = 1, .kv_heads = 2, .kv_head = 1, .training_rows = 3, .validation_rows = 3, .scale = 1, .staging_budget_bytes = 1024 * 1024 };
    var candidate = try kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref());
    defer candidate.deinit();
    try std.testing.expectEqualSlices(f16, &.{ -1, 1 }, halves(candidate.keys));
    try std.testing.expectApproxEqAbs(@as(f32, -2), @as(f32, halves(candidate.values)[0]), 0.002);
    try std.testing.expectApproxEqAbs(@as(f32, 4), @as(f32, halves(candidate.values)[1]), 0.002);
    try std.testing.expectApproxEqAbs(@log(@as(f32, 3)), candidate.status_mass.floats()[8], 1e-4);
    try std.testing.expectApproxEqAbs(@as(f32, 0), candidate.status_mass.floats()[9], 1e-4);
    try std.testing.expect(candidate.report.output_relative_rmse < 0.002);
    try std.testing.expectEqual(@as(u32, 7), candidate.report.evaluated);
    try std.testing.expectEqual(@as(usize, 32), candidate.status_readback_bytes);
    var packed_k = try Buffer.init(kernels.core.runtime, 2 * 2 * 2);
    defer packed_k.deinit();
    var packed_v = try Buffer.init(kernels.core.runtime, 2 * 2 * 2);
    defer packed_v.deinit();
    var packed_mass = try Buffer.init(kernels.core.runtime, 2 * 2 * 4);
    defer packed_mass.deinit();
    @memset(halves(packed_k), 17);
    @memset(halves(packed_v), 17);
    @memset(packed_mass.floats(), 17);
    try kernels.packHead(&candidate, 1, 2, packed_k.ref(), packed_v.ref(), packed_mass.ref());
    try std.testing.expectEqualSlices(f16, &.{ 17, -1, 17, 1 }, halves(packed_k));
    try std.testing.expectEqual(@as(f16, 17), halves(packed_v)[0]);
    try std.testing.expectApproxEqAbs(@as(f32, -2), @as(f32, halves(packed_v)[1]), 0.002);
    try std.testing.expectEqual(@as(f16, 17), halves(packed_v)[2]);
    try std.testing.expectApproxEqAbs(@as(f32, 4), @as(f32, halves(packed_v)[3]), 0.002);
    try std.testing.expectEqual(@as(f32, 17), packed_mass.floats()[0]);
    try std.testing.expectApproxEqAbs(@log(@as(f32, 3)), packed_mass.floats()[1], 1e-4);
    try std.testing.expectError(error.AliasedContextStaging, kernels.packHead(&candidate, 0, 1, candidate.keys.ref(), candidate.values.ref(), candidate.mass()));
    var denied = request;
    denied.staging_budget_bytes = 0;
    try std.testing.expectError(error.ContextStagingBudgetExceeded, kernels.fit(denied, keys.ref(), values.ref(), train.ref(), held.ref()));
    train.floats()[0] = std.math.nan(f32);
    try std.testing.expectError(error.NonFiniteCompactionInput, kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref()));
    train.floats()[0] = -0.5;
    // Held-out queries expose a fit that uniform training cannot identify.
    @memset(train.floats(), 0);
    try std.testing.expectError(error.CompactionQualityRejected, kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref()));
    const rejected = kernels.last_report.?;
    try std.testing.expectEqual(@as(u32, 7), rejected.evaluated);
    // Evaluate every validation row, including after another row fails the
    // mass gate. Otherwise thread scheduling can underreport the maximum.
    try std.testing.expectApproxEqAbs(@as(f32, @floatCast(1 - 4 / (3 + @exp(@as(f64, 4))))), rejected.mass_relative_max_error, 1e-5);
    try std.testing.expect(rejected.output_relative_rmse > 0.1);
    @memcpy(held.floats(), &[_]f32{ 2, 0, -1.5 });
    try std.testing.expectError(error.CompactionQualityRejected, kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref()));
    try std.testing.expectApproxEqAbs(rejected.mass_relative_max_error, kernels.last_report.?.mass_relative_max_error, 1e-6);
    try std.testing.expectApproxEqAbs(rejected.output_relative_rmse, kernels.last_report.?.output_relative_rmse, 1e-6);
    @memcpy(train.floats(), &[_]f32{ -0.5, 0.5, 1 });
    var recovered = try kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref());
    recovered.deinit();
    var stop = std.atomic.Value(bool).init(true);
    kernels.core.cancelled = &stop;
    try std.testing.expectError(error.Cancelled, kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref()));
}

test "context Metal fitter supports full age blocks at both Gemma head widths" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    for ([_]usize{ 256, 512 }) |d| {
        var keys = try Buffer.init(kernels.core.runtime, 512 * d * 2);
        defer keys.deinit();
        var values = try Buffer.init(kernels.core.runtime, 512 * d * 2);
        defer values.deinit();
        const training_rows: u32 = if (d == 512) 384 else 8;
        const validation_rows: u32 = if (d == 512) 128 else 4;
        var train = try Buffer.init(kernels.core.runtime, training_rows * d * 4);
        defer train.deinit();
        var held = try Buffer.init(kernels.core.runtime, validation_rows * d * 4);
        defer held.deinit();
        @memset(halves(keys), 0);
        @memset(halves(values), 1);
        @memset(train.floats(), 1);
        @memset(held.floats(), -1);
        var candidate = try kernels.fit(.{ .source_rows = 512, .head_dim = @intCast(d), .kv_heads = 1, .kv_head = 0, .training_rows = training_rows, .validation_rows = validation_rows, .scale = 1 / @sqrt(@as(f32, @floatFromInt(d))), .staging_budget_bytes = 8 * 1024 * 1024 }, keys.ref(), values.ref(), train.ref(), held.ref());
        defer candidate.deinit();
        for (halves(candidate.keys)) |key| try std.testing.expectEqual(@as(f16, 0), key);
        for (halves(candidate.values)) |value| try std.testing.expectApproxEqAbs(@as(f32, 1), @as(f32, value), 0.02);
        for (candidate.status_mass.floats()[8..]) |mass| try std.testing.expectApproxEqAbs(@log(@as(f32, 2)), mass, 1e-5);
        // Compose the actual fitted device buffers with a new exact token.
        // No candidate KV download is needed to feed segmented attention.
        const attention = @import("context_kernels.zig");
        const views = @import("../runtime/kv/context_view.zig");
        var attention_kernels = try attention.Kernels.init();
        defer attention_kernels.deinit();
        var appended_k = try Buffer.init(attention_kernels.core.runtime, 257 * d * 2);
        defer appended_k.deinit();
        var appended_v = try Buffer.init(attention_kernels.core.runtime, 257 * d * 2);
        defer appended_v.deinit();
        @memset(halves(appended_k)[256 * d ..], 0);
        @memset(halves(appended_v)[256 * d ..], -512);
        var packed_mass = try Buffer.init(attention_kernels.core.runtime, 256 * 4);
        defer packed_mass.deinit();
        try kernels.packHead(&candidate, 0, 1, appended_k.ref(), appended_v.ref(), packed_mass.ref());
        var output = try Buffer.init(attention_kernels.core.runtime, d * 4);
        defer output.deinit();
        const spans = [_]views.Span{
            .{ .physical_start = 0, .row_count = 256, .logical_start = 0, .logical_count = 512, .kind = .compacted },
            .{ .physical_start = 256, .row_count = 1, .logical_start = 512, .logical_count = 1 },
        };
        try attention_kernels.attention(.{
            .view = .{ .epoch = 1, .logical_tokens = 513, .capacity_rows = 257, .bias_rows = 256, .kv_heads = 1, .head_dim = @intCast(d), .spans = &spans },
            .current_epoch = 1,
            .query_start = 512,
            .query_rows = 1,
            .query_heads = 1,
            .scale = 1,
        }, try held.ref().slice(0, d * 4), appended_k.ref(), appended_v.ref(), packed_mass.ref(), output.ref());
        // Original history contributes +512, the appended exact token -512.
        for (output.floats()) |value| try std.testing.expectApproxEqAbs(@as(f32, 0), value, 1e-4);
    }
}

test "context Metal adaptive retention budgets preserve fitted mass and output" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var kernels = try Kernels.init();
    defer kernels.deinit();
    const dim = 32;
    var keys = try Buffer.init(kernels.core.runtime, 512 * dim * 2);
    defer keys.deinit();
    var values = try Buffer.init(kernels.core.runtime, 512 * dim * 2);
    defer values.deinit();
    var train = try Buffer.init(kernels.core.runtime, 8 * dim * 4);
    defer train.deinit();
    var held = try Buffer.init(kernels.core.runtime, 4 * dim * 4);
    defer held.deinit();
    @memset(halves(keys), 0);
    @memset(halves(values), 1);
    @memset(train.floats(), 1);
    @memset(held.floats(), -1);
    for ([_]usize{ 1, 128, 384, 448, 511 }) |count| {
        for (0..2) |selection| {
            const request = Request{ .source_rows = 512, .head_dim = dim, .kv_heads = 1, .kv_head = 0, .training_rows = 8, .validation_rows = 4, .validation_group_rows = 2, .scale = 1, .staging_budget_bytes = 8 * 1024 * 1024, .options = .{ .retained_rows = count, .key_selection = if (selection == 0) .rms else .matching_pursuit } };
            var candidate = try kernels.fit(request, keys.ref(), values.ref(), train.ref(), held.ref());
            defer candidate.deinit();
            try std.testing.expectEqual(count, candidate.rows);
            try std.testing.expectEqual((try allocation(request)).physical_bytes, candidate.peak_staging_bytes);
            var mass: f64 = 0;
            for (candidate.status_mass.floats()[8..]) |bias| mass += @exp(@as(f64, bias));
            try std.testing.expectApproxEqAbs(@as(f64, 512), mass, 0.005);
            for (halves(candidate.values)) |value| try std.testing.expectApproxEqAbs(@as(f32, 1), @as(f32, value), 0.02);
            try std.testing.expectEqual(@as(u32, 2), candidate.report.validation_groups);
            try std.testing.expect(candidate.report.mass_relative_max_error <= 0.05);
            try std.testing.expect(candidate.report.max_consumer_output_relative_rmse <= 0.1);
        }
    }
}
