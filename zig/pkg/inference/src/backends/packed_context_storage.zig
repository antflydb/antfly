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

//! Right-sized per-head generations for post-ingestion compaction. This is an
//! internal building block, not a serving capability. The request owner must
//! serialize mutation/publication and keep borrowed sources alive until each
//! synchronous copy returns. Readers use Publication leases; a transaction
//! cannot retire their buffers. No buffer handle owns borrowed host pages.
const std = @import("std");
const native_fitter = @import("context_attention_match.zig");
const builtin = @import("builtin");
const shared = @import("metal_generated_kernels.zig");
const mr = @import("metal_runtime.zig");
const views = @import("../runtime/kv/context_view.zig");
const fitter = @import("context_compaction.zig");
const Buffer = shared.Buffer;
const Ref = shared.Ref;

pub const Segment = struct {
    logical_count: u32,
    /// One count per KV head. Equal to logical_count means exact retention.
    retained: []const u32,
};
pub const Shape = struct {
    head_dim: u32,
    kv_heads: u32,
    continuation_rows: u32,
    max_retained_rows: u32,
};
pub const Allocation = struct {
    kv_bytes: usize,
    bias_bytes: usize,
    device_bytes: usize,
    metadata_bytes: usize,
    physical_bytes: usize,
};
fn rounded(bytes: usize) !usize {
    return (try std.math.add(usize, bytes, std.heap.pageSize() - 1)) & ~(std.heap.pageSize() - 1);
}

/// First pass: bounded counts only, no host/GPU allocation. The final segment
/// must be exact so reserved continuation extends it without new descriptors.
/// Payload eligibility belongs to the caller's provenance policy, not this ABI.
pub fn allocation(shape: Shape, segments: []const Segment) !Allocation {
    if (shape.head_dim == 0 or shape.head_dim > 512 or shape.head_dim % 2 != 0 or
        shape.kv_heads == 0 or shape.kv_heads > views.max_heads or segments.len == 0 or segments.len > views.max_spans)
        return error.InvalidPackedContextPlan;
    var logical: u64 = 0;
    for (segments) |segment| {
        if (segment.logical_count == 0 or segment.retained.len != shape.kv_heads) return error.InvalidPackedContextPlan;
        logical += segment.logical_count;
    }
    if (logical + shape.continuation_rows > std.math.maxInt(u32)) return error.InvalidPackedContextPlan;
    var rows: u64 = 0;
    var biases: u64 = 0;
    for (0..shape.kv_heads) |h| {
        var head_rows: u64 = shape.continuation_rows;
        for (segments, 0..) |segment, index| {
            const count = segment.retained[h];
            if (count == 0 or count > segment.logical_count or (count != segment.logical_count and
                (segment.logical_count != 512 or index == segments.len - 1))) return error.InvalidPackedContextPlan;
            head_rows += count;
            if (count != segment.logical_count) biases += count;
        }
        if (head_rows > shape.max_retained_rows) return error.ContextRetainedBudgetExceeded;
        rows += head_rows;
    }
    // Every device element and address is a uint in MSL.
    if (rows * shape.head_dim > std.math.maxInt(u32) or biases > std.math.maxInt(u32)) return error.InvalidPackedContextPlan;
    const kv: usize = @intCast(rows * shape.head_dim * 2);
    const bias: usize = @intCast(biases * 4);
    const device = try std.math.add(usize, try std.math.mul(usize, try rounded(kv), 2), try rounded(bias));
    const count = segments.len * shape.kv_heads;
    // Conservatively page-charge host descriptors and per-span completion bits.
    const metadata = try std.math.add(usize, try rounded(shape.kv_heads * @sizeOf(views.Head)), try std.math.add(usize, try rounded(count * @sizeOf(views.Span)), try rounded(count * @sizeOf(bool))));
    return .{ .kv_bytes = kv, .bias_bytes = bias, .device_bytes = device, .metadata_bytes = metadata, .physical_bytes = try std.math.add(usize, device, metadata) };
}

pub const Plan = struct {
    allocator: std.mem.Allocator,
    descriptor: views.View,
    heads: []views.Head,
    spans: []views.Span,
    sizes: Allocation,

    pub fn init(allocator: std.mem.Allocator, shape: Shape, segments: []const Segment) !Plan {
        const sizes = try allocation(shape, segments);
        const heads = try allocator.alloc(views.Head, shape.kv_heads);
        errdefer allocator.free(heads);
        const spans = try allocator.alloc(views.Span, segments.len * shape.kv_heads);
        errdefer allocator.free(spans);
        var kv_start: u32 = 0;
        var bias_start: u32 = 0;
        var logical: u32 = 0;
        for (heads, 0..) |*head, h| {
            var physical: u32 = 0;
            var bias: u32 = 0;
            logical = 0;
            const span_start = h * segments.len;
            for (segments, 0..) |segment, i| {
                const count = segment.retained[h];
                const compact = count != segment.logical_count;
                spans[span_start + i] = .{ .physical_start = physical, .row_count = count, .logical_start = logical, .logical_count = segment.logical_count, .kind = if (compact) .compacted else .exact, .bias_start = if (compact) bias else 0 };
                physical += count;
                logical += segment.logical_count;
                if (compact) bias += count;
            }
            head.* = .{ .kv_start = kv_start, .capacity_rows = physical + shape.continuation_rows, .bias_start = bias_start, .bias_rows = bias, .span_start = @intCast(span_start), .span_count = @intCast(segments.len) };
            kv_start += head.capacity_rows;
            bias_start += bias;
        }
        const descriptor = views.View{ .version = views.abi_version, .epoch = 1, .logical_tokens = logical, .capacity_rows = kv_start, .bias_rows = bias_start, .kv_heads = shape.kv_heads, .head_dim = shape.head_dim, .heads = heads, .spans = spans };
        _ = try descriptor.validate();
        return .{ .allocator = allocator, .descriptor = descriptor, .heads = heads, .spans = spans, .sizes = sizes };
    }
    pub fn deinit(self: *Plan) void {
        self.allocator.free(self.heads);
        self.allocator.free(self.spans);
        self.* = undefined;
    }
};

/// The producer must complete its GPU frame before this independent queue reads it.
pub const Source = struct {
    view: views.View,
    keys: Ref,
    values: Ref,

    pub fn validate(self: Source) !void {
        _ = try self.view.validate();
        if (self.view.kvElements() > std.math.maxInt(u32)) return error.InvalidContextView;
        const bytes = self.view.kvElements() * 2;
        if (self.keys.bytes < bytes or self.values.bytes < bytes or self.keys.offset % 4 != 0 or self.values.offset % 4 != 0)
            return error.MetalBufferRange;
    }
};

pub const Copier = struct {
    core: shared.Kernels,
    pub fn init() !Copier {
        return initTracked(null);
    }
    pub fn initTracked(tracker: ?*shared.BufferAllocationTracker) !Copier {
        return .{ .core = try shared.Kernels.initSourceTracked(@embedFile("context_storage.metal"), "context_storage", tracker) };
    }
    pub fn initBorrowedTracked(execution: *const shared.Execution, tracker: ?*shared.BufferAllocationTracker) !Copier {
        return .{ .core = try shared.Kernels.initBorrowedTracked(execution, tracker) };
    }
    pub fn deinit(self: *Copier) void {
        self.core.deinit();
    }
    /// Append projected f32 KV into a token-major sliding ring. The caller
    /// reserves window + chunk - 1 rows and publishes descriptors only after
    /// this synchronous copy completes.
    pub fn appendRing(self: *Copier, start: u32, rows: u32, capacity: u32, width: u32, keys: Ref, values: Ref, dest_keys: Ref, dest_values: Ref) !void {
        return self.appendRingImpl(null, start, rows, capacity, width, keys, values, dest_keys, dest_values);
    }
    pub fn appendRingOnActiveRuntime(self: *Copier, owner_runtime: *mr.RawMetalDecodeRuntime, start: u32, rows: u32, capacity: u32, width: u32, keys: Ref, values: Ref, dest_keys: Ref, dest_values: Ref) !void {
        return self.appendRingImpl(owner_runtime, start, rows, capacity, width, keys, values, dest_keys, dest_values);
    }
    fn appendRingImpl(self: *Copier, owner_runtime: ?*mr.RawMetalDecodeRuntime, start: u32, rows: u32, capacity: u32, width: u32, keys: Ref, values: Ref, dest_keys: Ref, dest_values: Ref) !void {
        const count = @as(u64, rows) * width;
        const dest_count = @as(u64, capacity) * width;
        if (rows == 0 or rows > capacity or width == 0 or count > std.math.maxInt(u32) or dest_count > std.math.maxInt(u32) or
            @as(u64, start) + rows > std.math.maxInt(u32) or keys.bytes < count * 4 or values.bytes < count * 4 or
            dest_keys.bytes < dest_count * 2 or dest_values.bytes < dest_count * 2) return error.MetalBufferRange;
        if (dest_keys.handle == dest_values.handle) return error.AliasedContextStaging;
        for ([_]Ref{ keys, values }) |r| if (r.handle == dest_keys.handle or r.handle == dest_values.handle) return error.AliasedContextStaging;
        const params = shared.Params{ .op = 1, .n = @intCast(count), .dim = width, .position = start, .stride = capacity };
        if (owner_runtime) |runtime|
            try self.encodeOnActiveRuntime(runtime, params, .{ keys, values, dest_keys, dest_values })
        else
            try self.encode(params, .{ keys, values, dest_keys, dest_values });
    }
    /// Gather selected post-RoPE query vectors into a branch-owned pool. The
    /// host map uses maxInt(u32) for slots that this dispatch leaves unchanged.
    pub fn captureQueries(self: *Copier, queries: Ref, mapping: Buffer, output: Ref, vectors: u32, dim: u32) !void {
        return self.captureQueriesImpl(null, queries, mapping.ref(), mapping.bytes[0..mapping.logical_bytes], output, vectors, dim);
    }
    pub fn captureQueriesRef(self: *Copier, queries: Ref, mapping: Ref, mapping_bytes: []const u8, output: Ref, vectors: u32, dim: u32) !void {
        return self.captureQueriesImpl(null, queries, mapping, mapping_bytes, output, vectors, dim);
    }
    /// Encode query capture into a decoder frame owned by the model runtime.
    /// `mapping_bytes` must remain immutable until that frame completes.
    pub fn captureQueriesOnActiveRuntime(self: *Copier, owner_runtime: *mr.RawMetalDecodeRuntime, queries: Ref, mapping: Ref, mapping_bytes: []const u8, output: Ref, vectors: u32, dim: u32) !void {
        return self.captureQueriesImpl(owner_runtime, queries, mapping, mapping_bytes, output, vectors, dim);
    }
    fn captureQueriesImpl(self: *Copier, owner_runtime: ?*mr.RawMetalDecodeRuntime, queries: Ref, mapping: Ref, mapping_bytes: []const u8, output: Ref, vectors: u32, dim: u32) !void {
        if (dim == 0 or dim > 512 or mapping.bytes == 0 or mapping.bytes % 4 != 0 or
            queries.bytes < @as(u64, vectors) * dim * 4 or output.bytes < @as(u64, mapping.bytes) * dim or
            mapping.bytes / 4 * dim > std.math.maxInt(u32)) return error.MetalBufferRange;
        if (output.handle == queries.handle or output.handle == mapping.handle) return error.AliasedContextStaging;
        if (mapping_bytes.len != mapping.bytes) return error.MetalBufferRange;
        const map = std.mem.bytesAsSlice(u32, @as([]align(4) const u8, @alignCast(mapping_bytes)));
        for (map) |index| if (index != std.math.maxInt(u32) and index >= vectors) return error.MetalBufferRange;
        const params = shared.Params{ .op = 2, .n = @intCast(map.len * dim), .dim = dim };
        if (owner_runtime) |runtime|
            try self.encodeOnActiveRuntime(runtime, params, .{ queries, mapping, output, self.core.dummy.ref() })
        else
            try self.encode(params, .{ queries, mapping, output, self.core.dummy.ref() });
    }
    fn encode(self: *Copier, params: shared.Params, inputs: [4]Ref) !void {
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        if (self.core.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        var handles: [8]?*anyopaque = @splat(self.core.dummy.handle);
        var offsets: [8]usize = @splat(0);
        var lengths: [8]usize = @splat(4);
        for (inputs, 0..) |ref, i| {
            if (ref.offset % 4 != 0) return error.MetalBufferRange;
            handles[i] = ref.handle;
            offsets[i] = ref.offset;
            lengths[i] = ref.bytes;
        }
        if (mr.termite_metal_generated_dispatch(self.core.runtime, self.core.pipeline, &handles, &offsets, &lengths, 8, &params, @sizeOf(shared.Params), params.n) != 0)
            return error.ContextMetalDispatchFailed;
        if (self.core.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
    }
    fn encodeOnActiveRuntime(self: *Copier, owner_runtime: *mr.RawMetalDecodeRuntime, params: shared.Params, inputs: [4]Ref) !void {
        if (self.core.batch_active or self.core.batch_submitted) return error.ContextTransactionBusy;
        try self.core.runOnActiveRuntime(owner_runtime, params, &inputs, self.core.dummy.ref());
    }
};

test "context Metal query capture uses logical non-page mapping bytes" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    var copier = try Copier.init();
    defer copier.deinit();
    var queries = try copier.core.buffer(3 * 4 * @sizeOf(f32));
    defer queries.deinit();
    var mapping = try copier.core.buffer(3 * @sizeOf(u32));
    defer mapping.deinit();
    var output = try copier.core.buffer(3 * 4 * @sizeOf(f32));
    defer output.deinit();
    for (queries.floats(), 0..) |*value, index| value.* = @floatFromInt(index + 1);
    const map = std.mem.bytesAsSlice(u32, @as([]align(4) u8, @alignCast(mapping.bytes[0..mapping.logical_bytes])));
    map[0] = 2;
    map[1] = std.math.maxInt(u32);
    map[2] = 0;
    @memset(output.floats(), -1);
    try copier.captureQueries(queries.ref(), mapping, output.ref(), 3, 4);
    try std.testing.expectEqualSlices(f32, queries.floats()[8..12], output.floats()[0..4]);
    try std.testing.expectEqualSlices(f32, &.{ -1, -1, -1, -1 }, output.floats()[4..8]);
    try std.testing.expectEqualSlices(f32, queries.floats()[0..4], output.floats()[8..12]);
}

/// A replay retains the host pages AND Metal objects. Retaining only an
/// MTLBuffer alias would leave its page_allocator backing dangling after owner
/// retirement. Acquire under the sequence lock; release only after GPU drain.
const SharedGeneration = struct {
    refs: std.atomic.Value(usize) = .init(1),
    generation: Generation,
    fn release(self: *SharedGeneration) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) {
            const allocator = self.generation.plan.allocator;
            self.generation.deinit();
            allocator.destroy(self);
        }
    }
};

/// Conservative metadata reservation for the first immutable replay lease.
/// The control stays charged to the original generation until it retires.
pub fn replayControlBytes() !usize {
    return rounded(@sizeOf(SharedGeneration));
}

pub const ReplayLease = struct {
    owner: ?*SharedGeneration,
    source: Source,
    pub fn deinit(self: *ReplayLease) void {
        if (self.owner) |owner| owner.release();
        self.owner = null;
    }
};

pub const Generation = struct {
    shared: ?*SharedGeneration = null,
    plan: Plan,
    keys: Buffer,
    values: Buffer,
    mass: ?Buffer,
    filled: []bool,
    ready: bool = false,
    /// Only initEmpty may create an unpublished zero-token original. No
    /// invalid empty descriptor is ever exposed to attention or replay.
    ingesting_empty: bool = false,
    workspace_limit: usize,

    pub fn emptyAllocation(head_dim: u32, kv_heads: u32, capacity: u32) !Allocation {
        if (capacity == 0) return error.InvalidPackedContextPlan;
        const counts: [views.max_heads]u32 = @splat(1);
        if (kv_heads > counts.len) return error.InvalidPackedContextPlan;
        return allocation(.{ .head_dim = head_dim, .kv_heads = kv_heads, .continuation_rows = capacity - 1, .max_retained_rows = capacity }, &.{.{ .logical_count = 1, .retained = counts[0..kv_heads] }});
    }
    pub fn initEmpty(allocator: std.mem.Allocator, copier: *Copier, head_dim: u32, kv_heads: u32, capacity: u32, epoch: u64, budget: usize) !Generation {
        if ((try emptyAllocation(head_dim, kv_heads, capacity)).physical_bytes > budget) return error.ContextStagingBudgetExceeded;
        if (epoch == 0) return error.StaleContextView;
        const counts: [views.max_heads]u32 = @splat(1);
        var plan = try Plan.init(allocator, .{ .head_dim = head_dim, .kv_heads = kv_heads, .continuation_rows = capacity - 1, .max_retained_rows = capacity }, &.{.{ .logical_count = 1, .retained = counts[0..kv_heads] }});
        errdefer plan.deinit();
        var self = try Generation.init(copier, plan, budget, 0);
        self.plan.descriptor.epoch = epoch;
        self.plan.descriptor.logical_tokens = 0;
        for (self.plan.spans) |*span| {
            span.row_count = 0;
            span.logical_count = 0;
        }
        self.ingesting_empty = true;
        return self;
    }

    /// Takes plan ownership only on success. All budgets must be admitted by
    /// the request before this allocation; Batch additionally charges coexistence.
    pub fn init(copier: *Copier, plan: Plan, budget: usize, workspace_limit: usize) !Generation {
        if (plan.sizes.physical_bytes > budget) return error.ContextStagingBudgetExceeded;
        var keys = try copier.core.buffer(plan.sizes.kv_bytes);
        errdefer keys.deinit();
        var values = try copier.core.buffer(plan.sizes.kv_bytes);
        errdefer values.deinit();
        var mass: ?Buffer = if (plan.sizes.bias_bytes > 0) try copier.core.buffer(plan.sizes.bias_bytes) else null;
        errdefer if (mass) |*b| b.deinit();
        const filled = try plan.allocator.alloc(bool, plan.spans.len);
        @memset(filled, false);
        return .{ .plan = plan, .keys = keys, .values = values, .mass = mass, .filled = filled, .workspace_limit = workspace_limit };
    }
    pub fn deinit(self: *Generation) void {
        if (self.shared) |owner| {
            owner.release();
            self.* = undefined;
            return;
        }
        self.keys.deinit();
        self.values.deinit();
        if (self.mass) |*b| b.deinit();
        self.plan.allocator.free(self.filled);
        self.plan.deinit();
        self.* = undefined;
    }
    pub fn hasReplayReaders(self: *const Generation) bool {
        return if (self.shared) |owner| owner.refs.load(.acquire) > 1 else false;
    }
    pub fn ownedBytes(self: *const Generation) !usize {
        return std.math.add(usize, self.plan.sizes.physical_bytes, if (self.shared != null) try rounded(@sizeOf(SharedGeneration)) else 0);
    }
    /// Original global KV must still be exact. The supplied budget is the
    /// additional control allocation; KV itself is shared without copies.
    /// The allocator must outlive every lease, including request cancellation.
    pub fn acquireReplay(self: *Generation, metadata_budget: usize) !ReplayLease {
        const src = try self.source();
        if (!src.view.coversExact(0, src.view.logical_tokens)) return error.InvalidReferenceReplay;
        if (self.shared == null) {
            if (try rounded(@sizeOf(SharedGeneration)) > metadata_budget) return error.ContextReplayBudgetExceeded;
            const owner = try self.plan.allocator.create(SharedGeneration);
            owner.* = .{ .generation = self.* };
            self.shared = owner;
        }
        const owner = self.shared.?;
        // Acquires and owner mutation are serialized by the sequence lock;
        // releases may happen on a different GPU-completion thread.
        if (owner.refs.load(.acquire) == std.math.maxInt(usize)) return error.ContextReadersOverflow;
        _ = owner.refs.fetchAdd(1, .acq_rel);
        return .{ .owner = owner, .source = src };
    }

    pub fn source(self: *const Generation) !Source {
        if (!self.ready) return error.IncompleteContextGeneration;
        return .{ .view = self.plan.descriptor, .keys = self.keys.ref(), .values = self.values.ref() };
    }
    fn spanIndex(self: *const Generation, head: usize, span: usize) !usize {
        if (self.ready or self.ingesting_empty or head >= self.plan.heads.len or span >= self.plan.heads[head].span_count) return error.InvalidContextTransaction;
        return self.plan.heads[head].span_start + span;
    }
    pub fn copyExact(self: *Generation, copier: *Copier, head: usize, span: usize, src: Source) !void {
        const index = try self.spanIndex(head, span);
        const target = self.plan.spans[index];
        const descriptor = self.plan.descriptor;
        if (target.kind != .exact or self.filled[index]) return error.InvalidContextTransaction;
        try src.validate();
        if (src.view.kv_heads != descriptor.kv_heads or src.view.head_dim != descriptor.head_dim) return error.InvalidContextView;
        for ([_]Ref{ src.keys, src.values }) |input| if (input.handle == self.keys.handle or input.handle == self.values.handle)
            return error.AliasedContextStaging;
        const end = target.logical_start + target.logical_count;
        var covered = target.logical_start;
        // Validate complete source coverage before copying any rows.
        for (src.view.headSpans(head)) |s| {
            const until = s.logical_start + s.logical_count;
            if (until <= covered) continue;
            if (s.logical_start > covered or s.kind != .exact) return error.MissingContextHistory;
            covered = @min(end, until);
            if (covered == end) break;
        }
        if (covered != end) return error.MissingContextHistory;
        const is_packed = src.view.version == views.abi_version;
        for (src.view.headSpans(head)) |s| {
            const start = @max(target.logical_start, s.logical_start);
            const until = @min(end, s.logical_start + s.logical_count);
            if (start >= until) continue;
            const physical = s.physical_start + (start - s.logical_start) + (if (is_packed) src.view.heads[head].kv_start else 0);
            try copier.encode(.{ .op = 3, .n = (until - start) * descriptor.head_dim, .dim = descriptor.head_dim, .groups = if (is_packed) 1 else descriptor.kv_heads, .index = if (is_packed) 0 else @intCast(head), .position = physical, .stride = self.plan.heads[head].kv_start + target.physical_start + (start - target.logical_start) }, .{ src.keys, src.values, self.keys.ref(), self.values.ref() });
        }
        self.filled[index] = true;
    }
    /// Second pass: refit one frozen retention decision within the admitted
    /// peak, pack it, and release candidate/workspace before the next head.
    pub fn fitAndPack(self: *Generation, kernels: *fitter.Kernels, head: usize, span: usize, request: fitter.Request, keys: Ref, values: Ref, training: Ref, validation: Ref) !void {
        const index = try self.spanIndex(head, span);
        const target = self.plan.spans[index];
        if (self.filled[index] or target.kind != .compacted or request.source_rows != target.logical_count or
            request.head_dim != self.plan.descriptor.head_dim or
            (request.options.retained_rows orelse request.source_rows / 2) != target.row_count) return error.InvalidContextTransaction;
        var bounded = request;
        bounded.staging_budget_bytes = @min(request.staging_budget_bytes, self.workspace_limit);
        var candidate = try kernels.fit(bounded, keys, values, training, validation);
        defer candidate.deinit();
        try self.packCandidate(kernels, head, span, &candidate);
    }
    /// Native v2 staging does not certify an individual block. The maintenance
    /// transaction must validate every rounded assembled head before commit.
    pub fn fitNativeAndPack(self: *Generation, kernels: *native_fitter.Kernels, allocator: std.mem.Allocator, head: usize, span: usize, request: native_fitter.FitRequest, keys: Ref, values: Ref, training: Ref) !void {
        const index = try self.spanIndex(head, span);
        const target = self.plan.spans[index];
        if (self.filled[index] or target.kind != .compacted or request.source_rows != target.logical_count or
            request.head_dim != self.plan.descriptor.head_dim or request.retained_rows != target.row_count)
            return error.InvalidContextTransaction;
        var bounded = request;
        bounded.staging_budget_bytes = @min(request.staging_budget_bytes, self.workspace_limit);
        var fitted = try kernels.fit(allocator, bounded, keys, values, training);
        defer fitted.deinit();
        const h = self.plan.heads[head];
        const dim = self.plan.descriptor.head_dim;
        const offset = @as(usize, h.kv_start + target.physical_start) * dim * 2;
        const bytes = @as(usize, target.row_count) * dim * 2;
        const mass = self.mass orelse return error.InvalidContextTransaction;
        try kernels.stage(&fitted, try self.keys.ref().slice(offset, bytes), try self.values.ref().slice(offset, bytes), try mass.ref().slice(@as(usize, h.bias_start + target.bias_start) * 4, @as(usize, target.row_count) * 4));
        self.filled[index] = true;
    }

    /// Test/qualification-only path for an externally owned stored payload.
    /// The combined kernel call performs real training+held validation before
    /// copying into this unpublished generation. No Candidate or Report is
    /// manufactured, and the span becomes filled only after copy completion.
    pub fn validateAndCopyStoredDiagnostic(
        self: *Generation,
        kernels: *fitter.Kernels,
        head: usize,
        span: usize,
        request: fitter.Request,
        keys: Ref,
        values: Ref,
        training: Ref,
        validation: Ref,
        stored: fitter.StoredCandidate,
    ) !fitter.StoredValidation {
        if (!builtin.is_test) return error.DiagnosticOnly;
        const index = try self.spanIndex(head, span);
        const target = self.plan.spans[index];
        const descriptor = self.plan.descriptor;
        if (self.filled[index] or target.kind != .compacted or
            request.source_rows != target.logical_count or
            request.head_dim != descriptor.head_dim or
            // The fitter operates on the one-head slices produced by
            // Maintenance.fitInput. `head` is the independent destination
            // coordinate in this multihead generation.
            request.kv_heads != 1 or request.kv_head != 0 or
            (request.options.retained_rows orelse request.source_rows / 2) != target.row_count or
            stored.rows != target.row_count) return error.InvalidContextTransaction;
        var bounded = request;
        bounded.staging_budget_bytes = @min(request.staging_budget_bytes, self.workspace_limit);
        const h = self.plan.heads[head];
        const dim = descriptor.head_dim;
        const offset = @as(usize, h.kv_start + target.physical_start) * dim * 2;
        const bytes = @as(usize, target.row_count) * dim * 2;
        const mass = self.mass orelse return error.InvalidContextTransaction;
        const result = try kernels.validateAndCopyStoredDiagnostic(
            bounded,
            keys,
            values,
            training,
            validation,
            stored,
            try self.keys.ref().slice(offset, bytes),
            try self.values.ref().slice(offset, bytes),
            try mass.ref().slice(@as(usize, h.bias_start + target.bias_start) * 4, @as(usize, target.row_count) * 4),
        );
        if (result.peak_staging_bytes > self.workspace_limit) return error.ContextStagingBudgetExceeded;
        self.filled[index] = true;
        return result;
    }
    pub fn packCandidate(self: *Generation, kernels: *fitter.Kernels, head: usize, span: usize, candidate: *const fitter.Candidate) !void {
        const index = try self.spanIndex(head, span);
        const target = self.plan.spans[index];
        const h = self.plan.heads[head];
        const dim = self.plan.descriptor.head_dim;
        if (target.kind != .compacted or self.filled[index] or candidate.rows != target.row_count or candidate.head_dim != dim)
            return error.InvalidContextTransaction;
        if (candidate.peak_staging_bytes > self.workspace_limit) return error.ContextStagingBudgetExceeded;
        // Candidates come only from a successful fitter; never reinterpret an
        // exact fallback or rejected report as a lossy block.
        if (candidate.report.failure_flags != 0 or candidate.report.evaluated != 7) return error.InvalidCompactionReport;
        const offset = @as(usize, h.kv_start + target.physical_start) * dim * 2;
        const bytes = @as(usize, target.row_count) * dim * 2;
        try kernels.packHead(candidate, 0, 1, try self.keys.ref().slice(offset, bytes), try self.values.ref().slice(offset, bytes), try self.mass.?.ref().slice(@as(usize, h.bias_start + target.bias_start) * 4, @as(usize, target.row_count) * 4));
        self.filled[index] = true;
    }
    pub fn finish(self: *Generation) !void {
        for (self.filled) |filled| if (!filled) return error.IncompleteContextGeneration;
        _ = try self.plan.descriptor.validate();
        self.ready = true;
    }
    /// Writes only unpublished tail capacity. A failed/cancelled append leaves
    /// descriptors unchanged and can be retried. Call with no outstanding readers.
    pub fn append(self: *Generation, copier: *Copier, publication: *const views.Publication, rows: u32, keys: Ref, values: Ref) !void {
        return self.appendImpl(copier, publication, null, rows, keys, values);
    }
    pub fn appendOnActiveRuntime(self: *Generation, copier: *Copier, publication: *const views.Publication, owner_runtime: *mr.RawMetalDecodeRuntime, rows: u32, keys: Ref, values: Ref) !void {
        return self.appendImpl(copier, publication, owner_runtime, rows, keys, values);
    }
    fn appendImpl(self: *Generation, copier: *Copier, publication: *const views.Publication, owner_runtime: ?*mr.RawMetalDecodeRuntime, rows: u32, keys: Ref, values: Ref) !void {
        if (self.hasReplayReaders()) return error.ContextReplayActive;
        if (publication.pending) return error.ContextTransactionBusy;
        if (publication.readers != 0) return error.ContextReadersActive;
        if (self.plan.descriptor.epoch != publication.epoch) return error.StaleContextView;
        if ((!self.ready and !self.ingesting_empty) or rows == 0) return error.InvalidContextTransaction;
        const v = self.plan.descriptor;
        const end = std.math.add(u32, v.logical_tokens, rows) catch return error.ContextRetainedBudgetExceeded;
        const elements = @as(u64, rows) * v.kv_heads * v.head_dim;
        if (elements > std.math.maxInt(u32) or keys.bytes < elements * 4 or values.bytes < elements * 4)
            return error.MetalBufferRange;
        for ([_]Ref{ keys, values }) |ref| {
            if (ref.offset % 4 != 0) return error.MetalBufferRange;
            if (ref.handle == self.keys.handle or ref.handle == self.values.handle) return error.AliasedContextStaging;
        }
        for (self.plan.heads) |h| {
            const last = self.plan.spans[h.span_start + h.span_count - 1];
            if (last.kind != .exact or @as(u64, last.physical_start) + last.row_count + rows > h.capacity_rows)
                return error.ContextRetainedBudgetExceeded;
        }
        for (self.plan.heads, 0..) |h, head| {
            const last = self.plan.spans[h.span_start + h.span_count - 1];
            const params = shared.Params{ .op = 4, .n = rows * v.head_dim, .dim = v.head_dim, .groups = v.kv_heads, .index = @intCast(head), .stride = h.kv_start + last.physical_start + last.row_count };
            if (owner_runtime) |runtime|
                try copier.encodeOnActiveRuntime(runtime, params, .{ keys, values, self.keys.ref(), self.values.ref() })
            else
                try copier.encode(params, .{ keys, values, self.keys.ref(), self.values.ref() });
        }
        for (self.plan.heads) |h| {
            const last = &self.plan.spans[h.span_start + h.span_count - 1];
            last.row_count += rows;
            last.logical_count += rows;
        }
        self.plan.descriptor.logical_tokens = end;
        if (self.ingesting_empty) {
            @memset(self.filled, true);
            self.ready = true;
            self.ingesting_empty = false;
        }
    }
};

/// The request's sequence lock serializes calls. A single batch owns all staged
/// generations and either replaces every owner or frees every candidate. Copies
/// and fits finish synchronously before commit/cancel, so no GPU work can retain
/// freed backing pages. Admission includes old+new arenas and one fitter peak.
pub const Batch = struct {
    allocator: std.mem.Allocator,
    publication: *views.Publication,
    live: []Generation,
    staged: []Generation,
    initialized: usize = 0,
    epoch: u64,
    published: bool = false,
    cancelled: ?*const std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, copier: *Copier, publication: *views.Publication, live: []Generation, shapes: []const Shape, segments: []const []const Segment, peak_budget: usize, workspace_bytes: usize, cancelled: ?*const std.atomic.Value(bool)) !Batch {
        if (live.len == 0 or live.len > 256 or live.len != shapes.len or live.len != segments.len) return error.InvalidContextTransaction;
        if (cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        var peak = workspace_bytes;
        var old_bytes: usize = 0;
        var new_bytes: usize = 0;
        var old_device: usize = 0;
        var new_device: usize = 0;
        for (live, shapes, segments) |*old, shape, parts| {
            if (old.hasReplayReaders()) return error.ContextReplayActive;
            if (!old.ready or old.plan.descriptor.epoch != publication.epoch or shape.kv_heads != old.plan.descriptor.kv_heads or
                shape.head_dim != old.plan.descriptor.head_dim) return error.StaleContextView;
            const sizes = try allocation(shape, parts);
            var logical: u64 = 0;
            for (parts) |part| logical += part.logical_count;
            if (logical != old.plan.descriptor.logical_tokens) return error.InvalidContextTransaction;
            for (old.plan.heads) |h| {
                const last = old.plan.spans[h.span_start + h.span_count - 1];
                if (shape.continuation_rows != h.capacity_rows - last.physical_start - last.row_count)
                    return error.InvalidContextTransaction;
            }
            old_device = try std.math.add(usize, old_device, old.plan.sizes.device_bytes);
            new_device = try std.math.add(usize, new_device, sizes.device_bytes);
            old_bytes = try std.math.add(usize, old_bytes, try old.ownedBytes());
            new_bytes = try std.math.add(usize, new_bytes, sizes.physical_bytes);
            peak = try std.math.add(usize, peak, try std.math.add(usize, try old.ownedBytes(), sizes.physical_bytes));
        }
        if (new_bytes >= old_bytes or new_device >= old_device) return error.ContextAllocationNotReduced;
        // Staged generation records themselves coexist during the transaction.
        peak = try std.math.add(usize, peak, try rounded(live.len * @sizeOf(Generation)));
        if (peak > peak_budget) return error.ContextStagingBudgetExceeded;
        const staged = try allocator.alloc(Generation, live.len);
        errdefer allocator.free(staged);
        const epoch = try publication.begin();
        var self = Batch{ .allocator = allocator, .publication = publication, .live = live, .staged = staged, .epoch = epoch, .cancelled = cancelled };
        errdefer {
            for (self.staged[0..self.initialized]) |*g| g.deinit();
            publication.cancel() catch unreachable;
        }
        for (shapes, segments, 0..) |shape, parts, index| {
            var plan = try Plan.init(allocator, shape, parts);
            errdefer plan.deinit();
            plan.descriptor.epoch = epoch;
            staged[index] = try Generation.init(copier, plan, plan.sizes.physical_bytes, workspace_bytes);
            self.initialized += 1;
        }
        return self;
    }
    pub fn commit(self: *Batch) !void {
        if (self.published or !self.publication.pending or self.publication.issued_epoch != self.epoch) return error.InvalidContextTransaction;
        if (self.cancelled) |stop| if (stop.load(.acquire)) return error.Cancelled;
        for (self.staged, self.live) |*g, *old| {
            if (old.hasReplayReaders()) return error.ContextReplayActive;
            if (!g.ready) return error.IncompleteContextGeneration;
            if (g.plan.descriptor.epoch != self.epoch or old.plan.descriptor.epoch != self.publication.epoch or
                g.plan.descriptor.logical_tokens != old.plan.descriptor.logical_tokens) return error.StaleContextView;
            _ = try g.plan.descriptor.validate();
        }
        if (!self.publication.submitted) {
            try self.publication.submit();
            try self.publication.complete(self.epoch, true);
        }
        try self.publication.commit(self.epoch);
        for (self.live, self.staged) |*old, next| {
            old.deinit();
            old.* = next;
        }
        self.published = true;
    }
    pub fn deinit(self: *Batch) void {
        if (!self.published) {
            // All write APIs are synchronous, including their error paths.
            self.publication.cancel() catch unreachable;
            for (self.staged[0..self.initialized]) |*g| g.deinit();
        }
        self.allocator.free(self.staged);
        self.* = undefined;
    }
};

fn planAllocationFailures(allocator: std.mem.Allocator) !void {
    const segments = [_]Segment{
        .{ .logical_count = 512, .retained = &.{ 128, 512 } },
        .{ .logical_count = 2, .retained = &.{ 2, 2 } },
    };
    var plan = try Plan.init(allocator, .{ .head_dim = 256, .kv_heads = 2, .continuation_rows = 3, .max_retained_rows = 517 }, &segments);
    defer plan.deinit();
    try std.testing.expectEqual(@as(u32, 514), try plan.descriptor.validate());
    try std.testing.expectEqual(@as(u32, 650), plan.descriptor.capacity_rows);
    try std.testing.expectEqual(@as(usize, 650 * 256 * 2), plan.sizes.kv_bytes);
}

test "context packed plans enforce per-head caps, exact tails and allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, planAllocationFailures, .{});
    var shape = Shape{ .head_dim = 256, .kv_heads = 2, .continuation_rows = 3, .max_retained_rows = 517 };
    var segments = [_]Segment{
        .{ .logical_count = 512, .retained = &.{ 128, 512 } },
        .{ .logical_count = 2, .retained = &.{ 2, 2 } },
    };
    shape.max_retained_rows = 516;
    try std.testing.expectError(error.ContextRetainedBudgetExceeded, allocation(shape, &segments));
    shape.max_retained_rows = 517;
    segments[1] = .{ .logical_count = 512, .retained = &.{ 128, 128 } };
    try std.testing.expectError(error.InvalidPackedContextPlan, allocation(shape, &segments));
    segments[1] = .{ .logical_count = 2, .retained = &.{ 2, 2 } };
    segments[0].retained = &.{ 0, 512 };
    try std.testing.expectError(error.InvalidPackedContextPlan, allocation(shape, &segments));
    segments[0].retained = &.{ 128, 513 };
    try std.testing.expectError(error.InvalidPackedContextPlan, allocation(shape, &segments));
    segments[0].retained = &.{128};
    try std.testing.expectError(error.InvalidPackedContextPlan, allocation(shape, &segments));
    segments[0].retained = &.{ 128, 512 };
    shape.continuation_rows = std.math.maxInt(u32);
    try std.testing.expectError(error.InvalidPackedContextPlan, allocation(shape, &segments));
}

fn halves(buffer: Buffer) []f16 {
    return std.mem.bytesAsSlice(f16, @as([]align(2) u8, @alignCast(buffer.bytes[0..buffer.logical_bytes])));
}

fn batchAllocationFailures(allocator: std.mem.Allocator, copier: *Copier, publication: *views.Publication, live: []Generation, shapes: []const Shape, segments: []const []const Segment) !void {
    const before = mr.currentAllocatedSize(copier.core.runtime);
    defer std.testing.expectEqual(before, mr.currentAllocatedSize(copier.core.runtime)) catch @panic("packed generation allocation leaked");
    var batch = try Batch.init(allocator, copier, publication, live, shapes, segments, 64 * 1024 * 1024, 8 * 1024 * 1024, null);
    defer batch.deinit();
}

test "context Metal packed all-owner publication releases allocation and preserves exact continuation" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const attention = @import("context_kernels.zig");
    const alloc = std.testing.allocator;
    var copier = try Copier.init();
    defer copier.deinit();
    var fit = try fitter.Kernels.init();
    defer fit.deinit();
    var attn = try attention.Kernels.init();
    defer attn.deinit();
    const dim = 256;
    const workspace = 8 * 1024 * 1024;
    var source_k = try Buffer.init(copier.core.runtime, 513 * 3 * dim * 2);
    defer source_k.deinit();
    var source_v = try Buffer.init(copier.core.runtime, 513 * 3 * dim * 2);
    defer source_v.deinit();
    @memset(halves(source_k), 0);
    for (0..513) |row| for (0..3) |head| {
        // Exact head contains distinct values at every position and dimension.
        for (0..dim) |d| halves(source_v)[(row * 3 + head) * dim + d] =
            if (head == 2) @floatCast(@as(f32, @floatFromInt((row + d) % 31)) / 8) else @floatFromInt(head + 1);
    };
    var q = try Buffer.init(copier.core.runtime, 8 * dim * 4);
    defer q.deinit();
    @memset(q.floats(), 1);
    var held = try Buffer.init(copier.core.runtime, 4 * dim * 4);
    defer held.deinit();
    @memset(held.floats(), -1);
    var appended_k = try Buffer.init(copier.core.runtime, 2 * 3 * dim * 4);
    defer appended_k.deinit();
    @memset(appended_k.floats(), 0);
    var appended_v = try Buffer.init(copier.core.runtime, 2 * 3 * dim * 4);
    defer appended_v.deinit();
    for (appended_v.floats(), 0..) |*v, i| v.* = @as(f32, @floatFromInt(i % 17)) / 4;
    var output = try Buffer.init(copier.core.runtime, 2 * 6 * dim * 4);
    defer output.deinit();
    var queries = try Buffer.init(copier.core.runtime, 2 * 6 * dim * 4);
    defer queries.deinit();
    @memset(queries.floats(), 1);
    const source_spans = [_]views.Span{.{ .physical_start = 0, .row_count = 513, .logical_start = 0, .logical_count = 513 }};
    const src = Source{ .view = .{ .epoch = 1, .logical_tokens = 513, .capacity_rows = 513, .kv_heads = 3, .head_dim = dim, .spans = &source_spans }, .keys = source_k.ref(), .values = source_v.ref() };
    const shape = Shape{ .head_dim = dim, .kv_heads = 3, .continuation_rows = 2, .max_retained_rows = 515 };
    const exact_parts = [_]Segment{.{ .logical_count = 513, .retained = &.{ 513, 513, 513 } }};
    const compact_parts = [_]Segment{
        .{ .logical_count = 512, .retained = &.{ 128, 384, 512 } },
        .{ .logical_count = 1, .retained = &.{ 1, 1, 1 } },
    };
    var live: [2]Generation = undefined;
    var initialized: usize = 0;
    defer for (live[0..initialized]) |*g| g.deinit();
    for (&live) |*g| {
        g.* = blk: {
            var plan = try Plan.init(alloc, shape, &exact_parts);
            errdefer plan.deinit();
            break :blk try Generation.init(&copier, plan, plan.sizes.physical_bytes, workspace);
        };
        initialized += 1;
        for (0..3) |h| try g.copyExact(&copier, h, 0, src);
        try g.finish();
    }
    var publication = views.Publication{};
    var cancelled = std.atomic.Value(bool).init(false);
    const shapes = [_]Shape{ shape, shape };
    const parts = [_][]const Segment{ &compact_parts, &compact_parts };
    const old_handles = [_]*anyopaque{ live[0].keys.handle, live[1].keys.handle };
    const baseline = mr.currentAllocatedSize(copier.core.runtime);
    try std.testing.expectError(error.ContextStagingBudgetExceeded, Batch.init(alloc, &copier, &publication, &live, &shapes, &parts, workspace, workspace, &cancelled));
    try std.testing.expect(!publication.pending);
    try std.testing.expectEqual(baseline, mr.currentAllocatedSize(copier.core.runtime));
    const exact_batches = [_][]const Segment{ &exact_parts, &exact_parts };
    try std.testing.expectError(error.ContextAllocationNotReduced, Batch.init(alloc, &copier, &publication, &live, &shapes, &exact_batches, 64 * 1024 * 1024, workspace, &cancelled));
    // Incomplete owner and cancellation must leave every live pointer/epoch intact.
    {
        var rejected = try Batch.init(alloc, &copier, &publication, &live, &shapes, &parts, 64 * 1024 * 1024, workspace, &cancelled);
        defer rejected.deinit();
        try std.testing.expectError(error.IncompleteContextGeneration, rejected.commit());
        cancelled.store(true, .release);
        try std.testing.expectError(error.Cancelled, rejected.commit());
        cancelled.store(false, .release);
        for (&live, old_handles) |*g, handle| try std.testing.expectEqual(handle, g.keys.handle);
        try std.testing.expectEqual(@as(u64, 1), publication.epoch);
    }
    try std.testing.expectEqual(baseline, mr.currentAllocatedSize(copier.core.runtime));
    try std.testing.checkAllAllocationFailures(alloc, batchAllocationFailures, .{ &copier, &publication, @as([]Generation, &live), @as([]const Shape, &shapes), @as([]const []const Segment, &parts) });
    try std.testing.expect(!publication.pending);
    var batch = try Batch.init(alloc, &copier, &publication, &live, &shapes, &parts, 64 * 1024 * 1024, workspace, &cancelled);
    defer batch.deinit();
    const old_device = live[0].plan.sizes.device_bytes + live[1].plan.sizes.device_bytes;
    const new_device = batch.staged[0].plan.sizes.device_bytes + batch.staged[1].plan.sizes.device_bytes;
    try std.testing.expect(new_device < old_device);
    for (batch.staged, &live) |*g, *original| {
        const old = try original.source();
        for (0..3) |h| {
            if (h == 2) {
                try g.copyExact(&copier, h, 0, old);
            } else {
                const request = fitter.Request{ .source_rows = 512, .head_dim = dim, .kv_heads = 3, .kv_head = @intCast(h), .training_rows = 8, .validation_rows = 4, .validation_group_rows = 2, .scale = 1, .staging_budget_bytes = workspace, .options = .{ .retained_rows = compact_parts[0].retained[h] } };
                const reads = fit.status_readback_bytes;
                g.workspace_limit = 0;
                try std.testing.expectError(error.ContextStagingBudgetExceeded, g.fitAndPack(&fit, h, 0, request, source_k.ref(), source_v.ref(), q.ref(), held.ref()));
                try std.testing.expectEqual(reads, fit.status_readback_bytes);
                g.workspace_limit = workspace;
                // A failed late fit cannot publish earlier owners/heads.
                q.floats()[0] = std.math.nan(f32);
                try std.testing.expectError(error.NonFiniteCompactionInput, g.fitAndPack(&fit, h, 0, request, source_k.ref(), source_v.ref(), q.ref(), held.ref()));
                q.floats()[0] = 1;
                try std.testing.expectEqual(@as(u64, 1), publication.epoch);
                for (&live, old_handles) |*owner, handle| try std.testing.expectEqual(handle, owner.keys.handle);
                try g.fitAndPack(&fit, h, 0, request, source_k.ref(), source_v.ref(), q.ref(), held.ref());
            }
            try g.copyExact(&copier, h, 1, old);
        }
        try g.finish();
    }
    cancelled.store(true, .release);
    try std.testing.expectError(error.Cancelled, batch.commit());
    cancelled.store(false, .release);
    try std.testing.expectError(error.ContextTransactionBusy, live[0].append(&copier, &publication, 1, appended_k.ref(), appended_v.ref()));
    const old_epoch = try publication.acquire();
    try std.testing.expectError(error.ContextReadersActive, batch.commit());
    for (&live, old_handles) |*g, handle| try std.testing.expectEqual(handle, g.keys.handle);
    const before_commit = mr.currentAllocatedSize(copier.core.runtime);
    try publication.release(old_epoch);
    try batch.commit();
    const after_commit = mr.currentAllocatedSize(copier.core.runtime);
    try std.testing.expectEqual(@as(u64, old_device), before_commit - after_commit);
    try std.testing.expectError(error.StaleContextView, publication.release(old_epoch));
    for (&live) |*g| {
        const h = g.plan.heads[2];
        const exact_bytes = halves(g.values)[h.kv_start * dim ..][0 .. 513 * dim];
        for (0..513) |r| for (0..dim) |d| try std.testing.expectEqual(halves(source_v)[(r * 3 + 2) * dim + d], exact_bytes[r * dim + d]);
        const lease = try publication.acquire();
        try std.testing.expectError(error.ContextReadersActive, g.append(&copier, &publication, 1, appended_k.ref(), appended_v.ref()));
        try publication.release(lease);
        // Cancellation may dirty unused tail pages, never published descriptors.
        copier.core.cancelled = &cancelled;
        cancelled.store(true, .release);
        try std.testing.expectError(error.Cancelled, g.append(&copier, &publication, 2, appended_k.ref(), appended_v.ref()));
        try std.testing.expectEqual(@as(u32, 513), g.plan.descriptor.logical_tokens);
        cancelled.store(false, .release);
        try g.append(&copier, &publication, 2, appended_k.ref(), appended_v.ref());
        try std.testing.expectError(error.ContextRetainedBudgetExceeded, g.append(&copier, &publication, 1, appended_k.ref(), appended_v.ref()));
        try attn.attention(.{ .view = g.plan.descriptor, .current_epoch = publication.epoch, .query_start = 513, .query_rows = 2, .query_heads = 6, .scale = 1 }, queries.ref(), g.keys.ref(), g.values.ref(), g.mass.?.ref(), output.ref());
        // Independent full-history arithmetic mean for zero keys, with causal
        // masking and two GQA consumers per KV head. The exact head is varied.
        for (0..2) |token| for (0..6) |qh| for (0..dim) |d| {
            const kh = qh / 2;
            var sum: f64 = 0;
            for (0..513) |r| sum += halves(source_v)[(r * 3 + kh) * dim + d];
            for (0..token + 1) |r| sum += @as(f16, @floatCast(appended_v.floats()[(r * 3 + kh) * dim + d]));
            try std.testing.expectApproxEqAbs(sum / @as(f64, @floatFromInt(514 + token)), @as(f64, output.floats()[(token * 6 + qh) * dim + d]), 2e-5);
        };
    }
    std.debug.print("context_packed_publication: owners=2 old_device_bytes={d} new_device_bytes={d} released_at_commit={d} epoch={d}\n", .{ old_device, new_device, before_commit - after_commit, publication.epoch });
}

test "context Metal shared replay owns immutable pages across original retirement" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var copier = try Copier.init();
    defer copier.deinit();
    var plan = try Plan.init(allocator, .{ .head_dim = 256, .kv_heads = 1, .continuation_rows = 1, .max_retained_rows = 5 }, &.{.{ .logical_count = 4, .retained = &.{4} }});
    var generation = Generation.init(&copier, plan, 1024 * 1024, 0) catch |err| {
        plan.deinit();
        return err;
    };
    var live = true;
    defer if (live) generation.deinit();
    @memset(halves(generation.keys), 0);
    @memset(halves(generation.values), 3);
    @memset(generation.filled, true);
    try generation.finish();
    try std.testing.expectError(error.ContextReplayBudgetExceeded, generation.acquireReplay(0));
    var first = try generation.acquireReplay(1024 * 1024);
    defer first.deinit();
    var second = try generation.acquireReplay(0);
    defer second.deinit();
    try std.testing.expectEqual(generation.keys.handle, first.source.keys.handle);
    try std.testing.expectEqual(generation.values.handle, second.source.values.handle);
    var publication = views.Publication{ .epoch = generation.plan.descriptor.epoch };
    try std.testing.expectError(error.ContextReplayActive, generation.append(&copier, &publication, 1, generation.keys.ref(), generation.values.ref()));
    const values = generation.values;
    generation.deinit();
    live = false;
    first.deinit();
    // Metadata and the actual alias backing remain alive until the last lease.
    try std.testing.expectEqual(@as(u32, 4), second.source.view.logical_tokens);
    _ = try second.source.view.validate();
    try std.testing.expectEqual(@as(f16, 3), halves(values)[0]);
    var destination_plan = try Plan.init(allocator, .{ .head_dim = 256, .kv_heads = 1, .continuation_rows = 0, .max_retained_rows = 4 }, &.{.{ .logical_count = 4, .retained = &.{4} }});
    var destination = Generation.init(&copier, destination_plan, 1024 * 1024, 0) catch |err| {
        destination_plan.deinit();
        return err;
    };
    defer destination.deinit();
    try destination.copyExact(&copier, 0, 0, second.source);
    try destination.finish();
    try std.testing.expectEqual(@as(f16, 3), halves(destination.values)[0]);
}

test "context Metal stored diagnostic fills only after validated copy" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var copier = try Copier.init();
    defer copier.deinit();
    var kernels = try fitter.Kernels.init();
    defer kernels.deinit();
    const dim: u32 = 2;
    const retained: u32 = 2;
    const segments = [_]Segment{
        .{ .logical_count = 512, .retained = &.{retained} },
        .{ .logical_count = 1, .retained = &.{1} },
    };
    var plan = try Plan.init(allocator, .{ .head_dim = dim, .kv_heads = 1, .continuation_rows = 0, .max_retained_rows = 3 }, &segments);
    const request = fitter.Request{ .source_rows = 512, .head_dim = dim, .kv_heads = 1, .kv_head = 0, .training_rows = 1, .validation_rows = 1, .validation_group_rows = 1, .scale = 1, .staging_budget_bytes = std.math.maxInt(usize), .options = .{ .retained_rows = retained } };
    const workspace = (try fitter.validationAllocation(request)).physical_bytes;
    var generation = Generation.init(&copier, plan, plan.sizes.physical_bytes, workspace) catch |err| {
        plan.deinit();
        return err;
    };
    defer generation.deinit();
    var source_keys = try Buffer.init(copier.core.runtime, 513 * dim * 2);
    defer source_keys.deinit();
    var source_values = try Buffer.init(copier.core.runtime, 513 * dim * 2);
    defer source_values.deinit();
    var training = try Buffer.init(copier.core.runtime, dim * 4);
    defer training.deinit();
    var held = try Buffer.init(copier.core.runtime, dim * 4);
    defer held.deinit();
    var stored_keys = try Buffer.init(copier.core.runtime, retained * dim * 2);
    defer stored_keys.deinit();
    var stored_values = try Buffer.init(copier.core.runtime, retained * dim * 2);
    defer stored_values.deinit();
    var stored_bias = try Buffer.init(copier.core.runtime, retained * 4);
    defer stored_bias.deinit();
    @memset(halves(source_keys), 0);
    @memset(halves(source_values), 1);
    @memset(training.floats(), 0);
    @memset(held.floats(), 0);
    @memcpy(halves(stored_keys), &[_]f16{ 3, -2, 5, 7 });
    @memset(halves(stored_values), 1);
    @memset(stored_bias.floats(), @log(@as(f32, 256)));
    const stored = fitter.StoredCandidate{ .rows = retained, .keys = stored_keys.ref(), .values = stored_values.ref(), .bias = stored_bias.ref() };

    @memset(halves(stored_values), 0);
    try std.testing.expectError(error.CompactionQualityRejected, generation.validateAndCopyStoredDiagnostic(&kernels, 0, 0, request, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), stored));
    try std.testing.expect(!generation.filled[0]);
    @memset(halves(stored_values), 1);
    const readbacks = kernels.status_readback_bytes;
    const report_before_invalid_destination = kernels.last_report;
    const aliased = fitter.StoredCandidate{ .rows = retained, .keys = generation.keys.ref(), .values = stored_values.ref(), .bias = stored_bias.ref() };
    try std.testing.expectError(error.AliasedContextStaging, generation.validateAndCopyStoredDiagnostic(&kernels, 0, 0, request, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), aliased));
    try std.testing.expectEqual(readbacks, kernels.status_readback_bytes);
    try std.testing.expectEqualDeep(report_before_invalid_destination, kernels.last_report);
    try std.testing.expect(!generation.filled[0]);
    var stop = std.atomic.Value(bool).init(true);
    kernels.core.cancelled = &stop;
    try std.testing.expectError(error.Cancelled, generation.validateAndCopyStoredDiagnostic(&kernels, 0, 0, request, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), stored));
    try std.testing.expect(!generation.filled[0]);
    stop.store(false, .release);
    const validated = try generation.validateAndCopyStoredDiagnostic(&kernels, 0, 0, request, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), stored);
    try std.testing.expectEqual(@as(u32, 7), validated.report.evaluated);
    try std.testing.expect(generation.filled[0]);
    try std.testing.expectEqualSlices(f16, halves(stored_keys), halves(generation.keys)[0 .. retained * dim]);
    try std.testing.expectEqualSlices(f16, halves(stored_values), halves(generation.values)[0 .. retained * dim]);
    try std.testing.expectEqualSlices(f32, stored_bias.floats(), generation.mass.?.floats()[0..retained]);

    const source_spans = [_]views.Span{.{ .physical_start = 0, .row_count = 513, .logical_start = 0, .logical_count = 513 }};
    try generation.copyExact(&copier, 0, 1, .{ .view = .{ .epoch = 1, .logical_tokens = 513, .capacity_rows = 513, .kv_heads = 1, .head_dim = dim, .spans = &source_spans }, .keys = source_keys.ref(), .values = source_values.ref() });
    try generation.finish();
}

test "context Metal stored diagnostic maps one-head request into selected multihead destination" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const allocator = std.testing.allocator;
    var copier = try Copier.init();
    defer copier.deinit();
    var kernels = try fitter.Kernels.init();
    defer kernels.deinit();
    const dim: u32 = 2;
    const retained: u32 = 2;
    const segments = [_]Segment{
        .{ .logical_count = 512, .retained = &.{ retained, retained } },
        .{ .logical_count = 1, .retained = &.{ 1, 1 } },
    };
    var plan = try Plan.init(allocator, .{ .head_dim = dim, .kv_heads = 2, .continuation_rows = 0, .max_retained_rows = retained + 1 }, &segments);
    var generation = Generation.init(&copier, plan, plan.sizes.physical_bytes, (try fitter.validationAllocation(.{
        .source_rows = 512,
        .head_dim = dim,
        .kv_heads = 1,
        .kv_head = 0,
        .training_rows = 1,
        .validation_rows = 1,
        .validation_group_rows = 1,
        .scale = 1,
        .staging_budget_bytes = std.math.maxInt(usize),
        .options = .{ .retained_rows = retained },
    })).physical_bytes) catch |err| {
        plan.deinit();
        return err;
    };
    defer generation.deinit();
    var source_keys = try Buffer.init(copier.core.runtime, 512 * dim * 2);
    defer source_keys.deinit();
    var source_values = try Buffer.init(copier.core.runtime, 512 * dim * 2);
    defer source_values.deinit();
    var training = try Buffer.init(copier.core.runtime, dim * 4);
    defer training.deinit();
    var held = try Buffer.init(copier.core.runtime, dim * 4);
    defer held.deinit();
    var stored_keys = try Buffer.init(copier.core.runtime, retained * dim * 2);
    defer stored_keys.deinit();
    var stored_values = try Buffer.init(copier.core.runtime, retained * dim * 2);
    defer stored_values.deinit();
    var stored_bias = try Buffer.init(copier.core.runtime, retained * 4);
    defer stored_bias.deinit();
    @memset(halves(source_keys), 0);
    @memset(halves(source_values), 1);
    @memset(training.floats(), 0);
    @memset(held.floats(), 0);
    @memcpy(halves(stored_keys), &[_]f16{ 3, -2, 5, 7 });
    @memset(halves(stored_values), 1);
    @memset(stored_bias.floats(), @log(@as(f32, 256)));
    @memset(halves(generation.keys), 17);
    @memset(halves(generation.values), 17);
    @memset(generation.mass.?.floats(), 17);
    const stored = fitter.StoredCandidate{ .rows = retained, .keys = stored_keys.ref(), .values = stored_values.ref(), .bias = stored_bias.ref() };
    const request = fitter.Request{ .source_rows = 512, .head_dim = dim, .kv_heads = 1, .kv_head = 0, .training_rows = 1, .validation_rows = 1, .validation_group_rows = 1, .scale = 1, .staging_budget_bytes = std.math.maxInt(usize), .options = .{ .retained_rows = retained } };
    const readbacks = kernels.status_readback_bytes;
    var wrong = request;
    wrong.kv_heads = 2;
    try std.testing.expectError(error.InvalidContextTransaction, generation.validateAndCopyStoredDiagnostic(&kernels, 1, 0, wrong, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), stored));
    wrong = request;
    wrong.kv_head = 1;
    try std.testing.expectError(error.InvalidContextTransaction, generation.validateAndCopyStoredDiagnostic(&kernels, 1, 0, wrong, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), stored));
    try std.testing.expectError(error.InvalidContextTransaction, generation.validateAndCopyStoredDiagnostic(&kernels, 2, 0, request, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), stored));
    try std.testing.expectEqual(readbacks, kernels.status_readback_bytes);

    const validated = try generation.validateAndCopyStoredDiagnostic(&kernels, 1, 0, request, source_keys.ref(), source_values.ref(), training.ref(), held.ref(), stored);
    try std.testing.expectEqual(@as(u32, 7), validated.report.evaluated);
    const head0_index = try generation.spanIndex(0, 0);
    const head1_index = try generation.spanIndex(1, 0);
    try std.testing.expect(!generation.filled[head0_index]);
    try std.testing.expect(generation.filled[head1_index]);
    const h0 = generation.plan.heads[0];
    const h1 = generation.plan.heads[1];
    const s0 = generation.plan.spans[head0_index];
    const s1 = generation.plan.spans[head1_index];
    const o0 = @as(usize, h0.kv_start + s0.physical_start) * dim;
    const o1 = @as(usize, h1.kv_start + s1.physical_start) * dim;
    const count = @as(usize, retained) * dim;
    try std.testing.expectEqualSlices(f16, &.{ 17, 17, 17, 17 }, halves(generation.keys)[o0 .. o0 + count]);
    try std.testing.expectEqualSlices(f16, &.{ 17, 17, 17, 17 }, halves(generation.values)[o0 .. o0 + count]);
    try std.testing.expectEqualSlices(f16, halves(stored_keys), halves(generation.keys)[o1 .. o1 + count]);
    try std.testing.expectEqualSlices(f16, halves(stored_values), halves(generation.values)[o1 .. o1 + count]);
    const b0 = @as(usize, h0.bias_start + s0.bias_start);
    const b1 = @as(usize, h1.bias_start + s1.bias_start);
    try std.testing.expectEqualSlices(f32, &.{ 17, 17 }, generation.mass.?.floats()[b0 .. b0 + retained]);
    try std.testing.expectEqualSlices(f32, stored_bias.floats(), generation.mass.?.floats()[b1 .. b1 + retained]);
}
