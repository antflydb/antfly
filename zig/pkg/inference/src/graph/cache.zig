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

// Graph compilation cache.
//
// Caches traced computation graphs keyed by (model config, batch, seq_len,
// attention mode). For autoregressive decoding the decode step graph
// (batch=1, seq_len=1, mode=paged_decode) is identical every step, so
// tracing once and reusing the graph eliminates tracing overhead from the
// hot decode loop.
//
// Prefill graphs vary by prompt length; callers can bucket to powers of 2
// for better cache hit rates.

const std = @import("std");
const ml = @import("ml");

const Graph = ml.graph.Graph;
const NodeId = ml.graph.NodeId;
const TracingCompute = @import("tracing_compute.zig").TracingCompute;
const WeightShape = @import("tracing_compute.zig").WeightShape;
const contracts = @import("backend_contracts.zig");
const ops_mod = @import("../ops/ops.zig");
const CT = contracts.CT;
const ComputeBackend = ops_mod.ComputeBackend;
const interpreter = @import("interpreter.zig");
const RuntimeInput = interpreter.RuntimeInput;
const CachedAnalysis = interpreter.CachedAnalysis;
const PartitionExecutor = @import("partition.zig").PartitionExecutor;
const model_runtime = @import("model_runtime.zig");

/// Attention mode mirroring DecodeContext.AttentionMode. Duplicated here
/// so the cache module doesn't depend on the full runtime import chain.
pub const AttentionMode = enum(u8) {
    full_recompute = 0,
    paged_prefill = 1,
    paged_decode = 2,
};

pub const CompiledAttachmentTarget = enum {
    partitioned,
    whole_model,
};

/// Cache key: identifies a unique graph shape.
pub const CacheKey = struct {
    /// Hash of the model configuration (architecture, dims, layer count, etc.).
    config_hash: u64,
    batch: u32,
    seq_len: u32,
    attention_mode: AttentionMode,

    fn eql(a: CacheKey, b: CacheKey) bool {
        return a.config_hash == b.config_hash and
            a.batch == b.batch and
            a.seq_len == b.seq_len and
            a.attention_mode == b.attention_mode;
    }
};

/// A compiled partition executor cached for reuse across decode steps.
/// Stores the PartitionExecutor value inline so that stable pointers
/// can be handed to partitions without separate heap allocations.
pub const CompiledPartition = struct {
    /// Index of the partition this executor corresponds to.
    partition_idx: u32,
    /// Type-erased executor.
    executor: PartitionExecutor,
    /// Graph node IDs that remain runtime inputs for this compiled partition.
    runtime_input_node_ids: []const NodeId = &.{},
};

pub const CompiledPartitionCacheStatus = enum {
    uninitialized,
    unavailable,
    ready,
};

pub const CompiledModelExecutor = struct {
    ptr: *anyopaque,
    deinit: *const fn (ctx: *anyopaque) void,

    pub fn deinitExecutor(self: CompiledModelExecutor) void {
        self.deinit(self.ptr);
    }
};

/// A cached, traced computation graph ready for interpreter execution.
pub const CacheEntry = struct {
    key: CacheKey,
    graph: Graph,
    /// Allocator for cached resources when the graph itself is borrowed.
    resource_allocator: ?std.mem.Allocator = null,
    /// Cached weight tensors for parameter nodes. Populated on first
    /// execution so that subsequent interpreter runs skip getWeight()
    /// entirely by passing these as runtime_inputs.
    weight_inputs: ?[]RuntimeInput = null,
    /// Pre-computed reachable set + last-use arrays.  Populated on
    /// first execution so the interpreter skips per-decode analysis.
    cached_analysis: ?CachedAnalysis = null,
    single_device_backend: ComputeBackend = undefined,
    single_device_executable: ?*@import("../execution/executable.zig").Executable = null,
    /// Cached compiled partition executors. Populated on
    /// first multi-device execution. Pointers into this array are borrowed
    /// by partitions during execution (set owns_executors=false on the plan).
    compiled_partitions_backend: ?contracts.BackendKind = null,
    compiled_partitions_attachment_target: ?CompiledAttachmentTarget = null,
    compiled_partitions_status: CompiledPartitionCacheStatus = .uninitialized,
    compiled_partitions: ?[]CompiledPartition = null,
    compiled_partitions_require_whole_graph: bool = false,
    compiled_model_backend: ?contracts.BackendKind = null,
    compiled_model_attachment_target: ?CompiledAttachmentTarget = null,
    compiled_model_status: CompiledPartitionCacheStatus = .uninitialized,
    compiled_model_executor: ?CompiledModelExecutor = null,
    compiled_model_runtime: ?model_runtime.ModelRuntime = null,

    pub fn resourceAllocator(self: *const CacheEntry) std.mem.Allocator {
        return self.resource_allocator orelse self.graph.allocator;
    }

    fn freeWeights(self: *CacheEntry, cb: *const ComputeBackend) void {
        if (self.weight_inputs) |inputs| {
            for (inputs) |ri| cb.free(ri.value);
            self.resourceAllocator().free(inputs);
            self.weight_inputs = null;
        }
    }

    pub fn resetSingleDeviceExecutable(self: *CacheEntry) void {
        if (self.single_device_executable) |executable| {
            executable.deinit();
            self.resourceAllocator().destroy(executable);
        }
        self.single_device_executable = null;
    }

    fn freeAnalysis(self: *CacheEntry) void {
        if (self.cached_analysis) |*ca| {
            var analysis = ca.*;
            analysis.deinit(self.resourceAllocator());
            self.cached_analysis = null;
        }
    }

    fn freeCompiledPartitions(self: *CacheEntry) void {
        if (self.compiled_partitions) |cps| {
            for (cps) |*cp| {
                cp.executor.deinitExecutor();
                self.resourceAllocator().free(cp.runtime_input_node_ids);
            }
            self.resourceAllocator().free(cps);
        }
        self.compiled_partitions = null;
        self.compiled_partitions_backend = null;
        self.compiled_partitions_attachment_target = null;
        self.compiled_partitions_status = .uninitialized;
        self.compiled_partitions_require_whole_graph = false;
    }

    fn freeCompiledModelExecutor(self: *CacheEntry) void {
        if (self.compiled_model_runtime) |*runtime_value| runtime_value.deinit();
        self.compiled_model_runtime = null;
        if (self.compiled_model_executor) |exec| exec.deinitExecutor();
        self.compiled_model_executor = null;
        self.compiled_model_backend = null;
        self.compiled_model_attachment_target = null;
        self.compiled_model_status = .uninitialized;
    }

    pub fn resetCompiledPartitions(self: *CacheEntry) void {
        self.freeCompiledPartitions();
    }

    pub fn resetCompiledModelExecutor(self: *CacheEntry) void {
        self.freeCompiledModelExecutor();
    }

    pub fn selectCompiledPartitionsBackend(
        self: *CacheEntry,
        backend: contracts.BackendKind,
        attachment_target: CompiledAttachmentTarget,
    ) void {
        const target_mismatch = self.compiled_partitions_attachment_target != null and
            self.compiled_partitions_attachment_target.? != attachment_target;
        if ((self.compiled_partitions_backend != null and self.compiled_partitions_backend.? != backend) or target_mismatch) {
            self.freeCompiledPartitions();
        }
        if (self.compiled_partitions_backend == null) {
            self.compiled_partitions_backend = backend;
            self.compiled_partitions_attachment_target = attachment_target;
            self.compiled_partitions_status = .uninitialized;
        }
    }

    pub fn selectCompiledModelExecutorBackend(
        self: *CacheEntry,
        backend: contracts.BackendKind,
        attachment_target: CompiledAttachmentTarget,
    ) void {
        const target_mismatch = self.compiled_model_attachment_target != null and
            self.compiled_model_attachment_target.? != attachment_target;
        if ((self.compiled_model_backend != null and self.compiled_model_backend.? != backend) or target_mismatch) {
            self.freeCompiledModelExecutor();
        }
        if (self.compiled_model_backend == null) {
            self.compiled_model_backend = backend;
            self.compiled_model_attachment_target = attachment_target;
            self.compiled_model_status = .uninitialized;
        }
    }
};

pub const max_entries: usize = 32;

/// LRU cache of traced computation graphs.
pub const GraphCache = struct {
    specializations: @import("../execution/specialization_cache.zig").Cache(CacheKey, *CacheEntry, entryKey, CacheKey.eql),
    allocator: std.mem.Allocator,
    /// Physical resources belong to a backend instance, not just its kind.
    /// The backend must outlive these bindings (including final retirement).
    prepared_backend: ?ComputeBackend = null,
    /// Session-level whole-model runtime state. Unlike CacheEntry executors,
    /// this survives across graph shapes so prefill and decode can share
    /// runtime-owned KV/cache state when a backend supports it.
    compiled_model_runtime_backend: ?contracts.BackendKind = null,
    compiled_model_runtime_attachment_target: ?CompiledAttachmentTarget = null,
    compiled_model_runtime: ?model_runtime.ModelRuntime = null,

    pub fn init(allocator: std.mem.Allocator) GraphCache {
        return .{
            .specializations = .init(max_entries),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *GraphCache) void {
        self.clear();
        self.specializations.deinit(self.allocator);
    }

    /// Compatibility for callers whose older cache has not selected a context.
    pub fn deinitWithBackend(self: *GraphCache, cb: *const ComputeBackend) void {
        if (self.prepared_backend == null) self.prepared_backend = cb.*;
        self.deinit();
    }

    /// Preserve semantic graphs/analysis, but retire every device-bound owner
    /// before changing contexts. Even two backends of the same kind may have
    /// disjoint tensor namespaces, streams and capture lifetimes.
    pub fn selectBackend(self: *GraphCache, cb: *const ComputeBackend) void {
        if (self.prepared_backend) |previous| {
            if (previous.ptr == cb.ptr and previous.vtable == cb.vtable) return;
            self.freeSessionCompiledModelRuntime();
            for (self.specializations.entries.items) |entry| retirePrepared(entry, &previous);
        }
        self.prepared_backend = cb.*;
    }

    fn retirePrepared(entry: *CacheEntry, cb: ?*const ComputeBackend) void {
        entry.freeCompiledModelExecutor();
        entry.freeCompiledPartitions();
        entry.resetSingleDeviceExecutable();
        if (cb) |backend| entry.freeWeights(backend);
    }

    fn freeSessionCompiledModelRuntime(self: *GraphCache) void {
        if (self.compiled_model_runtime) |*runtime_value| runtime_value.deinit();
        self.compiled_model_runtime = null;
        self.compiled_model_runtime_backend = null;
        self.compiled_model_runtime_attachment_target = null;
    }

    pub fn resetSessionCompiledModelRuntime(self: *GraphCache) void {
        self.freeSessionCompiledModelRuntime();
    }

    pub fn selectSessionCompiledModelRuntimeBackend(
        self: *GraphCache,
        backend: contracts.BackendKind,
        attachment_target: CompiledAttachmentTarget,
    ) void {
        const target_mismatch = self.compiled_model_runtime_attachment_target != null and
            self.compiled_model_runtime_attachment_target.? != attachment_target;
        if ((self.compiled_model_runtime_backend != null and self.compiled_model_runtime_backend.? != backend) or target_mismatch) {
            self.freeSessionCompiledModelRuntime();
        }
        if (self.compiled_model_runtime_backend == null) {
            self.compiled_model_runtime_backend = backend;
            self.compiled_model_runtime_attachment_target = attachment_target;
        }
    }

    pub fn getSessionCompiledModelRuntime(
        self: *GraphCache,
        backend: contracts.BackendKind,
        attachment_target: CompiledAttachmentTarget,
    ) ?*model_runtime.ModelRuntime {
        if (self.compiled_model_runtime_backend == null or self.compiled_model_runtime_attachment_target == null) return null;
        if (self.compiled_model_runtime_backend.? != backend or self.compiled_model_runtime_attachment_target.? != attachment_target) return null;
        if (self.compiled_model_runtime) |*runtime_value| return runtime_value;
        return null;
    }

    pub fn putSessionCompiledModelRuntime(
        self: *GraphCache,
        backend: contracts.BackendKind,
        attachment_target: CompiledAttachmentTarget,
        runtime_value: model_runtime.ModelRuntime,
    ) void {
        self.selectSessionCompiledModelRuntimeBackend(backend, attachment_target);
        if (self.compiled_model_runtime) |*existing| existing.deinit();
        self.compiled_model_runtime = runtime_value;
    }

    /// Look up a cached graph by key. Returns null on cache miss.
    pub fn get(self: *GraphCache, key: CacheKey) ?*const Graph {
        return if (self.getEntry(key)) |entry| &entry.graph else null;
    }

    pub fn getEntry(self: *GraphCache, key: CacheKey) ?*CacheEntry {
        return self.specializations.get(key);
    }

    fn entryKey(entry: *CacheEntry) CacheKey {
        return entry.key;
    }

    /// Insert a graph into the cache. If the cache is full, evicts the
    /// least recently used entry. Takes ownership of the graph.
    pub fn put(self: *GraphCache, key: CacheKey, graph: Graph) !void {
        return self.putWithBackend(key, graph, null);
    }

    /// Insert with optional backend for proper weight cache eviction.
    pub fn putWithBackend(self: *GraphCache, key: CacheKey, graph: Graph, cb: ?*const ComputeBackend) !void {
        if (cb) |backend| self.selectBackend(backend);
        const owner: ?*const ComputeBackend = if (self.prepared_backend) |*backend| backend else null;
        if (self.specializations.get(key)) |entry| {
            retireEntryContents(entry, owner);
            entry.graph = graph;
            return;
        }
        const inserted = try self.allocator.create(CacheEntry);
        errdefer self.allocator.destroy(inserted);
        try self.specializations.reserve(self.allocator);
        inserted.* = .{ .key = key, .graph = graph };
        if (self.specializations.insert(inserted)) |victim| {
            retireEntryContents(victim, owner);
            self.allocator.destroy(victim);
        }
    }

    fn retireEntryContents(entry: *CacheEntry, cb: ?*const ComputeBackend) void {
        retirePrepared(entry, cb);
        entry.freeAnalysis();
        entry.graph.deinit();
    }

    /// Number of cached entries.
    pub fn count(self: *const GraphCache) usize {
        return self.specializations.entries.items.len;
    }

    /// Remove all cached entries and free their graphs.
    pub fn clear(self: *GraphCache) void {
        self.freeSessionCompiledModelRuntime();
        const owner: ?*const ComputeBackend = if (self.prepared_backend) |*backend| backend else null;
        for (self.specializations.entries.items) |entry| {
            retireEntryContents(entry, owner);
            self.allocator.destroy(entry);
        }
        self.specializations.clear();
    }
};

/// Hash a model configuration struct by hashing the raw bytes of its
/// non-pointer fields. Works for any packed struct of integer/float/enum
/// fields — callers pass the byte representation.
pub fn hashConfigBytes(bytes: []const u8) u64 {
    return std.hash.Wyhash.hash(0x517cc1b727220a95, bytes);
}

/// Bucket a sequence length to the next power of 2 (for prefill cache
/// hit improvement). Decode seq_len=1 is returned as-is.
pub fn bucketSeqLen(seq_len: u32) u32 {
    if (seq_len <= 1) return seq_len;
    // Next power of 2
    return @as(u32, 1) << @intCast(@as(u6, @intCast(32 - @clz(seq_len - 1))));
}

const ShapeConstraint = ml.graph.ShapeConstraint;

/// Bucket a sequence length using a ShapeConstraint for finer control.
/// For bounded constraints, uses power-of-2 bucketing clamped to max.
/// For enumerated constraints, snaps to the
/// nearest valid enum value.
pub fn bucketSeqLenConstrained(seq_len: u32, constraint: ShapeConstraint) u32 {
    return @intCast(constraint.bucket(@intCast(seq_len)));
}

// ── Tests ──────────────────────────────────────────────────────────────

const Shape = ml.graph.Shape;
const Builder = ml.graph.Builder;

test "GraphCache basic put/get" {
    const allocator = std.testing.allocator;
    var cache = GraphCache.init(allocator);
    defer cache.deinit();

    // Build a small graph
    var g = Graph.init(allocator);
    var b = Builder.init(&g);
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const key = CacheKey{
        .config_hash = 42,
        .batch = 1,
        .seq_len = 1,
        .attention_mode = .paged_decode,
    };

    // Miss
    try std.testing.expect(cache.get(key) == null);

    // Put
    try cache.put(key, g);
    try std.testing.expectEqual(@as(usize, 1), cache.count());

    // Hit
    const cached = cache.get(key);
    try std.testing.expect(cached != null);
    try std.testing.expect(cached.?.nodeCount() > 0);
}

test "GraphCache LRU eviction" {
    const allocator = std.testing.allocator;
    var cache = GraphCache.init(allocator);
    defer cache.deinit();

    // Fill cache beyond max_entries
    for (0..max_entries + 5) |i| {
        var g = Graph.init(allocator);
        var b = Builder.init(&g);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
        const out = try b.gelu(x);
        try g.markOutput(out);

        try cache.put(.{
            .config_hash = @intCast(i),
            .batch = 1,
            .seq_len = 1,
            .attention_mode = .paged_decode,
        }, g);
    }

    // Should be capped at max_entries
    try std.testing.expectEqual(max_entries, cache.count());

    // Earliest entries (0..4) should have been evicted
    for (0..5) |i| {
        try std.testing.expect(cache.get(.{
            .config_hash = @intCast(i),
            .batch = 1,
            .seq_len = 1,
            .attention_mode = .paged_decode,
        }) == null);
    }

    // Later entries should still be present
    try std.testing.expect(cache.get(.{
        .config_hash = max_entries + 4,
        .batch = 1,
        .seq_len = 1,
        .attention_mode = .paged_decode,
    }) != null);
}

test "GraphCache replace duplicate key" {
    const allocator = std.testing.allocator;
    var cache = GraphCache.init(allocator);
    defer cache.deinit();

    const key = CacheKey{
        .config_hash = 1,
        .batch = 1,
        .seq_len = 1,
        .attention_mode = .full_recompute,
    };

    // First graph: 1 param + 1 op = some node count
    {
        var g = Graph.init(allocator);
        var b = Builder.init(&g);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
        const out = try b.gelu(x);
        try g.markOutput(out);
        try cache.put(key, g);
    }
    try std.testing.expectEqual(@as(usize, 1), cache.count());

    // Replace with a different graph
    {
        var g = Graph.init(allocator);
        var b = Builder.init(&g);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
        const w = try b.parameter("w", Shape.init(.f32, &.{4}));
        const normed = try b.rmsNorm(x, w, 4, 1e-5);
        const out = try b.gelu(normed);
        try g.markOutput(out);
        try cache.put(key, g);
    }

    // Still one entry, but with the new graph
    try std.testing.expectEqual(@as(usize, 1), cache.count());
    const cached = cache.get(key).?;
    try std.testing.expect(cached.nodeCount() > 3); // rmsNorm has decomposed nodes
}

test "bucketSeqLen powers of 2" {
    try std.testing.expectEqual(@as(u32, 1), bucketSeqLen(1));
    try std.testing.expectEqual(@as(u32, 2), bucketSeqLen(2));
    try std.testing.expectEqual(@as(u32, 4), bucketSeqLen(3));
    try std.testing.expectEqual(@as(u32, 4), bucketSeqLen(4));
    try std.testing.expectEqual(@as(u32, 8), bucketSeqLen(5));
    try std.testing.expectEqual(@as(u32, 128), bucketSeqLen(100));
    try std.testing.expectEqual(@as(u32, 2048), bucketSeqLen(1025));
}

test "CacheKey different modes are distinct" {
    const k1 = CacheKey{ .config_hash = 1, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode };
    const k2 = CacheKey{ .config_hash = 1, .batch = 1, .seq_len = 1, .attention_mode = .paged_prefill };
    try std.testing.expect(!CacheKey.eql(k1, k2));
    try std.testing.expect(!CacheKey.eql(k1, k2));
}

test "bucketSeqLenConstrained bounded" {
    const bounded = ShapeConstraint{ .bounded = .{ .max = 2048 } };
    try std.testing.expectEqual(@as(u32, 1), bucketSeqLenConstrained(1, bounded));
    try std.testing.expectEqual(@as(u32, 128), bucketSeqLenConstrained(100, bounded));
    try std.testing.expectEqual(@as(u32, 2048), bucketSeqLenConstrained(1025, bounded));
    // Clamped to max
    try std.testing.expectEqual(@as(u32, 2048), bucketSeqLenConstrained(4096, bounded));
}

test "bucketSeqLenConstrained enumerated" {
    const values = [_]i64{ 32, 64, 128, 256, 512 };
    const enumerated = ShapeConstraint{ .enumerated = &values };
    try std.testing.expectEqual(@as(u32, 32), bucketSeqLenConstrained(1, enumerated));
    try std.testing.expectEqual(@as(u32, 128), bucketSeqLenConstrained(65, enumerated));
    try std.testing.expectEqual(@as(u32, 512), bucketSeqLenConstrained(512, enumerated));
}

// ── compiled_partitions tests ─────────────────────────────────────────

/// Mock executor context that increments a shared counter on deinit.
const MockExecutorCtx = struct {
    deinit_count: *usize,

    const vtable = PartitionExecutor.VTable{
        .execute = &mockExecute,
        .deinit = &mockDeinit,
    };

    fn mockExecute(
        _: *anyopaque,
        _: []?CT,
        _: []@import("device_mesh.zig").DeviceId,
        _: []const ml.graph.NodeId,
        _: @import("device_mesh.zig").DeviceId,
        _: PartitionExecutor.ExecutionContext,
    ) anyerror!void {}
    fn mockDeinit(ctx: *anyopaque) void {
        const self: *MockExecutorCtx = @ptrCast(@alignCast(ctx));
        self.deinit_count.* += 1;
    }
};

fn makeMockCompiledPartitions(allocator: std.mem.Allocator, ctxs: []MockExecutorCtx) ![]CompiledPartition {
    const cps = try allocator.alloc(CompiledPartition, ctxs.len);
    for (ctxs, 0..) |*c, i| {
        cps[i] = .{
            .partition_idx = @intCast(i),
            .executor = .{ .ptr = @ptrCast(c), .vtable = &MockExecutorCtx.vtable },
        };
    }
    return cps;
}

const MockModelRuntimeCtx = struct {
    next_order: *usize,
    runtime_deinit_order: *usize,
    executor_deinit_order: *usize,

    const runtime_vtable = model_runtime.ModelRuntime.VTable{
        .prefill = &mockPrefill,
        .deinit = &mockRuntimeDeinit,
    };

    fn mockPrefill(
        _: *anyopaque,
        _: std.mem.Allocator,
        _: model_runtime.PrefillRequest,
    ) anyerror!model_runtime.ModelOutput {
        return error.UnexpectedPrefill;
    }

    fn mockRuntimeDeinit(ctx: *anyopaque) void {
        const self: *MockModelRuntimeCtx = @ptrCast(@alignCast(ctx));
        self.next_order.* += 1;
        self.runtime_deinit_order.* = self.next_order.*;
    }

    fn mockExecutorDeinit(ctx: *anyopaque) void {
        const self: *MockModelRuntimeCtx = @ptrCast(@alignCast(ctx));
        self.next_order.* += 1;
        self.executor_deinit_order.* = self.next_order.*;
    }
};

test "compiled_partitions defaults to null" {
    const allocator = std.testing.allocator;
    var cache = GraphCache.init(allocator);
    defer cache.deinit();

    var g = Graph.init(allocator);
    var b = Builder.init(&g);
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    const key = CacheKey{ .config_hash = 99, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode };
    try cache.put(key, g);

    const entry = cache.getEntry(key).?;
    try std.testing.expect(entry.compiled_partitions == null);
    try std.testing.expect(entry.compiled_partitions_backend == null);
    try std.testing.expectEqual(CompiledPartitionCacheStatus.uninitialized, entry.compiled_partitions_status);
}

test "freeCompiledModelExecutor deinitializes runtime before executor" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    var b = Builder.init(&g);
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var next_order: usize = 0;
    var runtime_deinit_order: usize = 0;
    var executor_deinit_order: usize = 0;
    var ctx = MockModelRuntimeCtx{
        .next_order = &next_order,
        .runtime_deinit_order = &runtime_deinit_order,
        .executor_deinit_order = &executor_deinit_order,
    };

    var entry = CacheEntry{
        .key = .{ .config_hash = 2, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode },
        .graph = g,
        .compiled_model_backend = .pjrt,
        .compiled_model_status = .ready,
        .compiled_model_executor = .{
            .ptr = &ctx,
            .deinit = &MockModelRuntimeCtx.mockExecutorDeinit,
        },
        .compiled_model_runtime = .{
            .ptr = &ctx,
            .vtable = &MockModelRuntimeCtx.runtime_vtable,
        },
    };
    defer entry.graph.deinit();

    entry.freeCompiledModelExecutor();

    try std.testing.expectEqual(@as(usize, 1), runtime_deinit_order);
    try std.testing.expectEqual(@as(usize, 2), executor_deinit_order);
    try std.testing.expect(entry.compiled_model_runtime == null);
    try std.testing.expect(entry.compiled_model_executor == null);
    try std.testing.expect(entry.compiled_model_backend == null);
    try std.testing.expectEqual(CompiledPartitionCacheStatus.uninitialized, entry.compiled_model_status);
}

test "GraphCache session compiled model runtime is independent of graph entries" {
    const allocator = std.testing.allocator;
    var cache = GraphCache.init(allocator);
    defer cache.deinit();

    var next_order: usize = 0;
    var runtime_deinit_order: usize = 0;
    var executor_deinit_order: usize = 0;
    var ctx = MockModelRuntimeCtx{
        .next_order = &next_order,
        .runtime_deinit_order = &runtime_deinit_order,
        .executor_deinit_order = &executor_deinit_order,
    };

    cache.putSessionCompiledModelRuntime(.metal, .whole_model, .{
        .ptr = &ctx,
        .vtable = &MockModelRuntimeCtx.runtime_vtable,
    });

    try std.testing.expect(cache.getSessionCompiledModelRuntime(.metal, .whole_model) != null);
    try std.testing.expect(cache.getSessionCompiledModelRuntime(.pjrt, .whole_model) == null);

    cache.selectSessionCompiledModelRuntimeBackend(.pjrt, .whole_model);

    try std.testing.expectEqual(@as(usize, 1), runtime_deinit_order);
    try std.testing.expectEqual(@as(usize, 0), executor_deinit_order);
    try std.testing.expect(cache.getSessionCompiledModelRuntime(.metal, .whole_model) == null);
    try std.testing.expect(cache.compiled_model_runtime == null);
    try std.testing.expectEqual(@as(?contracts.BackendKind, .pjrt), cache.compiled_model_runtime_backend);
    try std.testing.expectEqual(@as(?CompiledAttachmentTarget, .whole_model), cache.compiled_model_runtime_attachment_target);
}

test "freeCompiledPartitions calls executor deinit" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    var b = Builder.init(&g);
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var deinit_count: usize = 0;
    var ctxs = [_]MockExecutorCtx{
        .{ .deinit_count = &deinit_count },
        .{ .deinit_count = &deinit_count },
    };

    var entry = CacheEntry{
        .key = .{ .config_hash = 1, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode },
        .graph = g,
        .compiled_partitions_backend = .metal,
        .compiled_partitions_status = .ready,
        .compiled_partitions = try makeMockCompiledPartitions(allocator, &ctxs),
    };
    defer entry.graph.deinit();

    entry.freeCompiledPartitions();

    try std.testing.expectEqual(@as(usize, 2), deinit_count);
    try std.testing.expect(entry.compiled_partitions == null);
    try std.testing.expect(entry.compiled_partitions_backend == null);
    try std.testing.expectEqual(CompiledPartitionCacheStatus.uninitialized, entry.compiled_partitions_status);
}

test "LRU eviction calls freeCompiledPartitions" {
    const allocator = std.testing.allocator;
    var cache = GraphCache.init(allocator);
    defer cache.deinit();

    var deinit_count: usize = 0;

    // Fill the cache to max_entries. Attach mock executors to the first entry
    // so we can verify eviction calls deinit.
    // We need a stable allocation for the mock context since the cache entry
    // will live beyond this scope.
    var mock_ctx = MockExecutorCtx{ .deinit_count = &deinit_count };

    for (0..max_entries) |i| {
        var g = Graph.init(allocator);
        var b = Builder.init(&g);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
        const out = try b.gelu(x);
        try g.markOutput(out);

        try cache.put(.{
            .config_hash = @intCast(i),
            .batch = 1,
            .seq_len = 1,
            .attention_mode = .paged_decode,
        }, g);
    }

    // Attach compiled partitions to the LRU entry (config_hash=0, which was
    // inserted first and never touched again → oldest handle).
    const lru_entry = cache.getEntry(.{
        .config_hash = 0,
        .batch = 1,
        .seq_len = 1,
        .attention_mode = .paged_decode,
    }).?;

    const cps = try allocator.alloc(CompiledPartition, 1);
    cps[0] = .{
        .partition_idx = 0,
        .executor = .{ .ptr = @ptrCast(&mock_ctx), .vtable = &MockExecutorCtx.vtable },
    };
    lru_entry.compiled_partitions = cps;
    lru_entry.compiled_partitions_backend = .metal;
    lru_entry.compiled_partitions_status = .ready;

    // Now the LRU entry is config_hash=0 (even though we just touched it via
    // getEntry, its handle is now the newest). To make it the true LRU
    // again, touch all other entries.
    for (1..max_entries) |i| {
        _ = cache.get(.{
            .config_hash = @intCast(i),
            .batch = 1,
            .seq_len = 1,
            .attention_mode = .paged_decode,
        });
    }

    try std.testing.expectEqual(@as(usize, 0), deinit_count);

    // Insert one more entry → should evict config_hash=0 (the LRU).
    {
        var g = Graph.init(allocator);
        var b = Builder.init(&g);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
        const out = try b.gelu(x);
        try g.markOutput(out);

        try cache.put(.{
            .config_hash = max_entries,
            .batch = 1,
            .seq_len = 1,
            .attention_mode = .paged_decode,
        }, g);
    }

    // The evicted entry's compiled_partitions deinit should have been called.
    try std.testing.expectEqual(@as(usize, 1), deinit_count);
    try std.testing.expect(cache.get(.{
        .config_hash = 0,
        .batch = 1,
        .seq_len = 1,
        .attention_mode = .paged_decode,
    }) == null);
}

test "cache replacement calls freeCompiledPartitions" {
    const allocator = std.testing.allocator;
    var cache = GraphCache.init(allocator);
    defer cache.deinit();

    var deinit_count: usize = 0;
    var mock_ctx = MockExecutorCtx{ .deinit_count = &deinit_count };

    const key = CacheKey{ .config_hash = 1, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode };

    // Insert initial graph.
    {
        var g = Graph.init(allocator);
        var b = Builder.init(&g);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
        const out = try b.gelu(x);
        try g.markOutput(out);
        try cache.put(key, g);
    }

    // Attach compiled partitions.
    const entry = cache.getEntry(key).?;
    const cps = try allocator.alloc(CompiledPartition, 1);
    cps[0] = .{
        .partition_idx = 0,
        .executor = .{ .ptr = @ptrCast(&mock_ctx), .vtable = &MockExecutorCtx.vtable },
    };
    entry.compiled_partitions = cps;
    entry.compiled_partitions_backend = .metal;
    entry.compiled_partitions_status = .ready;

    try std.testing.expectEqual(@as(usize, 0), deinit_count);

    // Replace with a new graph under the same key.
    {
        var g = Graph.init(allocator);
        var b = Builder.init(&g);
        const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
        const out = try b.gelu(x);
        try g.markOutput(out);
        try cache.put(key, g);
    }

    // Replacement should have freed the old compiled partitions.
    try std.testing.expectEqual(@as(usize, 1), deinit_count);
    try std.testing.expectEqual(@as(usize, 1), cache.count());
}

test "selectCompiledPartitionsBackend resets mismatched backend caches" {
    const allocator = std.testing.allocator;

    var g = Graph.init(allocator);
    var b = Builder.init(&g);
    const x = try b.parameter("x", Shape.init(.f32, &.{ 2, 4 }));
    const out = try b.gelu(x);
    try g.markOutput(out);

    var deinit_count: usize = 0;
    var ctxs = [_]MockExecutorCtx{
        .{ .deinit_count = &deinit_count },
    };

    var entry = CacheEntry{
        .key = .{ .config_hash = 7, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode },
        .graph = g,
        .compiled_partitions_backend = .pjrt,
        .compiled_partitions_status = .ready,
        .compiled_partitions = try makeMockCompiledPartitions(allocator, &ctxs),
    };
    defer entry.graph.deinit();

    entry.selectCompiledPartitionsBackend(.metal, .partitioned);

    try std.testing.expectEqual(@as(usize, 1), deinit_count);
    try std.testing.expect(entry.compiled_partitions == null);
    try std.testing.expectEqual(@as(?contracts.BackendKind, .metal), entry.compiled_partitions_backend);
    try std.testing.expectEqual(@as(?CompiledAttachmentTarget, .partitioned), entry.compiled_partitions_attachment_target);
    try std.testing.expectEqual(CompiledPartitionCacheStatus.uninitialized, entry.compiled_partitions_status);
}

test "GraphCache entry addresses survive growth and unrelated LRU eviction" {
    const a = std.testing.allocator;
    var cache = GraphCache.init(a);
    defer cache.deinit();
    var first: ?*CacheEntry = null;
    const protected = CacheKey{ .config_hash = 0, .batch = 1, .seq_len = 1, .attention_mode = .full_recompute };
    for (0..max_entries + 8) |index| {
        if (first != null) try std.testing.expectEqual(first.?, cache.getEntry(protected).?);
        var graph = Graph.init(a);
        var owned = true;
        defer if (owned) graph.deinit();
        var b = Builder.init(&graph);
        const x = try b.parameter("x", Shape.init(.f32, &.{4}));
        const y = try b.neg(x);
        try graph.markOutput(y);
        var key = protected;
        key.config_hash = index;
        try cache.put(key, graph);
        owned = false;
        if (first == null) first = cache.getEntry(protected).?;
        try std.testing.expectEqual(&first.?.graph, cache.get(protected).?);
    }
}

test "GraphCache stable entry insertion and replacement clean up every allocation failure" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var cache = GraphCache.init(a);
            defer cache.deinit();
            for (0..3) |index| {
                var graph = Graph.init(a);
                var owned = true;
                defer if (owned) graph.deinit();
                var b = Builder.init(&graph);
                const x = try b.parameter("x", Shape.init(.f32, &.{4}));
                try graph.markOutput(try b.neg(x));
                const key = CacheKey{ .config_hash = index % 2, .batch = 1, .seq_len = 1, .attention_mode = .full_recompute };
                try cache.put(key, graph);
                owned = false;
            }
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "GraphCache switches same-kind contexts without reusing device bindings" {
    const Mock = struct {
        frees: usize = 0,
        fn free(raw: *anyopaque, tensor: CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(tensor == raw);
            self.frees += 1;
        }
    };
    var vt = @import("../ops/native_compute.zig").vtable_impl;
    vt.freeTensor = Mock.free;
    var first = Mock{};
    var second = Mock{};
    const first_cb = ComputeBackend{ .ptr = &first, .vtable = &vt };
    const second_cb = ComputeBackend{ .ptr = &second, .vtable = &vt };
    var cache = GraphCache.init(std.testing.allocator);
    defer cache.deinit();
    const key = CacheKey{ .config_hash = 1, .batch = 1, .seq_len = 1, .attention_mode = .paged_decode };
    var graph = Graph.init(std.testing.allocator);
    var builder = Builder.init(&graph);
    const x = try builder.parameter("x", Shape.init(.f32, &.{1}));
    try graph.markOutput(x);
    try cache.putWithBackend(key, graph, &first_cb);
    const entry = cache.getEntry(key).?;
    entry.weight_inputs = try entry.resourceAllocator().alloc(RuntimeInput, 1);
    entry.weight_inputs.?[0] = .{ .node_id = x, .value = &first };
    entry.cached_analysis = try CachedAnalysis.compute(entry.resourceAllocator(), &entry.graph);
    const analysis_ptr = entry.cached_analysis.?.reachable.ptr;
    cache.selectBackend(&first_cb);
    try std.testing.expectEqual(@as(usize, 0), first.frees);
    cache.selectBackend(&second_cb);
    try std.testing.expectEqual(@as(usize, 1), first.frees);
    try std.testing.expectEqual(@as(usize, 0), second.frees);
    try std.testing.expect(entry == cache.getEntry(key).?);
    try std.testing.expect(entry.weight_inputs == null);
    try std.testing.expect(entry.cached_analysis.?.reachable.ptr == analysis_ptr);
    entry.weight_inputs = try entry.resourceAllocator().alloc(RuntimeInput, 1);
    entry.weight_inputs.?[0] = .{ .node_id = x, .value = &second };
    cache.clear();
    try std.testing.expectEqual(@as(usize, 1), second.frees);
}
