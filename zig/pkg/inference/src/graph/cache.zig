// Copyright 2026 Antfly, Inc.
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

    fn hash(self: CacheKey) u64 {
        var h = std.hash.Wyhash.init(0);
        h.update(std.mem.asBytes(&self.config_hash));
        h.update(std.mem.asBytes(&self.batch));
        h.update(std.mem.asBytes(&self.seq_len));
        h.update(std.mem.asBytes(&self.attention_mode));
        return h.final();
    }

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
    /// Monotonic counter — higher = more recently used.
    last_used: u64,
    /// Cached weight tensors for parameter nodes. Populated on first
    /// execution so that subsequent interpreter runs skip getWeight()
    /// entirely by passing these as runtime_inputs.
    weight_inputs: ?[]RuntimeInput = null,
    /// Pre-computed reachable set + last-use arrays.  Populated on
    /// first execution so the interpreter skips per-decode analysis.
    cached_analysis: ?CachedAnalysis = null,
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

    fn freeWeights(self: *CacheEntry, cb: *const ComputeBackend) void {
        if (self.weight_inputs) |inputs| {
            for (inputs) |ri| cb.free(ri.value);
            self.graph.allocator.free(inputs);
            self.weight_inputs = null;
        }
    }

    fn freeAnalysis(self: *CacheEntry) void {
        if (self.cached_analysis) |*ca| {
            var analysis = ca.*;
            analysis.deinit(self.graph.allocator);
            self.cached_analysis = null;
        }
    }

    fn freeCompiledPartitions(self: *CacheEntry) void {
        if (self.compiled_partitions) |cps| {
            for (cps) |*cp| {
                cp.executor.deinitExecutor();
                self.graph.allocator.free(cp.runtime_input_node_ids);
            }
            self.graph.allocator.free(cps);
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
    entries: std.ArrayListUnmanaged(CacheEntry),
    allocator: std.mem.Allocator,
    /// Monotonic use counter for LRU eviction.
    use_counter: u64,
    /// Session-level whole-model runtime state. Unlike CacheEntry executors,
    /// this survives across graph shapes so prefill and decode can share
    /// runtime-owned KV/cache state when a backend supports it.
    compiled_model_runtime_backend: ?contracts.BackendKind = null,
    compiled_model_runtime_attachment_target: ?CompiledAttachmentTarget = null,
    compiled_model_runtime: ?model_runtime.ModelRuntime = null,

    pub fn init(allocator: std.mem.Allocator) GraphCache {
        return .{
            .entries = .empty,
            .allocator = allocator,
            .use_counter = 0,
        };
    }

    pub fn deinit(self: *GraphCache) void {
        self.freeSessionCompiledModelRuntime();
        for (self.entries.items) |*entry| {
            entry.freeCompiledModelExecutor();
            entry.freeCompiledPartitions();
            entry.freeAnalysis();
            entry.graph.deinit();
            // Note: weight CTs require a ComputeBackend to free.
            // Use deinitWithBackend() to properly release weight caches.
        }
        self.entries.deinit(self.allocator);
    }

    /// Deinit with proper weight cache cleanup.
    pub fn deinitWithBackend(self: *GraphCache, cb: *const ComputeBackend) void {
        self.freeSessionCompiledModelRuntime();
        for (self.entries.items) |*entry| {
            entry.freeCompiledModelExecutor();
            entry.freeCompiledPartitions();
            entry.freeWeights(cb);
            entry.freeAnalysis();
            entry.graph.deinit();
        }
        self.entries.deinit(self.allocator);
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
        for (self.entries.items) |*entry| {
            if (CacheKey.eql(entry.key, key)) {
                self.use_counter += 1;
                entry.last_used = self.use_counter;
                return &entry.graph;
            }
        }
        return null;
    }

    /// Look up a cached entry by key. Returns the full mutable entry
    /// so callers can access/populate the weight cache.
    pub fn getEntry(self: *GraphCache, key: CacheKey) ?*CacheEntry {
        for (self.entries.items) |*entry| {
            if (CacheKey.eql(entry.key, key)) {
                self.use_counter += 1;
                entry.last_used = self.use_counter;
                return entry;
            }
        }
        return null;
    }

    /// Insert a graph into the cache. If the cache is full, evicts the
    /// least recently used entry. Takes ownership of the graph.
    pub fn put(self: *GraphCache, key: CacheKey, graph: Graph) !void {
        return self.putWithBackend(key, graph, null);
    }

    /// Insert with optional backend for proper weight cache eviction.
    pub fn putWithBackend(self: *GraphCache, key: CacheKey, graph: Graph, cb: ?*const ComputeBackend) !void {
        // Check for duplicate key (replace existing)
        for (self.entries.items) |*entry| {
            if (CacheKey.eql(entry.key, key)) {
                entry.freeCompiledPartitions();
                entry.freeCompiledModelExecutor();
                if (cb) |backend| entry.freeWeights(backend);
                entry.freeAnalysis();
                entry.graph.deinit();
                entry.graph = graph;
                self.use_counter += 1;
                entry.last_used = self.use_counter;
                return;
            }
        }

        // Evict LRU if at capacity
        if (self.entries.items.len >= max_entries) {
            var lru_idx: usize = 0;
            var lru_time: u64 = std.math.maxInt(u64);
            for (self.entries.items, 0..) |entry, idx| {
                if (entry.last_used < lru_time) {
                    lru_time = entry.last_used;
                    lru_idx = idx;
                }
            }
            self.entries.items[lru_idx].freeCompiledPartitions();
            self.entries.items[lru_idx].freeCompiledModelExecutor();
            if (cb) |backend| self.entries.items[lru_idx].freeWeights(backend);
            self.entries.items[lru_idx].freeAnalysis();
            self.entries.items[lru_idx].graph.deinit();
            _ = self.entries.swapRemove(lru_idx);
        }

        self.use_counter += 1;
        try self.entries.append(self.allocator, .{
            .key = key,
            .graph = graph,
            .last_used = self.use_counter,
        });
    }

    /// Number of cached entries.
    pub fn count(self: *const GraphCache) usize {
        return self.entries.items.len;
    }

    /// Remove all cached entries and free their graphs.
    pub fn clear(self: *GraphCache) void {
        self.freeSessionCompiledModelRuntime();
        for (self.entries.items) |*entry| {
            entry.freeCompiledModelExecutor();
            entry.freeCompiledPartitions();
            entry.freeAnalysis();
            entry.graph.deinit();
        }
        self.entries.clearRetainingCapacity();
        self.use_counter = 0;
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
    try std.testing.expect(CacheKey.hash(k1) != CacheKey.hash(k2));
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
        .last_used = 0,
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

    cache.putSessionCompiledModelRuntime(.onnx, .whole_model, .{
        .ptr = &ctx,
        .vtable = &MockModelRuntimeCtx.runtime_vtable,
    });

    try std.testing.expect(cache.getSessionCompiledModelRuntime(.onnx, .whole_model) != null);
    try std.testing.expect(cache.getSessionCompiledModelRuntime(.pjrt, .whole_model) == null);

    cache.selectSessionCompiledModelRuntimeBackend(.pjrt, .whole_model);

    try std.testing.expectEqual(@as(usize, 1), runtime_deinit_order);
    try std.testing.expectEqual(@as(usize, 0), executor_deinit_order);
    try std.testing.expect(cache.getSessionCompiledModelRuntime(.onnx, .whole_model) == null);
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
        .last_used = 0,
        .compiled_partitions_backend = .onnx,
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
    // inserted first and never touched again → lowest last_used).
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
    lru_entry.compiled_partitions_backend = .onnx;
    lru_entry.compiled_partitions_status = .ready;

    // Now the LRU entry is config_hash=0 (even though we just touched it via
    // getEntry, its last_used is now the highest). To make it the true LRU
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
    entry.compiled_partitions_backend = .onnx;
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
        .last_used = 0,
        .compiled_partitions_backend = .pjrt,
        .compiled_partitions_status = .ready,
        .compiled_partitions = try makeMockCompiledPartitions(allocator, &ctxs),
    };
    defer entry.graph.deinit();

    entry.selectCompiledPartitionsBackend(.onnx, .partitioned);

    try std.testing.expectEqual(@as(usize, 1), deinit_count);
    try std.testing.expect(entry.compiled_partitions == null);
    try std.testing.expectEqual(@as(?contracts.BackendKind, .onnx), entry.compiled_partitions_backend);
    try std.testing.expectEqual(@as(?CompiledAttachmentTarget, .partitioned), entry.compiled_partitions_attachment_target);
    try std.testing.expectEqual(CompiledPartitionCacheStatus.uninitialized, entry.compiled_partitions_status);
}
