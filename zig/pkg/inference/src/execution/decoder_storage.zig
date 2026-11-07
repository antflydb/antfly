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

//! Physical KV storage policy, shared by task admission and decoder runtimes.
const std = @import("std");
const Config = @import("../models/gpt.zig").Config;
const pool = @import("../runtime/kv/pool.zig");
const CapacityPolicy = @import("../runtime/tier/memory.zig").GenerationKvCapacityPolicy;

pub const Options = struct {
    shared_kv: bool = true,
    /// Existing whole-model runtimes address every architectural layer. The
    /// compact recurrent route instead assigns slots only to stored KV layers.
    layer_layout: enum { architectural, stored } = .stored,
    force_sliding_trim: bool = false,
    host_mirrors: bool = true,
};

pub fn poolPlan(backend: pool.BackendKind, dtype: pool.KvDType, config: *const Config, options: Options) pool.KvPoolConfig {
    return .{
        .backend = backend,
        .dtype = dtype,
        .page_size_tokens = 16,
        .num_layers_packed = @intCast(switch (options.layer_layout) {
            .architectural => config.num_hidden_layers,
            .stored => config.kvStorageLayerCount(),
        }),
        .num_kv_heads = config.maxKvHeads(),
        .head_dim = config.maxHeadDim(),
        .sliding_window_size = config.kvPoolSlidingWindowSize(options.force_sliding_trim),
        .store_cpu_bytes = options.host_mirrors,
    };
}

/// A CUDA request manager owns logical positions; the numerical owner supplies
/// device KV hooks. Shared prompt-cache pools retain their existing storage.
pub fn requestMetadataPool(storage_config: pool.KvPoolConfig) pool.KvPoolConfig {
    var config = storage_config;
    if (config.backend == .cuda) config.store_cpu_bytes = false;
    return config;
}

/// Checkpoint preparation/restore preserves every original logical position.
/// Numerical KV is snapshotted through its device reader, without host mirrors.
pub fn checkpointPool(dtype: pool.KvDType, config: Config) pool.KvPoolConfig {
    var result = poolPlan(.cuda, dtype, &config, .{ .host_mirrors = false });
    result.sliding_window_size = null;
    return result;
}

pub const Route = enum { standard, metal_whole_model };

/// Task policy supplies request eligibility; execution owns the physical choice.
pub fn capacityPolicy(route: Route, split_ring_eligible: bool) CapacityPolicy {
    return if (route == .metal_whole_model and split_ring_eligible) .split_swa_ring else .full_history;
}

pub fn lazyDeviceReservation(config: Config) bool {
    if (config.family != .gemma or !config.hasPle()) return false;
    const mixed_width = config.hasGlobalAttentionLayers() and
        (config.maxKvHeads() != config.effectiveKVHeads() or config.maxHeadDim() != config.headDim());
    return mixed_width or config.num_kv_shared_layers > 0;
}

test "decoder storage preserves physical geometry and metadata-only CUDA owners" {
    const model = Config{
        .num_hidden_layers = 8,
        .num_attention_heads = 4,
        .num_key_value_heads = 2,
        .attention_head_dim = 32,
        .sliding_window = 64,
        .position_encoding = .rope,
    };
    for ([_]pool.BackendKind{ .native, .metal, .cuda }) |backend| {
        const plan = poolPlan(backend, .f16, &model, .{});
        try std.testing.expectEqual(@as(u16, 8), plan.num_layers_packed);
        try std.testing.expectEqual(@as(u32, 2), plan.num_kv_heads);
        try std.testing.expectEqual(@as(u32, 32), plan.head_dim);
        try std.testing.expectEqual(@as(?u32, 64), plan.sliding_window_size);
        const logical = requestMetadataPool(plan);
        try std.testing.expectEqual(backend != .cuda, logical.store_cpu_bytes);
        try std.testing.expect(plan.store_cpu_bytes);
    }
    const checkpoint = checkpointPool(.f16, model);
    try std.testing.expect(!checkpoint.store_cpu_bytes);
    try std.testing.expect(checkpoint.sliding_window_size == null);
    try std.testing.expectEqual(CapacityPolicy.full_history, capacityPolicy(.standard, true));
    try std.testing.expectEqual(CapacityPolicy.full_history, capacityPolicy(.metal_whole_model, false));
    try std.testing.expectEqual(CapacityPolicy.split_swa_ring, capacityPolicy(.metal_whole_model, true));
}

/// Task semantics supplied before choosing a physical cache layout. This
/// record contains no tokenizer, pipeline or task configuration dependency.
pub const Request = struct {
    pure_greedy: bool,
    grammar: bool,
    prompt_cache: bool,
    compaction: bool,
    speculation: bool,
    draft: bool,
};

pub fn slidingTrimForced() bool {
    return @import("antfly_platform").env.getenvBoolDefault("ANTFLY_INFERENCE_KV_SLIDING_TRIM", false);
}

pub fn ringRequestEligible(request: Request, sampled_qualified: bool) bool {
    // Select full-history storage before prefill for unsupported consumers;
    // never switch layouts or weaken the read guard after ring eviction.
    return ((request.pure_greedy and !request.grammar) or sampled_qualified) and
        !request.prompt_cache and !request.compaction and !request.speculation and !request.draft;
}

pub fn splitRingRequestEligible(config: Config, request: Request, sampled_qualified: bool) bool {
    if (comptime !@import("build_options").enable_metal) return false;
    const sampled_or_grammar = !request.pure_greedy or request.grammar;
    return config.supportsSplitSwaGlobalKvRing() and
        (!sampled_or_grammar or config.num_local_experts == 0) and
        !@import("../architectures/gemma4_runtime.zig").wholeFramePrefillExplicitlyDisabled() and
        ringRequestEligible(request, sampled_qualified) and
        !slidingTrimForced() and
        @import("../backends/metal_kv_storage.zig").MetalKvStorage.splitSwaKvRingEnabled();
}

pub fn splitRingEligible(config: Config, request: Request, dtype: pool.KvDType, sampled_qualified: bool) bool {
    if (comptime !@import("build_options").enable_metal) return false;
    return splitRingRequestEligible(config, request, sampled_qualified) and
        @import("../backends/metal_kv_storage.zig").KeyFormat.fromKvDType(dtype) != null;
}

pub fn routeCapacityPolicy(route: Route, config: Config, request: Request, dtype: pool.KvDType, sampled_qualified: bool) CapacityPolicy {
    if (route == .standard) return .full_history;
    return capacityPolicy(route, splitRingEligible(config, request, dtype, sampled_qualified));
}

pub fn selectedPeakBytes(config: Config, dtype: pool.KvDType, gpu_oracle: bool) !usize {
    const policy = @import("../runtime/kv/selection.zig").defaults;
    const transient = try policy.transientRows();
    var resident_device: usize = 0;
    var largest_row_bytes: usize = 0;
    var largest_key_width: usize = 0;
    var largest_query_width: usize = 0;
    var query_capture: usize = 0;
    for (0..@as(usize, @intCast(config.num_hidden_layers))) |layer| {
        const geometry = try config.kvDonorGeometry(layer);
        if (!config.layerUsesSlidingAttention(layer)) {
            const query_width = try std.math.mul(usize, @intCast(config.num_attention_heads), @intCast(geometry.head_dim));
            largest_query_width = @max(largest_query_width, query_width);
            query_capture = try std.math.add(usize, query_capture, try std.math.mul(usize, policy.query_horizon, try std.math.mul(usize, query_width, @sizeOf(f32))));
        }
        if (!config.isKvDonorLayer(layer) or geometry.sliding) continue;
        const row_bytes = try dtype.bytesForTokenPairChecked(geometry.num_kv_heads, geometry.head_dim);
        resident_device = try std.math.add(usize, resident_device, try std.math.mul(usize, transient, row_bytes));
        largest_row_bytes = @max(largest_row_bytes, row_bytes);
        largest_key_width = @max(largest_key_width, try std.math.mul(usize, @intCast(geometry.num_kv_heads), @intCast(geometry.head_dim)));
    }
    const staged_device = try std.math.mul(usize, transient, largest_row_bytes);
    // Positions, accumulator, GPU result, ranking records, keep bitmap, and
    // retained source/position arrays, rounded upward for alignment overhead.
    const host_selection = try std.math.mul(usize, transient, 80);
    // Metal owns copies of Q and positions, per-query/head normalization, and
    // scores. Consumer dispatches are serial, so only the largest is live.
    const gpu_q = try std.math.mul(usize, policy.query_horizon, try std.math.mul(usize, largest_query_width, @sizeOf(f32)));
    const gpu_query_positions = try std.math.mul(usize, policy.query_horizon, @sizeOf(u32));
    const gpu_key_positions = try std.math.mul(usize, transient, @sizeOf(u32));
    const gpu_norm = try std.math.mul(usize, policy.query_horizon, try std.math.mul(usize, @intCast(config.num_attention_heads), 2 * @sizeOf(f32)));
    const gpu_scores = try std.math.mul(usize, transient, @sizeOf(f32));
    var scorer = try std.math.add(usize, gpu_q, gpu_query_positions);
    scorer = try std.math.add(usize, scorer, gpu_key_positions);
    scorer = try std.math.add(usize, scorer, gpu_norm);
    scorer = try std.math.add(usize, scorer, gpu_scores);
    var oracle: usize = 0;
    if (gpu_oracle) {
        oracle = try std.math.mul(usize, transient, try std.math.mul(usize, largest_key_width, @sizeOf(f32)));
        oracle = try std.math.add(usize, oracle, try std.math.mul(usize, transient, @sizeOf(f64)));
    }
    var total = try std.math.add(usize, resident_device, staged_device);
    total = try std.math.add(usize, total, query_capture);
    total = try std.math.add(usize, total, host_selection);
    total = try std.math.add(usize, total, scorer);
    return std.math.add(usize, total, oracle);
}

/// Enumerate only actual KV owners. Recurrent layers and shared-tail consumers
/// do not acquire duplicate physical planes; the existing model metadata maps
/// donors into architectural or compact stored indices. Backend code sees only
/// typed IDs/head rows, never model family names or inferred dense numbering.
pub fn requiredKvGeometry(config: *const Config, options: Options) !@import("../runtime/kv/device_capacity.zig").Geometry {
    var result = @import("../runtime/kv/device_capacity.zig").Geometry{};
    for (0..config.num_hidden_layers) |layer| {
        if (config.layerUsesQwen35LinearAttention(layer)) continue;
        const donor = if (options.shared_kv) try config.kvStorageLayerIndex(layer) else layer;
        if (donor != layer) continue;
        const geometry = if (options.shared_kv) try config.kvDonorGeometry(layer) else Config.KvDonorGeometry{
            .donor_layer = layer,
            .sliding = config.layerUsesSlidingAttention(layer),
            .num_kv_heads = config.effectiveKVHeadsForLayer(layer),
            .head_dim = config.effectiveHeadDimForLayer(layer),
        };
        // Ring/selected/private layouts need their separate actual ownership
        // contract. Full-history storage may still have a sliding model mask.
        const storage_index = switch (options.layer_layout) {
            .architectural => donor,
            .stored => config.compactQwen35KvStorageLayerIndex(donor) orelse return error.InvalidDeviceCapacityLayout,
        };
        try result.append(.{ .index = std.math.cast(u16, storage_index) orelse return error.InvalidDeviceCapacityLayout, .num_kv_heads = geometry.num_kv_heads, .head_dim = geometry.head_dim });
    }
    return result;
}

test "decoder KV capacity geometry preserves sparse architectural layers and compact stored owners" {
    var model = Config{ .family = .qwen3_5, .num_hidden_layers = 8, .num_attention_heads = 4, .num_key_value_heads = 2, .attention_head_dim = 32, .qwen35_full_attention_interval = 4, .qwen35_has_linear_attention = true };
    const architectural = try requiredKvGeometry(&model, .{ .layer_layout = .architectural });
    try std.testing.expectEqual(@as(u16, 2), architectural.count);
    try std.testing.expectEqual(@as(u16, 3), architectural.layers[0].index);
    try std.testing.expectEqual(@as(u16, 7), architectural.layers[1].index);
    try std.testing.expectEqual(@as(usize, 64), try architectural.layers[0].width());
    model.compact_qwen35_kv = true;
    // A compact flag alone cannot change the validated architecture contract.
    const unsupported_compact = try requiredKvGeometry(&model, .{});
    try std.testing.expectEqual(@as(u16, 3), unsupported_compact.layers[0].index);
    var compact = try @import("../models/gpt.zig").parseConfig(std.testing.allocator, @embedFile("../models/testdata/lightonocr3/config.json"));
    compact.num_hidden_layers = 8;
    compact.compact_qwen35_kv = true;
    try std.testing.expect(compact.usesCompactQwen35Kv());
    const stored = try requiredKvGeometry(&compact, .{});
    try std.testing.expectEqual(@as(u16, 2), stored.count);
    try std.testing.expectEqual(@as(u16, 0), stored.layers[0].index);
    try std.testing.expectEqual(@as(u16, 1), stored.layers[1].index);
    const ordinary = Config{ .num_hidden_layers = 3, .num_attention_heads = 4, .num_key_value_heads = 2, .attention_head_dim = 32 };
    const dense = try requiredKvGeometry(&ordinary, .{});
    try std.testing.expectEqual(@as(u16, 3), dense.count);
    for (dense.items(), 0..) |layer, index| try std.testing.expectEqual(index, layer.index);
}

const DecodeState = @import("../graph/decode_state_runtime.zig").NativeDecodeState;
const PrefixCache = @import("../runtime/kv/prompt_cache.zig").PromptPrefixCache;
const Control = @import("../execution_control.zig").InferenceExecutionControl;

const ComputeBackend = @import("../ops/ops.zig").ComputeBackend;
const MetadataStorage = @import("../runtime/kv/storage_runtime.zig").KvStorageRuntime;

/// Borrowed only by the constructing request. Server owns the exact local
/// metadata object, backend and state through generation and subsequent close.
/// It is neither a model identity nor permission to substitute another storage.
pub const NativePrefixBinding = struct {
    backend_ptr: *anyopaque,
    backend_table: *const ComputeBackend.VTable,
    cache: *PrefixCache,
    state: *DecodeState,
    metadata: *MetadataStorage,
    phase: enum { fresh, manager_only, closed } = .fresh,

    pub fn init(backend: *const ComputeBackend, cache: ?*PrefixCache, state: *DecodeState, metadata: ?*MetadataStorage) ?NativePrefixBinding {
        const owner = cache orelse return null;
        const local = metadata orelse return null;
        if (!canonicalNative(backend) or !managerMatches(state, owner) or owner.storagePtr() != null or
            state.kv_storage != local or state.sequence_id != null or state.total_tokens != 0 or !emptyNativeMetadata(local, owner)) return null;
        return .{ .backend_ptr = backend.ptr, .backend_table = backend.vtable, .cache = owner, .state = state, .metadata = local };
    }
    /// Invalidate even when generation exits before attempting cache reuse.
    pub fn deinit(self: *NativePrefixBinding) void {
        self.phase = .closed;
    }
    fn matches(self: *const NativePrefixBinding, backend: *const ComputeBackend, state: *const DecodeState, owner: *PrefixCache) bool {
        if (self.phase == .closed or self.state != state or self.cache != owner or
            self.backend_ptr != backend.ptr or self.backend_table != backend.vtable or !canonicalNative(backend) or
            !managerMatches(state, owner) or owner.storagePtr() != null or !nativeMetadataGeometry(self.metadata, owner)) return false;
        return switch (self.phase) {
            .fresh => state.kv_storage == self.metadata,
            .manager_only => state.kv_storage == null,
            .closed => false,
        };
    }
};
fn canonicalNative(backend: *const ComputeBackend) bool {
    return backend.vtable == &@import("../ops/native_compute.zig").vtable_impl and backend.kind() == .native;
}
fn managerMatches(state: *const DecodeState, cache: *PrefixCache) bool {
    if (state.kv_manager != cache.managerPtr() or state.pool_id != cache.pool_id) return false;
    const storage = cache.manager.getPool(cache.pool_id orelse return false) orelse return false;
    return storage.config.backend == .native and storage.config.store_cpu_bytes;
}
fn nativeMetadataGeometry(metadata: *const MetadataStorage, owner: *PrefixCache) bool {
    const config = owner.pool_config orelse return false;
    return metadata.device_write_hook == null and metadata.storage.config.backend == .native and metadata.storage.config.compatible(config);
}
fn emptyNativeMetadata(metadata: *const MetadataStorage, owner: *PrefixCache) bool {
    if (!nativeMetadataGeometry(metadata, owner) or metadata.storage.liveBlocks() != 0) return false;
    for (metadata.sequences.items) |sequence| if (sequence.active) return false;
    return true;
}

/// Validate physical selected-position support before task request allocation.
pub fn validateSelectedRoute(selected: bool, backend: @import("../ops/ops.zig").BackendKind, scheduled: bool) !void {
    if (selected and (backend != .metal or scheduled)) return error.UnsupportedKvCachePolicy;
}

pub fn validateSelectedTokenBudget(selected: bool, max_tokens: usize, protected: []const @import("../runtime/kv/selection.zig").ProtectedSpan) !void {
    if (!selected) return;
    const selection = @import("../runtime/kv/selection.zig");
    if (try selection.mandatoryRowCount(selection.defaults, @intCast(max_tokens), protected) > selection.defaults.target_rows)
        return error.ProtectedRowsExceedTarget;
}

/// Borrowed request lifetime, not a new execution engine. The loaded runtime
/// owns state and model configuration; no allocation occurs on the ordinary path.
pub const SelectedRequest = struct {
    state: ?*DecodeState = null,
    pub fn begin(enabled: bool, backend: *const ComputeBackend, scheduled: bool, state: *DecodeState, model: *const Config, dtype: ?pool.KvDType, peak_limit: usize, gpu_oracle: bool, protected: []const @import("../runtime/kv/selection.zig").ProtectedSpan) !SelectedRequest {
        if (!enabled) return .{};
        state.selection_protected_spans = protected;
        var guard = SelectedRequest{ .state = state };
        errdefer guard.deinit();
        try validateSelectedRoute(true, backend.kind(), scheduled);
        const peak = try selectedPeakBytes(model.*, dtype orelse return error.UnsupportedKvCachePolicy, gpu_oracle);
        if (peak > peak_limit) return error.MemoryBudgetExceeded;
        try state.prepareSelectedGlobalDonors(model.*);
        return guard;
    }
    pub fn deinit(self: *SelectedRequest) void {
        const state = self.state orelse return;
        self.state = null;
        state.selection_capture_enabled = false;
        state.selection_protected_spans = &.{};
        state.clearSelectionQueries();
    }
};

/// Task supplies privacy/semantic eligibility and an opaque caller namespace.
/// Runtime alone validates the physical state/cache ownership and supported model.
pub const PrefixRequest = struct {
    enabled: bool,
    namespace: ?[]const u8,
    has_media: bool,
    speculation: bool,
    compaction: bool,
    compiled: bool,
};

fn prefixStorageMatches(backend: *const ComputeBackend, binding: ?*const NativePrefixBinding, state: *const DecodeState, cache: *PrefixCache) bool {
    if (state.kv_manager != cache.managerPtr() or state.pool_id != cache.pool_id) return false;
    if (cache.storagePtr()) |shared| return state.kv_storage == shared;
    const native = binding orelse return false;
    return native.matches(backend, state, cache);
}

fn prefixReusable(backend: *const ComputeBackend, binding: ?*const NativePrefixBinding, state: *const DecodeState, model: *const Config, owner: *PrefixCache, request: PrefixRequest) bool {
    return request.enabled and request.namespace != null and !request.has_media and !request.speculation and !request.compaction and !request.compiled and
        !DecodeState.requiresDeepSeekV4CompressedCache(model.*) and !model.isQwen35() and state.isPaged() and prefixStorageMatches(backend, binding, state, owner);
}

/// Cache hits become state-owned only after seeding succeeds. The temporary hit
/// is rolled back on cancellation or seed refusal, preserving the reusable entry.
pub fn attachPromptPrefix(backend: *const ComputeBackend, binding: ?*NativePrefixBinding, state: *DecodeState, model: *const Config, cache: ?*PrefixCache, request: PrefixRequest, tokens: []const i64, control: ?Control) !usize {
    const owner = cache orelse return 0;
    if (!prefixReusable(backend, binding, state, model, owner, request) or state.sequence_id != null) return 0;
    const page_size = owner.pageSize() orelse return 0;
    if (page_size == 0 or tokens.len <= page_size) return 0;
    if (binding) |native| if (owner.storagePtr() == null) {
        if (native.phase != .fresh or state.total_tokens != 0 or !emptyNativeMetadata(native.metadata, owner)) return 0;
    };
    if (control) |active| try active.check();
    const hit = (owner.attachLongestPrefix(request.namespace.?, tokens, tokens.len - page_size) catch |err| {
        std.log.warn("prompt cache lookup failed; continuing without reuse: {s}", .{@errorName(err)});
        return 0;
    }) orelse return 0;
    var seeded = false;
    defer if (!seeded) owner.releaseAttachedPrefix(hit);
    if (control) |active| try active.check();
    if (binding) |native| if (owner.storagePtr() == null) {
        if (!native.matches(backend, state, owner) or native.phase != .fresh or state.sequence_id != null or state.total_tokens != 0 or !emptyNativeMetadata(native.metadata, owner)) return 0;
    };
    // Native numerical attention owns its host payload in the shared manager.
    // The exact constructor-bound local metadata is unused before prefill.
    const previous_metadata = state.kv_storage;
    if (owner.storagePtr() == null) state.kv_storage = null;
    state.seedAttachedPrefix(hit.sequence_id, hit.token_count) catch |err| {
        state.kv_storage = previous_metadata;
        std.log.warn("prompt cache seed failed; continuing without reuse: {s}", .{@errorName(err)});
        return 0;
    };
    if (binding) |native| if (owner.storagePtr() == null) {
        native.phase = .manager_only;
    };
    seeded = true;
    return hit.token_count;
}

/// Insertion remains an optional acceleration after successful prefill. Task
/// semantics may prohibit sharing even when the storage would otherwise match.
pub fn storePromptPrefix(backend: *const ComputeBackend, binding: ?*const NativePrefixBinding, state: *const DecodeState, cache: ?*PrefixCache, request: PrefixRequest, tokens: []const i64) void {
    const owner = cache orelse return;
    if (!request.enabled or request.namespace == null or request.has_media or !prefixStorageMatches(backend, binding, state, owner)) return;
    const sequence = state.sequence_id orelse return;
    owner.storeFromSequence(request.namespace.?, tokens, sequence) catch |err| {
        std.log.warn("prompt cache store failed; continuing without insertion: {s}", .{@errorName(err)});
    };
}

/// Current executable/storage capabilities, supplied by the owning runtime.
/// No family-name inference or task configuration is part of physical routing.
pub const ActiveRoute = struct {
    backend: @import("../ops/ops.zig").BackendKind,
    compiled_backend: ?@import("../ops/ops.zig").BackendKind,
    whole_model: bool,
    has_graph: bool,
    dtype: ?pool.KvDType,
    pub fn storageRoute(self: ActiveRoute, state: *const DecodeState) Route {
        return if (self.compiled_backend == .metal and self.whole_model and self.has_graph and
            state.isPaged() and self.backend == .metal and self.dtype != null) .metal_whole_model else .standard;
    }
};

/// Task excludes the final sampled-but-unmaterialized token before calling.
/// Runtime retains only qualified current storage and the cache's radix policy.
pub fn storeGeneratedPromptPrefix(backend: *const ComputeBackend, binding: ?*const NativePrefixBinding, state: *const DecodeState, model: *const Config, cache: ?*PrefixCache, request: PrefixRequest, materialized_tokens: []const i64) void {
    const owner = cache orelse return;
    if (!prefixReusable(backend, binding, state, model, owner, request)) return;
    const sequence = state.sequence_id orelse return;
    owner.storeGeneratedPrefixFromSequence(request.namespace.?, materialized_tokens, sequence) catch |err| {
        std.log.warn("radix continuation cache store failed: {s}", .{@errorName(err)});
    };
}

const PrefixFixture = struct {
    const model = Config{ .hidden_size = 2, .num_hidden_layers = 1, .num_attention_heads = 1, .num_key_value_heads = 1, .attention_head_dim = 2 };
    fn populate(backend: *const ComputeBackend, cache: *PrefixCache) !void {
        cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });
        const ensured = (try cache.ensureStorage(.{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2 })).?;
        var state = DecodeState.initPaged(std.testing.allocator, cache.managerPtr(), cache.pool_id.?, null);
        state.kv_storage = ensured.storage;
        defer state.deinit();
        const sequence = try cache.manager.attachSequence(cache.pool_id.?);
        state.sequence_id = sequence;
        try cache.manager.appendTokens(sequence, 4);
        const stored_sequence = try ensured.storage.attachSequence(ensured.storage.poolId());
        try std.testing.expectEqual(sequence, stored_sequence);
        try ensured.storage.appendTokens(sequence, 4);
        state.total_tokens = 4;
        try ensured.storage.writeFullLayerKv(sequence, 0, 4, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 11, 12, 13, 14, 15, 16, 17, 18 });
        storePromptPrefix(backend, null, &state, cache, request, &.{ 1, 2, 3, 4 });
        try std.testing.expectEqual(@as(usize, 1), cache.stats().live_entries);
    }
    const request = PrefixRequest{ .enabled = true, .namespace = "tenant-a", .has_media = false, .speculation = false, .compaction = false, .compiled = false };
    fn activeSequences(cache: *PrefixCache) usize {
        var count: usize = 0;
        for (cache.manager.sequences.items) |sequence| if (sequence.active) {
            count += 1;
        };
        return count;
    }
};

test "decoder storage prefix runtime refuses unsupported model policy foreign storage and private namespace before attachment" {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(std.testing.allocator, .native, null);
    defer context.deinit(std.testing.allocator);
    const backend = context.computeBackend();
    var cache = PrefixCache.init(std.testing.allocator);
    defer cache.deinit();
    try PrefixFixture.populate(&backend, &cache);
    var state = DecodeState.initPaged(std.testing.allocator, cache.managerPtr(), cache.pool_id.?, null);
    state.kv_storage = cache.storagePtr();
    defer state.deinit();
    const tokens = [_]i64{ 1, 2, 3, 9 };
    const wrong_model = Config{ .family = .qwen3_5 };
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, null, &state, &wrong_model, &cache, PrefixFixture.request, &tokens, null));
    const model = PrefixFixture.model;
    const storage = state.kv_storage;
    state.kv_storage = null;
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, null, &state, &model, &cache, PrefixFixture.request, &tokens, null));
    state.kv_storage = storage;
    var foreign = PrefixCache.init(std.testing.allocator);
    defer foreign.deinit();
    try PrefixFixture.populate(&backend, &foreign);
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, null, &state, &model, &foreign, PrefixFixture.request, &tokens, null));
    var private_request = PrefixFixture.request;
    private_request.enabled = false;
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, null, &state, &model, &cache, private_request, &tokens, null));
    private_request = PrefixFixture.request;
    private_request.namespace = "tenant-b";
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, null, &state, &model, &cache, private_request, &tokens, null));
    try std.testing.expect(state.sequence_id == null);
    try std.testing.expectEqual(@as(usize, 0), PrefixFixture.activeSequences(&cache));
    try std.testing.expectEqual(@as(u64, 0), cache.stats().hits);
}

test "decoder storage prefix cancellation releases attached sequence and leaves cached prefix reusable" {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(std.testing.allocator, .native, null);
    defer context.deinit(std.testing.allocator);
    const backend = context.computeBackend();
    var cache = PrefixCache.init(std.testing.allocator);
    defer cache.deinit();
    try PrefixFixture.populate(&backend, &cache);
    var state = DecodeState.initPaged(std.testing.allocator, cache.managerPtr(), cache.pool_id.?, null);
    state.kv_storage = cache.storagePtr();
    defer state.deinit();
    const model = PrefixFixture.model;
    const Cancel = struct {
        cache: *PrefixCache,
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 2) {
                try std.testing.expectEqual(@as(usize, 1), PrefixFixture.activeSequences(self.cache));
                return error.Cancelled;
            }
        }
    };
    var cancel = Cancel{ .cache = &cache };
    try std.testing.expectError(error.Cancelled, attachPromptPrefix(&backend, null, &state, &model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, .{ .ptr = &cancel, .check_fn = Cancel.check }));
    try std.testing.expectEqual(@as(usize, 2), cancel.checks);
    try std.testing.expect(state.sequence_id == null and state.total_tokens == 0);
    try std.testing.expectEqual(@as(usize, 0), PrefixFixture.activeSequences(&cache));
    try std.testing.expectEqual(@as(usize, 1), cache.stats().live_entries);
    try std.testing.expectEqual(@as(usize, 2), try attachPromptPrefix(&backend, null, &state, &model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, null));
}

test "decoder storage seeded prefix retains actual Native KV payload after cache eviction" {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(std.testing.allocator, .native, null);
    defer context.deinit(std.testing.allocator);
    const backend = context.computeBackend();
    const a = std.testing.allocator;
    var cache = PrefixCache.init(a);
    defer cache.deinit();
    try PrefixFixture.populate(&backend, &cache);
    var state = DecodeState.initPaged(a, cache.managerPtr(), cache.pool_id.?, null);
    state.kv_storage = cache.storagePtr();
    defer state.deinit();
    const model = PrefixFixture.model;
    try std.testing.expectEqual(@as(usize, 2), try attachPromptPrefix(&backend, null, &state, &model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, null));
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 0 });
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);
    try std.testing.expectEqual(@as(usize, 2), state.total_tokens);
    try std.testing.expectEqual(@as(?usize, 2), cache.manager.tokenCount(state.sequence_id.?));
    const values = try state.kv_storage.?.gatherLayerKv(a, state.sequence_id.?, 0, 2);
    defer a.free(values.k);
    defer a.free(values.v);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values.k);
    try std.testing.expectEqualSlices(f32, &.{ 11, 12, 13, 14 }, values.v);
}

test "decoder selected request rejects unsupported physical routes and unwinds actual captured queries on preparation failure" {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(std.testing.allocator, .native, null);
    defer context.deinit(std.testing.allocator);
    const backend = context.computeBackend();
    try validateSelectedRoute(false, .native, false);
    try std.testing.expectError(error.UnsupportedKvCachePolicy, validateSelectedRoute(true, .cuda, false));
    try std.testing.expectError(error.UnsupportedKvCachePolicy, validateSelectedRoute(true, .metal, true));
    var state = DecodeState.initContiguous(std.testing.allocator);
    defer state.deinit();
    const model = PrefixFixture.model;
    var disabled = try SelectedRequest.begin(false, &backend, false, &state, &model, null, 0, false, &.{});
    disabled.deinit();
    state.selection_capture_enabled = true;
    try DecodeState.captureSelectionQueries(&state, 0, 0, 0, 1, 2, &.{ 3, 4 });
    try std.testing.expectEqual(@as(usize, 1), state.selection_queries.items.len);
    try std.testing.expectError(error.UnsupportedKvCachePolicy, SelectedRequest.begin(true, &backend, false, &state, &model, null, std.math.maxInt(usize), false, &.{}));
    try std.testing.expect(!state.selection_capture_enabled and state.selection_protected_spans.len == 0);
    try std.testing.expectEqual(@as(usize, 0), state.selection_queries.items.len);
    try std.testing.expectError(error.UnsupportedKvCachePolicy, SelectedRequest.begin(true, &backend, false, &state, &model, .f16, std.math.maxInt(usize), false, &.{}));
    try std.testing.expect(!state.selection_capture_enabled);
}

test "decoder storage physical route requires current backend executable and paged owner" {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(std.testing.allocator, .native, null);
    defer context.deinit(std.testing.allocator);
    const backend = context.computeBackend();
    var state = DecodeState.initContiguous(std.testing.allocator);
    defer state.deinit();
    var route = ActiveRoute{ .backend = .metal, .compiled_backend = .metal, .whole_model = true, .has_graph = true, .dtype = .f16 };
    try std.testing.expectEqual(Route.standard, route.storageRoute(&state));
    var cache = PrefixCache.init(std.testing.allocator);
    defer cache.deinit();
    try PrefixFixture.populate(&backend, &cache);
    var paged = DecodeState.initPaged(std.testing.allocator, cache.managerPtr(), cache.pool_id.?, null);
    paged.kv_storage = cache.storagePtr();
    defer paged.deinit();
    try std.testing.expectEqual(Route.metal_whole_model, route.storageRoute(&paged));
    route.backend = .native;
    try std.testing.expectEqual(Route.standard, route.storageRoute(&paged));
    route.backend = .metal;
    route.has_graph = false;
    try std.testing.expectEqual(Route.standard, route.storageRoute(&paged));
    route.has_graph = true;
    route.dtype = null;
    try std.testing.expectEqual(Route.standard, route.storageRoute(&paged));
}

test "decoder storage ordinary disabled policies perform no allocator requests" {
    var context = try @import("../backends/compute_context.zig").BackendContext.init(std.testing.allocator, .native, null);
    defer context.deinit(std.testing.allocator);
    const backend = context.computeBackend();
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var state = DecodeState.initContiguous(failing.allocator());
    defer state.deinit();
    var cache = PrefixCache.init(failing.allocator());
    defer cache.deinit();
    const model = PrefixFixture.model;
    var selected = try SelectedRequest.begin(false, &backend, false, &state, &model, null, 0, false, &.{});
    selected.deinit();
    var request = PrefixFixture.request;
    request.enabled = false;
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, null, &state, &model, &cache, request, &.{ 1, 2, 3, 4 }, null));
    storePromptPrefix(&backend, null, &state, &cache, request, &.{ 1, 2, 3, 4 });
    storeGeneratedPromptPrefix(&backend, null, &state, &model, &cache, request, &.{ 1, 2, 3, 4 });
    try std.testing.expectEqual(@as(usize, 0), failing.allocated_bytes);
    try std.testing.expect(!failing.has_induced_failure);
}

const NativePrefixFixture = struct {
    const config = pool.KvPoolConfig{ .backend = .native, .dtype = .f32, .page_size_tokens = 2, .num_layers_packed = 1, .num_kv_heads = 1, .head_dim = 2 };
    /// Mirrors both server constructors: ensurePool, request-local metadata,
    /// canonical Native backend, shared manager and matching request state.
    fn populate(cache: *PrefixCache) !void {
        const a = std.testing.allocator;
        var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
        defer context.deinit(a);
        const backend = context.computeBackend();
        cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 1 << 20 });
        const pool_id = (try cache.ensurePool(config)).?;
        var metadata = try MetadataStorage.init(a, config);
        defer metadata.deinit();
        var state = DecodeState.initPaged(a, cache.managerPtr(), pool_id, null);
        state.kv_storage = &metadata;
        defer state.deinit();
        var binding = NativePrefixBinding.init(&backend, cache, &state, &metadata).?;
        defer binding.deinit();
        var cursor = @import("../graph/decode_state_runtime.zig").BorrowedDecodeStateRuntime.init(&state);
        try cursor.notePrefill(4);
        // Actual Native attention writes this manager, not the local mirror.
        try cache.manager.writeFullLayerKv(state.sequence_id.?, 0, 4, &.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 11, 12, 13, 14, 15, 16, 17, 18 });
        storePromptPrefix(&backend, &binding, &state, cache, PrefixFixture.request, &.{ 1, 2, 3, 4 });
        try std.testing.expect(cache.storagePtr() == null);
        try std.testing.expectEqual(@as(usize, 1), cache.stats().live_entries);
        // State, its local metadata and actual source Native context all close.
    }
};

test "decoder Native prefix binding reuses server ensurePool payload after source close and cache eviction" {
    const a = std.testing.allocator;
    var cache = PrefixCache.init(a);
    defer cache.deinit();
    try NativePrefixFixture.populate(&cache);
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var metadata = try MetadataStorage.init(a, NativePrefixFixture.config);
    defer metadata.deinit();
    var state = DecodeState.initPaged(a, cache.managerPtr(), cache.pool_id.?, null);
    state.kv_storage = &metadata;
    defer state.deinit();
    var binding = NativePrefixBinding.init(&backend, &cache, &state, &metadata).?;
    defer binding.deinit();
    try std.testing.expectEqual(@as(usize, 2), try attachPromptPrefix(&backend, &binding, &state, &PrefixFixture.model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, null));
    try std.testing.expect(state.kv_storage == null and binding.phase == .manager_only);
    try std.testing.expectEqual(@as(usize, 0), metadata.storage.liveBlocks());
    cache.configure(.{ .enabled = true, .mode = .simple, .min_tokens = 2, .max_bytes = 0 });
    try std.testing.expectEqual(@as(usize, 0), cache.stats().live_entries);
    const values = try cache.manager.gatherLayerKv(a, state.sequence_id.?, 0, 2);
    defer a.free(values.k);
    defer a.free(values.v);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values.k);
    try std.testing.expectEqualSlices(f32, &.{ 11, 12, 13, 14 }, values.v);
    // Continue through the same runtime with no duplicate local KV store.
    var cursor = @import("../graph/decode_state_runtime.zig").BorrowedDecodeStateRuntime.init(&state);
    try cursor.appendPrefillChunk(2);
    try cache.manager.writeLayerKvSuffix(state.sequence_id.?, 0, 4, 2, &.{ 9, 10, 11, 12 }, &.{ 19, 20, 21, 22 });
    try std.testing.expectEqual(@as(usize, 0), metadata.storage.liveBlocks());
    try std.testing.expectEqual(@as(usize, 4), state.total_tokens);
}

test "decoder Native prefix cancellation preserves exact request metadata and rejected foreign or closed bindings never attach" {
    const a = std.testing.allocator;
    var cache = PrefixCache.init(a);
    defer cache.deinit();
    try NativePrefixFixture.populate(&cache);
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var metadata = try MetadataStorage.init(a, NativePrefixFixture.config);
    defer metadata.deinit();
    var foreign = try MetadataStorage.init(a, NativePrefixFixture.config);
    defer foreign.deinit();
    var state = DecodeState.initPaged(a, cache.managerPtr(), cache.pool_id.?, null);
    state.kv_storage = &metadata;
    defer state.deinit();
    var binding = NativePrefixBinding.init(&backend, &cache, &state, &metadata).?;
    defer binding.deinit();
    state.kv_storage = &foreign;
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, &binding, &state, &PrefixFixture.model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, null));
    try std.testing.expect(state.kv_storage == &foreign);
    state.kv_storage = &metadata;
    const original_pool = state.pool_id;
    state.pool_id = original_pool.? + 1;
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, &binding, &state, &PrefixFixture.model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, null));
    state.pool_id = original_pool;
    var copied_table = backend.vtable.*;
    std.mem.doNotOptimizeAway(&copied_table);
    try std.testing.expect(@intFromPtr(&copied_table) != @intFromPtr(backend.vtable));
    var forged = backend;
    forged.vtable = &copied_table;
    try std.testing.expect(NativePrefixBinding.init(&forged, &cache, &state, &metadata) == null);
    const Cancel = struct {
        checks: usize = 0,
        fn check(raw: ?*anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.checks += 1;
            if (self.checks == 2) return error.Cancelled;
        }
    };
    var cancel = Cancel{};
    try std.testing.expectError(error.Cancelled, attachPromptPrefix(&backend, &binding, &state, &PrefixFixture.model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, .{ .ptr = &cancel, .check_fn = Cancel.check }));
    try std.testing.expect(state.kv_storage == &metadata and state.sequence_id == null and state.total_tokens == 0 and binding.phase == .fresh);
    try std.testing.expectEqual(@as(usize, 0), PrefixFixture.activeSequences(&cache));
    binding.deinit();
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, &binding, &state, &PrefixFixture.model, &cache, PrefixFixture.request, &.{ 1, 2, 3, 9 }, null));
    try std.testing.expect(state.kv_storage == &metadata);
}

test "decoder Native populated cache disabled request makes no allocator requests and no hit" {
    const a = std.testing.allocator;
    var cache = PrefixCache.init(a);
    defer cache.deinit();
    try NativePrefixFixture.populate(&cache);
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const backend = context.computeBackend();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var state = DecodeState.initPaged(failing.allocator(), cache.managerPtr(), cache.pool_id.?, null);
    defer state.deinit();
    var request = PrefixFixture.request;
    request.enabled = false;
    var selected = try SelectedRequest.begin(false, &backend, false, &state, &PrefixFixture.model, null, 0, false, &.{});
    selected.deinit();
    try std.testing.expectEqual(@as(usize, 0), try attachPromptPrefix(&backend, null, &state, &PrefixFixture.model, &cache, request, &.{ 1, 2, 3, 9 }, null));
    storePromptPrefix(&backend, null, &state, &cache, request, &.{ 1, 2, 3, 9 });
    storeGeneratedPromptPrefix(&backend, null, &state, &PrefixFixture.model, &cache, request, &.{ 1, 2, 3, 9 });
    try std.testing.expectEqual(@as(usize, 0), failing.allocated_bytes);
    try std.testing.expect(!failing.has_induced_failure);
    try std.testing.expectEqual(@as(u64, 0), cache.stats().hits);
    try std.testing.expectEqual(@as(usize, 1), cache.stats().live_entries);
}
