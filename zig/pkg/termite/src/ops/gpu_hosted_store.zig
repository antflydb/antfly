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

const std = @import("std");
const build_options = @import("build_options");
const runtime = @import("../runtime/root.zig");
const tensor_store_mod = @import("../models/tensor_store.zig");
const weight_source_mod = @import("../models/weight_source.zig");
const tier_planner = runtime.tier.planner;
const tier_cache_mod = runtime.tier.cache;
const prefetch_mod = runtime.tier.prefetch;
const tier_shared_mod = runtime.tier.shared;
const moe_residency = runtime.moe.residency;
const supports_native_metal_provider = build_options.enable_metal;
const metal_native_provider_mod = if (supports_native_metal_provider) @import("../backends/metal_native_provider.zig") else struct {
    pub const MetalNativeProvider = void;
};

// MLX interop is only compiled under `-Dmlx=true`. Under `-Dmlx=false`, the
// stub provides enough shape for optional MLX-typed fields to exist as `void`.
const mlx = if (build_options.enable_mlx) @import("../backends/mlx.zig") else struct {
    pub const c = struct {
        pub const mlx_array = *anyopaque;
        pub const mlx_map_string_to_array = void;
        pub const mlx_stream = void;
    };
};
const mlx_quant = if (build_options.enable_mlx) @import("../backends/mlx_quant.zig") else struct {
    pub const Provider = void;
    pub fn nullProvider() void {
        return {};
    }
};

const c = mlx.c;
const QuantizedStorage = weight_source_mod.QuantizedStorage;
const ExpertCoord = moe_residency.ExpertCoord;
const ResidencyTier = tier_planner.ResidencyTier;
const PlacementPlan = tier_planner.PlacementPlan;

pub const QuantExecutionMode = enum {
    prefer_backend_dense,
    wrapper_direct_quant,
    device_native,
};

pub const PackedExpertViewEntry = struct {
    bytes: []const u8,
    owned_copy: ?[]const u8 = null,
    last_access_epoch: u64 = 0,
    pin_count: usize = 0,

    pub fn deinit(self: *PackedExpertViewEntry) void {
        if (self.owned_copy) |bytes| std.heap.c_allocator.free(@constCast(bytes));
    }
};

pub const LazyWeightEntry = struct {
    tensor_ref: tensor_store_mod.LazyTensorRef,
    quantized_storage: ?QuantizedStorage = null,
    host_loaded: ?@import("../models/weight_source.zig").LoadedWeight = null,
    loaded: if (build_options.enable_mlx) ?c.mlx_array else void =
        if (build_options.enable_mlx) null else {},
    loaded_quantized: if (build_options.enable_mlx) ?c.mlx_array else void =
        if (build_options.enable_mlx) null else {},
    loaded_transposed: if (build_options.enable_mlx) ?c.mlx_array else void =
        if (build_options.enable_mlx) null else {},
    expert_coord: ?ExpertCoord = null,
    projection_mask: u8 = 0,
    loaded_bytes: usize = 0,
    backend_loaded_bytes: usize = 0,
    pin_count: usize = 0,
    pending_prefetch: bool = false,
    prefetch_score: u64 = 0,
    guard: ?*std.atomic.Mutex = null,
    placement: PlacementPlan = .{
        .class = .other,
        .preferred_tier = .host,
        .spill_tier = .disk,
    },
    prefer_dense: bool = false,
    active_tier: ResidencyTier = .disk,
    last_access_epoch: u64 = 0,
};

pub const PrefetchQueue = prefetch_mod.Queue(*LazyWeightEntry);

pub const WeightStore = struct {
    allocator: std.mem.Allocator,
    resident_weights: if (build_options.enable_mlx) c.mlx_map_string_to_array else void,
    resident_transposed_weights: if (build_options.enable_mlx)
        std.StringHashMapUnmanaged(c.mlx_array)
    else
        void = if (build_options.enable_mlx) .empty else {},
    resident_weight_estimate_bytes: usize = 0,
    stream: if (build_options.enable_mlx) c.mlx_stream else void,
    prefix: []const u8,
    lazy_weights: std.StringHashMapUnmanaged(LazyWeightEntry),
    prefetch: PrefetchQueue = undefined,
    prefetch_initialized: bool = false,
    tensor_store: ?tensor_store_mod.TensorStore = null,
    moe_num_experts: usize = 0,
    residency: ?moe_residency.SharedResidency = null,
    tier_cache: ?tier_cache_mod.SharedCache = null,
    shared_prefetch: ?*tier_shared_mod.SharedPrefetchState = null,
    allow_direct_quant: bool = true,
    quant_execution_mode: QuantExecutionMode = .prefer_backend_dense,
    native_quant: mlx_quant.Provider = mlx_quant.nullProvider(),
    prefer_f32_dense_tensors: bool = false,
    mirror_kv_to_manager: bool = true,
    access_epoch: u64 = 1,
    packed_expert_views: std.StringHashMapUnmanaged(PackedExpertViewEntry) = .empty,
    packed_expert_view_bytes: usize = 0,
    shared_metal_native_provider: if (supports_native_metal_provider) ?*metal_native_provider_mod.MetalNativeProvider else void =
        if (supports_native_metal_provider) null else {},
    shared_metal_native_provider_lock: if (supports_native_metal_provider) std.Io.Mutex else void =
        if (supports_native_metal_provider) .init else {},
};

pub fn touchLazyWeight(data: *WeightStore, entry: *LazyWeightEntry) void {
    entry.last_access_epoch = data.access_epoch;
    data.access_epoch +|= 1;
}

/// Simple prefetch callback suitable for backends that don't do their own
/// background staging (e.g. the MLX-free Metal path). Just runs the
/// synchronous host load and resets the pending flag.
pub fn simplePrefetchProcess(ctx: *anyopaque, entry: *LazyWeightEntry) void {
    const data: *WeightStore = @ptrCast(@alignCast(ctx));
    entry.pending_prefetch = false;
    ensureHostLazyWeightLoadedSimple(data, entry) catch {};
}

pub fn simplePrefetchPriority(entry: *LazyWeightEntry) u64 {
    return entry.prefetch_score;
}

/// Install a prefetch queue on the store using caller-supplied callbacks.
/// The existing MLX-coupled `initPrefetchQueue` in mlx_compute.zig delegates
/// to this so the queue machinery itself stays MLX-agnostic.
pub fn installPrefetchQueue(
    data: *WeightStore,
    allocator: std.mem.Allocator,
    process_fn: *const fn (ctx: *anyopaque, entry: *LazyWeightEntry) void,
    priority_fn: *const fn (entry: *LazyWeightEntry) u64,
) void {
    if (data.prefetch_initialized) return;
    data.prefetch = PrefetchQueue.initWithPriority(allocator, data, process_fn, priority_fn);
    data.prefetch_initialized = true;
    var it = data.lazy_weights.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.guard = data.prefetch.mutexPtr();
    }
}

pub fn startPrefetchWorker(data: *WeightStore) !void {
    try data.prefetch.start();
}

pub fn stopPrefetchWorker(data: *WeightStore) void {
    data.prefetch.stop();
}

pub fn deinitPrefetchQueue(data: *WeightStore) void {
    if (!data.prefetch_initialized) return;
    data.prefetch.deinit();
    data.prefetch_initialized = false;
}

pub fn deinitPackedExpertViews(data: *WeightStore, allocator: std.mem.Allocator) void {
    var it = data.packed_expert_views.iterator();
    while (it.next()) |entry| {
        var view = entry.value_ptr.*;
        view.deinit();
        allocator.free(entry.key_ptr.*);
    }
    data.packed_expert_views.deinit(allocator);
    data.packed_expert_view_bytes = 0;
}

pub fn ensureHostLazyWeightLoadedSimple(data: *WeightStore, entry: *LazyWeightEntry) !void {
    if (entry.expert_coord) |coord| {
        if (data.residency) |*residency| {
            try residency.noteTouch(coord, data.moe_num_experts);
        }
    }
    if (entry.host_loaded != null) return;
    if (entry.quantized_storage != null and !entry.prefer_dense) return;

    const tensor_store = data.tensor_store orelse return error.MissingWeight;
    if (data.allow_direct_quant and !entry.prefer_dense) {
        if (try tensor_store.loadQuantizedStorageRef(&entry.tensor_ref)) |loaded_storage| {
            entry.loaded_bytes = loaded_storage.raw_bytes.len + loaded_storage.prepared.ownedBytes();
            entry.quantized_storage = loaded_storage;
            entry.active_tier = .host;
            if (entry.loaded_bytes != 0) {
                if (data.tier_cache) |*tier_cache| tier_cache.noteResident(.host, entry.loaded_bytes);
            }
            return;
        }
    }
    if (data.allow_direct_quant and entry.prefer_dense and entry.quantized_storage == null) {
        if (try tensor_store.loadQuantizedStorageRef(&entry.tensor_ref)) |loaded_storage| {
            entry.quantized_storage = loaded_storage;
        }
    }

    entry.host_loaded = try tensor_store.loadTensorRef(&entry.tensor_ref);
    if (entry.host_loaded.?.quantized_storage) |*storage| {
        storage.deinit();
        entry.host_loaded.?.quantized_storage = null;
        entry.host_loaded.?.quantized = false;
    }
    entry.loaded_bytes = entry.host_loaded.?.tensor.data.len;
    entry.active_tier = if (entry.loaded_bytes == 0) .disk else .host;
    if (entry.loaded_bytes != 0) {
        if (data.tier_cache) |*tier_cache| tier_cache.noteResident(.host, entry.loaded_bytes);
    }
}
