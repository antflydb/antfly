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
const safetensors_mod = @import("../models/safetensors.zig");
const native_linalg = @import("../backends/native.zig");
const tier_planner = runtime.tier.planner;
const tier_cache_mod = runtime.tier.cache;
const prefetch_mod = runtime.tier.prefetch;
const tier_shared_mod = runtime.tier.shared;
const moe_residency = runtime.moe.residency;
const supports_native_metal_provider = build_options.enable_metal;
const metal_native_provider_mod = if (supports_native_metal_provider) @import("../backends/metal_native_provider.zig") else struct {
    pub const MetalNativeProvider = void;
};

const QuantizedStorage = weight_source_mod.QuantizedStorage;
const LoadedWeight = weight_source_mod.LoadedWeight;
const Tensor = @import("../backends/tensor.zig").Tensor;
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

pub const JinaLoraAdapter = struct {
    allocator: std.mem.Allocator,
    reader: safetensors_mod.MMapReader,
    scale: f32,

    pub fn create(allocator: std.mem.Allocator, adapter_weights_path: []const u8, scale: f32) !*JinaLoraAdapter {
        const self = try allocator.create(JinaLoraAdapter);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .reader = try safetensors_mod.MMapReader.openFileAbsolute(allocator, adapter_weights_path),
            .scale = scale,
        };
        return self;
    }

    pub fn destroy(self: *JinaLoraAdapter) void {
        const allocator = self.allocator;
        self.reader.deinit();
        allocator.destroy(self);
    }

    pub fn mergeIntoLoadedWeight(self: *const JinaLoraAdapter, base_name: []const u8, loaded: *LoadedWeight) !void {
        if (!std.mem.endsWith(u8, base_name, ".weight")) return;

        const adapter_a_name = try jinaAdapterANameForBaseWeight(self.allocator, base_name);
        defer self.allocator.free(adapter_a_name);
        if (!self.reader.header.tensors.contains(adapter_a_name)) return;

        const adapter_b_name = try jinaAdapterBNameForAdapterA(self.allocator, adapter_a_name);
        defer self.allocator.free(adapter_b_name);
        if (!self.reader.header.tensors.contains(adapter_b_name)) return error.IncompleteJinaV5Adapter;

        try ensureLoadedWeightIsOwnedF32(self.allocator, loaded);

        var adapter_a = try readSafetensorAsF32(self.allocator, &self.reader, adapter_a_name);
        defer adapter_a.deinit();
        var adapter_b = try readSafetensorAsF32(self.allocator, &self.reader, adapter_b_name);
        defer adapter_b.deinit();

        try mergeLoraPairIntoWeight(loaded, adapter_a.tensor, adapter_b.tensor, self.scale);
    }
};

fn jinaAdapterANameForBaseWeight(allocator: std.mem.Allocator, base_name: []const u8) ![]const u8 {
    const suffix = ".weight";
    if (!std.mem.endsWith(u8, base_name, suffix)) return error.InvalidAdapterTensorName;
    return try std.fmt.allocPrint(allocator, "base_model.model.{s}.lora_A.weight", .{base_name[0 .. base_name.len - suffix.len]});
}

fn jinaAdapterBNameForAdapterA(allocator: std.mem.Allocator, adapter_a_name: []const u8) ![]const u8 {
    const suffix = ".lora_A.weight";
    if (!std.mem.endsWith(u8, adapter_a_name, suffix)) return error.InvalidAdapterTensorName;
    return try std.fmt.allocPrint(allocator, "{s}.lora_B.weight", .{adapter_a_name[0 .. adapter_a_name.len - suffix.len]});
}

fn readSafetensorAsF32(
    allocator: std.mem.Allocator,
    reader: *const safetensors_mod.MMapReader,
    name: []const u8,
) !LoadedWeight {
    var tensor = try reader.readTensor(name);
    if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
        const converted = try weight_source_mod.convertToF32(allocator, &tensor);
        tensor.deinit();
        tensor = converted;
    } else if (tensor.dtype == .f32 and !tensor.owns_data) {
        const converted = try Tensor.initFloat32(allocator, tensor.name, tensor.shape, tensor.asFloat32());
        tensor.deinit();
        tensor = converted;
    }
    if (tensor.dtype != .f32) {
        tensor.deinit();
        return error.UnsupportedAdapterTensorType;
    }
    return .{ .tensor = tensor, .quantized = false };
}

fn ensureLoadedWeightIsOwnedF32(allocator: std.mem.Allocator, loaded: *LoadedWeight) !void {
    if (loaded.quantized_storage) |*storage| {
        storage.deinit();
        loaded.quantized_storage = null;
        loaded.quantized = false;
    }

    if (loaded.tensor.dtype == .f32 and loaded.tensor.owns_data) return;

    const converted = switch (loaded.tensor.dtype) {
        .f16, .bf16 => try weight_source_mod.convertToF32(allocator, &loaded.tensor),
        .f32 => try Tensor.initFloat32(allocator, loaded.tensor.name, loaded.tensor.shape, loaded.tensor.asFloat32()),
        else => return error.UnsupportedJinaV5AdapterBaseWeight,
    };
    loaded.tensor.deinit();
    loaded.tensor = converted;
}

fn mergeLoraPairIntoWeight(base_weight: *LoadedWeight, adapter_a: Tensor, adapter_b: Tensor, scale: f32) !void {
    if (base_weight.quantized or base_weight.tensor.dtype != .f32) return error.UnsupportedJinaV5AdapterBaseWeight;
    if (base_weight.tensor.shape.len != 2 or adapter_a.shape.len != 2 or adapter_b.shape.len != 2) return error.InvalidAdapterTensorShape;

    const out_dim: usize = @intCast(base_weight.tensor.shape[0]);
    const in_dim: usize = @intCast(base_weight.tensor.shape[1]);
    const rank: usize = @intCast(adapter_a.shape[0]);
    if (rank == 0) return error.InvalidAdapterTensorShape;
    if (@as(usize, @intCast(adapter_a.shape[1])) != in_dim) return error.AdapterInputDimMismatch;
    if (@as(usize, @intCast(adapter_b.shape[0])) != out_dim) return error.AdapterOutputDimMismatch;
    if (@as(usize, @intCast(adapter_b.shape[1])) != rank) return error.AdapterRankMismatch;

    native_linalg.sgemmSync(
        out_dim,
        in_dim,
        rank,
        scale,
        adapter_b.asFloat32(),
        adapter_a.asFloat32(),
        1.0,
        base_weight.tensor.asFloat32Mut(),
    );
}

pub const WeightStore = struct {
    allocator: std.mem.Allocator,
    resident_weight_estimate_bytes: usize = 0,
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
    prefer_f32_dense_tensors: bool = false,
    mirror_kv_to_manager: bool = true,
    access_epoch: u64 = 1,
    packed_expert_views: std.StringHashMapUnmanaged(PackedExpertViewEntry) = .empty,
    packed_expert_view_bytes: usize = 0,
    shared_metal_native_provider: if (supports_native_metal_provider) ?*metal_native_provider_mod.MetalNativeProvider else void =
        if (supports_native_metal_provider) null else {},
    shared_metal_native_provider_lock: if (supports_native_metal_provider) std.Io.Mutex else void =
        if (supports_native_metal_provider) .init else {},
    jina_lora_adapter: ?*JinaLoraAdapter = null,
};

pub fn touchLazyWeight(data: *WeightStore, entry: *LazyWeightEntry) void {
    entry.last_access_epoch = data.access_epoch;
    data.access_epoch +|= 1;
}

/// Simple prefetch callback suitable for backends that don't do their own
/// Simple prefetch callback for Metal-hosted weights. Runs the synchronous host
/// load and resets the pending flag.
pub fn simplePrefetchProcess(ctx: *anyopaque, entry: *LazyWeightEntry) void {
    const data: *WeightStore = @ptrCast(@alignCast(ctx));
    entry.pending_prefetch = false;
    ensureHostLazyWeightLoadedSimple(data, entry) catch {};
}

pub fn simplePrefetchPriority(entry: *LazyWeightEntry) u64 {
    return entry.prefetch_score;
}

/// Install a prefetch queue on the store using caller-supplied callbacks.
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
    if (data.jina_lora_adapter) |adapter| {
        try adapter.mergeIntoLoadedWeight(entry.tensor_ref.name, &entry.host_loaded.?);
    }
    entry.loaded_bytes = entry.host_loaded.?.tensor.data.len;
    entry.active_tier = if (entry.loaded_bytes == 0) .disk else .host;
    if (entry.loaded_bytes != 0) {
        if (data.tier_cache) |*tier_cache| tier_cache.noteResident(.host, entry.loaded_bytes);
    }
}

test "jina adapter names map base weight to PEFT LoRA tensors" {
    const allocator = std.testing.allocator;
    const adapter_a = try jinaAdapterANameForBaseWeight(allocator, "layers.0.self_attn.q_proj.weight");
    defer allocator.free(adapter_a);
    try std.testing.expectEqualStrings("base_model.model.layers.0.self_attn.q_proj.lora_A.weight", adapter_a);

    const adapter_b = try jinaAdapterBNameForAdapterA(allocator, adapter_a);
    defer allocator.free(adapter_b);
    try std.testing.expectEqualStrings("base_model.model.layers.0.self_attn.q_proj.lora_B.weight", adapter_b);
}

test "jina adapter merge ignores non-weight tensor names" {
    const allocator = std.testing.allocator;
    const adapter_path = try std.fmt.allocPrint(allocator, "/tmp/termite_jina_lora_empty_{d}.safetensors", .{std.posix.system.getpid()});
    defer allocator.free(adapter_path);
    defer std.Io.Dir.deleteFileAbsolute(std.testing.io, adapter_path) catch {};

    const raw_header = "{}";
    const header_len = std.mem.alignForward(usize, raw_header.len, 8);
    const file_bytes = try allocator.alloc(u8, 8 + header_len);
    defer allocator.free(file_bytes);
    std.mem.writeInt(u64, file_bytes[0..8], header_len, .little);
    @memcpy(file_bytes[8 .. 8 + raw_header.len], raw_header);
    @memset(file_bytes[8 + raw_header.len ..], ' ');
    {
        var file = try std.Io.Dir.createFileAbsolute(std.testing.io, adapter_path, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, file_bytes);
    }

    var adapter = try JinaLoraAdapter.create(allocator, adapter_path, 1.0);
    defer adapter.destroy();

    const shape = [_]i64{1};
    const values = [_]f32{42.0};
    var base_weight = LoadedWeight{
        .tensor = try Tensor.initFloat32(allocator, "rotary_emb.inv_freq", &shape, &values),
    };
    defer base_weight.deinit();

    try adapter.mergeIntoLoadedWeight("rotary_emb.inv_freq", &base_weight);
    try std.testing.expectEqual(@as(f32, 42.0), base_weight.tensor.asFloat32()[0]);
}

test "gpu hosted jina lora merge applies scaled adapter update" {
    const allocator = std.testing.allocator;
    const base_shape = [_]i64{ 2, 3 };
    const a_shape = [_]i64{ 2, 3 };
    const b_shape = [_]i64{ 2, 2 };
    const base_values = [_]f32{
        1.0, 2.0, 3.0,
        4.0, 5.0, 6.0,
    };
    const a_values = [_]f32{
        1.0, 0.0, 2.0,
        0.0, 3.0, 1.0,
    };
    const b_values = [_]f32{
        2.0, 1.0,
        0.0, 4.0,
    };

    var base_weight = LoadedWeight{
        .tensor = try Tensor.initFloat32(allocator, "base", &base_shape, &base_values),
    };
    defer base_weight.deinit();
    var adapter_a = try Tensor.initFloat32(allocator, "a", &a_shape, &a_values);
    defer adapter_a.deinit();
    var adapter_b = try Tensor.initFloat32(allocator, "b", &b_shape, &b_values);
    defer adapter_b.deinit();

    try mergeLoraPairIntoWeight(&base_weight, adapter_a, adapter_b, 0.5);

    try std.testing.expectEqualSlices(f32, &.{
        2.0, 3.5,  5.5,
        4.0, 11.0, 8.0,
    }, base_weight.tensor.asFloat32());
}

test "jina lora adapter merges matching safetensors sidecar into loaded base weight" {
    const allocator = std.testing.allocator;
    const adapter_path = try std.fmt.allocPrint(allocator, "/tmp/termite_jina_lora_adapter_{d}.safetensors", .{std.posix.system.getpid()});
    defer allocator.free(adapter_path);
    defer std.Io.Dir.deleteFileAbsolute(std.testing.io, adapter_path) catch {};

    const a_values = [_]f32{
        1.0, 0.0, 2.0,
        0.0, 3.0, 1.0,
    };
    const b_values = [_]f32{
        2.0, 1.0,
        0.0, 4.0,
    };
    const a_bytes = std.mem.sliceAsBytes(&a_values);
    const b_bytes = std.mem.sliceAsBytes(&b_values);
    const raw_header = try std.fmt.allocPrint(
        allocator,
        "{{\"base_model.model.layers.0.self_attn.q_proj.lora_A.weight\":{{\"dtype\":\"F32\",\"shape\":[2,3],\"data_offsets\":[0,{d}]}},\"base_model.model.layers.0.self_attn.q_proj.lora_B.weight\":{{\"dtype\":\"F32\",\"shape\":[2,2],\"data_offsets\":[{d},{d}]}}}}",
        .{ a_bytes.len, a_bytes.len, a_bytes.len + b_bytes.len },
    );
    defer allocator.free(raw_header);
    const header_len = std.mem.alignForward(usize, raw_header.len, 8);
    const file_bytes = try allocator.alloc(u8, 8 + header_len + a_bytes.len + b_bytes.len);
    defer allocator.free(file_bytes);
    std.mem.writeInt(u64, file_bytes[0..8], header_len, .little);
    @memcpy(file_bytes[8 .. 8 + raw_header.len], raw_header);
    @memset(file_bytes[8 + raw_header.len .. 8 + header_len], ' ');
    @memcpy(file_bytes[8 + header_len .. 8 + header_len + a_bytes.len], a_bytes);
    @memcpy(file_bytes[8 + header_len + a_bytes.len ..], b_bytes);

    {
        var file = try std.Io.Dir.createFileAbsolute(std.testing.io, adapter_path, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, file_bytes);
    }

    var adapter = try JinaLoraAdapter.create(allocator, adapter_path, 0.5);
    defer adapter.destroy();

    const base_shape = [_]i64{ 2, 3 };
    const base_values = [_]f32{
        1.0, 2.0, 3.0,
        4.0, 5.0, 6.0,
    };
    var base_weight = LoadedWeight{
        .tensor = try Tensor.initFloat32(allocator, "layers.0.self_attn.q_proj.weight", &base_shape, &base_values),
    };
    defer base_weight.deinit();

    try adapter.mergeIntoLoadedWeight("layers.0.self_attn.q_proj.weight", &base_weight);

    try std.testing.expectEqualSlices(f32, &.{
        2.0, 3.5,  5.5,
        4.0, 11.0, 8.0,
    }, base_weight.tensor.asFloat32());
}
