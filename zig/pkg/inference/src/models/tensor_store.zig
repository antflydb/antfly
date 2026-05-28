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

// Storage-agnostic tensor store abstraction.
//
// This is the next layer above WeightSource: model/session code can ask what
// kind of store backs a model directory, retrieve GGUF metadata if present,
// and obtain a WeightSource for eager dense backends when supported.

const std = @import("std");
const builtin = @import("builtin");
const compat = @import("../io/compat.zig");
const manifest_mod = @import("manifest.zig");
const weight_source_mod = @import("weight_source.zig");
const gguf_mod = @import("../gguf/root.zig");
const c_file = if (builtin.os.tag == .freestanding) struct {
    pub const MmapRegion = struct {
        data: []u8 = &.{},

        pub fn init(_: std.mem.Allocator, _: []const u8) !MmapRegion {
            return error.UnsupportedOnFreestanding;
        }

        pub fn deinit(_: *MmapRegion) void {}

        pub fn adviseSequentialPrefix(_: *MmapRegion, _: usize) void {}
    };

    pub fn readRegion(_: std.mem.Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
        return error.UnsupportedOnFreestanding;
    }
} else @import("../util/c_file.zig");

pub const StoreKind = enum {
    safetensors,
    gguf,
};

pub const LazyTensorRef = struct {
    name: []const u8,
    source_name: ?[]const u8 = null,
    byte_len: usize = 0,
    quantized: bool = false,
    packed_expert_index: ?u32 = null,
    packed_expert_count: u32 = 0,
    fused_gate_up: bool = false,
    /// For fused gate+up tensors: 0 = first half (w1/gate), 1 = second half (w3/up).
    fused_gate_up_index: u8 = 0,

    pub fn deinit(self: *LazyTensorRef, allocator: std.mem.Allocator) void {
        allocator.free(self.name);
        if (self.source_name) |source_name| allocator.free(source_name);
        self.* = .{
            .name = &.{},
            .source_name = null,
            .byte_len = 0,
            .quantized = false,
            .packed_expert_index = null,
            .packed_expert_count = 0,
        };
    }
};

pub const TensorStore = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        kind: *const fn (*anyopaque) StoreKind,
        weightSource: *const fn (*anyopaque) anyerror!?weight_source_mod.WeightSource,
        describeTensor: *const fn (*anyopaque, allocator: std.mem.Allocator, name: []const u8) anyerror!LazyTensorRef,
        loadTensorRef: *const fn (*anyopaque, tensor_ref: *const LazyTensorRef) anyerror!weight_source_mod.LoadedWeight,
        loadQuantizedStorageRef: *const fn (*anyopaque, tensor_ref: *const LazyTensorRef) anyerror!?weight_source_mod.QuantizedStorage,
        ggufFile: *const fn (*anyopaque) ?*const gguf_mod.format.File,
        deinit: *const fn (*anyopaque) void,
    };

    pub fn kind(self: TensorStore) StoreKind {
        return self.vtable.kind(self.ptr);
    }

    pub fn weightSource(self: TensorStore) !?weight_source_mod.WeightSource {
        return self.vtable.weightSource(self.ptr);
    }

    pub fn describeTensor(self: TensorStore, allocator: std.mem.Allocator, name: []const u8) !LazyTensorRef {
        return self.vtable.describeTensor(self.ptr, allocator, name);
    }

    pub fn loadTensorRef(self: TensorStore, tensor_ref: *const LazyTensorRef) !weight_source_mod.LoadedWeight {
        return self.vtable.loadTensorRef(self.ptr, tensor_ref);
    }

    pub fn loadQuantizedStorageRef(self: TensorStore, tensor_ref: *const LazyTensorRef) !?weight_source_mod.QuantizedStorage {
        return self.vtable.loadQuantizedStorageRef(self.ptr, tensor_ref);
    }

    pub fn ggufFile(self: TensorStore) ?*const gguf_mod.format.File {
        return self.vtable.ggufFile(self.ptr);
    }

    pub fn deinit(self: TensorStore) void {
        self.vtable.deinit(self.ptr);
    }
};

pub const SafetensorsStore = struct {
    source: *weight_source_mod.SafetensorsSource,

    const vtable = TensorStore.VTable{
        .kind = @ptrCast(&kindImpl),
        .weightSource = @ptrCast(&weightSourceImpl),
        .describeTensor = @ptrCast(&describeTensorImpl),
        .loadTensorRef = @ptrCast(&loadTensorRefImpl),
        .loadQuantizedStorageRef = @ptrCast(&loadQuantizedStorageRefImpl),
        .ggufFile = @ptrCast(&ggufFileImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initAbsolute(allocator: std.mem.Allocator, path: []const u8) !*SafetensorsStore {
        const self = try allocator.create(SafetensorsStore);
        errdefer allocator.destroy(self);
        self.* = .{
            .source = try weight_source_mod.SafetensorsSource.initAbsolute(allocator, path),
        };
        return self;
    }

    pub fn tensorStore(self: *SafetensorsStore) TensorStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn kindImpl(_: *SafetensorsStore) StoreKind {
        return .safetensors;
    }

    fn weightSourceImpl(self: *SafetensorsStore) !?weight_source_mod.WeightSource {
        return self.source.weightSource();
    }

    fn describeTensorImpl(self: *SafetensorsStore, allocator: std.mem.Allocator, name: []const u8) !LazyTensorRef {
        const meta = self.source.reader.header.tensors.get(name) orelse return error.TensorNotFound;
        return .{
            .name = try allocator.dupe(u8, name),
            .byte_len = @intCast(meta.data_end - meta.data_start),
            .quantized = false,
        };
    }

    fn loadTensorRefImpl(self: *SafetensorsStore, tensor_ref: *const LazyTensorRef) !weight_source_mod.LoadedWeight {
        return .{
            .tensor = try self.source.reader.readTensor(tensor_ref.name),
            .quantized = false,
        };
    }

    fn loadQuantizedStorageRefImpl(_: *SafetensorsStore, _: *const LazyTensorRef) !?weight_source_mod.QuantizedStorage {
        return null;
    }

    fn ggufFileImpl(_: *SafetensorsStore) ?*const gguf_mod.format.File {
        return null;
    }

    fn deinitSelf(self: *SafetensorsStore) void {
        const allocator = self.source.reader.allocator;
        self.source.weightSource().deinit();
        allocator.destroy(self);
    }
};

pub const ShardedSafetensorsStore = struct {
    source: *weight_source_mod.ShardedSafetensorsSource,

    const vtable = TensorStore.VTable{
        .kind = @ptrCast(&kindImpl),
        .weightSource = @ptrCast(&weightSourceImpl),
        .describeTensor = @ptrCast(&describeTensorImpl),
        .loadTensorRef = @ptrCast(&loadTensorRefImpl),
        .loadQuantizedStorageRef = @ptrCast(&loadQuantizedStorageRefImpl),
        .ggufFile = @ptrCast(&ggufFileImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initAbsolute(allocator: std.mem.Allocator, index_path: []const u8) !*ShardedSafetensorsStore {
        const self = try allocator.create(ShardedSafetensorsStore);
        errdefer allocator.destroy(self);
        self.* = .{
            .source = try weight_source_mod.ShardedSafetensorsSource.initAbsolute(allocator, index_path),
        };
        return self;
    }

    pub fn tensorStore(self: *ShardedSafetensorsStore) TensorStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn kindImpl(_: *ShardedSafetensorsStore) StoreKind {
        return .safetensors;
    }

    fn weightSourceImpl(self: *ShardedSafetensorsStore) !?weight_source_mod.WeightSource {
        return self.source.weightSource();
    }

    fn describeTensorImpl(self: *ShardedSafetensorsStore, allocator: std.mem.Allocator, name: []const u8) !LazyTensorRef {
        const resolved = try self.source.findTensorMeta(name);
        return .{
            .name = try allocator.dupe(u8, name),
            .byte_len = @intCast(resolved.meta.data_end - resolved.meta.data_start),
            .quantized = false,
        };
    }

    fn loadTensorRefImpl(self: *ShardedSafetensorsStore, tensor_ref: *const LazyTensorRef) !weight_source_mod.LoadedWeight {
        const resolved = try self.source.findTensorMeta(tensor_ref.name);
        return .{
            .tensor = try resolved.reader.readTensor(tensor_ref.name),
            .quantized = false,
        };
    }

    fn loadQuantizedStorageRefImpl(_: *ShardedSafetensorsStore, _: *const LazyTensorRef) !?weight_source_mod.QuantizedStorage {
        return null;
    }

    fn ggufFileImpl(_: *ShardedSafetensorsStore) ?*const gguf_mod.format.File {
        return null;
    }

    fn deinitSelf(self: *ShardedSafetensorsStore) void {
        const allocator = self.source.allocator;
        self.source.weightSource().deinit();
        allocator.destroy(self);
    }
};

pub const GgufStore = struct {
    allocator: std.mem.Allocator,
    path: ?[]const u8 = null,
    mmap_region: ?c_file.MmapRegion = null,
    owned_bytes: ?[]u8 = null,
    parsed: gguf_mod.format.File,

    const vtable = TensorStore.VTable{
        .kind = @ptrCast(&kindImpl),
        .weightSource = @ptrCast(&weightSourceImpl),
        .describeTensor = @ptrCast(&describeTensorImpl),
        .loadTensorRef = @ptrCast(&loadTensorRefImpl),
        .loadQuantizedStorageRef = @ptrCast(&loadQuantizedStorageRefImpl),
        .ggufFile = @ptrCast(&ggufFileImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initAbsolute(allocator: std.mem.Allocator, path: []const u8) !*GgufStore {
        const self = try allocator.create(GgufStore);
        errdefer allocator.destroy(self);

        var mmap_region = try c_file.MmapRegion.init(allocator, path);
        errdefer mmap_region.deinit();

        var parsed = try gguf_mod.format.parse(allocator, mmap_region.data);
        errdefer parsed.deinit(allocator);

        // Mark the tensor data region as random-access to prevent kernel
        // readahead from faulting the entire file into RAM. Header/metadata
        // was already read sequentially by parse().
        mmap_region.adviseSequentialPrefix(parsed.data_region_offset);

        self.* = .{
            .allocator = allocator,
            .path = try allocator.dupe(u8, path),
            .mmap_region = mmap_region,
            .owned_bytes = null,
            .parsed = parsed,
        };
        return self;
    }

    pub fn initOwnedBytes(
        allocator: std.mem.Allocator,
        name_hint: []const u8,
        raw_bytes: []const u8,
    ) !*GgufStore {
        const self = try allocator.create(GgufStore);
        errdefer allocator.destroy(self);

        const owned_bytes = try allocator.dupe(u8, raw_bytes);
        errdefer allocator.free(owned_bytes);

        var parsed = try gguf_mod.format.parse(allocator, owned_bytes);
        errdefer parsed.deinit(allocator);

        self.* = .{
            .allocator = allocator,
            .path = try allocator.dupe(u8, name_hint),
            .mmap_region = null,
            .owned_bytes = owned_bytes,
            .parsed = parsed,
        };
        return self;
    }

    pub fn tensorStore(self: *GgufStore) TensorStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn rawData(self: *const GgufStore) []const u8 {
        if (self.owned_bytes) |owned| return owned;
        return self.mmap_region.?.data;
    }

    fn kindImpl(_: *GgufStore) StoreKind {
        return .gguf;
    }

    fn weightSourceImpl(self: *GgufStore) !?weight_source_mod.WeightSource {
        return .{ .ptr = self, .vtable = &gguf_weight_source_vtable };
    }

    fn describeTensorImpl(self: *GgufStore, allocator: std.mem.Allocator, name: []const u8) !LazyTensorRef {
        const tensor = gguf_mod.tensor_catalog.Catalog.init(&self.parsed).find(name) orelse return error.TensorNotFound;
        const byte_len_u64 = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
        return .{
            .name = try allocator.dupe(u8, name),
            .byte_len = @intCast(byte_len_u64),
            .quantized = tensor.tensor_type.isQuantized(),
        };
    }

    fn loadTensorRefImpl(self: *GgufStore, tensor_ref: *const LazyTensorRef) !weight_source_mod.LoadedWeight {
        return ggufGetTensorRef(self, tensor_ref);
    }

    fn loadQuantizedStorageRefImpl(self: *GgufStore, tensor_ref: *const LazyTensorRef) !?weight_source_mod.QuantizedStorage {
        return ggufGetQuantizedStorageRef(self, tensor_ref);
    }

    fn ggufFileImpl(self: *GgufStore) ?*const gguf_mod.format.File {
        return &self.parsed;
    }

    fn deinitSelf(self: *GgufStore) void {
        self.parsed.deinit(self.allocator);
        if (self.mmap_region) |*region| region.deinit();
        if (self.owned_bytes) |owned| self.allocator.free(owned);
        if (self.path) |path| self.allocator.free(path);
        self.allocator.destroy(self);
    }
};

const gguf_weight_source_vtable = weight_source_mod.WeightSource.VTable{
    .getTensor = @ptrCast(&ggufGetTensor),
    .listNames = @ptrCast(&ggufListNames),
    .deinit = @ptrCast(&ggufNoopDeinit),
};

fn ggufGetTensor(self: *GgufStore, name: []const u8) !weight_source_mod.LoadedWeight {
    const tensor = gguf_mod.tensor_catalog.Catalog.init(&self.parsed).find(name) orelse return error.TensorNotFound;
    const byte_len_u64 = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
    const byte_len: usize = @intCast(byte_len_u64);
    const data_off: usize = @intCast(tensor.data_offset);
    const raw_bytes = self.rawData()[data_off .. data_off + byte_len];

    const shape = try normalizedShapeFromDims(self.allocator, tensor.dimensions);
    defer self.allocator.free(shape);

    const quantized_storage = if (tensor.tensor_type.isQuantized()) blk: {
        const quant_shape = try self.allocator.dupe(i64, shape);
        const storage_source_name = try self.allocator.dupe(u8, tensor.name);
        errdefer self.allocator.free(storage_source_name);
        break :blk weight_source_mod.QuantizedStorage{
            .tensor_type = tensor.tensor_type,
            .raw_bytes = @constCast(raw_bytes),
            .shape = quant_shape,
            .source_name = storage_source_name,
            .raw_owned = false,
            .raw_mmap_backed = self.mmap_region != null,
            .allocator = self.allocator,
        };
    } else null;
    errdefer {
        if (quantized_storage) |storage_value| {
            var storage = storage_value;
            storage.deinit();
        }
    }

    if (ggufDenseTensorDType(tensor.tensor_type)) |dtype| {
        const owned_shape = try self.allocator.dupe(i64, shape);
        return .{
            .tensor = .{
                .data = @constCast(raw_bytes),
                .dtype = dtype,
                .shape = owned_shape,
                .name = tensor.name,
                .allocator = self.allocator,
                .owns_data = false,
                .owns_shape = true,
            },
            .quantized = false,
            .quantized_storage = quantized_storage,
        };
    }

    var materialized = (try gguf_mod.quant_codec.materializeDense(self.allocator, tensor.name, tensor.tensor_type, shape, raw_bytes)) orelse return error.UnsupportedTensorType;
    errdefer materialized.deinit();
    if (materialized.dtype == .f16 or materialized.dtype == .bf16) {
        const converted = try weight_source_mod.convertToF32(self.allocator, &materialized);
        materialized.deinit();
        materialized = converted;
    }
    return .{
        .tensor = materialized,
        .quantized = tensor.tensor_type.isQuantized(),
        .quantized_storage = quantized_storage,
    };
}

fn ggufGetTensorRef(self: *GgufStore, tensor_ref: *const LazyTensorRef) !weight_source_mod.LoadedWeight {
    if (tensor_ref.packed_expert_index) |expert_index| {
        return ggufGetPackedExpertTensor(self, tensor_ref, expert_index);
    }

    const source_name = tensor_ref.source_name orelse tensor_ref.name;
    return ggufGetTensor(self, source_name);
}

fn ggufGetQuantizedStorageRef(self: *GgufStore, tensor_ref: *const LazyTensorRef) !?weight_source_mod.QuantizedStorage {
    if (tensor_ref.packed_expert_index) |expert_index| {
        return ggufGetPackedExpertQuantizedStorage(self, tensor_ref, expert_index);
    }
    if (tensor_ref.packed_expert_count != 0 and tensor_ref.source_name != null) {
        return ggufGetPackedMoeQuantizedStorage(self, tensor_ref);
    }

    const source_name = tensor_ref.source_name orelse tensor_ref.name;
    const tensor = gguf_mod.tensor_catalog.Catalog.init(&self.parsed).find(source_name) orelse return error.TensorNotFound;
    if (!tensor.tensor_type.isQuantized()) return null;

    const full_shape = try normalizedShapeFromDims(self.allocator, tensor.dimensions);
    errdefer self.allocator.free(full_shape);
    const byte_len_u64 = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
    const byte_len: usize = @intCast(byte_len_u64);
    const data_off: usize = @intCast(tensor.data_offset);
    const raw_bytes = self.rawData()[data_off .. data_off + byte_len];

    return weight_source_mod.QuantizedStorage{
        .tensor_type = tensor.tensor_type,
        .raw_bytes = @constCast(raw_bytes),
        .shape = full_shape,
        .source_name = try self.allocator.dupe(u8, source_name),
        .raw_owned = false,
        .raw_mmap_backed = self.mmap_region != null,
        .allocator = self.allocator,
    };
}

fn ggufGetPackedExpertTensor(
    self: *GgufStore,
    tensor_ref: *const LazyTensorRef,
    expert_index: u32,
) !weight_source_mod.LoadedWeight {
    const source_name = tensor_ref.source_name orelse return error.TensorNotFound;
    const tensor = gguf_mod.tensor_catalog.Catalog.init(&self.parsed).find(source_name) orelse return error.TensorNotFound;

    const full_shape = try normalizedShapeFromDims(self.allocator, tensor.dimensions);
    defer self.allocator.free(full_shape);
    const expert_axis = findPackedExpertAxis(full_shape, tensor_ref.packed_expert_count) orelse return error.InvalidPackedExpertTensor;
    if (expert_index >= @as(u32, @intCast(full_shape[expert_axis]))) return error.InvalidPackedExpertTensor;

    const byte_len_u64 = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
    const byte_len: usize = @intCast(byte_len_u64);
    const data_off: usize = @intCast(tensor.data_offset);
    const raw_bytes = self.rawData()[data_off .. data_off + byte_len];

    if (tensor.tensor_type.isQuantized()) {
        const quant_shape = try self.allocator.dupe(i64, full_shape);
        errdefer self.allocator.free(quant_shape);
        var fused_row_offset: u32 = 0;
        var fused_row_count_override: ?usize = null;
        if (tensor_ref.fused_gate_up and full_shape.len == 3) {
            const out_dim_axis = try ggufPackedFusedGateUpOutputAxis(self, full_shape, expert_axis);
            const half_dim: u32 = @intCast(@divExact(full_shape[out_dim_axis], 2));
            fused_row_count_override = half_dim;
            if (tensor_ref.fused_gate_up_index == 1) {
                fused_row_offset = half_dim;
            }
        }
        const quantized_storage: weight_source_mod.QuantizedStorage = .{
            .tensor_type = tensor.tensor_type,
            .raw_bytes = @constCast(raw_bytes),
            .shape = quant_shape,
            .source_name = try self.allocator.dupe(u8, source_name),
            .packed_expert = .{
                .expert_index = expert_index,
                .expert_count = tensor_ref.packed_expert_count,
                .expert_axis = @intCast(expert_axis),
                .row_offset = fused_row_offset,
            },
            .raw_owned = false,
            .raw_mmap_backed = self.mmap_region != null,
            .allocator = self.allocator,
        };
        errdefer {
            var storage = quantized_storage;
            storage.deinit();
        }

        if (try gguf_mod.quant_codec.materializePackedExpertDense(
            self.allocator,
            tensor_ref.name,
            tensor.tensor_type,
            full_shape,
            raw_bytes,
            expert_axis,
            expert_index,
            fused_row_offset,
            fused_row_count_override,
        )) |materialized| {
            return .{
                .tensor = materialized,
                .quantized = true,
                .quantized_storage = quantized_storage,
            };
        }
    }

    var loaded = try ggufGetTensor(self, source_name);
    errdefer loaded.deinit();
    const sliced = try slicePackedExpertTensor(
        self.allocator,
        &loaded.tensor,
        expert_index,
        tensor_ref.packed_expert_count,
        tensor_ref.name,
    );
    loaded.deinit();
    return .{
        .tensor = sliced,
        .quantized = loaded.quantized,
        .quantized_storage = null,
    };
}

fn ggufGetPackedExpertQuantizedStorage(
    self: *GgufStore,
    tensor_ref: *const LazyTensorRef,
    expert_index: u32,
) !?weight_source_mod.QuantizedStorage {
    const source_name = tensor_ref.source_name orelse return error.TensorNotFound;
    const tensor = gguf_mod.tensor_catalog.Catalog.init(&self.parsed).find(source_name) orelse return error.TensorNotFound;
    if (!tensor.tensor_type.isQuantized()) return null;

    const full_shape = try normalizedShapeFromDims(self.allocator, tensor.dimensions);
    errdefer self.allocator.free(full_shape);
    const expert_axis = findPackedExpertAxis(full_shape, tensor_ref.packed_expert_count) orelse return error.InvalidPackedExpertTensor;
    if (expert_index >= @as(u32, @intCast(full_shape[expert_axis]))) return error.InvalidPackedExpertTensor;

    const byte_len_u64 = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
    const byte_len: usize = @intCast(byte_len_u64);
    const data_off: usize = @intCast(tensor.data_offset);
    const raw_bytes = self.rawData()[data_off .. data_off + byte_len];

    // For fused gate+up tensors, set row_offset so w1 (gate) uses the first
    // half of the out_dim axis and w3 (up) uses the second half.
    // Keep the full shape for correct stride calculation.
    var row_offset: u32 = 0;
    if (tensor_ref.fused_gate_up and full_shape.len == 3) {
        const out_dim_axis = try ggufPackedFusedGateUpOutputAxis(self, full_shape, expert_axis);
        const half_dim: u32 = @intCast(@divExact(full_shape[out_dim_axis], 2));
        if (tensor_ref.fused_gate_up_index == 1) {
            row_offset = half_dim;
        }
    }

    return weight_source_mod.QuantizedStorage{
        .tensor_type = tensor.tensor_type,
        .raw_bytes = @constCast(raw_bytes),
        .shape = full_shape,
        .source_name = try self.allocator.dupe(u8, source_name),
        .packed_expert = .{
            .expert_index = expert_index,
            .expert_count = tensor_ref.packed_expert_count,
            .expert_axis = @intCast(expert_axis),
            .row_offset = row_offset,
        },
        .raw_owned = false,
        .raw_mmap_backed = self.mmap_region != null,
        .allocator = self.allocator,
    };
}

fn ggufGetPackedMoeQuantizedStorage(
    self: *GgufStore,
    tensor_ref: *const LazyTensorRef,
) !?weight_source_mod.QuantizedStorage {
    const source_name = tensor_ref.source_name orelse return error.TensorNotFound;
    const tensor = gguf_mod.tensor_catalog.Catalog.init(&self.parsed).find(source_name) orelse return error.TensorNotFound;
    if (!tensor.tensor_type.isQuantized()) return null;

    const full_shape = try normalizedShapeFromDims(self.allocator, tensor.dimensions);
    errdefer self.allocator.free(full_shape);
    const expert_axis = findPackedExpertAxis(full_shape, tensor_ref.packed_expert_count) orelse return error.InvalidPackedExpertTensor;

    const byte_len_u64 = gguf_mod.tensor_types.byteLen(tensor.tensor_type, tensor.dimensions) orelse return error.UnsupportedTensorType;
    const byte_len: usize = @intCast(byte_len_u64);
    const data_off: usize = @intCast(tensor.data_offset);
    const raw_bytes = self.rawData()[data_off .. data_off + byte_len];

    var row_offset: u32 = 0;
    if (tensor_ref.fused_gate_up and full_shape.len == 3) {
        const out_dim_axis = try ggufPackedFusedGateUpOutputAxis(self, full_shape, expert_axis);
        const half_dim: u32 = @intCast(@divExact(full_shape[out_dim_axis], 2));
        if (tensor_ref.fused_gate_up_index == 1) {
            row_offset = half_dim;
        }
    }

    return weight_source_mod.QuantizedStorage{
        .tensor_type = tensor.tensor_type,
        .raw_bytes = @constCast(raw_bytes),
        .shape = full_shape,
        .source_name = try self.allocator.dupe(u8, source_name),
        .packed_expert = .{
            .expert_index = 0,
            .expert_count = tensor_ref.packed_expert_count,
            .expert_axis = @intCast(expert_axis),
            .row_offset = row_offset,
        },
        .raw_owned = false,
        .raw_mmap_backed = self.mmap_region != null,
        .allocator = self.allocator,
    };
}

fn reversedShape(allocator: std.mem.Allocator, shape: []const i64) ![]i64 {
    const reversed = try allocator.alloc(i64, shape.len);
    for (0..shape.len) |i| reversed[i] = shape[shape.len - 1 - i];
    return reversed;
}

fn normalizedShapeFromDims(allocator: std.mem.Allocator, dimensions: []const u64) ![]i64 {
    const shape = try allocator.alloc(i64, dimensions.len);
    for (0..dimensions.len) |i| shape[i] = @intCast(dimensions[dimensions.len - 1 - i]);
    return shape;
}

fn slicePackedExpertTensor(
    allocator: std.mem.Allocator,
    tensor: *const @import("../backends/tensor.zig").Tensor,
    expert_index: u32,
    expert_count: u32,
    name: []const u8,
) !@import("../backends/tensor.zig").Tensor {
    if (tensor.shape.len < 2) return error.InvalidPackedExpertTensor;
    if (expert_count == 0) return error.InvalidPackedExpertTensor;

    const axis = findPackedExpertAxis(tensor.shape, expert_count) orelse return error.InvalidPackedExpertTensor;
    if (expert_index >= @as(u32, @intCast(tensor.shape[axis]))) return error.InvalidPackedExpertTensor;

    const out_rank = tensor.shape.len - 1;
    const out_shape = try allocator.alloc(i64, out_rank);
    errdefer allocator.free(out_shape);
    {
        var dst: usize = 0;
        for (tensor.shape, 0..) |dim, i| {
            if (i == axis) continue;
            out_shape[dst] = dim;
            dst += 1;
        }
    }

    const elem_size = tensor.dtype.byteSize();
    const out_count = elementCount(out_shape) orelse return error.InvalidPackedExpertTensor;
    const out_bytes = try allocator.alloc(u8, out_count * elem_size);
    errdefer allocator.free(out_bytes);

    const in_strides = try computeRowMajorStrides(allocator, tensor.shape);
    defer allocator.free(in_strides);
    const out_strides = try computeRowMajorStrides(allocator, out_shape);
    defer allocator.free(out_strides);

    const out_indices = try allocator.alloc(usize, out_rank);
    defer allocator.free(out_indices);
    var in_indices = try allocator.alloc(usize, tensor.shape.len);
    defer allocator.free(in_indices);

    for (0..out_count) |linear_out| {
        linearIndexToIndices(linear_out, out_shape, out_strides, out_indices);
        {
            var src_i: usize = 0;
            for (0..tensor.shape.len) |dim_i| {
                if (dim_i == axis) {
                    in_indices[dim_i] = expert_index;
                } else {
                    in_indices[dim_i] = out_indices[src_i];
                    src_i += 1;
                }
            }
        }
        const in_linear = indicesToLinearIndex(in_indices, in_strides);
        const src_start = in_linear * elem_size;
        const dst_start = linear_out * elem_size;
        @memcpy(out_bytes[dst_start .. dst_start + elem_size], tensor.data[src_start .. src_start + elem_size]);
    }

    return .{
        .data = out_bytes,
        .dtype = tensor.dtype,
        .shape = out_shape,
        .name = name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn normalizeGgufTensorLayout(
    allocator: std.mem.Allocator,
    tensor: *const @import("../backends/tensor.zig").Tensor,
) !@import("../backends/tensor.zig").Tensor {
    const rank = tensor.shape.len;
    if (rank < 2) return error.InvalidPackedExpertTensor;

    const new_shape = try allocator.alloc(i64, rank);
    errdefer allocator.free(new_shape);
    for (0..rank) |i| new_shape[i] = tensor.shape[rank - 1 - i];

    const elem_size = tensor.dtype.byteSize();
    const count = elementCount(tensor.shape) orelse return error.InvalidTensorShape;
    const new_bytes = try allocator.alloc(u8, count * elem_size);
    errdefer allocator.free(new_bytes);

    const in_strides = try computeRowMajorStrides(allocator, tensor.shape);
    defer allocator.free(in_strides);
    const out_strides = try computeRowMajorStrides(allocator, new_shape);
    defer allocator.free(out_strides);

    const out_indices = try allocator.alloc(usize, rank);
    defer allocator.free(out_indices);
    var in_indices = try allocator.alloc(usize, rank);
    defer allocator.free(in_indices);

    for (0..count) |linear_out| {
        linearIndexToIndices(linear_out, new_shape, out_strides, out_indices);
        for (0..rank) |i| in_indices[rank - 1 - i] = out_indices[i];
        const in_linear = indicesToLinearIndex(in_indices, in_strides);
        const src_start = in_linear * elem_size;
        const dst_start = linear_out * elem_size;
        @memcpy(new_bytes[dst_start .. dst_start + elem_size], tensor.data[src_start .. src_start + elem_size]);
    }

    return .{
        .data = new_bytes,
        .dtype = tensor.dtype,
        .shape = new_shape,
        .name = tensor.name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

fn ggufDenseTensorDType(tensor_type: gguf_mod.tensor_types.TensorType) ?@import("../backends/tensor.zig").DType {
    return switch (tensor_type) {
        .known => |known| switch (known) {
            .F32 => .f32,
            .F16 => .f16,
            .BF16 => .bf16,
            .F64 => .f64,
            .I8 => .i8,
            .I16 => .i16,
            .I32 => .i32,
            .I64 => .i64,
            else => null,
        },
        .bitnet_tl2 => null,
        .unknown => null,
    };
}

fn findPackedExpertAxis(shape: []const i64, expert_count: u32) ?usize {
    var found: ?usize = null;
    for (shape, 0..) |dim, axis| {
        if (dim != @as(i64, @intCast(expert_count))) continue;
        if (found != null) return null;
        found = axis;
    }
    return found;
}

fn ggufPackedFusedGateUpOutputAxis(self: *GgufStore, shape: []const i64, expert_axis: usize) !usize {
    if (shape.len != 3 or expert_axis >= shape.len) return error.InvalidPackedExpertTensor;
    const meta = gguf_mod.metadata.View.init(&self.parsed);
    if (meta.getString("general.architecture")) |arch| {
        const key = try std.fmt.allocPrint(self.allocator, "{s}.expert_feed_forward_length", .{arch});
        defer self.allocator.free(key);
        if (meta.getU64(key)) |expert_ff| {
            const fused_rows: i64 = @intCast(expert_ff * 2);
            for (shape, 0..) |dim, axis| {
                if (axis != expert_axis and dim == fused_rows) return axis;
            }
        }
    }

    if (expert_axis == 0) return 1;
    if (expert_axis == 2) return 1;
    return 0;
}

fn computeRowMajorStrides(allocator: std.mem.Allocator, shape: []const i64) ![]usize {
    const strides = try allocator.alloc(usize, shape.len);
    var stride: usize = 1;
    var i: usize = shape.len;
    while (i > 0) {
        i -= 1;
        strides[i] = stride;
        stride *= @intCast(shape[i]);
    }
    return strides;
}

fn linearIndexToIndices(linear_index: usize, shape: []const i64, strides: []const usize, out_indices: []usize) void {
    var remaining = linear_index;
    for (shape, 0..) |dim, i| {
        const stride = strides[i];
        out_indices[i] = remaining / stride;
        remaining %= stride;
        std.debug.assert(out_indices[i] < @as(usize, @intCast(dim)));
    }
}

fn indicesToLinearIndex(indices: []const usize, strides: []const usize) usize {
    var linear: usize = 0;
    for (indices, 0..) |index, i| linear += index * strides[i];
    return linear;
}

fn elementCount(shape: []const i64) ?usize {
    var total: usize = 1;
    for (shape) |dim| {
        if (dim < 0) return null;
        total = std.math.mul(usize, total, @as(usize, @intCast(dim))) catch return null;
    }
    return total;
}

fn ggufListNames(self: *GgufStore, allocator: std.mem.Allocator) ![][]const u8 {
    const names = try allocator.alloc([]const u8, self.parsed.tensors.len);
    for (self.parsed.tensors, 0..) |tensor, i| names[i] = tensor.name;
    return names;
}

fn ggufNoopDeinit(_: *GgufStore) void {}

pub const CompositeGlinerStore = struct {
    allocator: std.mem.Allocator,
    encoder: *GgufStore,
    head_safetensors: ?*SafetensorsStore = null,
    head_gguf: ?*GgufStore = null,

    const vtable = TensorStore.VTable{
        .kind = @ptrCast(&kindImpl),
        .weightSource = @ptrCast(&weightSourceImpl),
        .describeTensor = @ptrCast(&describeTensorImpl),
        .loadTensorRef = @ptrCast(&loadTensorRefImpl),
        .loadQuantizedStorageRef = @ptrCast(&loadQuantizedStorageRefImpl),
        .ggufFile = @ptrCast(&ggufFileImpl),
        .deinit = @ptrCast(&deinitSelf),
    };

    const weight_source_vtable = weight_source_mod.WeightSource.VTable{
        .getTensor = @ptrCast(&getTensorImpl),
        .listNames = @ptrCast(&listNamesImpl),
        .deinit = @ptrCast(&weightSourceNoopDeinit),
    };

    pub fn initAbsolute(
        allocator: std.mem.Allocator,
        encoder_path: []const u8,
        head_path: []const u8,
        head_is_gguf: bool,
    ) !*CompositeGlinerStore {
        const self = try allocator.create(CompositeGlinerStore);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .encoder = try GgufStore.initAbsolute(allocator, encoder_path),
            .head_safetensors = if (head_is_gguf) null else try SafetensorsStore.initAbsolute(allocator, head_path),
            .head_gguf = if (head_is_gguf) try GgufStore.initAbsolute(allocator, head_path) else null,
        };
        return self;
    }

    pub fn tensorStore(self: *CompositeGlinerStore) TensorStore {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn kindImpl(_: *CompositeGlinerStore) StoreKind {
        return .gguf;
    }

    fn weightSourceImpl(self: *CompositeGlinerStore) !?weight_source_mod.WeightSource {
        return .{ .ptr = self, .vtable = &weight_source_vtable };
    }

    fn hasHeadTensor(self: *CompositeGlinerStore, name: []const u8) bool {
        if (self.head_safetensors) |head| return head.source.reader.header.tensors.contains(name);
        if (self.head_gguf) |head| {
            for (head.parsed.tensors) |tensor| {
                if (std.mem.eql(u8, tensor.name, name)) return true;
            }
        }
        return false;
    }

    fn describeTensorImpl(self: *CompositeGlinerStore, allocator: std.mem.Allocator, name: []const u8) !LazyTensorRef {
        if (self.hasHeadTensor(name)) {
            if (self.head_safetensors) |head| return head.describeTensorImpl(allocator, name);
            return self.head_gguf.?.describeTensorImpl(allocator, name);
        }
        return self.encoder.describeTensorImpl(allocator, name);
    }

    fn loadTensorRefImpl(self: *CompositeGlinerStore, tensor_ref: *const LazyTensorRef) !weight_source_mod.LoadedWeight {
        if (self.hasHeadTensor(tensor_ref.name)) {
            if (self.head_safetensors) |head| return head.loadTensorRefImpl(tensor_ref);
            return self.head_gguf.?.loadTensorRefImpl(tensor_ref);
        }
        return self.encoder.loadTensorRefImpl(tensor_ref);
    }

    fn loadQuantizedStorageRefImpl(self: *CompositeGlinerStore, tensor_ref: *const LazyTensorRef) !?weight_source_mod.QuantizedStorage {
        if (self.hasHeadTensor(tensor_ref.name)) {
            if (self.head_safetensors != null) return null;
            return self.head_gguf.?.loadQuantizedStorageRefImpl(tensor_ref);
        }
        return self.encoder.loadQuantizedStorageRefImpl(tensor_ref);
    }

    fn ggufFileImpl(self: *CompositeGlinerStore) ?*const gguf_mod.format.File {
        return &self.encoder.parsed;
    }

    fn deinitSelf(self: *CompositeGlinerStore) void {
        self.encoder.tensorStore().deinit();
        if (self.head_safetensors) |head| head.tensorStore().deinit();
        if (self.head_gguf) |head| head.tensorStore().deinit();
        self.allocator.destroy(self);
    }

    fn getTensorImpl(self: *CompositeGlinerStore, name: []const u8) !weight_source_mod.LoadedWeight {
        if (self.hasHeadTensor(name)) {
            if (self.head_safetensors) |head| {
                var tensor = try head.source.reader.readTensor(name);
                if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
                    const converted = try weight_source_mod.convertToF32(self.allocator, &tensor);
                    tensor.deinit();
                    return .{ .tensor = converted, .quantized = false };
                }
                return .{ .tensor = tensor, .quantized = false };
            }
            return ggufGetTensor(self.head_gguf.?, name);
        }
        return ggufGetTensor(self.encoder, name);
    }

    fn listNamesImpl(self: *CompositeGlinerStore, allocator: std.mem.Allocator) ![][]const u8 {
        const encoder_names = try ggufListNames(self.encoder, allocator);
        errdefer allocator.free(encoder_names);
        const head_names = if (self.head_safetensors) |head|
            try head.source.reader.header.tensorNames(allocator)
        else
            try ggufListNames(self.head_gguf.?, allocator);
        errdefer allocator.free(head_names);
        const names = try allocator.alloc([]const u8, encoder_names.len + head_names.len);
        @memcpy(names[0..encoder_names.len], encoder_names);
        @memcpy(names[encoder_names.len..], head_names);
        allocator.free(encoder_names);
        allocator.free(head_names);
        return names;
    }

    fn weightSourceNoopDeinit(_: *CompositeGlinerStore) void {}
};

pub fn openFromManifest(allocator: std.mem.Allocator, manifest: manifest_mod.ModelManifest) !TensorStore {
    if (manifest.hasIncompleteGlinerBundle()) return error.IncompleteGlinerBundle;
    if (manifest.gliner_model_type.len > 0 and manifest.gguf_path != null and (manifest.gliner_head_gguf_path != null or manifest.gliner_head_safetensors_path != null)) {
        const head_path = manifest.gliner_head_gguf_path orelse manifest.gliner_head_safetensors_path.?;
        const store = try CompositeGlinerStore.initAbsolute(allocator, manifest.gguf_path.?, head_path, manifest.gliner_head_gguf_path != null);
        return store.tensorStore();
    }
    if (manifest.safetensors_path) |path| {
        const store = try SafetensorsStore.initAbsolute(allocator, path);
        return store.tensorStore();
    }
    if (manifest.safetensors_index_path) |path| {
        const store = try ShardedSafetensorsStore.initAbsolute(allocator, path);
        return store.tensorStore();
    }
    if (manifest.gguf_path) |path| {
        const store = try GgufStore.initAbsolute(allocator, path);
        return store.tensorStore();
    }
    return error.NoTensorStoreFound;
}

test "open sharded safetensors tensor store from manifest" {
    const allocator = std.testing.allocator;

    const shard1_json =
        \\{"tensor_a": {"dtype": "F32", "shape": [2], "data_offsets": [0, 8]}}
    ;
    const shard2_json =
        \\{"tensor_b": {"dtype": "F32", "shape": [2], "data_offsets": [0, 8]}}
    ;
    const index_json =
        \\{"weight_map":{"tensor_a":"model-00001-of-00002.safetensors","tensor_b":"model-00002-of-00002.safetensors"}}
    ;

    var shard1 = std.ArrayListUnmanaged(u8).empty;
    defer shard1.deinit(allocator);
    try appendLe(u64, allocator, &shard1, shard1_json.len);
    try shard1.appendSlice(allocator, shard1_json);
    try shard1.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 1.0, 2.0 }));

    var shard2 = std.ArrayListUnmanaged(u8).empty;
    defer shard2.deinit(allocator);
    try appendLe(u64, allocator, &shard2, shard2_json.len);
    try shard2.appendSlice(allocator, shard2_json);
    try shard2.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 3.0, 4.0 }));

    const dir_path = try testScratchDir(allocator, "tensor-store-sharded-safetensors");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const shard1_path = try std.fs.path.join(allocator, &.{ dir_path, "model-00001-of-00002.safetensors" });
    defer allocator.free(shard1_path);
    const shard2_path = try std.fs.path.join(allocator, &.{ dir_path, "model-00002-of-00002.safetensors" });
    defer allocator.free(shard2_path);
    const index_path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors.index.json" });
    defer allocator.free(index_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = shard1_path, .data = shard1.items });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = shard2_path, .data = shard2.items });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = index_path, .data = index_json });

    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .safetensors_index_path = try allocator.dupe(u8, index_path),
    };
    defer manifest.deinit();

    const store = try openFromManifest(allocator, manifest);
    defer store.deinit();
    try std.testing.expectEqual(StoreKind.safetensors, store.kind());

    const source = (try store.weightSource()) orelse return error.TestUnexpectedResult;
    const names = try source.listNames(allocator);
    defer allocator.free(names);
    try std.testing.expectEqual(@as(usize, 2), names.len);

    var tensor_ref = try store.describeTensor(allocator, "tensor_b");
    defer tensor_ref.deinit(allocator);
    try std.testing.expectEqual(@as(usize, 8), tensor_ref.byte_len);

    var loaded = try store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), loaded.tensor.elementCount());
    try std.testing.expectApproxEqAbs(@as(f32, 3.0), loaded.tensor.asFloat32()[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 4.0), loaded.tensor.asFloat32()[1], 1e-6);
}

test "safetensors tensor store preserves f16 dtype" {
    const allocator = std.testing.allocator;

    const json =
        \\{"weights": {"dtype": "F16", "shape": [2], "data_offsets": [0, 4]}}
    ;

    var file = std.ArrayListUnmanaged(u8).empty;
    defer file.deinit(allocator);
    try appendLe(u64, allocator, &file, json.len);
    try file.appendSlice(allocator, json);
    const values = [_]u16{ 0x3C00, 0x4000 };
    try file.appendSlice(allocator, std.mem.asBytes(&values));

    const dir_path = try testScratchDir(allocator, "tensor-store-safetensors-f16");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const path = try std.fs.path.join(allocator, &.{ dir_path, "model.safetensors" });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = file.items });

    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .safetensors_path = try allocator.dupe(u8, path),
    };
    defer manifest.deinit();

    const store = try openFromManifest(allocator, manifest);
    defer store.deinit();

    var tensor_ref = try store.describeTensor(allocator, "weights");
    defer tensor_ref.deinit(allocator);
    var loaded = try store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    try std.testing.expectEqual(@import("../backends/tensor.zig").DType.f16, loaded.tensor.dtype);
    try std.testing.expectEqualSlices(u8, std.mem.asBytes(&values), loaded.tensor.data);
}

test "open gguf tensor store from manifest" {
    const allocator = std.testing.allocator;

    var data = std.ArrayListUnmanaged(u8).empty;
    defer data.deinit(allocator);

    try data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &data, 3);
    try appendLe(u64, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 0);
    try appendString(allocator, &data, "tok_embeddings.weight");
    try appendLe(u32, allocator, &data, 1);
    try appendLe(u64, allocator, &data, 4);
    try appendLe(u32, allocator, &data, @intFromEnum(gguf_mod.tensor_types.KnownTensorType.F16));
    try appendLe(u64, allocator, &data, 0);
    try padToAlignment(allocator, &data, gguf_mod.format.default_alignment);
    try data.appendSlice(allocator, &[_]u8{ 0x00, 0x3C, 0x00, 0x40, 0x00, 0x42, 0x00, 0x44 });

    const dir_path = try testScratchDir(allocator, "tensor-store-gguf");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const path = try std.fs.path.join(allocator, &.{ dir_path, "model.gguf" });
    defer allocator.free(path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = path, .data = data.items });

    var manifest = manifest_mod.ModelManifest{ .allocator = allocator, .gguf_path = try allocator.dupe(u8, path) };
    defer manifest.deinit();

    const store = try openFromManifest(allocator, manifest);
    defer store.deinit();
    try std.testing.expectEqual(StoreKind.gguf, store.kind());
    try std.testing.expect(store.ggufFile() != null);
    try std.testing.expect((try store.weightSource()) != null);
    var tensor_ref = try store.describeTensor(allocator, "tok_embeddings.weight");
    try std.testing.expectEqual(@as(usize, 8), tensor_ref.byte_len);
    var loaded = try store.loadTensorRef(&tensor_ref);
    defer loaded.deinit();
    try std.testing.expectEqual(@as(usize, 4), loaded.tensor.elementCount());
    try std.testing.expectEqual(@import("../backends/tensor.zig").DType.f16, loaded.tensor.dtype);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x00, 0x3C, 0x00, 0x40, 0x00, 0x42, 0x00, 0x44 }, loaded.tensor.data);
    tensor_ref.deinit(allocator);
}

test "open split gliner gguf bundle from manifest" {
    const allocator = std.testing.allocator;

    var gguf_data = std.ArrayListUnmanaged(u8).empty;
    defer gguf_data.deinit(allocator);
    try gguf_data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &gguf_data, 3);
    try appendLe(u64, allocator, &gguf_data, 1);
    try appendLe(u64, allocator, &gguf_data, 1);
    try appendString(allocator, &gguf_data, "general.architecture");
    try appendLe(u32, allocator, &gguf_data, 8);
    try appendString(allocator, &gguf_data, "deberta");
    try appendString(allocator, &gguf_data, "embeddings.word_embeddings.weight");
    try appendLe(u32, allocator, &gguf_data, 1);
    try appendLe(u64, allocator, &gguf_data, 4);
    try appendLe(u32, allocator, &gguf_data, @intFromEnum(gguf_mod.tensor_types.KnownTensorType.F16));
    try appendLe(u64, allocator, &gguf_data, 0);
    try padToAlignment(allocator, &gguf_data, gguf_mod.format.default_alignment);
    try gguf_data.appendSlice(allocator, &[_]u8{ 0x00, 0x3C, 0x00, 0x40, 0x00, 0x42, 0x00, 0x44 });

    var head_data = std.ArrayListUnmanaged(u8).empty;
    defer head_data.deinit(allocator);
    try head_data.appendSlice(allocator, "GGUF");
    try appendLe(u32, allocator, &head_data, 3);
    try appendLe(u64, allocator, &head_data, 1);
    try appendLe(u64, allocator, &head_data, 1);
    try appendString(allocator, &head_data, "general.architecture");
    try appendLe(u32, allocator, &head_data, 8);
    try appendString(allocator, &head_data, "termite-gliner-head");
    try appendString(allocator, &head_data, "span_rep.test");
    try appendLe(u32, allocator, &head_data, 1);
    try appendLe(u64, allocator, &head_data, 2);
    try appendLe(u32, allocator, &head_data, @intFromEnum(gguf_mod.tensor_types.KnownTensorType.F32));
    try appendLe(u64, allocator, &head_data, 0);
    try padToAlignment(allocator, &head_data, gguf_mod.format.default_alignment);
    try head_data.appendSlice(allocator, std.mem.asBytes(&[_]f32{ 5.0, 6.0 }));

    const dir_path = try testScratchDir(allocator, "tensor-store-gliner-split");
    defer {
        compat.cwd().deleteTree(compat.io(), dir_path) catch {};
        allocator.free(dir_path);
    }
    const gguf_path = try std.fs.path.join(allocator, &.{ dir_path, "encoder.gguf" });
    defer allocator.free(gguf_path);
    const head_path = try std.fs.path.join(allocator, &.{ dir_path, "gliner_head.gguf" });
    defer allocator.free(head_path);
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = gguf_path, .data = gguf_data.items });
    try compat.cwd().writeFile(compat.io(), .{ .sub_path = head_path, .data = head_data.items });

    var manifest = manifest_mod.ModelManifest{
        .allocator = allocator,
        .gguf_path = try allocator.dupe(u8, gguf_path),
        .gliner_head_gguf_path = try allocator.dupe(u8, head_path),
        .gliner_model_type = try allocator.dupe(u8, "gliner2"),
    };
    defer manifest.deinit();

    const store = try openFromManifest(allocator, manifest);
    defer store.deinit();
    try std.testing.expectEqual(StoreKind.gguf, store.kind());
    try std.testing.expect(store.ggufFile() != null);

    const source = (try store.weightSource()) orelse return error.TestUnexpectedResult;
    const names = try source.listNames(allocator);
    defer allocator.free(names);
    try std.testing.expectEqual(@as(usize, 2), names.len);

    var head_ref = try store.describeTensor(allocator, "span_rep.test");
    defer head_ref.deinit(allocator);
    var head_loaded = try store.loadTensorRef(&head_ref);
    defer head_loaded.deinit();
    try std.testing.expectEqual(@as(usize, 2), head_loaded.tensor.elementCount());
    const first_bits = std.mem.readInt(u32, head_loaded.tensor.data[0..4], .little);
    const second_bits = std.mem.readInt(u32, head_loaded.tensor.data[4..8], .little);
    try std.testing.expectApproxEqAbs(@as(f32, 5.0), @as(f32, @bitCast(first_bits)), 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 6.0), @as(f32, @bitCast(second_bits)), 1e-6);

    var encoder_ref = try store.describeTensor(allocator, "embeddings.word_embeddings.weight");
    defer encoder_ref.deinit(allocator);
    var encoder_loaded = try store.loadTensorRef(&encoder_ref);
    defer encoder_loaded.deinit();
    try std.testing.expectEqual(@as(usize, 4), encoder_loaded.tensor.elementCount());
}

fn appendLe(comptime T: type, allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: T) !void {
    var buf: [@sizeOf(T)]u8 = undefined;
    std.mem.writeInt(T, &buf, value, .little);
    try data.appendSlice(allocator, &buf);
}

fn appendString(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), value: []const u8) !void {
    try appendLe(u64, allocator, data, value.len);
    try data.appendSlice(allocator, value);
}

fn testScratchDir(allocator: std.mem.Allocator, name: []const u8) ![]u8 {
    const root = try std.fmt.allocPrint(allocator, "termite-model-tests-{d}", .{std.posix.system.getpid()});
    defer allocator.free(root);
    const dir_path = try std.fs.path.join(allocator, &.{ ".zig-cache", "tmp", root, name });
    errdefer allocator.free(dir_path);
    compat.cwd().deleteTree(compat.io(), dir_path) catch {};
    try compat.cwd().createDirPath(compat.io(), dir_path);
    return dir_path;
}

fn padToAlignment(allocator: std.mem.Allocator, data: *std.ArrayListUnmanaged(u8), alignment: u64) !void {
    const rem = data.items.len % alignment;
    if (rem == 0) return;
    try data.appendNTimes(allocator, 0, alignment - rem);
}
