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

// WeightSource abstraction for loading model weights from different formats.
//
// Adapts SafeTensors (and eventually GGUF) into a uniform interface that
// model architectures use to load their weights.

const std = @import("std");
const Tensor = @import("../backends/tensor.zig").Tensor;
const DType = @import("../backends/tensor.zig").DType;
const gguf_tensor_types = @import("../gguf/tensor_types.zig");
const safetensors = @import("safetensors.zig");
const c_file = @import("../util/c_file.zig");

pub const QuantizedStorage = struct {
    pub const PackedExpertView = struct {
        expert_index: u32,
        expert_count: u32,
        expert_axis: u32,
        /// For fused gate+up tensors: row offset into the out_dim axis to reach
        /// the correct half (0 for w1/gate, intermediate_size for w3/up).
        row_offset: u32 = 0,
    };

    pub const PreparedGroupCache = struct {
        pub const Format = enum {
            panel16_projection_group,
        };

        /// Backend-prepared bytes for a grouped projection layout.  The
        /// format tag describes the physical packing; tensor-type eligibility
        /// stays in the kernel/capability code that builds the cache.
        format: Format,
        packed_bytes: []u8,
        partner_keys: [2][]u8,
        partner_count: u8,
        projection_count: u8,
        panel_cols: u16,
        row_blocks: u32,

        pub fn ownedBytes(self: *const PreparedGroupCache) usize {
            var total = self.packed_bytes.len;
            for (self.partnerKeys()) |key| total += key.len;
            return total;
        }

        pub fn partnerKeys(self: *const PreparedGroupCache) []const []u8 {
            return self.partner_keys[0..self.partner_count];
        }

        pub fn deinit(self: *PreparedGroupCache, allocator: std.mem.Allocator) void {
            for (self.partnerKeys()) |key| allocator.free(key);
            allocator.free(self.packed_bytes);
        }
    };

    pub const PreparedQuantLayout = enum(u8) {
        /// Row-major prepared blocks matching the source tensor rows.
        row_major_blocks,
        /// Four-row panel layout used by legacy, K16, Q8_0/Q8_1, and K-family kernels.
        panel4,
        /// Eight-row panel layout for kernels that amortize activation work across wider row tiles.
        panel8,
        /// Sixteen-row panel layout for Q/K-family wide-tile kernels.
        panel16,
        /// Q3_K compact K16 panel for the no-DMN CPU kernel.
        panel4_k16_no_min,
    };

    pub const PreparedQuantBuffer = struct {
        bytes: []u8,
        panel_cols: u16 = 0,
        row_blocks: u32 = 0,
    };

    pub const PreparedQuantCache = struct {
        const layout_count = @typeInfo(PreparedQuantLayout).@"enum".fields.len;

        entries: [layout_count]?PreparedQuantBuffer = [_]?PreparedQuantBuffer{null} ** layout_count,

        fn index(layout: PreparedQuantLayout) usize {
            return @intFromEnum(layout);
        }

        pub fn get(self: *const PreparedQuantCache, layout: PreparedQuantLayout) ?[]u8 {
            return if (self.entries[index(layout)]) |entry| entry.bytes else null;
        }

        pub fn getBuffer(self: *const PreparedQuantCache, layout: PreparedQuantLayout) ?PreparedQuantBuffer {
            return self.entries[index(layout)];
        }

        pub fn setOwned(
            self: *PreparedQuantCache,
            allocator: std.mem.Allocator,
            layout: PreparedQuantLayout,
            bytes: []u8,
            panel_cols: u16,
            row_blocks: u32,
        ) void {
            const idx = index(layout);
            if (self.entries[idx]) |old| allocator.free(old.bytes);
            self.entries[idx] = .{
                .bytes = bytes,
                .panel_cols = panel_cols,
                .row_blocks = row_blocks,
            };
        }

        pub fn ownedBytes(self: *const PreparedQuantCache) usize {
            var total: usize = 0;
            for (self.entries) |entry| {
                if (entry) |buffer| total += buffer.bytes.len;
            }
            return total;
        }

        pub fn deinit(self: *PreparedQuantCache, allocator: std.mem.Allocator) void {
            for (&self.entries) |*entry| {
                if (entry.*) |buffer| allocator.free(buffer.bytes);
                entry.* = null;
            }
        }
    };

    tensor_type: gguf_tensor_types.TensorType,
    raw_bytes: []const u8,
    shape: []const i64,
    source_name: ?[]u8 = null,
    packed_expert: ?PackedExpertView = null,
    raw_owned: bool = true,
    /// The raw bytes are borrowed from a stable mmap-backed region. This is
    /// stronger than `raw_owned == false`: stack, heap, and synthetic borrowed
    /// buffers must not be treated as safe Metal no-copy sources.
    raw_mmap_backed: bool = false,
    prepared: PreparedQuantCache = .{},
    prepared_group_cache: ?PreparedGroupCache = null,
    allocator: std.mem.Allocator,

    pub fn preparedBytes(self: *const QuantizedStorage, layout: PreparedQuantLayout) ?[]u8 {
        return self.prepared.get(layout);
    }

    pub fn preparedBuffer(self: *const QuantizedStorage, layout: PreparedQuantLayout) ?PreparedQuantBuffer {
        return self.prepared.getBuffer(layout);
    }

    pub fn setPreparedBytes(
        self: *QuantizedStorage,
        layout: PreparedQuantLayout,
        bytes: []u8,
        panel_cols: u16,
        row_blocks: u32,
    ) void {
        self.prepared.setOwned(self.allocator, layout, bytes, panel_cols, row_blocks);
    }

    pub fn deinit(self: *QuantizedStorage) void {
        if (self.prepared_group_cache) |*cache| cache.deinit(self.allocator);
        self.prepared.deinit(self.allocator);
        if (self.raw_owned) self.allocator.free(@constCast(self.raw_bytes));
        if (self.source_name) |name| self.allocator.free(name);
        self.allocator.free(self.shape);
    }
};

/// A loaded weight with optional quantization metadata.
pub const LoadedWeight = struct {
    tensor: Tensor,
    quantized: bool = false,
    quantized_storage: ?QuantizedStorage = null,

    pub fn deinit(self: *LoadedWeight) void {
        if (self.quantized_storage) |*storage| storage.deinit();
        self.tensor.deinit();
    }
};

/// Uniform interface for loading model weights regardless of storage format.
pub const WeightSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        getTensor: *const fn (*anyopaque, name: []const u8) anyerror!LoadedWeight,
        listNames: *const fn (*anyopaque, allocator: std.mem.Allocator) anyerror![][]const u8,
        deinit: *const fn (*anyopaque) void,
    };

    pub fn getTensor(self: WeightSource, name: []const u8) !LoadedWeight {
        return self.vtable.getTensor(self.ptr, name);
    }

    pub fn listNames(self: WeightSource, allocator: std.mem.Allocator) ![][]const u8 {
        return self.vtable.listNames(self.ptr, allocator);
    }

    pub fn deinit(self: WeightSource) void {
        self.vtable.deinit(self.ptr);
    }
};

/// WeightSource backed by a SafeTensors file (single file).
pub const SafetensorsSource = struct {
    reader: safetensors.MMapReader,

    const vtable = WeightSource.VTable{
        .getTensor = @ptrCast(&getTensor),
        .listNames = @ptrCast(&listNames),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn init(allocator: std.mem.Allocator, dir: std.fs.Dir, path: []const u8) !*SafetensorsSource {
        const self = try allocator.create(SafetensorsSource);
        errdefer allocator.destroy(self);
        self.* = .{
            .reader = try safetensors.MMapReader.openFile(allocator, dir, path),
        };
        return self;
    }

    /// Initialize from an absolute file path (uses C I/O, no std.fs.Dir needed).
    pub fn initAbsolute(allocator: std.mem.Allocator, path: []const u8) !*SafetensorsSource {
        const self = try allocator.create(SafetensorsSource);
        errdefer allocator.destroy(self);
        self.* = .{
            .reader = try safetensors.MMapReader.openFileAbsolute(allocator, path),
        };
        return self;
    }

    pub fn weightSource(self: *SafetensorsSource) WeightSource {
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn getTensor(self: *SafetensorsSource, name: []const u8) !LoadedWeight {
        var tensor = try self.reader.readTensor(name);
        // Convert f16/bf16 to f32 for computation
        if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
            const converted = try convertToF32(self.reader.allocator, &tensor);
            tensor.deinit();
            return .{ .tensor = converted, .quantized = false };
        }
        return .{ .tensor = tensor, .quantized = false };
    }

    fn listNames(self: *SafetensorsSource, allocator: std.mem.Allocator) ![][]const u8 {
        return self.reader.header.tensorNames(allocator);
    }

    fn deinitSelf(self: *SafetensorsSource) void {
        const allocator = self.reader.allocator;
        self.reader.deinit();
        allocator.destroy(self);
    }
};

/// WeightSource backed by a sharded SafeTensors index plus multiple shard files.
pub const ShardedSafetensorsSource = struct {
    allocator: std.mem.Allocator,
    model_dir: []const u8,
    index: safetensors.ShardedIndex,
    readers: std.StringHashMapUnmanaged(*safetensors.MMapReader),

    const vtable = WeightSource.VTable{
        .getTensor = @ptrCast(&getTensor),
        .listNames = @ptrCast(&listNames),
        .deinit = @ptrCast(&deinitSelf),
    };

    pub fn initAbsolute(allocator: std.mem.Allocator, index_path: []const u8) !*ShardedSafetensorsSource {
        const self = try allocator.create(ShardedSafetensorsSource);
        errdefer allocator.destroy(self);

        const index_bytes = try c_file.readFile(allocator, index_path);
        defer allocator.free(index_bytes);

        const model_dir_slice = std.fs.path.dirname(index_path) orelse return error.InvalidPath;
        self.* = .{
            .allocator = allocator,
            .model_dir = try allocator.dupe(u8, model_dir_slice),
            .index = try safetensors.ShardedIndex.load(allocator, index_bytes),
            .readers = .{},
        };
        return self;
    }

    pub fn weightSource(self: *ShardedSafetensorsSource) WeightSource {
        return .{ .ptr = self, .vtable = &vtable };
    }

    pub fn findTensorMeta(self: *ShardedSafetensorsSource, name: []const u8) !struct {
        reader: *safetensors.MMapReader,
        meta: safetensors.TensorMeta,
    } {
        const shard_name = self.index.weight_map.get(name) orelse return error.TensorNotFound;
        const reader = try self.getOrOpenReader(shard_name);
        const meta = reader.header.tensors.get(name) orelse return error.TensorNotFound;
        return .{ .reader = reader, .meta = meta };
    }

    fn getOrOpenReader(self: *ShardedSafetensorsSource, shard_name: []const u8) !*safetensors.MMapReader {
        if (self.readers.get(shard_name)) |reader| return reader;

        const shard_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.model_dir, shard_name });
        defer self.allocator.free(shard_path);

        const reader = try self.allocator.create(safetensors.MMapReader);
        errdefer self.allocator.destroy(reader);
        reader.* = try safetensors.MMapReader.openFileAbsolute(self.allocator, shard_path);
        errdefer reader.deinit();

        try self.readers.put(self.allocator, try self.allocator.dupe(u8, shard_name), reader);
        return reader;
    }

    fn getTensor(self: *ShardedSafetensorsSource, name: []const u8) !LoadedWeight {
        const resolved = try self.findTensorMeta(name);
        var tensor = try resolved.reader.readTensor(name);
        if (tensor.dtype == .f16 or tensor.dtype == .bf16) {
            const converted = try convertToF32(self.allocator, &tensor);
            tensor.deinit();
            return .{ .tensor = converted, .quantized = false };
        }
        return .{ .tensor = tensor, .quantized = false };
    }

    fn listNames(self: *ShardedSafetensorsSource, allocator: std.mem.Allocator) ![][]const u8 {
        var names = std.ArrayListUnmanaged([]const u8).empty;
        errdefer names.deinit(allocator);
        var it = self.index.weight_map.iterator();
        while (it.next()) |entry| {
            try names.append(allocator, entry.key_ptr.*);
        }
        return try names.toOwnedSlice(allocator);
    }

    fn deinitSelf(self: *ShardedSafetensorsSource) void {
        var reader_it = self.readers.iterator();
        while (reader_it.next()) |entry| {
            entry.value_ptr.*.deinit();
            self.allocator.destroy(entry.value_ptr.*);
            self.allocator.free(entry.key_ptr.*);
        }
        self.readers.deinit(self.allocator);
        self.index.deinit();
        self.allocator.free(self.model_dir);
        self.allocator.destroy(self);
    }
};

/// A weight mapping entry: maps a safetensors tensor name to a model scope path.
pub const WeightMapping = struct {
    tensor_name: []const u8,
    scope_path: []const u8,
};

/// Load weights from a source using a name mapping.
/// Returns a map from scope_path -> LoadedWeight.
pub fn loadWeightsFromMapping(
    allocator: std.mem.Allocator,
    source: WeightSource,
    mapping: []const WeightMapping,
) !std.StringHashMapUnmanaged(LoadedWeight) {
    var weights = std.StringHashMapUnmanaged(LoadedWeight){};
    errdefer {
        var it = weights.iterator();
        while (it.next()) |entry| {
            var w = entry.value_ptr.*;
            w.deinit();
        }
        weights.deinit(allocator);
    }

    for (mapping) |entry| {
        var weight = source.getTensor(entry.tensor_name) catch |err| {
            // Skip missing tensors (some mappings are optional)
            if (err == error.TensorNotFound) continue;
            return err;
        };
        errdefer weight.deinit();
        const owned_path = try allocator.dupe(u8, entry.scope_path);
        try weights.put(allocator, owned_path, weight);
    }

    return weights;
}

/// Convert a f16 or bf16 tensor to f32.
pub fn convertToF32(allocator: std.mem.Allocator, tensor: *const Tensor) !Tensor {
    const count = tensor.elementCount();
    const byte_count = count * @sizeOf(f32);
    const out_bytes = try allocator.alloc(u8, byte_count);
    const owned_shape = try allocator.dupe(i64, tensor.shape);
    const f32_data: [*]f32 = @ptrCast(@alignCast(out_bytes.ptr));

    switch (tensor.dtype) {
        .f16 => {
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 2;
                const half: f16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                f32_data[i] = @floatCast(half);
            }
        },
        .bf16 => {
            // bf16: 1 sign bit, 8 exponent bits, 7 mantissa bits
            // Convert by shifting left 16 bits into f32 format
            const src_bytes: [*]const u8 = tensor.data.ptr;
            for (0..count) |i| {
                const offset = i * 2;
                const bits: u16 = @bitCast([2]u8{ src_bytes[offset], src_bytes[offset + 1] });
                const f32_bits: u32 = @as(u32, bits) << 16;
                f32_data[i] = @bitCast(f32_bits);
            }
        },
        else => return error.UnsupportedConversion,
    }

    return .{
        .data = out_bytes,
        .dtype = .f32,
        .shape = owned_shape,
        .name = tensor.name,
        .allocator = allocator,
        .owns_data = true,
        .owns_shape = true,
    };
}

// -- Tests --

test "convert bf16 to f32" {
    const allocator = std.testing.allocator;

    // bf16 representation of 1.0: sign=0, exp=01111111, mantissa=0000000 => 0x3F80
    const bf16_data = [_]u16{ 0x3F80, 0x4000 }; // 1.0, 2.0
    const shape = [_]i64{2};
    const tensor = Tensor{
        .data = @constCast(std.mem.sliceAsBytes(&bf16_data)),
        .dtype = .bf16,
        .shape = &shape,
        .name = "test",
        .allocator = allocator,
        .owns_data = false,
        .owns_shape = false,
    };

    var converted = try convertToF32(allocator, &tensor);
    defer converted.deinit();

    try std.testing.expectEqual(DType.f32, converted.dtype);
    const values = converted.asFloat32();
    try std.testing.expectApproxEqAbs(@as(f32, 1.0), values[0], 1e-6);
    try std.testing.expectApproxEqAbs(@as(f32, 2.0), values[1], 1e-6);
}
