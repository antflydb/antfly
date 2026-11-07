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

var panel_epoch: std.atomic.Value(u64) = .init(1);
fn nextPanelEpoch() u64 {
    var before = panel_epoch.load(.monotonic);
    while (true) {
        // Exhaustion is unknown coverage, never a recycled representation ID.
        if (before == std.math.maxInt(u64)) return 0;
        if (panel_epoch.cmpxchgWeak(before, before + 1, .monotonic, .monotonic)) |actual| before = actual else return before;
    }
}

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
        const Owner = struct {
            allocator: std.mem.Allocator,
            references: std.atomic.Value(usize) = .init(1),
            packed_bytes: []u8,
            partner_keys: [2][]u8,
            partner_count: u8,
            backing: ?@import("../runtime/tier/cache.zig").PhysicalBacking = null,
            retirement_references: std.atomic.Value(usize) = .init(1),
            creator_deferred: bool = false,
            retirement_node: @import("../execution/resource_retirement.zig").Node = undefined,

            fn deferCreator(self: *Owner) !void {
                const backing = if (self.backing) |*value| value else return error.MissingCacheRetirementDomain;
                std.debug.assert(!self.creator_deferred);
                self.creator_deferred = true;
                self.retirement_node = .{ .context = self, .release_fn = finishDeferredCreator };
                backing.deferCreatorDrop(&self.retirement_node) catch |err| {
                    self.creator_deferred = false;
                    return err;
                };
            }
            fn finishDeferredCreator(raw: *anyopaque) callconv(.c) void {
                const self: *Owner = @ptrCast(@alignCast(raw));
                std.debug.assert(self.creator_deferred);
                self.creator_deferred = false;
                // Actual Source/Manager/final B proved outside all gates.
                self.release(.{ .outside_execution = true }) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            }

            fn release(self: *Owner, current: @import("../execution/cleanup_context.zig").Context) !void {
                if (self.backing) |*backing| try backing.validateCleanup(current);
                // Nonfinal alias drops still validate BEFORE ref mutation.
                if (self.references.fetchSub(1, .acq_rel) != 1) return;
                if (self.backing != null and !current.outside_execution) {
                    const epoch = current.boundary.?;
                    const boundary = epoch.boundary() catch |err| {
                        _ = self.references.fetchAdd(1, .release);
                        return err;
                    };
                    self.retirement_references.store(2, .release);
                    self.retirement_node = .{ .context = self, .release_fn = finishRetirement };
                    boundary.register(boundary.context, boundary.nonce, &self.retirement_node) catch |err| {
                        self.retirement_references.store(1, .release);
                        _ = self.references.fetchAdd(1, .release);
                        return err;
                    };
                }
                for (self.partner_keys[0..self.partner_count]) |key| self.allocator.free(key);
                self.allocator.free(self.packed_bytes);
                // No payload/keys remain in published callback metadata.
                self.packed_bytes = &.{};
                self.partner_keys = .{ &.{}, &.{} };
                self.partner_count = 0;
                self.dropRetirementReference();
            }
            fn finishRetirement(raw: *anyopaque) callconv(.c) void {
                const self: *Owner = @ptrCast(@alignCast(raw));
                self.dropRetirementReference();
            }
            fn dropRetirementReference(self: *Owner) void {
                if (self.retirement_references.fetchSub(1, .acq_rel) != 1) return;
                const allocator = self.allocator;
                const backing = self.backing;
                allocator.destroy(self);
                // Owner header is physically dead too. Exact component
                // ledger/refund is pure, outside actual B and all gates.
                if (backing) |held| held.releaseAfterMetadataDeathOutsideExecution();
            }
        };

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
        source_epochs: [3]u64 = @splat(0),
        retention: ?*Owner = null,

        /// Adopt already staged bytes atomically, without a second payload.
        pub fn adopt(self: *PreparedGroupCache, allocator: std.mem.Allocator) !void {
            std.debug.assert(self.retention == null);
            const owner = try allocator.create(Owner);
            owner.* = .{ .allocator = allocator, .packed_bytes = self.packed_bytes, .partner_keys = self.partner_keys, .partner_count = self.partner_count };
            self.retention = owner;
        }
        fn retain(self: PreparedGroupCache) ?PreparedGroupCache {
            const owner = self.retention orelse return null;
            const before = owner.references.fetchAdd(1, .monotonic);
            std.debug.assert(before > 0);
            return self;
        }
        fn unique(self: PreparedGroupCache) bool {
            return if (self.retention) |owner| owner.references.load(.acquire) == 1 else true;
        }

        pub fn ownerMetadataBytes() usize {
            return @sizeOf(Owner);
        }
        pub fn hasPhysicalBacking(self: *const PreparedGroupCache) bool {
            return if (self.retention) |owner| owner.backing != null else false;
        }
        pub fn attachPhysicalBacking(self: *PreparedGroupCache, backing: @import("../runtime/tier/cache.zig").PhysicalBacking) void {
            const owner = self.retention orelse @panic("group owner missing");
            std.debug.assert(owner.references.load(.acquire) == 1 and owner.backing == null);
            // A retained managed group may outlive its source Model/arena.
            // SAME canonical process allocator owns packed data, keys, header.
            std.debug.assert(owner.allocator.vtable == std.heap.c_allocator.vtable);
            std.debug.assert(backing.bytes == self.ownedBytes() + @sizeOf(Owner));
            owner.allocator = std.heap.c_allocator; // discard irrelevant caller ptr bits before late publication
            owner.backing = backing;
        }

        pub fn ownedBytes(self: *const PreparedGroupCache) usize {
            var total = self.packed_bytes.len;
            for (self.partnerKeys()) |key| total += key.len;
            return total;
        }

        pub fn partnerKeys(self: *const PreparedGroupCache) []const []u8 {
            return self.partner_keys[0..self.partner_count];
        }

        fn deferCacheCreator(self: *PreparedGroupCache) !void {
            const owner = self.retention orelse return error.MissingPreparedGroupOwner;
            try owner.deferCreator();
            self.* = undefined; // exact creator ref moved, no ref decrement
        }
        pub fn validateCleanup(self: *const PreparedGroupCache, current: @import("../execution/cleanup_context.zig").Context) !void {
            if (self.retention) |owner| if (owner.backing) |*backing| try backing.validateCleanup(current);
        }
        pub fn deinit(self: *PreparedGroupCache, allocator: std.mem.Allocator) void {
            self.deinitWithCleanup(allocator, .{}) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        }
        pub fn deinitWithCleanup(self: *PreparedGroupCache, allocator: std.mem.Allocator, current: @import("../execution/cleanup_context.zig").Context) !void {
            if (self.retention) |owner| try owner.release(current) else {
                for (self.partnerKeys()) |key| allocator.free(key);
                allocator.free(self.packed_bytes);
            }
            self.* = undefined;
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
        const layout_count = @typeInfo(PreparedQuantLayout).@"enum".field_names.len;

        entries: [layout_count]?PreparedQuantBuffer = @as([layout_count]?PreparedQuantBuffer, @splat(null)),

        fn index(layout: PreparedQuantLayout) usize {
            return @backingInt(layout);
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
    /// Operation workload supplied by the model adapter; independent of kernel names.
    dense_workload: ?@import("../ops/dense_projection_policy.zig").Workload = null,
    packed_expert: ?PackedExpertView = null,
    raw_owned: bool = true,
    /// The raw bytes are borrowed from a stable mmap-backed region. This is
    /// stronger than `raw_owned == false`: stack, heap, and synthetic borrowed
    /// buffers must not be treated as safe Metal no-copy sources.
    raw_mmap_backed: bool = false,
    /// Full mmap that owns `raw_bytes`, when available. This lets high-memory
    /// runtimes publish one model-wide GPU allocation instead of creating a
    /// separate Metal allocation for every tensor view. The slice is borrowed
    /// and has the same lifetime as the tensor store.
    raw_mmap_source_bytes: ?[]const u8 = null,
    raw_mmap_region: ?c_file.MmapRegion = null,
    prepared: PreparedQuantCache = .{},
    prepared_group_cache: ?PreparedGroupCache = null,
    /// Cache publication/retention only; never held while a numerical program
    /// executes. Optional replacement declines while an old reader is active,
    /// preserving exact model-cache charging until the bytes actually retire.
    prepared_group_mutex: std.atomic.Mutex = .unlocked,
    panel16_epoch: std.atomic.Value(u64) = .init(0),
    allocator: std.mem.Allocator,

    pub fn retainPreparedGroup(self: *const QuantizedStorage) ?PreparedGroupCache {
        const mutable = @constCast(self);
        @import("antfly_platform").sync.lockYielding(&mutable.prepared_group_mutex);
        defer mutable.prepared_group_mutex.unlock();
        const cache = self.prepared_group_cache orelse return null;
        return cache.retain();
    }
    pub const GroupDescriptor = struct {
        format: PreparedGroupCache.Format,
        packed_bytes: usize,
        projection_count: u8,
        panel_cols: u16,
        row_blocks: usize,
        source_epochs: [3]u64,
        keys: [2][]const u8,
    };
    /// Pure layout/key comparison while the real cache metadata owner is held.
    /// No retained alias, mutable owner pointer or lifetime credit escapes.
    pub fn preparedGroupMatches(self: *const QuantizedStorage, expected: GroupDescriptor) bool {
        const mutable = @constCast(self);
        @import("antfly_platform").sync.lockYielding(&mutable.prepared_group_mutex);
        defer mutable.prepared_group_mutex.unlock();
        const cache = self.prepared_group_cache orelse return false;
        for (cache.source_epochs, expected.source_epochs) |saved, current| if (saved == 0 or saved != current) return false;
        if (cache.format != expected.format or cache.projection_count != expected.projection_count or cache.panel_cols != expected.panel_cols or cache.row_blocks != expected.row_blocks or cache.packed_bytes.len != expected.packed_bytes or cache.partner_count != 2) return false;
        const keys = cache.partnerKeys();
        return std.mem.eql(u8, keys[0], expected.keys[0]) and std.mem.eql(u8, keys[1], expected.keys[1]);
    }

    pub const GroupReplacement = union(enum) { busy, replaced: ?PreparedGroupCache };
    pub fn replaceUnusedPreparedGroup(self: *QuantizedStorage, replacement: ?PreparedGroupCache) GroupReplacement {
        return self.replaceUnusedPreparedGroupImpl(replacement, true);
    }
    /// Original raw producer cannot silently destroy a modern physical owner.
    /// Its cold receiving adapter performs the typed detach explicitly.
    pub fn replaceUnusedUnchargedPreparedGroup(self: *QuantizedStorage, replacement: ?PreparedGroupCache) GroupReplacement {
        return self.replaceUnusedPreparedGroupImpl(replacement, false);
    }
    fn replaceUnusedPreparedGroupImpl(self: *QuantizedStorage, replacement: ?PreparedGroupCache, allow_backed: bool) GroupReplacement {
        @import("antfly_platform").sync.lockYielding(&self.prepared_group_mutex);
        defer self.prepared_group_mutex.unlock();
        if (self.prepared_group_cache) |cache| if (!cache.unique() or (!allow_backed and cache.hasPhysicalBacking())) return .busy;
        const previous = self.prepared_group_cache;
        self.prepared_group_cache = replacement;
        return .{ .replaced = previous };
    }
    pub fn commitPreparedGroup(self: *QuantizedStorage, candidate: *?PreparedGroupCache, growth: *@import("../runtime/tier/cache.zig").SharedCache.PreparedGrowth) !void {
        @import("antfly_platform").sync.lockYielding(&self.prepared_group_mutex);
        defer self.prepared_group_mutex.unlock();
        if (self.prepared_group_cache != null) return error.NativeWeightPreparationChanged;
        var group = candidate.* orelse return error.MissingPreparedGroupOwner;
        try growth.validatePhysicalBacking();
        try growth.commit(); // final fallible step, no supplier under this lock
        group.attachPhysicalBacking(growth.retainCommittedPhysicalBacking());
        self.prepared_group_cache = group;
        candidate.* = null;
    }
    /// Weak owner identity is compared ONLY under actual metadata exclusion.
    /// No pointer escapes; a peer retain refuses before any ledger mutation.
    pub fn commitPreparedGroupUpgrade(self: *QuantizedStorage, expected_owner: usize, growth: *@import("../runtime/tier/cache.zig").SharedCache.PreparedGrowth) !void {
        @import("antfly_platform").sync.lockYielding(&self.prepared_group_mutex);
        defer self.prepared_group_mutex.unlock();
        const group = if (self.prepared_group_cache) |*value| value else return error.NativeWeightPreparationChanged;
        const owner = group.retention orelse return error.MissingPreparedGroupOwner;
        if (@intFromPtr(owner) != expected_owner or owner.references.load(.acquire) != 1 or owner.backing != null or owner.allocator.vtable != std.heap.c_allocator.vtable) return error.NativeWeightPreparationChanged;
        try growth.validatePhysicalBacking();
        try growth.commit();
        group.attachPhysicalBacking(growth.retainCommittedPhysicalBacking());
    }

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
        if (layout == .panel16) self.panel16_epoch.store(nextPanelEpoch(), .release);
    }

    pub fn deinit(self: *QuantizedStorage) void {
        self.deinitWithCleanup(.{}) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
    pub fn validateCleanup(self: *const QuantizedStorage, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.prepared_group_cache) |*cache| try cache.validateCleanup(current);
    }
    pub fn retainedGroupLogicalBytes(self: *const QuantizedStorage) usize {
        return if (self.prepared_group_cache) |*cache| if (cache.hasPhysicalBacking()) cache.ownedBytes() else 0 else 0;
    }
    pub fn deinitWithCleanup(self: *QuantizedStorage, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.prepared_group_cache) |*cache| try cache.deinitWithCleanup(self.allocator, current);
        self.deinitStorageAfterGroup();
    }
    pub fn deinitDeferringManagedGroup(self: *QuantizedStorage) !void {
        if (self.prepared_group_cache) |*cache| {
            if (cache.hasPhysicalBacking()) try cache.deferCacheCreator() else cache.deinit(self.allocator);
        }
        self.deinitStorageAfterGroup();
    }
    fn deinitStorageAfterGroup(self: *QuantizedStorage) void {
        self.prepared.deinit(self.allocator);
        if (self.raw_mmap_region) |*region| {
            region.deinit();
        } else if (self.raw_owned) {
            self.allocator.free(@constCast(self.raw_bytes));
        }
        if (self.source_name) |name| self.allocator.free(name);
        self.allocator.free(self.shape);
    }
};

/// Quantize a dense `[out, in]` f32/f16/bf16 weight to Q8_0 blocks along
/// each row, in the layout GGUF-backed storage uses. `in` must be a multiple
/// of 32. The storage owns its bytes and shape.
pub fn quantizeDenseQ8_0(allocator: std.mem.Allocator, tensor: *const Tensor) !QuantizedStorage {
    if (tensor.shape.len != 2 or tensor.shape[0] <= 0 or tensor.shape[1] <= 0 or @rem(tensor.shape[1], 32) != 0)
        return error.InvalidQuantizedInputSize;
    const count: usize = @intCast(tensor.shape[0] * tensor.shape[1]);
    const values = try allocator.alloc(f32, count);
    defer allocator.free(values);
    const width = tensor.dtype.byteSize();
    if (tensor.data.len != count * width) return error.InvalidTensorShape;
    for (values, 0..) |*value, i| value.* = switch (tensor.dtype) {
        .f32 => @bitCast(std.mem.readInt(u32, tensor.data[i * 4 ..][0..4], .little)),
        .f16 => @floatCast(@as(f16, @bitCast(std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little)))),
        .bf16 => @bitCast(@as(u32, std.mem.readInt(u16, tensor.data[i * 2 ..][0..2], .little)) << 16),
        else => return error.UnsupportedTensorType,
    };
    const raw = try @import("../gguf/quant_codec.zig").quantizeQ8_0FromF32(allocator, values);
    errdefer allocator.free(raw);
    const shape = try allocator.dupe(i64, tensor.shape);
    return .{ .tensor_type = .{ .known = .Q8_0 }, .raw_bytes = raw, .shape = shape, .allocator = allocator };
}

test "dense Q8_0 quantization round-trips within one step of each block scale" {
    const a = std.testing.allocator;
    var values: [2 * 64]f32 = undefined;
    for (&values, 0..) |*v, i| v.* = @sin(@as(f32, @floatFromInt(i)) * 0.37) * 3;
    var tensor = try Tensor.initFloat32(a, "w", &.{ 2, 64 }, &values);
    defer tensor.deinit();
    var storage = try quantizeDenseQ8_0(a, &tensor);
    defer storage.deinit();
    try std.testing.expectEqualSlices(i64, &.{ 2, 64 }, storage.shape);
    var back: [values.len]f32 = undefined;
    try @import("../gguf/quant_codec.zig").dequantizeToFloat32(storage.tensor_type, storage.raw_bytes, &back);
    for (values, back) |want, got| try std.testing.expect(@abs(want - got) <= 3.0 / 127.0);
    var odd = try Tensor.initFloat32(a, "w", &.{ 1, 3 }, &.{ 1, 2, 3 });
    defer odd.deinit();
    try std.testing.expectError(error.InvalidQuantizedInputSize, quantizeDenseQ8_0(a, &odd));
}

/// A loaded weight with optional quantization metadata.
pub const LoadedWeight = struct {
    tensor: Tensor,
    /// The maintained resident-slot replacement API alone advances this.
    /// Immutable lazy checkpoint unload/reload retains its separate slot kind.
    source_version: u64 = 0,
    quantized: bool = false,
    quantized_storage: ?QuantizedStorage = null,

    pub fn deinit(self: *LoadedWeight) void {
        self.deinitWithCleanup(.{}) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
    pub fn deinitDeferringManagedGroup(self: *LoadedWeight) !void {
        if (self.quantized_storage) |*storage| try storage.deinitDeferringManagedGroup();
        self.tensor.deinit();
    }
    pub fn validateCleanup(self: *const LoadedWeight, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.quantized_storage) |*storage| try storage.validateCleanup(current);
    }
    pub fn deinitWithCleanup(self: *LoadedWeight, current: @import("../execution/cleanup_context.zig").Context) !void {
        if (self.quantized_storage) |*storage| try storage.deinitWithCleanup(current);
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
        const resolved = try self.findTensorMetaWithShard(name);
        return .{ .reader = resolved.reader, .meta = resolved.meta };
    }

    pub fn findTensorMetaWithShard(self: *ShardedSafetensorsSource, name: []const u8) !struct {
        shard_name: []const u8,
        reader: *safetensors.MMapReader,
        meta: safetensors.TensorMeta,
    } {
        const shard_name = self.index.weight_map.get(name) orelse return error.TensorNotFound;
        const reader = try self.getOrOpenReader(shard_name);
        const meta = reader.header.tensors.get(name) orelse return error.TensorNotFound;
        return .{ .shard_name = shard_name, .reader = reader, .meta = meta };
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

/// Convert a dense scalar tensor to f32 for backends that keep resident weights
/// in float buffers.
pub fn convertToF32(allocator: std.mem.Allocator, tensor: *const Tensor) !Tensor {
    if (tensor.dtype == .f32) return error.UnsupportedConversion;
    const data = try tensor.toOwnedF32(allocator);
    errdefer allocator.free(data);
    return Tensor.initFloat32Owned(allocator, tensor.name, tensor.shape, data);
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

test "convert i64 token ordering to f32" {
    const allocator = std.testing.allocator;

    const i64_data = [_]i64{ 0, 2048, 262143 };
    const shape = [_]i64{3};
    const tensor = Tensor{
        .data = @constCast(std.mem.sliceAsBytes(&i64_data)),
        .dtype = .i64,
        .shape = &shape,
        .name = "masked_embedding.token_ordering",
        .allocator = allocator,
        .owns_data = false,
        .owns_shape = false,
    };

    var converted = try convertToF32(allocator, &tensor);
    defer converted.deinit();

    try std.testing.expectEqual(DType.f32, converted.dtype);
    try std.testing.expectEqualSlices(f32, &.{ 0.0, 2048.0, 262143.0 }, converted.asFloat32());
}

test "dense weight conversion releases every partial allocation" {
    const a = std.testing.allocator;
    var bits = [_]u16{ 0x3f80, 0xc000, 0x4040, 0xc080 };
    const tensor = Tensor{ .data = std.mem.sliceAsBytes(&bits), .dtype = .bf16, .shape = &.{ 2, 2 }, .name = "partial", .allocator = a, .owns_data = false, .owns_shape = false };
    const Check = struct {
        fn run(allocator: std.mem.Allocator, source: *const Tensor) !void {
            var result = try convertToF32(allocator, source);
            defer result.deinit();
            try std.testing.expectEqualSlices(f32, &.{ 1, -2, 3, -4 }, result.asFloat32());
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{&tensor});
    var already_f32 = try Tensor.initFloat32(a, "unsupported", &.{1}, &.{7});
    defer already_f32.deinit();
    try std.testing.expectError(error.UnsupportedConversion, convertToF32(a, &already_f32));
    try std.testing.expectEqualSlices(f32, &.{7}, already_f32.asFloat32());
}

fn exercisePreparedGroupOwnership(a: std.mem.Allocator) !void {
    var storage = QuantizedStorage{ .tensor_type = .{ .known = .Q4_K }, .raw_bytes = &.{}, .shape = try a.dupe(i64, &.{ 1, 1 }), .raw_owned = false, .allocator = a };
    var storage_owned = true;
    defer if (storage_owned) storage.deinit();
    const bytes = try a.dupe(u8, &.{ 1, 2, 3, 4 });
    var bytes_owned = true;
    defer if (bytes_owned) a.free(bytes);
    const key_b = try a.dupe(u8, "same.b");
    var key_b_owned = true;
    defer if (key_b_owned) a.free(key_b);
    const key_c = try a.dupe(u8, "same.c");
    var key_c_owned = true;
    defer if (key_c_owned) a.free(key_c);
    var staged = QuantizedStorage.PreparedGroupCache{ .format = .panel16_projection_group, .packed_bytes = bytes, .partner_keys = .{ key_b, key_c }, .partner_count = 2, .projection_count = 3, .panel_cols = 16, .row_blocks = 1, .source_epochs = .{ 1, 2, 3 } };
    try staged.adopt(a);
    bytes_owned = false;
    key_b_owned = false;
    key_c_owned = false;
    var staged_owned = true;
    defer if (staged_owned) staged.deinit(a);
    switch (storage.replaceUnusedPreparedGroup(staged)) {
        .busy => return error.UnexpectedBusyCache,
        .replaced => |previous| try std.testing.expect(previous == null),
    }
    staged_owned = false;
    var first = storage.retainPreparedGroup() orelse return error.MissingPreparedGroup;
    var first_owned = true;
    defer if (first_owned) first.deinit(a);
    try std.testing.expect(storage.replaceUnusedPreparedGroup(null) == .busy);
    try std.testing.expectEqual(@as(usize, 16), first.ownedBytes());
    var second = storage.retainPreparedGroup() orelse return error.MissingPreparedGroup;
    defer second.deinit(a);
    first.deinit(a);
    first_owned = false;
    try std.testing.expect(storage.replaceUnusedPreparedGroup(null) == .busy);
    // The copied program owns its bytes/keys independently of the mutable
    // storage slot. Managed model teardown separately requires quiescent users.
    storage.deinit();
    storage_owned = false;
    try std.testing.expectEqualSlices(u8, &.{ 1, 2, 3, 4 }, second.packed_bytes);
    try std.testing.expectEqualStrings("same.b", second.partnerKeys()[0]);
}

test "prepared quantized group owners pin immutable payload and reject replacement during numerical use" {
    try exercisePreparedGroupOwnership(std.testing.allocator);
}

test "prepared quantized group ownership unwinds every metadata allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, exercisePreparedGroupOwnership, .{});
}

test "prepared panel identities survive cache epochs without pointer identity reuse" {
    const a = std.testing.allocator;
    var first = QuantizedStorage{ .tensor_type = .{ .known = .Q4_K }, .raw_bytes = &.{}, .shape = try a.dupe(i64, &.{ 1, 1 }), .raw_owned = false, .allocator = a };
    defer first.deinit();
    var second = QuantizedStorage{ .tensor_type = .{ .known = .Q4_K }, .raw_bytes = &.{}, .shape = try a.dupe(i64, &.{ 1, 1 }), .raw_owned = false, .allocator = a };
    defer second.deinit();
    try std.testing.expectEqual(@as(u64, 0), first.panel16_epoch.load(.acquire));
    first.setPreparedBytes(.panel16, try a.dupe(u8, &.{1}), 16, 1);
    const original = first.panel16_epoch.load(.acquire);
    second.setPreparedBytes(.panel16, try a.dupe(u8, &.{1}), 16, 1);
    try std.testing.expect(original != 0 and second.panel16_epoch.load(.acquire) != original);
    first.setPreparedBytes(.panel16, try a.dupe(u8, &.{2}), 16, 1);
    try std.testing.expect(first.panel16_epoch.load(.acquire) != original);
}

test "Native grouped physical owner holds original Domain through later cleanup both seal orders" {
    const tier = @import("../runtime/tier/cache.zig");
    const domain_mod = @import("../runtime/tier/admission_domain.zig");
    const sinks = @import("../execution/retirement_sink.zig");
    for ([_]bool{ false, true }) |close_first| {
        const domain = try domain_mod.Domain.create();
        defer domain.release();
        const pin = domain.pin();
        defer pin.release();
        var cache = tier.SharedCache.init(.{ .host_limit_bytes = 4096 });
        defer cache.deinitAdmission();
        try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .cpu, .{ .host_limit_bytes = 4096 }, .{});
        try cache.installRetirementDomainOutsideExecution(&pin);
        var group = QuantizedStorage.PreparedGroupCache{
            .format = .panel16_projection_group,
            .packed_bytes = try std.heap.c_allocator.alloc(u8, 16),
            .partner_keys = .{ try std.heap.c_allocator.dupe(u8, "k"), try std.heap.c_allocator.dupe(u8, "v") },
            .partner_count = 2,
            .projection_count = 3,
            .panel_cols = 16,
            .row_blocks = 1,
        };
        try group.adopt(std.heap.c_allocator);
        const bytes = group.ownedBytes() + QuantizedStorage.PreparedGroupCache.ownerMetadataBytes();
        var growth = try cache.snapshotIndependentGrowthLocked(.host, bytes);
        defer growth.finishOutsideExecution();
        try growth.fundOutsideExecution(&pin);
        try growth.validatePhysicalBacking();
        try growth.commit();
        group.attachPhysicalBacking(growth.retainCommittedPhysicalBacking());
        var retained = group.retain().?;
        var close_sink = try sinks.Ref.create();
        defer close_sink.release();
        var close_guard = try close_sink.enter();
        defer close_guard.finish();
        var close_b = try close_guard.retainEpoch();
        defer close_b.release();
        var later_sink = try sinks.Ref.create();
        defer later_sink.release();
        var later_guard = try later_sink.enter();
        defer later_guard.finish();
        var later_c = try later_guard.retainEpoch();
        defer later_c.release();
        // Missing authority is failure-atomic even for a nonfinal alias.
        try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, group.deinitWithCleanup(std.heap.c_allocator, .{}));
        try std.testing.expectEqual(@as(usize, 2), retained.retention.?.references.load(.acquire));
        const stale = sinks.Epoch{ .record = later_c.record, .nonce = later_c.nonce + 1 };
        try std.testing.expectError(error.RetirementEpochConsumed, group.deinitWithCleanup(std.heap.c_allocator, .{ .boundary = &stale }));
        try std.testing.expectEqual(@as(usize, 2), retained.retention.?.references.load(.acquire));
        var sealed_sink = try sinks.Ref.create();
        defer sealed_sink.release();
        var sealed_guard = try sealed_sink.enter();
        var sealed = try sealed_guard.retainEpoch();
        defer sealed.release();
        sealed_guard.finish();
        try std.testing.expectError(error.RetirementEpochSealed, group.deinitWithCleanup(std.heap.c_allocator, .{ .boundary = &sealed }));
        try std.testing.expectEqual(@as(usize, 2), retained.retention.?.references.load(.acquire));
        try group.deinitWithCleanup(std.heap.c_allocator, .{ .boundary = &close_b });
        cache.noteLogicalPhysicalOwnerRemoval(bytes);
        try cache.deinitAdmissionWithCleanup(.{ .boundary = &close_b });
        try std.testing.expectEqual(bytes, pin.controller().snapshot().host_weight_bytes);
        if (close_first) close_guard.finish();
        try std.testing.expectEqual(bytes, pin.controller().snapshot().host_weight_bytes);
        var model: std.atomic.Mutex = .unlocked;
        try std.testing.expect(model.tryLock());
        try retained.deinitWithCleanup(std.heap.c_allocator, .{ .boundary = &later_c });
        // Payload death alone must not refund under the later Model gate.
        try std.testing.expectEqual(bytes, pin.controller().snapshot().host_weight_bytes);
        model.unlock();
        later_guard.finish();
        if (!close_first) {
            try std.testing.expectEqual(bytes, pin.controller().snapshot().host_weight_bytes);
            close_guard.finish();
        }
        try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
    }
}

test "Native grouped legacy eviction moves creator until actual outside settlement" {
    const tier = @import("../runtime/tier/cache.zig");
    const owner = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var cache = tier.SharedCache.init(.{ .host_limit_bytes = 4096 });
    defer cache.deinitAdmission();
    try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .cpu, .{ .host_limit_bytes = 4096 }, .{});
    try cache.installRetirementDomainOutsideExecution(&pin);
    var group = QuantizedStorage.PreparedGroupCache{
        .format = .panel16_projection_group,
        .packed_bytes = try std.heap.c_allocator.alloc(u8, 16),
        .partner_keys = .{ &.{}, &.{} },
        .partner_count = 0,
        .projection_count = 3,
        .panel_cols = 16,
        .row_blocks = 1,
    };
    try group.adopt(std.heap.c_allocator);
    const bytes = group.ownedBytes() + QuantizedStorage.PreparedGroupCache.ownerMetadataBytes();
    var growth = try cache.snapshotIndependentGrowthLocked(.host, bytes);
    defer growth.finishOutsideExecution();
    try growth.fundOutsideExecution(&pin);
    try growth.validatePhysicalBacking();
    try growth.commit();
    group.attachPhysicalBacking(growth.retainCommittedPhysicalBacking());
    // Legacy reclaim has no current-B capability. Its creator ref moves into
    // the SAME binding's preowned queue; no payload or charge is dropped here.
    const physical = group.retention.?;
    var gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(gate.tryLock());
    try group.deferCacheCreator();
    cache.noteLogicalPhysicalOwnerRemoval(bytes);
    try std.testing.expect(physical.creator_deferred);
    try std.testing.expectEqual(@as(usize, 1), physical.references.load(.acquire));
    try std.testing.expectEqual(bytes, pin.controller().snapshot().host_weight_bytes);
    gate.unlock();
    // Actual Source/Manager releases Model, lane and Native cache locks first.
    try cache.flushReleasedOutsideExecution();
    try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
}
