// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Immutable FP32 storage belonging to a loaded GLiNER2.5 session. All access
//! requires the session/provider execution lease. No request CT, allocator,
//! cancellation callback, or source pathname is retained here.
const std = @import("std");
const build_options = @import("build_options");
const model = @import("../models/gliner_boundary.zig");
const bundle = @import("../models/gliner_boundary_bundle.zig");
const artifact = @import("../models/gliner_boundary_artifact.zig");
const Spec = @import("../models/gliner_boundary_tensor_inventory.zig").Spec;
const store_mod = @import("../models/tensor_store.zig");
const metal_tensor = @import("../backends/metal_tensor.zig");
const device = @import("gliner_boundary_device_ops.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const memory = @import("../runtime/tier/memory.zig");

pub const upload_chunk_bytes: usize = 4 * 1024 * 1024;
pub const max_weights: usize = 334;
pub const max_layers: usize = 12;
pub const max_derived: usize = 1 + 2 * max_layers;

pub const Geometry = struct {
    layers: usize,
    relative_rows: usize,
    hidden: usize,

    pub fn published(backbone: model.Backbone) Geometry {
        return .{ .layers = 12, .relative_rows = 512, .hidden = if (backbone == .small) 384 else 768 };
    }

    fn bytes(self: Geometry) !usize {
        if (self.layers == 0 or self.layers > max_layers or self.relative_rows == 0 or self.hidden == 0)
            return error.InvalidGlinerBoundaryConfig;
        return std.math.mul(usize, try std.math.mul(usize, self.relative_rows, self.hidden), 4);
    }
};

pub const Estimate = struct {
    weight_bytes: usize,
    derived_bytes: usize,
    model_device_bytes: usize,
    host_metadata_bytes: usize,
    upload_staging_bytes: usize,
    /// A strict linear has a product and biased output concurrently. The
    /// latter transfers into the derived table; the product remains transient.
    derived_preparation_device_bytes: usize,
};

pub const Stats = struct {
    generation: u64,
    resident_model_live_bytes: usize = 0,
    weight_upload_bytes: u64 = 0,
    weight_upload_calls: u64 = 0,
    derived_bytes: usize = 0,
};

pub const Owner = OwnerWithDevice(MetalDevice);
pub const Workspace = WorkspaceWithDevice(MetalDevice);

pub const WorkspaceStats = struct {
    live_bytes: usize = 0,
    capacity_bytes: usize = 0,
    generation: u64 = 0,
    epoch: u64 = 0,
    active: bool = false,
};

pub fn estimate(backbone: model.Backbone) !Estimate {
    return estimateFor(Owner, MetalDevice.metadata_bytes, artifact.specs(backbone), Geometry.published(backbone));
}

fn estimateFor(comptime OwnerType: type, metadata_bytes: usize, specs: []const Spec, geometry: Geometry) !Estimate {
    if (specs.len == 0 or specs.len > max_weights) return error.IncompleteGlinerBoundaryTensorInventory;
    var weight_bytes: usize = 0;
    var largest: usize = 0;
    for (specs) |spec| {
        const size = try shapeBytes(spec.shape);
        weight_bytes = try std.math.add(usize, weight_bytes, size);
        largest = @max(largest, size);
    }
    const derived_one = try geometry.bytes();
    const derived_count = 1 + 2 * geometry.layers;
    const derived_bytes = try std.math.mul(usize, derived_one, derived_count);
    return .{
        .weight_bytes = weight_bytes,
        .derived_bytes = derived_bytes,
        .model_device_bytes = try std.math.add(usize, weight_bytes, derived_bytes),
        .host_metadata_bytes = try std.math.add(usize, @sizeOf(OwnerType), try std.math.mul(usize, specs.len + derived_count + 2, metadata_bytes)),
        .upload_staging_bytes = @min(largest, upload_chunk_bytes),
        .derived_preparation_device_bytes = try std.math.mul(usize, derived_one, 2),
    };
}

var next_generation: std.atomic.Value(u64) = .init(1);

fn newGeneration() !u64 {
    var value = next_generation.load(.monotonic);
    while (true) {
        if (value == std.math.maxInt(u64)) return error.ResourceLimitExceeded;
        if (next_generation.cmpxchgWeak(value, value + 1, .monotonic, .monotonic)) |updated| {
            value = updated;
        } else return value;
    }
}

fn validateIdentity(identity: bundle.Identity) !void {
    if (identity.precision != .fp32) return error.UnsupportedGlinerBoundaryPrecision;
    const digests = [_]bundle.Digest{ identity.weight, identity.sidecars[0], identity.sidecars[1], identity.sidecars[2], identity.sidecars[3] };
    for (digests) |digest| {
        if (digest.size_bytes == 0) return error.GlinerBoundaryArtifactMismatch;
        for (digest.sha256) |c| if (!std.ascii.isDigit(c) and !(c >= 'a' and c <= 'f'))
            return error.GlinerBoundaryArtifactMismatch;
    }
}

fn shapeBytes(shape: []const i64) !usize {
    if (shape.len == 0 or shape.len > metal_tensor.max_dims) return error.InvalidGlinerBoundaryWeightShape;
    var elements: usize = 1;
    for (shape) |dimension| {
        if (dimension <= 0 or dimension > std.math.maxInt(i32)) return error.InvalidGlinerBoundaryWeightShape;
        elements = try std.math.mul(usize, elements, @intCast(dimension));
    }
    if (elements > std.math.maxInt(i32)) return error.ResourceLimitExceeded;
    return std.math.mul(usize, elements, 4);
}

fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}

fn runtimeName(canonical: []const u8) []const u8 {
    if (std.mem.startsWith(u8, canonical, "encoder.embeddings.") or std.mem.startsWith(u8, canonical, "encoder.encoder."))
        return canonical["encoder.".len..];
    return canonical;
}

fn OwnerWithDevice(comptime Device: type) type {
    return struct {
        const Self = @This();
        const Tensor = Device.Tensor;
        const State = enum { empty, preparing, ready, poisoned };

        allocator: std.mem.Allocator,
        identity: bundle.Identity,
        generation: u64,
        runtime_identity: ?*anyopaque = null,
        geometry: Geometry,
        specs: []const Spec,
        workspace: WorkspaceWithDevice(Device),
        weights: [max_weights]?Tensor = @splat(null),
        derived: [max_derived]?Tensor = @splat(null),
        state: State = .empty,
        counters: Stats,

        pub fn create(allocator: std.mem.Allocator, identity: bundle.Identity) !*Self {
            return createWithSpecs(allocator, identity, artifact.specs(identity.backbone), Geometry.published(identity.backbone));
        }

        fn createWithSpecs(allocator: std.mem.Allocator, identity: bundle.Identity, specs: []const Spec, geometry: Geometry) !*Self {
            try validateIdentity(identity);
            _ = try estimateFor(Self, Device.metadata_bytes, specs, geometry);
            const self = try allocator.create(Self);
            errdefer allocator.destroy(self);
            const generation = try newGeneration();
            self.* = .{ .allocator = allocator, .identity = identity, .generation = generation, .geometry = geometry, .specs = specs, .workspace = .{ .allocator = allocator, .model_generation = generation }, .counters = .{ .generation = generation } };
            return self;
        }

        pub fn destroy(self: *Self) void {
            self.workspace.deinit();
            self.clearTensors();
            const allocator = self.allocator;
            allocator.destroy(self);
        }

        pub fn isReady(self: *const Self) bool {
            return self.state == .ready;
        }

        pub fn isPreparing(self: *const Self) bool {
            return self.state == .preparing;
        }

        pub fn checkRuntime(self: *const Self, runtime_identity: *anyopaque) !void {
            if (self.state == .poisoned) return error.GlinerBoundaryResidentPoisoned;
            if (self.runtime_identity != runtime_identity) return error.ForeignGlinerBoundaryResidentRuntime;
        }

        pub fn stats(self: *const Self) Stats {
            return self.counters;
        }

        /// Caller owns admission and the exclusive provider lease throughout.
        /// The complete raw artifact and every tensor descriptor/finite value
        /// are checked before the first GPU allocation. No source path opens.
        pub fn prepare(self: *Self, store: store_mod.TensorStore, runtime_identity: *anyopaque, control: ?Control) !void {
            const source = try StoreSource.init(store);
            return self.prepareSource(source, runtime_identity, control);
        }

        fn prepareSource(self: *Self, source: anytype, runtime_identity: *anyopaque, control: ?Control) !void {
            try check(control);
            if (self.state == .ready) {
                try self.checkRuntime(runtime_identity);
                return;
            }
            if (self.state != .empty) return error.GlinerBoundaryResidentPreparationState;
            try Device.checkIdle(runtime_identity);
            if (source.count() != self.specs.len) return error.IncompleteGlinerBoundaryTensorInventory;
            const raw_artifact = source.artifactBytes();
            if (raw_artifact.len != self.identity.weight.size_bytes) return error.GlinerBoundaryArtifactMismatch;
            var hash = std.crypto.hash.sha2.Sha256.init(.{});
            var offset: usize = 0;
            while (offset < raw_artifact.len) {
                try check(control);
                const end = offset + @min(upload_chunk_bytes, raw_artifact.len - offset);
                hash.update(raw_artifact[offset..end]);
                offset = end;
            }
            const hex = std.fmt.bytesToHex(hash.finalResult(), .lower);
            if (!std.mem.eql(u8, &hex, &self.identity.weight.sha256)) return error.GlinerBoundaryArtifactMismatch;
            for (self.specs) |spec| {
                try check(control);
                const raw = try source.tensor(spec);
                if (raw.len != try shapeBytes(spec.shape)) return error.InvalidGlinerBoundaryTensorByteLength;
                var cursor: usize = 0;
                while (cursor < raw.len) : (cursor += 4) {
                    if (cursor % upload_chunk_bytes == 0) try check(control);
                    const bits = std.mem.readInt(u32, raw[cursor..][0..4], .little);
                    if (!std.math.isFinite(@as(f32, @bitCast(bits)))) return error.NonFiniteGlinerBoundaryWeight;
                }
            }
            try check(control);
            self.runtime_identity = runtime_identity;
            self.state = .preparing;
            errdefer self.abortPreparation();
            for (self.specs, 0..) |spec, index| {
                try check(control);
                const raw = try source.tensor(spec);
                var dims: [metal_tensor.max_dims]i32 = undefined;
                for (spec.shape, 0..) |dimension, axis| dims[axis] = @intCast(dimension);
                self.weights[index] = try Device.allocate(self.allocator, runtime_identity, raw.len, dims[0..spec.shape.len]);
                self.counters.resident_model_live_bytes += raw.len;
                var cursor: usize = 0;
                while (cursor < raw.len) {
                    try check(control);
                    const end = cursor + @min(upload_chunk_bytes, raw.len - cursor);
                    try Device.upload(&self.weights[index].?, cursor, raw[cursor..end]);
                    self.counters.weight_upload_bytes += end - cursor;
                    self.counters.weight_upload_calls += 1;
                    cursor = end;
                }
            }
            try check(control);
        }

        pub fn finishPreparation(self: *Self) !void {
            if (self.state != .preparing) return error.GlinerBoundaryResidentPreparationState;
            try Device.checkIdle(self.runtime_identity.?);
            for (self.weights[0..self.specs.len]) |tensor| if (tensor == null) return error.IncompleteGlinerBoundaryTensorInventory;
            for (self.derived[0 .. 1 + 2 * self.geometry.layers]) |tensor| if (tensor == null) return error.IncompleteGlinerBoundaryDerivedInventory;
            self.state = .ready;
        }

        /// Only constructor/preparation rollback may clear a partial table.
        /// Published immutable state never returns to the preparing state.
        pub fn abortPreparation(self: *Self) void {
            if (self.state != .preparing) return;
            self.clearTensors();
            self.runtime_identity = null;
            self.state = .empty;
        }

        pub fn poison(self: *Self) void {
            self.state = .poisoned;
        }

        fn clearTensors(self: *Self) void {
            for (&self.derived) |*slot| if (slot.*) |*tensor| {
                Device.release(tensor);
                slot.* = null;
            };
            for (&self.weights) |*slot| if (slot.*) |*tensor| {
                Device.release(tensor);
                slot.* = null;
            };
            self.counters.resident_model_live_bytes = 0;
            self.counters.derived_bytes = 0;
        }

        fn requireAccessible(self: *const Self, runtime_identity: *anyopaque) !void {
            try self.checkRuntime(runtime_identity);
            if (self.state != .ready and self.state != .preparing) return error.GlinerBoundaryResidentNotReady;
        }

        /// Return a retained physical tensor. A request backend must wrap it in
        /// its own CT and free that wrapper before releasing its model handle.
        /// The preparing state is accessible only to the exclusive factory
        /// preparation CB; ordinary CBs require isReady() before calling here.
        pub fn acquire(self: *const Self, name: []const u8, shape: []const i64, runtime_identity: *anyopaque) !Tensor {
            try self.requireAccessible(runtime_identity);
            for (self.specs, 0..) |spec, index| {
                if (!std.mem.eql(u8, name, spec.name) and !std.mem.eql(u8, name, runtimeName(spec.name))) continue;
                if (!std.mem.eql(i64, shape, spec.shape)) return error.InvalidGlinerBoundaryWeightShape;
                const tensor = &(self.weights[index] orelse return error.GlinerBoundaryResidentNotReady);
                return Device.retain(tensor);
            }
            return error.MissingGlinerBoundaryResidentWeight;
        }

        fn derivedIndex(self: *const Self, key: device.DerivedKey) !usize {
            return switch (key) {
                .relative_normalized => 0,
                .relative_query => |layer| if (layer < self.geometry.layers) 1 + 2 * @as(usize, layer) else error.InvalidGlinerBoundaryDerivedKey,
                .relative_key => |layer| if (layer < self.geometry.layers) 2 + 2 * @as(usize, layer) else error.InvalidGlinerBoundaryDerivedKey,
            };
        }

        fn validateDerivedShape(self: *const Self, shape: []const i64) !void {
            if (shape.len != 2 or shape[0] != @as(i64, @intCast(self.geometry.relative_rows)) or shape[1] != @as(i64, @intCast(self.geometry.hidden)))
                return error.InvalidGlinerBoundaryWeightShape;
        }

        pub fn acquireDerived(self: *const Self, key: device.DerivedKey, shape: []const i64, runtime_identity: *anyopaque) !Tensor {
            try self.requireAccessible(runtime_identity);
            try self.validateDerivedShape(shape);
            const index = try self.derivedIndex(key);
            const tensor = &(self.derived[index] orelse return error.GlinerBoundaryResidentNotReady);
            return Device.retain(tensor);
        }

        pub fn publishDerived(self: *Self, key: device.DerivedKey, shape: []const i64, tensor: *const Tensor, runtime_identity: *anyopaque) !void {
            try self.checkRuntime(runtime_identity);
            if (self.state != .preparing) return error.GlinerBoundaryResidentPreparationState;
            try Device.checkIdle(runtime_identity);
            try self.validateDerivedShape(shape);
            const index = try self.derivedIndex(key);
            if (self.derived[index] != null) return error.DuplicateGlinerBoundaryDerivedTensor;
            try Device.validatePersistent(tensor, self.allocator, runtime_identity, shape);
            self.derived[index] = try Device.retain(tensor);
            const size = try self.geometry.bytes();
            self.counters.derived_bytes += size;
            self.counters.resident_model_live_bytes += size;
        }
    };
}

const StoreSource = struct {
    store: store_mod.TensorStore,
    raw: []const u8,

    fn init(store: store_mod.TensorStore) !StoreSource {
        if (store.singleSafetensorsReader()) |reader| return .{ .store = store, .raw = reader.file_bytes };
        return .{ .store = store, .raw = store.ggufArtifactBytes() orelse return error.UnsupportedGlinerBoundaryBundle };
    }

    fn count(self: StoreSource) usize {
        if (self.store.singleSafetensorsReader()) |reader| return reader.header.tensors.count();
        return self.store.ggufFile().?.tensors.len;
    }

    fn artifactBytes(self: StoreSource) []const u8 {
        return self.raw;
    }

    fn tensor(self: StoreSource, spec: Spec) ![]const u8 {
        const size = try shapeBytes(spec.shape);
        if (self.store.singleSafetensorsReader()) |reader| {
            const meta = reader.header.tensors.get(spec.name) orelse return error.MissingGlinerBoundaryResidentWeight;
            if (meta.dtype != .f32) return error.InvalidGlinerBoundaryTensorPrecision;
            if (!std.mem.eql(i64, spec.shape, meta.shape)) return error.InvalidGlinerBoundaryWeightShape;
            if (meta.data_end < meta.data_start or meta.data_end - meta.data_start != size) return error.InvalidGlinerBoundaryTensorByteLength;
            const start = try std.math.add(u64, reader.data_offset, meta.data_start);
            const end = try std.math.add(u64, start, size);
            if (end > self.raw.len) return error.InvalidGlinerBoundaryTensorByteLength;
            return self.raw[@intCast(start)..@intCast(end)];
        }
        const file = self.store.ggufFile() orelse return error.UnsupportedGlinerBoundaryBundle;
        for (file.tensors) |entry| {
            if (!std.mem.eql(u8, entry.name, spec.name)) continue;
            switch (entry.tensor_type) {
                .known => |kind| if (kind != .F32) return error.InvalidGlinerBoundaryTensorPrecision,
                else => return error.InvalidGlinerBoundaryTensorPrecision,
            }
            if (entry.dimensions.len != spec.shape.len) return error.InvalidGlinerBoundaryWeightShape;
            for (entry.dimensions, 0..) |dimension, axis| if (dimension != @as(u64, @intCast(spec.shape[spec.shape.len - 1 - axis])))
                return error.InvalidGlinerBoundaryWeightShape;
            const end = try std.math.add(u64, entry.data_offset, size);
            if (end > self.raw.len) return error.InvalidGlinerBoundaryTensorByteLength;
            return self.raw[@intCast(entry.data_offset)..@intCast(end)];
        }
        return error.MissingGlinerBoundaryResidentWeight;
    }
};

const MetalDevice = struct {
    const Tensor = metal_tensor.MetalTensor;
    const Ref = @typeInfo(@TypeOf(@as(metal_tensor.DeviceStorage, undefined).ref)).pointer.child;
    const metadata_bytes = @sizeOf(Ref);

    extern fn termite_metal_decode_runtime_has_active_frame(runtime_identity: *anyopaque) c_int;
    extern fn termite_metal_decode_runtime_has_submitted_frame(runtime_identity: *anyopaque) c_int;
    extern fn termite_metal_decode_runtime_gliner_boundary_ready(runtime_identity: *anyopaque) c_int;

    fn checkIdle(runtime_identity: *anyopaque) !void {
        if (comptime !build_options.enable_metal) return error.MetalUnavailable;
        if (termite_metal_decode_runtime_gliner_boundary_ready(runtime_identity) == 0)
            return error.UnsupportedGlinerBoundaryDevice;
        if (termite_metal_decode_runtime_has_active_frame(runtime_identity) != 0 or termite_metal_decode_runtime_has_submitted_frame(runtime_identity) != 0)
            return error.GlinerBoundaryExternalFrame;
    }

    fn allocate(allocator: std.mem.Allocator, runtime_identity: *anyopaque, size: usize, dims: []const i32) !Tensor {
        return Tensor.deviceAllocateFreshWithAllocator(allocator, runtime_identity, size, .private, dims);
    }

    fn upload(tensor: *Tensor, offset: usize, raw: []const u8) !void {
        const dims = [_]i32{@intCast(raw.len / 4)};
        var part = try tensor.retainedView(offset, raw.len, &dims);
        defer part.deinit();
        try part.uploadBytes(raw);
    }

    fn retain(tensor: *const Tensor) !Tensor {
        return tensor.retainedCopy();
    }

    fn release(tensor: *Tensor) void {
        tensor.deinit();
    }

    fn validatePersistent(tensor: *const Tensor, allocator: std.mem.Allocator, runtime_identity: *anyopaque, shape: []const i64) !void {
        const storage = tensor.device orelse return error.GlinerBoundaryRequiresResidentInput;
        if (storage.ref.runtime != runtime_identity) return error.ForeignGlinerBoundaryResidentRuntime;
        if (tensor.dtype != .f32 or tensor.deviceByteLen() != try shapeBytes(shape) or tensor.shape().len != shape.len)
            return error.InvalidGlinerBoundaryWeightShape;
        for (tensor.shape(), shape) |actual, expected| if (actual != expected) return error.InvalidGlinerBoundaryWeightShape;
        const backing = storage.ref.allocator;
        // Stateless global allocators intentionally have an undefined context
        // pointer. Only stateful allocator identities may compare that field.
        const global_lifetime = backing.vtable == std.heap.c_allocator.vtable or
            backing.vtable == std.heap.smp_allocator.vtable or backing.vtable == std.heap.page_allocator.vtable;
        if (!global_lifetime and !(backing.vtable == allocator.vtable and backing.ptr == allocator.ptr))
            return error.ForeignGlinerBoundaryResidentAllocator;
    }

    fn view(tensor: *const Tensor, offset: usize, size: usize, dims: []const i32) !Tensor {
        return tensor.retainedView(offset, size, dims);
    }

    fn exclusive(tensor: *const Tensor) bool {
        const storage = tensor.device orelse return false;
        return storage.ref.ref_count == 1;
    }
};

/// One admitted physical arena per model. Slot offsets and shapes remain
/// request-owned. There is no shape cache, implicit growth, or allocation during
/// borrow. The caller acquires replacement admission outside execution locks;
/// all methods that touch tensors run under the model/provider lease.
fn WorkspaceWithDevice(comptime Device: type) type {
    return struct {
        const Self = @This();
        const Tensor = Device.Tensor;

        allocator: std.mem.Allocator,
        model_generation: u64,
        runtime_identity: ?*anyopaque = null,
        tensor: ?Tensor = null,
        lease: ?memory.AdmissionLease = null,
        capacity_bytes: usize = 0,
        generation: u64 = 0,
        epoch: u64 = 0,
        active: bool = false,

        pub const Pending = struct {
            model_generation: u64,
            runtime_identity: *anyopaque,
            capacity_bytes: usize,
            tensor: ?Tensor,
            lease: ?memory.AdmissionLease,

            pub fn deinit(self: *Pending) void {
                // The lease remains owned until physical storage is released.
                if (self.tensor) |*tensor| Device.release(tensor);
                self.tensor = null;
                if (self.lease) |*lease| lease.release();
                self.lease = null;
            }
        };

        pub const Borrow = struct {
            workspace: ?*Self,
            generation: u64,
            epoch: u64,

            fn checked(self: *const Borrow) !*Self {
                const workspace = self.workspace orelse return error.GlinerBoundaryWorkspaceBorrowReleased;
                if (!workspace.active or workspace.generation != self.generation or workspace.epoch != self.epoch)
                    return error.GlinerBoundaryWorkspaceGenerationMismatch;
                return workspace;
            }

            pub fn view(self: *const Borrow, offset_bytes: usize, shape: []const i64) !Tensor {
                const workspace = try self.checked();
                const size = try shapeBytes(shape);
                if (offset_bytes % 4 != 0 or offset_bytes > workspace.capacity_bytes or size > workspace.capacity_bytes - offset_bytes)
                    return error.InvalidGlinerBoundaryWorkspaceRange;
                var dims: [metal_tensor.max_dims]i32 = undefined;
                for (shape, 0..) |dimension, axis| dims[axis] = @intCast(dimension);
                return Device.view(&workspace.tensor.?, offset_bytes, size, dims[0..shape.len]);
            }

            /// Drain commands and release every retained view first. A failed
            /// finish leaves the borrow active, preventing reuse or replacement.
            pub fn finish(self: *Borrow) !void {
                if (self.workspace == null) return;
                const workspace = try self.checked();
                try Device.checkIdle(workspace.runtime_identity.?);
                if (!Device.exclusive(&workspace.tensor.?)) return error.GlinerBoundaryWorkspaceViewsOutstanding;
                workspace.active = false;
                self.workspace = null;
            }
        };

        pub fn replacementAmounts(capacity_bytes: usize) !memory.AdmissionAmounts {
            if (capacity_bytes == 0 or capacity_bytes % 4 != 0 or capacity_bytes / 4 > std.math.maxInt(i32))
                return error.InvalidGlinerBoundaryWorkspaceRange;
            return .{ .host_scratch_bytes = Device.metadata_bytes, .backend_scratch_bytes = capacity_bytes };
        }

        /// The entire replacement is charged while any old generation remains
        /// resident. A lease carrying model/request resources cannot be stolen.
        pub fn prepareReplacement(self: *Self, runtime_identity: *anyopaque, model_generation: u64, capacity_bytes: usize, permit: *memory.AdmissionLease, control: ?Control) !Pending {
            try check(control);
            if (model_generation != self.model_generation) return error.GlinerBoundaryWorkspaceGenerationMismatch;
            if (self.runtime_identity) |bound| if (bound != runtime_identity) return error.ForeignGlinerBoundaryResidentRuntime;
            if (self.active) return error.GlinerBoundaryWorkspaceBusy;
            try Device.checkIdle(runtime_identity);
            const amounts = try replacementAmounts(capacity_bytes);
            if (permit.controller == null or permit.retain_backend_class != .gpu or
                !std.meta.eql(permit.amounts, amounts))
                return error.InvalidGlinerBoundaryWorkspaceAdmission;
            for (permit.amounts_by_backend, 0..) |backend_amounts, index| {
                const expected: memory.AdmissionAmounts = if (index == @intFromEnum(memory.BackendClass.gpu)) amounts else .{};
                if (!std.meta.eql(backend_amounts, expected)) return error.InvalidGlinerBoundaryWorkspaceAdmission;
            }
            const dims = [_]i32{@intCast(capacity_bytes / 4)};
            var tensor = try Device.allocate(self.allocator, runtime_identity, capacity_bytes, &dims);
            errdefer Device.release(&tensor);
            try check(control);
            try permit.retain(amounts);
            const owned_permit = permit.*;
            permit.* = .{ .controller = null, .amounts = .{}, .amounts_by_backend = @splat(.{}), .retain_backend_class = null, .live_reserved_bytes = 0 };
            return .{ .model_generation = model_generation, .runtime_identity = runtime_identity, .capacity_bytes = capacity_bytes, .tensor = tensor, .lease = owned_permit };
        }

        pub fn install(self: *Self, pending: *Pending) !void {
            if (self.active) return error.GlinerBoundaryWorkspaceBusy;
            if (pending.model_generation != self.model_generation or pending.tensor == null or pending.lease == null)
                return error.GlinerBoundaryWorkspaceGenerationMismatch;
            if (self.runtime_identity) |bound| if (bound != pending.runtime_identity) return error.ForeignGlinerBoundaryResidentRuntime;
            try Device.checkIdle(pending.runtime_identity);
            if (self.tensor) |*tensor| if (!Device.exclusive(tensor)) return error.GlinerBoundaryWorkspaceViewsOutstanding;
            const generation = try std.math.add(u64, self.generation, 1);
            // All checks precede this ownership swap. No fallible operation may
            // publish half a workspace or release capacity before its buffers.
            if (self.tensor) |*tensor| Device.release(tensor);
            if (self.lease) |*lease| lease.release();
            self.tensor = pending.tensor;
            self.lease = pending.lease;
            self.capacity_bytes = pending.capacity_bytes;
            self.runtime_identity = pending.runtime_identity;
            self.generation = generation;
            pending.tensor = null;
            pending.lease = null;
        }

        pub fn begin(self: *Self, runtime_identity: *anyopaque, model_generation: u64) !Borrow {
            if (model_generation != self.model_generation) return error.GlinerBoundaryWorkspaceGenerationMismatch;
            if (self.runtime_identity != runtime_identity) return error.ForeignGlinerBoundaryResidentRuntime;
            if (self.active) return error.GlinerBoundaryWorkspaceBusy;
            if (self.tensor == null) return error.GlinerBoundaryWorkspaceNotPrepared;
            try Device.checkIdle(runtime_identity);
            if (!Device.exclusive(&self.tensor.?)) return error.GlinerBoundaryWorkspaceViewsOutstanding;
            self.epoch = try std.math.add(u64, self.epoch, 1);
            self.active = true;
            return .{ .workspace = self, .generation = self.generation, .epoch = self.epoch };
        }

        pub fn stats(self: *const Self) WorkspaceStats {
            return .{ .live_bytes = if (self.tensor != null) self.capacity_bytes else 0, .capacity_bytes = self.capacity_bytes, .generation = self.generation, .epoch = self.epoch, .active = self.active };
        }

        /// Scalar accounting only; no lease token or release authority escapes.
        /// Read under the execution lock or after the model owner has proved
        /// there are no active handles and blocked acquisition of new handles.
        pub fn admittedAmounts(self: *const Self) memory.AdmissionAmounts {
            return if (self.lease) |lease| lease.amounts else .{};
        }

        pub fn deinit(self: *Self) void {
            std.debug.assert(!self.active);
            if (self.tensor) |*tensor| {
                std.debug.assert(Device.exclusive(tensor));
                Device.release(tensor);
            }
            self.tensor = null;
            if (self.lease) |*lease| lease.release();
            self.lease = null;
            self.capacity_bytes = 0;
        }
    };
}

const TestDevice = struct {
    const Context = struct {
        live: usize = 0,
        uploads: usize = 0,
        fail_upload: ?usize = null,
        active_frame: bool = false,
    };
    const Ref = struct {
        allocator: std.mem.Allocator,
        context: *Context,
        refs: usize = 1,
        raw: []u8,
    };
    const Tensor = struct {
        ref: *Ref,
        dims: [metal_tensor.max_dims]i32 = @splat(0),
        rank: usize,
        offset: usize = 0,
        byte_len: usize,
    };
    const metadata_bytes = @sizeOf(Ref);

    fn context(raw: *anyopaque) *Context {
        return @ptrCast(@alignCast(raw));
    }

    fn checkIdle(raw: *anyopaque) !void {
        if (context(raw).active_frame) return error.GlinerBoundaryExternalFrame;
    }

    fn allocate(allocator: std.mem.Allocator, runtime_identity: *anyopaque, size: usize, dims: []const i32) !Tensor {
        const ref = try allocator.create(Ref);
        errdefer allocator.destroy(ref);
        ref.* = .{ .allocator = allocator, .context = context(runtime_identity), .raw = try allocator.alloc(u8, size) };
        @memset(ref.raw, 0);
        ref.context.live += 1;
        var result = Tensor{ .ref = ref, .rank = dims.len, .byte_len = size };
        @memcpy(result.dims[0..dims.len], dims);
        return result;
    }

    fn upload(tensor: *Tensor, offset: usize, raw: []const u8) !void {
        const ctx = tensor.ref.context;
        ctx.uploads += 1;
        if (ctx.fail_upload == ctx.uploads) return error.TestUploadFailed;
        @memcpy(tensor.ref.raw[tensor.offset + offset ..][0..raw.len], raw);
    }

    fn retain(tensor: *const Tensor) !Tensor {
        tensor.ref.refs += 1;
        return tensor.*;
    }

    fn release(tensor: *Tensor) void {
        const ref = tensor.ref;
        ref.refs -= 1;
        if (ref.refs == 0) {
            ref.context.live -= 1;
            const allocator = ref.allocator;
            allocator.free(ref.raw);
            allocator.destroy(ref);
        }
        tensor.* = undefined;
    }

    fn validatePersistent(tensor: *const Tensor, allocator: std.mem.Allocator, runtime_identity: *anyopaque, shape: []const i64) !void {
        if (tensor.ref.context != context(runtime_identity)) return error.ForeignGlinerBoundaryResidentRuntime;
        if (tensor.ref.allocator.ptr != allocator.ptr or tensor.ref.allocator.vtable != allocator.vtable)
            return error.ForeignGlinerBoundaryResidentAllocator;
        if (tensor.rank != shape.len or tensor.byte_len != try shapeBytes(shape)) return error.InvalidGlinerBoundaryWeightShape;
        for (tensor.dims[0..tensor.rank], shape) |actual, expected| if (actual != expected) return error.InvalidGlinerBoundaryWeightShape;
    }

    fn view(tensor: *const Tensor, offset: usize, size: usize, dims: []const i32) !Tensor {
        var result = try retain(tensor);
        result.offset += offset;
        result.byte_len = size;
        result.rank = dims.len;
        @memcpy(result.dims[0..dims.len], dims);
        return result;
    }

    fn exclusive(tensor: *const Tensor) bool {
        return tensor.ref.refs == 1;
    }
};

const TestOwner = OwnerWithDevice(TestDevice);
const test_specs = [_]Spec{
    .{ .name = "encoder.embeddings.word_embeddings.weight", .shape = &.{ 2, 2 } },
    .{ .name = "encoder.encoder.LayerNorm.weight", .shape = &.{2} },
};
const test_geometry = Geometry{ .layers = 1, .relative_rows = 2, .hidden = 2 };
const test_keys = [_]device.DerivedKey{ .relative_normalized, .{ .relative_query = 0 }, .{ .relative_key = 0 } };

const TestSource = struct {
    raw: [24]u8 = @splat(0),
    missing: bool = false,

    fn count(_: TestSource) usize {
        return test_specs.len;
    }

    fn artifactBytes(self: *const TestSource) []const u8 {
        return &self.raw;
    }

    fn tensor(self: *const TestSource, spec: Spec) ![]const u8 {
        if (self.missing) return error.MissingGlinerBoundaryResidentWeight;
        if (std.mem.eql(u8, spec.name, test_specs[0].name)) return self.raw[0..16];
        if (std.mem.eql(u8, spec.name, test_specs[1].name)) return self.raw[16..24];
        return error.MissingGlinerBoundaryResidentWeight;
    }

    fn identity(self: *const TestSource) bundle.Identity {
        return .{ .backbone = .small, .precision = .fp32, .weight = bundle.Digest.of(&self.raw), .sidecars = @splat(bundle.Digest.of("sidecar")) };
    }
};

fn testPopulate(owner: *TestOwner, source: *const TestSource, ctx: *TestDevice.Context) !void {
    try owner.prepareSource(source, ctx, null);
    for (test_keys) |key| {
        var tensor = try TestDevice.allocate(owner.allocator, ctx, 16, &.{ 2, 2 });
        defer TestDevice.release(&tensor);
        try owner.publishDerived(key, &.{ 2, 2 }, &tensor, ctx);
    }
    try owner.finishPreparation();
}

test "gliner boundary resident model owns exact weights derived slots and runtime generation" {
    const a = std.testing.allocator;
    const source = TestSource{};
    var ctx = TestDevice.Context{};
    var other_runtime = TestDevice.Context{};
    const owner = try TestOwner.createWithSpecs(a, source.identity(), &test_specs, test_geometry);
    defer owner.destroy();
    try testPopulate(owner, &source, &ctx);
    try std.testing.expect(owner.isReady());
    try std.testing.expectEqual(@as(usize, 24 + 3 * 16), owner.stats().resident_model_live_bytes);
    try std.testing.expectEqual(@as(u64, 24), owner.stats().weight_upload_bytes);
    var tensor = try owner.acquire("embeddings.word_embeddings.weight", &.{ 2, 2 }, &ctx);
    defer TestDevice.release(&tensor);
    try std.testing.expectEqualSlices(u8, source.raw[0..16], tensor.ref.raw);
    try std.testing.expectError(error.ForeignGlinerBoundaryResidentRuntime, owner.acquire(test_specs[0].name, &.{ 2, 2 }, &other_runtime));
    try std.testing.expectError(error.InvalidGlinerBoundaryWeightShape, owner.acquire(test_specs[0].name, &.{4}, &ctx));
    try std.testing.expectError(error.MissingGlinerBoundaryResidentWeight, owner.acquire("missing", &.{ 2, 2 }, &ctx));
    const old_uploads = ctx.uploads;
    try owner.prepareSource(&source, &ctx, null);
    try std.testing.expectEqual(old_uploads, ctx.uploads);
    try std.testing.expectError(error.GlinerBoundaryResidentPreparationState, owner.publishDerived(.relative_normalized, &.{ 2, 2 }, &tensor, &ctx));
    const other = try TestOwner.createWithSpecs(a, source.identity(), &test_specs, test_geometry);
    defer other.destroy();
    try std.testing.expect(owner.generation != other.generation);
    owner.poison();
    try std.testing.expectError(error.GlinerBoundaryResidentPoisoned, owner.acquire(test_specs[0].name, &.{ 2, 2 }, &ctx));
}

test "gliner boundary resident rejects partial wrong identity and nonfinite before allocation" {
    const a = std.testing.allocator;
    var source = TestSource{};
    var ctx = TestDevice.Context{};
    const owner = try TestOwner.createWithSpecs(a, source.identity(), &test_specs, test_geometry);
    defer owner.destroy();
    source.missing = true;
    try std.testing.expectError(error.MissingGlinerBoundaryResidentWeight, owner.prepareSource(&source, &ctx, null));
    source.missing = false;
    source.raw[0] = 1;
    try std.testing.expectError(error.GlinerBoundaryArtifactMismatch, owner.prepareSource(&source, &ctx, null));
    try std.testing.expectEqual(@as(usize, 0), ctx.live);
    try std.testing.expectEqual(@as(usize, 0), ctx.uploads);
    source.raw[0] = 0;
    std.mem.writeInt(u32, source.raw[0..4], 0x7f800000, .little);
    const nonfinite = try TestOwner.createWithSpecs(a, source.identity(), &test_specs, test_geometry);
    defer nonfinite.destroy();
    try std.testing.expectError(error.NonFiniteGlinerBoundaryWeight, nonfinite.prepareSource(&source, &ctx, null));
    try std.testing.expectEqual(@as(usize, 0), ctx.live);
    var reduced = source.identity();
    reduced.precision = .fp16_encoder;
    try std.testing.expectError(error.UnsupportedGlinerBoundaryPrecision, TestOwner.createWithSpecs(a, reduced, &test_specs, test_geometry));
}

test "gliner boundary resident failed preparation frees prefix and retries without partial publication" {
    const a = std.testing.allocator;
    const source = TestSource{};
    var ctx = TestDevice.Context{ .fail_upload = 2 };
    const owner = try TestOwner.createWithSpecs(a, source.identity(), &test_specs, test_geometry);
    defer owner.destroy();
    try std.testing.expectError(error.TestUploadFailed, owner.prepareSource(&source, &ctx, null));
    try std.testing.expectEqual(@as(usize, 0), ctx.live);
    try std.testing.expectEqual(@as(usize, 0), owner.stats().resident_model_live_bytes);
    try std.testing.expect(!owner.isReady() and !owner.isPreparing());
    ctx.fail_upload = null;
    try owner.prepareSource(&source, &ctx, null);
    try std.testing.expectError(error.IncompleteGlinerBoundaryDerivedInventory, owner.finishPreparation());
    var derived = try TestDevice.allocate(a, &ctx, 16, &.{ 2, 2 });
    defer TestDevice.release(&derived);
    try owner.publishDerived(.relative_normalized, &.{ 2, 2 }, &derived, &ctx);
    try std.testing.expectError(error.DuplicateGlinerBoundaryDerivedTensor, owner.publishDerived(.relative_normalized, &.{ 2, 2 }, &derived, &ctx));
    try std.testing.expectError(error.InvalidGlinerBoundaryDerivedKey, owner.publishDerived(.{ .relative_query = 1 }, &.{ 2, 2 }, &derived, &ctx));
    owner.abortPreparation();
    try std.testing.expectEqual(@as(usize, 1), ctx.live); // The caller's own derived tensor remains alive.
    try testPopulate(owner, &source, &ctx);
    try std.testing.expect(owner.isReady());
}

test "gliner boundary resident cancellation and external frame preserve empty owner" {
    const Cancel = struct {
        fn checkControl(raw: ?*anyopaque) !void {
            const ctx: *const TestDevice.Context = @ptrCast(@alignCast(raw.?));
            if (ctx.uploads != 0) return error.Cancelled;
        }
    };
    const a = std.testing.allocator;
    const source = TestSource{};
    var ctx = TestDevice.Context{};
    const owner = try TestOwner.createWithSpecs(a, source.identity(), &test_specs, test_geometry);
    defer owner.destroy();
    try std.testing.expectError(error.Cancelled, owner.prepareSource(&source, &ctx, .{ .ptr = &ctx, .check_fn = Cancel.checkControl }));
    try std.testing.expectEqual(@as(usize, 0), ctx.live);
    try std.testing.expect(!owner.isReady() and !owner.isPreparing());
    ctx.active_frame = true;
    try std.testing.expectError(error.GlinerBoundaryExternalFrame, owner.prepareSource(&source, &ctx, null));
    try std.testing.expectEqual(@as(usize, 0), ctx.live);
}

fn testAllocationFailures(a: std.mem.Allocator) !void {
    const source = TestSource{};
    var ctx = TestDevice.Context{};
    defer std.debug.assert(ctx.live == 0);
    const owner = try TestOwner.createWithSpecs(a, source.identity(), &test_specs, test_geometry);
    defer owner.destroy();
    try testPopulate(owner, &source, &ctx);
    var weight = try owner.acquire(test_specs[0].name, test_specs[0].shape, &ctx);
    defer TestDevice.release(&weight);
    var relative = try owner.acquireDerived(.{ .relative_key = 0 }, &.{ 2, 2 }, &ctx);
    defer TestDevice.release(&relative);
}

test "gliner boundary resident allocation failures release every owned tensor and metadata" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testAllocationFailures, .{});
}

test "gliner boundary resident model estimate includes constants and bounded upload staging" {
    for ([_]model.Backbone{ .small, .base, .multi }) |backbone| {
        const result = try estimate(backbone);
        var exact: usize = 0;
        for (artifact.specs(backbone)) |spec| exact += try shapeBytes(spec.shape);
        const geometry = Geometry.published(backbone);
        try std.testing.expectEqual(exact, result.weight_bytes);
        try std.testing.expectEqual((1 + 2 * geometry.layers) * geometry.relative_rows * geometry.hidden * 4, result.derived_bytes);
        try std.testing.expectEqual(result.weight_bytes + result.derived_bytes, result.model_device_bytes);
        try std.testing.expect(result.host_metadata_bytes >= @sizeOf(Owner));
        try std.testing.expectEqual(upload_chunk_bytes, result.upload_staging_bytes);
        try std.testing.expect(result.derived_preparation_device_bytes <= result.derived_bytes);
    }
}

test "gliner boundary resident workspace admits full replacement and retains old generation on failure" {
    const W = WorkspaceWithDevice(TestDevice);
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var ctx = TestDevice.Context{};
    var workspace = W{ .allocator = std.testing.allocator, .model_generation = 7 };
    defer workspace.deinit();
    const limits = memory.Limits{ .backend_limit_bytes = 128 };
    var permit = try controller.tryAcquire(.gpu, limits, try W.replacementAmounts(32), false);
    defer permit.release();
    var first = try workspace.prepareReplacement(&ctx, 7, 32, &permit, null);
    defer first.deinit();
    try std.testing.expect(permit.controller == null);
    try workspace.install(&first);
    const first_generation = workspace.generation;
    try std.testing.expectEqual(@as(usize, 32), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(try W.replacementAmounts(32), workspace.admittedAmounts());

    // A delta-only permit cannot cover coexistence of old and new allocations.
    var delta = try controller.tryAcquire(.gpu, limits, try W.replacementAmounts(32), false);
    defer delta.release();
    try std.testing.expectError(error.InvalidGlinerBoundaryWorkspaceAdmission, workspace.prepareReplacement(&ctx, 7, 64, &delta, null));
    try std.testing.expectEqual(@as(usize, 1), ctx.live);
    try std.testing.expectEqual(first_generation, workspace.generation);
    delta.release();

    var full = try controller.tryAcquire(.gpu, limits, try W.replacementAmounts(64), false);
    defer full.release();
    var replacement = try workspace.prepareReplacement(&ctx, 7, 64, &full, null);
    defer replacement.deinit();
    try std.testing.expectEqual(@as(usize, 96), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2), ctx.live);
    try workspace.install(&replacement);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(try W.replacementAmounts(64), workspace.admittedAmounts());
    try std.testing.expectEqual(@as(usize, 1), ctx.live);
    try std.testing.expectEqual(first_generation + 1, workspace.generation);
    workspace.deinit();
    try std.testing.expectEqual(@as(usize, 0), ctx.live);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, controller.snapshot());
    try std.testing.expectEqual(memory.AdmissionAmounts{}, workspace.admittedAmounts());
}

test "gliner boundary resident workspace rejects active frames views and stale borrow epochs" {
    const W = WorkspaceWithDevice(TestDevice);
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var ctx = TestDevice.Context{};
    var workspace = W{ .allocator = std.testing.allocator, .model_generation = 11 };
    defer workspace.deinit();
    var permit = try controller.tryAcquire(.gpu, .{}, try W.replacementAmounts(32), false);
    defer permit.release();
    var pending = try workspace.prepareReplacement(&ctx, 11, 32, &permit, null);
    defer pending.deinit();
    try workspace.install(&pending);
    var borrow = try workspace.begin(&ctx, 11);
    defer borrow.finish() catch unreachable;
    const stale = borrow;
    var view = try borrow.view(16, &.{ 2, 2 });
    var view_alive = true;
    defer if (view_alive) TestDevice.release(&view);
    try std.testing.expectEqual(@as(usize, 16), view.offset);
    try std.testing.expectError(error.GlinerBoundaryWorkspaceViewsOutstanding, borrow.finish());
    try std.testing.expect(workspace.stats().active);
    try std.testing.expectError(error.GlinerBoundaryWorkspaceBusy, workspace.begin(&ctx, 11));
    try std.testing.expectError(error.InvalidGlinerBoundaryWorkspaceRange, borrow.view(20, &.{ 2, 2 }));
    try std.testing.expectError(error.InvalidGlinerBoundaryWorkspaceRange, borrow.view(1, &.{1}));
    TestDevice.release(&view);
    view_alive = false;
    ctx.active_frame = true;
    defer ctx.active_frame = false;
    try std.testing.expectError(error.GlinerBoundaryExternalFrame, borrow.finish());
    ctx.active_frame = false;
    try borrow.finish();
    var next = try workspace.begin(&ctx, 11);
    defer next.finish() catch unreachable;
    try std.testing.expectError(error.GlinerBoundaryWorkspaceGenerationMismatch, stale.view(0, &.{1}));
    try std.testing.expect(next.epoch > stale.epoch);
}

test "gliner boundary resident workspace cannot consume a request or CPU permit" {
    const W = WorkspaceWithDevice(TestDevice);
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var ctx = TestDevice.Context{};
    var workspace = W{ .allocator = std.testing.allocator, .model_generation = 11 };
    defer workspace.deinit();
    const exact = try W.replacementAmounts(32);
    const cases = [_]struct { backend: memory.BackendClass, amounts: memory.AdmissionAmounts }{
        .{ .backend = .cpu, .amounts = exact },
        .{ .backend = .gpu, .amounts = .{ .host_scratch_bytes = exact.host_scratch_bytes + 1, .backend_scratch_bytes = 32 } },
        .{ .backend = .gpu, .amounts = .{ .host_scratch_bytes = exact.host_scratch_bytes, .backend_scratch_bytes = 64 } },
        .{ .backend = .gpu, .amounts = .{ .host_scratch_bytes = exact.host_scratch_bytes, .backend_scratch_bytes = 32, .backend_weight_bytes = 4 } },
    };
    for (cases) |case| {
        var permit = try controller.tryAcquire(case.backend, .{}, case.amounts, false);
        defer permit.release();
        const before = controller.snapshot();
        try std.testing.expectError(error.InvalidGlinerBoundaryWorkspaceAdmission, workspace.prepareReplacement(&ctx, 11, 32, &permit, null));
        try std.testing.expectEqual(before, controller.snapshot());
        try std.testing.expect(permit.controller != null);
        try std.testing.expectEqual(@as(usize, 0), ctx.live);
        try std.testing.expectEqual(@as(u64, 0), workspace.generation);
    }
}

fn testWorkspaceAllocationFailures(a: std.mem.Allocator) !void {
    const W = WorkspaceWithDevice(TestDevice);
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var ctx = TestDevice.Context{};
    defer std.debug.assert(ctx.live == 0);
    var workspace = W{ .allocator = a, .model_generation = 3 };
    defer workspace.deinit();
    var permit = try controller.tryAcquire(.gpu, .{}, try W.replacementAmounts(16), false);
    defer permit.release();
    var first = try workspace.prepareReplacement(&ctx, 3, 16, &permit, null);
    defer first.deinit();
    try workspace.install(&first);
    var second_permit = try controller.tryAcquire(.gpu, .{}, try W.replacementAmounts(64), false);
    defer second_permit.release();
    var replacement = try workspace.prepareReplacement(&ctx, 3, 64, &second_permit, null);
    defer replacement.deinit();
    // An abandoned successful preparation also frees only the private pending
    // allocation, retaining the published first generation and its admission.
    replacement.deinit();
    try std.testing.expectEqual(@as(usize, 16), workspace.stats().live_bytes);
    try std.testing.expectEqual(@as(usize, 16), controller.snapshot().backend_scratch_bytes);
}

test "gliner boundary resident workspace allocation failures preserve leases and old storage" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testWorkspaceAllocationFailures, .{});
}

test "gliner boundary resident workspace cancellation keeps permit with caller" {
    const Cancel = struct {
        fn afterAllocation(raw: ?*anyopaque) !void {
            const ctx: *const TestDevice.Context = @ptrCast(@alignCast(raw.?));
            if (ctx.live != 0) return error.Cancelled;
        }
    };
    const W = WorkspaceWithDevice(TestDevice);
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var ctx = TestDevice.Context{};
    var workspace = W{ .allocator = std.testing.allocator, .model_generation = 3 };
    defer workspace.deinit();
    var permit = try controller.tryAcquire(.gpu, .{}, try W.replacementAmounts(16), false);
    defer permit.release();
    try std.testing.expectError(error.Cancelled, workspace.prepareReplacement(&ctx, 3, 16, &permit, .{ .ptr = &ctx, .check_fn = Cancel.afterAllocation }));
    try std.testing.expect(permit.controller != null);
    try std.testing.expectEqual(@as(usize, 0), ctx.live);
    try std.testing.expectEqual(@as(usize, 0), workspace.stats().live_bytes);
    try std.testing.expectEqual(@as(usize, 16), controller.snapshot().backend_scratch_bytes);
}
