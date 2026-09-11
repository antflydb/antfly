// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Stable managed CPU/resident-Metal training backend. The verified Source
//! outlives this owner. Only unselected original parameters are uploaded here;
//! the optimizer remains the sole owner of every selected parameter/state.
const std = @import("std");
const build_options = @import("build_options");
const ml = @import("ml").graph;
const native = @import("../ops/native_compute.zig");
const metal = @import("../ops/metal_compute.zig");
const gpu_store = @import("../ops/gpu_hosted_store.zig");
const metal_runtime = @import("../backends/metal_runtime.zig");
const ops = @import("../ops/ops.zig");
const run = @import("gliner_boundary_run.zig");
const controller = @import("seeded_gradient_trainer.zig");
const interpreter = @import("../graph/interpreter.zig");
const Control = @import("../execution_control.zig").InferenceExecutionControl;
const Budget = @import("../runtime/bounded_allocator.zig").BoundedAllocator;
const Allocator = std.mem.Allocator;
const mib = 1024 * 1024;

pub const Execution = controller.Execution;
pub const Limits = struct {
    max_parameters: usize = 4096,
    max_frozen_device_bytes: usize = 2 * 1024 * mib,
    max_upload_staging_bytes: usize = 1024 * mib,
    max_host_metadata_bytes: usize = 32 * mib,
    /// This owner's additional frozen payload/staging/metadata only. Source,
    /// optimizer, activation/tape, and transaction limits are admitted by the
    /// enclosing run. Backend/driver allocations use its backend allocator.
    max_combined_bytes: usize = 4 * 1024 * mib,
    primitive: ops.resident_training.Limits = .{},
};
pub const Admission = struct {
    execution: Execution,
    original_parameters: usize,
    selected_parameters: usize,
    frozen_parameters: usize,
    frozen_device_bytes: usize,
    upload_staging_bytes: usize,
    host_metadata_upper_bound_bytes: usize,
    initialize_upper_bound_bytes: usize,
    binding_metadata_upper_bound_bytes: usize,
};
pub const Receipt = struct { upload_bytes: usize = 0, upload_tensors: usize = 0 };
const Frozen = struct { parameter: run.Parameter, tensor: ?ops.CT = null };
const Selected = struct { name: []u8, dimensions: []i32 };
const Name = union(enum) { frozen: usize, selected: usize };
const MetalOwner = if (build_options.enable_metal) struct {
    store: gpu_store.WeightStore,
    backend: metal.MetalCompute,
} else struct {};

fn add(a: usize, b: usize) !usize {
    return std.math.add(usize, a, b) catch error.BoundaryTrainingBackendLimitExceeded;
}
fn mul(a: usize, b: usize) !usize {
    return std.math.mul(usize, a, b) catch error.BoundaryTrainingBackendLimitExceeded;
}
fn check(control: ?Control) !void {
    if (control) |active| try active.check();
}
fn nameValid(name: []const u8) bool {
    if (name.len == 0 or name.len > 1024 or std.mem.startsWith(u8, name, "__")) return false;
    for (name) |byte| if (byte < 32 or byte == 127) return false;
    return true;
}
fn selectedIndex(selected: []const controller.Parameter, name: []const u8) ?usize {
    for (selected, 0..) |parameter, i| if (std.mem.eql(u8, parameter.name, name)) return i;
    return null;
}
fn validateShape(dimensions: []const i32, values: []const f32, limits: Limits) !usize {
    const count = try ops.resident_training.shapeElements(i32, dimensions, limits.primitive);
    if (count != values.len) return error.InvalidBoundaryTrainingBackendParameter;
    return count;
}

/// No allocation or device dispatch. A full frozen inventory, its largest
/// synchronous host upload staging, and bounded name/shape/lease metadata are
/// admitted before creating the Metal provider or reading learned values.
pub fn estimate(originals: []const run.Parameter, selected: []const controller.Parameter, execution: Execution, limits: Limits) !Admission {
    if (limits.max_parameters == 0 or limits.max_parameters > 4096 or limits.max_host_metadata_bytes <= @sizeOf(Owner) or
        limits.max_combined_bytes == 0 or originals.len == 0 or originals.len > limits.max_parameters or selected.len > limits.max_parameters)
        return error.InvalidBoundaryTrainingBackendLimits;
    if (comptime !build_options.enable_metal) if (execution == .resident_metal) return error.UnsupportedBoundaryTrainingBackend;
    var frozen_count: usize = 0;
    var payload: usize = 0;
    var largest: usize = 0;
    var strings: usize = 0;
    for (selected, 0..) |parameter, i| {
        if (!nameValid(parameter.name)) return error.InvalidBoundaryTrainingBackendParameter;
        _ = try validateShape(parameter.dimensions, parameter.values, limits);
        for (selected[0..i]) |prior| if (std.mem.eql(u8, prior.name, parameter.name)) return error.DuplicateBoundaryTrainingBackendParameter;
        strings = try add(strings, try add(parameter.name.len, try mul(parameter.dimensions.len, @sizeOf(i32))));
    }
    for (originals, 0..) |parameter, i| {
        if (parameter.kind != .original or !nameValid(parameter.name) or !nameValid(parameter.canonical_name))
            return error.InvalidBoundaryTrainingBackendParameter;
        const count = try validateShape(parameter.dimensions, parameter.values, limits);
        for (originals[0..i]) |prior| if (std.mem.eql(u8, prior.name, parameter.name) or std.mem.eql(u8, prior.canonical_name, parameter.canonical_name))
            return error.DuplicateBoundaryTrainingBackendParameter;
        if (selectedIndex(selected, parameter.name)) |index| {
            if (!std.mem.eql(i32, parameter.dimensions, selected[index].dimensions)) return error.TrainingBindingShapeMismatch;
            continue;
        }
        frozen_count += 1;
        const bytes = try mul(count, 4);
        payload = try add(payload, bytes);
        largest = @max(largest, bytes);
    }
    if (execution == .native) {
        // Preserve the existing CPU path: Source.store resolves frozen graph
        // parameters, so no frozen map, payload copy, or explicit binding is
        // created by this owner.
        const metadata = try add(@sizeOf(Owner), @sizeOf(native.NativeCompute));
        if (metadata > limits.max_host_metadata_bytes or metadata > limits.max_combined_bytes)
            return error.BoundaryTrainingBackendLimitExceeded;
        return .{ .execution = execution, .original_parameters = originals.len, .selected_parameters = selected.len, .frozen_parameters = frozen_count, .frozen_device_bytes = 0, .upload_staging_bytes = 0, .host_metadata_upper_bound_bytes = metadata, .initialize_upper_bound_bytes = metadata, .binding_metadata_upper_bound_bytes = 0 };
    }
    const bindings = try mul(frozen_count, @sizeOf(interpreter.RuntimeInput) + 2048);
    const metadata = try add(try add(@sizeOf(Owner), @sizeOf(MetalOwner)), try add(strings, try add(bindings, try mul(try add(originals.len, selected.len), 4096))));
    const combined = try add(payload, try add(largest, metadata));
    if (payload > limits.max_frozen_device_bytes or largest > limits.max_upload_staging_bytes or
        metadata > limits.max_host_metadata_bytes or combined > limits.max_combined_bytes)
        return error.BoundaryTrainingBackendLimitExceeded;
    return .{ .execution = execution, .original_parameters = originals.len, .selected_parameters = selected.len, .frozen_parameters = frozen_count, .frozen_device_bytes = payload, .upload_staging_bytes = largest, .host_metadata_upper_bound_bytes = metadata, .initialize_upper_bound_bytes = combined, .binding_metadata_upper_bound_bytes = bindings };
}

pub const Owner = struct {
    backing: Allocator,
    metadata: Budget,
    cb: ops.ComputeBackend,
    execution: Execution,
    limits: Limits,
    admission: Admission,
    receipt: Receipt = .{},
    native_backend: ?*native.NativeCompute = null,
    metal_backend: ?*MetalOwner = null,
    frozen: []Frozen = &.{},
    selected: []Selected = &.{},
    initialized_frozen: usize = 0,
    initialized_selected: usize = 0,
    names: std.StringHashMapUnmanaged(Name) = .empty,
    active_bindings: usize = 0,

    pub fn init(a: Allocator, source_store: *native.WeightStore, originals: []const run.Parameter, selected: []const controller.Parameter, execution: Execution, limits: Limits, control: ?Control) !*Owner {
        try check(control);
        if (comptime !build_options.enable_metal) if (execution == .resident_metal) return error.UnsupportedBoundaryTrainingBackend;
        const admission = try estimate(originals, selected, execution, limits);
        if (execution == .resident_metal) {
            // Reject invalid source payloads before any uploads. Selected
            // values are independently validated by the optimizer owner.
            for (originals) |parameter| {
                if (selectedIndex(selected, parameter.name) != null) continue;
                for (parameter.values, 0..) |value, i| {
                    if (i % 65536 == 0) try check(control);
                    if (!std.math.isFinite(value)) return error.NonFiniteTrainingParameter;
                }
            }
        }
        const self = try a.create(Owner);
        self.* = .{ .backing = a, .metadata = .{ .backing = a, .limit = limits.max_host_metadata_bytes - @sizeOf(Owner) }, .cb = undefined, .execution = execution, .limits = limits, .admission = admission };
        errdefer self.deinit();
        if (execution == .native) {
            const backend = try a.create(native.NativeCompute);
            backend.* = native.NativeCompute.init(a, source_store, null);
            backend.quantized_activation_policy = .strict_f32;
            self.native_backend = backend;
            self.cb = backend.computeBackend();
            try check(control);
            return self;
        }
        if (comptime build_options.enable_metal) {
            self.metal_backend = try createMetal(a);
            self.cb = self.metal_backend.?.backend.computeBackend();
            const scratch = self.metadata.allocator();
            self.frozen = try scratch.alloc(Frozen, admission.frozen_parameters);
            self.selected = try scratch.alloc(Selected, selected.len);
            try self.names.ensureTotalCapacity(scratch, @intCast(admission.frozen_parameters + selected.len));
            for (selected, 0..) |parameter, i| {
                const name = try scratch.dupe(u8, parameter.name);
                errdefer scratch.free(name);
                const dims = try scratch.dupe(i32, parameter.dimensions);
                self.selected[i] = .{ .name = name, .dimensions = dims };
                self.initialized_selected += 1;
                self.names.putAssumeCapacityNoClobber(name, .{ .selected = i });
            }
            var controlled = self.cb;
            controlled.execution_control = control;
            for (originals) |parameter| {
                if (selectedIndex(selected, parameter.name) != null) continue;
                try check(control);
                const index = self.initialized_frozen;
                self.frozen[index] = .{ .parameter = parameter };
                self.initialized_frozen += 1;
                self.frozen[index].tensor = try controlled.residentTrainingPrimitive(&.{ .upload_f32 = .{
                    .values = parameter.values,
                    .shape = parameter.dimensions,
                } }, limits.primitive);
                self.receipt.upload_bytes += parameter.values.len * 4;
                self.receipt.upload_tensors += 1;
                self.names.putAssumeCapacityNoClobber(parameter.name, .{ .frozen = index });
            }
            try check(control);
            std.debug.assert(self.receipt.upload_bytes == admission.frozen_device_bytes);
            return self;
        } else return error.UnsupportedBoundaryTrainingBackend;
    }

    /// An immutable source/provider lease must cover the owner and every
    /// returned binding. Callers serialize execution on this backend. All
    /// bindings/tapes are released before deinit; no selected weight is owned
    /// by this module. Source paths are never reopened.
    pub fn deinit(self: *Owner) void {
        std.debug.assert(self.active_bindings == 0);
        if (comptime build_options.enable_metal) if (self.metal_backend) |backend| {
            const runtime = backend.backend.provider_impl.raw_decode_runtime;
            if (metal_runtime.hasActiveFrame(runtime)) metal_runtime.cancelFrame(runtime) catch {};
            if (metal_runtime.hasSubmittedFrame(runtime)) metal_runtime.waitFrame(runtime) catch {};
        };
        for (self.frozen[0..self.initialized_frozen]) |entry| if (entry.tensor) |tensor| self.cb.free(tensor);
        const scratch = self.metadata.allocator();
        self.names.deinit(scratch);
        for (self.selected[0..self.initialized_selected]) |entry| {
            scratch.free(entry.name);
            scratch.free(entry.dimensions);
        }
        scratch.free(self.selected);
        scratch.free(self.frozen);
        if (self.native_backend) |backend| {
            backend.deinit();
            self.backing.destroy(backend);
        }
        if (comptime build_options.enable_metal) if (self.metal_backend) |backend| {
            backend.backend.deinit();
            metal.deinitSharedNativeProvider(&backend.store);
            metal.deinitPrefetchQueue(&backend.store);
            backend.store.lazy_weights.deinit(self.backing);
            self.backing.destroy(backend);
        };
        std.debug.assert(self.metadata.live == 0);
        self.backing.destroy(self);
    }

    /// Only original frozen parameters become bindings. Known selected names
    /// are left to Controller.bind and __ runtime inputs to Step. The caller
    /// filters merged bindings through differentiated.id_map before execution.
    pub fn bindFrozen(self: *Owner, graph: *const ml.Graph, control: ?Control) !Bindings {
        try check(control);
        if (self.active_bindings != 0) return error.TrainingBackendBindingStillLive;
        if (self.execution == .native) {
            self.active_bindings += 1;
            return .{ .owner = self, .inputs = &.{} };
        }
        if (graph.parameters.items.len > self.limits.max_parameters * 8) return error.BoundaryTrainingBackendLimitExceeded;
        const scratch = self.metadata.allocator();
        const seen = try scratch.alloc(bool, self.frozen.len + self.selected.len);
        defer scratch.free(seen);
        @memset(seen, false);
        var count: usize = 0;
        // Validate the complete request before acquiring even the first lease.
        for (graph.parameters.items) |id| {
            try check(control);
            if (id >= graph.nodes.items.len or graph.node(id).op != .parameter) return error.InvalidBoundaryTrainingBinding;
            const name = graph.parameterName(graph.node(id));
            if (std.mem.startsWith(u8, name, "__")) continue;
            const entry = self.names.get(name) orelse return error.UnknownBoundaryTrainingParameter;
            const seen_index = switch (entry) {
                .frozen => |index| index,
                .selected => |index| self.frozen.len + index,
            };
            if (seen[seen_index]) return error.DuplicateBoundaryTrainingBinding;
            seen[seen_index] = true;
            const shape = graph.node(id).output_shape;
            const dimensions = switch (entry) {
                .selected => |index| self.selected[index].dimensions,
                .frozen => |index| self.frozen[index].parameter.dimensions,
            };
            if (shape.dtype != .f32 or shape.rank_ != dimensions.len) return error.TrainingBindingShapeMismatch;
            for (dimensions, shape.dims[0..shape.rank_]) |actual, expected| if (actual != expected) return error.TrainingBindingShapeMismatch;
            if (entry == .frozen) count += 1;
        }
        const inputs = try scratch.alloc(interpreter.RuntimeInput, count);
        errdefer scratch.free(inputs);
        var initialized: usize = 0;
        errdefer for (inputs[0..initialized]) |input| self.cb.free(input.value);
        var controlled = self.cb;
        controlled.execution_control = control;
        for (graph.parameters.items) |id| {
            const name = graph.parameterName(graph.node(id));
            if (std.mem.startsWith(u8, name, "__")) continue;
            const entry = self.names.get(name).?;
            if (entry != .frozen) continue;
            const frozen = self.frozen[entry.frozen];
            const value = try controlled.residentTrainingPrimitive(&.{ .reshape = .{
                .input = frozen.tensor.?,
                .shape = frozen.parameter.dimensions,
            } }, self.limits.primitive);
            inputs[initialized] = .{ .node_id = id, .value = value };
            initialized += 1;
        }
        try check(control);
        self.active_bindings += 1;
        return .{ .owner = self, .inputs = inputs };
    }
};

pub const Bindings = struct {
    owner: *Owner,
    inputs: []interpreter.RuntimeInput,
    pub fn deinit(self: *Bindings) void {
        for (self.inputs) |input| self.owner.cb.free(input.value);
        self.owner.metadata.allocator().free(self.inputs);
        std.debug.assert(self.owner.active_bindings > 0);
        self.owner.active_bindings -= 1;
        self.* = undefined;
    }
};

fn createMetal(a: Allocator) !*MetalOwner {
    if (comptime !build_options.enable_metal) return error.UnsupportedBoundaryTrainingBackend;
    if (!metal_runtime.metalDeviceAvailable()) return error.MetalDeviceUnavailable;
    const owner = try a.create(MetalOwner);
    errdefer a.destroy(owner);
    owner.store = .{ .allocator = a, .prefix = "", .lazy_weights = .empty, .prefer_f32_dense_tensors = true };
    errdefer owner.store.lazy_weights.deinit(a);
    metal.initPrefetchQueue(&owner.store, a);
    errdefer metal.deinitPrefetchQueue(&owner.store);
    errdefer metal.deinitSharedNativeProvider(&owner.store);
    owner.backend = try metal.MetalCompute.init(a, &owner.store, null);
    return owner;
}
