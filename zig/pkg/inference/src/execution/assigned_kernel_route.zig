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

//! Input-aware ownership for existing direct descriptor programs. Vendor or
//! data-dependent families provide their own exact route instead of using this.
const std = @import("std");
const ml = @import("ml");
const ops = @import("../ops/ops.zig");
const contracts = @import("../graph/backend_contracts.zig");
const route = @import("kernel_route.zig");
const ownership = @import("storage_ownership.zig");

pub const Support = *const fn (*anyopaque, *const ops.kernel_descriptor.Descriptor) bool;
const Direct = struct {
    allocator: std.mem.Allocator,
    backend: ops.ComputeBackend,
    descriptor: ops.kernel_descriptor.Descriptor,
    slot: route.OutputSlot,
    support: Support,
    immutable_resources: bool,
    workspace: ops.kernel_descriptor.WorkspaceRequirement,
    auxiliaries: ops.kernel_descriptor.AuxiliaryRequirements,

    fn matches(raw: *anyopaque, request: route.Validate) bool {
        const self: *Direct = @ptrCast(@alignCast(raw));
        if (!self.support(self.backend.ptr, &self.descriptor) or request.output.epoch != self.slot.epoch or request.output.node != self.slot.node or request.output.allocation != self.slot.allocation or !shapeEqual(request.output.logical, self.descriptor.output_shape) or request.inputs.len != self.descriptor.input_count) return false;
        for (request.inputs, 0..) |input, index| if (!bindingDense(self.backend, input, self.descriptor.input_shapes[index])) return false;
        // Canonical prepareImmutable callers attest descriptor-only resource
        // recipes. Keep support policy, current slots and every input checked
        // above while reusing the cold workspace and auxiliary requirements.
        if (self.immutable_resources) return true;
        const functions = self.backend.vtable.kernels orelse return false;
        const workspace = if (functions.assignedKernelWorkspace) |query| query(self.backend.ptr, &self.descriptor) catch return false else ops.kernel_descriptor.WorkspaceRequirement{};
        if (!std.meta.eql(workspace, self.workspace)) return false;
        const auxiliaries = if (functions.assignedKernelAuxiliaries) |query| query(self.backend.ptr, &self.descriptor) catch return false else ops.kernel_descriptor.AuxiliaryRequirements{};
        if (auxiliaries.count != self.auxiliaries.count) return false;
        for (auxiliaries.items(), self.auxiliaries.items()) |current, selected| if (current.role != selected.role or !shapeEqual(current.shape, selected.shape)) return false;
        return true;
    }
    fn validate(raw: *anyopaque, request: route.Validate) !void {
        if (!matches(raw, request)) return error.PreparedKernelBindingChanged;
    }
    fn deinit(raw: *anyopaque) void {
        const self: *Direct = @ptrCast(@alignCast(raw));
        self.allocator.destroy(self);
    }
    const vtable = route.Owned.VTable{ .matches = matches, .validate = validate, .deinit = deinit };
};
fn shapeEqual(a: ml.graph.Shape, b: ml.graph.Shape) bool {
    return a.dtype == b.dtype and a.rank() == b.rank() and std.mem.eql(i64, a.dims[0..a.rank()], b.dims[0..b.rank()]);
}
fn planeDense(backend: ops.ComputeBackend, plane: ownership.Plane, expected: ml.graph.Shape) bool {
    if (plane.role != .primary or plane.descriptor.current_backend != backend.kind() or plane.descriptor.quant_format != null) return false;
    var address: ?usize = null;
    if (backend.kind() == .native and expected.dtype == .f32) if (plane.backing) |backing| {
        if (backing.kind != .host) return false;
        address = std.math.add(usize, backing.base, plane.byte_offset) catch return false;
    };
    const actual = plane.descriptor.shape;
    if (actual.rank() > actual.dims.len) return false;
    if (!denseShapeExtentMatches(expected, actual.dtype, actual.dims[0..actual.rank()], plane.logical_bytes, plane.readable_bytes, address)) return false;
    const dense = contracts.TensorStrides.dense(expected);
    return plane.descriptor.strides.rank == dense.rank and std.mem.eql(i64, plane.descriptor.strides.values[0..dense.rank], dense.values[0..dense.rank]);
}

/// Shared exact logical/extent predicate. Backing/complete/provider/stride
/// authority remains with the actual reporting owner, never these scalar facts.
/// Native's contiguous current-header path can avoid constructing a TensorDesc
/// solely to discard it after the same comparison.
pub inline fn denseShapeExtentMatches(expected: ml.graph.Shape, actual_dtype: ml.graph.DType, dimensions: []const i64, logical_bytes: usize, readable_bytes: usize, host_address: ?usize) bool {
    if (expected.rank() > expected.dims.len or actual_dtype != expected.dtype or dimensions.len != expected.rank() or !std.mem.eql(i64, dimensions, expected.dims[0..expected.rank()])) return false;
    const count = expected.numElements() orelse return false;
    if (count <= 0) return false;
    const bytes = std.math.mul(usize, @intCast(count), expected.dtype.byteSize()) catch return false;
    if (logical_bytes != bytes or readable_bytes < bytes) return false;
    if (expected.dtype == .f32) if (host_address) |address| if (address % @alignOf(f32) != 0) return false;
    return true;
}

fn planesDense(backend: ops.ComputeBackend, logical: ml.graph.Shape, planes: []const ownership.Plane, expected: ml.graph.Shape) bool {
    if (!shapeEqual(logical, expected)) return false;
    for (planes) |plane| if (plane.role == .primary) return planeDense(backend, plane, expected);
    return false;
}
pub fn bindingDense(backend: ops.ComputeBackend, binding: route.Binding, expected: ml.graph.Shape) bool {
    return switch (binding) {
        .imported_host => |host| host.target == backend.kind() and shapeEqual(host.logical, expected),
        .planned => |planned| planesDense(backend, planned.logical, planned.planes, expected),
        .resident => |resident| resident.implementation.context == backend.ptr and resident.implementation.table == @as(*const anyopaque, @ptrCast(backend.vtable)) and planesDense(backend, resident.logical, resident.planes, expected),
        .tensor => |known| actualDense(backend, known, expected),
        .unknown => false,
    };
}
fn actualDense(backend: ops.ComputeBackend, known: anytype, expected: ml.graph.Shape) bool {
    const source = ops.ComputeBackend{ .ptr = known.implementation.context, .vtable = @ptrCast(@alignCast(known.implementation.table)) };
    if (source.kind() != backend.kind() or (backend.kind() != .native and source.ptr != backend.ptr)) return false;
    const functions = source.vtable.kernels orelse return false;
    if (!functions.accepts(source.vtable)) return false;
    const query = functions.storageOwnership orelse return false;
    const Check = struct {
        backend: ops.ComputeBackend,
        expected: ml.graph.Shape,
        dense: bool = false,
        fn emit(raw: *anyopaque, report: ownership.Report) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            switch (report) {
                .plane => |plane| if (plane.role == .primary) {
                    self.dense = planeDense(self.backend, plane, self.expected);
                },
                .backing => {},
            }
        }
    };
    var check = Check{ .backend = backend, .expected = expected };
    const coverage = query(source.ptr, known.value, .{ .context = &check, .visit = Check.emit }) catch return false;
    return coverage == .complete and check.dense;
}

pub fn prepare(backend: ops.ComputeBackend, request: route.Prepare, support: Support) !?route.Owned {
    return prepareImpl(backend, request, support, false);
}

/// Only canonical backends with descriptor-only resource callbacks call this.
/// Current support policy and model Tensor operands still require validation.
/// Runtime input equivalence remains mandatory for either validation scope.
pub fn prepareImmutable(backend: ops.ComputeBackend, request: route.Prepare, support: Support) !?route.Owned {
    return prepareImpl(backend, request, support, true);
}

fn prepareImpl(backend: ops.ComputeBackend, request: route.Prepare, support: Support, immutable_program: bool) !?route.Owned {
    if (request.descriptor.elements() == 0) return null;
    const implementation = request.implementation orelse return null;
    if (implementation.context != backend.ptr or implementation.table != @as(*const anyopaque, @ptrCast(backend.vtable))) return null;
    const canonical = backend.vtable.kernels orelse return null;
    if (!canonical.accepts(backend.vtable)) return null;
    if (!support(backend.ptr, request.descriptor) or request.inputs.len != request.descriptor.input_count) return null;
    for (request.inputs, 0..) |input, index| if (!bindingDense(backend, input, request.descriptor.input_shapes[index])) return null;
    const functions = backend.vtable.kernels orelse return null;
    const workspace = if (functions.assignedKernelWorkspace) |query| try query(backend.ptr, request.descriptor) else ops.kernel_descriptor.WorkspaceRequirement{};
    const auxiliaries = if (functions.assignedKernelAuxiliaries) |query| try query(backend.ptr, request.descriptor) else ops.kernel_descriptor.AuxiliaryRequirements{};
    const primary_bytes = try std.math.mul(usize, request.descriptor.elements(), request.descriptor.output_shape.dtype.byteSize());
    var stable = immutable_program;
    for (request.inputs, 0..) |input, index| {
        if (index >= request.inputs_witnessed.len or !request.inputs_witnessed[index] or input == .tensor) stable = false;
    }
    const owner = try request.allocator.create(Direct);
    owner.* = .{ .allocator = request.allocator, .backend = backend.withoutSynchronousBorrows(), .descriptor = request.descriptor.*, .slot = request.output, .support = support, .immutable_resources = immutable_program, .workspace = workspace, .auxiliaries = auxiliaries };
    return .{ .validation_scope = if (stable) .prepared_program else .invocation, .context = owner, .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .key = @intCast(request.output.node), .requirements = .{ .primary_alignment = request.descriptor.alignment, .primary_bytes = primary_bytes, .profile = @intCast(@backingInt(std.meta.activeTag(request.descriptor.operation))), .workspace = workspace, .auxiliaries = auxiliaries, .capture = if (functions.physicalReplayKernelSupported) |query| (if (query(backend.ptr, request.descriptor)) .supported else .unsupported) else .unsupported }, .vtable = &Direct.vtable };
}

pub fn isDirect(selected: *const route.Owned) bool {
    return selected.vtable == &Direct.vtable;
}

test "direct program validation stays dynamic by default and for model tensor operands" {
    const a = std.testing.allocator;
    const native = @import("../ops/native_compute.zig");
    var store = native.WeightStore{ .allocator = a, .resident_weights = .empty, .lazy_weights = .empty };
    defer store.deinitOwned();
    var compute = native.NativeCompute.init(a, &store, null);
    defer compute.deinit();
    const backend = compute.computeBackend();
    const Probe = struct {
        var allowed: bool = true;
        var fixed_queries: usize = 0;
        fn dynamic(_: *anyopaque, _: *const ops.kernel_descriptor.Descriptor) bool {
            return allowed;
        }
        fn fixed(_: *anyopaque, _: *const ops.kernel_descriptor.Descriptor) bool {
            fixed_queries += 1;
            return true;
        }
    };
    Probe.allowed = true;
    const shape = ml.graph.Shape.init(.f32, &.{4});
    const descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .unary = .negate }, .input_count = 1, .input_shapes = .{ shape, .{}, .{} }, .output_shape = shape };
    const implementation = route.Implementation{ .context = backend.ptr, .table = backend.vtable };
    const facts = [_]route.Binding{.{ .imported_host = .{ .logical = shape, .target = .native } }};
    const slot = route.OutputSlot{ .allocation = 0, .node = 1, .epoch = 1, .logical = shape, .writable_bytes = 16 };
    const request = route.Prepare{ .allocator = a, .implementation = implementation, .descriptor = &descriptor, .inputs = &facts, .inputs_witnessed = .{ true, false, false }, .output = slot };
    var dynamic_route = (try prepare(backend, request, Probe.dynamic)) orelse return error.MissingPreparedKernelRoute;
    defer dynamic_route.deinit();
    try std.testing.expectEqual(route.ValidationScope.invocation, dynamic_route.validation_scope);
    Probe.allowed = false;
    try std.testing.expectError(error.PreparedKernelBindingChanged, dynamic_route.validate(.{ .inputs = &facts, .output = slot }));
    Probe.allowed = true;
    var fixed_route = (try prepareImmutable(backend, request, Probe.fixed)) orelse return error.MissingPreparedKernelRoute;
    defer fixed_route.deinit();
    try std.testing.expectEqual(route.ValidationScope.prepared_program, fixed_route.validation_scope);
    const cold_queries = Probe.fixed_queries;
    try fixed_route.validate(.{ .inputs = &facts, .output = slot });
    try std.testing.expectEqual(cold_queries + 1, Probe.fixed_queries);
    var foreign_slot = slot;
    foreign_slot.epoch += 1;
    try std.testing.expectError(error.PreparedKernelBindingChanged, fixed_route.validate(.{ .inputs = &facts, .output = foreign_slot }));
    var unwitnessed_request = request;
    unwitnessed_request.inputs_witnessed = @splat(false);
    var unwitnessed = (try prepareImmutable(backend, unwitnessed_request, Probe.fixed)) orelse return error.MissingPreparedKernelRoute;
    defer unwitnessed.deinit();
    try std.testing.expectEqual(route.ValidationScope.invocation, unwitnessed.validation_scope);
    const input = try backend.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
    defer backend.free(input);
    const model = [_]route.Binding{.{ .tensor = .{ .value = input, .implementation = implementation } }};
    var model_request = request;
    model_request.inputs = &model;
    var retained = (try prepareImmutable(backend, model_request, Probe.fixed)) orelse return error.MissingPreparedKernelRoute;
    defer retained.deinit();
    try std.testing.expectEqual(route.ValidationScope.invocation, retained.validation_scope);
    try retained.validate(.{ .inputs = &model, .output = slot });
    const wrong_input = try backend.fromFloat32Shape(&.{ 1, 2 }, &.{2});
    defer backend.free(wrong_input);
    const wrong_model = [_]route.Binding{.{ .tensor = .{ .value = wrong_input, .implementation = implementation } }};
    try std.testing.expectError(error.PreparedKernelBindingChanged, retained.validate(.{ .inputs = &wrong_model, .output = slot }));
}

test "Native exact dense facts reject unaligned or oversized source F32 planes before route selection" {
    const native = @import("../ops/native_compute.zig");
    const shape = ml.graph.Shape.init(.f32, &.{2});
    const backend = ops.ComputeBackend{ .ptr = @ptrFromInt(8), .vtable = &native.vtable_impl };
    var descriptor = contracts.TensorDesc.init(shape, .host_dense);
    descriptor.current_backend = .native;
    var plane = ownership.Plane{ .role = .primary, .descriptor = descriptor, .backing = .{ .kind = .host, .base = 4096 }, .byte_offset = 0, .logical_bytes = 8, .readable_bytes = 8 };
    try std.testing.expect(planeDense(backend, plane, shape));
    plane.backing.?.base += 1;
    try std.testing.expect(!planeDense(backend, plane, shape));
    plane.backing.?.base -= 1;
    plane.logical_bytes = 12;
    plane.readable_bytes = 12;
    try std.testing.expect(!planeDense(backend, plane, shape));
    plane.logical_bytes = 8;
    plane.readable_bytes = 7;
    try std.testing.expect(!planeDense(backend, plane, shape));
    plane.readable_bytes = 8;
    plane.descriptor.shape.dtype = .bf16;
    try std.testing.expect(!planeDense(backend, plane, shape));
}

/// Cold attestation of the private Direct owner, after the concrete provider's
/// full compute-table acceptance. Other private vtable users grant no context.
pub fn belongsTo(selected: *const route.Owned, backend: ops.ComputeBackend) bool {
    if (!isDirect(selected) or selected.implementation.context != backend.ptr or selected.implementation.table != @as(*const anyopaque, @ptrCast(backend.vtable))) return false;
    const owner: *const Direct = @ptrCast(@alignCast(selected.context));
    return owner.backend.ptr == backend.ptr and owner.backend.vtable == backend.vtable;
}

/// A provider may attest only its exact private Direct implementation. This
/// does not change invocation validation or certify a model Tensor's layout.
pub fn belongsToProgram(selected: *const route.Owned, backend: ops.ComputeBackend, support: Support) bool {
    if (!belongsTo(selected, backend)) return false;
    const owner: *const Direct = @ptrCast(@alignCast(selected.context));
    return owner.support == support;
}

/// Exact authored-operation metadata, after the target selected this private
/// Direct implementation. No caller-supplied opaque descriptor is trusted.
pub fn validateDescriptor(selected: *const route.Owned, descriptor: *const ops.kernel_descriptor.Descriptor) !void {
    if (!isDirect(selected)) return error.InvalidPreparedKernelRoute;
    const owner: *const Direct = @ptrCast(@alignCast(selected.context));
    if (!std.meta.eql(owner.descriptor, descriptor.*)) return error.PreparedKernelBindingChanged;
}

/// A cold-bound private Direct descriptor. The selected route/Frame owns this
/// object; no invocation, operand span or cleanup scope is retained here.
pub const NativeProgramBinding = struct {
    owner: *const Direct,
    pub fn descriptor(self: @This()) *const ops.kernel_descriptor.Descriptor {
        return &self.owner.descriptor;
    }
    pub fn validateCurrentMetadata(self: @This(), slot: route.OutputSlot) !void {
        const selected = self.owner;
        if (!selected.support(selected.backend.ptr, &selected.descriptor)) return error.PreparedKernelBindingChanged;
        try self.validateCurrentOutputSlot(slot);
    }
    pub fn hasSupport(self: @This(), support: Support) bool {
        return self.owner.support == support;
    }
    /// Slot metadata only. The canonical Native caller separately validates
    /// its exact known support identity and that owner's live policy.
    pub fn validateCurrentOutputSlot(self: @This(), slot: route.OutputSlot) !void {
        const selected = self.owner;
        if (slot.epoch != selected.slot.epoch or slot.node != selected.slot.node or slot.allocation != selected.slot.allocation or !shapeEqual(slot.logical, selected.descriptor.output_shape)) return error.PreparedKernelBindingChanged;
    }
};

/// Native calls only after its exact canonical table check. Other providers,
/// dynamic recipes and foreign/private route owners keep ordinary dispatch.
pub fn bindNativeProgram(selected: *const route.Owned, backend: ops.ComputeBackend, descriptor: *const ops.kernel_descriptor.Descriptor, slot: route.OutputSlot) !?NativeProgramBinding {
    if (backend.vtable != &@import("../ops/native_compute.zig").vtable_impl or !belongsTo(selected, backend)) return null;
    const owner: *const Direct = @ptrCast(@alignCast(selected.context));
    if (!owner.immutable_resources) return null;
    try validateDescriptor(selected, descriptor);
    const bound = NativeProgramBinding{ .owner = owner };
    try bound.validateCurrentMetadata(slot);
    return bound;
}

fn referencePlaneDense(backend: ops.ComputeBackend, plane: ownership.Plane, expected: ml.graph.Shape) bool {
    if (plane.role != .primary or !shapeEqual(plane.descriptor.shape, expected) or plane.descriptor.current_backend != backend.kind() or plane.descriptor.quant_format != null) return false;
    const count = expected.numElements() orelse return false;
    if (count <= 0) return false;
    const bytes = std.math.mul(usize, @intCast(count), expected.dtype.byteSize()) catch return false;
    if (plane.logical_bytes != bytes) return false;
    if (backend.kind() == .native and expected.dtype == .f32) if (plane.backing) |backing| {
        if (backing.kind != .host) return false;
        const address = std.math.add(usize, backing.base, plane.byte_offset) catch return false;
        if (address % @alignOf(f32) != 0) return false;
    };
    const dense = contracts.TensorStrides.dense(expected);
    return plane.descriptor.strides.rank == dense.rank and std.mem.eql(i64, plane.descriptor.strides.values[0..dense.rank], dense.values[0..dense.rank]) and plane.readable_bytes >= bytes;
}

test "compact dense extent predicate preserves original Native plane rules across malformed facts" {
    const native = @import("../ops/native_compute.zig");
    const backend = ops.ComputeBackend{ .ptr = @ptrFromInt(16), .vtable = &native.vtable_impl };
    const expected = ml.graph.Shape.init(.f32, &.{ 2, 4 });
    const base = ownership.Plane{ .descriptor = contracts.TensorDesc.init(expected, .host_f32), .role = .primary, .backing = .{ .kind = .host, .base = 4096 }, .byte_offset = 0, .logical_bytes = 32, .readable_bytes = 32 };
    var original = base;
    original.descriptor.current_backend = .native;
    for (0..13) |kind| {
        var plane = original;
        switch (kind) {
            0 => {},
            1 => plane.logical_bytes -= 1,
            2 => plane.readable_bytes -= 1,
            3 => plane.descriptor.shape.dtype = .i32,
            4 => plane.descriptor.shape.dims[0] = 3,
            5 => plane.descriptor.strides.values[0] += 1,
            6 => plane.byte_offset = 1,
            7 => plane.backing.?.kind = .metal,
            8 => plane.backing = null,
            9 => plane.role = .host_mirror,
            10 => plane.descriptor.current_backend = .cuda,
            11 => plane.descriptor.shape.rank_ = 1,
            else => plane.readable_bytes += 32,
        }
        try std.testing.expectEqual(referencePlaneDense(backend, plane, expected), planeDense(backend, plane, expected));
    }
    var invalid = original;
    invalid.descriptor.shape.rank_ = 9;
    try std.testing.expect(!planeDense(backend, invalid, expected));
    var forged = expected;
    forged.rank_ = 9;
    try std.testing.expect(!denseShapeExtentMatches(forged, .f32, &.{ 2, 4 }, 32, 32, 4096));
}
