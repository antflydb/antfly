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

//! Shared ownership for physical allocations and tensor views. The execution
//! plan owns one reference; outstanding values pin storage across plan eviction.
const std = @import("std");
const ops = @import("ops.zig");
const admission = @import("../execution/storage_admission.zig");
const accounting = @import("../execution/storage_accounting.zig");

pub const Lease = struct {
    allocator: std.mem.Allocator,
    backend: ops.ComputeBackend,
    tensor: ops.CT,
    byte_size: usize,
    admission: ?*admission.Reservation = null,
    retirement_owner: ?*@import("../execution/backing_retirement.zig").BackingOwner = null,
    ledger: ?*accounting.Ledger = null,
    accounting_class: accounting.Class = .activation,
    backing_byte_size: ?usize = null,
    references: std.atomic.Value(usize) = .init(1),
    slot_authority: ?@import("../execution/slot_authority.zig").Slot = null,
    host_index_certificate: ?IndexCertificate = null,
    /// A by-value certificate owns no resource and borrows no Frame/Upload.
    /// Its token belongs to this retained Lease's immutable slot authority.
    const IndexCertificate = struct {
        lease_identity: usize,
        authority: *const @import("../execution/storage_ownership.zig").AllocationSlotAuthority,
        generation: u64,
        extent: usize,
        bytes: usize,
        allocation: u32,
        epoch: u64,
        node: @import("ml").graph.NodeId,
    };
    pub fn invalidateHostIndices(self: *Lease) void {
        self.host_index_certificate = null;
    }
    pub fn certifyHostIndices(self: *Lease, assigned: @import("../execution/kernel_route.zig").AssignedOutput, extent: usize) !void {
        self.invalidateHostIndices();
        if (assigned.input_generation == 0 or extent == 0 or !self.acceptsWritableRegion(assigned.slot.allocation, assigned.slot.epoch, assigned.slot.writable_bytes, assigned.region)) return error.InvalidHostIndexAuthority;
        self.host_index_certificate = .{ .lease_identity = @intFromPtr(self), .authority = assigned.region.authority, .generation = assigned.input_generation, .extent = extent, .bytes = assigned.slot.writable_bytes, .allocation = assigned.slot.allocation, .epoch = assigned.slot.epoch, .node = assigned.slot.node };
    }
    pub fn hostIndicesCertified(self: *const Lease, extent: usize, generation: u64, bytes: usize) bool {
        const certificate = self.host_index_certificate orelse return false;
        const slot = if (self.slot_authority) |*value| value else return false;
        return generation != 0 and certificate.lease_identity == @intFromPtr(self) and certificate.authority == slot.token() and certificate.generation == generation and certificate.extent <= extent and certificate.bytes == bytes and certificate.allocation == slot.allocation and certificate.epoch == slot.epoch and bytes <= slot.byte_size and !self.isReadOnly();
    }
    pub fn hostInputMatches(self: *const Lease, issued: @import("../execution/kernel_route.zig").AssignedInput, extent: usize, generation: u64, bytes: usize) bool {
        const certificate = self.host_index_certificate orelse return false;
        return issued.lease_identity == @intFromPtr(self) and issued.authority == certificate.authority and issued.generation == generation and issued.node == certificate.node and self.hostIndicesCertified(extent, generation, bytes);
    }

    pub fn bindAllocationSlot(self: *Lease, allocation: u32, epoch: u64, bytes: usize) !void {
        if (self.isReadOnly()) return error.ReadOnlyAllocationSlot;
        if (self.slot_authority) |*slot| {
            if (slot.allocation != allocation or slot.epoch != epoch or slot.byte_size != bytes) return error.InvalidAllocationSlot;
            return;
        }
        const authority = @import("../execution/slot_authority.zig");
        self.slot_authority = try authority.Slot.init(allocation, epoch, bytes, self.byte_size, try authority.canonicalBacking(&self.backend, self.tensor));
    }
    pub fn writableRegion(self: *const Lease, allocation: u32, epoch: u64, bytes: usize) !@import("../execution/storage_ownership.zig").WritableRegion {
        const slot = if (self.slot_authority) |*value| value else return error.InvalidAllocationSlot;
        return slot.region(allocation, epoch, bytes);
    }
    pub fn acceptsWritableRegion(self: *const Lease, allocation: u32, epoch: u64, bytes: usize, region: @import("../execution/storage_ownership.zig").WritableRegion) bool {
        const slot = if (self.slot_authority) |*value| value else return false;
        return !self.isReadOnly() and slot.accepts(allocation, epoch, bytes, region);
    }

    pub fn isReadOnly(self: *const Lease) bool {
        return self.accounting_class == .constant;
    }

    pub fn create(allocator: std.mem.Allocator, backend: ops.ComputeBackend, byte_size: usize) !?*Lease {
        return createTracked(allocator, backend, byte_size, null, .activation);
    }
    pub fn createTracked(allocator: std.mem.Allocator, backend: ops.ComputeBackend, byte_size: usize, ledger: ?*accounting.Ledger, class: accounting.Class) !?*Lease {
        return createAdmitted(allocator, backend, byte_size, ledger, class, null);
    }
    pub fn createAdmitted(allocator: std.mem.Allocator, backend: ops.ComputeBackend, byte_size: usize, ledger: ?*accounting.Ledger, class: accounting.Class, capability: ?admission.Capability) !?*Lease {
        if (byte_size == 0 or byte_size % @sizeOf(f32) != 0 or byte_size / @sizeOf(f32) > std.math.maxInt(i32)) return null;
        const self = try allocator.create(Lease);
        errdefer allocator.destroy(self);
        const reserved = if (capability) |cap| try reserveBinding(allocator, backend, byte_size, class, cap) else AdmissionBinding{};
        errdefer reserved.rollback(allocator);
        const owned = reserved.backing;
        const functions = backend.vtable.kernels;
        const allocate_storage = if (functions) |f| if (owned != null and backend.kind() == .metal) f.allocateAdmittedPhysicalStorage else f.allocatePhysicalStorage else null;
        const tensor = (if (allocate_storage) |allocate| try allocate(backend.ptr, byte_size) else try backend.allocUninitF32Shape(&.{@intCast(byte_size / @sizeOf(f32))})) orelse {
            if (capability != null) return error.PhysicalStorageAllocationDeclined;
            allocator.destroy(self);
            return null;
        };
        // Canonical physical bases have metadata-only storage destruction;
        // rollback conveys no inferred numerical authority from its epoch.
        errdefer backend.freeWithCleanup(tensor, .{ .boundary = if (owned) |value| value.construction_epoch else null });
        if (owned) |binding| try attachPhysicalCharge(backend, tensor, binding);
        const backing_bytes = try backingBytes(backend, tensor);
        if (reserved.reservation) |value| {
            const actual = backing_bytes orelse byte_size;
            if (actual > value.reservedBytes()) return error.PhysicalStorageExceededReservation;
            if (owned) |binding| binding.owner.measure(binding.construction_epoch, actual) else value.retainBytes(actual);
        }
        self.* = .{ .admission = reserved.reservation, .retirement_owner = if (owned) |binding| binding.owner else null, .allocator = allocator, .backend = backend, .tensor = tensor, .byte_size = byte_size, .ledger = ledger, .accounting_class = class, .backing_byte_size = backing_bytes };
        if (ledger) |owner| owner.allocated(class, byte_size, backing_bytes);
        return self;
    }
    const AdmissionBinding = admission.Binding;
    fn validatePreparedBackingSupport(backend: ops.ComputeBackend) !void {
        const functions = backend.vtable.kernels orelse return error.UnsupportedPhysicalStorageRetirement;
        if (!functions.accepts(backend.vtable) or functions.allocatePhysicalStorage == null) return error.UnsupportedPhysicalStorageRetirement;
        switch (backend.kind()) {
            .native, .cuda => {},
            .metal => {
                const services = backend.vtable.resource_retirement_services orelse return error.UnsupportedPhysicalStorageRetirement;
                if (functions.allocateAdmittedPhysicalStorage == null or functions.markPhysicalStorageAdmitted == null or !services.accepts(backend.vtable)) return error.UnsupportedPhysicalStorageRetirement;
            },
            else => return error.UnsupportedPhysicalStorageRetirement,
        }
    }
    fn reserveBinding(allocator: std.mem.Allocator, backend: ops.ComputeBackend, bytes: usize, class: accounting.Class, capability: admission.Capability) !AdmissionBinding {
        const functions = backend.vtable.kernels;
        if (capability.services.takeBacking != null) try validatePreparedBackingSupport(backend);
        const bound = if (functions) |f| (if (f.physicalStorageReservationBytes) |query| try query(backend.ptr, bytes) else bytes) else bytes;
        if (bound < bytes) return error.InvalidPhysicalStorageReservation;
        return capability.reserveOwned(allocator, backend.kind(), class, bound);
    }
    fn attachPhysicalCharge(backend: ops.ComputeBackend, tensor: ops.CT, binding: admission.BackingReservation) !void {
        if (backend.kind() != .metal) return;
        const retirement = @import("../execution/resource_retirement.zig");
        const ticket = binding.attachment orelse return error.UnpreparedPhysicalRetirement;
        var scopes = (try retirement.begin(backend, &.{tensor})) orelse return error.UnsupportedPhysicalStorageRetirement;
        defer scopes.end();
        // A canonical physical bank is exactly one raw allocating Ref/runtime.
        // No hidden multi-origin publication can consume a one-origin ticket.
        if (scopes.count != 1) return error.UnsupportedPhysicalStorageRetirement;
        try ticket.attach(&scopes.scopes()[0]);
    }
    pub fn ensureAdmission(self: *Lease, capability: admission.Capability) !void {
        if (self.admission) |existing| {
            if (!existing.identity.eql(capability.identity)) return error.PhysicalStorageAdmissionMismatch;
            return;
        }
        const bytes = self.backing_byte_size orelse self.byte_size;
        if (capability.services.takeBacking != null) {
            try validatePreparedBackingSupport(self.backend);
            if (self.backend.kind() == .metal) {
                const mark = self.backend.vtable.kernels.?.markPhysicalStorageAdmitted.?;
                try mark(self.backend.ptr, self.tensor);
            }
        }
        if (try capability.reserveBacking(self.allocator, self.backend.kind(), self.accounting_class, bytes)) |backing| {
            const reservation = backing.owner.charge.storageReservation() orelse @panic("prepared physical owner lacks exact storage reservation");
            // The existing SDK bank has already escaped allocation. Install its
            // independently retained token BEFORE any fallible witness work;
            // failure retires the frame, and final bank destruction keeps quota.
            self.retirement_owner = backing.owner;
            self.admission = reservation;
            if (!reservation.identity.eql(capability.identity) or backing.owner.admittedBytes() != bytes)
                return error.InvalidPhysicalStorageReservation;
            try attachPhysicalCharge(self.backend, self.tensor, backing);
            backing.owner.measure(backing.construction_epoch, bytes);
            return;
        }
        const owner = try self.allocator.create(admission.Reservation);
        errdefer self.allocator.destroy(owner);
        owner.* = try capability.reserve(self.allocator, self.backend.kind(), self.accounting_class, bytes);
        self.admission = owner;
    }
    pub fn retain(self: *Lease) void {
        const before = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(before > 0);
    }
    pub fn unique(self: *const Lease) bool {
        return self.references.load(.acquire) == 1;
    }
    pub fn admittedBytes(self: *const Lease) usize {
        if (self.retirement_owner) |owner| return @min(owner.admittedBytes(), self.backing_byte_size orelse self.byte_size);
        return if (self.admission) |value| value.reservedBytes() else 0;
    }
    pub fn admittedAmounts(self: *const Lease) @import("../runtime/tier/memory.zig").AdmissionAmounts {
        if (self.retirement_owner) |owner| return owner.charge.storageAmountsForBytes(self.admittedBytes());
        return if (self.admission) |value| value.amounts() else .{};
    }
    pub fn release(self: *Lease) void {
        self.releaseWithBoundary(null);
    }
    /// The caller retains its actual numerical/allocator owner through backend
    /// free. Quota metadata afterward needs only independent Domain/SDK witnesses.
    pub fn releaseWithBoundary(self: *Lease, cleanup: ?*const @import("../execution/retirement_sink.zig").Epoch) void {
        self.releaseWithCleanup(.{ .boundary = cleanup });
    }
    pub fn releaseWithCleanup(self: *Lease, cleanup_context: @import("../execution/cleanup_context.zig").Context) void {
        const cleanup = cleanup_context.boundary;
        var before = self.references.load(.acquire);
        while (true) {
            std.debug.assert(before > 0);
            // Do not mutate the last ref/SDK state before proving authority.
            if (before == 1 and self.retirement_owner != null and cleanup == null)
                @import("antfly_platform").inference_process_supervisor.restartWorker();
            before = self.references.cmpxchgWeak(before, before - 1, .acq_rel, .acquire) orelse break;
        }
        if (before != 1) return;
        self.backend.freeWithCleanup(self.tensor, cleanup_context);
        const reservation = self.admission;
        const retired = self.retirement_owner;
        const allocator = self.allocator;
        const ledger = self.ledger;
        const class = self.accounting_class;
        const requested = self.byte_size;
        const backing = self.backing_byte_size;
        allocator.destroy(self);
        if (ledger) |owner| owner.freed(class, requested, backing);
        if (retired) |owner| owner.release(cleanup.?) else if (reservation) |owner| {
            owner.release();
            allocator.destroy(owner);
        }
    }
};

/// A captured executable owns every planned address it references. This is
/// separate from an invocation's tensor handles: graph replay omits the host
/// calls which would otherwise keep those handles alive. Release only after
/// destroying the executable and completing its last submission.
pub const CapturedLeases = struct {
    values: std.ArrayListUnmanaged(*Lease) = .empty,

    pub fn retain(self: *CapturedLeases, allocator: std.mem.Allocator, lease: *Lease) !void {
        for (self.values.items) |existing| if (existing == lease) return;
        try self.values.append(allocator, lease);
        lease.retain();
    }
    pub fn deinit(self: *CapturedLeases, allocator: std.mem.Allocator) void {
        for (self.values.items) |lease| lease.release();
        self.values.deinit(allocator);
        self.* = .{};
    }
};

pub const View = struct {
    storage: *Lease,
    shape: []const i32,
    dtype: ops.GraphDType = .f32,
    /// Clone only logical/view metadata over new owned backing. Used by
    /// planned independent publication; never aliases the source's data.
    layout_source: ?ops.CT = null,
};

/// A frame-scoped captured submission. Replay can reuse its own address pins;
/// published views and foreign captures block mutable storage reuse. Immutable
/// program storage is shared according to its allocation's ownership.
/// Begin/commit only record and instantiate; they never submit GPU work.
/// Failed recording/instantiation can therefore abort and run ordinary kernels.
/// The backend context must outlive this owner.
pub const Replay = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        begin: *const fn (*anyopaque, []const ?*Lease) anyerror!bool,
        commit: *const fn (*anyopaque) anyerror!void,
        abort: *const fn (*anyopaque) void,
        launch: *const fn (*anyopaque) anyerror!void,
        retains: *const fn (*anyopaque, *const Lease) bool,
        deinit: *const fn (*anyopaque) void,
    };
    pub fn begin(self: Replay, leases: []const ?*Lease) !bool {
        return self.vtable.begin(self.ptr, leases);
    }
    pub fn commit(self: Replay) !void {
        return self.vtable.commit(self.ptr);
    }
    pub fn abort(self: Replay) void {
        self.vtable.abort(self.ptr);
    }
    pub fn launch(self: Replay) !void {
        return self.vtable.launch(self.ptr);
    }
    pub fn retains(self: Replay, lease: *const Lease) bool {
        return self.vtable.retains(self.ptr, lease);
    }
    pub fn deinit(self: Replay) void {
        self.vtable.deinit(self.ptr);
    }
};

/// A recorded submission fence. Waiting may enqueue a device dependency; it
/// need not block the host. The owner records before publishing a frame as idle.
pub const Completion = struct {
    ptr: *anyopaque,
    vtable: *const VTable,
    pub const VTable = struct {
        record: *const fn (*anyopaque) anyerror!void,
        before_reuse: *const fn (*anyopaque) anyerror!void,
        wait: ?*const fn (*anyopaque) anyerror!void = null,
        deinit: *const fn (*anyopaque) void,
    };
    pub fn record(self: Completion) !void {
        return self.vtable.record(self.ptr);
    }
    pub fn beforeReuse(self: Completion) !void {
        return self.vtable.before_reuse(self.ptr);
    }
    pub fn wait(self: Completion) !void {
        if (self.vtable.wait) |synchronize| try synchronize(self.ptr);
    }
    pub fn deinit(self: Completion) void {
        self.vtable.deinit(self.ptr);
    }
};

test "physical storage pins views across ownership release and assigned writes preserve inputs" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const storage = (try Lease.create(a, cb, 16 * @sizeOf(f32))).?;
    var storage_owned = true;
    defer if (storage_owned) storage.release();
    try std.testing.expect(storage.unique());
    const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{ 2, 4 } })).?;
    defer cb.free(view);
    try std.testing.expect(!storage.unique());
    const input = try cb.fromFloat32Shape(&.{ -4, -3, -2, -1, 0, 1, 2, 3 }, &.{ 2, 4 });
    defer cb.free(input);
    const ml = @import("ml");
    const shape = ml.graph.Shape.init(.f32, &.{ 2, 4 });
    const descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .unary = .relu }, .output_shape = shape, .input_shapes = .{ shape, undefined, undefined }, .input_count = 1 };
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{input} }));
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{input}, .output = view }));
    const values = try cb.toFloat32(view, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 0, 0, 0, 0, 0, 1, 2, 3 }, values);
    const unchanged = try cb.toFloat32(input, a);
    defer a.free(unchanged);
    try std.testing.expectEqualSlices(f32, &.{ -4, -3, -2, -1, 0, 1, 2, 3 }, unchanged);
    // Drop the plan's ownership. The returned view still owns its storage.
    storage.release();
    storage_owned = false;
    const retained = try cb.toFloat32(view, a);
    defer a.free(retained);
    try std.testing.expectEqualSlices(f32, values, retained);
}

test "physical storage and view metadata release on every allocation failure" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            const cb = context.computeBackend();
            const lease = (try Lease.create(a, cb, 64)).?;
            defer lease.release();
            const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = &.{ 2, 4 } })).?;
            defer cb.free(view);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "assigned normalization and softmax match allocating kernels on stable storage" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const gamma = try b.parameter("gamma", ml.graph.Shape.init(.f32, &.{4}));
    const beta = try b.parameter("beta", ml.graph.Shape.init(.f32, &.{4}));
    const nodes = [_]ml.graph.NodeId{ try b.layerNorm(x, gamma, beta, 4, 1e-5), try b.rmsNorm(x, gamma, 4, 1e-5), try b.softmax(x), try b.logSoftmax(x) };
    const input = try cb.fromFloat32Shape(&.{ -100, 0, 100, 101, 1, 1, 1, 1 }, &.{ 2, 4 });
    defer cb.free(input);
    const weight = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{4});
    defer cb.free(weight);
    const bias = try cb.fromFloat32Shape(&.{ -1, 1, -2, 2 }, &.{4});
    defer cb.free(bias);
    const storage = (try Lease.create(a, cb, 8 * @sizeOf(f32))).?;
    defer storage.release();
    const output = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{ 2, 4 } })).?;
    defer cb.free(output);
    for (nodes, 0..) |node, index| {
        const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
        const operands = [_]@import("../graph/backend_contracts.zig").CT{ input, weight, bias };
        const inputs = operands[0..descriptor.input_count];
        try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = inputs }));
        try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = inputs, .output = output }));
        const reference = switch (index) {
            0 => try cb.layerNorm(input, weight, bias, 4, 1e-5),
            1 => try cb.rmsNorm(input, weight, 4, 1e-5),
            2 => try cb.primSoftmax(input, 4),
            else => try cb.primLogSoftmax(input, 4),
        };
        defer cb.free(reference);
        const expected = try cb.toFloat32(reference, a);
        defer a.free(expected);
        const actual = try cb.toFloat32(output, a);
        defer a.free(actual);
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    // A malformed learned width must decline before any target submission.
    graph.nodeMut(nodes[0]).op.fused_layer_norm.dim = 3;
    try std.testing.expect(ops.kernel_descriptor.lower(&graph, nodes[0]) == null);
}

test "assigned reductions share axis geometry preserve traversal and reject invalid axes" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 3, 2 }));
    const nodes = [_]ml.graph.NodeId{ try b.reduceSum(x, &.{ 0, 2 }), try b.reduceMean(x, &.{ 0, 2 }), try b.reduceMax(x, &.{ 0, 2 }), try b.reduceSum(x, &.{}) };
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 }, &.{ 2, 3, 2 });
    defer cb.free(input);
    const storage = (try Lease.create(a, cb, 12 * @sizeOf(f32))).?;
    defer storage.release();
    for (nodes, 0..) |node, index| {
        const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
        var dims: [8]i32 = undefined;
        for (descriptor.output_shape.dims[0..descriptor.output_shape.rank()], 0..) |dim, i| dims[i] = @intCast(dim);
        const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = dims[0..descriptor.output_shape.rank()] })).?;
        defer cb.free(view);
        try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{input}, .output = view }));
        const actual = try cb.toFloat32(view, a);
        defer a.free(actual);
        const expected: []const f32 = switch (index) {
            0 => &.{ 18, 26, 34 },
            1 => &.{ 4.5, 6.5, 8.5 },
            2 => &.{ 8, 10, 12 },
            else => &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 },
        };
        try std.testing.expectEqualSlices(f32, expected, actual);
    }
    const geometry = try ops.kernel_descriptor.ReductionGeometry.init(&.{ 2, 3, 2 }, &.{ 0, 2, 0 });
    try std.testing.expectEqual(@as(usize, 4), geometry.reduce_count);
    try std.testing.expectEqual(@as(usize, 3), geometry.output_count);
    try std.testing.expectError(error.InvalidShape, ops.kernel_descriptor.ReductionGeometry.init(&.{ 2, 3, 2 }, &.{3}));
    graph.nodeMut(nodes[0]).op.reduce_sum.axes[0] = 8;
    try std.testing.expect(ops.kernel_descriptor.lower(&graph, nodes[0]) == null);
}

test "assigned dense contractions preserve all matrix layouts and batched accumulation" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    for ([_]bool{ false, true }) |batched| {
        for ([_]bool{ false, true }) |left_transposed| for ([_]bool{ false, true }) |right_transposed| {
            const left_dims = [_]i64{ 2, if (left_transposed) 4 else 2, if (left_transposed) 2 else 4 };
            const right_dims = [_]i64{ 2, if (right_transposed) 3 else 4, if (right_transposed) 4 else 3 };
            const left_shape = left_dims[if (batched) @as(usize, 0) else 1..];
            const right_shape = right_dims[if (batched) @as(usize, 0) else 1..];
            var graph = ml.graph.Graph.init(a);
            defer graph.deinit();
            var b = ml.graph.Builder.init(&graph);
            const lhs = try b.parameter("lhs", ml.graph.Shape.init(.f32, left_shape));
            const rhs = try b.parameter("rhs", ml.graph.Shape.init(.f32, right_shape));
            const node = if (batched) try b.matmul3DLayout(lhs, rhs, left_transposed, right_transposed) else try b.matmul2DLayout(lhs, rhs, left_transposed, right_transposed);
            const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
            var left_values: [16]f32 = undefined;
            var right_values: [24]f32 = undefined;
            for (&left_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 7)) / 8;
            for (&right_values, 0..) |*value, i| value.* = @as(f32, @floatFromInt(@as(i32, @intCast(i)) - 11)) / 8;
            const left_count = if (batched) @as(usize, 16) else 8;
            const right_count = if (batched) @as(usize, 24) else 12;
            var ld: [3]i32 = undefined;
            var rd: [3]i32 = undefined;
            for (left_shape, 0..) |dim, i| ld[i] = @intCast(dim);
            for (right_shape, 0..) |dim, i| rd[i] = @intCast(dim);
            const left = try cb.fromFloat32Shape(left_values[0..left_count], ld[0..left_shape.len]);
            defer cb.free(left);
            const right = try cb.fromFloat32Shape(right_values[0..right_count], rd[0..right_shape.len]);
            defer cb.free(right);
            const attrs = graph.node(node).op.dot_general;
            const reference = try cb.primDotGeneral(left, right, left_shape, right_shape, attrs.lhs_contracting[0..1], attrs.rhs_contracting[0..1], attrs.lhs_batch[0..attrs.num_batch], attrs.rhs_batch[0..attrs.num_batch]);
            defer cb.free(reference);
            const storage = (try Lease.create(a, cb, descriptor.elements() * @sizeOf(f32))).?;
            defer storage.release();
            var output_shape: [8]i32 = undefined;
            for (descriptor.output_shape.dims[0..descriptor.output_shape.rank()], 0..) |dim, i| output_shape[i] = @intCast(dim);
            const output = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = output_shape[0..descriptor.output_shape.rank()] })).?;
            defer cb.free(output);
            try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{ left, right }, .output = output }));
            const expected = try cb.toFloat32(reference, a);
            defer a.free(expected);
            const actual = try cb.toFloat32(output, a);
            defer a.free(actual);
            try std.testing.expectEqualSlices(f32, expected, actual);
        };
    }
}

test "assigned linear keeps source-backed weight scheduling without materializing its input handle" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const ml = @import("ml");
    var graph = ml.graph.Graph.init(a);
    defer graph.deinit();
    var b = ml.graph.Builder.init(&graph);
    const x = try b.parameter("x", ml.graph.Shape.init(.f32, &.{ 2, 4 }));
    const w = try b.parameter("weight", ml.graph.Shape.init(.f32, &.{ 3, 4 }));
    const bias_node = try b.parameter("bias", ml.graph.Shape.init(.f32, &.{3}));
    const node = try b.linear(x, w, bias_node, 2, 4, 3);
    const descriptor = ops.kernel_descriptor.lower(&graph, node).?;
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4, 5, 6, 7, 8 }, &.{ 2, 4 });
    defer cb.free(input);
    const bias = try cb.fromFloat32Shape(&.{ 0.5, -0.5, 1 }, &.{3});
    defer cb.free(bias);
    const tensor = try @import("../backends/tensor.zig").Tensor.initFloat32(a, "weight", &.{ 3, 4 }, &.{ 1, 0, -1, 0, 0, 1, 0, -1, 1, 1, 1, 1 });
    const weight = try context.native.compute.importOwnedStaticTensor(tensor);
    defer cb.free(weight);
    const storage = (try Lease.create(a, cb, 6 * @sizeOf(f32))).?;
    defer storage.release();
    const output = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{ 2, 3 } })).?;
    defer cb.free(output);
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{ input, weight, bias } }));
    try std.testing.expect(try cb.vtable.kernels.?.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{ input, weight, bias }, .output = output }));
    const values = try cb.toFloat32(output, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ -1.5, -2.5, 11, -1.5, -2.5, 27 }, values);
}

test "captured physical addresses deduplicate and survive executable plan eviction" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            const cb = context.computeBackend();
            const lease = (try Lease.create(a, cb, 32)).?;
            var owner = true;
            defer if (owner) lease.release();
            var captured = CapturedLeases{};
            defer captured.deinit(a);
            try captured.retain(a, lease);
            try captured.retain(a, lease);
            try std.testing.expectEqual(@as(usize, 1), captured.values.items.len);
            try std.testing.expectEqual(@as(usize, 2), lease.references.load(.acquire));
            lease.release();
            owner = false;
            // The executable's owner is gone; replay still owns valid storage.
            try std.testing.expect(lease.unique());
            const data = (try cb.hostFloat32Write(lease.tensor)).?;
            data[0] = 42;
            try std.testing.expectEqual(@as(f32, 42), (try cb.hostFloat32Read(lease.tensor)).?[0]);
            captured.deinit(a);
            // Aborted/failed capture cleanup is idempotent.
            captured.deinit(a);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "row snapshots keep their own values after source storage changes" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const input = try cb.fromFloat32Shape(&.{ 1, 2, 3, 4 }, &.{ 2, 2 });
    defer cb.free(input);
    const published = try cb.snapshotRows2D(a, input, 1, 1, 2);
    defer cb.free(published);
    @memset((try cb.hostFloat32Write(input)).?, 9);
    const values = try cb.toFloat32(published, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 3, 4 }, values);
}

test "physical storage accounting survives program eviction and capture pins at every allocation failure" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
            defer context.deinit(a);
            const cb = context.computeBackend();
            const ledger = try accounting.Ledger.create(a);
            defer ledger.release(); // Diagnostic observer, independent of the program.
            ledger.retain();
            var program_owned = true;
            defer if (program_owned) ledger.release();
            const constant = (try Lease.createTracked(a, cb, 64, ledger, .constant)).?;
            defer constant.release();
            const activation = (try Lease.createTracked(a, cb, 128, ledger, .activation)).?;
            var activation_owned = true;
            defer if (activation_owned) activation.release();
            const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = activation, .shape = &.{ 2, 16 } })).?;
            var view_owned = true;
            defer if (view_owned) cb.free(view);
            var capture = CapturedLeases{};
            defer capture.deinit(a);
            try capture.retain(a, activation);
            try capture.retain(a, activation); // A second address use is not a new allocation.
            const warm = ledger.snapshot();
            try std.testing.expectEqual(@as(u64, 2), warm.total.leases_created);
            try std.testing.expectEqual(@as(?u64, 192), warm.total.backing_live_bytes);
            try std.testing.expectEqual(@as(u64, 64), warm.constant.requested_live_bytes);
            try std.testing.expectEqual(@as(u64, 128), warm.activation.requested_live_bytes);
            program_owned = false;
            ledger.release();
            activation_owned = false;
            activation.release();
            capture.deinit(a);
            try std.testing.expectEqual(@as(?u64, 192), ledger.snapshot().total.backing_live_bytes);
            // Only the published tensor retains the retired activation now.
            view_owned = false;
            cb.free(view);
            const retired = ledger.snapshot();
            try std.testing.expectEqual(@as(?u64, 64), retired.total.backing_live_bytes);
            try std.testing.expectEqual(@as(?u64, 192), retired.total.backing_peak_bytes);
            try std.testing.expectEqual(@as(u64, 1), retired.total.leases_released);
            try std.testing.expectEqual(@as(u64, 0), retired.activation.requested_live_bytes);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "physical CUDA storage accounting measures its actual backing span" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = @import("../backends/compute_context.zig").BackendContext.init(a, .cuda, null) catch |err| switch (err) {
        error.CudaUnavailable, error.NoCudaDevices => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const ledger = try accounting.Ledger.create(a);
    defer ledger.release();
    const lease = (try Lease.createTracked(a, context.computeBackend(), 68, ledger, .workspace)).?;
    var lease_owned = true;
    defer if (lease_owned) lease.release();
    const live = ledger.snapshot();
    try std.testing.expectEqual(@as(u64, 68), live.workspace.requested_live_bytes);
    try std.testing.expect(live.workspace.backing_live_bytes.? >= 68);
    try std.testing.expectEqual(lease.backing_byte_size.?, live.workspace.backing_live_bytes.?);
    lease_owned = false;
    lease.release();
    const done = ledger.snapshot();
    try std.testing.expectEqual(@as(?u64, 0), done.total.backing_live_bytes);
    try std.testing.expectEqual(@as(u64, 1), done.total.leases_created);
    try std.testing.expectEqual(@as(u64, 1), done.total.leases_released);
}

test "physical Metal allocation admission bound covers SDK span without audio buckets" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const functions = cb.vtable.kernels.?;
    for ([_]usize{ 4, 8, 12, 16, 252, 256, 260, 4092, 4096, 4100, 16384 }) |bytes| {
        const upper = try functions.physicalStorageReservationBytes.?(cb.ptr, bytes);
        try std.testing.expect(upper >= bytes);
        const tensor = (try functions.allocatePhysicalStorage.?(cb.ptr, bytes)).?;
        defer cb.free(tensor);
        const actual = (try backingBytes(cb, tensor)) orelse return error.MissingPhysicalBackingMeasurement;
        try std.testing.expect(actual <= upper);
        if (bytes <= 16) try std.testing.expect(upper < 16384);
    }
}

test "physical storage admission outlives its request and plan while retained views exist" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        run: memory.AdmissionLease,
        credit: memory.AdmissionAmounts = .{ .host_scratch_bytes = 64 },
        refs: usize = 1,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.run, .{
                .controller = &self.controller,
                .backend_class = .cpu,
                .limits = .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 },
                .check_live_memory = false,
                .credit = &self.credit,
                .lifetime = .{ .context = self, .retain = retain, .release = release },
            }, kind, class, bytes);
        }
    };
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    var owner = Owner{ .run = undefined };
    defer owner.controller.deinit();
    owner.run = try owner.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, .{ .host_scratch_bytes = 64 }, false);
    defer owner.run.release();
    const cap = admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .cpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
    const storage = (try Lease.createAdmitted(a, cb, 64, null, .activation, cap)).?;
    var storage_owned = true;
    defer if (storage_owned) storage.release();
    try std.testing.expectEqual(@as(usize, 2), owner.refs);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.run.amounts);
    const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{ 4, 4 } })).?;
    var view_owned = true;
    defer if (view_owned) cb.free(view);
    owner.run.release();
    storage.release();
    storage_owned = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, owner.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 2), owner.refs);
    cb.free(view);
    view_owned = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), owner.refs);
}

/// Common metadata-only projection of the authoritative owner stream.
pub fn backingBytes(backend: ops.ComputeBackend, tensor: ops.CT) !?usize {
    const functions = backend.vtable.kernels orelse return null;
    if (!functions.accepts(backend.vtable)) return null;
    if (functions.storageOwnership) |query| {
        var projection = @import("../execution/storage_ownership.zig").BackingBytes{};
        const coverage = try query(backend.ptr, tensor, projection.visitor());
        return if (coverage == .complete and projection.known) projection.value else null;
    }
    const legacy = functions.physicalStorageBytes orelse return null;
    return legacy(backend.ptr, tensor);
}

test "physical immutable literals reject writes through retained aliases before dispatch" {
    try testImmutableLiteralWrites(.native);
}
test "physical immutable Metal literals reject aliased writes without submission" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    try testImmutableLiteralWrites(.metal);
}
test "physical immutable CUDA literals reject aliased writes without submission" {
    if (comptime !@import("build_options").enable_cuda) return error.SkipZigTest;
    try testImmutableLiteralWrites(.cuda);
}
fn testImmutableLiteralWrites(comptime kind: @import("../backends/backends.zig").BackendType) !void {
    const a = std.testing.allocator;
    var context = @import("../backends/compute_context.zig").BackendContext.init(a, kind, std.testing.io) catch |err| switch (err) {
        error.CudaUnavailable => return error.SkipZigTest,
        else => return err,
    };
    defer context.deinit(a);
    const cb = context.computeBackend();
    const lease = (try Lease.createTracked(a, cb, 16, null, .constant)).?;
    defer lease.release();
    const functions = cb.vtable.kernels.?;
    const view = (try functions.physicalTensorView.?(cb.ptr, .{ .storage = lease, .shape = &.{ 2, 2 } })).?;
    defer cb.free(view);
    try functions.writePhysicalFloat32.?(cb.ptr, view, &.{ 1, 2, 3, 4 });
    const alias = try cb.primReshape(view, &.{ 1, 4 });
    defer cb.free(alias);
    const input = try cb.ensureDeviceResidentOwned(try cb.fromFloat32Shape(&.{ -1, -2, -3, -4 }, &.{ 1, 4 }));
    defer cb.free(input);
    try std.testing.expectError(error.ReadOnlyPhysicalStorage, functions.copyPhysicalInput.?(cb.ptr, alias, input));
    const shape = @import("ml").graph.Shape.init(.f32, &.{ 1, 4 });
    const descriptor = ops.kernel_descriptor.Descriptor{ .operation = .{ .unary = .negate }, .output_shape = shape, .input_shapes = .{ shape, undefined, undefined }, .input_count = 1 };
    try std.testing.expectError(error.ReadOnlyPhysicalStorage, functions.runAssignedKernel.?(cb.ptr, .{ .descriptor = &descriptor, .inputs = &.{input}, .output = alias }));
    const values = try cb.toFloat32(view, a);
    defer a.free(values);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4 }, values);
    const ownership = @import("../execution/storage_ownership.zig");
    var read_only = ownership.ReadOnly{};
    try std.testing.expectEqual(ownership.Coverage.complete, try functions.storageOwnership.?(cb.ptr, alias, read_only.visitor()));
    try std.testing.expect(read_only.value);
}

test "host index certificates reject another frame same generation stale writes and tighter bounds" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const a = std.testing.allocator;
    const route = @import("../execution/kernel_route.zig");
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const first = (try Lease.create(a, cb, 16)).?;
    defer first.release();
    const second = (try Lease.create(a, cb, 16)).?;
    defer second.release();
    try first.bindAllocationSlot(1, 7, 16);
    try second.bindAllocationSlot(1, 7, 16);
    const slot = route.OutputSlot{ .allocation = 1, .node = 3, .epoch = 7, .logical = @import("ml").graph.Shape.init(.i64, &.{2}), .writable_bytes = 16 };
    const assignment1 = route.AssignedOutput{ .slot = slot, .region = try first.writableRegion(1, 7, 16), .input_generation = 1 };
    const assignment2 = route.AssignedOutput{ .slot = slot, .region = try second.writableRegion(1, 7, 16), .input_generation = 1 };
    const proof1 = route.AssignedInput{ .node = 3, .lease_identity = @intFromPtr(first), .authority = assignment1.region.authority, .generation = 1 };
    const proof2 = route.AssignedInput{ .node = 3, .lease_identity = @intFromPtr(second), .authority = assignment2.region.authority, .generation = 1 };
    try first.certifyHostIndices(assignment1, 3);
    try second.certifyHostIndices(assignment2, 3);
    try std.testing.expect(first.hostInputMatches(proof1, 3, 1, 16));
    try std.testing.expect(second.hostInputMatches(proof2, 3, 1, 16));
    try std.testing.expect(!first.hostInputMatches(proof2, 3, 1, 16));
    try std.testing.expect(!second.hostInputMatches(proof1, 3, 1, 16));
    try std.testing.expect(first.hostInputMatches(proof1, 5, 1, 16));
    try std.testing.expect(!first.hostInputMatches(proof1, 2, 1, 16));
    var next = assignment1;
    next.input_generation = 2;
    try first.certifyHostIndices(next, 3);
    try std.testing.expect(!first.hostInputMatches(proof1, 3, 1, 16));
    var current = proof1;
    current.generation = 2;
    try std.testing.expect(first.hostInputMatches(current, 3, 2, 16));
    first.invalidateHostIndices();
    try std.testing.expect(!first.hostInputMatches(current, 3, 2, 16));
}

test "Metal standalone physical reservation covers model scale allocation extents" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const functions = cb.vtable.kernels.?;
    var exceeded: usize = 0;
    // Includes standalone page boundaries and MiniLM positional/word tables.
    for ([_]usize{ 16, 4100, 16380, 16384, 16388, 24576, 32764, 32768, 32772, 98304, 512 * 384 * 4, 30522 * 384 * 4 }) |bytes| {
        const upper = try functions.physicalStorageReservationBytes.?(cb.ptr, bytes);
        const tensor = (try functions.allocatePhysicalStorage.?(cb.ptr, bytes)).?;
        defer cb.free(tensor);
        const actual = (try backingBytes(cb, tensor)) orelse return error.MissingPhysicalBackingMeasurement;
        if (upper < bytes or actual > upper) {
            std.debug.print("METAL_PHYSICAL_BOUND requested={d} reserved={d} actual={d}\n", .{ bytes, upper, actual });
            exceeded += 1;
        }
    }
    if (exceeded != 0) return error.MetalPhysicalAllocationBoundViolated;
    try std.testing.expectError(error.InvalidPhysicalStorageReservation, functions.physicalStorageReservationBytes.?(cb.ptr, std.math.maxInt(usize)));
}

test "Metal standalone planned banks admit their exact bound and reject one byte less" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const Owner = struct {
        controller: memory.AdmissionController = .{},
        run: memory.AdmissionLease,
        limit: usize,
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, kind: @import("../graph/backend_contracts.zig").BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return admission.fromRunLease(allocator, &self.run, .{ .controller = &self.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = self.limit }, .check_live_memory = false }, kind, class, bytes);
        }
    };
    for ([_]usize{ 16, 16388, 24576, 32764, 32772, 30522 * 384 * 4 }) |bytes| {
        const upper = try cb.vtable.kernels.?.physicalStorageReservationBytes.?(cb.ptr, bytes);
        var owner = Owner{ .run = undefined, .limit = upper - 1 };
        defer owner.controller.deinit();
        owner.run = try owner.controller.tryAcquire(.gpu, .{}, .{}, false);
        defer owner.run.release();
        const cap = admission.Capability{ .identity = .{ .controller = &owner.controller, .backend_class = .gpu }, .context = &owner, .services = &.{ .reserve = Owner.reserve } };
        if (Lease.createAdmitted(a, cb, bytes, null, .activation, cap)) |unexpected| {
            if (unexpected) |storage| storage.release();
            return error.UnderfundedPhysicalBankAdmitted;
        } else |err| try std.testing.expectEqual(error.ResourceLimitExceeded, err);
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.controller.snapshot());
        owner.limit = upper;
        const storage = (try Lease.createAdmitted(a, cb, bytes, null, .activation, cap)).?;
        var storage_live = true;
        defer if (storage_live) storage.release();
        const actual = storage.backing_byte_size orelse return error.MissingPhysicalBackingMeasurement;
        try std.testing.expect(actual >= bytes and actual <= upper);
        try std.testing.expectEqual(bytes, storage.byte_size);
        try std.testing.expectEqual(actual, storage.admission.?.reservedBytes());
        try std.testing.expectEqual(actual, owner.controller.snapshot().backendTotalBytes());
        const view = (try cb.vtable.kernels.?.physicalTensorView.?(cb.ptr, .{ .storage = storage, .shape = &.{@intCast(bytes / 4)} })).?;
        var view_live = true;
        defer if (view_live) cb.free(view);
        storage.release();
        storage_live = false;
        try std.testing.expectEqual(actual, owner.controller.snapshot().backendTotalBytes());
        cb.free(view);
        view_live = false;
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, owner.controller.snapshot());
    }
}

fn preparedBankCacheLifetimeCase(case_allocator: std.mem.Allocator) !void {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const sink = @import("../execution/retirement_sink.zig");
    const prepared_storage = @import("../execution/prepared_storage_admission.zig");
    const Source = struct {
        controller: memory.AdmissionController = .{},
        run: memory.AdmissionLease = undefined,
        credit: memory.AdmissionAmounts = .{ .host_scratch_bytes = 64 },
        gate: std.atomic.Mutex = .unlocked,
        refs: usize = 1,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.gate.tryLock()) @panic("quota callback ran inside the physical owner gate");
            self.gate.unlock();
            self.refs += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.gate.tryLock()) @panic("quota callback ran inside the physical owner gate");
            self.gate.unlock();
            self.refs -= 1;
        }
        fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, backend: @import("../graph/backend_contracts.zig").BackendKind, class: accounting.Class, bytes: usize) !admission.Reservation {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.gate.tryLock()) @panic("quota callback ran inside the physical owner gate");
            self.gate.unlock();
            return admission.fromRunLease(allocator, &self.run, .{ .controller = &self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, .check_live_memory = false, .credit = &self.credit, .lifetime = .{ .context = self, .retain = retain, .release = release } }, backend, class, bytes);
        }
    };
    var backend_context = try @import("../backends/compute_context.zig").BackendContext.init(case_allocator, .native, null);
    defer backend_context.deinit(case_allocator);
    const backend = backend_context.computeBackend();
    var source = Source{};
    defer source.controller.deinit();
    source.run = try source.controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64, .combined_limit_bytes = 64 }, .{ .host_scratch_bytes = 64 }, false);
    defer source.run.release();
    const capability = admission.Capability{ .identity = .{ .controller = &source.controller, .backend_class = .cpu }, .context = &source, .services = &.{ .reserve = Source.reserve } };
    var ref = try sink.Ref.create();
    defer ref.release();
    var view: ?ops.CT = null;
    defer if (view) |remaining| {
        var fallback = ref.enter() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        defer fallback.finish();
        var boundary = fallback.retainEpoch() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        defer boundary.release();
        backend.freeWithBoundary(remaining, &boundary);
    };
    {
        const preparation = (try prepared_storage.Prepared.prepare(capability, &.{.{ .backend = .native, .class = .activation, .bytes = 64 }})).?;
        var unbound = true;
        defer if (unbound) preparation.abortBeforeExecution() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        var guard = try ref.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try preparation.bindConstructionEpoch(&epoch);
        unbound = false;
        defer preparation.releaseAfterExecution(&epoch);
        @import("antfly_platform").sync.lockYielding(&source.gate);
        defer source.gate.unlock();
        const bank = (try Lease.createAdmitted(case_allocator, backend, 64, null, .activation, prepared_storage.Prepared.forDispatch(preparation, capability.identity))).?;
        defer bank.releaseWithBoundary(&epoch);
        view = (try backend.vtable.kernels.?.physicalTensorView.?(backend.ptr, .{ .storage = bank, .shape = &.{ 4, 4 } })).?;
        try backend.vtable.kernels.?.writePhysicalFloat32.?(backend.ptr, view.?, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 });
        const CacheOwner = struct {
            allocator: std.mem.Allocator,
            bank: *Lease,
            dimension: i64,
            references: usize = 1,
            destroyed: *usize,
            const Factory = struct { allocator: std.mem.Allocator, bank: *Lease, destroyed: *usize };
            fn create(factory: *Factory, facts: []const @import("../execution/kernel_route.zig").InputBinding) !?*@This() {
                const owner = try factory.allocator.create(@This());
                factory.bank.retain();
                owner.* = .{ .allocator = factory.allocator, .bank = factory.bank, .dimension = facts[0].value.imported_host.logical.dim(0), .destroyed = factory.destroyed };
                return owner;
            }
            pub fn retain(self: *@This()) *@This() {
                self.references += 1;
                return self;
            }
            pub fn release(self: *@This()) void {
                self.releaseWithCleanup(.{});
            }
            pub fn releaseWithCleanup(self: *@This(), cleanup: @import("../execution/cleanup_context.zig").Context) void {
                self.references -= 1;
                if (self.references != 0) return;
                self.bank.releaseWithCleanup(cleanup);
                self.destroyed.* += 1;
                self.allocator.destroy(self);
            }
            pub fn matches(self: *const @This(), facts: []const @import("../execution/kernel_route.zig").InputBinding) bool {
                return facts.len == 1 and facts[0].value == .imported_host and self.dimension == facts[0].value.imported_host.logical.dim(0);
            }
        };
        const All = struct {
            pub fn accept(_: @This(), _: *CacheOwner) bool {
                return true;
            }
        };
        var destroyed: usize = 0;
        var factory = CacheOwner.Factory{ .allocator = case_allocator, .bank = bank, .destroyed = &destroyed };
        var cache = @import("../execution/prepared_routes.zig").InternedVariants(CacheOwner){ .allocator = case_allocator, .limit = 1 };
        var cache_open = true;
        defer if (cache_open) cache.deinitWithCleanup(.{ .boundary = &epoch });
        const Fact = @import("../execution/kernel_route.zig").InputBinding;
        const first_fact = [_]Fact{.{ .node_id = 0, .value = .{ .imported_host = .{ .logical = @import("ml").graph.Shape.init(.f32, &.{4}), .target = .native } } }};
        const second_fact = [_]Fact{.{ .node_id = 0, .value = .{ .imported_host = .{ .logical = @import("ml").graph.Shape.init(.f32, &.{8}), .target = .native } } }};
        const first_route = (try cache.prepareFilteredWithCleanup(&factory, &first_fact, CacheOwner.create, All{}, .{ .boundary = &epoch })).?;
        var first_open = true;
        defer if (first_open) first_route.releaseWithCleanup(.{ .boundary = &epoch });
        const next_route = (try cache.prepareFilteredWithCleanup(&factory, &second_fact, CacheOwner.create, All{}, .{ .boundary = &epoch })).?;
        next_route.releaseWithCleanup(.{ .boundary = &epoch });
        // The held first selection survives a real LRU eviction. Dropping all
        // route roots still cannot retire the physical view's 64-byte backing.
        try std.testing.expectEqual(@as(usize, 1), cache.snapshot().evictions);
        first_route.releaseWithCleanup(.{ .boundary = &epoch });
        first_open = false;
        cache.deinitWithCleanup(.{ .boundary = &epoch });
        cache_open = false;
        try std.testing.expectEqual(@as(usize, 2), destroyed);
    }
    // Construction A has ended and can no longer authorize future cleanup.
    // Reuse every finite Sink record while only the true output keeps quota.
    for (0..70) |_| {
        var later = try ref.enter();
        later.finish();
    }
    try std.testing.expectEqual(@as(usize, 64), source.controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2), source.refs);
    const actual = try backend.toFloat32(view.?, case_allocator);
    defer case_allocator.free(actual);
    try std.testing.expectEqualSlices(f32, &.{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16 }, actual);
    var cleanup = try ref.enter();
    defer cleanup.finish();
    var current = try cleanup.retainEpoch();
    defer current.release();
    {
        @import("antfly_platform").sync.lockYielding(&source.gate);
        defer source.gate.unlock();
        backend.freeWithBoundary(view.?, &current);
        view = null;
        try std.testing.expectEqual(@as(usize, 64), source.controller.snapshot().host_scratch_bytes);
    }
    cleanup.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, source.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), source.refs);
}

test "actual Native bank survives route eviction held view and exact final cleanup epoch" {
    try preparedBankCacheLifetimeCase(std.testing.allocator);
}
test "actual Native bank route cleanup evictions unwind every allocation failure" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, preparedBankCacheLifetimeCase, .{});
}
