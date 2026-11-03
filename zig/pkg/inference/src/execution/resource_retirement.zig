// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Complete run charges retire on a checked CPU boundary after backing dies.
//! The aggregate charge owns only quota and an independent controller-domain
//! pin. Per-origin groups contain no provider/model/allocator-arena borrows.
//! GPU buffer witnesses only change group atomics; a process-stable intrusive
//! queue keeps their metadata valid even after an origin provider has closed.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");
const admission = @import("../runtime/tier/admission_domain.zig");
const ops = @import("../ops/ops.zig");
const platform = @import("antfly_platform");
const metadata_allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());

pub const Node = extern struct {
    next: ?*Node = null,
    context: *anyopaque,
    release_fn: *const fn (*anyopaque) callconv(.c) void,
};
pub const Payload = extern struct {
    context: *anyopaque,
    retain_fn: *const fn (*anyopaque) callconv(.c) void,
    release_fn: *const fn (*anyopaque) callconv(.c) void,
    /// Static process notification only: schedule CPU poll, never destroy here.
    notify_fn: ?*const fn () callconv(.c) void = null,
};

/// Value identity only. No callback dereferences the provider's queue field.
pub const Queue = struct {
    identity: u64,
    pub fn init() Queue {
        return .{ .identity = createIdentity() };
    }
    pub fn createIdentity() callconv(.c) u64 {
        return @import("source_identity.zig").next();
    }
};

pub const Scope = struct {
    context: *anyopaque,
    origin: Queue = .{ .identity = 0 },
    pending_batches: usize = 0,
    outputs: []const ops.CT,
    active: bool = false,
    attached: bool = false,
    vtable: *const VTable,
    pub const Begun = struct { origin: Queue, pending_batches: usize };
    pub const VTable = struct {
        begin: *const fn (*anyopaque, []const ops.CT) anyerror!Begun,
        /// Error publishes NO batch nodes. Partial payload witnesses may live;
        /// all retains happen synchronously BEFORE this callback returns.
        attach: *const fn (*anyopaque, []const ops.CT, []Node, Payload) anyerror!void,
        attach_prepared: ?*const fn (*anyopaque, []const ops.CT, []Node, Payload, *BackendPreparation) anyerror!void = null,
        end: *const fn (*anyopaque) void,
        /// Schedules independent CPU cleanup; no context/provider borrow.
        notify: ?*const fn () callconv(.c) void = null,
    };
    pub fn attach(self: *Scope, nodes: []Node, payload: Payload) !void {
        if (!self.active or self.attached or nodes.len != self.pending_batches) return error.InvalidResourceRetirement;
        self.attached = true;
        try self.vtable.attach(self.context, self.outputs, nodes, payload);
    }
    pub fn attachPrepared(self: *Scope, nodes: []Node, payload: Payload, prepared: *BackendPreparation) !void {
        if (!self.active or self.attached or nodes.len != self.pending_batches) return error.InvalidResourceRetirement;
        const callback = self.vtable.attach_prepared orelse return error.UnsupportedPreparedResourceRetirement;
        self.attached = true;
        try callback(self.context, self.outputs, nodes, payload, prepared);
    }
    pub fn end(self: *Scope) void {
        if (!self.active) return;
        self.active = false;
        self.vtable.end(self.context);
    }
};
/// Inline values are addressed only after this set reaches its final caller.
/// There is no pointer/slice into a pre-return stack copy, and every output or
/// provider context is borrowed only until end() finishes synchronous cleanup.
pub const ScopeSet = struct {
    inline_scopes: [4]Scope = undefined,
    spill: ?[]Scope = null,
    count: usize = 0,
    started: bool = false,
    prepared_capacity: ?usize = null,
    borrowed_spill: bool = false,
    metadata: std.mem.Allocator = metadata_allocator,
    pub fn scopes(self: *ScopeSet) []Scope {
        return if (self.spill) |values| values[0..self.count] else self.inline_scopes[0..self.count];
    }
    pub fn add(self: *ScopeSet, context: *anyopaque, outputs: []const ops.CT, vtable: *const Scope.VTable) !void {
        if (self.started) return error.InvalidResourceRetirement;
        for (self.scopes()) |scope| if (scope.context == context) {
            if (scope.vtable != vtable or scope.outputs.len != outputs.len or (outputs.len != 0 and scope.outputs.ptr != outputs.ptr)) return error.ForeignResourceRetirementOrigin;
            return;
        };
        if (self.prepared_capacity) |capacity| if (self.count == capacity) return error.PreparedOriginCapacityExceeded;
        if (self.prepared_capacity != null and self.count == (if (self.spill) |storage| storage.len else self.inline_scopes.len)) return error.PreparedOriginCapacityExceeded;
        if (self.count == if (self.spill) |values| values.len else self.inline_scopes.len) {
            const old = self.scopes();
            const expanded = try self.metadata.alloc(Scope, try std.math.mul(usize, old.len, 2));
            @memcpy(expanded[0..old.len], old);
            if (self.spill) |values| self.metadata.free(values);
            self.spill = expanded;
        }
        const next = Scope{ .context = context, .outputs = outputs, .vtable = vtable };
        if (self.spill) |values| values[self.count] = next else self.inline_scopes[self.count] = next;
        self.count += 1;
    }
    pub fn beginAll(self: *ScopeSet) !void {
        if (self.started) return error.InvalidResourceRetirement;
        self.started = true;
        const order = struct {
            fn less(_: void, a: Scope, b: Scope) bool {
                return @intFromPtr(a.context) < @intFromPtr(b.context);
            }
        };
        std.mem.sort(Scope, self.scopes(), {}, order.less);
        errdefer self.end();
        for (self.scopes()) |*scope| {
            const value = try scope.vtable.begin(scope.context, scope.outputs);
            // Once begin returns it owns the origin's publication exclusion.
            scope.active = true;
            scope.origin = value.origin;
            scope.pending_batches = value.pending_batches;
            if (value.origin.identity == 0 or value.pending_batches > 2) return error.InvalidResourceRetirement;
        }
    }
    /// Reverse unlock BEFORE backend-owner cleanup; no queue polling or SDK
    /// destruction is performed while any publication exclusion remains held.
    pub fn end(self: *ScopeSet) void {
        const values = self.scopes();
        var index = values.len;
        while (index != 0) {
            index -= 1;
            values[index].end();
        }
        if (self.borrowed_spill) {
            @memset(values, undefined); // erase all borrowed runtime/CT pointers
        } else if (self.spill) |spill| self.metadata.free(spill);
        self.spill = null;
        self.count = 0;
        self.started = false;
    }
};
pub const Services = struct {
    accepts: *const fn (*const ops.ComputeBackend.VTable) bool,
    /// Gather all actual origins into a set and call beginAll. Every origin is
    /// sorted/deduplicated BEFORE locks; errors must end any local partial set.
    begin: *const fn (*anyopaque, []const ops.CT) anyerror!ScopeSet,
    begin_prepared: ?*const fn (*anyopaque, []const ops.CT, *BackendPreparation) anyerror!ScopeSet = null,
};
pub fn begin(backend: ops.ComputeBackend, outputs: []const ops.CT) !?ScopeSet {
    const services = backend.vtable.resource_retirement_services orelse return null;
    if (!services.accepts(backend.vtable)) return null;
    return try services.begin(backend.ptr, outputs);
}

/// Maximum actual SDK origins for the concrete backend representation. Metal
/// Buf owns one direct Ref and two nonrecursive LazyMultiply MetalTensor Refs.
/// One additional origin covers the destination's current command buffers.
pub fn outputOriginCapacity(kind: ?@import("../graph/backend_contracts.zig").BackendKind, outputs: usize) !usize {
    return std.math.add(usize, if (kind == .metal) try std.math.mul(usize, outputs, 3) else outputs, 1);
}

/// One Charge's inert output-known metadata. No model/runtime/IO/command
/// buffer is captured; published witnesses immediately leave the arena owner.
pub const BackendPreparation = struct {
    kind: ?@import("../graph/backend_contracts.zig").BackendKind,
    output_capacity: usize,
    origin_capacity: usize,
    backing_capacity: usize,
    scope_storage: ?[]Scope = null,
    metal_arena: ?*anyopaque = null,
    pub fn prepare(allocator: std.mem.Allocator, kind: ?@import("../graph/backend_contracts.zig").BackendKind, output_capacity: usize, origin_capacity: usize) !BackendPreparation {
        if (origin_capacity == 0) return error.PreparedOriginCapacityExceeded;
        const backing_capacity = if (kind == .metal) try std.math.mul(usize, output_capacity, 3) else output_capacity;
        _ = try std.math.add(usize, backing_capacity, try std.math.mul(usize, origin_capacity, 2));
        const scopes = if (origin_capacity > 4) try allocator.alloc(Scope, origin_capacity) else null;
        errdefer if (scopes) |values| allocator.free(values);
        var arena: ?*anyopaque = null;
        if (kind == .metal) {
            if (comptime @import("build_options").enable_metal) {
                const metal = @import("../backends/metal_runtime.zig");
                const initialized = metal.antfly_metal_set_retirement_poll(pollRetirementChargesCPU);
                if (initialized == -1) return error.OutOfMemory;
                if (initialized != 0) return error.InvalidResourceRetirement;
                arena = metal.antfly_metal_retirement_arena_prepare(backing_capacity, origin_capacity) orelse return error.OutOfMemory;
            } else return error.UnsupportedPreparedResourceRetirement;
        }
        return .{ .kind = kind, .output_capacity = output_capacity, .origin_capacity = origin_capacity, .backing_capacity = backing_capacity, .scope_storage = scopes, .metal_arena = arena };
    }
    pub fn scopeSet(self: *BackendPreparation, outputs: []const ops.CT) !ScopeSet {
        if (outputs.len > self.output_capacity) return error.PreparedOutputCapacityExceeded;
        return .{ .spill = self.scope_storage, .prepared_capacity = self.origin_capacity, .borrowed_spill = true };
    }
    pub fn handles(self: *BackendPreparation, count: usize) ![]*anyopaque {
        if (count > self.backing_capacity) return error.PreparedOutputCapacityExceeded;
        if (comptime @import("build_options").enable_metal) {
            const arena = self.metal_arena orelse return error.UnsupportedPreparedResourceRetirement;
            const result = @import("../backends/metal_runtime.zig").antfly_metal_retirement_arena_handles(arena, count) orelse return error.PreparedOutputCapacityExceeded;
            return result[0..count];
        } else return error.UnsupportedPreparedResourceRetirement;
    }
    pub fn armMetal(self: *BackendPreparation, runtime: ?*@import("../backends/metal_runtime.zig").RawMetalDecodeRuntime, buffers: []const *anyopaque, payload: Payload) !*anyopaque {
        if (comptime @import("build_options").enable_metal) {
            const arena = self.metal_arena orelse return error.UnsupportedPreparedResourceRetirement;
            return @import("../backends/metal_runtime.zig").antfly_metal_retirement_arena_arm(arena, runtime, buffers.ptr, buffers.len, payload) orelse error.PreparedWitnessCapacityExceeded;
        } else return error.UnsupportedPreparedResourceRetirement;
    }
    pub fn deinit(self: *BackendPreparation, allocator: std.mem.Allocator) void {
        if (self.scope_storage) |values| allocator.free(values);
        if (comptime @import("build_options").enable_metal) if (self.metal_arena) |arena| @import("../backends/metal_runtime.zig").antfly_metal_retirement_arena_destroy(arena);
        self.* = undefined;
    }
};

pub fn pollRetirementChargesCPU() callconv(.c) void {
    _ = poll();
}

/// Empty metadata shell prepared BEFORE any model/provider execution lane.
/// Copy only by explicit move (clear the old optional). It carries no quota,
/// provider/model borrow, operation token or creation epoch before arm().
pub const PreparedCharge = struct {
    owner: ?*Charge,
    pub fn prepare(origin_capacity: usize) !PreparedCharge {
        return prepareWithAllocator(metadata_allocator, origin_capacity);
    }
    pub fn prepareForBackend(kind: ?@import("../graph/backend_contracts.zig").BackendKind, output_capacity: usize, origin_capacity: usize) !PreparedCharge {
        var prepared = try prepare(origin_capacity);
        errdefer prepared.destroyUnused();
        if (kind == .metal) try prepared.owner.?.prepareBackendMetadata(kind, output_capacity, origin_capacity);
        return prepared;
    }
    pub fn begin(self: *PreparedCharge, backend: ops.ComputeBackend, outputs: []const ops.CT) !?ScopeSet {
        const owner = self.owner orelse return error.PreparedChargeAlreadyArmed;
        const prepared = if (owner.prepared_backend) |*value| value else {
            const legacy = backend.vtable.resource_retirement_services orelse return null;
            if (!legacy.accepts(backend.vtable)) return null;
            // Advertising the prepared ABI makes its cold metadata mandatory.
            // A legacy shell must not re-enable allocating retirement under B.
            if (legacy.begin_prepared != null) return error.UnpreparedOutputRetirement;
            return try legacy.begin(backend.ptr, outputs);
        };
        const services = backend.vtable.resource_retirement_services orelse {
            if (prepared.kind == .metal) return error.UnsupportedPreparedResourceRetirement;
            return null;
        };
        if (!services.accepts(backend.vtable)) return null; // copied/custom table keeps its original destructor policy
        const begin_prepared = services.begin_prepared orelse return error.UnsupportedPreparedResourceRetirement;
        return try begin_prepared(backend.ptr, outputs, prepared);
    }
    fn prepareWithAllocator(allocator: std.mem.Allocator, origin_capacity: usize) !PreparedCharge {
        const owner = try allocator.create(Charge);
        errdefer allocator.destroy(owner);
        const groups = try allocator.alloc(*Group, origin_capacity);
        errdefer allocator.free(groups);
        var created: usize = 0;
        errdefer for (groups[0..created]) |group| allocator.destroy(group);
        for (groups) |*group| {
            group.* = try allocator.create(Group);
            created += 1;
        }
        const boundary = try allocator.create(Charge.DeferredRoot);
        owner.* = .{ .quota = .unarmed, .domain = null, .metadata = allocator, .prepared_groups = groups, .prepared_boundary = boundary };
        return .{ .owner = owner };
    }
    /// No allocation or callback. On error both caller authorities and this
    /// empty shell remain unchanged. Success transfers its ONE root to caller.
    pub fn arm(self: *PreparedCharge, lease: *?memory.AdmissionLease, domain: *?admission.Pin) !*Charge {
        const owner = self.owner orelse return error.PreparedChargeAlreadyArmed;
        const retained = lease.* orelse return error.MissingResourceRetirementLease;
        if (domain.*) |pin| if (pin.controller() != retained.controller) return error.ForeignAdmissionDomain;
        std.debug.assert(owner.quota == .unarmed);
        owner.quota = .{ .run = retained };
        owner.domain = domain.*;
        lease.* = null;
        domain.* = null;
        self.owner = null;
        return owner;
    }
    /// Outside execution lanes. Safe after successful arm (then empty); the
    /// returned Charge owns all consumed/unused tickets until final retirement.
    pub fn destroyUnused(self: *PreparedCharge) void {
        const owner = self.owner orelse return;
        self.owner = null;
        owner.release();
    }
};

pub const Charge = struct {
    references: std.atomic.Value(usize) = .init(1),
    quota: union(enum) {
        unarmed,
        run: memory.AdmissionLease,
        storage: @import("storage_admission.zig").Reservation,
    },
    domain: ?admission.Pin,
    // Production records/groups always use independent process allocator. The private override
    // exists only for deterministic construction/OOM ownership tests.
    metadata: std.mem.Allocator = metadata_allocator,
    // Non-null marks a prepared-only path: exhaustion must fail closed, never
    // allocate late under a provider/model lane. Each used group is independent
    // because its witnesses may outlive all other origins and the caller root.
    prepared_groups: ?[]*Group = null,
    prepared_group_cursor: usize = 0,
    prepared_boundary: ?*DeferredRoot = null,
    prepared_backend: ?BackendPreparation = null,

    pub fn prepareBackendMetadata(self: *Charge, kind: ?@import("../graph/backend_contracts.zig").BackendKind, output_capacity: usize, origin_capacity: usize) !void {
        if (self.prepared_backend != null) return error.PreparedRetirementAlreadyInitialized;
        if (self.references.load(.acquire) != 1 or self.prepared_group_cursor != 0) return error.PreparedRetirementAlreadyPublished;
        if (self.prepared_groups) |groups| if (groups.len < origin_capacity) return error.PreparedOriginCapacityExceeded;
        if (self.prepared_groups == null) {
            const groups = try self.metadata.alloc(*Group, origin_capacity);
            var created: usize = 0;
            errdefer {
                for (groups[0..created]) |group| self.metadata.destroy(group);
                self.metadata.free(groups);
            }
            for (groups) |*group| {
                group.* = try self.metadata.create(Group);
                created += 1;
            }
            const boundary = try self.metadata.create(DeferredRoot);
            self.prepared_groups = groups;
            self.prepared_boundary = boundary;
        }
        self.prepared_backend = try BackendPreparation.prepare(self.metadata, kind, output_capacity, origin_capacity);
    }
    /// Moves lease and an OWNED pure controller-domain pin on success only.
    /// A missing pin is a raw-caller contract: the controller must outlive all
    /// groups, and only explicit pollRaw may retire those groups.
    pub fn create(lease: *?memory.AdmissionLease, domain: *?admission.Pin) !*Charge {
        return createWithAllocator(metadata_allocator, lease, domain);
    }
    fn createWithAllocator(allocator: std.mem.Allocator, lease: *?memory.AdmissionLease, domain: *?admission.Pin) !*Charge {
        const retained_lease = lease.* orelse return error.MissingResourceRetirementLease;
        if (domain.*) |pin| if (pin.controller() != retained_lease.controller) return error.ForeignAdmissionDomain;
        const owner = try allocator.create(Charge);
        owner.* = .{ .quota = .{ .run = retained_lease }, .domain = domain.*, .metadata = allocator };
        lease.* = null;
        domain.* = null;
        return owner;
    }
    /// Moves an independently owned physical-storage reservation only on
    /// success. Actual buffer witnesses keep both of its exact-once leases
    /// alive; GPU deallocation callbacks still perform only group atomics.
    pub fn createReservation(reservation: *?@import("storage_admission.zig").Reservation, domain: *?admission.Pin) !*Charge {
        return createReservationWithAllocator(metadata_allocator, reservation, domain);
    }
    fn createReservationWithAllocator(allocator: std.mem.Allocator, reservation: *?@import("storage_admission.zig").Reservation, domain: *?admission.Pin) !*Charge {
        const owned = reservation.* orelse return error.MissingResourceRetirementLease;
        if (domain.*) |pin| if (pin.controller() != owned.identity.controller) return error.ForeignAdmissionDomain;
        const owner = try allocator.create(Charge);
        errdefer allocator.destroy(owner);
        // The reservation may own a pure controller pin even when its caller
        // did not need to pass a second explicit publication-domain handle.
        const held_domain: ?admission.Pin = if (domain.*) |pin| pin else try owned.retainAdmissionDomain();
        owner.* = .{ .quota = .{ .storage = owned }, .domain = held_domain, .metadata = allocator };
        reservation.* = null;
        domain.* = null;
        return owner;
    }
    fn controller(self: *const Charge) ?*memory.AdmissionController {
        return switch (self.quota) {
            .unarmed => null,
            .run => |lease| lease.controller,
            .storage => |reservation| reservation.identity.controller,
        };
    }
    pub const DeferredBoundary = struct {
        context: *anyopaque,
        nonce: u64,
        /// Borrows context synchronously. Success takes the preallocated Node
        /// until AFTER the operation gate/mutex ends, including error cleanup.
        /// The operation record must validate its live nonce before accepting.
        register: *const fn (*anyopaque, u64, *Node) anyerror!void,
    };
    const DeferredRoot = struct {
        node: Node,
        charge: *Charge,
        allocator: std.mem.Allocator,
        fn release(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const charge = self.charge;
            const allocator = self.allocator;
            allocator.destroy(self);
            charge.release();
        }
    };
    /// Register before any SDK allocation/submission or physical retirement.
    /// The caller's existing root remains held on every registration error.
    /// This extra root prevents quota callbacks under the numerical gate even
    /// if the last actual backing dies, or a background group becomes ready.
    pub fn holdUntilBoundary(self: *Charge, boundary: DeferredBoundary) !void {
        if (self.quota == .unarmed) return error.UnarmedResourceRetirement;
        if (self.prepared_groups != null) {
            const holder = self.prepared_boundary orelse return error.PreparedBoundaryAlreadyUsed;
            holder.* = .{ .node = .{ .context = holder, .release_fn = DeferredRoot.release }, .charge = self.retainOwner(), .allocator = self.metadata };
            boundary.register(boundary.context, boundary.nonce, &holder.node) catch |err| {
                holder.charge.release(); // caller root remains authoritative
                return err;
            };
            self.prepared_boundary = null; // boundary owns exactly this ticket
            return;
        }
        return self.holdUntilBoundaryWithAllocator(metadata_allocator, boundary);
    }
    fn holdUntilBoundaryWithAllocator(self: *Charge, allocator: std.mem.Allocator, boundary: DeferredBoundary) !void {
        const holder = try allocator.create(DeferredRoot);
        errdefer allocator.destroy(holder);
        holder.* = .{ .node = .{ .context = holder, .release_fn = DeferredRoot.release }, .charge = self.retainOwner(), .allocator = allocator };
        errdefer holder.charge.release();
        try boundary.register(boundary.context, boundary.nonce, &holder.node);
    }
    /// CPU boundary only: actual SDK measurements shrink conservative padding
    /// after the relevant outer gate has ended. Failure retains the safe bound.
    pub fn shrinkStorageBytes(self: *Charge, bytes: usize) void {
        switch (self.quota) {
            .storage => |*reservation| reservation.retainBytes(bytes),
            .unarmed, .run => @panic("non-storage charge cannot impersonate a backing reservation"),
        }
    }
    /// Borrow identity/domain metadata while a physical owner pins this
    /// Charge. Amount readers use its atomic projection, never these leases.
    pub fn storageReservation(self: *Charge) ?*@import("storage_admission.zig").Reservation {
        return switch (self.quota) {
            .storage => |*value| value,
            .unarmed, .run => null,
        };
    }
    pub fn storageAmountsForBytes(self: *const Charge, bytes: usize) memory.AdmissionAmounts {
        return switch (self.quota) {
            .storage => |*value| value.amountsForBytes(bytes),
            .unarmed, .run => @panic("non-storage quota cannot impersonate physical backing"),
        };
    }
    pub fn storageBytes(self: *const Charge) ?usize {
        return switch (self.quota) {
            .storage => |reservation| reservation.reservedBytes(),
            .unarmed, .run => null,
        };
    }
    /// Explicit ownership for an actual backing owner or an outer operation
    /// record. The latter releases only after its provider gate has ended.
    pub fn retainOwner(self: *Charge) *Charge {
        self.retain();
        return self;
    }
    fn retain(self: *Charge) void {
        var previous = self.references.load(.monotonic);
        while (true) {
            if (previous == 0 or previous == std.math.maxInt(usize)) @panic("invalid resource retirement reference");
            if (self.references.cmpxchgWeak(previous, previous + 1, .monotonic, .monotonic)) |observed| previous = observed else return;
        }
    }
    /// One independently queued group per actual origin. The same aggregate
    /// may attach several origins; a Scope itself is one-shot on success/error.
    fn validateAttachment(scope: *const Scope) !void {
        if (!scope.active or scope.attached or scope.origin.identity == 0 or scope.pending_batches > 2) return error.InvalidResourceRetirement;
    }
    /// Cold metadata ticket. The owner pins Charge until this ticket is
    /// consumed/cancelled. No SDK origin or buffer address is guessed early.
    pub const PreparedAttachment = struct {
        charge: *Charge,
        allocator: std.mem.Allocator,
        group: ?*Group,
        pub fn deinit(self: *PreparedAttachment) void {
            if (self.group) |group| self.allocator.destroy(group);
            self.* = undefined;
        }
        pub fn attach(self: *PreparedAttachment, scope: *Scope) !void {
            try Charge.validateAttachment(scope);
            const group = self.group orelse return error.ResourceRetirementAttachmentConsumed;
            const charge = self.charge;
            self.group = null;
            charge.retain();
            group.* = .{ .charge = charge, .origin = scope.origin, .batch_references = .init(scope.pending_batches), .notify = if (charge.domain != null) scope.vtable.notify else null };
            for (&group.nodes) |*node| node.* = .{ .context = group, .release_fn = Group.releaseBatch };
            collector.append(group);
            (if (charge.prepared_backend) |*metadata| scope.attachPrepared(group.nodes[0..scope.pending_batches], .{ .context = group, .retain_fn = Group.retainPayload, .release_fn = Group.releasePayload, .notify_fn = group.notify }, metadata) else scope.attach(group.nodes[0..scope.pending_batches], .{ .context = group, .retain_fn = Group.retainPayload, .release_fn = Group.releasePayload, .notify_fn = group.notify })) catch |err| {
                // Backend contract: no batch node was published on failure. Payload
                // witnesses installed before a partial OOM can still outlive it.
                group.batch_references.store(0, .monotonic);
                const notify = group.notify;
                group.sealed.store(true, .release);
                if (notify) |schedule| schedule();
                // Do not dereference group after sealing: another CPU may collect.
                return err;
            };
            const notify = group.notify;
            group.sealed.store(true, .release);
            if (notify) |schedule| schedule();
        }
    };
    pub fn prepareAttachment(self: *Charge) !PreparedAttachment {
        if (self.quota == .unarmed) return error.UnarmedResourceRetirement;
        const group = if (self.prepared_groups) |groups| prepared: {
            if (self.prepared_group_cursor == groups.len) return error.PreparedOriginCapacityExceeded;
            const value = groups[self.prepared_group_cursor];
            self.prepared_group_cursor += 1;
            break :prepared value;
        } else try self.metadata.create(Group);
        return .{ .charge = self, .allocator = self.metadata, .group = group };
    }
    pub fn attach(self: *Charge, scope: *Scope) !void {
        try validateAttachment(scope);
        var prepared = try self.prepareAttachment();
        defer prepared.deinit();
        try prepared.attach(scope);
    }
    /// Raw explicit CPU drain. The caller owns a root ref and guarantees the
    /// unpinned controller is still alive. No other charge is collected here.
    pub fn pollRaw(self: *Charge) usize {
        std.debug.assert(self.domain == null);
        return collector.collect(self, null);
    }
    /// Caller root reference must span CT and backend-owner cleanup, AFTER all
    /// publication locks end. This and poll are CPU-only; neither runs on a GPU
    /// completion/deallocation handler or while holding provider/model locks.
    pub fn release(self: *Charge) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous > 0);
        if (previous != 1) return;
        switch (self.quota) {
            .unarmed => {},
            .run => |*lease| lease.release(),
            .storage => |*reservation| reservation.release(),
        }
        const domain = self.domain;
        const allocator = self.metadata;
        if (self.prepared_backend) |*prepared| prepared.deinit(allocator);
        if (self.prepared_boundary) |holder| allocator.destroy(holder);
        if (self.prepared_groups) |groups| {
            for (groups[self.prepared_group_cursor..]) |group| allocator.destroy(group);
            allocator.free(groups);
        }
        allocator.destroy(self);
        if (domain) |value| value.release();
    }
};

const Group = struct {
    next: ?*Group = null,
    charge: *Charge,
    origin: Queue,
    nodes: [2]Node = undefined,
    batch_references: std.atomic.Value(usize),
    payload_references: std.atomic.Value(usize) = .init(0),
    sealed: std.atomic.Value(bool) = .init(false),
    notify: ?*const fn () callconv(.c) void = null,
    fn releaseBatch(raw: *anyopaque) callconv(.c) void {
        const self: *Group = @ptrCast(@alignCast(raw));
        const notify = self.notify;
        const previous = self.batch_references.fetchSub(1, .release);
        std.debug.assert(previous > 0);
        if (previous == 1) if (notify) |schedule| schedule();
    }
    fn retainPayload(raw: *anyopaque) callconv(.c) void {
        const self: *Group = @ptrCast(@alignCast(raw));
        // No new witnesses may be installed after synchronous attach returns.
        std.debug.assert(!self.sealed.load(.acquire));
        const previous = self.payload_references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != std.math.maxInt(usize));
    }
    fn releasePayload(raw: *anyopaque) callconv(.c) void {
        const self: *Group = @ptrCast(@alignCast(raw));
        const previous = self.payload_references.fetchSub(1, .release);
        std.debug.assert(previous > 0);
        // No queue/provider/domain access and no self dereference after this.
    }
    fn ready(self: *const Group) bool {
        return self.sealed.load(.acquire) and self.batch_references.load(.acquire) == 0 and self.payload_references.load(.acquire) == 0;
    }
};
const Collector = struct {
    mutex: std.atomic.Mutex = .unlocked,
    first: ?*Group = null,
    pending: std.atomic.Value(bool) = .init(false),
    fn lock(self: *Collector) void {
        @import("antfly_platform").sync.lockYielding(&self.mutex);
    }
    fn append(self: *Collector, group: *Group) void {
        self.lock();
        defer self.mutex.unlock();
        group.next = self.first;
        self.first = group;
        self.pending.store(true, .release);
    }
    fn collect(self: *Collector, raw: ?*Charge, raw_controller: ?*memory.AdmissionController) usize {
        if (!self.pending.load(.acquire)) return 0;
        var ready: ?*Group = null;
        self.lock();
        var cursor = &self.first;
        while (cursor.*) |group| {
            const authorized = if (raw) |charge| group.charge == charge else if (raw_controller) |controller| group.charge.domain == null and group.charge.controller() == controller else group.charge.domain != null;
            if (authorized and group.ready()) {
                cursor.* = group.next;
                group.next = ready;
                ready = group;
            } else cursor = &group.next;
        }
        self.pending.store(self.first != null, .release);
        self.mutex.unlock();
        var count: usize = 0;
        while (ready) |group| {
            ready = group.next;
            const charge = group.charge;
            const allocator = charge.metadata;
            allocator.destroy(group);
            charge.release();
            count += 1;
        }
        return count;
    }
};
var collector: Collector = .{};
/// Checked CPU boundary only, outside model/provider/registry/metadata locks.
/// Never waits for GPU/foreign-origin work. Unready groups remain valid without
/// their producer provider; subsequent admission/cleanup polls can retire them.
/// The caller must drain its autorelease pool before polling for final backing.
pub fn poll() usize {
    return collector.collect(null, null);
}

/// Explicit raw-controller drain, never a background sweep. The caller must
/// preserve controller AND any caller-owned shared-token metadata allocator
/// until this returns after actual backend/payload cleanup. It performs no wait.
pub fn pollRawController(controller: *memory.AdmissionController) usize {
    return collector.collect(null, controller);
}

const TestOrigin = struct {
    identity: Queue = .{ .identity = 0 },
    witnesses: [8]Payload = undefined,
    witness_count: usize = 0,
    requested_witnesses: usize = 1,
    requested_batches: usize = 1,
    batches: [2]?*Node = @splat(null),
    partial_allocator: ?std.mem.Allocator = null,
    drop_during_attach: bool = false,
    ended: bool = false,
    fail_begin: bool = false,
    lock_count: ?*usize = null,
    fn scope(self: *TestOrigin, batches: usize) Scope {
        return .{ .context = self, .origin = self.identity, .pending_batches = batches, .outputs = &.{}, .active = true, .vtable = &table };
    }
    fn beginScope(raw: *anyopaque, _: []const ops.CT) !Scope.Begun {
        const self: *TestOrigin = @ptrCast(@alignCast(raw));
        if (self.fail_begin) return error.QueueFull;
        if (self.lock_count) |count| count.* += 1;
        return .{ .origin = self.identity, .pending_batches = self.requested_batches };
    }
    fn attach(raw: *anyopaque, _: []const ops.CT, nodes: []Node, payload: Payload) !void {
        const self: *TestOrigin = @ptrCast(@alignCast(raw));
        if (self.witness_count != 0) return error.OriginAlreadyAttached;
        for (0..self.requested_witnesses) |index| {
            payload.retain_fn(payload.context);
            self.witnesses[self.witness_count] = payload;
            self.witness_count += 1;
            if (self.drop_during_attach) {
                self.witness_count -= 1;
                payload.release_fn(payload.context);
                if (payload.notify_fn) |schedule| schedule();
            }
            if (index == 0) if (self.partial_allocator) |allocator| {
                const bytes = try allocator.alloc(u8, 1);
                allocator.free(bytes);
            };
        }
        for (nodes, 0..) |*node, index| self.batches[index] = node;
    }
    fn end(raw: *anyopaque) void {
        const self: *TestOrigin = @ptrCast(@alignCast(raw));
        self.ended = true;
        if (self.lock_count) |count| count.* -= 1;
    }
    fn retireBatches(self: *TestOrigin) void {
        for (&self.batches) |*entry| if (entry.*) |node| {
            entry.* = null;
            node.release_fn(node.context);
        };
    }
    fn retirePayloads(self: *TestOrigin) void {
        const count = self.witness_count;
        self.witness_count = 0;
        for (self.witnesses[0..count]) |payload| {
            payload.release_fn(payload.context);
            if (payload.notify_fn) |schedule| schedule();
        }
    }
    const table = Scope.VTable{ .begin = beginScope, .attach = attach, .end = end };
};

fn testCharge(domain: *admission.Domain, bytes: usize) !*Charge {
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = bytes }, false);
    errdefer if (lease) |*value| value.release();
    var pin: ?admission.Pin = domain.pin();
    errdefer if (pin) |value| value.release();
    return try Charge.create(&lease, &pin);
}

test "complete charge independent origins retain real quota after source domain owner release" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    const inspection = domain.pin();
    defer inspection.release();
    const charge = try testCharge(domain, 96);
    domain.release();
    var first = TestOrigin{ .identity = Queue.init() };
    var second = TestOrigin{ .identity = Queue.init() };
    var a = first.scope(1);
    var b = second.scope(2);
    try charge.attach(&a);
    try charge.attach(&b);
    try std.testing.expectError(error.InvalidResourceRetirement, charge.attach(&a));
    a.end();
    b.end();
    charge.release();
    first.retireBatches();
    first.retirePayloads();
    try std.testing.expectEqual(@as(usize, 1), poll());
    try std.testing.expectEqual(@as(usize, 96), inspection.controller().snapshot().backend_scratch_bytes);
    second.retirePayloads();
    try std.testing.expectEqual(@as(usize, 0), poll());
    second.retireBatches();
    try std.testing.expectEqual(@as(usize, 1), poll());
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, inspection.controller().snapshot());
}

test "complete charge partial witness OOM survives caller scope and allocator expiry" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    defer domain.release();
    const charge = try testCharge(domain, 64);
    var survivor: Payload = undefined;
    {
        var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
        var origin = TestOrigin{ .identity = Queue.init(), .partial_allocator = failure.allocator() };
        var scope = origin.scope(2);
        try std.testing.expectError(error.OutOfMemory, charge.attach(&scope));
        try std.testing.expectError(error.InvalidResourceRetirement, charge.attach(&scope));
        try std.testing.expect(origin.batches[0] == null and origin.batches[1] == null);
        survivor = origin.witnesses[0];
        origin.witness_count = 0;
        scope.end();
    }
    charge.release();
    try std.testing.expectEqual(@as(usize, 0), poll());
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_scratch_bytes);
    survivor.release_fn(survivor.context);
    try std.testing.expectEqual(@as(usize, 1), poll());
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "complete charge constructor group and foreign-domain failures preserve exact ownership" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    defer domain.release();
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false);
    defer if (lease) |*value| value.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |value| value.release();
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Charge.createWithAllocator(failure.allocator(), &lease, &pin));
    try std.testing.expect(lease != null and pin != null);
    const foreign = try admission.Domain.create();
    defer foreign.release();
    var foreign_pin: ?admission.Pin = foreign.pin();
    defer if (foreign_pin) |value| value.release();
    try std.testing.expectError(error.ForeignAdmissionDomain, Charge.create(&lease, &foreign_pin));
    try std.testing.expect(lease != null and foreign_pin != null);
    var group_failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    const charge = try Charge.createWithAllocator(group_failure.allocator(), &lease, &pin);
    try std.testing.expect(lease == null and pin == null);
    var origin = TestOrigin{ .identity = Queue.init() };
    var scope = origin.scope(1);
    try std.testing.expectError(error.OutOfMemory, charge.attach(&scope));
    try std.testing.expect(!scope.attached);
    scope.end();
    charge.release();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "complete charge concurrent payload destruction retires only through CPU polling" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    defer domain.release();
    const charge = try testCharge(domain, 128);
    const workers = 4;
    var origins: [workers]TestOrigin = @splat(.{});
    for (&origins) |*origin| {
        origin.identity = Queue.init();
        origin.requested_witnesses = 8;
        var scope = origin.scope(1);
        try charge.attach(&scope);
        scope.end();
        origin.retireBatches();
    }
    charge.release();
    const Worker = struct {
        origin: *TestOrigin,
        start: *std.atomic.Value(bool),
        done: *std.atomic.Value(usize),
        fn run(self: *@This()) void {
            while (!self.start.load(.acquire)) std.atomic.spinLoopHint();
            self.origin.retirePayloads();
            _ = self.done.fetchAdd(1, .release);
        }
    };
    var start: std.atomic.Value(bool) = .init(false);
    var done: std.atomic.Value(usize) = .init(0);
    var work: [workers]Worker = undefined;
    var threads: [workers]std.Thread = undefined;
    var spawned: usize = 0;
    defer {
        start.store(true, .release);
        for (threads[0..spawned]) |thread| thread.join();
        for (&origins) |*origin| origin.retirePayloads();
        _ = poll();
    }
    for (&origins, 0..) |*origin, index| {
        work[index] = .{ .origin = origin, .start = &start, .done = &done };
        threads[index] = try std.Thread.spawn(.{}, Worker.run, .{&work[index]});
        spawned += 1;
    }
    start.store(true, .release);
    while (done.load(.acquire) != workers) {
        _ = poll();
        std.atomic.spinLoopHint();
    }
    for (threads[0..spawned]) |thread| thread.join();
    spawned = 0;
    _ = poll();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "complete charge repeated origin generations retain full quota only until actual payload retirement" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    defer domain.release();
    const identity = Queue.init();
    for (0..64) |_| {
        const charge = try testCharge(domain, 64);
        var origin = TestOrigin{ .identity = identity };
        var scope = origin.scope(0);
        try charge.attach(&scope);
        scope.end();
        charge.release();
        try std.testing.expectEqual(@as(usize, 0), poll());
        try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_scratch_bytes);
        origin.retirePayloads();
        try std.testing.expectEqual(@as(usize, 1), poll());
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    }
}

test "raw complete charges require explicit caller-lifetime drain and never background poll" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false);
    var no_pin: ?admission.Pin = null;
    const charge = try Charge.create(&lease, &no_pin);
    var origin = TestOrigin{ .identity = Queue.init() };
    var scope = origin.scope(1);
    try charge.attach(&scope);
    scope.end();
    try std.testing.expectEqual(@as(usize, 0), charge.pollRaw());
    charge.release();
    origin.retirePayloads();
    origin.retireBatches();
    try std.testing.expectEqual(@as(usize, 0), poll());
    try std.testing.expectEqual(@as(usize, 32), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1), pollRawController(&controller));
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "retirement ScopeSet deduplicates sorts spills and rolls back begun origins before freeing metadata" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    var origins: [6]TestOrigin = @splat(.{});
    var locks: usize = 0;
    for (&origins) |*origin| {
        origin.identity = Queue.init();
        origin.lock_count = &locks;
    }
    origins[4].fail_begin = true;
    var set = ScopeSet{ .metadata = std.testing.allocator };
    defer set.end();
    var index: usize = origins.len;
    while (index != 0) {
        index -= 1;
        try set.add(&origins[index], &.{}, &TestOrigin.table);
    }
    try set.add(&origins[0], &.{}, &TestOrigin.table);
    try std.testing.expectEqual(@as(usize, 6), set.scopes().len);
    try std.testing.expect(set.spill != null);
    try std.testing.expectError(error.QueueFull, set.beginAll());
    try std.testing.expectEqual(@as(usize, 0), locks);
    try std.testing.expect(set.spill == null and set.count == 0);
    for (origins[0..4]) |origin| try std.testing.expect(origin.ended);
    try std.testing.expect(!origins[5].ended);
}

test "complete charge external releases are CPU-only outside collector mutex and reentrant" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const Budget = struct {
        cpu: std.Thread.Id,
        contexts: usize = 1,
        releases: usize = 0,
        fn retainContext(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.contexts += 1;
            return true;
        }
        fn releaseContext(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(std.Thread.getCurrentId() == self.cpu);
            std.debug.assert(collector.mutex.tryLock());
            collector.mutex.unlock();
            self.contexts -= 1;
        }
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 71;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, token: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(token == 71 and std.Thread.getCurrentId() == self.cpu);
            std.debug.assert(collector.mutex.tryLock());
            collector.mutex.unlock();
            _ = poll(); // Resource callbacks may re-enter a checked CPU drain.
            self.releases += 1;
        }
    };
    var budget = Budget{ .cpu = std.Thread.getCurrentId() };
    const domain = try admission.Domain.create();
    var domain_live = true;
    defer if (domain_live) domain.release();
    try domain.controller.configureResourceBudget(.{ .context = &budget, .retain_context = Budget.retainContext, .release_context = Budget.releaseContext, .try_reserve = Budget.reserve, .retain = Budget.retain, .release = Budget.release });
    const charge = try testCharge(domain, 48);
    var origin = TestOrigin{ .identity = Queue.init() };
    var scope = origin.scope(1);
    try charge.attach(&scope);
    scope.end();
    charge.release();
    domain.release();
    domain_live = false;
    origin.retireBatches();
    origin.retirePayloads();
    try std.testing.expectEqual(@as(usize, 1), poll());
    try std.testing.expectEqual(@as(usize, 1), budget.releases);
    try std.testing.expectEqual(@as(usize, 1), budget.contexts);
}

test "retirement ScopeSet metadata allocation failure acquires no provider exclusion" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var set = ScopeSet{ .metadata = failure.allocator() };
    defer set.end();
    var origins: [5]TestOrigin = @splat(.{});
    var locks: usize = 0;
    for (&origins) |*origin| {
        origin.identity = Queue.init();
        origin.lock_count = &locks;
    }
    for (origins[0..4]) |*origin| try set.add(origin, &.{}, &TestOrigin.table);
    try std.testing.expectError(error.OutOfMemory, set.add(&origins[4], &.{}, &TestOrigin.table));
    try std.testing.expectEqual(@as(usize, 0), locks);
    try std.testing.expectEqual(@as(usize, 4), set.scopes().len);
    try std.testing.expect(set.spill == null);
}

test "recycled shared backing retains every complete charge until final payload destruction" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    defer domain.release();
    const identity = Queue.init();
    var origins: [8]TestOrigin = @splat(.{});
    for (&origins, 0..) |*origin, index| {
        origin.identity = identity;
        const charge = try testCharge(domain, 16);
        var scope = origin.scope(1);
        try charge.attach(&scope);
        scope.end();
        charge.release();
        origin.retireBatches();
        try std.testing.expectEqual(@as(usize, 0), poll());
        try std.testing.expectEqual((index + 1) * 16, domain.controller.snapshot().backend_scratch_bytes);
    }
    // Reuse/another logical owner is not evidence of backing destruction. A
    // checked backend purge/final object destruction releases the witnesses.
    for (&origins) |*origin| origin.retirePayloads();
    try std.testing.expectEqual(origins.len, poll());
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "empty retirement polling never takes the process queue lock" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    try std.testing.expect(!collector.pending.load(.acquire));
    try std.testing.expect(collector.mutex.tryLock());
    defer collector.mutex.unlock();
    try std.testing.expectEqual(@as(usize, 0), poll());
}

var test_wakes: std.atomic.Value(usize) = .init(0);
fn testNotify() callconv(.c) void {
    _ = test_wakes.fetchAdd(1, .monotonic);
    // Deterministically emulate a CPU worker arriving immediately; production
    // notification only schedules that worker and never polls on a GPU handler.
    _ = poll();
}
const wake_table = Scope.VTable{ .begin = TestOrigin.beginScope, .attach = TestOrigin.attach, .end = TestOrigin.end, .notify = testNotify };

test "managed retirement wakes after seal even when final payload callback preceded sealing" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    defer domain.release();
    test_wakes.store(0, .monotonic);
    const charge = try testCharge(domain, 64);
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var origin = TestOrigin{ .identity = Queue.init(), .drop_during_attach = true, .partial_allocator = failure.allocator() };
    var scope = origin.scope(2);
    scope.vtable = &wake_table;
    try std.testing.expectError(error.OutOfMemory, charge.attach(&scope));
    scope.end();
    try std.testing.expectEqual(@as(usize, 2), test_wakes.load(.acquire));
    // The second wake may have already destroyed Group metadata. Root still
    // owns the complete lease until all synchronous backend cleanup finishes.
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_scratch_bytes);
    charge.release();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "managed retirement wakes success seal and final batch after earlier payload destruction" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    defer domain.release();
    test_wakes.store(0, .monotonic);
    const empty = try testCharge(domain, 16);
    var no_payload = TestOrigin{ .identity = Queue.init(), .requested_witnesses = 0 };
    var empty_scope = no_payload.scope(0);
    empty_scope.vtable = &wake_table;
    try empty.attach(&empty_scope);
    empty_scope.end();
    try std.testing.expectEqual(@as(usize, 1), test_wakes.load(.acquire));
    empty.release();
    const charge = try testCharge(domain, 48);
    var origin = TestOrigin{ .identity = Queue.init() };
    var scope = origin.scope(2);
    scope.vtable = &wake_table;
    try charge.attach(&scope);
    scope.end();
    charge.release();
    origin.retirePayloads();
    try std.testing.expectEqual(@as(usize, 48), domain.controller.snapshot().backend_scratch_bytes);
    origin.retireBatches();
    try std.testing.expectEqual(@as(usize, 4), test_wakes.load(.acquire));
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "raw retirement never advertises asynchronous notifications" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 16 }, false);
    var no_pin: ?admission.Pin = null;
    const charge = try Charge.create(&lease, &no_pin);
    test_wakes.store(0, .monotonic);
    var origin = TestOrigin{ .identity = Queue.init() };
    var scope = origin.scope(1);
    scope.vtable = &wake_table;
    try charge.attach(&scope);
    scope.end();
    try std.testing.expect(origin.witnesses[0].notify_fn == null);
    charge.release();
    origin.retirePayloads();
    origin.retireBatches();
    try std.testing.expectEqual(@as(usize, 0), test_wakes.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), pollRawController(&controller));
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "physical KV charge follows actual payload witnesses beyond request and domain root retirement" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const domain = try admission.Domain.create();
    var root_live = true;
    defer if (root_live) domain.release();
    const inspection = domain.pin();
    defer inspection.release();
    var request = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_kv_bytes = 64 }, false);
    defer request.release();
    var reservation: ?@import("storage_admission.zig").Reservation = try @import("storage_admission.zig").fromRunKvLease(std.testing.allocator, &request, .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 192 }, .check_live_memory = false }, 128);
    defer if (reservation) |*value| value.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |value| value.release();
    const charge = try Charge.createReservation(&reservation, &pin);
    var charge_live = true;
    defer if (charge_live) charge.release();
    try std.testing.expect(reservation == null and pin == null);
    domain.release();
    root_live = false;
    var origin = TestOrigin{ .identity = Queue.init(), .requested_batches = 0 };
    defer {
        origin.retireBatches();
        origin.retirePayloads();
        _ = poll();
    }
    var scope = origin.scope(0);
    defer scope.end();
    try charge.attach(&scope);
    scope.end();
    request.release();
    charge.release();
    charge_live = false;
    try std.testing.expectEqual(@as(usize, 128), inspection.controller().snapshot().backend_kv_bytes);
    try std.testing.expectEqual(@as(usize, 0), poll());
    origin.retirePayloads();
    try std.testing.expectEqual(@as(usize, 1), poll());
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, inspection.controller().snapshot());
}

test "Metal physical KV reservation follows real buffer aliases until SDK backing retirement" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    if (!@import("../backends/metal_runtime.zig").metalDeviceAvailable()) return error.SkipZigTest;
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .metal, std.testing.io);
    defer context.deinit(a);
    const cb = context.computeBackend();
    const domain = try admission.Domain.create();
    var root_live = true;
    defer if (root_live) domain.release();
    const inspect = domain.pin();
    defer inspect.release();
    var request = try domain.controller.tryAcquire(.gpu, .{}, .{}, false);
    defer request.release();
    const bytes: usize = 24576;
    const upper = try cb.vtable.kernels.?.physicalStorageReservationBytes.?(cb.ptr, bytes);
    var reservation: ?@import("storage_admission.zig").Reservation = try @import("storage_admission.zig").fromRunKvLease(a, &request, .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = upper }, .check_live_memory = false }, upper);
    defer if (reservation) |*value| value.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |value| value.release();
    const charge = try Charge.createReservation(&reservation, &pin);
    var charge_live = true;
    defer if (charge_live) charge.release();
    defer _ = poll();
    const tensor = (try cb.vtable.kernels.?.allocatePhysicalStorage.?(cb.ptr, bytes)).?;
    var tensor_live = true;
    defer if (tensor_live) cb.free(tensor);
    const alias = (try cb.cloneTensorShape(tensor, &.{@intCast(bytes / 4)})).?;
    var alias_live = true;
    defer if (alias_live) cb.free(alias);
    var scopes = (try begin(cb, &.{tensor})) orelse return error.MissingPhysicalRetirementScope;
    defer scopes.end();
    for (scopes.scopes()) |*scope| try charge.attach(scope);
    scopes.end();
    request.release();
    domain.release();
    root_live = false;
    cb.free(tensor);
    tensor_live = false;
    charge.release();
    charge_live = false;
    _ = poll();
    try std.testing.expectEqual(upper, inspect.controller().snapshot().backend_kv_bytes);
    cb.free(alias);
    alias_live = false;
    // Default typed-domain notification can race this explicit CPU poll.
    for (0..1_000_000) |_| {
        _ = poll();
        if (inspect.controller().snapshot().backend_kv_bytes == 0) break;
        std.atomic.spinLoopHint();
    }
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, inspect.controller().snapshot());
}

test "physical reservation retirement construction failure moves neither quota nor domain pin" {
    const a = std.testing.allocator;
    const domain = try admission.Domain.create();
    defer domain.release();
    var request = try domain.controller.tryAcquire(.gpu, .{}, .{}, false);
    defer request.release();
    var reservation: ?@import("storage_admission.zig").Reservation = try @import("storage_admission.zig").fromRunKvLease(a, &request, .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 64 }, .check_live_memory = false }, 64);
    defer if (reservation) |*owned| owned.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |owned| owned.release();
    var failure = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Charge.createReservationWithAllocator(failure.allocator(), &reservation, &pin));
    try std.testing.expect(reservation != null and pin != null);
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_kv_bytes);
    const other = try admission.Domain.create();
    defer other.release();
    var wrong: ?admission.Pin = other.pin();
    defer if (wrong) |owned| owned.release();
    try std.testing.expectError(error.ForeignAdmissionDomain, Charge.createReservation(&reservation, &wrong));
    try std.testing.expect(reservation != null and wrong != null and pin != null);
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_kv_bytes);
}

test "physical charge root cannot retire quota until explicit operation boundary ends" {
    const domain = try admission.Domain.create();
    defer domain.release();
    var request = try domain.controller.tryAcquire(.gpu, .{}, .{}, false);
    defer request.release();
    var reservation: ?@import("storage_admission.zig").Reservation = try @import("storage_admission.zig").fromRunKvLease(std.testing.allocator, &request, .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 64 }, .check_live_memory = false }, 64);
    defer if (reservation) |*owned| owned.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |owned| owned.release();
    const charge = try Charge.createReservation(&reservation, &pin);
    var charge_live = true;
    defer if (charge_live) charge.release();
    const Gate = struct {
        held: bool = true,
        node: ?*Node = null,
        fn register(raw: *anyopaque, nonce: u64, node: *Node) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.held or nonce != 1) return error.ExpiredOperationScope;
            std.debug.assert(self.node == null);
            self.node = node;
        }
        fn finish(self: *@This()) void {
            self.held = false; // Mirrors ticket.end + mutex unlock.
            if (self.node) |node| {
                self.node = null;
                node.release_fn(node.context);
            }
        }
    };
    var gate = Gate{};
    defer gate.finish();
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, charge.holdUntilBoundaryWithAllocator(failure.allocator(), .{ .context = &gate, .nonce = 1, .register = Gate.register }));
    try std.testing.expect(gate.node == null);
    gate.held = false;
    try std.testing.expectError(error.ExpiredOperationScope, charge.holdUntilBoundaryWithAllocator(std.testing.allocator, .{ .context = &gate, .nonce = 1, .register = Gate.register }));
    gate.held = true;
    try std.testing.expectError(error.ExpiredOperationScope, charge.holdUntilBoundaryWithAllocator(std.testing.allocator, .{ .context = &gate, .nonce = 2, .register = Gate.register }));
    try charge.holdUntilBoundary(.{ .context = &gate, .nonce = 1, .register = Gate.register });
    charge.release();
    charge_live = false;
    _ = poll();
    try std.testing.expectEqual(@as(usize, 64), domain.controller.snapshot().backend_kv_bytes);
    gate.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "prepared backing witness installs the preallocated group without numerical metadata allocation" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false);
    defer if (lease) |*held| held.release();
    var no_pin: ?admission.Pin = null;
    // Charge and Group are the only two metadata allocations. Every later
    // allocator request must fail, while genuine free operations still work.
    var allocations = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 2 });
    const charge = try Charge.createWithAllocator(allocations.allocator(), &lease, &no_pin);
    var origin = TestOrigin{ .identity = Queue.init() };
    defer {
        origin.retirePayloads();
        origin.retireBatches();
        _ = charge.pollRaw();
        charge.release();
    }
    var ticket = try charge.prepareAttachment();
    defer ticket.deinit();
    var scope = origin.scope(1);
    defer scope.end();
    try ticket.attach(&scope);
    try std.testing.expect(scope.attached);
    try std.testing.expect(!allocations.has_induced_failure);
    try std.testing.expectEqual(@as(usize, 32), controller.snapshot().backend_scratch_bytes);
    var second_origin = TestOrigin{ .identity = Queue.init() };
    var second_scope = second_origin.scope(0);
    defer second_scope.end();
    try std.testing.expectError(error.ResourceRetirementAttachmentConsumed, ticket.attach(&second_scope));
    try std.testing.expect(!second_scope.attached);
    origin.retirePayloads();
    origin.retireBatches();
    try std.testing.expectEqual(@as(usize, 1), charge.pollRaw());
    // The independent physical root remains until its true final release.
    try std.testing.expectEqual(@as(usize, 32), controller.snapshot().backend_scratch_bytes);
}

const PreparedTestGate = struct {
    held: bool = true,
    node: ?*Node = null,
    fn register(raw: *anyopaque, nonce: u64, node: *Node) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (!self.held or nonce != 7) return error.ExpiredOperationScope;
        if (self.node != null) return error.AlreadyRegistered;
        self.node = node;
    }
    fn boundary(self: *@This()) Charge.DeferredBoundary {
        return .{ .context = self, .nonce = 7, .register = register };
    }
    fn finish(self: *@This()) void {
        self.held = false; // actual integration must unlock real B first
        if (self.node) |node| {
            self.node = null;
            node.release_fn(node.context);
        }
    }
};

test "prepared retirement charge OOM before lanes and unused shell have no quota authority" {
    for (0..5) |index| {
        var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = index });
        try std.testing.expectError(error.OutOfMemory, PreparedCharge.prepareWithAllocator(failure.allocator(), 2));
    }
    var empty = try PreparedCharge.prepareWithAllocator(std.testing.allocator, 0);
    empty.destroyUnused();
    empty.destroyUnused();
}

test "prepared retirement charge arm boundary and exact two-node origin allocate nothing under B" {
    const domain = try admission.Domain.create();
    defer domain.release();
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 72 }, false);
    defer if (lease) |*owned| owned.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |owned| owned.release();
    var allocator = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var prepared = try PreparedCharge.prepareWithAllocator(allocator.allocator(), 1);
    defer prepared.destroyUnused();
    const allocations = allocator.alloc_index;
    allocator.fail_index = allocations; // any late metadata allocation fails
    var gate = PreparedTestGate{};
    defer gate.finish();
    const charge = try prepared.arm(&lease, &pin);
    try std.testing.expect(lease == null and pin == null and prepared.owner == null);
    var alive = true;
    defer if (alive) charge.release();
    try std.testing.expectError(error.PreparedChargeAlreadyArmed, prepared.arm(&lease, &pin));
    gate.held = false;
    try std.testing.expectError(error.ExpiredOperationScope, charge.holdUntilBoundary(gate.boundary()));
    gate.held = true;
    try charge.holdUntilBoundary(gate.boundary());
    try std.testing.expectError(error.PreparedBoundaryAlreadyUsed, charge.holdUntilBoundary(gate.boundary()));
    var origin = TestOrigin{ .identity = Queue.init() };
    var scope = origin.scope(2);
    try charge.attach(&scope);
    var overflow = TestOrigin{ .identity = Queue.init() };
    var overflow_scope = overflow.scope(1);
    try std.testing.expectError(error.PreparedOriginCapacityExceeded, charge.attach(&overflow_scope));
    try std.testing.expect(!overflow_scope.attached);
    overflow_scope.end();
    scope.end();
    origin.retireBatches();
    origin.retirePayloads();
    try std.testing.expectEqual(@as(usize, 1), poll());
    charge.release();
    alive = false;
    try std.testing.expectEqual(allocations, allocator.alloc_index);
    try std.testing.expectEqual(@as(usize, 72), domain.controller.snapshot().backend_scratch_bytes);
    gate.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "prepared retirement charge foreign-domain failure keeps shell lease and pin until corrected" {
    const domain = try admission.Domain.create();
    defer domain.release();
    const foreign = try admission.Domain.create();
    defer foreign.release();
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false);
    defer if (lease) |*owned| owned.release();
    var wrong: ?admission.Pin = foreign.pin();
    defer if (wrong) |owned| owned.release();
    var correct: ?admission.Pin = domain.pin();
    defer if (correct) |owned| owned.release();
    var shell = try PreparedCharge.prepareWithAllocator(std.testing.allocator, 0);
    defer shell.destroyUnused();
    try std.testing.expectError(error.ForeignAdmissionDomain, shell.arm(&lease, &wrong));
    try std.testing.expect(shell.owner != null and lease != null and wrong != null);
    const owner = try shell.arm(&lease, &correct);
    owner.release();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

test "prepared retirement charge partial witness failure retains physical origin without late allocation" {
    const domain = try admission.Domain.create();
    defer domain.release();
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 48 }, false);
    defer if (lease) |*owned| owned.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |owned| owned.release();
    var shell = try PreparedCharge.prepareWithAllocator(std.testing.allocator, 2);
    defer shell.destroyUnused();
    const charge = try shell.arm(&lease, &pin);
    var gate = PreparedTestGate{};
    defer gate.finish();
    try charge.holdUntilBoundary(gate.boundary());
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var failed = TestOrigin{ .identity = Queue.init(), .partial_allocator = failure.allocator() };
    var scope = failed.scope(2);
    try std.testing.expectError(error.OutOfMemory, charge.attach(&scope));
    try std.testing.expect(scope.attached and failed.witness_count == 1);
    try std.testing.expect(failed.batches[0] == null and failed.batches[1] == null);
    scope.end();
    charge.release();
    gate.finish();
    try std.testing.expectEqual(@as(usize, 0), poll());
    try std.testing.expectEqual(@as(usize, 48), domain.controller.snapshot().backend_scratch_bytes);
    failed.retirePayloads();
    try std.testing.expectEqual(@as(usize, 1), poll());
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

fn preparedBackendMetadataOomCase(allocator: std.mem.Allocator) !void {
    var shell = try PreparedCharge.prepareWithAllocator(allocator, 7);
    defer shell.destroyUnused();
    try shell.owner.?.prepareBackendMetadata(null, 2, 7);
    var set = try shell.owner.?.prepared_backend.?.scopeSet(&.{});
    defer set.end();
    try std.testing.expectEqual(@as(usize, 7), set.spill.?.len);
}

test "prepared backend origin storage exhausts before publication without late allocator fallback" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, preparedBackendMetadataOomCase, .{});
    try std.testing.expectEqual(@as(usize, 7), try outputOriginCapacity(.metal, 2));
    try std.testing.expectEqual(@as(usize, 3), try outputOriginCapacity(null, 2));
    try std.testing.expectError(error.Overflow, outputOriginCapacity(.metal, std.math.maxInt(usize)));
    var failure = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    var shell = try PreparedCharge.prepareWithAllocator(failure.allocator(), 7);
    defer shell.destroyUnused();
    try shell.owner.?.prepareBackendMetadata(null, 2, 7);
    const cold_allocations = failure.alloc_index;
    failure.fail_index = cold_allocations;
    var set = try shell.owner.?.prepared_backend.?.scopeSet(&.{});
    defer set.end();
    var origins: [8]TestOrigin = undefined;
    for (&origins) |*origin| origin.* = .{ .identity = Queue.init() };
    for (origins[0..7]) |*origin| try set.add(origin, &.{}, &TestOrigin.table);
    try std.testing.expectError(error.PreparedOriginCapacityExceeded, set.add(&origins[7], &.{}, &TestOrigin.table));
    try std.testing.expectEqual(@as(usize, 7), set.count);
    try std.testing.expectEqual(cold_allocations, failure.alloc_index);
    try std.testing.expect(!failure.has_induced_failure);
    try set.beginAll();
    set.end();
    try std.testing.expect(!set.started and set.count == 0 and set.spill == null);
}

test "Metal prepared witness arena unwinds inert header failures before publication" {
    if (comptime !@import("build_options").enable_metal) return error.SkipZigTest;
    const metal = @import("../backends/metal_runtime.zig");
    try std.testing.expectEqual(@as(c_int, 0), metal.antfly_metal_retirement_arena_test_partial_failure());
}

test "prepared retirement veto preserves copied destructor and complete opaque lease through B" {
    const session = @import("../backends/session.zig");
    const sink = @import("retirement_sink.zig");
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const original = context.computeBackend();
    const value = try original.fromFloat32(&.{ 1, 2, 3, 4 });
    var value_open = true;
    defer if (value_open) original.free(value);
    const Probe = struct {
        original: ops.ComputeBackend,
        frees: usize = 0,
        fn accepts(_: *const ops.ComputeBackend.VTable) bool {
            return false;
        }
        fn forbidden(_: *anyopaque, _: []const ops.CT) !ScopeSet {
            return error.UnexpectedCustomRetirementCast;
        }
        fn preparedForbidden(_: *anyopaque, _: []const ops.CT, _: *BackendPreparation) !ScopeSet {
            return error.UnexpectedCustomPreparedRetirementCast;
        }
        fn free(raw: *anyopaque, tensor: ops.CT) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.frees += 1;
            self.original.free(tensor);
        }
        const services = Services{ .accepts = accepts, .begin = forbidden, .begin_prepared = preparedForbidden };
    };
    var probe = Probe{ .original = original };
    var table = original.vtable.*;
    table.freeTensor = Probe.free;
    table.cold_cleanup_services = null;
    table.resource_retirement_services = &Probe.services;
    var custom = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false);
    defer if (lease) |*held| held.release();
    var shell = try PreparedCharge.prepareWithAllocator(a, 1);
    defer shell.destroyUnused();
    try shell.owner.?.prepareBackendMetadata(null, 1, 1);
    const values = try a.alloc(ops.CT, 1);
    values[0] = value;
    var outputs = session.ResidentOutputs{ .outputs = values, .backend = &custom, .allocator = a, .resource_lease = lease, .retirement_shell = shell };
    shell.owner = null;
    lease = null;
    value_open = false;
    var outputs_open = true;
    defer if (outputs_open) outputs.deinit();
    var owner = try sink.Ref.create();
    defer owner.release();
    var guard = try owner.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    outputs.deinitWithBoundary(&epoch);
    outputs_open = false;
    try std.testing.expectEqual(@as(usize, 1), probe.frees);
    try std.testing.expectEqual(@as(usize, 16), controller.snapshot().host_scratch_bytes);
    guard.finish();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}

test "accepted prepared retirement rejects legacy shell before backend callback" {
    const a = std.testing.allocator;
    var context = try @import("../backends/compute_context.zig").BackendContext.init(a, .native, null);
    defer context.deinit(a);
    const original = context.computeBackend();
    const Probe = struct {
        legacy_calls: usize = 0,
        prepared_calls: usize = 0,
        fn accepts(_: *const ops.ComputeBackend.VTable) bool {
            return true;
        }
        fn legacy(raw: *anyopaque, _: []const ops.CT) !ScopeSet {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.legacy_calls += 1;
            return .{};
        }
        fn prepared(raw: *anyopaque, _: []const ops.CT, _: *BackendPreparation) !ScopeSet {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.prepared_calls += 1;
            return .{};
        }
        const prepared_services = Services{ .accepts = accepts, .begin = legacy, .begin_prepared = prepared };
        const legacy_services = Services{ .accepts = accepts, .begin = legacy };
    };
    var probe = Probe{};
    var table = original.vtable.*;
    table.resource_retirement_services = &Probe.prepared_services;
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    var shell = try PreparedCharge.prepareWithAllocator(a, 1);
    defer shell.destroyUnused();
    try std.testing.expectError(error.UnpreparedOutputRetirement, shell.begin(backend, &.{}));
    try std.testing.expectEqual(@as(usize, 0), probe.legacy_calls);
    try std.testing.expectEqual(@as(usize, 0), probe.prepared_calls);
    // A genuine legacy provider has made no prepared-allocation guarantee.
    table.resource_retirement_services = &Probe.legacy_services;
    var legacy = try shell.begin(backend, &.{});
    legacy.?.end();
    try std.testing.expectEqual(@as(usize, 1), probe.legacy_calls);
    // The same opted-in provider is legal only after actual cold preparation.
    table.resource_retirement_services = &Probe.prepared_services;
    try shell.owner.?.prepareBackendMetadata(null, 0, 1);
    var prepared = try shell.begin(backend, &.{});
    prepared.?.end();
    try std.testing.expectEqual(@as(usize, 1), probe.prepared_calls);
    try std.testing.expectEqual(@as(usize, 1), probe.legacy_calls);
}
