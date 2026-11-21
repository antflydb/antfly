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

//! Stable, admitted preparation metadata. Numerical banks have their own
//! physical declarations; metadata never borrows those bytes or an estimate.
const std = @import("std");
const sessions = @import("../backends/session.zig");
const AdmittedAllocator = @import("../backends/admitted_allocator.zig").AdmittedAllocator;
const memory = @import("../runtime/tier/memory.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
const BackendType = @import("../backends/backends.zig").BackendType;

pub const Owner = struct {
    const max_held = 4;
    const Phase = enum { preparing, sealed };

    backing: std.mem.Allocator,
    admitted: AdmittedAllocator,
    session: sessions.Session,
    model: ?*sessions.Session.ModelLifetime,
    wrapper_lease: ?memory.AdmissionLease,
    held: [max_held]?*const sessions.RunPermit = @splat(null),
    held_count: usize = 0,
    held_floor: ?memory.AdmissionAmounts = null,
    shared_metadata_floor: ?memory.AdmissionAmounts = null,
    shared_preparing: ?*Shared = null,
    shared_contributions: u8 = 0,
    shared_identities: [2]usize = @splat(0),
    pending_additional: ?memory.AdmissionAmounts = null,
    phase: Phase = .preparing,

    /// The supplied permits stay at their final addresses and retain their
    /// exact leases until sealBeforeExecution. The owner itself never moves.
    /// Backing frees must reclaim storage without quota/provider callbacks;
    /// the preparing runtime proves that allocator contract before opting in.
    pub fn create(backing: std.mem.Allocator, session: sessions.Session, permits: []const *const sessions.RunPermit) !*Owner {
        const floor = try sumPermits(session, permits);
        const model = if (session.model_lifetime) |value| value.retain() else null;
        errdefer if (model) |value| value.release();
        const wrapper = memory.AdmissionAmounts{ .host_scratch_bytes = @sizeOf(Owner) };
        var lease: ?memory.AdmissionLease = null;
        if (session.run_admission) |policy| {
            try policy.validateOwnedRunAmounts(try floor.merge(wrapper));
            lease = try acquireHost(session, @sizeOf(Owner));
        }
        errdefer if (lease) |*value| value.release();
        const self = try backing.create(Owner);
        self.* = .{
            .backing = backing,
            .admitted = .{ .backing = backing, .session = if (session.run_admission == null) session else null },
            .session = session,
            .model = model,
            .wrapper_lease = lease,
        };
        for (permits, 0..) |permit, index| self.held[index] = permit;
        self.held_count = permits.len;
        if (session.run_admission != null) {
            self.admitted.admission_context = self;
            self.admitted.admit = acquire;
        }
        return self;
    }

    fn sumPermits(session: sessions.Session, permits: []const *const sessions.RunPermit) !memory.AdmissionAmounts {
        if (permits.len > max_held) return error.TooManyMetadataAdmissionOwners;
        var total = memory.AdmissionAmounts{};
        for (permits, 0..) |permit, index| {
            if (permit.session.ptr != session.ptr or permit.session.vtable != session.vtable) return error.ForeignMetadataAdmissionOwner;
            for (permits[0..index]) |prior| if (prior == permit) return error.DuplicateMetadataAdmissionOwner;
            if (permit.lease) |lease| {
                const policy = session.run_admission orelse return error.ForeignMetadataAdmissionOwner;
                if (lease.controller != policy.controller or lease.retain_backend_class != policy.backend_class) return error.ForeignMetadataAdmissionOwner;
                total = try total.merge(lease.amounts);
            }
        }
        return total;
    }

    pub fn allocator(self: *Owner) std.mem.Allocator {
        return self.admitted.allocator();
    }

    /// Replace borrowed facts only after checking their complete actual sum.
    /// This does not merge, move, reduce or lend any of the supplied leases.
    pub fn setHeldPermits(self: *Owner, permits: []const *const sessions.RunPermit) !void {
        if (self.pending_additional != null) return error.MetadataPendingAdmissionMustBeReplaced;
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.held_floor != null) return error.MetadataAdmissionOwnersFrozen;
        const floor = try sumPermits(self.session, permits);
        if (self.session.run_admission) |policy| try policy.validateOwnedRunAmounts(try (try floor.merge(self.admittedAmounts())).merge(try self.sharedMetadataAmounts()));
        @memset(&self.held, null);
        for (permits, 0..) |permit, index| self.held[index] = permit;
        self.held_count = permits.len;
    }

    /// An exact mandatory cold recipe remains a denial floor while earlier
    /// metadata/weight preparation runs. It supplies no quota or credit.
    pub fn setPendingAdditional(self: *Owner, amounts: memory.AdmissionAmounts) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.held_floor != null) return error.MetadataAdmissionOwnersFrozen;
        if (self.pending_additional != null) return error.MetadataPendingAdmissionAlreadyBound;
        try self.validateAdditional(amounts);
        self.pending_additional = amounts;
    }
    /// Replace the pending recipe with actual independently acquired permits.
    /// No lease moves here. Failure preserves both old held facts and pending
    /// denial bytes; success checks the actual full classes before publication.
    pub fn setHeldPermitsReplacingPending(self: *Owner, permits: []const *const sessions.RunPermit) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.held_floor != null) return error.MetadataAdmissionOwnersFrozen;
        const pending = self.pending_additional orelse return error.MetadataPendingAdmissionMissing;
        const floor = try sumPermits(self.session, permits);
        const required = try (try self.heldAmounts()).merge(pending);
        inline for (@typeInfo(memory.AdmissionAmounts).@"struct".field_names) |field| {
            if (@field(floor, field) < @field(required, field)) return error.MetadataPendingAdmissionIncomplete;
        }
        if (self.session.run_admission) |policy| try policy.validateOwnedRunAmounts(try (try floor.merge(self.admittedAmounts())).merge(try self.sharedMetadataAmounts()));
        @memset(&self.held, null);
        for (permits, 0..) |permit, index| self.held[index] = permit;
        self.held_count = permits.len;
        self.pending_additional = null;
    }

    /// A retained immutable Definition supplies its actual independently
    /// charged metadata amounts once, before banks grow. This is only a full
    /// request denial floor: it grants no lease, storage credit or refund and
    /// retains no pointer to the shared owner. It survives lease partitioning.
    pub fn setSharedMetadataFloor(self: *Owner, amounts: memory.AdmissionAmounts) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.shared_metadata_floor != null or self.shared_preparing != null) return error.MetadataSharedFloorAlreadyBound;
        try self.validateAdditional(amounts);
        self.shared_metadata_floor = amounts;
        self.shared_contributions = 1;
    }

    /// Two independent immutable phase Definitions may be prepared in order.
    /// Only one actual Shared owner is borrowed at a time; sealed contributions
    /// remain value-only denial floors and confer no quota or bank credit.
    pub fn addSharedDefinitionFloor(self: *Owner, shared: *const Shared) !void {
        const identity = @intFromPtr(shared);
        for (self.shared_identities[0..self.shared_contributions]) |prior| if (prior == identity) return error.DuplicateMetadataSharedOwner;
        const amounts = shared.admittedAmounts();
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.shared_preparing != null or self.shared_contributions >= 2) return error.MetadataSharedFloorAlreadyBound;
        const combined = try (self.shared_metadata_floor orelse memory.AdmissionAmounts{}).merge(amounts);
        try self.validateAdditional(amounts);
        self.shared_metadata_floor = combined;
        self.shared_identities[self.shared_contributions] = identity;
        self.shared_contributions += 1;
    }
    fn sharedMetadataAmounts(self: *const Owner) !memory.AdmissionAmounts {
        const sealed = self.shared_metadata_floor orelse memory.AdmissionAmounts{};
        return if (self.shared_preparing) |shared| try sealed.merge(shared.admittedAmounts()) else sealed;
    }
    fn registerSharedConstruction(self: *Owner, shared: *Shared) !void {
        const identity = @intFromPtr(shared);
        for (self.shared_identities[0..self.shared_contributions]) |prior| if (prior == identity) return error.DuplicateMetadataSharedOwner;
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.shared_preparing != null or self.shared_contributions >= 2) return error.MetadataSharedFloorAlreadyBound;
        try self.validateAdditional(shared.admittedAmounts());
        self.shared_preparing = shared;
        self.shared_identities[self.shared_contributions] = identity;
        self.shared_contributions += 1;
    }
    fn detachSharedConstruction(self: *Owner, shared: *Shared) !void {
        std.debug.assert(self.shared_preparing == shared);
        const combined = try (self.shared_metadata_floor orelse memory.AdmissionAmounts{}).merge(shared.admittedAmounts());
        self.shared_metadata_floor = combined;
        self.shared_preparing = null;
    }
    fn restoreSharedConstruction(self: *Owner, shared: *Shared) void {
        std.debug.assert(self.shared_preparing == null and self.shared_metadata_floor != null);
        var previous = self.shared_metadata_floor.?;
        const amount = shared.admittedAmounts();
        inline for (@typeInfo(memory.AdmissionAmounts).@"struct".field_names) |field| {
            @field(previous, field) = std.math.sub(usize, @field(previous, field), @field(amount, field)) catch unreachable;
        }
        self.shared_metadata_floor = if (self.shared_contributions == 1) null else previous;
        self.shared_preparing = shared;
    }

    /// Actual independently held metadata quota, including tracking headers
    /// and reserved chunk slack. Unmanaged owners have no admission claim.
    pub fn admittedAmounts(self: *const Owner) memory.AdmissionAmounts {
        if (self.session.run_admission == null) return .{};
        var result = if (self.wrapper_lease) |lease| lease.amounts else memory.AdmissionAmounts{};
        result.host_scratch_bytes += self.admitted.reserved_bytes;
        return result;
    }

    fn heldAmounts(self: *const Owner) !memory.AdmissionAmounts {
        if (self.held_floor) |floor| return floor;
        var permits: [max_held]*const sessions.RunPermit = undefined;
        for (self.held[0..self.held_count], 0..) |permit, index| permits[index] = permit orelse return error.MissingMetadataAdmissionOwner;
        return sumPermits(self.session, permits[0..self.held_count]);
    }

    /// Keep the actual complete request as a conservative denial floor while
    /// its leases partition into backing owners. This value grants no quota,
    /// storage credit or refund. No new bank permit may join after this point;
    /// only metadata for those already admitted banks can continue to grow.
    pub fn preserveHeldFloorBeforePartition(self: *Owner) !void {
        if (self.pending_additional != null) return error.MetadataPendingAdmissionMustBeReplaced;
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.held_floor != null) return error.MetadataAdmissionOwnersFrozen;
        try self.validateAdditional(.{});
        self.held_floor = try self.heldAmounts();
        @memset(&self.held, null);
        self.held_count = 0;
    }

    /// Necessary full-request fit before an additional numerical/cache lease.
    /// Existing peer requests remain controller contention, never subtraction.
    pub fn validateAdditional(self: *const Owner, additional: memory.AdmissionAmounts) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        const policy = self.session.run_admission orelse return;
        const owned = try (try self.heldAmounts()).merge(self.admittedAmounts());
        const shared = try owned.merge(try self.sharedMetadataAmounts());
        const total = try shared.merge(self.pending_additional orelse .{});
        try policy.validateOwnedRunAmounts(try total.merge(additional));
    }

    fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.validateAdditional(.{ .host_scratch_bytes = bytes });
        return acquireHost(self.session, bytes);
    }

    fn acquireHost(session: sessions.Session, bytes: usize) !memory.AdmissionLease {
        var permit = try session.admitHostPreprocess(bytes);
        defer permit.deinit();
        const lease = permit.lease orelse return error.MissingRunAdmission;
        permit.lease = null;
        // The Owner's actual Model pin already retains these services through
        // every chunk refund. Release only this redundant transient pin.
        return lease;
    }

    pub fn admissionError(self: *const Owner) ?anyerror {
        return self.admitted.admission_error;
    }

    /// No supplier or refund is allowed while the real decoder owns its lane.
    /// Existing prefunded capacity can still serve metadata without growing.
    pub fn sealBeforeExecution(self: *Owner) !void {
        if (self.pending_additional != null) return error.MetadataPendingAdmissionMustBeReplaced;
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        try self.validateAdditional(.{});
        self.admitted.numerical_scope_active = true;
        @memset(&self.held, null);
        self.held_count = 0;
        self.phase = .sealed;
    }

    /// First close the program/Graph/Pool and leave all actual Model gates.
    /// Leases and their real process-manager callbacks retire only here.
    pub fn deinitOutsideExecution(self: *Owner) void {
        std.debug.assert(self.shared_preparing == null);
        const backing = self.backing;
        const model = self.model;
        var wrapper = self.wrapper_lease;
        self.admitted.numerical_scope_active = false;
        self.admitted.deinit();
        backing.destroy(self);
        if (wrapper) |*lease| lease.release();
        if (model) |value| value.release();
    }
};

/// Cycle-free metadata for an immutable shared definition. Only construction
/// borrows the request's actual full-floor validator. Publication keeps the
/// independent admission Domain and its own leases, never a Model or Session.
/// The Definition supplies its existing reference count and exclusively owns
/// this endpoint; final drop consumes it exactly once.
pub const Shared = struct {
    const DomainPin = @import("../runtime/tier/admission_domain.zig").Pin;
    const Epoch = @import("retirement_sink.zig").Epoch;
    const Cleanup = @import("cleanup_context.zig").Context;
    const Phase = enum { preparing, sealed, retiring };
    backing: std.mem.Allocator,
    admitted: AdmittedAllocator,
    wrapper_lease: ?memory.AdmissionLease,
    domain: DomainPin,
    preparing: ?*Owner,
    finalizer: ?struct { body: *anyopaque, destroy: *const fn (*anyopaque) void } = null,
    phase: Phase = .preparing,
    epoch: ?Epoch = null,
    node: @import("resource_retirement.zig").Node = undefined,

    /// The caller retains the request Owner and obeys its outside-gate backing
    /// allocator contract until seal. This backing context must independently
    /// outlive final shared retirement; it cannot be the request AA or a Model
    /// arena. Raw/private owners keep request-local
    /// compatibility; no independent Domain is invented for their callbacks.
    pub fn create(backing: std.mem.Allocator, request: *Owner) !?*Shared {
        if (request.phase != .preparing) return error.MetadataPreparationSealed;
        const policy = request.session.run_admission orelse return null;
        const model = request.session.model_lifetime orelse return null;
        const domain = model.retainAdmissionDomain(policy.controller) orelse return null;
        errdefer domain.release();
        try request.validateAdditional(.{ .host_scratch_bytes = @sizeOf(Shared) });
        var wrapper: ?memory.AdmissionLease = try Owner.acquireHost(request.session, @sizeOf(Shared));
        errdefer if (wrapper) |*lease| lease.release();
        const self = try backing.create(Shared);
        self.* = .{ .backing = backing, .admitted = .{ .backing = backing }, .wrapper_lease = wrapper, .domain = domain, .preparing = request };
        errdefer backing.destroy(self);
        self.admitted.admission_context = self;
        self.admitted.admit = acquire;
        try request.registerSharedConstruction(self);
        return self;
    }
    pub fn allocator(self: *Shared) std.mem.Allocator {
        return self.admitted.allocator();
    }
    pub fn admittedAmounts(self: *const Shared) memory.AdmissionAmounts {
        var amounts = if (self.wrapper_lease) |lease| lease.amounts else memory.AdmissionAmounts{};
        amounts.host_scratch_bytes += self.admitted.reserved_bytes;
        return amounts;
    }
    pub fn admissionError(self: *const Shared) ?anyerror {
        return self.admitted.admission_error;
    }
    fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
        const self: *Shared = @ptrCast(@alignCast(raw));
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        const request = self.preparing orelse return error.MetadataPreparationSealed;
        // Parent includes this actual current Shared contribution while cold;
        // only the newly requested bytes are additional, never the total twice.
        try request.validateAdditional(.{ .host_scratch_bytes = bytes });
        return Owner.acquireHost(request.session, bytes);
    }
    /// The callback owns only definition metadata allocated by this allocator.
    /// It runs after the caller's actual B, before AA physical death/refunds.
    pub fn setFinalizer(self: *Shared, body: *anyopaque, destroy: *const fn (*anyopaque) void) !void {
        if (self.phase != .preparing or self.finalizer != null) return error.MetadataFinalizerAlreadyBound;
        self.finalizer = .{ .body = body, .destroy = destroy };
    }
    pub fn sealBeforeExecution(self: *Shared) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        const request = self.preparing orelse return error.MetadataPreparationSealed;
        try request.validateAdditional(.{});
        if (self.finalizer == null) return error.MetadataFinalizerRequired;
        try request.detachSharedConstruction(self);
        self.admitted.numerical_scope_active = true;
        self.admitted.admission_context = null;
        self.admitted.admit = null;
        self.preparing = null;
        self.phase = .sealed;
    }
    /// Called only by the existing Definition's final reference drop, outside
    /// cache locks. A real current B defers ALL body/AA/quota callbacks; absence
    /// of B never implies outside execution. No allocation occurs here.
    pub fn releaseWithCleanup(self: *Shared, current: Cleanup) !void {
        if (self.phase == .retiring) return error.MetadataRetirementAlreadyPending;
        if (current.boundary) |epoch| {
            const boundary = try epoch.boundaryBeforeSeal();
            const retained = try epoch.retain();
            const previous = self.phase;
            const preparing = self.preparing;
            if (preparing) |request| request.detachSharedConstruction(self) catch |err| {
                var held = retained;
                held.release();
                return err;
            };
            self.preparing = null;
            self.epoch = retained;
            self.node = .{ .context = self, .release_fn = ended };
            self.phase = .retiring;
            boundary.register(boundary.context, boundary.nonce, &self.node) catch |err| {
                self.phase = previous;
                self.preparing = preparing;
                if (preparing) |request| request.restoreSharedConstruction(self);
                self.epoch = null;
                var held = retained;
                held.release();
                return err;
            };
            // The successful callback can already have consumed self.
            return;
        }
        if (!current.outside_execution) return error.MetadataCleanupBoundaryRequired;
        if (self.preparing) |request| try request.detachSharedConstruction(self);
        self.preparing = null;
        self.phase = .retiring;
        self.drop();
    }
    fn ended(raw: *anyopaque) callconv(.c) void {
        const self: *Shared = @ptrCast(@alignCast(raw));
        self.drop();
    }
    fn drop(self: *Shared) void {
        const backing = self.backing;
        const domain = self.domain;
        var wrapper = self.wrapper_lease;
        var epoch = self.epoch;
        if (self.finalizer) |body| body.destroy(body.body);
        self.admitted.numerical_scope_active = false;
        self.admitted.deinit();
        backing.destroy(self);
        if (wrapper) |*lease| lease.release();
        domain.release();
        if (epoch) |*held| held.release();
    }
};

const TestSession = struct {
    fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
        return error.UnexpectedCallback;
    }
    fn info(_: *anyopaque) []const TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) BackendType {
        return .native;
    }
    fn close(_: *anyopaque) void {}
    const table: sessions.Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
};

test "cold metadata admits actual chunks with the whole held request and forbids numerical growth" {
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var marker: u8 = 0;
    const limit = 1024 * 1024;
    const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = limit }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var base = sessions.RunPermit{ .session = session, .lease = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = limit }, .{ .host_kv_bytes = 64 }, false) };
    defer base.deinit();
    const original = controller.snapshot();
    const owner = try Owner.create(a, session, &.{&base});
    var open = true;
    defer if (open) owner.deinitOutsideExecution();
    const values = try owner.allocator().alloc(f32, 64);
    const admitted = controller.snapshot();
    try std.testing.expect(admitted.host_scratch_bytes >= @sizeOf(Owner) + 64 * @sizeOf(f32));
    try std.testing.expectEqualDeep(try original.merge(owner.admittedAmounts()), admitted);
    try std.testing.expectEqual(original.host_kv_bytes, admitted.host_kv_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, owner.validateAdditional(.{ .host_scratch_bytes = limit - admitted.hostTotalBytes() + 1 }));
    try std.testing.expectEqualDeep(admitted, controller.snapshot());
    try std.testing.expectError(error.DuplicateMetadataAdmissionOwner, owner.setHeldPermits(&.{ &base, &base }));
    try owner.sealBeforeExecution();
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, limit));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceLimitExceeded), owner.admissionError());
    try std.testing.expectEqualDeep(admitted, controller.snapshot());
    owner.allocator().free(values);
    try std.testing.expectEqualDeep(admitted, controller.snapshot()); // no callbacks under the lane
    owner.deinitOutsideExecution();
    open = false;
    try std.testing.expectEqualDeep(original, controller.snapshot());
}

test "cold metadata owner and payload allocation failures refund every acquired lease" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var controller = memory.AdmissionController{};
            defer controller.deinit();
            defer std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot()) catch @panic("cold metadata leaked admission");
            var marker: u8 = 0;
            const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
            const owner = try Owner.create(a, session, &.{});
            defer owner.deinitOutsideExecution();
            const values = try owner.allocator().alloc(u32, 16);
            defer owner.allocator().free(values);
            @memset(values, 7);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "cold metadata denial floor survives actual lease partition without granting storage credit" {
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var marker: u8 = 0;
    const limit = 1024 * 1024;
    const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = limit }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var base = sessions.RunPermit{ .session = session, .lease = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = limit }, .{ .host_kv_bytes = 128 }, false), .request = .{} };
    defer base.deinit();
    const owner = try Owner.create(a, session, &.{&base});
    defer owner.deinitOutsideExecution();
    const values = try owner.allocator().alloc(u8, 32);
    defer owner.allocator().free(values);
    const before = controller.snapshot();
    try owner.preserveHeldFloorBeforePartition();
    var bank = try base.lease.?.partition(a, .{ .host_kv_bytes = 128 });
    defer bank.release();
    try std.testing.expectEqual(@as(usize, 0), base.lease.?.amounts.host_kv_bytes);
    try std.testing.expectEqualDeep(before, controller.snapshot());
    try std.testing.expectError(error.ResourceLimitExceeded, owner.validateAdditional(.{ .host_scratch_bytes = limit - before.hostTotalBytes() + 1 }));
    try std.testing.expectError(error.MetadataAdmissionOwnersFrozen, owner.setHeldPermits(&.{&base}));
    try std.testing.expectEqualDeep(before, controller.snapshot());
}

test "cold metadata frees real payload and owner before refund callbacks and retains Model until last refund" {
    const Probe = struct {
        backing: std.mem.Allocator,
        live_bytes: usize = 0,
        next: usize = 0,
        releases: usize = 0,
        violation: bool = false,
        model_closed: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
            self.live_bytes += len;
            return result;
        }
        fn free(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(buffer, alignment, ra);
            self.live_bytes -= buffer.len;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.next += 1;
            return self.next;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, id: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const expected: usize = if (id == 1) 0 else @sizeOf(Owner);
            if (self.live_bytes != expected or self.model_closed) self.violation = true;
            self.releases += 1;
        }
        fn destroyModel(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.live_bytes != 0 or self.releases != self.next) self.violation = true;
            self.model_closed = true;
        }
        const allocator_table: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free };
    };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var probe = Probe{ .backing = std.testing.allocator };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var model = sessions.Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroyModel };
    const session = sessions.Session{ .ptr = &probe, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
    const owner = try Owner.create(.{ .ptr = &probe, .vtable = &Probe.allocator_table }, session, &.{});
    const values = try owner.allocator().alloc(u8, 32);
    try owner.sealBeforeExecution();
    session.close();
    try std.testing.expect(!probe.model_closed);
    owner.allocator().free(values);
    try std.testing.expectEqual(@as(usize, 0), probe.releases);
    owner.deinitOutsideExecution();
    try std.testing.expect(probe.model_closed and !probe.violation);
    try std.testing.expectEqual(probe.next, probe.releases);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

const SharedMetadataFixture = struct {
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    backing: std.mem.Allocator,
    live_bytes: usize = 0,
    armed: bool = false,
    model_closed: bool = false,
    body_dead: bool = false,
    violation: bool = false,
    contexts: usize = 0,
    tokens: usize = 0,
    releases: usize = 0,
    fn alloc(raw: *anyopaque, bytes: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const result = self.backing.rawAlloc(bytes, alignment, ra) orelse return null;
        self.live_bytes += bytes;
        return result;
    }
    fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.backing.rawFree(bytes, alignment, ra);
        self.live_bytes -= bytes.len;
    }
    fn retainContext(raw: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.contexts += 1;
        return true;
    }
    fn releaseContext(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.armed and (!self.body_dead or self.live_bytes != 0 or self.releases != self.tokens)) self.violation = true;
        self.contexts -= 1;
    }
    fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.tokens += 1;
        return self.tokens;
    }
    fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
    fn release(raw: *anyopaque, _: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        if (self.armed and (!self.model_closed or !self.body_dead or self.live_bytes > @sizeOf(Shared))) self.violation = true;
        self.releases += 1;
    }
    fn closeModel(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.model_closed = true;
    }
    const Body = struct {
        allocator: std.mem.Allocator,
        probe: *SharedMetadataFixture,
        values: []u8,
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const a = self.allocator;
            const probe = self.probe;
            a.free(self.values);
            a.destroy(self);
            probe.body_dead = true;
        }
    };
    const allocator_table: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free };

    fn run(a: std.mem.Allocator) !void {
        var probe = SharedMetadataFixture{ .backing = a };
        const backing = std.mem.Allocator{ .ptr = &probe, .vtable = &allocator_table };
        const domain = try Domain.create();
        var domain_open = true;
        defer if (domain_open) domain.release();
        try domain.controller.configureResourceBudget(.{ .context = &probe, .retain_context = retainContext, .release_context = releaseContext, .try_reserve = reserve, .retain = retain, .release = release });
        var model = sessions.Session.ModelLifetime{ .owner = &probe, .destroy_fn = closeModel, .admission_domain = domain.pin() };
        const session = sessions.Session{ .ptr = &probe, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
        var session_open = true;
        defer if (session_open) session.close();
        const request = try Owner.create(backing, session, &.{});
        var request_open = true;
        defer if (request_open) request.deinitOutsideExecution();
        const shared = (try Shared.create(backing, request)) orelse return error.ExpectedSharedMetadata;
        var shared_open = true;
        defer if (shared_open) shared.releaseWithCleanup(.{ .outside_execution = true }) catch @panic("shared metadata abort failed");
        const body = try shared.allocator().create(Body);
        body.* = .{ .allocator = shared.allocator(), .probe = &probe, .values = &.{} };
        try shared.setFinalizer(body, Body.destroy);
        body.values = try shared.allocator().alloc(u8, 97);
        @memset(body.values, 7);
        try shared.sealBeforeExecution();
        try std.testing.expect(shared.preparing == null and shared.admitted.session == null and shared.admitted.admit == null);
        try std.testing.expectError(error.OutOfMemory, shared.allocator().alloc(u8, 2 * 1024 * 1024));
        const shared_amounts = shared.admittedAmounts();
        try std.testing.expect(shared_amounts.host_scratch_bytes > @sizeOf(Shared));
        const before_floor = domain.controller.snapshot();
        try std.testing.expect(request.shared_preparing == null);
        try std.testing.expectEqualDeep(shared_amounts, request.shared_metadata_floor.?);
        try std.testing.expectEqualDeep(before_floor, domain.controller.snapshot());
        try std.testing.expectError(error.MetadataSharedFloorAlreadyBound, request.setSharedMetadataFloor(.{}));
        const bad_bank = memory.AdmissionAmounts{ .host_scratch_bytes = 1024 * 1024 - before_floor.hostTotalBytes() + 1 };
        try std.testing.expectError(error.ResourceLimitExceeded, request.validateAdditional(bad_bank));
        try request.preserveHeldFloorBeforePartition();
        try std.testing.expectError(error.ResourceLimitExceeded, request.validateAdditional(bad_bank));
        try std.testing.expectEqualDeep(before_floor, domain.controller.snapshot());
        request.deinitOutsideExecution();
        request_open = false;
        session.close();
        session_open = false;
        try std.testing.expect(probe.model_closed);
        try std.testing.expectEqualDeep(shared_amounts, domain.controller.snapshot());
        domain.release();
        domain_open = false;
        probe.armed = true;
        const sink = try @import("retirement_sink.zig").Ref.create();
        defer {
            sink.close() catch @panic("sink close failed");
            var owned = sink;
            owned.release();
        }
        var sealed_guard = try sink.enter();
        var sealed_epoch = try sealed_guard.retainEpoch();
        defer sealed_epoch.release();
        sealed_guard.finish();
        try std.testing.expectError(error.RetirementEpochSealed, shared.releaseWithCleanup(.{ .boundary = &sealed_epoch }));
        try std.testing.expect(!probe.body_dead);
        var guard = try sink.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try std.testing.expectError(error.MetadataCleanupBoundaryRequired, shared.releaseWithCleanup(.{}));
        const before = probe.releases;
        try shared.releaseWithCleanup(.{ .boundary = &epoch });
        shared_open = false;
        try std.testing.expect(!probe.body_dead and probe.releases == before and probe.contexts == 1);
        guard.finish();
        try std.testing.expect(probe.body_dead and !probe.violation);
        try std.testing.expectEqual(probe.tokens, probe.releases);
        try std.testing.expectEqual(@as(usize, 0), probe.live_bytes);
        try std.testing.expectEqual(@as(usize, 0), probe.contexts);
    }
};

test "shared cold metadata retains only Domain through Model close and destroys body before actual B refunds" {
    try SharedMetadataFixture.run(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, SharedMetadataFixture.run, .{});
}

test "shared cold metadata preserves actual held request floor and raw owners decline before allocation" {
    const a = std.testing.allocator;
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const domain = try Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    var model = sessions.Session.ModelLifetime{ .owner = &marker, .destroy_fn = TestSession.close, .admission_domain = domain.pin() };
    const limit = 4096 + @sizeOf(Owner) + @sizeOf(Shared) + 128;
    const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = limit }, .static_workspace_bytes = 0, .check_live_memory = false } };
    defer session.close();
    var base = sessions.RunPermit{ .session = session, .lease = try domain.controller.tryAcquire(.cpu, .{ .host_limit_bytes = limit }, .{ .host_kv_bytes = 4096 }, false) };
    defer base.deinit();
    const request = try Owner.create(a, session, &.{&base});
    defer request.deinitOutsideExecution();
    try request.setPendingAdditional(.{ .host_kv_bytes = 32 });
    const shared = (try Shared.create(a, request)) orelse return error.ExpectedSharedMetadata;
    defer shared.releaseWithCleanup(.{ .outside_execution = true }) catch @panic("shared abort failed");
    const before = domain.controller.snapshot();
    try std.testing.expectError(error.OutOfMemory, shared.allocator().alloc(u8, 1024));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceLimitExceeded), shared.admissionError());
    try std.testing.expectEqualDeep(before, domain.controller.snapshot());
    // Private request metadata growth interleaves with the independently
    // admitted Shared graph. Its actual live wrapper must be an intrinsic
    // floor, not be mistaken for unrelated controller contention.
    try std.testing.expectError(error.OutOfMemory, request.allocator().alloc(u8, @sizeOf(Shared)));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceLimitExceeded), request.admissionError());
    try std.testing.expectEqualDeep(before, domain.controller.snapshot());
    try std.testing.expectError(error.MetadataPendingAdmissionMustBeReplaced, request.setHeldPermits(&.{&base}));
    try std.testing.expectError(error.MetadataPendingAdmissionIncomplete, request.setHeldPermitsReplacingPending(&.{&base}));
    try std.testing.expectError(error.MetadataPendingAdmissionMustBeReplaced, request.preserveHeldFloorBeforePartition());
    try std.testing.expectError(error.MetadataPendingAdmissionMustBeReplaced, request.sealBeforeExecution());
    try std.testing.expect(request.pending_additional != null);
    var actual_cache = sessions.RunPermit{ .session = session, .lease = try domain.controller.tryAcquire(.cpu, .{ .host_limit_bytes = limit }, .{ .host_kv_bytes = 32 }, false) };
    defer actual_cache.deinit();
    const after_cache = domain.controller.snapshot();
    try request.setHeldPermitsReplacingPending(&.{ &base, &actual_cache });
    try std.testing.expect(request.pending_additional == null);
    try std.testing.expectEqualDeep(after_cache, domain.controller.snapshot());
    const raw_session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table };
    const raw = try Owner.create(a, raw_session, &.{});
    defer raw.deinitOutsideExecution();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expect((try Shared.create(failing.allocator(), raw)) == null);
    try std.testing.expect(!failing.has_induced_failure);
}

test "aborted shared construction detaches its request before deferred metadata retirement" {
    const a = std.testing.allocator;
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    var model = sessions.Session.ModelLifetime{ .owner = &marker, .destroy_fn = TestSession.close, .admission_domain = domain.pin() };
    const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
    defer session.close();
    const request = try Owner.create(a, session, &.{});
    var request_open = true;
    defer if (request_open) request.deinitOutsideExecution();
    const shared = (try Shared.create(a, request)) orelse return error.ExpectedSharedMetadata;
    var shared_open = true;
    defer if (shared_open) shared.releaseWithCleanup(.{ .outside_execution = true }) catch @panic("shared abort failed");
    _ = try shared.allocator().alloc(u8, 73); // real abandoned partial graph
    const actual = shared.admittedAmounts();
    const sink = try @import("retirement_sink.zig").Ref.create();
    defer {
        sink.close() catch @panic("sink close failed");
        var held = sink;
        held.release();
    }
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try shared.releaseWithCleanup(.{ .boundary = &epoch });
    shared_open = false;
    try std.testing.expect(request.shared_preparing == null and shared.preparing == null);
    request.deinitOutsideExecution();
    request_open = false;
    try std.testing.expectEqualDeep(actual, domain.controller.snapshot());
    guard.finish(); // request allocator endpoint is already physically gone
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
}

fn sequentialPhaseMetadataCase(a: std.mem.Allocator) !void {
    const domain = try @import("../runtime/tier/admission_domain.zig").Domain.create();
    defer domain.release();
    defer std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot()) catch @panic("phase metadata quota leaked");
    var closed = false;
    const Close = struct {
        fn destroy(raw: *anyopaque) void {
            const flag: *bool = @ptrCast(@alignCast(raw));
            flag.* = true;
        }
    };
    var model = sessions.Session.ModelLifetime{ .owner = &closed, .destroy_fn = Close.destroy, .admission_domain = domain.pin() };
    const session = sessions.Session{ .ptr = &closed, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &domain.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var model_open = true;
    defer if (model_open) session.close();
    const request = try Owner.create(a, session, &.{});
    var request_open = true;
    defer if (request_open) request.deinitOutsideExecution();
    var deaths: usize = 0;
    const Body = struct {
        allocator: std.mem.Allocator,
        deaths: *usize,
        payload: []u8 = &.{},
        fn destroy(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const allocator = self.allocator;
            const count = self.deaths;
            allocator.free(self.payload);
            allocator.destroy(self);
            count.* += 1;
        }
        fn init(shared: *Shared, count: *usize) !void {
            const allocator = shared.allocator();
            const self = try allocator.create(@This());
            self.* = .{ .allocator = allocator, .deaths = count };
            try shared.setFinalizer(self, destroy);
            self.payload = try allocator.alloc(u8, 701);
            @memset(self.payload, 17);
        }
    };
    const decode = (try Shared.create(a, request)) orelse return error.ExpectedSharedMetadata;
    var decode_open = true;
    defer if (decode_open) decode.releaseWithCleanup(.{ .outside_execution = true }) catch @panic("decode metadata abort");
    try Body.init(decode, &deaths);
    try decode.sealBeforeExecution();
    const decode_amounts = decode.admittedAmounts();
    const initial = (try Shared.create(a, request)) orelse return error.ExpectedSharedMetadata;
    var initial_open = true;
    defer if (initial_open) initial.releaseWithCleanup(.{ .outside_execution = true }) catch @panic("initial metadata abort");
    try Body.init(initial, &deaths);
    // Actual private growth interleaves with the second phase's real Shared AA.
    const private = try request.allocator().alloc(u8, 1003);
    defer if (request_open) request.allocator().free(private);
    const live_total = try decode_amounts.merge(initial.admittedAmounts());
    try std.testing.expectEqualDeep(live_total, try request.sharedMetadataAmounts());
    const before = domain.controller.snapshot();
    const too_much = memory.AdmissionAmounts{ .host_scratch_bytes = 1024 * 1024 - before.hostTotalBytes() + 1 };
    try std.testing.expectError(error.ResourceLimitExceeded, request.validateAdditional(too_much));
    try initial.sealBeforeExecution();
    try std.testing.expectEqualDeep(live_total, try request.sharedMetadataAmounts());
    try std.testing.expectError(error.ResourceLimitExceeded, request.validateAdditional(too_much));
    // A fresh cached-hit owner binds the two actual distinct retained owners.
    const hit = try Owner.create(a, session, &.{});
    var hit_open = true;
    defer if (hit_open) hit.deinitOutsideExecution();
    try hit.addSharedDefinitionFloor(decode);
    try std.testing.expectError(error.DuplicateMetadataSharedOwner, hit.addSharedDefinitionFloor(decode));
    try hit.addSharedDefinitionFloor(initial);
    try std.testing.expectEqualDeep(live_total, try hit.sharedMetadataAmounts());
    hit.deinitOutsideExecution();
    hit_open = false;
    request.allocator().free(private);
    request.deinitOutsideExecution();
    request_open = false;
    session.close();
    model_open = false;
    // Both independently admitted Shared definitions retain Domain only.
    try std.testing.expect(closed);
    const sink = try @import("retirement_sink.zig").Ref.create();
    defer {
        sink.close() catch @panic("phase sink close");
        var owned = sink;
        owned.release();
    }
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try decode.releaseWithCleanup(.{ .boundary = &epoch });
    decode_open = false;
    try initial.releaseWithCleanup(.{ .boundary = &epoch });
    initial_open = false;
    try std.testing.expectEqual(@as(usize, 0), deaths);
    const reserved = domain.controller.snapshot();
    try std.testing.expect(reserved.host_scratch_bytes >= live_total.host_scratch_bytes);
    guard.finish();
    try std.testing.expectEqual(@as(usize, 2), deaths);
}
test "sequential decode and initial Shared definitions preserve both live and sealed denial floors without duplicated quota" {
    try sequentialPhaseMetadataCase(std.testing.allocator);
}
test "sequential two phase Shared cold creation and partial publication unwind every allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, sequentialPhaseMetadataCase, .{});
}
