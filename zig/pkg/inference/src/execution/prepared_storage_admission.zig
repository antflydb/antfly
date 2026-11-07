// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold funding of exact physical-plan spans. Numerical dispatch only moves
//! preallocated owners; it cannot call an admission or lifetime producer.
const std = @import("std");
const storage = @import("storage_admission.zig");
const accounting = @import("storage_accounting.zig");
const Backend = @import("../graph/backend_contracts.zig").BackendKind;
const sink = @import("retirement_sink.zig");
const retirement = @import("resource_retirement.zig");
const BackingOwner = @import("backing_retirement.zig").BackingOwner;
const platform = @import("antfly_platform");
const metadata_allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
pub const Requirement = struct { backend: Backend, class: accounting.Class, bytes: usize };

/// Caller-owned cold metadata, so an empty/full-warm plan allocates nothing.
/// items() borrows this exact Builder until deinit; Prepared copies entries.
pub const Builder = struct {
    allocator: std.mem.Allocator,
    inline_items: [8]Requirement = undefined,
    spill: ?[]Requirement = null,
    count: usize = 0,
    pub fn items(self: *const Builder) []const Requirement {
        return if (self.spill) |values| values[0..self.count] else self.inline_items[0..self.count];
    }
    pub fn append(self: *Builder, required: Requirement) !void {
        if (required.bytes == 0) return;
        const capacity = if (self.spill) |values| values.len else self.inline_items.len;
        if (self.count == capacity) {
            const expanded = try self.allocator.alloc(Requirement, try std.math.mul(usize, capacity, 2));
            @memcpy(expanded[0..self.count], self.items());
            if (self.spill) |old| self.allocator.free(old);
            self.spill = expanded;
        }
        if (self.spill) |values| values[self.count] = required else self.inline_items[self.count] = required;
        self.count += 1;
    }
    pub fn deinit(self: *Builder) void {
        if (self.spill) |values| self.allocator.free(values);
        self.spill = null;
        self.count = 0;
    }
};

pub const Prepared = struct {
    allocator: std.mem.Allocator,
    identity: storage.Identity,
    entries: []Entry,
    initialized: usize = 0,
    references: std.atomic.Value(usize) = .init(1),
    mutex: std.atomic.Mutex = .unlocked,
    attempted: bool = false,
    retired: bool = false,
    construction_epoch: ?sink.Epoch = null,
    const Entry = struct { requirement: Requirement, backing: BackingOwner = undefined, attachment: ?retirement.Charge.PreparedAttachment = null, held: bool = true };

    /// No metadata/Sink allocation for an empty or fully charged warm plan.
    /// PRECONDITION: outside actual provider/model gates, including producers.
    pub fn prepare(source_capability: storage.Capability, requirements: []const Requirement) !?*Prepared {
        if (requirements.len == 0) return null;
        return try prepareWithAllocator(metadata_allocator, source_capability, requirements);
    }
    fn prepareWithAllocator(allocator: std.mem.Allocator, source_capability: storage.Capability, requirements: []const Requirement) !*Prepared {
        if (requirements.len == 0) return error.InvalidPhysicalStorageReservation;
        for (requirements) |required| if (required.bytes == 0 or (required.backend != .native and required.backend != .metal and required.backend != .cuda)) return error.InvalidPhysicalStorageReservation;
        const owner = try allocator.create(Prepared);
        errdefer allocator.destroy(owner);
        const entries = try allocator.alloc(Entry, requirements.len);
        errdefer allocator.free(entries);
        for (entries, requirements) |*entry, required| entry.* = .{ .requirement = required };
        owner.* = .{ .allocator = allocator, .identity = source_capability.identity, .entries = entries };
        errdefer for (entries[0..owner.initialized]) |*entry| {
            if (entry.attachment) |*ticket| ticket.deinit();
            entry.backing.charge.release();
        };
        for (entries) |*entry| {
            const required = entry.requirement;
            var reserved: ?storage.Reservation = try source_capability.reserve(allocator, required.backend, required.class, required.bytes);
            errdefer if (reserved) |*value| value.release();
            const expected: @import("../runtime/tier/memory.zig").AdmissionAmounts = if (required.backend == .native or required.class == .host_workspace)
                .{ .host_scratch_bytes = required.bytes }
            else
                .{ .backend_scratch_bytes = required.bytes };
            if (!reserved.?.identity.eql(source_capability.identity) or !std.meta.eql(reserved.?.amounts(), expected)) return error.InvalidPhysicalStorageReservation;
            var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
            const charge = try retirement.Charge.createReservation(&reserved, &domain);
            if (required.backend != .native and charge.domain == null) {
                charge.release();
                return error.MissingPureBankDomain;
            }
            if (required.backend == .metal) charge.prepareBackendMetadata(.metal, 1, 1) catch |err| {
                charge.release();
                return err;
            };
            if (required.backend == .metal) entry.attachment = charge.prepareAttachment() catch |err| {
                charge.release();
                return err;
            };
            entry.backing.init(charge, .{ .context = owner, .release = releaseCell });
            owner.initialized += 1;
            _ = owner.references.fetchAdd(1, .monotonic);
        }
        return owner;
    }
    fn releaseCell(raw: *anyopaque) void {
        const self: *Prepared = @ptrCast(@alignCast(raw));
        self.releaseMetadata();
    }
    fn releaseMetadata(self: *Prepared) void {
        const count = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(count != 0);
        if (count != 1) return;
        const allocator = self.allocator;
        for (self.entries) |*entry| if (entry.attachment) |*ticket| ticket.deinit();
        allocator.free(self.entries);
        allocator.destroy(self);
    }
    /// Bound explicitly by the actual outer owner, only for a nonempty cold
    /// preparation. No default operation/Sink allocation is needed for warm.
    pub fn bindConstructionEpoch(self: *Prepared, epoch: *const sink.Epoch) !void {
        var retained = try epoch.retain();
        var moved = false;
        defer if (!moved) retained.release();
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.retired or self.attempted) return error.PreparedStorageAdmissionConsumed;
        if (try retained.isSealed()) return error.RetirementEpochSealed;
        if (self.construction_epoch) |old| {
            if (old.record != retained.record or old.nonce != retained.nonce) return error.BackendOperationCleanupChanged;
            return;
        }
        self.construction_epoch = retained;
        moved = true;
    }

    /// The actual numerical scope supplies its exact current outer epoch.
    /// This value is borrowed only through synchronous dispatch, never retained
    /// as an old growth/geometry epoch for future physical cleanup.
    pub fn forDispatch(self: ?*Prepared, identity: storage.Identity) storage.Capability {
        return .{ .identity = identity, .context = if (self) |value| value else identity.controller, .services = if (self != null) &prepared_services else &empty_services };
    }
    const prepared_services: storage.Capability.VTable = .{ .reserve = denyBare, .takeBacking = takeBacking, .constructionEpoch = constructionEpoch };
    const empty_services: storage.Capability.VTable = .{ .reserve = denyBare, .takeBacking = denyBacking };
    fn constructionEpoch(raw: *anyopaque) ?*const sink.Epoch {
        const self: *Prepared = @ptrCast(@alignCast(raw));
        // Owner publishes the bound pin before dispatch and clears it only
        // after dispatch/rollback ends. The returned pointer never escapes.
        return if (self.construction_epoch) |*value| value else null;
    }
    fn denyBare(_: *anyopaque, _: std.mem.Allocator, _: Backend, _: accounting.Class, _: usize) !storage.Reservation {
        return error.UnpreparedPhysicalStorageReservation;
    }
    fn denyBacking(_: *anyopaque, _: std.mem.Allocator, _: Backend, _: accounting.Class, _: usize) !storage.BackingReservation {
        return error.UnpreparedPhysicalStorageReservation;
    }
    fn takeBacking(raw: *anyopaque, _: std.mem.Allocator, backend: Backend, class: accounting.Class, bytes: usize) !storage.BackingReservation {
        const self: *Prepared = @ptrCast(@alignCast(raw));
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.retired) return error.PreparedStorageAdmissionConsumed;
        const epoch = if (self.construction_epoch) |*value| value else return error.MissingCurrentCleanupEpoch;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        for (self.entries) |*entry| {
            if (!entry.held or entry.requirement.backend != backend or entry.requirement.class != class or entry.requirement.bytes != bytes) continue;
            self.attempted = true;
            entry.held = false;
            return .{ .owner = &entry.backing, .construction_epoch = epoch, .attachment = if (entry.attachment) |*ticket| ticket else null };
        }
        return error.UnpreparedPhysicalStorageReservation;
    }
    /// Explicit pre-execution cancellation, outside every actual gate. No
    /// owner has been transferred and no SDK payload/witness can exist.
    pub fn abortBeforeExecution(self: *Prepared) !void {
        platform.sync.lockYielding(&self.mutex);
        if (self.retired or self.attempted) {
            self.mutex.unlock();
            return error.PreparedStorageAdmissionConsumed;
        }
        self.retired = true;
        self.mutex.unlock();
        var epoch = self.construction_epoch;
        self.construction_epoch = null;
        defer if (epoch) |*held| held.release();
        for (self.entries) |*entry| entry.backing.discardUnpublished();
        self.releaseMetadata();
    }
    /// Cancellation before dispatch has no SDK owner. Once a dispatch binds
    /// its current epoch, unused tokens retire through that exact same scope.
    pub fn finishInvocation(self: *Prepared) void {
        self.finishInvocationWithBoundary(null);
    }
    pub fn finishInvocationWithBoundary(self: *Prepared, current: ?*const sink.Epoch) void {
        if (self.construction_epoch != null) {
            const boundary = current orelse @import("antfly_platform").inference_process_supervisor.restartWorker();
            self.releaseAfterExecution(boundary);
        } else self.abortBeforeExecution() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
    }
    pub fn releaseAfterExecution(self: *Prepared, cleanup: *const sink.Epoch) void {
        platform.sync.lockYielding(&self.mutex);
        if (self.retired) @panic("prepared bank set released twice");
        self.retired = true;
        self.mutex.unlock();
        var epoch = self.construction_epoch;
        self.construction_epoch = null;
        defer if (epoch) |*held| held.release();
        for (self.entries) |*entry| if (entry.held) {
            entry.held = false;
            entry.backing.release(cleanup);
        };
        // Moved cells may outlive the invocation: their independent metadata
        // refs keep this one slab alive, while their own quota retires exactly.
        self.releaseMetadata();
    }
};

const Fixture = if (@import("builtin").is_test) struct {
    controller: *@import("../runtime/tier/memory.zig").AdmissionController,
    source: *@import("../runtime/tier/memory.zig").AdmissionLease,
    credit: *@import("../runtime/tier/memory.zig").AdmissionAmounts,
    gate: std.atomic.Mutex = .unlocked,
    reserve_calls: usize = 0,
    lifetime_refs: usize = 0,
    fn outside(self: *@This()) void {
        if (!self.gate.tryLock()) @panic("producer callback ran under actual outer gate");
        self.gate.unlock();
    }
    fn retain(raw: *anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.outside();
        self.lifetime_refs += 1;
        return true;
    }
    fn release(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.outside();
        self.lifetime_refs -= 1;
    }
    fn reserve(raw: *anyopaque, allocator: std.mem.Allocator, backend: Backend, class: accounting.Class, bytes: usize) !storage.Reservation {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.outside();
        self.reserve_calls += 1;
        return storage.fromRunLease(allocator, self.source, .{ .controller = self.controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = 64 }, .check_live_memory = false, .credit = self.credit, .lifetime = .{ .context = self, .retain = retain, .release = release } }, backend, class, bytes);
    }
    fn capability(self: *@This()) storage.Capability {
        return .{ .identity = .{ .controller = self.controller, .backend_class = .cpu }, .context = self, .services = &.{ .reserve = reserve } };
    }
} else void;

fn preparedBanksCase(allocator: std.mem.Allocator) !void {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var source = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_scratch_bytes = 64 }, false);
    defer source.release();
    var credit = memory.AdmissionAmounts{ .host_scratch_bytes = 48 };
    var fixture = Fixture{ .controller = &controller, .source = &source, .credit = &credit };
    var owner: ?*Prepared = try Prepared.prepareWithAllocator(allocator, fixture.capability(), &.{ .{ .backend = .native, .class = .activation, .bytes = 16 }, .{ .backend = .native, .class = .host_workspace, .bytes = 32 } });
    defer if (owner) |value| value.abortBeforeExecution() catch @panic("unretired prepared bank set");
    try std.testing.expectEqual(@as(usize, 2), fixture.reserve_calls);
    try std.testing.expectEqual(@as(usize, 2), fixture.lifetime_refs);
    var receiver = try sink.Ref.create();
    defer receiver.release();
    var guard = try receiver.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try owner.?.bindConstructionEpoch(&epoch);
    const dispatched = Prepared.forDispatch(owner, fixture.capability().identity);
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    const physical = (try dispatched.reserveBacking(failing.allocator(), .native, .activation, 16)).?.owner;
    var physical_live = true;
    defer if (physical_live) physical.release(&epoch);
    try std.testing.expectError(error.UnpreparedPhysicalStorageReservation, dispatched.reserveBacking(failing.allocator(), .metal, .activation, 16));
    try std.testing.expectError(error.UnpreparedPhysicalStorageReservation, dispatched.reserveBacking(failing.allocator(), .native, .activation, 16));
    try std.testing.expectEqual(@as(usize, 2), fixture.reserve_calls);

    try std.testing.expect(fixture.gate.tryLock());
    var gate_held = true;
    defer if (gate_held) fixture.gate.unlock();
    owner.?.releaseAfterExecution(&epoch);
    owner = null;
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 2), fixture.lifetime_refs);
    fixture.gate.unlock();
    gate_held = false;
    guard.finish();
    try std.testing.expectEqual(@as(usize, 32), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 1), fixture.lifetime_refs);
    physical.release(&epoch);
    physical_live = false;
    try std.testing.expectEqual(@as(usize, 16), controller.snapshot().host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), fixture.lifetime_refs);
    source.release();
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().host_scratch_bytes);
}
test "prepared bank admission moves tokens without producer callbacks and retires unused quota after exact outer epoch" {
    try preparedBanksCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, preparedBanksCase, .{});
}

test "unused bank tokens retire through distinct current B after construction A seals" {
    const memory = @import("../runtime/tier/memory.zig");
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var source = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = 64 }, .{ .host_scratch_bytes = 64 }, false);
    defer source.release();
    var credit = memory.AdmissionAmounts{ .host_scratch_bytes = 16 };
    var fixture = Fixture{ .controller = &controller, .source = &source, .credit = &credit };
    const prepared = try Prepared.prepareWithAllocator(std.testing.allocator, fixture.capability(), &.{.{ .backend = .native, .class = .activation, .bytes = 16 }});
    var owner = try sink.Ref.create();
    defer owner.release();
    var old_guard = try owner.enter();
    defer old_guard.finish();
    var old_epoch = try old_guard.retainEpoch();
    defer old_epoch.release();
    try prepared.bindConstructionEpoch(&old_epoch);
    old_guard.finish();
    try std.testing.expect(try old_epoch.isSealed());
    var current_guard = try owner.enter();
    defer current_guard.finish();
    var current_epoch = try current_guard.retainEpoch();
    defer current_epoch.release();
    try std.testing.expect(fixture.gate.tryLock());
    prepared.finishInvocationWithBoundary(&current_epoch);
    // A is already sealed, but cannot authorize refund under B's live gate.
    try std.testing.expectEqual(@as(usize, 1), fixture.lifetime_refs);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().host_scratch_bytes);
    fixture.gate.unlock();
    current_guard.finish();
    try std.testing.expectEqual(@as(usize, 0), fixture.lifetime_refs);
    try std.testing.expectEqual(@as(usize, 48), controller.snapshot().host_scratch_bytes);
}
