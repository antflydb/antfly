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
    // Additional preparation alone owns this disjoint host quota. Its final
    // bank-cell drop releases it after all Set/Entry/consumer metadata dies.
    additional_metadata: ?@import("../runtime/tier/memory.zig").AdmissionLease = null,
    additional_domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null,
    additional_floor: ?@import("../runtime/tier/memory.zig").AdmissionAmounts = null,
    additional_root: retirement.Node = undefined,
    const Entry = struct { requirement: Requirement, backing: BackingOwner = undefined, attachment: ?retirement.Charge.PreparedAttachment = null, held: bool = true };

    /// No metadata/Sink allocation for an empty or fully charged warm plan.
    /// PRECONDITION: outside actual provider/model gates, including producers.
    pub fn prepare(source_capability: storage.Capability, requirements: []const Requirement) !?*Prepared {
        if (requirements.len == 0) return null;
        return try prepareWithAllocator(metadata_allocator, source_capability, requirements);
    }
    pub const AdditionalOptions = struct {
        /// Exact retained consumer Lease/view/header/shape records, separately
        /// derived from that consumer's real backend metadata profile. Includes
        /// no vendor guesses, physical payload, or original request allowance.
        consumer_metadata_bytes: usize = 0,
    };

    pub fn additionalMetadataBytes(count: usize, options: AdditionalOptions) !usize {
        if (count == 0) {
            if (options.consumer_metadata_bytes != 0) return error.InvalidPhysicalStorageReservation;
            return 0;
        }
        const per_entry = try std.math.add(usize, @sizeOf(Entry), @sizeOf(retirement.Charge));
        const owned = try std.math.add(usize, @sizeOf(Prepared), try std.math.mul(usize, count, per_entry));
        return std.math.add(usize, owned, options.consumer_metadata_bytes);
    }

    /// Full new quota beside the actual original run/PAGE. PRECONDITION: all
    /// numerical/provider/Model/cohort lanes are released. Empty work allocates
    /// nothing. Only the pure Domain survives: no Model, primary-lease pointer,
    /// caller allocator, or reservation callback escapes this constructor.
    /// Consumer records use process-stable allocation and must die no later
    /// than the last moved bank cell; its exact declared metadata stays paid.
    pub fn prepareAdditionalOutside(session: @import("../backends/session.zig").Session, primary: *@import("../runtime/tier/memory.zig").AdmissionLease, requirements: []const Requirement, options: AdditionalOptions) !?*Prepared {
        return prepareAdditionalWithAllocator(metadata_allocator, session, primary, requirements, options);
    }
    fn prepareAdditionalWithAllocator(allocator: std.mem.Allocator, session: @import("../backends/session.zig").Session, primary: *@import("../runtime/tier/memory.zig").AdmissionLease, requirements: []const Requirement, options: AdditionalOptions) !?*Prepared {
        const memory = @import("../runtime/tier/memory.zig");
        if (requirements.len == 0) {
            if (options.consumer_metadata_bytes != 0) return error.InvalidPhysicalStorageReservation;
            return null;
        }
        const policy = session.run_admission orelse return error.MissingRunAdmission;
        const model = session.model_lifetime orelse return error.MissingModelLifetime;
        if (primary.controller != policy.controller or primary.retain_backend_class != policy.backend_class) return error.ForeignAdmissionDomain;
        const domain = model.retainAdmissionDomain(policy.controller) orelse return error.ForeignAdmissionDomain;
        var domain_owned = true;
        defer if (domain_owned) domain.release();
        var physical = memory.AdmissionAmounts{};
        for (requirements) |required| {
            // Metal preparation additionally allocates opaque SDK retirement
            // arena metadata. It keeps the original constructor until that
            // backend supplies an exact separately funded metadata profile.
            if (required.bytes == 0 or (required.backend != .native and required.backend != .cuda)) return error.UnsupportedAdditionalStorageBackend;
            if (required.backend == .cuda and policy.backend_class != .gpu) return error.ForeignAdmissionDomain;
            if (required.backend == .native or required.class == .host_workspace)
                physical.host_scratch_bytes = try std.math.add(usize, physical.host_scratch_bytes, required.bytes)
            else
                physical.backend_scratch_bytes = try std.math.add(usize, physical.backend_scratch_bytes, required.bytes);
        }
        // These paths allocate exactly Set+Entry array+one Charge per entry.
        // BackingOwner is embedded in Entry; Domain pins allocate no object.
        const metadata_bytes = try additionalMetadataBytes(requirements.len, options);
        const extra = try physical.merge(.{ .host_scratch_bytes = metadata_bytes });
        const floor = try primary.amounts.merge(extra);
        // Intrinsic complete-request denial precedes partial acquisitions;
        // existing peer requests remain genuine controller contention.
        try policy.validateOwnedRunAmounts(floor);
        var metadata: ?memory.AdmissionLease = try policy.controller.tryAcquire(policy.backend_class, policy.limits, .{ .host_scratch_bytes = metadata_bytes }, policy.check_live_memory);
        errdefer if (metadata) |*held| held.release();
        const Source = struct {
            primary: *memory.AdmissionLease,
            policy: storage.Policy,
            fn reserve(raw: *anyopaque, a: std.mem.Allocator, backend: Backend, class: accounting.Class, bytes: usize) !storage.Reservation {
                const source: *@This() = @ptrCast(@alignCast(raw));
                return storage.fromRunLease(a, source.primary, source.policy, backend, class, bytes);
            }
        };
        var source = Source{ .primary = primary, .policy = .{ .controller = policy.controller, .backend_class = policy.backend_class, .limits = policy.limits, .check_live_memory = policy.check_live_memory, .credit = null, .lifetime = storage.Lifetime.forAdmissionDomain(domain) } };
        const prepared = try prepareWithAllocator(allocator, .{ .identity = .{ .controller = policy.controller, .backend_class = policy.backend_class }, .context = &source, .services = &.{ .reserve = Source.reserve } }, requirements);
        prepared.additional_metadata = metadata;
        prepared.additional_domain = domain;
        prepared.additional_floor = floor;
        prepared.additional_root = .{ .context = prepared, .release_fn = releaseAdditionalMetadata };
        metadata = null;
        domain_owned = false;
        return prepared;
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
        var metadata = self.additional_metadata;
        const domain = self.additional_domain;
        for (self.entries) |*entry| if (entry.attachment) |*ticket| ticket.deinit();
        allocator.free(self.entries);
        allocator.destroy(self);
        if (metadata) |*held| held.release();
        if (domain) |held| held.release();
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
        if (self.additional_metadata != null) {
            self.releaseAdditionalAfterExecution(cleanup) catch platform.inference_process_supervisor.restartWorker();
            return;
        }
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
    fn releaseAdditionalMetadata(raw: *anyopaque) callconv(.c) void {
        const self: *Prepared = @ptrCast(@alignCast(raw));
        self.releaseMetadata();
    }
    /// The invocation root itself may be the LAST metadata reference after
    /// every moved bank died at an earlier epoch. Publish its existing Node
    /// before mutating custody, so invalid/sealed registration loses no root.
    fn releaseAdditionalAfterExecution(self: *Prepared, cleanup: *const sink.Epoch) !void {
        const boundary = try cleanup.boundaryBeforeSeal();
        platform.sync.lockYielding(&self.mutex);
        if (self.retired) {
            self.mutex.unlock();
            return error.PreparedStorageAdmissionConsumed;
        }
        boundary.register(boundary.context, boundary.nonce, &self.additional_root) catch |err| {
            self.mutex.unlock();
            return err;
        };
        self.retired = true;
        var epoch = self.construction_epoch;
        self.construction_epoch = null;
        self.mutex.unlock();
        defer if (epoch) |*held| held.release();
        for (self.entries) |*entry| if (entry.held) {
            entry.held = false;
            entry.backing.release(cleanup);
        };
        // Successful registration transferred the invocation root to its Node.
        // No immediate metadata decrement/refund is permitted in this scope.
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

const AdditionalFixture = if (@import("builtin").is_test) struct {
    const sessions = @import("../backends/session.zig");
    const memory = @import("../runtime/tier/memory.zig");
    const Tensor = @import("../backends/tensor.zig").Tensor;
    const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
    gate: ?*std.atomic.Mutex = null,
    next: usize = 0,
    attempts: usize = 0,
    deny_at: ?usize = null,
    releases: usize = 0,
    violation: bool = false,
    model_closed: bool = false,
    fn outside(self: *@This()) void {
        const gate = self.gate orelse @panic("actual Model gate not installed");
        if (!gate.tryLock()) {
            self.violation = true;
            return;
        }
        gate.unlock();
    }
    fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.outside();
        self.attempts += 1;
        if (self.deny_at == self.attempts) return error.ResourceTemporarilyUnavailable;
        self.next += 1;
        return self.next;
    }
    fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.outside();
    }
    fn release(raw: *anyopaque, _: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.outside();
        self.releases += 1;
    }
    fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
        return error.UnexpectedNumericalInvocation;
    }
    fn info(_: *anyopaque) []const TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
        return .cuda;
    }
    fn close(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.model_closed = true;
    }
    const table: sessions.Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
} else void;

fn additionalStorageCase(a: std.mem.Allocator, finish_first: bool) !void {
    const sessions = @import("../backends/session.zig");
    const memory = @import("../runtime/tier/memory.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const domain = try Domain.create();
    defer domain.release();
    var fixture = AdditionalFixture{};
    try domain.controller.configureResourceBudget(.{ .context = &fixture, .try_reserve = AdditionalFixture.reserve, .retain = AdditionalFixture.retain, .release = AdditionalFixture.release });
    var model = sessions.Session.ModelLifetime{ .owner = &fixture, .destroy_fn = AdditionalFixture.close, .admission_domain = domain.pin() };
    fixture.gate = &model.execution_gate;
    var model_live = true;
    const session = sessions.Session{ .ptr = &fixture, .vtable = &AdditionalFixture.table, .model_lifetime = &model, .execution_gate = &model.execution_gate, .run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 1024 * 1024, .backend_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    defer if (model_live) session.close();
    var primary = try domain.controller.tryAcquire(.gpu, session.run_admission.?.limits, .{ .host_kv_bytes = 7, .host_scratch_bytes = 11, .backend_scratch_bytes = 32 }, false);
    var primary_live = true;
    defer if (primary_live) primary.release();
    const original = primary;
    const requirements = [_]Requirement{ .{ .backend = .cuda, .class = .activation, .bytes = 16 }, .{ .backend = .cuda, .class = .workspace, .bytes = 32 } };
    const options: Prepared.AdditionalOptions = .{ .consumer_metadata_bytes = 37 };
    const bytes = try Prepared.additionalMetadataBytes(requirements.len, options);
    const before = domain.controller.snapshot();
    defer {
        // Model/primary may already have closed on the held-backing path.
        const expected = if (primary_live) before else memory.AdmissionAmounts{};
        std.debug.assert(std.meta.eql(expected, domain.controller.snapshot()));
        std.debug.assert(!fixture.violation);
    }
    const calls = fixture.next;
    var denied = session;
    denied.run_admission.?.limits.backend_limit_bytes = 79; // 32+16+32, not just new48
    try std.testing.expectError(error.ResourceLimitExceeded, Prepared.prepareAdditionalWithAllocator(a, denied, &primary, &requirements, options));
    try std.testing.expectEqual(calls, fixture.next);
    try std.testing.expect(std.meta.eql(original, primary));
    denied = session;
    denied.run_admission.?.limits.host_limit_bytes = before.host_scratch_bytes + before.host_kv_bytes + bytes - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, Prepared.prepareAdditionalWithAllocator(a, denied, &primary, &requirements, options));
    try std.testing.expectEqual(calls, fixture.next);
    try std.testing.expectError(error.Overflow, Prepared.additionalMetadataBytes(std.math.maxInt(usize), options));
    var wrong = session;
    const foreign = try Domain.create();
    defer foreign.release();
    wrong.run_admission.?.controller = &foreign.controller;
    try std.testing.expectError(error.ForeignAdmissionDomain, Prepared.prepareAdditionalWithAllocator(a, wrong, &primary, &requirements, options));
    try std.testing.expectError(error.UnsupportedAdditionalStorageBackend, Prepared.prepareAdditionalWithAllocator(a, session, &primary, &.{.{ .backend = .metal, .class = .activation, .bytes = 4 }}, .{}));
    try std.testing.expectError(error.InvalidPhysicalStorageReservation, Prepared.prepareAdditionalWithAllocator(a, session, &primary, &.{}, options));
    try std.testing.expect((try Prepared.prepareAdditionalWithAllocator(a, session, &primary, &.{}, .{})) == null);
    const prepared = (try Prepared.prepareAdditionalWithAllocator(a, session, &primary, &requirements, options)) orelse return error.MissingAdditionalPreparedStorage;
    var invocation_live = true;
    defer if (invocation_live) prepared.abortBeforeExecution() catch @panic("additional preparation unexpectedly consumed");
    try std.testing.expect(std.meta.eql(original, primary));
    try std.testing.expectEqual(@as(usize, 1), model.references.load(.acquire));
    const expected = try before.merge(.{ .backend_scratch_bytes = 48, .host_scratch_bytes = bytes });
    try std.testing.expectEqualDeep(expected, domain.controller.snapshot());
    try std.testing.expectEqualDeep(expected, prepared.additional_floor.?);
    var owner = try sink.Ref.create();
    defer owner.release();
    var guard = try owner.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try prepared.bindConstructionEpoch(&epoch);
    const capability = Prepared.forDispatch(prepared, .{ .controller = &domain.controller, .backend_class = .gpu });
    const first = (try capability.reserveBacking(a, .cuda, .activation, 16)).?.owner;
    var first_live = true;
    defer if (first_live) first.release(&epoch);
    const last = (try capability.reserveBacking(a, .cuda, .workspace, 32)).?.owner;
    var last_live = true;
    defer if (last_live) last.release(&epoch);
    try std.testing.expectError(error.UnpreparedPhysicalStorageReservation, capability.reserveBacking(a, .cuda, .activation, 16));
    try std.testing.expect(fixture.gate.?.tryLock());
    if (finish_first) {
        prepared.finishInvocationWithBoundary(&epoch);
        invocation_live = false;
    }
    first.release(&epoch);
    first_live = false;
    fixture.gate.?.unlock();
    // Ending the actual B retires first, but last still retains the complete
    // metadata quota. The base and Model may die without invalidating last.
    guard.finish();
    try std.testing.expectEqualDeep(try before.merge(.{ .backend_scratch_bytes = 32, .host_scratch_bytes = bytes }), domain.controller.snapshot());
    primary.release();
    primary_live = false;
    session.close();
    model_live = false;
    try std.testing.expect(fixture.model_closed);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = bytes, .backend_scratch_bytes = 32 }, domain.controller.snapshot());
    var next_guard = try owner.enter();
    defer next_guard.finish();
    var next_epoch = try next_guard.retainEpoch();
    defer next_epoch.release();
    try std.testing.expect(fixture.gate.?.tryLock());
    last.release(&next_epoch);
    last_live = false;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = bytes, .backend_scratch_bytes = 32 }, domain.controller.snapshot());
    fixture.gate.?.unlock();
    next_guard.finish();
    if (!finish_first) {
        try std.testing.expectEqual(@as(usize, 1), prepared.references.load(.acquire));
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = bytes }, domain.controller.snapshot());
        try std.testing.expectError(error.RetirementEpochSealed, prepared.releaseAdditionalAfterExecution(&next_epoch));
        try std.testing.expect(!prepared.retired and prepared.references.load(.acquire) == 1);
        var current_guard = try owner.enter();
        defer current_guard.finish();
        var current_epoch = try current_guard.retainEpoch();
        defer current_epoch.release();
        var invalid = current_epoch; // nonowning invalid nonce control, never released
        invalid.nonce +%= 1;
        try std.testing.expectError(error.RetirementEpochConsumed, prepared.releaseAdditionalAfterExecution(&invalid));
        try std.testing.expect(!prepared.retired and prepared.references.load(.acquire) == 1);
        try std.testing.expect(fixture.gate.?.tryLock());
        try prepared.releaseAdditionalAfterExecution(&current_epoch);
        invocation_live = false;
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = bytes }, domain.controller.snapshot());
        fixture.gate.?.unlock();
        current_guard.finish();
    }
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    try std.testing.expectEqual(fixture.next, fixture.releases);
}

test "additional prepared storage preserves actual primary and retains metadata until last current-B bank death" {
    try additionalStorageCase(std.testing.allocator, true);
    try additionalStorageCase(std.testing.allocator, false);
}

test "additional prepared storage allocator failures restore exact primary and fresh quota" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, additionalStorageCase, .{true});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, additionalStorageCase, .{false});
}

test "additional storage supplier denial after metadata and first bank restores every fresh owner" {
    const sessions = @import("../backends/session.zig");
    const memory = @import("../runtime/tier/memory.zig");
    const Domain = @import("../runtime/tier/admission_domain.zig").Domain;
    const domain = try Domain.create();
    defer domain.release();
    var fixture = AdditionalFixture{};
    try domain.controller.configureResourceBudget(.{ .context = &fixture, .try_reserve = AdditionalFixture.reserve, .retain = AdditionalFixture.retain, .release = AdditionalFixture.release });
    var model = sessions.Session.ModelLifetime{ .owner = &fixture, .destroy_fn = AdditionalFixture.close, .admission_domain = domain.pin() };
    fixture.gate = &model.execution_gate;
    const session = sessions.Session{ .ptr = &fixture, .vtable = &AdditionalFixture.table, .model_lifetime = &model, .execution_gate = &model.execution_gate, .run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{ .host_limit_bytes = 1024 * 1024, .backend_limit_bytes = 1024 * 1024 }, .static_workspace_bytes = 0, .check_live_memory = false } };
    defer session.close();
    // Preserve nonzero values in ALL six classes, not merely scratch totals.
    const original_amounts = memory.AdmissionAmounts{ .host_weight_bytes = 3, .backend_weight_bytes = 5, .host_kv_bytes = 7, .backend_kv_bytes = 11, .host_scratch_bytes = 13, .backend_scratch_bytes = 17 };
    var primary = try domain.controller.tryAcquire(.gpu, session.run_admission.?.limits, original_amounts, false);
    defer primary.release();
    const original = primary;
    const refs = domain.references.load(.acquire);
    const attempts = fixture.attempts;
    const acquired = fixture.next;
    const released = fixture.releases;
    fixture.deny_at = attempts + 3; // metadata, first physical bank, denied second bank
    const requirements = [_]Requirement{ .{ .backend = .cuda, .class = .activation, .bytes = 16 }, .{ .backend = .cuda, .class = .workspace, .bytes = 32 } };
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, Prepared.prepareAdditionalOutside(session, &primary, &requirements, .{ .consumer_metadata_bytes = 37 }));
    try std.testing.expectEqual(attempts + 3, fixture.attempts);
    try std.testing.expectEqual(acquired + 2, fixture.next);
    try std.testing.expectEqual(released + 2, fixture.releases);
    try std.testing.expect(std.meta.eql(original, primary));
    try std.testing.expectEqualDeep(original_amounts, domain.controller.snapshot());
    try std.testing.expectEqual(refs, domain.references.load(.acquire));
    try std.testing.expectEqual(@as(usize, 1), model.references.load(.acquire));
    try std.testing.expect(!fixture.violation and !fixture.model_closed);
}
