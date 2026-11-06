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

const std = @import("std");
const platform = @import("antfly_platform");
const planner = @import("planner.zig");
const memory = @import("memory.zig");
const domain = @import("admission_domain.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;

pub const ResidencyTier = planner.ResidencyTier;

pub const Budget = struct {
    host_limit_bytes: usize = 0,
    backend_limit_bytes: usize = 0,
};

pub const Denial = struct {
    tier: ResidencyTier,
    current_bytes: usize,
    request_bytes: usize,
    requested_total_bytes: usize,
    limit_bytes: usize,
};

const AdmissionCredit = struct {
    lease: memory.AdmissionLease,
    tier: ResidencyTier,
    independent: bool = false,
    // Pure cold-prepared custody for this exact growth token. Legacy credits
    // retain their original raw provenance until an actual cold upgrade.
    shell: retirement.PreparedCharge = .{ .owner = null },
    pin: ?domain.Pin = null,

    fn validateCleanup(self: *const @This(), current: Cleanup) !void {
        if (current.outside_execution) return;
        const epoch = current.boundary orelse return error.CacheRetirementCleanupAuthorityRequired;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        if (self.shell.owner == null or self.pin == null) return error.MissingPreparedCacheRetirement;
        if (self.pin.?.controller() != self.lease.controller) return error.ForeignAdmissionDomain;
    }
    fn retireAfterBackingDeath(self: *@This(), current: Cleanup) void {
        self.validateCleanup(current) catch platform.inference_process_supervisor.restartWorker();
        if (current.outside_execution) {
            self.lease.release();
            self.shell.destroyUnused();
            if (self.pin) |pin| pin.release();
            self.pin = null;
            return;
        }
        var lease: ?memory.AdmissionLease = self.lease;
        const charge = self.shell.arm(&lease, &self.pin) catch platform.inference_process_supervisor.restartWorker();
        charge.holdUntilBoundary(current.boundary.?.boundary() catch platform.inference_process_supervisor.restartWorker()) catch platform.inference_process_supervisor.restartWorker();
        charge.release(); // pure oldDomain/token wait actual NOW B outside
    }
};

pub const ReserveError = error{
    MemoryBudgetExceeded,
    ResourceTemporarilyUnavailable,
};

/// New two-tier ledger metadata has a binding lifetime, not an entry lifetime.
/// It owns only independent host quota and process allocator records; no Model
/// or PhysicalBacking self-reference. All leases release outside binding locks.
const BindingMetadata = struct {
    next: ?*BindingMetadata = null,
    shell_lease: ?memory.AdmissionLease = null,
    array_lease: ?memory.AdmissionLease = null,
    shell_bytes: usize = 0,
    array_bytes: usize = 0,
};

/// Exact process-wide ownership for physical cache growth beyond bytes already
/// covered by the model's resident lease. The binding is heap-stable because a
/// backend session may move its SharedCache value while it is being built.
const AdmissionBinding = struct {
    allocator: std.mem.Allocator,
    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    baseline: Budget,
    mutex: std.atomic.Mutex = .unlocked,
    host_bytes: usize = 0,
    backend_bytes: usize = 0,
    host_credited_bytes: usize = 0,
    backend_credited_bytes: usize = 0,
    independent_host_bytes: usize = 0,
    independent_backend_bytes: usize = 0,
    credits: std.ArrayListUnmanaged(AdmissionCredit) = .empty,
    // Actual per-Binding mutation identity. Saturation prevents raw metadata
    // ABA after wrap; cold commits reserve one final revision for rollback.
    revision: u64 = 0,
    deferred_releases: bool = false,
    flush_in_flight: bool = false,
    physical_references: std.atomic.Value(usize) = .init(1),
    physical_component_bytes: usize = 0,
    physical_backend_component_bytes: usize = 0,
    metadata_records: ?*BindingMetadata = null,
    array_metadata: ?*BindingMetadata = null,
    metadata_host_bytes: usize = 0,
    pending_physical: ?*retirement.Node = null,
    pure_final_metadata: bool = false,
    final_domain: ?domain.Pin = null,
    final_epoch: ?@import("../../execution/retirement_sink.zig").Epoch = null,
    final_node: retirement.Node = undefined,
    final_queued: bool = false,

    fn lock(self: *@This()) void {
        platform.sync.lockYielding(&self.mutex);
    }

    fn actualPtr(self: *@This(), tier: ResidencyTier) ?*usize {
        return switch (tier) {
            .disk => null,
            .host => &self.host_bytes,
            .backend => &self.backend_bytes,
        };
    }

    fn creditedPtr(self: *@This(), tier: ResidencyTier) ?*usize {
        return switch (tier) {
            .disk => null,
            .host => &self.host_credited_bytes,
            .backend => &self.backend_credited_bytes,
        };
    }

    fn independentPtr(self: *@This(), tier: ResidencyTier) ?*usize {
        return switch (tier) {
            .disk => null,
            .host => &self.independent_host_bytes,
            .backend => &self.independent_backend_bytes,
        };
    }
    fn desiredCredit(self: *const @This(), tier: ResidencyTier, actual: usize, independent: usize) usize {
        std.debug.assert(actual >= independent);
        return independent + ((actual - independent) -| self.baselineBytes(tier));
    }

    fn baselineBytes(self: *const @This(), tier: ResidencyTier) usize {
        return switch (tier) {
            .disk => 0,
            .host => self.baseline.host_limit_bytes,
            .backend => self.baseline.backend_limit_bytes,
        };
    }

    fn amountsFor(tier: ResidencyTier, bytes: usize) memory.AdmissionAmounts {
        return switch (tier) {
            .disk => .{},
            .host => .{ .host_weight_bytes = bytes },
            .backend => .{ .backend_weight_bytes = bytes },
        };
    }

    fn acquireCredit(self: *@This(), tier: ResidencyTier, bytes: usize) ReserveError!void {
        if (bytes == 0) return;
        self.credits.ensureUnusedCapacity(self.allocator, 1) catch
            return error.ResourceTemporarilyUnavailable;
        const amounts = amountsFor(tier, bytes);
        var pressure: ?memory.AdmissionPressure = null;
        var lease = self.controller.tryAcquireWithPressure(
            self.backend_class,
            self.limits,
            amounts,
            true,
            &pressure,
        ) catch |err| return switch (err) {
            error.ResourceTemporarilyUnavailable => error.ResourceTemporarilyUnavailable,
            else => error.MemoryBudgetExceeded,
        };
        // Cache bytes remain resident after this growth operation. Settle the
        // transient live-pressure epoch while keeping exact policy ownership.
        lease.retain(amounts) catch {
            lease.release();
            return error.MemoryBudgetExceeded;
        };
        self.credits.appendAssumeCapacity(.{ .lease = lease, .tier = tier });
    }

    fn releaseCredit(self: *@This(), tier: ResidencyTier, bytes: usize) void {
        var remaining = bytes;
        var index = self.credits.items.len;
        while (remaining > 0 and index > 0) {
            index -= 1;
            const credit = &self.credits.items[index];
            if (credit.tier != tier or credit.independent) continue;
            if (credit.shell.owner != null or credit.pin != null) @panic("prepared native cache credit requires typed retirement");
            const owned = switch (tier) {
                .disk => 0,
                .host => credit.lease.amounts.host_weight_bytes,
                .backend => credit.lease.amounts.backend_weight_bytes,
            };
            std.debug.assert(owned > 0);
            if (owned <= remaining) {
                remaining -= owned;
                credit.lease.release();
                _ = self.credits.orderedRemove(index);
            } else {
                credit.lease.retain(amountsFor(tier, owned - remaining)) catch
                    @panic("cache admission lease rejected a valid reduction");
                remaining = 0;
            }
        }
        std.debug.assert(remaining == 0);
    }

    fn grow(self: *@This(), tier: ResidencyTier, bytes: usize) ReserveError!void {
        if (tier == .disk or bytes == 0) return;
        self.lock();
        defer self.mutex.unlock();

        if (self.flush_in_flight) return error.ResourceTemporarilyUnavailable;
        const actual = self.actualPtr(tier).?;
        const credited = self.creditedPtr(tier).?;
        const next = std.math.add(usize, actual.*, bytes) catch
            return error.MemoryBudgetExceeded;
        const desired_credit = self.desiredCredit(tier, next, self.independentPtr(tier).?.*);
        if (desired_credit > credited.*) {
            try self.acquireCredit(tier, desired_credit - credited.*);
            credited.* = desired_credit;
        }
        actual.* = next;
        self.revision +|= 1;
    }

    fn release(self: *@This(), tier: ResidencyTier, bytes: usize) void {
        if (tier == .disk or bytes == 0) return;
        self.lock();
        defer self.mutex.unlock();

        const actual = self.actualPtr(tier).?;
        const credited = self.creditedPtr(tier).?;
        std.debug.assert(bytes <= actual.*);
        std.debug.assert(bytes <= actual.* - self.independentPtr(tier).?.*);
        const next = actual.* - bytes;
        const desired_credit = self.desiredCredit(tier, next, self.independentPtr(tier).?.*);
        if (desired_credit < credited.* and !self.deferred_releases) {
            self.releaseCredit(tier, credited.* - desired_credit);
            credited.* = desired_credit;
        }
        actual.* = next;
        self.revision +|= 1;
    }

    /// Called only after the actual CPU backing died. The Native cache owner
    /// still serializes physical counters; this mutex protects only the ledger.
    /// A prepared owner settles reductions after releasing ALL execution and
    /// Native cache locks, never from the physical destructor itself.
    fn releasePrepared(self: *@This(), tier: ResidencyTier, bytes: usize, independent: usize) void {
        self.lock();
        defer self.mutex.unlock();
        const actual = self.actualPtr(tier).?;
        const derived = self.independentPtr(tier).?;
        std.debug.assert(independent <= bytes and independent <= derived.*);
        std.debug.assert(bytes - independent <= actual.* - derived.*);
        actual.* -= bytes;
        derived.* -= independent;
        self.deferred_releases = true;
        self.revision +|= 1;
    }

    fn deferPhysicalDrop(self: *@This(), node: *retirement.Node) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.final_queued) return error.CachePhysicalOwnerAlreadyClosing;
        std.debug.assert(node.next == null);
        node.next = self.pending_physical;
        self.pending_physical = node;
    }
    fn drainPhysicalDropsOutsideExecution(self: *@This()) void {
        while (true) {
            self.lock();
            const node = self.pending_physical orelse {
                self.mutex.unlock();
                return;
            };
            self.pending_physical = node.next;
            node.next = null; // Node may be reused by a later final alias C
            self.mutex.unlock();
            node.release_fn(node.context);
        }
    }

    /// The caller pins the actual Model/ledger and holds NO Model, numerical
    /// lane, Native prefetch/preparation, or binding lock. No metadata allocation
    /// is needed. A detached token has exclusive custody through its callback;
    /// reentrant growth refuses before mutation, while physical removals may
    /// coalesce a further monotonic reduction. A refused external reduction
    /// restores the exact token and conservatively keeps its previous charge.
    fn flushOutsideExecution(self: *@This()) !void {
        self.lock();
        if (self.flush_in_flight) {
            self.mutex.unlock();
            return error.ResourceTemporarilyUnavailable;
        }
        self.flush_in_flight = true;
        self.mutex.unlock();
        defer {
            self.lock();
            self.flush_in_flight = false;
            self.mutex.unlock();
        }
        self.drainPhysicalDropsOutsideExecution();
        while (true) {
            self.lock();
            var chosen: ?usize = null;
            var reduction: usize = 0;
            var index = self.credits.items.len;
            while (index != 0) {
                index -= 1;
                const credit = &self.credits.items[index];
                const desired = self.desiredCredit(credit.tier, self.actualPtr(credit.tier).?.*, self.independentPtr(credit.tier).?.*);
                const surplus = self.creditedPtr(credit.tier).?.* -| desired;
                if (surplus != 0) {
                    chosen = index;
                    reduction = @min(surplus, switch (credit.tier) {
                        .host => credit.lease.amounts.host_weight_bytes,
                        .backend => credit.lease.amounts.backend_weight_bytes,
                        .disk => unreachable,
                    });
                    break;
                }
            }
            const selected = chosen orelse {
                self.mutex.unlock();
                return;
            };
            var detached = self.credits.orderedRemove(selected);
            const owned = switch (detached.tier) {
                .host => detached.lease.amounts.host_weight_bytes,
                .backend => detached.lease.amounts.backend_weight_bytes,
                .disk => unreachable,
            };
            self.mutex.unlock();
            if (reduction == owned) {
                detached.retireAfterBackingDeath(.{ .outside_execution = true });
            } else {
                detached.lease.retain(amountsFor(detached.tier, owned - reduction)) catch |err| {
                    self.lock();
                    // orderedRemove left one existing slot. No callback can
                    // grow this array while flush_in_flight is published.
                    self.credits.appendAssumeCapacity(detached);
                    self.mutex.unlock();
                    return err;
                };
            }
            self.lock();
            self.creditedPtr(detached.tier).?.* -= reduction;
            if (reduction != owned) self.credits.appendAssumeCapacity(detached);
            self.revision +|= 1;
            self.mutex.unlock();
        }
    }

    fn finalDrop(raw: *anyopaque) callconv(.c) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        // Actual Model/store backing is dead. Only independently retained
        // physical components remain; settle the dead remainder outside B.
        self.flushOutsideExecution() catch {}; // refusal keeps conservative tokens
        self.releasePhysicalReference();
    }

    fn markFinalBackingDead(self: *@This()) void {
        self.lock();
        defer self.mutex.unlock();
        self.host_bytes = self.physical_component_bytes;
        self.independent_host_bytes = self.physical_component_bytes;
        self.backend_bytes = self.physical_backend_component_bytes;
        self.independent_backend_bytes = self.physical_backend_component_bytes;
        self.revision +|= 1;
    }
    fn physicalComponentPtr(self: *@This(), tier: ResidencyTier) *usize {
        return switch (tier) {
            .host => &self.physical_component_bytes,
            .backend => &self.physical_backend_component_bytes,
            .disk => unreachable,
        };
    }
    fn releasePhysicalComponent(self: *@This(), tier: ResidencyTier, bytes: usize) void {
        self.lock();
        defer self.mutex.unlock();
        std.debug.assert(self.deferred_releases);
        const physical = self.physicalComponentPtr(tier);
        const actual = self.actualPtr(tier).?;
        const independent = self.independentPtr(tier).?;
        std.debug.assert(bytes <= physical.* and bytes <= actual.* and bytes <= independent.*);
        physical.* -= bytes;
        actual.* -= bytes;
        independent.* -= bytes;
        self.revision +|= 1;
    }

    fn retainPhysicalReference(self: *@This()) void {
        var before = self.physical_references.load(.acquire);
        while (true) {
            if (before == 0 or before == std.math.maxInt(usize)) @panic("invalid cache backing reference");
            before = self.physical_references.cmpxchgWeak(before, before + 1, .acq_rel, .acquire) orelse return;
        }
    }

    fn releasePhysicalReference(self: *@This()) void {
        if (self.physical_references.fetchSub(1, .acq_rel) != 1) return;
        // No Model/store/SDK/arena/Io or receiving header enters this callback.
        // The exact existing token array and certified process metadata remain
        // alive through the actual NOW B; this is quota-only retirement.
        var epoch = self.final_epoch;
        self.final_epoch = null;
        for (self.credits.items) |*credit| credit.retireAfterBackingDeath(.{ .outside_execution = true });
        self.credits.clearRetainingCapacity();
        self.host_bytes = 0;
        self.backend_bytes = 0;
        self.host_credited_bytes = 0;
        self.backend_credited_bytes = 0;
        self.independent_host_bytes = 0;
        self.independent_backend_bytes = 0;
        self.deinit();
        if (epoch) |*held| held.release();
    }

    pub fn deinit(self: *@This()) void {
        self.lock();
        std.debug.assert(self.host_bytes == 0);
        std.debug.assert(self.backend_bytes == 0);
        std.debug.assert(self.host_credited_bytes == 0);
        std.debug.assert(self.backend_credited_bytes == 0);
        std.debug.assert(self.credits.items.len == 0);
        std.debug.assert(self.pending_physical == null);
        self.credits.deinit(self.allocator);
        const allocator = self.allocator;
        var metadata_records = self.metadata_records;
        self.metadata_records = null;
        self.array_metadata = null;
        const old_domain = self.final_domain;
        self.final_domain = null;
        self.mutex.unlock();
        allocator.destroy(self);
        while (metadata_records) |record| {
            metadata_records = record.next;
            var shell = record.shell_lease;
            var array = record.array_lease;
            allocator.destroy(record);
            if (array) |*lease| lease.release();
            if (shell) |*lease| lease.release();
        }
        if (old_domain) |pin| pin.release();
    }
};

/// Exact already-backed component retained by an actual physical owner.
/// It grants no numerical capability and owns no separate quota token. Only
/// physical death decrements the existing ledger; the owner's actual-B Node
/// must release this reference after its own metadata and outer gates end.
pub const PhysicalBacking = struct {
    binding: *AdmissionBinding,
    bytes: usize,
    tier: ResidencyTier = .host,

    /// Partition ONE existing physical component; no ledger/reservation growth.
    /// Both nonoverlapping pieces retain this same actual binding independently.
    pub fn takePart(self: *@This(), bytes: usize) !PhysicalBacking {
        if (bytes == 0 or bytes > self.bytes) return error.InvalidPhysicalBackingPart;
        self.binding.retainPhysicalReference();
        self.bytes -= bytes;
        return .{ .binding = self.binding, .bytes = bytes, .tier = self.tier };
    }
    pub fn controller(self: @This()) *memory.AdmissionController {
        return self.binding.controller;
    }
    /// Pure provenance pin only. Suppliers still run exclusively at the real
    /// Source outside boundary, never while holding metadata/numerical gates.
    pub fn retainAdmissionDomain(self: @This()) !domain.Pin {
        self.binding.lock();
        defer self.binding.mutex.unlock();
        if (!self.binding.pure_final_metadata) return error.MissingCacheRetirementDomain;
        return (self.binding.final_domain orelse return error.MissingCacheRetirementDomain).retain();
    }

    /// Moves an actual cache creator ref to its existing outside settlement
    /// boundary. No physical death/quota/ref mutation occurs at publication.
    pub fn deferCreatorDrop(self: *const @This(), node: *retirement.Node) !void {
        try self.binding.deferPhysicalDrop(node);
    }
    pub fn validateCleanup(self: *const @This(), current: Cleanup) !void {
        if (current.outside_execution) return;
        const epoch = current.boundary orelse return error.CacheRetirementCleanupAuthorityRequired;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        if (!self.binding.pure_final_metadata or self.binding.final_domain == null) return error.MissingCacheRetirementDomain;
    }
    /// Actual payload (including every SDK retain) is already dead. No supplier callback: managed binding
    /// installation permanently selected deferred reductions before use.
    /// Caller has ended actual B and destroyed ALL payload and owner metadata.
    /// May refund exact tokens; never invoke under cache/Model/lane ownership.
    pub fn releaseAfterMetadataDeathOutsideExecution(self: @This()) void {
        const binding = self.binding;
        binding.releasePhysicalComponent(self.tier, self.bytes);
        // Exact reductions settle through the existing outside Source/Manager
        // flush. Explicit outside destruction may still hold a Native metadata
        // lock; it never invokes a growth/shrink supplier here.
        binding.releasePhysicalReference();
    }
};

/// Identity is valid only during a census that retains every actual Model /
/// shared-context owner. It is not a persistent ID, payload pin or quota.
pub const AdmissionSnapshot = struct {
    identity: usize,
    amounts: memory.AdmissionAmounts,
};

pub const SharedCache = struct {
    budget: Budget,
    /// Serving policy installs non-zero ceilings before the session is
    /// published. Architecture-specific floors may widen auto-derived cache
    /// defaults, but must never widen past an explicit hard limit.
    hard_budget: ?Budget = null,
    host_bytes: usize = 0,
    backend_bytes: usize = 0,
    host_loads: u64 = 0,
    backend_loads: u64 = 0,
    host_evictions: u64 = 0,
    backend_evictions: u64 = 0,
    denials: u64 = 0,
    last_denial: ?Denial = null,
    admission: ?*AdmissionBinding = null,

    pub fn init(budget: Budget) SharedCache {
        return .{ .budget = budget };
    }

    /// Install process-owner limits before the cache becomes observable by a
    /// request. Zero retains the auto-derived limit for that tier.
    pub fn configureHardBudget(self: *SharedCache, hard_budget: Budget) void {
        std.debug.assert(self.host_bytes == 0);
        std.debug.assert(self.backend_bytes == 0);
        self.hard_budget = hard_budget;
        if (hard_budget.host_limit_bytes != 0)
            self.budget.host_limit_bytes = hard_budget.host_limit_bytes;
        if (hard_budget.backend_limit_bytes != 0)
            self.budget.backend_limit_bytes = hard_budget.backend_limit_bytes;
    }

    /// Bind post-load cache growth to the same aggregate controller used by
    /// model and request leases. `baseline` is the portion already owned by the
    /// model's resident lease, so mapped source weights are never double-counted
    /// while prepared layouts and promoted weights remain exact.
    pub fn configureAdmission(
        self: *SharedCache,
        allocator: std.mem.Allocator,
        controller: *memory.AdmissionController,
        backend_class: memory.BackendClass,
        limits: memory.Limits,
        baseline: Budget,
    ) !void {
        std.debug.assert(self.host_bytes == 0);
        std.debug.assert(self.backend_bytes == 0);
        std.debug.assert(self.admission == null);
        const binding = try allocator.create(AdmissionBinding);
        binding.* = .{
            .allocator = allocator,
            .controller = controller,
            .backend_class = backend_class,
            .limits = limits,
            .baseline = baseline,
        };
        self.admission = binding;
    }

    /// Actual canonical Native construction uses independent process metadata,
    /// so the existing binding/token array can outlive Model/provider teardown.
    /// Domain installation occurs at the loader's actual resource-owner seam,
    /// before any serving request; no guessed controller lifetime is accepted.
    pub fn configureManagedAdmissionOutsideExecution(self: *SharedCache, controller: *memory.AdmissionController, backend_class: memory.BackendClass, limits: memory.Limits, baseline: Budget) !void {
        const metadata = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
        try self.configureAdmission(metadata, controller, backend_class, limits, baseline);
        self.admission.?.pure_final_metadata = true;
    }

    /// Exact pure provenance for shared-context callers. No supplier runs and
    /// this never authorizes numerical entry or an outside-execution phase.
    pub fn retirementDomainMatches(self: *SharedCache, pin: *const domain.Pin) bool {
        const binding = self.admission orelse return false;
        binding.lock();
        defer binding.mutex.unlock();
        return binding.pure_final_metadata and !binding.final_queued and binding.controller == pin.controller() and
            (if (binding.final_domain) |actual| actual.owner == pin.owner else false);
    }

    pub fn installRetirementDomainOutsideExecution(self: *SharedCache, pin: *const domain.Pin) !void {
        const binding = self.admission orelse return;
        binding.lock();
        defer binding.mutex.unlock();
        if (!binding.pure_final_metadata) return error.CacheRetirementMetadataNotProcessOwned;
        if (binding.controller != pin.controller()) return error.ForeignAdmissionDomain;
        if (binding.final_queued or binding.flush_in_flight) return error.PreparedCacheGrowthChanged;
        if (binding.final_domain) |old| {
            if (old.owner != pin.owner) return error.ForeignAdmissionDomain;
            return;
        }
        // Retain is pure metadata/atomic. No supplier/allocation/payload move.
        binding.final_domain = pin.retain();
        // Ordinary cache destructors may call noteRelease before complete
        // WeightStore death. They must never bypass the actual final B node.
        binding.deferred_releases = true;
    }

    /// Reserve aggregate and live capacity before allocating cache memory.
    /// Callers must release the reservation if the subsequent allocation fails.
    pub fn reserve(self: *SharedCache, tier: ResidencyTier, bytes: usize) ReserveError!void {
        if (tier == .disk or bytes == 0) return;
        if (!self.canFitAdditional(tier, bytes)) return error.MemoryBudgetExceeded;
        if (self.admission) |binding| {
            try binding.grow(tier, bytes);
        }
        self.noteResidentUnchecked(tier, bytes);
    }

    /// One exact pending growth of the EXISTING tier ledger. The caller owns
    /// actual cache exclusion at snapshot/commit and keeps its Model alive.
    /// Only the raw quota value/pure Domain enter persistent Credit custody;
    /// this synchronous candidate and its binding pointer never escape it.
    pub const PreparedGrowth = struct {
        cache: *SharedCache,
        binding: ?*AdmissionBinding,
        metadata_allocator: ?std.mem.Allocator = null,
        tier: ResidencyTier,
        bytes: usize,
        // Physical quota may include an existing owner's exact metadata which
        // is not part of the historical payload-retention policy ceiling.
        logical_bytes: ?usize = null,
        independent: bool = false,
        independent_bytes: usize = 0,
        physical_component_bytes: usize = 0,
        expected_revision: u64 = 0,
        expected_host: usize = 0,
        expected_backend: usize = 0,
        credit_bytes: usize = 0,
        replacement_required: usize = 0,
        funded: bool = false,
        lease: ?memory.AdmissionLease = null,
        shell: retirement.PreparedCharge = .{ .owner = null },
        pin: ?domain.Pin = null,
        replacement: std.ArrayListUnmanaged(AdmissionCredit) = .empty,
        old_metadata: std.ArrayListUnmanaged(AdmissionCredit) = .empty,
        old_array_metadata: ?*BindingMetadata = null,
        old_array_lease: ?memory.AdmissionLease = null,
        old_array_domain: ?domain.Pin = null,
        committed: bool = false,

        fn logicalBytes(self: *const @This()) usize {
            return self.logical_bytes orelse self.bytes;
        }

        /// Exact original tier/baseline arithmetic and pressure settle. Uses
        /// copied recipe + immutable published binding policy only. All
        /// suppliers/metadata run outside Model/prefetch/quantized/binding.
        pub fn fundOutsideExecution(self: *@This(), old_domain: ?*const domain.Pin) !void {
            if (self.funded or self.committed or self.lease != null or self.pin != null or self.shell.owner != null) return error.InvalidPreparedCacheGrowth;
            errdefer self.finishOutsideExecution();
            const binding = self.binding orelse {
                self.funded = true;
                return;
            };
            const pin = old_domain orelse return error.MissingCacheRetirementDomain;
            if (pin.controller() != binding.controller) return error.ForeignAdmissionDomain;
            if (self.credit_bytes != 0) {
                if (self.replacement_required != 0) self.replacement.ensureTotalCapacity(binding.allocator, self.replacement_required) catch return error.ResourceTemporarilyUnavailable;
                self.pin = pin.retain();
                self.shell = try retirement.PreparedCharge.prepare(0);
                const amounts = AdmissionBinding.amountsFor(self.tier, self.credit_bytes);
                var pressure: ?memory.AdmissionPressure = null;
                self.lease = binding.controller.tryAcquireWithPressure(binding.backend_class, binding.limits, amounts, true, &pressure) catch |err| return switch (err) {
                    error.ResourceTemporarilyUnavailable => error.ResourceTemporarilyUnavailable,
                    else => error.MemoryBudgetExceeded,
                };
                self.lease.?.retain(amounts) catch return error.MemoryBudgetExceeded;
            }
            self.funded = true;
        }

        const CommitState = struct { next: usize, independent: usize, desired: usize };
        fn commitStateLocked(self: *const @This(), binding: *AdmissionBinding) !CommitState {
            const next = try std.math.add(usize, binding.actualPtr(self.tier).?.*, self.bytes);
            const independent = try std.math.add(usize, binding.independentPtr(self.tier).?.*, self.independent_bytes);
            const desired = binding.desiredCredit(self.tier, next, independent);
            if (desired -| binding.creditedPtr(self.tier).?.* != self.credit_bytes) return error.PreparedCacheGrowthChanged;
            if (self.credit_bytes != 0 and (self.lease == null or self.pin == null or self.shell.owner == null)) return error.InvalidPreparedCacheGrowth;
            return .{ .next = next, .independent = independent, .desired = desired };
        }
        fn applyCommitLocked(self: *@This(), binding: *AdmissionBinding, state: CommitState) void {
            if (self.credit_bytes != 0) {
                binding.credits.appendAssumeCapacity(.{ .lease = self.lease.?, .tier = self.tier, .independent = self.independent, .shell = self.shell, .pin = self.pin });
                self.lease = null;
                self.shell = .{ .owner = null };
                self.pin = null;
            }
            binding.actualPtr(self.tier).?.* = state.next;
            binding.independentPtr(self.tier).?.* = state.independent;
            binding.creditedPtr(self.tier).?.* = state.desired;
        }

        /// Actual caller revalidates source/key/layout/policy under the SAME
        /// cache metadata exclusion BEFORE this final ledger commit. There is
        /// no supplier/allocation/callback from this function.
        pub fn commit(self: *@This()) !void {
            if (self.committed or !self.funded) return error.PreparedCacheGrowthAlreadyCommitted;
            const cache = self.cache;
            if (cache.admission != self.binding or cache.host_bytes != self.expected_host or cache.backend_bytes != self.expected_backend or !cache.canFitAdditional(self.tier, self.logicalBytes())) return error.PreparedCacheGrowthChanged;
            if (self.binding) |binding| {
                binding.lock();
                defer binding.mutex.unlock();
                if (binding.flush_in_flight or binding.revision >= std.math.maxInt(u64) - 1 or binding.revision != self.expected_revision) return error.PreparedCacheGrowthChanged;
                const state = try self.commitStateLocked(binding);
                if (self.credit_bytes != 0) {
                    if (self.replacement.capacity != 0) {
                        // Array entries move, never duplicate ownership. The
                        // obsolete array is freed only after ALL gates unlock.
                        if (self.replacement.capacity < binding.credits.items.len + 1) return error.PreparedCacheGrowthChanged;
                        self.replacement.appendSliceAssumeCapacity(binding.credits.items);
                        self.old_metadata = binding.credits;
                        binding.credits = self.replacement;
                        self.replacement = .empty;
                        self.old_array_metadata = binding.array_metadata;
                        if (binding.array_metadata) |old| {
                            self.old_array_lease = old.array_lease;
                            old.array_lease = null;
                            self.old_array_domain = binding.final_domain.?.retain();
                            binding.metadata_host_bytes -= old.array_bytes;
                        }
                        // This legacy caller keeps its original common-ledger
                        // metadata scope; it cannot inherit the old array token.
                        binding.array_metadata = null;
                    } else if (binding.credits.items.len == binding.credits.capacity) return error.PreparedCacheGrowthChanged;
                }
                self.applyCommitLocked(binding, state);
                binding.deferred_releases = true;
                binding.revision +|= 1;
            }
            if (self.logicalBytes() != 0) cache.noteResidentUnchecked(self.tier, self.logicalBytes());
            self.committed = true;
        }
        pub fn validatePhysicalBacking(self: *const @This()) !void {
            const binding = self.binding orelse return error.MissingCacheRetirementDomain;
            const physical = if (self.physical_component_bytes != 0) self.physical_component_bytes else self.bytes;
            if ((self.tier != .host and self.tier != .backend) or self.independent_bytes != physical or physical == 0 or !binding.pure_final_metadata or binding.final_domain == null) return error.MissingCacheRetirementDomain;
        }
        /// Same actual cache exclusion spans validation/commit/attach. This
        /// existing physical owner now pins the SAME token ledger, not a copy.
        pub fn retainCommittedPhysicalBacking(self: *const @This()) PhysicalBacking {
            const bytes = if (self.physical_component_bytes != 0) self.physical_component_bytes else self.bytes;
            return self.retainCommittedPhysicalBackingPart(bytes);
        }
        /// One exact independently owned part of this same committed growth.
        /// The serialized caller proves payload/header membership and retains
        /// each part once. Unretained entries keep their original ledger path.
        pub fn retainCommittedPhysicalBackingPart(self: *const @This(), bytes: usize) PhysicalBacking {
            std.debug.assert(self.committed);
            self.validatePhysicalBacking() catch @panic("invalid committed cache backing");
            const total = if (self.physical_component_bytes != 0) self.physical_component_bytes else self.bytes;
            std.debug.assert(bytes > 0 and bytes <= total);
            const binding = self.binding.?;
            binding.lock();
            const physical = binding.physicalComponentPtr(self.tier);
            std.debug.assert(bytes <= binding.independentPtr(self.tier).?.* - physical.*);
            physical.* += bytes;
            binding.retainPhysicalReference();
            binding.mutex.unlock();
            return .{ .binding = binding, .bytes = bytes, .tier = self.tier };
        }

        /// CPU preparation is transactional. If it fails before publishing
        /// any new representation, undo ONLY this exact latest ledger commit.
        /// Same actual exclusion spans commit/prepare/rollback; no callback.
        pub fn rollbackUnpublishedLocked(self: *@This()) !void {
            if (!self.committed) return error.InvalidPreparedCacheGrowth;
            const cache = self.cache;
            if (cache.admission != self.binding) return error.PreparedCacheGrowthChanged;
            if (self.binding) |binding| {
                binding.lock();
                defer binding.mutex.unlock();
                if (binding.revision != self.expected_revision +% 1) return error.PreparedCacheGrowthChanged;
                const actual = binding.actualPtr(self.tier).?;
                const credited = binding.creditedPtr(self.tier).?;
                if (self.credit_bytes != 0) {
                    const moved = binding.credits.pop() orelse return error.PreparedCacheGrowthChanged;
                    self.lease = moved.lease;
                    self.shell = moved.shell;
                    self.pin = moved.pin;
                    credited.* -= self.credit_bytes;
                    if (self.old_metadata.capacity != 0) {
                        self.replacement = binding.credits;
                        binding.credits = self.old_metadata;
                        self.old_metadata = .empty;
                        binding.array_metadata = self.old_array_metadata;
                        if (self.old_array_metadata) |old| {
                            old.array_lease = self.old_array_lease;
                            self.old_array_lease = null;
                            binding.metadata_host_bytes += old.array_bytes;
                        }
                        self.old_array_metadata = null;
                    }
                }
                actual.* -= self.bytes;
                binding.independentPtr(self.tier).?.* -= self.independent_bytes;
                binding.revision +|= 1;
            }
            switch (self.tier) {
                .host => {
                    cache.host_bytes -= self.logicalBytes();
                    if (self.logicalBytes() != 0) cache.host_evictions +|= 1;
                },
                .backend => {
                    cache.backend_bytes -= self.logicalBytes();
                    cache.backend_evictions +|= 1;
                },
                .disk => unreachable,
            }
            self.committed = false;
        }

        /// Outside ALL gates. A refused candidate has not changed the cache,
        /// and owns its ONE unused token. Moved old metadata owns no tokens.
        pub fn finishOutsideExecution(self: *@This()) void {
            if (self.lease) |*lease| lease.release();
            self.lease = null;
            self.shell.destroyUnused();
            if (self.pin) |pin| pin.release();
            self.pin = null;
            if (self.metadata_allocator) |allocator| {
                self.replacement.deinit(allocator);
                self.old_metadata.deinit(allocator);
            }
            self.replacement = .empty;
            self.old_metadata = .empty;
            if (self.old_array_lease) |*lease| lease.release();
            self.old_array_lease = null;
            if (self.old_array_domain) |pin| pin.release();
            self.old_array_domain = null;
            self.old_array_metadata = null;
        }
    };

    /// One cold host-metadata plus backend-payload transaction. Both suppliers
    /// finish outside execution; one binding lock validates/publishes both.
    pub const PreparedPhysicalGrowth = struct {
        parts: [2]PreparedGrowth,
        metadata_allocator: std.mem.Allocator,
        metadata: ?*BindingMetadata = null,
        old_array_metadata: ?*BindingMetadata = null,
        old_array_lease: ?memory.AdmissionLease = null,
        old_array_domain: ?domain.Pin = null,
        replacement_required: usize,
        replacement: std.ArrayListUnmanaged(AdmissionCredit) = .empty,
        old_metadata: std.ArrayListUnmanaged(AdmissionCredit) = .empty,
        replaced: bool = false,
        published_metadata: bool = false,
        committed: bool = false,

        pub fn fundingMetadataBytes(self: *const @This()) !usize {
            var count: usize = 0;
            for (self.parts) |part| if (part.credit_bytes != 0) {
                count += 1;
            };
            if (count == 0 and self.replacement_required == 0) return 0;
            const shells = try std.math.mul(usize, count, try retirement.PreparedCharge.requestedMetadataBytes(0));
            const array = try std.math.mul(usize, self.replacement_required, @sizeOf(AdmissionCredit));
            return std.math.add(usize, @sizeOf(BindingMetadata), try std.math.add(usize, shells, array));
        }
        fn validateFundingRequest(self: *const @This(), metadata_bytes: usize) !void {
            const binding = self.parts[0].binding orelse return error.MissingCacheRetirementDomain;
            var requested = memory.AdmissionAmounts{ .host_weight_bytes = metadata_bytes };
            for (self.parts) |part| requested = requested.merge(AdmissionBinding.amountsFor(part.tier, part.credit_bytes)) catch return error.MemoryBudgetExceeded;
            if ((requested.firstLimitExceeded(binding.limits) catch return error.MemoryBudgetExceeded) != null) return error.MemoryBudgetExceeded;
            const shared = binding.controller.sharedLimitsSnapshot();
            if ((requested.firstLimitExceeded(.{ .host_limit_bytes = shared.host_limit_bytes, .combined_limit_bytes = shared.unified_limit_bytes }) catch return error.MemoryBudgetExceeded) != null) return error.MemoryBudgetExceeded;
        }
        pub fn fundOutsideExecution(self: *@This(), pin: *const domain.Pin) !void {
            self.fundOutsideExecutionImpl(pin) catch |err| {
                // Shared caps may tighten during the outside acquisitions.
                // Full failed-candidate cleanup has already run. Revalidate
                // the WHOLE request so own partial leases never misclassify a
                // newly intrinsic denial as retryable external contention.
                if (err == error.ResourceTemporarilyUnavailable) try self.validateFundingRequest(try self.fundingMetadataBytes());
                return err;
            };
        }
        fn fundOutsideExecutionImpl(self: *@This(), pin: *const domain.Pin) !void {
            if (self.committed) return error.InvalidPreparedCacheGrowth;
            errdefer self.finishOutsideExecution();
            const binding = self.parts[0].binding orelse return error.MissingCacheRetirementDomain;
            if (self.metadata != null or pin.controller() != binding.controller) return error.ForeignAdmissionDomain;
            const metadata_bytes = try self.fundingMetadataBytes();
            // These leases are ONE cold transaction. Check the entire intrinsic
            // increment before the first acquisition: its earlier HOST leases
            // must not masquerade as external contention for a later piece.
            try self.validateFundingRequest(metadata_bytes);
            if (metadata_bytes != 0) {
                const node = try self.metadata_allocator.create(BindingMetadata);
                node.* = .{};
                self.metadata = node;
                node.array_bytes = try std.math.mul(usize, self.replacement_required, @sizeOf(AdmissionCredit));
                node.shell_bytes = (try self.fundingMetadataBytes()) - node.array_bytes;
                node.shell_lease = binding.controller.tryAcquire(binding.backend_class, binding.limits, .{ .host_weight_bytes = node.shell_bytes }, true) catch |err| return switch (err) {
                    error.ResourceTemporarilyUnavailable => error.ResourceTemporarilyUnavailable,
                    else => error.MemoryBudgetExceeded,
                };
                if (node.array_bytes != 0) node.array_lease = binding.controller.tryAcquire(binding.backend_class, binding.limits, .{ .host_weight_bytes = node.array_bytes }, true) catch |err| return switch (err) {
                    error.ResourceTemporarilyUnavailable => error.ResourceTemporarilyUnavailable,
                    else => error.MemoryBudgetExceeded,
                };
                try node.shell_lease.?.retain(.{ .host_weight_bytes = node.shell_bytes });
                if (node.array_lease) |*lease| try lease.retain(.{ .host_weight_bytes = node.array_bytes });
            }
            if (self.replacement_required != 0) try self.replacement.ensureTotalCapacityPrecise(binding.allocator, self.replacement_required);
            for (&self.parts) |*part| try part.fundOutsideExecution(pin);
        }
        pub fn validatePhysicalBacking(self: *const @This()) !void {
            for (&self.parts) |*part| try part.validatePhysicalBacking();
        }
        pub fn commit(self: *@This()) !void {
            if (self.committed) return error.PreparedCacheGrowthAlreadyCommitted;
            const cache = self.parts[0].cache;
            const binding = self.parts[0].binding orelse return error.MissingCacheRetirementDomain;
            binding.lock();
            defer binding.mutex.unlock();
            var count: usize = 0;
            var states: [2]PreparedGrowth.CommitState = undefined;
            for (&self.parts, 0..) |*part, index| {
                if (!part.funded or part.committed or cache.admission != binding or part.binding != binding or binding.flush_in_flight or binding.final_queued or binding.revision >= std.math.maxInt(u64) - 1 or binding.revision != part.expected_revision or cache.host_bytes != part.expected_host or cache.backend_bytes != part.expected_backend or !cache.canFitAdditional(part.tier, part.logicalBytes())) return error.PreparedCacheGrowthChanged;
                states[index] = try part.commitStateLocked(binding);
                if (part.credit_bytes != 0) count += 1;
            }
            const node = self.metadata;
            if (node) |record| {
                if (record.shell_lease == null or (record.array_bytes != 0 and record.array_lease == null)) return error.InvalidPreparedCacheGrowth;
            } else if (count != 0 or self.replacement.capacity != 0) return error.InvalidPreparedCacheGrowth;
            const next_metadata = try std.math.add(usize, binding.metadata_host_bytes, if (node) |record| try std.math.add(usize, record.shell_bytes, record.array_bytes) else 0);
            const required = try std.math.add(usize, binding.credits.items.len, count);
            if ((if (self.replacement.capacity != 0) self.replacement.capacity else binding.credits.capacity) < required) return error.PreparedCacheGrowthChanged;
            // Every fallible check precedes either ledger publication.
            if (self.replacement.capacity != 0) {
                self.replacement.appendSliceAssumeCapacity(binding.credits.items);
                self.old_metadata = binding.credits;
                binding.credits = self.replacement;
                self.replacement = .empty;
                self.replaced = true;
                self.old_array_metadata = binding.array_metadata;
                if (binding.array_metadata) |old| {
                    self.old_array_lease = old.array_lease;
                    old.array_lease = null;
                    self.old_array_domain = binding.final_domain.?.retain();
                }
                binding.array_metadata = node;
            }
            binding.metadata_host_bytes = next_metadata - (if (self.old_array_metadata) |old| old.array_bytes else 0);
            if (node) |record| {
                record.next = binding.metadata_records;
                binding.metadata_records = record;
                self.published_metadata = true;
                self.metadata = null; // binding owns metadata, never an entry
            }
            for (&self.parts, states) |*part, state| {
                part.applyCommitLocked(binding, state);
                if (part.logicalBytes() != 0) cache.noteResidentUnchecked(part.tier, part.logicalBytes());
                part.committed = true;
            }
            binding.deferred_releases = true;
            binding.revision +|= 1;
            self.committed = true;
        }
        /// No SDK entry or physical component may have published yet. Same
        /// actual exclusion spans commit/SDK setup/this exact rollback.
        pub fn rollbackUnpublishedLocked(self: *@This()) !void {
            if (!self.committed) return error.InvalidPreparedCacheGrowth;
            const cache = self.parts[0].cache;
            const binding = self.parts[0].binding.?;
            binding.lock();
            defer binding.mutex.unlock();
            if (cache.admission != binding or binding.flush_in_flight or binding.revision != self.parts[0].expected_revision +% 1) return error.PreparedCacheGrowthChanged;
            if (self.published_metadata) {
                const metadata = binding.metadata_records orelse return error.PreparedCacheGrowthChanged;
                binding.metadata_records = metadata.next;
                metadata.next = null;
                self.metadata = metadata;
                binding.metadata_host_bytes -= metadata.shell_bytes + metadata.array_bytes;
                self.published_metadata = false;
            }
            if (self.old_array_metadata) |old| {
                old.array_lease = self.old_array_lease;
                self.old_array_lease = null;
                binding.metadata_host_bytes += old.array_bytes;
            }
            var index = self.parts.len;
            while (index != 0) {
                index -= 1;
                const part = &self.parts[index];
                if (part.credit_bytes != 0) {
                    const credit = binding.credits.pop().?;
                    std.debug.assert(credit.tier == part.tier);
                    part.lease = credit.lease;
                    part.pin = credit.pin;
                    part.shell = credit.shell;
                    binding.creditedPtr(part.tier).?.* -= part.credit_bytes;
                }
                binding.actualPtr(part.tier).?.* -= part.bytes;
                binding.independentPtr(part.tier).?.* -= part.independent_bytes;
                switch (part.tier) {
                    .host => cache.host_bytes -= part.logicalBytes(),
                    .backend => cache.backend_bytes -= part.logicalBytes(),
                    .disk => unreachable,
                }
                part.committed = false;
            }
            if (self.replaced) {
                self.replacement = binding.credits;
                binding.credits = self.old_metadata;
                self.old_metadata = .empty;
                self.replaced = false;
                binding.array_metadata = self.old_array_metadata;
                self.old_array_metadata = null;
            }
            binding.revision +|= 1;
            self.committed = false;
        }
        pub fn retainCommittedPhysicalBackings(self: *const @This()) [2]PhysicalBacking {
            std.debug.assert(self.committed);
            return .{ self.parts[0].retainCommittedPhysicalBacking(), self.parts[1].retainCommittedPhysicalBacking() };
        }
        pub fn finishOutsideExecution(self: *@This()) void {
            for (&self.parts) |*part| part.finishOutsideExecution();
            self.replacement.deinit(self.metadata_allocator);
            self.old_metadata.deinit(self.metadata_allocator);
            self.replacement = .empty;
            self.old_metadata = .empty;
            if (self.old_array_lease) |*lease| lease.release();
            self.old_array_lease = null;
            if (self.old_array_domain) |pin| pin.release();
            self.old_array_domain = null;
            self.old_array_metadata = null;
            if (self.metadata) |node| {
                var shell = node.shell_lease;
                var array = node.array_lease;
                self.metadata_allocator.destroy(node);
                if (array) |*lease| lease.release();
                if (shell) |*lease| lease.release();
            }
            self.metadata = null;
        }
    };
    pub fn snapshotPhysicalGrowthLocked(self: *SharedCache, host_metadata_bytes: usize, backend_logical_bytes: usize, backend_physical_bytes: usize) !PreparedPhysicalGrowth {
        var parts = [2]PreparedGrowth{
            try self.snapshotPhysicalOwnerGrowthForTierLocked(.host, 0, host_metadata_bytes),
            try self.snapshotPhysicalOwnerGrowthForTierLocked(.backend, backend_logical_bytes, backend_physical_bytes),
        };
        const binding = self.admission orelse return error.MissingCacheRetirementDomain;
        binding.lock();
        defer binding.mutex.unlock();
        if (!binding.pure_final_metadata or binding.final_domain == null or binding.flush_in_flight or parts[0].expected_revision != binding.revision or parts[1].expected_revision != binding.revision) return error.PreparedCacheGrowthChanged;
        var count: usize = 0;
        for (&parts) |*part| {
            if (part.credit_bytes != 0) count += 1;
            part.replacement_required = 0; // shared destination prepared once
        }
        const required = try std.math.add(usize, binding.credits.items.len, count);
        return .{ .parts = parts, .metadata_allocator = binding.allocator, .replacement_required = if (required > binding.credits.capacity) try std.math.ceilPowerOfTwo(usize, required) else 0 };
    }

    /// Value-only recipe collected under the actual caller's cache exclusion
    /// (Native prefetch/quantized owner). Model pin alone is NOT that proof.
    /// No supplier/allocation here; mutable counters never get read outside.
    pub fn snapshotGrowthLocked(self: *SharedCache, tier: ResidencyTier, bytes: usize) !PreparedGrowth {
        return self.snapshotGrowthClassLocked(tier, bytes, 0);
    }
    /// NEW owned representations are disjoint from the resident/mapped source
    /// baseline. Never lend that source's existing floor a second time.
    pub fn snapshotIndependentGrowthLocked(self: *SharedCache, tier: ResidencyTier, bytes: usize) !PreparedGrowth {
        return self.snapshotGrowthClassLocked(tier, bytes, bytes);
    }
    /// A mapped lazy load has its encoded source covered by the original
    /// resident floor, plus disjoint newly allocated prepared representations.
    /// One physical recipe/commit preserves the exact original LRU geometry.
    pub fn snapshotMappedGrowthLocked(self: *SharedCache, bytes: usize, prepared_bytes: usize) !PreparedGrowth {
        return self.snapshotGrowthClassLocked(.host, bytes, prepared_bytes);
    }
    fn snapshotGrowthClassLocked(self: *SharedCache, tier: ResidencyTier, bytes: usize, independent_bytes: usize) !PreparedGrowth {
        return self.snapshotGrowthWithLogicalBytesLocked(tier, bytes, independent_bytes, bytes);
    }
    /// SAME retention policy bytes; independently declared owner metadata is
    /// additionally backed by the actual Domain, never borrowed from a floor.
    pub fn snapshotPhysicalOwnerGrowthLocked(self: *SharedCache, logical_bytes: usize, physical_bytes: usize) !PreparedGrowth {
        return self.snapshotPhysicalOwnerGrowthForTierLocked(.host, logical_bytes, physical_bytes);
    }
    /// Independently charged physical packed/cache representations. Backend
    /// roots retain their exact ledger just as Native host components do.
    pub fn snapshotPhysicalOwnerGrowthForTierLocked(self: *SharedCache, tier: ResidencyTier, logical_bytes: usize, physical_bytes: usize) !PreparedGrowth {
        return self.snapshotGrowthWithLogicalBytesLocked(tier, physical_bytes, physical_bytes, logical_bytes);
    }
    fn snapshotGrowthWithLogicalBytesLocked(self: *SharedCache, tier: ResidencyTier, bytes: usize, independent_bytes: usize, logical_bytes: usize) !PreparedGrowth {
        if (tier == .disk or bytes == 0 or independent_bytes > bytes or logical_bytes > bytes) return error.InvalidPreparedCacheGrowth;
        if (!self.canFitAdditional(tier, logical_bytes)) return error.MemoryBudgetExceeded;
        var result = PreparedGrowth{ .cache = self, .binding = self.admission, .metadata_allocator = if (self.admission) |binding| binding.allocator else null, .tier = tier, .bytes = bytes, .logical_bytes = logical_bytes, .independent = independent_bytes == bytes, .independent_bytes = independent_bytes, .expected_host = self.host_bytes, .expected_backend = self.backend_bytes };
        const binding = self.admission orelse return result;
        binding.lock();
        defer binding.mutex.unlock();
        if (binding.flush_in_flight or binding.revision >= std.math.maxInt(u64) - 1) return error.ResourceTemporarilyUnavailable;
        const next = std.math.add(usize, binding.actualPtr(tier).?.*, bytes) catch return error.MemoryBudgetExceeded;
        result.expected_revision = binding.revision;
        const next_independent = try std.math.add(usize, binding.independentPtr(tier).?.*, independent_bytes);
        result.credit_bytes = binding.desiredCredit(tier, next, next_independent) -| binding.creditedPtr(tier).?.*;
        result.replacement_required = if (binding.credits.items.len == binding.credits.capacity) binding.credits.items.len + 1 else 0;
        return result;
    }

    /// Reclassify one exact already-live physical component: original packed
    /// payload/key bytes remain resident, and only its previously uncounted
    /// owner header grows logical capacity. The actual caller revalidates the
    /// SAME unique owner under its cache lock before final commit/attachment.
    pub fn snapshotExistingPhysicalComponentLocked(self: *SharedCache, old_bytes: usize, metadata_bytes: usize) !PreparedGrowth {
        const physical = try std.math.add(usize, old_bytes, metadata_bytes);
        if (old_bytes == 0 or metadata_bytes == 0 or old_bytes > self.host_bytes) return error.InvalidPreparedCacheGrowth;
        const binding = self.admission orelse return error.MissingCacheRetirementDomain;
        binding.lock();
        defer binding.mutex.unlock();
        if (binding.flush_in_flight or binding.final_queued or binding.revision >= std.math.maxInt(u64) - 1) return error.ResourceTemporarilyUnavailable;
        const next = try std.math.add(usize, binding.host_bytes, metadata_bytes);
        const next_independent = try std.math.add(usize, binding.independent_host_bytes, physical);
        if (next_independent > next) return error.InvalidPreparedCacheGrowth;
        return .{ .cache = self, .binding = binding, .metadata_allocator = binding.allocator, .tier = .host, .bytes = metadata_bytes, .logical_bytes = 0, .independent = true, .independent_bytes = physical, .physical_component_bytes = physical, .expected_host = self.host_bytes, .expected_backend = self.backend_bytes, .expected_revision = binding.revision, .credit_bytes = binding.desiredCredit(.host, next, next_independent) -| binding.host_credited_bytes, .replacement_required = if (binding.credits.items.len == binding.credits.capacity) binding.credits.items.len + 1 else 0 };
    }

    /// Query the serving owner's current physical host-pressure signal. This
    /// is deliberately independent of cache geometry: mmap page faults can
    /// increase cgroup working set without changing `host_bytes`.
    pub fn isLiveHostUnderPressure(self: *const SharedCache) bool {
        const binding = self.admission orelse return false;
        return binding.controller.isLiveHostUnderPressure();
    }

    pub fn noteResident(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        // Serving sessions use tryReserve so process ownership is established
        // before memory grows. Direct/offline runtimes intentionally retain the
        // lightweight counter-only API.
        std.debug.assert(self.admission == null);
        self.noteResidentUnchecked(tier, bytes);
    }

    fn noteResidentUnchecked(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        switch (tier) {
            .disk => {},
            .host => {
                self.host_bytes += bytes;
                self.host_loads += 1;
            },
            .backend => {
                self.backend_bytes += bytes;
                self.backend_loads += 1;
            },
        }
    }

    pub fn notePreparedRelease(self: *SharedCache, tier: ResidencyTier, bytes: usize, independent: usize) void {
        self.notePreparedReleaseWithLogical(tier, bytes, bytes, independent);
    }
    /// Exact physical data/key/map death may exceed the unchanged cache-policy
    /// payload. It grants no credit; reductions settle only outside all gates.
    pub fn notePreparedReleaseWithLogical(self: *SharedCache, tier: ResidencyTier, logical: usize, physical: usize, independent: usize) void {
        std.debug.assert(tier != .disk and logical <= physical);
        if (self.admission) |binding| binding.releasePrepared(tier, physical, independent);
        switch (tier) {
            .host => {
                self.host_bytes -= logical;
                if (logical != 0) self.host_evictions +|= 1;
            },
            .backend => {
                self.backend_bytes -= logical;
                if (logical != 0) self.backend_evictions +|= 1;
            },
            .disk => unreachable,
        }
    }

    pub fn releasedCreditPending(self: *SharedCache) bool {
        const binding = self.admission orelse return false;
        binding.lock();
        defer binding.mutex.unlock();
        return binding.host_credited_bytes > binding.desiredCredit(.host, binding.host_bytes, binding.independent_host_bytes) or
            binding.backend_credited_bytes > binding.desiredCredit(.backend, binding.backend_bytes, binding.independent_backend_bytes);
    }

    pub fn deferPhysicalReleases(self: *SharedCache) void {
        const binding = self.admission orelse return;
        binding.lock();
        binding.deferred_releases = true;
        binding.mutex.unlock();
    }

    pub fn flushReleasedOutsideExecution(self: *SharedCache) !void {
        if (self.admission) |binding| try binding.flushOutsideExecution();
    }

    /// Logical cache removal of an independently retained physical component.
    /// Its exact PhysicalBacking performs the ledger decrement at actual death.
    pub fn noteLogicalPhysicalOwnerRemoval(self: *SharedCache, bytes: usize) void {
        self.noteLogicalPhysicalOwnerRemovalForTier(.host, bytes);
    }
    pub fn noteLogicalPhysicalOwnerRemovalForTier(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        switch (tier) {
            .host => {
                std.debug.assert(bytes <= self.host_bytes);
                self.host_bytes -= bytes;
                self.host_evictions +|= 1;
            },
            .backend => {
                std.debug.assert(bytes <= self.backend_bytes);
                self.backend_bytes -= bytes;
                self.backend_evictions +|= 1;
            },
            .disk => unreachable,
        }
    }

    pub fn noteRelease(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        if (self.admission) |binding| binding.release(tier, bytes);
        switch (tier) {
            .disk => {},
            .host => {
                self.host_bytes -|= bytes;
                self.host_evictions += 1;
            },
            .backend => {
                self.backend_bytes -|= bytes;
                self.backend_evictions += 1;
            },
        }
    }

    /// Existing actual growth tokens gain pure retirement metadata; no new
    /// quota, baseline credit or payload ownership is minted. Synchronous only.
    pub const PreparedCreditUpgrades = struct {
        binding: ?*AdmissionBinding = null,
        revision: u64 = 0,
        entries: []Entry = &.{},
        const Entry = struct {
            index: usize,
            expected: memory.AdmissionLease,
            shell: retirement.PreparedCharge = .{ .owner = null },
            pin: ?domain.Pin = null,
        };
        pub fn installLocked(self: *@This(), cache: *SharedCache) !void {
            if (cache.admission != self.binding) return error.PreparedCacheGrowthChanged;
            const binding = self.binding orelse return;
            binding.lock();
            defer binding.mutex.unlock();
            if (binding.flush_in_flight or binding.revision >= std.math.maxInt(u64) - 1 or binding.revision != self.revision) return error.PreparedCacheGrowthChanged;
            // Whole set prevalidation before any metadata transfer.
            for (self.entries) |entry| {
                if (entry.index >= binding.credits.items.len) return error.PreparedCacheGrowthChanged;
                const actual = &binding.credits.items[entry.index];
                if (!std.meta.eql(actual.lease, entry.expected) or actual.shell.owner != null or actual.pin != null or entry.shell.owner == null or entry.pin == null or entry.pin.?.controller() != actual.lease.controller) return error.PreparedCacheGrowthChanged;
            }
            for (self.entries) |*entry| {
                const actual = &binding.credits.items[entry.index];
                actual.shell = entry.shell;
                actual.pin = entry.pin;
                entry.shell = .{ .owner = null };
                entry.pin = null;
            }
            if (self.entries.len != 0) {
                binding.deferred_releases = true;
                binding.revision +|= 1;
            }
        }
        pub fn finishOutsideExecution(self: *@This()) void {
            for (self.entries) |*entry| {
                entry.shell.destroyUnused();
                if (entry.pin) |pin| pin.release();
            }
            if (self.binding) |binding| binding.allocator.free(self.entries);
            self.entries = &.{};
        }
    };
    pub fn rawCreditsNeedPreparation(self: *SharedCache) bool {
        const binding = self.admission orelse return false;
        binding.lock();
        defer binding.mutex.unlock();
        // The SAME complete ledger already owns pure final metadata/oldDomain;
        // do not allocate redundant per-token upgrades on this warm path.
        if (binding.pure_final_metadata and binding.final_domain != null) return false;
        for (binding.credits.items) |credit| if (credit.shell.owner == null) return true;
        return false;
    }
    /// Outside Model/prefetch/quantized ownership. Mutable token identities
    /// are copied under the ACTUAL binding mutex, never under Model pin alone.
    pub fn prepareCreditUpgradesOutsideExecution(self: *SharedCache, old_domain: *const domain.Pin) !PreparedCreditUpgrades {
        const binding = self.admission orelse return .{};
        if (old_domain.controller() != binding.controller) return error.ForeignAdmissionDomain;
        binding.lock();
        if (binding.flush_in_flight or binding.revision >= std.math.maxInt(u64) - 1) {
            binding.mutex.unlock();
            return error.PreparedCacheGrowthChanged;
        }
        if (binding.pure_final_metadata and binding.final_domain != null) {
            const revision = binding.revision;
            binding.mutex.unlock();
            return .{ .binding = binding, .revision = revision };
        }
        var count: usize = 0;
        for (binding.credits.items) |credit| if (credit.shell.owner == null) {
            count += 1;
        };
        const revision = binding.revision;
        binding.mutex.unlock();
        if (count == 0) return .{ .binding = binding, .revision = revision };
        const entries = try binding.allocator.alloc(PreparedCreditUpgrades.Entry, count);
        var result = PreparedCreditUpgrades{ .binding = binding, .revision = revision, .entries = entries };
        // Null initialize even when a changed ledger refuses before snapshot.
        for (entries) |*entry| entry.* = .{ .index = 0, .expected = undefined };
        errdefer result.finishOutsideExecution();
        binding.lock();
        if (binding.flush_in_flight or binding.revision != revision) {
            binding.mutex.unlock();
            return error.PreparedCacheGrowthChanged;
        }
        var index: usize = 0;
        for (binding.credits.items, 0..) |credit, slot| if (credit.shell.owner == null) {
            entries[index] = .{ .index = slot, .expected = credit.lease };
            index += 1;
        };
        binding.mutex.unlock();
        for (entries) |*entry| {
            if (entry.expected.controller != old_domain.controller()) return error.ForeignAdmissionDomain;
            entry.shell = try retirement.PreparedCharge.prepare(0);
            entry.pin = old_domain.retain();
        }
        return result;
    }

    /// Preflight BEFORE any physical CPU cache death. A typed final owner
    /// never converts old raw provenance into fictitious outside authority.
    pub fn validateCleanup(self: *SharedCache, current: Cleanup) !void {
        const binding = self.admission orelse return;
        binding.lock();
        defer binding.mutex.unlock();
        if (binding.flush_in_flight or binding.final_queued) return error.PreparedCacheGrowthChanged;
        if (!current.outside_execution and current.boundary == null and binding.final_domain != null) return error.CacheRetirementCleanupAuthorityRequired;
        if (current.boundary) |epoch| {
            if (try epoch.isSealed()) return error.RetirementEpochSealed;
            if (binding.pure_final_metadata) if (binding.final_domain) |pin| {
                if (pin.controller() != binding.controller) return error.ForeignAdmissionDomain;
                for (binding.credits.items) |credit| if (credit.lease.controller != pin.controller()) return error.ForeignAdmissionDomain;
                return;
            };
        }
        for (binding.credits.items) |*credit| try credit.validateCleanup(current);
    }
    /// The actual cache owner proved ALL backing dead. Detach metadata under
    /// its lock; only pure Charge/Domain/token can outlive this binding/Model.
    pub fn deinitAdmissionWithCleanup(self: *SharedCache, current: Cleanup) !void {
        try self.validateCleanup(current);
        const binding = self.admission orelse return;
        if (binding.pure_final_metadata and binding.final_domain != null) {
            if (current.outside_execution) {
                binding.markFinalBackingDead();
                binding.lock();
                binding.final_queued = true;
                binding.mutex.unlock();
                self.admission = null;
                self.host_bytes = 0;
                self.backend_bytes = 0;
                binding.flushOutsideExecution() catch {};
                binding.releasePhysicalReference();
                return;
            }
            const epoch = current.boundary orelse return error.CacheRetirementCleanupAuthorityRequired;
            const boundary = try epoch.boundary();
            const held = try epoch.retain();
            binding.markFinalBackingDead();
            binding.lock();
            binding.final_epoch = held;
            binding.final_node = .{ .context = binding, .release_fn = AdmissionBinding.finalDrop };
            binding.final_queued = true;
            binding.mutex.unlock();
            self.admission = null;
            self.host_bytes = 0;
            self.backend_bytes = 0;
            boundary.register(boundary.context, boundary.nonce, &binding.final_node) catch platform.inference_process_supervisor.restartWorker();
            // Queue may synchronously consume binding. Never read it again.
            return;
        }
        binding.lock();
        var credits = binding.credits;
        binding.credits = .empty;
        binding.host_bytes = 0;
        binding.backend_bytes = 0;
        binding.host_credited_bytes = 0;
        binding.backend_credited_bytes = 0;
        binding.independent_host_bytes = 0;
        binding.independent_backend_bytes = 0;
        binding.revision +|= 1;
        binding.mutex.unlock();
        self.admission = null;
        self.host_bytes = 0;
        self.backend_bytes = 0;
        for (credits.items) |*credit| credit.retireAfterBackingDeath(current);
        credits.deinit(binding.allocator);
        binding.deinit();
    }

    /// Release the binding after every physical cache allocation has been
    /// destroyed. Ordinary eviction reports exact releases incrementally;
    /// session teardown drains any entries that remained resident at close.
    pub fn deinitAdmission(self: *SharedCache) void {
        self.deinitAdmissionWithCleanup(.{ .outside_execution = true }) catch platform.inference_process_supervisor.restartWorker();
    }

    /// Admission credits owned by physical cache bytes above the session's
    /// resident model baseline. The binding lock makes this an exact ownership
    /// snapshot suitable for idle-domain conservation checks.
    pub fn admissionSnapshot(self: *SharedCache) ?AdmissionSnapshot {
        const binding = self.admission orelse return null;
        binding.lock();
        defer binding.mutex.unlock();
        return .{ .identity = @intFromPtr(binding), .amounts = .{
            .host_weight_bytes = binding.host_credited_bytes + binding.metadata_host_bytes,
            .backend_weight_bytes = binding.backend_credited_bytes,
        } };
    }
    pub fn admissionAmounts(self: *SharedCache) memory.AdmissionAmounts {
        return if (self.admissionSnapshot()) |value| value.amounts else .{};
    }

    pub fn isOverBudget(self: *const SharedCache, tier: ResidencyTier) bool {
        return switch (tier) {
            .disk => false,
            .host => self.budget.host_limit_bytes != 0 and self.host_bytes > self.budget.host_limit_bytes,
            .backend => self.budget.backend_limit_bytes != 0 and self.backend_bytes > self.budget.backend_limit_bytes,
        };
    }

    pub fn canFitAdditional(self: *const SharedCache, tier: ResidencyTier, bytes: usize) bool {
        return switch (tier) {
            .disk => true,
            .host => fitsAdditional(self.host_bytes, bytes, self.budget.host_limit_bytes),
            .backend => fitsAdditional(self.backend_bytes, bytes, self.budget.backend_limit_bytes),
        };
    }

    pub fn noteDenied(self: *SharedCache, tier: ResidencyTier, bytes: usize) void {
        if (tier == .disk) return;
        const current_bytes, const limit_bytes = switch (tier) {
            .disk => unreachable,
            .host => .{ self.host_bytes, self.budget.host_limit_bytes },
            .backend => .{ self.backend_bytes, self.budget.backend_limit_bytes },
        };
        self.denials += 1;
        self.last_denial = .{
            .tier = tier,
            .current_bytes = current_bytes,
            .request_bytes = bytes,
            .requested_total_bytes = std.math.add(usize, current_bytes, bytes) catch
                std.math.maxInt(usize),
            .limit_bytes = limit_bytes,
        };
    }

    pub fn lastDenialString(self: *const SharedCache, buf: []u8) ![]const u8 {
        const denial = self.last_denial orelse {
            return std.fmt.bufPrint(buf, "shared tier cache memory budget exceeded", .{});
        };
        return std.fmt.bufPrint(
            buf,
            "shared tier cache budget exceeded: tier={s} current={d} request={d} next={d} limit={d}",
            .{
                @tagName(denial.tier),
                denial.current_bytes,
                denial.request_bytes,
                denial.requested_total_bytes,
                denial.limit_bytes,
            },
        );
    }

    pub fn widenToAtLeast(self: *SharedCache, floor: Budget) void {
        self.budget.host_limit_bytes = boundedFloor(
            self.budget.host_limit_bytes,
            floor.host_limit_bytes,
            if (self.hard_budget) |hard| hard.host_limit_bytes else 0,
        );
        self.budget.backend_limit_bytes = boundedFloor(
            self.budget.backend_limit_bytes,
            floor.backend_limit_bytes,
            if (self.hard_budget) |hard| hard.backend_limit_bytes else 0,
        );
    }
};

fn fitsAdditional(current: usize, additional: usize, limit: usize) bool {
    const next = std.math.add(usize, current, additional) catch return false;
    return limit == 0 or next <= limit;
}

fn boundedFloor(current: usize, floor: usize, hard_limit: usize) usize {
    const widened = @max(current, floor);
    return if (hard_limit == 0) widened else @min(widened, hard_limit);
}

pub fn defaultBudgetForBackend(backend: planner.BackendClass) Budget {
    const limits = memory.defaultLimitsForBackend(backend);
    return .{
        .host_limit_bytes = limits.host_limit_bytes,
        .backend_limit_bytes = limits.backend_limit_bytes,
    };
}

test "shared cache tracks host and backend budgets" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 50,
    });

    cache.noteResident(.host, 70);
    cache.noteResident(.backend, 30);
    try std.testing.expect(!cache.isOverBudget(.host));
    try std.testing.expect(!cache.isOverBudget(.backend));

    cache.noteResident(.host, 40);
    cache.noteResident(.backend, 25);
    try std.testing.expect(cache.isOverBudget(.host));
    try std.testing.expect(cache.isOverBudget(.backend));

    cache.noteRelease(.host, 40);
    cache.noteRelease(.backend, 25);
    try std.testing.expect(!cache.isOverBudget(.host));
    try std.testing.expect(!cache.isOverBudget(.backend));
}

test "shared cache records denial details" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 100,
        .backend_limit_bytes = 50,
    });
    cache.noteResident(.host, 90);
    cache.noteDenied(.host, 20);

    var buf: [256]u8 = undefined;
    const msg = try cache.lastDenialString(&buf);
    try std.testing.expect(std.mem.indexOf(u8, msg, "tier=host") != null);
    try std.testing.expect(std.mem.indexOf(u8, msg, "request=20") != null);
}

test "hard cache budget bounds architecture floors" {
    var cache = SharedCache.init(.{
        .host_limit_bytes = 1024,
        .backend_limit_bytes = 2048,
    });
    cache.configureHardBudget(.{
        .host_limit_bytes = 512,
        .backend_limit_bytes = 768,
    });
    cache.widenToAtLeast(.{
        .host_limit_bytes = 4096,
        .backend_limit_bytes = 8192,
    });
    try std.testing.expectEqual(@as(usize, 512), cache.budget.host_limit_bytes);
    try std.testing.expectEqual(@as(usize, 768), cache.budget.backend_limit_bytes);
}

test "serving cache growth leases only bytes beyond model baseline" {
    var controller = memory.AdmissionController{};
    controller.configureSharedLimits(.{ .host_limit_bytes = 120 });
    defer controller.deinit();

    const limits = memory.Limits{
        .host_limit_bytes = 120,
        .combined_limit_bytes = 120,
    };
    const resident = memory.AdmissionAmounts{ .host_weight_bytes = 100 };
    var model_lease = try controller.tryAcquire(.cpu, limits, resident, false);
    defer model_lease.release();
    try model_lease.retain(resident);

    var cache = SharedCache.init(.{ .host_limit_bytes = 1024 });
    try cache.configureAdmission(
        std.testing.allocator,
        &controller,
        .cpu,
        limits,
        .{ .host_limit_bytes = 100 },
    );
    errdefer cache.deinitAdmission();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, cache.admissionAmounts());

    try cache.reserve(.host, 80);
    try std.testing.expectEqual(@as(usize, 100), controller.snapshot().host_weight_bytes);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, cache.admissionAmounts());

    try cache.reserve(.host, 30);
    try std.testing.expectEqual(@as(usize, 110), controller.snapshot().host_weight_bytes);
    try std.testing.expectEqual(memory.AdmissionAmounts{ .host_weight_bytes = 10 }, cache.admissionAmounts());

    // The request fits the configured policy in isolation, but not alongside
    // the currently resident model and cache credit, so callers may retry it
    // after eviction or another lease release.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.reserve(.host, 20));
    try std.testing.expectEqual(@as(usize, 110), cache.host_bytes);
    try std.testing.expectEqual(@as(usize, 110), controller.snapshot().host_weight_bytes);
    try std.testing.expectEqual(memory.AdmissionAmounts{ .host_weight_bytes = 10 }, cache.admissionAmounts());

    cache.noteRelease(.host, 30);
    try std.testing.expectEqual(@as(usize, 80), cache.host_bytes);
    try std.testing.expectEqual(@as(usize, 100), controller.snapshot().host_weight_bytes);
    try std.testing.expectEqual(memory.AdmissionAmounts{}, cache.admissionAmounts());
    cache.noteRelease(.host, 80);
    cache.deinitAdmission();
    try std.testing.expectEqual(memory.AdmissionAmounts{}, cache.admissionAmounts());
}

test "serving cache preserves retryable live-pressure denial" {
    var controller = memory.AdmissionController{};
    controller.configureSharedLimits(.{ .host_limit_bytes = 1024 });
    // Two bytes is deliberately tiny but still leaves a non-zero safety
    // reserve on every supported live-memory policy (including macOS).
    controller.configureProcessMemoryLimit(2, .explicit);
    defer controller.deinit();

    const limits = memory.Limits{
        .host_limit_bytes = 1024,
        .combined_limit_bytes = 1024,
    };
    var cache = SharedCache.init(.{ .host_limit_bytes = 1024 });
    try cache.configureAdmission(
        std.testing.allocator,
        &controller,
        .cpu,
        limits,
        .{},
    );
    defer cache.deinitAdmission();

    try std.testing.expectError(
        error.ResourceTemporarilyUnavailable,
        cache.reserve(.host, 2),
    );
    try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
    try std.testing.expectEqual(
        memory.AdmissionAmounts{},
        controller.snapshot(),
    );
}

test "Native selected tier growth funds outside actual locks commits same token and retires after backing at current B" {
    const Sink = @import("../../execution/retirement_sink.zig");
    const a = std.testing.allocator;
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var model: std.atomic.Mutex = .unlocked;
    var prefetch: std.atomic.Mutex = .unlocked;
    var quantized: std.atomic.Mutex = .unlocked;
    const Probe = struct {
        locks: [3]*std.atomic.Mutex,
        cache: *SharedCache,
        violation: bool = false,
        reserves: usize = 0,
        releases: usize = 0,
        fn observe(self: *@This()) void {
            for (self.locks) |lock| {
                if (lock.tryLock()) lock.unlock() else self.violation = true;
            }
            if (self.cache.admission) |binding| {
                if (binding.mutex.tryLock()) binding.mutex.unlock() else self.violation = true;
            }
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.reserves += 1;
            return self.reserves;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.observe();
            self.releases += 1;
        }
    };
    var cache = SharedCache.init(.{ .host_limit_bytes = 64 });
    defer cache.deinitAdmission();
    try cache.configureAdmission(a, pin.controller(), .cpu, .{ .host_limit_bytes = 64 }, .{});
    var probe: Probe = .{ .locks = .{ &model, &prefetch, &quantized }, .cache = &cache };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    try std.testing.expect(model.tryLock() and prefetch.tryLock() and quantized.tryLock());
    var prepared = try cache.snapshotGrowthLocked(.host, 64);
    quantized.unlock();
    prefetch.unlock();
    model.unlock();
    defer prepared.finishOutsideExecution();
    try prepared.fundOutsideExecution(&pin);
    const token = prepared.lease.?.external_lease;
    try std.testing.expectEqual(@as(usize, 1), probe.reserves);
    const payload = try a.alloc(u8, 64); // SAME declaration precedes actual birth
    var payload_live = true;
    defer if (payload_live) a.free(payload);
    try std.testing.expect(model.tryLock() and prefetch.tryLock() and quantized.tryLock());
    try prepared.commit();
    try std.testing.expectEqual(token, cache.admission.?.credits.items[0].lease.external_lease);
    try std.testing.expectEqual(@as(usize, 64), cache.host_bytes);
    quantized.unlock();
    prefetch.unlock();
    model.unlock();
    prepared.finishOutsideExecution();
    var sink = try Sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try std.testing.expect(model.tryLock() and prefetch.tryLock() and quantized.tryLock());
    try cache.validateCleanup(.{ .boundary = &epoch }); // before backing mutation
    a.free(payload);
    payload_live = false;
    try cache.deinitAdmissionWithCleanup(.{ .boundary = &epoch });
    try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 0), probe.releases);
    quantized.unlock();
    prefetch.unlock();
    model.unlock();
    guard.finish();
    try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expect(!probe.violation);
}

test "Native selected tier growth changed ledger or failed unpublished payload never refunds under locks" {
    const a = std.testing.allocator;
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var cache = SharedCache.init(.{ .host_limit_bytes = 128 });
    defer cache.deinitAdmission();
    try cache.configureAdmission(a, pin.controller(), .cpu, .{ .host_limit_bytes = 128 }, .{});
    {
        var first = try cache.snapshotGrowthLocked(.host, 64);
        defer first.finishOutsideExecution();
        try first.fundOutsideExecution(&pin);
        try cache.reserve(.host, 1); // legitimate different cold peer mutation
        try std.testing.expectError(error.PreparedCacheGrowthChanged, first.commit());
        try std.testing.expectEqual(@as(usize, 1), cache.host_bytes);
    }
    try std.testing.expectEqual(@as(usize, 1), pin.controller().snapshot().host_weight_bytes);
    cache.noteRelease(.host, 1);
    var prepared = try cache.snapshotGrowthLocked(.host, 64);
    defer prepared.finishOutsideExecution();
    try prepared.fundOutsideExecution(&pin);
    try prepared.commit();
    try prepared.rollbackUnpublishedLocked(); // original constructor failed atomic
    try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
    try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_weight_bytes);
    prepared.finishOutsideExecution();
    try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
}

test "Native owned prepared representations cannot borrow original resident source baseline twice" {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var cache = SharedCache.init(.{ .host_limit_bytes = 128 });
    defer cache.deinitAdmission();
    try cache.configureAdmission(std.testing.allocator, pin.controller(), .cpu, .{ .host_limit_bytes = 192 }, .{ .host_limit_bytes = 128 });
    var model = try pin.controller().tryAcquire(.cpu, .{ .host_limit_bytes = 192 }, .{ .host_weight_bytes = 128 }, false);
    defer model.release();
    var prepared = try cache.snapshotIndependentGrowthLocked(.host, 64);
    defer prepared.finishOutsideExecution();
    try std.testing.expectEqual(@as(usize, 64), prepared.credit_bytes);
    try prepared.fundOutsideExecution(&pin);
    try prepared.commit();
    try std.testing.expectEqual(@as(usize, 192), pin.controller().snapshot().host_weight_bytes);
    // Ordinary borrowed/mapped source still uses its original baseline.
    var source = try cache.snapshotGrowthLocked(.host, 16);
    defer source.finishOutsideExecution();
    try std.testing.expectEqual(@as(usize, 0), source.credit_bytes);
    try source.fundOutsideExecution(&pin);
    try source.commit();
    try std.testing.expectEqual(@as(usize, 192), pin.controller().snapshot().host_weight_bytes);
}

test "Native deferred physical eviction settles same token outside locks and refused shrink preserves custody" {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var cache = SharedCache.init(.{ .host_limit_bytes = 64 });
    defer cache.deinitAdmission();
    try cache.configureAdmission(std.testing.allocator, pin.controller(), .cpu, .{ .host_limit_bytes = 64 }, .{});
    var model: std.atomic.Mutex = .unlocked;
    var prefetch: std.atomic.Mutex = .unlocked;
    const Probe = struct {
        cache: *SharedCache,
        locks: [2]*std.atomic.Mutex,
        fail_shrink: bool = false,
        reenter: bool = false,
        violation: bool = false,
        reserves: usize = 0,
        retains: usize = 0,
        releases: usize = 0,
        fn inspect(self: *@This()) void {
            for (self.locks) |mutex| if (mutex.tryLock()) mutex.unlock() else {
                self.violation = true;
            };
            if (self.cache.admission.?.mutex.tryLock()) self.cache.admission.?.mutex.unlock() else self.violation = true;
            if (self.reenter) {
                self.cache.reserve(.host, 1) catch |err| {
                    if (err != error.ResourceTemporarilyUnavailable) self.violation = true;
                    return;
                };
                self.violation = true;
            }
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            self.reserves += 1;
            return self.reserves;
        }
        fn retain(raw: *anyopaque, _: usize, amounts: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            self.retains += 1;
            if (self.fail_shrink and amounts.host_weight_bytes < 64) return error.ResourceTemporarilyUnavailable;
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.inspect();
            self.releases += 1;
        }
    };
    var probe = Probe{ .cache = &cache, .locks = .{ &model, &prefetch } };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var initial = try cache.snapshotIndependentGrowthLocked(.host, 64);
    defer initial.finishOutsideExecution();
    try initial.fundOutsideExecution(&pin);
    try initial.commit();
    const token = cache.admission.?.credits.items[0].lease.external_lease;
    const callbacks = probe.retains + probe.releases;
    try std.testing.expect(model.tryLock() and prefetch.tryLock());
    cache.notePreparedRelease(.host, 16, 16); // actual CPU payload already dead
    try std.testing.expectEqual(callbacks, probe.retains + probe.releases);
    try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_weight_bytes);
    prefetch.unlock();
    model.unlock();
    probe.fail_shrink = true;
    probe.reenter = true;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.flushReleasedOutsideExecution());
    try std.testing.expectEqual(token, cache.admission.?.credits.items[0].lease.external_lease);
    try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_weight_bytes);
    try std.testing.expectEqual(@as(usize, 48), cache.host_bytes);
    probe.fail_shrink = false;
    try cache.flushReleasedOutsideExecution();
    try std.testing.expectEqual(token, cache.admission.?.credits.items[0].lease.external_lease);
    try std.testing.expectEqual(@as(usize, 48), pin.controller().snapshot().host_weight_bytes);
    try std.testing.expect(model.tryLock() and prefetch.tryLock());
    cache.notePreparedRelease(.host, 48, 48);
    try std.testing.expectEqual(@as(usize, 0), probe.releases);
    prefetch.unlock();
    model.unlock();
    try cache.flushReleasedOutsideExecution();
    try std.testing.expectEqual(@as(usize, 1), probe.reserves);
    try std.testing.expectEqual(@as(usize, 1), probe.releases);
    try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
    try std.testing.expect(!probe.violation);
}

test "Native complete existing binding keeps raw oldDomain tokens until actual foreign B after backing death" {
    const sink_mod = @import("../../execution/retirement_sink.zig");
    for ([_]bool{ false, true }) |seal_old_first| {
        const old = try domain.Domain.create();
        defer old.release();
        const pin = old.pin();
        defer pin.release();
        const foreign = try domain.Domain.create();
        defer foreign.release();
        const foreign_pin = foreign.pin();
        defer foreign_pin.release();
        var cache = SharedCache.init(.{ .host_limit_bytes = 64 });
        defer cache.deinitAdmission();
        try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .cpu, .{ .host_limit_bytes = 64 }, .{});
        try std.testing.expectError(error.ForeignAdmissionDomain, cache.installRetirementDomainOutsideExecution(&foreign_pin));
        try std.testing.expect(cache.admission.?.final_domain == null);
        try cache.installRetirementDomainOutsideExecution(&pin);
        try cache.installRetirementDomainOutsideExecution(&pin); // idempotent exact owner
        const retained = old.references.load(.acquire);
        try cache.installRetirementDomainOutsideExecution(&pin);
        try std.testing.expectEqual(retained, old.references.load(.acquire));
        var model: std.atomic.Mutex = .unlocked;
        const Probe = struct {
            model: *std.atomic.Mutex,
            releases: usize = 0,
            violation: bool = false,
            fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
                return 43;
            }
            fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
            fn release(raw: *anyopaque, token: usize) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                if (token != 43) self.violation = true;
                if (self.model.tryLock()) self.model.unlock() else self.violation = true;
                self.releases += 1;
            }
        };
        var probe = Probe{ .model = &model };
        try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        // Legitimate historical raw growth: no per-token shell or credit is
        // invented. The one actual binding owns this SAME token and oldDomain.
        try cache.reserve(.host, 64);
        try std.testing.expect(cache.admission.?.credits.items[0].shell.owner == null);
        var old_sink = try sink_mod.Ref.create();
        defer old_sink.release();
        var old_guard = try old_sink.enter();
        defer old_guard.finish();
        var old_epoch = try old_guard.retainEpoch();
        defer old_epoch.release();
        var new_sink = try sink_mod.Ref.create();
        defer new_sink.release();
        var new_guard = try new_sink.enter();
        defer new_guard.finish();
        var new_epoch = try new_guard.retainEpoch();
        defer new_epoch.release();
        if (seal_old_first) old_guard.finish();
        try std.testing.expect(model.tryLock());
        try cache.validateCleanup(.{ .boundary = &new_epoch });
        cache.noteRelease(.host, 64); // original populated-cache destructor path
        try std.testing.expectEqual(@as(usize, 0), probe.releases);
        try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_weight_bytes);
        try cache.deinitAdmissionWithCleanup(.{ .boundary = &new_epoch }); // actual backing died immediately before
        try std.testing.expect(cache.admission == null);
        try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_weight_bytes);
        try std.testing.expectEqual(@as(usize, 0), probe.releases);
        if (!seal_old_first) old_guard.finish();
        try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().host_weight_bytes);
        model.unlock();
        new_guard.finish();
        try std.testing.expectEqual(@as(usize, 1), probe.releases);
        try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
        try std.testing.expectEqual(@as(usize, 0), foreign_pin.controller().snapshot().host_weight_bytes);
        try std.testing.expect(!probe.violation);
    }
}

fn nativeBindingOwnerAllocationCase(a: std.mem.Allocator) !void {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var cache = SharedCache.init(.{ .host_limit_bytes = 16 });
    defer cache.deinitAdmission();
    // Test-only injected allocator models the SAME independent metadata
    // allocation; production configureManaged uses the certified process one.
    try cache.configureAdmission(a, pin.controller(), .cpu, .{ .host_limit_bytes = 16 }, .{});
    cache.admission.?.pure_final_metadata = true;
    try cache.installRetirementDomainOutsideExecution(&pin);
    cache.reserve(.host, 16) catch |err| {
        if (err == error.ResourceTemporarilyUnavailable) return error.OutOfMemory;
        return err;
    };
    cache.noteRelease(.host, 16);
}

test "Native binding metadata allocation failure leaves no published ledger or Domain pin" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, nativeBindingOwnerAllocationCase, .{});
}

test "Native physical component preserves original payload cap while backing exact extra metadata" {
    for ([_]usize{ 31, 32 }) |limit| {
        const old = try domain.Domain.create();
        defer old.release();
        const pin = old.pin();
        defer pin.release();
        var cache = SharedCache.init(.{ .host_limit_bytes = 16 });
        defer cache.deinitAdmission();
        try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .cpu, .{ .host_limit_bytes = limit }, .{});
        try cache.installRetirementDomainOutsideExecution(&pin);
        var growth = try cache.snapshotPhysicalOwnerGrowthLocked(16, 32);
        defer growth.finishOutsideExecution();
        if (limit == 31) {
            try std.testing.expectError(error.MemoryBudgetExceeded, growth.fundOutsideExecution(&pin));
            try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
            try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
            continue;
        }
        try growth.fundOutsideExecution(&pin);
        try growth.validatePhysicalBacking();
        try growth.commit();
        const backing = growth.retainCommittedPhysicalBacking();
        try std.testing.expectEqual(@as(usize, 16), cache.host_bytes);
        try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().host_weight_bytes);
        cache.noteLogicalPhysicalOwnerRemoval(16);
        var sink = try @import("../../execution/retirement_sink.zig").Ref.create();
        defer sink.release();
        var guard = try sink.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try cache.deinitAdmissionWithCleanup(.{ .boundary = &epoch });
        guard.finish();
        try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().host_weight_bytes);
        backing.releaseAfterMetadataDeathOutsideExecution();
        try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().host_weight_bytes);
    }
}

test "Native existing Binding revision exhaustion never certifies a recycled raw map recipe" {
    const old = try domain.Domain.create();
    defer old.release();
    const pin = old.pin();
    defer pin.release();
    var cache = SharedCache.init(.{ .host_limit_bytes = 128 });
    defer cache.deinitAdmission();
    try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .cpu, .{ .host_limit_bytes = 128 }, .{});
    try cache.installRetirementDomainOutsideExecution(&pin);
    var stale = try cache.snapshotPhysicalOwnerGrowthLocked(0, 16);
    defer stale.finishOutsideExecution();
    try stale.fundOutsideExecution(&pin);
    // Same real raw reserve/release path, with unchanged final counters.
    cache.admission.?.revision = std.math.maxInt(u64) - 1;
    try cache.reserve(.host, 1);
    cache.noteRelease(.host, 1);
    try std.testing.expectEqual(std.math.maxInt(u64), cache.admission.?.revision);
    try std.testing.expectError(error.PreparedCacheGrowthChanged, stale.commit());
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, cache.snapshotPhysicalOwnerGrowthLocked(0, 16));
}

test "backend physical cache components are independent of source floor and survive actual current B and owner close" {
    for ([_]usize{ 31, 32 }) |limit| {
        const old = try domain.Domain.create();
        defer old.release();
        const pin = old.pin();
        defer pin.release();
        var cache = SharedCache.init(.{ .backend_limit_bytes = 16 });
        defer cache.deinitAdmission();
        // The source floor cannot lend its bytes to this independent packed copy.
        try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, .{ .backend_limit_bytes = limit }, .{ .backend_limit_bytes = 64 });
        try cache.installRetirementDomainOutsideExecution(&pin);
        var growth = try cache.snapshotPhysicalOwnerGrowthForTierLocked(.backend, 16, 32);
        defer growth.finishOutsideExecution();
        if (limit == 31) {
            try std.testing.expectError(error.MemoryBudgetExceeded, growth.fundOutsideExecution(&pin));
            try std.testing.expectEqual(@as(usize, 0), cache.backend_bytes);
            try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().backend_weight_bytes);
            continue;
        }
        try growth.fundOutsideExecution(&pin);
        try growth.validatePhysicalBacking();
        try growth.commit();
        const weight = growth.retainCommittedPhysicalBackingPart(24);
        const bias = growth.retainCommittedPhysicalBackingPart(8);
        cache.noteLogicalPhysicalOwnerRemovalForTier(.backend, 16);
        var sink = try @import("../../execution/retirement_sink.zig").Ref.create();
        defer sink.release();
        var guard = try sink.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try cache.deinitAdmissionWithCleanup(.{ .boundary = &epoch });
        guard.finish();
        try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
        weight.releaseAfterMetadataDeathOutsideExecution();
        // Existing ledger settles after actual final component metadata dies.
        // Never claim a partial callback refund while another component lives.
        try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
        bias.releaseAfterMetadataDeathOutsideExecution();
        try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().backend_weight_bytes);
    }
}

test "two tier physical cache transaction denies either exact cap without publishing the other tier" {
    const metadata = @sizeOf(BindingMetadata) + 2 * (try retirement.PreparedCharge.requestedMetadataBytes(0)) + 2 * @sizeOf(AdmissionCredit);
    const limits = [_]memory.Limits{
        .{ .host_limit_bytes = metadata + 15, .backend_limit_bytes = 32 },
        .{ .host_limit_bytes = metadata + 16, .backend_limit_bytes = 31 },
        .{ .host_limit_bytes = metadata + 16, .backend_limit_bytes = 32, .combined_limit_bytes = metadata + 47 },
        .{ .host_limit_bytes = metadata + 16, .backend_limit_bytes = 32, .combined_limit_bytes = metadata + 48 },
    };
    for (limits, 0..) |limit, index| {
        const owner = try domain.Domain.create();
        defer owner.release();
        const pin = owner.pin();
        defer pin.release();
        var cache = SharedCache.init(.{ .host_limit_bytes = 16, .backend_limit_bytes = 16 });
        defer cache.deinitAdmission();
        try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, limit, .{ .backend_limit_bytes = 64 });
        try cache.installRetirementDomainOutsideExecution(&pin);
        var growth = try cache.snapshotPhysicalGrowthLocked(16, 16, 32);
        defer growth.finishOutsideExecution();
        if (index != limits.len - 1) {
            try std.testing.expectError(error.MemoryBudgetExceeded, growth.fundOutsideExecution(&pin));
            try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
            try std.testing.expectEqual(@as(usize, 0), cache.backend_bytes);
            try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
            continue;
        }
        try growth.fundOutsideExecution(&pin);
        try growth.validatePhysicalBacking();
        try growth.commit();
        try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
        try std.testing.expectEqual(@as(usize, 16), cache.backend_bytes);
        try std.testing.expectEqual(metadata + 16, pin.controller().snapshot().host_weight_bytes);
        try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
        // A failed cold constructor rolls back BOTH exact unpublished changes.
        try growth.rollbackUnpublishedLocked();
        growth.finishOutsideExecution();
        try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
        try std.testing.expectEqual(@as(usize, 0), cache.backend_bytes);
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
        var replacement = try cache.snapshotPhysicalGrowthLocked(16, 16, 32);
        defer replacement.finishOutsideExecution();
        try replacement.fundOutsideExecution(&pin);
        try replacement.commit();
        const backing = replacement.retainCommittedPhysicalBackings();
        cache.noteLogicalPhysicalOwnerRemovalForTier(.backend, 16);
        try cache.deinitAdmissionWithCleanup(.{ .outside_execution = true });
        try std.testing.expectEqual(metadata + 16, pin.controller().snapshot().host_weight_bytes);
        try std.testing.expectEqual(@as(usize, 32), pin.controller().snapshot().backend_weight_bytes);
        for (backing) |part| part.releaseAfterMetadataDeathOutsideExecution();
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
    }
}

test "two tier cache intrinsic shared host and unified caps precede every partial lease" {
    const metadata = @sizeOf(BindingMetadata) + 2 * (try retirement.PreparedCharge.requestedMetadataBytes(0)) + 2 * @sizeOf(AdmissionCredit);
    const shared_limits = [_]memory.SharedAdmissionLimits{
        .{ .host_limit_bytes = metadata + 15 },
        .{ .unified_limit_bytes = metadata + 47 },
    };
    for (shared_limits) |shared| {
        const owner = try domain.Domain.create();
        defer owner.release();
        const pin = owner.pin();
        defer pin.release();
        pin.controller().configureSharedLimits(shared);
        var cache = SharedCache.init(.{ .host_limit_bytes = 16, .backend_limit_bytes = 16 });
        defer cache.deinitAdmission();
        // Per-backend request limits fit exactly; only the independent shared
        // physical owner denies the intrinsic full metadata+HOST+BACKEND sum.
        try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, .{ .host_limit_bytes = metadata + 16, .backend_limit_bytes = 32, .combined_limit_bytes = metadata + 48 }, .{ .backend_limit_bytes = 64 });
        try cache.installRetirementDomainOutsideExecution(&pin);
        const binding = cache.admission.?;
        const revision = binding.revision;
        var growth = try cache.snapshotPhysicalGrowthLocked(16, 16, 32);
        defer growth.finishOutsideExecution();
        try std.testing.expectError(error.MemoryBudgetExceeded, growth.fundOutsideExecution(&pin));
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
        try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
        try std.testing.expectEqual(@as(usize, 0), cache.backend_bytes);
        try std.testing.expectEqual(revision, binding.revision);
        try std.testing.expectEqual(@as(usize, 0), binding.credits.items.len);
        try std.testing.expect(growth.metadata == null and growth.replacement.capacity == 0);
        for (growth.parts) |part| try std.testing.expect(part.lease == null and part.pin == null and part.shell.owner == null);
    }
}

test "two tier cache late backend contention unwinds earlier host funding and preserves old cache" {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    const limits = memory.Limits{ .backend_limit_bytes = 40 };
    var cache = SharedCache.init(.{ .backend_limit_bytes = 64 });
    defer cache.deinitAdmission();
    try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, limits, .{ .backend_limit_bytes = 64 });
    try cache.installRetirementDomainOutsideExecution(&pin);
    // A genuine existing physical component/credit must survive both the
    // denied transaction and a subsequent successful commit/rollback.
    var seed = try cache.snapshotPhysicalGrowthLocked(8, 8, 8);
    defer seed.finishOutsideExecution();
    try seed.fundOutsideExecution(&pin);
    try seed.commit();
    var seed_backings: ?[2]PhysicalBacking = seed.retainCommittedPhysicalBackings();
    defer if (seed_backings) |parts| for (parts) |part| part.releaseAfterMetadataDeathOutsideExecution();
    seed.finishOutsideExecution();
    const existing = pin.controller().snapshot();
    const binding = cache.admission.?;
    const old_revision = binding.revision;
    const old_credit_count = binding.credits.items.len;
    const old_credit_pointer = binding.credits.items.ptr;
    const old_metadata = binding.metadata_host_bytes;
    var external = try pin.controller().tryAcquire(.gpu, limits, .{ .backend_weight_bytes = 1 }, true);
    var external_live = true;
    defer if (external_live) external.release();
    const occupied = pin.controller().snapshot();
    var refused = try cache.snapshotPhysicalGrowthLocked(16, 16, 32);
    defer refused.finishOutsideExecution();
    // The whole request fits alone. Earlier HOST metadata/payload funding
    // succeeds, then the independently occupied BACKEND denies the last piece.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, refused.fundOutsideExecution(&pin));
    try std.testing.expectEqualDeep(occupied, pin.controller().snapshot());
    try std.testing.expectEqual(@as(usize, 0), cache.host_bytes);
    try std.testing.expectEqual(@as(usize, 8), cache.backend_bytes);
    try std.testing.expectEqual(old_revision, binding.revision);
    try std.testing.expectEqual(old_credit_count, binding.credits.items.len);
    try std.testing.expect(old_credit_pointer == binding.credits.items.ptr);
    try std.testing.expectEqual(old_metadata, binding.metadata_host_bytes);
    try std.testing.expectEqual(@as(usize, 8), binding.independent_host_bytes);
    try std.testing.expectEqual(@as(usize, 8), binding.independent_backend_bytes);
    try std.testing.expect(refused.metadata == null and refused.replacement.capacity == 0);
    for (refused.parts) |part| {
        try std.testing.expect(part.lease == null and part.pin == null and part.shell.owner == null);
    }
    external.release();
    external_live = false;
    try std.testing.expectEqualDeep(existing, pin.controller().snapshot());
    var retry = try cache.snapshotPhysicalGrowthLocked(16, 16, 32);
    defer retry.finishOutsideExecution();
    try retry.fundOutsideExecution(&pin);
    try retry.commit();
    try std.testing.expectEqual(@as(usize, 24), cache.backend_bytes);
    try std.testing.expectEqual(@as(usize, 40), pin.controller().snapshot().backend_weight_bytes);
    try retry.rollbackUnpublishedLocked();
    retry.finishOutsideExecution();
    try std.testing.expectEqualDeep(existing, pin.controller().snapshot());
    try std.testing.expectEqual(@as(usize, 8), cache.backend_bytes);
    try std.testing.expectEqual(old_credit_count, binding.credits.items.len);
    try std.testing.expect(old_credit_pointer == binding.credits.items.ptr);
    try std.testing.expectEqual(old_metadata, binding.metadata_host_bytes);
    cache.noteLogicalPhysicalOwnerRemovalForTier(.backend, 8);
    try cache.deinitAdmissionWithCleanup(.{ .outside_execution = true });
    for (seed_backings.?) |part| part.releaseAfterMetadataDeathOutsideExecution();
    seed_backings = null;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
}

fn twoTierCacheAllocationFailureCase(a: std.mem.Allocator) !void {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var cache = SharedCache.init(.{ .host_limit_bytes = 64, .backend_limit_bytes = 64 });
    defer cache.deinitAdmission();
    // Test-only injectable metadata allocator; this actual binding is fully
    // destroyed before the test allocator leaves, including every failure.
    const metadata = @sizeOf(BindingMetadata) + 2 * (try retirement.PreparedCharge.requestedMetadataBytes(0)) + 2 * @sizeOf(AdmissionCredit);
    try cache.configureAdmission(a, pin.controller(), .gpu, .{ .host_limit_bytes = metadata + 16, .backend_limit_bytes = 64 }, .{});
    cache.admission.?.pure_final_metadata = true;
    try cache.installRetirementDomainOutsideExecution(&pin);
    var growth = try cache.snapshotPhysicalGrowthLocked(16, 16, 32);
    defer growth.finishOutsideExecution();
    try growth.fundOutsideExecution(&pin);
    try growth.commit();
    try growth.rollbackUnpublishedLocked();
    growth.finishOutsideExecution();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, pin.controller().snapshot());
}

test "two tier cache preparation metadata OOM leaves no partial host backend token or Domain custody" {
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, twoTierCacheAllocationFailureCase, .{});
}

test "managed shared cache Domain provenance is exact and does not rebind to a foreign model" {
    const first = try domain.Domain.create();
    defer first.release();
    const other = try domain.Domain.create();
    defer other.release();
    const pin = first.pin();
    defer pin.release();
    const foreign = other.pin();
    defer foreign.release();
    var cache = SharedCache.init(.{});
    defer cache.deinitAdmission();
    try cache.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, .{}, .{});
    try std.testing.expect(!cache.retirementDomainMatches(&pin));
    try cache.installRetirementDomainOutsideExecution(&pin);
    try std.testing.expect(cache.retirementDomainMatches(&pin));
    try std.testing.expect(!cache.retirementDomainMatches(&foreign));
    try std.testing.expectError(error.ForeignAdmissionDomain, cache.installRetirementDomainOutsideExecution(&foreign));
    try std.testing.expect(cache.retirementDomainMatches(&pin));
}

test "cache admission snapshot identifies actual shared binding only within a held owner census" {
    const owner = try domain.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var first = SharedCache.init(.{});
    defer first.deinitAdmission();
    var second = SharedCache.init(.{});
    defer second.deinitAdmission();
    try first.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, .{}, .{});
    try second.configureManagedAdmissionOutsideExecution(pin.controller(), .gpu, .{}, .{});
    try first.installRetirementDomainOutsideExecution(&pin);
    try second.installRetirementDomainOutsideExecution(&pin);
    try first.reserve(.backend, 8);
    const one = first.admissionSnapshot().?;
    const shared = (&first).admissionSnapshot().?;
    const different = second.admissionSnapshot().?;
    try std.testing.expectEqual(one.identity, shared.identity);
    try std.testing.expect(one.identity != different.identity);
    try std.testing.expectEqualDeep(first.admissionAmounts(), one.amounts);
    try std.testing.expectEqual(@as(usize, 8), one.amounts.backend_weight_bytes);
    first.noteRelease(.backend, 8);
    try first.flushReleasedOutsideExecution();
}
