// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One physical cache owner's exact quota. A prepared producer transfers its
//! already allocated Charge shell; no creation Epoch/model/Io/arena is stored.
//! The compatibility arm is explicit raw admission provenance, not an inference
//! from buffer flags, dtype, storage mode or a nullable cleanup boundary.
const std = @import("std");
const memory = @import("../../runtime/tier/memory.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const admission = @import("../../runtime/tier/admission_domain.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;
const carriers = @import("cache_retirement.zig");

pub const Quota = union(enum) {
    none,
    raw: memory.AdmissionLease,
    prepared: *retirement.Charge,

    pub fn fromRawAdmission(lease: ?memory.AdmissionLease) Quota {
        return if (lease) |value| .{ .raw = value } else .none;
    }
    /// Synchronous transfer only. The caller has already prefunded its exact
    /// lease and prepared the shell before every numerical/provider lane.
    pub fn arm(shell: *retirement.PreparedCharge, lease: *?memory.AdmissionLease, pin: *?admission.Pin) !Quota {
        if (lease.* == null) return .none;
        const domain = pin.* orelse return error.MissingCacheRetirementDomain;
        if (domain.controller() != lease.*.?.controller) return error.ForeignAdmissionDomain;
        return .{ .prepared = try shell.arm(lease, pin) };
    }
    pub fn hasLease(self: Quota) bool {
        return self != .none;
    }
    /// A value identity snapshot only, read under the real cache owner. It is
    /// never an extra quota/lease owner and cannot independently be released.
    pub fn peek(self: Quota) ?memory.AdmissionLease {
        return switch (self) {
            .none => null,
            .raw => |lease| lease,
            .prepared => |charge| switch (charge.quota) {
                .run => |lease| lease,
                .unarmed, .storage => @panic("invalid prepared cache quota"),
            },
        };
    }
    /// Validate BEFORE SDK mutation. Null does not manufacture outside-gate
    /// authority, and raw leases cannot allocate a missing shell under B.
    pub fn validateCleanup(self: Quota, current: Cleanup) !void {
        if (self == .none or current.outside_execution) return;
        const epoch = current.boundary orelse return error.CacheRetirementCleanupAuthorityRequired;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        switch (self) {
            .none => unreachable,
            .raw => |lease| {
                const pending = current.synchronous_outside_raw orelse return error.MissingPreparedCacheRetirement;
                try pending.validateValue(lease, epoch);
            },
            .prepared => |charge| {
                if (charge.domain == null or charge.prepared_groups == null or charge.prepared_boundary == null) return error.MissingPreparedCacheRetirement;
            },
        }
    }
    /// The concrete owner has CHECKED actual backing death. Only pure Charge
    /// metadata survives this call; it captures neither allocator/SDK endpoint
    /// nor model/Io/request arena. The actual current B is supplied NOW.
    pub fn releaseAfterSdkDeath(self: *Quota, current: Cleanup) !void {
        try self.validateCleanup(current);
        switch (self.*) {
            .none => {},
            .raw => |value| {
                if (!current.outside_execution) {
                    const pending = current.synchronous_outside_raw orelse return error.MissingPreparedCacheRetirement;
                    try pending.take(&self.raw, current.boundary.?);
                    self.* = .none;
                } else {
                    self.* = .none;
                    var lease = value;
                    lease.release(); // explicit outside-execution contract above
                }
            },
            .prepared => |charge| {
                if (!current.outside_execution) try charge.holdUntilBoundary(try current.boundary.?.boundary());
                self.* = .none;
                charge.release();
            },
        }
    }
    /// Compatibility ONLY for the preexisting raw producer path. This is
    /// deliberately not outside-execution authority: it preserves that legacy
    /// callback behavior until the actual caller adopts cold preparation.
    /// Prepared custody can never enter this branch, even after SDK death.
    pub fn validateLegacyRaw(self: Quota) !void {
        if (self == .prepared) return error.CacheRetirementCleanupAuthorityRequired;
    }
    pub fn releaseLegacyRawAfterSdkDeath(self: *Quota) !void {
        try self.validateLegacyRaw();
        switch (self.*) {
            .none => {},
            .raw => |value| {
                self.* = .none;
                var lease = value;
                lease.release();
            },
            .prepared => unreachable,
        }
    }
    pub fn releaseOutsideExecution(self: *Quota) void {
        self.releaseAfterSdkDeath(.{ .outside_execution = true }) catch @panic("invalid outside cache quota");
    }

    const Handoff = struct {
        original: Quota,
        lease: ?memory.AdmissionLease,
        fn commit(self: *Handoff) void {
            std.debug.assert(self.lease == null);
            if (self.original == .prepared) self.original.prepared.release();
            self.original = .none;
        }
        fn abort(self: *Handoff, destination: *Quota) void {
            std.debug.assert(destination.* == .none);
            if (self.original == .prepared) self.original.prepared.quota = .{ .run = self.lease.? };
            destination.* = self.original;
            self.original = .none;
            self.lease = null;
        }
    };
    /// Existing44's independently prepared SDK-root carrier takes the actual
    /// lease while this shell's pure Domain pin remains alive until commit.
    /// No callback can run during this infallible metadata move.
    fn handoff(self: *Quota) Handoff {
        const original = self.*;
        const lease = original.peek();
        if (original == .prepared) {
            const charge = original.prepared;
            std.debug.assert(charge.references.load(.acquire) == 1);
            std.debug.assert(charge.prepared_group_cursor == 0 and charge.prepared_boundary != null);
            charge.quota = .unarmed;
        }
        self.* = .none;
        return .{ .original = original, .lease = lease };
    }
    pub fn park(self: *Quota, owner: *carriers.PreparedQuotas) void {
        if (self.* == .none) return;
        const slot = owner.reserve(self.peek()) catch @panic("cache quota snapshot changed before park");
        var move = self.handoff();
        slot.park(&move.lease);
        move.commit(); // carrier's SDK root+Domain now own exact quota
    }
    /// A retained constant still owns this Quota until its REAL final backing
    /// dies. Existing44 Slot supplies SDK-pending AND actual-current-B proof.
    /// A stale B error restores this exact original owner without allocation.
    pub fn finishRetainedAfterSdkDeath(self: *Quota, slot: *carriers.PreparedQuotas.Slot, current: Cleanup) !void {
        try slot.validateCleanup(current);
        try slot.owner.validateLease(self.peek());
        var move = self.handoff();
        slot.finishAfterSdkDeath(&move.lease, current) catch |err| {
            move.abort(self);
            return err;
        };
        move.commit();
    }
};

/// Cold metadata for the SAME already-owned raw lease. The caller strongly
/// retains its actual immutable payload and snapshots under real exclusion;
/// prepare runs outside ALL numerical lanes using the actual OLD Domain.
/// This never reserves, refunds, splits or reconstructs quota from byte totals.
pub const RawUpgrade = struct {
    expected: memory.AdmissionLease,
    shell: retirement.PreparedCharge,
    domain: ?admission.Pin,

    pub fn prepare(expected: memory.AdmissionLease, old_domain: *const admission.Pin) !RawUpgrade {
        try validateDomain(expected, old_domain);
        var pin = old_domain.retain();
        errdefer pin.release();
        const shell = try retirement.PreparedCharge.prepare(0);
        return .{ .expected = expected, .shell = shell, .domain = pin };
    }
    fn prepareForTest(allocator: std.mem.Allocator, expected: memory.AdmissionLease, old_domain: *const admission.Pin) !RawUpgrade {
        try validateDomain(expected, old_domain);
        var pin = old_domain.retain();
        errdefer pin.release();
        const shell = try retirement.PreparedCharge.prepareWithAllocatorForTest(allocator, 0);
        return .{ .expected = expected, .shell = shell, .domain = pin };
    }
    fn validateDomain(expected: memory.AdmissionLease, old_domain: *const admission.Pin) !void {
        const controller = expected.controller orelse return error.MissingResourceRetirementLease;
        if (old_domain.controller() != controller) return error.ForeignAdmissionDomain;
    }
    /// The concrete owner revalidates key, SAME retained parent and backing
    /// BEFORE this call under its actual cohort. All lease fields (including
    /// external/shared token and settled/live epochs) must still match. Failed
    /// arm leaves both this cold metadata and the original owner unchanged.
    pub fn install(self: *RawUpgrade, target: *Quota) !bool {
        const actual = target.peek() orelse return error.PreparedCachePlanChanged;
        if (!std.meta.eql(actual, self.expected)) return error.PreparedCachePlanChanged;
        if (target.* == .prepared) return false; // another exact upgrade won
        if (target.* != .raw) return error.PreparedCachePlanChanged;
        const domain = self.domain orelse return error.RawQuotaUpgradeConsumed;
        if (self.shell.owner == null) return error.RawQuotaUpgradeConsumed;
        var lease: ?memory.AdmissionLease = target.raw;
        var pin: ?admission.Pin = domain;
        const prepared = try Quota.arm(&self.shell, &lease, &pin);
        target.* = prepared;
        self.domain = null; // transferred, not copied ownership
        return true;
    }
    /// Only inert unused metadata remains after successful arm. The expected
    /// lease is a NON-owning identity snapshot and is never independently freed.
    pub fn deinitOutsideExecution(self: *RawUpgrade) void {
        self.shell.destroyUnused();
        if (self.domain) |pin| pin.release();
        self.domain = null;
    }
};

test "CUDA cache quota prepared custody fits original optional lease and refuses raw current B before mutation" {
    try std.testing.expect(@sizeOf(Quota) <= @sizeOf(?memory.AdmissionLease));
    const domain = try admission.Domain.create();
    defer domain.release();
    var raw = Quota.fromRawAdmission(try domain.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 16 }, false));
    defer raw.releaseOutsideExecution();
    var sink = try @import("../../execution/retirement_sink.zig").Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    try std.testing.expectError(error.MissingPreparedCacheRetirement, raw.validateCleanup(.{ .boundary = &epoch }));
    try std.testing.expectEqual(@as(usize, 16), domain.controller.snapshot().backend_scratch_bytes);
    try std.testing.expect(raw == .raw);
}

test "CUDA legacy raw cache compatibility cannot consume prepared custody or mint outside authority" {
    const owner = try admission.Domain.create();
    defer owner.release();
    var pin: ?admission.Pin = owner.pin();
    defer if (pin) |value| value.release();
    var lease: ?memory.AdmissionLease = try owner.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 16 }, false);
    defer if (lease) |*value| value.release();
    var shell = try retirement.PreparedCharge.prepare(0);
    defer shell.destroyUnused();
    var quota = try Quota.arm(&shell, &lease, &pin);
    defer quota.releaseOutsideExecution(); // actual test owns no execution lane
    // Both helpers refuse without changing the original sole owner. A concrete
    // caller must perform this preflight BEFORE any physical SDK mutation.
    try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, quota.validateLegacyRaw());
    try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, quota.releaseLegacyRawAfterSdkDeath());
    try std.testing.expect(quota == .prepared);
    try std.testing.expectEqual(@as(usize, 16), owner.controller.snapshot().backend_scratch_bytes);
}

fn rawUpgradeAllocationCase(allocator: std.mem.Allocator) !void {
    const owner = try admission.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var quota = Quota.fromRawAdmission(try owner.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false));
    defer quota.releaseOutsideExecution();
    const original = quota.peek().?;
    errdefer {
        std.testing.expect(quota == .raw) catch @panic("upgrade OOM mutated raw quota");
        std.testing.expect(std.meta.eql(original, quota.peek().?)) catch @panic("upgrade OOM changed lease identity");
        std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().backend_scratch_bytes) catch @panic("upgrade OOM refunded quota");
    }
    var upgrade = try RawUpgrade.prepareForTest(allocator, original, &pin);
    defer upgrade.deinitOutsideExecution();
    try std.testing.expect(try upgrade.install(&quota));
    try std.testing.expect(quota == .prepared);
    try std.testing.expect(std.meta.eql(original, quota.peek().?));
    try std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().backend_scratch_bytes);
}

test "CUDA raw cache quota upgrade preserves exact lease through every cold metadata allocation failure" {
    try rawUpgradeAllocationCase(std.testing.allocator);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, rawUpgradeAllocationCase, .{});
}

test "CUDA raw cache quota upgrade rejects foreign old Domain and changed full lease identity before mutation" {
    const owner = try admission.Domain.create();
    defer owner.release();
    const other = try admission.Domain.create();
    defer other.release();
    const pin = owner.pin();
    defer pin.release();
    const foreign = other.pin();
    defer foreign.release();
    var quota = Quota.fromRawAdmission(try owner.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false));
    defer quota.releaseOutsideExecution();
    const original = quota.peek().?;
    try std.testing.expectError(error.ForeignAdmissionDomain, RawUpgrade.prepareForTest(std.testing.allocator, original, &foreign));
    var upgrade = try RawUpgrade.prepareForTest(std.testing.allocator, original, &pin);
    defer upgrade.deinitOutsideExecution();
    upgrade.expected.live_settled_epoch +%= 1;
    try std.testing.expectError(error.PreparedCachePlanChanged, upgrade.install(&quota));
    try std.testing.expect(quota == .raw);
    try std.testing.expect(std.meta.eql(original, quota.peek().?));
    upgrade.expected = original;
    try std.testing.expect(try upgrade.install(&quota));
    try std.testing.expect(!(try upgrade.install(&quota)));
    try std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().backend_scratch_bytes);
}

test "CUDA raw cache quota upgrade retires on actual later current B after old A sealed" {
    const owner = try admission.Domain.create();
    defer owner.release();
    const pin = owner.pin();
    defer pin.release();
    var quota = Quota.fromRawAdmission(try owner.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false));
    defer quota.releaseOutsideExecution();
    var sink = try @import("../../execution/retirement_sink.zig").Ref.create();
    defer sink.release();
    var old = try sink.enter();
    var a = try old.retainEpoch();
    defer a.release();
    old.finish();
    try std.testing.expect(try a.isSealed());
    var current = try sink.enter();
    defer current.finish();
    var b = try current.retainEpoch();
    defer b.release();
    var upgrade = try RawUpgrade.prepareForTest(std.testing.allocator, quota.peek().?, &pin);
    defer upgrade.deinitOutsideExecution();
    try std.testing.expect(try upgrade.install(&quota));
    try std.testing.expectError(error.RetirementEpochSealed, quota.releaseAfterSdkDeath(.{ .boundary = &a }));
    try std.testing.expect(quota == .prepared);
    try quota.releaseAfterSdkDeath(.{ .boundary = &b });
    try std.testing.expect(quota == .none);
    try std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().backend_scratch_bytes);
    current.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.controller.snapshot().backend_scratch_bytes);
}
