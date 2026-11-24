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
                .unarmed, .storage, .cache_component => @panic("invalid prepared cache quota"),
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
            .raw => return error.MissingPreparedCacheRetirement,
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
                std.debug.assert(current.outside_execution);
                self.* = .none;
                var lease = value;
                lease.release();
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

test "CUDA prepared cache quota retires on actual later current B after old A sealed" {
    const owner = try admission.Domain.create();
    defer owner.release();
    var pin: ?admission.Pin = owner.pin();
    defer if (pin) |held| held.release();
    var lease: ?memory.AdmissionLease = try owner.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false);
    defer if (lease) |*held| held.release();
    var shell = try retirement.PreparedCharge.prepare(0);
    defer shell.destroyUnused();
    var quota = try Quota.arm(&shell, &lease, &pin);
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
    try std.testing.expectError(error.RetirementEpochSealed, quota.releaseAfterSdkDeath(.{ .boundary = &a }));
    try std.testing.expect(quota == .prepared);
    try quota.releaseAfterSdkDeath(.{ .boundary = &b });
    try std.testing.expect(quota == .none);
    try std.testing.expectEqual(@as(usize, 32), owner.controller.snapshot().backend_scratch_bytes);
    current.finish();
    try std.testing.expectEqual(@as(usize, 0), owner.controller.snapshot().backend_scratch_bytes);
}
