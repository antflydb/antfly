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

//! Cold exact-lease metadata for a fenced cache detach. Reuses the existing
//! Charge/DeferredRoot and Epoch queue; there is no independent worker/queue.
//! SDK roots prevent refunds before payload death AND the pending-acquire guard
//! clears. A retained cache root supplies its actual final cleanup epoch later.
const std = @import("std");
const platform = @import("antfly_platform");
const memory = @import("../../runtime/tier/memory.zig");
const admission = @import("../../runtime/tier/admission_domain.zig");
const retirement = @import("../../execution/resource_retirement.zig");
const sink = @import("../../execution/retirement_sink.zig");
const Cleanup = @import("../../execution/cleanup_context.zig").Context;

pub const PreparedQuotas = struct {
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    slots: []Slot,
    issued: usize = 0,
    sdk_finished: bool = false,
    domain: admission.Pin,

    pub const Slot = struct {
        owner: *PreparedQuotas,
        prepared: retirement.PreparedCharge,
        sdk_root: ?*retirement.Charge,
        domain: ?admission.Pin = null,
        parked: ?memory.AdmissionLease = null,
        epoch: ?sink.Epoch = null,
        epoch_node: retirement.Node = undefined,
        reserved: bool = false,

        /// Must precede actual SDK/host payload destruction. A null boundary
        /// never means outside execution. The raw outside API is explicit.
        pub fn validateCleanup(_: *const Slot, current: Cleanup) !void {
            if (current.outside_execution) return;
            const epoch = current.boundary orelse return error.CacheRetirementCleanupAuthorityRequired;
            if (try epoch.isSealed()) return error.RetirementEpochSealed;
        }
        fn epochFinished(raw: *anyopaque) callconv(.c) void {
            const self: *Slot = @ptrCast(@alignCast(raw));
            var epoch = self.epoch.?;
            self.epoch = null;
            const owner = self.owner;
            epoch.release();
            owner.release(); // No self dereference after the slab may die.
        }
        /// The lease value moves exactly once, only after its actual payload
        /// has died. There is no allocation, partition or supplier call here
        /// while either the SDK root or CURRENT epoch remains outstanding.
        pub fn finishAfterSdkDeath(self: *Slot, lease: *?memory.AdmissionLease, current: Cleanup) !void {
            try self.validateCleanup(current);
            try self.owner.validateLease(lease.*);
            const owner = self.owner;
            // Pin the exact current record before moving the opaque lease. A
            // stale authority failure leaves the prepared shell and lease intact.
            var held: ?sink.Epoch = if (!current.outside_execution) try current.boundary.?.retain() else null;
            errdefer if (held) |*epoch| epoch.release();
            const boundary = if (held) |*epoch| try epoch.boundary() else null;
            var pin = self.domain;
            const charge = try self.prepared.arm(lease, &pin);
            self.domain = null;
            if (current.outside_execution) {
                charge.release(); // SDK root still protects a pending detach.
                owner.release();
                return;
            }
            // Charge owns its prepared boundary Node. Keep a separate pure
            // epoch pin through retirement of that exact reusable record.
            charge.holdUntilBoundary(boundary.?) catch platform.inference_process_supervisor.restartWorker();
            self.epoch = held;
            held = null;
            self.epoch_node = .{ .context = self, .release_fn = epochFinished };
            charge.release();
            // A sealed/late registration may synchronously release the slab.
            // This is the last self access; no SDK/model/Io is captured.
            boundary.?.register(boundary.?.context, boundary.?.nonce, &self.epoch_node) catch platform.inference_process_supervisor.restartWorker();
        }
        pub fn park(self: *Slot, lease: *?memory.AdmissionLease) void {
            std.debug.assert(self.parked == null and self.prepared.owner != null);
            self.parked = lease.*;
            lease.* = null;
        }
        fn releaseParkedOutside(self: *Slot) void {
            if (self.parked == null) return; // A retained root owns its slot.
            self.finishAfterSdkDeath(&self.parked, .{ .outside_execution = true }) catch platform.inference_process_supervisor.restartWorker();
        }
    };

    pub fn prepare(capacity: usize, domain: *const admission.Pin) !*PreparedQuotas {
        return prepareWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()), capacity, domain);
    }
    fn prepareWithAllocator(allocator: std.mem.Allocator, capacity: usize, domain: *const admission.Pin) !*PreparedQuotas {
        if (capacity == 0) return error.InvalidCacheRetirementCapacity;
        const self = try allocator.create(PreparedQuotas);
        errdefer allocator.destroy(self);
        const slots = try allocator.alloc(Slot, capacity);
        errdefer allocator.free(slots);
        var initialized: usize = 0;
        errdefer for (slots[0..initialized]) |*slot| {
            slot.sdk_root.?.release();
            slot.prepared.destroyUnused();
        };
        for (slots) |*slot| {
            const prepared = try retirement.PreparedCharge.prepare(0);
            slot.* = .{ .owner = self, .prepared = prepared, .sdk_root = prepared.owner.?.retainOwner() };
            initialized += 1;
        }
        self.* = .{ .allocator = allocator, .slots = slots, .domain = domain.retain() };
        return self;
    }
    pub fn validateLease(self: *const PreparedQuotas, lease: ?memory.AdmissionLease) !void {
        const value = lease orelse return error.MissingCacheRetirementLease;
        if (value.controller != self.domain.controller()) return error.ForeignAdmissionDomain;
    }
    /// Exclusive cold/fenced metadata owner only; preflight every member before
    /// any cache ownership mutation. Capacity failure consumes nothing.
    pub fn reserve(self: *PreparedQuotas, lease: ?memory.AdmissionLease) !*Slot {
        try self.validateLease(lease);
        if (self.sdk_finished or self.issued == self.slots.len) return error.CacheRetirementCapacityChanged;
        const slot = &self.slots[self.issued];
        self.issued += 1;
        slot.reserved = true;
        slot.domain = self.domain.retain();
        _ = self.references.fetchAdd(1, .monotonic);
        return slot;
    }
    /// Caller has checked-freed every detached SDK/host payload and cleared its
    /// actual pending acquisition guard BEFORE this. No numerical owner remains
    /// in these pure roots. Ordinary quotas refund now, not at the task's end.
    pub fn completeSdkRetirementOutside(self: *PreparedQuotas) void {
        std.debug.assert(!self.sdk_finished);
        self.sdk_finished = true;
        for (self.slots) |*slot| {
            const charge = slot.sdk_root.?;
            slot.sdk_root = null;
            charge.release();
        }
        for (self.slots[0..self.issued]) |*slot| slot.releaseParkedOutside();
    }
    /// Unpublished metadata only; no cache lease/root has moved into a slot.
    pub fn destroyUnused(self: *PreparedQuotas) void {
        std.debug.assert(self.issued == 0 and !self.sdk_finished);
        self.completeSdkRetirementOutside();
        self.release();
    }
    /// Drop carrier ownership after completion. Reserved retained-root slots
    /// independently pin this slab until their actual final cleanup callbacks.
    pub fn release(self: *PreparedQuotas) void {
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous != 1) return;
        std.debug.assert(self.sdk_finished);
        for (self.slots) |*slot| {
            std.debug.assert(slot.epoch == null and slot.parked == null and slot.sdk_root == null);
            slot.prepared.destroyUnused();
            if (slot.domain) |pin| pin.release();
        }
        const allocator = self.allocator;
        const domain = self.domain;
        allocator.free(self.slots);
        allocator.destroy(self);
        domain.release();
    }
};

/// Pure all-member preflight before a single old cache owner moves.
pub fn scanLease(lease: ?memory.AdmissionLease, controller: *?*memory.AdmissionController) !usize {
    const value = lease orelse return 0;
    const actual = value.controller orelse return error.InvalidCacheRetirementLease;
    if (controller.*) |expected| {
        if (expected != actual) return error.ForeignAdmissionDomain;
    } else controller.* = actual;
    return 1;
}
pub fn parkLease(quotas: *PreparedQuotas, lease: *?memory.AdmissionLease) void {
    if (lease.* == null) return;
    const slot = quotas.reserve(lease.*) catch unreachable; // whole detach preflight
    slot.park(lease);
}

fn preparedQuotaCase(allocator: std.mem.Allocator) !void {
    const domain = try admission.Domain.create();
    defer domain.release();
    const pin = domain.pin();
    defer pin.release();
    var lease: ?memory.AdmissionLease = try pin.controller().tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false);
    defer if (lease) |*value| value.release();
    const original = lease.?.amounts;
    const prepared = PreparedQuotas.prepareWithAllocator(allocator, 3, &pin) catch |err| {
        try std.testing.expectEqualDeep(original, lease.?.amounts);
        try std.testing.expectEqual(@as(usize, 64), pin.controller().snapshot().backend_scratch_bytes);
        return err;
    };
    defer prepared.destroyUnused();
    try std.testing.expectEqualDeep(original, lease.?.amounts);
}
test "CUDA cache retirement cold pure quota slots unwind every metadata allocation failure without moving source admission" {
    try preparedQuotaCase(std.testing.allocator);
    try platform.allocator.checkAllAllocationFailures(std.testing.allocator, preparedQuotaCase, .{});
}

test "CUDA cache retirement refunds parked quota before task epoch ends but waits exact final B and SDK roots for retained constants" {
    const Session = @import("../../backends/session.zig").Session;
    const MetadataOwner = struct {
        fn close(_: *anyopaque) void {}
    };
    var first_owner: u8 = 0;
    var second_owner: u8 = 0;
    var first_model: Session.ModelLifetime = .{ .owner = &first_owner, .destroy_fn = MetadataOwner.close };
    defer first_model.release();
    var second_model: Session.ModelLifetime = .{ .owner = &second_owner, .destroy_fn = MetadataOwner.close };
    defer second_model.release();
    const Probe = struct {
        models: [2]*Session.ModelLifetime,
        calls: usize = 0,
        blocked: bool = false,
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            for (self.models) |model| {
                if (!model.execution_gate.tryLock()) self.blocked = true else model.execution_gate.unlock();
            }
        }
    };
    const domain = try admission.Domain.create();
    var domain_open = true;
    defer if (domain_open) domain.release();
    const pin = domain.pin();
    defer pin.release();
    var probe = Probe{ .models = .{ &first_model, &second_model } };
    try pin.controller().configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var owner = try sink.Ref.create();
    defer owner.release();
    var task = try owner.enter();
    defer task.finish();
    var task_epoch = try task.retainEpoch();
    defer task_epoch.release();
    {
        var lease: ?memory.AdmissionLease = try pin.controller().tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 32 }, false);
        defer if (lease) |*value| value.release();
        const quotas = try PreparedQuotas.prepare(1, &pin);
        const slot = try quotas.reserve(lease);
        slot.park(&lease);
        try std.testing.expect(lease == null);
        quotas.completeSdkRetirementOutside();
        quotas.release();
        try std.testing.expect(!try task_epoch.isSealed());
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
        try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().backend_scratch_bytes);
    }
    inline for (.{ false, true }) |finish_b_first| {
        var source_arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        var arena_open = true;
        defer if (arena_open) source_arena.deinit();
        const source_lease = try source_arena.allocator().create(?memory.AdmissionLease);
        source_lease.* = try pin.controller().tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false);
        defer if (arena_open) if (source_lease.*) |*value| value.release();
        const quotas = try PreparedQuotas.prepare(1, &pin);
        const slot = try quotas.reserve(source_lease.*);
        try std.testing.expectError(error.CacheRetirementCleanupAuthorityRequired, slot.validateCleanup(.{}));
        try std.testing.expect(source_lease.* != null); // no ownership mutation
        var final_b = try owner.enter();
        defer final_b.finish();
        var b = try final_b.retainEpoch();
        defer b.release();
        try std.testing.expect(first_model.execution_gate.tryLock());
        try std.testing.expect(second_model.execution_gate.tryLock());
        const calls = probe.calls;
        try slot.finishAfterSdkDeath(source_lease, .{ .boundary = &b });
        try std.testing.expect(source_lease.* == null);
        source_arena.deinit(); // no callback retains this borrowed header/arena
        arena_open = false;
        try std.testing.expectEqual(calls, probe.calls);
        first_model.execution_gate.unlock();
        second_model.execution_gate.unlock();
        if (finish_b_first) {
            final_b.finish();
            try std.testing.expectEqual(calls, probe.calls); // actual SDK barrier remains
            quotas.completeSdkRetirementOutside();
        } else {
            quotas.completeSdkRetirementOutside();
            try std.testing.expectEqual(calls, probe.calls); // exact B remains
            final_b.finish();
        }
        quotas.release();
        try std.testing.expectEqual(calls + 1, probe.calls);
        try std.testing.expectEqual(@as(usize, 0), pin.controller().snapshot().backend_scratch_bytes);
    }
    domain.release();
    domain_open = false;
    try std.testing.expect(!probe.blocked);
}

test "CUDA cache retirement cold quota reservation rejects foreign controller and exhausted capacity before moving ownership" {
    const first = try admission.Domain.create();
    defer first.release();
    const second = try admission.Domain.create();
    defer second.release();
    const pin = first.pin();
    defer pin.release();
    const quotas = try PreparedQuotas.prepare(1, &pin);
    var lease: ?memory.AdmissionLease = try second.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 16 }, false);
    defer if (lease) |*value| value.release();
    try std.testing.expectError(error.ForeignAdmissionDomain, quotas.reserve(lease));
    try std.testing.expectEqual(@as(usize, 0), quotas.issued);
    try std.testing.expect(lease != null);
    quotas.destroyUnused();
    const bounded = try PreparedQuotas.prepare(1, &pin);
    var one: ?memory.AdmissionLease = try first.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 16 }, false);
    defer if (one) |*value| value.release();
    var two: ?memory.AdmissionLease = try first.controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 16 }, false);
    defer if (two) |*value| value.release();
    const first_slot = try bounded.reserve(one);
    first_slot.park(&one);
    try std.testing.expectError(error.CacheRetirementCapacityChanged, bounded.reserve(two));
    try std.testing.expect(two != null);
    bounded.completeSdkRetirementOutside();
    bounded.release();
}
