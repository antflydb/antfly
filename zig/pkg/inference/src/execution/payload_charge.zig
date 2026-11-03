// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! A partition of actual numerical payload quota with independent domain life.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");
const domain = @import("../runtime/tier/admission_domain.zig");
pub const Owned = struct {
    lease: memory.AdmissionLease,
    pin: domain.Pin,
    pub fn partition(lease: *memory.AdmissionLease, pin: domain.Pin, bytes: usize) !?Owned {
        var prepared = (try PreparedOwned.prepare(lease, pin, bytes)) orelse return null;
        return prepared.commit();
    }
    pub fn release(self: *Owned) void {
        self.lease.release();
        self.pin.release();
        self.* = undefined;
    }
};

/// Exclusive parent-lease borrow. All metadata allocation/context callbacks
/// happen in prepare BEFORE model/provider gates; commit is infallible and
/// invokes no external callback. Abort likewise belongs outside those gates.
pub const PreparedOwned = struct {
    partition: memory.AdmissionLease.PreparedPartition,
    pin: domain.Pin,
    held: bool = true,
    pub fn prepare(lease: *memory.AdmissionLease, pin: domain.Pin, bytes: usize) !?PreparedOwned {
        var pin_owned = true;
        defer if (pin_owned) pin.release();
        if (bytes == 0 or lease.controller != pin.controller() or lease.amounts.backend_scratch_bytes < bytes) return null;
        const allocator = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());
        const partition = try lease.preparePartition(allocator, .{ .backend_scratch_bytes = bytes });
        pin_owned = false;
        return .{ .partition = partition, .pin = pin };
    }
    pub fn commit(self: *PreparedOwned) Owned {
        std.debug.assert(self.held);
        self.held = false;
        return .{ .lease = self.partition.commit(), .pin = self.pin };
    }
    pub fn abort(self: *PreparedOwned) void {
        if (!self.held) return;
        self.held = false;
        self.partition.abort();
        self.pin.release();
    }
};

/// Synchronous borrowed authority. The complete original resource holder keeps
/// context alive only while reserve/take run; no callback escapes into a fence.
pub const Capability = struct {
    context: *anyopaque,
    /// Set only by the typed task root that controls original EncoderOutputs
    /// and every retained Source sibling through their final actual cleanup.
    final_cleanup_qualified: bool = false,
    background_supervised: bool = false,
    /// Exact original public tensor header protected by the complete holder.
    /// A different tensor cannot spend this source's retained-output quota.
    bound_value: ?@import("../ops/ops.zig").CT = null,
    /// Actual enclosing task deadline+cleanup grace in process monotonic time.
    /// Null means the qualified task supplied no deadline, not a fresh timeout.
    root_cleanup_deadline_ns: ?u64 = null,
    prepare_fn: *const fn (*anyopaque, usize, ?u64) anyerror!bool,
    commit_fn: *const fn (*anyopaque) void,
    abort_fn: *const fn (*anyopaque) void,
    take_fn: *const fn (*anyopaque) Owned,
};

/// Failed cold preparation keeps a carved source charge with the original
/// output holder. Its final cleanup queues only this independent value, so no
/// external quota callback executes under a model/provider cleanup lane.
pub const Deferred = struct {
    charge: ?Owned = null,
    prepared: ?PreparedOwned = null,
    /// Immutable whole-source task deadline, never refreshed by retry policy.
    deadline_ns: ?u64,
    cleanup_epoch: ?@import("retirement_sink.zig").Epoch = null,
    ticket: @import("completion_retirement.zig").Ticket,
    node: @import("completion_retirement.zig").Node = undefined,
    pub fn create(deadline_ns: ?u64) !?*Deferred {
        var ticket = (try @import("completion_retirement.zig").reserve()) orelse return null;
        errdefer ticket.release();
        const allocator = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());
        const self = try allocator.create(Deferred);
        self.* = .{ .ticket = ticket, .deadline_ns = deadline_ns };
        return self;
    }
    pub fn prepare(self: *Deferred, lease: *memory.AdmissionLease, pin: domain.Pin, bytes: usize) !bool {
        self.prepared = try PreparedOwned.prepare(lease, pin, bytes);
        return self.prepared != null;
    }
    pub fn commit(self: *Deferred) void {
        if (self.prepared) |*owned| {
            self.charge = owned.commit();
            self.prepared = null;
        } else std.debug.assert(self.charge != null);
    }
    pub fn abortPreparation(self: *Deferred) bool {
        if (self.prepared) |*owned| {
            owned.abort();
            self.prepared = null;
            return true;
        }
        return false;
    }
    pub fn take(self: *Deferred) Owned {
        const value = self.charge.?;
        self.charge = null;
        self.destroyUnused();
        return value;
    }
    pub fn destroyUnused(self: *Deferred) void {
        std.debug.assert(self.charge == null and self.prepared == null);
        self.ticket.release();
        @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback()).destroy(self);
    }
    pub fn retire(self: *Deferred, boundary: ?*const @import("retirement_sink.zig").Epoch) void {
        std.debug.assert(self.prepared == null);
        const actual = boundary orelse @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.cleanup_epoch = actual.retain() catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        self.node = .{ .context = self, .ready_fn = ready, .retire_fn = release, .deadline_ns = self.deadline_ns };
        self.ticket.submit(&self.node);
    }
    fn ready(raw: *anyopaque) !bool {
        const self: *Deferred = @ptrCast(@alignCast(raw));
        return try self.cleanup_epoch.?.isSealed();
    }
    fn release(raw: *anyopaque) void {
        const self: *Deferred = @ptrCast(@alignCast(raw));
        self.charge.?.release();
        self.cleanup_epoch.?.release();
        @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback()).destroy(self);
    }
};

test "failed preparation source charge retains its whole task cleanup deadline through epoch retirement" {
    if (comptime @import("build_options").enable_wasm or (@import("builtin").os.tag != .linux and @import("builtin").os.tag != .macos)) return error.SkipZigTest;
    const completion = @import("completion_retirement.zig");
    const Sink = @import("retirement_sink.zig");
    const platform = @import("antfly_platform");
    const d = try domain.Domain.create();
    defer d.release();
    var lease = try d.controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 8 }, .{ .backend_scratch_bytes = 8 }, false);
    defer lease.release();
    var sink = try Sink.Ref.create();
    defer sink.release();
    var guard = try sink.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    defer {
        guard.finish();
        const end = platform.time.monotonicNs() + 5 * std.time.ns_per_s;
        while (completion.testingPending() != 0) {
            if (platform.time.monotonicNs() >= end) @panic("pending charge did not finish");
            std.testing.io.sleep(.fromMilliseconds(1), .awake) catch unreachable;
        }
    }
    const root_deadline = platform.time.monotonicNs() + 30 * std.time.ns_per_s;
    const pending = (try Deferred.create(root_deadline)) orelse return error.UnexpectedRetirementCapacity;
    var pending_owned = true;
    defer if (pending_owned) {
        if (pending.charge) |*charge| charge.release();
        pending.charge = null;
        pending.destroyUnused();
    };
    pending.charge = (try Owned.partition(&lease, d.pin(), 8)).?;
    try std.testing.expectEqual(@as(?u64, root_deadline), pending.deadline_ns);
    const later_attempt_deadline = root_deadline + std.time.ns_per_s;
    pending.retire(&epoch);
    pending_owned = false;
    // Unsealed real cleanup authority keeps this independently allocated
    // record alive, so reading its immutable policy is race-free.
    try std.testing.expectEqual(@as(?u64, root_deadline), pending.node.deadline_ns);
    try std.testing.expect(pending.node.deadline_ns.? != later_attempt_deadline);
    try std.testing.expectEqual(@as(usize, 8), d.controller.snapshot().backend_scratch_bytes);
    guard.finish();
}

/// Request-private pre-funding, prepared before ALL source/destination gates.
/// The complete source holder and exclusive parent leases outlive this value.
/// Always abort unused members after source unlock and BEFORE destination lock.
pub const ReceiverFunding = struct {
    source: Capability,
    source_ready: bool,
    bytes: usize,
    destination: union(enum) { prepared: PreparedOwned, owned: Owned, moved },
    pub fn prepare(source: ?Capability, value: @import("../ops/ops.zig").CT, destination_lease: ?*memory.AdmissionLease, destination_pin: ?domain.Pin, bytes: usize) !?ReceiverFunding {
        var pin = destination_pin;
        defer if (pin) |owned| owned.release();
        const capability = source orelse return null;
        if (!capability.final_cleanup_qualified or !capability.background_supervised or capability.root_cleanup_deadline_ns == null or capability.bound_value != value) return null;
        try @import("completion_retirement.zig").prepareSupervision();
        const lease = destination_lease orelse return null;
        const owned_pin = pin orelse return null;
        pin = null;
        var prepared = (try PreparedOwned.prepare(lease, owned_pin, bytes)) orelse return null;
        errdefer prepared.abort();
        const source_ready = try capability.prepare_fn(capability.context, bytes, capability.root_cleanup_deadline_ns);
        return .{ .source = capability, .source_ready = source_ready, .bytes = bytes, .destination = .{ .prepared = prepared } };
    }
    pub fn commitSource(self: *ReceiverFunding) void {
        std.debug.assert(self.source_ready);
        self.source.commit_fn(self.source.context);
    }
    pub fn takeSource(self: *ReceiverFunding) Owned {
        std.debug.assert(self.source_ready);
        self.source_ready = false;
        return self.source.take_fn(self.source.context);
    }
    pub fn commitDestination(self: *ReceiverFunding) void {
        switch (self.destination) {
            .prepared => |*value| {
                const owned = value.commit();
                self.destination = .{ .owned = owned };
            },
            .owned => {},
            .moved => @panic("consumed receiver funding"),
        }
    }
    pub fn takeDestination(self: *ReceiverFunding) Owned {
        const owned = switch (self.destination) {
            .owned => |value| value,
            else => @panic("uncommitted receiver funding"),
        };
        self.destination = .moved;
        return owned;
    }
    pub fn abort(self: *ReceiverFunding) void {
        if (self.source_ready) {
            self.source_ready = false;
            self.source.abort_fn(self.source.context);
        }
        switch (self.destination) {
            .prepared => |*value| value.abort(),
            .owned => |*value| value.release(),
            .moved => {},
        }
        self.destination = .moved;
    }
};

test "unqualified background policy declines before source callbacks quota preparation or SDK supervision" {
    const Probe = struct {
        calls: usize = 0,
        fn prepare(raw: *anyopaque, _: usize, _: ?u64) !bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            return error.UnexpectedBackgroundPreparation;
        }
        fn noop(_: *anyopaque) void {}
        fn take(_: *anyopaque) Owned {
            unreachable;
        }
    };
    var probe = Probe{};
    const value: @import("../ops/ops.zig").CT = @ptrCast(&probe);
    var capability = Capability{ .context = &probe, .bound_value = value, .final_cleanup_qualified = true, .root_cleanup_deadline_ns = 100, .prepare_fn = Probe.prepare, .commit_fn = Probe.noop, .abort_fn = Probe.noop, .take_fn = Probe.take };
    // The task's explicit in_process policy never grants background hard-kill
    // authority, even when it carries a deadline and owns both final roots.
    try std.testing.expect((try ReceiverFunding.prepare(capability, value, null, null, 8)) == null);
    capability.background_supervised = true;
    capability.root_cleanup_deadline_ns = null;
    try std.testing.expect((try ReceiverFunding.prepare(capability, value, null, null, 8)) == null);
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
}
