// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//! One synchronous invocation peak for temporary fixed-geometry device state.
//! The parent charge remains alive through the actual invocation boundary;
//! returning a loan never refunds external quota between decoder layers.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");
const admission = @import("../runtime/tier/admission_domain.zig");
const retirement = @import("resource_retirement.zig");
const Cleanup = @import("cleanup_context.zig").Context;
const Epoch = @import("retirement_sink.zig").Epoch;
const allocator = @import("antfly_platform").allocator.processAllocator(@import("antfly_platform").allocator.concurrentFallback());

pub const Layout = struct {
    key_heads: usize,
    value_heads: usize,
    pub fn bytes(self: Layout) !usize {
        if (self.key_heads == 0 or self.value_heads == 0 or self.key_heads > 256 or self.value_heads > 256 or self.value_heads % self.key_heads != 0) return error.InvalidTensorShape;
        const channels = try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, self.key_heads, 2), self.value_heads), 128);
        const histories_and_convolution = try std.math.mul(usize, channels, 4 * 4 * 3);
        const recurrent = try std.math.mul(usize, self.value_heads, 128 * 128 * 4);
        const side = try std.math.mul(usize, try std.math.add(usize, try std.math.mul(usize, self.value_heads, 2), 128), 4);
        return std.math.add(usize, try std.math.add(usize, histories_and_convolution, recurrent), side);
    }
};

pub const Budget = struct {
    metadata: std.mem.Allocator,
    layout: Layout,
    expected_compute: usize,
    shell: retirement.PreparedCharge,
    charge: ?*retirement.Charge = null,
    // Borrowed ONLY while the actual synchronous invocation is executing.
    current: ?*const Epoch = null,
    boundary_held: bool = false,
    in_use: bool = false,

    pub fn prepare(layout: Layout, expected_compute: usize) !*Budget {
        return prepareWithAllocator(allocator, layout, expected_compute);
    }
    fn prepareWithAllocator(metadata: std.mem.Allocator, layout: Layout, expected_compute: usize) !*Budget {
        _ = try layout.bytes();
        if (expected_compute == 0) return error.MissingRecurrentStateOwner;
        const self = try metadata.create(Budget);
        errdefer metadata.destroy(self);
        self.* = .{ .metadata = metadata, .layout = layout, .expected_compute = expected_compute, .shell = try retirement.PreparedCharge.prepare(0) };
        return self;
    }
    /// Moves an independently funded exact peak; no reservation or callback.
    pub fn arm(self: *Budget, lease: *?memory.AdmissionLease, pin: *?admission.Pin) !void {
        if (self.charge != null) return error.RecurrentStateBudgetAlreadyArmed;
        const value = lease.* orelse return error.MissingResourceRetirementLease;
        if (!std.meta.eql(value.amounts, memory.AdmissionAmounts{ .backend_scratch_bytes = try self.layout.bytes() })) return error.RecurrentStateBudgetChanged;
        const domain = pin.* orelse return error.MissingRecurrentStateDomain;
        if (domain.controller() != value.controller) return error.ForeignAdmissionDomain;
        self.charge = try self.shell.arm(lease, pin);
    }
    pub fn bind(self: *Budget, epoch: *const Epoch) !void {
        if (self.current != null or self.in_use or self.boundary_held) return error.RecurrentStateBudgetAlreadyBound;
        const charge = self.charge orelse return error.UnpreparedRecurrentStateBudget;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        try charge.holdUntilBoundary(try epoch.boundary());
        self.boundary_held = true;
        self.current = epoch;
    }
    pub fn acquire(self: *Budget, compute: usize, layout: Layout) !void {
        if (compute != self.expected_compute or !std.meta.eql(layout, self.layout)) return error.ForeignRecurrentStateBudget;
        const epoch = self.current orelse return error.UnboundRecurrentStateBudget;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        if (self.in_use) return error.RecurrentStateLoanStillLive;
        self.in_use = true;
    }
    pub fn validateCleanup(self: *const Budget, current: Cleanup) !void {
        const bound = self.current orelse return error.UnboundRecurrentStateBudget;
        const actual = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (current.outside_execution or actual.record != bound.record or actual.nonce != bound.nonce or try actual.isSealed()) return error.ForeignRecurrentStateCleanup;
        if (!self.in_use) return error.RecurrentStateLoanAlreadyReturned;
    }
    /// Called only after every allocating descriptor has checked SDK death or
    /// moved into the existing independently funded retained-buffer cache.
    pub fn returnAfterBackingRetired(self: *Budget, current: Cleanup) !void {
        try self.validateCleanup(current);
        self.in_use = false;
    }
    pub fn cleanup(self: *const Budget) !Cleanup {
        const epoch = self.current orelse return error.UnboundRecurrentStateBudget;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        return .{ .boundary = epoch };
    }
    pub fn retireWithCleanup(self: *Budget, current: Cleanup) !void {
        if (current.outside_execution) return error.ForeignRecurrentStateCleanup;
        if (self.in_use) return error.RecurrentStateLoanStillLive;
        if (self.current) |bound| {
            const actual = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            if (actual.record != bound.record or actual.nonce != bound.nonce) return error.ForeignRecurrentStateCleanup;
            if (try actual.isSealed()) return error.RetirementEpochSealed;
        } else if (!self.boundary_held) {
            const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
            try self.bind(epoch);
        }
        try self.unbind();
        try self.destroyBoundRoot();
    }
    fn unbind(self: *Budget) !void {
        if (self.in_use) return error.RecurrentStateLoanStillLive;
        self.current = null;
    }
    /// The actual synchronous producer has ended and returned every loan.
    /// Its prepared boundary ticket prevents supplier refunds while the caller
    /// still holds its Model/lane. No Epoch survives in the late charge.
    fn destroyBoundRoot(self: *Budget) !void {
        if (self.in_use or self.current != null) return error.RecurrentStateLoanStillLive;
        const charge = self.charge;
        if (charge != null and !self.boundary_held) return error.RecurrentStateBudgetNeverBound;
        self.charge = null;
        self.shell.destroyUnused();
        const metadata = self.metadata;
        metadata.destroy(self);
        if (charge) |owner| owner.release();
    }
    /// Cold rollback only, BEFORE dispatch binding and all numerical lanes.
    pub fn destroyUnusedOutsideExecution(self: *Budget) !void {
        if (self.current != null or self.in_use or self.boundary_held) return error.RecurrentStateBudgetAlreadyBound;
        const charge = self.charge;
        self.charge = null;
        self.shell.destroyUnused();
        const metadata = self.metadata;
        metadata.destroy(self);
        if (charge) |owner| owner.release();
    }
};

test "temporary recurrent peak loans preserve exact quota across sequential returns until actual invocation B seals" {
    const Owner = @import("cleanup_owner.zig").Owner;
    const domain = try admission.Domain.create();
    defer domain.release();
    const Probe = struct {
        gate: *std.atomic.Mutex,
        held: bool = false,
        releases: usize = 0,
        fn reserve(_: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            return 1;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.releases += 1;
            if (!self.gate.tryLock()) self.held = true else self.gate.unlock();
        }
    };
    var gate: std.atomic.Mutex = .unlocked;
    var probe = Probe{ .gate = &gate };
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    const layout = Layout{ .key_heads = 1, .value_heads = 1 };
    const bytes = try layout.bytes();
    var lease: ?memory.AdmissionLease = try domain.controller.tryAcquire(.gpu, .{ .backend_limit_bytes = bytes }, .{ .backend_scratch_bytes = bytes }, false);
    defer if (lease) |*value| value.release();
    var pin: ?admission.Pin = domain.pin();
    defer if (pin) |value| value.release();
    var owner = try Owner.beginOutsideCleanup();
    defer owner.finishAfterUnlock();
    const epoch = owner.epoch().?;
    const budget = try Budget.prepare(layout, 17);
    var budget_live = true;
    defer if (budget_live) {
        if (budget.current != null or budget.boundary_held) budget.retireWithCleanup(.{ .boundary = epoch }) catch unreachable else budget.destroyUnusedOutsideExecution() catch unreachable;
    };
    try budget.arm(&lease, &pin);
    try budget.bind(epoch);
    while (!gate.tryLock()) std.atomic.spinLoopHint();
    var held = true;
    defer if (held) gate.unlock();
    for (0..3) |_| {
        try std.testing.expectError(error.ForeignRecurrentStateBudget, budget.acquire(18, layout));
        try budget.acquire(17, layout);
        try std.testing.expectError(error.RecurrentStateLoanStillLive, budget.acquire(17, layout));
        try std.testing.expectError(error.ForeignRecurrentStateCleanup, budget.returnAfterBackingRetired(.{ .boundary = epoch, .outside_execution = true }));
        try budget.returnAfterBackingRetired(.{ .boundary = epoch });
        try std.testing.expectEqual(bytes, domain.controller.snapshot().backend_scratch_bytes);
        try std.testing.expectEqual(@as(usize, 0), probe.releases);
    }
    gate.unlock();
    held = false;
    try budget.unbind();
    try budget.destroyBoundRoot();
    budget_live = false;
    try std.testing.expectEqual(bytes, domain.controller.snapshot().backend_scratch_bytes);
    owner.finishAfterUnlock();
    try std.testing.expectEqual(@as(usize, 0), domain.controller.snapshot().backend_scratch_bytes);
    try std.testing.expect(!probe.held and probe.releases == 1);
}

test "temporary recurrent layout shares exact original fixed geometry bound and rejects malformed heads" {
    try std.testing.expectEqual(@as(usize, 3 * 128 * 4 * 4 * 3 + 128 * 128 * 4 + 130 * 4), try (Layout{ .key_heads = 1, .value_heads = 1 }).bytes());
    try std.testing.expectError(error.InvalidTensorShape, (Layout{ .key_heads = 0, .value_heads = 1 }).bytes());
    try std.testing.expectError(error.InvalidTensorShape, (Layout{ .key_heads = 2, .value_heads = 3 }).bytes());
    try std.testing.expectError(error.InvalidTensorShape, (Layout{ .key_heads = 1, .value_heads = 257 }).bytes());
}

test "temporary recurrent cold header OOM precedes any quota handoff" {
    var failed = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Budget.prepareWithAllocator(failed.allocator(), .{ .key_heads = 1, .value_heads = 1 }, 1));
    try std.testing.expectEqual(@as(usize, 0), failed.allocated_bytes);
}
