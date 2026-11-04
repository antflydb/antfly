// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! One cold reusable intrusive fence per independently retiring owner/scope.
//! Distinct foreign boundaries NEVER share an active node, including aliases
//! of the SAME Ref. Actual capacity must be declared before publication; no
//! hot allocation fallback. Callback custody is pure BackingOwner/Epoch/process
//! metadata; no SDK, Model, provider, IO, Arena or captured numerical operation.
const std = @import("std");
const retirement = @import("../execution/resource_retirement.zig");
const sink = @import("../execution/retirement_sink.zig");
const Backing = @import("../execution/backing_retirement.zig").BackingOwner;
const Cleanup = @import("../execution/cleanup_context.zig").Context;
const platform = @import("antfly_platform");

pub const Fence = struct {
    const State = enum(u8) { idle, preparing, bound, queued, draining };
    allocator: std.mem.Allocator,
    references: std.atomic.Value(usize) = .init(1),
    state: std.atomic.Value(State) = .init(.idle),
    backing: ?*Backing = null,
    epoch: ?sink.Epoch = null,
    node: retirement.Node = undefined,

    pub fn prepare() !*Fence {
        return prepareWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()));
    }
    fn prepareWithAllocator(a: std.mem.Allocator) !*Fence {
        const self = try a.create(Fence);
        self.* = .{ .allocator = a };
        return self;
    }
    /// Caller owns its independent bank/cell reference throughout this method.
    /// Cold outside only: a busy padding callback refuses before numerical
    /// ownership; a use blocks BOTH final credit and pending padding refunds.
    pub fn bindBackingOutsideExecution(self: *Fence, backing: *Backing) !void {
        if (self.state.cmpxchgStrong(.idle, .preparing, .acq_rel, .acquire) != null) return error.WorkspaceFenceBusy;
        errdefer self.state.store(.idle, .release);
        try backing.beginUseOutsideExecution();
        self.backing = backing;
        self.state.store(.bound, .release);
    }
    /// Initial root constraint, before the actual allocator creates any SDK
    /// object. Slot and this cell were already prepared outside all lanes.
    pub fn bindInitialBackingBeforePublication(self: *Fence, backing: *Backing) !void {
        if (self.state.cmpxchgStrong(.idle, .preparing, .acq_rel, .acquire) != null) return error.WorkspaceFenceBusy;
        errdefer self.state.store(.idle, .release);
        try backing.beginInitialUseBeforePublication();
        self.backing = backing;
        self.state.store(.bound, .release);
    }
    /// Caller has EXCLUSIVE mutation custody through bind/retire/cancellation;
    /// same-cell deinit must not race this method. Different foreign scopes
    /// use different cells, including aliases of one Ref. Refcount protects
    /// only the independently queued Node AFTER publication, not concurrent
    /// mutation of a shared bank reference. Register BEFORE dropping ownership
    /// or entering numerics; Node may then outlive bank/Reader/Model owner.
    pub fn retire(self: *Fence, current: Cleanup) !void {
        if (self.state.cmpxchgStrong(.bound, .preparing, .acq_rel, .acquire) != null) return error.InvalidWorkspaceFence;
        errdefer self.state.store(.bound, .release);
        const epoch = current.boundary orelse return error.MissingWorkspaceCleanupEpoch;
        if (try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        var held = try epoch.retain();
        errdefer held.release();
        const boundary = try held.boundary();
        var before = self.references.load(.monotonic);
        while (true) {
            if (before == 0 or before == std.math.maxInt(usize)) return error.WorkspaceFenceReferenceOverflow;
            before = self.references.cmpxchgWeak(before, before + 1, .monotonic, .monotonic) orelse break;
        }
        self.epoch = held;
        self.node = .{ .context = self, .release_fn = finish };
        self.state.store(.queued, .release);
        boundary.register(boundary.context, boundary.nonce, &self.node) catch |err| {
            self.epoch = null;
            self.state.store(.bound, .release);
            self.releaseReference();
            return err;
        };
        // Registration can transfer to another finishing thread. No self read.
    }
    fn finish(raw: *anyopaque) callconv(.c) void {
        const self: *Fence = @ptrCast(@alignCast(raw));
        std.debug.assert(self.state.load(.acquire) == .queued);
        self.state.store(.draining, .release);
        const backing = self.backing.?;
        var epoch = self.epoch.?;
        self.backing = null;
        self.epoch = null;
        backing.endUseOutsideExecution();
        epoch.release();
        // Only after ALL external callbacks have finished may a warm bank
        // reuse the detached node. This invocation touches only refs afterward.
        self.state.store(.idle, .release);
        self.releaseReference();
    }
    fn releaseReference(self: *Fence) void {
        const before = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(before != 0);
        if (before != 1) return;
        std.debug.assert(self.state.load(.acquire) == .idle and self.backing == null and self.epoch == null);
        self.allocator.destroy(self);
    }
    /// Cold cancellation or bank's known outside teardown. A queued node owns
    /// an independent ref and may outlive the bank without any source borrow.
    pub fn deinitOutsideExecution(self: *Fence) void {
        if (self.state.cmpxchgStrong(.bound, .draining, .acq_rel, .acquire) == null) {
            const backing = self.backing.?;
            self.backing = null;
            backing.endUseOutsideExecution();
            self.state.store(.idle, .release);
        }
        self.releaseReference();
    }
};

test "workspace foreign use fences protect padding refunds and final quota in both seal orders" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("../execution/storage_admission.zig");
    inline for (.{ false, true }) |foreign_first| {
        const Probe = struct {
            owner: ?*Backing = null,
            a: std.atomic.Mutex = .unlocked,
            b: std.atomic.Mutex = .unlocked,
            unsafe: bool = false,
            retains: usize = 0,
            releases: usize = 0,
            metadata_deaths: usize = 0,
            fn check(self: *@This()) void {
                if (self.a.tryLock()) self.a.unlock() else self.unsafe = true;
                if (self.b.tryLock()) self.b.unlock() else self.unsafe = true;
                if (self.owner) |owner| {
                    if (owner.mutex.tryLock()) owner.mutex.unlock() else self.unsafe = true;
                }
            }
            fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.check();
                return 7;
            }
            fn retain(raw: *anyopaque, token: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                std.debug.assert(token == 7);
                self.check();
                self.retains += 1;
                // Reentry cannot acquire a new use during a pending supplier
                // callback; refusal changes no owner/ref state and holds no
                // owner mutex. Caller retries only outside numerical lanes.
                if (self.owner) |owner| {
                    if (owner.beginUseOutsideExecution()) |_| {
                        self.unsafe = true;
                        owner.endUseOutsideExecution();
                    } else |err| std.debug.assert(err == error.ResourceTemporarilyUnavailable or err == error.BackingAlreadyRetired);
                }
            }
            fn release(raw: *anyopaque, token: usize) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                std.debug.assert(token == 7);
                self.check();
                self.releases += 1;
            }
            fn metadata(raw: *anyopaque) void {
                const self: *@This() = @ptrCast(@alignCast(raw));
                self.check();
                self.metadata_deaths += 1;
            }
        };
        var probe: Probe = .{};
        var controller: memory.AdmissionController = .{};
        defer controller.deinit();
        try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
        var reservation: ?storage.Reservation = .{
            .identity = .{ .controller = &controller, .backend_class = .gpu },
            .leases = .{ try controller.tryAcquire(.gpu, .{ .backend_limit_bytes = 64 }, .{ .backend_scratch_bytes = 64 }, false), null },
            .domain = .backend,
        };
        var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
        const charge = try retirement.Charge.createReservation(&reservation, &domain);
        var owner: Backing = undefined;
        owner.init(charge, .{ .context = &probe, .release = Probe.metadata });
        probe.owner = &owner;
        const foreign = try Fence.prepareWithAllocator(std.testing.allocator);
        defer foreign.deinitOutsideExecution();
        try foreign.bindBackingOutsideExecution(&owner);
        var reference = try sink.Ref.create();
        defer reference.release();
        var guard_a = try reference.enter();
        defer guard_a.finish();
        var guard_b = try reference.enter();
        defer guard_b.finish();
        var a = try guard_a.retainEpoch();
        defer a.release();
        var b = try guard_b.retainEpoch();
        defer b.release();
        try foreign.retire(.{ .boundary = &b }); // before actual foreign lane
        platform.sync.lockYielding(&probe.a);
        var a_locked = true;
        defer if (a_locked) probe.a.unlock();
        platform.sync.lockYielding(&probe.b);
        var b_locked = true;
        defer if (b_locked) probe.b.unlock();
        owner.measure(&a, 32);
        owner.release(&a); // physical cleanup has its OWN current A
        if (foreign_first) {
            probe.b.unlock();
            b_locked = false;
            guard_b.finish();
        } else {
            probe.a.unlock();
            a_locked = false;
            guard_a.finish();
        }
        try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
        try std.testing.expectEqual(@as(usize, 0), probe.retains);
        try std.testing.expectEqual(@as(usize, 0), probe.releases);
        if (foreign_first) {
            probe.a.unlock();
            a_locked = false;
            guard_a.finish();
        } else {
            probe.b.unlock();
            b_locked = false;
            guard_b.finish();
        }
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
        try std.testing.expectEqual(@as(usize, 1), probe.retains);
        try std.testing.expectEqual(@as(usize, 1), probe.releases);
        try std.testing.expectEqual(@as(usize, 1), probe.metadata_deaths);
        try std.testing.expect(!probe.unsafe);
    }
}

test "workspace use fence reuses cold node without warm allocation and survives bank close before seal" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("../execution/storage_admission.zig");
    const Metadata = struct {
        deaths: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.deaths += 1;
        }
    };
    var metadata: Metadata = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var reservation: ?storage.Reservation = .{
        .identity = .{ .controller = &controller, .backend_class = .gpu },
        .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null },
        .domain = .backend,
    };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var owner: Backing = undefined;
    owner.init(charge, .{ .context = &metadata, .release = Metadata.release });
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
    const fence = try Fence.prepareWithAllocator(failing.allocator());
    var bank_owned = true;
    defer if (bank_owned) fence.deinitOutsideExecution();
    var reference = try sink.Ref.create();
    defer reference.release();
    for (0..3) |round| {
        try fence.bindBackingOutsideExecution(&owner);
        var guard = try reference.enter();
        defer guard.finish();
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        try fence.retire(.{ .boundary = &epoch });
        try std.testing.expectError(error.WorkspaceFenceBusy, fence.bindBackingOutsideExecution(&owner));
        if (round == 2) {
            fence.deinitOutsideExecution(); // only Node retains cell now
            bank_owned = false;
        }
        guard.finish();
        try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
        try std.testing.expectEqual(@as(usize, 1), failing.allocations);
        if (bank_owned) try std.testing.expect(fence.state.load(.acquire) == .idle);
    }
    try std.testing.expectEqual(@as(usize, 1), failing.deallocations);
    owner.releaseOutsideExecution();
    try std.testing.expectEqual(@as(usize, 1), metadata.deaths);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "workspace use fence refuses missing sealed authority failure atomically and cold OOM" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, Fence.prepareWithAllocator(failing.allocator()));
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("../execution/storage_admission.zig");
    const Metadata = struct {
        fn release(_: *anyopaque) void {}
    };
    var marker: u8 = 0;
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var reservation: ?storage.Reservation = .{
        .identity = .{ .controller = &controller, .backend_class = .gpu },
        .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null },
        .domain = .backend,
    };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var owner: Backing = undefined;
    owner.init(charge, .{ .context = &marker, .release = Metadata.release });
    const fence = try Fence.prepareWithAllocator(std.testing.allocator);
    try fence.bindBackingOutsideExecution(&owner);
    var reference = try sink.Ref.create();
    defer reference.release();
    var guard = try reference.enter();
    defer guard.finish();
    var epoch = try guard.retainEpoch();
    defer epoch.release();
    guard.finish();
    try std.testing.expectError(error.MissingWorkspaceCleanupEpoch, fence.retire(.{}));
    try std.testing.expectError(error.SealedWorkspaceCleanupEpoch, fence.retire(.{ .boundary = &epoch }));
    try std.testing.expect(fence.state.load(.acquire) == .bound and fence.epoch == null and fence.backing == &owner);
    try std.testing.expectEqual(@as(usize, 1), owner.active_uses);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    fence.deinitOutsideExecution();
    try std.testing.expectEqual(@as(usize, 0), owner.active_uses);
    owner.releaseOutsideExecution();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
