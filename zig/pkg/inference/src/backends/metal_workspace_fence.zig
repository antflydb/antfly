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
    metadata_charge: ?*retirement.Charge = null,
    registration: Backing.UseRegistration = .{},
    retirement_only: bool = false,

    pub fn prepare() !*Fence {
        return prepareWithAllocator(platform.allocator.processAllocator(platform.allocator.concurrentFallback()));
    }
    fn prepareWithAllocator(a: std.mem.Allocator) !*Fence {
        const self = try a.create(Fence);
        self.* = .{ .allocator = a };
        return self;
    }
    /// Cold caller prepares/funds this cell's actual process metadata. Its
    /// independent node reference may outlive the original bank and Slot.
    pub fn bindMetadataChargeOutsideExecution(self: *Fence, charge: *retirement.Charge) !void {
        if (!self.isIdle() or self.metadata_charge != null) return error.WorkspaceFenceBusy;
        self.metadata_charge = charge.retainOwner();
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
    /// No owner/list/SDK mutation is permitted until EVERY permanent-close
    /// cell passes this preflight. No callback, wait or allocation occurs here.
    pub fn preflightClosingRetirement(self: *const Fence, backing: *Backing, current: Cleanup) !void {
        const epoch = current.boundary orelse return error.MissingWorkspaceCleanupEpoch;
        if (current.outside_execution or try epoch.isSealed()) return error.SealedWorkspaceCleanupEpoch;
        if (!self.isIdle() or self.backing != null or self.epoch != null or self.registration.owner != null or self.retirement_only) return error.WorkspaceFenceBusy;
        try backing.preflightClosingUse();
    }
    /// Serialized permanent-close owner only. This pin may join an in-flight
    /// OUTSIDE shrink, and deliberately never enters the numerical-use list.
    pub fn bindClosingBeforeRetirement(self: *Fence, backing: *Backing) !void {
        if (self.state.cmpxchgStrong(.idle, .preparing, .acq_rel, .acquire) != null) return error.WorkspaceFenceBusy;
        errdefer self.state.store(.idle, .release);
        try backing.beginClosingUseBeforeRetirement();
        self.backing = backing;
        self.retirement_only = true;
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
        const backing = self.backing orelse return error.InvalidWorkspaceFence;
        if (!self.retirement_only) try backing.registerUseEpoch(&self.registration, &held);
        errdefer if (!self.retirement_only) backing.unregisterUseEpoch(&self.registration) catch platform.inference_process_supervisor.restartWorker();
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
    pub fn isIdle(self: *const Fence) bool {
        return self.state.load(.acquire) == .idle;
    }
    /// Synchronous use-capability check only. Caller owns the supplied actual
    /// unsealed epoch and exclusive bank custody until numerical return; no
    /// same-cell mutation or epoch finish may race that scope. No B is inferred
    /// from an older root, thread, Engine, or merely nonzero active-use count.
    pub fn validatesUse(self: *const Fence, backing: *const Backing, actual: *const sink.Epoch) bool {
        if (actual.isSealed() catch return false) return false;
        if (self.retirement_only or self.state.load(.acquire) != .queued) return false;
        const held = self.epoch orelse return false;
        return self.backing == backing and held.record == actual.record and held.nonce == actual.nonce and backing.hasUseEpoch(actual);
    }
    fn finish(raw: *anyopaque) callconv(.c) void {
        const self: *Fence = @ptrCast(@alignCast(raw));
        std.debug.assert(self.state.load(.acquire) == .queued);
        self.state.store(.draining, .release);
        const backing = self.backing.?;
        var epoch = self.epoch.?;
        self.backing = null;
        self.epoch = null;
        if (self.retirement_only) {
            self.retirement_only = false;
            backing.endClosingUseOutsideExecution();
        } else {
            backing.unregisterUseEpoch(&self.registration) catch platform.inference_process_supervisor.restartWorker();
            backing.endUseOutsideExecution();
        }
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
        const charge = self.metadata_charge;
        self.allocator.destroy(self);
        if (charge) |owner| owner.release();
    }
    /// Cold cancellation or bank's known outside teardown. A queued node owns
    /// an independent ref and may outlive the bank without any source borrow.
    pub fn deinitOutsideExecution(self: *Fence) void {
        if (self.state.cmpxchgStrong(.bound, .draining, .acq_rel, .acquire) == null) {
            const backing = self.backing.?;
            self.backing = null;
            if (self.retirement_only) {
                self.retirement_only = false;
                backing.endClosingUseOutsideExecution();
            } else backing.endUseOutsideExecution();
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

test "workspace intrusive use registration names exact initial and foreign B and unlinks before warm reuse" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("../execution/storage_admission.zig");
    const Probe = struct {
        owner: ?*Backing = null,
        deaths: usize = 0,
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const owner = self.owner.?;
            std.debug.assert(owner.registered_uses == null);
            std.debug.assert(owner.mutex.tryLock());
            owner.mutex.unlock();
            self.deaths += 1;
        }
    };
    var probe: Probe = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    var reservation: ?storage.Reservation = .{ .identity = .{ .controller = &controller, .backend_class = .gpu }, .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null }, .domain = .backend };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var owner: Backing = undefined;
    owner.init(charge, .{ .context = &probe, .release = Probe.release });
    probe.owner = &owner;
    const initial = try Fence.prepareWithAllocator(std.testing.allocator);
    defer initial.deinitOutsideExecution();
    const foreign = try Fence.prepareWithAllocator(std.testing.allocator);
    defer foreign.deinitOutsideExecution();
    try initial.bindInitialBackingBeforePublication(&owner);
    try foreign.bindBackingOutsideExecution(&owner);
    var source = try sink.Ref.create();
    defer source.release();
    var origin = try source.enter();
    defer origin.finish();
    var other = try source.enter();
    defer other.finish();
    var a = try origin.retainEpoch();
    defer a.release();
    var b = try other.retainEpoch();
    defer b.release();
    try std.testing.expect(!owner.hasUseEpoch(&a) and !owner.hasUseEpoch(&b));
    try initial.retire(.{ .boundary = &a });
    try std.testing.expect(owner.hasUseEpoch(&a) and !owner.hasUseEpoch(&b));
    try std.testing.expectError(error.BackingUseAlreadyRegistered, owner.registerUseEpoch(&initial.registration, &b));
    try std.testing.expect(owner.hasUseEpoch(&a) and !owner.hasUseEpoch(&b));
    try foreign.retire(.{ .boundary = &b });
    try std.testing.expect(owner.hasUseEpoch(&a) and owner.hasUseEpoch(&b));
    var forged = b;
    forged.nonce +%= 1;
    try std.testing.expect(!owner.hasUseEpoch(&forged));
    owner.measure(&a, 32);
    origin.finish();
    try std.testing.expect(!owner.hasUseEpoch(&a) and owner.hasUseEpoch(&b));
    var refused: Backing.UseRegistration = .{};
    try std.testing.expectError(error.SealedBackingUseEpoch, owner.registerUseEpoch(&refused, &a));
    try std.testing.expect(refused.owner == null and refused.record == null and refused.next == null and refused.nonce == 0);
    try std.testing.expectError(error.ForeignBackingUseRegistration, owner.unregisterUseEpoch(&refused));
    try std.testing.expect(owner.registered_uses == &foreign.registration);
    try std.testing.expect(initial.isIdle() and initial.registration.owner == null);
    try std.testing.expectEqual(@as(usize, 64), controller.snapshot().backend_scratch_bytes);
    other.finish();
    try std.testing.expect(owner.registered_uses == null and foreign.registration.owner == null);
    try std.testing.expectEqual(@as(usize, 32), controller.snapshot().backend_scratch_bytes);
    var again = try source.enter();
    defer again.finish();
    var now = try again.retainEpoch();
    defer now.release();
    try initial.bindBackingOutsideExecution(&owner);
    try initial.retire(.{ .boundary = &now });
    try std.testing.expect(owner.hasUseEpoch(&now));
    owner.release(&now);
    try std.testing.expectEqual(@as(usize, 0), probe.deaths);
    again.finish();
    try std.testing.expect(initial.registration.owner == null and owner.registered_uses == null);
    try std.testing.expectEqual(@as(usize, 1), probe.deaths);
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
}

test "workspace permanent close joins in-flight outside padding without numerical authority or callback wait" {
    if (comptime @import("build_options").enable_wasm) return error.SkipZigTest;
    const memory = @import("../runtime/tier/memory.zig");
    const storage = @import("../execution/storage_admission.zig");
    const Probe = struct {
        owner: ?*Backing = null,
        entered: std.atomic.Value(bool) = .init(false),
        proceed: std.atomic.Value(bool) = .init(false),
        gate: std.atomic.Mutex = .unlocked,
        unsafe: std.atomic.Value(bool) = .init(false),
        deaths: std.atomic.Value(usize) = .init(0),
        fn check(self: *@This()) void {
            if (self.gate.tryLock()) self.gate.unlock() else self.unsafe.store(true, .release);
            if (self.owner) |owner| {
                if (owner.mutex.tryLock()) owner.mutex.unlock() else self.unsafe.store(true, .release);
            }
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            return 1;
        }
        fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            self.entered.store(true, .release);
            while (!self.proceed.load(.acquire)) std.Thread.yield() catch {};
            self.check();
        }
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
        }
        fn metadata(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.check();
            _ = self.deaths.fetchAdd(1, .release);
        }
    };
    var probe: Probe = .{};
    var controller: memory.AdmissionController = .{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var reservation: ?storage.Reservation = .{ .identity = .{ .controller = &controller, .backend_class = .gpu }, .leases = .{ try controller.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false), null }, .domain = .backend };
    var domain: ?@import("../runtime/tier/admission_domain.zig").Pin = null;
    const charge = try retirement.Charge.createReservation(&reservation, &domain);
    var owner: Backing = undefined;
    owner.init(charge, .{ .context = &probe, .release = Probe.metadata });
    probe.owner = &owner;
    var physical_live = true;
    defer if (physical_live) owner.releaseOutsideExecution();
    const close = try Fence.prepareWithAllocator(std.testing.allocator);
    defer close.deinitOutsideExecution();
    var source = try sink.Ref.create();
    defer source.release();
    var origin = try source.enter();
    defer origin.finish();
    var closing = try source.enter();
    defer closing.finish();
    var a = try origin.retainEpoch();
    defer a.release();
    var b = try closing.retainEpoch();
    defer b.release();
    owner.measure(&a, 32);
    const Worker = struct {
        fn run(guard: *sink.Guard) void {
            guard.finish();
        }
    };
    const worker = try std.Thread.spawn(.{}, Worker.run, .{&origin});
    var joined = false;
    defer if (!joined) {
        probe.proceed.store(true, .release);
        worker.join();
    };
    var attempts: usize = 0;
    while (!probe.entered.load(.acquire)) : (attempts += 1) {
        if (attempts == 100_000) return error.ExpectedOutsideShrinkCallback;
        std.Thread.yield() catch {};
    }
    try std.testing.expect(probe.gate.tryLock());
    var gate_owned = true;
    defer if (gate_owned) probe.gate.unlock();
    try close.preflightClosingRetirement(&owner, .{ .boundary = &b });
    try close.bindClosingBeforeRetirement(&owner); // returns while callback is still paused
    try close.retire(.{ .boundary = &b });
    try std.testing.expect(!owner.hasUseEpoch(&b) and !close.validatesUse(&owner, &b));
    try std.testing.expectEqual(@as(usize, 0), owner.active_uses);
    try std.testing.expectEqual(@as(usize, 1), owner.closing_uses);
    try std.testing.expectError(error.BackingClosing, owner.beginUseOutsideExecution());
    owner.releaseOutsideExecution();
    physical_live = false;
    probe.gate.unlock();
    gate_owned = false;
    probe.proceed.store(true, .release);
    // Never wait while the actual closing owner gate is held. The pure close
    // pin remains active after this already-running outside callback finishes.
    worker.join();
    joined = true;
    try std.testing.expectEqual(@as(usize, 32), owner.admittedBytes());
    try std.testing.expectEqual(@as(usize, 32), controller.snapshot().backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 0), probe.deaths.load(.acquire));
    closing.finish();
    try std.testing.expectEqual(@as(usize, 1), probe.deaths.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), controller.snapshot().backend_scratch_bytes);
    try std.testing.expect(!probe.unsafe.load(.acquire));
}
