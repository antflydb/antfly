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

//! Stable metadata quota authority. The process/host keeps its anchor until
//! Close completes. Allocations and active builders retain it independently of
//! a backend. Freed capacity stays charged through accounting contention.
const std = @import("std");
const platform = @import("antfly_platform");
const admission = @import("../../common/read_admission.zig");
const resources = @import("../resource_manager.zig");
const Counter = platform.atomic.Value(u64);
pub const slice: resources.Slice = .lsm_registry_metadata;

/// An already acquired ancestor retention. Its producer must keep this exact
/// parent context alive until release_joined. The creator transfers it once;
/// errors from init leave ownership with the caller. Destruction is joined.
pub const ParentLifetime = struct {
    parent: resources.ResourceManager.MemoryParent,
    context: *anyopaque,
    release_joined: *const fn (*anyopaque) void,
};

pub const Owner = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    retirement: *Close,
    mutex: std.atomic.Mutex = .unlocked,
    references: Counter = .init(1),
    freed: Counter = .init(0),
    debt: u64 = 0,
    ready: bool = false,
    closing: bool = false,
    pub fn acquireWithAdmission(self: *Owner, original: admission.Port) !*Owner {
        try @import("../../common/bounded_mutex.zig").lockAtomic(&self.mutex, original);
        defer self.mutex.unlock();
        if (!self.ready or self.closing) return error.BackendRuntimeShuttingDown;
        _ = self.references.fetchAdd(1, .acq_rel);
        return self;
    }
    pub fn retainAllocation(self: *Owner) *Owner {
        std.debug.assert(self.references.fetchAdd(1, .acq_rel) > 0);
        return self;
    }
    pub fn releaseBorrow(self: *Owner) void {
        self.manager.notifyCleanupReady();
        std.debug.assert(self.references.fetchSub(1, .acq_rel) > 1);
    }
    pub fn reportFreed(self: *Owner, bytes: usize) void {
        _ = self.freed.fetchAdd(bytes, .acq_rel);
        self.releaseBorrow();
    }
    pub fn adoptWithAdmission(self: *Owner, credit: *resources.ConstructionCredit, bytes: u64, original: admission.Port) !void {
        try original.checkpoint();
        if (credit.manager != self.manager) return error.ResourceAccountingMismatch;
        while (true) {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&self.mutex, original);
            const result = credit.tryAdoptPreparedUsageWithAdmission(slice, &self.retirement.usage, bytes, original);
            self.mutex.unlock();
            if (try result) return;
            try original.waitForProgress();
        }
    }
    pub fn reconcileWithAdmission(self: *Owner, original: admission.Port) !void {
        while (true) {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&self.mutex, original);
            const pending = self.freed.load(.acquire);
            var busy = false;
            if (pending != 0) switch (self.manager.tryRetireUsageWithRetainedDebt(slice, &self.retirement.usage, &self.debt, pending, struct {
                pub fn retire(_: @This()) void {}
            }{})) {
                .retired => _ = self.freed.fetchSub(pending, .acq_rel),
                .busy => busy = true,
                .mismatch => {
                    self.mutex.unlock();
                    return error.ResourceAccountingMismatch;
                },
            };
            if (!busy and self.debt != 0) switch (self.manager.tryFinishRetirementDebt(slice, &self.retirement.usage, &self.debt)) {
                .released => {},
                .busy => busy = true,
                .unsupported => {
                    self.mutex.unlock();
                    return error.ExecutionWaitUnsupported;
                },
                .mismatch => {
                    self.mutex.unlock();
                    return error.ResourceAccountingMismatch;
                },
            };
            self.mutex.unlock();
            if (!busy) return;
            try original.waitForProgress();
        }
    }
    pub fn reconcileJoined(self: *Owner) void {
        platform.sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const pending = self.freed.load(.acquire);
        if (pending != 0) {
            self.manager.retireUsageToDebtWithAdmission(slice, &self.retirement.usage, &self.debt, pending, joined(), struct {
                pub fn retire(_: @This()) void {}
            }{}) catch unreachable;
            _ = self.freed.fetchSub(pending, .acq_rel);
        }
        if (self.debt != 0) {
            self.manager.adjustUsage(slice, &self.retirement.usage, self.retirement.usage - self.debt) catch unreachable;
            self.debt = 0;
        }
    }
};

/// Caller prepays and registers Creation and Close at stable addresses. Close's
/// observer is prepared before any output can escape: physical teardown never
/// allocates a new identity, even after OOM or the original manager owner exits.
pub const Creation = struct {
    allocator: std.mem.Allocator,
    manager: *resources.ResourceManager,
    retirement: *Close,
    credit: ?resources.ConstructionCredit = null,
    output: ?*Owner = null,
    ready: bool = false,
    taken: bool = false,
    retiring: bool = false,
    retired: bool = false,
    pub fn init(allocator: std.mem.Allocator, manager: *resources.ResourceManager, retirement: *Close, parent: ?ParentLifetime) !@This() {
        if (retirement.manager != null) return error.InvalidArgument;
        if (manager.memory_parent) |port| {
            const guard = parent orelse return error.ExecutionWaitUnsupported;
            if (!guard.parent.valid() or guard.parent.ptr != port.ptr or guard.parent.reserve_fn != port.reserve_fn) return error.InvalidMemoryParent;
        } else if (parent != null) return error.InvalidMemoryParent;
        const lifetime = manager.retainLifetime() orelse return error.ExecutionWaitUnsupported;
        retirement.* = .{ .manager = manager, .lifetime = lifetime, .parent_lifetime = parent };
        return .{ .allocator = allocator, .manager = manager, .retirement = retirement };
    }
    pub fn resumeWithAdmission(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.taken or self.retiring or self.retired) return error.InvalidArgument;
        if (self.ready) return;
        if (self.credit == null) self.credit = try self.manager.reserveConstructionWithAdmission(slice, @sizeOf(Owner), original);
        try self.manager.prepareUsageWithAdmission(slice, &self.retirement.usage, original);
        if (self.output == null) {
            try original.checkpoint();
            const owner = try self.allocator.create(Owner);
            owner.* = .{ .allocator = self.allocator, .manager = self.manager, .retirement = self.retirement };
            self.output = owner;
        }
        try self.credit.?.adoptUsageWithAdmission(slice, &self.retirement.usage, @sizeOf(Owner), original);
        self.credit.?.retire();
        self.credit = null;
        self.output.?.ready = true;
        self.ready = true;
    }
    pub fn takeWithAdmission(self: *@This(), original: admission.Port) !*Owner {
        try original.checkpoint();
        if (!self.ready or self.taken or self.retiring or self.retired) return error.InvalidArgument;
        const owner = self.output.?;
        self.retirement.owner = owner;
        self.retirement.activated = true;
        self.output = null;
        self.taken = true;
        return owner;
    }
    pub fn retireWithAdmission(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.retired or self.taken) return;
        self.retiring = true;
        if (self.output) |owner| {
            if (self.ready) {
                self.retirement.owner = owner;
                self.output = null;
            } else {
                try self.manager.adjustUsageWithAdmission(slice, &self.retirement.usage, 0, original);
                try original.checkpoint();
                self.allocator.destroy(owner);
                self.output = null;
            }
        }
        if (self.credit) |*credit| credit.retire();
        self.credit = null;
        self.retirement.activated = true;
        try self.retirement.resumeWithAdmission(original);
        self.retired = true;
    }
    pub fn deinitJoined(self: *@This()) void {
        if (self.taken) return;
        self.retireWithAdmission(joined()) catch |cause| {
            if (cause != error.ExecutionWaitUnsupported) unreachable;
        };
        self.retirement.deinitJoined();
    }
};

/// Owns the process anchor and the stable observer through physical destruction.
/// Busy/error preserves all ownership. Joined release of the manager/ancestor
/// is separate from admitted metadata callbacks and from physical owner free.
pub const Close = struct {
    owner: ?*Owner = null,
    manager: ?*resources.ResourceManager = null,
    lifetime: ?*resources.SharedResourceManager = null,
    parent_lifetime: ?ParentLifetime = null,
    usage: u64 = 0,
    debt: u64 = 0,
    activated: bool = false,
    claimed: bool = false,
    complete: bool = false,
    pub fn resumeWithAdmission(self: *@This(), original: admission.Port) !void {
        try original.checkpoint();
        if (self.complete) return;
        if (!self.activated) return error.InvalidArgument;
        const manager = self.manager orelse return error.InvalidArgument;
        if (self.owner) |owner| {
            try @import("../../common/bounded_mutex.zig").lockAtomic(&owner.mutex, original);
            if (!self.claimed and owner.closing) {
                owner.mutex.unlock();
                return error.InvalidArgument;
            }
            owner.closing = true;
            self.claimed = true;
            const idle = owner.references.load(.acquire) == 1;
            owner.mutex.unlock();
            if (!idle) return error.ParentMemoryBusy;
            try owner.reconcileWithAdmission(original);
            if (self.usage != @sizeOf(Owner)) return error.ResourceAccountingMismatch;
            // Record debt without returning capacity. Physical destruction
            // must run after unlocking the ledger: an allocator may itself
            // release funded capacity through this same manager.
            try manager.retireUsageToDebtWithAdmission(slice, &self.usage, &self.debt, @sizeOf(Owner), original, struct {
                pub fn retire(_: @This()) void {}
            }{});
            const allocator = owner.allocator;
            allocator.destroy(owner);
            self.owner = null;
        } else if (self.usage == 0 and self.debt == 0) {
            // An abandoned constructor may have prepared only a zero identity.
            try manager.adjustUsageWithAdmission(slice, &self.usage, 0, original);
        }
        while (self.debt != 0) {
            try original.checkpoint();
            switch (manager.tryFinishRetirementDebt(slice, &self.usage, &self.debt)) {
                .released => {},
                .busy => try original.waitForProgress(),
                .unsupported => return error.ExecutionWaitUnsupported,
                .mismatch => return error.ResourceAccountingMismatch,
            }
        }
        self.complete = true;
    }
    pub fn deinitJoined(self: *@This()) void {
        if (self.manager == null) return;
        while (!self.complete) {
            self.resumeWithAdmission(joined()) catch |cause| {
                if (cause == error.ParentMemoryBusy) {
                    joinedWait(&joined_tag) catch unreachable;
                    continue;
                }
                if (cause == error.ExecutionWaitUnsupported) {
                    if (self.owner) |owner| owner.reconcileJoined() else {
                        self.manager.?.adjustUsage(slice, &self.usage, self.usage - self.debt) catch unreachable;
                        self.debt = 0;
                        self.complete = true;
                    }
                    continue;
                }
                unreachable;
            };
        }
        releaseLifetimes(&self.lifetime, &self.parent_lifetime);
    }
};
fn releaseLifetimes(lifetime: *?*resources.SharedResourceManager, parent: *?ParentLifetime) void {
    if (lifetime.*) |owned| owned.release();
    lifetime.* = null;
    if (parent.*) |owned| owned.release_joined(owned.context);
    parent.* = null;
}
var joined_tag: u8 = 0;
fn joinedCheck(_: *anyopaque) !void {}
fn joinedWait(_: *anyopaque) !void {
    std.Thread.yield() catch {};
}
fn joined() admission.Port {
    const Holder = struct {
        const payload: admission.Payload = .{ .ptr = &joined_tag, .vtable = &.{ .checkpoint = joinedCheck, .check_lifetime = joinedCheck, .wait_for_progress = joinedWait } };
    };
    return admission.Port.init(&Holder.payload);
}

const Fault = struct {
    checks: usize = 0,
    at: usize = std.math.maxInt(usize),
    failure: anyerror = error.Canceled,
    fn check(raw: *anyopaque) !void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const at = self.checks;
        self.checks += 1;
        if (at == self.at) return self.failure;
    }
    fn payload(self: *@This()) admission.Payload {
        return .{ .ptr = self, .vtable = &.{ .checkpoint = check, .check_lifetime = check, .wait_for_progress = joinedWait } };
    }
};
fn createForTest(manager: *resources.ResourceManager, retirement: *Close, original: admission.Port) !*Owner {
    var creation = try Creation.init(std.testing.allocator, manager, retirement, null);
    defer creation.deinitJoined();
    try creation.resumeWithAdmission(original);
    return creation.takeWithAdmission(original);
}

test "controlled physical sync pin quota retains constructor adoption close and original errors without freed observer identities" {
    const a = std.testing.allocator;
    for (0..4) |stage| {
        var total: usize = 0;
        var target: usize = 0;
        while (target <= total) : (target += 1) {
            for ([_]bool{ false, true }) |abandon| {
                if (target == 0 and abandon) continue;
                const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a });
                defer manager.releaseOwned(a);
                var close: Close = .{};
                defer close.deinitJoined();
                var creation = try Creation.init(a, manager, &close, null);
                defer creation.deinitJoined();
                var fault: Fault = .{ .failure = switch (target % 3) {
                    0 => error.Canceled,
                    1 => error.DeadlineExceeded,
                    else => error.FileNotFound,
                } };
                const payload = fault.payload();
                const original = admission.Port.init(&payload);
                if (stage != 0) try creation.resumeWithAdmission(original);
                var output: ?*Owner = null;
                if (stage == 3) _ = try creation.takeWithAdmission(original);
                fault.checks = 0;
                fault.at = if (target == 0) std.math.maxInt(usize) else target - 1;
                const result: anyerror!void = switch (stage) {
                    0 => creation.resumeWithAdmission(original),
                    1 => blk: {
                        output = creation.takeWithAdmission(original) catch |err| break :blk err;
                        break :blk {};
                    },
                    2 => creation.retireWithAdmission(original),
                    else => close.resumeWithAdmission(original),
                };
                if (target == 0) {
                    try result;
                    total = fault.checks;
                } else try std.testing.expectError(fault.failure, result);
                if (stage == 3) {
                    try close.resumeWithAdmission(original);
                    try std.testing.expectEqual(@as(u64, 0), close.usage);
                    fault.at = fault.checks;
                    try std.testing.expectError(fault.failure, close.resumeWithAdmission(original));
                } else if (abandon or stage == 2) {
                    try creation.retireWithAdmission(original);
                } else {
                    if (output == null) {
                        try creation.resumeWithAdmission(original);
                        output = try creation.takeWithAdmission(original);
                    }
                    try std.testing.expectEqual(@as(u64, @sizeOf(Owner)), close.usage);
                    try close.resumeWithAdmission(original);
                }
                creation.deinitJoined();
                close.deinitJoined();
                try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(slice).used_bytes);
            }
        }
    }
}

test "controlled physical sync prepared pin quota adoption never waits or allocates under owner locks" {
    const a = std.testing.allocator;
    const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a });
    defer manager.releaseOwned(a);
    var fault: Fault = .{};
    const payload = fault.payload();
    const original = admission.Port.init(&payload);
    var close: Close = .{};
    defer close.deinitJoined();
    const owner = try createForTest(manager, &close, original);
    var credit = try manager.reserveConstructionWithAdmission(.lsm_table_builder_working_set, 37, original);
    defer credit.retire();
    const before = close.usage;
    try std.testing.expect(manager.mutex.tryLock());
    {
        defer manager.mutex.unlock();
        try std.testing.expect(!(try credit.tryAdoptPreparedUsageWithAdmission(slice, &close.usage, 37, original)));
        fault.at = fault.checks;
        try std.testing.expectError(fault.failure, credit.tryAdoptPreparedUsageWithAdmission(slice, &close.usage, 37, original));
        try std.testing.expectEqual(before, close.usage);
    }
    _ = owner.retainAllocation(); // Model the physical allocation's own reference.
    try owner.adoptWithAdmission(&credit, 37, original);
    try std.testing.expectEqual(before + 37, close.usage);
    try std.testing.expectError(error.ParentMemoryBusy, close.resumeWithAdmission(original));
    try std.testing.expectError(error.BackendRuntimeShuttingDown, owner.acquireWithAdmission(original));
    owner.reportFreed(37);
    try owner.reconcileWithAdmission(original);
    try std.testing.expectEqual(before, close.usage);
    credit.retire();
    try close.resumeWithAdmission(original);
}

test "controlled physical sync pin quota holds ancestor lifetime and release debt after physical retirement" {
    const Parent = resources.ResourceManager.MemoryParent;
    const Fake = struct {
        used: u64 = 0,
        busy: bool = false,
        alive: bool = true,
        guard_refs: usize = 0,
        bounded_release: bool = true,
        fn cast(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn reserve(raw: *anyopaque, bytes: u64) callconv(.c) bool {
            const self = cast(raw);
            std.debug.assert(self.alive);
            self.used += bytes;
            return true;
        }
        fn observe(raw: *anyopaque, bytes: u64) callconv(.c) void {
            _ = reserve(raw, bytes);
        }
        fn release(raw: *anyopaque, bytes: u64) callconv(.c) void {
            const self = cast(raw);
            std.debug.assert(self.alive and self.used >= bytes);
            self.used -= bytes;
        }
        fn reclaim(_: *anyopaque, _: u64) callconv(.c) u64 {
            return 0;
        }
        fn snapshot(raw: *anyopaque, out: *@import("runtime_memory_abi").ParentSnapshot) callconv(.c) void {
            out.* = .{ .used_bytes = cast(raw).used, .soft_limit_bytes = 0, .hard_limit_bytes = 0 };
        }
        fn epoch(_: *anyopaque) callconv(.c) u64 {
            return 0;
        }
        fn wait(_: *anyopaque, _: u64) callconv(.c) void {}
        fn notify(_: *anyopaque) callconv(.c) void {}
        fn tryReserve(raw: *anyopaque, bytes: u64) callconv(.c) Parent.ReserveResult {
            if (cast(raw).busy) return .busy;
            _ = reserve(raw, bytes);
            return .reserved;
        }
        fn tryRelease(raw: *anyopaque, bytes: u64) callconv(.c) Parent.ReleaseResult {
            if (cast(raw).busy) return .busy;
            release(raw, bytes);
            return .released;
        }
        fn port(self: *@This()) Parent {
            return .{ .ptr = self, .reserve_fn = reserve, .reclaim_fn = reclaim, .observe_fn = observe, .release_fn = release, .snapshot_fn = snapshot, .epoch_fn = epoch, .wait_fn = wait, .notify_fn = notify, .try_reserve_fn = tryReserve, .try_release_fn = if (self.bounded_release) tryRelease else null };
        }
        fn end(raw: *anyopaque) void {
            const self = cast(raw);
            std.debug.assert(self.used == 0 and self.guard_refs == 1);
            self.guard_refs = 0;
            self.alive = false;
        }
        fn guard(self: *@This()) ParentLifetime {
            self.guard_refs += 1;
            return .{ .parent = self.port(), .context = self, .release_joined = end };
        }
    };
    const RejectWait = struct {
        fn check(_: *anyopaque) !void {}
        fn wait(_: *anyopaque) !void {
            return error.DeadlineExceeded;
        }
    };
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |supported| {
        var parent: Fake = .{ .bounded_release = supported };
        var wrong: Fake = .{};
        const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a });
        var manager_owned = true;
        defer if (manager_owned) manager.releaseOwned(a);
        try manager.installMemoryParent(parent.port());
        var close: Close = .{};
        defer close.deinitJoined();
        try std.testing.expectError(error.ExecutionWaitUnsupported, Creation.init(a, manager, &close, null));
        const invalid = wrong.guard();
        try std.testing.expectError(error.InvalidMemoryParent, Creation.init(a, manager, &close, invalid));
        invalid.release_joined(invalid.context);
        var creator = try Creation.init(a, manager, &close, parent.guard());
        defer creator.deinitJoined();
        try creator.resumeWithAdmission(joined());
        const owner = try creator.takeWithAdmission(joined());
        manager.releaseOwned(a);
        manager_owned = false;
        var credit = try manager.reserveConstructionWithAdmission(slice, 43, joined());
        defer credit.retire();
        _ = owner.retainAllocation();
        try owner.adoptWithAdmission(&credit, 43, joined());
        credit.retire();
        owner.reportFreed(43);
        parent.busy = true;
        var tag: u8 = 0;
        const payload: admission.Payload = .{ .ptr = &tag, .vtable = &.{ .checkpoint = RejectWait.check, .check_lifetime = RejectWait.check, .wait_for_progress = RejectWait.wait } };
        const original = admission.Port.init(&payload);
        const charged = parent.used;
        const expected = if (supported) error.DeadlineExceeded else error.ExecutionWaitUnsupported;
        try std.testing.expectError(expected, owner.reconcileWithAdmission(original));
        try std.testing.expectEqual(charged, parent.used);
        try std.testing.expectEqual(@as(u64, 43), owner.debt);
        try std.testing.expectEqual(@as(u64, @sizeOf(Owner) + 43), close.usage);
        try std.testing.expectError(expected, close.resumeWithAdmission(original));
        try std.testing.expect(parent.alive and parent.guard_refs == 1);
        parent.busy = false;
        if (supported) {
            try close.resumeWithAdmission(joined());
            try std.testing.expectEqual(@as(u64, 0), close.usage);
        } else {
            try std.testing.expectError(error.ExecutionWaitUnsupported, close.resumeWithAdmission(joined()));
            try std.testing.expectEqual(charged, parent.used);
        }
        try std.testing.expect(parent.alive); // Manager and ancestor are still retained.
        close.deinitJoined();
        try std.testing.expect(!parent.alive and parent.used == 0 and parent.guard_refs == 0);
    }
}

test "controlled physical sync pin quota OOM teardown uses its preprepared retirement identity" {
    const Fixture = struct {
        fn run(a: std.mem.Allocator) !void {
            const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a });
            defer manager.releaseOwned(a);
            var close: Close = .{};
            defer close.deinitJoined();
            var creator = try Creation.init(a, manager, &close, null);
            defer creator.deinitJoined();
            try creator.resumeWithAdmission(joined());
            _ = try creator.takeWithAdmission(joined());
            try close.resumeWithAdmission(joined());
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
    var borrowed = resources.ResourceManager.init(.{ .identity_allocator = std.testing.allocator });
    defer borrowed.deinit(std.testing.allocator);
    var close: Close = .{};
    try std.testing.expectError(error.ExecutionWaitUnsupported, Creation.init(std.testing.allocator, &borrowed, &close, null));
}

test "controlled physical sync pin quota header free keeps its charge outside accounting locks with budgeted allocator reentry" {
    const a = std.testing.allocator;
    const Backing = struct {
        manager: *resources.ResourceManager,
        frees: usize = 0,
        fn alloc(_: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            return std.testing.allocator.rawAlloc(len, alignment, ra);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const unlocked = self.manager.mutex.tryLock();
            std.debug.assert(unlocked);
            self.manager.mutex.unlock();
            std.debug.assert(self.manager.sliceStats(slice).used_bytes >= @sizeOf(Owner));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.frees += 1;
        }
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
    };
    for ([_]bool{ false, true }) |budgeted| {
        const manager = try resources.SharedResourceManager.create(a, .{ .identity_allocator = a });
        defer manager.releaseOwned(a);
        var backing: Backing = .{ .manager = manager };
        var budget = resources.BudgetedAllocator.init(manager, .lsm_in_memory_state, backing.allocator(), 1);
        defer budget.deinit();
        var close: Close = .{};
        defer close.deinitJoined();
        var creator = try Creation.init(if (budgeted) budget.threadSafeNoReclaimAllocator() else backing.allocator(), manager, &close, null);
        defer creator.deinitJoined();
        try creator.resumeWithAdmission(joined());
        _ = try creator.takeWithAdmission(joined());
        try close.resumeWithAdmission(joined());
        try std.testing.expectEqual(@as(usize, 1), backing.frees);
        try std.testing.expectEqual(@as(u64, 0), close.usage);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_in_memory_state).used_bytes);
        try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    }
}
