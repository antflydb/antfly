// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Lifetime ownership of prepared buffer reservations. Invocation capabilities
//! borrow their run permit synchronously; returned reservations own independent
//! tokens and a controller-lifetime reference across plan/view retirement.
const std = @import("std");
const memory = @import("../runtime/tier/memory.zig");
const BackendKind = @import("../graph/backend_contracts.zig").BackendKind;
const Class = @import("storage_accounting.zig").Class;

/// A borrowed capability for the controller's actual owner. Every successful
/// storage reservation retains it once; its release follows all token releases.
pub const Lifetime = struct {
    context: *anyopaque,
    retain: ?*const fn (*anyopaque) bool = null,
    /// A pin already held by the caller may expose an infallible clone. Store
    /// its real owner pointer/callbacks, never the address of a stack wrapper.
    retain_infallible: ?*const fn (*anyopaque) void = null,
    release: *const fn (*anyopaque) void,

    pub fn isValid(self: Lifetime) bool {
        return (self.retain != null) != (self.retain_infallible != null);
    }

    pub fn acquire(self: Lifetime) bool {
        if (!self.isValid()) return false;
        if (self.retain) |take| return take(self.context);
        self.retain_infallible.?(self.context);
        return true;
    }
};

pub const Policy = struct {
    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,
    limits: memory.Limits,
    check_live_memory: bool,
    lifetime: ?Lifetime = null,
    /// Only physical scratch, excluding host IO/retained-output protection.
    /// This pointer is borrowed during the synchronous reservation callback.
    /// Absence supplies no credit, never unrestricted access to the run peak.
    credit: ?*memory.AdmissionAmounts = null,
};

pub const Identity = struct {
    controller: *memory.AdmissionController,
    backend_class: memory.BackendClass,

    pub fn eql(self: Identity, other: Identity) bool {
        return self.controller == other.controller and self.backend_class == other.backend_class;
    }
};

pub const Capability = struct {
    identity: Identity,
    context: *anyopaque,
    reserve_fn: *const fn (*anyopaque, std.mem.Allocator, BackendKind, Class, usize) anyerror!Reservation,

    pub fn reserve(self: Capability, allocator: std.mem.Allocator, backend: BackendKind, class: Class, bytes: usize) !Reservation {
        return self.reserve_fn(self.context, allocator, backend, class, bytes);
    }
};

const Domain = enum { host, backend };

pub const Reservation = struct {
    /// Credit from the current request and an independently admitted deficit
    /// remain separate exactly-once tokens. Never reconstruct/merge external
    /// process-owner handles from their byte amounts.
    identity: Identity,
    leases: [2]?memory.AdmissionLease = @splat(null),
    lifetime: ?Lifetime = null,
    domain: Domain = .host,

    pub fn release(self: *Reservation) void {
        const lifetime = self.lifetime;
        self.lifetime = null;
        for (&self.leases) |*lease| {
            if (lease.*) |*owned| owned.release();
            lease.* = null;
        }
        if (lifetime) |owner| owner.release(owner.context);
    }

    /// Shrink conservative padding after measuring the actual owned span.
    /// A provider refusing a shrink retains the larger reservation safely.
    pub fn retainBytes(self: *Reservation, bytes: usize) void {
        var remaining = bytes;
        for (&self.leases) |*lease| {
            const owned = if (lease.*) |*value| value else continue;
            const available = domainBytes(owned.amounts, self.domain);
            const wanted = @min(remaining, available);
            owned.retain(amountsForDomain(self.domain, wanted)) catch {};
            remaining -= wanted;
        }
        std.debug.assert(remaining == 0);
    }

    pub fn amounts(self: *const Reservation) memory.AdmissionAmounts {
        return amountsForDomain(self.domain, self.reservedBytes());
    }

    pub fn reservedBytes(self: *const Reservation) usize {
        var bytes: usize = 0;
        for (self.leases) |lease| if (lease) |owned| {
            bytes = std.math.add(usize, bytes, domainBytes(owned.amounts, self.domain)) catch @panic("invalid storage reservation");
        };
        return bytes;
    }
};

fn domainFor(backend: BackendKind, class: Class) !Domain {
    if (class == .host_workspace) return .host;
    return switch (backend) {
        .native => .host,
        .metal, .cuda => .backend,
        else => error.UnsupportedPhysicalStorageAdmission,
    };
}

fn domainBytes(amounts: memory.AdmissionAmounts, domain: Domain) usize {
    return switch (domain) {
        .host => amounts.host_scratch_bytes,
        .backend => amounts.backend_scratch_bytes,
    };
}

fn amountsForDomain(domain: Domain, bytes: usize) memory.AdmissionAmounts {
    return switch (domain) {
        .host => .{ .host_scratch_bytes = bytes },
        .backend => .{ .backend_scratch_bytes = bytes },
    };
}

/// Partition existing request credit, admitting only a missing physical peak.
/// A denied deficit or failed partition leaves the original lease unchanged.
/// The backend must allocate only after success, and abort rather than retry
/// an execution whose subsequent allocation fails under its reduced permit.
pub fn fromRunLease(allocator: std.mem.Allocator, source: *memory.AdmissionLease, policy: Policy, backend: BackendKind, class: Class, bytes: usize) !Reservation {
    if (bytes == 0) return error.InvalidStorageAdmission;
    if (source.controller != policy.controller or source.retain_backend_class != policy.backend_class) return error.InvalidStorageAdmission;
    const domain = try domainFor(backend, class);
    if (policy.lifetime) |owner| {
        if (!owner.isValid()) return error.InvalidStorageLifetime;
        if (!owner.acquire()) return error.ResourceOwnerShuttingDown;
    }
    errdefer if (policy.lifetime) |owner| owner.release(owner.context);
    const allowed = if (policy.credit) |remaining| domainBytes(remaining.*, domain) else 0;
    const credit = @min(bytes, @min(domainBytes(source.amounts, domain), allowed));
    const deficit = bytes - credit;
    var supplemental: ?memory.AdmissionLease = if (deficit != 0)
        try policy.controller.tryAcquire(policy.backend_class, policy.limits, amountsForDomain(domain, deficit), policy.check_live_memory)
    else
        null;
    errdefer if (supplemental) |*lease| lease.release();
    const partitioned = if (credit != 0) try source.partition(allocator, amountsForDomain(domain, credit)) else null;
    // Partition is the final fallible step. Protected IO/output quota stays
    // with the run permit and a failed deficit never consumes this allowance.
    if (policy.credit) |remaining| switch (domain) {
        .host => remaining.host_scratch_bytes -= credit,
        .backend => remaining.backend_scratch_bytes -= credit,
    };
    return .{ .identity = .{ .controller = policy.controller, .backend_class = policy.backend_class }, .leases = .{ partitioned, supplemental }, .lifetime = policy.lifetime, .domain = domain };
}

test "physical storage admission transfers exact credit and retains only the missing peak" {
    const Owner = struct {
        references: usize = 1,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.references > 1);
            self.references -= 1;
        }
    };
    const a = std.testing.allocator;
    var owner = Owner{};
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    const limits = memory.Limits{ .backend_limit_bytes = 96, .combined_limit_bytes = 128 };
    var source = try controller.tryAcquire(.gpu, limits, .{ .host_scratch_bytes = 32, .backend_scratch_bytes = 64 }, false);
    defer source.release();
    var credit = memory.AdmissionAmounts{ .backend_scratch_bytes = 64 };
    const policy = Policy{ .credit = &credit, .controller = &controller, .backend_class = .gpu, .limits = limits, .check_live_memory = false, .lifetime = .{ .context = &owner, .retain = Owner.retain, .release = Owner.release } };
    var reserved = try fromRunLease(a, &source, policy, .cuda, .activation, 96);
    defer reserved.release();
    try std.testing.expectEqual(@as(usize, 2), owner.references);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 32 }, source.amounts);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 32, .backend_scratch_bytes = 96 }, controller.snapshot());
    try std.testing.expectEqual(@as(usize, 96), reserved.reservedBytes());
    source.release();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .backend_scratch_bytes = 96 }, controller.snapshot());
    reserved.retainBytes(48);
    try std.testing.expectEqual(@as(usize, 48), reserved.reservedBytes());
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .backend_scratch_bytes = 48 }, controller.snapshot());
    reserved.release();
    try std.testing.expectEqual(@as(usize, 1), owner.references);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "physical storage admission denial leaves request quota and owner lifetime unchanged" {
    const Owner = struct {
        references: usize = 1,
        closing: bool = false,
        fn retain(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.closing) return false;
            self.references += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.references -= 1;
        }
    };
    const a = std.testing.allocator;
    var owner = Owner{};
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var source = try controller.tryAcquire(.gpu, .{}, .{ .host_scratch_bytes = 32, .backend_scratch_bytes = 64 }, false);
    defer source.release();
    var credit = memory.AdmissionAmounts{ .host_scratch_bytes = 16, .backend_scratch_bytes = 64 };
    var policy = Policy{ .credit = &credit, .controller = &controller, .backend_class = .gpu, .limits = .{ .backend_limit_bytes = 80 }, .check_live_memory = false, .lifetime = .{ .context = &owner, .retain = Owner.retain, .release = Owner.release } };
    const original = source;
    // The 32-byte deficit fits alone; the original 64-byte reservation makes
    // admission temporarily unavailable without consuming any request credit.
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, fromRunLease(a, &source, policy, .cuda, .workspace, 96));
    try std.testing.expectEqualDeep(original, source);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 16, .backend_scratch_bytes = 64 }, credit);
    try std.testing.expectEqualDeep(original.amounts, controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), owner.references);
    policy.limits.backend_limit_bytes = 128;
    owner.closing = true;
    try std.testing.expectError(error.ResourceOwnerShuttingDown, fromRunLease(a, &source, policy, .cuda, .workspace, 96));
    try std.testing.expectEqualDeep(original, source);
    try std.testing.expectEqual(@as(usize, 1), owner.references);
    owner.closing = false;
    // Host metadata belongs to the active GPU workload's host scratch domain.
    var host = try fromRunLease(a, &source, policy, .cuda, .host_workspace, 16);
    defer host.release();
    try std.testing.expectEqual(@as(usize, 16), source.amounts.host_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 64), source.amounts.backend_scratch_bytes);
    try std.testing.expectEqual(@as(usize, 16), host.reservedBytes());
}

test "physical storage admission cannot spend another controller or workload class" {
    const a = std.testing.allocator;
    var first = memory.AdmissionController{};
    defer first.deinit();
    var second = memory.AdmissionController{};
    defer second.deinit();
    var source = try first.tryAcquire(.gpu, .{}, .{ .backend_scratch_bytes = 64 }, false);
    defer source.release();
    try std.testing.expectError(error.InvalidStorageAdmission, fromRunLease(a, &source, .{ .controller = &second, .backend_class = .gpu, .limits = .{}, .check_live_memory = false }, .cuda, .activation, 32));
    try std.testing.expectError(error.InvalidStorageAdmission, fromRunLease(a, &source, .{ .controller = &first, .backend_class = .cpu, .limits = .{}, .check_live_memory = false }, .cuda, .activation, 32));
    try std.testing.expectEqual(@as(usize, 64), source.amounts.backend_scratch_bytes);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, second.snapshot());
}

test "physical storage admission external deficit denial and split OOM preserve request ownership" {
    const Budget = struct {
        active: usize = 0,
        next: usize = 1,
        contexts: usize = 1,
        reserves: usize = 0,
        releases: usize = 0,
        deny: bool = false,
        fn retainContext(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.contexts += 1;
            return true;
        }
        fn releaseContext(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.contexts -= 1;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.deny) return error.ResourceLimitExceeded;
            self.active += 1;
            self.reserves += 1;
            const id = self.next;
            self.next += 1;
            return id;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, _: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.active > 0);
            self.active -= 1;
            self.releases += 1;
        }
    };
    const a = std.testing.allocator;
    var budget = Budget{};
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    try controller.configureResourceBudget(.{
        .context = &budget,
        .try_reserve = Budget.reserve,
        .retain = Budget.retain,
        .release = Budget.release,
        .retain_context = Budget.retainContext,
        .release_context = Budget.releaseContext,
    });
    const limits = memory.Limits{ .backend_limit_bytes = 96, .combined_limit_bytes = 96 };
    var source = try controller.tryAcquire(.gpu, limits, .{ .backend_scratch_bytes = 64 }, false);
    defer source.release();
    const original = source;
    var credit = memory.AdmissionAmounts{ .backend_scratch_bytes = 64 };
    const policy = Policy{ .controller = &controller, .backend_class = .gpu, .limits = limits, .check_live_memory = false, .credit = &credit };
    budget.deny = true;
    try std.testing.expectError(error.ResourceLimitExceeded, fromRunLease(a, &source, policy, .cuda, .activation, 96));
    try std.testing.expectEqualDeep(original, source);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .backend_scratch_bytes = 64 }, credit);
    try std.testing.expectEqual(@as(usize, 1), budget.active);
    budget.deny = false;
    var fail = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, fromRunLease(fail.allocator(), &source, policy, .cuda, .activation, 96));
    try std.testing.expectEqualDeep(original, source);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .backend_scratch_bytes = 64 }, credit);
    try std.testing.expectEqual(@as(usize, 1), budget.active);
    try std.testing.expectEqual(@as(usize, 2), budget.reserves);
    try std.testing.expectEqual(@as(usize, 1), budget.releases);
    var reservation = try fromRunLease(a, &source, policy, .cuda, .activation, 96);
    defer reservation.release();
    try std.testing.expectEqual(@as(usize, 2), budget.active);
    source.release();
    reservation.release();
    try std.testing.expectEqual(@as(usize, 0), budget.active);
    try std.testing.expectEqual(budget.reserves, budget.releases);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
    try controller.configureResourceBudget(null);
    try std.testing.expectEqual(@as(usize, 1), budget.contexts);
}

test "physical storage admission clones infallible owner pins without stack adapters" {
    const Owner = struct {
        refs: usize = 1,
        fn hold(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs += 1;
        }
        fn take(raw: *anyopaque) bool {
            hold(raw);
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.refs -= 1;
        }
    };
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var source = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 64 }, false);
    defer source.release();
    var owner = Owner{};
    var credit = memory.AdmissionAmounts{ .host_scratch_bytes = 64 };
    var policy = Policy{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .check_live_memory = false, .credit = &credit };
    const original = source;
    policy.lifetime = .{ .context = &owner, .release = Owner.release };
    try std.testing.expectError(error.InvalidStorageLifetime, fromRunLease(a, &source, policy, .native, .activation, 64));
    policy.lifetime.?.retain = Owner.take;
    policy.lifetime.?.retain_infallible = Owner.hold;
    try std.testing.expectError(error.InvalidStorageLifetime, fromRunLease(a, &source, policy, .native, .activation, 64));
    try std.testing.expectEqual(@as(usize, 1), owner.refs);
    try std.testing.expectEqualDeep(original, source);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, credit);
    policy.lifetime.?.retain = null;
    var reservation = try fromRunLease(a, &source, policy, .native, .activation, 64);
    defer reservation.release();
    try std.testing.expectEqual(@as(usize, 2), owner.refs);
    source.release();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{ .host_scratch_bytes = 64 }, controller.snapshot());
    reservation.release();
    try std.testing.expectEqual(@as(usize, 1), owner.refs);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
