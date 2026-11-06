// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Stable, admitted preparation metadata. Numerical banks have their own
//! physical declarations; metadata never borrows those bytes or an estimate.
const std = @import("std");
const sessions = @import("../backends/session.zig");
const AdmittedAllocator = @import("../backends/admitted_allocator.zig").AdmittedAllocator;
const memory = @import("../runtime/tier/memory.zig");
const Tensor = @import("../backends/tensor.zig").Tensor;
const TensorInfo = @import("../backends/tensor.zig").TensorInfo;
const BackendType = @import("../backends/backends.zig").BackendType;

pub const Owner = struct {
    const max_held = 4;
    const Phase = enum { preparing, sealed };

    backing: std.mem.Allocator,
    admitted: AdmittedAllocator,
    session: sessions.Session,
    model: ?*sessions.Session.ModelLifetime,
    wrapper_lease: ?memory.AdmissionLease,
    held: [max_held]?*const sessions.RunPermit = @splat(null),
    held_count: usize = 0,
    held_floor: ?memory.AdmissionAmounts = null,
    phase: Phase = .preparing,

    /// The supplied permits stay at their final addresses and retain their
    /// exact leases until sealBeforeExecution. The owner itself never moves.
    /// Backing frees must reclaim storage without quota/provider callbacks;
    /// the preparing runtime proves that allocator contract before opting in.
    pub fn create(backing: std.mem.Allocator, session: sessions.Session, permits: []const *const sessions.RunPermit) !*Owner {
        const floor = try sumPermits(session, permits);
        const model = if (session.model_lifetime) |value| value.retain() else null;
        errdefer if (model) |value| value.release();
        const wrapper = memory.AdmissionAmounts{ .host_scratch_bytes = @sizeOf(Owner) };
        var lease: ?memory.AdmissionLease = null;
        if (session.run_admission) |policy| {
            try policy.validateOwnedRunAmounts(try floor.merge(wrapper));
            lease = try acquireHost(session, @sizeOf(Owner));
        }
        errdefer if (lease) |*value| value.release();
        const self = try backing.create(Owner);
        self.* = .{
            .backing = backing,
            .admitted = .{ .backing = backing, .session = if (session.run_admission == null) session else null },
            .session = session,
            .model = model,
            .wrapper_lease = lease,
        };
        for (permits, 0..) |permit, index| self.held[index] = permit;
        self.held_count = permits.len;
        if (session.run_admission != null) {
            self.admitted.admission_context = self;
            self.admitted.admit = acquire;
        }
        return self;
    }

    fn sumPermits(session: sessions.Session, permits: []const *const sessions.RunPermit) !memory.AdmissionAmounts {
        if (permits.len > max_held) return error.TooManyMetadataAdmissionOwners;
        var total = memory.AdmissionAmounts{};
        for (permits, 0..) |permit, index| {
            if (permit.session.ptr != session.ptr or permit.session.vtable != session.vtable) return error.ForeignMetadataAdmissionOwner;
            for (permits[0..index]) |prior| if (prior == permit) return error.DuplicateMetadataAdmissionOwner;
            if (permit.lease) |lease| {
                const policy = session.run_admission orelse return error.ForeignMetadataAdmissionOwner;
                if (lease.controller != policy.controller or lease.retain_backend_class != policy.backend_class) return error.ForeignMetadataAdmissionOwner;
                total = try total.merge(lease.amounts);
            }
        }
        return total;
    }

    pub fn allocator(self: *Owner) std.mem.Allocator {
        return self.admitted.allocator();
    }

    /// Replace borrowed facts only after checking their complete actual sum.
    /// This does not merge, move, reduce or lend any of the supplied leases.
    pub fn setHeldPermits(self: *Owner, permits: []const *const sessions.RunPermit) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.held_floor != null) return error.MetadataAdmissionOwnersFrozen;
        const floor = try sumPermits(self.session, permits);
        if (self.session.run_admission) |policy| try policy.validateOwnedRunAmounts(try floor.merge(self.admittedAmounts()));
        @memset(&self.held, null);
        for (permits, 0..) |permit, index| self.held[index] = permit;
        self.held_count = permits.len;
    }

    /// Actual independently held metadata quota, including tracking headers
    /// and reserved chunk slack. Unmanaged owners have no admission claim.
    pub fn admittedAmounts(self: *const Owner) memory.AdmissionAmounts {
        if (self.session.run_admission == null) return .{};
        var result = if (self.wrapper_lease) |lease| lease.amounts else memory.AdmissionAmounts{};
        result.host_scratch_bytes += self.admitted.reserved_bytes;
        return result;
    }

    fn heldAmounts(self: *const Owner) !memory.AdmissionAmounts {
        if (self.held_floor) |floor| return floor;
        var permits: [max_held]*const sessions.RunPermit = undefined;
        for (self.held[0..self.held_count], 0..) |permit, index| permits[index] = permit orelse return error.MissingMetadataAdmissionOwner;
        return sumPermits(self.session, permits[0..self.held_count]);
    }

    /// Keep the actual complete request as a conservative denial floor while
    /// its leases partition into backing owners. This value grants no quota,
    /// storage credit or refund. No new bank permit may join after this point;
    /// only metadata for those already admitted banks can continue to grow.
    pub fn preserveHeldFloorBeforePartition(self: *Owner) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        if (self.held_floor != null) return error.MetadataAdmissionOwnersFrozen;
        try self.validateAdditional(.{});
        self.held_floor = try self.heldAmounts();
        @memset(&self.held, null);
        self.held_count = 0;
    }

    /// Necessary full-request fit before an additional numerical/cache lease.
    /// Existing peer requests remain controller contention, never subtraction.
    pub fn validateAdditional(self: *const Owner, additional: memory.AdmissionAmounts) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        const policy = self.session.run_admission orelse return;
        const total = try (try self.heldAmounts()).merge(self.admittedAmounts());
        try policy.validateOwnedRunAmounts(try total.merge(additional));
    }

    fn acquire(raw: *anyopaque, bytes: usize) !memory.AdmissionLease {
        const self: *Owner = @ptrCast(@alignCast(raw));
        try self.validateAdditional(.{ .host_scratch_bytes = bytes });
        return acquireHost(self.session, bytes);
    }

    fn acquireHost(session: sessions.Session, bytes: usize) !memory.AdmissionLease {
        var permit = try session.admitHostPreprocess(bytes);
        defer permit.deinit();
        const lease = permit.lease orelse return error.MissingRunAdmission;
        permit.lease = null;
        // The Owner's actual Model pin already retains these services through
        // every chunk refund. Release only this redundant transient pin.
        return lease;
    }

    pub fn admissionError(self: *const Owner) ?anyerror {
        return self.admitted.admission_error;
    }

    /// No supplier or refund is allowed while the real decoder owns its lane.
    /// Existing prefunded capacity can still serve metadata without growing.
    pub fn sealBeforeExecution(self: *Owner) !void {
        if (self.phase != .preparing) return error.MetadataPreparationSealed;
        try self.validateAdditional(.{});
        self.admitted.numerical_scope_active = true;
        @memset(&self.held, null);
        self.held_count = 0;
        self.phase = .sealed;
    }

    /// First close the program/Graph/Pool and leave all actual Model gates.
    /// Leases and their real process-manager callbacks retire only here.
    pub fn deinitOutsideExecution(self: *Owner) void {
        const backing = self.backing;
        const model = self.model;
        var wrapper = self.wrapper_lease;
        self.admitted.numerical_scope_active = false;
        self.admitted.deinit();
        backing.destroy(self);
        if (wrapper) |*lease| lease.release();
        if (model) |value| value.release();
    }
};

const TestSession = struct {
    fn run(_: *anyopaque, _: []const Tensor, _: std.mem.Allocator) ![]Tensor {
        return error.UnexpectedCallback;
    }
    fn info(_: *anyopaque) []const TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) BackendType {
        return .native;
    }
    fn close(_: *anyopaque) void {}
    const table: sessions.Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
};

test "cold metadata admits actual chunks with the whole held request and forbids numerical growth" {
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var marker: u8 = 0;
    const limit = 1024 * 1024;
    const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = limit }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var base = sessions.RunPermit{ .session = session, .lease = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = limit }, .{ .host_kv_bytes = 64 }, false) };
    defer base.deinit();
    const original = controller.snapshot();
    const owner = try Owner.create(a, session, &.{&base});
    var open = true;
    defer if (open) owner.deinitOutsideExecution();
    const values = try owner.allocator().alloc(f32, 64);
    const admitted = controller.snapshot();
    try std.testing.expect(admitted.host_scratch_bytes >= @sizeOf(Owner) + 64 * @sizeOf(f32));
    try std.testing.expectEqualDeep(try original.merge(owner.admittedAmounts()), admitted);
    try std.testing.expectEqual(original.host_kv_bytes, admitted.host_kv_bytes);
    try std.testing.expectError(error.ResourceLimitExceeded, owner.validateAdditional(.{ .host_scratch_bytes = limit - admitted.hostTotalBytes() + 1 }));
    try std.testing.expectEqualDeep(admitted, controller.snapshot());
    try std.testing.expectError(error.DuplicateMetadataAdmissionOwner, owner.setHeldPermits(&.{ &base, &base }));
    try owner.sealBeforeExecution();
    try std.testing.expectError(error.OutOfMemory, owner.allocator().alloc(u8, limit));
    try std.testing.expectEqual(@as(?anyerror, error.ResourceLimitExceeded), owner.admissionError());
    try std.testing.expectEqualDeep(admitted, controller.snapshot());
    owner.allocator().free(values);
    try std.testing.expectEqualDeep(admitted, controller.snapshot()); // no callbacks under the lane
    owner.deinitOutsideExecution();
    open = false;
    try std.testing.expectEqualDeep(original, controller.snapshot());
}

test "cold metadata owner and payload allocation failures refund every acquired lease" {
    const Case = struct {
        fn run(a: std.mem.Allocator) !void {
            var controller = memory.AdmissionController{};
            defer controller.deinit();
            defer std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot()) catch @panic("cold metadata leaked admission");
            var marker: u8 = 0;
            const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
            const owner = try Owner.create(a, session, &.{});
            defer owner.deinitOutsideExecution();
            const values = try owner.allocator().alloc(u32, 16);
            defer owner.allocator().free(values);
            @memset(values, 7);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Case.run, .{});
}

test "cold metadata denial floor survives actual lease partition without granting storage credit" {
    const a = std.testing.allocator;
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var marker: u8 = 0;
    const limit = 1024 * 1024;
    const session = sessions.Session{ .ptr = &marker, .vtable = &TestSession.table, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{ .host_limit_bytes = limit }, .static_workspace_bytes = 0, .check_live_memory = false } };
    var base = sessions.RunPermit{ .session = session, .lease = try controller.tryAcquire(.cpu, .{ .host_limit_bytes = limit }, .{ .host_kv_bytes = 128 }, false), .request = .{} };
    defer base.deinit();
    const owner = try Owner.create(a, session, &.{&base});
    defer owner.deinitOutsideExecution();
    const values = try owner.allocator().alloc(u8, 32);
    defer owner.allocator().free(values);
    const before = controller.snapshot();
    try owner.preserveHeldFloorBeforePartition();
    var bank = try base.lease.?.partition(a, .{ .host_kv_bytes = 128 });
    defer bank.release();
    try std.testing.expectEqual(@as(usize, 0), base.lease.?.amounts.host_kv_bytes);
    try std.testing.expectEqualDeep(before, controller.snapshot());
    try std.testing.expectError(error.ResourceLimitExceeded, owner.validateAdditional(.{ .host_scratch_bytes = limit - before.hostTotalBytes() + 1 }));
    try std.testing.expectError(error.MetadataAdmissionOwnersFrozen, owner.setHeldPermits(&.{&base}));
    try std.testing.expectEqualDeep(before, controller.snapshot());
}

test "cold metadata frees real payload and owner before refund callbacks and retains Model until last refund" {
    const Probe = struct {
        backing: std.mem.Allocator,
        live_bytes: usize = 0,
        next: usize = 0,
        releases: usize = 0,
        violation: bool = false,
        model_closed: bool = false,
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const result = self.backing.rawAlloc(len, alignment, ra) orelse return null;
            self.live_bytes += len;
            return result;
        }
        fn free(raw: *anyopaque, buffer: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(buffer, alignment, ra);
            self.live_bytes -= buffer.len;
        }
        fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.next += 1;
            return self.next;
        }
        fn retain(_: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {}
        fn release(raw: *anyopaque, id: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            const expected: usize = if (id == 1) 0 else @sizeOf(Owner);
            if (self.live_bytes != expected or self.model_closed) self.violation = true;
            self.releases += 1;
        }
        fn destroyModel(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.live_bytes != 0 or self.releases != self.next) self.violation = true;
            self.model_closed = true;
        }
        const allocator_table: std.mem.Allocator.VTable = .{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free };
    };
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var probe = Probe{ .backing = std.testing.allocator };
    try controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    var model = sessions.Session.ModelLifetime{ .owner = &probe, .destroy_fn = Probe.destroyModel };
    const session = sessions.Session{ .ptr = &probe, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0, .check_live_memory = false } };
    const owner = try Owner.create(.{ .ptr = &probe, .vtable = &Probe.allocator_table }, session, &.{});
    const values = try owner.allocator().alloc(u8, 32);
    try owner.sealBeforeExecution();
    session.close();
    try std.testing.expect(!probe.model_closed);
    owner.allocator().free(values);
    try std.testing.expectEqual(@as(usize, 0), probe.releases);
    owner.deinitOutsideExecution();
    try std.testing.expect(probe.model_closed and !probe.violation);
    try std.testing.expectEqual(probe.next, probe.releases);
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}
