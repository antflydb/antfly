// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Exact synchronous packed-CPU packet custody. Only the existing Reader's
//! real outside window prepares this loan; its receiving proof binds immutable
//! Session/implementation/byte identities before CPU arrays or model math.
//! Late retirement owns ONLY the pure Charge/Domain, never a Reader, Model,
//! provider, allocator arena, operation token or saved creation Epoch.
const std = @import("std");
const Session = @import("../backends/session.zig").Session;
const memory = @import("../runtime/tier/memory.zig");
const retirement = @import("resource_retirement.zig");
const admission = @import("../runtime/tier/admission_domain.zig");
const Implementation = @import("kernel_route.zig").Implementation;
const Cleanup = @import("cleanup_context.zig").Context;

pub const Loan = struct {
    charge: ?*retirement.Charge,
    bytes: usize,
    implementation: Implementation,
    session_context: *anyopaque,
    session_table: *const anyopaque,
    model_identity: usize,
    bound: bool = false,

    pub fn metadataBytes() !usize {
        return retirement.PreparedCharge.requestedMetadataBytes(0);
    }
    pub fn requiredBytes(bytes: usize) !usize {
        if (bytes == 0) return error.InvalidPackedHostGeometry;
        return std.math.add(usize, bytes, try metadataBytes());
    }
    pub fn prepareOutsideExecution(session: Session, implementation: Implementation, bytes: usize) !Loan {
        return prepareUsing(false, undefined, session, implementation, bytes);
    }
    fn prepareUsing(comptime test_allocator: bool, allocator: std.mem.Allocator, session: Session, implementation: Implementation, bytes: usize) !Loan {
        const policy = session.run_admission orelse return error.UnmanagedPackedHostLoan;
        const model = session.model_lifetime orelse return error.MissingSourceModelLifetime;
        var pin: ?admission.Pin = model.retainAdmissionDomain(policy.controller) orelse return error.ForeignAdmissionDomain;
        defer if (pin) |value| value.release();
        // Original host packet floor is unchanged. Only the exact new pure
        // retirement metadata payload is additional to that same acquisition.
        var permit = try session.admitHostPreprocess(try requiredBytes(bytes));
        defer permit.deinit(); // resource-pin callbacks also run OUTSIDE
        var shell = if (test_allocator)
            try retirement.PreparedCharge.prepareWithAllocatorForTest(allocator, 0)
        else
            try retirement.PreparedCharge.prepare(0);
        defer shell.destroyUnused();
        const charge = try shell.arm(&permit.lease, &pin);
        return .{ .charge = charge, .bytes = bytes, .implementation = implementation, .session_context = session.ptr, .session_table = session.vtable, .model_identity = @intFromPtr(model) };
    }
    /// Value checks only. Actual Reader cohort/provider validation precedes
    /// this call; these identities confer NO numerical/provider authority.
    fn validateIdentity(self: *const Loan, session: Session, implementation: Implementation, bytes: usize) !void {
        const charge = self.charge orelse return error.PackedHostLoanConsumed;
        if (session.ptr != self.session_context or @as(*const anyopaque, session.vtable) != self.session_table or
            @intFromPtr(session.model_lifetime orelse return error.MissingSourceModelLifetime) != self.model_identity)
            return error.ForeignPackedHostSession;
        if (implementation.context != self.implementation.context or implementation.table != self.implementation.table or implementation.policy_epoch != self.implementation.policy_epoch)
            return error.ForeignPackedHostReceiver;
        if (bytes != self.bytes) return error.PackedHostGeometryChanged;
        const policy = session.run_admission orelse return error.UnmanagedPackedHostLoan;
        if (charge.domain == null or charge.domain.?.controller() != policy.controller or charge.quota != .run or
            charge.quota.run.controller != policy.controller or
            !std.meta.eql(charge.quota.run.amounts, memory.AdmissionAmounts{ .host_scratch_bytes = try requiredBytes(bytes) }))
            return error.ForeignPackedHostAdmission;
    }
    pub fn bind(self: *Loan, session: Session, implementation: Implementation, bytes: usize, current: Cleanup) !void {
        if (self.bound) return error.PackedHostLoanAlreadyBound;
        try self.validateIdentity(session, implementation, bytes);
        if (current.outside_execution) return error.MissingCurrentCleanupEpoch;
        try self.validateCleanup(current);
        self.bound = true;
    }
    pub fn validateBound(self: *const Loan, session: Session, implementation: Implementation, bytes: usize, current: Cleanup) !void {
        if (!self.bound) return error.UnboundPackedHostLoan;
        try self.validateIdentity(session, implementation, bytes);
        if (current.outside_execution) return error.MissingCurrentCleanupEpoch;
        try self.validateCleanup(current);
    }
    pub fn validateCleanup(self: *const Loan, current: Cleanup) !void {
        if (self.charge == null or current.outside_execution) return;
        const epoch = current.boundary orelse return error.MissingCurrentCleanupEpoch;
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
    }
    /// CPU arrays are already dead. Use the caller's NOW B, including after
    /// optional host-read pauses. Never retain the creation/receiving Epoch.
    pub fn finishWithCleanup(self: *Loan, current: Cleanup) !void {
        try self.validateCleanup(current);
        const charge = self.charge orelse return;
        if (!current.outside_execution) try charge.holdUntilBoundary(try current.boundary.?.boundary());
        self.charge = null; // consume before any possible outside callback
        self.bound = false;
        charge.release();
    }
    pub fn finishOutsideExecution(self: *Loan) void {
        self.finishWithCleanup(.{ .outside_execution = true }) catch unreachable;
    }
};

const TestSession = struct {
    fn run(_: *anyopaque, _: []const @import("../backends/tensor.zig").Tensor, _: std.mem.Allocator) anyerror![]@import("../backends/tensor.zig").Tensor {
        return error.UnexpectedNumericalDispatch;
    }
    fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
        return &.{};
    }
    fn backend(_: *anyopaque) @import("../backends/backends.zig").BackendType {
        return .cuda;
    }
    fn close(_: *anyopaque) void {}
    const table: Session.VTable = .{ .run = run, .inputInfo = info, .outputInfo = info, .backend = backend, .close = close };
};
const Probe = struct {
    gate: std.atomic.Mutex = .unlocked,
    violation: bool = false,
    releases: usize = 0,
    reservations: usize = 0,
    fn observe(self: *@This()) void {
        if (self.gate.tryLock()) self.gate.unlock() else self.violation = true;
    }
    fn reserve(raw: *anyopaque, _: memory.AdmissionAmounts) memory.AdmissionResourceError!usize {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.observe();
        self.reservations += 1;
        return self.reservations;
    }
    fn retain(raw: *anyopaque, _: usize, _: memory.AdmissionAmounts) memory.AdmissionResourceError!void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.observe();
    }
    fn release(raw: *anyopaque, _: usize) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.observe();
        self.releases += 1;
    }
};
fn allocationCase(allocator: std.mem.Allocator) !void {
    const domain = try admission.Domain.create();
    defer domain.release();
    var probe: Probe = .{};
    try domain.controller.configureResourceBudget(.{ .context = &probe, .try_reserve = Probe.reserve, .retain = Probe.retain, .release = Probe.release });
    defer domain.controller.configureResourceBudget(null) catch @panic("packed host live quota");
    var marker: u8 = 0;
    var model: Session.ModelLifetime = .{ .owner = &marker, .destroy_fn = TestSession.close, .admission_domain = domain.pin() };
    defer model.release();
    const session: Session = .{ .ptr = &marker, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{}, .static_workspace_bytes = 4096, .opaque_workspace_bytes = 8192, .check_live_memory = false } };
    // Opaque VALUE identity only: this pure unit never constructs a fake SDK
    // backend or invokes table callbacks. Actual Reader proves its full table.
    const identity: Implementation = .{ .context = &marker, .table = &TestSession.table };
    const bytes: usize = 35028;
    var loan = Loan.prepareUsing(true, allocator, session, identity, bytes) catch |err| {
        try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
        try std.testing.expect(!probe.violation);
        return err;
    };
    defer loan.finishOutsideExecution();
    const expected = memory.AdmissionAmounts{ .host_scratch_bytes = try Loan.requiredBytes(bytes) };
    try std.testing.expectEqualDeep(expected, domain.controller.snapshot());
    try std.testing.expectEqual(@as(usize, 1), probe.reservations);
    const Sink = @import("retirement_sink.zig");
    var sink = try Sink.Ref.create();
    defer sink.release();
    var first = try sink.enter();
    defer first.finish();
    var first_epoch = try first.retainEpoch();
    defer first_epoch.release();
    try std.testing.expectError(error.MissingCurrentCleanupEpoch, loan.bind(session, identity, bytes, .{}));
    try std.testing.expect(!loan.bound);
    try std.testing.expectError(error.PackedHostGeometryChanged, loan.bind(session, identity, bytes + 1, .{ .boundary = &first_epoch }));
    var foreign = identity;
    foreign.context = @ptrFromInt(16);
    try std.testing.expectError(error.ForeignPackedHostReceiver, loan.bind(session, foreign, bytes, .{ .boundary = &first_epoch }));
    foreign = identity;
    foreign.table = @ptrFromInt(16);
    try std.testing.expectError(error.ForeignPackedHostReceiver, loan.bind(session, foreign, bytes, .{ .boundary = &first_epoch }));
    var foreign_session = session;
    foreign_session.ptr = @ptrFromInt(16);
    try std.testing.expectError(error.ForeignPackedHostSession, loan.bind(foreign_session, identity, bytes, .{ .boundary = &first_epoch }));
    try loan.bind(session, identity, bytes, .{ .boundary = &first_epoch });
    try loan.validateBound(session, identity, bytes, .{ .boundary = &first_epoch });
    try std.testing.expectError(error.PackedHostLoanAlreadyBound, loan.bind(session, identity, bytes, .{ .boundary = &first_epoch }));
    first.finish(); // a real read/park can seal the creation/receiving B
    try std.testing.expectEqualDeep(expected, domain.controller.snapshot());
    try std.testing.expectError(error.RetirementEpochSealed, loan.finishWithCleanup(.{ .boundary = &first_epoch }));
    try std.testing.expect(loan.charge != null);
    var later = try sink.enter();
    defer later.finish();
    var later_epoch = try later.retainEpoch();
    defer later_epoch.release();
    try loan.validateBound(session, identity, bytes, .{ .boundary = &later_epoch });
    try std.testing.expect(probe.gate.tryLock());
    var held = true;
    defer if (held) probe.gate.unlock();
    const before = probe.releases;
    try loan.finishWithCleanup(.{ .boundary = &later_epoch });
    try std.testing.expect(loan.charge == null);
    _ = retirement.poll();
    try std.testing.expectEqual(before, probe.releases);
    try std.testing.expectEqualDeep(expected, domain.controller.snapshot());
    probe.gate.unlock();
    held = false;
    later.finish();
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, domain.controller.snapshot());
    try std.testing.expect(!probe.violation);
}

test "packed host cold loan every OOM preserves original admission and later actual B holds callbacks outside gates" {
    try @import("antfly_platform").allocator.checkAllAllocationFailures(std.testing.allocator, allocationCase, .{});
}

test "packed host mandatory cap missing foreign Domain refuse before allocation and preserve all outer class floors" {
    const domain = try admission.Domain.create();
    defer domain.release();
    var marker: u8 = 0;
    var model: Session.ModelLifetime = .{ .owner = &marker, .destroy_fn = TestSession.close, .admission_domain = domain.pin() };
    defer model.release();
    var session: Session = .{ .ptr = &marker, .vtable = &TestSession.table, .model_lifetime = &model, .run_admission = .{ .controller = &domain.controller, .backend_class = .gpu, .limits = .{}, .check_live_memory = false, .static_workspace_bytes = 0 } };
    const identity: Implementation = .{ .context = &marker, .table = &TestSession.table };
    const bytes: usize = 35028;
    var outside = try domain.controller.tryAcquire(.gpu, .{}, .{ .host_scratch_bytes = 17, .backend_scratch_bytes = 19, .host_kv_bytes = 23, .backend_kv_bytes = 29, .host_weight_bytes = 31, .backend_weight_bytes = 37 }, false);
    defer outside.release();
    const floor = domain.controller.snapshot();
    session.model_lifetime = null;
    try std.testing.expectError(error.MissingSourceModelLifetime, Loan.prepareOutsideExecution(session, identity, bytes));
    session.model_lifetime = &model;
    const saved = model.admission_domain;
    model.admission_domain = null;
    try std.testing.expectError(error.ForeignAdmissionDomain, Loan.prepareOutsideExecution(session, identity, bytes));
    model.admission_domain = saved;
    session.run_admission.?.limits.host_limit_bytes = try Loan.requiredBytes(bytes) - 1;
    try std.testing.expectError(error.ResourceLimitExceeded, Loan.prepareOutsideExecution(session, identity, bytes));
    try std.testing.expectEqualDeep(floor, domain.controller.snapshot());
    session.run_admission.?.limits.host_limit_bytes = floor.hostTotalBytes() + try Loan.requiredBytes(bytes) - 1;
    try std.testing.expectError(error.ResourceTemporarilyUnavailable, Loan.prepareOutsideExecution(session, identity, bytes));
    try std.testing.expectEqualDeep(floor, domain.controller.snapshot());
    session.run_admission.?.limits.host_limit_bytes += 1;
    var loan = try Loan.prepareOutsideExecution(session, identity, bytes);
    defer loan.finishOutsideExecution();
    var full = floor;
    full.host_scratch_bytes += try Loan.requiredBytes(bytes);
    try std.testing.expectEqualDeep(full, domain.controller.snapshot());
    loan.finishOutsideExecution();
    try std.testing.expectEqualDeep(floor, domain.controller.snapshot());
}
