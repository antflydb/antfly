// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const operation = @import("backend_operation.zig");
const sink_module = @import("retirement_sink.zig");
const Owner = @import("operation_owner.zig").Owner;
const SharedBackendContext = @import("../backends/compute_context.zig").SharedBackendContext;
const ops = @import("../ops/ops.zig");

const Reentry = struct {
    backend: *const ops.ComputeBackend,
    model_gate: *std.atomic.Mutex,
    calls: usize = 0,
    failure: ?anyerror = null,
    fn release(raw: *anyopaque) callconv(.c) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        self.calls += 1;
        self.execute() catch |err| {
            self.failure = err;
        };
    }
    fn execute(self: *@This()) !void {
        if (!self.model_gate.tryLock()) return error.OuterModelGateStillHeld;
        defer self.model_gate.unlock();
        var scope: operation.Scope = undefined;
        try operation.enterInto(self.backend, null, null, .managed, &scope);
        defer scope.deinitChecked() catch @panic("reentry cleanup failed");
        const input = try self.backend.fromFloat32(&.{ 1, 2 });
        defer self.backend.free(input);
        const doubled = try self.backend.add(input, input);
        defer self.backend.free(doubled);
        const values = try self.backend.toFloat32(doubled, std.testing.allocator);
        defer std.testing.allocator.free(values);
        try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, values);
        try scope.finish();
    }
};

test "operation cleanup epoch seals after actual outer unlock and permits same-provider numerical reentry" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const shared = try SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    const backend = shared.stableBackend();
    var sink = try sink_module.Ref.create();
    defer sink.release();
    // Cold owner acquires the exact epoch BEFORE the actual outer mutex.
    var outer = try sink.enter();
    defer outer.finish();
    var epoch = try outer.retainEpoch();
    defer epoch.release();
    var model_gate: std.atomic.Mutex = .unlocked;
    try std.testing.expect(model_gate.tryLock());
    var model_locked = true;
    defer if (model_locked) model_gate.unlock();
    var scope: operation.Scope = undefined;
    try operation.enterInto(backend, null, null, .managed, &scope);
    defer scope.deinitChecked() catch @panic("outer cleanup failed");
    try std.testing.expect((try scope.retainCleanupEpoch()) == null);
    try std.testing.expect(try scope.owned.?.bindCleanupEpoch(&epoch));
    // Binding the same exact owner is idempotent, never a second record pin.
    try std.testing.expect(try scope.owned.?.bindCleanupEpoch(&epoch));
    const borrowed = scope.token().?;
    var saved = (try borrowed.retainCleanupEpoch()).?;
    defer saved.release();
    try std.testing.expect(saved.record == epoch.record and saved.nonce == epoch.nonce);
    const record: *Owner.Record = @ptrCast(@alignCast(borrowed.context));
    var independent = try (try record.snapshot(borrowed.nonce)).ticket.retain();
    defer if (independent.held) independent.release() catch @panic("ticket cleanup failed");
    var later = try sink.enter();
    defer later.finish();
    var different_epoch = try later.retainEpoch();
    defer different_epoch.release();
    try std.testing.expectError(error.BackendOperationCleanupChanged, scope.owned.?.bindCleanupEpoch(&different_epoch));
    var probe = Reentry{ .backend = backend, .model_gate = &model_gate };
    var node = @import("resource_retirement.zig").Node{ .context = &probe, .release_fn = Reentry.release };
    const boundary = try saved.boundary();
    try boundary.register(boundary.context, boundary.nonce, &node);
    try scope.finish();
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    // An independent actual-CB-style Ticket keeps metadata alive, but cannot
    // lend the expired host operation or its detached cleanup epoch.
    try std.testing.expectError(error.BackendOperationInactive, borrowed.retainCleanupEpoch());
    try std.testing.expect(record.cleanup_epoch == null);
    model_gate.unlock();
    model_locked = false;
    outer.finish();
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    try std.testing.expect(probe.failure == null);
    try std.testing.expect(!(try different_epoch.isSealed()));
}

test "operation cleanup epoch refuses prepared metadata sealed and stale owners" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const shared = try SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    const backend = shared.stableBackend();
    var sink = try sink_module.Ref.create();
    defer sink.release();
    var outer = try sink.enter();
    defer outer.finish();
    var epoch = try outer.retainEpoch();
    defer epoch.release();
    var owner = Owner{};
    defer owner.assertIdle();
    const request = operation.Request{ .implementation = .{ .context = backend.ptr, .table = backend.vtable }, .program = null, .mode = .managed };
    const pending = try owner.prepare(backend.ptr, request);
    var pending_live = true;
    defer if (pending_live) pending.record.release(pending.nonce) catch @panic("pending cleanup failed");
    try std.testing.expectError(error.BackendOperationInactive, pending.record.bindCleanupEpoch(pending.nonce, &epoch));
    try pending.record.release(pending.nonce);
    pending_live = false;
    try std.testing.expectError(error.BackendOperationConsumed, pending.record.retainCleanupEpoch(pending.nonce));
    var metadata: operation.Scope = undefined;
    try operation.enterInto(backend, null, null, .metadata, &metadata);
    defer metadata.deinitChecked() catch @panic("metadata cleanup failed");
    try std.testing.expect(!try metadata.owned.?.bindCleanupEpoch(&epoch));
    try std.testing.expect((try metadata.retainCleanupEpoch()) == null);
    try metadata.finish();
    outer.finish();
    var numeric: operation.Scope = undefined;
    try operation.enterInto(backend, null, null, .managed, &numeric);
    defer numeric.deinitChecked() catch @panic("numeric cleanup failed");
    try std.testing.expectError(error.RetirementEpochSealed, numeric.owned.?.bindCleanupEpoch(&epoch));
    try std.testing.expect((try numeric.retainCleanupEpoch()) == null);
    try numeric.finish();
}

test "operation cleanup getter refuses a prematurely sealed epoch under a live numerical gate" {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const shared = try SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    const backend = shared.stableBackend();
    var sink = try sink_module.Ref.create();
    defer sink.release();
    var outer = try sink.enter();
    defer outer.finish();
    var epoch = try outer.retainEpoch();
    defer epoch.release();
    var scope: operation.Scope = undefined;
    try operation.enterInto(backend, null, null, .managed, &scope);
    defer scope.deinitChecked() catch @panic("premature epoch cleanup failed");
    try std.testing.expect(try scope.owned.?.bindCleanupEpoch(&epoch));
    // Bounded misuse proof: no callbacks are queued. Sealing early cannot
    // authorize a new quota callback while the actual provider gate is held.
    outer.finish();
    try std.testing.expectError(error.RetirementEpochSealed, scope.retainCleanupEpoch());
    try scope.finish();
}
