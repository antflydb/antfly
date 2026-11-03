// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Cold outer execution lifetime. The actual caller reserves it before its
//! model/provider gates, forwards the exact Epoch, and finishes after unlock.
//! No numerical target, model, request callback, allocator arena or Io is held.
const Session = @import("../backends/session.zig").Session;
const sink = @import("retirement_sink.zig");
const operation = @import("backend_operation.zig");
const ops = @import("../ops/ops.zig");

pub const Owner = struct {
    retained_sink: ?sink.Ref = null,
    guard: ?sink.Guard = null,
    owned_epoch: ?sink.Epoch = null,
    prepared_finish: ?*@import("retirement_finish_holder.zig").Holder = null,

    /// Explicit admitted owners only. Ordinary unadmitted CPU runs do not
    /// create a sink, epoch or metadata allocation through this entry point.
    pub fn beginAdmitted(session: Session) !Owner {
        if (session.run_admission == null) return .{};
        const retained = (try session.retainRetirementSink()) orelse return .{};
        return beginRetained(session, retained);
    }
    /// Explicit resident invocation: the actual outer caller owns its gate
    /// even for a custom/raw Session with no admission or ModelLifetime.
    /// Reserve independent pure metadata before preparation and gate acquisition.
    pub fn beginResident(session: Session) !Owner {
        const retained = (try session.retainRetirementSink()) orelse try sink.Ref.create();
        return beginRetained(session, retained);
    }
    fn beginRetained(session: Session, owned: sink.Ref) !Owner {
        var retained = owned;
        errdefer retained.release();
        var guard = try retained.enter();
        errdefer guard.finish();
        const retained_epoch = try guard.retainEpoch();
        errdefer {
            var retained_cleanup_epoch = retained_epoch;
            retained_cleanup_epoch.release();
        }
        const prepared = if (session.backend() == .metal) try @import("retirement_finish_holder.zig").Holder.prepare() else null;
        return .{ .retained_sink = retained, .guard = guard, .owned_epoch = retained_epoch, .prepared_finish = prepared };
    }
    pub fn epoch(self: *const Owner) ?*const sink.Epoch {
        return if (self.owned_epoch) |*value| value else null;
    }
    pub fn bindScope(self: *const Owner, scope: *const operation.Scope) !bool {
        const current = self.epoch() orelse return false;
        return scope.bindCleanupEpoch(current);
    }
    pub fn bindBackend(self: *const Owner, backend: *const ops.ComputeBackend) !bool {
        const current = self.epoch() orelse return false;
        return backend.bindCleanupEpoch(current);
    }
    /// Attach before publication. The backend may keep exclusion after the
    /// synchronous forward while its actual output/backend owner remains live.
    /// Both that owner and this outer lane must finish the SAME epoch.
    pub fn retainThroughBackend(self: *Owner, backend: *const ops.ComputeBackend) !bool {
        const current = if (self.guard) |*guard| guard else return false;
        const prepared = self.prepared_finish orelse {
            if (backend.kind() == .metal) return error.BackendCleanupFinishUnprepared;
            return false;
        };
        var provider_guard = try current.fork();
        defer provider_guard.finish();
        try prepared.adopt(&provider_guard);
        self.prepared_finish = null;
        const transferred = backend.retainCleanupGuard(prepared) catch |err| {
            prepared.withdrawUnpublishedWithOuter(current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
            return err;
        };
        if (!transferred) prepared.withdrawUnpublishedWithOuter(current) catch @import("antfly_platform").inference_process_supervisor.restartWorker();
        return transferred;
    }
    /// Caller has ended ALL actual provider and outer model/lane ownership.
    /// Clear publication before callbacks; callbacks can create a fresh owner
    /// but cannot recover this old epoch from a half-finished owner object.
    pub fn finishAfterUnlock(self: *Owner) void {
        var retained = self.retained_sink;
        var guard = self.guard;
        var retained_epoch = self.owned_epoch;
        const prepared = self.prepared_finish;
        self.* = .{};
        if (prepared) |cell| cell.discardEmpty();
        defer if (retained) |*value| value.release();
        defer if (retained_epoch) |*value| value.release();
        if (guard) |*value| value.finish();
    }
};

test "admitted outer cleanup owner binds real shared provider and seals only after model unlock" {
    const std = @import("std");
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const shared = try @import("../backends/compute_context.zig").SharedBackendContext.init(std.testing.allocator, .native, null);
    defer shared.release();
    const backend = shared.stableBackend();
    const Probe = struct {
        cleanup: *Owner,
        gate: *std.atomic.Mutex,
        backend: *const ops.ComputeBackend,
        calls: usize = 0,
        failure: ?anyerror = null,
        fn retire(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            self.check() catch |err| {
                self.failure = err;
            };
        }
        fn check(self: *@This()) !void {
            if (self.cleanup.epoch() != null) return error.OldEpochStillPublished;
            if (!self.gate.tryLock()) return error.ModelGateStillHeld;
            defer self.gate.unlock();
            var scope: operation.Scope = undefined;
            try operation.enterInto(self.backend, null, null, .managed, &scope);
            defer scope.deinitChecked() catch unreachable;
            const a = try self.backend.fromFloat32(&.{ 1, 2 });
            defer self.backend.free(a);
            const result = try self.backend.add(a, a);
            defer self.backend.free(result);
            const values = try self.backend.toFloat32(result, std.testing.allocator);
            defer std.testing.allocator.free(values);
            try std.testing.expectEqualSlices(f32, &.{ 2, 4 }, values);
            try scope.finish();
        }
        fn run(_: *anyopaque, _: []const @import("../backends/tensor.zig").Tensor, _: std.mem.Allocator) anyerror![]@import("../backends/tensor.zig").Tensor {
            return error.UnexpectedRun;
        }
        fn backendKind(_: *anyopaque) @import("../backends/backends.zig").BackendType {
            return .native;
        }
        fn info(_: *anyopaque) []const @import("../backends/tensor.zig").TensorInfo {
            return &.{};
        }
        fn close(_: *anyopaque) void {}
        const table = Session.VTable{ .backend = backendKind, .run = run, .inputInfo = info, .outputInfo = info, .close = close };
    };
    var marker: u8 = 0;
    var model = Session.ModelLifetime{ .owner = &marker, .destroy_fn = Probe.close };
    defer model.release();
    var controller = @import("../runtime/tier/memory.zig").AdmissionController{};
    defer controller.deinit();
    const session = Session{ .ptr = &marker, .vtable = &Probe.table, .model_lifetime = &model, .run_admission = .{ .controller = &controller, .backend_class = .cpu, .limits = .{}, .static_workspace_bytes = 0 } };
    var cleanup = try Owner.beginAdmitted(session);
    defer cleanup.finishAfterUnlock();
    try std.testing.expect(cleanup.epoch() != null);
    try std.testing.expect(model.execution_gate.tryLock());
    var locked = true;
    defer if (locked) model.execution_gate.unlock();
    var scope: operation.Scope = undefined;
    try operation.enterInto(backend, null, null, .managed, &scope);
    defer scope.deinitChecked() catch unreachable;
    try std.testing.expect(try cleanup.bindScope(&scope));
    var probe = Probe{ .cleanup = &cleanup, .gate = &model.execution_gate, .backend = backend };
    var node = @import("resource_retirement.zig").Node{ .context = &probe, .release_fn = Probe.retire };
    const boundary = try cleanup.epoch().?.boundary();
    try boundary.register(boundary.context, boundary.nonce, &node);
    try scope.finish();
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    model.execution_gate.unlock();
    locked = false;
    cleanup.finishAfterUnlock();
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    if (probe.failure) |err| return err;
}
