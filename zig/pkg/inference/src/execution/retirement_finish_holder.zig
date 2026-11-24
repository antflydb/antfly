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

//! Pure, preallocated retirement metadata for one retained provider Guard A.
//! Actual provider death publishes BX independently from metadata references.
//! The final local close publishes BY after its SDK/close scopes end. Both
//! distinct boundaries must pass before A may finish. Nonfinal closes store no B.
const std = @import("std");
const sink = @import("retirement_sink.zig");
const retirement = @import("resource_retirement.zig");
const platform = @import("antfly_platform");
const stable_allocator = platform.allocator.processAllocator(platform.allocator.concurrentFallback());
const fatal = platform.inference_process_supervisor.restartWorker;

pub const Holder = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    references: usize = 1, // provider ownership plus explicit local-close pins
    guard: ?sink.Guard = null,
    provider_boundary: ?sink.Epoch = null,
    local_boundary: ?sink.Epoch = null,
    provider_finished: bool = false,
    provider_outside_execution: bool = false,
    local_node: retirement.Node,
    provider_node: retirement.Node,
    phase: enum { empty, armed, waiting_local, waiting_provider, finished } = .empty,

    /// Allocate before actual provider/model gates, never during final cleanup.
    pub fn prepare() !*Holder {
        return prepareWithAllocator(stable_allocator);
    }
    fn prepareWithAllocator(allocator: std.mem.Allocator) !*Holder {
        const self = try allocator.create(Holder);
        self.* = .{ .allocator = allocator, .local_node = .{ .context = self, .release_fn = localPassed }, .provider_node = .{ .context = self, .release_fn = providerPassed } };
        return self;
    }
    fn lock(self: *Holder) void {
        platform.sync.lockYielding(&self.mutex);
    }
    /// Caller retains unchanged ownership on failure. Success moves only the
    /// pre-existing fork; no SDK, quota producer or allocation occurs here.
    pub fn adopt(self: *Holder, guard: *sink.Guard) !void {
        var epoch = try guard.retainEpoch();
        defer epoch.release();
        if (try epoch.isSealed()) return error.RetirementEpochSealed;
        self.lock();
        defer self.mutex.unlock();
        if (self.phase != .empty or self.guard != null or self.references != 1) return error.RetirementFinishHolderConsumed;
        self.guard = guard.*;
        guard.held = false;
        self.phase = .armed;
    }
    /// Borrowed only while the actual provider owner still owns this cell.
    pub fn sourceEpoch(self: *const Holder) ?*const sink.Epoch {
        return if (self.guard) |*guard| &guard.epoch else null;
    }
    pub fn discardEmpty(self: *Holder) void {
        if (self.phase != .empty or self.guard != null or self.references != 1) @panic("retirement finish holder still owns guard");
        self.allocator.destroy(self);
    }
    /// Acquire before invoking an accepted concrete owner cleanup. Generic
    /// child/view reference retirement cannot stand in for provider death.
    pub fn retainLocal(self: *Holder) !*Holder {
        self.lock();
        defer self.mutex.unlock();
        if (self.phase != .armed or self.provider_finished) return error.RetirementFinishHolderConsumed;
        if (self.references == std.math.maxInt(usize)) return error.RetirementEpochReferenceOverflow;
        self.references += 1;
        return self;
    }
    /// Actual provider/model exclusion has ended. Preserve its CURRENT BX
    /// even when another metadata closer outlives the concrete destruction.
    /// At least one local owner must keep callbacks out of its close scope.
    pub fn providerFinished(self: *Holder, boundary: ?*const sink.Epoch) !void {
        const current = boundary orelse return error.MissingCurrentCleanupEpoch;
        var held = try current.retain();
        var moved = false;
        defer if (!moved) held.release();
        if (try held.isSealed()) return error.RetirementEpochSealed;
        self.lock();
        defer self.mutex.unlock();
        if (self.phase != .armed or self.provider_finished or self.references < 2) return error.RetirementFinishLocalOwnerRequired;
        self.provider_boundary = held;
        moved = true;
        self.provider_finished = true;
        self.references -= 1;
    }
    /// Explicit caller proof: provider and outer execution scopes have ended.
    /// No missing/null boundary is implicitly upgraded to this contract.
    pub fn providerFinishedOutsideExecution(self: *Holder) !void {
        self.lock();
        defer self.mutex.unlock();
        if (self.phase != .armed or self.provider_finished or self.references < 2) return error.RetirementFinishLocalOwnerRequired;
        self.provider_finished = true;
        self.provider_outside_execution = true;
        self.references -= 1;
    }
    /// Prepublication rollback only: the ORIGINAL outer Guard is still held,
    /// so dropping this unused fork cannot seal A. Caller has not published
    /// the cell to any backend or local closer. No allocation occurs here.
    pub fn withdrawUnpublished(self: *Holder) void {
        self.lock();
        if (self.phase != .armed or self.provider_finished or self.references != 1) @panic("published retirement finish holder withdrawal");
        self.phase = .finished;
        var guard = self.guard.?;
        self.guard = null;
        self.mutex.unlock();
        const allocator = self.allocator;
        guard.finish();
        allocator.destroy(self);
    }
    pub fn withdrawUnpublishedWithOuter(self: *Holder, outer: *const sink.Guard) !void {
        var held = try outer.retainEpoch();
        defer held.release();
        const source = self.sourceEpoch() orelse return error.RetirementFinishHolderConsumed;
        if (!same(source, &held)) return error.BackendOperationCleanupChanged;
        self.withdrawUnpublished();
    }
    /// Nonfinal local closes keep no old B. Only final post-close metadata
    /// retirement selects BY; provider-death BX remains independently owned.
    pub fn releaseLocalWithBoundary(self: *Holder, boundary: ?*const sink.Epoch) !void {
        self.lock();
        if (self.phase != .armed or self.references == 0) {
            self.mutex.unlock();
            return error.RetirementFinishHolderConsumed;
        }
        if (!self.provider_finished or self.references > 1) {
            if (!self.provider_finished and self.references == 1) {
                self.mutex.unlock();
                return error.RetirementFinishLocalOwnerRequired;
            }
            self.references -= 1;
            self.mutex.unlock();
            return;
        }
        // Keep the last local reference alive across validation. Provider has
        // finished; no accepted service may hand out a new local close pin.
        self.mutex.unlock();
        const current = boundary orelse return error.MissingCurrentCleanupEpoch;
        var held = try current.retain();
        var moved = false;
        defer if (!moved) held.release();
        if (try held.isSealed()) return error.RetirementEpochSealed;
        self.lock();
        if (self.phase != .armed or !self.provider_finished or self.references != 1) {
            self.mutex.unlock();
            return error.RetirementFinishHolderConsumed;
        }
        self.references = 0;
        self.local_boundary = held;
        moved = true;
        self.phase = .waiting_local;
        self.mutex.unlock();
        try self.scheduleLocal();
        // A legitimate concurrently sealed BY may consume self synchronously.
    }
    /// Explicit historical outside-execution cleanup contract. Null Context
    /// alone is not proof; provider-death BX is still awaited independently.
    pub fn releaseLocalOutsideExecution(self: *Holder) !void {
        self.lock();
        if (self.phase != .armed or self.references == 0) {
            self.mutex.unlock();
            return error.RetirementFinishHolderConsumed;
        }
        if (!self.provider_finished or self.references > 1) {
            if (!self.provider_finished and self.references == 1) {
                self.mutex.unlock();
                return error.RetirementFinishLocalOwnerRequired;
            }
            self.references -= 1;
            self.mutex.unlock();
            return;
        }
        self.references = 0;
        self.phase = .waiting_local;
        self.mutex.unlock();
        try self.afterLocal();
    }
    fn same(a: *const sink.Epoch, b: *const sink.Epoch) bool {
        return a.record == b.record and a.nonce == b.nonce;
    }
    fn scheduleLocal(self: *Holder) !void {
        const source = &self.guard.?.epoch;
        if (self.local_boundary) |*boundary| {
            if (!same(source, boundary)) {
                const deferred = try boundary.boundary();
                try deferred.register(deferred.context, deferred.nonce, &self.local_node);
                return;
            }
        }
        try self.afterLocal();
    }
    fn localPassed(raw: *anyopaque) callconv(.c) void {
        const self: *Holder = @ptrCast(@alignCast(raw));
        self.afterLocal() catch fatal();
    }
    fn afterLocal(self: *Holder) !void {
        if (self.phase != .waiting_local) return error.RetirementFinishHolderConsumed;
        const source = &self.guard.?.epoch;
        const provider = if (self.provider_boundary) |*value| value else null;
        if (provider == null and !self.provider_outside_execution) return error.MissingProviderCleanupEpoch;
        const local = if (self.local_boundary) |*value| value else null;
        self.phase = .waiting_provider;
        if (provider != null and !same(source, provider.?) and (local == null or !same(local.?, provider.?))) {
            const deferred = try provider.?.boundary();
            try deferred.register(deferred.context, deferred.nonce, &self.provider_node);
            return;
        }
        self.finishNow();
    }
    fn providerPassed(raw: *anyopaque) callconv(.c) void {
        const self: *Holder = @ptrCast(@alignCast(raw));
        if (self.phase != .waiting_provider) fatal();
        self.finishNow();
    }
    fn finishNow(self: *Holder) void {
        var guard = self.guard.?;
        var provider = self.provider_boundary;
        var local = self.local_boundary;
        const allocator = self.allocator;
        self.guard = null;
        self.provider_boundary = null;
        self.local_boundary = null;
        self.phase = .finished;
        // All metadata mutexes and local SDK/close scopes have ended. Source
        // callbacks can reenter either actual provider without old authority.
        guard.finish();
        if (provider) |*epoch| epoch.release();
        if (local) |*epoch| epoch.release();
        allocator.destroy(self);
    }
};

fn testHolderActualTwoProviders(provider_first: bool) !void {
    if (comptime !@import("build_options").enable_native) return error.SkipZigTest;
    const Shared = @import("../backends/compute_context.zig").SharedBackendContext;
    const Session = @import("../backends/session.zig").Session;
    const operation = @import("backend_operation.zig");
    const ops = @import("../ops/ops.zig");
    const memory = @import("../runtime/tier/memory.zig");
    const a = std.testing.allocator;
    const Destroy = struct {
        fn context(raw: *anyopaque) void {
            const shared: *Shared = @ptrCast(@alignCast(raw));
            shared.release();
        }
    };
    const shared_a = try Shared.init(a, .native, null);
    var model_a = Session.ModelLifetime{ .owner = shared_a, .destroy_fn = Destroy.context };
    defer model_a.release();
    const shared_b = try Shared.init(a, .native, null);
    var model_b = Session.ModelLifetime{ .owner = shared_b, .destroy_fn = Destroy.context };
    defer model_b.release();
    const backend_a = shared_a.computeBackend();
    const backend_b = shared_b.computeBackend();
    var controller = memory.AdmissionController{};
    defer controller.deinit();
    var lease: ?memory.AdmissionLease = try controller.tryAcquire(.cpu, .{}, .{ .host_scratch_bytes = 16 }, false);
    defer if (lease) |*value| value.release();
    const Probe = struct {
        models: [2]*Session.ModelLifetime,
        backends: [2]ops.ComputeBackend,
        lease: *?memory.AdmissionLease,
        calls: usize = 0,
        failure: ?anyerror = null,
        fn retire(raw: *anyopaque) callconv(.c) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.lease.*) |*value| value.release();
            self.lease.* = null;
            self.reenter() catch |err| {
                self.failure = err;
            };
            self.calls += 1;
        }
        fn reenter(self: *@This()) !void {
            if (!self.models[0].execution_gate.tryLock()) return error.SourceModelStillOwned;
            defer self.models[0].execution_gate.unlock();
            if (!self.models[1].execution_gate.tryLock()) return error.DestinationModelStillOwned;
            defer self.models[1].execution_gate.unlock();
            for (&self.backends) |*backend| {
                var scope = try operation.enter(backend.*, null, null, .managed);
                defer scope.deinitChecked() catch fatal();
                const x = try backend.fromFloat32(&.{ 1, 3 });
                defer backend.free(x);
                const y = try backend.add(x, x);
                defer backend.free(y);
                const actual = try backend.toFloat32(y, std.testing.allocator);
                defer std.testing.allocator.free(actual);
                try std.testing.expectEqualSlices(f32, &.{ 2, 6 }, actual);
                try scope.finish();
            }
        }
    };
    var probe = Probe{ .models = .{ &model_a, &model_b }, .backends = .{ backend_a, backend_b }, .lease = &lease };
    var ref = try sink.Ref.create();
    defer ref.release();
    var origin = try ref.enter();
    defer origin.finish();
    var origin_epoch = try origin.retainEpoch();
    defer origin_epoch.release();
    const holder = try Holder.prepareWithAllocator(a);
    var fork = try origin.fork();
    try holder.adopt(&fork);
    const local_x = try holder.retainLocal();
    const local_y = try holder.retainLocal();
    var node = retirement.Node{ .context = &probe, .release_fn = Probe.retire };
    const boundary = try origin_epoch.boundary();
    try boundary.register(boundary.context, boundary.nonce, &node);
    var holder_live = true;
    var provider_done = false;
    var x_live = true;
    var y_live = true;
    // Registered before every actual scope/lock below: emergency retirement
    // executes only after their checked cleanup and unlock, never under a gate.
    defer if (holder_live) {
        if (!provider_done) holder.providerFinishedOutsideExecution() catch fatal();
        if (x_live) local_x.releaseLocalOutsideExecution() catch fatal();
        if (y_live) local_y.releaseLocalOutsideExecution() catch fatal();
    };
    platform.sync.lockYielding(&model_a.execution_gate);
    var a_locked = true;
    defer if (a_locked) model_a.execution_gate.unlock();
    var source_scope = try operation.enter(backend_a, null, null, .managed);
    defer source_scope.deinitChecked() catch fatal();
    try std.testing.expect(try source_scope.bindCleanupEpoch(&origin_epoch));
    const payload = try backend_a.fromFloat32(&.{ 2, 4, 6, 8 });
    var payload_live = true;
    defer if (payload_live) backend_a.free(payload);
    model_a.execution_gate.unlock();
    a_locked = false;
    // A's outer model turn ended; its actual provider is still occupied.
    origin.finish();
    if (operation.enter(backend_a, null, null, .managed)) |unexpected| {
        var owned = unexpected;
        owned.deinitChecked() catch fatal();
        return error.SourceProviderGateWasNotHeld;
    } else |err| try std.testing.expectEqual(error.QueueFull, err);
    var bx = try ref.enter();
    defer bx.finish();
    var bx_epoch = try bx.retainEpoch();
    defer bx_epoch.release();
    platform.sync.lockYielding(&model_b.execution_gate);
    var b_locked = true;
    defer if (b_locked) model_b.execution_gate.unlock();
    var scope_bx = try operation.enter(backend_b, null, null, .managed);
    defer scope_bx.deinitChecked() catch fatal();
    try std.testing.expect(try scope_bx.bindCleanupEpoch(&bx_epoch));
    // Actual source payload death + actual source provider unlock occur under
    // destination BX. Y's metadata pin must never replace BX's authority.
    backend_a.free(payload);
    payload_live = false;
    try source_scope.finish();
    try holder.providerFinished(&bx_epoch);
    provider_done = true;
    try local_x.releaseLocalWithBoundary(&bx_epoch);
    x_live = false;
    var by = try ref.enter();
    defer by.finish();
    var by_epoch = try by.retainEpoch();
    defer by_epoch.release();
    platform.sync.lockYielding(&model_a.execution_gate);
    a_locked = true;
    var scope_by = try operation.enter(backend_a, null, null, .managed);
    defer scope_by.deinitChecked() catch fatal();
    try std.testing.expect(try scope_by.bindCleanupEpoch(&by_epoch));
    try local_y.releaseLocalWithBoundary(&by_epoch);
    y_live = false;
    holder_live = false; // intrusive nodes now own the cell through BX/BY
    try std.testing.expectEqual(@as(usize, 0), probe.calls);
    if (provider_first) {
        try scope_bx.finish();
        model_b.execution_gate.unlock();
        b_locked = false;
        bx.finish();
        try std.testing.expectEqual(@as(usize, 0), probe.calls);
        try scope_by.finish();
        model_a.execution_gate.unlock();
        a_locked = false;
        by.finish();
    } else {
        try scope_by.finish();
        model_a.execution_gate.unlock();
        a_locked = false;
        by.finish();
        try std.testing.expectEqual(@as(usize, 0), probe.calls);
        // Later reserved epochs do not postpone old eligible quota.
        var unrelated = try ref.enter();
        defer unrelated.finish();
        try scope_bx.finish();
        model_b.execution_gate.unlock();
        b_locked = false;
        bx.finish();
        try std.testing.expectEqual(@as(usize, 1), probe.calls);
    }
    try std.testing.expectEqual(@as(usize, 1), probe.calls);
    if (probe.failure) |err| return err;
    try std.testing.expectEqualDeep(memory.AdmissionAmounts{}, controller.snapshot());
}

test "retained Guard waits for independent provider BX and final closer BY in both actual provider model gate orders" {
    try testHolderActualTwoProviders(false);
    try testHolderActualTwoProviders(true);
}

test "retained Guard same A finishes without cycle after local scopes and nonfinal child stores no boundary" {
    var ref = try sink.Ref.create();
    defer ref.release();
    var outer = try ref.enter();
    defer outer.finish();
    var epoch = try outer.retainEpoch();
    defer epoch.release();
    const holder = try Holder.prepareWithAllocator(std.testing.allocator);
    var fork = try outer.fork();
    try holder.adopt(&fork);
    const nonfinal = try holder.retainLocal();
    try nonfinal.releaseLocalWithBoundary(null); // no B is captured before provider death
    const final = try holder.retainLocal();
    try holder.providerFinished(&epoch);
    try final.releaseLocalWithBoundary(&epoch);
    try std.testing.expect(!try epoch.isSealed());
    outer.finish();
    try std.testing.expect(try epoch.isSealed());
}

test "retained Guard outside final close still waits for already sealed provider BX metadata safely" {
    var ref = try sink.Ref.create();
    defer ref.release();
    var source = try ref.enter();
    defer source.finish();
    var source_epoch = try source.retainEpoch();
    defer source_epoch.release();
    const holder = try Holder.prepareWithAllocator(std.testing.allocator);
    var fork = try source.fork();
    try holder.adopt(&fork);
    const local = try holder.retainLocal();
    source.finish();
    var provider = try ref.enter();
    defer provider.finish();
    var provider_epoch = try provider.retainEpoch();
    defer provider_epoch.release();
    try holder.providerFinished(&provider_epoch);
    provider.finish(); // registration later must accept valid retained sealed BX
    try std.testing.expect(!try source_epoch.isSealed());
    try local.releaseLocalOutsideExecution();
    try std.testing.expect(try source_epoch.isSealed());
}
