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

//! Test-binary-only request-scoped fault checkpoints. A checkpoint may
//! interrupt only after the owning production loop reports genuine progress.
//! Release builds carry `Slot = void`, so request or environment input cannot
//! enable this facility.
const std = @import("std");
const builtin = @import("builtin");

pub const Kind = enum { cancel_prefill, cancel_replay, cancel_fit, cancel_publish, oom, nonfinite, gpu_failure };
pub const Stage = enum { prefill, replay, fit, publish, admission, gpu };

pub const Observation = struct {
    injected: bool = false,
    kind: ?Kind = null,
    stage: ?Stage = null,
    completed_units: u32 = 0,
    total_units: u32 = 0,
};

pub const Controller = struct {
    kind: Kind,
    mutex: std.atomic.Mutex = .unlocked,
    observation: Observation = .{},

    fn expectedStage(kind: Kind) Stage {
        return switch (kind) {
            .cancel_prefill => .prefill,
            .cancel_replay => .replay,
            .cancel_fit => .fit,
            .cancel_publish => .publish,
            .oom => .admission,
            .nonfinite => .fit,
            .gpu_failure => .gpu,
        };
    }

    /// Record a real request-local tracked allocator failure. The allocation
    /// attempt happens first; this method only binds the observed failure to
    /// the fault journal and returns the same error.
    pub fn recordOutOfMemory(self: *Controller) !void {
        if (!builtin.is_test or self.kind != .oom) return error.UnqualifiedFaultInjection;
        self.lock();
        defer self.mutex.unlock();
        if (self.observation.injected) return error.DuplicateFaultInjection;
        self.observation = .{ .injected = true, .kind = .oom, .stage = .admission, .completed_units = 1, .total_units = 1 };
        return error.OutOfMemory;
    }

    /// True only after at least one real fit completed and before the final
    /// fit. The caller may then poison one device-visible query scalar; the
    /// ordinary fitter must detect it before this controller records proof.
    pub fn shouldInjectNonfinite(self: *Controller, completed_units: u32, total_units: u32) bool {
        if (!builtin.is_test or self.kind != .nonfinite or completed_units == 0 or completed_units >= total_units) return false;
        self.lock();
        defer self.mutex.unlock();
        return !self.observation.injected;
    }

    /// Bind an error returned by the real fitter finite-status readback. This
    /// does not synthesize the error: callers invoke it only after observing
    /// `NonFiniteCompactionInput` from the production dispatch.
    pub fn recordNonfiniteDetection(self: *Controller, completed_units: u32, total_units: u32) !void {
        if (!builtin.is_test or self.kind != .nonfinite or completed_units == 0 or completed_units >= total_units)
            return error.UnqualifiedFaultInjection;
        self.lock();
        defer self.mutex.unlock();
        if (self.observation.injected) return error.DuplicateFaultInjection;
        self.observation = .{ .injected = true, .kind = .nonfinite, .stage = .fit, .completed_units = completed_units, .total_units = total_units };
        return error.NonFiniteCompactionInput;
    }

    /// Report a qualification-only simulated command-completion error after a
    /// real fitter dispatch has synchronously completed. This never performs
    /// an invalid Metal access or claims an observed hardware failure.
    pub fn recordGpuCompletionFailure(self: *Controller, completed_units: u32, total_units: u32) !void {
        if (!builtin.is_test or self.kind != .gpu_failure or completed_units == 0 or completed_units > total_units)
            return error.UnqualifiedFaultInjection;
        self.lock();
        defer self.mutex.unlock();
        if (self.observation.injected) return error.DuplicateFaultInjection;
        self.observation = .{ .injected = true, .kind = .gpu_failure, .stage = .gpu, .completed_units = completed_units, .total_units = total_units };
        return error.ContextMetalDispatchFailed;
    }

    fn lock(self: *Controller) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// `completed_units` is observed work from the production loop. Streaming
    /// stages require a strict partial prefix; publication interrupts only at
    /// the final transactional boundary after campaign construction.
    pub fn checkpoint(self: *Controller, stage: Stage, completed_units: u32, total_units: u32) !void {
        if (!builtin.is_test) return error.UnqualifiedFaultInjection;
        if (total_units == 0 or completed_units > total_units) return error.InvalidFaultCheckpoint;
        if (self.kind == .oom or self.kind == .nonfinite or self.kind == .gpu_failure) return;
        if (stage != expectedStage(self.kind)) return;
        const partial = switch (stage) {
            .prefill, .replay, .fit => completed_units > 0 and completed_units < total_units,
            .publish => completed_units == total_units,
            .admission, .gpu => false,
        };
        if (!partial) return;
        self.lock();
        defer self.mutex.unlock();
        if (self.observation.injected) return error.DuplicateFaultInjection;
        self.observation = .{
            .injected = true,
            .kind = self.kind,
            .stage = stage,
            .completed_units = completed_units,
            .total_units = total_units,
        };
        return error.Cancelled;
    }

    pub fn snapshot(self: *Controller) Observation {
        self.lock();
        defer self.mutex.unlock();
        return self.observation;
    }
};

pub const Slot = if (builtin.is_test) ?*Controller else void;

comptime {
    if (!builtin.is_test and Slot != void) @compileError("release build exposes context fault injection");
}

test "context fault checkpoint requires genuine partial work and injects once" {
    var replay = Controller{ .kind = .cancel_replay };
    try replay.checkpoint(.prefill, 1, 8);
    try replay.checkpoint(.replay, 0, 8);
    try replay.checkpoint(.replay, 8, 8);
    try std.testing.expect(!replay.snapshot().injected);
    try std.testing.expectError(error.Cancelled, replay.checkpoint(.replay, 2, 8));
    const observed = replay.snapshot();
    try std.testing.expect(observed.injected);
    try std.testing.expectEqual(Stage.replay, observed.stage.?);
    try std.testing.expectEqual(@as(u32, 2), observed.completed_units);
    try std.testing.expectError(error.DuplicateFaultInjection, replay.checkpoint(.replay, 3, 8));
}

test "context publish cancellation occurs only at transactional boundary" {
    var publish = Controller{ .kind = .cancel_publish };
    try publish.checkpoint(.publish, 0, 1);
    try std.testing.expectError(error.Cancelled, publish.checkpoint(.publish, 1, 1));
}

test "context OOM observation preserves the allocation error" {
    var oom = Controller{ .kind = .oom };
    try std.testing.expectError(error.OutOfMemory, oom.recordOutOfMemory());
    const observed = oom.snapshot();
    try std.testing.expectEqual(Stage.admission, observed.stage.?);
    try std.testing.expectEqual(@as(u32, 1), observed.completed_units);
}

test "context nonfinite observation requires completed fitter work" {
    var controller = Controller{ .kind = .nonfinite };
    try std.testing.expect(!controller.shouldInjectNonfinite(0, 2));
    try std.testing.expect(controller.shouldInjectNonfinite(1, 2));
    try std.testing.expectError(error.NonFiniteCompactionInput, controller.recordNonfiniteDetection(1, 2));
    const observed = controller.snapshot();
    try std.testing.expectEqual(Stage.fit, observed.stage.?);
    try std.testing.expectEqual(@as(u32, 1), observed.completed_units);
}

test "context GPU completion injection follows real completed work" {
    var controller = Controller{ .kind = .gpu_failure };
    try std.testing.expectError(error.UnqualifiedFaultInjection, controller.recordGpuCompletionFailure(0, 2));
    try std.testing.expectError(error.ContextMetalDispatchFailed, controller.recordGpuCompletionFailure(1, 2));
    const observed = controller.snapshot();
    try std.testing.expectEqual(Stage.gpu, observed.stage.?);
    try std.testing.expectEqual(@as(u32, 1), observed.completed_units);
}
