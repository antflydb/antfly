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

const std = @import("std");
const ops = @import("../ops/ops.zig");
const session_mod = @import("session.zig");
const Session = session_mod.Session;
const Options = session_mod.ComputeBackendOptions;
const control_mod = @import("../execution_control.zig");
const Control = control_mod.InferenceExecutionControl;
const memory = @import("../runtime/tier/memory.zig");
const platform = @import("antfly_platform");

/// Completion authority for a generation of physical workspace. The backend
/// may borrow its views, but only this scope releases the request's generation.
/// No heap allocation or model-specific workspace type crosses this boundary.
pub const Completion = struct {
    ptr: *anyopaque,
    generation: u64,
    epoch: u64,
    finish_fn: *const fn (*anyopaque, u64, u64) anyerror!void,

    pub fn finish(self: Completion) !void {
        try self.finish_fn(self.ptr, self.generation, self.epoch);
    }
};

/// Keeps process protection armed until commands and physical views are drained.
pub const ManagedComputeBackend = struct {
    backend: ops.ComputeBackend,
    guard: control_mod.UninterruptibleGuard,
    owns_backend: bool = true,
    completion: ?Completion = null,

    pub fn deinit(self: *ManagedComputeBackend) void {
        if (!self.owns_backend) return;
        self.owns_backend = false;
        defer self.guard.deinit();
        self.backend.deinit();
        if (self.completion) |completion| {
            // An incomplete drain cannot safely permit arena reuse or unload.
            completion.finish() catch platform.inference_process_supervisor.restartWorker();
        }
        self.completion = null;
    }
};

pub const Acquire = *const fn (Session, std.mem.Allocator, Options) anyerror!ops.ComputeBackend;

fn acquirePublished(session: Session, allocator: std.mem.Allocator, options: Options) !ops.ComputeBackend {
    return session.acquireComputeBackend(allocator, options);
}

pub fn acquire(session: Session, allocator: std.mem.Allocator, run_budget: ?*memory.RunBudget, control: ?Control) !ManagedComputeBackend {
    return acquireWith(session, allocator, run_budget, control, acquirePublished);
}

pub fn acquireWithControl(session: Session, allocator: std.mem.Allocator, control: ?Control) !ManagedComputeBackend {
    return acquire(session, allocator, null, control);
}

/// Compatibility constructors participate in exactly the same protection and
/// rollback protocol as independently published compute providers.
pub fn acquireWith(session: Session, allocator: std.mem.Allocator, run_budget: ?*memory.RunBudget, control: ?Control, constructor: Acquire) !ManagedComputeBackend {
    const effective = control orelse Control{};
    try effective.check();
    var guard = try effective.enterUninterruptible(session.interruption());
    errdefer guard.deinit();
    var backend = try constructor(session, allocator, .{ .run_budget = run_budget });
    errdefer backend.deinit();
    backend.execution_control = control;
    if (control) |active| try active.check();
    return .{ .backend = backend, .guard = guard };
}

test "published compute provider protects construction rollback and generation completion" {
    const Probe = struct {
        armed: bool = false,
        cancelled: bool = false,
        fail: bool = false,
        cancel_on_create: bool = false,
        stage: u8 = 0,
        fn backend(_: *anyopaque) @import("backends.zig").BackendType {
            return .metal;
        }
        fn arm(raw: *anyopaque, _: control_mod.MonitorControl) !u64 {
            const p: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(!p.armed);
            p.armed = true;
            return 7;
        }
        fn disarm(raw: *anyopaque, generation: u64) void {
            const p: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(p.armed and generation == 7);
            p.armed = false;
        }
        fn isCancelled(raw: ?*anyopaque) bool {
            const p: *@This() = @ptrCast(@alignCast(raw.?));
            return p.cancelled;
        }
        fn close(raw: *anyopaque) void {
            const p: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(p.armed and p.stage == 0);
            p.stage = 1;
        }
        fn finish(raw: *anyopaque, generation: u64, epoch: u64) !void {
            const p: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(p.armed);
            try std.testing.expectEqual(@as(u8, 1), p.stage);
            try std.testing.expectEqual(@as(u64, 41), generation);
            try std.testing.expectEqual(@as(u64, 9), epoch);
            p.stage = 2;
        }
        fn create(raw: *anyopaque, _: Session, _: std.mem.Allocator, _: Options) !ops.ComputeBackend {
            const p: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expect(p.armed);
            if (p.fail) return error.ConstructionFailed;
            p.cancelled = p.cancel_on_create;
            return .{ .ptr = p, .vtable = &compute_vtable };
        }
        const compute_vtable: ops.ComputeBackend.VTable = blk: {
            var v: ops.ComputeBackend.VTable = undefined;
            v.deinitBackend = close;
            break :blk v;
        };
    };
    var probe = Probe{};
    var vtable: Session.VTable = undefined;
    vtable.backend = Probe.backend;
    vtable.interruption = null;
    const session = Session{ .ptr = &probe, .vtable = &vtable, .compute_backend_provider = .{ .ptr = &probe, .acquire = Probe.create } };
    const control = Control{
        .hard_cancellation = .{ .ptr = &probe, .arm_fn = Probe.arm, .disarm_fn = Probe.disarm },
        .cancellation = .{ .ptr = &probe, .is_cancelled_fn = Probe.isCancelled },
    };
    try std.testing.expectError(error.ProcessIsolationRequired, acquire(session, std.testing.allocator, null, null));
    probe.fail = true;
    try std.testing.expectError(error.ConstructionFailed, acquire(session, std.testing.allocator, null, control));
    try std.testing.expect(!probe.armed);
    probe.fail = false;
    probe.cancel_on_create = true;
    try std.testing.expectError(error.Cancelled, acquire(session, std.testing.allocator, null, control));
    try std.testing.expect(!probe.armed);
    try std.testing.expectEqual(@as(u8, 1), probe.stage);
    probe.cancelled = false;
    probe.cancel_on_create = false;
    probe.stage = 0;
    var managed = try acquire(session, std.testing.allocator, null, control);
    managed.completion = .{ .ptr = &probe, .generation = 41, .epoch = 9, .finish_fn = Probe.finish };
    managed.deinit();
    managed.deinit();
    try std.testing.expect(!probe.armed);
    try std.testing.expectEqual(@as(u8, 2), probe.stage);
}
