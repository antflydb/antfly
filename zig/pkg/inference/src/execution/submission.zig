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

//! A borrowed backend command-batch scope. The caller owns its execution lane.
//! Only a scope which opened a batch may submit/cancel it. Submission errors
//! never cause cancellation/replay of work that may already have committed.
const std = @import("std");
const ops = @import("../ops/ops.zig");

/// One asynchronous submission belonging to the caller's operation.
pub const Pending = struct {
    backend: *const ops.ComputeBackend,
    active: bool = false,

    pub fn wait(self: *Pending) !void {
        if (!self.active) return;
        const commands = self.backend.vtable.regions.commands;
        commands.waitSubmittedFrame.?(self.backend.ptr) catch |err| {
            self.active = commands.hasSubmittedFrame.?(self.backend.ptr);
            return err;
        };
        self.active = false;
    }
};

pub const Scope = struct {
    backend: *const ops.ComputeBackend,
    owns: bool = false,
    foreign: bool = false,
    submit_attempted: bool = false,

    pub fn begin(backend: *const ops.ComputeBackend) !Scope {
        return beginIfNeeded(backend, true);
    }

    pub fn beginIfNeeded(backend: *const ops.ComputeBackend, needed: bool) !Scope {
        return beginUsing(backend, needed, false);
    }

    /// The caller owns the preceding submission and will wait for it before
    /// committing this frame. A foreign active encoder is never borrowed.
    pub fn beginPipelined(backend: *const ops.ComputeBackend) !Scope {
        const commands = backend.vtable.regions.commands;
        if (commands.submitFrame == null or commands.waitSubmittedFrame == null) return .{ .backend = backend };
        return beginUsing(backend, true, true);
    }

    fn beginUsing(backend: *const ops.ComputeBackend, needed: bool, allow_submitted: bool) !Scope {
        const commands = backend.vtable.regions.commands;
        const active = if (commands.hasActiveFrame) |f| f(backend.ptr) else false;
        const submitted = if (commands.hasSubmittedFrame) |f| f(backend.ptr) else false;
        if (active or (submitted and !allow_submitted)) return .{ .backend = backend, .foreign = true };
        if (!needed) return .{ .backend = backend };
        const begin_frame = commands.beginFrame orelse return .{ .backend = backend };
        if (commands.hasActiveFrame == null or commands.submitAndWaitFrame == null or commands.cancelFrame == null)
            return .{ .backend = backend };
        return .{ .backend = backend, .owns = try begin_frame(backend.ptr) };
    }

    pub fn finish(self: *Scope) !void {
        if (!self.owns) return;
        try self.finishUsing(self.backend.vtable.regions.commands.submitAndWaitFrame.?);
    }

    pub fn submitInto(self: *Scope, pending: *Pending) !void {
        if (!self.owns or pending.active or pending.backend != self.backend) return error.InvalidFrameSubmissionOwner;
        const commands = self.backend.vtable.regions.commands;
        const has_submitted = commands.hasSubmittedFrame orelse return error.UnsupportedOperation;
        if (commands.waitSubmittedFrame == null) return error.UnsupportedOperation;
        if (has_submitted(self.backend.ptr)) return error.InvalidFrameSubmissionOwner;
        const submit_frame = commands.submitFrame orelse return error.UnsupportedOperation;
        self.finishUsing(submit_frame) catch |err| {
            // A provider error can arrive after the frame was queued.
            pending.active = has_submitted(self.backend.ptr);
            return err;
        };
        pending.active = true;
    }

    fn finishUsing(self: *Scope, submit_frame: *const fn (*anyopaque) anyerror!void) !void {
        if (self.submit_attempted) return error.FrameSubmissionAlreadyAttempted;
        self.submit_attempted = true;
        self.owns = false;
        const commands = self.backend.vtable.regions.commands;
        submit_frame(self.backend.ptr) catch |err| {
            // A failed submit can leave an unsubmitted encoding active.
            self.owns = commands.hasActiveFrame.?(self.backend.ptr);
            return err;
        };
    }

    pub fn deinitChecked(self: *Scope) !void {
        if (!self.owns) return;
        self.owns = false;
        const commands = self.backend.vtable.regions.commands;
        if (commands.hasActiveFrame.?(self.backend.ptr)) try commands.cancelFrame.?(self.backend.ptr);
    }

    pub fn deinit(self: *Scope) void {
        self.deinitChecked() catch {};
    }
};

test "submission scopes preserve foreign batches unwind encoding and never replay after commit" {
    const Probe = struct {
        active: bool = false,
        submitted: bool = false,
        decline: bool = false,
        fail_submit: bool = false,
        fail_before_commit: bool = false,
        fail_after_queue: bool = false,
        starts: usize = 0,
        commits: usize = 0,
        cancels: usize = 0,
        fn begin(ptr: *anyopaque) !bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.decline) return false;
            self.starts += 1;
            self.active = true;
            return true;
        }
        fn activeFrame(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.active;
        }
        fn submittedFrame(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.submitted;
        }
        fn wait(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.submitted = false;
        }
        fn submit(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.fail_before_commit) return error.SubmissionFailed;
            self.commits += 1;
            self.active = false;
            if (self.fail_submit) return error.SubmissionFailed;
        }
        fn submitAsync(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try submit(ptr);
            self.submitted = true;
            if (self.fail_after_queue) return error.SubmissionFailed;
        }
        fn cancel(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cancels += 1;
            self.active = false;
        }
    };
    var probe = Probe{};
    var table: ops.ComputeBackend.VTable = undefined;
    table.regions = .{ .commands = .{ .beginFrame = Probe.begin, .hasActiveFrame = Probe.activeFrame, .submitAndWaitFrame = Probe.submit, .submitFrame = Probe.submitAsync, .waitSubmittedFrame = Probe.wait, .hasSubmittedFrame = Probe.submittedFrame, .cancelFrame = Probe.cancel } };
    const backend = ops.ComputeBackend{ .ptr = &probe, .vtable = &table };
    var completed = try Scope.begin(&backend);
    try completed.finish();
    completed.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.commits);
    try std.testing.expectEqual(@as(usize, 0), probe.cancels);
    var aborted = try Scope.begin(&backend);
    aborted.deinit();
    try std.testing.expectEqual(@as(usize, 1), probe.cancels);
    var empty_tail = try Scope.beginIfNeeded(&backend, false);
    try empty_tail.finish();
    empty_tail.deinit();
    try std.testing.expectEqual(@as(usize, 2), probe.starts);
    probe.active = true;
    var foreign = try Scope.beginIfNeeded(&backend, false);
    try std.testing.expect(foreign.foreign);
    try foreign.finish();
    foreign.deinit();
    try std.testing.expect(probe.active);
    try std.testing.expectEqual(@as(usize, 2), probe.starts);
    probe.active = false;
    probe.fail_submit = true;
    var failed = try Scope.begin(&backend);
    try std.testing.expectError(error.SubmissionFailed, failed.finish());
    failed.deinit();
    try std.testing.expectEqual(@as(usize, 2), probe.commits);
    try std.testing.expectEqual(@as(usize, 1), probe.cancels);
    probe.decline = true;
    var declined = try Scope.begin(&backend);
    try declined.finish();
    declined.deinit();
    try std.testing.expectEqual(@as(usize, 3), probe.starts);
    probe.decline = false;
    probe.fail_submit = false;
    probe.fail_before_commit = true;
    var unsubmitted = try Scope.begin(&backend);
    try std.testing.expectError(error.SubmissionFailed, unsubmitted.finish());
    try std.testing.expect(unsubmitted.owns and probe.active);
    probe.fail_before_commit = false;
    try std.testing.expectError(error.FrameSubmissionAlreadyAttempted, unsubmitted.finish());
    try unsubmitted.deinitChecked();
    try std.testing.expect(!probe.active);
    try std.testing.expectEqual(@as(usize, 2), probe.commits);
    try std.testing.expectEqual(@as(usize, 2), probe.cancels);

    probe.submitted = true;
    const ordinary = try Scope.begin(&backend);
    try std.testing.expect(ordinary.foreign and !ordinary.owns);
    var pipelined = try Scope.beginPipelined(&backend);
    defer pipelined.deinit();
    try std.testing.expect(pipelined.owns and probe.submitted);
    try Probe.wait(&probe);
    var pending: Pending = .{ .backend = &backend };
    probe.fail_after_queue = true;
    try std.testing.expectError(error.SubmissionFailed, pipelined.submitInto(&pending));
    try std.testing.expect(!pipelined.owns and !probe.active and pending.active and probe.submitted);
    try pending.wait();
    try std.testing.expect(!pending.active and !probe.submitted);
    try std.testing.expectEqual(@as(usize, 3), probe.commits);
}
