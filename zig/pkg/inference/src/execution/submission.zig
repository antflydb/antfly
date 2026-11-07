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

pub const Scope = struct {
    backend: *const ops.ComputeBackend,
    owns: bool = false,
    foreign: bool = false,

    pub fn begin(backend: *const ops.ComputeBackend) !Scope {
        return beginIfNeeded(backend, true);
    }

    pub fn beginIfNeeded(backend: *const ops.ComputeBackend, needed: bool) !Scope {
        const commands = backend.vtable.regions.commands;
        const active = if (commands.hasActiveFrame) |f| f(backend.ptr) else false;
        const submitted = if (commands.hasSubmittedFrame) |f| f(backend.ptr) else false;
        if (active or submitted) return .{ .backend = backend, .foreign = true };
        if (!needed) return .{ .backend = backend };
        const begin_frame = commands.beginFrame orelse return .{ .backend = backend };
        if (commands.hasActiveFrame == null or commands.submitAndWaitFrame == null or commands.cancelFrame == null)
            return .{ .backend = backend };
        return .{ .backend = backend, .owns = try begin_frame(backend.ptr) };
    }

    pub fn finish(self: *Scope) !void {
        if (!self.owns) return;
        self.owns = false;
        try self.backend.vtable.regions.commands.submitAndWaitFrame.?(self.backend.ptr);
    }

    pub fn deinit(self: *Scope) void {
        if (!self.owns) return;
        self.owns = false;
        self.backend.vtable.regions.commands.cancelFrame.?(self.backend.ptr) catch {};
    }
};

test "submission scopes preserve foreign batches unwind encoding and never replay after commit" {
    const Probe = struct {
        active: bool = false,
        decline: bool = false,
        fail_submit: bool = false,
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
        fn submit(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.commits += 1;
            self.active = false;
            if (self.fail_submit) return error.SubmissionFailed;
        }
        fn cancel(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.cancels += 1;
            self.active = false;
        }
    };
    var probe = Probe{};
    var table: ops.ComputeBackend.VTable = undefined;
    table.regions = .{ .commands = .{ .beginFrame = Probe.begin, .hasActiveFrame = Probe.activeFrame, .submitAndWaitFrame = Probe.submit, .cancelFrame = Probe.cancel } };
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
}
