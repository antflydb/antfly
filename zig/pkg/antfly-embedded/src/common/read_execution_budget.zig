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
const runtime = @import("antfly_runtime_abi");

/// Synchronous read admission under the caller's original controls. The budget
/// and executor outlive all cursor callbacks; no returned owner retains them.
pub fn Budget(comptime Controls: type, comptime check: anytype) type {
    return struct {
        controls: Controls,
        io: std.Io,
        rejection: ?anyerror = null,
        parked_class: ?runtime.runnable_abi.Class = null,
        publication_scope: bool = false,
        publication_parked: bool = false,

        pub fn checkpoint(self: *@This()) !void {
            if (self.rejection) |err| return err;
            self.checkControls() catch |err| {
                self.rejection = err;
                return err;
            };
        }
        /// Locked metadata readers may inspect original lifetime without
        /// parking or reacquiring CPU inside their physical critical section.
        pub fn checkLifetime(self: *@This()) !void {
            if (self.rejection) |err| return err;
            self.checkLifetimeControls() catch |err| {
                self.rejection = err;
                return err;
            };
        }
        fn checkLifetimeControls(self: *@This()) !void {
            var controls = self.controls;
            if (controls.execution_runnable) |borrow| {
                if (borrow.check_lifetime == null) return error.ExecutionWaitUnsupported;
                try borrow.checkLifetime();
                controls.execution_runnable = null;
            }
            try check(controls);
        }
        fn checkControls(self: *@This()) !void {
            var controls = self.controls;
            if (self.parked_class != null) {
                // Admission can check before a grant exists and again just
                // after granting it. Neither restriction may perform a CPU
                // checkpoint or yield the grant while enterChecked owns it.
                const borrow = controls.execution_runnable orelse return error.ExecutionWaitUnsupported;
                try borrow.checkLifetime();
                controls.execution_runnable = null;
            }
            try check(controls);
        }
        pub fn publication(self: *@This()) ?runtime.execution_wait.GuardedPublication {
            const borrow = self.controls.execution_runnable orelse return null;
            if (borrow.park_publication == null or borrow.resume_publication == null or borrow.finish_publication == null) return null;
            return .{
                .scope = .{ .ptr = self, .begin = beginPublication, .end = finishPublication },
                .wait = .{ .ptr = self, .begin = parkPublication, .end = resumePublication },
            };
        }
        fn beginPublication(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkLifetime();
            if (self.publication_scope or self.parked_class != null) return error.InvalidRunnableTransition;
            self.publication_scope = true;
        }
        fn parkPublication(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkLifetime();
            if (!self.publication_scope or self.publication_parked or self.parked_class != null) return error.InvalidRunnableTransition;
            self.publication_parked = try self.controls.execution_runnable.?.parkPublication();
        }
        fn resumePublication(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.publication_scope) return error.InvalidRunnableTransition;
            if (self.publication_parked) {
                self.publication_parked = false;
                // The scope retains the exact DB fences. Revalidate original
                // lifetime, not a read callback that could acquire them again.
                try self.controls.execution_runnable.?.resumePublication(&.{ .context = self, .check = publicationReentry });
            }
            try self.checkLifetime();
        }
        fn publicationReentry(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checkLifetime() catch |cause| return runtime.error_abi.statusFromError(cause);
            return .ok;
        }
        fn finishPublication(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.publication_scope) return error.InvalidRunnableTransition;
            self.publication_scope = false;
            self.publication_parked = false;
            // Always retire the reserve, even after a sticky caller rejection.
            try self.controls.execution_runnable.?.finishPublication();
        }

        pub fn executionBoundary(self: *@This()) ?@import("execution_wait.zig").Boundary {
            if (self.controls.execution_runnable == null) return null;
            return .{ .ptr = self, .begin = begin, .end = end };
        }
        pub fn delegationBoundary(self: *@This()) ?@import("read_admission.zig").Delegation {
            if (self.controls.execution_runnable == null) return null;
            return .{ .suspension = .{ .ptr = self, .begin = begin, .end = end } };
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkpoint();
            if (self.parked_class != null) return error.InvalidRunnableTransition;
            const borrow = self.controls.execution_runnable orelse return error.ExecutionWaitUnsupported;
            try borrow.validate();
            // Reject missing retained-lifetime authority before releasing CPU.
            if (borrow.check_lifetime == null) return error.ExecutionWaitUnsupported;
            if (@hasField(Controls, "require_checked_reentry")) {
                if (self.controls.require_checked_reentry and borrow.enter_checked == null) return error.ExecutionWaitUnsupported;
            }
            const phase = try borrow.currentClass();
            try checked(borrow.park(borrow.context, @backingInt(runtime.runnable_abi.Wait.resource)));
            self.parked_class = phase;
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.rejection) |err| return err;
            const phase = self.parked_class orelse return error.InvalidRunnableTransition;
            const borrow = self.controls.execution_runnable orelse return error.ExecutionWaitUnsupported;
            const resumed = if (borrow.enter_checked != null)
                borrow.enterChecked(phase, &.{ .context = self, .check = reentryCheck })
            else
                checked(borrow.enter(borrow.context, @backingInt(phase)));
            resumed catch |err| {
                self.rejection = err;
                return err;
            };
            self.parked_class = null;
            try self.checkpoint();
        }
        fn reentryCheck(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checkpoint() catch |err| return runtime.error_abi.statusFromError(err);
            return .ok;
        }
        pub fn waitForProgress(self: *@This()) !void {
            try self.checkpoint();
            if (self.executionBoundary()) |boundary| {
                boundary.run(std.Io.sleep, .{ self.io, std.Io.Duration.fromMilliseconds(1), std.Io.Clock.awake }) catch |err| {
                    self.rejection = err;
                    return err;
                };
            } else try self.io.sleep(.fromMilliseconds(1), .awake);
            try self.checkpoint();
        }
        fn checked(status: runtime.error_abi.Status) !void {
            if (!status.isOk()) return runtime.error_abi.errorFromStatus(status);
        }
    };
}
