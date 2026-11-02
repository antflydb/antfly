// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Per-physical-worker validation state. Copies for new children start unvalidated;
//! a metadata wait may reenter this same worker without recursively validating.
const runtime = @import("antfly_runtime_abi");
const native = @import("antfly_local_sources").common_request_admission.runnable_scheduler;

pub const Hook = struct {
    context: *anyopaque,
    check: *const fn (*anyopaque, runtime.runnable_abi.Borrowed) anyerror!void,
    last_generation: ?u64 = null,
    active: bool = false,
    failure: ?anyerror = null,

    pub fn fresh(self: Hook) Hook {
        return .{ .context = self.context, .check = self.check };
    }
    pub fn precheck(self: *const Hook) !void {
        if (self.failure) |cause| return cause;
    }
    pub fn reject(self: *Hook, execution: *native.Execution, cause: anyerror) void {
        self.failure = self.failure orelse cause;
        self.last_generation = null;
        execution.rejectRetained(self.failure.?) catch {
            execution.park(.resource_blocked) catch {};
        };
    }
    fn fail(self: *Hook, execution: *native.Execution, cause: anyerror) anyerror {
        self.reject(execution, cause);
        return self.failure.?;
    }
    pub fn ensure(self: *Hook, execution: *native.Execution, borrow: runtime.runnable_abi.Borrowed) !void {
        try self.precheck();
        // Reentrant metadata work and cached policy never extend the original
        // root/child lifetime. Refusal returns CPU but retains physical owners.
        execution.checkLifetime() catch |err| return self.fail(execution, err);
        if (self.active or self.last_generation == execution.resume_generation) return;
        self.active = true;
        defer self.active = false;
        // A full validator may allocate or wait for catalog IO. It is outside
        // the prepared stage, so return any CPU loan and clear eligibility
        // before invoking it. Keep the original owner through this wait.
        execution.endCpuBorrowStage() catch |err| return self.fail(execution, err);
        const outcome = self.check(self.context, borrow);
        // A validator may wait or reenter this hook before returning. Keep the
        // first recorded failure, even if the outer callback reports another.
        execution.checkLifetime() catch |err| return self.fail(execution, err);
        try self.precheck();
        outcome catch |err| return self.fail(execution, err);
        self.last_generation = execution.resume_generation;
    }
};
