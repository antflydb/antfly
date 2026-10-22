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

//! Whole-operation compiled-owner entry for uncommitted transaction assembly.
const std = @import("std");
const db = @import("db.zig");
const engine = @import("../engine.zig");
const types = @import("types.zig");
const policy = @import("row_policy_gate.zig");
const control = @import("../../common/read_admission.zig");
const physical = @import("artifact_authored_acceptance.zig");

pub fn invoke(self: *db.DB, alloc: std.mem.Allocator, txn_id: types.TxnId, commit_version: u64, lease: ?*const policy.Gate.Lease, original: ?control.Port) engine.Outcome(physical.OwnedPhysicalPlan) {
    return .{ .value = self.prepareTransactionAssemblyWithPolicyLease(alloc, txn_id, commit_version, lease, original) catch |cause| return .{ .failure = .init(@errorName(cause)) } };
}

pub fn refresh(self: *db.DB, plan: *physical.OwnedPhysicalPlan, original: ?control.Port) engine.Outcome(void) {
    return .{ .value = self.refreshTransactionAssembly(plan, original) catch |cause| return .{ .failure = .init(@errorName(cause)) } };
}

/// The original port is a synchronous borrow. Retirement callbacks remain in
/// the physical owner; only the named outcome crosses back to the caller.
pub fn drain(self: *db.DB, original: control.Port) engine.Outcome(void) {
    return .{ .value = self.drainPreparedAssembliesWithAdmission(original) catch |cause| return .{ .failure = .init(@errorName(cause)) } };
}

/// Runs in the caller's compilation so arbitrary original control failures
/// never need a status-catalog entry. The borrowed relay cannot escape invoke.
/// Reentry is mandatory after successful begin, even after lifetime rejection.
pub fn callDrain(self: *db.DB, original: control.Port, comptime invoke_owner: fn (*db.DB, control.Port) anyerror!void) !void {
    var relay: DrainRelay = .{};
    const borrowed = try relay.bind(original);
    invoke_owner(self, borrowed) catch |cause| return relay.errors.rejection orelse cause;
    if (relay.errors.rejection) |cause| return cause;
}

const DrainRelay = struct {
    errors: control.ErrorRelay = .{},
    original_wait: ?@import("../../common/execution_wait.zig").Boundary = null,
    fn bind(self: *DrainRelay, original: control.Port) !control.Port {
        const borrowed = try self.errors.bind(original);
        self.original_wait = self.errors.payload.execution_wait;
        if (self.original_wait != null) self.errors.payload.execution_wait = .{ .ptr = self, .begin = begin, .end = end };
        return borrowed;
    }
    fn reject(self: *DrainRelay, cause: anyerror) anyerror {
        self.errors.rejection = self.errors.rejection orelse cause;
        return error.Canceled;
    }
    fn begin(raw: *anyopaque) !void {
        const self: *DrainRelay = @ptrCast(@alignCast(raw));
        if (self.errors.rejection != null) return error.Canceled;
        self.original_wait.?.beginWait() catch |cause| return self.reject(cause);
    }
    fn end(raw: *anyopaque) !void {
        const self: *DrainRelay = @ptrCast(@alignCast(raw));
        // Do not gate reentry on a prior rejection: the original caller still
        // owns this balanced wait pair and must get its grant back or its own
        // reentry failure before any retirement callback can run.
        self.original_wait.?.endWait() catch |cause| return self.reject(cause);
    }
};

comptime {
    const drain_signature: @FieldType(engine.VTable, "drainPreparedAssembliesWithAdmission") = &drain;
    _ = drain_signature;
    const signature: @FieldType(engine.VTable, "prepareTransactionAssemblyWithPolicyLease") = &invoke;
    _ = signature;
    const refresh_signature: @FieldType(engine.VTable, "refreshTransactionAssembly") = &refresh;
    _ = refresh_signature;
}
