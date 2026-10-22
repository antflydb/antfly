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

comptime {
    const signature: @FieldType(engine.VTable, "prepareTransactionAssemblyWithPolicyLease") = &invoke;
    _ = signature;
    const refresh_signature: @FieldType(engine.VTable, "refreshTransactionAssembly") = &refresh;
    _ = refresh_signature;
}
