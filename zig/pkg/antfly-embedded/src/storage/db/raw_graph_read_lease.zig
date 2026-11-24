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

//! Physical policy-gate ownership for the opaque raw graph lease.
const std = @import("std");
const contract = @import("../raw_graph_read_lease.zig");
const Outcome = @import("../native_outcome.zig").Outcome;
const Gate = @import("row_policy_gate.zig").Gate;
const Owner = struct { allocator: std.mem.Allocator, policy: Gate.Lease };
pub fn begin(db: anytype) !contract.Handle {
    if (db.local_execution.initial_child_hidden.load(.acquire)) return error.InitialChildNotPublished;
    var policy = try db.local_execution.row_policy_gate.enterUnsupportedSearch();
    errdefer policy.release();
    const owner = try db.alloc.create(Owner);
    owner.* = .{ .allocator = db.alloc, .policy = policy };
    return .{ .context = owner, .operations = &operations, .dispatch = contract.Boundary.local_dispatch };
}
fn check(handle: contract.Handle) Outcome(void) {
    const owner = handle.resolve(Owner) catch |err| return .{ .failure = .init(@errorName(err)) };
    owner.policy.checkAt(@intCast(@divFloor(@import("antfly_platform").time.realtimeNs(), std.time.ns_per_s))) catch |err| return .{ .failure = .init(@errorName(err)) };
    return .{ .value = {} };
}
fn close(handle: contract.Handle) Outcome(void) {
    const owner = handle.resolve(Owner) catch |err| return .{ .failure = .init(@errorName(err)) };
    const allocator = owner.allocator;
    owner.policy.release();
    allocator.destroy(owner);
    return .{ .value = {} };
}
const operations: contract.Operations = .{ .check = check, .close = close };

test "raw graph opaque lease retains the actual gate and releases failed ownership allocation" {
    const Local = struct {
        initial_child_hidden: std.atomic.Value(bool) = .init(false),
        row_policy_gate: Gate = .{},
    };
    const Fixture = struct { alloc: std.mem.Allocator, local_execution: *Local };
    var state: Local = .{};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var fixture: Fixture = .{ .alloc = failing.allocator(), .local_execution = &state };
    try std.testing.expectError(error.OutOfMemory, begin(&fixture));
    try std.testing.expect(state.row_policy_gate.quiesced());
    fixture.alloc = std.testing.allocator;
    var lease: contract.Lease = .{ .handle = try begin(&fixture) };
    try state.row_policy_gate.beginPreparing(.disabled);
    try std.testing.expect(!state.row_policy_gate.quiesced());
    try std.testing.expectError(error.RowPolicyUnsupported, begin(&fixture));
    try lease.check();
    lease.release();
    try std.testing.expect(state.row_policy_gate.quiesced());
}
