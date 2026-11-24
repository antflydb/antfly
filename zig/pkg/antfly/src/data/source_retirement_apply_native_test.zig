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

//! Real native retirement dispatch/retry. Coordinator ABORT is modeled test
//! input; successful production proposal provenance and legacy handover are open.
const std = @import("std");
const retirement = @import("source_retirement_entry.zig");
const raft_batch = @import("raft_batch.zig");
const raft = @import("runtime_root.zig").raft;
const Entry = @import("raft_engine").core.Entry;
pub fn run(a: std.mem.Allocator, state: anytype, projection: anytype, owner_source: anytype, descriptor: @import("../storage/kernel_owner_descriptor.zig").Descriptor, owner: @FieldType(retirement.Entry, "source_owner"), comptime Harness: type) !void {
    var fixture: @import("source_retirement_test_fixture.zig").Fixture = .{};
    try fixture.init(owner);
    const control: retirement.Entry = .{ .table = "docs", .storage_owner = descriptor, .source_owner = owner, .source_retirement = fixture.command };
    const bytes = try retirement.encode(a, control, retirement.required_protocol_version);
    defer a.free(bytes);
    const pending: Entry = .{ .term = 1, .index = 12, .entry_type = .normal, .data = bytes };
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, Harness.apply(state, pending));
    try std.testing.expectEqual(@as(u64, 10), Harness.index(state));
    const encoded = try raft.state_machine.encodeCommittedEntries(a, &.{pending});
    defer a.free(encoded);
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, projection.applyBatch(601, pending.index, encoded));
    try std.testing.expectEqual(@as(u64, 10), (try projection.latestBatchForTransition(601)).?.last_entry_index);
    const barrier = try raft_batch.encodeProtocolBarrier(a, "docs", retirement.required_protocol_version);
    defer a.free(barrier);
    const barrier_entry: Entry = .{ .term = 1, .index = 11, .entry_type = .normal, .data = barrier };
    const barrier_bytes = try raft.state_machine.encodeCommittedEntries(a, &.{barrier_entry});
    defer a.free(barrier_bytes);
    try projection.applyBatch(601, 11, barrier_bytes);
    try Harness.apply(state, barrier_entry);
    try projection.applyBatch(601, 12, encoded);
    try Harness.lostRetirementReply(state, pending);
    try std.testing.expectEqual(@as(u64, 12), Harness.index(state));
    // There is no registered scope: the original first strict binding remains
    // blocked by genuine legacy-worker drainage. Retirement cannot create it.
    const recovered = try owner_source.applyPreparedOrderedSourceRetirementGroupLocalAtRaftEntry(a, 601, "docs", descriptor, fixture.command, 1, 12);
    try std.testing.expectEqual(retirement.wire.Rejection.conflict, recovered.rejected);
    var different_read = fixture.command;
    different_read.observation.cut.index += 1;
    try std.testing.expectError(error.ConflictingOrderedApplyReceipt, owner_source.applyPreparedOrderedSourceRetirementGroupLocalAtRaftEntry(a, 601, "docs", descriptor, different_read, 1, 12));
}
