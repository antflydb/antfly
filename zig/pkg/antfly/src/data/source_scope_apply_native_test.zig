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

//! Real native apply/retry qualification; metadata command authority is test
//! input here. This does not qualify a production source or worker handover.
const std = @import("std");
const scope = @import("source_scope_entry.zig");
const raft_batch = @import("raft_batch.zig");
const raft = @import("runtime_root.zig").raft;
const Entry = @import("raft_engine").core.Entry;
pub fn run(a: std.mem.Allocator, state: anytype, projection: anytype, descriptor: @import("../storage/kernel_owner_descriptor.zig").Descriptor, owner: @FieldType(scope.Entry, "source_owner"), comptime Harness: type) !void {
    const control: scope.Entry = .{ .table = "docs", .storage_owner = descriptor, .source_owner = owner, .source_scope_control = .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .spec = .{ .actor_table_id = 7, .source_id = @splat(71), .generation = 1, .binding = @splat(72), .checkpoint_owner = try scope.wire.control.scope.Basis.fromOwner(owner) }, .minimum_metadata_index = 10, .action = .bind }, .metadata_cut = .{ .group_id = owner.metadata_group_id, .incarnation = owner.metadata_incarnation, .index = 10 } } };
    const bytes = try scope.encode(a, control, scope.required_protocol_version);
    defer a.free(bytes);
    const pending: Entry = .{ .term = 1, .index = 10, .entry_type = .normal, .data = bytes };
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, Harness.apply(state, pending));
    try std.testing.expectEqual(@as(u64, 8), Harness.index(state));
    const encoded = try raft.state_machine.encodeCommittedEntries(a, &.{pending});
    defer a.free(encoded);
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, projection.applyBatch(601, pending.index, encoded));
    try std.testing.expectEqual(@as(u64, 8), (try projection.latestBatchForTransition(601)).?.last_entry_index);
    const barrier = try raft_batch.encodeProtocolBarrier(a, "docs", scope.required_protocol_version);
    defer a.free(barrier);
    const barrier_entry: Entry = .{ .term = 1, .index = 9, .entry_type = .normal, .data = barrier };
    const barrier_bytes = try raft.state_machine.encodeCommittedEntries(a, &.{barrier_entry});
    defer a.free(barrier_bytes);
    try projection.applyBatch(601, 9, barrier_bytes);
    try Harness.apply(state, barrier_entry);
    try projection.applyBatch(601, 10, encoded);
    try Harness.lostReply(state, pending);
    try std.testing.expectEqual(@as(u64, 10), Harness.index(state));
}
