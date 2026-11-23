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

//! Actual native closure dispatch, activation and lost-reply replay.
const std = @import("std");
const closure = @import("source_producer_closure_entry.zig");
const raft_batch = @import("raft_batch.zig");
const raft = @import("runtime_root.zig").raft;
const Entry = @import("raft_engine").core.Entry;
pub fn run(a: std.mem.Allocator, state: anytype, projection: anytype, owner_source: anytype, descriptor: @import("../storage/kernel_owner_descriptor.zig").Descriptor, owner: @FieldType(closure.Entry, "source_owner"), comptime Harness: type) !void {
    const control: closure.Entry = .{ .table = "docs", .storage_owner = descriptor, .source_owner = owner, .source_producer_closure = true };
    const bytes = try closure.encode(a, control, closure.required_protocol_version);
    defer a.free(bytes);
    const pending: Entry = .{ .term = 1, .index = 14, .entry_type = .normal, .data = bytes };
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, Harness.apply(state, pending));
    try std.testing.expectEqual(@as(u64, 12), Harness.index(state));
    const encoded = try raft.state_machine.encodeCommittedEntries(a, &.{pending});
    defer a.free(encoded);
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, projection.applyBatch(601, 14, encoded));
    try std.testing.expectEqual(@as(u64, 12), (try projection.latestBatchForTransition(601)).?.last_entry_index);
    const barrier = try raft_batch.encodeProtocolBarrier(a, "docs", closure.required_protocol_version);
    defer a.free(barrier);
    const barrier_entry: Entry = .{ .term = 1, .index = 13, .entry_type = .normal, .data = barrier };
    const barrier_bytes = try raft.state_machine.encodeCommittedEntries(a, &.{barrier_entry});
    defer a.free(barrier_bytes);
    try projection.applyBatch(601, 13, barrier_bytes);
    try Harness.apply(state, barrier_entry);
    try projection.applyBatch(601, 14, encoded);
    try Harness.lostProducerClosureReply(state, pending);
    try std.testing.expectEqual(@as(u64, 14), Harness.index(state));
    const recovered = try owner_source.applyPreparedOrderedSourceProducerClosureGroupLocalAtRaftEntry(a, 601, "docs", descriptor, owner, 1, 14);
    try std.testing.expectEqual(@as(u64, 14), recovered.first_closed_at.index);
    try std.testing.expectEqual(@as(u64, 1), recovered.first_closed_at.term);
    try std.testing.expectEqualDeep(try @import("antfly_local_sources").storage_source_scope.Basis.fromOwner(owner), recovered.root);
    // Intake closure cannot stand in for original-decision drainage.
    const scope = @import("antfly_local_sources").storage_source_scope_control;
    const command: scope.Command = .{ .version = 1, .intent = .{ .version = 1, .owner = owner, .spec = .{ .actor_table_id = 7, .source_id = @splat(3), .generation = 1, .binding = @splat(4), .checkpoint_owner = recovered.root }, .minimum_metadata_index = 1, .action = .bind }, .metadata_cut = .{ .group_id = owner.metadata_group_id, .incarnation = owner.metadata_incarnation, .index = 1 } };
    const binding_bytes = try @import("source_scope_entry.zig").encode(a, .{ .table = "docs", .storage_owner = descriptor, .source_owner = owner, .source_scope_control = command }, closure.required_protocol_version);
    defer a.free(binding_bytes);
    const binding_entry: Entry = .{ .term = 1, .index = 15, .entry_type = .normal, .data = binding_bytes };
    const encoded_binding = try raft.state_machine.encodeCommittedEntries(a, &.{binding_entry});
    defer a.free(encoded_binding);
    try projection.applyBatch(601, 15, encoded_binding);
    try Harness.closureStillRequiresDrainage(state, binding_entry);
    try std.testing.expectEqual(@as(u64, 15), Harness.index(state));
    const binding = try owner_source.applyPreparedOrderedSourceScopeControlGroupLocalAtRaftEntry(a, 601, "docs", descriptor, command, 1, 15);
    try std.testing.expect(binding == .rejected and binding.rejected == .pending);
}
