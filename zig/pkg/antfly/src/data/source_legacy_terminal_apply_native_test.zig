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

//! Actual activated native dispatch, durable rejection and lost-reply replay.
const std = @import("std");
const legacy = @import("source_legacy_terminal_entry.zig");
const raft_batch = @import("raft_batch.zig");
const raft = @import("runtime_root.zig").raft;
const Entry = @import("raft_engine").core.Entry;
pub fn run(a: std.mem.Allocator, state: anytype, projection: anytype, owner_source: anytype, descriptor: @import("../storage/kernel_owner_descriptor.zig").Descriptor, owner: @FieldType(legacy.Entry, "source_owner"), comptime Harness: type) !void {
    const policy = legacy.wire.policy;
    const command: policy.LegacyTerminalCommand = .{ .version = 1, .owner = owner, .proof = .{ .root = try @import("antfly_local_sources").storage_source_scope.Basis.fromOwner(owner), .transaction_id = @splat(191), .record_digest = @splat(192) } };
    const control: legacy.Entry = .{ .table = "docs", .storage_owner = descriptor, .source_owner = owner, .source_legacy_terminal = command };
    const bytes = try legacy.encode(a, control, legacy.required_protocol_version);
    defer a.free(bytes);
    const pending: Entry = .{ .term = 1, .index = 17, .entry_type = .normal, .data = bytes };
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, Harness.apply(state, pending));
    try std.testing.expectEqual(@as(u64, 15), Harness.index(state));
    const encoded = try raft.state_machine.encodeCommittedEntries(a, &.{pending});
    defer a.free(encoded);
    try std.testing.expectError(error.UnsupportedBatchProtocolVersion, projection.applyBatch(601, 17, encoded));
    try std.testing.expectEqual(@as(u64, 15), (try projection.latestBatchForTransition(601)).?.last_entry_index);
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const unsupported_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/legacy-no-native", .{tmp.sub_path});
    defer a.free(unsupported_path);
    var unsupported = try @TypeOf(projection.*).init(a, .{ .root_dir = unsupported_path, .native_source_delegate = false });
    defer unsupported.deinit();
    try std.testing.expectError(error.StorageKernelOwnerUnavailable, unsupported.applyBatch(601, 17, encoded));
    const barrier = try raft_batch.encodeProtocolBarrier(a, "docs", legacy.required_protocol_version);
    defer a.free(barrier);
    const barrier_entry: Entry = .{ .term = 1, .index = 16, .entry_type = .normal, .data = barrier };
    const barrier_bytes = try raft.state_machine.encodeCommittedEntries(a, &.{barrier_entry});
    defer a.free(barrier_bytes);
    try projection.applyBatch(601, 16, barrier_bytes);
    try Harness.apply(state, barrier_entry);
    try projection.applyBatch(601, 17, encoded);
    try Harness.legacyTerminal(state, pending, true);
    const recovered = try owner_source.applyPreparedOrderedLegacyTerminalResolutionGroupLocalAtRaftEntry(a, 601, "docs", descriptor, command, 1, 17);
    try std.testing.expect(recovered == .rejected and recovered.rejected == .not_found);
    var next = control;
    next.source_legacy_terminal.proof.transaction_id = @splat(193);
    const next_bytes = try legacy.encode(a, next, legacy.required_protocol_version);
    defer a.free(next_bytes);
    const second: Entry = .{ .term = 1, .index = 18, .entry_type = .normal, .data = next_bytes };
    const encoded_second = try raft.state_machine.encodeCommittedEntries(a, &.{second});
    defer a.free(encoded_second);
    try projection.applyBatch(601, 18, encoded_second);
    try Harness.legacyTerminal(state, second, false);
    try std.testing.expectEqual(@as(u64, 18), Harness.index(state));
    const superseded = try owner_source.applyPreparedOrderedLegacyTerminalResolutionGroupLocalAtRaftEntry(a, 601, "docs", descriptor, command, 1, 17);
    try std.testing.expect(superseded == .superseded);
}
