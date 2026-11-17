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

//! Qualified registry reads through the real native opaque owner boundary.
const std = @import("std");
const wire = @import("antfly_local_sources").storage_source_scope_control;
const metadata = @import("../metadata/api.zig");
const completion = @import("antfly_local_sources").storage_source_scope_retirement;
pub fn verify(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, source_owner: @FieldType(completion.Intent, "owner")) !void {
    const query: wire.Query = .{ .version = 1, .owner = source_owner, .domain = @splat(71), .minimum_applied_index = 1 };
    const observed = try readQualified(a, owner, fence, query);
    try observed.validate(query);
    try std.testing.expect(observed.record == null);
    try std.testing.expect(!observed.header.strict);
    try std.testing.expectEqual(@as(u16, 0), observed.header.domains);
    try std.testing.expectEqual(@as(u64, 0), observed.header.metadata_floor);
    const again = try readQualified(a, owner, fence, query);
    try std.testing.expectEqualDeep(observed.header, again.header);
    var changed = query;
    changed.owner.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, owner.sourceScopeObservationRouted(a, fence, "docs", changed));
    try verifyReadFence(a, owner, fence, query, observed.cut.index);
    try verifyOutcome(a, owner, fence, source_owner, observed.cut);
    try verifyControlGuards();
}
fn verifyReadFence(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, query: wire.Query, index: u64) !void {
    var future = query;
    future.minimum_applied_index = index + 1000000;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, readQualified(a, owner, fence, future));
    var cancelled: std.atomic.Value(bool) = .init(true);
    var cancelled_fence = fence;
    cancelled_fence.admission_cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled);
    try std.testing.expectError(error.Canceled, owner.sourceScopeObservationRouted(a, cancelled_fence, "docs", query));
}

fn verifyOutcome(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, source_owner: @FieldType(completion.Intent, "owner"), cut: wire.scope.Cut) !void {
    const ordered = @import("antfly_local_sources").storage_source_scope_ordered;
    // This command is only a recovery query, not metadata authorization or a
    // proposed mutation. The real root has no ordered control receipt yet.
    const recovery: ordered.Recovery = .{ .command = .{ .version = 1, .intent = .{ .version = 1, .owner = source_owner, .spec = .{ .actor_table_id = 7, .source_id = @splat(72), .generation = 1, .binding = @splat(73), .checkpoint_owner = try wire.scope.Basis.fromOwner(source_owner) }, .minimum_metadata_index = 1, .action = .bind }, .metadata_cut = .{ .group_id = source_owner.metadata_group_id, .incarnation = source_owner.metadata_incarnation, .index = 1 } }, .entry = cut, .minimum_applied_index = cut.index };
    try std.testing.expect(try readOutcomeQualified(a, owner, fence, recovery) == .superseded);
    // Instantiate the trusted apply wrapper and its typed ABI client, but do
    // not invent an ordered entry in this live Raft group. Wrong-group calls
    // must fail before acquiring an owner or attempting mutation dispatch.
    var descriptor = try owner.loadCompletionDescriptor(a, source_owner.group_id, "docs", .{});
    defer descriptor.deinit(a);
    try std.testing.expectError(error.HistoryResynchronizationRequired, owner.applyPreparedOrderedSourceScopeControlGroupLocalAtRaftEntry(a, source_owner.group_id + 1, "docs", descriptor.view(), recovery.command, cut.term, cut.index));
    var changed = recovery;
    changed.command.intent.owner.root_generation += 1;
    try std.testing.expectError(error.TopologyChanged, owner.sourceScopeOutcomeRouted(a, fence, "docs", changed));
    var future = recovery;
    future.minimum_applied_index += 1000000;
    try std.testing.expectError(error.StorageReadTemporarilyUnavailable, readOutcomeQualified(a, owner, fence, future));
    var cancelled: std.atomic.Value(bool) = .init(true);
    var cancelled_fence = fence;
    cancelled_fence.admission_cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled);
    try std.testing.expectError(error.Canceled, owner.sourceScopeOutcomeRouted(a, cancelled_fence, "docs", recovery));
}
fn readOutcomeQualified(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, recovery: @import("antfly_local_sources").storage_source_scope_ordered.Recovery) !@import("antfly_local_sources").storage_source_scope_ordered.Outcome {
    for (0..1000) |_| {
        return owner.sourceScopeOutcomeRouted(a, fence, "docs", recovery) catch |err| switch (err) {
            error.ReadUnavailable => {
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                continue;
            },
            else => return err,
        };
    }
    return error.TestReadFenceUnavailable;
}

fn verifyControlGuards() !void {
    const abi = @import("kernel_owner_abi");
    var request: abi.ReplicatedBatchAtRaftEntryRequest = .{ .version = abi.abi_version - 1, .raft_term = 1, .raft_index = 1 };
    var response: abi.OwnedBytes = .{ .len = 99 };
    try std.testing.expectEqual(abi.Status.invalid_abi, abi.antfly_storage_owner_ordered_source_scope_control(null, &request, &response));
    try std.testing.expectEqual(@as(usize, 0), response.len);
    request.version = abi.abi_version;
    request._reserved0 = 1;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_ordered_source_scope_control(null, &request, &response));
    request._reserved0 = 0;
    request.raft_index = 0;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_ordered_source_scope_control(null, &request, &response));
    request.raft_index = 1;
    request.request_json.len = wire.max_bytes + 1;
    try std.testing.expectEqual(abi.Status.invalid_argument, abi.antfly_storage_owner_ordered_source_scope_control(null, &request, &response));
    try std.testing.expectEqual(@as(usize, 0), response.len);
}

fn readQualified(a: std.mem.Allocator, owner: anytype, fence: metadata.CatalogRouteFence, query: wire.Query) !wire.Observation {
    for (0..1000) |_| {
        return owner.sourceScopeObservationRouted(a, fence, "docs", query) catch |err| switch (err) {
            // The real serving barrier uses tryLock. Contention cannot establish
            // a read cut; wait only for that transient failure, not future cuts.
            error.ReadUnavailable => {
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                continue;
            },
            else => return err,
        };
    }
    return error.TestReadFenceUnavailable;
}

pub fn verifyRouted(a: std.mem.Allocator, port: @import("source_scope_operations.zig").Port, fence: metadata.CatalogRouteFence, source_owner: @FieldType(completion.Intent, "owner")) !void {
    const query: wire.Query = .{ .version = 1, .owner = source_owner, .domain = @splat(71), .minimum_applied_index = 1 };
    const observed = try port.read(a, fence, "docs", query, .{});
    try observed.validate(query);
    try std.testing.expect(observed.record == null);
    try std.testing.expect(!observed.header.strict);
    const again = try port.read(a, fence, "docs", query, .{});
    try std.testing.expectEqualDeep(observed.header, again.header);
    var cancelled: std.atomic.Value(bool) = .init(true);
    try std.testing.expectError(error.Canceled, port.read(a, fence, "docs", query, .{ .cancellation = @import("antfly_cancellation").CancellationToken.fromAtomic(&cancelled) }));
}

pub fn verifySubmission(a: std.mem.Allocator, server: anytype, fence: metadata.CatalogRouteFence, source_owner: @FieldType(completion.Intent, "owner")) !void {
    var intent: wire.Intent = .{ .version = 1, .owner = source_owner, .spec = .{ .actor_table_id = 7, .source_id = @splat(72), .generation = 1, .binding = @splat(73), .checkpoint_owner = try wire.scope.Basis.fromOwner(source_owner) }, .minimum_metadata_index = 1, .action = .bind };
    intent.owner.group_id += 1;
    try std.testing.expectError(error.HistoryResynchronizationRequired, server.submitSourceScopeIntentLocal(a, fence, "docs", intent));
}
