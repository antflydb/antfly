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

//! Deterministic grouped controls staged into the ordinary owner commit.
const std = @import("std");
const contract = @import("group_source_contract.zig");
const mutations = @import("group_source_mutations.zig");
const state = @import("group_source_state.zig");
const index = @import("group_source_index.zig");
const snapshot = @import("group_source_snapshot.zig");
const keys = @import("../internal_keys.zig");
const A = std.mem.Allocator;

pub fn requireCommandContext(overlay: *mutations.Overlay, plan: *const mutations.Plan, command: contract.Command, table_id: u64, root_incarnation: u128, namespace: [24]u8) !?mutations.Plan.Entry {
    var selected: ?mutations.Plan.Entry = null;
    for (plan.entries) |entry| if (std.mem.eql(u8, entry.name, command.artifact) and std.mem.eql(u8, &entry.configuration, &command.definition.configuration)) {
        selected = entry;
        break;
    };
    if (selected == null and command.action != .retire and !command.isActivation()) return error.PreparedGenerationChanged;
    if (try mutations.tableIdentity(overlay, table_id, root_incarnation) != command.definition.table_incarnation or
        !std.mem.eql(u8, &namespace, &command.namespace)) return error.InvalidGroupedSourceCommand;
    return selected;
}

/// A local owner has one source contribution. Its lease can remain current
/// while a newer source event is still waiting in the outbox, so lease CAS
/// alone cannot authorize fan-in computed before that event. Check the exact
/// partition revision under the same primary admission as publication.
pub fn requireLocalSourceCut(a: A, txn: anytype, publication: contract.Publish, expected_owner: state.Owner) !void {
    if (publication.topology_epoch != 0 or publication.sources.len != 1 or
        !std.meta.eql(publication.sources[0].owner, expected_owner)) return error.EnrichmentSourceChanged;
    if (publication.sources[0].revision != try index.partitionRevision(a, txn, publication.held.group.definition, publication.held.partition)) return error.EnrichmentSourceChanged;
}

pub fn stage(a: A, overlay: *mutations.Overlay, command: contract.Command, next_sequence: u64, horizons: []const u16) !snapshot.Staged {
    try command.validate();
    const empty = snapshot.Staged{ .changed = &.{}, .deleted = &.{}, .sequence = 0 };
    switch (command.action) {
        .invalidate => {
            try @import("group_source_propagation.zig").advanceHorizons(a, overlay, command.invalidate.?.owner, command.invalidate.?.event, horizons);
        },
        .claim => {
            const value = command.claim.?;
            _ = try state.claim(a, overlay, value.group, value.pending, value.owner, value.now_ms, value.ttl_ms);
        },
        .heartbeat => {
            const value = command.held.?;
            try state.heartbeat(a, overlay, value.group, value.partition, value.lease, value.now_ms, value.ttl_ms);
        },
        .publish => {
            var prepared = try snapshot.Prepared.init(a, command.artifact, command.publish.?, try state.readSnapshot(a, overlay, command.publish.?.held.group, command.publish.?.held.partition));
            defer prepared.deinit();
            return snapshot.stage(keys, a, overlay, &prepared, command.publish.?, next_sequence);
        },
        .finish => {
            const value = command.finish.?;
            const observed = (try state.readSnapshot(a, overlay, value.held.group, value.held.partition)) orelse return error.EnrichmentSourceChanged;
            if (observed.sequence != value.through_sequence or !std.mem.eql(u8, &observed.input_digest, &value.input_digest) or observed.revision != value.held.lease.revision) return error.EnrichmentSourceChanged;
            for (observed.units) |document| if (!try overlay.sourceSettled(document)) return error.ArtifactPublicationPending;
            for (observed.retired) |document| if (!try overlay.sourceSettled(document)) return error.ArtifactPublicationPending;
            try state.finish(a, overlay, value.held.group, value.held.partition, value.held.lease, value.held.now_ms, value.publication);
            for (observed.retired) |document| try @import("artifact_completion_progress.zig").clearGroupReady(overlay, overlay.completion_root, document);
        },
        .acknowledge => {
            const value = command.acknowledge.?;
            const selected = try decode(a, value.key_base64);
            const raw = try decode(a, value.observed_base64);
            const event = try index.readInvalidation(a, selected, raw);
            if (!std.meta.eql(event.definition, command.definition) or !std.mem.eql(u8, event.document, value.document)) return error.InvalidGroupedSourceCommand;
            if (value.delivery) |delivered| {
                _ = try index.setDelivery(a, overlay, selected, raw, delivered);
            } else _ = try index.acknowledge(a, overlay, selected, raw);
        },
        .backfill, .retire, .activate, .activate_catalog => return error.GroupedSourceBaselinePending,
    }
    return empty;
}
fn decode(a: A, raw: []const u8) ![]u8 {
    const decoder = std.base64.standard.Decoder;
    const bytes = try a.alloc(u8, decoder.calcSizeForSlice(raw) catch return error.InvalidGroupedSourceCommand);
    decoder.decode(bytes, raw) catch return error.InvalidGroupedSourceCommand;
    return bytes;
}

test "grouped sources production local publication fences source edits before outbox invalidation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Reader = struct {
        allocator: A,
        revision: u64 = 1,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            var bytes: [8]u8 = undefined;
            std.mem.writeInt(u64, &bytes, self.revision, .big);
            return @import("group_source_queue.zig").encode(self.allocator, key, &bytes);
        }
    };
    var reader: Reader = .{ .allocator = a };
    const owner: state.Owner = .{ .namespace = @splat(1), .incarnation = 7 };
    var cut = [_]contract.SourceCut{.{ .owner = owner, .revision = 1 }};
    var publication: contract.Publish = .{
        .held = .{ .group = .{ .definition = .{ .table_incarnation = 1, .configuration = @splat(2) }, .key = "thread" }, .partition = .{ .group = "thread", .segment = "turn", .cell = 0 }, .lease = .{ .incarnation = 7, .owner = @splat(3), .partition = @splat(4), .token = 1, .revision = 1, .expires_ms = 100 }, .now_ms = 1 },
        .topology_epoch = 0,
        .sources = &cut,
        .units = &.{},
        .input_digest = @splat(0),
    };
    try requireLocalSourceCut(a, &reader, publication, owner);
    // The group lease/revision has not changed: only the source's durable
    // partition contribution advanced while its invalidation is still queued.
    reader.revision = 2;
    try std.testing.expectError(error.EnrichmentSourceChanged, requireLocalSourceCut(a, &reader, publication, owner));
    cut[0].revision = 2;
    try requireLocalSourceCut(a, &reader, publication, owner);
    cut[0].owner.incarnation += 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, requireLocalSourceCut(a, &reader, publication, owner));
    cut[0].owner = owner;
    publication.topology_epoch = 1;
    try std.testing.expectError(error.EnrichmentSourceChanged, requireLocalSourceCut(a, &reader, publication, owner));
    publication.topology_epoch = 0;
    publication.sources = &.{};
    try std.testing.expectError(error.EnrichmentSourceChanged, requireLocalSourceCut(a, &reader, publication, owner));
}
