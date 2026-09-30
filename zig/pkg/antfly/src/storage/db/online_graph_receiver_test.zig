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

const std = @import("std");
const DB = @import("db.zig").DB;
const OpenOptions = @import("db.zig").OpenOptions;
const TestDirectory = @import("../../common/test_directory.zig").TestDirectory;
const types = @import("types.zig");
const merge_state_mod = @import("merge_state.zig");
const internal_keys = @import("../internal_keys.zig");
const enrichment_artifact_codec = @import("enrichment/artifact_codec.zig");

test "online graph snapshot receiver rebinds native effects without changing source receipts after restart" {
    const alloc = std.testing.allocator;
    const pages = @import("merge_page_contract.zig");
    const inventory = @import("artifact_inventory.zig");
    const source_catalog = @import("merge_artifact_catalog.zig");
    var source_dir = try TestDirectory.init("graph-copy-source-layout");
    defer source_dir.cleanup();
    var target_dir = try TestDirectory.init("graph-copy-target-layout");
    defer target_dir.cleanup();
    const options: OpenOptions = .{ .identity_namespace = .{ .table_id = 1, .shard_id = 3, .range_id = 3 }, .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } };
    var donor = try DB.open(alloc, source_dir.path(), .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_optional_runtimes = false, .primary_backend = .{ .lsm = .{} } });
    defer donor.close();
    var receiver = try DB.open(alloc, target_dir.path(), options);
    defer receiver.close();
    for ([_]*DB{ &donor, &receiver }) |db| {
        try db.setSchemaJson(alloc, "{}");
        try db.addIndex(.{ .name = "g", .kind = .graph, .config_json = "{}" });
    }
    try receiver.batch(.{ .writes = &.{.{ .key = "a", .value = "{}" }}, .sync_level = .write });
    try receiver.updateRange(.{ .start = "m", .end = "z" });
    var source_command = try donor.artifactInventoryCommand(alloc);
    defer source_command.catalogs.deinit(alloc);
    var target_command = try receiver.artifactInventoryCommand(alloc);
    defer target_command.catalogs.deinit(alloc);
    source_command.binding.effect_protocol = 15;
    target_command.binding.effect_protocol = 15;
    const source_generation = donor.core.index_manager.coverageGenerationForIndex("g").?;
    const target_generation = receiver.core.index_manager.coverageGenerationForIndex("g").?;
    try std.testing.expect(source_generation != target_generation);
    const identity: pages.Source = .{ .namespace = donor.core.identity_namespace, .pin_digest = @splat(3), .applied_index = 2, .retention = .{ .epoch = 1, .after_sequence = 0 }, .artifact_catalog = source_command.binding };
    const context: types.MergeReplicationContext = .{ .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .identity_namespace = receiver.core.identity_namespace, .copy_attempt = .{ .donor_term = 1, .sequence = 1 } };
    const progress: pages.Progress = .{ .version = 2, .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .receiver_namespace = receiver.core.identity_namespace, .attempt = context.copy_attempt, .source = identity, .phase = .tail };
    const checkpoint: types.MergeReplicationCheckpoint = .{ .kind = .begin_copy, .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .receiver_base_start = "m", .receiver_base_end = "z", .merged_start = "a", .merged_end = "z", .page_receiver_namespace = receiver.core.identity_namespace, .page_source = identity, .page_source_catalogs = source_command.catalogs };
    // Seed the receiver apply boundary directly. This does not exercise source
    // admission; protocol15 stays gated until every writer/tail is covered.
    var encoded_state: std.ArrayListUnmanaged(u8) = .empty;
    defer encoded_state.deinit(alloc);
    try merge_state_mod.encode(&encoded_state, alloc, .{ .transition_id = 77, .donor_group_id = 2, .receiver_group_id = 3, .phase = .accepting, .receiver_base_range = .{ .start = "m", .end = "z" }, .merged_range = .{ .start = "a", .end = "z" }, .copy_attempt = context.copy_attempt });
    const encoded_progress = try pages.encode(alloc, progress);
    defer alloc.free(encoded_progress);
    const encoded_catalog = (try source_catalog.encode(alloc, checkpoint, progress)).?;
    defer alloc.free(encoded_catalog);
    {
        var txn = try receiver.core.store.beginWriteTxn();
        errdefer txn.abort();
        try inventory.stageOrdered(alloc, &txn, target_command, 1);
        try txn.put(merge_state_mod.key, encoded_state.items);
        try txn.put(pages.key, encoded_progress);
        try txn.put(source_catalog.key, encoded_catalog);
        try txn.commit();
    }
    const edge = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "a", "g", "links", "b");
    defer alloc.free(edge);
    const value = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, 5, source_generation, 1, 0, 0, "{}");
    defer alloc.free(value);
    const expected = try enrichment_artifact_codec.encodeGraphEdgeAlloc(alloc, 5, target_generation, 1, 0, 0, "{}");
    defer alloc.free(expected);
    var request: types.BatchRequest = .{ .merge_replication = context, .merge_page = .{ .source = identity, .sequence = 1, .phase = .tail, .exhausted = false, .digest = @splat(0), .tail = .{ .fragment = .{ .sequence = 1, .offset = 0, .total_effects = 1, .frame_digest = @splat(4) } }, .artifact_effects = &.{.{ .key = edge, .value = value }} }, .sync_level = .write };
    request.merge_page.?.digest = pages.commandDigest(request);
    const source_digest = request.merge_page.?.digest;
    try receiver.batchRaftReplicatedApply(request, .{ .term = 1, .index = 2 });
    {
        const actual = try receiver.core.store.get(alloc, edge);
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, expected, actual);
        try std.testing.expectEqualSlices(u8, &source_digest, &pages.commandDigest(request));
        const raw = try receiver.core.store.get(alloc, pages.key);
        defer alloc.free(raw);
        var receipt = try pages.decode(alloc, raw);
        defer receipt.deinit();
        try std.testing.expectEqualSlices(u8, &source_digest, &receipt.value.last_digest);
    }
    receiver.close();
    receiver = try DB.open(alloc, target_dir.path(), options);
    // A delayed acknowledgement is marker-only and requires no surviving
    // source-layout cache or ordered catalog receipt.
    const ordered = try receiver.core.store.get(alloc, inventory.ordered_key);
    defer alloc.free(ordered);
    try receiver.core.store.delete(inventory.ordered_key);
    try receiver.batchRaftReplicatedApply(request, .{ .term = 1, .index = 3 });
    try receiver.core.store.put(inventory.ordered_key, ordered);
    request.merge_page.?.sequence = 2;
    request.merge_page.?.tail.?.fragment.sequence = 2;
    request.merge_page.?.artifact_effects = &.{.{ .key = edge, .value = null }};
    request.merge_page.?.digest = pages.commandDigest(request);
    try receiver.batchRaftReplicatedApply(request, .{ .term = 1, .index = 4 });
    try std.testing.expectError(error.NotFound, receiver.core.store.get(alloc, edge));
}
