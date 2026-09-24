// Copyright 2026 Antfly, Inc.
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

//! Physical backup/topology regressions. Imported only by the implementation
//! test root; the API facade and its consumers remain storage-independent.

test "relational backup cohort pin cancellation survives absent live catalog and late seal" {
    const std = @import("std");
    const db = @import("../storage/db/mod.zig");
    const seal = @import("../storage/db/native_backup_seal.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    const path = try std.fmt.allocPrint(a, "{s}/retired-source", .{root});
    var runtime = try db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer runtime.deinit();
    const namespace: @import("../storage/db/doc_identity.zig").Namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 4 };
    var source = try db.DB.open(alloc, path, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
    var source_open = true;
    defer if (source_open) source.close();
    const identity = try source.relationalTopologyIdentity();
    const fence: @import("../storage/db/relational_integrity_topology_contract.zig").Fence = .{ .transition_id = 92, .attempt = 1, .admission_epoch = identity.next_epoch, .owner_group_id = 31, .peer_group_id = 31, .role = .backup_snapshot, .namespace = namespace, .catalog_digest = identity.catalog_digest };
    try source.batch(.{ .relational_topology = .{ .fence = fence, .action = .begin } });
    source.close();
    source_open = false;
    // No DB instance or public catalog is needed to consume a planned pin.
    try seal.reclaim(alloc, std.testing.io, path, .{ .cancel = fence }, .none);
    try seal.reclaim(alloc, std.testing.io, path, .{ .cancel = fence }, .none);
    source = try db.DB.open(alloc, path, .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .primary_backend = .{ .lsm = .{} }, .start_optional_runtimes = false, .start_index_workers = false });
    source_open = true;
    try std.testing.expectError(error.BackupSealReleased, source.sealBackupCohort("late-delivery", fence, .none));
}

test "relational backup cohort HA controls retain freeze and release across replay" {
    try testTopologyHAControls(false, false);
}

test "relational backup cohort and topology Raft controls retain freeze and abort across HA replay" {
    try testTopologyHAControls(true, false);
}

test "relational backup cohort topology HA split cutover preserves binary range and coverage" {
    try testTopologyHAControls(true, true);
}

fn testTopologyHAControls(comptime replicated: bool, comptime split: bool) !void {
    const std = @import("std");
    const db = @import("../storage/db/mod.zig");
    const ha = @import("../storage/hot_standby/primary.zig");
    const topology = @import("../storage/db/relational_integrity_topology_contract.zig");
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    const a = arena.allocator();
    const root = try tmp.dir.realPathFileAlloc(std.testing.io, ".", a);
    var primary = try ha.Primary.open(alloc, try std.fmt.allocPrintSentinel(a, "{s}/log", .{root}, 0), try std.fmt.allocPrintSentinel(a, "{s}/slots", .{root}, 0), .{ .cluster_id = 71, .timeline_id = 1, .epoch = 1 }, .{});
    defer primary.close();
    var runtime = try db.background_runtime.BackendRuntimeHandle.init(alloc, .{});
    defer runtime.deinit();
    const namespace: @import("../storage/db/doc_identity.zig").Namespace = .{ .table_id = 2, .shard_id = 3, .range_id = 4 };
    var source = try db.DB.open(alloc, try std.fmt.allocPrint(a, "{s}/source", .{root}), .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .ha_async_batch_mirror = .{ .primary = &primary }, .ha_write_gate = .{ .primary = &primary }, .start_optional_runtimes = false, .start_index_workers = false });
    defer source.close();
    var target = try db.DB.open(alloc, try std.fmt.allocPrint(a, "{s}/target", .{root}), .{ .backend_runtime = runtime.ptr(), .identity_namespace = namespace, .start_optional_runtimes = false, .start_index_workers = false });
    defer target.close();
    if (replicated) {
        const schema =
            \\{"version":1,"storage_mode":"relational","default_type":"row","unique_constraints":[{"name":"id_unique","columns":["id"]}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"}},"additionalProperties":false}}}}
        ;
        try source.setSchemaJson(alloc, schema);
        try target.setSchemaJson(alloc, schema);
    }
    const identity = try source.relationalTopologyIdentity();
    const fence: topology.Fence = .{ .transition_id = 81, .attempt = 1, .admission_epoch = identity.next_epoch, .owner_group_id = 31, .peer_group_id = if (replicated) 32 else 31, .role = if (split) .split_source else if (replicated) .merge_source else .backup_snapshot, .namespace = namespace, .catalog_digest = identity.catalog_digest };
    const begin_request: @import("../storage/db/types.zig").BatchRequest = .{ .relational_topology = .{ .fence = fence, .action = .begin } };
    if (replicated) {
        try source.batchRaftReplicatedApply(begin_request, .{ .index = 1, .term = 1 });
        const first_lsn = primary.lastLsn();
        try source.batchRaftReplicatedApply(begin_request, .{ .index = 1, .term = 1 });
        try std.testing.expectEqual(first_lsn, primary.lastLsn());
    } else try source.batch(begin_request);
    var begin = (try primary.log.entryAt(alloc, primary.lastLsn())).?;
    defer begin.deinit(alloc);
    try target.applyHAReplicationRecord(begin.record);
    try std.testing.expect((try target.relationalTopologyStatus()).fence.?.eql(fence));
    if (replicated) {
        const fresh = try target.beginTransaction(1);
        try std.testing.expectError(error.IntegrityTopologyBusy, target.writeTransaction(fresh, .{ .predicates = &.{.{ .key = "blocked", .expected_version = 0 }} }));
        try target.abortTransaction(fresh, 2);
    } else try std.testing.expectError(error.IntegrityTopologyBusy, target.batch(.{ .writes = &.{.{ .key = "blocked", .value = "{}" }} }));
    const release_request: @import("../storage/db/types.zig").BatchRequest = if (split)
        .{ .split_transition = .{ .kind = .finalize, .transition_id = fence.transition_id, .attempt_epoch = fence.attempt, .destination_group_id = fence.peer_group_id, .split_key = "\x80" } }
    else
        .{ .relational_topology = .{ .fence = fence, .action = if (replicated) .abort_transition else .release } };
    if (replicated) {
        try source.batchRaftReplicatedApply(release_request, .{ .index = 2, .term = 1 });
        const final_lsn = primary.lastLsn();
        try source.batchRaftReplicatedApply(release_request, .{ .index = 2, .term = 1 });
        try std.testing.expectEqual(final_lsn, primary.lastLsn());
    } else try source.batch(release_request);
    var release = (try primary.log.entryAt(alloc, primary.lastLsn())).?;
    defer release.deinit(alloc);
    try target.applyHAReplicationRecord(release.record);
    try target.applyHAReplicationRecord(release.record);
    try std.testing.expect((try target.relationalTopologyStatus()).fence == null);
    if (split) {
        try std.testing.expectEqualSlices(u8, "\x80", source.getRange().end);
        try std.testing.expectEqualSlices(u8, "\x80", target.getRange().end);
        const activation = @import("../storage/db/relational_integrity_activation_contract.zig");
        const source_coverage = (try source.core.getStoreValue(a, activation.key)).?;
        const target_coverage = (try target.core.getStoreValue(a, activation.key)).?;
        try std.testing.expectEqualSlices(u8, source_coverage, target_coverage);
    }
    if (!replicated) try target.batch(.{ .writes = &.{.{ .key = "resumed", .value = "{}" }} });
}
