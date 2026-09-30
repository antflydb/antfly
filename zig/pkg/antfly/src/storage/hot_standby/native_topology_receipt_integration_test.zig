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

//! Hot standby integration for local native_topology_receipt mutations.
const engine = @import("../db/native_topology_receipt.zig");
const authority = @import("../source_authority.zig");
const key = engine.test_support.key;
const replication_ingress = @import("../db/replication_ingress.zig");
const stage = engine.stage;
const std = @import("std");
const topology = @import("../db/relational_integrity_topology_contract.zig");

const hot_standby_publisher_adapter = @import("db_commit.zig");

test "native topology receipts survive restart and exact standby replay without Raft watermarks" {
    const alloc = std.testing.allocator;
    const db_mod = @import("../db/db.zig");
    const effects = @import("../db/replication_effects.zig");
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const primary_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native-topology-primary", .{tmp.sub_path});
    defer alloc.free(primary_path);
    const replica_path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/native-topology-replica", .{tmp.sub_path});
    defer alloc.free(replica_path);
    const log_path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/native-topology-log", .{tmp.sub_path}, 0);
    defer alloc.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(alloc, ".zig-cache/tmp/{s}/native-topology-slots", .{tmp.sub_path}, 0);
    defer alloc.free(slots_path);
    var stream = try @import("primary.zig").Primary.open(alloc, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 11, .shard_id = 12 }, .{});
    defer stream.close();
    const ns: @import("../db/doc_identity_namespace.zig").Namespace = .{ .table_id = 11, .shard_id = 12, .range_id = 13 };
    const options: db_mod.OpenOptions = .{ .identity_namespace = ns, .online_source_authority = .native, .primary_backend = .{ .lsm = .{} }, .start_index_workers = false, .start_optional_runtimes = false };
    var primary_options = options;
    primary_options.replication_async_batch_mirror = .{ .publisher = hot_standby_publisher_adapter.bind(&stream) };
    var primary = try db_mod.DB.open(alloc, primary_path, primary_options);
    var primary_open = true;
    defer if (primary_open) primary.close();
    var replica = try db_mod.DB.open(alloc, replica_path, options);
    defer replica.close();
    try primary.setSchemaJson(alloc, "{}");
    try replica.setSchemaJson(alloc, "{}");
    const identity = try primary.relationalTopologyIdentity();
    const fence: topology.Fence = .{ .namespace = ns, .role = .rewrite_source, .owner_group_id = 12, .peer_group_id = 14, .transition_id = 15, .attempt = 1, .admission_epoch = identity.next_epoch, .catalog_digest = identity.catalog_digest };
    const begin: @import("../db/types.zig").BatchRequest = .{ .relational_topology = .{ .fence = fence, .action = .begin } };
    try std.testing.expectError(error.OnlineSourceScopeChanged, primary.batchRaftReplicatedApply(begin, .{ .term = 1, .index = 1 }));
    try primary.batch(begin);
    try primary.batch(begin); // Lost acknowledgement, identical position.
    {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), (try authority.load(&read)).?.sequence);
    }
    try std.testing.expectEqual(@as(u64, 2), stream.lastLsn());
    var first = (try stream.log.entryAt(alloc, 1)).?;
    defer first.deinit(alloc);
    var duplicate = (try stream.log.entryAt(alloc, 2)).?;
    defer duplicate.deinit(alloc);
    var decoded = try effects.decodeBatchMutationRequest(alloc, first.record);
    defer decoded.deinit();
    try std.testing.expectEqual(@as(u64, 1), decoded.value.native_topology_position.?.sequence);
    try std.testing.expect(decoded.value.graph_retirement_raft_entry == null);
    try replication_ingress.applyRecord(&replica, first.record);
    try replication_ingress.applyRecord(&replica, first.record);
    try replication_ingress.applyRecord(&replica, duplicate.record);
    try std.testing.expect((try replica.relationalTopologyStatus()).fence.?.eql(fence));
    // Invalid seal must roll back its clock and dedup record with the effect.
    var invalid = begin;
    invalid.relational_topology.?.action = .seal_generation_handoff;
    invalid.relational_topology.?.generation_handoff_seal = .{ .plan_digest = @splat(1), .admissions_digest = @splat(2), .retired_digest = @splat(3), .retired_count = 0 };
    try std.testing.expectError(error.GenerationHandoffIntentMissing, primary.batch(invalid));
    try std.testing.expectEqual(@as(u64, 2), stream.lastLsn());
    {
        var read = try primary.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 1), (try authority.load(&read)).?.sequence);
    }
    var cancel = begin;
    cancel.relational_topology.?.action = .cancel;
    try primary.batch(cancel);
    var canceled = (try stream.log.entryAt(alloc, 3)).?;
    defer canceled.deinit(alloc);
    try replication_ingress.applyRecord(&replica, canceled.record);
    primary.close();
    primary_open = false;
    primary = try db_mod.DB.open(alloc, primary_path, primary_options);
    primary_open = true;
    // Delayed begin retries cannot resurrect a canceled fence.
    try primary.batch(begin);
    try primary.batch(cancel);
    try std.testing.expect((try primary.relationalTopologyStatus()).fence == null);
    try std.testing.expect((try replica.relationalTopologyStatus()).fence == null);
    try std.testing.expect((try primary.raftAppliedEntry()) == null);
    try std.testing.expect((try replica.raftAppliedEntry()) == null);
    for ([_]*db_mod.DB{ &primary, &replica }) |owner| {
        var read = try owner.core.store.beginReadTxn();
        defer read.abort();
        try std.testing.expectEqual(@as(u64, 2), (try authority.load(&read)).?.sequence);
    }
    var smuggled = begin;
    smuggled.writes = &.{.{ .key = "bad", .value = "{}" }};
    try std.testing.expectError(error.InvalidBatchRequest, effects.encodeNativeTopologyMutationRequestAlloc(alloc, smuggled, .{ .namespace = ns, .sequence = 3 }));
    try std.testing.expectError(error.IdentityNamespaceMismatch, effects.encodeNativeTopologyMutationRequestAlloc(alloc, begin, .{ .namespace = .{ .table_id = 11, .shard_id = 99, .range_id = 13 }, .sequence = 3 }));
    // A new stream LSN does not authorize changing a previously committed
    // native receipt position. Reject it without advancing either cursor.
    const forged_bytes = try effects.encodeNativeTopologyMutationRequestAlloc(alloc, begin, .{ .namespace = ns, .sequence = 99 });
    defer alloc.free(forged_bytes);
    var forged = canceled.record;
    forged.lsn = 4;
    forged.previous_lsn = 3;
    forged.payload = forged_bytes;
    try std.testing.expectError(error.InvalidControlReceiptPosition, replication_ingress.applyRecord(&replica, forged));
    try std.testing.expectEqual(@as(u64, 3), try replica.replicationAppliedSequence());
    {
        var txn = try replica.core.store.beginWriteTxn();
        defer txn.abort();
        const adopted: @import("../db/doc_identity_namespace.zig").Namespace = .{ .table_id = 21, .shard_id = 22, .range_id = 23 };
        const namespace_bytes = @import("../db/online_source_contract.zig").namespaceBytes(adopted);
        try txn.put(&@import("../internal_keys.zig").identity_namespace_key, &namespace_bytes);
        try authority.bind(&txn, .native, namespace_bytes);
        var adopted_command = begin.relational_topology.?;
        adopted_command.fence.namespace = adopted;
        const prepared = try stage(alloc, &txn, adopted_command, null);
        try std.testing.expectEqual(@as(u64, 1), prepared.receipt.sequence);
        try std.testing.expect(!prepared.duplicate);
        try std.testing.expect(prepared.receipt.namespace.eql(adopted));
    }
}
