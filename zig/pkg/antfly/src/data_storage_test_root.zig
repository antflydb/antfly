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

const std = @import("std");

pub const storage = @import("data/storage/mod.zig");
pub const db_split_handoff = @import("data/storage/db_split_handoff.zig");

const data_store = @import("data/storage/raft_apply_store.zig");
const doc_identity = @import("storage/db/doc_identity.zig");
const range_transition = @import("data/storage/range_transition.zig");
const raft_state_machine = @import("raft/state_machine/mod.zig");
const raft_storage = @import("raft/storage/mod.zig");
const snapshot_payload_store = @import("raft/storage/snapshot_payload_store.zig");
const persistent_replica_state = @import("raft/storage/replica_state.zig");
const wal_replica_state = @import("raft/storage/wal_replica_state.zig");

test "data storage module tests are reachable" {
    std.testing.refAllDecls(storage.shard_state_store);
    std.testing.refAllDecls(storage.raft_apply_store);
    std.testing.refAllDecls(db_split_handoff);
    std.testing.refAllDecls(raft_storage.catalog);
    std.testing.refAllDecls(raft_storage.snapshot_payload_store);
    std.testing.refAllDecls(raft_storage.replica_state);
    std.testing.refAllDecls(raft_storage.wal_replica_state);
    std.testing.refAllDecls(raft_storage.wal_provider);
}

test "raft snapshot durability tests are reachable" {
    std.testing.refAllDecls(snapshot_payload_store);
    std.testing.refAllDecls(persistent_replica_state);
    std.testing.refAllDecls(wal_replica_state);
}

test "db merge coordinator reapplies target namespace for persisted reassignment opt-in" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-recover-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-recover-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    const old_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 391,
        .range_id = 9201,
    };
    const target_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 391,
        .range_id = 9202,
    };

    {
        var receiver = try db_split_handoff.Destination.init(std.testing.allocator, .{
            .root_dir = receiver_root,
            .db = .{ .identity_namespace = old_namespace },
        });
        defer receiver.deinit();
        try receiver.db.batch(.{
            .writes = &.{
                .{ .key = "doc:b", .value = "{\"v\":\"receiver\"}" },
            },
        });
    }

    {
        var coord = try db_split_handoff.MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 390,
            .receiver_group_id = 391,
            .receiver = .{
                .root_dir = receiver_root,
                .db = .{ .identity_namespace = old_namespace },
            },
        });
        defer coord.deinit();
        try coord.recordDocIdentityReassignmentOptIn();
        try coord.acceptDonorRange();
    }

    var reopened = try db_split_handoff.MergeCoordinator.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 390,
        .receiver_group_id = 391,
        .receiver = .{
            .root_dir = receiver_root,
            .db = .{
                .identity_namespace = target_namespace,
                .prefer_existing_identity_namespace = true,
            },
        },
        .receiver_identity_reassignment_namespace = target_namespace,
    });
    defer reopened.deinit();
    try std.testing.expect(reopened.allow_doc_identity_reassignment);

    const before = try reopened.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
    try std.testing.expectEqual(old_namespace.range_id, before.doc_identity.namespace_range_id);

    try reopened.acceptDonorRange();
    const after = try reopened.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
    try std.testing.expectEqual(target_namespace.table_id, after.doc_identity.namespace_table_id);
    try std.testing.expectEqual(target_namespace.shard_id, after.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(target_namespace.range_id, after.doc_identity.namespace_range_id);
}
