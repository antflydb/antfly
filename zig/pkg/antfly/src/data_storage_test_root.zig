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

test "db split sync coordinator allocates destination identity namespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const src_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-identity-src", .{tmp.sub_path});
    defer std.testing.allocator.free(src_root);
    const dst_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-sync-identity-dst", .{tmp.sub_path});
    defer std.testing.allocator.free(dst_root);

    {
        var source = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = src_root });
        defer source.deinit();
        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:a:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"right-0\"}") },
            .{ .term = 1, .index = 3, .entry_type = .normal, .data = @constCast("split_prepare:doc:m") },
            .{ .term = 1, .index = 4, .entry_type = .normal, .data = @constCast("split_start:7022:doc:m") },
        });
        defer std.testing.allocator.free(setup);
        try source.snapshotBuilder().applyBatch(.{
            .group_id = 7021,
            .commit_index = 4,
            .entries_bytes = setup,
        });
    }

    const destination_namespace = doc_identity.Namespace{
        .table_id = 70,
        .shard_id = 7022,
        .range_id = 9102,
    };
    var coord = try db_split_handoff.SyncCoordinator.init(std.testing.allocator, .{
        .source_root_dir = src_root,
        .dest_root_dir = dst_root,
        .source_group_id = 7021,
        .dest_group_id = 7022,
        .dest = .{ .root_dir = dst_root, .db = .{ .identity_namespace = destination_namespace } },
    });
    defer coord.deinit();

    const result = try coord.syncOnce();
    try std.testing.expect(result.bootstrapped);
    const value = (try coord.dest.get(std.testing.allocator, "doc:t")) orelse return error.TestExpectedEqual;
    defer std.testing.allocator.free(value);
    try std.testing.expectEqualStrings("{\"v\":\"right-0\"}", value);

    const stats = try coord.dest.db.stats(std.testing.allocator);
    try std.testing.expectEqual(destination_namespace.table_id, stats.doc_identity.namespace_table_id);
    try std.testing.expectEqual(destination_namespace.shard_id, stats.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(destination_namespace.range_id, stats.doc_identity.namespace_range_id);
    try std.testing.expectEqual(@as(u64, 1), stats.doc_identity.allocated_ordinals);
    try std.testing.expect(!stats.doc_identity.rebuild_required);
}

test "db merge coordinator opt-in applies configured receiver identity namespace" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-target-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-target-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    const old_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 291,
        .range_id = 9101,
    };
    const target_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 291,
        .range_id = 9102,
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

    var coord = try db_split_handoff.MergeCoordinator.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 290,
        .receiver_group_id = 291,
        .receiver = .{
            .root_dir = receiver_root,
            .db = .{ .identity_namespace = old_namespace },
        },
        .receiver_identity_reassignment_namespace = target_namespace,
    });
    defer coord.deinit();

    try std.testing.expectError(error.DocIdentityReassignmentNotAllowed, coord.acceptDonorRange());
    try std.testing.expect(!coord.allow_doc_identity_reassignment);

    try coord.recordDocIdentityReassignmentOptIn();
    try coord.acceptDonorRange();

    {
        var reopened = try db_split_handoff.MergeCoordinator.init(std.testing.allocator, .{
            .donor_root_dir = donor_root,
            .receiver_root_dir = receiver_root,
            .donor_group_id = 290,
            .receiver_group_id = 291,
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
        const stats = try reopened.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
        try std.testing.expectEqual(target_namespace.table_id, stats.doc_identity.namespace_table_id);
        try std.testing.expectEqual(target_namespace.shard_id, stats.doc_identity.namespace_shard_id);
        try std.testing.expectEqual(target_namespace.range_id, stats.doc_identity.namespace_range_id);
    }
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

test "db merge coordinator rollback reapplies target namespace for persisted reassignment opt-in" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const donor_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-rollback-donor", .{tmp.sub_path});
    defer std.testing.allocator.free(donor_root);
    const receiver_root = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/db-merge-reassign-rollback-receiver", .{tmp.sub_path});
    defer std.testing.allocator.free(receiver_root);

    {
        var donor = try data_store.RaftApplyStore.init(std.testing.allocator, .{ .root_dir = donor_root });
        defer donor.deinit();
        const setup = try raft_state_machine.encodeCommittedEntries(std.testing.allocator, &.{
            .{ .term = 1, .index = 1, .entry_type = .normal, .data = @constCast("range:doc:m:doc:z") },
            .{ .term = 1, .index = 2, .entry_type = .normal, .data = @constCast("put:doc:t={\"v\":\"donor\"}") },
        });
        defer std.testing.allocator.free(setup);
        try donor.snapshotBuilder().applyBatch(.{
            .group_id = 490,
            .commit_index = 2,
            .entries_bytes = setup,
        });
    }

    const old_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 491,
        .range_id = 9301,
    };
    const target_namespace = doc_identity.Namespace{
        .table_id = 8,
        .shard_id = 491,
        .range_id = 9302,
    };

    {
        var receiver = try db_split_handoff.Destination.init(std.testing.allocator, .{
            .root_dir = receiver_root,
            .db = .{ .identity_namespace = old_namespace },
        });
        defer receiver.deinit();
        try receiver.db.updateRange(.{ .start = "doc:a", .end = "doc:m" });
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
            .donor_group_id = 490,
            .receiver_group_id = 491,
            .receiver = .{
                .root_dir = receiver_root,
                .db = .{ .identity_namespace = old_namespace },
            },
        });
        defer coord.deinit();
        try coord.recordDocIdentityReassignmentOptIn();
        try coord.acceptDonorRange();
        _ = try coord.syncOnce();
    }

    var reopened = try db_split_handoff.MergeCoordinator.init(std.testing.allocator, .{
        .donor_root_dir = donor_root,
        .receiver_root_dir = receiver_root,
        .donor_group_id = 490,
        .receiver_group_id = 491,
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

    const before = try reopened.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
    try std.testing.expectEqual(old_namespace.range_id, before.doc_identity.namespace_range_id);

    try std.testing.expect(try reopened.rollbackMerge());
    const status = try reopened.status();
    try std.testing.expectEqual(range_transition.TransitionPhase.rolled_back, status.phase);
    try std.testing.expectEqualStrings("doc:a", reopened.receiver.getRange().start);
    try std.testing.expectEqualStrings("doc:m", reopened.receiver.getRange().end);
    try std.testing.expect((try reopened.receiver.get(std.testing.allocator, "doc:t")) == null);

    const after = try reopened.receiver.db.runtimeStatusStatsConsistent(std.testing.allocator);
    try std.testing.expectEqual(target_namespace.table_id, after.doc_identity.namespace_table_id);
    try std.testing.expectEqual(target_namespace.shard_id, after.doc_identity.namespace_shard_id);
    try std.testing.expectEqual(target_namespace.range_id, after.doc_identity.namespace_range_id);

    var txn = try reopened.receiver.db.core.store.beginProbeTxn();
    defer txn.abort();
    const ordinal = (try doc_identity.lookupOrdinalTxn(std.testing.allocator, &txn, "doc:b")) orelse return error.TestUnexpectedResult;
    const state = (try doc_identity.lookupStateTxn(&txn, ordinal)) orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(doc_identity.canonicalDocIdForNamespace(target_namespace, "doc:b"), state.canonical_doc_id);
}
