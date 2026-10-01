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

//! Server entry provenance, lifecycle replay, and snapshot protocol adaptation.
//! Local DB methods own the durable mutation/receipt transaction and pin safety.
const std = @import("std");
const builtin = @import("builtin");
const types = @import("db/types.zig");
const snapshots = @import("../raft/storage/native_snapshot.zig");

fn physicalOwner(owner: anytype) if (@typeInfo(@TypeOf(owner.*)) == .pointer) @TypeOf(owner.*) else @TypeOf(owner) {
    return if (@typeInfo(@TypeOf(owner.*)) == .pointer) owner.* else owner;
}
const requiresDurableLifecycleReplication = @import("db/replication_contract.zig").requiresDurableLifecycleReplication;

pub fn applyOrdered(
    owner: anytype,
    req: types.BatchRequest,
    identity: types.RaftAppliedEntryIdentity,
) anyerror!void {
    const db = physicalOwner(owner);
    const mirror_scoped_restore = requiresDurableLifecycleReplication(req) and db.local_execution.replication_async_batch_mirror != null;
    // Hot standby is process-local and includes this node's Raft follower roots.
    // Recover the local committed obligation before a Raft receipt can
    // short-circuit replay after a crash between store and hot-standby publication.
    if (mirror_scoped_restore) try db.flushDurableReplicationOutboxes();
    // Most restart replays should avoid executor health checks, resource
    // admission, transform expansion, and derived-payload construction.
    // batchInternal repeats this check under the mutation lock, which is
    // the correctness fence if another caller advances the marker here.
    if (try db.orderedMutationAlreadyApplied(identity)) {
        // Admission commits its write fence before sealing the immutable
        // pin. A crash in that window must repair the exact pending cut
        // before this outer replay fast path acknowledges the entry.
        if (req.online_source) |command| if (command == .admit) {
            if (comptime builtin.os.tag == .freestanding) {
                return error.UnsupportedPlatform;
            } else {
                try db.recoverOrderedSourceAdmission(req, identity, !mirror_scoped_restore);
            }
        };
        if (req.restore_staging) |command| if (command == .finish and
            (command.finish.phase == .validated or command.finish.phase == .published))
        {
            const current = try db.orderedApplyReceipt();
            if (current != null and current.?.index == identity.index and current.?.term == identity.term)
                try db.recoverReplicatedRestoreFinish(command.finish);
        };
        return;
    }
    var apply_req = req;
    apply_req.sync_level = .write;
    if (req.artifact_catalog != null) if (req.online_source != null or req.merge_checkpoint != null) {
        try @import("db/artifact_inventory.zig").validateRequest(req);
        try db.reconcileReplicatedArtifactAdmission(req, identity);
    };
    db.applyOrderedCommittedMutation(apply_req, identity, !mirror_scoped_restore) catch |err| switch (err) {
        error.GraphMaintenanceInProgress => return error.RaftApplyWriterUnavailable,
        else => return err,
    };
}

pub fn captureSnapshot(owner: anytype, group_id: u64, through_index: u64) !snapshots.Capture {
    const db = physicalOwner(owner);
    if (group_id == 0) return error.InvalidSnapshot;
    var pin = try db.pinTransferablePrimarySnapshot();
    errdefer pin.primary.deinit();
    if (pin.receipt != null and pin.receipt.?.index > through_index) return error.InvalidSnapshot;
    return .{ .alloc = pin.alloc, .io = pin.io, .primary = pin.primary, .identity = .{
        .group_id = group_id,
        .namespace = pin.namespace,
        .through_index = through_index,
        .native_term = if (pin.receipt) |value| value.term else 0,
        .native_index = if (pin.receipt) |value| value.index else 0,
    } };
}

pub fn verifySnapshot(owner: anytype, expected: snapshots.Identity) !void {
    try physicalOwner(owner).verifyTransferablePrimarySnapshot(expected.namespace, if (expected.native_index == 0 and expected.native_term == 0) null else .{ .term = expected.native_term, .index = expected.native_index });
}

pub fn applyStorageKernelReplicatedBatchAtRaftEntry(
    alloc: std.mem.Allocator,
    db: anytype,
    table_name: []const u8,
    group_id: u64,
    req: types.BatchRequest,
    raft_entry: types.RaftAppliedEntryIdentity,
) !void {
    // The leader admitted this immutable command under the descriptor pinned
    // in its Raft entry. A follower may already have a newer durable schema
    // when it catches up; validating against that schema would make apply
    // order depend on metadata delivery and can even reject an already
    // applied entry before the native marker gets a chance to short-circuit.
    @import("../api/local_write_test_hooks.zig").runTestBeforeBatchExecutionHook();
    if (req.transaction != null)
        try @import("server_transaction_dispatch.zig").applyReplicatedTransactionMutationAtRaftEntry(alloc, db, table_name, group_id, req, raft_entry)
    else
        try applyOrdered(&db, req, raft_entry);
}

/// The seed coordinator verifies the complete manifest and every artifact before
/// installation, then publishes the repaired staged generation before reads.
pub fn restoreAuthenticatedReplicaToStagedGeneration(
    staged: *const @import("db/generation_lifecycle.zig").StagedGeneration,
    alloc: std.mem.Allocator,
    snapshot_root: []const u8,
    path: []const u8,
    opts: @import("db/db.zig").OpenOptions,
    namespace: @import("db/doc_identity.zig").Namespace,
) !void {
    try @import("db/db.zig").DB.restoreIdentityPreservingSnapshotToStagedGeneration(staged, alloc, snapshot_root, path, opts, namespace);
}
