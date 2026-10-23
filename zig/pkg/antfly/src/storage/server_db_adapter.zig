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

//! Server entry provenance, lifecycle replay, and snapshot protocol adaptation.
//! Local DB methods own the durable mutation/receipt transaction and pin safety.
const std = @import("std");
const builtin = @import("builtin");
const types = @import("antfly_local_sources").storage_db_types;
const snapshots = @import("../raft/storage/native_snapshot.zig");

fn physicalOwner(owner: anytype) if (@typeInfo(@TypeOf(owner.*)) == .pointer) @TypeOf(owner.*) else @TypeOf(owner) {
    return if (@typeInfo(@TypeOf(owner.*)) == .pointer) owner.* else owner;
}
const requiresDurableLifecycleReplication = @import("antfly_local_sources").storage_db_replication_contract.requiresDurableLifecycleReplication;

pub fn applyOrdered(
    owner: anytype,
    req: types.BatchRequest,
    identity: types.OrderedApplyReceipt,
) anyerror!void {
    if (req.relational_index_owner_control != null) return error.InvalidBatchRequest;
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
        try @import("antfly_local_sources").storage_db_artifact_inventory.validateRequest(req);
        try db.reconcileReplicatedArtifactAdmission(req, identity);
    };
    db.applyOrderedCommittedMutation(apply_req, identity, !mirror_scoped_restore) catch |err| switch (err) {
        error.GraphMaintenanceInProgress => return error.RaftApplyWriterUnavailable,
        else => return err,
    };
}

/// The group and position originate in the committed-entry callback, not in
/// the command. Old replay skips preparation against a newer owner catalog.
pub fn applyOrderedGroup(owner: anytype, group_id: u64, req: types.BatchRequest, identity: types.OrderedApplyReceipt) anyerror!void {
    const control = @import("antfly_local_sources").storage_db_relational_index_raft_contract;
    try control.validateBatchRequest(req);
    const command = req.relational_index_owner_control orelse return applyOrdered(owner, req, identity);
    const fence = command.fence();
    if (group_id == 0 or fence.owner_group_id != group_id) return error.PrivateIndexOwnerJobChanged;
    const db = physicalOwner(owner);
    if (!fence.namespace.eql(db.core.identity_namespace)) return error.IdentityNamespaceMismatch;
    if (try db.orderedMutationAlreadyApplied(identity)) return;
    applyIndexEffect(db, group_id, command, identity) catch |err| {
        if (try db.orderedMutationAlreadyApplied(identity)) return;
        return err;
    };
}

fn applyIndexEffect(db: anytype, group_id: u64, command: @import("antfly_local_sources").storage_db_relational_index_raft_contract.Command, identity: types.OrderedApplyReceipt) !void {
    const local = @import("antfly_local_sources");
    const prepared_ops = local.storage_db_relational_index_owner_control;
    switch (command) {
        .control, .cancel_control => |value| {
            var prepared = try db.prepareRaftIndexOwnerControl(value);
            defer prepared.deinit();
            if (command == .cancel_control) try db.applyCommittedRaftIndexCancellationControl(&prepared, group_id, identity) else try db.applyCommittedRaftIndexOwnerControl(&prepared, group_id, identity);
        },
        .cancel_intent => |value| {
            var prepared = try prepared_ops.PreparedCancellation.init(db.alloc, value);
            defer prepared.deinit();
            try db.applyCommittedRaftIndexCancellation(&prepared, group_id, identity);
        },
        .cancel_cleanup => |value| {
            var prepared = try prepared_ops.PreparedCleanup.init(db.alloc, value);
            defer prepared.deinit();
            try db.applyCommittedRaftIndexCancellationCleanup(&prepared, group_id, identity);
        },
        .cancel_complete => |value| {
            var prepared: prepared_ops.PreparedCancellationCompletion = .{ .alloc = db.alloc, .binding = value };
            defer prepared.deinit();
            try db.applyCommittedRaftIndexCancellationCompletion(&prepared, group_id, identity);
        },
        .cancel_artifacts => |value| {
            var page = try local.storage_db_relational_index_gc.Page.prepareReplay(db.alloc, null, db.core, value);
            defer if (page) |*selected| selected.deinit();
            var prepared: prepared_ops.PreparedCancellationGc = .{
                .binding = value.binding,
                .work = if (page) |*selected| .{ .artifacts = selected } else .{ .artifact_selection_applied = &value },
            };
            try db.applyCommittedRaftIndexCancellationGc(&prepared, group_id, identity);
        },
        .cancel_claims => |value| {
            const claims = local.storage_db_relational_index_owner_job.private_claims;
            var page = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :blk try claims.GcPage.prepareReplay(db.alloc, null, &read, value);
            };
            defer if (page) |*selected| selected.deinit();
            var prepared: prepared_ops.PreparedCancellationGc = .{
                .binding = value.binding,
                .work = if (page) |*selected| .{ .claims = selected } else .{ .claim_selection_applied = &value },
            };
            try db.applyCommittedRaftIndexCancellationGc(&prepared, group_id, identity);
        },
        .cancel_release => |value| {
            var prepared = try prepared_ops.PreparedCancellationRelease.init(db.alloc, db.core, value, .{});
            defer prepared.deinit();
            try db.applyCommittedRaftIndexCancellationRelease(&prepared, group_id, identity);
        },
        .build => |value| {
            var prepared = try prepared_ops.PreparedBuild.replayPrepared(db.alloc, db.core, value);
            defer prepared.deinit();
            try db.applyCommittedRaftIndexBuild(&prepared, group_id, identity);
        },
        .claims => |value| {
            var prepared = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :blk try prepared_ops.PreparedClaims.init(db.alloc, &read, value);
            };
            defer prepared.deinit();
            try db.applyCommittedRaftIndexClaims(&prepared, group_id, identity);
        },
        .consume => |value| {
            var prepared = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :blk try prepared_ops.PreparedConsumption.init(db.alloc, &read, value);
            };
            defer prepared.deinit();
            try db.applyCommittedRaftIndexConsumption(&prepared, group_id, identity);
        },
        .inventory_validation => |value| {
            var prepared: prepared_ops.PreparedInventoryValidation = .{ .alloc = db.alloc, .batch = blk: {
                var read = try db.core.store.beginReadTxn();
                defer read.abort();
                break :blk try local.storage_db_relational_index_owner_job.private_claims.InventoryValidation.Prepared.replay(db.alloc, &read, value);
            } };
            defer prepared.deinit();
            try db.applyCommittedRaftIndexInventory(&prepared, group_id, identity);
        },
        .generation_validation => |value| {
            var prepared = try prepared_ops.PreparedGenerationValidation.replay(db.alloc, db.core, value);
            defer prepared.deinit();
            try db.applyCommittedRaftIndexGeneration(&prepared, group_id, identity);
        },
        .validation_seal => |value| {
            var prepared: prepared_ops.PreparedValidationSeal = .{ .alloc = db.alloc, .command = value };
            defer prepared.deinit();
            try db.applyCommittedRaftIndexValidation(&prepared, group_id, identity);
        },
        .install => |value| {
            var prepared = try prepared_ops.PreparedInstall.init(db.alloc, db.core, value, .{});
            defer prepared.deinit();
            try db.applyCommittedRaftIndexInstallation(&prepared, group_id, identity);
        },
        .release => |value| {
            var prepared = try prepared_ops.PreparedRelease.init(db.alloc, db.core, value, .{});
            defer prepared.deinit();
            try db.applyCommittedRaftIndexRelease(&prepared, group_id, identity);
        },
        .release_gc => |value| {
            var prepared: prepared_ops.PreparedReleaseGc = .{ .alloc = db.alloc, .command = value };
            defer prepared.deinit();
            try db.applyCommittedRaftIndexReleaseGc(&prepared, group_id, identity);
        },
    }
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
        .origin = pin.origin,
        .schema_header = pin.schema_header,
    } };
}

pub fn verifySnapshot(owner: anytype, expected: snapshots.Identity) !void {
    const cut: ?types.OrderedApplyReceipt = if (expected.native_index == 0 and expected.native_term == 0) null else .{ .term = expected.native_term, .index = expected.native_index };
    if (expected.origin) |origin| {
        try physicalOwner(owner).verifyTransferablePrimarySnapshotWithOrigin(expected.namespace, cut, origin);
    } else try physicalOwner(owner).verifyTransferablePrimarySnapshot(expected.namespace, cut);
}

pub fn applyStorageKernelReplicatedBatchAtRaftEntry(
    alloc: std.mem.Allocator,
    db: anytype,
    table_name: []const u8,
    group_id: u64,
    req: types.BatchRequest,
    raft_entry: types.OrderedApplyReceipt,
) !void {
    // The leader admitted this immutable command under the descriptor pinned
    // in its Raft entry. A follower may already have a newer durable schema
    // when it catches up; validating against that schema would make apply
    // order depend on metadata delivery and can even reject an already
    // applied entry before the native marker gets a chance to short-circuit.
    @import("antfly_local_sources").api_local_write_test_hooks.runTestBeforeBatchExecutionHook();
    if (req.transaction != null)
        try @import("server_transaction_dispatch.zig").applyReplicatedTransactionMutationAtRaftEntry(alloc, db, table_name, group_id, req, raft_entry)
    else
        try applyOrderedGroup(&db, group_id, req, raft_entry);
}

/// The seed coordinator verifies the complete manifest and every artifact before
/// installation, then publishes the repaired staged generation before reads.
pub fn restoreAuthenticatedReplicaToStagedGeneration(
    staged: *const @import("antfly_local_sources").storage_db_generation_lifecycle.StagedGeneration,
    alloc: std.mem.Allocator,
    snapshot_root: []const u8,
    path: []const u8,
    opts: @import("antfly_source_root").antfly_sources.physical_db.OpenOptions,
    namespace: @import("antfly_local_sources").storage_db_doc_identity.Namespace,
) !void {
    try @import("antfly_source_root").antfly_sources.physical_db.DB.restoreIdentityPreservingSnapshotToStagedGeneration(staged, alloc, snapshot_root, path, opts, namespace);
}
