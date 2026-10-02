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

//! Server transaction command validation, participant selection and acknowledgement policy.
const std = @import("std");
const db_mod = @import("antfly_local_sources").storage_db_mod;
const transactions_mod = @import("antfly_local_sources").storage_transactions;
const local_transaction_contract = @import("antfly_local_sources").api_local_transaction_contract;
const local_write = @import("antfly_local_sources").storage_local_write;
const validateTableBatchAgainstLocalSchema = local_write.validateTableBatchAgainstLocalSchema;
const runTestBeforeBatchExecutionHook = @import("antfly_local_sources").api_local_write_test_hooks.runTestBeforeBatchExecutionHook;
const batchWritesAsTransactionWrites = local_write.batchWritesAsTransactionWrites;

pub fn applyStorageKernelReplicatedBatch(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
) !void {
    try validateTableBatchAgainstLocalSchema(alloc, db, req.writes, req.deletes, req.transforms);
    runTestBeforeBatchExecutionHook();
    if (req.transaction != null)
        try applyReplicatedTransactionMutation(alloc, db, table_name, group_id, req)
    else
        try db.batchReplicatedApply(req);
}

pub fn applyReplicatedTransactionMutation(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
) !void {
    try applyReplicatedTransactionMutationInternal(alloc, db, table_name, group_id, req, .none, null);
}

pub fn applyReplicatedTransactionMutationAtRaftEntry(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
    raft_entry: db_mod.RaftAppliedEntryIdentity,
) !void {
    try applyReplicatedTransactionMutationInternal(alloc, db, table_name, group_id, req, .none, raft_entry);
}

pub fn applyReplicatedTransactionMutationInternal(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
    visibility_cancellation: db_mod.types.CancellationToken,
    raft_entry: ?db_mod.RaftAppliedEntryIdentity,
) !void {
    const mutation = req.transaction orelse return error.InvalidBatchRequest;
    try @import("antfly_local_sources").storage_range_protection.validateRequest(req);
    if (req.relational_index_maintenance) |command| if (command.owner_group_id != group_id) return error.PreparedGenerationChanged;
    switch (mutation) {
        .begin => |begin| {
            const local_participant = try local_transaction_contract.participantIdForGroupScoped(alloc, table_name, group_id, req.restore_staging_scope, req.restore_staging_plan_id);
            defer alloc.free(local_participant);
            if (begin.participants.len == 0) return error.InvalidBatchRequest;
            var seen = std.StringHashMapUnmanaged(void).empty;
            defer seen.deinit(alloc);
            var local_present = false;
            for (begin.participants) |participant| {
                if (local_transaction_contract.parseParticipantRef(participant) == null) return error.InvalidBatchRequest;
                const entry = try seen.getOrPut(alloc, participant);
                if (entry.found_existing) return error.InvalidBatchRequest;
                if (std.mem.eql(u8, participant, local_participant)) local_present = true;
            }
            if (!local_present) return error.InvalidBatchRequest;
            const coordinator = std.mem.eql(u8, begin.participants[0], local_participant);
            const local_only = [_][]const u8{local_participant};
            // Only the coordinator owns the full participant fan-out. A
            // follower tracks itself, making successful cleanup O(N) rather
            // than every participant retrying every other participant.
            const durable_participants: []const []const u8 = if (coordinator) begin.participants else &local_only;
            if (raft_entry) |entry|
                _ = try db.beginReplicatedTransactionScoped(
                    begin.txn_id,
                    begin.begin_timestamp,
                    begin.created_at_ns,
                    durable_participants,
                    coordinator,
                    begin.retain_terminal,
                    entry,
                    req.restore_staging_scope,
                )
            else
                _ = try db.beginTransactionScoped(
                    begin.txn_id,
                    begin.begin_timestamp,
                    begin.created_at_ns,
                    durable_participants,
                    coordinator,
                    begin.retain_terminal,
                    req.restore_staging_scope,
                );
        },
        .prepare => |prepare| {
            const intents: db_mod.types.TransactionIntentRequest = .{
                .writes = batchWritesAsTransactionWrites(req.writes),
                .deletes = req.deletes,
                .transforms = req.transforms,
                .predicates = req.predicates,
                .integrity = req.integrity,
                .integrity_commands = req.integrity_commands,
                .range_guards = req.range_guards,
                .relational_activation = req.relational_activation,
                .relational_retirement = req.relational_retirement,
                .relational_index_maintenance = req.relational_index_maintenance,
                .schema_version = req.schema_version,
                .relational_schema_version = req.relational_schema_version,
                .relational_integrity_generation_set = req.relational_integrity_generation_set,
                .restore_staging_scope = req.restore_staging_scope,
                .restore_staging_plan_id = req.restore_staging_plan_id,
                .relational_repair = req.relational_repair,
            };
            if (raft_entry) |entry|
                try db.writeReplicatedTransactionAtRaftEntry(prepare.txn_id, intents, entry)
            else
                try db.writeTransaction(prepare.txn_id, intents);
        },
        .resolve => |resolve| {
            const local_participant = try local_transaction_contract.participantIdForGroupScoped(alloc, table_name, group_id, req.restore_staging_scope, req.restore_staging_plan_id);
            defer alloc.free(local_participant);
            if (raft_entry) |entry| {
                // Retained coordinators keep their own acknowledgement pending
                // until the API session registry durably records the response.
                // Everyone else records resolution, acknowledgement, and the
                // Raft receipt in one backend batch.
                const defer_coordinator_ack = db.transactionRetainsCoordinatorAcknowledgement(resolve.txn_id) catch |err| switch (err) {
                    transactions_mod.TxnError.TxnNotFound => if (resolve.status == .aborted) false else return err,
                    else => return err,
                };
                try db.resolveReplicatedTransactionAtRaftEntry(
                    resolve.txn_id,
                    resolve.status,
                    resolve.commit_version,
                    req.sync_level,
                    visibility_cancellation,
                    entry,
                    if (defer_coordinator_ack) null else local_participant,
                );
            } else {
                try db.resolveTransactionIntentsWithSyncLevelAndCancellation(
                    resolve.txn_id,
                    resolve.status,
                    resolve.commit_version,
                    req.sync_level,
                    visibility_cancellation,
                );
                const defer_coordinator_ack = db.transactionDefersCoordinatorAcknowledgement(resolve.txn_id) catch |err| switch (err) {
                    transactions_mod.TxnError.TxnNotFound => if (resolve.status == .aborted) false else return err,
                    else => return err,
                };
                if (!defer_coordinator_ack) {
                    db.markTransactionParticipantResolved(resolve.txn_id, local_participant) catch |err| switch (err) {
                        transactions_mod.TxnError.TxnNotFound => if (resolve.status != .aborted) return err,
                        else => return err,
                    };
                }
            }
        },
        .acknowledge => |ack| (if (raft_entry) |entry|
            db.markReplicatedTransactionParticipantResolvedAtRaftEntry(ack.txn_id, ack.participant, entry)
        else
            db.markTransactionParticipantResolved(ack.txn_id, ack.participant)) catch |err| switch (err) {
            // Cleanup and acknowledgements are independently retryable Raft
            // commands. Once cleanup wins, a late acknowledgement is a safe
            // no-op and must not recreate coordinator sidecar metadata.
            transactions_mod.TxnError.TxnNotFound => {},
            else => return err,
        },
        .acknowledge_many => |ack| (if (raft_entry) |entry|
            db.markReplicatedTransactionParticipantsResolvedAtRaftEntry(ack.txn_id, ack.participants, entry)
        else
            db.markTransactionParticipantsResolved(ack.txn_id, ack.participants)) catch |err| switch (err) {
            transactions_mod.TxnError.TxnNotFound => {},
            else => return err,
        },
        .cleanup => |cleanup| {
            if (raft_entry) |entry|
                _ = try db.cleanupReplicatedTransactionAtRaftEntry(
                    cleanup.txn_id,
                    cleanup.cutoff_timestamp,
                    cleanup.retained_cutoff_timestamp,
                    entry,
                )
            else
                _ = try db.cleanupTransactionMetadataIfEligible(
                    cleanup.txn_id,
                    cleanup.cutoff_timestamp,
                    cleanup.retained_cutoff_timestamp,
                );
        },
    }
}
