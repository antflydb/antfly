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

//! Server transaction command validation, participant selection and acknowledgement policy.
const std = @import("std");
const db_mod = @import("antfly_local_sources").storage_db_mod;
const transactions_mod = @import("antfly_local_sources").storage_transactions;
const local_transaction_contract = @import("antfly_local_sources").api_local_transaction_contract;
const local_write = @import("antfly_local_sources").storage_local_write;
const validateTableBatchAgainstLocalSchema = local_write.validateTableBatchAgainstLocalSchema;
const runTestBeforeBatchExecutionHook = @import("antfly_local_sources").api_local_write_test_hooks.runTestBeforeBatchExecutionHook;
const batchWritesAsTransactionWrites = local_write.batchWritesAsTransactionWrites;

fn validatePreparationBinding(alloc: std.mem.Allocator, table_name: []const u8, group_id: u64, epoch: u64, scope: ?[32]u8, plan: ?[16]u8, binding: ?@import("antfly_local_sources").storage_prepared_transaction_effects.Binding, execution: ?@import("antfly_runtime_abi").runnable_abi.Borrowed) !void {
    const value = binding orelse return;
    if (execution) |owner| try owner.checkLifetime();
    if (value.group_id != group_id or value.topology_epoch != epoch) return error.PreparationContractConflict;
    const locator = try local_transaction_contract.participantIdForGroupScoped(alloc, table_name, group_id, scope, plan);
    defer alloc.free(locator);
    var digest: [32]u8 = undefined;
    std.crypto.hash.Blake3.hash(locator, &digest, .{});
    if (!std.mem.eql(u8, &digest, &value.participant_digest)) return error.PreparationContractConflict;
    // The route owner validates authority before BEGIN. Apply preserves the
    // exact accepted route digest; it must never perform metadata RPC here.
}

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
    raft_entry: db_mod.OrderedApplyReceipt,
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
    raft_entry: ?db_mod.OrderedApplyReceipt,
) !void {
    return applyReplicatedTransactionMutationInternalWithExecution(alloc, db, table_name, group_id, req, visibility_cancellation, raft_entry, null);
}

pub fn applyReplicatedTransactionMutationInternalWithExecution(
    alloc: std.mem.Allocator,
    db: *db_mod.DB,
    table_name: []const u8,
    group_id: u64,
    req: db_mod.types.BatchRequest,
    visibility_cancellation: db_mod.types.CancellationToken,
    raft_entry: ?db_mod.OrderedApplyReceipt,
    execution: ?@import("antfly_runtime_abi").runnable_abi.Borrowed,
) !void {
    const mutation = req.transaction orelse return error.InvalidBatchRequest;
    try @import("antfly_local_sources").storage_range_protection.validateRequest(req);
    if (req.relational_index_maintenance) |command| if (command.owner_group_id != group_id) return error.PreparedGenerationChanged;
    var admission_borrow: @import("transaction_execution_admission.zig").Borrow = .{};
    switch (mutation) {
        .begin => |begin| {
            try validatePreparationBinding(alloc, table_name, group_id, begin.topology_epoch, req.restore_staging_scope, req.restore_staging_plan_id, begin.preparation_binding, if (raft_entry == null) execution else null);
            if (begin.preparation_requirement != null and raft_entry == null) return error.PreparedTransactionProtocolUnavailable;
            const local_participant = try local_transaction_contract.participantIdForGroupScoped(alloc, table_name, group_id, req.restore_staging_scope, req.restore_staging_plan_id);
            defer alloc.free(local_participant);
            const coordinator = try @import("transaction_participant_admission.zig").validate(alloc, begin.participants, local_participant, if (raft_entry == null) execution else null);
            const local_only = [_][]const u8{local_participant};
            // Only the coordinator owns the full participant fan-out. A
            // follower tracks itself, making successful cleanup O(N) rather
            // than every participant retrying every other participant.
            const durable_participants: []const []const u8 = if (coordinator) begin.participants else &local_only;
            if (raft_entry) |entry|
                _ = try db.beginReplicatedTransactionScopedWithBinding(
                    begin.txn_id,
                    begin.begin_timestamp,
                    begin.created_at_ns,
                    durable_participants,
                    coordinator,
                    begin.retain_terminal,
                    entry,
                    req.restore_staging_scope,
                    begin.preparation_requirement,
                    begin.preparation_binding,
                )
            else if (execution) |original| {
                var borrow: @import("transaction_execution_admission.zig").Borrow = .{};
                const controls = try borrow.bind(original);
                _ = try db.beginTransactionScopedWithAdmissionControls(
                    begin.txn_id,
                    begin.begin_timestamp,
                    begin.created_at_ns,
                    durable_participants,
                    coordinator,
                    begin.retain_terminal,
                    req.restore_staging_scope,
                    controls,
                );
            } else _ = try db.beginTransactionScoped(
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
            try validatePreparationBinding(alloc, table_name, group_id, prepare.topology_epoch, req.restore_staging_scope, req.restore_staging_plan_id, prepare.preparation_binding, if (raft_entry == null) execution else null);
            if (prepare.sealed_effects) |frame| {
                try @import("antfly_local_sources").api_batch.validatePreparedSealRequest(req);
                const required = prepare.preparation_requirement orelse return error.InvalidBatchRequest;
                const entry = raft_entry orelse return error.PreparedTransactionProtocolUnavailable;
                try db.sealReplicatedTransactionEffectsWithBindingAtOrderedReceipt(prepare.txn_id, frame, required.mode, entry, req.restore_staging_scope, prepare.preparation_binding);
                return;
            }
            if (prepare.preparation_requirement != null and raft_entry == null) return error.PreparedTransactionProtocolUnavailable;
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
                try db.writeReplicatedTransactionWithBindingAtOrderedReceipt(prepare.txn_id, intents, entry, prepare.preparation_requirement, prepare.preparation_binding)
            else if (execution) |original| {
                var borrow: @import("transaction_execution_admission.zig").Borrow = .{};
                try db.writeTransactionWithAdmissionControls(prepare.txn_id, intents, try borrow.bind(original));
            } else try db.writeTransaction(prepare.txn_id, intents);
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
                try db.resolveReplicatedTransactionAtOrderedReceipt(
                    resolve.txn_id,
                    resolve.status,
                    resolve.commit_version,
                    req.sync_level,
                    visibility_cancellation,
                    entry,
                    if (defer_coordinator_ack) null else local_participant,
                );
            } else {
                try db.resolveTransactionIntentsAndAcknowledgeParticipant(resolve.txn_id, resolve.status, resolve.commit_version, req.sync_level, visibility_cancellation, local_participant, execution);
            }
        },
        .acknowledge => |ack| (if (raft_entry) |entry|
            db.markReplicatedTransactionParticipantResolvedAtOrderedReceipt(ack.txn_id, ack.participant, entry)
        else if (execution) |original|
            db.markTransactionParticipantResolvedWithAdmissionControls(ack.txn_id, ack.participant, try admission_borrow.bind(original))
        else
            db.markTransactionParticipantResolved(ack.txn_id, ack.participant)) catch |err| switch (err) {
            // Cleanup and acknowledgements are independently retryable Raft
            // commands. Once cleanup wins, a late acknowledgement is a safe
            // no-op and must not recreate coordinator sidecar metadata.
            transactions_mod.TxnError.TxnNotFound => {},
            else => return err,
        },
        .acknowledge_many => |ack| (if (raft_entry) |entry|
            db.markReplicatedTransactionParticipantsResolvedAtOrderedReceipt(ack.txn_id, ack.participants, entry)
        else if (execution) |original|
            db.markTransactionParticipantsResolvedWithAdmissionControls(ack.txn_id, ack.participants, try admission_borrow.bind(original))
        else
            db.markTransactionParticipantsResolved(ack.txn_id, ack.participants)) catch |err| switch (err) {
            transactions_mod.TxnError.TxnNotFound => {},
            else => return err,
        },
        .cleanup => |cleanup| {
            if (raft_entry) |entry|
                _ = try db.cleanupReplicatedTransactionAtOrderedReceipt(
                    cleanup.txn_id,
                    cleanup.cutoff_timestamp,
                    cleanup.retained_cutoff_timestamp,
                    entry,
                )
            else if (execution) |original|
                _ = try db.cleanupTransactionMetadataIfEligibleWithAdmissionControls(
                    cleanup.txn_id,
                    cleanup.cutoff_timestamp,
                    cleanup.retained_cutoff_timestamp,
                    try admission_borrow.bind(original),
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

test "controlled transaction dispatch commands reject original lifetime before DB access" {
    const runtime = @import("antfly_runtime_abi");
    const State = struct {
        checks: usize = 0,
        fail_at: usize,
        cause: anyerror,
        fn lifetime(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            return if (self.checks >= self.fail_at) runtime.error_abi.statusFromError(self.cause) else .ok;
        }
        fn cpu(_: *anyopaque) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            return runtime.error_abi.statusFromError(error.TestUnexpectedResult);
        }
    };
    var state: State = undefined;
    const original: runtime.runnable_abi.Borrowed = .{ .context = &state, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .check_lifetime = State.lifetime, .checkpoint = State.cpu, .park = State.phase, .enter = State.phase };
    const scope: [32]u8 = @splat(3);
    const plan: [16]u8 = @splat(4);
    const participant = try local_transaction_contract.participantIdForGroupScoped(std.testing.allocator, "docs", 7001, scope, plan);
    defer std.testing.allocator.free(participant);
    const participants = [_][]const u8{participant};
    const requests = [_]db_mod.types.BatchRequest{
        .{ .restore_staging_scope = scope, .restore_staging_plan_id = plan, .transaction = .{ .begin = .{ .txn_id = @splat(1), .begin_timestamp = 10, .created_at_ns = 20, .topology_epoch = 30, .retain_terminal = true, .participants = &participants } } },
        .{ .restore_staging_scope = scope, .restore_staging_plan_id = plan, .transaction = .{ .prepare = .{ .txn_id = @splat(1), .topology_epoch = 30 } } },
        .{ .transaction = .{ .acknowledge = .{ .txn_id = @splat(1), .participant = participant } } },
        .{ .transaction = .{ .acknowledge_many = .{ .txn_id = @splat(1), .participants = &participants } } },
        .{ .transaction = .{ .cleanup = .{ .txn_id = @splat(1), .cutoff_timestamp = 40, .retained_cutoff_timestamp = 50 } } },
    };
    var db: db_mod.DB = undefined;
    for (requests) |request| for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| for (1..3) |fail_at| {
        state = .{ .checks = 0, .fail_at = fail_at, .cause = cause };
        try std.testing.expectError(cause, applyReplicatedTransactionMutationInternalWithExecution(std.testing.allocator, &db, "docs", 7001, request, .none, null, original));
        try std.testing.expectEqual(fail_at, state.checks);
    };
}

test "controlled transaction dispatch atomic resolve rejects original lifetime before DB access" {
    const runtime = @import("antfly_runtime_abi");
    const State = struct {
        cause: anyerror,
        fn check(raw: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return runtime.error_abi.statusFromError(self.cause);
        }
        fn phase(_: *anyopaque, _: c_int) callconv(.c) runtime.error_abi.Status {
            @panic("rejected resolution cannot acquire CPU");
        }
    };
    var db: db_mod.DB = undefined;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        var state: State = .{ .cause = cause };
        const original: runtime.runnable_abi.Borrowed = .{ .context = &state, .cpu_slots = 1, .parent_class = @backingInt(runtime.runnable_abi.Class.write), .checkpoint = State.check, .check_lifetime = State.check, .park = State.phase, .enter = State.phase };
        try std.testing.expectError(cause, applyReplicatedTransactionMutationInternalWithExecution(std.testing.allocator, &db, "docs", 7001, .{ .transaction = .{ .resolve = .{ .txn_id = @splat(1), .status = .committed, .commit_version = 200 } } }, .none, null, original));
    }
}
