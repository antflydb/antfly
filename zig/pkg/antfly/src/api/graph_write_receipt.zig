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

//! Graph statements retain the ordinary durable transaction session receipt.
//! Once the plan is sealed, failures become explicit pending/unknown outcomes.
const std = @import("std");
const local = @import("antfly_local_sources");
const http = @import("http_server.zig");
const sessions = @import("transactions.zig");
const contract = local.api_local_transaction_contract;
pub const Status = enum { committed, committed_pending, committed_repair_required, aborted, unknown };
pub const Result = struct { status: Status, outcome: ?contract.CommitOutcome = null, failure: ?anyerror = null };
pub fn execute(server: *http.ApiHttpServer, identity: ?http.AuthenticatedIdentity, context: local.api_operation.RequestContext, id: local.storage_db_types.TxnId, tables: []const contract.TableCommitRequest, alloc: std.mem.Allocator) !Result {
    const owned = server.alloc;
    const registry = &server.txn_sessions;
    const info = registry.beginForPrincipalWithSettingsAndId(owned, .{ .sync_level = .full_index }, server.localSessionNodeId(), http.transactionPrincipal(identity), &.{}, null, id) catch |err| {
        std.log.warn("graph receipt admission failed err={s}", .{@errorName(err)});
        return err;
    };
    var execution_sealed = false;
    errdefer if (!execution_sealed) {
        _ = registry.recordTerminalAbort(owned, id) catch null;
    };
    const lease = registry.tryAcquireCommitExecution(id) orelse return error.SqlWriteCapacityUnavailable;
    defer lease.release();
    var input_arena = std.heap.ArenaAllocator.init(owned);
    defer input_arena.deinit();
    const input_alloc = input_arena.allocator();
    var input: sessions.OwnedTransactionCommitRequest = .{ .sync_level = .full_index, .tables = try input_alloc.alloc(sessions.TableCommitRequest, tables.len) };
    for (tables, input.tables) |table, *entry| {
        const writes = try input_alloc.alloc(local.storage_db_types.BatchWrite, table.writes.len);
        for (table.writes, writes) |write, *output| output.* = .{ .key = write.key, .value = write.value, .json_null_fields = write.json_null_fields };
        entry.* = .{
            .table_name = @constCast(table.table_name),
            .schema_version = table.schema_version,
            .relational_schema_version = table.relational_schema_version,
            .batch = .{ .writes = writes, .deletes = @constCast(table.deletes), .transforms = @constCast(table.transforms), .graph_writes = @constCast(table.graph_writes), .graph_deletes = @constCast(table.graph_deletes) },
            .predicates = .empty,
            .range_guards = if (table.range_guards.len == 0) null else .{ .arena = std.heap.ArenaAllocator.init(input_alloc), .value = table.range_guards },
        };
        try entry.predicates.appendSlice(input_alloc, table.predicates);
    }
    var staged = (try registry.cloneCommitRequest(owned, id, &input)) orelse return error.InvalidTransactionSessionRecord;
    defer staged.deinit(owned);
    var prepared = server.preparePublicCommitWithIntegrity(alloc, tables, context) catch |err| {
        std.log.warn("graph receipt integrity preparation failed err={s}", .{@errorName(err)});
        return err;
    };
    defer prepared.deinit();
    var proof_arena = std.heap.ArenaAllocator.init(alloc);
    defer proof_arena.deinit();
    const signed = server.signCurrentRowPolicyMutationParticipants(proof_arena.allocator(), prepared.tables, context) catch |err| {
        std.log.warn("graph receipt principal preparation failed err={s}", .{@errorName(err)});
        return err;
    };
    execution_sealed = true;
    var plan = (registry.sealExecutionPlan(owned, id, signed) catch return .{ .status = .unknown }) orelse return .{ .status = .unknown };
    defer plan.deinit();
    const writer = server.table_writes orelse return .{ .status = .unknown };
    const normalized = context.platformDeadline() catch return .{ .status = .unknown };
    const outcome = (writer.commitTransactionWithIdAndCancellation(alloc, id, info.begin_timestamp, plan.value, .full_index, normalized.cancellation) catch |err| {
        if (err == error.TransactionPrepareAbortedRowPolicyDenied) {
            _ = registry.recordTerminalAbort(owned, id) catch return .{ .status = .unknown };
            return .{ .status = .aborted, .failure = error.RowPolicyDenied };
        }
        if (err == error.EnrichmentWorkerFailed) {
            _ = registry.recordTerminalCommitWithRepair(owned, id, .committed, true, null, null) catch return .{ .status = .committed_pending };
            return .{ .status = .committed_repair_required };
        }
        return .{ .status = switch (err) {
            error.CommitVisibilityNotSatisfied, error.EnrichmentWaitCanceled, error.EnrichmentWaitTimeout, error.EnrichmentRetryInProgress, error.CommitPropagationIncomplete => .committed_pending,
            else => .unknown,
        } };
    }) orelse return .{ .status = .unknown };
    switch (outcome) {
        .conflict => {
            _ = registry.recordTerminalAbort(owned, id) catch return .{ .status = .unknown, .outcome = outcome };
            return .{ .status = .aborted, .outcome = outcome };
        },
        .committed => |committed| {
            const status = sessions.terminalCommitStatusForOutcome(committed.propagation_pending, committed.visibility_pending, committed.visibility_retry_pending, committed.visibility_repair_required);
            _ = registry.recordTerminalCommitWithRepair(owned, id, status, committed.visibility_repair_required, committed.coordinator_group_id, committed.coordinator_table_name) catch return .{ .status = .committed_pending, .outcome = outcome };
            if (status == .committed) if (committed.coordinator_group_id) |group| {
                const table = committed.coordinator_table_name orelse return .{ .status = .committed_pending, .outcome = outcome };
                _ = writer.acknowledgeTransactionCommit(alloc, id, group, table) catch return .{ .status = .committed_pending, .outcome = outcome };
                _ = registry.markTerminalCoordinatorAcknowledged(owned, id) catch return .{ .status = .committed_pending, .outcome = outcome };
            };
            return .{ .status = if (committed.visibility_repair_required) .committed_repair_required else if (status == .committed) .committed else .committed_pending, .outcome = outcome };
        },
    }
}
