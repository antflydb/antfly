// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Normal transaction admission over the same qualified ports as recovery.
const std = @import("std");
const local = @import("antfly_local_sources");
const types = local.storage_db_selected_root.db.types;
const plan_mod = @import("completion_plan.zig");
const completion = plan_mod.completion;
const catalog_api = @import("table_catalog.zig");
const metadata = @import("../metadata/api.zig");
const manager = @import("../metadata/table_manager.zig");
const Context = local.api_operation.RequestContext;
const A = std.mem.Allocator;
pub const Input = struct {
    table_name: []const u8,
    group_id: u64,
    topology_epoch: u64,
    intent: types.TransactionIntentRequest,
};
pub const Result = struct {
    coordinator_group_id: u64,
    coordinator_table_name: []const u8,
    propagation_pending: bool,
};
pub const Coordinator = struct {
    owners: @import("history_owner_operations.zig").Port,
    recovery: @import("completion_recovery.zig").Recovery,

    /// Null means every root explicitly lacks retained-history capability.
    /// A mixed transaction is rejected before BEGIN; enabled roots must never
    /// silently re-enter the legacy protocol and lose their completion proof.
    pub fn commit(self: Coordinator, a: A, transaction_id: [16]u8, begin_timestamp: u64, commit_version: u64, inputs: []const Input, sync_level: types.SyncLevel, original: Context) !?Result {
        if (inputs.len == 0) return null;
        if (commit_version <= begin_timestamp) return error.InvalidTxnRecord;
        var context = try original.platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
        try context.ensureActive();
        var budget = self.recovery.catalog.budget(null);
        budget.deadline_ns = budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        budget.cancellation = context.cancellation;
        var session = try catalog_api.RoutingSession.initWithBudget(a, self.recovery.catalog, budget);
        defer session.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const scratch = arena.allocator();
        const Captured = struct { input: plan_mod.Input, fence: metadata.CatalogRouteFence, table_name: []const u8, retention_limit_bytes: u64 };
        var captured: std.ArrayList(Captured) = .empty;
        var unsupported: usize = 0;
        for (inputs) |input| {
            try context.ensureActive();
            const view = try session.tableRoutingView(input.table_name, budget);
            if (view.topology_epoch != input.topology_epoch) return error.TopologyChanged;
            const table = session.snapshot.value.tables[session.table_id_indexes.get(view.table_id) orelse return error.TableNotFound];
            const range = try session.rangeForGroup(view.table_id, input.group_id, budget);
            const shard_id = manager.rangeDocIdentityShardId(range.*);
            const range_id = manager.rangeDocIdentityRangeId(range.*);
            const fence: metadata.CatalogRouteFence = .{
                .metadata_group_id = session.snapshot.value.metadata_group_id,
                .metadata_incarnation = session.snapshot.value.metadata_incarnation,
                .catalog_revision = session.snapshot.value.catalog_revision,
                .table_id = view.table_id,
                .topology_epoch = view.topology_epoch,
                .route = .{ .group_id = input.group_id, .range_id = range_id, .identity_namespace = .{ .table_id = view.table_id, .shard_id = shard_id, .range_id = range_id } },
                .admission_deadline_ns = context.deadline_ns,
                .admission_deadline_io = context.deadline_io,
                .admission_cancellation = context.cancellation,
            };
            const qualified = self.owners.read(scratch, fence, input.table_name, .{ .version = 1, .expected_table_binding = @import("../metadata/changefeed_control.zig").tableBinding(table) }, context) catch |err| switch (err) {
                error.ChangefeedCapabilityUnsupported => {
                    unsupported += 1;
                    continue;
                },
                else => return err,
            };
            var intent = input.intent;
            intent.completion_owner = qualified.owner;
            const request = try intent.completionBatch(transaction_id, begin_timestamp, input.topology_epoch);
            const locator = try local.api_local_transaction_contract.participantIdForGroup(scratch, input.table_name, input.group_id);
            var roles: u8 = 0;
            if (request.writes.len != 0 or request.deletes.len != 0 or request.transforms.len != 0 or request.graph_writes.len != 0 or request.graph_deletes.len != 0 or request.integrity.len != 0 or request.integrity_commands.len != 0 or request.relational_activation != null or request.relational_retirement != null or request.relational_index_maintenance != null) roles |= completion.Roles.effects;
            if (request.predicates.len != 0 or request.range_guards.len != 0) roles |= completion.Roles.guards;
            if (roles == 0) roles = completion.Roles.guards;
            try captured.append(scratch, .{ .input = .{ .owner = qualified.owner, .locator = locator, .roles = roles, .prepare = request }, .fence = fence, .table_name = input.table_name, .retention_limit_bytes = qualified.observation.retention_limit_bytes });
        }
        if (unsupported == inputs.len) return null;
        if (unsupported != 0) return error.ChangefeedCapabilityUnsupported;
        // Visibility levels require their existing derived-index barriers;
        // never report them satisfied by durable history publication alone.
        if (sync_level != .write) return error.UnsupportedOperation;
        std.mem.sort(Captured, captured.items, {}, struct {
            fn less(_: void, left: Captured, right: Captured) bool {
                return std.mem.order(u8, &left.input.owner.namespace, &right.input.owner.namespace) == .lt;
            }
        }.less);
        const frozen_inputs = try scratch.alloc(plan_mod.Input, captured.items.len);
        for (captured.items, frozen_inputs) |item, *input| input.* = item.input;
        const coordinator = captured.items[0];
        var plan = try plan_mod.Plan.freezeDerived(a, transaction_id, coordinator.input.owner, frozen_inputs, @import("antfly_platform").time.realtimeNs());
        defer plan.deinit();
        const reservation = try plan.admission.reservationBytes(plan.admission_bytes.len);
        for (captured.items) |item| if (reservation > item.retention_limit_bytes) return error.HistoryTooLarge;
        const query: @import("history_decision_operations.zig").Request = .{ .version = 1, .owner = coordinator.input.owner, .transaction_id = transaction_id, .minimum_applied_index = 1 };
        var observed = try self.recovery.decisions.read(a, coordinator.fence, coordinator.table_name, query, context);
        defer observed.deinit();
        if (observed.value.decision) |existing| {
            if (!std.mem.eql(u8, &existing.admission.digest, &plan.admission.digest())) return error.SourceTransactionIdentityConflict;
            switch (existing.record.status) {
                .pending => return error.CommitDecisionUnknown,
                .aborted => return error.DecisionConflict,
                .committed => if (existing.record.commit_version != commit_version) return error.SourceTransactionIdentityConflict,
            }
        } else {
            // Every owner, complete payload, and quota reservation size has
            // been checked before the first mutation. Unknown results leave
            // retained recovery ownership; no phase is retried in this pass.
            for (captured.items, 0..) |item, index| self.recovery.participants.submit(a, &plan, index, item.fence, .begin, context) catch return error.CommitDecisionUnknown;
            for (captured.items, 0..) |item, index| self.recovery.participants.submit(a, &plan, index, item.fence, .prepare, context) catch return error.CommitDecisionUnknown;
            self.recovery.participants.submit(a, &plan, 0, coordinator.fence, .{ .resolve = .{ .status = .committed, .commit_version = commit_version } }, context) catch return error.CommitDecisionUnknown;
        }
        const outcome = self.recovery.run(a, coordinator.input.owner, coordinator.fence, coordinator.table_name, transaction_id, context) catch return .{ .coordinator_group_id = coordinator.input.owner.group_id, .coordinator_table_name = coordinator.table_name, .propagation_pending = true };
        return .{ .coordinator_group_id = coordinator.input.owner.group_id, .coordinator_table_name = coordinator.table_name, .propagation_pending = outcome != .complete };
    }
};
