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
    visibility_pending: bool = false,
    visibility_retry_pending: bool = false,
    visibility_repair_required: bool = false,

    fn visibilityFailure(self: *Result, err: anyerror) void {
        self.visibility_pending = true;
        if (err == error.EnrichmentWorkerFailed) self.visibility_repair_required = true else self.visibility_retry_pending = true;
    }
};
const Route = struct { owner: completion.RoutedOwner, fence: metadata.CatalogRouteFence, table_name: []const u8, retention_limit_bytes: u64 };
const Captured = struct { input: plan_mod.Input, fence: metadata.CatalogRouteFence, table_name: []const u8, retention_limit_bytes: u64 };
/// An owned, read-only admission preparation. Source delivery retains the encoded
/// plan before invoking commitPrepared; preparation itself issues no writes.
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    plan: plan_mod.Plan,
    routes: []const Route,
    coordinator_index: usize,
    pub fn deinit(self: *Prepared) void {
        self.plan.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};
pub const Coordinator = struct {
    owners: @import("history_owner_operations.zig").Port,
    recovery: @import("completion_recovery.zig").Recovery,

    /// Null means every root explicitly lacks retained-history capability.
    /// Mixed capability is rejected before BEGIN. Callers can persist the frozen
    /// artifact and metadata reservation between preparation and admission.
    pub fn prepare(self: Coordinator, a: A, transaction_id: [16]u8, begin_timestamp: u64, inputs: []const Input, original: Context) !?Prepared {
        if (inputs.len == 0) return null;
        var context = try original.platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
        try context.ensureActive();
        var budget = self.recovery.catalog.budget(null);
        budget.deadline_ns = budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        budget.cancellation = context.cancellation;
        var session = try catalog_api.RoutingSession.initWithBudget(a, self.recovery.catalog, budget);
        defer session.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
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
            if (intent.completion_owner) |expected| if (!std.meta.eql(expected, qualified.owner)) return error.SourceBindingConflict;
            intent.completion_owner = qualified.owner;
            const request = try intent.completionBatch(transaction_id, begin_timestamp, input.topology_epoch);
            const locator = try local.api_local_transaction_contract.participantIdForGroup(scratch, input.table_name, input.group_id);
            var roles: u8 = 0;
            if (request.writes.len != 0 or request.deletes.len != 0 or request.transforms.len != 0 or request.graph_writes.len != 0 or request.graph_deletes.len != 0 or request.integrity.len != 0 or request.integrity_commands.len != 0 or request.relational_activation != null or request.relational_retirement != null or request.relational_index_maintenance != null) roles |= completion.Roles.effects;
            if (request.predicates.len != 0 or request.range_guards.len != 0) roles |= completion.Roles.guards;
            if (intent.completion_source_checkpoint != null) roles |= completion.Roles.checkpoint;
            if (roles == 0) roles = completion.Roles.guards;
            try captured.append(scratch, .{ .input = .{ .owner = qualified.owner, .locator = locator, .roles = roles, .prepare = request }, .fence = fence, .table_name = try scratch.dupe(u8, input.table_name), .retention_limit_bytes = qualified.observation.retention_limit_bytes });
        }
        if (unsupported == inputs.len) {
            for (inputs) |input| if (input.intent.completion_source_purpose != null or input.intent.completion_source_scope != null or input.intent.completion_source_checkpoint != null or input.intent.source_admission != null) return error.ChangefeedCapabilityUnsupported;
            arena.deinit();
            return null;
        }
        if (unsupported != 0) return error.ChangefeedCapabilityUnsupported;
        std.mem.sort(Captured, captured.items, {}, struct {
            fn less(_: void, left: Captured, right: Captured) bool {
                return std.mem.order(u8, &left.input.owner.namespace, &right.input.owner.namespace) == .lt;
            }
        }.less);
        const frozen_inputs = try scratch.alloc(plan_mod.Input, captured.items.len);
        for (captured.items, frozen_inputs) |item, *input| input.* = item.input;
        var coordinator_index: usize = 0;
        for (captured.items, 0..) |item, index| if (item.input.roles & completion.Roles.checkpoint != 0) {
            coordinator_index = index;
            break;
        };
        const coordinator = captured.items[coordinator_index];
        var plan = try plan_mod.Plan.freezeDerived(a, transaction_id, coordinator.input.owner, frozen_inputs, @import("antfly_platform").time.realtimeNs());
        errdefer plan.deinit();
        const reservation = try plan.admission.reservationBytes(plan.admission_bytes.len);
        for (captured.items) |item| if (reservation > item.retention_limit_bytes) return error.HistoryTooLarge;
        const routes = try scratch.alloc(Route, captured.items.len);
        for (captured.items, routes) |item, *route| route.* = .{ .owner = item.input.owner, .fence = item.fence, .table_name = item.table_name, .retention_limit_bytes = item.retention_limit_bytes };
        try context.ensureActive();
        return .{ .arena = arena, .plan = plan, .routes = routes, .coordinator_index = coordinator_index };
    }

    /// Reopen retained bytes without rebuilding effects or changing any owner.
    /// Current routing supplies transport authority only; every original owner
    /// must still match a fresh qualified observation before admission.
    pub fn restorePrepared(self: Coordinator, a: A, bytes: []const u8, digest: completion.Digest, original: Context) !Prepared {
        var context = try original.platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
        try context.ensureActive();
        var plan = try plan_mod.Plan.restoreFrozen(a, bytes, digest);
        errdefer plan.deinit();
        var arena = std.heap.ArenaAllocator.init(a);
        errdefer arena.deinit();
        const scratch = arena.allocator();
        var budget = self.recovery.catalog.budget(null);
        budget.deadline_ns = budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        budget.cancellation = context.cancellation;
        var session = try catalog_api.RoutingSession.initWithBudget(a, self.recovery.catalog, budget);
        defer session.deinit();
        const routes = try scratch.alloc(Route, plan.participants.len);
        var coordinator_index: ?usize = null;
        const reservation = try plan.admission.reservationBytes(plan.admission_bytes.len);
        for (plan.participants, routes, 0..) |participant, *route, index| {
            const fence = try @import("completion_recovery.zig").routeRetainedOwner(&session, participant.owner, participant.locator, budget, context);
            const locator = local.api_local_transaction_contract.parseParticipantRef(participant.locator) orelse return error.InvalidParticipant;
            const qualified = try self.owners.read(scratch, fence, locator.table_name, .{ .version = 1, .expected_table_binding = participant.owner.table_binding }, context);
            if (!std.meta.eql(qualified.owner, participant.owner)) return error.SourceBindingConflict;
            if (reservation > qualified.observation.retention_limit_bytes) return error.HistoryTooLarge;
            route.* = .{ .owner = participant.owner, .fence = fence, .table_name = try scratch.dupe(u8, locator.table_name), .retention_limit_bytes = qualified.observation.retention_limit_bytes };
            if (std.mem.eql(u8, &participant.owner.namespace, &plan.admission.coordinator_namespace)) coordinator_index = index;
        }
        try context.ensureActive();
        return .{ .arena = arena, .plan = plan, .routes = routes, .coordinator_index = coordinator_index orelse return error.InvalidParticipant };
    }

    pub fn commit(self: Coordinator, a: A, transaction_id: [16]u8, begin_timestamp: u64, commit_version: u64, inputs: []const Input, sync_level: types.SyncLevel, original: Context) !?Result {
        if (commit_version <= begin_timestamp) return error.InvalidTxnRecord;
        var prepared = (try self.prepare(a, transaction_id, begin_timestamp, inputs, original)) orelse return null;
        defer prepared.deinit();
        var result = try self.commitPrepared(a, &prepared, commit_version, sync_level, original);
        // Preserve commit's borrowed-input name contract after Prepared dies.
        for (inputs) |input| if (input.group_id == result.coordinator_group_id and std.mem.eql(u8, input.table_name, result.coordinator_table_name)) {
            result.coordinator_table_name = input.table_name;
            return result;
        };
        return error.InvalidParticipant;
    }

    /// Admit the original qualified plan, without consulting rows or rebuilding
    /// input. Expired captured route leases fail at the participant port.
    pub fn commitPrepared(self: Coordinator, a: A, prepared: *const Prepared, commit_version: u64, sync_level: types.SyncLevel, original: Context) !Result {
        return self.commitPreparedMode(a, prepared, commit_version, sync_level, original, false);
    }

    /// Source actors call this only after observing their sealed metadata grant
    /// and loading its original native artifact. A matching positive pending
    /// coordinator permits idempotent replay of that exact BEGIN/PREPARE input.
    /// Native scope fencing remains authoritative at every participant. Derived
    /// index visibility is independent of source commitment.
    pub fn commitSourcePrepared(self: Coordinator, a: A, prepared: *const Prepared, commit_version: u64, original: Context) !Result {
        if (prepared.plan.source_scope == null or prepared.plan.source_purpose == null or !prepared.plan.source_checkpoint_receipts) return error.UnsupportedOperation;
        return self.commitPreparedMode(a, prepared, commit_version, .write, original, true);
    }

    fn commitPreparedMode(self: Coordinator, a: A, prepared: *const Prepared, commit_version: u64, sync_level: types.SyncLevel, original: Context, source_recovery: bool) !Result {
        const plan = &prepared.plan;
        if (commit_version <= plan.begin_timestamp or prepared.routes.len != plan.participants.len or prepared.routes.len == 0 or prepared.coordinator_index >= prepared.routes.len) return error.InvalidTxnRecord;
        const captured = prepared.routes;
        const coordinator = captured[prepared.coordinator_index];
        const transaction_id = plan.admission.transaction_id;
        var context = try original.platformDeadline();
        context.deadline_ns = @min(context.deadline_ns orelse std.math.maxInt(u64), @import("antfly_platform").time.monotonicNs() +| 5 * std.time.ns_per_s);
        try context.ensureActive();
        const query: @import("history_decision_operations.zig").Request = .{ .version = 1, .owner = coordinator.owner, .transaction_id = transaction_id, .minimum_applied_index = 1 };
        var observed = try self.recovery.decisions.read(a, coordinator.fence, coordinator.table_name, query, context);
        defer observed.deinit();
        var dispatch = observed.value.decision == null;
        if (observed.value.decision) |existing| {
            if (!std.mem.eql(u8, &existing.admission.digest, &plan.admission.digest())) return error.SourceTransactionIdentityConflict;
            if (source_recovery and (existing.record.begin_timestamp != plan.begin_timestamp or existing.record.created_at_ns != plan.created_at_ns or !std.meta.eql(existing.record.source_scope, plan.source_scope) or !std.meta.eql(existing.record.source_purpose, plan.source_purpose))) return error.SourceTransactionIdentityConflict;
            switch (existing.record.status) {
                .pending => if (source_recovery) {
                    dispatch = true;
                } else return error.CommitDecisionUnknown,
                .aborted => return error.DecisionConflict,
                .committed => if (existing.record.commit_version != commit_version) return error.SourceTransactionIdentityConflict,
            }
        }
        if (dispatch) {
            // Every owner, complete payload, and quota reservation size has
            // been checked before the first mutation. Unknown results leave
            // retained recovery ownership; no phase is retried in this pass.
            for (captured, 0..) |item, index| self.recovery.participants.submit(a, plan, index, item.fence, .begin, context) catch return error.CommitDecisionUnknown;
            for (captured, 0..) |item, index| self.recovery.participants.submit(a, plan, index, item.fence, .prepare, context) catch return error.CommitDecisionUnknown;
            self.recovery.participants.submit(a, plan, prepared.coordinator_index, coordinator.fence, .{ .resolve = .{ .status = .committed, .commit_version = commit_version } }, context) catch return error.CommitDecisionUnknown;
        }
        const wants_visibility = switch (sync_level) {
            .full_text, .enrichments, .full_index => true,
            .propose, .write => false,
        };
        const outcome = self.recovery.run(a, coordinator.owner, coordinator.fence, coordinator.table_name, transaction_id, context) catch {
            return .{ .coordinator_group_id = coordinator.owner.group_id, .coordinator_table_name = coordinator.table_name, .propagation_pending = true, .visibility_pending = wants_visibility, .visibility_retry_pending = wants_visibility };
        };
        var result: Result = .{ .coordinator_group_id = coordinator.owner.group_id, .coordinator_table_name = coordinator.table_name, .propagation_pending = outcome != .complete };
        if (!wants_visibility) return result;
        if (outcome != .complete) {
            result.visibilityFailure(error.CommitPropagationIncomplete);
            return result;
        }
        // Completion and derived visibility are separate obligations. A fresh
        // retained COMMIT authorizes only an idempotent terminal resolution
        // carrying the requested barrier; BEGIN/PREPARE are never replayed.
        var committed = self.recovery.decisions.read(a, coordinator.fence, coordinator.table_name, query, context) catch |err| {
            result.visibilityFailure(err);
            return result;
        };
        defer committed.deinit();
        const decision = committed.value.decision orelse {
            result.visibilityFailure(error.SourceOutcomeUnresolved);
            return result;
        };
        if (decision.record.status != .committed or decision.record.commit_version != commit_version or !std.mem.eql(u8, &decision.admission.digest, &plan.admission.digest())) {
            result.visibilityFailure(error.SourceTransactionIdentityConflict);
            return result;
        }
        for (captured, 0..) |item, index| {
            self.recovery.participants.submitRetained(a, query.binding(), committed.value, index, item.fence, .{ .visibility = sync_level }, context) catch |err| result.visibilityFailure(err);
        }
        return result;
    }
};
