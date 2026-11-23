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

//! Source destination admission from a metadata-owned retained grant. Accepted
//! checkpoint evidence is not an all-owner ACK, retention release or index barrier.
const std = @import("std");
const local = @import("antfly_local_sources");
const source = @import("../metadata/source_control.zig");
const artifact = @import("../metadata/source_artifact_control.zig");
const destination = @import("source_destination_plan.zig");
const checkpoints = @import("source_checkpoint_operations.zig");
const metadata = @import("../metadata/api.zig");
const A = std.mem.Allocator;
const Context = local.api_operation.RequestContext;

pub const Grant = struct {
    manifest: std.json.Parsed(destination.Manifest),
    record: artifact.Record,
    pub fn deinit(self: *Grant) void {
        self.manifest.deinit();
        self.* = undefined;
    }
    /// The actor supplies one applied metadata snapshot's pending reservation
    /// and ledger record. Neither a manifest alone nor native absence grants it.
    pub fn init(a: A, scope: source.Scope, pending: source.Reservation, record: artifact.Record) !Grant {
        try scope.validate();
        try pending.validate();
        try (artifact.State{ .next_token = record.spec.token(), .current = record }).validate();
        if (record.phase == .garbage) return error.SourceReceiptExpired;
        if (scope.destination != .native or pending.destination != .native or pending.artifact == null or !std.meta.eql(pending.artifact.?, record.spec) or !std.mem.eql(u8, &pending.identity(scope), &record.reservation)) return error.SourceTransactionIdentityConflict;
        var manifest = try destination.parse(a, pending.delivery_manifest, pending.plan_digest);
        errdefer manifest.deinit();
        const retained = record.recovery orelse return error.SourcePendingWork;
        if (!pending.retain_manifest or !std.mem.eql(u8, &retained.digest, &pending.plan_digest) or !std.mem.eql(u8, retained.bytes, pending.delivery_manifest)) return error.SourceTransactionIdentityConflict;
        const value = manifest.value;
        if (pending.retain_payload != (value.retained_payload != null)) return error.SourceTransactionIdentityConflict;
        const plan = try value.deliveryPlan(a, pending.delivery_manifest);
        const purpose = try pending.inputFingerprint(scope);
        const checkpoint: local.storage_source_checkpoint.Command = .{ .binding = scope.identity(), .purpose = purpose, .sequence = pending.sequence, .checkpoint = pending.checkpoint, .expected_checkpoint = pending.expected_checkpoint, .accepted_at_ns = pending.accepted_at_ns, .receipt_window = value.checkpoint.receipt_window };
        const binding: local.storage_source_scope.Spec = .{ .actor_table_id = scope.table_id, .source_id = scope.source_id, .generation = scope.generation, .binding = scope.identity(), .checkpoint_owner = try local.storage_source_scope.Basis.fromOwner(value.checkpoint_authority.owner) };
        try binding.validate();
        if (!std.meta.eql(plan.artifact.?, record.spec) or !std.mem.eql(u8, &pending.transaction_id, &value.transaction_id) or
            !std.mem.eql(u8, &binding.domain(), &value.source_scope.domain) or binding.generation != value.source_scope.generation or
            !std.mem.eql(u8, &checkpoint.digest(), &value.checkpoint.digest())) return error.SourceTransactionIdentityConflict;
        var owned_record = record;
        owned_record.recovery.?.bytes = try manifest.arena.allocator().dupe(u8, retained.bytes);
        return .{ .manifest = manifest, .record = owned_record };
    }
};
/// Fixed owned native evidence. It proves acceptance at the original checkpoint
/// root; the caller retains separate completion and reclamation obligations.
pub const Accepted = struct { reservation_digest: [32]u8, checkpoint: checkpoints.wire.Reply };
pub const Admission = struct {
    coordinator: @import("completion_admission.zig").Coordinator,
    artifacts: destination.Ports,
    retained_payload_store: ?destination.payload_retention.Store = null,
    checkpoints: checkpoints.Port,
    scope_reads: ?@import("source_scope_operations.zig").Port = null,
    retirements: ?@import("source_retirement_operations.zig").Port = null,
    scope_controls: ?@import("source_scope_control_operations.zig").Port = null,

    producer_reads: ?@import("source_producer_closure_operations.zig").Port = null,
    producer_controls: ?@import("source_producer_control_operations.zig").Port = null,

    fn requestNonce(a: A) ![16]u8 {
        var io_impl = std.Io.Threaded.init(a, .{});
        defer io_impl.deinit();
        var nonce: [16]u8 = undefined;
        try @import("antfly_platform").entropy.fill(io_impl.io(), &nonce);
        if (std.mem.allEqual(u8, &nonce, 0)) nonce[0] = 1;
        return nonce;
    }

    fn requireProducerClosure(self: Admission, a: A, fence: metadata.CatalogRouteFence, table: []const u8, owner: local.storage_transaction_completion.RoutedOwner, context: Context) !void {
        const reads = self.producer_reads orelse return error.ChangefeedCapabilityUnsupported;
        const controls = self.producer_controls orelse return error.ChangefeedCapabilityUnsupported;
        const query: @import("source_producer_closure_operations.zig").wire.Query = .{
            .version = 1,
            .owner = owner,
            .minimum_applied_index = 1,
        };
        const observed = try reads.read(a, fence, table, query, context);
        try observed.validate(query);
        if (observed.closure != null) return;
        const command: @import("source_producer_control_operations.zig").wire.Request = .{
            .version = 1,
            .owner = owner,
            .request_id = try requestNonce(a),
        };
        const result = try controls.execute(a, fence, table, command, context);
        try result.validate(command, try command.digest(a));
        if (!result.closed()) return error.SourceOutcomeUnresolved;
    }

    pub fn requireScope(self: Admission, a: A, fence: metadata.CatalogRouteFence, table: []const u8, intent: local.storage_source_scope_control.Intent, context: Context) !void {
        const reads = self.scope_reads orelse return error.ChangefeedCapabilityUnsupported;
        const controls = self.scope_controls orelse return error.ChangefeedCapabilityUnsupported;
        try intent.validate();
        const query: local.storage_source_scope_control.Query = .{
            .version = 1,
            .owner = intent.owner,
            .domain = intent.spec.domain(),
            .minimum_applied_index = 1,
        };
        const observed = try reads.read(a, fence, table, query, context);
        try observed.validate(query);
        if (try scopeReached(observed, intent)) return;
        if (intent.action == .bind or intent.action == .close_checkpoint) try self.requireProducerClosure(a, fence, table, intent.owner, context);
        const input: @import("source_scope_control_operations.zig").wire.Request = .{
            .version = 1,
            .request_id = try requestNonce(a),
            .operation = .submit,
            .intent = intent,
        };
        const result = try controls.execute(a, fence, table, input, context);
        try result.validate(input, try input.digest(a));
        if (!result.confirmed()) return error.SourceOutcomeUnresolved;
        const expected_version: u8 = switch (intent.action) {
            .bind => 2,
            .close_checkpoint => 4,
            else => 1,
        };
        if (result.command.?.version != expected_version and !(intent.action == .bind and result.command.?.version == 4)) return error.SourceBindingConflict;
        switch (result.outcome.?) {
            .applied => |applied| if (!try scopeReached(applied, intent)) return error.SourceOutcomeUnresolved,
            .rejected => |rejection| switch (rejection) {
                .pending => return error.SourcePendingWork,
                else => return error.SourceBindingConflict,
            },
            .superseded => unreachable,
        }
    }

    pub fn installCheckpoint(self: Admission, a: A, fence: metadata.CatalogRouteFence, table: []const u8, intent: local.storage_source_scope_control.Intent, context: Context) !local.storage_source_scope.CheckpointInstallReceipt {
        if (intent.action != .install_checkpoint) return error.InvalidSourceAdmission;
        try intent.validate();
        const reads = self.scope_reads orelse return error.ChangefeedCapabilityUnsupported;
        const controls = self.scope_controls orelse return error.ChangefeedCapabilityUnsupported;
        const query: local.storage_source_scope_control.Query = .{
            .version = 1,
            .owner = intent.owner,
            .domain = intent.spec.domain(),
            .minimum_applied_index = 1,
            .checkpoint_evidence = .successor,
        };
        const prior = reads.read(a, fence, table, query, context) catch |err| switch (err) {
            error.SourceOutcomeUnresolved, error.ReadUnavailable => null,
            else => return err,
        };
        if (prior) |observed| {
            try observed.validate(query);
            return observed.checkpoint_install.?;
        }
        try self.requireProducerClosure(a, fence, table, intent.owner, context);
        const wire = @import("source_scope_control_operations.zig").wire;
        const input: wire.Request = .{ .version = 1, .request_id = try requestNonce(a), .operation = .submit, .intent = intent };
        const result = try controls.execute(a, fence, table, input, context);
        try result.validate(input, try input.digest(a));
        if (!result.confirmed()) return error.SourceOutcomeUnresolved;
        return switch (result.outcome.?) {
            .applied => |observation| observation.checkpoint_install orelse error.SourceOutcomeUnresolved,
            .rejected => |rejection| rejection.asError(),
            .superseded => error.SourceOutcomeUnresolved,
        };
    }

    pub fn dropClosedScope(self: Admission, a: A, fence: metadata.CatalogRouteFence, table: []const u8, closing: local.storage_source_scope_control.Intent, context: Context) !void {
        if (closing.action != .close) return error.SourceBindingConflict;
        try closing.validate();
        const reads = self.scope_reads orelse return error.ChangefeedCapabilityUnsupported;
        const query: local.storage_source_scope_control.Query = .{
            .version = 1,
            .owner = closing.owner,
            .domain = closing.spec.domain(),
            .minimum_applied_index = 1,
        };
        const observed = try reads.read(a, fence, table, query, context);
        try observed.validate(query);
        const record = observed.record orelse return self.requireScope(a, fence, table, closing, context);
        if (!std.meta.eql(record.spec, closing.spec)) return error.SourceBindingConflict;
        var drop = closing;
        drop.action = .{ .drop = record.closed_at orelse return error.SourcePendingWork };
        try self.requireScope(a, fence, table, drop, context);
    }

    fn scopeReached(observed: local.storage_source_scope_control.Observation, intent: local.storage_source_scope_control.Intent) !bool {
        if (observed.record) |record| {
            if (!std.meta.eql(record.spec, intent.spec)) {
                var refreshed = record.spec;
                refreshed.checkpoint_owner = intent.spec.checkpoint_owner;
                if (intent.action == .bind and std.meta.eql(refreshed, intent.spec)) return false;
                return error.SourceBindingConflict;
            }
            return switch (intent.action) {
                .bind => if (record.phase == .active) true else error.SourceBindingConflict,
                .fence_attempt, .install_checkpoint => return error.InvalidSourceAdmission,
                .close, .close_checkpoint => switch (record.phase) {
                    .active => false,
                    .closing => error.SourcePendingWork,
                    .closed => true,
                },
                .drop => |closed_at| if (record.phase != .closed)
                    error.SourcePendingWork
                else if (!std.meta.eql(record.closed_at.?, closed_at))
                    error.SourceBindingConflict
                else
                    false,
            };
        }
        return intent.action != .bind and intent.action != .fence_attempt and intent.action != .install_checkpoint and intent.action != .close_checkpoint and observed.header.strict and
            observed.header.metadata_floor >= intent.minimum_metadata_index;
    }

    /// A native ordered result is required. An absent checkpoint read alone
    /// does not rule out an earlier or concurrently ordered BEGIN.
    pub fn fenceUnadmittedAttempt(self: Admission, a: A, state: source.State, manifest: destination.Manifest, fence: metadata.CatalogRouteFence, context: Context) ![]u8 {
        const controls = self.scope_controls orelse return error.ChangefeedCapabilityUnsupported;
        const pending = state.pending orelse return error.SourcePendingWork;
        const binding = state.scope orelse return error.SourceBindingConflict;
        const spec = (try @import("../metadata/source_scope_binding.zig").spec(state)) orelse return error.SourceBindingConflict;
        const id = pending.identity(binding);
        var routed = manifest.owner;
        routed.topology_epoch = fence.topology_epoch;
        const wire = @import("source_scope_control_operations.zig").wire;
        const request: wire.Request = .{
            .version = 1,
            .request_id = try requestNonce(a),
            .operation = .submit,
            .intent = .{
                .version = 1,
                .owner = routed,
                .spec = spec,
                .minimum_metadata_index = 1,
                .action = .{ .fence_attempt = .{ .reference = manifest.source_scope, .transaction_id = manifest.transaction_id, .reservation = id } },
            },
        };
        const reply = try controls.execute(a, fence, manifest.checkpoint_authority.table_name, request, context);
        try reply.validate(request, try request.digest(a));
        if (!reply.confirmed()) return error.SourceOutcomeUnresolved;
        const observed = switch (reply.outcome.?) {
            .applied => |observed| observed,
            .rejected => |reason| return reason.asError(),
            .superseded => return error.SourceOutcomeUnresolved,
        };
        try context.ensureActive();
        return std.json.Stringify.valueAlloc(a, source.NativeNoAdmissionProof{
            .command = reply.command.?,
            .cut = reply.receipt.?,
            .observation = observed,
        }, .{});
    }

    fn grantFromState(a: A, state: source.State) !Grant {
        try state.validate();
        if (!state.scope_fencing or !state.manifest_retention or state.retired) return error.UnsupportedOperation;
        const pending = state.pending orelse return error.SourcePendingWork;
        const record = state.artifact_control.?.current orelse return error.SourcePendingWork;
        return Grant.init(a, state.scope.?, pending, record);
    }
    /// Actor-facing result for metadata acceptance. Current applied state gates
    /// every call; the returned receipt does not advance committed progress.
    pub fn resolveDelivery(self: Admission, a: A, state: source.State, fence: metadata.CatalogRouteFence, original: Context) !?@import("../metadata/source_delivery.zig").Acceptance {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        var grant = try grantFromState(a, state);
        defer grant.deinit();
        const native = (try self.resolve(a, &grant, fence, context)) orelse return null;
        const result = try @import("source_destination_receipt.zig").accept(a, state, native);
        errdefer a.free(result.receipt);
        try context.ensureActive();
        return result;
    }
    pub fn admitDelivery(self: Admission, a: A, state: source.State, fence: metadata.CatalogRouteFence, original: Context) !@import("../metadata/source_delivery.zig").Acceptance {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        if (state.closing or state.checkpoint_transfer_target != null) return error.SourcePendingWork;
        var grant = try grantFromState(a, state);
        defer grant.deinit();
        const native = try self.admit(a, &grant, fence, context);
        const result = try @import("source_destination_receipt.zig").accept(a, state, native);
        errdefer a.free(result.receipt);
        try context.ensureActive();
        return result;
    }
    /// Retire an exact enlisted attempt after the catalog revoked one of its
    /// routes. No frozen writes are replayed under the refreshed leases.
    pub fn retireDisplacedDelivery(self: Admission, svc: anytype, a: A, manifest: destination.Manifest, fence: metadata.CatalogRouteFence, context: Context) !bool {
        const recovery = self.coordinator.recovery;
        const request: @import("history_decision_operations.zig").Request = .{
            .version = 1,
            .owner = manifest.owner,
            .transaction_id = manifest.transaction_id,
            .minimum_applied_index = 1,
        };
        var observed = try recovery.decisions.read(a, fence, manifest.checkpoint_authority.table_name, request, context);
        defer observed.deinit();
        try observed.value.validate(request.binding());
        const decision = observed.value.decision orelse return false;
        if (!std.meta.eql(decision.admission.digest, manifest.admission_digest) or
            !std.meta.eql(decision.record.source_scope, @as(?local.storage_source_scope.Reference, manifest.source_scope)) or
            !std.meta.eql(decision.record.source_purpose, @as(?source.Digest, manifest.checkpoint.purpose)) or
            decision.record.begin_timestamp != manifest.begin_timestamp)
            return error.SourceTransactionIdentityConflict;
        if (decision.record.status == .committed) return error.SourcePendingWork;

        const admission = decision.admission.admission;
        const coordinator = try admission.coordinatorMember();
        const catalog = @import("table_catalog.zig");
        var budget = recovery.catalog.budget(null);
        budget.deadline_ns = budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
        budget.cancellation = context.cancellation;
        var session = try catalog.RoutingSession.initWithBudget(a, recovery.catalog, budget);
        defer session.deinit();
        const routes = try a.alloc(metadata.CatalogRouteFence, admission.participants.len);
        defer a.free(routes);
        var displaced = false;
        for (admission.participants, routes) |member, *route| {
            const owner = member.routed_owner orelse return error.InvalidSourceAdmission;
            route.* = try @import("completion_recovery.zig").routeRetainedOwnerForCleanup(&session, owner, member.locator, budget, context);
            displaced = displaced or route.topology_epoch != owner.topology_epoch;
        }
        if (decision.record.status == .pending and !displaced) return false;
        const retirement_port = if (admission.participants.len > 1)
            self.retirements orelse return error.ChangefeedCapabilityUnsupported
        else
            null;
        _ = try svc.ensureTableTopologyProtocolReadyWithContext(context, source.epoch_attempt_retirement_protocol_version);
        if (decision.record.status == .pending) {
            for (admission.participants, routes) |member, route| {
                var owner = member.routed_owner.?;
                owner.topology_epoch = route.topology_epoch;
                const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidParticipant;
                const qualified = try self.coordinator.owners.read(a, route, locator.table_name, .{ .version = 1, .expected_table_binding = owner.table_binding }, context);
                if (!std.meta.eql(qualified.owner, owner)) return error.TopologyChanged;
            }
            const now = @import("antfly_platform").time.realtimeNs();
            const timestamp = @max(now, std.math.add(u64, manifest.begin_timestamp, 1) catch return error.InvalidTxnRecord);
            try recovery.participants.abortDisplacedSource(a, request.binding(), observed.value, fence, timestamp, context);
            return error.SourcePendingWork;
        }
        if (!decision.record.intents_resolved) return error.SourcePendingWork;
        if (!decision.record.acknowledgments_pending) return true;

        // Retirement proves both the enlisted and never-enlisted cases at
        // each follower before the coordinator may acknowledge its cleanup.
        for (admission.participants, routes) |member, route| {
            if (std.meta.eql(member.owner_fence, admission.coordinator_fence)) continue;
            try context.ensureActive();
            const locator = local.api_local_transaction_contract.parseParticipantRef(member.locator) orelse return error.InvalidParticipant;
            const wire = @import("source_retirement_operations.zig").wire;
            const retirement: wire.Request = .{
                .version = 1,
                .request_id = try requestNonce(a),
                .operation = .submit,
                .intent = .{
                    .version = 1,
                    .owner = member.routed_owner.?,
                    .coordinator = request.binding(),
                    .reference = manifest.source_scope,
                    .source_purpose = manifest.checkpoint.purpose,
                    .admission_digest = manifest.admission_digest,
                },
                .coordinator_locator = coordinator.locator,
            };
            var reply = try retirement_port.?.execute(a, route, locator.table_name, retirement, context);
            defer reply.deinit();
            try reply.value.validate(a, retirement);
            if (!reply.value.confirmed()) return error.SourceOutcomeUnresolved;
            switch (reply.value.outcome.?) {
                .applied => {},
                .rejected => |reason| return reason.asError(),
                .superseded => return error.SourceOutcomeUnresolved,
            }
        }
        try recovery.participants.acknowledgeResolved(a, request.binding(), observed.value, fence, context);
        return error.SourcePendingWork;
    }

    pub fn resumeEnlistedDelivery(self: Admission, a: A, state: source.State, fence: metadata.CatalogRouteFence, context: Context) !@import("../metadata/source_delivery.zig").Acceptance {
        var grant = try grantFromState(a, state);
        defer grant.deinit();
        const manifest = grant.manifest.value;
        const request: @import("history_decision_operations.zig").Request = .{
            .version = 1,
            .owner = manifest.owner,
            .transaction_id = manifest.transaction_id,
            .minimum_applied_index = 1,
        };
        var observed = try self.coordinator.recovery.decisions.read(a, fence, manifest.checkpoint_authority.table_name, request, context);
        defer observed.deinit();
        try observed.value.validate(request.binding());
        const decision = observed.value.decision orelse return error.SourceOutcomeUnresolved;
        if (!std.meta.eql(decision.admission.digest, manifest.admission_digest) or
            !std.meta.eql(decision.record.source_scope, @as(?local.storage_source_scope.Reference, manifest.source_scope)) or
            !std.meta.eql(decision.record.source_purpose, @as(?source.Digest, manifest.checkpoint.purpose)) or
            decision.record.begin_timestamp != manifest.begin_timestamp)
            return error.SourceTransactionIdentityConflict;
        if (decision.record.status == .aborted) return error.SourceOutcomeUnresolved;
        if (decision.record.status == .committed and decision.record.commit_version != manifest.commit_version)
            return error.SourceTransactionIdentityConflict;
        const accepted = try self.admit(a, &grant, fence, context);
        return @import("source_destination_receipt.zig").accept(a, state, accepted);
    }

    pub fn resolve(self: Admission, a: A, grant: *const Grant, fence: metadata.CatalogRouteFence, original: Context) !?Accepted {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        const manifest = grant.manifest.value;
        _ = try manifest.cleanupTransfer(fence, self.artifacts);
        const query: checkpoints.wire.Request = .{ .version = 1, .source_owner = manifest.owner, .transaction_id = manifest.transaction_id, .command = manifest.checkpoint, .minimum_applied_index = 1 };
        const observed = try self.checkpoints.read(a, fence, manifest.checkpoint_authority.table_name, query, context);
        try observed.validate(query);
        try context.ensureActive();
        return switch (observed.disposition) {
            .absent => null,
            .expired => error.SourceReceiptExpired,
            .committed => .{ .reservation_digest = grant.record.reservation, .checkpoint = observed },
        };
    }
    pub fn admit(self: Admission, a: A, grant: *const Grant, fence: metadata.CatalogRouteFence, original: Context) !Accepted {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        if (grant.record.phase != .sealed or grant.record.stamp == null) return error.SourcePendingWork;
        if (try self.resolve(a, grant, fence, context)) |accepted| return accepted;
        const manifest = grant.manifest.value;
        const retained = try manifest.load(a, grant.record, fence, self.artifacts, context);
        var prepared = try self.coordinator.qualifyRetained(a, retained, context);
        defer prepared.deinit();
        _ = self.coordinator.commitSourcePrepared(a, &prepared, manifest.commit_version, context) catch |err| switch (err) {
            // Read-only checkpoint resolution may prove that the original
            // COMMIT survived an unknown mutation response. Never resend here.
            error.CommitDecisionUnknown => null,
            else => return err,
        };
        return (try self.resolve(a, grant, fence, context)) orelse error.SourceOutcomeUnresolved;
    }
};

test "completion frozen plan source admission binds metadata grant before checkpoint proof" {
    const a = std.testing.allocator;
    const plans = @import("completion_plan.zig");
    const scope: source.Scope = .{ .table_id = 7, .source_id = @splat(1), .source_ordinal = 0, .generation = 2, .definition = @splat(2), .provider = @splat(3), .destination = .native, .destination_binding = @splat(4) };
    var namespace: [24]u8 = undefined;
    std.mem.writeInt(u64, namespace[0..8], 7, .big);
    std.mem.writeInt(u64, namespace[8..16], 9, .big);
    std.mem.writeInt(u64, namespace[16..24], 9, .big);
    const owner: plans.completion.RoutedOwner = .{ .metadata_group_id = 1, .metadata_incarnation = "11111111111111111111111111111111".*, .namespace = namespace, .table_binding = @splat(5), .root_generation = 0, .group_id = 9, .topology_epoch = 1 };
    var pending: source.Reservation = .{ .sequence = 1, .batch_id = "one", .checkpoint = "next", .accepted_at_ns = 11, .payload_digest = @splat(6), .plan_digest = @splat(7), .transaction_id = @splat(8), .destination = .native, .delivery_manifest = "{}" };
    const binding: local.storage_source_scope.Spec = .{ .actor_table_id = scope.table_id, .source_id = scope.source_id, .generation = scope.generation, .binding = scope.identity(), .checkpoint_owner = try local.storage_source_scope.Basis.fromOwner(owner) };
    const reference: local.storage_source_scope.Reference = .{ .domain = binding.domain(), .binding = binding.binding, .generation = scope.generation, .token = 3, .sequence = 1 };
    const command: local.storage_source_checkpoint.Command = .{ .binding = scope.identity(), .purpose = try pending.inputFingerprint(scope), .sequence = 1, .checkpoint = pending.checkpoint, .accepted_at_ns = pending.accepted_at_ns, .receipt_window = 3 };
    const request = try (local.storage_db_selected_root.db.types.TransactionIntentRequest{ .completion_owner = owner, .completion_source_scope = reference, .completion_source_purpose = command.purpose, .completion_source_checkpoint = command }).completionBatch(pending.transaction_id, 10, 1);
    var plan = try plans.Plan.freezeDerived(a, pending.transaction_id, owner, &.{.{ .owner = owner, .locator = "table2:00000004:docs:9", .roles = plans.completion.Roles.checkpoint, .prepare = request }}, 11);
    defer plan.deinit();
    var cut: [32]u8 = @splat(0);
    std.mem.writeInt(u64, cut[0..8], 1, .little);
    std.mem.writeInt(u64, cut[8..16], 1, .little);
    cut[31] = 1;
    var frozen = try destination.Frozen.init(a, &plan, .{ .binding = binding.binding, .table_name = "docs", .owner = owner }, .{ .root = try @import("../data/source_plan_entry.zig").rootFor(owner), .cut = cut, .quota = .{} }, 20);
    defer frozen.deinit();
    const compiled = try frozen.manifest.deliveryPlan(a, frozen.encoded_manifest);
    pending.delivery_manifest = compiled.manifest;
    pending.plan_digest = compiled.digest;
    pending.artifact = compiled.artifact;
    pending.retain_manifest = compiled.retain_manifest;
    const record: artifact.Record = .{ .reservation = pending.identity(scope), .spec = compiled.artifact.?, .recovery = .{ .digest = pending.plan_digest, .bytes = pending.delivery_manifest } };
    var grant = try Grant.init(a, scope, pending, record);
    defer grant.deinit();
    var transient_pending = pending;
    transient_pending.delivery_manifest = try a.dupe(u8, pending.delivery_manifest);
    var transient_record = record;
    transient_record.recovery.?.bytes = transient_pending.delivery_manifest;
    var independent = try Grant.init(a, scope, transient_pending, transient_record);
    defer independent.deinit();
    a.free(transient_pending.delivery_manifest);
    try std.testing.expectEqualStrings(pending.delivery_manifest, independent.record.recovery.?.bytes);
    const Fake = struct {
        committed: bool = false,
        forge: bool = false,
        calls: usize = 0,
        fn read(ptr: *anyopaque, alloc: A, _: metadata.CatalogRouteFence, _: []const u8, query: checkpoints.wire.Request, _: Context) !checkpoints.wire.Reply {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            var result: checkpoints.wire.Reply = .{ .request_digest = query.digest(), .cut = .{ .term = 1, .index = 1 }, .disposition = .absent };
            if (self.committed) {
                const head: local.storage_source_checkpoint.Head = .{ .binding = query.command.binding, .purpose = query.command.purpose, .transaction_id = query.transaction_id, .sequence = query.command.sequence, .checkpoint = query.command.checkpoint, .accepted_at_ns = query.command.accepted_at_ns, .receipt_window = query.command.receipt_window, .request_digest = query.command.digest() };
                const encoded = try head.encode(alloc);
                defer alloc.free(encoded);
                result.head = try checkpoints.wire.Evidence.init(encoded);
                result.receipt = result.head;
                result.disposition = .committed;
            }
            if (self.forge) result.request_digest[0] ^= 1;
            return result;
        }
    };
    var fake: Fake = .{};
    const admission: Admission = .{ .coordinator = undefined, .artifacts = undefined, .checkpoints = .{ .ptr = &fake, .read_fn = Fake.read } };
    const fence: metadata.CatalogRouteFence = .{ .metadata_group_id = 1, .metadata_incarnation = owner.metadata_incarnation, .catalog_revision = 1, .table_id = 7, .topology_epoch = 1, .route = .{ .group_id = 9, .range_id = 9, .identity_namespace = .{ .table_id = 7, .shard_id = 9, .range_id = 9 } } };
    try std.testing.expect(try admission.resolve(a, &grant, fence, .{}) == null);
    try std.testing.expectError(error.SourcePendingWork, admission.admit(a, &grant, fence, .{}));
    try std.testing.expectEqual(@as(usize, 1), fake.calls);
    grant.record.phase = .sealed;
    grant.record.stamp = @splat(9);
    fake.committed = true;
    const accepted = try admission.admit(a, &grant, fence, .{});
    try std.testing.expectEqualDeep(record.reservation, accepted.reservation_digest);
    try std.testing.expectEqual(.committed, accepted.checkpoint.disposition);
    fake.forge = true;
    try std.testing.expectError(error.SourceAdmissionCorrupt, admission.resolve(a, &grant, fence, .{}));
    var wrong = pending;
    wrong.payload_digest[0] ^= 1;
    var wrong_record = record;
    wrong_record.reservation = wrong.identity(scope);
    try std.testing.expectError(error.SourceTransactionIdentityConflict, Grant.init(a, scope, wrong, wrong_record));
    wrong_record = record;
    wrong_record.reservation[0] ^= 1;
    try std.testing.expectError(error.SourceTransactionIdentityConflict, Grant.init(a, scope, pending, wrong_record));
}
