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

//! Completion recovery from the exact metadata acceptance receipt. A complete
//! result confirms participant resolution/publication, not source ACK or private
//! transaction/registry retirement. Those release obligations remain separate.
const std = @import("std");
const source = @import("../metadata/source_control.zig");
const receipts = @import("source_destination_receipt.zig");
const recovery = @import("completion_recovery.zig");
const metadata = @import("../metadata/api.zig");
const Context = @import("antfly_local_sources").api_operation.RequestContext;
pub const Outcome = enum { unproven, incomplete, complete, reclaimed };
pub const Completion = struct {
    recovery: recovery.Recovery,
    pub fn run(self: Completion, a: std.mem.Allocator, state: source.State, receipt: []const u8, fence: metadata.CatalogRouteFence, original: Context) !Outcome {
        var scope = try @import("history_publication_operations.zig").Scope.init(fence, original);
        const context = scope.context();
        try context.ensureActive();
        var bound = try receipts.decode(a, state, receipt);
        defer bound.deinit();
        if (bound == .reclaimed) return .reclaimed;
        if (state.pending != null) return error.SourcePendingWork;
        const manifest = bound.retained.manifest.value;
        try @import("source_owner_scope.zig").requireFence(manifest.owner, fence);
        const result = try self.recovery.runSource(a, manifest.owner, fence, manifest.checkpoint_authority.table_name, manifest.transaction_id, .{ .admission_digest = manifest.admission_digest, .scope = manifest.source_scope, .purpose = manifest.checkpoint.purpose, .begin_timestamp = manifest.begin_timestamp, .commit_version = manifest.commit_version }, context);
        try context.ensureActive();
        return switch (result) {
            .unproven => .unproven,
            .incomplete => .incomplete,
            .complete => .complete,
            .pending => error.SourceOutcomeUnresolved,
            .aborted => error.DecisionConflict,
        };
    }
};

/// Advance at most one durable recovery phase before returning to the CDC
/// scheduler. This pass only uses a manifest reserved by the source actor.
pub fn recover(
    svc: anytype,
    admission: @import("source_destination_admission.zig").Admission,
    a: std.mem.Allocator,
    binding: source.Scope,
    catalog_digest: source.Digest,
    context: Context,
) !void {
    const raw = try source.readOnService(svc, a, binding.table_id, binding.source_id, context);
    defer a.free(raw);
    var state = try source.parse(a, raw);
    defer state.deinit();
    const current = state.value;
    if (!current.scope_fencing or !current.manifest_retention or current.retired or
        !std.meta.eql(current.scope, @as(?source.Scope, binding))) return error.SourceBindingConflict;
    const record = current.artifact_control.?.current orelse return;

    if (current.pending) |pending| {
        if (record.phase == .garbage) {
            const retained = record.recovery orelse return error.SourceOutcomeUnresolved;
            if (!std.meta.eql(record.reservation, pending.identity(binding)) or
                !std.meta.eql(retained.digest, pending.plan_digest) or !std.mem.eql(u8, retained.bytes, pending.delivery_manifest))
                return error.SourceTransactionIdentityConflict;
            var parsed = try @import("source_destination_plan.zig").parse(a, retained.bytes, retained.digest);
            defer parsed.deinit();
            const fence = try retainedRoute(admission.coordinator.recovery, a, parsed.value, context);
            return reclaim(svc, a, current, catalog_digest, try parsed.value.transfer(fence, admission.artifacts), context);
        }
        var grant = try @import("source_destination_admission.zig").Grant.init(a, binding, pending, record);
        defer grant.deinit();
        const manifest = grant.manifest.value;
        const fence = try retainedRoute(admission.coordinator.recovery, a, manifest, context);
        if (record.phase == .reserved) {
            if (current.closing) return;
            const transfer = try manifest.transfer(fence, admission.artifacts);
            const stamp = (try transfer.recoverSeal(a, record, context)) orelse return;
            return mutate(svc, a, binding, catalog_digest, current, .{ .seal_artifact = .{ .reservation_digest = record.reservation, .stamp = stamp } }, context);
        }
        const acceptance = if (current.closing)
            (try admission.resolveDelivery(a, current, fence, context)) orelse return
        else
            try admission.admitDelivery(a, current, fence, context);
        defer a.free(acceptance.receipt);
        return mutate(svc, a, binding, catalog_digest, current, .{ .accept = .{
            .reservation_digest = acceptance.reservation_digest,
            .receipt = acceptance.receipt,
        } }, context);
    }

    var receipt = try receipts.decode(a, current, current.accepted.reference);
    defer receipt.deinit();
    if (receipt == .reclaimed) return;
    const manifest = receipt.retained.manifest.value;
    const fence = try retainedRoute(admission.coordinator.recovery, a, manifest, context);
    if (current.committed.sequence != current.accepted.sequence) {
        const worker: Completion = .{ .recovery = admission.coordinator.recovery };
        if (try worker.run(a, current, current.accepted.reference, fence, context) != .complete) return;
        const reference = try std.fmt.allocPrint(a, "{x}:{d}", .{ &manifest.admission_digest, manifest.commit_version });
        defer a.free(reference);
        return mutate(svc, a, binding, catalog_digest, current, .{ .commit = .{
            .sequence = current.accepted.sequence,
            .receipt_digest = current.accepted.receipt_digest,
            .reference = reference,
        } }, context);
    }
    if (record.phase == .accepted) {
        return mutate(svc, a, binding, catalog_digest, current, .{ .retire_artifact = .{
            .reservation_digest = record.reservation,
            .proof = source.digest(current.committed.reference),
            .reason = .committed,
        } }, context);
    }
    if (record.phase == .garbage) {
        try reclaim(svc, a, current, catalog_digest, try manifest.transfer(fence, admission.artifacts), context);
    }
}

fn reclaim(svc: anytype, a: std.mem.Allocator, state: source.State, catalog_digest: source.Digest, transfer: @import("source_plan_delivery.zig").Transfer, context: Context) !void {
    const record = state.artifact_control.?.current.?;
    const proof = (try transfer.reclaim(a, record, context)) orelse return;
    try mutate(svc, a, state.scope.?, catalog_digest, state, .{ .reclaim_artifact = .{ .reservation_digest = record.reservation, .proof = proof } }, context);
}

fn retainedRoute(recovery_worker: recovery.Recovery, a: std.mem.Allocator, manifest: @import("source_destination_plan.zig").Manifest, context: Context) !metadata.CatalogRouteFence {
    const catalog = @import("table_catalog.zig");
    var budget = recovery_worker.catalog.budget(null);
    budget.deadline_ns = budget.deadlineFrom(.{ .deadline_ns = context.deadline_ns, .io = context.deadline_io });
    budget.cancellation = context.cancellation;
    var session = try catalog.RoutingSession.initWithBudget(a, recovery_worker.catalog, budget);
    defer session.deinit();
    const locator = try @import("antfly_local_sources").api_local_transaction_contract.participantIdForGroup(a, manifest.checkpoint_authority.table_name, manifest.owner.group_id);
    defer a.free(locator);
    return recovery.routeRetainedOwner(&session, manifest.owner, locator, budget, context);
}

fn mutate(svc: anytype, a: std.mem.Allocator, scope: source.Scope, catalog_digest: source.Digest, state: source.State, mutation: source.Mutation, context: Context) !void {
    try source.mutateOnService(svc, a, .{
        .table_id = scope.table_id,
        .source_id = scope.source_id,
        .expected_revision = state.revision,
        .expected_binding = scope.identity(),
        .expected_catalog_digest = catalog_digest,
        .mutation = mutation,
    }, context);
}

/// Persist an exact native plan before uploading its bytes. A replay of the
/// provider batch can finish an interrupted upload with the original identity.
pub fn stage(
    svc: anytype,
    admission: @import("source_destination_admission.zig").Admission,
    a: std.mem.Allocator,
    binding: source.Scope,
    catalog_digest: source.Digest,
    input: @import("../metadata/source_delivery.zig").Input,
    tables: []const @import("distributed_txn.zig").TableCommitRequest,
    snapshot: ?@import("../metadata/source_scope_capture.zig").Snapshot,
    context: Context,
) !void {
    const raw = try source.readOnService(svc, a, binding.table_id, binding.source_id, context);
    defer a.free(raw);
    var state = try source.parse(a, raw);
    defer state.deinit();
    const current = state.value;
    if (!current.scope_fencing or current.closing or current.retired or
        !std.meta.eql(current.scope, @as(?source.Scope, binding))) return error.SourceBindingConflict;
    const authority = current.checkpoint_authority orelse return error.SourceBindingConflict;
    const ledger = current.artifact_control orelse return error.SourceBindingConflict;
    const previous: ?[]const u8 = if (current.accepted.sequence == 0) null else current.accepted.checkpoint;
    if ((previous == null) != (input.expected_checkpoint == null)) return error.SourceCheckpointConflict;
    if (previous) |checkpoint| if (!std.mem.eql(u8, checkpoint, input.expected_checkpoint.?)) return error.SourceCheckpointConflict;

    const sequence = std.math.add(u64, current.accepted.sequence, 1) catch return error.SourceReceiptExpired;
    const accepted_at = if (current.pending) |pending| pending.accepted_at_ns else input.accepted_at_ns;
    const logical: source.LogicalInput = .{
        .source_sequence = sequence,
        .batch_id = input.batch_id,
        .checkpoint = input.checkpoint,
        .expected_checkpoint = previous,
        .payload_digest = input.payload_digest,
        .accepted_at_ns = accepted_at,
    };
    const purpose = try source.inputFingerprint(binding, logical);
    const token = if (current.pending) |pending|
        (pending.artifact orelse return error.UnsupportedOperation).token()
    else
        std.math.add(u64, ledger.next_token, 1) catch return error.SourceReceiptExpired;
    const scoped = (try @import("../metadata/source_scope_binding.zig").reference(current, sequence, token)) orelse return error.SourceBindingConflict;
    var txn_id = purpose[0..16].*;
    if (std.mem.allEqual(u8, &txn_id, 0)) txn_id[15] = 1;
    if (current.pending) |pending| {
        if (!std.meta.eql(try pending.inputFingerprint(binding), purpose) or !std.meta.eql(pending.transaction_id, txn_id))
            return error.SourcePayloadConflict;
        const record = ledger.current orelse return error.SourcePendingWork;
        switch (record.phase) {
            .reserved => {},
            .sealed => return,
            else => return error.SourcePendingWork,
        }
    } else if (ledger.current != null or current.outstanding.len != 0) return error.SourcePendingWork;

    const checkpoint: @import("antfly_local_sources").storage_source_checkpoint.Command = .{
        .binding = binding.identity(),
        .purpose = purpose,
        .sequence = sequence,
        .checkpoint = input.checkpoint,
        .expected_checkpoint = previous,
        .accepted_at_ns = accepted_at,
        .receipt_window = 1,
    };
    var prepared = try @import("distributed_txn.zig").prepareSourceCommit(a, admission.coordinator, txn_id, accepted_at, tables, .{
        .authority = authority,
        .reference = scoped,
        .checkpoint = checkpoint,
    }, context);
    defer prepared.deinit();
    if (current.native_bootstrap_checkpoint != null) {
        const lifecycle = snapshot orelse return error.SourceBindingConflict;
        try lifecycle.requireBind();
        if (!lifecycle.native_bootstrap_certified or
            !std.meta.eql(lifecycle.spec, (try @import("../metadata/source_scope_binding.zig").spec(current)).?))
            return error.SourceBindingConflict;
        for (prepared.routes) |participant| try admission.requireScope(a, participant.fence, participant.table_name, .{
            .version = 1,
            .owner = participant.owner,
            .spec = lifecycle.spec,
            .minimum_metadata_index = lifecycle.cut.index,
            .action = .bind,
        }, context);
    }
    const route = prepared.routes[prepared.coordinator_index];
    if (current.pending) |pending| {
        const record = ledger.current.?;
        var original = try @import("source_destination_admission.zig").Grant.init(a, binding, pending, record);
        defer original.deinit();
        const bytes = try prepared.plan.encodeFrozen(a);
        defer a.free(bytes);
        const transfer = try original.manifest.value.transfer(route.fence, admission.artifacts);
        const stamp = try transfer.seal(a, record, bytes, context);
        return mutate(svc, a, binding, catalog_digest, current, .{ .seal_artifact = .{ .reservation_digest = record.reservation, .stamp = stamp } }, context);
    }

    const quota = try admission.artifacts.observations.read(a, route.fence, authority.table_name, .{
        .version = 1,
        .source_owner = route.owner,
        .query = .{ .root = try @import("../data/source_plan_entry.zig").rootFor(route.owner), .minimum_applied_index = 1 },
    }, context);
    const commit_version = std.math.add(u64, accepted_at, 1) catch return error.InvalidTxnRecord;
    var frozen = try @import("source_destination_plan.zig").Frozen.init(a, &prepared.plan, authority, quota, commit_version);
    defer frozen.deinit();
    const plan = try frozen.manifest.deliveryPlan(a, frozen.encoded_manifest);
    try mutate(svc, a, binding, catalog_digest, current, .{ .reserve = .{
        .sequence = sequence,
        .batch_id = input.batch_id,
        .checkpoint = input.checkpoint,
        .expected_checkpoint = previous,
        .accepted_at_ns = accepted_at,
        .payload_digest = input.payload_digest,
        .plan_digest = plan.digest,
        .transaction_id = txn_id,
        .destination = .native,
        .delivery_manifest = plan.manifest,
        .artifact = plan.artifact,
        .retain_manifest = true,
    } }, context);

    const reserved_raw = try source.readOnService(svc, a, binding.table_id, binding.source_id, context);
    defer a.free(reserved_raw);
    var reserved = try source.parse(a, reserved_raw);
    defer reserved.deinit();
    if (reserved.value.closing or reserved.value.retired or
        !std.meta.eql(reserved.value.scope, @as(?source.Scope, binding))) return error.SourceBindingConflict;
    const pending = reserved.value.pending orelse return error.SourceOutcomeUnresolved;
    const record = reserved.value.artifact_control.?.current orelse return error.SourceOutcomeUnresolved;
    if (!std.meta.eql(pending.plan_digest, plan.digest) or !std.meta.eql(record.reservation, pending.identity(binding)))
        return error.SourceTransactionIdentityConflict;
    const transfer = try frozen.manifest.transfer(route.fence, admission.artifacts);
    const stamp = try transfer.seal(a, record, frozen.bytes, context);
    try mutate(svc, a, binding, catalog_digest, reserved.value, .{ .seal_artifact = .{ .reservation_digest = record.reservation, .stamp = stamp } }, context);
}
