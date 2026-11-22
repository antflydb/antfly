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
        if (record.phase == .garbage) return;
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
        const transfer = try manifest.transfer(fence, admission.artifacts);
        const proof = (try transfer.reclaim(a, record, context)) orelse return;
        try mutate(svc, a, binding, catalog_digest, current, .{ .reclaim_artifact = .{ .reservation_digest = record.reservation, .proof = proof } }, context);
    }
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
