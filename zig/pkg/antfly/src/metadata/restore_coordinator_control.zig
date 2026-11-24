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

//! Durable coordinator access. Data owners use placement-scoped restore authority.
const std = @import("std");
const metadata_service = @import("service.zig");
const staging = @import("restore_staging.zig");
const operation = @import("antfly_local_sources").api_operation;

pub fn read(store: anytype, alloc: std.mem.Allocator, group: u64, id: staging.Id) !?[]u8 {
    var loaded = (try store.loadRestoreStaging(alloc, group, id)) orelse return null;
    defer loaded.deinit();
    return try std.json.Stringify.valueAlloc(alloc, loaded.value, .{});
}

pub fn apply(svc: anytype, alloc: std.mem.Allocator, command_json: []const u8, request: operation.RequestContext, compact: bool) ![]u8 {
    return applyInner(svc, alloc, command_json, request, compact) catch |err| {
        if (diagnostics.admit(@import("antfly_platform").time.monotonicNs()))
            std.log.warn("restore coordinator metadata request failed class={s}", .{@errorName(err)});
        return err;
    };
}

const CatalogParticipant = @import("../system_catalog/prepared_transactions.zig").CatalogParticipant;

/// Submit only after the SQL session has durably sealed this participant.
/// Retrying the same plan observes the first immutable reservation.
pub fn reserveCatalog(svc: anytype, alloc: std.mem.Allocator, plan: staging.Plan, participant: CatalogParticipant, request: operation.RequestContext) !std.json.Parsed(staging.Job) {
    try request.ensureActive();
    try request.checkMetadataIncarnation(plan.expected_incarnation);
    try requireCatalogPlan(alloc, plan, participant);
    const command: staging.Command = .{ .id = participant.staging_id, .action = .reserve, .plan = plan };
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    if (bytes.len > staging.max_encoded_bytes) return error.InvalidRestoreStaging;
    request.markSQLSubmission();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .reserve_sql_catalog_restore = bytes });
    try svc.waitForTransitionAppliedWithContext(receipt, request);
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return observeCatalogReservation(store, alloc, svc.metadata_group_id, participant);
}

pub fn acceptCatalogProgress(svc: anytype, alloc: std.mem.Allocator, command: staging.Command, request: operation.RequestContext) !void {
    try request.ensureActive();
    const bytes = try std.json.Stringify.valueAlloc(alloc, command, .{});
    defer alloc.free(bytes);
    var arena = std.heap.ArenaAllocator.init(alloc);
    defer arena.deinit();
    _ = try staging.parseCatalogProgress(arena.allocator(), bytes);
    request.markSQLSubmission();
    const proposal = try svc.proposeTransitionCommandWithReceipt(.{ .accept_sql_catalog_progress = bytes });
    try svc.waitForTransitionAppliedWithContext(proposal, request);
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    if (command.receipt) |owner| {
        const expected = try command.receiptIdentity(alloc);
        const recorded = (try store.loadRestoreStagingReceiptIdentity(alloc, svc.metadata_group_id, command.id, staging.Command.receiptPhase(command.action).?, owner.group_id)) orelse return error.CatalogPublicationProofPending;
        defer alloc.free(recorded);
        if (!std.mem.eql(u8, recorded, &expected)) return error.ExternalTransactionDecisionChanged;
        return;
    }
    const expected: staging.State = switch (command.action) {
        .begin_cutover => .cutover,
        .begin_activation => .activating,
        .begin_cancel => .canceling,
        else => unreachable,
    };
    const progress = (try store.loadRestoreStagingProgress(alloc, svc.metadata_group_id, command.id)) orelse return error.CatalogPublicationProofPending;
    if (progress.state != expected or progress.revision <= command.expected_revision) return error.CatalogPublicationProofPending;
}
pub fn observeCatalogReservation(store: anytype, alloc: std.mem.Allocator, group: u64, participant: CatalogParticipant) !std.json.Parsed(staging.Job) {
    var job = (try store.loadRestoreStaging(alloc, group, participant.staging_id)) orelse return error.CatalogPublicationProofPending;
    errdefer job.deinit();
    try requireCatalogPlan(alloc, job.value.plan, participant);
    if (!std.mem.eql(u8, &job.value.plan_digest, &participant.plan_digest)) return error.ExternalTransactionDecisionChanged;
    return job;
}

fn requireCatalogPlan(alloc: std.mem.Allocator, plan: staging.Plan, participant: CatalogParticipant) !void {
    try participant.validate();
    const binding = plan.catalog_binding orelse return error.InvalidCatalogMutation;
    if (!std.mem.eql(u8, &participant.staging_id, &plan.id) or
        !std.mem.eql(u8, &participant.intent_digest, &binding.intent_digest) or
        !std.mem.eql(u8, &participant.plan_digest, &try plan.digest(alloc)))
        return error.ExternalTransactionDecisionChanged;
}

var diagnostics: @import("antfly_local_sources").api_bounded_diagnostic_gate.Gate = .{};

fn applyInner(svc: anytype, alloc: std.mem.Allocator, command_json: []const u8, request: operation.RequestContext, compact: bool) ![]u8 {
    try request.ensureActive();
    var command = try staging.parseCommand(alloc, command_json);
    defer command.deinit();
    try metadata_service.ensureQueryPopulationDecoderWithContext(svc, command_json, request);
    try metadata_service.ensureRestoreStagingDecoderWithContext(svc, command_json, request);
    if (compact and !staging.Command.isProgressOnly(command.value.action)) return error.InvalidRestoreStaging;
    request.markSQLSubmission();
    const receipt = try svc.proposeTransitionCommandWithReceipt(.{ .apply_restore_staging = command_json });
    try svc.waitForTransitionAppliedWithContext(receipt, request);
    try svc.ensureLinearizableReadWithContext(request);
    const store = svc.projectedStore() orelse return error.MissingMetadataStore;
    return appliedResponse(store, alloc, svc.metadata_group_id, command.value);
}

pub fn appliedResponse(store: anytype, alloc: std.mem.Allocator, group: u64, command: staging.Command) ![]u8 {
    if (command.record_receipt_identity) {
        const phase = staging.Command.receiptPhase(command.action).?;
        const expected = command.receipt.?;
        const accepted = checked: {
            const stored = (try store.loadRestoreStagingReceiptIdentity(alloc, group, command.id, phase, expected.group_id)) orelse break :checked false;
            defer alloc.free(stored);
            break :checked std.mem.eql(u8, stored, &try command.receiptIdentity(alloc));
        };
        const progress = (try store.loadRestoreStagingProgress(alloc, group, command.id)) orelse return error.RestoreStagingChanged;
        return std.json.Stringify.valueAlloc(alloc, staging.CompactApply{ .restore_progress = progress, .receipt_accepted = accepted }, .{});
    }
    if (try read(store, alloc, group, command.id)) |job| return job;
    if (command.record_reservation_outcome) {
        if (try store.loadRestoreStagingProgress(alloc, group, command.id)) |progress| {
            if (progress.reservation_rejection) |reason|
                return std.json.Stringify.valueAlloc(alloc, staging.RejectedReserve{ .restore_reservation_rejection = reason }, .{});
        }
        // The command applied without installing a plan. Let the caller
        // rebuild its observations without treating this as lost delivery.
        return alloc.dupe(u8, "null");
    }
    if (command.action == .cancel_reservation) {
        const progress = (try store.loadRestoreStagingProgress(alloc, group, command.id)) orelse return error.RestoreStagingChanged;
        return std.json.Stringify.valueAlloc(alloc, progress, .{});
    }
    return error.RestoreStagingChanged;
}
