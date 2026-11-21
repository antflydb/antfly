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

//! Private bounded owner for ordered root controls. A confirmed begin CAS
//! permits one submit. An unproven renewal can repeat its native expiration
//! CAS. Accepted work uses its original receipt for read-only recovery.
//! A known receipt is returned even when metadata persistence fails.
const std = @import("std");
const control = @import("changefeed_control.zig");
const outcome = @import("root_control_outcome.zig");
const intent = control.root_intent;
const operations = @import("../api/history_control_operations.zig");
const wire = operations.wire;
const grants = @import("../api/history_grant_operations.zig");
const observation = @import("root_control_observation.zig");
const metadata = @import("api.zig");
const A = std.mem.Allocator;
pub const Acceptance = struct {
    sequence: u64,
    actor: intent.Actor,
    request_digest: intent.Digest,
    receipt: intent.Receipt,
};
pub const Result = struct {
    status: enum { idle, prepared, ambiguous, accepted, settled, superseded, cancelled_prepared, unproven, reconciled },
    acceptance: ?Acceptance = null,
    settlement: ?intent.Settlement = null,
    reconciliation: ?intent.Reconciliation = null,
    deferred_error: ?anyerror = null,
};
fn same(a: anytype, b: @TypeOf(a)) bool {
    return std.mem.eql(u8, &a, &b);
}
fn load(svc: anytype, a: A, id: control.Id, request: anytype) !control.Parsed {
    try request.ensureActive();
    const bytes = try control.readOnService(svc, a, id, request);
    defer a.free(bytes);
    var parsed = try control.parse(a, bytes);
    errdefer parsed.deinit();
    if (!same(parsed.value.id, id)) return error.ChangefeedConflict;
    return parsed;
}
fn mutate(svc: anytype, a: A, state: control.State, namespace: control.Namespace, operation: control.RootControlMutation, request: anytype) !void {
    const definition = state.definition orelse return error.ChangefeedConflict;
    return control.mutateOnService(svc, a, .{ .id = state.id, .authority = state.authority, .owner = definition.owner, .authorization = definition.authorization, .expected_revision = state.revision, .expected_epoch = state.epoch, .mutation = .{ .root_control = .{ .namespace = namespace, .operation = operation } } }, request);
}
fn fence(a: A, payload: intent.Payload) !std.json.Parsed(metadata.CatalogRouteFence) {
    return std.json.parseFromSlice(metadata.CatalogRouteFence, a, payload.fence, .{ .ignore_unknown_fields = false, .duplicate_field_behavior = .@"error" });
}
/// Policy methods: authorizeRoot(state, namespace, request),
/// validateRoot(state, namespace, frozen_fence, table, request),
/// newAttempt(state, namespace, request), and context(request). validateRoot must
/// check the current table incarnation, implemented capability and owner binding.
/// newAttempt must produce a fresh unpredictable actor for every invocation.
/// context preserves the original deadline/cancellation for the root transport.
pub fn prepare(svc: anytype, policy: anytype, a: A, id: control.Id, namespace: control.Namespace, payload: intent.Payload, request: anytype) !void {
    var current = try load(svc, a, id, request);
    defer current.deinit();
    const state = current.value;
    try policy.authorizeRoot(state, namespace, request);
    _ = try outcome.validateRequest(a, state, namespace, payload);
    var route = try fence(a, payload);
    defer route.deinit();
    try policy.validateRoot(state, namespace, route.value, payload.table, request);
    const item = state.roots[try state.rootIndex(namespace)];
    try mutate(svc, a, state, namespace, .{ .prepare = .{ .ownership_epoch = item.ownership_epoch, .grant_sequence = item.grant_sequence, .payload = payload } }, request);
}
/// Begin a distinct irreversible cleanup attempt, never a retry of the old
/// command. The old request identity/known receipt remains in bounded metadata.
/// This does not submit anything; drive must acquire a fresh begin permission.
pub fn prepareRetirementFence(svc: anytype, policy: anytype, a: A, id: control.Id, namespace: control.Namespace, request: anytype) !void {
    var current = try load(svc, a, id, request);
    defer current.deinit();
    const state = current.value;
    try policy.authorizeRoot(state, namespace, request);
    if (state.phase != .closing and state.phase != .reset_required) return error.ChangefeedConflict;
    const item = state.roots[try state.rootIndex(namespace)];
    const previous = item.pending_control orelse return error.ChangefeedConflict;
    const input: wire.Request = .{ .version = 1, .operation = .submit, .command = .{ .retire = .{ .namespace = namespace, .authority = state.authority, .consumer = state.consumerId(), .definition = state.definition.?.identity(), .grant = item.grant_sequence, .sequence = state.authority_sequence } } };
    const body = try std.json.Stringify.valueAlloc(a, input, .{});
    defer a.free(body);
    const payload: intent.Payload = .{ .command = body, .fence = previous.payload.fence, .table = previous.payload.table };
    _ = try outcome.validateNewRequest(a, state, namespace, payload);
    var route = try fence(a, payload);
    defer route.deinit();
    try policy.validateRoot(state, namespace, route.value, payload.table, request);
    try mutate(svc, a, state, namespace, .{ .prepare_fence = .{ .previous_sequence = previous.sequence, .retirement_sequence = state.authority_sequence, .payload = payload } }, request);
}
/// Attach a receipt retained by the owner after a metadata write/ACK failure.
/// This never dispatches or settles anything. Stale attempts cannot overwrite a
/// newer pending request. The caller supplies only a trusted transport result.
pub fn recordAcceptance(svc: anytype, policy: anytype, a: A, id: control.Id, namespace: control.Namespace, accepted: Acceptance, request: anytype) !void {
    var current = try load(svc, a, id, request);
    defer current.deinit();
    const state = current.value;
    try policy.authorizeRoot(state, namespace, request);
    const item = state.roots[try state.rootIndex(namespace)];
    const pending = item.pending_control orelse {
        if (item.last_control) |last| if (last.sequence == accepted.sequence and same(last.actor, accepted.actor) and same(last.request_digest, accepted.request_digest) and std.meta.eql(last.receipt, accepted.receipt)) return;
        if (item.last_reconciliation) |last| if (last.sequence == accepted.sequence and same(last.actor, accepted.actor) and same(last.request_digest, accepted.request_digest) and (last.original_receipt == null or std.meta.eql(last.original_receipt.?, accepted.receipt))) return;
        return error.ChangefeedConflict;
    };
    if (pending.sequence != accepted.sequence or !same(pending.actor, accepted.actor) or !same(pending.digest, accepted.request_digest)) return error.ChangefeedConflict;
    try mutate(svc, a, state, namespace, .{ .accepted = .{ .sequence = accepted.sequence, .actor = accepted.actor, .receipt = accepted.receipt } }, request);
}
pub fn drive(svc: anytype, policy: anytype, port: operations.Port, a: A, id: control.Id, namespace: control.Namespace, request: anytype) !Result {
    var current = try load(svc, a, id, request);
    defer current.deinit();
    const state = current.value;
    try policy.authorizeRoot(state, namespace, request);
    const item = state.roots[try state.rootIndex(namespace)];
    const pending = item.pending_control orelse return .{ .status = .idle, .settlement = item.last_control, .reconciliation = item.last_reconciliation };
    switch (pending.stage) {
        .dispatching => return .{ .status = .ambiguous },
        .accepted => return perform(svc, policy, port, a, state, namespace, false, request),
        .prepared => {
            const validated = try outcome.validateRequest(a, state, namespace, pending.payload);
            const cleanup = validated.input.command == .retire or validated.input.command == .collect or validated.input.command == .reclaim;
            if ((state.phase == .closing or state.phase == .reset_required) and !cleanup) {
                try mutate(svc, a, state, namespace, .{ .cancel_prepared = pending.sequence }, request);
                return .{ .status = .cancelled_prepared };
            }
            var route = try fence(a, pending.payload);
            defer route.deinit();
            try policy.validateRoot(state, namespace, route.value, pending.payload.table, request);
            const context = try policy.context(request);
            try context.ensureActive();
            const actor = try policy.newAttempt(state, namespace, request);
            try mutate(svc, a, state, namespace, .{ .begin = .{ .sequence = pending.sequence, .actor = actor } }, request);
            // A confirmed begin belongs only to this stack invocation. Reload
            // before dispatch; an ACK-lost begin never reaches this branch.
            var begun = load(svc, a, id, request) catch |err| return .{ .status = .ambiguous, .deferred_error = err };
            defer begun.deinit();
            const active = begun.value.roots[try begun.value.rootIndex(namespace)].pending_control orelse return error.ChangefeedConflict;
            if (active.sequence != pending.sequence or active.stage != .dispatching or !same(active.actor, actor) or !same(active.digest, pending.digest)) return error.ChangefeedConflict;
            return perform(svc, policy, port, a, begun.value, namespace, true, request);
        },
    }
}
/// Renewal is a native compare-and-set on the same grant, epoch, and
/// previous expiration. It can be submitted again after an unproven read:
/// racing submissions either advance to the same target or lose the CAS.
pub fn resumeRenewal(svc: anytype, policy: anytype, port: operations.Port, a: A, id: control.Id, namespace: control.Namespace, sequence: u64, request: anytype) !Result {
    var current = try load(svc, a, id, request);
    defer current.deinit();
    const state = current.value;
    try policy.authorizeRoot(state, namespace, request);
    const item = state.roots[try state.rootIndex(namespace)];
    const pending = item.pending_control orelse return error.ChangefeedConflict;
    if (state.phase != .streaming or pending.sequence != sequence or pending.stage != .dispatching) return error.ChangefeedConflict;
    const validated = try outcome.validateRequest(a, state, namespace, pending.payload);
    if (validated.input.command != .renew) return error.ChangefeedConflict;
    return perform(svc, policy, port, a, state, namespace, true, request);
}

/// Read-only semantic recovery. No path through this function submits a
/// control or promotes an observation into the original command's receipt.
pub fn reconcile(svc: anytype, policy: anytype, port: grants.Port, a: A, id: control.Id, namespace: control.Namespace, request_context: anytype) !Result {
    var current = try load(svc, a, id, request_context);
    defer current.deinit();
    try policy.authorizeRoot(current.value, namespace, request_context);
    const item = current.value.roots[try current.value.rootIndex(namespace)];
    const pending = item.pending_control orelse return .{ .status = .idle, .settlement = item.last_control, .reconciliation = item.last_reconciliation };
    if (pending.stage == .prepared) return .{ .status = .prepared };
    const input = try observation.request(a, current.value, namespace);
    var route = try fence(a, pending.payload);
    defer route.deinit();
    try policy.validateRoot(current.value, namespace, route.value, pending.payload.table, request_context);
    const context = try policy.context(request_context);
    try context.ensureActive();
    const observed = try port.read(a, route.value, pending.payload.table, input, context);
    try context.ensureActive();
    var latest = try load(svc, a, id, request_context);
    defer latest.deinit();
    try policy.authorizeRoot(latest.value, namespace, request_context);
    const latest_item = latest.value.roots[try latest.value.rootIndex(namespace)];
    const latest_pending = latest_item.pending_control orelse return .{ .status = .idle, .settlement = latest_item.last_control, .reconciliation = latest_item.last_reconciliation };
    if (latest_pending.sequence != pending.sequence or !same(latest_pending.actor, pending.actor) or !same(latest_pending.digest, pending.digest)) return error.ChangefeedConflict;
    try policy.validateRoot(latest.value, namespace, route.value, pending.payload.table, request_context);
    const proof = (try observation.resolve(a, latest.value, namespace, observed)) orelse return .{ .status = .unproven };
    const Policy = @typeInfo(@TypeOf(policy)).pointer.child;
    if (comptime @hasDecl(Policy, "deferRetirementSettlement")) {
        if (proof.effect == .released and policy.deferRetirementSettlement()) return .{ .status = .unproven };
    }
    try mutate(svc, a, latest.value, namespace, .{ .reconciled = proof }, request_context);
    return .{ .status = .reconciled, .reconciliation = proof };
}
fn perform(svc: anytype, policy: anytype, port: operations.Port, a: A, state: control.State, namespace: control.Namespace, submit: bool, request: anytype) !Result {
    const pending = state.roots[try state.rootIndex(namespace)].pending_control.?;
    var result: Result = .{ .status = if (submit) .ambiguous else .accepted };
    if (pending.receipt) |receipt| result.acceptance = .{ .sequence = pending.sequence, .actor = pending.actor, .request_digest = pending.digest, .receipt = receipt };
    try policy.authorizeRoot(state, namespace, request);
    const validated = try outcome.validateRequest(a, state, namespace, pending.payload);
    var route = try fence(a, pending.payload);
    defer route.deinit();
    try policy.validateRoot(state, namespace, route.value, pending.payload.table, request);
    var input = validated.input;
    if (!submit) {
        input.operation = .recover;
        input.receipt = .{ .term = pending.receipt.?.term, .index = pending.receipt.?.index };
    }
    const context = try policy.context(request);
    try context.ensureActive();
    const response = port.execute(a, route.value, pending.payload.table, input, context) catch |err| {
        result.deferred_error = err;
        return result;
    };
    response.validate(input) catch |err| {
        result.deferred_error = err;
        return result;
    };
    const receipt = response.receipt orelse return result;
    const accepted: Acceptance = .{ .sequence = pending.sequence, .actor = pending.actor, .request_digest = pending.digest, .receipt = .{ .term = receipt.term, .index = receipt.index } };
    result.status = .accepted;
    result.acceptance = accepted;
    if (submit) recordAcceptance(svc, policy, a, state.id, namespace, accepted, request) catch |err| {
        result.deferred_error = err;
        return result;
    };
    if (response.outcome) |value| {
        if (value == .superseded) {
            result.status = .superseded;
            return result;
        }
    }
    if (!response.confirmed()) return result;
    // The control receipt proves enrollment. Only an independently qualified
    // grant read binds the immutable snapshot image to the metadata cut.
    if (input.command == .capture_bootstrap and response.outcome.? == .applied) return result;
    var latest = load(svc, a, state.id, request) catch |err| {
        result.deferred_error = err;
        return result;
    };
    defer latest.deinit();
    const settlement = outcome.settle(a, latest.value, namespace, response) catch |err| {
        result.deferred_error = err;
        return result;
    };
    policy.authorizeRoot(latest.value, namespace, request) catch |err| {
        result.deferred_error = err;
        return result;
    };
    policy.validateRoot(latest.value, namespace, route.value, pending.payload.table, request) catch |err| {
        result.deferred_error = err;
        return result;
    };
    // Some owners have durable post-retirement work. Leave the accepted
    // intent available for semantic reconciliation after that work finishes.
    const Policy = @typeInfo(@TypeOf(policy)).pointer.child;
    if (comptime @hasDecl(Policy, "deferRetirementSettlement")) {
        if (settlement.effect == .retired and policy.deferRetirementSettlement()) return result;
    }
    mutate(svc, a, latest.value, namespace, .{ .settled = settlement }, request) catch |err| {
        result.deferred_error = err;
        return result;
    };
    return .{ .status = .settled, .acceptance = accepted, .settlement = settlement };
}
