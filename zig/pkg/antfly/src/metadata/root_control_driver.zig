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

//! Private bounded owner for ordered root controls. Only a confirmed begin CAS
//! permits one submit. Recovered dispatching work is ambiguous and never gets
//! submitted again. Accepted work uses its original receipt for read-only
//! recovery. A known receipt is returned even when metadata persistence fails.
const std = @import("std");
const control = @import("changefeed_control.zig");
const outcome = @import("root_control_outcome.zig");
const intent = control.root_intent;
const operations = @import("../api/history_control_operations.zig");
const wire = operations.wire;
const metadata = @import("api.zig");
const A = std.mem.Allocator;
pub const Acceptance = struct {
    sequence: u64,
    actor: intent.Actor,
    request_digest: intent.Digest,
    receipt: intent.Receipt,
};
pub const Result = struct {
    status: enum { idle, ambiguous, accepted, settled, superseded, cancelled_prepared },
    acceptance: ?Acceptance = null,
    settlement: ?intent.Settlement = null,
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
    const pending = item.pending_control orelse return .{ .status = .idle, .settlement = item.last_control };
    switch (pending.stage) {
        .dispatching => return .{ .status = .ambiguous },
        .accepted => return perform(svc, policy, port, a, state, namespace, false, request),
        .prepared => {
            if (state.phase == .closing or state.phase == .reset_required) {
                try mutate(svc, a, state, namespace, .{ .cancel_prepared = pending.sequence }, request);
                return .{ .status = .cancelled_prepared };
            }
            _ = try outcome.validateRequest(a, state, namespace, pending.payload);
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
    mutate(svc, a, latest.value, namespace, .{ .settled = settlement }, request) catch |err| {
        result.deferred_error = err;
        return result;
    };
    return .{ .status = .settled, .acceptance = accepted, .settlement = settlement };
}
