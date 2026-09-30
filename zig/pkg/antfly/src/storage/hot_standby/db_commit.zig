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

//! Runtime implementation of the storage publisher contract. Local commit
//! locking and pending-record ownership stay in storage; this adapter owns
//! HA log publication, recovery matching, policy evaluation, and waits.
const std = @import("std");
const ha_contract = @import("../db/ha_contract.zig");
const HAAsyncEffectMirror = ha_contract.AsyncEffectMirror;
const ha_primary_mod = @import("primary.zig");
const ha_commit_gate_mod = @import("commit_gate.zig");
const ha_effects_mod = @import("effects.zig");
const outbox = @import("../db/durable_outbox.zig");
const Namespace = @import("../db/doc_identity_namespace.zig").Namespace;

pub fn bind(primary: *ha_primary_mod.Primary) ha_contract.Publisher {
    return .{ .ptr = primary, .vtable = &publisher_vtable };
}

/// Server integrations needing the concrete log must explicitly unwrap this
/// adapter. Reject foreign implementations before dereferencing their pointer.
pub fn runtimePrimary(mirror: HAAsyncEffectMirror) !*ha_primary_mod.Primary {
    if (mirror.publisher.vtable != &publisher_vtable) return error.UnsupportedReplicationPublisher;
    return @ptrCast(@alignCast(mirror.publisher.ptr));
}

const publisher_vtable: ha_contract.Publisher.VTable = .{
    .next_lsn = nextLsn,
    .identity = identity,
    .publish = publish,
    .recover = recover,
    .preflight = preflight,
    .complete = evaluateHAMirrorCommitGate,
};

fn nextLsn(ptr: *anyopaque) u64 {
    const primary: *ha_primary_mod.Primary = @ptrCast(@alignCast(ptr));
    return primary.nextLsn();
}

fn identity(ptr: *anyopaque) ha_contract.Publisher.Identity {
    const primary: *ha_primary_mod.Primary = @ptrCast(@alignCast(ptr));
    return .{ .table_id = primary.identity.table_id, .shard_id = primary.identity.shard_id, .timeline_id = primary.identity.timeline_id, .epoch = primary.identity.epoch };
}

fn publish(mirror: HAAsyncEffectMirror, kind: outbox.Kind, payload: []const u8, namespace: Namespace) !u64 {
    const primary = try runtimePrimary(mirror);
    return switch (kind) {
        .batch, .restore_batch => ha_effects_mod.appendEncodedBatchMutationRequest(primary, payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }),
        .replay, .primary_effect => ha_effects_mod.appendEncodedDerivedChangeRecord(primary, payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }),
        .schema, .row_policy => ha_effects_mod.appendEncodedSchemaMetadataMutation(primary, payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }),
    };
}

fn recover(mirror: HAAsyncEffectMirror, kind: outbox.Kind, pending: outbox.DurableHAOutbox, namespace: Namespace) !u64 {
    const primary = try runtimePrimary(mirror);
    if (try primary.findMatchingRecordFrom(pending.from_lsn, switch (kind) {
        .batch, .restore_batch => .batch_mutation,
        .replay, .primary_effect => .derived_effect,
        .schema, .row_policy => .metadata_mutation,
    }, pending.payload, namespace.shard_id, namespace.table_id)) |existing| return existing;
    return try publish(mirror, kind, pending.payload, namespace);
}

fn preflight(mirror: HAAsyncEffectMirror, record_decision: bool) !void {
    if (mirror.sync_policy.mode == .async or mirror.sync_policy.failure_policy != .fail_closed) return;
    const primary = try runtimePrimary(mirror);
    const target_lsn = primary.nextLsn();
    const decision = try primary.evaluateAppendDurability(target_lsn, mirror.sync_policy);
    const gate = haCommitGateResultFromDecision(target_lsn, decision);
    if (record_decision or gate.action == .reject) recordHAMirrorGate(mirror, gate);
    if (gate.action == .reject) {
        return error.SyncPolicyUnsatisfied;
    }
}

pub fn evaluateHAMirrorCommitGate(mirror: HAAsyncEffectMirror, lsn: u64) !void {
    if (mirror.sync_policy.mode == .async) return;
    const primary = try runtimePrimary(mirror);
    var gate = try ha_commit_gate_mod.evaluate(primary, lsn, mirror.sync_policy);
    recordHAMirrorGate(mirror, gate);
    switch (gate.action) {
        .acknowledge => return,
        .acknowledge_degraded => return,
        .reject => return error.SyncPolicyUnsatisfied,
        .wait_for_standby => {
            const wait_fn = mirror.sync_wait_fn orelse return error.HASyncCommitWouldBlock;
            const wait_ctx = mirror.sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext;
            try wait_fn(wait_ctx, mirror.publisher.ptr, lsn, mirror.sync_policy);
            gate = try ha_commit_gate_mod.evaluate(primary, lsn, mirror.sync_policy);
            recordHAMirrorGate(mirror, gate);
            switch (gate.action) {
                .acknowledge => return,
                .acknowledge_degraded => return,
                .reject => return error.SyncPolicyUnsatisfied,
                .wait_for_standby => return error.HASyncCommitWouldBlock,
            }
        },
    }
}

pub fn recordHAMirrorGate(mirror: HAAsyncEffectMirror, gate: ha_commit_gate_mod.GateResult) void {
    if (mirror.last_gate_lsn) |last_lsn| last_lsn.store(gate.target_lsn, .release);
    if (mirror.last_gate_action) |last_action| last_action.store(@intFromEnum(gate.action), .release);
    switch (gate.action) {
        .acknowledge => {},
        .acknowledge_degraded => {
            if (mirror.sync_degraded_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
        .reject => {
            if (mirror.sync_reject_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
        .wait_for_standby => {
            if (mirror.sync_wait_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
    }
}

pub fn haCommitGateResultFromDecision(target_lsn: u64, decision: ha_primary_mod.DurabilityDecision) ha_commit_gate_mod.GateResult {
    return .{
        .target_lsn = target_lsn,
        .action = switch (decision.status) {
            .satisfied => .acknowledge,
            .would_block => .wait_for_standby,
            .fail_closed => .reject,
            .degraded_to_async => .acknowledge_degraded,
        },
        .decision = decision,
    };
}
