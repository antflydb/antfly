// Copyright 2026 Antfly, Inc.
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

//! Runtime implementation of the storage publisher contract. Local commit
//! locking and pending-record ownership stay in storage; this adapter owns
//! HA log publication, recovery matching, policy evaluation, and waits.
const std = @import("std");
const replication_contract = @import("../db/replication_contract.zig");
const ReplicationAsyncEffectMirror = replication_contract.AsyncEffectMirror;
const hot_standby_primary_mod = @import("primary.zig");
const hot_standby_commit_gate_mod = @import("commit_gate.zig");
const replication_effects_mod = @import("effects.zig");
const outbox = @import("../db/durable_outbox.zig");
const Namespace = @import("../db/doc_identity_namespace.zig").Namespace;

pub fn bind(primary: *hot_standby_primary_mod.Primary) replication_contract.Publisher {
    return .{ .ptr = primary, .vtable = &publisher_vtable };
}

/// Server integrations needing the concrete log must explicitly unwrap this
/// adapter. Reject foreign implementations before dereferencing their pointer.
pub fn runtimePrimary(mirror: ReplicationAsyncEffectMirror) !*hot_standby_primary_mod.Primary {
    if (mirror.publisher.vtable != &publisher_vtable) return error.UnsupportedReplicationPublisher;
    return @ptrCast(@alignCast(mirror.publisher.ptr));
}

const publisher_vtable: replication_contract.Publisher.VTable = .{
    .next_lsn = nextLsn,
    .identity = identity,
    .publish = publish,
    .recover = recover,
    .preflight = preflight,
    .complete = evaluateReplicationMirrorCommitGate,
};

fn nextLsn(ptr: *anyopaque) u64 {
    const primary: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(ptr));
    return primary.nextLsn();
}

fn identity(ptr: *anyopaque) replication_contract.Publisher.Identity {
    const primary: *hot_standby_primary_mod.Primary = @ptrCast(@alignCast(ptr));
    return .{ .table_id = primary.identity.table_id, .shard_id = primary.identity.shard_id, .timeline_id = primary.identity.timeline_id, .epoch = primary.identity.epoch };
}

fn publish(mirror: ReplicationAsyncEffectMirror, kind: outbox.Kind, payload: []const u8, namespace: Namespace) !u64 {
    const primary = try runtimePrimary(mirror);
    return switch (kind) {
        .batch, .restore_batch => replication_effects_mod.appendEncodedBatchMutationRequest(primary, payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }),
        .replay, .primary_effect => replication_effects_mod.appendEncodedDerivedChangeRecord(primary, payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }),
        .schema, .row_policy => replication_effects_mod.appendEncodedSchemaMetadataMutation(primary, payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }),
    };
}

fn recover(mirror: ReplicationAsyncEffectMirror, kind: outbox.Kind, pending: outbox.DurableReplicationOutbox, namespace: Namespace) !u64 {
    const primary = try runtimePrimary(mirror);
    if (try primary.findMatchingRecordFrom(pending.from_lsn, switch (kind) {
        .batch, .restore_batch => .batch_mutation,
        .replay, .primary_effect => .derived_effect,
        .schema, .row_policy => .metadata_mutation,
    }, pending.payload, namespace.shard_id, namespace.table_id)) |existing| return existing;
    return try publish(mirror, kind, pending.payload, namespace);
}

fn preflight(mirror: ReplicationAsyncEffectMirror, record_decision: bool) !void {
    if (mirror.sync_policy.mode == .async or mirror.sync_policy.failure_policy != .fail_closed) return;
    const primary = try runtimePrimary(mirror);
    const target_lsn = primary.nextLsn();
    const decision = try primary.evaluateAppendDurability(target_lsn, mirror.sync_policy);
    const gate = hotStandbyCommitGateResultFromDecision(target_lsn, decision);
    if (record_decision or gate.action == .reject) recordHotStandbyMirrorGate(mirror, gate);
    if (gate.action == .reject) {
        return error.SyncPolicyUnsatisfied;
    }
}

pub fn evaluateReplicationMirrorCommitGate(mirror: ReplicationAsyncEffectMirror, lsn: u64) !void {
    if (mirror.sync_policy.mode == .async) return;
    const primary = try runtimePrimary(mirror);
    var gate = try hot_standby_commit_gate_mod.evaluate(primary, lsn, mirror.sync_policy);
    recordHotStandbyMirrorGate(mirror, gate);
    switch (gate.action) {
        .acknowledge => return,
        .acknowledge_degraded => return,
        .reject => return error.SyncPolicyUnsatisfied,
        .wait_for_standby => {
            const wait_fn = mirror.sync_wait_fn orelse return error.HASyncCommitWouldBlock;
            const wait_ctx = mirror.sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext;
            try wait_fn(wait_ctx, mirror.publisher.ptr, lsn, mirror.sync_policy);
            gate = try hot_standby_commit_gate_mod.evaluate(primary, lsn, mirror.sync_policy);
            recordHotStandbyMirrorGate(mirror, gate);
            switch (gate.action) {
                .acknowledge => return,
                .acknowledge_degraded => return,
                .reject => return error.SyncPolicyUnsatisfied,
                .wait_for_standby => return error.HASyncCommitWouldBlock,
            }
        },
    }
}

pub fn recordHotStandbyMirrorGate(mirror: ReplicationAsyncEffectMirror, gate: hot_standby_commit_gate_mod.GateResult) void {
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

pub fn hotStandbyCommitGateResultFromDecision(target_lsn: u64, decision: hot_standby_primary_mod.DurabilityDecision) hot_standby_commit_gate_mod.GateResult {
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
