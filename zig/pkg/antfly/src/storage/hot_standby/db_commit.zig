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

pub const SyncWaitFn = *const fn (*anyopaque, *anyopaque, u64, hot_standby_primary_mod.SyncPolicy) anyerror!void;

/// Server-owned policy and telemetry, captured by value in the local binding.
/// Pointer/slice targets remain borrowed; no target may be a temporary.
pub const Options = struct {
    mutation_barrier: ?*@import("antfly_runtime_abi").mutation_barrier.MutationBarrier = null,
    transition_mutex: ?*std.atomic.Mutex = null,
    last_lsn: ?*@import("antfly_platform").atomic.Value(u64) = null,
    failure_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
    sync_policy: hot_standby_primary_mod.SyncPolicy = .{},
    sync_wait_ctx: ?*anyopaque = null,
    sync_wait_fn: ?SyncWaitFn = null,
    last_gate_lsn: ?*@import("antfly_platform").atomic.Value(u64) = null,
    last_gate_action: ?*std.atomic.Value(u8) = null,
    sync_reject_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
    sync_wait_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
    sync_degraded_count: ?*@import("antfly_platform").atomic.Value(u64) = null,
};

pub fn bindMirror(primary: *hot_standby_primary_mod.Primary, configuration: Options) ReplicationAsyncEffectMirror {
    return .{
        .publisher = bind(primary),
        .mutation_barrier = configuration.mutation_barrier,
        .transition_mutex = configuration.transition_mutex,
        .requirements = .{
            .synchronous = configuration.sync_policy.mode != .async,
            .durable_outbox = configuration.sync_policy.mode != .async and configuration.sync_policy.failure_policy != .degrade_to_async,
            .preflight = configuration.sync_policy.mode != .async and configuration.sync_policy.failure_policy == .fail_closed,
        },
        .capture = replication_contract.BorrowedCapture.init(Options, configuration),
    };
}

pub fn options(binding: ReplicationAsyncEffectMirror) Options {
    std.debug.assert(binding.publisher.vtable == &publisher_vtable);
    return binding.capture.read(Options);
}

fn notePublished(binding: ReplicationAsyncEffectMirror, lsn: u64) void {
    if (options(binding).last_lsn) |last| last.store(lsn, .release);
}
fn noteFailure(binding: ReplicationAsyncEffectMirror) void {
    if (options(binding).failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
}

fn equalCapture(a: ReplicationAsyncEffectMirror, b: ReplicationAsyncEffectMirror) bool {
    const left = options(a);
    const right = options(b);
    inline for (std.meta.fields(Options)) |field| {
        if (comptime std.mem.eql(u8, field.name, "sync_policy")) {
            const x = left.sync_policy;
            const y = right.sync_policy;
            if (x.mode != y.mode or x.selection != y.selection or x.required != y.required or
                x.failure_policy != y.failure_policy or x.standby_names.len != y.standby_names.len) return false;
            for (x.standby_names, y.standby_names) |xn, yn| {
                if (!std.mem.eql(u8, xn, yn)) return false;
            }
        } else {
            if (@field(left, field.name) != @field(right, field.name)) return false;
        }
    }
    return true;
}

fn bind(primary: *hot_standby_primary_mod.Primary) replication_contract.Publisher {
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
    .note_published = notePublished,
    .note_failure = noteFailure,
    .equal_capture = equalCapture,
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
    if (options(mirror).sync_policy.mode == .async or options(mirror).sync_policy.failure_policy != .fail_closed) return;
    const primary = try runtimePrimary(mirror);
    const target_lsn = primary.nextLsn();
    const decision = try primary.evaluateAppendDurability(target_lsn, options(mirror).sync_policy);
    const gate = hotStandbyCommitGateResultFromDecision(target_lsn, decision);
    if (record_decision or gate.action == .reject) recordHotStandbyMirrorGate(mirror, gate);
    if (gate.action == .reject) {
        return error.SyncPolicyUnsatisfied;
    }
}

pub fn evaluateReplicationMirrorCommitGate(mirror: ReplicationAsyncEffectMirror, lsn: u64) !void {
    if (options(mirror).sync_policy.mode == .async) return;
    const primary = try runtimePrimary(mirror);
    var gate = try hot_standby_commit_gate_mod.evaluate(primary, lsn, options(mirror).sync_policy);
    recordHotStandbyMirrorGate(mirror, gate);
    switch (gate.action) {
        .acknowledge => return,
        .acknowledge_degraded => return,
        .reject => return error.SyncPolicyUnsatisfied,
        .wait_for_standby => {
            const wait_fn = options(mirror).sync_wait_fn orelse return error.HASyncCommitWouldBlock;
            const wait_ctx = options(mirror).sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext;
            try wait_fn(wait_ctx, mirror.publisher.ptr, lsn, options(mirror).sync_policy);
            gate = try hot_standby_commit_gate_mod.evaluate(primary, lsn, options(mirror).sync_policy);
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
    if (options(mirror).last_gate_lsn) |last_lsn| last_lsn.store(gate.target_lsn, .release);
    if (options(mirror).last_gate_action) |last_action| last_action.store(@intFromEnum(gate.action), .release);
    switch (gate.action) {
        .acknowledge => {},
        .acknowledge_degraded => {
            if (options(mirror).sync_degraded_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
        .reject => {
            if (options(mirror).sync_reject_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        },
        .wait_for_standby => {
            if (options(mirror).sync_wait_count) |counter| _ = counter.fetchAdd(1, .monotonic);
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

test "storage.hot_standby adapter captures policy by value and borrows telemetry" {
    // Only the pointer identity is used; no log operation touches this primary.
    var primary: hot_standby_primary_mod.Primary = undefined;
    var last: @import("antfly_platform").atomic.Value(u64) = .init(0);
    var failures: @import("antfly_platform").atomic.Value(u64) = .init(0);
    var configuration: Options = .{
        .sync_policy = .{ .mode = .remote_apply, .failure_policy = .fail_closed },
        .last_lsn = &last,
        .failure_count = &failures,
    };
    const original = bindMirror(&primary, configuration);
    const copy = original;
    configuration.sync_policy = .{};
    try std.testing.expectEqual(hot_standby_primary_mod.DurabilityMode.remote_apply, options(copy).sync_policy.mode);
    try std.testing.expect(copy.requirements.synchronous);
    try std.testing.expect(copy.requirements.durable_outbox);
    try std.testing.expect(copy.requirements.preflight);
    copy.notePublished(19);
    copy.noteFailure();
    try std.testing.expectEqual(@as(u64, 19), last.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), failures.load(.acquire));
    try std.testing.expect(original.eql(copy));
    try std.testing.expect(!original.eql(bindMirror(&primary, configuration)));
}

test "storage.hot_standby adapter maps durability policy to local commit requirements" {
    var primary: hot_standby_primary_mod.Primary = undefined;
    for (std.enums.values(hot_standby_primary_mod.DurabilityMode)) |mode| {
        for (std.enums.values(hot_standby_primary_mod.FailurePolicy)) |failure| {
            const binding = bindMirror(&primary, .{ .sync_policy = .{ .mode = mode, .failure_policy = failure } });
            try std.testing.expectEqual(mode != .async, binding.requirements.synchronous);
            try std.testing.expectEqual(mode != .async and failure != .degrade_to_async, binding.requirements.durable_outbox);
            try std.testing.expectEqual(mode != .async and failure == .fail_closed, binding.requirements.preflight);
        }
    }
}
