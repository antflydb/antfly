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

//! HA commit policy and completion, separate from local mutation execution.
//! The caller commits under its apply fence, releases that fence before waits,
//! and uses this owner to recheck authority before acknowledging the client.
const std = @import("std");
const builtin = @import("builtin");
const ha_contract = @import("../db/ha_contract.zig");
const HAAsyncEffectMirror = ha_contract.AsyncEffectMirror;
const HAWriteGate = ha_contract.WriteGate;
const ha_commit_gate_mod = @import("commit_gate.zig");
const ha_primary_mod = @import("primary.zig");
const ha_effects_mod = @import("effects.zig");
const durable_outbox = @import("../db/durable_outbox.zig");
const Namespace = @import("../db/doc_identity_namespace.zig").Namespace;
const types = @import("../db/types.zig");
const schema_mod = @import("../schema.zig");

pub const HADeferredCommitGate = struct {
    mirror: HAAsyncEffectMirror,
    lsn: u64,
};

pub const HADeferredCommitGates = struct {
    transition_mutex: ?*std.atomic.Mutex = null,
    transition_locked: bool = false,
    gates: [2]HADeferredCommitGate = undefined,
    gate_count: usize = 0,

    pub fn begin(transition_mutex: ?*std.atomic.Mutex) @This() {
        if (transition_mutex) |mutex| lockAtomic(mutex);
        return .{
            .transition_mutex = transition_mutex,
            .transition_locked = transition_mutex != null,
        };
    }

    pub fn append(self: *@This(), gate: ?HADeferredCommitGate) void {
        const item = gate orelse return;
        std.debug.assert(self.gate_count < self.gates.len);
        self.gates[self.gate_count] = item;
        self.gate_count += 1;
    }

    pub fn releaseTransition(self: *@This()) void {
        if (!self.transition_locked) return;
        self.transition_mutex.?.unlock();
        self.transition_locked = false;
    }

    pub fn waitForDurabilityAndAuthority(self: *@This(), write_gate: ?HAWriteGate) !void {
        // The HA records are already durable and ordered with the local commit.
        // Remote acknowledgement must not retain the DB apply lock or the
        // transition mutex: status updates and safe reads need both paths to
        // remain live while a synchronous policy is pending.
        self.releaseTransition();
        for (self.gates[0..self.gate_count]) |gate| {
            try evaluateHAMirrorCommitGate(gate.mirror, gate.lsn);
        }

        // Serialize the final success decision with fencing after every remote
        // durability condition has passed. Error/pending outcomes never claim
        // client acknowledgement and therefore need no success recheck.
        if (self.transition_mutex) |mutex| {
            lockAtomic(mutex);
            self.transition_locked = true;
        }
        defer self.releaseTransition();
        try enforceHAWriteGateOptional(write_gate);
    }
};

pub fn evaluateHAMirrorCommitGate(mirror: HAAsyncEffectMirror, lsn: u64) !void {
    if (mirror.sync_policy.mode == .async) return;
    var gate = try ha_commit_gate_mod.evaluate(mirror.primary, lsn, mirror.sync_policy);
    recordHAMirrorGate(mirror, gate);
    switch (gate.action) {
        .acknowledge => return,
        .acknowledge_degraded => return,
        .reject => return error.SyncPolicyUnsatisfied,
        .wait_for_standby => {
            const wait_fn = mirror.sync_wait_fn orelse return error.HASyncCommitWouldBlock;
            const wait_ctx = mirror.sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext;
            try wait_fn(wait_ctx, mirror.primary, lsn, mirror.sync_policy);
            gate = try ha_commit_gate_mod.evaluate(mirror.primary, lsn, mirror.sync_policy);
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

pub fn haMirrorSyncEnabled(mirror: HAAsyncEffectMirror) bool {
    return mirror.sync_policy.mode != .async;
}

pub fn haMirrorRequiresDurableOutbox(mirror: HAAsyncEffectMirror) bool {
    return haMirrorSyncEnabled(mirror) and mirror.sync_policy.failure_policy != .degrade_to_async;
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

pub fn noteHAMirrorFailure(mirror: HAAsyncEffectMirror, comptime label: []const u8, err: anyerror) void {
    if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
    std.log.warn("failed to mirror DB " ++ label ++ " into HA stream: {s}", .{@errorName(err)});
}

pub fn enforceHAWriteGateOptional(gate: ?HAWriteGate) !void {
    const configured = gate orelse return;
    try configured.check();
}

/// Check fail-closed availability before committing locally. The borrowed log
/// lock serializes this decision with append and the next-LSN observation.
pub fn preflight(mirror: ?HAAsyncEffectMirror, log_mutex: *std.atomic.Mutex) !void {
    const configured = mirror orelse return;
    if (configured.sync_policy.mode == .async) return;
    if (configured.sync_policy.failure_policy != .fail_closed) return;
    lockAtomic(log_mutex);
    defer log_mutex.unlock();
    const target_lsn = configured.primary.nextLsn();
    const decision = try configured.primary.evaluateAppendDurability(target_lsn, configured.sync_policy);
    const gate = haCommitGateResultFromDecision(target_lsn, decision);
    if (gate.action == .reject) {
        recordHAMirrorGate(configured, gate);
        return error.SyncPolicyUnsatisfied;
    }
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        if (builtin.os.tag == .freestanding) {
            std.atomic.spinLoopHint();
        } else {
            @import("antfly_platform").time.yieldNow();
        }
    }
}

/// Borrowed for one recovery operation. This owner cannot access DB internals
/// or delete pending records; the storage owner clears them after success.
pub const RecoveryContext = struct {
    transition_mutex: ?*std.atomic.Mutex,
    log_mutex: *std.atomic.Mutex,
    namespace: Namespace,
    write_gate: ?HAWriteGate,
};

/// Match an already published record before appending, fail closed when its
/// retention fence is gone, and complete acknowledgement without a DB lock.
pub fn recoverDurableOutbox(
    context: RecoveryContext,
    mirror: HAAsyncEffectMirror,
    outbox: durable_outbox.DurableHAOutbox,
    kind: durable_outbox.Kind,
) !void {
    var deferred = HADeferredCommitGates.begin(context.transition_mutex);
    defer deferred.releaseTransition();

    const lsn = blk: {
        lockAtomic(context.log_mutex);
        defer context.log_mutex.*.unlock();

        if (try mirror.primary.findMatchingRecordFrom(
            outbox.from_lsn,
            switch (kind) {
                .batch, .restore_batch => .batch_mutation,
                .replay, .primary_effect => .derived_effect,
                .schema, .row_policy => .metadata_mutation,
            },
            outbox.payload,
            context.namespace.shard_id,
            context.namespace.table_id,
        )) |existing_lsn| break :blk existing_lsn;

        break :blk switch (kind) {
            .batch, .restore_batch => ha_effects_mod.appendEncodedBatchMutationRequest(mirror.primary, outbox.payload, .{
                .shard_id = context.namespace.shard_id,
                .table_id = context.namespace.table_id,
            }),
            .replay, .primary_effect => ha_effects_mod.appendEncodedDerivedChangeRecord(mirror.primary, outbox.payload, .{
                .shard_id = context.namespace.shard_id,
                .table_id = context.namespace.table_id,
            }),
            .schema, .row_policy => ha_effects_mod.appendEncodedSchemaMetadataMutation(mirror.primary, outbox.payload, .{
                .shard_id = context.namespace.shard_id,
                .table_id = context.namespace.table_id,
            }),
        } catch |err| {
            switch (kind) {
                .batch, .restore_batch => noteHAMirrorFailure(mirror, "batch mutation recovery", err),
                .replay, .primary_effect => noteHAMirrorFailure(mirror, "derived effect recovery", err),
                .schema, .row_policy => noteHAMirrorFailure(mirror, "metadata mutation recovery", err),
            }
            return err;
        };
    };
    if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
    deferred.append(.{ .mirror = mirror, .lsn = lsn });
    try deferred.waitForDurabilityAndAuthority(context.write_gate);
}

/// Borrowed controls for one publication. Store access and mutation execution
/// are intentionally unavailable here. The caller still owns commit ordering.
pub const CommitContext = struct {
    alloc: std.mem.Allocator,
    identity_namespace: Namespace,
    transition_mutex: ?*std.atomic.Mutex,
    log_mutex: *std.atomic.Mutex,
    ha_write_gate: ?HAWriteGate,
    ha_async_effect_mirror: ?HAAsyncEffectMirror,
    ha_async_batch_mirror: ?HAAsyncEffectMirror,
    ha_async_metadata_mirror: ?HAAsyncEffectMirror,
    append_pending: ?*const std.atomic.Value(bool),
};

fn checkWrite(ctx: *const CommitContext) !void {
    try enforceHAWriteGateOptional(ctx.ha_write_gate);
    if (ctx.append_pending) |pending| if (pending.load(.acquire)) return error.HAMirrorUnavailable;
}

pub fn mirrorHAReplayPayloadBestEffortContext(ctx: *const CommitContext, payload: []const u8) void {
    const mirror = ctx.ha_async_effect_mirror orelse return;
    const transition_mutex = mirror.transition_mutex;
    if (transition_mutex) |mutex| lockAtomic(mutex);
    defer if (transition_mutex) |mutex| mutex.unlock();
    checkWrite(ctx) catch return;
    lockAtomic(ctx.log_mutex);
    defer ctx.log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendEncodedDerivedChangeRecord(mirror.primary, payload, .{
        .shard_id = ctx.identity_namespace.shard_id,
        .table_id = ctx.identity_namespace.table_id,
    }) catch |err| {
        if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB derived effect into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorHAReplayPayloadCommitContext(ctx: *const CommitContext, payload: []const u8) !void {
    var deferred = HADeferredCommitGates.begin(ctx.transition_mutex);
    defer deferred.releaseTransition();
    deferred.append(try appendHAReplayPayloadCommitLockedContext(ctx, payload));
    try deferred.waitForDurabilityAndAuthority(ctx.ha_write_gate);
}

pub fn appendHAReplayPayloadCommitLockedContext(ctx: *const CommitContext, payload: []const u8) !?HADeferredCommitGate {
    const mirror = ctx.ha_async_effect_mirror orelse return null;
    // The local store has already committed. Always represent that mutation in
    // the HA tail; a fence that arrived after the preflight gate may reject the
    // client acknowledgement below, but must not create an unlogged local fork.
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendEncodedDerivedChangeRecord(mirror.primary, payload, .{
            .shard_id = ctx.identity_namespace.shard_id,
            .table_id = ctx.identity_namespace.table_id,
        }) catch |err| {
            noteHAMirrorFailure(mirror, "derived effect", err);
            if (haMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn mirrorHABatchMutationBestEffortContext(ctx: *const CommitContext, request: types.BatchRequest) void {
    const mirror = ctx.ha_async_batch_mirror orelse return;
    const transition_mutex = mirror.transition_mutex;
    if (transition_mutex) |mutex| lockAtomic(mutex);
    defer if (transition_mutex) |mutex| mutex.unlock();
    checkWrite(ctx) catch return;
    lockAtomic(ctx.log_mutex);
    defer ctx.log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendBatchMutationRequest(ctx.alloc, mirror.primary, request, .{
        .shard_id = ctx.identity_namespace.shard_id,
        .table_id = ctx.identity_namespace.table_id,
    }) catch |err| {
        if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB batch mutation into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorHABatchMutationCommitContext(ctx: *const CommitContext, request: types.BatchRequest) !void {
    var deferred = HADeferredCommitGates.begin(ctx.transition_mutex);
    defer deferred.releaseTransition();
    deferred.append(try appendHABatchMutationCommitLockedContext(ctx, request));
    try deferred.waitForDurabilityAndAuthority(ctx.ha_write_gate);
}

pub fn appendHABatchMutationCommitLockedContext(ctx: *const CommitContext, request: types.BatchRequest) !?HADeferredCommitGate {
    const mirror = ctx.ha_async_batch_mirror orelse return null;
    // The local store has already committed. Always append before applying the
    // final authority check so rejoin cannot mistake local divergence for an
    // exact fork boundary.
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendBatchMutationRequest(ctx.alloc, mirror.primary, request, .{
            .shard_id = ctx.identity_namespace.shard_id,
            .table_id = ctx.identity_namespace.table_id,
        }) catch |err| {
            noteHAMirrorFailure(mirror, "batch mutation", err);
            if (haMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn mirrorHAEncodedBatchMutationCommitContext(ctx: *const CommitContext, payload: []const u8) !void {
    var deferred = HADeferredCommitGates.begin(ctx.transition_mutex);
    defer deferred.releaseTransition();
    deferred.append(try appendHAEncodedBatchMutationCommitLockedContext(ctx, payload));
    try deferred.waitForDurabilityAndAuthority(ctx.ha_write_gate);
}

pub fn appendHAEncodedBatchMutationCommitLockedContext(ctx: *const CommitContext, payload: []const u8) !?HADeferredCommitGate {
    return appendHAEncodedBatchMutationCommitLockedContextStrict(ctx, payload, false);
}

pub fn appendHAEncodedBatchMutationCommitLockedContextStrict(ctx: *const CommitContext, payload: []const u8, strict_append: bool) !?HADeferredCommitGate {
    const mirror = ctx.ha_async_batch_mirror orelse return null;
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendEncodedBatchMutationRequest(mirror.primary, payload, .{
            .shard_id = ctx.identity_namespace.shard_id,
            .table_id = ctx.identity_namespace.table_id,
        }) catch |err| {
            noteHAMirrorFailure(mirror, "batch mutation", err);
            if (strict_append or haMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn mirrorHASchemaMetadataBestEffortContext(
    ctx: *const CommitContext,
    table_schema: schema_mod.TableSchema,
    public_schema_json: ?[]const u8,
) void {
    const mirror = ctx.ha_async_metadata_mirror orelse return;
    const transition_mutex = mirror.transition_mutex;
    if (transition_mutex) |mutex| lockAtomic(mutex);
    defer if (transition_mutex) |mutex| mutex.unlock();
    checkWrite(ctx) catch return;
    lockAtomic(ctx.log_mutex);
    defer ctx.log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendSchemaMetadataMutation(ctx.alloc, mirror.primary, table_schema, public_schema_json, .{
        .shard_id = ctx.identity_namespace.shard_id,
        .table_id = ctx.identity_namespace.table_id,
    }) catch |err| {
        if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB schema metadata into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn appendHASchemaMetadataCommitLockedContext(
    ctx: *const CommitContext,
    table_schema: schema_mod.TableSchema,
    public_schema_json: ?[]const u8,
) !?HADeferredCommitGate {
    const mirror = ctx.ha_async_metadata_mirror orelse return null;
    // As with document batches, committed metadata must remain represented in
    // the HA tail even when authority expires before acknowledgement.
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendSchemaMetadataMutation(ctx.alloc, mirror.primary, table_schema, public_schema_json, .{
            .shard_id = ctx.identity_namespace.shard_id,
            .table_id = ctx.identity_namespace.table_id,
        }) catch |err| {
            noteHAMirrorFailure(mirror, "metadata mutation", err);
            if (haMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn appendHAEncodedSchemaMetadataCommitLockedContext(
    ctx: *const CommitContext,
    payload: []const u8,
) !?HADeferredCommitGate {
    const mirror = ctx.ha_async_metadata_mirror orelse return null;
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendEncodedSchemaMetadataMutation(mirror.primary, payload, .{
            .shard_id = ctx.identity_namespace.shard_id,
            .table_id = ctx.identity_namespace.table_id,
        }) catch |err| {
            noteHAMirrorFailure(mirror, "metadata mutation", err);
            if (haMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}
