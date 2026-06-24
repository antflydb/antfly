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

const std = @import("std");
const platform = @import("antfly_platform");

const ha_commit_gate_mod = @import("../ha/commit_gate.zig");
const ha_effects_mod = @import("../ha/effects.zig");
const ha_primary_mod = @import("../ha/primary.zig");
const ha_session_mod = @import("../ha/session.zig");
const ha_standby_mod = @import("../ha/standby.zig");
const ha_write_gate_mod = @import("../ha/write_gate.zig");
const docstore_mod = @import("../docstore.zig");
const internal_keys = @import("../internal_keys.zig");
const ha_replication_record_mod = @import("../ha/replication_record.zig");
const schema_mod = @import("../schema.zig");
const types = @import("types.zig");

const Allocator = std.mem.Allocator;

pub const AsyncEffectMirror = struct {
    primary: *ha_primary_mod.Primary,
    last_lsn: ?*std.atomic.Value(u64) = null,
    failure_count: ?*std.atomic.Value(u64) = null,
    sync_policy: ha_primary_mod.SyncPolicy = .{},
    sync_wait_ctx: ?*anyopaque = null,
    sync_wait_fn: ?SyncWaitFn = null,
    last_gate_lsn: ?*std.atomic.Value(u64) = null,
    last_gate_action: ?*std.atomic.Value(u8) = null,
    sync_reject_count: ?*std.atomic.Value(u64) = null,
    sync_wait_count: ?*std.atomic.Value(u64) = null,
    sync_degraded_count: ?*std.atomic.Value(u64) = null,
};

pub const AsyncBatchMirror = AsyncEffectMirror;
pub const AsyncMetadataMirror = AsyncEffectMirror;

pub const SyncWaitFn = *const fn (
    ctx: *anyopaque,
    primary: *ha_primary_mod.Primary,
    target_lsn: u64,
    policy: ha_primary_mod.SyncPolicy,
) anyerror!void;

pub const ProgressPollFn = *const fn (
    ctx: *anyopaque,
    primary: *ha_primary_mod.Primary,
    target_lsn: u64,
    policy: ha_primary_mod.SyncPolicy,
    round: usize,
) anyerror!void;

pub const PrimaryProgressSyncWait = struct {
    max_rounds: usize = 64,
    sleep_ns: u64 = 0,
    poll_ctx: ?*anyopaque = null,
    poll_fn: ?ProgressPollFn = null,

    pub fn wait(ctx: *anyopaque, primary: *ha_primary_mod.Primary, target_lsn: u64, policy: ha_primary_mod.SyncPolicy) !void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (policy.mode == .async) return;
        if (self.max_rounds == 0) return error.HASyncCommitWaitLimitExceeded;

        var round: usize = 0;
        while (round < self.max_rounds) : (round += 1) {
            if (self.poll_fn) |poll| {
                const poll_ctx = self.poll_ctx orelse return error.HASyncCommitWaitMissingContext;
                try poll(poll_ctx, primary, target_lsn, policy, round);
            }

            const gate = try ha_commit_gate_mod.evaluate(primary, target_lsn, policy);
            if (gate.shouldAcknowledge()) return;
            if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
            if (self.sleep_ns > 0) platform.time.sleepNs(self.sleep_ns);
        }

        return error.HASyncCommitWouldBlock;
    }
};

pub const SessionSyncWait = struct {
    alloc: Allocator,
    slot_name: []const u8,
    standby: *ha_standby_mod.Standby,
    apply_ctx: *anyopaque,
    apply_fn: ha_standby_mod.ApplyFn,
    max_rounds: usize = 8,

    pub fn wait(ctx: *anyopaque, primary: *ha_primary_mod.Primary, target_lsn: u64, policy: ha_primary_mod.SyncPolicy) !void {
        const self: *@This() = @ptrCast(@alignCast(ctx));
        if (policy.mode == .async) return;
        if (!syncPolicyIncludesStandby(policy, self.slot_name)) return error.HASyncCommitWaitStandbyNotInPolicy;
        if (self.max_rounds == 0) return error.HASyncCommitWaitLimitExceeded;

        var progress = self.standby.currentProgress();
        var round: usize = 0;
        while (round < self.max_rounds) : (round += 1) {
            const result = ha_session_mod.replicateAvailable(
                self.alloc,
                primary,
                self.slot_name,
                self.standby,
                self.apply_ctx,
                self.apply_fn,
            ) catch |err| {
                if (policy.mode == .remote_write) {
                    const gate = ha_commit_gate_mod.evaluate(primary, target_lsn, policy) catch return err;
                    if (gate.shouldAcknowledge()) return;
                }
                return err;
            };

            const gate = try ha_commit_gate_mod.evaluate(primary, target_lsn, policy);
            if (gate.shouldAcknowledge()) return;
            if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
            if (result.received_count == 0 and result.applied_count == 0) break;

            const next_progress = self.standby.currentProgress();
            if (next_progress.received_lsn == progress.received_lsn and
                next_progress.applied_lsn == progress.applied_lsn and
                next_progress.safe_read_lsn == progress.safe_read_lsn)
            {
                break;
            }
            progress = next_progress;
        }

        return error.HASyncCommitWouldBlock;
    }
};

pub const WriteGate = union(enum) {
    primary: *ha_primary_mod.Primary,
    fenced_primary: ha_write_gate_mod.FencedPrimary,
    standby: *ha_standby_mod.Standby,
};

pub const applied_lsn_value_len: usize = @sizeOf(u64);

pub fn appliedReplicationLsnWrite(lsn: u64, value_buf: *[applied_lsn_value_len]u8) docstore_mod.KVPair {
    std.mem.writeInt(u64, value_buf, lsn, .little);
    return .{
        .key = internal_keys.ha_applied_lsn_key[0..],
        .value = value_buf[0..],
    };
}

pub fn readAppliedReplicationLsn(alloc: Allocator, store: *docstore_mod.DocStore) !u64 {
    const raw = store.get(alloc, internal_keys.ha_applied_lsn_key[0..]) catch |err| switch (err) {
        error.NotFound => return 0,
        else => return err,
    };
    defer alloc.free(raw);
    if (raw.len != applied_lsn_value_len) return error.CorruptHAAppliedReplicationLsn;
    return std.mem.readInt(u64, raw[0..applied_lsn_value_len], .little);
}

pub fn Impl(comptime DB: type) type {
    return struct {
        pub fn enforceDBWriteGate(self: *DB) !void {
            try enforceWriteGateOptional(self.ha_write_gate);
        }

        pub fn mirrorDBReplayPayloadBestEffort(self: *DB, payload: []const u8) void {
            const resources = self.core.batchExecutionResources();
            mirrorReplayPayloadBestEffort(resources.log_mutex, self.ha_async_effect_mirror, payload);
        }

        pub fn preflightDBBatchSyncCommit(self: *DB) !void {
            const resources = self.core.batchExecutionResources();
            try preflightMirrorSyncCommit(resources.log_mutex, self.ha_async_batch_mirror);
            try preflightMirrorSyncCommit(resources.log_mutex, self.ha_async_effect_mirror);
        }

        pub fn mirrorDBBatchMutationCommit(self: *DB, request: types.BatchRequest) !void {
            const resources = self.core.batchExecutionResources();
            try mirrorBatchMutationCommit(self.alloc, resources.log_mutex, self.ha_async_batch_mirror, request);
        }

        pub fn mirrorDBReplayPayloadCommit(self: *DB, payload: []const u8) !void {
            const resources = self.core.batchExecutionResources();
            try mirrorReplayPayloadCommit(resources.log_mutex, self.ha_async_effect_mirror, payload);
        }

        pub fn appliedReplicationLsn(self: *DB) anyerror!u64 {
            return try readAppliedReplicationLsn(self.alloc, self.core.store);
        }

        pub fn replicationRecordAlreadyApplied(self: *DB, record: ha_replication_record_mod.RecordView) !bool {
            if (record.lsn == 0) return false;
            return (try Self.appliedReplicationLsn(self)) >= record.lsn;
        }

        pub fn markReplicationRecordApplied(self: *DB, lsn: u64) !void {
            if (lsn == 0) return;
            const current = try Self.appliedReplicationLsn(self);
            if (current >= lsn) return;
            var value_buf: [applied_lsn_value_len]u8 = undefined;
            const marker = appliedReplicationLsnWrite(lsn, &value_buf);
            try self.core.store.putBatch(&.{marker}, &.{});
        }

        pub fn setSchemaReplicatedApplyWithMarker(self: *DB, table_schema: schema_mod.TableSchema, applied_lsn_marker: ?u64) anyerror!void {
            try self.core.setSchema(table_schema);
            if (applied_lsn_marker) |lsn| try Self.markReplicationRecordApplied(self, lsn);
        }

        pub fn applyReplicationRecord(self: *DB, record: ha_replication_record_mod.RecordView) anyerror!void {
            if (try Self.replicationRecordAlreadyApplied(self, record)) return;

            switch (record.kind) {
                .batch_mutation => {
                    var decoded = try ha_effects_mod.decodeBatchMutationRequest(self.alloc, record);
                    defer decoded.deinit();
                    try DB.HAReplicationCallbacks.batch_replicated_apply_with_marker(self, decoded.value.request, record.lsn);
                },
                .metadata_mutation => {
                    const decoded_schema = try ha_effects_mod.decodeSchemaMetadataMutation(self.alloc, record);
                    defer schema_mod.freeSchema(self.alloc, decoded_schema);
                    try Self.setSchemaReplicatedApplyWithMarker(self, decoded_schema, record.lsn);
                },
                .derived_effect => {
                    _ = try DB.HAReplicationCallbacks.apply_ha_derived_effect_record(self, record);
                    try Self.markReplicationRecordApplied(self, record.lsn);
                },
                .backup_start,
                .backup_end,
                .checkpoint,
                .manifest,
                .truncate,
                .timeline_switch,
                => try Self.markReplicationRecordApplied(self, record.lsn),
                _ => return error.HAReplicationRecordApplyUnsupported,
            }
        }

        pub fn applyReplicationRecordCallback(ctx: *anyopaque, record: ha_replication_record_mod.RecordView) anyerror!void {
            const self: *DB = @ptrCast(@alignCast(ctx));
            try Self.applyReplicationRecord(self, record);
        }

        const Self = @This();
    };
}

pub fn writeGateIsStandby(gate: ?WriteGate) bool {
    const configured = gate orelse return false;
    return switch (configured) {
        .primary => false,
        .fenced_primary => false,
        .standby => true,
    };
}

pub fn enforceWriteGateOptional(gate: ?WriteGate) !void {
    const configured = gate orelse return;
    const decision = switch (configured) {
        .primary => |primary| try ha_write_gate_mod.evaluatePrimary(primary, .{}),
        .fenced_primary => |fenced| try ha_write_gate_mod.evaluateFencedPrimary(fenced, .{}),
        .standby => |standby| try ha_write_gate_mod.evaluateStandby(standby, .{}),
    };
    switch (decision.action) {
        .allow_write => return,
        .reject_read_only_standby => return error.HAReadOnlyStandby,
        .open_promoted_primary => return error.HAPromotedStandbyRequiresPrimaryOpen,
        .reject_fenced_primary => return error.HAFencedPrimary,
    }
}

pub fn preflightMirrorSyncCommit(log_mutex: *std.atomic.Mutex, mirror: ?AsyncEffectMirror) !void {
    const configured = mirror orelse return;
    if (configured.sync_policy.mode == .async) return;
    if (configured.sync_policy.failure_policy != .fail_closed) return;

    platform.sync.lockYielding(log_mutex);
    defer log_mutex.*.unlock();

    const target_lsn = configured.primary.nextLsn();
    const decision = try configured.primary.evaluateAppendDurability(target_lsn, configured.sync_policy);
    const gate = commitGateResultFromDecision(target_lsn, decision);
    if (gate.action == .reject) {
        recordMirrorGate(configured, gate);
        return error.SyncPolicyUnsatisfied;
    }
}

pub fn mirrorReplayPayloadBestEffort(log_mutex: *std.atomic.Mutex, mirror: ?AsyncEffectMirror, payload: []const u8) void {
    const configured = mirror orelse return;
    platform.sync.lockYielding(log_mutex);
    defer log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendEncodedDerivedChangeRecord(configured.primary, payload, .{}) catch |err| {
        if (configured.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB derived effect into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorReplayPayloadCommit(log_mutex: *std.atomic.Mutex, mirror: ?AsyncEffectMirror, payload: []const u8) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        platform.sync.lockYielding(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendEncodedDerivedChangeRecord(configured.primary, payload, .{}) catch |err| {
            noteMirrorFailure(configured, "derived effect", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

pub fn mirrorBatchMutationBestEffort(alloc: Allocator, log_mutex: *std.atomic.Mutex, mirror: ?AsyncBatchMirror, request: types.BatchRequest) void {
    const configured = mirror orelse return;
    platform.sync.lockYielding(log_mutex);
    defer log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendBatchMutationRequest(alloc, configured.primary, request, .{}) catch |err| {
        if (configured.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB batch mutation into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorBatchMutationCommit(alloc: Allocator, log_mutex: *std.atomic.Mutex, mirror: ?AsyncBatchMirror, request: types.BatchRequest) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        platform.sync.lockYielding(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendBatchMutationRequest(alloc, configured.primary, request, .{}) catch |err| {
            noteMirrorFailure(configured, "batch mutation", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

pub fn mirrorSchemaMetadataBestEffort(alloc: Allocator, log_mutex: *std.atomic.Mutex, mirror: ?AsyncMetadataMirror, table_schema: schema_mod.TableSchema) void {
    const configured = mirror orelse return;
    platform.sync.lockYielding(log_mutex);
    defer log_mutex.*.unlock();
    const lsn = ha_effects_mod.appendSchemaMetadataMutation(alloc, configured.primary, table_schema, .{}) catch |err| {
        if (configured.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB schema metadata into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorSchemaMetadataCommit(alloc: Allocator, log_mutex: *std.atomic.Mutex, mirror: ?AsyncMetadataMirror, table_schema: schema_mod.TableSchema) !void {
    const configured = mirror orelse return;
    const lsn = blk: {
        platform.sync.lockYielding(log_mutex);
        defer log_mutex.*.unlock();
        const lsn = ha_effects_mod.appendSchemaMetadataMutation(alloc, configured.primary, table_schema, .{}) catch |err| {
            noteMirrorFailure(configured, "metadata mutation", err);
            if (mirrorSyncEnabled(configured)) return err;
            return;
        };
        if (configured.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    try evaluateMirrorCommitGate(configured, lsn);
}

fn syncPolicyIncludesStandby(policy: ha_primary_mod.SyncPolicy, slot_name: []const u8) bool {
    for (policy.standby_names) |name| {
        if (std.mem.eql(u8, name, slot_name)) return true;
    }
    return false;
}

fn evaluateMirrorCommitGate(mirror: AsyncEffectMirror, lsn: u64) !void {
    if (mirror.sync_policy.mode == .async) return;
    var gate = try ha_commit_gate_mod.evaluate(mirror.primary, lsn, mirror.sync_policy);
    recordMirrorGate(mirror, gate);
    switch (gate.action) {
        .acknowledge => return,
        .acknowledge_degraded => return,
        .reject => return error.SyncPolicyUnsatisfied,
        .wait_for_standby => {
            const wait_fn = mirror.sync_wait_fn orelse return error.HASyncCommitWouldBlock;
            const wait_ctx = mirror.sync_wait_ctx orelse return error.HASyncCommitWaitMissingContext;
            try wait_fn(wait_ctx, mirror.primary, lsn, mirror.sync_policy);
            gate = try ha_commit_gate_mod.evaluate(mirror.primary, lsn, mirror.sync_policy);
            recordMirrorGate(mirror, gate);
            switch (gate.action) {
                .acknowledge => return,
                .acknowledge_degraded => return,
                .reject => return error.SyncPolicyUnsatisfied,
                .wait_for_standby => return error.HASyncCommitWouldBlock,
            }
        },
    }
}

fn mirrorSyncEnabled(mirror: AsyncEffectMirror) bool {
    return mirror.sync_policy.mode != .async;
}

fn recordMirrorGate(mirror: AsyncEffectMirror, gate: ha_commit_gate_mod.GateResult) void {
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

fn commitGateResultFromDecision(target_lsn: u64, decision: ha_primary_mod.DurabilityDecision) ha_commit_gate_mod.GateResult {
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

fn noteMirrorFailure(mirror: AsyncEffectMirror, comptime label: []const u8, err: anyerror) void {
    if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
    std.log.warn("failed to mirror DB " ++ label ++ " into HA stream: {s}", .{@errorName(err)});
}
