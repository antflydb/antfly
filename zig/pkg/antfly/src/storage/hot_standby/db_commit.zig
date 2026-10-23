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
const replication_contract = @import("antfly_local_sources").storage_db_replication_contract;
const ReplicationAsyncEffectMirror = replication_contract.AsyncEffectMirror;
const hot_standby_primary_mod = @import("primary.zig");
const hot_standby_commit_gate_mod = @import("commit_gate.zig");
const replication_effects_mod = @import("effects.zig");
const outbox = @import("antfly_local_sources").storage_db_durable_outbox;
const Namespace = @import("antfly_local_sources").storage_db_doc_identity_namespace.Namespace;

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
    inline for (@typeInfo(Options).@"struct".field_names) |field_name| {
        if (comptime std.mem.eql(u8, field_name, "sync_policy")) {
            const x = left.sync_policy;
            const y = right.sync_policy;
            if (x.mode != y.mode or x.selection != y.selection or x.required != y.required or
                x.failure_policy != y.failure_policy or x.standby_names.len != y.standby_names.len) return false;
            for (x.standby_names, y.standby_names) |xn, yn| {
                if (!std.mem.eql(u8, xn, yn)) return false;
            }
        } else {
            if (@field(left, field_name) != @field(right, field_name)) return false;
        }
    }
    return true;
}

fn bind(primary: *hot_standby_primary_mod.Primary) replication_contract.Publisher {
    return .{ .ptr = primary, .vtable = &publisher_vtable, .poll_completion = pollReplicationMirrorCommitGate };
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
    .prepare = prepare,
    .recover = recover,
    .recover_with_admission = recoverWithAdmission,
    .preflight = preflight,
    .preflight_with_admission = preflightWithAdmission,
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

const PreparedState = struct {
    allocator: std.mem.Allocator,
    append: hot_standby_primary_mod.Primary.PreparedAppend,
    fn publish(raw: *anyopaque) !u64 {
        const self: *@This() = @ptrCast(@alignCast(raw));
        return self.append.publish();
    }
    fn deinit(raw: *anyopaque) void {
        const self: *@This() = @ptrCast(@alignCast(raw));
        const allocator = self.allocator;
        self.append.deinit();
        allocator.destroy(self);
    }
};

fn prepare(mirror: ReplicationAsyncEffectMirror, kind: outbox.Kind, payload: []const u8, namespace: Namespace, control: ?@import("antfly_local_sources").common_read_admission.Port) !replication_contract.PreparedPublication {
    const primary = try runtimePrimary(mirror);
    if (kind == .replay or kind == .primary_effect) {
        if (!@import("antfly_local_sources").storage_db_derived_change_journal.looksLikeBinaryRecord(payload) and
            !@import("antfly_local_sources").storage_db_primary_effect.isPrimaryEffect(payload)) return error.UnsupportedDerivedEffectPayload;
    }
    const state = try primary.alloc.create(PreparedState);
    errdefer primary.alloc.destroy(state);
    state.* = .{ .allocator = primary.alloc, .append = try primary.prepareAppend(.{
        .kind = switch (kind) {
            .batch, .restore_batch => .batch_mutation,
            .replay, .primary_effect => .derived_effect,
            .schema, .row_policy => .metadata_mutation,
        },
        .payload_codec = if (kind == .replay or kind == .primary_effect) .binary else .json,
        .shard_id = namespace.shard_id,
        .table_id = namespace.table_id,
        .payload = payload,
    }, control) };
    return .{ .ptr = state, .vtable = &.{ .publish = PreparedState.publish, .deinit = PreparedState.deinit } };
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

fn recoverWithAdmission(mirror: ReplicationAsyncEffectMirror, kind: outbox.Kind, pending: outbox.DurableReplicationOutbox, namespace: Namespace, control: @import("antfly_local_sources").common_read_admission.Port) !u64 {
    const primary = try runtimePrimary(mirror);
    var retained: @import("antfly_local_sources").common_read_admission.RetainedDependency = .{};
    const lifetime = retained.bind(control).?;
    try lifetime.checkpoint();
    const record_kind: @import("antfly_local_sources").storage_db_replication_record.RecordKind = switch (kind) {
        .batch, .restore_batch => .batch_mutation,
        .replay, .primary_effect => .derived_effect,
        .schema, .row_policy => .metadata_mutation,
    };
    const scanning = if (pending.recovery_identity) |intent_identity|
        primary.findMatchingRecordForRecoveryStep(pending.recovery_cursor orelse return error.ExecutionWaitUnsupported, intent_identity, pending.from_lsn, record_kind, pending.payload, namespace.shard_id, namespace.table_id, lifetime)
    else
        primary.findMatchingRecordForRecovery(pending.from_lsn, record_kind, pending.payload, namespace.shard_id, namespace.table_id, lifetime);
    const match = scanning catch |err| {
        // Backend contention while retaining publication fences cannot yield.
        // Preserve an original caller rejection, but retry internal contention
        // without counting it as a delivery failure.
        if (err == error.Backpressured and retained.rejection == null) return error.CommitCompletionPending;
        return err;
    };
    const position = switch (match) {
        .found => |lsn| return lsn,
        .busy, .pending => return error.CommitCompletionPending,
        .absent => |value| value,
    };
    const result = try switch (kind) {
        .batch, .restore_batch => replication_effects_mod.appendEncodedBatchMutationRequestIfPosition(primary, position, pending.payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }, control),
        .replay, .primary_effect => replication_effects_mod.appendEncodedDerivedChangeRecordIfPosition(primary, position, pending.payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }, control),
        .schema, .row_policy => replication_effects_mod.appendEncodedSchemaMetadataMutationIfPosition(primary, position, pending.payload, .{ .shard_id = namespace.shard_id, .table_id = namespace.table_id }, control),
    };
    // Once the conditional append succeeds, completion is mandatory. Busy or
    // changed means retry the retained obligation; it never authorizes cleanup.
    return switch (result) {
        .appended => |lsn| lsn,
        .busy, .changed => error.CommitCompletionPending,
    };
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

fn preflightWithAdmission(mirror: ReplicationAsyncEffectMirror, record_decision: bool, control: @import("antfly_local_sources").common_read_admission.Port) !void {
    try control.checkpoint();
    if (options(mirror).sync_policy.mode == .async or options(mirror).sync_policy.failure_policy != .fail_closed) return;
    const primary = try runtimePrimary(mirror);
    const target_lsn = try primary.nextLsnWithAdmission(control);
    const decision = try primary.evaluateAppendDurabilityWithAdmission(target_lsn, options(mirror).sync_policy, control);
    const gate = hotStandbyCommitGateResultFromDecision(target_lsn, decision);
    if (record_decision or gate.action == .reject) recordHotStandbyMirrorGate(mirror, gate);
    if (gate.action == .reject) return error.SyncPolicyUnsatisfied;
    try control.checkpoint();
}

/// Observe the same configured durability policy without entering its wait
/// hook. The storage owner retains the durable intent on a pending result and
/// performs its final authority check before any retirement/acknowledgement.
pub fn pollReplicationMirrorCommitGate(mirror: ReplicationAsyncEffectMirror, lsn: u64) !bool {
    if (options(mirror).sync_policy.mode == .async) return true;
    const primary = try runtimePrimary(mirror);
    const gate = try hot_standby_commit_gate_mod.evaluate(primary, lsn, options(mirror).sync_policy);
    recordHotStandbyMirrorGate(mirror, gate);
    return switch (gate.action) {
        .acknowledge, .acknowledge_degraded => true,
        .reject => error.SyncPolicyUnsatisfied,
        .wait_for_standby => false,
    };
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
    if (options(mirror).last_gate_action) |last_action| last_action.store(@backingInt(gate.action), .release);
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

test "controlled publisher preflight preserves native policy decisions and original scan rejection" {
    const read_control = @import("antfly_local_sources").common_read_admission;
    var primary: hot_standby_primary_mod.Primary = undefined;
    primary.log.wal.mutex = .init;
    primary.log.wal.sync_io = std.testing.io;
    primary.log.wal.next_lsn = 10;
    primary.identity.timeline_id = 1;
    primary.slots.slots = .empty;
    defer primary.slots.slots.deinit(std.testing.allocator);
    for (0..130) |_| try primary.slots.slots.append(std.testing.allocator, .{ .state = .{ .name = "unused", .timeline_id = 1, .restart_lsn = 1, .received_lsn = 12, .applied_lsn = 9, .safe_read_lsn = 9 } });
    for ([_][]const u8{ "a", "b", "c" }, 0..) |name, index| {
        try primary.slots.slots.append(std.testing.allocator, .{ .state = .{ .name = name, .timeline_id = 1, .restart_lsn = 1, .received_lsn = 12, .applied_lsn = 8 + @as(u64, @intCast(index)), .safe_read_lsn = 8, .active = index != 2 } });
    }
    const Authority = struct {
        checks: usize = 0,
        fail_at: usize = std.math.maxInt(usize),
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const scope: *@This() = @ptrCast(@alignCast(raw));
            scope.checks += 1;
            if (scope.checks == scope.fail_at) return scope.cause;
        }
        fn wait(_: *anyopaque) !void {
            return error.UnexpectedUncontendedWait;
        }
    };
    var authority: Authority = .{};
    const payload: read_control.Payload = .{ .ptr = &authority, .vtable = &.{ .checkpoint = Authority.check, .wait_for_progress = Authority.wait } };
    const control = read_control.Port.init(&payload);
    for ([_]hot_standby_primary_mod.DurabilityMode{ .async, .remote_write, .remote_apply }) |mode| {
        for ([_]hot_standby_primary_mod.StandbySelection{ .any, .first, .all }) |selection| {
            for ([_]hot_standby_primary_mod.FailurePolicy{ .block, .fail_closed, .degrade_to_async }) |failure_policy| {
                const policy: hot_standby_primary_mod.SyncPolicy = .{ .mode = mode, .selection = selection, .required = 2, .failure_policy = failure_policy, .standby_names = &.{ "a", "b", "c" } };
                authority = .{};
                const expected = try primary.evaluateAppendDurability(10, policy);
                try std.testing.expectEqualDeep(expected, try primary.evaluateAppendDurabilityWithAdmission(10, policy, control));
                const checks = authority.checks;
                for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
                    for (1..checks + 1) |fail_at| {
                        authority = .{ .cause = cause, .fail_at = fail_at };
                        try std.testing.expectError(cause, primary.evaluateAppendDurabilityWithAdmission(10, policy, control));
                        try std.testing.expect(primary.log.wal.mutex.tryLock());
                        primary.log.wal.mutex.unlock(std.testing.io);
                    }
                }
            }
        }
    }
    authority = .{};
    const accepted = bindMirror(&primary, .{ .sync_policy = .{ .mode = .remote_write, .required = 2, .failure_policy = .fail_closed, .standby_names = &.{ "a", "b" } } });
    try accepted.publisher.preflightWithAdmission(accepted, false, control);
    var local_log: std.atomic.Mutex = .unlocked;
    const integration = @import("antfly_local_sources").storage_db_commit_integration;
    try integration.preflightWithAdmission(accepted, &local_log, control);
    // A nested WAL wait retains CPU while the local log fence is held. The
    // original wait callback deliberately rejects if it is invoked here.
    try std.testing.expect(primary.log.wal.mutex.tryLock());
    authority = .{ .fail_at = 20, .cause = error.CatalogGenerationChanged };
    try std.testing.expectError(error.CatalogGenerationChanged, integration.preflightWithAdmission(accepted, &local_log, control));
    try std.testing.expect(!primary.log.wal.mutex.tryLock());
    primary.log.wal.mutex.unlock(std.testing.io);
    try std.testing.expect(local_log.tryLock());
    local_log.unlock();
    authority = .{};
    var unsupported = accepted;
    var legacy_vtable = accepted.publisher.vtable.*;
    legacy_vtable.preflight_with_admission = null;
    unsupported.publisher.vtable = &legacy_vtable;
    try std.testing.expectError(error.ExecutionWaitUnsupported, integration.preflightWithAdmission(unsupported, &local_log, control));
    try std.testing.expect(local_log.tryLock());
    local_log.unlock();
    const rejected = bindMirror(&primary, .{ .sync_policy = .{ .mode = .remote_apply, .required = 2, .failure_policy = .fail_closed, .standby_names = &.{ "a", "b" } } });
    try std.testing.expectError(error.SyncPolicyUnsatisfied, rejected.publisher.preflightWithAdmission(rejected, false, control));
    authority = .{ .fail_at = 1, .cause = error.CatalogGenerationChanged };
    try std.testing.expectError(error.CatalogGenerationChanged, accepted.publisher.preflightWithAdmission(accepted, false, control));
    try std.testing.expectEqual(@as(u64, 10), primary.log.wal.next_lsn);
    try std.testing.expectEqual(@as(usize, 133), primary.slots.slots.items.len);
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

test "publication completion poll uses the configured policy without calling its standby wait hook" {
    var primary: hot_standby_primary_mod.Primary = undefined;
    primary.log.wal.mutex = .init;
    primary.log.wal.sync_io = std.testing.io;
    primary.log.wal.next_lsn = 10;
    primary.identity.timeline_id = 1;
    primary.slots.slots = .empty;
    defer primary.slots.slots.deinit(std.testing.allocator);
    try primary.slots.slots.append(std.testing.allocator, .{ .state = .{ .name = "standby", .timeline_id = 1, .restart_lsn = 1, .received_lsn = 8, .applied_lsn = 8, .safe_read_lsn = 8, .active = true } });
    const Wait = struct {
        calls: usize = 0,
        fn run(ptr: *anyopaque, _: *anyopaque, _: u64, _: hot_standby_primary_mod.SyncPolicy) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return error.InjectedStandbyWait;
        }
    };
    var wait: Wait = .{};
    for ([_]hot_standby_primary_mod.DurabilityMode{ .remote_write, .remote_apply }) |mode| {
        for ([_]hot_standby_primary_mod.FailurePolicy{ .block, .fail_closed, .degrade_to_async }) |policy| {
            const mirror = bindMirror(&primary, .{ .sync_policy = .{ .mode = mode, .selection = .any, .required = 1, .failure_policy = policy, .standby_names = &.{"standby"} }, .sync_wait_ctx = &wait, .sync_wait_fn = Wait.run });
            switch (policy) {
                .block => try std.testing.expect(!try mirror.publisher.pollCompletion(mirror, 9)),
                .fail_closed => try std.testing.expectError(error.SyncPolicyUnsatisfied, mirror.publisher.pollCompletion(mirror, 9)),
                .degrade_to_async => try std.testing.expect(try mirror.publisher.pollCompletion(mirror, 9)),
            }
            try std.testing.expectEqual(@as(usize, 0), wait.calls);
        }
    }
    const blocked = bindMirror(&primary, .{ .sync_policy = .{ .mode = .remote_apply, .selection = .any, .required = 1, .failure_policy = .block, .standby_names = &.{"standby"} }, .sync_wait_ctx = &wait, .sync_wait_fn = Wait.run });
    // Foreground completion retains its existing waiting behavior.
    try std.testing.expectError(error.InjectedStandbyWait, blocked.publisher.complete(blocked, 9));
    try std.testing.expectEqual(@as(usize, 1), wait.calls);
    primary.slots.slots.items[0].state.applied_lsn = 9;
    primary.slots.slots.items[0].state.received_lsn = 9;
    try std.testing.expect(try blocked.publisher.pollCompletion(blocked, 9));
    try std.testing.expectEqual(@as(usize, 1), wait.calls);
}

test "controlled publisher recovery checks WAL matching without CPU handoff or duplicate publication" {
    const read = @import("antfly_local_sources").common_read_admission;
    const integration = @import("antfly_local_sources").storage_db_commit_integration;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    const bytes = try a.alloc(u8, 2 * 64 * 1024 + 17);
    defer a.free(bytes);
    @memset(bytes, 51);
    _ = try primary.append(.{ .payload = "different" });
    _ = try primary.append(.{ .payload = bytes });
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.cause;
        }
        fn cpu(_: *anyopaque) !void {
            return error.UnsafeCpuHandoffUnderPublicationFence;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.cpu, .check_lifetime = State.lifetime } };
    var transition: std.atomic.Mutex = .unlocked;
    var log: std.atomic.Mutex = .unlocked;
    var last_lsn: @import("antfly_platform").atomic.Value(u64) = .init(0);
    const mirror = bindMirror(&primary, .{ .transition_mutex = &transition, .last_lsn = &last_lsn });
    const context: integration.RecoveryContext = .{ .completion = .poll, .admission = read.Port.init(&payload), .transition_mutex = &transition, .log_mutex = &log, .namespace = .{ .table_id = 1, .shard_id = 2 }, .write_gate = null };
    const pending: outbox.DurableReplicationOutbox = .{ .from_lsn = 1, .payload = bytes };
    try integration.recoverDurableOutbox(context, mirror, pending, .batch);
    try std.testing.expectEqual(@as(u64, 2), last_lsn.load(.acquire));
    const checks = state.checks;
    try std.testing.expect(checks > 20);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            last_lsn.store(0, .release);
            state = .{ .cause = cause, .reject_at = reject_at };
            try std.testing.expectError(cause, integration.recoverDurableOutbox(context, mirror, pending, .batch));
            try std.testing.expectEqual(@as(u64, 0), last_lsn.load(.acquire));
            try std.testing.expectEqual(@as(u64, 2), primary.lastLsn());
            try std.testing.expect(transition.tryLock());
            transition.unlock();
            try std.testing.expect(log.tryLock());
            log.unlock();
        }
    }
    state = .{};
    // Legacy publishers lacking controlled recovery refuse the admitted call.
    var legacy = mirror;
    var vtable = publisher_vtable;
    vtable.recover_with_admission = null;
    legacy.publisher.vtable = &vtable;
    try std.testing.expectError(error.ExecutionWaitUnsupported, integration.recoverDurableOutbox(context, legacy, pending, .batch));
    // New append remains mandatory after the last pre-publication check. An
    // extra request check after append would replace success with cancellation.
    state = .{};
    const not_found: outbox.DurableReplicationOutbox = .{ .from_lsn = 3, .payload = "new mutation" };
    try integration.recoverDurableOutbox(context, mirror, not_found, .batch);
    const append_checks = state.checks;
    try std.testing.expectEqual(@as(u64, 3), primary.lastLsn());
    state = .{ .reject_at = append_checks + 1 };
    try integration.recoverDurableOutbox(context, mirror, .{ .from_lsn = 4, .payload = "another mutation" }, .batch);
    try std.testing.expectEqual(append_checks, state.checks);
    try std.testing.expectEqual(@as(u64, 4), last_lsn.load(.acquire));
    try std.testing.expectEqual(@as(u64, 4), primary.lastLsn());
}

test "controlled publisher recovery background DB uses actual HA matching and preserves intent at every shutdown check" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const read = @import("antfly_local_sources").common_read_admission;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    const db_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/db", .{tmp.sub_path});
    defer a.free(db_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    try primary.createSlot("standby", 0);
    const bytes = try a.alloc(u8, 2 * 64 * 1024 + 17);
    defer a.free(bytes);
    @memset(bytes, 63);
    _ = try primary.append(.{ .kind = .derived_effect, .payload = bytes });
    const Probe = struct {
        actual: ReplicationAsyncEffectMirror,
        closing: ?*std.atomic.Value(bool) = null,
        checks: usize = 0,
        close_at: usize = 0,
        fn cast(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn next(raw: *anyopaque) u64 {
            return cast(raw).actual.publisher.nextLsn();
        }
        fn identity(raw: *anyopaque) replication_contract.Publisher.Identity {
            return cast(raw).actual.publisher.identity();
        }
        fn publish(_: ReplicationAsyncEffectMirror, _: outbox.Kind, _: []const u8, _: Namespace) !u64 {
            return error.UnexpectedPublication;
        }
        fn recover(_: ReplicationAsyncEffectMirror, _: outbox.Kind, _: outbox.DurableReplicationOutbox, _: Namespace) !u64 {
            return error.UnexpectedLegacyRecovery;
        }
        fn preflight(_: ReplicationAsyncEffectMirror, _: bool) !void {}
        fn complete(_: ReplicationAsyncEffectMirror, _: u64) !void {
            return error.UnexpectedBlockingCompletion;
        }
        fn poll(m: ReplicationAsyncEffectMirror, lsn: u64) !bool {
            const self = cast(m.publisher.ptr);
            return self.actual.publisher.pollCompletion(self.actual, lsn);
        }
        fn controlled(m: ReplicationAsyncEffectMirror, kind: outbox.Kind, pending: outbox.DurableReplicationOutbox, namespace: Namespace, original: read.Port) !u64 {
            const self = cast(m.publisher.ptr);
            const Self = @This();
            const Relay = struct {
                probe: *Self,
                original: read.Port,
                fn check(raw: *anyopaque) !void {
                    const relay: *@This() = @ptrCast(@alignCast(raw));
                    relay.probe.checks += 1;
                    if (relay.probe.checks == relay.probe.close_at) relay.probe.closing.?.store(true, .release);
                    // Cancellation comes from the actual DB owner, never a
                    // replacement request lifetime installed by this fixture.
                    try relay.original.checkLifetime();
                }
            };
            var relay: Relay = .{ .probe = self, .original = original };
            const payload: read.Payload = .{ .ptr = &relay, .vtable = &.{ .checkpoint = Relay.check, .check_lifetime = Relay.check } };
            return self.actual.publisher.recoverWithAdmission(self.actual, kind, pending, namespace, read.Port.init(&payload));
        }
        fn mirror(self: *@This()) ReplicationAsyncEffectMirror {
            return .{ .publisher = .{ .ptr = self, .vtable = &.{ .next_lsn = @This().next, .identity = @This().identity, .publish = @This().publish, .recover = @This().recover, .recover_with_admission = @This().controlled, .preflight = @This().preflight, .complete = @This().complete }, .poll_completion = @This().poll }, .requirements = self.actual.requirements };
        }
    };
    var probe: Probe = .{ .actual = bindMirror(&primary, .{ .sync_policy = .{ .mode = .remote_apply, .required = 1, .standby_names = &.{"standby"}, .failure_policy = .block } }) };
    var db = try db_mod.DB.open(a, db_path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_index_workers = false, .start_optional_runtimes = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false }, .replication_async_effect_mirror = probe.mirror() });
    defer db.close();
    probe.closing = &db.async_context.background_closing;
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .replay, 1, db.core.root_generation, bytes);
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 1, bytes);
    defer a.free(raw);
    try db.core.store.putBatch(&.{.{ .key = key, .value = raw }}, &.{});
    db.local_execution.durable_replication_outbox_maybe.store(true, .release);
    // Use the public production background entrypoint. Its owned lifetime
    // must reach real native WAL CRC and exact matching before policy polling.
    try std.testing.expectError(error.CommitCompletionPending, db.flushDurableReplicationOutboxesStep());
    for (0..1000) |_| {
        if (db.local_execution.replication_recovery_cursor.state.matched_lsn != 0) break;
        try std.testing.expectError(error.CommitCompletionPending, db.flushDurableReplicationOutboxesStep());
    }
    try std.testing.expectEqual(@as(u64, 1), db.local_execution.replication_recovery_cursor.state.matched_lsn);
    const checks = probe.checks;
    try std.testing.expect(checks > 8);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    for (1..checks + 1) |close_at| {
        // Recreate the cold matching path at every injected shutdown boundary.
        // Warm, verified match reuse is exercised separately by cursor tests.
        db.local_execution.replication_recovery_cursor.reset();
        probe.checks = 0;
        probe.close_at = close_at;
        var canceled = false;
        for (0..1000) |_| {
            db.flushDurableReplicationOutboxesStep() catch |err| {
                if (err == error.Canceled) {
                    canceled = true;
                    break;
                }
                if (err != error.CommitCompletionPending) return err;
            };
        }
        db.async_context.background_closing.store(false, .release);
        try std.testing.expect(canceled);
        try std.testing.expectEqual(close_at, probe.checks);
        try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
        const retained = try db.core.store.get(a, key);
        defer a.free(retained);
        try std.testing.expectEqualSlices(u8, raw, retained);
        try std.testing.expect(primary.log.wal.mutex.tryLock());
        primary.log.wal.mutex.unlock(primary.log.wal.sync_io);
    }
    probe.close_at = 0;
    try primary.standbyStatusUpdate("standby", 1, 1, 1);
    for (0..1000) |_| {
        db.flushDurableReplicationOutboxesStep() catch |err| {
            if (err == error.CommitCompletionPending) continue;
            return err;
        };
        break;
    }
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectError(error.NotFound, db.core.store.get(a, key));
}

test "controlled publisher recovery retries changed absence and busy writers without duplicate append or failure telemetry" {
    const read = @import("antfly_local_sources").common_read_admission;
    const integration = @import("antfly_local_sources").storage_db_commit_integration;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    const Injector = struct {
        primary: *hot_standby_primary_mod.Primary,
        backing: std.mem.Allocator,
        inject: bool = true,
        failure: ?anyerror = null,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = @This().alloc, .resize = @This().resize, .remap = @This().remap, .free = @This().free } };
        }
        fn alloc(raw: *anyopaque, len: usize, alignment: std.mem.Alignment, address: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (self.inject) {
                self.inject = false;
                // Deterministically interleave another publisher after the
                // absence scan, while encoding outside WAL serialization.
                _ = self.primary.append(.{ .payload_codec = .json, .payload = "wanted" }) catch |err| {
                    self.failure = err;
                    return null;
                };
            }
            return self.backing.rawAlloc(len, alignment, address);
        }
        fn resize(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, address: usize) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawResize(bytes, alignment, len, address);
        }
        fn remap(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, len: usize, address: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(raw));
            return self.backing.rawRemap(bytes, alignment, len, address);
        }
        fn free(raw: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, address: usize) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.backing.rawFree(bytes, alignment, address);
        }
    };
    var injector: Injector = .{ .primary = &primary, .backing = a };
    primary.alloc = injector.allocator();
    defer primary.alloc = a;
    const Check = struct {
        fn run(_: *anyopaque) !void {}
    };
    var unused: u8 = 0;
    const payload: read.Payload = .{ .ptr = &unused, .vtable = &.{ .checkpoint = Check.run } };
    var transition: std.atomic.Mutex = .unlocked;
    var log: std.atomic.Mutex = .unlocked;
    var failures: @import("antfly_platform").atomic.Value(u64) = .init(0);
    var published: @import("antfly_platform").atomic.Value(u64) = .init(0);
    const mirror = bindMirror(&primary, .{ .transition_mutex = &transition, .failure_count = &failures, .last_lsn = &published });
    const context: integration.RecoveryContext = .{ .completion = .poll, .admission = read.Port.init(&payload), .transition_mutex = &transition, .log_mutex = &log, .namespace = .{ .table_id = 1, .shard_id = 2 }, .write_gate = null };
    const pending: outbox.DurableReplicationOutbox = .{ .from_lsn = 1, .payload = "wanted" };
    try std.testing.expectError(error.CommitCompletionPending, integration.recoverDurableOutbox(context, mirror, pending, .batch));
    try std.testing.expect(injector.failure == null and !injector.inject);
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 0), published.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expect(primary.log.wal.mutex.tryLock());
    const busy = integration.recoverDurableOutbox(context, mirror, pending, .batch);
    primary.log.wal.mutex.unlock(primary.log.wal.sync_io);
    try std.testing.expectError(error.CommitCompletionPending, busy);
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expect(transition.tryLock());
    transition.unlock();
    try std.testing.expect(log.tryLock());
    log.unlock();
    const backend = primary.log.wal.store_owner.lsm.backend;
    try std.testing.expect(backend.mu.tryLock());
    const snapshot_busy = integration.recoverDurableOutbox(context, mirror, pending, .batch);
    backend.mu.unlock();
    try std.testing.expectError(error.CommitCompletionPending, snapshot_busy);
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    try integration.recoverDurableOutbox(context, mirror, pending, .batch);
    try std.testing.expectEqual(@as(u64, 1), published.load(.acquire));
    try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
}

test "controlled publisher recovery background DB resumes v2 intent scans between record quanta" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    const db_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/db", .{tmp.sub_path});
    defer a.free(db_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    try primary.createSlot("standby", 0);
    for (0..3 * hot_standby_primary_mod.recovery_record_quantum) |_| _ = try primary.append(.{ .payload_codec = .json, .payload = "other!" });
    const wanted_lsn = try primary.append(.{ .payload_codec = .json, .payload = "wanted" });
    var failures: @import("antfly_platform").atomic.Value(u64) = .init(0);
    var published: @import("antfly_platform").atomic.Value(u64) = .init(0);
    const mirror = bindMirror(&primary, .{ .failure_count = &failures, .last_lsn = &published, .sync_policy = .{ .mode = .remote_apply, .required = 1, .standby_names = &.{"standby"}, .failure_policy = .block } });
    var db = try db_mod.DB.open(a, db_path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_index_workers = false, .start_optional_runtimes = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false }, .replication_async_batch_mirror = mirror });
    defer db.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .batch, 1, db.core.root_generation, "wanted");
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 1, "wanted");
    defer a.free(raw);
    try db.core.store.putBatch(&.{.{ .key = key, .value = raw }}, &.{});
    db.local_execution.durable_replication_outbox_maybe.store(true, .release);
    for (0..3) |_| {
        try std.testing.expectError(error.CommitCompletionPending, db.flushDurableReplicationOutboxesStep());
        const retained = try db.core.store.get(a, key);
        defer a.free(retained);
        try std.testing.expectEqualSlices(u8, raw, retained);
        try std.testing.expectEqual(@as(u64, 0), published.load(.acquire));
        try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
        try std.testing.expectEqual(wanted_lsn, primary.lastLsn());
        try std.testing.expect(primary.log.wal.store_owner.lsm.backend.active_readers <= 1);
        try std.testing.expect(db.local_execution.replication_recovery_cursor.mutex.tryLock());
        db.local_execution.replication_recovery_cursor.mutex.unlock();
        try std.testing.expect(primary.log.wal.mutex.tryLock());
        primary.log.wal.mutex.unlock(primary.log.wal.sync_io);
    }
    for (0..1000) |_| {
        if (published.load(.acquire) == wanted_lsn) break;
        try std.testing.expectError(error.CommitCompletionPending, db.flushDurableReplicationOutboxesStep());
    }
    try std.testing.expectEqual(@as(usize, 0), primary.log.wal.store_owner.lsm.backend.active_readers);
    // Matching is complete but acknowledgement is still mandatory.
    try std.testing.expectError(error.CommitCompletionPending, db.flushDurableReplicationOutboxesStep());
    try std.testing.expectEqual(wanted_lsn, published.load(.acquire));
    try std.testing.expectEqual(wanted_lsn, primary.lastLsn());
    try primary.standbyStatusUpdate("standby", primary.identity.timeline_id, wanted_lsn, wanted_lsn);
    try db.flushDurableReplicationOutboxesStep();
    try std.testing.expectError(error.NotFound, db.core.store.get(a, key));
    try std.testing.expectEqual(wanted_lsn, primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
}

test "controlled publisher recovery background DB retires pending byte reads on intent removal and close" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    const db_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/db", .{tmp.sub_path});
    defer a.free(db_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    const bytes = try a.alloc(u8, 256 * 1024 + 19);
    defer a.free(bytes);
    @memset(bytes, 'v');
    _ = try primary.append(.{ .kind = .derived_effect, .payload = bytes });
    var db = try db_mod.DB.open(a, db_path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_index_workers = false, .start_optional_runtimes = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false }, .replication_async_effect_mirror = bindMirror(&primary, .{}) });
    var open = true;
    defer if (open) db.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .primary_effect, 1, db.core.root_generation, bytes);
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 1, bytes);
    defer a.free(raw);
    const backend = primary.log.wal.store_owner.lsm.backend;
    for (0..3) |mode| {
        try db.core.store.putBatch(&.{.{ .key = key, .value = raw }}, &.{});
        db.local_execution.durable_replication_outbox_maybe.store(true, .release);
        // Intent capture precedes WAL matching. Each poll must return pending
        // without recapturing the value; reach the WAL reader in finite units.
        var polls: usize = 0;
        while (db.local_execution.replication_recovery_cursor.pending == null) : (polls += 1) {
            try std.testing.expect(polls < 1000);
            try std.testing.expectError(error.CommitCompletionPending, db.flushDurableReplicationOutboxesStep());
            try std.testing.expect(db.local_execution.replication_intent_capture.hasWork() or db.local_execution.replication_intent_capture.value != null);
        }
        try std.testing.expect(polls > 1);
        try std.testing.expect(db.local_execution.replication_intent_capture.value != null);
        try std.testing.expect(db.local_execution.replication_recovery_cursor.pending != null);
        try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
        switch (mode) {
            0 => {
                // The public key-retirement path releases just this immutable
                // intent's continuation after unlocking its apply transaction.
                try db.clearDurableReplicationOutbox(key);
                try std.testing.expect(db.local_execution.replication_recovery_cursor.pending == null);
            },
            1 => {
                // Reconciliation of an authoritative empty collection must
                // also retire a cursor whose durable key disappeared elsewhere.
                try db.core.store.putBatch(&.{}, &.{key});
                var retired_polls: usize = 0;
                while (db.local_execution.replication_recovery_cursor.pending != null) : (retired_polls += 1) {
                    try std.testing.expect(retired_polls < 1000);
                    db.flushDurableReplicationOutboxesStep() catch |err| if (err != error.CommitCompletionPending) return err;
                }
                try std.testing.expect(db.local_execution.replication_recovery_cursor.pending == null);
            },
            2 => {
                db.close();
                open = false;
            },
            else => unreachable,
        }
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
        try std.testing.expectEqual(@as(u64, 1), primary.lastLsn());
    }
    // DB teardown must not strand the external Primary behind a read pin.
    try std.testing.expectEqual(@as(u64, 2), try primary.append(.{ .payload = "after DB close" }));
}

test "controlled publisher recovery resident DB queues WAL work promptly and retains acknowledgement backoff" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    const db_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/db", .{tmp.sub_path});
    defer a.free(db_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    try primary.createSlot("standby", 0);
    const bytes = try a.alloc(u8, 2 * 1024 * 1024 + 19);
    defer a.free(bytes);
    @memset(bytes, 'v');
    const wanted = try primary.append(.{ .kind = .derived_effect, .payload = bytes });
    var failures: @import("antfly_platform").atomic.Value(u64) = .init(0);
    var published: @import("antfly_platform").atomic.Value(u64) = .init(0);
    const mirror = bindMirror(&primary, .{ .failure_count = &failures, .last_lsn = &published, .sync_policy = .{ .mode = .remote_apply, .required = 1, .standby_names = &.{"standby"}, .failure_policy = .block } });
    var db = try db_mod.DB.open(a, db_path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_index_workers = false, .start_optional_runtimes = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false }, .replication_async_effect_mirror = mirror });
    defer db.close();
    const key = try outbox.durableReplicationOutboxKeyAlloc(a, .primary_effect, 1, db.core.root_generation, bytes);
    defer a.free(key);
    const raw = try outbox.encodeDurableReplicationOutboxAlloc(a, 1, bytes);
    defer a.free(raw);
    try db.core.store.putBatch(&.{.{ .key = key, .value = raw }}, &.{});
    db.local_execution.durable_replication_outbox_maybe.store(true, .release);
    // Only the resident owner may advance matching; this loop never calls the
    // direct drain helper or advances fake time to bypass its retry policy.
    db.stable_address = true;
    db.scheduleDurableReplicationOutboxRecovery();
    const match_deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (published.load(.acquire) != wanted and @import("antfly_platform").time.monotonicNs() < match_deadline) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    try std.testing.expectEqual(wanted, published.load(.acquire));
    db.backend_runtime.durable_jobs.drainOwner(db.publication_recovery.owner_id);
    const progress = db.local_execution.replication_recovery_cursor.progress();
    try std.testing.expect(progress.generation > 0);
    try std.testing.expect(!progress.runnable);
    try std.testing.expect(db.local_execution.durable_replication_outbox_maybe.load(.acquire));
    try std.testing.expect(db.publication_recovery.driver.next_attempt_ns.load(.acquire) != 0);
    try std.testing.expectEqual(@as(u32, 0), db.publication_recovery.driver.failure_streak);
    try std.testing.expectEqual(@as(usize, 0), primary.log.wal.store_owner.lsm.backend.active_readers);
    const retained = try db.core.store.get(a, key);
    defer a.free(retained);
    try std.testing.expectEqualSlices(u8, raw, retained);
    try std.testing.expectEqual(wanted, primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
    try primary.standbyStatusUpdate("standby", primary.identity.timeline_id, wanted, wanted);
    const clear_deadline = @import("antfly_platform").time.monotonicNs() + 5 * std.time.ns_per_s;
    while (db.local_execution.durable_replication_outbox_maybe.load(.acquire) and @import("antfly_platform").time.monotonicNs() < clear_deadline) try std.testing.io.sleep(.fromMilliseconds(1), .awake);
    db.backend_runtime.durable_jobs.drainOwner(db.publication_recovery.owner_id);
    try std.testing.expect(!db.local_execution.durable_replication_outbox_maybe.load(.acquire));
    try std.testing.expectError(error.NotFound, db.core.store.get(a, key));
    try std.testing.expectEqual(wanted, primary.lastLsn());
    try std.testing.expectEqual(@as(u64, 0), failures.load(.acquire));
}

test "prepared publication owns its envelope before append and refuses reused or changed identity" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    const mirror = bindMirror(&primary, .{});
    const namespace: Namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 };
    var input: [8193]u8 = @splat(17);
    const original = input;
    const Original = struct {
        canceled: bool = false,
        calls: usize = 0,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.calls += 1;
            if (self.canceled) return error.Canceled;
        }
    };
    var original_control: Original = .{};
    var payload_control: @import("antfly_local_sources").common_read_admission.Payload = .{ .ptr = &original_control, .vtable = &.{ .checkpoint = Original.check } };
    var prepared = (try mirror.publisher.prepare(mirror, .batch, &input, namespace, @import("antfly_local_sources").common_read_admission.Port.init(&payload_control))).?;
    const construction_checks = original_control.calls;
    original_control.canceled = true;
    defer prepared.deinit();
    try std.testing.expectEqual(@as(u64, 1), primary.nextLsn());
    @memset(&input, 1);
    var empty: [0]u8 = .{};
    var deny = std.heap.FixedBufferAllocator.init(&empty);
    primary.alloc = deny.allocator();
    defer primary.alloc = a;
    try std.testing.expectError(error.OutOfMemory, mirror.publisher.publish(mirror, .batch, &input, namespace));
    const wal_allocator = primary.log.wal.allocator;
    primary.log.wal.allocator = deny.allocator();
    defer primary.log.wal.allocator = wal_allocator;
    // Backend allocations remain funded. Both encoding allocators are denied.
    try std.testing.expectError(error.OutOfMemory, primary.log.wal.append(&input));
    const lsn = try prepared.publish();
    primary.log.wal.allocator = wal_allocator;
    try std.testing.expectEqual(construction_checks, original_control.calls);
    try std.testing.expectEqual(@as(u64, 1), lsn);
    try std.testing.expectError(error.InvalidArgument, prepared.publish());
    var entry = (try primary.log.entryAt(a, lsn)).?;
    defer entry.deinit(a);
    try std.testing.expectEqualSlices(u8, &original, entry.record.payload);
    try std.testing.expectEqual(@as(u64, 2), primary.nextLsn());
    try std.testing.expectError(error.OutOfMemory, mirror.publisher.prepare(mirror, .batch, "refused", namespace, null));
    primary.alloc = a;
    var abandoned = (try mirror.publisher.prepare(mirror, .batch, "abandoned", namespace, null)).?;
    abandoned.deinit();
    try std.testing.expectEqual(@as(u64, 2), primary.nextLsn());
    var stale = (try mirror.publisher.prepare(mirror, .batch, "old epoch", namespace, null)).?;
    defer stale.deinit();
    primary.identity.epoch += 1;
    defer primary.identity.epoch -= 1;
    try std.testing.expectError(error.InvalidArgument, stale.publish());
    try std.testing.expectEqual(@as(u64, 2), primary.nextLsn());
}

test "prepared publication refuses before real DB commit and consumes prepaid native envelope after commit" {
    const db_mod = @import("antfly_source_root").antfly_sources.physical_db;
    const outbox_store = @import("antfly_local_sources").storage_db_durable_outbox_store;
    const keys = @import("antfly_local_sources").storage_internal_keys;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const log_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/log", .{tmp.sub_path}, 0);
    defer a.free(log_path);
    const slots_path = try std.fmt.allocPrintSentinel(a, ".zig-cache/tmp/{s}/slots", .{tmp.sub_path}, 0);
    defer a.free(slots_path);
    const db_path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/db", .{tmp.sub_path});
    defer a.free(db_path);
    var primary = try hot_standby_primary_mod.Primary.open(a, log_path, slots_path, .{ .cluster_id = 1, .timeline_id = 1, .epoch = 1, .table_id = 1, .shard_id = 2 }, .{});
    defer primary.close();
    const Fixture = struct {
        primary: *hot_standby_primary_mod.Primary,
        real: ReplicationAsyncEffectMirror,
        store: ?@FieldType(@typeInfo(@FieldType(db_mod.DB, "core")).pointer.child, "store") = null,
        key: []const u8,
        db: ?*db_mod.DB = null,
        expected_schema_version: ?u32 = null,
        original_allocator: std.mem.Allocator,
        deny_allocator: std.mem.Allocator,
        inner: ?replication_contract.PreparedPublication = null,
        prepares: usize = 0,
        publications: usize = 0,
        releases: usize = 0,
        expire: ?struct { ptr: *anyopaque, run: *const fn (*anyopaque) void } = null,
        fn self(mirror: ReplicationAsyncEffectMirror) *@This() {
            return @ptrCast(@alignCast(mirror.publisher.ptr));
        }
        fn next(raw: *anyopaque) u64 {
            const f: *@This() = @ptrCast(@alignCast(raw));
            return f.primary.nextLsn();
        }
        fn identity(raw: *anyopaque) replication_contract.Publisher.Identity {
            const f: *@This() = @ptrCast(@alignCast(raw));
            return f.real.publisher.identity();
        }
        fn publish(_: ReplicationAsyncEffectMirror, _: outbox.Kind, _: []const u8, _: Namespace) !u64 {
            return error.InvalidArgument;
        }
        fn recover(_: ReplicationAsyncEffectMirror, _: outbox.Kind, _: outbox.DurableReplicationOutbox, _: Namespace) !u64 {
            return error.InvalidArgument;
        }
        fn preflight(_: ReplicationAsyncEffectMirror, _: bool) !void {}
        fn complete(_: ReplicationAsyncEffectMirror, _: u64) !void {}
        fn prepare(mirror: ReplicationAsyncEffectMirror, kind: outbox.Kind, bytes: []const u8, namespace: Namespace, control: ?@import("antfly_local_sources").common_read_admission.Port) !replication_contract.PreparedPublication {
            const f = self(mirror);
            f.prepares += 1;
            f.inner = (try f.real.publisher.prepare(f.real, kind, bytes, namespace, control)).?;
            if (f.expire) |expiry| expiry.run(expiry.ptr);
            return .{ .ptr = f, .vtable = &.{ .publish = publishPrepared, .deinit = release } };
        }
        fn publishPrepared(raw: *anyopaque) !u64 {
            const f: *@This() = @ptrCast(@alignCast(raw));
            // This callback runs only after the actual local source commit.
            const value = try f.store.?.get(f.original_allocator, f.key);
            defer f.original_allocator.free(value);
            try std.testing.expect(value.len != 0);
            if (f.expected_schema_version) |version| try std.testing.expectEqual(version, f.db.?.core.schema.?.version);
            f.primary.alloc = f.deny_allocator;
            defer f.primary.alloc = f.original_allocator;
            try std.testing.expectError(error.OutOfMemory, f.real.publisher.publish(f.real, .batch, "would allocate", .{ .table_id = 1, .shard_id = 2, .range_id = 2 }));
            const lsn = try f.inner.?.publish();
            f.publications += 1;
            return lsn;
        }
        fn release(raw: *anyopaque) void {
            const f: *@This() = @ptrCast(@alignCast(raw));
            f.inner.?.deinit();
            f.inner = null;
            f.releases += 1;
        }
        fn binding(f: *@This()) ReplicationAsyncEffectMirror {
            return .{ .publisher = .{ .ptr = f, .vtable = &.{ .next_lsn = @This().next, .identity = @This().identity, .publish = @This().publish, .prepare = @This().prepare, .recover = @This().recover, .preflight = @This().preflight, .complete = @This().complete } }, .requirements = .{ .synchronous = true, .durable_outbox = true } };
        }
    };
    const key = try keys.documentKeyAlloc(a, "source");
    defer a.free(key);
    var empty: [0]u8 = .{};
    var deny = std.heap.FixedBufferAllocator.init(&empty);
    var fixture: Fixture = .{ .primary = &primary, .real = bindMirror(&primary, .{}), .key = key, .original_allocator = a, .deny_allocator = deny.allocator() };
    var db = try db_mod.DB.open(a, db_path, .{ .identity_namespace = .{ .table_id = 1, .shard_id = 2, .range_id = 2 }, .start_index_workers = false, .start_optional_runtimes = false, .start_optional_runtime_workers = false, .ttl_cleanup = .{ .enabled = false }, .replication_async_batch_mirror = fixture.binding() });
    defer db.close();
    fixture.store = db.core.store;
    fixture.db = &db;
    primary.alloc = deny.allocator();
    defer primary.alloc = a;
    const Batch = @typeInfo(@TypeOf(db_mod.DB.batch)).@"fn".param_types[1].?;
    const request: Batch = .{ .writes = &.{.{ .key = "source", .value = "{\"ok\":true}" }} };
    try std.testing.expectError(error.OutOfMemory, db.batch(request));
    try std.testing.expectError(error.NotFound, db.core.store.get(a, key));
    try std.testing.expectEqual(@as(usize, 0), fixture.publications);
    try std.testing.expectEqual(@as(u64, 1), primary.nextLsn());
    {
        var pending = try outbox_store.readPending(a, db.core.store);
        defer pending.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), pending.entries.len);
    }
    primary.alloc = a;
    // Force the signed lease to expire only after a real native envelope is
    // funded. The final policy fence must still refuse the source commit and
    // destroy that unused envelope; no wall-clock sleeps are involved.
    var lease = try db.local_execution.row_policy_gate.enterRaw();
    defer lease.release();
    const Lease = @TypeOf(lease);
    const Expiry = struct {
        fn expire(raw: *anyopaque) void {
            const target: *Lease = @ptrCast(@alignCast(raw));
            target.expires_at_seconds = 0;
        }
    };
    fixture.expire = .{ .ptr = &lease, .run = Expiry.expire };
    const GuardPtr = @typeInfo(@TypeOf(db_mod.DB.batchInternalWithPreparationAllocator)).@"fn".param_types[4].?;
    var guard: @typeInfo(GuardPtr).pointer.child = .{ .child = a, .io = std.testing.io };
    try std.testing.expectError(error.RowPolicyAuthenticationRequired, db.batchInternalWithPreparationAllocator(request, null, .{ .row_policy_lease = &lease }, &guard));
    fixture.expire = null;
    lease.release();
    try std.testing.expectError(error.NotFound, db.core.store.get(a, key));
    try std.testing.expectEqual(@as(usize, 0), fixture.publications);
    try std.testing.expectEqual(@as(usize, 1), fixture.releases);
    try std.testing.expectEqual(@as(u64, 1), primary.nextLsn());
    try db.batch(request);
    try std.testing.expectEqual(@as(usize, 3), fixture.prepares);
    try std.testing.expectEqual(@as(usize, 1), fixture.publications);
    try std.testing.expectEqual(@as(usize, 2), fixture.releases);
    try std.testing.expectEqual(@as(u64, 2), primary.nextLsn());
    var pending = try outbox_store.readPending(a, db.core.store);
    defer pending.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), pending.entries.len);
    // Both public schema routes must fund their native record before changing
    // the durable schema or publishing its new runtime version.
    fixture.key = "\x00\x00__metadata__:schema";
    fixture.expected_schema_version = 1;
    db.local_execution.replication_async_metadata_mirror = fixture.binding();
    primary.alloc = deny.allocator();
    try std.testing.expectError(error.OutOfMemory, db.setSchemaJson(a, "{\"version\":1}"));
    try std.testing.expect(db.core.schema == null);
    try std.testing.expectError(error.NotFound, db.core.store.get(a, fixture.key));
    try std.testing.expectEqual(@as(u64, 2), primary.nextLsn());
    primary.alloc = a;
    try db.setSchemaJson(a, "{\"version\":1}");
    try std.testing.expectEqual(@as(u64, 3), primary.nextLsn());
    const Schema = @typeInfo(@TypeOf(db_mod.DB.setSchema)).@"fn".param_types[1].?;
    const next_schema: Schema = .{ .version = 2 };
    fixture.expected_schema_version = 2;
    primary.alloc = deny.allocator();
    try std.testing.expectError(error.OutOfMemory, db.setSchema(next_schema));
    try std.testing.expectEqual(@as(u32, 1), db.core.schema.?.version);
    try std.testing.expectEqual(@as(u64, 3), primary.nextLsn());
    primary.alloc = a;
    try db.setSchema(next_schema);
    try std.testing.expectEqual(@as(u32, 2), db.core.schema.?.version);
    try std.testing.expectEqual(@as(u64, 4), primary.nextLsn());
    try std.testing.expectEqual(@as(usize, 7), fixture.prepares);
    try std.testing.expectEqual(@as(usize, 3), fixture.publications);
    try std.testing.expectEqual(@as(usize, 4), fixture.releases);
}

test "prepared publication foreign preparation preserves original refusal and retires ignored success" {
    const runtime = @import("antfly_runtime_abi");
    const read = @import("antfly_local_sources").common_read_admission;
    const Fixture = struct {
        mode: u8,
        checks: usize = 0,
        prepares: usize = 0,
        forwards: usize = 0,
        releases: usize = 0,
        publications: usize = 0,
        native_dispatch: @FieldType(replication_contract.Publisher, "callback_dispatch") = undefined,
        fn unbox(raw: *anyopaque) *@This() {
            return @ptrCast(@alignCast(raw));
        }
        fn check(raw: *anyopaque) !void {
            const self = unbox(raw);
            self.checks += 1;
            if (self.mode == 3 or (self.mode != 2 and self.checks == 2)) return error.DeadlineExceeded;
        }
        fn next(_: *anyopaque) u64 {
            return 1;
        }
        fn identity(_: *anyopaque) replication_contract.Publisher.Identity {
            return .{ .table_id = 1, .shard_id = 2, .timeline_id = 1, .epoch = 1 };
        }
        fn publish(_: ReplicationAsyncEffectMirror, _: outbox.Kind, _: []const u8, _: Namespace) !u64 {
            return error.InvalidArgument;
        }
        fn recover(_: ReplicationAsyncEffectMirror, _: outbox.Kind, _: outbox.DurableReplicationOutbox, _: Namespace) !u64 {
            return error.InvalidArgument;
        }
        fn preflight(_: ReplicationAsyncEffectMirror, _: bool) !void {}
        fn complete(_: ReplicationAsyncEffectMirror, _: u64) !void {}
        fn prepare(mirror: ReplicationAsyncEffectMirror, _: outbox.Kind, _: []const u8, _: Namespace, control: ?read.Port) !replication_contract.PreparedPublication {
            const self = unbox(mirror.publisher.ptr);
            self.prepares += 1;
            if (control) |port| port.checkpoint() catch {};
            if (self.mode != 0) return error.OutOfMemory;
            return .{ .ptr = self, .vtable = &.{ .publish = preparedPublish, .deinit = release } };
        }
        fn preparedPublish(raw: *anyopaque) !u64 {
            unbox(raw).publications += 1;
            return 1;
        }
        fn release(raw: *anyopaque) void {
            unbox(raw).releases += 1;
        }
        fn relay(contract: *const runtime.native_abi.CallContract, callback: *const anyopaque, args: *const anyopaque, output: *anyopaque) callconv(.c) runtime.error_abi.Status {
            const Args = std.meta.ArgsTuple(@typeInfo(replication_contract.Publisher.PrepareFn).pointer.child);
            const typed: *const Args = @ptrCast(@alignCast(args));
            const self = unbox(typed.@"0".publisher.ptr);
            self.forwards += 1;
            return self.native_dispatch(contract, callback, args, output);
        }
        fn binding(self: *@This()) ReplicationAsyncEffectMirror {
            var result: ReplicationAsyncEffectMirror = .{ .publisher = .{ .ptr = self, .vtable = &.{ .next_lsn = @This().next, .identity = @This().identity, .publish = @This().publish, .prepare = @This().prepare, .recover = @This().recover, .preflight = @This().preflight, .complete = @This().complete } } };
            self.native_dispatch = result.publisher.callback_dispatch;
            result.publisher.callback_dispatch = relay;
            return result;
        }
    };
    for (0..4) |mode| {
        var fixture: Fixture = .{ .mode = @intCast(mode) };
        const mirror = fixture.binding();
        var payload: read.Payload = .{ .ptr = &fixture, .vtable = &.{ .checkpoint = Fixture.check } };
        const result = mirror.publisher.prepare(mirror, .batch, "payload", .{ .table_id = 1, .shard_id = 2 }, read.Port.init(&payload));
        if (mode == 2) try std.testing.expectError(error.OutOfMemory, result) else try std.testing.expectError(error.DeadlineExceeded, result);
        try std.testing.expectEqual(@as(usize, if (mode == 3) 0 else 1), fixture.prepares);
        try std.testing.expectEqual(fixture.prepares, fixture.forwards);
        try std.testing.expectEqual(@as(usize, if (mode == 0) 1 else 0), fixture.releases);
        try std.testing.expectEqual(@as(usize, 0), fixture.publications);
    }
}
