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

//! Local commit ordering around borrowed replication publisher operations.
//! The caller commits under its apply fence, releases that fence before waits,
//! and uses this owner to recheck authority before acknowledging the client.
const std = @import("std");
const builtin = @import("builtin");
const replication_contract = @import("replication_contract.zig");
const ReplicationAsyncEffectMirror = replication_contract.AsyncEffectMirror;
const ReplicationWriteGate = replication_contract.WriteGate;
const replication_effects_mod = @import("replication_effects.zig");
const durable_outbox = @import("durable_outbox.zig");
const Namespace = @import("doc_identity_namespace.zig").Namespace;
const types = @import("types.zig");
const schema_mod = @import("../schema.zig");

pub const ReplicationDeferredCommitGate = struct {
    mirror: ReplicationAsyncEffectMirror,
    lsn: u64,
};

pub const ReplicationDeferredCommitGates = struct {
    transition_mutex: ?*std.atomic.Mutex = null,
    transition_locked: bool = false,
    gates: [2]ReplicationDeferredCommitGate = undefined,
    gate_count: usize = 0,

    pub fn begin(transition_mutex: ?*std.atomic.Mutex) @This() {
        if (transition_mutex) |mutex| lockAtomic(mutex);
        return .{
            .transition_mutex = transition_mutex,
            .transition_locked = transition_mutex != null,
        };
    }

    pub fn append(self: *@This(), gate: ?ReplicationDeferredCommitGate) void {
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

    pub fn waitForDurabilityAndAuthority(self: *@This(), write_gate: ?ReplicationWriteGate) !void {
        // The HA records are already durable and ordered with the local commit.
        // Remote acknowledgement must not retain the DB apply lock or the
        // transition mutex: status updates and safe reads need both paths to
        // remain live while a synchronous policy is pending.
        self.releaseTransition();
        for (self.gates[0..self.gate_count]) |gate| {
            try evaluateReplicationMirrorCommitGate(gate.mirror, gate.lsn);
        }

        // Serialize the final success decision with fencing after every remote
        // durability condition has passed. Error/pending outcomes never claim
        // client acknowledgement and therefore need no success recheck.
        if (self.transition_mutex) |mutex| {
            lockAtomic(mutex);
            self.transition_locked = true;
        }
        defer self.releaseTransition();
        try enforceReplicationWriteGateOptional(write_gate);
    }
};

pub fn evaluateReplicationMirrorCommitGate(mirror: ReplicationAsyncEffectMirror, lsn: u64) !void {
    try mirror.publisher.complete(mirror, lsn);
}

pub fn replicationMirrorSyncEnabled(mirror: ReplicationAsyncEffectMirror) bool {
    return mirror.sync_policy.mode != .async;
}

pub fn replicationMirrorRequiresDurableOutbox(mirror: ReplicationAsyncEffectMirror) bool {
    return replicationMirrorSyncEnabled(mirror) and mirror.sync_policy.failure_policy != .degrade_to_async;
}

pub fn noteReplicationMirrorFailure(mirror: ReplicationAsyncEffectMirror, comptime label: []const u8, err: anyerror) void {
    if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
    std.log.warn("failed to mirror DB " ++ label ++ " into HA stream: {s}", .{@errorName(err)});
}

pub fn enforceReplicationWriteGateOptional(gate: ?ReplicationWriteGate) !void {
    const configured = gate orelse return;
    try configured.check();
}

/// Check fail-closed availability before committing locally. The borrowed log
/// lock serializes this decision with append and the next-LSN observation.
pub fn preflight(mirror: ?ReplicationAsyncEffectMirror, log_mutex: *std.atomic.Mutex) !void {
    const configured = mirror orelse return;
    if (configured.sync_policy.mode == .async or configured.sync_policy.failure_policy != .fail_closed) return;
    lockAtomic(log_mutex);
    defer log_mutex.unlock();
    try configured.publisher.preflight(configured);
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
    write_gate: ?ReplicationWriteGate,
};

/// Match an already published record before appending, fail closed when its
/// retention fence is gone, and complete acknowledgement without a DB lock.
pub fn recoverDurableOutbox(
    context: RecoveryContext,
    mirror: ReplicationAsyncEffectMirror,
    outbox: durable_outbox.DurableReplicationOutbox,
    kind: durable_outbox.Kind,
) !void {
    var deferred = ReplicationDeferredCommitGates.begin(context.transition_mutex);
    defer deferred.releaseTransition();

    const lsn = blk: {
        lockAtomic(context.log_mutex);
        defer context.log_mutex.*.unlock();

        break :blk mirror.publisher.recover(mirror, kind, outbox, context.namespace) catch |err| {
            switch (kind) {
                .batch, .restore_batch => noteReplicationMirrorFailure(mirror, "batch mutation recovery", err),
                .replay, .primary_effect => noteReplicationMirrorFailure(mirror, "derived effect recovery", err),
                .schema, .row_policy => noteReplicationMirrorFailure(mirror, "metadata mutation recovery", err),
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
    replication_write_gate: ?ReplicationWriteGate,
    replication_async_effect_mirror: ?ReplicationAsyncEffectMirror,
    replication_async_batch_mirror: ?ReplicationAsyncEffectMirror,
    replication_async_metadata_mirror: ?ReplicationAsyncEffectMirror,
    append_pending: ?*const std.atomic.Value(bool),
};

fn checkWrite(ctx: *const CommitContext) !void {
    try enforceReplicationWriteGateOptional(ctx.replication_write_gate);
    if (ctx.append_pending) |pending| if (pending.load(.acquire)) return error.HAMirrorUnavailable;
}

pub fn mirrorReplicationReplayPayloadBestEffortContext(ctx: *const CommitContext, payload: []const u8) void {
    const mirror = ctx.replication_async_effect_mirror orelse return;
    const transition_mutex = mirror.transition_mutex;
    if (transition_mutex) |mutex| lockAtomic(mutex);
    defer if (transition_mutex) |mutex| mutex.unlock();
    checkWrite(ctx) catch return;
    lockAtomic(ctx.log_mutex);
    defer ctx.log_mutex.*.unlock();
    const lsn = mirror.publisher.publish(mirror, .replay, payload, ctx.identity_namespace) catch |err| {
        if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB derived effect into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorReplicationReplayPayloadCommitContext(ctx: *const CommitContext, payload: []const u8) !void {
    var deferred = ReplicationDeferredCommitGates.begin(ctx.transition_mutex);
    defer deferred.releaseTransition();
    deferred.append(try appendReplicationReplayPayloadCommitLockedContext(ctx, payload));
    try deferred.waitForDurabilityAndAuthority(ctx.replication_write_gate);
}

pub fn appendReplicationReplayPayloadCommitLockedContext(ctx: *const CommitContext, payload: []const u8) !?ReplicationDeferredCommitGate {
    const mirror = ctx.replication_async_effect_mirror orelse return null;
    // The local store has already committed. Always represent that mutation in
    // the HA tail; a fence that arrived after the preflight gate may reject the
    // client acknowledgement below, but must not create an unlogged local fork.
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = mirror.publisher.publish(mirror, .replay, payload, ctx.identity_namespace) catch |err| {
            noteReplicationMirrorFailure(mirror, "derived effect", err);
            if (replicationMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn mirrorReplicationBatchMutationBestEffortContext(ctx: *const CommitContext, request: types.BatchRequest) void {
    const mirror = ctx.replication_async_batch_mirror orelse return;
    const transition_mutex = mirror.transition_mutex;
    if (transition_mutex) |mutex| lockAtomic(mutex);
    defer if (transition_mutex) |mutex| mutex.unlock();
    checkWrite(ctx) catch return;
    lockAtomic(ctx.log_mutex);
    defer ctx.log_mutex.*.unlock();
    const lsn = publishBatch(ctx, mirror, request) catch |err| {
        if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB batch mutation into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn mirrorReplicationBatchMutationCommitContext(ctx: *const CommitContext, request: types.BatchRequest) !void {
    var deferred = ReplicationDeferredCommitGates.begin(ctx.transition_mutex);
    defer deferred.releaseTransition();
    deferred.append(try appendReplicationBatchMutationCommitLockedContext(ctx, request));
    try deferred.waitForDurabilityAndAuthority(ctx.replication_write_gate);
}

pub fn appendReplicationBatchMutationCommitLockedContext(ctx: *const CommitContext, request: types.BatchRequest) !?ReplicationDeferredCommitGate {
    const mirror = ctx.replication_async_batch_mirror orelse return null;
    // The local store has already committed. Always append before applying the
    // final authority check so rejoin cannot mistake local divergence for an
    // exact fork boundary.
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = publishBatch(ctx, mirror, request) catch |err| {
            noteReplicationMirrorFailure(mirror, "batch mutation", err);
            if (replicationMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn mirrorReplicationEncodedBatchMutationCommitContext(ctx: *const CommitContext, payload: []const u8) !void {
    var deferred = ReplicationDeferredCommitGates.begin(ctx.transition_mutex);
    defer deferred.releaseTransition();
    deferred.append(try appendReplicationEncodedBatchMutationCommitLockedContext(ctx, payload));
    try deferred.waitForDurabilityAndAuthority(ctx.replication_write_gate);
}

pub fn appendReplicationEncodedBatchMutationCommitLockedContext(ctx: *const CommitContext, payload: []const u8) !?ReplicationDeferredCommitGate {
    return appendReplicationEncodedBatchMutationCommitLockedContextStrict(ctx, payload, false);
}

pub fn appendReplicationEncodedBatchMutationCommitLockedContextStrict(ctx: *const CommitContext, payload: []const u8, strict_append: bool) !?ReplicationDeferredCommitGate {
    const mirror = ctx.replication_async_batch_mirror orelse return null;
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = mirror.publisher.publish(mirror, .batch, payload, ctx.identity_namespace) catch |err| {
            noteReplicationMirrorFailure(mirror, "batch mutation", err);
            if (strict_append or replicationMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn mirrorReplicationSchemaMetadataBestEffortContext(
    ctx: *const CommitContext,
    table_schema: schema_mod.TableSchema,
    public_schema_json: ?[]const u8,
) void {
    const mirror = ctx.replication_async_metadata_mirror orelse return;
    const transition_mutex = mirror.transition_mutex;
    if (transition_mutex) |mutex| lockAtomic(mutex);
    defer if (transition_mutex) |mutex| mutex.unlock();
    checkWrite(ctx) catch return;
    lockAtomic(ctx.log_mutex);
    defer ctx.log_mutex.*.unlock();
    const lsn = publishSchema(ctx, mirror, table_schema, public_schema_json) catch |err| {
        if (mirror.failure_count) |counter| _ = counter.fetchAdd(1, .monotonic);
        std.log.warn("failed to mirror DB schema metadata into HA stream: {s}", .{@errorName(err)});
        return;
    };
    if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
}

pub fn appendReplicationSchemaMetadataCommitLockedContext(
    ctx: *const CommitContext,
    table_schema: schema_mod.TableSchema,
    public_schema_json: ?[]const u8,
) !?ReplicationDeferredCommitGate {
    const mirror = ctx.replication_async_metadata_mirror orelse return null;
    // As with document batches, committed metadata must remain represented in
    // the HA tail even when authority expires before acknowledgement.
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = publishSchema(ctx, mirror, table_schema, public_schema_json) catch |err| {
            noteReplicationMirrorFailure(mirror, "metadata mutation", err);
            if (replicationMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

pub fn appendReplicationEncodedSchemaMetadataCommitLockedContext(
    ctx: *const CommitContext,
    payload: []const u8,
) !?ReplicationDeferredCommitGate {
    const mirror = ctx.replication_async_metadata_mirror orelse return null;
    const lsn = blk: {
        lockAtomic(ctx.log_mutex);
        defer ctx.log_mutex.*.unlock();
        const lsn = mirror.publisher.publish(mirror, .schema, payload, ctx.identity_namespace) catch |err| {
            noteReplicationMirrorFailure(mirror, "metadata mutation", err);
            if (replicationMirrorSyncEnabled(mirror)) return err;
            return null;
        };
        if (mirror.last_lsn) |last_lsn| last_lsn.store(lsn, .release);
        break :blk lsn;
    };
    return .{ .mirror = mirror, .lsn = lsn };
}

fn publishBatch(ctx: *const CommitContext, mirror: ReplicationAsyncEffectMirror, request: types.BatchRequest) !u64 {
    const payload = try replication_effects_mod.encodeBatchMutationRequestAlloc(ctx.alloc, request);
    defer ctx.alloc.free(payload);
    return try mirror.publisher.publish(mirror, .batch, payload, ctx.identity_namespace);
}

fn publishSchema(ctx: *const CommitContext, mirror: ReplicationAsyncEffectMirror, schema: schema_mod.TableSchema, public_schema_json: ?[]const u8) !u64 {
    const payload = try replication_effects_mod.encodeSchemaMetadataMutationAlloc(ctx.alloc, schema, public_schema_json);
    defer ctx.alloc.free(payload);
    return try mirror.publisher.publish(mirror, .schema, payload, ctx.identity_namespace);
}

// A publisher with no dependency on the HA runtime exercises local ordering
// independently of transports, WAL implementations, and standby policy.
const TestPublisher = struct {
    transition: std.atomic.Mutex = .unlocked,
    log: std.atomic.Mutex = .unlocked,
    completed: bool = false,
    fenced: bool = false,
    fail_completion: bool = false,
    preflight_locked: bool = false,
    recovered: bool = false,

    fn cast(ptr: *anyopaque) *@This() {
        return @ptrCast(@alignCast(ptr));
    }
    fn mirror(self: *@This()) ReplicationAsyncEffectMirror {
        return .{ .publisher = .{ .ptr = self, .vtable = &vtable }, .transition_mutex = &self.transition, .sync_policy = .{ .mode = .remote_apply, .failure_policy = .fail_closed } };
    }
    fn next(_: *anyopaque) u64 {
        return 7;
    }
    fn identity(_: *anyopaque) replication_contract.Publisher.Identity {
        return .{ .table_id = 1, .shard_id = 2, .timeline_id = 3, .epoch = 4 };
    }
    fn publish(_: ReplicationAsyncEffectMirror, _: durable_outbox.Kind, _: []const u8, _: Namespace) !u64 {
        return 7;
    }
    fn recover(m: ReplicationAsyncEffectMirror, kind: durable_outbox.Kind, pending: durable_outbox.DurableReplicationOutbox, ns: Namespace) !u64 {
        const self = cast(m.publisher.ptr);
        try std.testing.expect(!self.log.tryLock());
        try std.testing.expect(!self.transition.tryLock());
        try std.testing.expectEqual(durable_outbox.Kind.replay, kind);
        try std.testing.expectEqual(@as(u64, 5), pending.from_lsn);
        try std.testing.expectEqualStrings("payload", pending.payload);
        try std.testing.expectEqual(@as(u64, 2), ns.shard_id);
        self.recovered = true;
        return 7;
    }
    fn preflight(m: ReplicationAsyncEffectMirror, _: bool) !void {
        const self = cast(m.publisher.ptr);
        self.preflight_locked = !self.log.tryLock();
        if (!self.preflight_locked) self.log.unlock();
        return error.SyncPolicyUnsatisfied;
    }
    fn complete(m: ReplicationAsyncEffectMirror, lsn: u64) !void {
        const self = cast(m.publisher.ptr);
        try std.testing.expectEqual(@as(u64, 7), lsn);
        try std.testing.expect(self.transition.tryLock());
        self.transition.unlock();
        try std.testing.expect(self.log.tryLock());
        self.log.unlock();
        self.completed = true;
        if (self.fail_completion) return error.HASyncCommitWouldBlock;
    }
    fn check(ptr: *const anyopaque) !void {
        const self: *const @This() = @ptrCast(@alignCast(ptr));
        try std.testing.expect(self.completed);
        try std.testing.expect(!@constCast(&self.transition).tryLock());
        if (self.fenced) return error.PrimaryFenced;
    }
    fn gate(self: *@This()) ReplicationWriteGate {
        return .{ .primary = .{ .ptr = self, .check_fn = check } };
    }
    const vtable: replication_contract.Publisher.VTable = .{ .next_lsn = next, .identity = identity, .publish = publish, .recover = recover, .preflight = @This().preflight, .complete = complete };
};

test "storage.hot_standby engine preflight holds the local log fence and propagates rejection" {
    var publisher: TestPublisher = .{};
    try std.testing.expectError(error.SyncPolicyUnsatisfied, preflight(publisher.mirror(), &publisher.log));
    try std.testing.expect(publisher.preflight_locked);
    try std.testing.expect(publisher.log.tryLock());
    publisher.log.unlock();
    try std.testing.expect(!publisher.completed);
}

test "storage.hot_standby engine recovery releases locks for durability and rechecks fenced authority" {
    var publisher: TestPublisher = .{ .fenced = true };
    try std.testing.expectError(error.PrimaryFenced, recoverDurableOutbox(.{ .transition_mutex = &publisher.transition, .log_mutex = &publisher.log, .namespace = .{ .table_id = 1, .shard_id = 2 }, .write_gate = publisher.gate() }, publisher.mirror(), .{ .from_lsn = 5, .payload = "payload" }, .replay));
    try std.testing.expect(publisher.recovered and publisher.completed);
    try std.testing.expect(publisher.transition.tryLock());
    publisher.transition.unlock();
}

test "storage.hot_standby engine pending durability releases the transition fence without acknowledging" {
    var publisher: TestPublisher = .{ .fail_completion = true };
    var gates = ReplicationDeferredCommitGates.begin(&publisher.transition);
    defer gates.releaseTransition();
    gates.append(.{ .mirror = publisher.mirror(), .lsn = 7 });
    try std.testing.expectError(error.HASyncCommitWouldBlock, gates.waitForDurabilityAndAuthority(publisher.gate()));
    try std.testing.expect(publisher.transition.tryLock());
    publisher.transition.unlock();
}
