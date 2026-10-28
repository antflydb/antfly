// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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

//! Actual DB TTL mutation, acknowledged cleanup and replay-capture retirement.
const std = @import("std");
const engine = @import("antfly_local_sources").storage_db_db;
const DB = engine.DB;
const MutationBarrier = engine.MutationBarrier;
const TestDirectory = @import("antfly_local_sources").common_test_directory.TestDirectory;
const GraphPrimaryPublicationTest = engine.test_support.GraphPrimaryPublicationTest;
const TtlCleanupContext = engine.test_support.TtlCleanupContext;
const applyDerivedBatchToIndexAsync = engine.test_support.applyDerivedBatchToIndexAsync;
const executeDeleteBatchContext = engine.test_support.executeDeleteBatchContext;
const expireDirectGraphTtlCandidateContext = engine.test_support.expireDirectGraphTtlCandidateContext;
const expireGraphTtlCandidateContext = engine.test_support.expireGraphTtlCandidateContext;
const durable_outbox = @import("antfly_local_sources").storage_db_durable_outbox;
const doc_identity = @import("antfly_local_sources").storage_db_doc_identity;
const docstore_mod = @import("antfly_local_sources").storage_docstore;
const graph_edge_ttl_expiration = @import("antfly_local_sources").storage_db_graph_edge_ttl_expiration;
const internal_keys = @import("antfly_local_sources").storage_internal_keys;
const platform_clock = @import("antfly_platform").clock;
const replication_outbox_v2_prefix = durable_outbox.replication_outbox_v2_prefix;

const TtlCleanupCaptureMode = enum { document, direct_graph, source_graph };
const TtlCleanupCaptureFailure = enum { reply_loss, fence_at_apply, capture_at_cleanup, replay_capture_at_apply };

fn runTtlCleanupCaptureProtection(mode: TtlCleanupCaptureMode, failure: TtlCleanupCaptureFailure, explicit_root: bool) !void {
    const contract = @import("antfly_local_sources").storage_db_replication_contract;
    const Failure = TtlCleanupCaptureFailure;
    const Fixture = struct {
        db: *DB,
        barrier: *MutationBarrier,
        root: ?*MutationBarrier.RootSharedLease,
        failure: ?Failure,
        completed: bool = false,
        captured: bool = false,
        cleanup_observed: bool = false,
        capture_worker: ?std.Thread = null,
        capture_ready: std.atomic.Value(bool) = .init(false),
        cleanup_wait_observed: bool = false,
        capture_failure: ?anyerror = null,
        fn next(_: *anyopaque) u64 {
            return 7;
        }
        fn identity(_: *anyopaque) contract.Publisher.Identity {
            return .{ .table_id = 0, .shard_id = 0, .timeline_id = 1, .epoch = 1 };
        }
        fn publish(m: contract.AsyncEffectMirror, kind: durable_outbox.Kind, payload: []const u8, _: doc_identity.Namespace) !u64 {
            const self: *@This() = @ptrCast(@alignCast(m.publisher.ptr));
            try std.testing.expectEqual(durable_outbox.Kind.primary_effect, kind);
            try std.testing.expect(payload.len != 0);
            if (self.barrier.tryAcquireExclusive()) |lease| {
                var unexpected = lease;
                unexpected.release();
                return error.TtlAppendOutsideMutationProtection;
            }
            return 7;
        }
        fn recover(m: contract.AsyncEffectMirror, kind: durable_outbox.Kind, pending: durable_outbox.DurableReplicationOutbox, ns: doc_identity.Namespace) !u64 {
            return publish(m, kind, pending.payload, ns);
        }
        fn preflight(_: contract.AsyncEffectMirror, _: bool) !void {}
        fn capture(self: *@This()) void {
            if (self.barrier.tryAcquireExclusive()) |lease| {
                var owned = lease;
                owned.release();
                self.captured = true;
            }
        }
        fn holdCapture(self: *@This()) void {
            self.holdCaptureImpl() catch |err| {
                self.capture_failure = err;
            };
            self.capture_ready.store(true, .release);
        }
        fn holdCaptureImpl(self: *@This()) !void {
            var capture_lease = self.barrier.tryAcquireExclusive() orelse return error.TtlCaptureHandoffBlocked;
            defer capture_lease.release();
            const pending = try self.db.core.store.scanPrefix(std.testing.allocator, replication_outbox_v2_prefix);
            defer docstore_mod.DocStore.freeResults(std.testing.allocator, pending);
            try std.testing.expectEqual(@as(usize, 1), pending.len);
            self.capture_ready.store(true, .release);
            // The watchdog belongs to the fixture, not the accepted operation.
            for (0..10_000) |_| {
                if (self.barrier.pendingSharedAcquisitions() != 0) {
                    const debt = try self.db.core.store.scanPrefix(std.testing.allocator, replication_outbox_v2_prefix);
                    defer docstore_mod.DocStore.freeResults(std.testing.allocator, debt);
                    try std.testing.expectEqual(@as(usize, 1), debt.len);
                    self.cleanup_wait_observed = true;
                    return;
                }
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            return error.TtlCleanupDidNotWaitForCapture;
        }
        fn holdReplayCapture(self: *@This()) void {
            self.holdReplayCaptureImpl() catch |err| {
                self.capture_failure = err;
            };
            self.capture_ready.store(true, .release);
        }
        fn holdReplayCaptureImpl(self: *@This()) !void {
            var replay_capture = self.db.core.snapshot_replay_admission.tryAcquireCapture() orelse return error.TtlReplayCaptureHandoffBlocked;
            defer replay_capture.release();
            self.capture_ready.store(true, .release);
            for (0..10_000) |_| {
                if (self.db.core.snapshot_replay_admission.lock.shared_waiters.load(.acquire) != 0) {
                    // Capture takes replay before apply. The waiting fallback
                    // must leave apply free or both workers would deadlock.
                    if (!self.db.core.apply_mutex.tryLockShared()) return error.TtlReplayWaitHeldApply;
                    self.db.core.apply_mutex.unlockShared();
                    const debt = try self.db.core.store.scanPrefix(std.testing.allocator, replication_outbox_v2_prefix);
                    defer docstore_mod.DocStore.freeResults(std.testing.allocator, debt);
                    try std.testing.expectEqual(@as(usize, 0), debt.len);
                    self.cleanup_wait_observed = true;
                    return;
                }
                try std.testing.io.sleep(.fromMilliseconds(1), .awake);
            }
            return error.TtlReplayDidNotWaitForCapture;
        }
        fn complete(m: contract.AsyncEffectMirror, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(m.publisher.ptr));
            if (self.root) |root| try std.testing.expect(!root.held);
            try std.testing.expect(self.db.core.apply_mutex.tryLockShared());
            self.db.core.apply_mutex.unlockShared();
            self.captured = false;
            const worker = try std.Thread.spawn(.{}, capture, .{self});
            worker.join();
            try std.testing.expect(self.captured);
            self.completed = true;
            if (self.failure == .reply_loss) return error.CommitCompletionPending;
            if (self.failure == .capture_at_cleanup or self.failure == .replay_capture_at_apply) {
                self.capture_worker = if (self.failure == .replay_capture_at_apply)
                    try std.Thread.spawn(.{}, holdReplayCapture, .{self})
                else
                    try std.Thread.spawn(.{}, holdCapture, .{self});
                for (0..10_000) |_| {
                    if (self.capture_ready.load(.acquire)) return;
                    try std.testing.io.sleep(.fromMilliseconds(1), .awake);
                }
                return error.TtlCaptureWorkerDidNotStart;
            }
        }
        fn check(raw: *const anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(@constCast(raw)));
            if (!self.completed) return;
            if (self.db.core.apply_mutex.tryLockShared()) {
                self.db.core.apply_mutex.unlockShared();
                return;
            }
            // Only the actual acknowledged deletion phase holds apply here.
            // Both legacy and explicit-root cleanup must exclude capture.
            if (self.barrier.tryAcquireExclusive()) |lease| {
                var unexpected = lease;
                unexpected.release();
                return error.TtlCleanupOutsideMutationProtection;
            }
            if (self.root) |root| {
                try std.testing.expect(root.held);
                try std.testing.expectEqual(@as(usize, 1), root.borrowers.load(.acquire));
            }
            self.cleanup_observed = true;
            if (self.failure == .fence_at_apply) return error.Forbidden;
        }
    };
    const alloc = std.testing.allocator;
    var directory = try TestDirectory.init("ttl-cleanup-capture-admission");
    defer directory.cleanup();
    var db = try DB.open(alloc, directory.path(), .{ .start_optional_runtimes = false, .start_index_workers = false });
    defer db.close();
    if (mode == .source_graph) {
        const artifact = try GraphPrimaryPublicationTest.seed(&db, true);
        defer alloc.free(artifact);
        // The seed leaves a durable artifact awaiting source replay. Materialize
        // its contender and TTL deadline before installing the fault probes.
        _ = try applyDerivedBatchToIndexAsync(db.async_context, .{ .changed_artifact_keys = &.{artifact} }, .{ .name = "g", .kind = .graph }, .{});
    } else {
        try db.addIndex(.{ .name = "direct", .kind = .graph, .config_json = "{\"ttl\":{\"duration\":\"1h\"}}" });
        try db.batch(.{ .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"keep\"}" }}, .graph_writes = &.{.{ .index_name = "direct", .source = "doc:a", .target = "doc:b", .edge_type = "links" }}, .sync_level = .full_index });
    }
    const due = try db.core.store.scanPrefix(alloc, &internal_keys.graph_edge_expiration_index_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, due);
    try std.testing.expect(due.len != 0);
    var barrier: MutationBarrier = .{};
    var root = MutationBarrier.RootSharedLease.initRetired(&barrier);
    defer root.release();
    var fixture: Fixture = .{ .db = &db, .barrier = &barrier, .root = if (explicit_root) &root else null, .failure = failure };
    defer if (fixture.capture_worker) |worker| worker.join();
    db.local_execution.replication_async_effect_mirror = .{ .publisher = .{ .ptr = &fixture, .vtable = &.{ .next_lsn = Fixture.next, .identity = Fixture.identity, .publish = Fixture.publish, .recover = Fixture.recover, .preflight = Fixture.preflight, .complete = Fixture.complete } }, .mutation_barrier = &barrier, .requirements = .{ .synchronous = true, .durable_outbox = true } };
    db.local_execution.replication_write_gate = .{ .borrowed = .{ .ptr = &fixture, .check_fn = Fixture.check } };
    defer {
        db.local_execution.replication_async_effect_mirror = null;
        db.local_execution.replication_write_gate = null;
    }
    var clock = platform_clock.ManualClock{};
    const source = (try graph_edge_ttl_expiration.decodeDue(due[0].value));
    clock.setRealtimeNs(if (mode == .source_graph) source.source.deadline_ns + 1 else source.direct.deadline_ns + 1);
    var ttl = TtlCleanupContext{ .batch = engine.test_support.batchContext(&db), .grace_period_ns = 0, .clock = clock.clock() };
    ttl.batch.mutation_root = fixture.root;
    const result: anyerror!void = switch (mode) {
        .document => blk: {
            _ = executeDeleteBatchContext(&ttl.batch, &.{"doc:a"}, .full_index, null) catch |err| break :blk err;
            break :blk {};
        },
        .direct_graph => blk: {
            _ = expireDirectGraphTtlCandidateContext(&ttl, source.direct) catch |err| break :blk err;
            break :blk {};
        },
        .source_graph => blk: {
            _ = expireGraphTtlCandidateContext(&ttl, source.source) catch |err| break :blk err;
            break :blk {};
        },
    };
    if (fixture.capture_worker) |worker| {
        worker.join();
        fixture.capture_worker = null;
    }
    if (fixture.capture_failure) |err| return err;
    if (failure == .capture_at_cleanup or failure == .replay_capture_at_apply) {
        try result;
        try std.testing.expect(fixture.cleanup_wait_observed and fixture.cleanup_observed);
        const debt = try db.core.store.scanPrefix(alloc, replication_outbox_v2_prefix);
        defer docstore_mod.DocStore.freeResults(alloc, debt);
        try std.testing.expectEqual(@as(usize, 0), debt.len);
        try std.testing.expect(!root.held);
        var after_capture = barrier.tryAcquireExclusive() orelse return error.TtlCleanupLeakedMutationReader;
        after_capture.release();
        return;
    }
    try std.testing.expectError(if (failure == .reply_loss) error.CommitCompletionPending else error.Forbidden, result);
    try std.testing.expect(fixture.completed and fixture.captured);
    try std.testing.expect(!root.held);
    const pending = try db.core.store.scanPrefix(alloc, replication_outbox_v2_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, pending);
    try std.testing.expectEqual(@as(usize, 1), pending.len);
    if (failure == .fence_at_apply) try std.testing.expect(fixture.cleanup_observed);
    fixture.failure = null;
    fixture.completed = false;
    fixture.cleanup_observed = false;
    try db.flushDurableReplicationOutboxesWithAdmissionAndRoot(null, fixture.root);
    try std.testing.expect(fixture.completed and fixture.captured and fixture.cleanup_observed);
    const remaining = try db.core.store.scanPrefix(alloc, replication_outbox_v2_prefix);
    defer docstore_mod.DocStore.freeResults(alloc, remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
    try std.testing.expect(!root.held);
    var after = barrier.tryAcquireExclusive() orelse return error.TtlCleanupLeakedMutationReader;
    after.release();
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document reply_loss legacy" {
    try runTtlCleanupCaptureProtection(.document, .reply_loss, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document reply_loss rooted" {
    try runTtlCleanupCaptureProtection(.document, .reply_loss, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document fence_at_apply legacy" {
    try runTtlCleanupCaptureProtection(.document, .fence_at_apply, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document fence_at_apply rooted" {
    try runTtlCleanupCaptureProtection(.document, .fence_at_apply, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document capture_at_cleanup legacy" {
    try runTtlCleanupCaptureProtection(.document, .capture_at_cleanup, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document capture_at_cleanup rooted" {
    try runTtlCleanupCaptureProtection(.document, .capture_at_cleanup, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document replay_capture_at_apply legacy" {
    try runTtlCleanupCaptureProtection(.document, .replay_capture_at_apply, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: document replay_capture_at_apply rooted" {
    try runTtlCleanupCaptureProtection(.document, .replay_capture_at_apply, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph reply_loss legacy" {
    try runTtlCleanupCaptureProtection(.direct_graph, .reply_loss, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph reply_loss rooted" {
    try runTtlCleanupCaptureProtection(.direct_graph, .reply_loss, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph fence_at_apply legacy" {
    try runTtlCleanupCaptureProtection(.direct_graph, .fence_at_apply, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph fence_at_apply rooted" {
    try runTtlCleanupCaptureProtection(.direct_graph, .fence_at_apply, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph capture_at_cleanup legacy" {
    try runTtlCleanupCaptureProtection(.direct_graph, .capture_at_cleanup, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph capture_at_cleanup rooted" {
    try runTtlCleanupCaptureProtection(.direct_graph, .capture_at_cleanup, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph replay_capture_at_apply legacy" {
    try runTtlCleanupCaptureProtection(.direct_graph, .replay_capture_at_apply, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: direct_graph replay_capture_at_apply rooted" {
    try runTtlCleanupCaptureProtection(.direct_graph, .replay_capture_at_apply, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph reply_loss legacy" {
    try runTtlCleanupCaptureProtection(.source_graph, .reply_loss, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph reply_loss rooted" {
    try runTtlCleanupCaptureProtection(.source_graph, .reply_loss, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph fence_at_apply legacy" {
    try runTtlCleanupCaptureProtection(.source_graph, .fence_at_apply, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph fence_at_apply rooted" {
    try runTtlCleanupCaptureProtection(.source_graph, .fence_at_apply, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph capture_at_cleanup legacy" {
    try runTtlCleanupCaptureProtection(.source_graph, .capture_at_cleanup, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph capture_at_cleanup rooted" {
    try runTtlCleanupCaptureProtection(.source_graph, .capture_at_cleanup, true);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph replay_capture_at_apply legacy" {
    try runTtlCleanupCaptureProtection(.source_graph, .replay_capture_at_apply, false);
}

test "storage.hot_standby TTL acknowledged cleanup keeps capture and authority protection: source_graph replay_capture_at_apply rooted" {
    try runTtlCleanupCaptureProtection(.source_graph, .replay_capture_at_apply, true);
}
