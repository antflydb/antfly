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

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const db_mod = @import("../db.zig");
const DB = db_mod.DB;
const Indexes = @import("mod.zig").Indexes;

/// Full-text index state owned by `DB.indexes`: the optional segment merge runtime, which pauses around structural index mutations.
pub const TextIndexes = struct {
    merge_owner: @import("../local_runtime_owner.zig").RuntimeOwner(db_mod.text_merge_runtime_mod.TextMergeRuntime) = .{},
    merge_runtime: ?*db_mod.text_merge_runtime_mod.TextMergeRuntime = null,

    fn dbPtr(self: *TextIndexes) *DB {
        const parent: *Indexes = @alignCast(@fieldParentPtr("text", self));
        return parent.dbPtr();
    }

    pub fn initMergeRuntime(self: *TextIndexes, cfg: db_mod.text_merge_runtime_mod.Config) !void {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("initOptionalTextMergeRuntime", .{ db, cfg });
        if (!db.start_index_workers) return;
        if (!cfg.enabled) return;
        const resources = db.core.asyncResources();
        self.merge_owner = try @TypeOf(self.merge_owner).create(db.runtime_alloc, .{
            db.runtime_alloc,
            resources.index_manager,
            resources.apply_mutex,
            db.backend_runtime,
            cfg,
        });
        const runtime = self.merge_owner.runtime.?;
        self.merge_runtime = runtime;
        db.async_context.text_merge_runtime = runtime;
    }

    fn snapshotMergeStats(self: *TextIndexes) db_mod.types.TextMergeStats {
        const db = self.dbPtr();
        db_mod.lockApplyShared(db);
        defer db.core.unlockApplyShared();
        var merge_stats = db.core.index_manager.textMergeStatsSnapshot();
        if (self.merge_runtime) |runtime| runtime.overlayRuntimeStats(&merge_stats);
        return merge_stats;
    }

    pub fn trySnapshotMergeStats(self: *TextIndexes) ?db_mod.types.TextMergeStats {
        const db = self.dbPtr();
        if (comptime db_mod.storage_engine.enabled) return db_mod.storage_engine.call("trySnapshotTextMergeStats", .{db}) catch @panic("incompatible storage owner");
        if (!db.core.tryLockApplyShared()) return null;
        defer db.core.unlockApplyShared();
        var merge_stats = db.core.index_manager.textMergeStatsSnapshot();
        if (self.merge_runtime) |runtime| runtime.overlayRuntimeStats(&merge_stats);
        return merge_stats;
    }

    pub fn attemptMergeRestart(ptr: *anyopaque) !bool {
        const ctx: *db_mod.AsyncContext = @ptrCast(@alignCast(ptr));
        return if (ctx.text_merge_runtime) |runtime| try runtime.ensureRunning() else true;
    }

    pub fn quiesceMerge(self: *TextIndexes) bool {
        const runtime = self.merge_runtime orelse return false;
        return runtime.pause();
    }

    pub fn restartMerge(self: *TextIndexes, operation: []const u8, index_name: []const u8) void {
        const db = self.dbPtr();
        const runtime = self.merge_runtime orelse return;
        runtime.resumeAfterPause() catch |err| {
            // Index catalog durability is already decided at this point.
            // Queries and foreground indexing remain available; surface the
            // maintenance degradation without misreporting the mutation.
            std.log.warn("failed to restart text merge runtime after {s} index={s} err={s}", .{ operation, index_name, @errorName(err) });
            DB.scheduleMaintenanceRestartContext(
                db.async_context,
                db.backend_runtime.durable_jobs,
                db.indexes.repair.cleanup_owner_id,
                .text_merge,
            );
        };
    }

    pub fn drainScheduledMerges(self: *TextIndexes) !void {
        const db = self.dbPtr();
        if (db_mod.openModeRequiresReadOnlyBackends(db.open_mode)) return error.ReadOnly;
        var snapshot_replay = try db.acquireSnapshotReplayMutation();
        defer snapshot_replay.release();
        try db.lockApplyForPortableRuntime();
        defer db.core.unlockApply();
        try db.core.drainScheduledTextMerges();
    }
};

test "db delete full text index drains active merge before closing generation" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .text_merge = .{
            .enabled = true,
            .idle_interval_ms = 1,
            .error_interval_ms = 1,
        },
    });
    defer db.close();
    try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });

    const runtime = db.indexes.text.merge_runtime orelse return error.TestUnexpectedResult;
    // Prevent the fast maintenance worker from consuming every candidate
    // before the blocking hook is installed. This also makes the test
    // independent of scheduler speed and host load.
    try std.testing.expect(runtime.pause());

    try db.beginBulkIngestSession();
    var bulk_session_active = true;
    defer if (bulk_session_active) db.abortBulkIngestSession();
    for (0..12) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);
        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_text,
        });
    }

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
    bulk_session_active = false;
    try std.testing.expect(db.pendingWorkStats().text_merge.pending_segments > 0);

    db_mod.text_merge_runtime_mod.test_task_begin_entered.store(false, .release);
    db_mod.text_merge_runtime_mod.test_release_after_task_begin.store(false, .release);
    db_mod.text_merge_runtime_mod.test_block_after_task_begin.store(true, .release);
    db_mod.text_merge_runtime_mod.test_stop_entered.store(false, .release);
    defer {
        db_mod.text_merge_runtime_mod.test_release_after_task_begin.store(true, .release);
        db_mod.text_merge_runtime_mod.test_block_after_task_begin.store(false, .release);
        db_mod.text_merge_runtime_mod.test_stop_entered.store(false, .release);
    }
    try runtime.resumeAfterPause();

    var merge_started = false;
    for (0..5_000) |_| {
        if (db_mod.text_merge_runtime_mod.test_task_begin_entered.load(.acquire)) {
            merge_started = true;
            break;
        }
        try db.async_context.io.?.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    if (!merge_started) return error.TestTimeout;

    const Delete = struct {
        db: *DB,
        completed: std.atomic.Value(bool) = .init(false),
        removed: bool = false,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.removed = self.db.deleteIndex("ft_v1") catch |err| {
                self.err = err;
                self.completed.store(true, .release);
                return;
            };
            self.completed.store(true, .release);
        }
    };
    var deletion = Delete{ .db = &db };
    var delete_thread = try std.testing.io.concurrent(Delete.run, .{&deletion});
    var joined = false;
    defer if (!joined) {
        db_mod.text_merge_runtime_mod.test_release_after_task_begin.store(true, .release);
        delete_thread.await(std.testing.io);
    };

    var stop_entered = false;
    for (0..5_000) |_| {
        if (db_mod.text_merge_runtime_mod.test_stop_entered.load(.acquire)) {
            stop_entered = true;
            break;
        }
        try db.async_context.io.?.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
    }
    if (!stop_entered) return error.TestTimeout;

    // The delete thread is now inside the maintenance stopper. It must wait
    // for the task that borrowed the index generation rather than acknowledge
    // removal and close storage underneath that task.
    try std.testing.expect(!deletion.completed.load(.acquire));

    db_mod.text_merge_runtime_mod.test_release_after_task_begin.store(true, .release);
    delete_thread.await(std.testing.io);
    joined = true;
    try std.testing.expect(deletion.err == null);
    try std.testing.expect(deletion.removed);

    const source_doc = (try db.get(alloc, "doc:0")) orelse return error.TestExpectedEqual;
    defer alloc.free(source_doc);
    try std.testing.expect(std.mem.indexOf(u8, source_doc, "common token") != null);
    try std.testing.expectError(error.IndexNotFound, db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
    }));
}

test "db text merge backpressure drains sustained segment debt to low watermark" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    // Build the backlog without an automatic merge worker so the admission
    // boundary and producer assistance are deterministic.
    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .start_optional_runtime_workers = false,
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    for (0..24) |i| {
        var id_buf: [32]u8 = undefined;
        const id = try std.fmt.bufPrint(&id_buf, "doc:{d}", .{i});
        var stored_buf: [96]u8 = undefined;
        const stored = try std.fmt.bufPrint(&stored_buf, "{{\"body\":\"common token {d}\"}}", .{i});
        var body_buf: [48]u8 = undefined;
        const body = try std.fmt.bufPrint(&body_buf, "common token {d}", .{i});
        const fields = [_]db_mod.introducer_mod.TextField{.{ .field_name = "body", .text = body }};
        const docs = [_]db_mod.introducer_mod.TextDocument{.{
            .id = id,
            .stored_data = stored,
            .text_fields = &fields,
            .doc_ordinal = @intCast(i + 1),
        }};
        _ = try db.indexTextKernelDocuments("ft_v1", &docs);
    }

    // Disabling derived workers alone does not stop optional merge workers.
    // This fixture owns the scheduler directly until its explicit runtimes start.
    if (db.indexes.text.merge_runtime) |automatic| try std.testing.expect(!automatic.isStarted());
    const before = db.core.index_manager.textMergeStatsSnapshot();
    try std.testing.expect(before.pending_segments > 8);

    const resources = db.core.batchExecutionResources();
    var held_task = (try resources.index_manager.beginTextMergeTask()) orelse return error.TestUnexpectedResult;
    var held_task_active = true;
    defer if (held_task_active) {
        resources.apply_mutex.lockExclusive();
        resources.index_manager.cancelTextMergeTask(&held_task);
        resources.apply_mutex.unlockExclusive();
        held_task.deinit(resources.index_manager.alloc);
    };

    var bounded_runtime = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .max_pending_segments = 8,
            .resume_pending_segments = 4,
            .max_pending_bytes = 0,
            .backpressure_max_wait_ms = 3,
        },
    );
    try bounded_runtime.start();
    const bounded_outcome = bounded_runtime.applyBackpressure();
    try std.testing.expectEqual(db_mod.text_merge_runtime_mod.BackpressureOutcome.timed_out, bounded_outcome);
    try std.testing.expectError(error.TextMergeBackpressureTimeout, bounded_runtime.acquireProducerPermit("ft_v1", 1, 1));
    try std.testing.expectEqual(@as(u64, 2), bounded_runtime.stats().backpressure_timeouts);
    bounded_runtime.deinit();

    var byte_only_runtime = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .max_pending_segments = 0,
            .resume_pending_segments = 0,
            .max_pending_bytes = before.pending_heap_bytes + 1,
            .backpressure_max_wait_ms = 3,
        },
    );
    try byte_only_runtime.start();
    // A disk-backed corpus can exceed the byte watermark while a merge is
    // pending. Its retained bytes must not block a publication that fits the
    // remaining heap budget, even if that merge cannot finish yet.
    try std.testing.expect(before.pending_bytes > before.pending_heap_bytes + 1);
    var byte_permit = try byte_only_runtime.acquireProducerPermit("ft_v1", 0, 1);
    try std.testing.expectError(error.TextMergeBackpressureTimeout, byte_only_runtime.acquireProducerPermit("ft_v1", 0, 1));
    byte_permit.release();
    byte_only_runtime.deinit();

    resources.apply_mutex.lockExclusive();
    resources.index_manager.cancelTextMergeTask(&held_task);
    resources.apply_mutex.unlockExclusive();
    held_task.deinit(resources.index_manager.alloc);
    held_task_active = false;

    var runtime = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .max_pending_segments = 8,
            .resume_pending_segments = 4,
            .max_pending_bytes = 0,
        },
    );
    defer runtime.deinit();
    try runtime.start();

    const outcome = runtime.applyBackpressure();
    try std.testing.expectEqual(db_mod.text_merge_runtime_mod.BackpressureOutcome.drained, outcome);
    // Retire the worker before comparing two separately acquired snapshots;
    // crossing the low watermark does not promise the scheduler stays idle.
    _ = runtime.stop();

    const after = runtime.stats();
    try std.testing.expect(after.pending_segments <= 4);
    const active_text_index = resources.index_manager.textIndex("ft_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, @intCast(active_text_index.snapshot().segments.len)), after.active_segments);
    try std.testing.expectEqual(after.active_segments, after.max_active_segments_per_index);
    try std.testing.expectEqual(@as(u64, 1), after.backpressure_events);
    try std.testing.expect(after.merge_input_segments_total >= 10);

    // Admission includes the target index's steady live segments even when it
    // is no longer marked compaction-pending. This is the actual query fan-out
    // bound; pending-only accounting allowed the live baseline plus another
    // full watermark of concurrent publications.
    var fanout_runtime = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .max_pending_segments = after.active_segments + 1,
            .resume_pending_segments = after.active_segments,
            .max_pending_bytes = 0,
            .backpressure_max_wait_ms = 3,
        },
    );
    defer fanout_runtime.deinit();
    var fanout_permit = try fanout_runtime.acquireProducerPermit("ft_v1", 1, 0);
    try std.testing.expectError(
        error.TextMergeBackpressureTimeout,
        fanout_runtime.acquireProducerPermit("ft_v1", 1, 0),
    );
    fanout_permit.release();

    var admission_runtime = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .max_pending_segments = 8,
            .resume_pending_segments = 4,
            .max_pending_bytes = 100,
            .backpressure_max_wait_ms = 100,
        },
    );
    defer admission_runtime.deinit();
    try admission_runtime.start();

    var first_permit = try admission_runtime.acquireProducerPermit("ft_v1", 1, 80);
    try std.testing.expectError(error.TextMergeBackpressureTimeout, admission_runtime.acquireProducerPermit("ft_v1", 1, 30));
    first_permit.release();
    try std.testing.expectEqual(@as(usize, 0), admission_runtime.producer_segment_reservations_by_index.count());

    // Admission sleeps on a release epoch rather than rescanning the complete
    // segment catalog at a fixed polling interval.
    var held_permit = try admission_runtime.acquireProducerPermit("ft_v1", 1, 80);
    const Waiter = struct {
        runtime: *db_mod.text_merge_runtime_mod.TextMergeRuntime,
        acquired: std.atomic.Value(bool) = .init(false),
        failed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var permit = self.runtime.acquireProducerPermit("ft_v1", 1, 30) catch {
                self.failed.store(true, .release);
                return;
            };
            self.acquired.store(true, .release);
            permit.release();
        }
    };
    var waiter = Waiter{ .runtime = &admission_runtime };
    const events_before = admission_runtime.stats().backpressure_events;
    var waiter_thread = try std.testing.io.concurrent(Waiter.run, .{&waiter});
    var waiter_joined = false;
    defer if (!waiter_joined) {
        held_permit.release();
        waiter_thread.await(std.testing.io);
    };
    const waiter_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (admission_runtime.stats().backpressure_events == events_before and db_mod.monotonicTimeNs() < waiter_deadline) {
        std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    }
    try std.testing.expect(admission_runtime.stats().backpressure_events > events_before);
    held_permit.release();
    waiter_thread.await(std.testing.io);
    waiter_joined = true;
    try std.testing.expect(waiter.acquired.load(.acquire));
    try std.testing.expect(!waiter.failed.load(.acquire));

    // Weighted FIFO admission prevents a younger small publication from
    // overtaking an older catch-up batch, even when the small request would fit
    // in the currently available capacity.
    var fair_runtime = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .max_pending_segments = 100,
            .resume_pending_segments = 50,
            .max_pending_bytes = 0,
            .backpressure_max_wait_ms = 2_000,
        },
    );
    defer fair_runtime.deinit();

    // An older waiter blocked only on index A's segment ceiling must not
    // become a global queue head that stalls independent index B.
    var cross_index_blocker = try fair_runtime.acquireProducerPermit("admission-a", 100, 0);
    var cross_index_acquired = std.atomic.Value(bool).init(false);
    const CrossIndexWaiter = struct {
        runtime: *db_mod.text_merge_runtime_mod.TextMergeRuntime,
        acquired: *std.atomic.Value(bool),

        fn run(self: *@This()) void {
            var permit = self.runtime.acquireProducerPermit("admission-a", 1, 0) catch return;
            self.acquired.store(true, .release);
            permit.release();
        }
    };
    var cross_index_waiter = CrossIndexWaiter{ .runtime = &fair_runtime, .acquired = &cross_index_acquired };
    const cross_events_before = fair_runtime.stats().backpressure_events;
    var cross_index_thread = try std.testing.io.concurrent(CrossIndexWaiter.run, .{&cross_index_waiter});
    var cross_index_joined = false;
    defer if (!cross_index_joined) {
        cross_index_blocker.release();
        cross_index_thread.await(std.testing.io);
    };
    const cross_wait_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (fair_runtime.stats().backpressure_events == cross_events_before and db_mod.monotonicTimeNs() < cross_wait_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(fair_runtime.stats().backpressure_events > cross_events_before);
    var independent_index_permit = try fair_runtime.acquireProducerPermit("admission-b", 100, 0);
    independent_index_permit.release();
    try std.testing.expect(!cross_index_acquired.load(.acquire));
    cross_index_blocker.release();
    cross_index_thread.await(std.testing.io);
    cross_index_joined = true;
    try std.testing.expect(cross_index_acquired.load(.acquire));

    // A waiter blocked on both dimensions still owns FIFO priority for the
    // process-wide byte dimension. A younger request for another index must
    // not consume the remaining bytes merely because the older waiter is also
    // blocked on its index-local segment ceiling.
    const mixed_byte_limit = runtime.stats().pending_bytes +| 100;
    var mixed_runtime = try db_mod.text_merge_runtime_mod.TextMergeRuntime.init(
        alloc,
        resources.index_manager,
        resources.apply_mutex,
        db.backend_runtime,
        .{
            .enabled = true,
            .max_pending_segments = 100,
            .resume_pending_segments = 50,
            .max_pending_bytes = mixed_byte_limit,
            .backpressure_max_wait_ms = 2_000,
        },
    );
    defer mixed_runtime.deinit();
    var mixed_segment_blocker = try mixed_runtime.acquireProducerPermit("mixed-a", 100, 0);
    var mixed_segment_blocker_active = true;
    defer if (mixed_segment_blocker_active) mixed_segment_blocker.release();
    var mixed_byte_blocker = try mixed_runtime.acquireProducerPermit("mixed-byte", 0, 80);
    var mixed_byte_blocker_active = true;
    defer if (mixed_byte_blocker_active) mixed_byte_blocker.release();

    const MixedWaiter = struct {
        runtime: *db_mod.text_merge_runtime_mod.TextMergeRuntime,
        index_name: []const u8,
        segment_count: u64,
        byte_count: u64,
        acquisition_counter: *std.atomic.Value(u32),
        acquisition_order: *std.atomic.Value(u32),
        release_gate: *const std.atomic.Value(bool),
        failed: *std.atomic.Value(bool),

        fn run(self: *@This()) void {
            var permit = self.runtime.acquireProducerPermit(self.index_name, self.segment_count, self.byte_count) catch {
                self.failed.store(true, .release);
                return;
            };
            self.acquisition_order.store(self.acquisition_counter.fetchAdd(1, .acq_rel) + 1, .release);
            while (!self.release_gate.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            permit.release();
        }
    };
    var mixed_acquisition_counter = std.atomic.Value(u32).init(0);
    var mixed_older_order = std.atomic.Value(u32).init(0);
    var mixed_younger_order = std.atomic.Value(u32).init(0);
    var mixed_release = std.atomic.Value(bool).init(false);
    var mixed_older_failed = std.atomic.Value(bool).init(false);
    var mixed_younger_failed = std.atomic.Value(bool).init(false);
    var mixed_older = MixedWaiter{
        .runtime = &mixed_runtime,
        .index_name = "mixed-a",
        .segment_count = 1,
        .byte_count = 90,
        .acquisition_counter = &mixed_acquisition_counter,
        .acquisition_order = &mixed_older_order,
        .release_gate = &mixed_release,
        .failed = &mixed_older_failed,
    };
    const mixed_events_before = mixed_runtime.stats().backpressure_events;
    var mixed_older_thread = try std.testing.io.concurrent(MixedWaiter.run, .{&mixed_older});
    var mixed_older_joined = false;
    defer if (!mixed_older_joined) {
        if (mixed_segment_blocker_active) {
            mixed_segment_blocker.release();
            mixed_segment_blocker_active = false;
        }
        if (mixed_byte_blocker_active) {
            mixed_byte_blocker.release();
            mixed_byte_blocker_active = false;
        }
        mixed_release.store(true, .release);
        mixed_older_thread.await(std.testing.io);
    };
    const mixed_older_wait_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (mixed_runtime.stats().backpressure_events == mixed_events_before and db_mod.monotonicTimeNs() < mixed_older_wait_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(mixed_runtime.stats().backpressure_events > mixed_events_before);

    var mixed_younger = MixedWaiter{
        .runtime = &mixed_runtime,
        .index_name = "mixed-b",
        .segment_count = 1,
        .byte_count = 20,
        .acquisition_counter = &mixed_acquisition_counter,
        .acquisition_order = &mixed_younger_order,
        .release_gate = &mixed_release,
        .failed = &mixed_younger_failed,
    };
    var mixed_younger_thread = try std.testing.io.concurrent(MixedWaiter.run, .{&mixed_younger});
    var mixed_younger_joined = false;
    defer if (!mixed_younger_joined) {
        if (mixed_segment_blocker_active) {
            mixed_segment_blocker.release();
            mixed_segment_blocker_active = false;
        }
        if (mixed_byte_blocker_active) {
            mixed_byte_blocker.release();
            mixed_byte_blocker_active = false;
        }
        mixed_release.store(true, .release);
        mixed_younger_thread.await(std.testing.io);
    };
    const mixed_younger_wait_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (mixed_runtime.stats().backpressure_events < mixed_events_before + 2 and db_mod.monotonicTimeNs() < mixed_younger_wait_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(mixed_runtime.stats().backpressure_events >= mixed_events_before + 2);
    try std.testing.expectEqual(@as(u32, 0), mixed_older_order.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), mixed_younger_order.load(.acquire));

    mixed_segment_blocker.release();
    mixed_segment_blocker_active = false;
    mixed_byte_blocker.release();
    mixed_byte_blocker_active = false;
    const mixed_older_acquired_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (mixed_older_order.load(.acquire) == 0 and db_mod.monotonicTimeNs() < mixed_older_acquired_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expectEqual(@as(u32, 1), mixed_older_order.load(.acquire));
    try std.testing.expectEqual(@as(u32, 0), mixed_younger_order.load(.acquire));
    try std.testing.expect(!mixed_older_failed.load(.acquire));
    try std.testing.expect(!mixed_younger_failed.load(.acquire));
    mixed_release.store(true, .release);
    mixed_older_thread.await(std.testing.io);
    mixed_older_joined = true;
    const mixed_younger_acquired_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (mixed_younger_order.load(.acquire) == 0 and db_mod.monotonicTimeNs() < mixed_younger_acquired_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expectEqual(@as(u32, 2), mixed_younger_order.load(.acquire));
    mixed_younger_thread.await(std.testing.io);
    mixed_younger_joined = true;

    var blocking_permit = try fair_runtime.acquireProducerPermit("admission-test", 80, 0);
    var blocking_active = true;
    defer if (blocking_active) blocking_permit.release();

    const FairWaiter = struct {
        runtime: *db_mod.text_merge_runtime_mod.TextMergeRuntime,
        segment_count: u64,
        acquired: *std.atomic.Value(bool),
        release_gate: ?*const std.atomic.Value(bool),

        fn run(self: *@This()) void {
            var permit = self.runtime.acquireProducerPermit("admission-test", self.segment_count, 0) catch return;
            self.acquired.store(true, .release);
            if (self.release_gate) |gate| {
                while (!gate.load(.acquire)) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
            }
            permit.release();
        }
    };
    var large_acquired = std.atomic.Value(bool).init(false);
    var small_acquired = std.atomic.Value(bool).init(false);
    var release_large = std.atomic.Value(bool).init(false);
    const fair_events_before = fair_runtime.stats().backpressure_events;
    var large_waiter = FairWaiter{
        .runtime = &fair_runtime,
        .segment_count = 95,
        .acquired = &large_acquired,
        .release_gate = &release_large,
    };
    var large_thread = try std.testing.io.concurrent(FairWaiter.run, .{&large_waiter});
    var large_joined = false;
    defer if (!large_joined) {
        if (blocking_active) {
            blocking_permit.release();
            blocking_active = false;
        }
        release_large.store(true, .release);
        large_thread.await(std.testing.io);
    };
    const large_wait_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (fair_runtime.stats().backpressure_events < fair_events_before + 1 and db_mod.monotonicTimeNs() < large_wait_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(fair_runtime.stats().backpressure_events >= fair_events_before + 1);

    var small_waiter = FairWaiter{
        .runtime = &fair_runtime,
        .segment_count = 10,
        .acquired = &small_acquired,
        .release_gate = null,
    };
    var small_thread = try std.testing.io.concurrent(FairWaiter.run, .{&small_waiter});
    var small_joined = false;
    defer if (!small_joined) {
        if (blocking_active) {
            blocking_permit.release();
            blocking_active = false;
        }
        release_large.store(true, .release);
        small_thread.await(std.testing.io);
    };
    const small_wait_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (fair_runtime.stats().backpressure_events < fair_events_before + 2 and db_mod.monotonicTimeNs() < small_wait_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(fair_runtime.stats().backpressure_events >= fair_events_before + 2);
    try std.testing.expect(!small_acquired.load(.acquire));

    blocking_permit.release();
    blocking_active = false;
    const fair_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (!large_acquired.load(.acquire) and db_mod.monotonicTimeNs() < fair_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(large_acquired.load(.acquire));
    try std.testing.expect(!small_acquired.load(.acquire));
    release_large.store(true, .release);
    large_thread.await(std.testing.io);
    large_joined = true;
    small_thread.await(std.testing.io);
    small_joined = true;
    try std.testing.expect(small_acquired.load(.acquire));

    // Runtime cancellation is not a capacity timeout and must leave the FIFO
    // queue usable by subsequent producers.
    var cancel_blocker = try fair_runtime.acquireProducerPermit("admission-test", 80, 0);
    var cancel_blocker_active = true;
    defer if (cancel_blocker_active) cancel_blocker.release();
    const CancelWaiter = struct {
        runtime: *db_mod.text_merge_runtime_mod.TextMergeRuntime,
        outcome: *std.atomic.Value(u8),

        fn run(self: *@This()) void {
            var permit = self.runtime.acquireProducerPermit("admission-test", 30, 0) catch |err| {
                self.outcome.store(if (err == error.Canceled) 1 else 2, .release);
                return;
            };
            permit.release();
            self.outcome.store(3, .release);
        }
    };
    var cancel_outcome = std.atomic.Value(u8).init(0);
    var cancel_waiter = CancelWaiter{ .runtime = &fair_runtime, .outcome = &cancel_outcome };
    const cancel_events_before = fair_runtime.stats().backpressure_events;
    const timeouts_before = fair_runtime.stats().backpressure_timeouts;
    const fair_io = db.backend_runtime.io().?;
    var cancel_group: std.Io.Group = .init;
    try cancel_group.concurrent(fair_io, CancelWaiter.run, .{&cancel_waiter});
    var cancel_group_active = true;
    defer if (cancel_group_active) cancel_group.cancel(fair_io);
    const cancel_wait_deadline = db_mod.monotonicTimeNs() +| std.time.ns_per_s;
    while (fair_runtime.stats().backpressure_events == cancel_events_before and db_mod.monotonicTimeNs() < cancel_wait_deadline) std.testing.io.sleep(.fromNanoseconds(1), .awake) catch {};
    try std.testing.expect(fair_runtime.stats().backpressure_events > cancel_events_before);
    cancel_group.cancel(fair_io);
    cancel_group_active = false;
    try std.testing.expectEqual(@as(u8, 1), cancel_outcome.load(.acquire));
    try std.testing.expectEqual(timeouts_before, fair_runtime.stats().backpressure_timeouts);
    cancel_blocker.release();
    cancel_blocker_active = false;
    var post_cancel_permit = try fair_runtime.acquireProducerPermit("admission-test", 1, 0);
    post_cancel_permit.release();

    // Segment reservations are isolated by index, while the byte budget stays
    // shared. A large publication cannot create cross-index head-of-line
    // blocking for query-fanout capacity.
    var index_a_permit = try fair_runtime.acquireProducerPermit("admission-a", 95, 0);
    var index_b_permit = try fair_runtime.acquireProducerPermit("admission-b", 95, 0);
    index_b_permit.release();
    index_a_permit.release();

    // Segment publications must be chunked by callers and can never bypass
    // the hard fanout limit. A byte-oversized indivisible chunk may still make
    // exclusive progress when its segment reservation is within the limit.
    try std.testing.expectError(
        error.TextPublicationExceedsSegmentLimit,
        admission_runtime.acquireProducerPermit("ft_v1", 9, 0),
    );
    var oversized_byte_permit = try admission_runtime.acquireProducerPermit("ft_v1", 1, 200);
    oversized_byte_permit.release();

    // Pausing the background worker for a structural mutation must not poison
    // foreground admission while capacity is available.
    try std.testing.expect(admission_runtime.pause());
    var paused_permit = try admission_runtime.acquireProducerPermit("ft_v1", 1, 10);
    paused_permit.release();
    try admission_runtime.resumeAfterPause();

    const text_index = db.core.index_manager.textIndex("ft_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(u64, 24), text_index.snapshot().liveDocCount());
}

test "db text kernel admits natural segments below hard segment limit" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .text_merge = .{
            .enabled = true,
            .idle_interval_ms = 10_000,
            .error_interval_ms = 10_000,
            .max_pending_segments = 4,
            .resume_pending_segments = 2,
            .max_pending_bytes = 0,
        },
    });
    defer db.close();
    try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });

    const runtime = db.indexes.text.merge_runtime orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 2), runtime.producerSegmentReservationLimit());
    try std.testing.expect(runtime.pause());

    const fields = [_]db_mod.introducer_mod.TextField{.{ .field_name = "body", .text = "bounded publication" }};
    const docs = [_]db_mod.introducer_mod.TextDocument{
        .{ .id = "doc:0", .stored_data = "{}", .text_fields = &fields, .doc_ordinal = 1 },
        .{ .id = "doc:1", .stored_data = "{}", .text_fields = &fields, .doc_ordinal = 2 },
        .{ .id = "doc:2", .stored_data = "{}", .text_fields = &fields, .doc_ordinal = 3 },
        .{ .id = "doc:3", .stored_data = "{}", .text_fields = &fields, .doc_ordinal = 4 },
        .{ .id = "doc:4", .stored_data = "{}", .text_fields = &fields, .doc_ordinal = 5 },
    };
    try std.testing.expectEqual(@as(usize, 1), try db.indexTextKernelDocuments("ft_v1", &docs));
    const stats = db.core.index_manager.textMergeStatsSnapshotForIndex("ft_v1");
    try std.testing.expectEqual(@as(u64, 1), stats.active_segments);
    try std.testing.expect(stats.active_segments <= 4);
}

test "db derived text replay admits natural segments below hard segment limit" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .text_merge = .{
            .enabled = true,
            .idle_interval_ms = 10_000,
            .error_interval_ms = 10_000,
            .max_pending_segments = 4,
            .resume_pending_segments = 2,
            .max_pending_bytes = 0,
        },
    });
    defer db.close();
    try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });

    const runtime = db.indexes.text.merge_runtime orelse return error.TestUnexpectedResult;
    try std.testing.expect(runtime.pause());
    defer runtime.resumeAfterPause() catch {};

    try db.batch(.{
        .writes = &.{
            .{ .key = "doc:0", .value = "{\"body\":\"bounded publication zero\"}" },
            .{ .key = "doc:1", .value = "{\"body\":\"bounded publication one\"}" },
            .{ .key = "doc:2", .value = "{\"body\":\"bounded publication two\"}" },
            .{ .key = "doc:3", .value = "{\"body\":\"bounded publication three\"}" },
            .{ .key = "doc:4", .value = "{\"body\":\"bounded publication four\"}" },
        },
        .sync_level = .full_index,
    });

    const stats = db.core.index_manager.textMergeStatsSnapshotForIndex("ft_v1");
    try std.testing.expectEqual(@as(u64, 1), stats.active_segments);
    try std.testing.expect(stats.active_segments <= 4);
    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "bounded" } },
        .limit = 10,
    });
    defer result.deinit();
    try std.testing.expectEqual(@as(u32, 5), result.total_hits);
}

test "db runUntilIdle drains scheduled text merges without index workers" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
    });
    defer db.close();

    try std.testing.expect(db.indexes.text.merge_runtime == null);

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    for (0..6) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"title\":\"doc {d}\",\"body\":\"common token {d}\"}}", .{ i, i });
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{
                .key = key,
                .value = value,
            }},
            .sync_level = .write,
        });
    }

    const before = db.pendingWorkStats().text_merge;

    try db.runUntilIdle();

    const after = db.pendingWorkStats().text_merge;
    try std.testing.expect(after.pending_segments <= before.pending_segments);

    const text_index = db.core.index_manager.textIndex("ft_v1").?;
    try std.testing.expect(text_index.snapshot().segments.len < 6);
}

test "db full_text sync does not require draining scheduled text merges" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    for (0..6) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_text,
        });
    }

    var result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match = .{ .field = "body", .text = "common" } },
        .limit = 10,
    });
    defer result.deinit();

    try std.testing.expectEqual(@as(u32, 6), result.total_hits);

    const before = db.pendingWorkStats().text_merge;
    try db.indexes.text.drainScheduledMerges();
    const after = db.pendingWorkStats().text_merge;
    try std.testing.expect(after.pending_segments <= before.pending_segments);
}

test "db bulk ingest keeps full text merge maintenance live" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .text_merge = .{
            .enabled = true,
            .idle_interval_ms = 10_000,
            .error_interval_ms = 10_000,
        },
    });
    defer db.close();

    try db.addIndex(.{
        .name = "ft_v1",
        .kind = .full_text,
        .config_json = "{}",
    });

    const runtime = db.indexes.text.merge_runtime orelse return error.TestUnexpectedResult;
    // Keep background execution deterministic; runOnce below proves that the
    // dense bulk deferral flag no longer suppresses text maintenance.
    try std.testing.expect(runtime.pause());

    try db.beginBulkIngestSession();
    errdefer db.abortBulkIngestSession();

    for (0..12) |i| {
        const key = try std.fmt.allocPrint(alloc, "doc:{d}", .{i});
        defer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"body\":\"common token {d}\"}}", .{i});
        defer alloc.free(value);

        try db.batch(.{
            .writes = &.{.{ .key = key, .value = value }},
            .sync_level = .full_text,
        });
    }

    const before = db.pendingWorkStats().text_merge;
    try std.testing.expect(before.pending_segments > 0);
    try std.testing.expect(db.async_context.text_merge_deferred.load(.acquire));

    try std.testing.expect(try runtime.runOnce());
    try runtime.resumeAfterPause();

    try db.finishBulkIngestSessionWithOptions(.{ .compact = false });
    try std.testing.expect(!db.async_context.text_merge_deferred.load(.acquire));

    try db.runUntilIdle();

    const after = db.pendingWorkStats().text_merge;
    try std.testing.expect(after.pending_segments <= before.pending_segments);
}
