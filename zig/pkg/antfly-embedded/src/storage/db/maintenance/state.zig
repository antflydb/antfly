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

/// Maintenance state owned by `DB`: quarantined index retries, independent maintenance, local source recovery and run-until-idle diagnostics.
pub const Maintenance = struct {
    quarantine: db_mod.quarantine_recovery.Owner = .{},
    independent: db_mod.independent_maintenance.Owner = .{},
    local_source: LocalSource = .{},
    run_until_idle: RunUntilIdle = .{},

    /// Local source recovery and completion audit flags.
    pub const LocalSource = struct {
        completion_audit_pending: bool = true,
        recovery_pending: bool = false,
    };

    /// The no-progress timeout for run-until-idle and the last no-progress diagnostic.
    pub const RunUntilIdle = struct {
        /// Bounded no-progress guard for `runUntilIdle` (see `OpenOptions.
        /// run_until_idle_no_progress_timeout_ms` and `ReplayDrainOptions.
        /// no_progress_timeout_ns`); 0 disables it. Copied into `ReplayDrainOptions`
        /// only by `runUntilIdle`, so a busy but progressing background replay is
        /// never subject to it.
        no_progress_timeout_ns: u64 = db_mod.default_run_until_idle_no_progress_timeout_ns,
        /// Most recent `error.RunUntilIdleNoProgress` diagnostic, retained past the
        /// error return for callers (tests, capi JSON status) that want the exact
        /// stuck index and its indexed/expected counters without re-parsing a log
        /// line. Overwritten on each new occurrence; freed at `deinitWrapperState`.
        last_no_progress: ?DB.NoProgressDiagnostic = null,
    };

    fn dbPtr(self: *Maintenance) *DB {
        return @alignCast(@fieldParentPtr("maintenance", self));
    }

    fn dbConstPtr(self: *const Maintenance) *const DB {
        return @alignCast(@fieldParentPtr("maintenance", self));
    }

    pub fn setLastRunUntilIdleNoProgressDiagnostic(
        self: *Maintenance,
        index_name: []const u8,
        indexed: u64,
        expected: u64,
        stuck_ns: u64,
    ) !void {
        const db = self.dbPtr();
        const owned_name = try db.alloc.dupe(u8, index_name);
        if (self.run_until_idle.last_no_progress) |*previous| previous.deinit(db.alloc);
        self.run_until_idle.last_no_progress = .{
            .index_name = owned_name,
            .indexed = indexed,
            .expected = expected,
            .stuck_ns = stuck_ns,
        };
    }

    /// Read-only view of the most recent no-progress diagnostic, if any.
    /// Valid until the next `runUntilIdle` call or `DB.close`.
    pub fn lastRunUntilIdleNoProgressDiagnostic(self: *const Maintenance) ?DB.NoProgressDiagnostic {
        return self.run_until_idle.last_no_progress;
    }

    /// Re-attempts opening quarantined indexes (artifacts that failed to
    /// load at open time). `force` ignores per-index backoff — used by tests
    /// and operator-triggered recovery. Runs the same detached-open path as
    /// the initial load, so a recovered full-text/sparse/graph index resumes
    /// its persisted rebuild state and catches up before registering.
    pub fn retryQuarantinedIndexLoads(self: *Maintenance, force: bool) !db_mod.index_manager_mod.IndexManager.QuarantineRetryResult {
        const db = self.dbPtr();
        var publication_fence = DB.QuarantineIndexPublicationFence{ .db = db };
        const execution_limit = if (force)
            db.core.index_manager.failedIndexLoadSummary().total
        else
            @as(usize, 8);
        return try db.core.index_manager.retryFailedIndexLoads(
            db.core.store,
            db_mod.monotonicTimeNs(),
            force,
            null,
            execution_limit,
            publication_fence.iface(),
        );
    }

    /// Retry exactly one quarantined index. Ignoring backoff is orthogonal to
    /// scope: targeted rollback and operator repair must never inspect, clone,
    /// open, or publish unrelated failed indexes.
    pub fn retryQuarantinedIndexLoad(
        self: *Maintenance,
        index_name: []const u8,
        force: bool,
    ) !db_mod.index_manager_mod.IndexManager.QuarantineRetryResult {
        const db = self.dbPtr();
        const failure = db.core.index_manager.failedIndexLoadSnapshot(index_name) orelse
            return .{
                .remaining = db.core.index_manager.failedIndexLoadSummary().total,
                .target_outcome = .absent,
            };
        return try self.retryQuarantinedIndexLoadTarget(.{
            .name = index_name,
            .token = failure.token,
        }, force);
    }

    pub fn retryQuarantinedIndexLoadTarget(
        self: *Maintenance,
        target: db_mod.index_manager_mod.IndexManager.FailedIndexLoadRetryTarget,
        force: bool,
    ) !db_mod.index_manager_mod.IndexManager.QuarantineRetryResult {
        const db = self.dbPtr();
        var publication_fence = DB.QuarantineIndexPublicationFence{ .db = db };
        return try db.core.index_manager.retryFailedIndexLoads(
            db.core.store,
            db_mod.monotonicTimeNs(),
            force,
            target,
            1,
            publication_fence.iface(),
        );
    }

    /// Start only after the DB has reached its final address. The scheduler
    /// registration retains `self` until its stop/join boundary.
    pub fn startQuarantineRetryWorkerIfNeeded(self: *Maintenance) void {
        const db = self.dbPtr();
        if (comptime !builtin.is_test and (builtin.single_threaded or builtin.os.tag == .freestanding)) return;
        if (!builtin.is_test and (!db.local_execution.optional_runtime_workers_enabled or db.open_mode != .writer)) return;
        if (db.open_mode == .writer and db.physical_root_mode == .filesystem_managed and db.primary_backend == .lsm and
            db.core.identity_namespace.table_id != 0 and db.backend_runtime.query_cut_repository != null and
            db.backend_runtime.query_cut_repository.?.vtable.warm != null and db.backend_runtime.query_cut_repository.?.limits.max_cuts > 1)
            db.query.warming.start(.{ .ptr = db, .runtime = db.backend_runtime, .turn = DB.warmNativeCheckpoint });
        self.quarantine.start(.{
            .ptr = db,
            .runtime = db.backend_runtime,
            .has_failures = quarantineHasFailures,
            .retry = retryQuarantineFromOwner,
        });
    }

    fn quarantineHasFailures(ptr: *anyopaque) bool {
        const db: *DB = @ptrCast(@alignCast(ptr));
        return db.core.index_manager.hasLoadFailures();
    }

    fn retryQuarantineFromOwner(ptr: *anyopaque) !usize {
        const db: *DB = @ptrCast(@alignCast(ptr));
        return (try db.maintenance.retryQuarantinedIndexLoads(false)).remaining;
    }

    pub fn quarantineRetryWorkerStartedAtCurrentAddressForTest(self: *const Maintenance) bool {
        const db = self.dbConstPtr();
        return builtin.is_test and self.quarantine.start_address_for_test == @intFromPtr(db);
    }

    pub fn stopQuarantineRetryWorker(self: *Maintenance) void {
        const db = self.dbPtr();
        self.quarantine.stop(db.backend_runtime);
    }

    /// Queries that explicitly reference an index whose persisted artifacts
    /// failed to load get a distinct error instead of IndexNotFound, so
    /// clients can tell "broken, drop+recreate to recover" from "missing".
    pub fn failIfIndexQuarantined(self: *Maintenance, index_name: ?[]const u8) !void {
        const db = self.dbPtr();
        const name = index_name orelse return;
        switch (db.observeResidentIndexAdmission(db.alloc, name, null)) {
            .admitted => {},
            .rebuilding => return error.IndexRebuilding,
            .unavailable => return error.IndexUnavailable,
        }
    }
};

test "owned DB open starts self-retaining workers at the stable allocation" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(alloc, ".zig-cache/tmp/{s}/owned-db-worker", .{tmp.sub_path});
    defer alloc.free(path);

    const db = try DB.openOwned(alloc, path, .{});
    defer db.closeOwned();
    try std.testing.expect(db.maintenance.quarantineRetryWorkerStartedAtCurrentAddressForTest());
}

test "db query repair gate revalidates stale debt" {
    const alloc = std.testing.allocator;
    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{ .start_index_workers = false });
    defer db.close();
    try db.addIndex(.{
        .name = "stale_repair",
        .kind = .full_text,
        .config_json = "{}",
    });

    // Model a reader that loaded repair debt immediately before its durable
    // intent was removed. The first rejected request proves absence and heals
    // the process-local gate instead of wedging the reader indefinitely.
    try db.core.index_manager.markUnscopedRepairUnavailable("stale_repair");
    try db.maintenance.failIfIndexQuarantined("stale_repair");
    try std.testing.expect(!db.core.index_manager.repairUnavailable("stale_repair"));
}

// `checkTargetAdvanceNoProgress` is `runUntilIdle`'s opt-in stall guard (task
// requirement: fail fast with a named, bounded diagnostic instead of
// spinning on `canAdvanceDerivedToTargetAsync`'s deferred artifact-maintenance
// check indefinitely). Exercised directly against a real (otherwise idle) DB
// rather than by reproducing a genuinely broken pipeline, so the pass/fail
// signal is deterministic and does not depend on timing.
test "runUntilIdle no-progress guard fails fast with a named stuck-index diagnostic" {
    @import("antfly_test_error_logs").expectErrorLogs(1);
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .start_index_workers = false,
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();

    try std.testing.expect(db.maintenance.lastRunUntilIdleNoProgressDiagnostic() == null);

    const now_ns = db_mod.monotonicTimeNs();
    // Simulate `canAdvanceDerivedToTargetAsync` having observed
    // "chunk_vectors" stuck (indexed=5017, expected_docs=0, matching the
    // dogfood reproduction) for longer than the configured timeout.
    try db_mod.noteTargetAdvanceStuck(db.async_context, "chunk_vectors", now_ns -| (2 * std.time.ns_per_s), 5017, 0);
    try std.testing.expectError(error.RunUntilIdleNoProgress, db.checkTargetAdvanceNoProgress(1 * std.time.ns_per_s));

    const diagnostic = db.maintenance.lastRunUntilIdleNoProgressDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("chunk_vectors", diagnostic.index_name);
    try std.testing.expectEqual(@as(u64, 5017), diagnostic.indexed);
    try std.testing.expectEqual(@as(u64, 0), diagnostic.expected);

    // A shorter observed duration than the configured timeout must not fire.
    db_mod.clearTargetAdvanceStuck(db.async_context, "chunk_vectors");
    try db_mod.noteTargetAdvanceStuck(db.async_context, "chunk_vectors", now_ns, 12, 0);
    try db.checkTargetAdvanceNoProgress(60 * std.time.ns_per_s);
}

test "runUntilIdle no-progress guard interrupts the threaded derived wait" {
    // Reviewer-reported P2: with background index workers the guard only ran
    // AFTER runDerivedUntilWithOptions returned, and the threaded
    // waitForAll has no deadline — a derived index that can never satisfy
    // its replay target parked the wait forever and the configured timeout
    // never fired. The stall diagnostic now rides the wait's cancellation
    // checkpoint, so the wait itself is interrupted. This test exercises the
    // real threaded wait path: workers enabled, a target the workers cannot
    // reach, and a stuck record older than the timeout. Environments whose io
    // backend starts no derived workers skip (the guard mechanics have their
    // own unit test below); the expected-error-log declaration must come
    // AFTER the skip decision or the boundary check flags the skip itself.
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    var db = try DB.open(alloc, std.mem.span(path), .{
        .ttl_cleanup = .{ .enabled = false },
    });
    defer db.close();
    if (!db.executor.hasWorkers()) return error.SkipZigTest;
    @import("antfly_test_error_logs").expectErrorLogs(1);

    try db.batch(.{
        .writes = &.{.{ .key = "doc:a", .value = "{\"title\":\"a\"}" }},
        .sync_level = .write,
    });

    const now_ns = db_mod.monotonicTimeNs();
    try db_mod.noteTargetAdvanceStuck(db.async_context, "chunk_vectors", now_ns -| (2 * std.time.ns_per_s), 5017, 0);
    // A target far past anything committed: without the in-wait guard this
    // call never returns on a stuck index.
    const unreachable_sequence = db.core.nextDerivedSequence() + 1_000_000;
    try std.testing.expectError(error.RunUntilIdleNoProgress, db.runDerivedUntilWithOptions(unreachable_sequence, .{
        .no_progress_timeout_ns = 1 * std.time.ns_per_s,
    }));

    const diagnostic = db.maintenance.lastRunUntilIdleNoProgressDiagnostic() orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("chunk_vectors", diagnostic.index_name);
    db_mod.clearTargetAdvanceStuck(db.async_context, "chunk_vectors");
}

test "db quarantined index self-heals via retryQuarantinedIndexLoads" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();

        var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }
        for (0..300) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"alpha\",\"n\":{d}}}", .{i}),
            });
        }
        try db.batch(.{ .writes = writes.items });
        try db.core.index_manager.addAllNoBackfill(db.core.store, &.{.{
            .name = "ft_v1",
            .kind = .full_text,
            .config_json = "{}",
        }});
    }

    db_mod.index_manager_mod.test_abort_text_backfill_after_batches = 1;
    defer db_mod.index_manager_mod.test_abort_text_backfill_after_batches = null;

    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") != null);
    try std.testing.expect(db.core.textIndexEntry("ft_v1") == null);

    // Writes keep flowing while the index is quarantined; the heal's resumed
    // backfill must pick them up. They also guarantee the next backfill
    // attempt flushes at least once, so the still-set abort knob fires.
    {
        var writes = std.ArrayListUnmanaged(db_mod.types.BatchWrite).empty;
        defer {
            for (writes.items) |item| {
                alloc.free(@constCast(item.key));
                alloc.free(@constCast(item.value));
            }
            writes.deinit(alloc);
        }
        for (300..400) |i| {
            try writes.append(alloc, .{
                .key = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i}),
                .value = try std.fmt.allocPrint(alloc, "{{\"title\":\"alpha\",\"n\":{d}}}", .{i}),
            });
        }
        try db.batch(.{ .writes = writes.items });
    }

    // While the cause persists, a forced retry fails and applies backoff.
    {
        const result = try db.maintenance.retryQuarantinedIndexLoads(true);
        try std.testing.expectEqual(@as(usize, 0), result.recovered);
        try std.testing.expectEqual(@as(usize, 1), result.remaining);
    }
    // Cause fixed — but the failed attempt set a backoff deadline, so a
    // non-forced retry is skipped without re-attempting the open (it would
    // recover if it attempted).
    db_mod.index_manager_mod.test_abort_text_backfill_after_batches = null;
    {
        const result = try db.maintenance.retryQuarantinedIndexLoads(false);
        try std.testing.expectEqual(@as(usize, 0), result.recovered);
        try std.testing.expectEqual(@as(usize, 1), result.remaining);
    }

    // Forced retry recovers the index in-process — no reopen, no
    // drop+recreate — resuming the persisted rebuild state. Hold a simulated
    // query lease across the final publication and prove recovery waits for
    // it rather than reallocating the inline catalog beneath a reader.
    const RetryState = struct {
        db: *DB,
        result: ?db_mod.index_manager_mod.IndexManager.QuarantineRetryResult = null,
        err: ?anyerror = null,
        completed: std.atomic.Value(bool) = .init(false),

        fn run(state: *@This()) void {
            state.result = state.db.maintenance.retryQuarantinedIndexLoads(true) catch |err| {
                state.err = err;
                state.completed.store(true, .release);
                return;
            };
            state.completed.store(true, .release);
        }
    };
    db_mod.test_quarantine_publication_fence_entered.store(false, .release);
    defer db_mod.test_quarantine_publication_fence_entered.store(false, .release);
    db_mod.lockApplyShared(&db);
    var apply_shared_held = true;
    var retry_state = RetryState{ .db = &db };
    var retry_thread = try std.testing.io.concurrent(RetryState.run, .{&retry_state});
    var retry_thread_joined = false;
    defer {
        if (apply_shared_held) db.core.unlockApplyShared();
        if (!retry_thread_joined) retry_thread.await(std.testing.io);
    }

    const publication_deadline = db_mod.monotonicTimeNs() +| 30 * std.time.ns_per_s;
    while (!db_mod.test_quarantine_publication_fence_entered.load(.acquire) and db_mod.monotonicTimeNs() < publication_deadline) {
        db_mod.sleepNs(100 * std.time.ns_per_us);
    }
    const publication_fence_entered = db_mod.test_quarantine_publication_fence_entered.load(.acquire);
    const publication_waited_for_reader = publication_fence_entered and !retry_state.completed.load(.acquire);
    db.core.unlockApplyShared();
    apply_shared_held = false;
    retry_thread.await(std.testing.io);
    retry_thread_joined = true;

    try std.testing.expect(publication_fence_entered);
    try std.testing.expect(publication_waited_for_reader);
    if (retry_state.err) |err| return err;
    const result = retry_state.result orelse return error.TestUnexpectedResult;
    try std.testing.expectEqual(@as(usize, 1), result.recovered);
    try std.testing.expectEqual(@as(usize, 0), result.remaining);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
    try std.testing.expect(db.core.textIndexEntry("ft_v1") != null);

    try db.runUntilIdle();
    var search_result = try db.search(alloc, .{
        .index_name = "ft_v1",
        .query = .{ .match_all = {} },
        .limit = 500,
    });
    defer search_result.deinit();
    try std.testing.expectEqual(@as(u32, 400), search_result.total_hits);
}

test "db targeted quarantine retry does not open unrelated failed indexes" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.core.index_manager.addAllNoBackfill(db.core.store, &.{
            .{ .name = "ft_target", .kind = .full_text, .config_json = "{}" },
            .{ .name = "ft_unrelated", .kind = .full_text, .config_json = "{}" },
        });
    }

    db_mod.index_manager_mod.test_inject_index_open_error = error.InvalidIndexConfig;
    defer db_mod.index_manager_mod.test_inject_index_open_error = null;
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    try std.testing.expect(db.core.index_manager.loadFailure("ft_target") != null);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_unrelated") != null);

    db_mod.index_manager_mod.test_inject_index_open_error = null;
    const result = try db.maintenance.retryQuarantinedIndexLoad("ft_target", true);
    try std.testing.expectEqual(@as(usize, 1), result.recovered);
    try std.testing.expectEqual(@as(usize, 1), result.remaining);
    try std.testing.expectEqual(.recovered, result.target_outcome);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_target") == null);
    try std.testing.expect(db.core.textIndexEntry("ft_target") != null);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_unrelated") != null);
    try std.testing.expect(db.core.textIndexEntry("ft_unrelated") == null);
}

test "db read-only open propagates transient index load errors instead of quarantining" {
    const alloc = std.testing.allocator;

    var path_tmp = try db_mod.TestDirectory.init("db");
    defer path_tmp.cleanup();
    const path = path_tmp.path().ptr;
    defer db_mod.cleanupTempDir(path);

    {
        var db = try DB.open(alloc, std.mem.span(path), .{});
        defer db.close();
        try db.addIndex(.{ .name = "ft_v1", .kind = .full_text, .config_json = "{}" });
    }

    db_mod.index_manager_mod.test_inject_index_open_error = error.FileNotFound;
    defer db_mod.index_manager_mod.test_inject_index_open_error = null;

    // A read-only (replica) open must NOT quarantine a transient read race
    // (e.g. the writer reclaiming obsolete LSM runs mid-open): the error
    // propagates so the query layer reopens against a fresh manifest and
    // retries. A quarantined replica would instead serve IndexUnavailable
    // until the read cache next invalidates it — the quarantine retry
    // worker only runs on the writer.
    try std.testing.expectError(error.FileNotFound, DB.open(alloc, std.mem.span(path), .{
        .open_mode = .query_readonly,
    }));

    // The writer open quarantines the same error: the writer cannot race
    // its own (not yet started) reclaim, so a missing artifact there is
    // persistent damage — and the in-process retry recovers it once the
    // cause clears.
    var db = try DB.open(alloc, std.mem.span(path), .{});
    defer db.close();
    const recorded = db.core.index_manager.loadFailure("ft_v1") orelse return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("FileNotFound", recorded);

    db_mod.index_manager_mod.test_inject_index_open_error = null;
    const result = try db.maintenance.retryQuarantinedIndexLoads(true);
    try std.testing.expectEqual(@as(usize, 1), result.recovered);
    try std.testing.expectEqual(@as(usize, 0), result.remaining);
    try std.testing.expect(db.core.index_manager.loadFailure("ft_v1") == null);
}
