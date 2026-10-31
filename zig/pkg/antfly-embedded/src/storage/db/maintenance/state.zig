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
