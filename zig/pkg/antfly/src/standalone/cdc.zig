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

//! Standalone scheduling for PostgreSQL change data capture.
//!
//! Cluster metadata runs the snapshot backfill and streaming replication
//! coordinators from its Raft leader. A standalone process is the only owner
//! of its catalog, so it runs the same coordinators from its control loop.
//! This file owns only the job lifecycle: one bounded round at a time on the
//! backend runtime's maintenance class, a fixed cadence between rounds, and a
//! shutdown that closes the job owner before the catalog goes away.

const std = @import("std");
const platform_time = @import("antfly_platform").time;
const foreign_mod = @import("../foreign/mod.zig");
const background_runtime = @import("antfly_local_sources").storage_db_selected_root.db.background_runtime;
const replication_backfill = @import("../metadata/replication_backfill.zig");

/// Matches the cluster metadata cadence.
pub const round_interval_ms: u64 = 1_000;

pub const Scheduler = struct {
    registry: foreign_mod.Registry = .{},
    registry_ready: bool = false,
    mutex: std.Io.Mutex = .init,
    in_flight: std.atomic.Value(bool) = .init(false),
    shutdown_requested: std.atomic.Value(bool) = .init(false),
    owner_id: u64 = 0,
    next_round_at_ms: u64 = 0,
    runtime: ?*background_runtime.BackendRuntime = null,
    // Resume points for the snapshot and streaming coordinators, carried
    // across rounds by this long-lived scheduler so a source stuck past its
    // round's quantum does not starve sources later in the table/source
    // list on every subsequent round (see SourceRoundCursor). In-memory
    // scheduling state only: it resets to the top of the list on restart or
    // on a leadership change that recreates this scheduler, which is fine —
    // the worst case is one extra round of the old ordering, not lost work.
    // Guarded by `mutex`, the same lock that already serializes this
    // scheduler's other round state.
    snapshot_cursor: replication_backfill.SourceRoundCursor = .{},
    streaming_cursor: replication_backfill.SourceRoundCursor = .{},

    /// Submit one round unless a round is in flight, the cadence has not
    /// elapsed, or shutdown started. `run` executes on a maintenance worker.
    pub fn schedule(
        self: *Scheduler,
        alloc: std.mem.Allocator,
        runtime: *background_runtime.BackendRuntime,
        context: anytype,
        comptime run: fn (@TypeOf(context)) anyerror!void,
        comptime is_expected_error: fn (anyerror) bool,
    ) !void {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        if (self.shutdown_requested.load(.acquire)) return;
        if (self.in_flight.load(.acquire)) return;
        const now_ms: u64 = @intCast(@divTrunc(platform_time.monotonicNs(), std.time.ns_per_ms));
        if (now_ms < self.next_round_at_ms) return;
        if (!self.registry_ready) {
            try foreign_mod.registerDefaultPostgresExecutor(alloc, &self.registry);
            self.registry_ready = true;
        }
        if (self.owner_id == 0) self.owner_id = try runtime.allocOwnerId();
        self.runtime = runtime;

        const Context = @TypeOf(context);
        const Job = struct {
            alloc: std.mem.Allocator,
            scheduler: *Scheduler,
            context: Context,

            fn runJob(ptr: *anyopaque) !void {
                const job: *@This() = @ptrCast(@alignCast(ptr));
                run(job.context) catch |err| {
                    if (is_expected_error(err)) {
                        std.log.warn("standalone CDC round deferred: {s}", .{@errorName(err)});
                    } else {
                        std.log.err("standalone CDC round failed: {s}", .{@errorName(err)});
                    }
                };
            }

            fn deinitJob(ptr: *anyopaque) void {
                const job: *@This() = @ptrCast(@alignCast(ptr));
                job.scheduler.in_flight.store(false, .release);
                job.alloc.destroy(job);
            }
        };

        const job = try alloc.create(Job);
        job.* = .{ .alloc = alloc, .scheduler = self, .context = context };
        self.in_flight.store(true, .release);
        runtime.durable_jobs.submit(.{
            .owner_id = self.owner_id,
            .class = .maintenance,
            .ptr = job,
            .run = Job.runJob,
            .deinit = Job.deinitJob,
        }) catch |err| {
            Job.deinitJob(job);
            return err;
        };
        self.next_round_at_ms = now_ms + round_interval_ms;
    }

    /// Fence new rounds and wait for the in-flight round to release its owner.
    pub fn shutdown(self: *Scheduler) void {
        self.shutdown_requested.store(true, .release);
        self.mutex.lockUncancelable(std.Options.debug_io);
        const runtime = self.runtime;
        const owner_id = self.owner_id;
        self.mutex.unlock(std.Options.debug_io);
        if (runtime) |value| {
            if (owner_id != 0) value.durable_jobs.closeOwner(owner_id);
        }
    }

    pub fn deinit(self: *Scheduler, alloc: std.mem.Allocator) void {
        self.registry.deinit(alloc);
    }

    /// Read both round cursors under the same lock that guards the rest of
    /// this scheduler's round state. A round job is single-flight
    /// (`in_flight`), but this keeps cursor access consistent with every
    /// other field here rather than relying on that alone.
    pub fn loadCursors(self: *Scheduler) struct {
        snapshot: replication_backfill.SourceRoundCursor,
        streaming: replication_backfill.SourceRoundCursor,
    } {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        return .{ .snapshot = self.snapshot_cursor, .streaming = self.streaming_cursor };
    }

    /// Persist both round cursors after a round, under the same lock.
    pub fn storeCursors(
        self: *Scheduler,
        snapshot: replication_backfill.SourceRoundCursor,
        streaming: replication_backfill.SourceRoundCursor,
    ) void {
        self.mutex.lockUncancelable(std.Options.debug_io);
        defer self.mutex.unlock(std.Options.debug_io);
        self.snapshot_cursor = snapshot;
        self.streaming_cursor = streaming;
    }
};

test "standalone cdc scheduler persists independent snapshot and streaming cursors across rounds" {
    var scheduler = Scheduler{};
    defer scheduler.deinit(std.testing.allocator);

    // A fresh scheduler, like a fresh process, starts every coordinator at
    // the top of its table/source list.
    const initial = scheduler.loadCursors();
    try std.testing.expect(!initial.snapshot.valid);
    try std.testing.expect(!initial.streaming.valid);

    // runCdcRoundJob (standalone) and runReplicationBackfillWorker (cluster)
    // both load the pair, hand each half to its own coordinator for one
    // round, and store the pair back -- this is that load/mutate/store
    // cycle, repeated across rounds, without a data server or registry.
    var cursors = scheduler.loadCursors();
    cursors.snapshot = .{ .valid = true, .table_id = 7, .source_ordinal = 2 };
    scheduler.storeCursors(cursors.snapshot, cursors.streaming);

    const after_snapshot_round = scheduler.loadCursors();
    try std.testing.expect(after_snapshot_round.snapshot.valid);
    try std.testing.expectEqual(@as(u64, 7), after_snapshot_round.snapshot.table_id);
    try std.testing.expectEqual(@as(u32, 2), after_snapshot_round.snapshot.source_ordinal);
    // The streaming cursor is a separate resume point: storing a new
    // snapshot cursor must not disturb it.
    try std.testing.expect(!after_snapshot_round.streaming.valid);

    var cursors2 = scheduler.loadCursors();
    cursors2.streaming = .{ .valid = true, .table_id = 9, .source_ordinal = 1 };
    scheduler.storeCursors(cursors2.snapshot, cursors2.streaming);

    const after_streaming_round = scheduler.loadCursors();
    // The snapshot cursor from the earlier round survives this round that
    // only touched streaming -- the persistence this scheduler exists for.
    try std.testing.expect(after_streaming_round.snapshot.valid);
    try std.testing.expectEqual(@as(u64, 7), after_streaming_round.snapshot.table_id);
    try std.testing.expect(after_streaming_round.streaming.valid);
    try std.testing.expectEqual(@as(u64, 9), after_streaming_round.streaming.table_id);
    try std.testing.expectEqual(@as(u32, 1), after_streaming_round.streaming.source_ordinal);
}
