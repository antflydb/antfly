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
};
