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
const Io = std.Io;
const Allocator = std.mem.Allocator;
const backend_erased = @import("../../backend_erased.zig");
const transactions_mod = @import("../../transactions.zig");
const types = @import("../types.zig");
const ownership_mod = @import("../ownership.zig");
const background_runtime_mod = @import("../../background_runtime.zig");

pub const default_lease_key = "\x00\x00__metadata__:transaction_recovery_lease";

pub const RunSummary = struct {
    recovery: transactions_mod.RecoveryStats = .{},
    notification_attempts: u64 = 0,
    notification_successes: u64 = 0,
    notification_failures: u64 = 0,
    record_failures: u64 = 0,
    next_scan_after: ?transactions_mod.TxnId = null,
};

pub const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,

    pub fn deinit(self: *@This()) void {
        if (self.owned) self.store.deinit();
    }
};

pub fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = true };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

/// Shared scheduling, lease and draining lifecycle. Policy supplies only validation
/// and a bounded recovery pass; the driver has no participant or coordinator types.
pub fn Driver(comptime Policy: type) type {
    const Config = Policy.Config;
    return if (builtin.os.tag == .freestanding) struct {
        config: Config,
        stats_value: types.TransactionRecoveryStats = .{},

        pub fn init(
            alloc: Allocator,
            store: anytype,
            _: *background_runtime_mod.BackendRuntime,
            config: Config,
        ) !@This() {
            try Policy.validate(config);
            _ = alloc;
            _ = store;
            return .{
                .config = config,
                .stats_value = .{
                    .enabled = config.enabled,
                },
            };
        }

        pub fn deinit(self: *@This()) void {
            self.* = undefined;
        }

        pub fn start(self: *@This()) !void {
            if (self.config.enabled) return error.UnsupportedPlatform;
        }

        pub fn beginTeardown(_: *@This()) void {}

        pub fn stop(_: *@This()) bool {
            return false;
        }

        pub fn pause(_: *@This()) bool {
            return false;
        }

        pub fn resumeAfterPause(_: *@This()) !void {}

        pub fn ensureRunning(_: *@This()) !bool {
            return true;
        }

        pub fn isStarted(_: *const @This()) bool {
            return false;
        }

        pub fn stats(self: *@This()) types.TransactionRecoveryStats {
            return self.stats_value;
        }

        pub fn runOnce(self: *@This()) !void {
            if (self.config.enabled) return error.UnsupportedPlatform;
        }
    } else struct {
        const Self = @This();
        alloc: Allocator,
        /// Borrowed backend-neutral executor. The owning BackendRuntime keeps the
        /// implementation alive through this runtime's deinit, so the same worker
        /// lifecycle runs on Threaded and deterministic VoprIo backends.
        io: ?Io,
        store: backend_erased.Store,
        owns_store: bool,
        config: Config,
        ownership: ownership_mod.State,
        mutex: Io.Mutex = .init,
        lifecycle_mutex: std.atomic.Mutex = .unlocked,
        desired_running: bool = false,
        paused: bool = false,
        shutdown: std.atomic.Value(bool) = .init(false),
        stats_value: types.TransactionRecoveryStats = .{},
        future: ?background_runtime_mod.MaintenanceScheduler.Handle = null,
        backend_runtime: ?*background_runtime_mod.BackendRuntime = null,
        scan_after: ?transactions_mod.TxnId = null,

        pub fn init(
            alloc: Allocator,
            store: anytype,
            backend_runtime: *background_runtime_mod.BackendRuntime,
            config: Config,
        ) !Self {
            try Policy.validate(config);
            const io = backend_runtime.io();
            if (config.enabled and io == null) return error.MissingBackendRuntimeIo;
            var runtime_store = try initRuntimeStore(alloc, store);
            errdefer runtime_store.deinit();
            return .{
                .alloc = alloc,
                .io = io,
                .backend_runtime = backend_runtime,
                .store = runtime_store.store,
                .owns_store = runtime_store.owned,
                .config = config,
                .ownership = try ownership_mod.State.init(alloc, store, default_lease_key, .{
                    .lease_owned = config.lease_owned,
                    .owner_id = config.owner_id,
                    .lease_ttl_ms = config.lease_ttl_ms,
                }),
                .stats_value = .{
                    .enabled = config.enabled,
                },
            };
        }

        pub fn deinit(self: *Self) void {
            _ = self.stop();
            self.ownership.deinit(self.alloc);
            if (self.owns_store) self.store.deinit();
            self.* = undefined;
        }

        pub fn start(self: *Self) !void {
            if (!self.config.enabled) return;
            lockAtomicWithBackoff(&self.lifecycle_mutex);
            defer self.lifecycle_mutex.unlock();
            self.desired_running = true;
            self.paused = false;
            try self.startLocked();
        }

        pub fn stop(self: *Self) bool {
            if (!self.config.enabled) return false;
            lockAtomicWithBackoff(&self.lifecycle_mutex);
            defer self.lifecycle_mutex.unlock();
            self.desired_running = false;
            self.paused = true;
            return self.stopLocked();
        }

        pub fn pause(self: *Self) bool {
            if (!self.config.enabled) return false;
            lockAtomicWithBackoff(&self.lifecycle_mutex);
            defer self.lifecycle_mutex.unlock();
            self.paused = true;
            const desired = self.desired_running;
            _ = self.stopLocked();
            return desired;
        }

        pub fn resumeAfterPause(self: *Self) !void {
            if (!self.config.enabled) return;
            lockAtomicWithBackoff(&self.lifecycle_mutex);
            defer self.lifecycle_mutex.unlock();
            self.paused = false;
            if (self.desired_running) try self.startLocked();
        }

        pub fn ensureRunning(self: *Self) !bool {
            if (!self.config.enabled) return true;
            lockAtomicWithBackoff(&self.lifecycle_mutex);
            defer self.lifecycle_mutex.unlock();
            if (!self.desired_running) return true;
            if (self.paused) return false;
            try self.startLocked();
            return true;
        }

        pub fn isStarted(self: *const Self) bool {
            return self.future != null;
        }

        fn startLocked(self: *Self) !void {
            if (self.future != null or self.paused or !self.desired_running) return;
            const io = self.io orelse return error.MissingBackendRuntimeIo;
            self.mutex.lockUncancelable(io);
            self.shutdown.store(false, .release);
            self.mutex.unlock(io);
            self.future = try (try self.backend_runtime.?.maintenanceScheduler()).register(self, workerStep);
        }

        fn stopLocked(self: *Self) bool {
            const io = self.io orelse return false;
            if (self.future == null) return false;
            self.mutex.lockUncancelable(io);
            self.shutdown.store(true, .release);
            self.mutex.unlock(io);
            self.future.?.cancel(io);
            self.future = null;
            self.ownership.release();
            return true;
        }

        /// Publish shutdown without joining the worker. Borrowed deterministic
        /// schedulers use this before draining fibers; ordinary owners continue to
        /// use `stop`, which publishes the same flag and joins the future.
        pub fn beginTeardown(self: *Self) void {
            self.shutdown.store(true, .release);
        }

        pub fn stats(self: *Self) types.TransactionRecoveryStats {
            const maybe_io = self.io;
            if (maybe_io) |io| self.mutex.lockUncancelable(io);
            defer if (maybe_io) |io| self.mutex.unlock(io);
            var snapshot = self.stats_value;
            const ownership_stats = self.ownership.stats();
            snapshot.lease_owned = ownership_stats.lease_owned;
            snapshot.has_lease = ownership_stats.has_lease;
            snapshot.acquisition_count = ownership_stats.acquisition_count;
            snapshot.lease_acquire_failures = ownership_stats.lease_acquire_failures;
            snapshot.lost_leases = ownership_stats.lost_leases;
            snapshot.last_acquired_ms = ownership_stats.last_acquired_ms;
            return snapshot;
        }

        pub fn runOnce(self: *Self) !void {
            if (!self.config.enabled) return;
            const now_ns = self.config.clock.nowRealtimeNs();
            if (!ensureLease(self, now_ns)) return;
            const summary = try runRecovery(self, now_ns);
            recordRun(self, now_ns, summary, false);
        }
        fn workerStep(runtime: *Self) ?u64 {
            if (isShutdown(runtime)) return null;
            const now_ns = runtime.config.clock.nowRealtimeNs();
            if (ensureLease(runtime, now_ns)) {
                const summary = runRecovery(runtime, now_ns) catch {
                    recordRun(runtime, now_ns, .{}, true);
                    return @max(1, runtime.config.interval_ms);
                };
                recordRun(runtime, now_ns, summary, false);
            }
            return @max(1, runtime.config.interval_ms);
        }

        fn ensureLease(runtime: *Self, now_ns: u64) bool {
            const now_ms: u64 = @intCast(now_ns / std.time.ns_per_ms);
            const io = runtime.io orelse return false;
            runtime.mutex.lockUncancelable(io);
            defer runtime.mutex.unlock(io);
            const acquired = runtime.ownership.ensureLease(now_ms) catch {
                runtime.ownership.noteAcquireFailure();
                return false;
            };
            return acquired;
        }

        fn runRecovery(runtime: *Self, now_ns: u64) !RunSummary {
            const summary = try Policy.runPage(
                runtime.alloc,
                runtime.store,
                runtime.config,
                now_ns,
                runtime.scan_after,
                @max(1, runtime.config.max_records_per_run),
            );
            runtime.scan_after = summary.next_scan_after;
            return summary;
        }

        fn isShutdown(runtime: *Self) bool {
            return runtime.shutdown.load(.acquire);
        }

        fn recordRun(runtime: *Self, now_ns: u64, summary: RunSummary, failed: bool) void {
            const maybe_io = runtime.io;
            if (maybe_io) |io| runtime.mutex.lockUncancelable(io);
            defer if (maybe_io) |io| runtime.mutex.unlock(io);
            runtime.stats_value.runs += 1;
            runtime.stats_value.scanned_records += summary.recovery.scanned_records;
            runtime.stats_value.auto_aborted += summary.recovery.auto_aborted;
            runtime.stats_value.resolved_finalized += summary.recovery.resolved_finalized;
            runtime.stats_value.cleaned_records += summary.recovery.cleaned_records;
            runtime.stats_value.kept_recent_pending += summary.recovery.kept_recent_pending;
            runtime.stats_value.deferred_unresolved += summary.recovery.deferred_unresolved;
            runtime.stats_value.notification_attempts += summary.notification_attempts;
            runtime.stats_value.notification_successes += summary.notification_successes;
            runtime.stats_value.notification_failures += summary.notification_failures;
            runtime.stats_value.last_run_ns = now_ns;
            runtime.stats_value.error_count += summary.record_failures;
            if (failed) runtime.stats_value.error_count += 1;
        }

        fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
            while (!mutex.tryLock()) @import("antfly_platform").time.yieldNow();
        }
    };
}
