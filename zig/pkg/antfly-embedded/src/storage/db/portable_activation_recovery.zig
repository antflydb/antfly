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

const native_platform = @import("antfly_platform");
const std = @import("std");
const runtime_mod = @import("../background_runtime.zig");
const platform_time = @import("antfly_platform").time;
const AtomicU64 = @import("antfly_platform").atomic.Value(u64);
var portable_activation_retry_jitter_nonce: AtomicU64 = .init(0);
fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) @import("antfly_platform").time.yieldNow();
}
fn monotonicTimeNs() u64 {
    return platform_time.monotonicNs();
}

/// Owner-scoped local activation supervision. DB supplies the fenced activation
/// operation; this owner controls admission, retry cadence, and close barriers.
pub const Owner = struct {
    pub const Port = struct {
        ptr: *anyopaque,
        path: []const u8,
        runtime: *runtime_mod.BackendRuntime,
        owner_id: u64,
        pending: *std.atomic.Value(bool),
        retry: *const fn (*anyopaque) anyerror!bool,
        before_start: ?*const fn (*anyopaque) void = null,
        refuse_launch: ?*const fn (*anyopaque) bool = null,
    };
    port: Port = undefined,
    bound: bool = false,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    worker_running: std.atomic.Value(bool) = .init(false),
    stopping: std.atomic.Value(bool) = .init(false),
    jitter_salt: u64 = 0,
    launch_failure_streak: u32 = 0,
    next_launch_ns: u64 = 0,
    pub const portable_activation_retry_base_ns: u64 = 250 * std.time.ns_per_ms;
    pub const portable_activation_retry_max_ns: u64 = 30 * std.time.ns_per_s;
    const sleep_slice_ns: u64 = 25 * std.time.ns_per_ms;
    pub fn start(self: *Owner, port: Port) void {
        if (!port.pending.load(.acquire)) {
            port.runtime.disarmOwnerMaintenanceProbe(port.owner_id);
            return;
        }
        if (port.before_start) |hook| hook(port.ptr);
        _ = lockAtomic(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        // This lifecycle-owned stop flag is permanent once close begins. A
        // probe may already have been claimed by the reaper when close disarms
        // it, so recheck under the same mutex used by stop before launching.
        if (self.stopping.load(.acquire)) {
            port.runtime.disarmOwnerMaintenanceProbe(port.owner_id);
            return;
        }
        // A claimed probe reads this port outside the lifecycle lock. Publish
        // it once before queue admission and keep it immutable through close.
        if (!self.bound) {
            self.port = port;
            self.bound = true;
        } else {
            std.debug.assert(self.port.ptr == port.ptr and self.port.runtime == port.runtime and self.port.owner_id == port.owner_id);
        }
        if (self.worker_running.load(.acquire)) {
            self.port.runtime.disarmOwnerMaintenanceProbe(self.port.owner_id);
            return;
        }
        const now_ns = monotonicTimeNs();
        if (now_ns < self.next_launch_ns) return;
        if (self.jitter_salt == 0) {
            const nonce = portable_activation_retry_jitter_nonce.fetchAdd(1, .monotonic);
            var entropy: [16]u8 = undefined;
            std.mem.writeInt(u64, entropy[0..8], now_ns, .little);
            std.mem.writeInt(u64, entropy[8..16], nonce, .little);
            const path_hash = std.hash.Wyhash.hash(0x5052544143545048, self.port.path);
            self.jitter_salt = std.hash.Wyhash.hash(path_hash, &entropy) | 1;
        }
        self.worker_running.store(true, .release);
        _ = self.launchPortableActivationRetryWorkerLocked() catch |err| {
            self.worker_running.store(false, .release);
            self.launch_failure_streak +|= 1;
            const retry_delay_ns = portableActivationRetryDelayNs(
                self.port.path,
                self.jitter_salt,
                self.launch_failure_streak - 1,
            );
            self.next_launch_ns = now_ns +| retry_delay_ns;
            self.port.runtime.armOwnerMaintenanceProbe(self.port.owner_id, .{
                .ptr = self,
                .run = portableActivationRetryMaintenanceProbeMain,
            }) catch |arm_err| {
                std.log.warn(
                    "portable activation retry supervisor arm failed path={s} launch_err={s} arm_err={s}",
                    .{ self.port.path, @errorName(err), @errorName(arm_err) },
                );
                return;
            };
            std.log.warn(
                "portable activation retry launch deferred path={s} err={s} failures={d} next_retry_ms={d}",
                .{
                    self.port.path,
                    @errorName(err),
                    self.launch_failure_streak,
                    retry_delay_ns / std.time.ns_per_ms,
                },
            );
            return;
        };
        self.launch_failure_streak = 0;
        self.next_launch_ns = 0;
        self.port.runtime.disarmOwnerMaintenanceProbe(self.port.owner_id);
    }

    pub const PortableActivationRetryLaunch = enum {
        backend_runtime,
    };

    /// Portable activation belongs to the runtime's bounded maintenance lane,
    /// not an untracked OS thread. Owner shutdown is the join barrier and the
    /// runtime controls concurrency across all resident databases.
    pub fn launchPortableActivationRetryWorkerLocked(self: *Owner) !PortableActivationRetryLaunch {
        std.debug.assert(self.worker_running.load(.acquire));
        if (self.port.runtime.durable_jobs.executesInline()) return error.BackgroundRuntimeUnavailable;
        if (self.port.refuse_launch) |hook| {
            if (hook(self.port.ptr)) return error.InjectedPortableActivationRetrySubmitFailure;
        }
        try self.port.runtime.durable_jobs.submit(.{
            .owner_id = self.port.owner_id,
            .class = .maintenance,
            .ptr = self,
            .run = portableActivationRetryDurableJobMain,
            .deinit = portableActivationRetryDurableJobDeinit,
        });
        return .backend_runtime;
    }

    fn portableActivationRetryDurableJobMain(ptr: *anyopaque) !void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.portableActivationRetryWorkerMain();
    }

    fn portableActivationRetryDurableJobDeinit(_: *anyopaque) void {}

    fn portableActivationRetryMaintenanceProbeMain(ptr: *anyopaque) void {
        const self: *Owner = @ptrCast(@alignCast(ptr));
        self.start(self.port);
    }

    fn finishPortableActivationRetryWorker(self: *Owner) void {
        _ = lockAtomic(&self.lifecycle_mutex);
        self.worker_running.store(false, .release);
        self.lifecycle_mutex.unlock();
    }

    pub fn stop(self: *Owner, port: Port) void {
        port.runtime.disarmOwnerMaintenanceProbe(port.owner_id);
        _ = lockAtomic(&self.lifecycle_mutex);
        self.stopping.store(true, .release);
        self.lifecycle_mutex.unlock();

        // The owner-scoped worker publishes completion. Wait outside the
        // lifecycle mutex so its final handshake cannot deadlock shutdown.
        while (self.worker_running.load(.acquire)) {
            const io = port.runtime.io() orelse native_platform.debug_io;
            io.sleep(.fromMilliseconds(1), .awake) catch {};
        }

        // Completion is published immediately before the worker releases this
        // mutex. Reacquiring it is the final memory-lifetime barrier.
        _ = lockAtomic(&self.lifecycle_mutex);
        std.debug.assert(!self.worker_running.load(.acquire));
        self.lifecycle_mutex.unlock();
    }

    pub fn portableActivationRetryDelayNs(path: []const u8, jitter_salt: u64, failure_streak: u32) u64 {
        const exponent: u6 = @intCast(@min(failure_streak, 7));
        const nominal = @min(portable_activation_retry_base_ns << exponent, portable_activation_retry_max_ns);
        // Stable 80-100% jitter desynchronizes restored replicas without ever
        // exceeding the operator-facing retry cap. Including the DB path keeps
        // independent databases from marching in lockstep after process start.
        var streak_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &streak_bytes, failure_streak, .little);
        const path_hash = std.hash.Wyhash.hash(0x5052544143545259, path);
        const entropy = std.hash.Wyhash.hash(path_hash ^ jitter_salt, &streak_bytes);
        const spread = @max(@as(u64, 1), nominal / 5);
        return nominal - spread + entropy % (spread + 1);
    }

    fn sleepPortableActivationRetryWorker(self: *Owner, target_ns: u64) bool {
        const io = self.port.runtime.io() orelse native_platform.debug_io;
        var slept: u64 = 0;
        while (slept < target_ns) {
            if (self.stopping.load(.acquire)) return false;
            const slice = @min(sleep_slice_ns, target_ns - slept);
            io.sleep(.fromNanoseconds(slice), .awake) catch {};
            slept += slice;
        }
        return !self.stopping.load(.acquire);
    }

    pub fn portableActivationRetryWorkerMain(self: *Owner) void {
        var failure_streak: u32 = 0;
        while (true) {
            while (self.port.pending.load(.acquire)) {
                const delay_ns = portableActivationRetryDelayNs(self.port.path, self.jitter_salt, failure_streak);
                if (!self.sleepPortableActivationRetryWorker(delay_ns)) {
                    self.finishPortableActivationRetryWorker();
                    return;
                }
                _ = self.port.retry(self.port.ptr) catch |err| {
                    // Contention means an explicit recovery caller owns the single
                    // flight; it is not another activation failure and must not
                    // advance backoff or emit a misleading repair warning.
                    if (err == error.PortableRuntimeActivationPending) continue;
                    failure_streak +|= 1;
                    std.log.warn(
                        "portable runtime activation retry failed path={s} class={s} failures={d} next_retry_ms={d}",
                        .{
                            self.port.path,
                            @errorName(err),
                            failure_streak,
                            portableActivationRetryDelayNs(self.port.path, self.jitter_salt, failure_streak) / std.time.ns_per_ms,
                        },
                    );
                    continue;
                };
            }

            if (self.stopping.load(.acquire)) {
                self.finishPortableActivationRetryWorker();
                return;
            }
            _ = lockAtomic(&self.lifecycle_mutex);
            if (self.stopping.load(.acquire)) {
                self.worker_running.store(false, .release);
                self.lifecycle_mutex.unlock();
                return;
            }
            if (self.port.pending.load(.acquire)) {
                // A new generation became degraded while start() observed this
                // handle as live. Keep servicing it instead of stranding the
                // generation between the old worker's condition check and exit.
                failure_streak = 0;
                self.lifecycle_mutex.unlock();
                continue;
            }
            self.worker_running.store(false, .release);
            self.lifecycle_mutex.unlock();
            return;
        }
    }
};
