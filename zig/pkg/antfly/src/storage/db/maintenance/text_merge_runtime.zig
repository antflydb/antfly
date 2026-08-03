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

const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const Allocator = std.mem.Allocator;
const apply_rw_lock_mod = @import("../apply_rw_lock.zig");
const index_manager_mod = @import("../catalog/index_manager.zig");
const types = @import("../types.zig");
const platform_clock = @import("antfly_platform").clock;
const background_runtime_mod = @import("../../background_runtime.zig");

pub const Config = struct {
    enabled: bool = builtin.os.tag != .freestanding and !builtin.is_test,
    idle_interval_ms: u64 = 50,
    error_interval_ms: u64 = 250,
    // The policy's steady state is ten segments. Start producer assistance
    // before query fan-out grows into visible tail latency; the independent
    // storage FD admission domain protects descriptor safety.
    max_pending_segments: u64 = 64,
    resume_pending_segments: u64 = 32,
    max_pending_bytes: u64 = 256 * 1024 * 1024,
    // Bound producer latency when a source is corrupt, quarantined, or owned
    // by a stuck worker. FD admission remains the final safety boundary.
    backpressure_max_wait_ms: u64 = 5_000,
    clock: platform_clock.Clock = platform_clock.Clock.real(),
};

pub const BackpressureOutcome = enum {
    not_needed,
    drained,
    shutdown,
    timed_out,
    merge_failed,
};

pub var test_block_after_task_begin: std.atomic.Value(bool) = .init(false);
pub var test_task_begin_entered: std.atomic.Value(bool) = .init(false);
pub var test_release_after_task_begin: std.atomic.Value(bool) = .init(false);
pub var test_stop_entered: std.atomic.Value(bool) = .init(false);
pub var test_start_failures_remaining: std.atomic.Value(u32) = .init(0);

pub const TextMergeRuntime = if (builtin.os.tag == .freestanding) struct {
    config: Config,

    pub fn init(
        _: Allocator,
        _: *index_manager_mod.IndexManager,
        _: *apply_rw_lock_mod.ApplyRwLock,
        _: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !@This() {
        return .{ .config = config };
    }

    pub fn deinit(self: *@This()) void {
        self.* = undefined;
    }

    pub fn start(self: *@This()) !void {
        if (self.config.enabled) return error.UnsupportedPlatform;
    }

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

    pub fn notify(self: *@This()) void {
        _ = self;
    }

    pub fn runOnce(self: *@This()) !bool {
        _ = self;
        return false;
    }

    pub fn applyBackpressure(self: *@This()) BackpressureOutcome {
        _ = self;
        return .not_needed;
    }

    pub const ProducerPermit = struct {
        pub fn release(_: *@This()) void {}
    };

    pub fn acquireProducerPermit(_: *@This(), _: u64, _: u64) !ProducerPermit {
        return .{};
    }

    pub fn stats(self: *@This()) types.TextMergeStats {
        return self.statsAssumeApplyLockHeld();
    }

    pub fn statsAssumeApplyLockHeld(self: *@This()) types.TextMergeStats {
        return .{
            .enabled = self.config.enabled,
            .max_pending_segments = self.config.max_pending_segments,
            .max_pending_bytes = self.config.max_pending_bytes,
        };
    }
} else struct {
    alloc: Allocator,
    io_impl: ?*Io.Threaded,
    index_manager: *index_manager_mod.IndexManager,
    apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
    config: Config,
    mutex: Io.Mutex = .init,
    cond: Io.Condition = .init,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    desired_running: bool = false,
    paused: bool = false,
    shutdown: bool = false,
    notified: bool = false,
    backpressure_events: u64 = 0,
    backpressure_ns: u64 = 0,
    backpressure_timeouts: u64 = 0,
    backpressure_failures: u64 = 0,
    producer_segment_reservations: u64 = 0,
    producer_byte_reservations: u64 = 0,
    producer_release_epoch: u64 = 0,
    producer_wait_epoch: std.atomic.Value(u32) = .init(0),
    admission_closed: bool = false,
    future: ?Io.Future(void) = null,

    pub const ProducerPermit = struct {
        runtime: *TextMergeRuntime,
        segment_count: u64,
        byte_count: u64,
        active: bool = true,

        pub fn release(self: *ProducerPermit) void {
            if (!self.active) return;
            self.active = false;
            const runtime = self.runtime;
            const io = runtime.io_impl.?.io();
            runtime.mutex.lockUncancelable(io);
            std.debug.assert(runtime.producer_segment_reservations >= self.segment_count);
            std.debug.assert(runtime.producer_byte_reservations >= self.byte_count);
            runtime.producer_segment_reservations -= self.segment_count;
            runtime.producer_byte_reservations -= self.byte_count;
            // Publication completes before release. Advancing the epoch makes
            // an acquirer retry if its debt snapshot straddled this handoff.
            runtime.producer_release_epoch +%= 1;
            runtime.notified = true;
            runtime.cond.broadcast(io);
            runtime.mutex.unlock(io);
            runtime.signalProducerAdmissionChanged();
        }
    };

    pub fn init(
        alloc: Allocator,
        index_manager: *index_manager_mod.IndexManager,
        apply_mutex: *apply_rw_lock_mod.ApplyRwLock,
        backend_runtime: *background_runtime_mod.BackendRuntime,
        config: Config,
    ) !TextMergeRuntime {
        const io_impl = backend_runtime.io_impl;
        if (config.enabled and io_impl == null) return error.MissingBackendRuntimeIo;
        if (config.enabled and
            (config.max_pending_segments != 0 or config.max_pending_bytes != 0) and
            config.backpressure_max_wait_ms == 0)
        {
            return error.InvalidTextMergeBackpressureConfig;
        }
        return .{
            .alloc = alloc,
            .io_impl = io_impl,
            .index_manager = index_manager,
            .apply_mutex = apply_mutex,
            .config = config,
        };
    }

    pub fn deinit(self: *TextMergeRuntime) void {
        _ = self.stop();
        self.* = undefined;
    }

    pub fn start(self: *TextMergeRuntime) !void {
        if (!self.config.enabled) return;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.desired_running = true;
        self.paused = false;
        self.setAdmissionClosedLocked(false);
        try self.startLocked();
    }

    /// Gracefully drains the active merge, if any, and stops the worker.
    /// Structural catalog mutations call this before moving or closing an
    /// inline index runtime, so no task can retain a pointer across mutation.
    pub fn stop(self: *TextMergeRuntime) bool {
        if (!self.config.enabled) return false;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.desired_running = false;
        self.paused = true;
        self.setAdmissionClosedLocked(true);
        return self.stopLocked();
    }

    /// Temporarily prevents worker publication while preserving the desired
    /// running state. A failed resume can therefore be retried safely without
    /// racing a later structural catalog mutation.
    pub fn pause(self: *TextMergeRuntime) bool {
        if (!self.config.enabled) return false;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.paused = true;
        const desired = self.desired_running;
        _ = self.stopLocked();
        return desired;
    }

    pub fn resumeAfterPause(self: *TextMergeRuntime) !void {
        if (!self.config.enabled) return;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        self.paused = false;
        if (self.desired_running) try self.startLocked();
    }

    /// Used by the DB restart supervisor. False means a structural mutation
    /// currently owns the pause; true means the runtime is running or no
    /// longer desires a worker.
    pub fn ensureRunning(self: *TextMergeRuntime) !bool {
        if (!self.config.enabled) return true;
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        if (!self.desired_running) return true;
        if (self.paused) return false;
        try self.startLocked();
        return true;
    }

    pub fn isStarted(self: *TextMergeRuntime) bool {
        lockAtomicWithBackoff(&self.lifecycle_mutex);
        defer self.lifecycle_mutex.unlock();
        return self.future != null;
    }

    fn startLocked(self: *TextMergeRuntime) !void {
        if (self.future != null or self.paused or !self.desired_running) return;
        const io_impl = self.io_impl orelse return error.MissingBackendRuntimeIo;
        const io = io_impl.io();
        if (builtin.is_test and consumeTestStartFailure()) return error.TestTransientMaintenanceRestart;
        self.mutex.lockUncancelable(io);
        self.shutdown = false;
        self.notified = true;
        self.mutex.unlock(io);
        self.future = try io.concurrent(workerMain, .{self});
    }

    fn stopLocked(self: *TextMergeRuntime) bool {
        const io_impl = self.io_impl orelse return false;
        const io = io_impl.io();
        if (self.future == null) return false;
        if (builtin.is_test) test_stop_entered.store(true, .release);

        self.mutex.lockUncancelable(io);
        self.shutdown = true;
        self.notified = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);

        _ = self.future.?.await(io);
        self.future = null;
        return true;
    }

    pub fn notify(self: *TextMergeRuntime) void {
        if (!self.config.enabled) return;
        const io_impl = self.io_impl orelse return;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.notified = true;
        self.cond.broadcast(io);
        self.mutex.unlock(io);
    }

    pub fn runOnce(self: *TextMergeRuntime) !bool {
        var maybe_task: ?index_manager_mod.IndexManager.TextMergeTask = null;
        if (!self.apply_mutex.tryLockExclusive()) return false;
        maybe_task = self.index_manager.beginTextMergeTask() catch |err| {
            self.apply_mutex.unlockExclusive();
            return err;
        };
        self.apply_mutex.unlockExclusive();

        var task = maybe_task orelse return false;
        const work_alloc = self.index_manager.alloc;
        defer task.deinit(work_alloc);

        if (builtin.is_test and test_block_after_task_begin.load(.acquire)) {
            test_task_begin_entered.store(true, .release);
            while (!test_release_after_task_begin.load(.acquire)) std.Thread.yield() catch {};
        }

        var result = index_manager_mod.IndexManager.executeTextMergeTask(work_alloc, &task) catch |err| {
            // Once a task has borrowed an index runtime it must retire its
            // scheduler state before a graceful stop can complete. Structural
            // mutation waits for stop before taking the apply lock, so this
            // acquisition cannot form a shutdown lock cycle.
            lockApplyExclusive(self.apply_mutex);
            if (err == error.ResourceBudgetExceeded) {
                self.index_manager.cancelTextMergeTask(&task);
                self.apply_mutex.unlockExclusive();
                self.signalProducerAdmissionChanged();
                return false;
            }
            self.index_manager.noteTextMergeFailure(&task, err);
            self.apply_mutex.unlockExclusive();
            self.signalProducerAdmissionChanged();
            return err;
        };
        defer result.deinit(work_alloc);

        lockApplyExclusive(self.apply_mutex);
        _ = self.index_manager.finishTextMergeTask(&task, &result) catch |err| {
            self.index_manager.noteTextMergeFailure(&task, err);
            self.apply_mutex.unlockExclusive();
            self.signalProducerAdmissionChanged();
            return err;
        };
        self.apply_mutex.unlockExclusive();
        self.signalProducerAdmissionChanged();
        return true;
    }

    pub fn applyBackpressure(self: *TextMergeRuntime) BackpressureOutcome {
        if (!self.config.enabled) return .not_needed;
        if (self.config.max_pending_segments == 0 and self.config.max_pending_bytes == 0) return .not_needed;
        if (!self.backpressureNeeded()) return .not_needed;

        const started_ns = self.backpressureNowNs();
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.backpressure_events += 1;
            self.mutex.unlock(io);
        } else {
            self.backpressure_events += 1;
        }
        defer self.recordBackpressureElapsed(started_ns);
        self.notify();
        while (!isShutdown(self)) {
            const wait_epoch = self.producer_wait_epoch.load(.acquire);
            if (self.backpressureDrained()) break;
            if (self.backpressureExpired(started_ns)) {
                self.recordBackpressureTerminal(.timed_out);
                return .timed_out;
            }
            if (self.backpressureBlockedByQuarantine()) {
                self.recordBackpressureTerminal(.merge_failed);
                return .merge_failed;
            }
            if (self.backpressureDrained()) break;
            // Merge execution stays on the background runtime. Producer
            // deadlines therefore cover the entire wait even when a merge
            // itself takes longer than the configured maximum.
            if (!self.waitForProducerAdmissionChange(wait_epoch, started_ns)) {
                if (isShutdown(self)) return .shutdown;
                self.recordBackpressureTerminal(.timed_out);
                return .timed_out;
            }
        }
        if (isShutdown(self)) return .shutdown;
        return .drained;
    }

    /// Reserve segment publication capacity before any writer makes a new
    /// segment visible. Concurrent producers are included in the bound, so
    /// the configured high watermark is an admission limit rather than a
    /// best-effort cleanup trigger.
    pub fn acquireProducerPermit(self: *TextMergeRuntime, segment_count: u64, byte_count: u64) !ProducerPermit {
        if ((segment_count == 0 and byte_count == 0) or !self.config.enabled or
            (self.config.max_pending_segments == 0 and self.config.max_pending_bytes == 0))
        {
            return .{ .runtime = self, .segment_count = 0, .byte_count = 0, .active = false };
        }

        const started_ns = self.backpressureNowNs();
        var recorded_wait = false;
        defer if (recorded_wait) self.recordBackpressureElapsed(started_ns);
        self.notify();
        while (true) {
            if (self.backpressureExpired(started_ns)) {
                self.recordBackpressureTerminal(.timed_out);
                return error.TextMergeBackpressureTimeout;
            }
            if (self.backpressureBlockedByQuarantine()) {
                self.recordBackpressureTerminal(.merge_failed);
                return error.TextMergeBackpressureUnavailable;
            }

            const io = self.io_impl.?.io();
            self.mutex.lockUncancelable(io);
            if (self.admission_closed) {
                self.mutex.unlock(io);
                return error.TextMergeRuntimeShutdown;
            }
            const snapshot_epoch = self.producer_release_epoch;
            const snapshot_wait_epoch = self.producer_wait_epoch.load(.acquire);
            self.mutex.unlock(io);

            lockApplyShared(self.apply_mutex);
            const stats_snapshot = self.index_manager.textMergeStatsSnapshot();
            self.apply_mutex.unlockShared();

            self.mutex.lockUncancelable(io);
            if (self.admission_closed) {
                self.mutex.unlock(io);
                return error.TextMergeRuntimeShutdown;
            }
            if (snapshot_epoch != self.producer_release_epoch or
                snapshot_wait_epoch != self.producer_wait_epoch.load(.acquire))
            {
                self.mutex.unlock(io);
                continue;
            }
            const reserved_segments = std.math.add(u64, self.producer_segment_reservations, segment_count) catch std.math.maxInt(u64);
            const reserved_bytes = std.math.add(u64, self.producer_byte_reservations, byte_count) catch std.math.maxInt(u64);
            const admitted_segments = std.math.add(u64, stats_snapshot.pending_segments, reserved_segments) catch std.math.maxInt(u64);
            const admitted_bytes = std.math.add(u64, stats_snapshot.pending_bytes, reserved_bytes) catch std.math.maxInt(u64);
            const request_oversized =
                (self.config.max_pending_segments > 0 and segment_count > self.config.max_pending_segments) or
                (self.config.max_pending_bytes > 0 and byte_count > self.config.max_pending_bytes);
            const no_existing_debt = stats_snapshot.pending_segments == 0 and stats_snapshot.pending_bytes == 0 and
                self.producer_segment_reservations == 0 and self.producer_byte_reservations == 0;
            const segments_admissible = self.config.max_pending_segments == 0 or admitted_segments <= self.config.max_pending_segments;
            const bytes_admissible = self.config.max_pending_bytes == 0 or admitted_bytes <= self.config.max_pending_bytes;
            if ((request_oversized and no_existing_debt) or (!request_oversized and segments_admissible and bytes_admissible)) {
                self.producer_segment_reservations = reserved_segments;
                self.producer_byte_reservations = reserved_bytes;
                self.mutex.unlock(io);
                return .{ .runtime = self, .segment_count = segment_count, .byte_count = byte_count };
            }
            if (!recorded_wait) {
                self.backpressure_events += 1;
                recorded_wait = true;
            }
            self.mutex.unlock(io);
            if (!self.waitForProducerAdmissionChange(snapshot_wait_epoch, started_ns)) {
                if (self.isAdmissionClosed()) return error.TextMergeRuntimeShutdown;
                self.recordBackpressureTerminal(.timed_out);
                return error.TextMergeBackpressureTimeout;
            }
        }
    }

    fn setAdmissionClosedLocked(self: *TextMergeRuntime, closed: bool) void {
        const io_impl = self.io_impl orelse return;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        self.admission_closed = closed;
        self.cond.broadcast(io);
        self.mutex.unlock(io);
        self.signalProducerAdmissionChanged();
    }

    fn recordBackpressureElapsed(self: *TextMergeRuntime, started_ns: u64) void {
        const elapsed_ns = self.backpressureNowNs() -| started_ns;
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            self.backpressure_ns += elapsed_ns;
            self.mutex.unlock(io);
        } else {
            self.backpressure_ns += elapsed_ns;
        }
    }

    fn recordBackpressureTerminal(self: *TextMergeRuntime, outcome: BackpressureOutcome) void {
        if (self.io_impl) |io_impl| {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            defer self.mutex.unlock(io);
            if (outcome == .timed_out) self.backpressure_timeouts += 1;
            if (outcome == .merge_failed) self.backpressure_failures += 1;
        } else {
            if (outcome == .timed_out) self.backpressure_timeouts += 1;
            if (outcome == .merge_failed) self.backpressure_failures += 1;
        }
    }

    fn backpressureExpired(self: *TextMergeRuntime, started_ns: u64) bool {
        const max_wait_ns = std.math.mul(u64, self.config.backpressure_max_wait_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
        return self.backpressureNowNs() -| started_ns >= max_wait_ns;
    }

    fn backpressureNowNs(self: *TextMergeRuntime) u64 {
        const io = self.io_impl.?.io();
        return @intCast(Io.Timestamp.now(io, .awake).toNanoseconds());
    }

    fn waitForProducerAdmissionChange(self: *TextMergeRuntime, observed_epoch: u32, started_ns: u64) bool {
        if (self.isAdmissionClosed() or self.backpressureExpired(started_ns)) return false;
        const max_wait_ns = std.math.mul(u64, self.config.backpressure_max_wait_ms, std.time.ns_per_ms) catch std.math.maxInt(u64);
        const elapsed_ns = self.backpressureNowNs() -| started_ns;
        if (elapsed_ns >= max_wait_ns) return false;
        const io = self.io_impl.?.io();
        std.Io.futexWaitTimeout(
            io,
            u32,
            &self.producer_wait_epoch.raw,
            observed_epoch,
            .{ .duration = .{
                .clock = .awake,
                .raw = .fromNanoseconds(@intCast(max_wait_ns - elapsed_ns)),
            } },
        ) catch |err| switch (err) {
            error.Canceled => return false,
        };
        return !self.isAdmissionClosed() and !self.backpressureExpired(started_ns);
    }

    fn signalProducerAdmissionChanged(self: *TextMergeRuntime) void {
        const io_impl = self.io_impl orelse return;
        const io = io_impl.io();
        _ = self.producer_wait_epoch.fetchAdd(1, .release);
        std.Io.futexWake(io, u32, &self.producer_wait_epoch.raw, std.math.maxInt(u32));
    }

    fn isAdmissionClosed(self: *TextMergeRuntime) bool {
        const io_impl = self.io_impl orelse return self.admission_closed;
        const io = io_impl.io();
        self.mutex.lockUncancelable(io);
        defer self.mutex.unlock(io);
        return self.admission_closed;
    }

    pub fn stats(self: *TextMergeRuntime) types.TextMergeStats {
        lockApplyShared(self.apply_mutex);
        defer self.apply_mutex.unlockShared();
        return self.statsAssumeApplyLockHeld();
    }

    pub fn statsAssumeApplyLockHeld(self: *TextMergeRuntime) types.TextMergeStats {
        var snapshot = self.index_manager.textMergeStatsSnapshot();

        const backpressure = if (self.io_impl) |io_impl| blk: {
            const io = io_impl.io();
            self.mutex.lockUncancelable(io);
            const events = self.backpressure_events;
            const ns = self.backpressure_ns;
            const timeouts = self.backpressure_timeouts;
            const failures = self.backpressure_failures;
            self.mutex.unlock(io);
            break :blk .{ events, ns, timeouts, failures };
        } else .{ self.backpressure_events, self.backpressure_ns, self.backpressure_timeouts, self.backpressure_failures };
        snapshot.enabled = self.config.enabled;
        snapshot.backpressure_events = backpressure[0];
        snapshot.backpressure_ns = backpressure[1];
        snapshot.backpressure_timeouts = backpressure[2];
        snapshot.backpressure_failures = backpressure[3];
        snapshot.max_pending_segments = self.config.max_pending_segments;
        snapshot.max_pending_bytes = self.config.max_pending_bytes;
        return snapshot;
    }

    fn backpressureNeeded(self: *TextMergeRuntime) bool {
        lockApplyShared(self.apply_mutex);
        const stats_snapshot = self.index_manager.textMergeStatsSnapshot();
        self.apply_mutex.unlockShared();
        return (self.config.max_pending_segments > 0 and stats_snapshot.pending_segments > self.config.max_pending_segments) or
            (self.config.max_pending_bytes > 0 and stats_snapshot.pending_bytes > self.config.max_pending_bytes);
    }

    fn backpressureDrained(self: *TextMergeRuntime) bool {
        lockApplyShared(self.apply_mutex);
        const stats_snapshot = self.index_manager.textMergeStatsSnapshot();
        self.apply_mutex.unlockShared();
        const segment_low = @min(self.config.resume_pending_segments, self.config.max_pending_segments);
        const bytes_low = self.config.max_pending_bytes / 2;
        return (self.config.max_pending_segments == 0 or stats_snapshot.pending_segments <= segment_low) and
            (self.config.max_pending_bytes == 0 or stats_snapshot.pending_bytes <= bytes_low);
    }

    fn backpressureBlockedByQuarantine(self: *TextMergeRuntime) bool {
        lockApplyShared(self.apply_mutex);
        const stats_snapshot = self.index_manager.textMergeStatsSnapshot();
        self.apply_mutex.unlockShared();
        return stats_snapshot.pending_segments > 0 and
            stats_snapshot.quarantined_segments >= stats_snapshot.pending_segments;
    }
};

fn workerMain(runtime: *TextMergeRuntime) void {
    while (true) {
        if (isShutdown(runtime)) return;
        const ran = runtime.runOnce() catch |err| {
            if (err == error.ResourceBudgetExceeded) {
                sleepMs(runtime, runtime.config.error_interval_ms);
                continue;
            }
            if (builtin.os.tag != .freestanding) {
                std.log.err("text merge worker failed: {s}", .{@errorName(err)});
            }
            sleepMs(runtime, runtime.config.error_interval_ms);
            continue;
        };
        if (ran) continue;
        waitForWork(runtime);
    }
}

fn waitForWork(runtime: *TextMergeRuntime) void {
    var remaining_ms = runtime.config.idle_interval_ms;
    if (remaining_ms == 0) remaining_ms = 1;

    const io_impl = runtime.io_impl orelse return;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    if (runtime.notified or runtime.shutdown) {
        runtime.notified = false;
        runtime.mutex.unlock(io);
        return;
    }
    runtime.mutex.unlock(io);

    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 10);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
        runtime.mutex.lockUncancelable(io);
        const notified = runtime.notified;
        runtime.notified = false;
        runtime.mutex.unlock(io);
        if (notified) return;
    }
}

fn sleepMs(runtime: *TextMergeRuntime, ms: u64) void {
    var remaining_ms = if (ms == 0) 1 else ms;
    while (remaining_ms > 0) {
        if (isShutdown(runtime)) return;
        const slice_ms: u64 = @min(remaining_ms, 10);
        runtime.config.clock.sleepMs(slice_ms);
        remaining_ms -= slice_ms;
    }
}

fn isShutdown(runtime: *TextMergeRuntime) bool {
    const io_impl = runtime.io_impl orelse return runtime.shutdown;
    const io = io_impl.io();
    runtime.mutex.lockUncancelable(io);
    defer runtime.mutex.unlock(io);
    return runtime.shutdown;
}

fn lockApplyExclusive(lock: *apply_rw_lock_mod.ApplyRwLock) void {
    lock.lockExclusive();
}

fn lockAtomicWithBackoff(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.Thread.yield() catch {};
}

fn consumeTestStartFailure() bool {
    var remaining = test_start_failures_remaining.load(.acquire);
    while (remaining != 0) {
        if (test_start_failures_remaining.cmpxchgWeak(
            remaining,
            remaining - 1,
            .acq_rel,
            .acquire,
        )) |actual| {
            remaining = actual;
            continue;
        }
        return true;
    }
    return false;
}

fn lockApplyShared(lock: *apply_rw_lock_mod.ApplyRwLock) void {
    lock.lockShared();
}
