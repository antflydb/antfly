// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the License at https://www.antfly.io/licensing/ELv2-license

//! Shared lifecycle primitives for process-long runtime tasks.
//!
//! Executors are borrowed from a runtime lane. Components own the tasks they
//! submit, publish failure through this state, and join before releasing their
//! executor lease.

const std = @import("std");
const httpx = @import("httpx");
const platform_sync = @import("antfly_platform").sync;
const platform_time = @import("antfly_platform").time;

var process_signal_requested: std.atomic.Value(bool) = .init(false);

fn processSignalHandler(_: std.posix.SIG) callconv(.c) void {
    // Atomic publication is async-signal-safe. Teardown remains on the role's
    // owning task and therefore retains normal allocator and Io invariants.
    process_signal_requested.store(true, .release);
}

/// Process-wide SIGINT/SIGTERM bridge. The scope restores prior handlers and
/// presents signal state as an owned cancellation source to each role instead
/// of requiring role-local globals and handlers.
pub const ProcessSignalScope = struct {
    old_int: std.posix.Sigaction,
    old_term: std.posix.Sigaction,

    pub fn install() ProcessSignalScope {
        process_signal_requested.store(false, .release);
        const action = std.posix.Sigaction{
            .handler = .{ .handler = processSignalHandler },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        };
        var old_int: std.posix.Sigaction = undefined;
        var old_term: std.posix.Sigaction = undefined;
        std.posix.sigaction(.INT, &action, &old_int);
        std.posix.sigaction(.TERM, &action, &old_term);
        return .{ .old_int = old_int, .old_term = old_term };
    }

    pub fn cancellationRequested(_: *const ProcessSignalScope) bool {
        return process_signal_requested.load(.acquire);
    }

    pub fn deinit(self: *ProcessSignalScope) void {
        std.posix.sigaction(.INT, &self.old_int, null);
        std.posix.sigaction(.TERM, &self.old_term, null);
        process_signal_requested.store(false, .release);
        self.* = undefined;
    }
};

pub const CancellationToken = struct {
    cancelled: *const std.atomic.Value(bool),

    pub fn isCancelled(self: CancellationToken) bool {
        return self.cancelled.load(.acquire);
    }

    pub fn check(self: CancellationToken) !void {
        if (self.isCancelled()) return error.Cancelled;
    }
};

pub const CancellationSource = struct {
    cancelled: std.atomic.Value(bool) = .init(false),

    pub fn token(self: *const CancellationSource) CancellationToken {
        return .{ .cancelled = &self.cancelled };
    }

    pub fn cancel(self: *CancellationSource) void {
        self.cancelled.store(true, .release);
    }
};

/// One absolute monotonic shutdown deadline shared across every drain phase.
pub const ShutdownDeadline = struct {
    deadline_ns: u64,

    pub fn afterMilliseconds(timeout_ms: u64) ShutdownDeadline {
        const now: u64 = @intCast(@max(platform_time.monotonicNs(), 0));
        return .{ .deadline_ns = now +| timeout_ms *| std.time.ns_per_ms };
    }

    pub fn remainingMilliseconds(self: ShutdownDeadline) u64 {
        const now: u64 = @intCast(@max(platform_time.monotonicNs(), 0));
        const remaining_ns = self.deadline_ns -| now;
        if (remaining_ns == 0) return 0;
        return @max(@as(u64, 1), @divFloor(remaining_ns, std.time.ns_per_ms));
    }

    pub fn expired(self: ShutdownDeadline) bool {
        return self.remainingMilliseconds() == 0;
    }

    /// Lazily create one process-owned deadline and return it to each teardown
    /// phase. This is intentionally not synchronized: the process supervisor
    /// is the sole writer during ordered shutdown.
    pub fn shared(slot: *?ShutdownDeadline, timeout_ms: u64) ShutdownDeadline {
        if (slot.* == null) slot.* = afterMilliseconds(timeout_ms);
        return slot.*.?;
    }
};

/// Process-level coordination for composed runtime roles. Components retain
/// ownership of their Futures and stop callbacks; the supervisor owns the
/// cross-component cancellation state, first-failure record, readiness phase,
/// and one absolute shutdown deadline.
pub const RuntimeSupervisor = struct {
    pub const State = enum(u8) {
        starting,
        ready,
        quiescing,
        failed,
        stopped,
    };

    pub const Failure = struct {
        component: []const u8,
        task: []const u8,
        err: anyerror,
        observed_ns: u64,
    };

    state: std.atomic.Value(State) = .init(.starting),
    cancellation: CancellationSource = .{},
    failure_mutex: std.atomic.Mutex = .unlocked,
    first_failure: ?Failure = null,
    shutdown_timeout_ms: u64,
    shutdown_deadline: ?ShutdownDeadline = null,

    pub fn init(shutdown_timeout_ms: u64) RuntimeSupervisor {
        return .{ .shutdown_timeout_ms = shutdown_timeout_ms };
    }

    pub fn token(self: *const RuntimeSupervisor) CancellationToken {
        return self.cancellation.token();
    }

    pub fn currentState(self: *const RuntimeSupervisor) State {
        return self.state.load(.acquire);
    }

    pub fn publishReady(self: *RuntimeSupervisor) !void {
        if (self.state.cmpxchgStrong(.starting, .ready, .acq_rel, .acquire)) |actual| {
            return switch (actual) {
                .failed => error.RuntimeFailed,
                .quiescing, .stopped => error.RuntimeStopping,
                .ready => error.RuntimeAlreadyReady,
                .starting => unreachable,
            };
        }
    }

    /// Records only the first fatal background failure, cancels the process,
    /// and returns the original error for direct `return supervisor.fail(...)`
    /// propagation from a role loop.
    pub fn fail(
        self: *RuntimeSupervisor,
        component: []const u8,
        task: []const u8,
        err: anyerror,
    ) anyerror {
        platform_sync.lockYielding(&self.failure_mutex);
        if (self.first_failure == null) {
            self.first_failure = .{
                .component = component,
                .task = task,
                .err = err,
                .observed_ns = @intCast(@max(platform_time.monotonicNs(), 0)),
            };
        }
        self.failure_mutex.unlock();
        self.cancellation.cancel();
        self.state.store(.failed, .release);
        return err;
    }

    pub fn failure(self: *RuntimeSupervisor) ?Failure {
        platform_sync.lockYielding(&self.failure_mutex);
        defer self.failure_mutex.unlock();
        return self.first_failure;
    }

    pub fn requestShutdown(self: *RuntimeSupervisor) void {
        self.cancellation.cancel();
        var observed = self.state.load(.acquire);
        while (observed == .starting or observed == .ready) {
            observed = self.state.cmpxchgWeak(observed, .quiescing, .acq_rel, .acquire) orelse return;
        }
    }

    pub fn shouldStop(self: *RuntimeSupervisor, process_signal_requested_now: bool) bool {
        if (process_signal_requested_now) self.requestShutdown();
        return self.token().isCancelled();
    }

    /// The process owner is the sole caller during ordered teardown, so the
    /// lazy deadline slot requires no additional synchronization.
    pub fn deadline(self: *RuntimeSupervisor) ShutdownDeadline {
        return ShutdownDeadline.shared(&self.shutdown_deadline, self.shutdown_timeout_ms);
    }

    pub fn markStopped(self: *RuntimeSupervisor) void {
        self.cancellation.cancel();
        self.state.store(.stopped, .release);
    }
};

/// Cross-task state for one HTTP listener. This object does not own the
/// listener future; the spawning component retains and joins that future.
pub const HttpServerLifecycle = struct {
    pub const State = enum(u8) { starting, ready, failed, stopping, stopped };

    state: std.atomic.Value(State) = .init(.starting),
    mutex: std.atomic.Mutex = .unlocked,
    server: ?*httpx.Server = null,
    failure: anyerror = error.Unexpected,
    cancellation: CancellationSource = .{},

    pub fn attach(self: *HttpServerLifecycle, server: *httpx.Server) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        self.server = server;
    }

    pub fn detach(self: *HttpServerLifecycle, server: *httpx.Server) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.server == server) self.server = null;
    }

    pub fn token(self: *const HttpServerLifecycle) CancellationToken {
        return self.cancellation.token();
    }

    pub fn publishReady(self: *HttpServerLifecycle) void {
        self.state.store(.ready, .release);
    }

    pub fn publishFailure(self: *HttpServerLifecycle, err: anyerror) void {
        if (self.state.load(.acquire) == .stopping) {
            self.state.store(.stopped, .release);
            return;
        }
        self.failure = err;
        self.cancellation.cancel();
        self.state.store(.failed, .release);
    }

    pub fn publishStopped(self: *HttpServerLifecycle) void {
        self.state.store(.stopped, .release);
    }

    pub fn waitForStartup(self: *HttpServerLifecycle) !void {
        while (true) switch (self.state.load(.acquire)) {
            .starting => std.Thread.yield() catch {},
            .ready => return,
            .failed => return self.failure,
            .stopping, .stopped => return error.ServerStopped,
        };
    }

    pub fn runtimeFailure(self: *HttpServerLifecycle) ?anyerror {
        return if (self.state.load(.acquire) == .failed) self.failure else null;
    }

    pub fn runtimeStats(self: *HttpServerLifecycle) ?httpx.Server.RuntimeStats {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const server = self.server orelse return null;
        return server.runtimeStats();
    }

    pub fn httpRuntimeStats(self: *HttpServerLifecycle) ?httpx.HttpRuntime.Stats {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        const server = self.server orelse return null;
        return server.httpRuntimeStats();
    }

    pub fn stop(self: *HttpServerLifecycle) void {
        self.cancellation.cancel();
        const prior = self.state.swap(.stopping, .acq_rel);
        if (prior == .failed or prior == .stopped) return;
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.server) |server| server.requestStop();
    }

    pub fn shutdown(self: *HttpServerLifecycle, deadline: ShutdownDeadline) void {
        self.cancellation.cancel();
        const prior = self.state.swap(.stopping, .acq_rel);
        if (prior == .failed or prior == .stopped) return;
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.server) |server| server.shutdown(deadline.remainingMilliseconds());
    }
};

test "runtime lifecycle cancellation and shutdown deadline share state" {
    var lifecycle = HttpServerLifecycle{};
    const token = lifecycle.token();
    try std.testing.expect(!token.isCancelled());
    lifecycle.stop();
    try std.testing.expect(token.isCancelled());
    try std.testing.expectError(error.Cancelled, token.check());

    const deadline = ShutdownDeadline.afterMilliseconds(100);
    try std.testing.expect(!deadline.expired());
    try std.testing.expect(deadline.remainingMilliseconds() <= 100);

    var shared_deadline: ?ShutdownDeadline = null;
    const first = ShutdownDeadline.shared(&shared_deadline, 100);
    const second = ShutdownDeadline.shared(&shared_deadline, 10_000);
    try std.testing.expectEqual(first.deadline_ns, second.deadline_ns);
}

test "runtime lifecycle supervisor retains first failure and one shutdown deadline" {
    var supervisor = RuntimeSupervisor.init(100);
    try supervisor.publishReady();
    try std.testing.expectEqual(RuntimeSupervisor.State.ready, supervisor.currentState());
    try std.testing.expect(!supervisor.shouldStop(false));

    const first = supervisor.fail("public-api", "listener", error.AddressInUse);
    try std.testing.expectEqual(error.AddressInUse, first);
    const second = supervisor.fail("health", "listener", error.ConnectionRefused);
    try std.testing.expectEqual(error.ConnectionRefused, second);
    try std.testing.expect(supervisor.shouldStop(false));
    try std.testing.expectEqual(RuntimeSupervisor.State.failed, supervisor.currentState());
    const failure = supervisor.failure().?;
    try std.testing.expectEqualStrings("public-api", failure.component);
    try std.testing.expectEqualStrings("listener", failure.task);
    try std.testing.expectEqual(error.AddressInUse, failure.err);

    const first_deadline = supervisor.deadline();
    const second_deadline = supervisor.deadline();
    try std.testing.expectEqual(first_deadline.deadline_ns, second_deadline.deadline_ns);
    supervisor.markStopped();
    try std.testing.expectEqual(RuntimeSupervisor.State.stopped, supervisor.currentState());
}
