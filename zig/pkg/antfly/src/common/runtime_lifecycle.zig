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

const shutdown_watchdog_poll_ns: u64 = 10 * std.time.ns_per_ms;

const WindowsSleep = if (@import("builtin").os.tag == .windows) struct {
    extern "kernel32" fn Sleep(timeout_ms: u32) callconv(.winapi) void;
} else struct {};

fn sleepWatchdog(ns: u64) void {
    if (comptime @import("builtin").os.tag == .windows) {
        const ms = @max(@as(u64, 1), @divFloor(ns +| std.time.ns_per_ms - 1, std.time.ns_per_ms));
        WindowsSleep.Sleep(@intCast(@min(ms, @as(u64, std.math.maxInt(u32)))));
        return;
    }
    var req = std.posix.timespec{
        .sec = @intCast(ns / std.time.ns_per_s),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

/// Last-resort process termination is deliberately independent of every
/// `std.Io` executor being drained. A stuck task can consume or deadlock those
/// executors, so scheduling the deadline on one of them would make the
/// deadline advisory precisely when it is needed.
const ShutdownWatchdog = struct {
    const State = enum(u8) { idle, armed, disarmed, expired };
    const Expiration = struct {
        context: ?*anyopaque = null,
        run: *const fn (?*anyopaque) void = terminateProcess,

        fn terminateProcess(_: ?*anyopaque) void {
            // Do not run destructors after the graceful deadline: they may
            // free memory still referenced by the task that failed to drain.
            std.process.exit(1);
        }
    };

    state: std.atomic.Value(State) = .init(.idle),
    deadline_ns: u64 = 0,
    expiration: Expiration = .{},
    thread: ?std.Thread = null,

    fn arm(self: *ShutdownWatchdog, deadline: ShutdownDeadline) void {
        std.debug.assert(self.thread == null);
        std.debug.assert(self.state.load(.acquire) == .idle);
        self.deadline_ns = deadline.deadline_ns;
        self.state.store(.armed, .release);
        self.thread = std.Thread.spawn(.{}, run, .{self}) catch {
            // Without an executor-independent watchdog the promised hard
            // deadline cannot be upheld. Failing immediately is safer than
            // entering teardown with an unbounded or use-after-free outcome.
            self.expiration.run(self.expiration.context);
            unreachable;
        };
    }

    fn disarm(self: *ShutdownWatchdog) void {
        const previous = self.state.swap(.disarmed, .acq_rel);
        if (self.thread) |thread| thread.join();
        self.thread = null;
        if (previous == .idle) self.state.store(.disarmed, .release);
    }

    fn run(self: *ShutdownWatchdog) void {
        while (self.state.load(.acquire) == .armed) {
            const now: u64 = @intCast(@max(platform_time.monotonicNs(), 0));
            if (now >= self.deadline_ns) {
                if (self.state.cmpxchgStrong(.armed, .expired, .acq_rel, .acquire) == null) {
                    self.expiration.run(self.expiration.context);
                }
                return;
            }
            sleepWatchdog(@min(self.deadline_ns - now, shutdown_watchdog_poll_ns));
        }
    }
};

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

    pub fn token(_: *const ProcessSignalScope) CancellationToken {
        return .{ .cancelled = &process_signal_requested };
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
    shutdown_watchdog: ShutdownWatchdog = .{},

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
        const deadline_value = ShutdownDeadline.shared(&self.shutdown_deadline, self.shutdown_timeout_ms);
        if (self.shutdown_watchdog.state.load(.acquire) == .idle)
            self.shutdown_watchdog.arm(deadline_value);
        return deadline_value;
    }

    /// Startup uses the same configured budget without arming the hard
    /// teardown watchdog. A healthy process must not be terminated merely
    /// because its listener became ready near the end of startup.
    pub fn startupDeadline(self: *const RuntimeSupervisor) ShutdownDeadline {
        return ShutdownDeadline.afterMilliseconds(self.shutdown_timeout_ms);
    }

    pub fn markStopped(self: *RuntimeSupervisor) void {
        self.shutdown_watchdog.disarm();
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
    startup_io: std.Io,
    startup_event: std.Io.Event = .unset,

    pub fn init(startup_io: std.Io) HttpServerLifecycle {
        return .{ .startup_io = startup_io };
    }

    fn publishStartupTerminal(self: *HttpServerLifecycle) void {
        self.startup_event.set(self.startup_io);
    }

    /// Attaches the concrete listener only while startup still owns the
    /// lifecycle. A stop that wins this race is terminal, so a late listener
    /// is rejected instead of being allowed to enter `listen` unnoticed.
    pub fn attach(self: *HttpServerLifecycle, server: *httpx.Server) !void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.state.load(.monotonic) != .starting) {
            server.requestStop();
            return error.ServerStopped;
        }
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

    pub fn publishReady(self: *HttpServerLifecycle) !void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        if (self.state.load(.monotonic) != .starting) return error.ServerStopped;
        self.state.store(.ready, .release);
        self.publishStartupTerminal();
    }

    pub fn publishFailure(self: *HttpServerLifecycle, err: anyerror) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        switch (self.state.load(.monotonic)) {
            .starting, .ready => {
                self.failure = err;
                self.cancellation.cancel();
                self.state.store(.failed, .release);
                self.publishStartupTerminal();
            },
            .stopping => {
                self.state.store(.stopped, .release);
                self.publishStartupTerminal();
            },
            .failed, .stopped => {},
        }
    }

    pub fn publishStopped(self: *HttpServerLifecycle) void {
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        switch (self.state.load(.monotonic)) {
            .starting, .ready, .stopping => {
                self.state.store(.stopped, .release);
                self.publishStartupTerminal();
            },
            .failed, .stopped => {},
        }
    }

    pub fn waitForStartup(
        self: *HttpServerLifecycle,
        deadline: ShutdownDeadline,
        external_cancellation: CancellationToken,
    ) !void {
        while (true) {
            switch (self.state.load(.acquire)) {
                .starting => {},
                .ready => return,
                .failed => return self.failure,
                .stopping, .stopped => return error.ServerStopped,
            }
            if (external_cancellation.isCancelled()) {
                self.stop();
                return error.Cancelled;
            }
            const remaining_ms = deadline.remainingMilliseconds();
            if (remaining_ms == 0) {
                self.stop();
                return error.StartupTimeout;
            }
            self.startup_event.waitTimeout(self.startup_io, .{ .duration = .{
                .raw = std.Io.Duration.fromMilliseconds(@intCast(@min(remaining_ms, 25))),
                .clock = .awake,
            } }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => return error.Canceled,
            };
        }
    }

    pub fn runtimeFailure(self: *HttpServerLifecycle) ?anyerror {
        return if (self.state.load(.acquire) == .failed) self.failure else null;
    }

    pub fn currentState(self: *const HttpServerLifecycle) State {
        return self.state.load(.acquire);
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
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        switch (self.state.load(.monotonic)) {
            .starting, .ready => {
                self.state.store(.stopping, .release);
                self.publishStartupTerminal();
            },
            .failed, .stopped => return,
            .stopping => {},
        }
        if (self.server) |server| server.requestStop();
    }

    pub fn shutdown(self: *HttpServerLifecycle, deadline: ShutdownDeadline) void {
        self.cancellation.cancel();
        platform_sync.lockYielding(&self.mutex);
        defer self.mutex.unlock();
        switch (self.state.load(.monotonic)) {
            .starting, .ready => {
                self.state.store(.stopping, .release);
                self.publishStartupTerminal();
            },
            .failed, .stopped => return,
            .stopping => {},
        }
        if (self.server) |server| server.shutdown(deadline.remainingMilliseconds());
    }
};

test "runtime lifecycle cancellation and shutdown deadline share state" {
    var lifecycle = HttpServerLifecycle.init(std.testing.io);
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

test "http lifecycle startup wait observes event cancellation and timeout" {
    var ready = HttpServerLifecycle.init(std.testing.io);
    try ready.publishReady();
    var inactive = CancellationSource{};
    try ready.waitForStartup(ShutdownDeadline.afterMilliseconds(100), inactive.token());

    var canceled = HttpServerLifecycle.init(std.testing.io);
    var cancellation = CancellationSource{};
    cancellation.cancel();
    try std.testing.expectError(
        error.Cancelled,
        canceled.waitForStartup(ShutdownDeadline.afterMilliseconds(100), cancellation.token()),
    );
    try std.testing.expectEqual(HttpServerLifecycle.State.stopping, canceled.currentState());

    var timed_out = HttpServerLifecycle.init(std.testing.io);
    try std.testing.expectError(
        error.StartupTimeout,
        timed_out.waitForStartup(ShutdownDeadline.afterMilliseconds(0), inactive.token()),
    );
    try std.testing.expectEqual(HttpServerLifecycle.State.stopping, timed_out.currentState());
}

test "http lifecycle startup wait parks until publication" {
    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();
    var lifecycle = HttpServerLifecycle.init(io);
    const Publisher = struct {
        fn run(publisher_io: std.Io, target: *HttpServerLifecycle) !void {
            try publisher_io.sleep(std.Io.Duration.fromMilliseconds(1), .awake);
            try target.publishReady();
        }
    };
    var publisher = try io.concurrent(Publisher.run, .{ io, &lifecycle });
    defer _ = publisher.await(io) catch {};
    var inactive = CancellationSource{};
    try lifecycle.waitForStartup(ShutdownDeadline.afterMilliseconds(1_000), inactive.token());
    try std.testing.expectEqual(HttpServerLifecycle.State.ready, lifecycle.currentState());
}

test "http lifecycle stop cannot be overwritten by ready or failure" {
    var lifecycle = HttpServerLifecycle.init(std.testing.io);
    lifecycle.stop();
    try std.testing.expectEqual(HttpServerLifecycle.State.stopping, lifecycle.currentState());
    try std.testing.expectError(error.ServerStopped, lifecycle.publishReady());
    lifecycle.publishFailure(error.AddressInUse);
    try std.testing.expectEqual(HttpServerLifecycle.State.stopped, lifecycle.currentState());
    try std.testing.expectEqual(@as(?anyerror, null), lifecycle.runtimeFailure());
}

test "http lifecycle rejects a listener that attaches after stop" {
    var lifecycle = HttpServerLifecycle.init(std.testing.io);
    lifecycle.stop();
    var server = httpx.Server.init(std.testing.allocator, std.testing.io);
    defer server.deinit();

    try std.testing.expectError(error.ServerStopped, lifecycle.attach(&server));
    try std.testing.expectEqual(HttpServerLifecycle.State.stopping, lifecycle.currentState());
}

test "http lifecycle retains its first terminal failure" {
    var lifecycle = HttpServerLifecycle.init(std.testing.io);
    lifecycle.publishFailure(error.AddressInUse);
    lifecycle.publishFailure(error.ConnectionRefused);
    lifecycle.publishStopped();
    try std.testing.expectEqual(HttpServerLifecycle.State.failed, lifecycle.currentState());
    try std.testing.expectEqual(error.AddressInUse, lifecycle.runtimeFailure().?);
}

test "runtime lifecycle supervisor retains first failure and one shutdown deadline" {
    var supervisor = RuntimeSupervisor.init(10_000);
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

test "runtime supervisor hard deadline expires outside std Io" {
    const ExpirationState = struct {
        fired: std.atomic.Value(bool) = .init(false),

        fn expire(raw: ?*anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw orelse return));
            self.fired.store(true, .release);
        }
    };

    var expiration = ExpirationState{};
    var supervisor = RuntimeSupervisor.init(1);
    supervisor.shutdown_watchdog.expiration = .{
        .context = &expiration,
        .run = ExpirationState.expire,
    };
    _ = supervisor.deadline();
    while (!expiration.fired.load(.acquire)) sleepWatchdog(std.time.ns_per_ms);
    try std.testing.expectEqual(ShutdownWatchdog.State.expired, supervisor.shutdown_watchdog.state.load(.acquire));
    supervisor.markStopped();
}
