//! Shared transport services for one or more HTTP listeners.
//!
//! `HttpRuntime` is independent of the executor used by `Server`. It owns the
//! bounded HTTP/1 cancellation monitor needed by `std.Io.Threaded` without
//! consuming a worker from a listener or application lane.

const std = @import("std");
const CancellationObserver = @import("cancellation_observer.zig").Observer;

pub const HttpRuntime = struct {
    pub const Config = struct {
        max_active_h1_requests: usize = 4096,
        /// The observer is a shallow polling loop, not an application worker.
        /// Keep its stack independent from (and much smaller than) executor
        /// worker stacks while allowing embedders to raise it for a platform.
        observer_thread_stack_size: usize = 1024 * 1024,
    };

    pub const Stats = struct {
        active_listener_leases: usize,
        active_h1_cancellation_observers: usize,
        h1_peer_cancellations_total: u64,
        h1_cancellation_observer_failures_total: u64,
        h1_cancellation_registration_failures_total: u64,
    };

    pub const ListenerLease = struct {
        runtime: ?*HttpRuntime = null,

        pub fn release(self: *ListenerLease) void {
            const runtime = self.runtime orelse return;
            runtime.releaseListener();
            self.* = .{};
        }

        pub fn registerH1Request(
            self: *const ListenerLease,
            fd: std.posix.fd_t,
            cancellation: *std.atomic.Value(bool),
            observe_orderly_eof: bool,
        ) !CancellationObserver.Registration {
            const runtime = self.runtime orelse return error.HttpRuntimeUnavailable;
            return runtime.registerH1Request(fd, cancellation, observe_orderly_eof);
        }
    };

    observer: CancellationObserver,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    listener_leases: std.atomic.Value(usize) = .init(0),
    registration_failures_total: std.atomic.Value(u64) = .init(0),

    pub fn init(alloc: std.mem.Allocator, config: Config) HttpRuntime {
        return .{
            .observer = CancellationObserver.init(
                alloc,
                config.max_active_h1_requests,
                config.observer_thread_stack_size,
            ),
        };
    }

    pub fn deinit(self: *HttpRuntime) void {
        std.debug.assert(self.listener_leases.load(.acquire) == 0);
        self.observer.deinit();
        self.* = undefined;
    }

    pub fn acquireListener(self: *HttpRuntime) !ListenerLease {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        const leases = self.listener_leases.load(.acquire);
        if (leases == 0) try self.observer.start();
        self.listener_leases.store(leases + 1, .release);
        return .{ .runtime = self };
    }

    pub fn stats(self: *const HttpRuntime) Stats {
        return .{
            .active_listener_leases = self.listener_leases.load(.acquire),
            .active_h1_cancellation_observers = self.observer.activeCount(),
            .h1_peer_cancellations_total = self.observer.cancellations(),
            .h1_cancellation_observer_failures_total = self.observer.failures(),
            .h1_cancellation_registration_failures_total = self.registration_failures_total.load(.acquire),
        };
    }

    pub fn registerH1Request(
        self: *HttpRuntime,
        fd: std.posix.fd_t,
        cancellation: *std.atomic.Value(bool),
        observe_orderly_eof: bool,
    ) !CancellationObserver.Registration {
        return self.observer.register(fd, cancellation, observe_orderly_eof) catch |err| {
            _ = self.registration_failures_total.fetchAdd(1, .monotonic);
            return err;
        };
    }

    fn releaseListener(self: *HttpRuntime) void {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        const leases = self.listener_leases.load(.acquire);
        std.debug.assert(leases > 0);
        self.listener_leases.store(leases - 1, .release);
        if (leases == 1) self.observer.stop();
    }

    fn lock(self: *HttpRuntime) void {
        while (!self.lifecycle_mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

test "HTTP runtime listener leases share one cancellation observer lifecycle" {
    if (@import("builtin").os.tag == .windows or @import("builtin").os.tag == .freestanding) return;
    var runtime = HttpRuntime.init(std.testing.allocator, .{ .max_active_h1_requests = 2 });
    defer runtime.deinit();

    var first = try runtime.acquireListener();
    var second = try runtime.acquireListener();
    try std.testing.expectEqual(@as(usize, 2), runtime.stats().active_listener_leases);
    first.release();
    try std.testing.expectEqual(@as(usize, 1), runtime.stats().active_listener_leases);
    second.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.stats().active_listener_leases);
}
