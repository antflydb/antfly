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
        h1_request_capacity: usize,
        reserved_h1_request_capacity: usize,
        active_listener_leases: usize,
        active_h1_cancellation_observers: usize,
        h1_hard_disconnect_cancellations_total: u64,
        h1_cancellation_observer_failures_total: u64,
        h1_cancellation_registration_failures_total: u64,
        healthy: bool,
    };

    pub const ListenerLease = struct {
        runtime: ?*HttpRuntime = null,
        reserved_h1_requests: usize = 0,

        pub fn release(self: *ListenerLease) void {
            const runtime = self.runtime orelse return;
            runtime.releaseListener(self.reserved_h1_requests);
            self.* = .{};
        }

        pub fn registerH1Request(
            self: *const ListenerLease,
            fd: std.posix.fd_t,
            cancellation: *std.atomic.Value(bool),
        ) !CancellationObserver.Registration {
            const runtime = self.runtime orelse return error.HttpRuntimeUnavailable;
            return runtime.registerH1Request(fd, cancellation);
        }
    };

    observer: CancellationObserver,
    h1_request_capacity: usize,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    listener_leases: std.atomic.Value(usize) = .init(0),
    reserved_h1_request_capacity: std.atomic.Value(usize) = .init(0),
    registration_failures_total: std.atomic.Value(u64) = .init(0),

    pub fn init(alloc: std.mem.Allocator, config: Config) HttpRuntime {
        return .{
            .h1_request_capacity = config.max_active_h1_requests,
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

    /// Reserves the listener's maximum concurrent H1 ownership before it is
    /// published. This turns an undersized shared runtime into a startup error
    /// instead of request-time 503s after multiple listeners are already live.
    pub fn acquireListener(self: *HttpRuntime, max_h1_requests: usize) !ListenerLease {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        const leases = self.listener_leases.load(.acquire);
        const reserved = self.reserved_h1_request_capacity.load(.acquire);
        if (max_h1_requests > self.h1_request_capacity -| reserved)
            return error.HttpRuntimeCapacityExceeded;
        if (reserved == 0 and max_h1_requests > 0) try self.observer.start();
        self.listener_leases.store(leases + 1, .release);
        self.reserved_h1_request_capacity.store(reserved + max_h1_requests, .release);
        return .{ .runtime = self, .reserved_h1_requests = max_h1_requests };
    }

    pub fn stats(self: *const HttpRuntime) Stats {
        return .{
            .h1_request_capacity = self.h1_request_capacity,
            .reserved_h1_request_capacity = self.reserved_h1_request_capacity.load(.acquire),
            .active_listener_leases = self.listener_leases.load(.acquire),
            .active_h1_cancellation_observers = self.observer.activeCount(),
            .h1_hard_disconnect_cancellations_total = self.observer.cancellations(),
            .h1_cancellation_observer_failures_total = self.observer.failures(),
            .h1_cancellation_registration_failures_total = self.registration_failures_total.load(.acquire),
            .healthy = self.observer.isHealthy(),
        };
    }

    pub fn registerH1Request(
        self: *HttpRuntime,
        fd: std.posix.fd_t,
        cancellation: *std.atomic.Value(bool),
    ) !CancellationObserver.Registration {
        if (self.reserved_h1_request_capacity.load(.acquire) == 0) return error.HttpRuntimeUnavailable;
        return self.observer.register(fd, cancellation) catch |err| {
            _ = self.registration_failures_total.fetchAdd(1, .monotonic);
            return err;
        };
    }

    fn releaseListener(self: *HttpRuntime, reserved_h1_requests: usize) void {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        const leases = self.listener_leases.load(.acquire);
        const reserved = self.reserved_h1_request_capacity.load(.acquire);
        std.debug.assert(leases > 0);
        std.debug.assert(reserved >= reserved_h1_requests);
        self.listener_leases.store(leases - 1, .release);
        const remaining_reserved = reserved - reserved_h1_requests;
        self.reserved_h1_request_capacity.store(remaining_reserved, .release);
        if (reserved > 0 and remaining_reserved == 0) self.observer.stop();
    }

    fn lock(self: *HttpRuntime) void {
        while (!self.lifecycle_mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

test "HTTP runtime listener leases share one cancellation observer lifecycle" {
    if (@import("builtin").os.tag == .windows or @import("builtin").os.tag == .freestanding) return;
    var runtime = HttpRuntime.init(std.testing.allocator, .{ .max_active_h1_requests = 2 });
    defer runtime.deinit();

    // A bounded control listener does not consume observer capacity or start
    // the observer, but it may coexist with and outlive application listeners.
    var control = try runtime.acquireListener(0);
    var first = try runtime.acquireListener(1);
    var second = try runtime.acquireListener(1);
    try std.testing.expectEqual(@as(usize, 3), runtime.stats().active_listener_leases);
    try std.testing.expectEqual(@as(usize, 2), runtime.stats().reserved_h1_request_capacity);
    try std.testing.expectError(error.HttpRuntimeCapacityExceeded, runtime.acquireListener(1));
    first.release();
    try std.testing.expectEqual(@as(usize, 2), runtime.stats().active_listener_leases);
    second.release();
    try std.testing.expectEqual(@as(usize, 1), runtime.stats().active_listener_leases);
    try std.testing.expectEqual(@as(usize, 0), runtime.stats().reserved_h1_request_capacity);
    var restarted = try runtime.acquireListener(2);
    restarted.release();
    control.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.stats().active_listener_leases);
}
