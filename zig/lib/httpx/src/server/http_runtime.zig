//! Shared transport services for one or more HTTP listeners.
//!
//! `HttpRuntime` is independent of the executor injected into `Server` for
//! nested operations. It owns bounded HTTP listener, connection, and request
//! executors plus the HTTP/1 cancellation monitor needed by `std.Io.Threaded`.

const std = @import("std");
const CancellationObserver = @import("cancellation_observer.zig").Observer;

pub const HttpRuntime = struct {
    pub const Config = struct {
        max_active_h1_requests: usize = 4096,
        /// Aggregate connection-task capacity shared by every listener. Null
        /// inherits `max_active_h1_requests` for source compatibility with a
        /// single application listener.
        max_active_connections: ?usize = null,
        /// Aggregate application request-task capacity shared by every
        /// listener. Request execution is independent from connection
        /// lifetimes so keep-alive sockets and HTTP/2 frame pumps cannot
        /// consume the workers that run handlers.
        max_active_requests: ?usize = null,
        /// Maximum number of long-lived listener tasks owned by this runtime.
        /// Listener tasks use a dedicated executor so accepting connections
        /// never consumes capacity from an application or backend lane.
        max_listeners: usize = 16,
        /// Null follows Zig's platform thread-stack contract. Although the
        /// observer itself is shallow, linked runtimes may impose target- and
        /// libc-specific TLS/stack requirements that httpx cannot safely infer.
        /// The reservation is virtual on supported hosts; embedders may set an
        /// explicit smaller value only after validating every deployment target.
        observer_thread_stack_size: ?usize = null,
    };

    pub const Stats = struct {
        h1_request_capacity: usize,
        reserved_h1_request_capacity: usize,
        connection_capacity: usize,
        reserved_connection_capacity: usize,
        request_capacity: usize,
        reserved_request_capacity: usize,
        active_listener_leases: usize,
        listener_capacity: usize,
        active_h1_cancellation_observers: usize,
        h1_hard_disconnect_cancellations_total: u64,
        h1_cancellation_observer_failures_total: u64,
        h1_cancellation_registration_failures_total: u64,
        healthy: bool,
    };

    pub const ListenerLease = struct {
        runtime: ?*HttpRuntime = null,
        reserved_h1_requests: usize = 0,
        reserved_connections: usize = 0,
        reserved_requests: usize = 0,

        pub fn release(self: *ListenerLease) void {
            const runtime = self.runtime orelse return;
            runtime.releaseListener(
                self.reserved_h1_requests,
                self.reserved_connections,
                self.reserved_requests,
            );
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

        pub fn listenerIo(self: *const ListenerLease) std.Io {
            const runtime = self.runtime orelse @panic("released HTTP listener lease");
            return runtime.listener_io_impl.io();
        }

        pub fn connectionIo(self: *const ListenerLease) std.Io {
            const runtime = self.runtime orelse @panic("released HTTP listener lease");
            return runtime.connection_io_impl.io();
        }

        pub fn requestIo(self: *const ListenerLease) std.Io {
            const runtime = self.runtime orelse @panic("released HTTP listener lease");
            return runtime.request_io_impl.io();
        }
    };

    pub const ListenerRequirements = struct {
        max_h1_requests: usize,
        max_connections: usize,
        max_requests: usize,
    };

    observer: CancellationObserver,
    listener_io_impl: std.Io.Threaded,
    connection_io_impl: std.Io.Threaded,
    request_io_impl: std.Io.Threaded,
    h1_request_capacity: usize,
    connection_capacity: usize,
    request_capacity: usize,
    listener_capacity: usize,
    lifecycle_mutex: std.atomic.Mutex = .unlocked,
    listener_leases: std.atomic.Value(usize) = .init(0),
    reserved_h1_request_capacity: std.atomic.Value(usize) = .init(0),
    reserved_connection_capacity: std.atomic.Value(usize) = .init(0),
    reserved_request_capacity: std.atomic.Value(usize) = .init(0),
    registration_failures_total: std.atomic.Value(u64) = .init(0),

    pub fn init(alloc: std.mem.Allocator, config: Config) HttpRuntime {
        const listener_capacity = @max(config.max_listeners, 1);
        const connection_capacity = @max(config.max_active_connections orelse config.max_active_h1_requests, 1);
        const request_capacity = @max(config.max_active_requests orelse connection_capacity, 1);
        return .{
            .h1_request_capacity = config.max_active_h1_requests,
            .connection_capacity = connection_capacity,
            .request_capacity = request_capacity,
            .listener_capacity = listener_capacity,
            .listener_io_impl = std.Io.Threaded.init(alloc, .{
                .concurrent_limit = .limited(listener_capacity),
            }),
            .connection_io_impl = std.Io.Threaded.init(alloc, .{
                .concurrent_limit = .limited(connection_capacity),
            }),
            .request_io_impl = std.Io.Threaded.init(alloc, .{
                .concurrent_limit = .limited(request_capacity),
            }),
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
        self.listener_io_impl.deinit();
        self.connection_io_impl.deinit();
        self.request_io_impl.deinit();
        self.* = undefined;
    }

    /// Reserves the listener's complete H1 observer, connection-task, and
    /// request-task bounds before it is published. This turns an undersized
    /// shared runtime into a startup error instead of a partial listener that
    /// only fails after the process is already live.
    pub fn acquireListener(self: *HttpRuntime, requirements: ListenerRequirements) !ListenerLease {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        const leases = self.listener_leases.load(.acquire);
        const reserved_h1 = self.reserved_h1_request_capacity.load(.acquire);
        const reserved_connections = self.reserved_connection_capacity.load(.acquire);
        const reserved_requests = self.reserved_request_capacity.load(.acquire);
        if (leases >= self.listener_capacity) return error.HttpRuntimeListenerCapacityExceeded;
        if (requirements.max_h1_requests > self.h1_request_capacity -| reserved_h1)
            return error.HttpRuntimeCapacityExceeded;
        if (requirements.max_connections > self.connection_capacity -| reserved_connections)
            return error.HttpRuntimeConnectionCapacityExceeded;
        if (requirements.max_requests > self.request_capacity -| reserved_requests)
            return error.HttpRuntimeRequestCapacityExceeded;
        if (reserved_h1 == 0 and requirements.max_h1_requests > 0) try self.observer.start();
        self.listener_leases.store(leases + 1, .release);
        self.reserved_h1_request_capacity.store(reserved_h1 + requirements.max_h1_requests, .release);
        self.reserved_connection_capacity.store(reserved_connections + requirements.max_connections, .release);
        self.reserved_request_capacity.store(reserved_requests + requirements.max_requests, .release);
        return .{
            .runtime = self,
            .reserved_h1_requests = requirements.max_h1_requests,
            .reserved_connections = requirements.max_connections,
            .reserved_requests = requirements.max_requests,
        };
    }

    pub fn stats(self: *const HttpRuntime) Stats {
        return .{
            .h1_request_capacity = self.h1_request_capacity,
            .reserved_h1_request_capacity = self.reserved_h1_request_capacity.load(.acquire),
            .connection_capacity = self.connection_capacity,
            .reserved_connection_capacity = self.reserved_connection_capacity.load(.acquire),
            .request_capacity = self.request_capacity,
            .reserved_request_capacity = self.reserved_request_capacity.load(.acquire),
            .active_listener_leases = self.listener_leases.load(.acquire),
            .listener_capacity = self.listener_capacity,
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

    fn releaseListener(
        self: *HttpRuntime,
        reserved_h1_requests: usize,
        reserved_connections: usize,
        reserved_requests: usize,
    ) void {
        self.lock();
        defer self.lifecycle_mutex.unlock();
        const leases = self.listener_leases.load(.acquire);
        const reserved_h1 = self.reserved_h1_request_capacity.load(.acquire);
        const current_connections = self.reserved_connection_capacity.load(.acquire);
        const current_requests = self.reserved_request_capacity.load(.acquire);
        std.debug.assert(leases > 0);
        std.debug.assert(reserved_h1 >= reserved_h1_requests);
        std.debug.assert(current_connections >= reserved_connections);
        std.debug.assert(current_requests >= reserved_requests);
        self.listener_leases.store(leases - 1, .release);
        const remaining_h1 = reserved_h1 - reserved_h1_requests;
        self.reserved_h1_request_capacity.store(remaining_h1, .release);
        self.reserved_connection_capacity.store(current_connections - reserved_connections, .release);
        self.reserved_request_capacity.store(current_requests - reserved_requests, .release);
        if (reserved_h1 > 0 and remaining_h1 == 0) self.observer.stop();
    }

    fn lock(self: *HttpRuntime) void {
        while (!self.lifecycle_mutex.tryLock()) std.atomic.spinLoopHint();
    }
};

test "HTTP runtime listener leases share one cancellation observer lifecycle" {
    if (@import("builtin").os.tag == .freestanding) return;
    var runtime = HttpRuntime.init(std.testing.allocator, .{ .max_active_h1_requests = 2 });
    defer runtime.deinit();

    // A bounded control listener does not consume observer capacity or start
    // the observer, but it may coexist with and outlive application listeners.
    const control_requirements: HttpRuntime.ListenerRequirements = .{
        .max_h1_requests = 0,
        .max_connections = 0,
        .max_requests = 0,
    };
    const one_request: HttpRuntime.ListenerRequirements = .{
        .max_h1_requests = 1,
        .max_connections = 1,
        .max_requests = 1,
    };
    var control = try runtime.acquireListener(control_requirements);
    var first = try runtime.acquireListener(one_request);
    var second = try runtime.acquireListener(one_request);
    try std.testing.expectEqual(@as(usize, 3), runtime.stats().active_listener_leases);
    try std.testing.expectEqual(@as(usize, 2), runtime.stats().reserved_h1_request_capacity);
    try std.testing.expectError(error.HttpRuntimeCapacityExceeded, runtime.acquireListener(one_request));
    first.release();
    try std.testing.expectEqual(@as(usize, 2), runtime.stats().active_listener_leases);
    second.release();
    try std.testing.expectEqual(@as(usize, 1), runtime.stats().active_listener_leases);
    try std.testing.expectEqual(@as(usize, 0), runtime.stats().reserved_h1_request_capacity);
    var restarted = try runtime.acquireListener(.{
        .max_h1_requests = 2,
        .max_connections = 2,
        .max_requests = 2,
    });
    restarted.release();
    control.release();
    try std.testing.expectEqual(@as(usize, 0), runtime.stats().active_listener_leases);
}

test "HTTP runtime reserves bounded dedicated listener workers" {
    if (@import("builtin").os.tag == .freestanding) return;
    var runtime = HttpRuntime.init(std.testing.allocator, .{
        .max_active_h1_requests = 0,
        .max_listeners = 1,
    });
    defer runtime.deinit();

    const requirements: HttpRuntime.ListenerRequirements = .{
        .max_h1_requests = 0,
        .max_connections = 0,
        .max_requests = 0,
    };
    var listener = try runtime.acquireListener(requirements);
    try std.testing.expectEqual(@as(usize, 1), runtime.stats().listener_capacity);
    try std.testing.expectError(error.HttpRuntimeListenerCapacityExceeded, runtime.acquireListener(requirements));
    listener.release();
    var replacement = try runtime.acquireListener(requirements);
    replacement.release();
}

test "HTTP runtime reserves connection and request execution independently" {
    if (@import("builtin").os.tag == .freestanding) return;
    var runtime = HttpRuntime.init(std.testing.allocator, .{
        .max_active_h1_requests = 4,
        .max_active_connections = 3,
        .max_active_requests = 2,
    });
    defer runtime.deinit();

    var first = try runtime.acquireListener(.{
        .max_h1_requests = 2,
        .max_connections = 2,
        .max_requests = 1,
    });
    defer first.release();
    try std.testing.expectError(
        error.HttpRuntimeConnectionCapacityExceeded,
        runtime.acquireListener(.{
            .max_h1_requests = 1,
            .max_connections = 2,
            .max_requests = 1,
        }),
    );
    try std.testing.expectError(
        error.HttpRuntimeRequestCapacityExceeded,
        runtime.acquireListener(.{
            .max_h1_requests = 1,
            .max_connections = 1,
            .max_requests = 2,
        }),
    );
    const stats_snapshot = runtime.stats();
    try std.testing.expectEqual(@as(usize, 2), stats_snapshot.reserved_connection_capacity);
    try std.testing.expectEqual(@as(usize, 1), stats_snapshot.reserved_request_capacity);
}
