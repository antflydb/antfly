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

const builtin = @import("builtin");
const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const common = @import("http_common.zig");
const PeerObserver = @import("peer_disconnect_observer.zig").Observer;
const threaded_io_limits = @import("../threaded_io_limits.zig");

pub const default_max_request_bytes: usize = 32 * 1024 * 1024;
pub const default_request_stack_size: usize = 8 * 1024 * 1024;
pub const default_header_read_timeout_ms: u32 = 30_000;
pub const default_body_read_timeout_ms: u32 = 120_000;
pub const default_accept_error_backoff_initial_ms: u32 = 5;
pub const default_accept_error_backoff_max_ms: u32 = 1_000;
pub const default_process_io_concurrent_limit: u32 = threaded_io_limits.service;

const ProcessIo = struct {
    var lock: std.atomic.Mutex = .unlocked;
    var runtime: ?*std.Io.Threaded = null;
    var ref_count: usize = 0;

    fn acquire() *std.Io.Threaded {
        platform_sync.lockYielding(&lock);
        defer lock.unlock();

        if (runtime == null) {
            const io_impl = std.heap.page_allocator.create(std.Io.Threaded) catch @panic("OOM");
            io_impl.* = std.Io.Threaded.init(std.heap.page_allocator, .{
                .stack_size = default_request_stack_size,
                // Public listeners multiplex deadlines and peer observation
                // outside the executor. Keep a finite backstop for any other
                // concurrent work submitted through this process-wide runtime.
                .concurrent_limit = .limited(default_process_io_concurrent_limit),
            });
            runtime = io_impl;
        }
        ref_count += 1;
        return runtime.?;
    }

    fn release(io_impl: *std.Io.Threaded) void {
        platform_sync.lockYielding(&lock);
        defer lock.unlock();

        std.debug.assert(runtime == io_impl);
        std.debug.assert(ref_count > 0);
        ref_count -= 1;
        if (ref_count != 0) return;

        io_impl.deinit();
        std.heap.page_allocator.destroy(io_impl);
        runtime = null;
    }
};

fn sleepMs(ms: u64) void {
    var req = std.posix.timespec{
        .sec = @intCast(ms / std.time.ms_per_s),
        .nsec = @intCast((ms % std.time.ms_per_s) * std.time.ns_per_ms),
    };
    while (true) switch (std.posix.errno(std.posix.system.nanosleep(&req, &req))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn isRequestCancellation(err: anyerror) bool {
    return err == error.Cancelled or err == error.Canceled;
}

pub const StdHttpListenerConfig = struct {
    bind_host: []const u8 = "127.0.0.1",
    bind_port: u16 = 0,
    kernel_backlog: u31 = 512,
    reuse_address: bool = true,
    recv_buffer_bytes: usize = 8 * 1024,
    send_buffer_bytes: usize = 8 * 1024,
    max_request_bytes: usize = default_max_request_bytes,
    header_read_timeout_ms: u32 = default_header_read_timeout_ms,
    body_read_timeout_ms: u32 = default_body_read_timeout_ms,
    thread_stack_size: usize = default_request_stack_size,
    serve_in_connection_threads: bool = false,
    max_connection_threads: u32 = 0,
    /// Bounds admitted executor work separately from accepted sockets. Zero
    /// keeps the historical unlimited behavior.
    max_active_requests: u32 = 0,
    accept_error_backoff_initial_ms: u32 = default_accept_error_backoff_initial_ms,
    accept_error_backoff_max_ms: u32 = default_accept_error_backoff_max_ms,
};

pub const StdHttpListener = struct {
    pub const RuntimeStats = struct {
        max_connection_threads: u32,
        active_connection_threads: u32,
        peak_connection_threads: u32,
        max_active_requests: u32,
        active_requests: u32,
        peak_active_requests: u32,
        rejected_requests_total: u64,
        accept_errors_total: u64,
        cancellation_watcher_start_failures_total: u64,
        deadline_observer_failures_total: u64,
        deadline_expirations_total: u64,
        peer_disconnects_total: u64,
        active_peer_observers: usize,
        active_deadline_observers: usize,
    };

    const IoOwner = enum {
        process_shared,
        shared,
    };

    /// Heap-stable ownership published before a connection task is handed to
    /// `std.Io.Group`. `stop()` can therefore shut down every accepted socket
    /// even when the executor has not started the task yet.
    const ConnectionTask = struct {
        listener: *StdHttpListener,
        stream: std.Io.net.Stream,

        fn deinit(self: *ConnectionTask) void {
            const listener = self.listener;
            listener.unregisterActiveStream(&self.stream);
            self.stream.close(listener.io_impl.io());
            listener.alloc.destroy(self);
        }
    };

    alloc: std.mem.Allocator,
    cfg: StdHttpListenerConfig,
    app: common.RequestExecutor,
    streaming_app: ?common.StreamingRequestExecutor = null,
    io_impl: *std.Io.Threaded,
    io_owner: IoOwner,
    server: ?std.Io.net.Server = null,
    thread: ?std.Thread = null,
    connection_group: std.Io.Group = .init,
    stopping: std.atomic.Value(bool) = .init(false),
    active_connection_threads: std.atomic.Value(u32) = .init(0),
    peak_connection_threads: std.atomic.Value(u32) = .init(0),
    active_requests: std.atomic.Value(u32) = .init(0),
    peak_active_requests: std.atomic.Value(u32) = .init(0),
    rejected_requests_total: std.atomic.Value(u64) = .init(0),
    accept_errors_total: std.atomic.Value(u64) = .init(0),
    cancellation_watcher_start_failures_total: std.atomic.Value(u64) = .init(0),
    deadline_observer_failures_total: std.atomic.Value(u64) = .init(0),
    deadline_expirations_total: std.atomic.Value(u64) = .init(0),
    peer_disconnects_total: std.atomic.Value(u64) = .init(0),
    peer_observer: ?PeerObserver = null,
    active_streams_lock: std.atomic.Mutex = .unlocked,
    active_streams: std.ArrayListUnmanaged(*const std.Io.net.Stream) = .empty,

    pub fn init(
        alloc: std.mem.Allocator,
        cfg: StdHttpListenerConfig,
        app: common.RequestExecutor,
    ) StdHttpListener {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .app = app,
            .io_impl = ProcessIo.acquire(),
            .io_owner = .process_shared,
        };
    }

    pub fn initShared(
        alloc: std.mem.Allocator,
        cfg: StdHttpListenerConfig,
        app: common.RequestExecutor,
        io_impl: *std.Io.Threaded,
    ) StdHttpListener {
        return .{
            .alloc = alloc,
            .cfg = cfg,
            .app = app,
            .io_impl = io_impl,
            .io_owner = .shared,
        };
    }

    pub fn setStreamingExecutor(self: *StdHttpListener, app: common.StreamingRequestExecutor) void {
        self.streaming_app = app;
    }

    pub fn deinit(self: *StdHttpListener) void {
        self.stop();
        self.active_streams.deinit(self.alloc);
        if (self.io_owner == .process_shared) ProcessIo.release(self.io_impl);
        self.* = undefined;
    }

    pub fn start(self: *StdHttpListener) !void {
        if (self.server != null) return error.AlreadyListening;
        self.stopping.store(false, .release);

        const observer_capacity = if (self.cfg.max_connection_threads > 0)
            self.cfg.max_connection_threads
        else if (self.cfg.max_active_requests > 0)
            self.cfg.max_active_requests
        else
            64;
        self.peer_observer = PeerObserver.init(self.alloc, observer_capacity);
        self.peer_observer.?.start() catch |err| {
            self.peer_observer.?.deinit();
            self.peer_observer = null;
            return err;
        };
        errdefer {
            self.peer_observer.?.deinit();
            self.peer_observer = null;
        }

        const listen_addr = try std.Io.net.IpAddress.parse(self.cfg.bind_host, self.cfg.bind_port);
        const TestBindFailure = struct {
            fn handle(err: anyerror) anyerror {
                if (builtin.is_test and err == error.Unexpected) {
                    std.debug.print("skipping listener test: local bind unavailable in this environment\n", .{});
                    return error.SkipZigTest;
                }
                return err;
            }
        };
        self.server = listen_addr.listen(self.io_impl.io(), .{
            .kernel_backlog = self.cfg.kernel_backlog,
            .reuse_address = self.cfg.reuse_address,
        }) catch |err| blk: {
            if (err == error.Unexpected and self.cfg.reuse_address) {
                if (builtin.is_test) {
                    std.debug.print(
                        "std http listener bind retrying without reuse_address host={s} port={d}\n",
                        .{ self.cfg.bind_host, self.cfg.bind_port },
                    );
                } else {
                    std.log.err(
                        "std http listener bind retrying without reuse_address host={s} port={d}",
                        .{ self.cfg.bind_host, self.cfg.bind_port },
                    );
                }
                break :blk listen_addr.listen(self.io_impl.io(), .{
                    .kernel_backlog = self.cfg.kernel_backlog,
                    .reuse_address = false,
                }) catch |retry_err| return TestBindFailure.handle(retry_err);
            }
            return TestBindFailure.handle(err);
        };
        errdefer {
            self.server.?.deinit(self.io_impl.io());
            self.server = null;
        }

        self.thread = try std.Thread.spawn(.{ .stack_size = self.cfg.thread_stack_size }, serve, .{self});
    }

    pub fn stop(self: *StdHttpListener) void {
        const io = self.io_impl.io();
        const bound_addr = self.boundAddress();
        self.stopping.store(true, .release);
        self.shutdownActiveStreams(io);
        if (self.thread) |thread| {
            if (bound_addr) |addr| {
                const wake_io = std.Io.Threaded.global_single_threaded.io();
                const wake_addr = listenerWakeAddress(addr);
                if (wake_addr.connect(wake_io, .{ .mode = .stream })) |stream| {
                    var wake_stream = stream;
                    wake_stream.close(wake_io);
                } else |_| {}
            }
            thread.join();
            self.thread = null;
        }
        self.shutdownActiveStreams(io);
        // The accept task is joined above, so no new group members can be
        // admitted. Cancellation joins every connection task before stream,
        // observer, server, or shared-I/O state is released.
        self.connection_group.cancel(io);
        std.debug.assert(self.active_connection_threads.load(.acquire) == 0);
        if (self.peer_observer) |*observer| observer.deinit();
        self.peer_observer = null;
        if (self.server) |*server| {
            server.deinit(io);
            self.server = null;
        }
    }

    fn listenerWakeAddress(bound: std.Io.net.IpAddress) std.Io.net.IpAddress {
        return switch (bound) {
            .ip4 => |addr| if (std.mem.allEqual(u8, &addr.bytes, 0))
                .{ .ip4 = std.Io.net.Ip4Address.loopback(addr.port) }
            else
                bound,
            .ip6 => |addr| if (std.mem.allEqual(u8, &addr.bytes, 0))
                .{ .ip6 = std.Io.net.Ip6Address.loopback(addr.port) }
            else
                bound,
        };
    }

    pub fn boundAddress(self: *const StdHttpListener) ?std.Io.net.IpAddress {
        const server = self.server orelse return null;
        return server.socket.address;
    }

    pub fn baseUri(self: *const StdHttpListener, alloc: std.mem.Allocator) ![]u8 {
        const addr = self.boundAddress() orelse return error.NotListening;
        return try std.fmt.allocPrint(alloc, "http://{f}", .{addr});
    }

    pub fn runtimeStats(self: *const StdHttpListener) RuntimeStats {
        return .{
            .max_connection_threads = self.cfg.max_connection_threads,
            .active_connection_threads = self.active_connection_threads.load(.acquire),
            .peak_connection_threads = self.peak_connection_threads.load(.acquire),
            .max_active_requests = self.cfg.max_active_requests,
            .active_requests = self.active_requests.load(.acquire),
            .peak_active_requests = self.peak_active_requests.load(.acquire),
            .rejected_requests_total = self.rejected_requests_total.load(.acquire),
            .accept_errors_total = self.accept_errors_total.load(.acquire),
            .cancellation_watcher_start_failures_total = self.cancellation_watcher_start_failures_total.load(.acquire),
            .deadline_observer_failures_total = self.deadline_observer_failures_total.load(.acquire),
            .deadline_expirations_total = self.deadline_expirations_total.load(.acquire),
            .peer_disconnects_total = self.peer_disconnects_total.load(.acquire),
            .active_peer_observers = if (self.peer_observer) |*observer| observer.activePeerCount() else 0,
            .active_deadline_observers = if (self.peer_observer) |*observer| observer.activeDeadlineCount() else 0,
        };
    }

    fn serve(self: *StdHttpListener) void {
        const io = self.io_impl.io();
        defer if (self.stopping.load(.acquire)) self.shutdownActiveStreams(io);
        var slot_held = false;
        var accept_error_backoff_ms: u32 = self.acceptErrorBackoffInitialMs();
        defer if (slot_held) self.releaseConnectionThreadSlot();
        while (true) {
            if (self.stopping.load(.acquire)) return;
            if (self.cfg.serve_in_connection_threads and !slot_held) {
                // Hold a connection-thread slot before accepting so that
                // saturation backpressures into the kernel backlog. The
                // alternatives both misbehave: accept-then-close sends RST to
                // a client that already wrote its request, and serving the
                // connection inline blocks the accept thread on a client
                // read, stalling accepts (and stop()) on an idle peer.
                if (!self.tryAcquireConnectionThreadSlot()) {
                    sleepMs(1);
                    continue;
                }
                slot_held = true;
            }
            const stream = if (self.server) |*server|
                server.accept(io) catch |err| switch (err) {
                    error.SocketNotListening => return,
                    error.Canceled => return,
                    else => {
                        if (self.stopping.load(.acquire)) return;
                        _ = self.accept_errors_total.fetchAdd(1, .monotonic);
                        std.log.warn("std http listener accept failed; backing off delay_ms={d} err={s}", .{ accept_error_backoff_ms, @errorName(err) });
                        sleepMs(accept_error_backoff_ms);
                        accept_error_backoff_ms = @min(self.acceptErrorBackoffMaxMs(), accept_error_backoff_ms *| 2);
                        continue;
                    },
                }
            else
                return;
            // Successful accepts reset an earlier FD/resource-exhaustion
            // backoff, so recovery traffic is not penalized after load falls.
            accept_error_backoff_ms = self.acceptErrorBackoffInitialMs();
            if (self.stopping.load(.acquire)) {
                var wake_stream = stream;
                wake_stream.close(io);
                return;
            }
            if (self.cfg.serve_in_connection_threads) {
                const task = self.prepareConnectionTask(stream) catch |err| {
                    var rejected_stream = stream;
                    rejected_stream.close(io);
                    self.releaseConnectionThreadSlot();
                    slot_held = false;
                    if (err != error.OutOfMemory)
                        std.log.warn("std http listener connection registration failed err={s}", .{@errorName(err)});
                    continue;
                };
                self.connection_group.concurrent(
                    io,
                    serveStreamThread,
                    .{task},
                ) catch |err| {
                    if (self.stopping.load(.acquire)) {
                        task.deinit();
                        self.releaseConnectionThreadSlot();
                        slot_held = false;
                        return;
                    }
                    // Resource exhaustion must degrade to bounded inline
                    // service, not drop an already accepted request. The
                    // listener thread remains joinable by stop(), and retains
                    // this connection's admission slot while serving it.
                    if (err != error.ConcurrencyUnavailable)
                        std.log.warn("std http listener connection task handoff failed err={s}", .{@errorName(err)});
                    self.serveRegisteredStream(&task.stream);
                    task.deinit();
                    self.releaseConnectionThreadSlot();
                    slot_held = false;
                    continue;
                };
                // The structured group owns the task and its admission slot.
                // stop() cancels and joins the group before destroying any
                // listener or shared-I/O state.
                slot_held = false;
                continue;
            }
            self.serveStream(stream);
        }
    }

    fn releaseConnectionThreadSlot(self: *StdHttpListener) void {
        _ = self.active_connection_threads.fetchSub(1, .acq_rel);
    }

    fn tryAcquireConnectionThreadSlot(self: *StdHttpListener) bool {
        const max_threads = self.cfg.max_connection_threads;
        while (true) {
            const active = self.active_connection_threads.load(.acquire);
            if (max_threads > 0 and active >= max_threads) return false;
            if (self.active_connection_threads.cmpxchgWeak(active, active + 1, .acq_rel, .acquire) == null) {
                atomicMaxU32(&self.peak_connection_threads, active + 1);
                return true;
            }
        }
    }

    fn tryAcquireRequestSlot(self: *StdHttpListener) bool {
        const max_requests = self.cfg.max_active_requests;
        while (true) {
            const active = self.active_requests.load(.acquire);
            if (max_requests > 0 and active >= max_requests) {
                _ = self.rejected_requests_total.fetchAdd(1, .monotonic);
                return false;
            }
            if (self.active_requests.cmpxchgWeak(active, active + 1, .acq_rel, .acquire) == null) {
                atomicMaxU32(&self.peak_active_requests, active + 1);
                return true;
            }
        }
    }

    fn releaseRequestSlot(self: *StdHttpListener) void {
        _ = self.active_requests.fetchSub(1, .acq_rel);
    }

    fn atomicMaxU32(value: *std.atomic.Value(u32), candidate: u32) void {
        var observed = value.load(.acquire);
        while (observed < candidate) {
            if (value.cmpxchgWeak(observed, candidate, .acq_rel, .acquire) == null) return;
            observed = value.load(.acquire);
        }
    }

    fn requestConsumesExpensiveSlot(method: common.Method, uri: []const u8) bool {
        // Query execution is bounded independently from connection handling.
        // Health, recovery, and other public control routes retain capacity
        // even while every expensive query slot is held.
        if (method != .POST) return false;
        const path = if (std.mem.indexOfScalar(u8, uri, '?')) |query_index| uri[0..query_index] else uri;
        return std.mem.eql(u8, path, "/query") or std.mem.endsWith(u8, path, "/query");
    }

    fn acceptErrorBackoffInitialMs(self: *const StdHttpListener) u32 {
        return @max(@as(u32, 1), self.cfg.accept_error_backoff_initial_ms);
    }

    fn acceptErrorBackoffMaxMs(self: *const StdHttpListener) u32 {
        return @max(self.acceptErrorBackoffInitialMs(), self.cfg.accept_error_backoff_max_ms);
    }

    fn prepareConnectionTask(self: *StdHttpListener, stream: std.Io.net.Stream) !*ConnectionTask {
        const task = try self.alloc.create(ConnectionTask);
        errdefer self.alloc.destroy(task);
        task.* = .{ .listener = self, .stream = stream };
        try self.registerActiveStream(&task.stream);
        if (self.stopping.load(.acquire)) task.stream.shutdown(self.io_impl.io(), .both) catch {};
        return task;
    }

    fn serveStreamThread(task: *ConnectionTask) std.Io.Cancelable!void {
        const self = task.listener;
        defer self.releaseConnectionThreadSlot();
        defer task.deinit();
        self.serveRegisteredStream(&task.stream);
    }

    fn serveStream(self: *StdHttpListener, stream: std.Io.net.Stream) void {
        const io = self.io_impl.io();
        var owned_stream = stream;
        self.registerActiveStream(&owned_stream) catch |err| {
            std.log.warn("std http listener active stream registration failed err={s}", .{@errorName(err)});
            owned_stream.close(io);
            return;
        };
        if (self.stopping.load(.acquire)) owned_stream.shutdown(io, .both) catch {};
        defer {
            self.unregisterActiveStream(&owned_stream);
            owned_stream.close(io);
        }

        self.serveRegisteredStream(&owned_stream);
    }

    fn serveRegisteredStream(self: *StdHttpListener, owned_stream: *std.Io.net.Stream) void {
        const io = self.io_impl.io();

        const recv_buffer = self.alloc.alloc(u8, self.cfg.recv_buffer_bytes) catch return;
        defer self.alloc.free(recv_buffer);
        const send_buffer = self.alloc.alloc(u8, self.cfg.send_buffer_bytes) catch return;
        defer self.alloc.free(send_buffer);

        var stream_reader = owned_stream.reader(io, recv_buffer);
        var stream_writer = owned_stream.writer(io, send_buffer);
        var server: std.http.Server = .init(&stream_reader.interface, &stream_writer.interface);

        var request = self.receiveHeadWithTimeout(io, owned_stream, &server) catch |err| {
            // Closing here with unread request bytes sends RST to a client
            // that is awaiting a response; keep the common idle-close quiet
            // but record anything else so resets are diagnosable from logs.
            if (err == error.Timeout) {
                std.log.warn("http receive head timed out timeout_ms={d}", .{self.cfg.header_read_timeout_ms});
            } else if (err != error.HttpConnectionClosing and err != error.EndOfStream) {
                std.log.warn("http receive head failed err={s}", .{@errorName(err)});
            }
            return;
        };
        const request_method = @tagName(request.head.method);
        var request_target_buf: [512]u8 = undefined;
        const request_target_len = @min(request.head.target.len, request_target_buf.len);
        @memcpy(request_target_buf[0..request_target_len], request.head.target[0..request_target_len]);
        const request_target = request_target_buf[0..request_target_len];
        self.handleRequest(owned_stream, &request) catch |err| {
            // Request-lifecycle cancellation is an expected terminal state:
            // the peer has gone away or the owning I/O task is being torn
            // down. Do not turn disconnect storms into error-log storms or
            // spend work attempting a 500 on a connection that cannot use it.
            if (isRequestCancellation(err)) return;
            if (self.stopping.load(.acquire)) {
                std.log.warn("http request canceled during listener stop method={s} target={s} err={s}", .{
                    request_method,
                    request_target,
                    @errorName(err),
                });
                return;
            }
            std.log.err("http request handler error method={s} target={s} err={s}", .{
                request_method,
                request_target,
                @errorName(err),
            });
            _ = request.respond("internal server error", .{
                .status = .internal_server_error,
                .keep_alive = false,
            }) catch {};
        };
    }

    fn receiveHeadWithTimeout(
        self: *StdHttpListener,
        io: std.Io,
        stream: *const std.Io.net.Stream,
        server: *std.http.Server,
    ) !std.http.Server.Request {
        const timeout_ms = self.cfg.header_read_timeout_ms;
        if (timeout_ms == 0) return try server.receiveHead();

        if (comptime builtin.os.tag != .windows and builtin.os.tag != .freestanding) {
            var deadline: PeerObserver.Deadline = .{};
            const observer = if (self.peer_observer) |*value| value else {
                _ = self.deadline_observer_failures_total.fetchAdd(1, .monotonic);
                return error.ObserverUnavailable;
            };
            var registration = observer.registerDeadline(
                stream.socket.handle,
                timeout_ms,
                &deadline,
                &self.deadline_expirations_total,
            ) catch |err| {
                _ = self.deadline_observer_failures_total.fetchAdd(1, .monotonic);
                return err;
            };
            const receive_result = server.receiveHead();
            // Retire the registration before inspecting its outcome. This is
            // the completion/expiration arbitration point: once deinit()
            // returns, the observer no longer owns the stack-backed deadline
            // and cannot shut down a successfully completed socket between
            // the state check and this function returning.
            switch (registration.finishDeadline(&deadline)) {
                .completed => {},
                .observer_failed => {
                    _ = self.deadline_observer_failures_total.fetchAdd(1, .monotonic);
                    _ = receive_result catch {};
                    return error.ObserverUnavailable;
                },
                .expired => {
                    _ = receive_result catch {};
                    return error.Timeout;
                },
            }
            return try receive_result;
        }

        // Windows retains the threaded timeout race until the socket observer
        // has a native wait-set implementation there. POSIX production paths
        // never submit one sleeping Io.concurrent task per connection.
        const RaceState = enum(u8) {
            pending,
            receive_complete,
            timed_out,
        };

        const Task = struct {
            fn timeoutTask(
                task_io: std.Io,
                timeout: std.Io.Timeout,
                state: *std.atomic.Value(RaceState),
                active_stream: *const std.Io.net.Stream,
            ) std.Io.Cancelable!void {
                try timeout.sleep(task_io);
                if (state.cmpxchgStrong(.pending, .timed_out, .acq_rel, .acquire) == null) {
                    active_stream.shutdown(task_io, .both) catch {};
                }
            }
        };

        var state = std.atomic.Value(RaceState).init(.pending);
        var timeout_group: std.Io.Group = .init;
        try timeout_group.concurrent(io, Task.timeoutTask, .{
            io,
            std.Io.Timeout{ .duration = .{
                .raw = std.Io.Duration.fromMilliseconds(@intCast(timeout_ms)),
                .clock = .awake,
            } },
            &state,
            stream,
        });
        defer timeout_group.cancel(io);

        const receive_result = server.receiveHead();
        if (state.cmpxchgStrong(.pending, .receive_complete, .acq_rel, .acquire) != null) {
            _ = receive_result catch {};
            return error.Timeout;
        }
        return try receive_result;
    }

    fn registerActiveStream(self: *StdHttpListener, stream: *const std.Io.net.Stream) !void {
        lockAtomic(&self.active_streams_lock);
        defer self.active_streams_lock.unlock();
        try self.active_streams.append(self.alloc, stream);
    }

    fn unregisterActiveStream(self: *StdHttpListener, stream: *const std.Io.net.Stream) void {
        lockAtomic(&self.active_streams_lock);
        defer self.active_streams_lock.unlock();
        for (self.active_streams.items, 0..) |active_stream, i| {
            if (active_stream == stream) {
                _ = self.active_streams.swapRemove(i);
                return;
            }
        }
    }

    fn shutdownActiveStreams(self: *StdHttpListener, io: std.Io) void {
        lockAtomic(&self.active_streams_lock);
        defer self.active_streams_lock.unlock();
        for (self.active_streams.items) |stream| {
            stream.shutdown(io, .both) catch {};
        }
    }

    fn lockAtomic(mutex: *std.atomic.Mutex) void {
        platform_sync.lockYielding(mutex);
    }

    fn handleRequest(
        self: *StdHttpListener,
        stream: ?*const std.Io.net.Stream,
        request: *std.http.Server.Request,
    ) !void {
        const method = mapMethod(request.head.method) orelse {
            try request.respond("method not allowed", .{
                .status = .method_not_allowed,
                .keep_alive = false,
            });
            return;
        };

        const uri = try self.alloc.dupe(u8, request.head.target);
        defer self.alloc.free(uri);
        var authorization: ?[]u8 = null;
        defer if (authorization) |value| self.alloc.free(value);
        var headers = std.ArrayListUnmanaged(common.RequestHeader).empty;
        defer {
            for (headers.items) |header| {
                self.alloc.free(header.name);
                self.alloc.free(header.value);
            }
            headers.deinit(self.alloc);
        }
        var header_it = request.iterateHeaders();
        while (header_it.next()) |header| {
            const name = try self.alloc.dupe(u8, header.name);
            const value = self.alloc.dupe(u8, header.value) catch |err| {
                self.alloc.free(name);
                return err;
            };
            headers.append(self.alloc, .{ .name = name, .value = value }) catch |err| {
                self.alloc.free(name);
                self.alloc.free(value);
                return err;
            };
            if (std.ascii.eqlIgnoreCase(header.name, "authorization") and authorization == null) {
                authorization = try self.alloc.dupe(u8, header.value);
            }
        }
        const content_type = if (request.head.content_type) |value|
            try self.alloc.dupe(u8, value)
        else
            null;
        defer if (content_type) |value| self.alloc.free(value);

        const consumes_expensive_slot = requestConsumesExpensiveSlot(method, uri);
        if (consumes_expensive_slot and !self.tryAcquireRequestSlot()) {
            // Shed before application execution. Clients get a deterministic,
            // retryable response instead of leaving expensive work running
            // until their own timeout while the listener runs out of FDs.
            try request.respond("service overloaded; retry later", .{
                .status = @enumFromInt(429),
                .keep_alive = false,
                .extra_headers = &.{.{ .name = "retry-after", .value = "1" }},
            });
            return;
        }
        defer if (consumes_expensive_slot) self.releaseRequestSlot();

        // Reserve query capacity before a slow request body can occupy every
        // public connection worker. Control routes retain the unreserved
        // connection budget while rejected query uploads receive 429 early.
        const body = (try self.readRequestBody(stream, request)) orelse return;
        defer self.alloc.free(body);

        var cancellation = common.RequestCancellation{};
        var peer_registration: PeerObserver.Registration = .{};
        if (stream) |active_stream| {
            if (self.peer_observer) |*observer| {
                peer_registration = observer.register(
                    active_stream.socket.handle,
                    &cancellation,
                    &self.peer_disconnects_total,
                    null,
                ) catch blk: {
                    _ = self.cancellation_watcher_start_failures_total.fetchAdd(1, .monotonic);
                    if (consumes_expensive_slot) {
                        try request.respond("query cancellation capacity unavailable", .{
                            .status = @enumFromInt(503),
                            .keep_alive = false,
                            .extra_headers = &.{.{ .name = "retry-after", .value = "1" }},
                        });
                        return;
                    }
                    cancellation.cancel();
                    break :blk .{};
                };
            } else {
                _ = self.cancellation_watcher_start_failures_total.fetchAdd(1, .monotonic);
                if (consumes_expensive_slot) {
                    try request.respond("query cancellation capacity unavailable", .{
                        .status = @enumFromInt(503),
                        .keep_alive = false,
                        .extra_headers = &.{.{ .name = "retry-after", .value = "1" }},
                    });
                    return;
                }
                cancellation.cancel();
            }
        }
        defer peer_registration.deinit();
        const http_req: common.HttpRequest = .{
            .method = method,
            .uri = uri,
            .headers = headers.items,
            .authorization = authorization,
            .content_type = content_type,
            .body = body,
            .cancellation = &cancellation,
        };

        if (self.streaming_app) |streaming_app| {
            const body_buffer = try self.alloc.alloc(u8, self.cfg.send_buffer_bytes);
            defer self.alloc.free(body_buffer);
            var stream_writer = StreamingBodyWriter{
                .request = request,
                .buffer = body_buffer,
            };
            const handled = streaming_app.execute(self.alloc, http_req, stream_writer.iface()) catch |err| {
                if (stream_writer.started()) {
                    // Once a streaming request is cancelled, closing the
                    // connection is the response boundary. Finalizing a
                    // partial body would incorrectly present it as complete.
                    if (isRequestCancellation(err)) return;
                    std.log.err("http streaming request handler error after response start: {s}", .{@errorName(err)});
                    _ = stream_writer.end() catch {};
                    return;
                }
                return err;
            };
            if (handled) {
                try stream_writer.end();
                return;
            }
        }

        var response = try self.app.execute(self.alloc, http_req);
        defer response.deinit(self.alloc);

        const header_count = response.headers.len + @intFromBool(response.content_type != null);
        var extra_headers: []std.http.Header = if (header_count > 0)
            try self.alloc.alloc(std.http.Header, header_count)
        else
            @constCast((&[_]std.http.Header{})[0..]);
        defer if (header_count > 0) self.alloc.free(extra_headers);

        var header_index: usize = 0;
        if (response.content_type) |value| {
            extra_headers[header_index] = .{ .name = "content-type", .value = value };
            header_index += 1;
        }
        for (response.headers) |header| {
            extra_headers[header_index] = .{ .name = header.name, .value = header.value };
            header_index += 1;
        }

        try request.respond(response.body, .{
            .status = @enumFromInt(response.status),
            .keep_alive = false,
            .extra_headers = extra_headers,
        });
    }

    const StreamingBodyWriter = struct {
        request: *std.http.Server.Request,
        buffer: []u8,
        body_writer: ?std.http.BodyWriter = null,

        fn iface(self: *@This()) common.StreamWriter {
            return .{
                .ptr = self,
                .vtable = &.{
                    .start = startResponse,
                    .write_all = writeAll,
                    .flush = flush,
                },
            };
        }

        fn startResponse(ptr: *anyopaque, alloc: std.mem.Allocator, response: common.StreamingResponse) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body_writer != null) return error.ResponseAlreadyStarted;

            const header_count = response.headers.len + @intFromBool(response.content_type != null);
            var extra_headers: []std.http.Header = if (header_count > 0)
                try alloc.alloc(std.http.Header, header_count)
            else
                @constCast((&[_]std.http.Header{})[0..]);
            defer if (header_count > 0) alloc.free(extra_headers);

            var header_index: usize = 0;
            if (response.content_type) |value| {
                extra_headers[header_index] = .{ .name = "content-type", .value = value };
                header_index += 1;
            }
            for (response.headers) |header| {
                extra_headers[header_index] = .{ .name = header.name, .value = header.value };
                header_index += 1;
            }

            self.body_writer = try self.request.respondStreaming(self.buffer, .{
                .respond_options = .{
                    .status = @enumFromInt(response.status),
                    .keep_alive = false,
                    .extra_headers = extra_headers,
                },
            });
        }

        fn writeAll(ptr: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body_writer) |*body_writer| {
                try body_writer.writer.writeAll(bytes);
                return;
            }
            return error.ResponseNotStarted;
        }

        fn flush(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body_writer) |*body_writer| {
                try body_writer.writer.flush();
                try body_writer.flush();
                return;
            }
            return error.ResponseNotStarted;
        }

        fn started(self: *const @This()) bool {
            return self.body_writer != null;
        }

        fn end(self: *@This()) !void {
            if (self.body_writer) |*body_writer| {
                try body_writer.end();
                self.body_writer = null;
            }
        }
    };

    fn readRequestBody(
        self: *StdHttpListener,
        stream: ?*const std.Io.net.Stream,
        request: *std.http.Server.Request,
    ) !?[]u8 {
        const has_body = request.head.content_length != null or request.head.transfer_encoding != .none;
        if (!has_body) return try self.alloc.dupe(u8, &.{});

        if (request.head.content_length) |content_length| {
            if (content_length > self.cfg.max_request_bytes) {
                if (request.head.expect != null) request.head.expect = null;
                try request.respond("request too large", .{
                    .status = .payload_too_large,
                    .keep_alive = false,
                });
                return null;
            }
        }

        const body_reader = if (request.head.expect) |expect| blk: {
            if (!std.ascii.eqlIgnoreCase(expect, "100-continue")) {
                request.head.expect = null;
                try request.respond("expectation failed", .{
                    .status = .expectation_failed,
                    .keep_alive = false,
                });
                return null;
            }

            // Zig stdlib expects the normalized token when writing the interim response.
            request.head.expect = "100-continue";
            break :blk request.readerExpectContinue(&.{}) catch |err| switch (err) {
                error.HttpExpectationFailed => unreachable,
                else => return err,
            };
        } else request.readerExpectNone(&.{});

        return self.readRequestBodyWithTimeout(stream, body_reader) catch |err| switch (err) {
            error.Timeout => blk: {
                std.log.warn("http request body read timed out timeout_ms={d}", .{self.cfg.body_read_timeout_ms});
                break :blk null;
            },
            error.StreamTooLong => blk: {
                try request.respond("request too large", .{
                    .status = .payload_too_large,
                    .keep_alive = false,
                });
                break :blk null;
            },
            else => return err,
        };
    }

    fn readRequestBodyWithTimeout(
        self: *StdHttpListener,
        stream: ?*const std.Io.net.Stream,
        body_reader: *std.Io.Reader,
    ) ![]u8 {
        const timeout_ms = self.cfg.body_read_timeout_ms;
        if (timeout_ms == 0 or stream == null) return try body_reader.allocRemaining(self.alloc, .limited(self.cfg.max_request_bytes));

        if (comptime builtin.os.tag != .windows and builtin.os.tag != .freestanding) {
            var deadline: PeerObserver.Deadline = .{};
            const observer = if (self.peer_observer) |*value| value else {
                _ = self.deadline_observer_failures_total.fetchAdd(1, .monotonic);
                return error.ObserverUnavailable;
            };
            var registration = observer.registerDeadline(
                stream.?.socket.handle,
                timeout_ms,
                &deadline,
                &self.deadline_expirations_total,
            ) catch |err| {
                _ = self.deadline_observer_failures_total.fetchAdd(1, .monotonic);
                return err;
            };
            const read_result = body_reader.allocRemaining(self.alloc, .limited(self.cfg.max_request_bytes));
            // See receiveHeadWithTimeout: unregister synchronously so normal
            // completion and expiration have one mutex-ordered winner.
            switch (registration.finishDeadline(&deadline)) {
                .completed => {},
                .observer_failed => {
                    _ = self.deadline_observer_failures_total.fetchAdd(1, .monotonic);
                    if (read_result) |body| self.alloc.free(body) else |_| {}
                    return error.ObserverUnavailable;
                },
                .expired => {
                    if (read_result) |body| self.alloc.free(body) else |_| {}
                    return error.Timeout;
                },
            }
            return try read_result;
        }

        // See receiveHeadWithTimeout: this fallback is non-POSIX only.
        const RaceState = enum(u8) {
            pending,
            read_complete,
            timed_out,
        };

        const Task = struct {
            fn timeoutTask(
                task_io: std.Io,
                timeout: std.Io.Timeout,
                state: *std.atomic.Value(RaceState),
                active_stream: *const std.Io.net.Stream,
            ) std.Io.Cancelable!void {
                try timeout.sleep(task_io);
                if (state.cmpxchgStrong(.pending, .timed_out, .acq_rel, .acquire) == null) {
                    active_stream.shutdown(task_io, .both) catch {};
                }
            }
        };

        const io = self.io_impl.io();
        var state = std.atomic.Value(RaceState).init(.pending);
        var timeout_group: std.Io.Group = .init;
        try timeout_group.concurrent(io, Task.timeoutTask, .{
            io,
            std.Io.Timeout{ .duration = .{
                .raw = std.Io.Duration.fromMilliseconds(@intCast(timeout_ms)),
                .clock = .awake,
            } },
            &state,
            stream.?,
        });
        defer timeout_group.cancel(io);

        const read_result = body_reader.allocRemaining(self.alloc, .limited(self.cfg.max_request_bytes));
        if (state.cmpxchgStrong(.pending, .read_complete, .acq_rel, .acquire) != null) {
            if (read_result) |body| self.alloc.free(body) else |_| {}
            return error.Timeout;
        }
        return try read_result;
    }

    fn mapMethod(method: std.http.Method) ?common.Method {
        return switch (method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
            else => null,
        };
    }
};

test "std http listener wake address maps wildcard binds to loopback" {
    const expected4 = std.Io.net.Ip4Address.loopback(4200);
    const wake4 = StdHttpListener.listenerWakeAddress(try std.Io.net.IpAddress.parse("0.0.0.0", 4200));
    switch (wake4) {
        .ip4 => |addr| {
            try std.testing.expectEqualSlices(u8, &expected4.bytes, &addr.bytes);
            try std.testing.expectEqual(@as(u16, 4200), addr.port);
        },
        else => return error.TestUnexpectedResult,
    }

    const expected6 = std.Io.net.Ip6Address.loopback(4201);
    const wake6 = StdHttpListener.listenerWakeAddress(try std.Io.net.IpAddress.parse("::", 4201));
    switch (wake6) {
        .ip6 => |addr| {
            try std.testing.expectEqualSlices(u8, &expected6.bytes, &addr.bytes);
            try std.testing.expectEqual(@as(u16, 4201), addr.port);
        },
        else => return error.TestUnexpectedResult,
    }

    const bound = try std.Io.net.IpAddress.parse("127.0.0.2", 4202);
    const unchanged = StdHttpListener.listenerWakeAddress(bound);
    switch (unchanged) {
        .ip4 => |addr| switch (bound) {
            .ip4 => |expected| {
                try std.testing.expectEqualSlices(u8, &expected.bytes, &addr.bytes);
                try std.testing.expectEqual(expected.port, addr.port);
            },
            else => unreachable,
        },
        else => return error.TestUnexpectedResult,
    }
}

test "std http listener classifies request cancellation as expected termination" {
    try std.testing.expect(isRequestCancellation(error.Cancelled));
    try std.testing.expect(isRequestCancellation(error.Canceled));
    try std.testing.expect(!isRequestCancellation(error.Timeout));
    try std.testing.expect(!isRequestCancellation(error.Unexpected));
}

test "std http listener and executor round-trip raft batch route" {
    const raft_engine = @import("raft_engine");
    const http_driver = @import("../../raft/transport/http_driver.zig");
    const http_server = @import("../../raft/transport/http_server.zig");
    const std_http_executor = @import("std_http_executor.zig");

    const Handler = struct {
        seen: usize = 0,

        fn iface(self: *@This()) http_server.BatchHandler {
            return .{
                .ptr = self,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(ptr: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.seen += batch.groups.len;
        }
    };

    var handler = Handler{};
    var app = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        handler.iface(),
        null,
        null,
    );
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor: std_http_executor.StdHttpExecutor = undefined;
    executor.initInPlace(std.testing.allocator, .{});
    defer executor.deinit();
    var driver = http_driver.HttpFrameDriver.init(std.testing.allocator, .{}, executor.executor(), executor.io_impl.io());

    const msg = raft_engine.core.Message{
        .msg_type = .heartbeat,
        .from = 1,
        .to = 2,
        .term = 3,
    };
    const batch = raft_engine.runtime.transport_iface.PeerBatch{
        .peer_id = 2,
        .groups = (&[_]raft_engine.runtime.transport_iface.GroupMessageBatch{
            .{
                .group_id = 55,
                .messages = (&[_]raft_engine.core.Message{msg})[0..],
            },
        })[0..],
    };
    const frame = try raft_engine.runtime.BinaryCodec.codec().encodePeerBatch(std.testing.allocator, batch);
    defer raft_engine.runtime.BinaryCodec.codec().freeFrame(std.testing.allocator, frame);

    try driver.sendBatch(.{
        .peer_id = 2,
        .base_uri = base_uri,
        .body = frame.bytes,
        .content_type = frame.media_type,
    });
    try std.testing.expectEqual(@as(usize, 1), handler.seen);
}

test "std http executor enforces request timeout while waiting for response" {
    const std_http_executor = @import("std_http_executor.zig");

    const SlowApp = struct {
        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            _ = req;
            sleepMs(50);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "late"),
            };
        }
    };

    var app = SlowApp{};
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.iface());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/slow", .{base_uri});
    defer std.testing.allocator.free(uri);

    var executor: std_http_executor.StdHttpExecutor = undefined;
    executor.initInPlace(std.testing.allocator, .{});
    defer executor.deinit();

    try std.testing.expectError(error.Timeout, executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = uri,
        .timeout_ms = 5,
    }));
}

test "std http executor connect deadline does not expire an established request" {
    const std_http_executor = @import("std_http_executor.zig");

    const SlowApp = struct {
        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            // This exceeds the DNS/connect budget by design. Once connected,
            // only the response idle timeout or an explicit whole-request
            // deadline may terminate the request.
            sleepMs(250);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain"),
                .body = try alloc.dupe(u8, "progressing"),
            };
        }
    };

    var app = SlowApp{};
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.iface());
    defer listener.deinit();
    try listener.start();

    const bound = listener.boundAddress().?;
    const uri = try std.fmt.allocPrint(
        std.testing.allocator,
        "http://localhost:{d}/replication/start",
        .{bound.getPort()},
    );
    defer std.testing.allocator.free(uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{
        .resolve_before_connect = true,
        .connect_timeout_ms = 100,
        .request_timeout_ms = 0,
    });
    defer executor.deinit();

    var response = try executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = uri,
    });
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    try std.testing.expectEqualStrings("progressing", response.body);
}

test "std http executor cancels an in-flight production request and retires its socket" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        entered: std.atomic.Value(bool) = .init(false),
        observed_peer_cancel: std.atomic.Value(bool) = .init(false),
        release: std.atomic.Value(bool) = .init(false),

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, "/fast")) {
                return .{
                    .status = 200,
                    .body = try alloc.dupe(u8, "ok"),
                };
            }
            const cancellation = req.cancellation orelse return error.TestExpectedCancellation;
            self.entered.store(true, .release);
            while (!self.release.load(.acquire)) {
                if (cancellation.isCancelled()) {
                    self.observed_peer_cancel.store(true, .release);
                    return error.Cancelled;
                }
                sleepMs(1);
            }
            return .{
                .status = 200,
                .body = try alloc.dupe(u8, "late"),
            };
        }
    };

    const RequestThread = struct {
        executor: common.RequestExecutor,
        uri: []const u8,
        cancellation: *const common.RequestCancellation,
        outcome: std.atomic.Value(u8) = .init(0),

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = self.uri,
                .cancellation = self.cancellation,
            }) catch |err| {
                self.outcome.store(if (err == error.Cancelled) 1 else 2, .release);
                return;
            };
            response.deinit(std.heap.page_allocator);
            self.outcome.store(3, .release);
        }
    };

    var app = App{};
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.iface());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/expensive", .{base_uri});
    defer std.testing.allocator.free(uri);
    const fast_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/fast", .{base_uri});
    defer std.testing.allocator.free(fast_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{ .keep_alive = true });
    defer executor.deinit();
    var cancellation: common.RequestCancellation = .{};
    var request_state = RequestThread{
        .executor = executor.executor(),
        .uri = uri,
        .cancellation = &cancellation,
    };
    const request_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&request_state});
    defer {
        app.release.store(true, .release);
        request_thread.join();
    }

    for (0..10_000) |_| {
        if (app.entered.load(.acquire)) break;
        sleepMs(1);
    }
    try std.testing.expect(app.entered.load(.acquire));

    cancellation.cancel();
    for (0..2_000) |_| {
        if (request_state.outcome.load(.acquire) != 0 and app.observed_peer_cancel.load(.acquire)) break;
        sleepMs(1);
    }
    try std.testing.expectEqual(@as(u8, 1), request_state.outcome.load(.acquire));
    try std.testing.expect(app.observed_peer_cancel.load(.acquire));

    // A canceled pooled request must not poison the serialized client or
    // return its interrupted connection to the pool.
    var fast_response = try executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = fast_uri,
    });
    defer fast_response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), fast_response.status);
    try std.testing.expectEqualStrings("ok", fast_response.body);
}

test "std http executor deinit drains the complete timed operation" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        entered: std.atomic.Value(bool) = .init(false),

        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            sleepMs(50);
            return .{
                .status = 200,
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    const RequestThread = struct {
        executor: common.RequestExecutor,
        uri: []const u8,
        failed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = self.uri,
                .timeout_ms = 1_000,
            }) catch {
                self.failed.store(true, .release);
                return;
            };
            response.deinit(std.heap.page_allocator);
        }
    };

    const DeinitThread = struct {
        executor: *std_http_executor.StdHttpExecutor,
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.executor.deinit();
            self.done.store(true, .release);
        }
    };

    var app = App{};
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.iface());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    var request = RequestThread{ .executor = executor.executor(), .uri = base_uri };
    const request_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&request});
    defer request_thread.join();

    var entered = false;
    for (0..1_000) |_| {
        if (app.entered.load(.acquire)) {
            entered = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(entered);

    var shutdown = DeinitThread{ .executor = &executor };
    const shutdown_thread = try std.Thread.spawn(.{}, DeinitThread.run, .{&shutdown});
    defer shutdown_thread.join();

    for (0..1_000) |_| {
        if (shutdown.done.load(.acquire)) break;
        sleepMs(1);
    }
    try std.testing.expect(shutdown.done.load(.acquire));
    try std.testing.expect(!request.failed.load(.acquire));
}

test "HA hostname requests survive successful connect cleanup" {
    const std_http_executor = @import("std_http_executor.zig");

    const NotFoundApp = struct {
        fn iface(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{
                .status = 404,
                .content_type = try alloc.dupe(u8, "application/json"),
                .body = try alloc.dupe(u8, "{}"),
            };
        }
    };

    var app = NotFoundApp{};
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.iface());
    defer listener.deinit();
    try listener.start();

    const bound = listener.boundAddress().?;
    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{
        .resolve_before_connect = true,
    });
    defer executor.deinit();

    // A short response repeatedly exercises hostname resolution followed by a
    // successful connection cleanup. The HA transport must never enter Zig
    // 0.16's racy HostName.connect cancellation path.
    for (0..2_000) |_| {
        const uri = try std.fmt.allocPrint(
            std.testing.allocator,
            "http://localhost:{d}/internal/ha/identify-system",
            .{bound.getPort()},
        );
        defer std.testing.allocator.free(uri);
        var response = try executor.executor().execute(std.testing.allocator, .{
            .method = .GET,
            .uri = uri,
        });
        defer response.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u16, 404), response.status);
    }
}

test "HA resolved transport soaks an external wrong-route endpoint" {
    const std_http_executor = @import("std_http_executor.zig");
    const url_c = std.c.getenv("ANTFLY_HA_HTTP_SOAK_URL") orelse return error.SkipZigTest;
    const url = std.mem.span(url_c);

    var executor = std_http_executor.StdHttpExecutor.init(std.testing.allocator, .{
        .resolve_before_connect = true,
    });
    defer executor.deinit();

    for (0..10_000) |_| {
        const request_uri = try std.testing.allocator.dupe(u8, url);
        defer std.testing.allocator.free(request_uri);
        var response = try executor.executor().execute(std.testing.allocator, .{
            .method = .GET,
            .uri = request_uri,
        });
        defer response.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u16, 404), response.status);
    }
}

test "std http listener and executor round-trip snapshot routes" {
    const raft_engine = @import("raft_engine");
    const http_server = @import("../../raft/transport/http_server.zig");
    const http_snapshot = @import("../../raft/transport/http_snapshot.zig");
    const std_http_executor = @import("std_http_executor.zig");
    const routes = @import("../../raft/transport/routes.zig");

    const Store = struct {
        body: ?[]u8 = null,

        fn iface(self: *@This()) http_server.SnapshotStore {
            return .{
                .ptr = self,
                .vtable = &.{
                    .put_snapshot = putSnapshot,
                    .get_snapshot = getSnapshot,
                },
            };
        }

        fn putSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8, body: []const u8) !void {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (self.body) |existing| alloc.free(existing);
            self.body = try alloc.dupe(u8, body);
        }

        fn getSnapshot(ptr: *anyopaque, alloc: std.mem.Allocator, snapshot_id: []const u8) ![]u8 {
            _ = snapshot_id;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return try alloc.dupe(u8, self.body.?);
        }
    };

    const Noop = struct {
        fn iface(_: *@This()) http_server.BatchHandler {
            return .{
                .ptr = undefined,
                .vtable = &.{
                    .handle_peer_batch = handlePeerBatch,
                },
            };
        }

        fn handlePeerBatch(_: *anyopaque, batch: raft_engine.runtime.transport_iface.PeerBatch) !void {
            _ = batch;
        }
    };

    const Receiver = struct {
        seen: usize = 0,
        index: u64 = 0,

        fn iface(self: *@This()) raft_engine.runtime.snapshot_transport_iface.SnapshotReceiver {
            return .{
                .ptr = self,
                .vtable = &.{
                    .receive_snapshot = receiveSnapshot,
                },
            };
        }

        fn receiveSnapshot(
            ptr: *anyopaque,
            req: raft_engine.runtime.snapshot_transport_iface.SnapshotFetchRequest,
            snapshot: raft_engine.core.types.Snapshot,
        ) !void {
            _ = req;
            const self: *@This() = @ptrCast(@alignCast(ptr));
            var owned = snapshot;
            defer owned.deinit(std.testing.allocator);
            self.seen += 1;
            self.index = snapshot.metadata.index;
        }
    };

    var store = Store{};
    defer if (store.body) |body| std.testing.allocator.free(body);
    var noop = Noop{};
    var app = http_server.HttpServer.init(
        std.testing.allocator,
        .{},
        raft_engine.runtime.BinaryCodec.codec(),
        noop.iface(),
        store.iface(),
        null,
    );
    var listener = StdHttpListener.init(std.testing.allocator, .{}, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor: std_http_executor.StdHttpExecutor = undefined;
    executor.initInPlace(std.testing.allocator, .{});
    defer executor.deinit();
    var transport = try http_snapshot.HttpSnapshotTransport.init(
        std.testing.allocator,
        .{ .root_dir = "/tmp" },
        executor.executor(),
        null,
    );
    defer transport.deinit();

    const upload_path = try routes.Routes.snapshotUploadPath(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(upload_path);
    const upload_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_uri, upload_path });
    defer std.testing.allocator.free(upload_uri);

    const fetch_path = try routes.Routes.snapshotFetchPath(std.testing.allocator, "snap-1");
    defer std.testing.allocator.free(fetch_path);
    const fetch_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}{s}", .{ base_uri, fetch_path });
    defer std.testing.allocator.free(fetch_uri);

    var voters = [_]u64{ 1, 2 };
    const snapshot_bytes = try std.testing.allocator.alloc(u8, 16 * 1024 + 37);
    defer std.testing.allocator.free(snapshot_bytes);
    for (snapshot_bytes, 0..) |*byte, i| byte.* = @intCast('a' + (i % 26));

    try transport.transport().sendSnapshot(.{
        .group_id = 91,
        .to = 2,
        .snapshot = .{
            .metadata = .{
                .index = 12,
                .term = 4,
                .conf_state = .{
                    .voters = voters[0..],
                },
            },
            .data = snapshot_bytes,
        },
        .locator = .{ .snapshot_id = "snap-1", .uri = upload_uri },
    });

    var receiver = Receiver{};
    try transport.transport().fetchSnapshot(.{
        .group_id = 91,
        .from = 2,
        .locator = .{ .snapshot_id = "snap-1", .uri = fetch_uri },
    }, receiver.iface());
    try std.testing.expectEqual(@as(usize, 1), receiver.seen);
    try std.testing.expectEqual(@as(u64, 12), receiver.index);
}

test "std http listener accepts Expect 100-continue request bodies" {
    var input_reader: std.Io.Reader = .fixed(
        "POST /echo HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 4\r\n" ++
            "Expect: 100-continue\r\n" ++
            "\r\n" ++
            "ping",
    );
    var output_buffer: [256]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{},
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    const body = (try listener.readRequestBody(null, &request)).?;
    defer std.testing.allocator.free(body);
    try std.testing.expectEqualStrings("ping", body);
    try std.testing.expectEqualStrings("HTTP/1.1 100 Continue\r\n\r\n", output_writer.buffered());
}

test "std http listener rejects oversized request body with 413" {
    var input_reader: std.Io.Reader = .fixed(
        "POST /echo HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 4\r\n" ++
            "\r\n",
    );
    var output_buffer: [256]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{
            .max_request_bytes = 3,
        },
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try std.testing.expectEqual(null, try listener.readRequestBody(null, &request));
    try std.testing.expect(std.mem.indexOf(u8, output_writer.buffered(), "HTTP/1.1 413 Payload Too Large\r\n") != null);
}

test "std http listener rejects unsupported Expect header values with 417" {
    var input_reader: std.Io.Reader = .fixed(
        "POST /echo HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 4\r\n" ++
            "Expect: wait-for-me\r\n" ++
            "\r\n",
    );
    var output_buffer: [256]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{},
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try std.testing.expectEqual(null, try listener.readRequestBody(null, &request));
    try std.testing.expect(std.mem.indexOf(u8, output_writer.buffered(), "HTTP/1.1 417 Expectation Failed\r\n") != null);
}

test "std http listener can stream a chunked response through optional executor" {
    const App = struct {
        fn streamingExecutor(self: *@This()) common.StreamingRequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = executeStreaming,
                },
            };
        }

        fn executeStreaming(_: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest, writer: common.StreamWriter) !bool {
            if (!std.mem.eql(u8, req.uri, "/events")) return false;
            try writer.start(alloc, .{ .status = 200, .content_type = "text/event-stream" });
            try writer.writeAll("event: message\ndata: {\"ok\":true}\n\n");
            try writer.flush();
            return true;
        }
    };

    var input_reader: std.Io.Reader = .fixed(
        "GET /events HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "\r\n",
    );
    var output_buffer: [1024]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();
    var app = App{};

    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{},
        .app = undefined,
        .streaming_app = app.streamingExecutor(),
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try listener.handleRequest(null, &request);
    const output = output_writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "HTTP/1.1 200 OK\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "transfer-encoding: chunked\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "content-type: text/event-stream\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "event: message\ndata: {\"ok\":true}\n\n") != null);
}

test "std http listener caps active connection handoff threads" {
    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{ .max_connection_threads = 1 },
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    try std.testing.expect(listener.tryAcquireConnectionThreadSlot());
    try std.testing.expect(!listener.tryAcquireConnectionThreadSlot());
    try std.testing.expectEqual(@as(u32, 1), listener.active_connection_threads.load(.acquire));

    _ = listener.active_connection_threads.fetchSub(1, .acq_rel);
    try std.testing.expect(listener.tryAcquireConnectionThreadSlot());
    _ = listener.active_connection_threads.fetchSub(1, .acq_rel);
    try std.testing.expectEqual(@as(u32, 0), listener.active_connection_threads.load(.acquire));
}

test "std http listener bounds 128 admitted requests and keeps a retryable reserve" {
    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{ .max_active_requests = 32 },
        .app = undefined,
        .io_impl = undefined,
        .io_owner = .shared,
    };

    for (0..32) |_| try std.testing.expect(listener.tryAcquireRequestSlot());
    // This models the excess of the 128-client overload fixture: all 96
    // attempts are rejected before they can enter the expensive executor.
    for (0..96) |_| try std.testing.expect(!listener.tryAcquireRequestSlot());
    try std.testing.expectEqual(@as(u32, 32), listener.active_requests.load(.acquire));
    const saturated = listener.runtimeStats();
    try std.testing.expectEqual(@as(u32, 32), saturated.max_active_requests);
    try std.testing.expectEqual(@as(u32, 32), saturated.active_requests);
    try std.testing.expectEqual(@as(u64, 96), saturated.rejected_requests_total);

    for (0..32) |_| listener.releaseRequestSlot();
    try std.testing.expectEqual(@as(u32, 0), listener.active_requests.load(.acquire));
    try std.testing.expect(listener.tryAcquireRequestSlot());
    listener.releaseRequestSlot();
}

test "std http listener query admission matches paths without query-string false positives" {
    try std.testing.expect(StdHttpListener.requestConsumesExpensiveSlot(.POST, "/query"));
    try std.testing.expect(StdHttpListener.requestConsumesExpensiveSlot(.POST, "/db/v1/tables/products/query?limit=10"));
    try std.testing.expect(!StdHttpListener.requestConsumesExpensiveSlot(.GET, "/db/v1/tables/products/query"));
    try std.testing.expect(!StdHttpListener.requestConsumesExpensiveSlot(.POST, "/db/v1/tables/products/query-preflight"));
    try std.testing.expect(!StdHttpListener.requestConsumesExpensiveSlot(.POST, "/control?next=/query"));
}

test "std http listener recovers after 128 real clients abandon saturated queries" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        release_queries: std.atomic.Value(bool) = .init(false),
        cancelled_queries: std.atomic.Value(u32) = .init(0),

        fn executor(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.indexOf(u8, req.uri, "/query") != null) {
                const cancellation = req.cancellation orelse return error.TestExpectedCancellation;
                while (!self.release_queries.load(.acquire)) {
                    if (cancellation.isCancelled()) {
                        _ = self.cancelled_queries.fetchAdd(1, .monotonic);
                        return .{ .status = 499, .body = try alloc.dupe(u8, "client closed") };
                    }
                    sleepMs(1);
                }
            }
            return .{ .status = 200, .body = try alloc.dupe(u8, "ok") };
        }
    };

    var app = App{};
    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        // These are deliberately tighter than production. They turn the test
        // into a deterministic connection/worker resource constraint while
        // retaining ample capacity to parse 429s and serve control traffic.
        .max_connection_threads = 32,
        .max_active_requests = 8,
        .kernel_backlog = 256,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const bound_addr = listener.boundAddress() orelse return error.TestUnexpectedResult;
    const client_io = std.Io.Threaded.global_single_threaded.io();
    var clients = [_]?std.Io.net.Stream{null} ** 128;
    defer for (&clients) |*slot| {
        if (slot.*) |*client| client.close(client_io);
        slot.* = null;
    };

    for (&clients) |*slot| {
        var client = try bound_addr.connect(client_io, .{ .mode = .stream });
        errdefer client.close(client_io);
        var write_buffer: [256]u8 = undefined;
        var writer = client.writer(client_io, &write_buffer);
        try writer.interface.writeAll("POST /query HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
        try writer.interface.flush();
        slot.* = client;
    }

    var saturated = listener.runtimeStats();
    for (0..10_000) |_| {
        saturated = listener.runtimeStats();
        if (saturated.active_requests == 8 and saturated.rejected_requests_total == 120) break;
        sleepMs(1);
    }
    try std.testing.expectEqual(@as(u32, 8), saturated.active_requests);
    try std.testing.expectEqual(@as(u64, 120), saturated.rejected_requests_total);
    try std.testing.expect(saturated.peak_active_requests <= saturated.max_active_requests);
    try std.testing.expect(saturated.peak_connection_threads <= saturated.max_connection_threads);
    try std.testing.expectEqual(@as(usize, 8), saturated.active_peer_observers);

    // At least one real overload client must receive the retryable wire
    // response; the counter alone is not sufficient evidence of shedding.
    var saw_429 = false;
    var response_buffer: [512]u8 = undefined;
    for (clients[8..]) |maybe_client| {
        const client = maybe_client orelse continue;
        const received = std.c.recv(
            client.socket.handle,
            &response_buffer,
            response_buffer.len,
            @intCast(std.posix.MSG.DONTWAIT),
        );
        if (received > 0 and std.mem.indexOf(u8, response_buffer[0..@intCast(received)], "429 Too Many Requests") != null) {
            saw_429 = true;
            break;
        }
    }
    try std.testing.expect(saw_429);

    // A control request remains usable while every expensive slot is held and
    // 120 abandoned/rejected client sockets are still retained by the test.
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const control_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/control", .{base_uri});
    defer std.testing.allocator.free(control_uri);
    var control = try executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = control_uri,
        .timeout_ms = 2_000,
    });
    defer control.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), control.status);

    // Abandon every client. The eight admitted executions must observe the
    // disconnect and release their slots without requiring a server restart.
    for (&clients) |*slot| {
        if (slot.*) |*client| client.close(client_io);
        slot.* = null;
    }
    var recovered = listener.runtimeStats();
    for (0..10_000) |_| {
        recovered = listener.runtimeStats();
        if (recovered.active_requests == 0 and recovered.active_peer_observers == 0 and app.cancelled_queries.load(.acquire) == 8) break;
        sleepMs(1);
    }
    try std.testing.expectEqual(@as(u32, 0), recovered.active_requests);
    try std.testing.expectEqual(@as(u32, 8), app.cancelled_queries.load(.acquire));
    try std.testing.expect(recovered.peer_disconnects_total >= 8);
    try std.testing.expectEqual(@as(usize, 0), recovered.active_peer_observers);

    app.release_queries.store(true, .release);
    const query_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/query", .{base_uri});
    defer std.testing.allocator.free(query_uri);
    var post_load = try executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = query_uri,
        .timeout_ms = 2_000,
    });
    defer post_load.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), post_load.status);
}

test "std http listener overload admission returns retryable 429 without invoking executor" {
    const App = struct {
        calls: u32 = 0,

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return .{ .status = 200, .body = try alloc.dupe(u8, "unexpected") };
        }
    };

    var input_reader: std.Io.Reader = .fixed("POST /query HTTP/1.1\r\nHost: localhost\r\nContent-Length: 0\r\n\r\n");
    var output_buffer: [512]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();
    var app = App{};
    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{ .max_active_requests = 1 },
        .app = .{ .ptr = &app, .vtable = &.{ .execute = App.execute } },
        .io_impl = undefined,
        .io_owner = .shared,
    };
    try std.testing.expect(listener.tryAcquireRequestSlot());
    defer listener.releaseRequestSlot();

    try listener.handleRequest(null, &request);
    const output = output_writer.buffered();
    try std.testing.expect(std.mem.indexOf(u8, output, "HTTP/1.1 429 Too Many Requests\r\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "retry-after: 1\r\n") != null);
    try std.testing.expectEqual(@as(u32, 0), app.calls);
}

test "std http listener keeps control route capacity while query slots are full" {
    const App = struct {
        calls: u32 = 0,

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.calls += 1;
            return .{ .status = 200, .body = try alloc.dupe(u8, "ok") };
        }
    };

    var input_reader: std.Io.Reader = .fixed("GET /health HTTP/1.1\r\nHost: localhost\r\n\r\n");
    var output_buffer: [512]u8 = undefined;
    var output_writer: std.Io.Writer = .fixed(&output_buffer);
    var server: std.http.Server = .init(&input_reader, &output_writer);
    var request = try server.receiveHead();
    var app = App{};
    var listener: StdHttpListener = .{
        .alloc = std.testing.allocator,
        .cfg = .{ .max_active_requests = 32 },
        .app = .{ .ptr = &app, .vtable = &.{ .execute = App.execute } },
        .io_impl = undefined,
        .io_owner = .shared,
    };
    for (0..32) |_| try std.testing.expect(listener.tryAcquireRequestSlot());
    defer for (0..32) |_| listener.releaseRequestSlot();

    try listener.handleRequest(null, &request);
    try std.testing.expectEqual(@as(u32, 1), app.calls);
    try std.testing.expect(std.mem.indexOf(u8, output_writer.buffered(), "HTTP/1.1 200 OK\r\n") != null);
}

test "std http listener propagates raw peer disconnect into in-flight executor cancellation" {
    const App = struct {
        entered: std.atomic.Value(bool) = .init(false),
        observed_cancel: std.atomic.Value(bool) = .init(false),

        fn executor(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.entered.store(true, .release);
            const cancellation = req.cancellation orelse return error.TestExpectedCancellation;
            for (0..2_000) |_| {
                if (cancellation.isCancelled()) {
                    self.observed_cancel.store(true, .release);
                    return .{ .status = 499, .body = try alloc.dupe(u8, "client closed") };
                }
                sleepMs(1);
            }
            return error.TestCancellationNotObserved;
        }
    };

    var app = App{};
    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 1,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const bound_addr = listener.boundAddress() orelse return error.TestUnexpectedResult;
    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try bound_addr.connect(client_io, .{ .mode = .stream });
    var write_buffer: [256]u8 = undefined;
    var writer = client.writer(client_io, &write_buffer);
    try writer.interface.writeAll("GET /expensive HTTP/1.1\r\nHost: localhost\r\n\r\n");
    try writer.interface.flush();

    var entered = false;
    for (0..2_000) |_| {
        if (app.entered.load(.acquire)) {
            entered = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(entered);
    client.close(client_io);

    var cancelled = false;
    for (0..2_000) |_| {
        if (app.observed_cancel.load(.acquire)) {
            cancelled = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(cancelled);
}

test "std http listener saturated connection slots queue instead of resetting" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        entered_slow: std.atomic.Value(bool) = .init(false),
        release_slow: std.atomic.Value(bool) = .init(false),

        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, "/slow")) {
                self.entered_slow.store(true, .release);
                while (!self.release_slow.load(.acquire)) sleepMs(1);
            }
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    const RequestThread = struct {
        uri: []const u8,
        executor: common.RequestExecutor,
        status: std.atomic.Value(u16) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = self.uri,
            }) catch {
                self.failed.store(true, .release);
                self.done.store(true, .release);
                return;
            };
            defer response.deinit(std.heap.page_allocator);

            self.status.store(response.status, .release);
            self.done.store(true, .release);
        }
    };

    var app = App{};
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const request_executor = executor.executor();

    // One connection-thread slot: while the slow request occupies it, a
    // second connection must queue unaccepted in the kernel backlog (no
    // accept-then-close RST, no inline serve blocking the accept thread)
    // and complete once the slot frees.
    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 1,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const slow_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/slow", .{base_uri});
    defer std.testing.allocator.free(slow_uri);
    const fast_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/fast", .{base_uri});
    defer std.testing.allocator.free(fast_uri);

    var slow_req = RequestThread{ .uri = slow_uri, .executor = request_executor };
    const slow_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&slow_req});
    defer slow_thread.join();
    defer app.release_slow.store(true, .release);

    var saw_slow = false;
    for (0..10_000) |_| {
        if (app.entered_slow.load(.acquire)) {
            saw_slow = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(saw_slow);

    var fast_req = RequestThread{ .uri = fast_uri, .executor = request_executor };
    const fast_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&fast_req});
    defer fast_thread.join();

    // While the slot is saturated the second request must not be reset.
    sleepMs(50);
    try std.testing.expect(!fast_req.failed.load(.acquire));

    app.release_slow.store(true, .release);

    var fast_completed = false;
    for (0..10_000) |_| {
        if (fast_req.done.load(.acquire)) {
            fast_completed = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(fast_completed);
    try std.testing.expect(!fast_req.failed.load(.acquire));
    try std.testing.expectEqual(@as(u16, 200), fast_req.status.load(.acquire));

    var slow_completed = false;
    for (0..10_000) |_| {
        if (slow_req.done.load(.acquire)) {
            slow_completed = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(slow_completed);
    try std.testing.expect(!slow_req.failed.load(.acquire));
    try std.testing.expectEqual(@as(u16, 200), slow_req.status.load(.acquire));
}

test "std http listener serves timed requests without threaded async or concurrent capacity" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    var app = App{};
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const request_executor = executor.executor();

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer io_impl.deinit();
    var listener = StdHttpListener.initShared(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .header_read_timeout_ms = 10_000,
        .body_read_timeout_ms = 10_000,
    }, app.executor(), &io_impl);
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    for (0..32) |_| {
        var response = try request_executor.execute(std.testing.allocator, .{
            .method = .GET,
            .uri = base_uri,
            .timeout_ms = 10_000,
        });
        defer response.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u16, 200), response.status);

        var body_response = try request_executor.execute(std.testing.allocator, .{
            .method = .POST,
            .uri = base_uri,
            .body = "ping",
            .timeout_ms = 10_000,
        });
        defer body_response.deinit(std.testing.allocator);
        try std.testing.expectEqual(@as(u16, 200), body_response.status);
    }
}

test "std http listener header timeout releases accepted idle connection slot" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    var app = App{};
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const request_executor = executor.executor();

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer io_impl.deinit();
    var listener = StdHttpListener.initShared(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 1,
        .header_read_timeout_ms = 25,
    }, app.executor(), &io_impl);
    defer listener.deinit();
    try listener.start();

    const bound_addr = listener.boundAddress() orelse return error.TestUnexpectedResult;
    const idle_io = std.Io.Threaded.global_single_threaded.io();
    var idle_stream = try bound_addr.connect(idle_io, .{ .mode = .stream });
    defer idle_stream.close(idle_io);

    var saw_slot = false;
    for (0..10_000) |_| {
        if (listener.active_connection_threads.load(.acquire) == 1) {
            saw_slot = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(saw_slot);

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var response = try request_executor.execute(std.testing.allocator, .{
        .method = .GET,
        .uri = base_uri,
        .timeout_ms = 10_000,
    });
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    const stats = listener.runtimeStats();
    try std.testing.expect(stats.deadline_expirations_total >= 1);
    try std.testing.expectEqual(@as(u64, 0), stats.deadline_observer_failures_total);
    try std.testing.expectEqual(@as(usize, 0), stats.active_deadline_observers);
}

test "std http listener ready request does not wait for async timeout budget" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{ .execute = execute },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    // Reproduce a saturated async budget deterministically. Connection
    // handlers are concurrent tasks; a ready request must not execute its
    // timeout inline merely because no bounded async slot remains.
    var server_io = std.Io.Threaded.init(std.heap.page_allocator, .{
        .async_limit = .nothing,
    });
    defer server_io.deinit();

    var app = App{};
    var listener = StdHttpListener.initShared(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 4,
        .header_read_timeout_ms = 1_000,
    }, app.executor(), &server_io);
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    var response = try executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = base_uri,
        .timeout_ms = 250,
    });
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
}

test "std http listener body timeout releases accepted slow body connection slot" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    var app = App{};
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const request_executor = executor.executor();

    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{
        .async_limit = .nothing,
        .concurrent_limit = .nothing,
    });
    defer io_impl.deinit();
    var listener = StdHttpListener.initShared(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 1,
        .header_read_timeout_ms = 5_000,
        .body_read_timeout_ms = 50,
    }, app.executor(), &io_impl);
    defer listener.deinit();
    try listener.start();

    const bound_addr = listener.boundAddress() orelse return error.TestUnexpectedResult;
    const idle_io = std.Io.Threaded.global_single_threaded.io();
    var idle_stream = try bound_addr.connect(idle_io, .{ .mode = .stream });
    defer idle_stream.close(idle_io);

    var write_buffer: [256]u8 = undefined;
    var writer = idle_stream.writer(idle_io, &write_buffer);
    try writer.interface.writeAll(
        "POST / HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 100\r\n" ++
            "\r\n",
    );
    try writer.interface.flush();

    var saw_slot = false;
    for (0..10_000) |_| {
        if (listener.active_connection_threads.load(.acquire) == 1) {
            saw_slot = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(saw_slot);

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    var response = try request_executor.execute(std.testing.allocator, .{
        .method = .GET,
        .uri = base_uri,
        .timeout_ms = 10_000,
    });
    defer response.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(u16, 200), response.status);
    const stats = listener.runtimeStats();
    try std.testing.expect(stats.deadline_expirations_total >= 1);
    try std.testing.expectEqual(@as(u64, 0), stats.deadline_observer_failures_total);
    try std.testing.expectEqual(@as(usize, 0), stats.active_deadline_observers);
}

fn countLinuxProcEntries(path: []const u8) !usize {
    const io = std.Io.Threaded.global_single_threaded.io();
    var dir = try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true });
    defer dir.close(io);
    var iterator = dir.iterate();
    var count: usize = 0;
    while (try iterator.next(io)) |_| count += 1;
    return count;
}

test "std http listener retains a bounded worker plateau and recovers descriptors after cancellation storms" {
    if (comptime builtin.os.tag != .linux) return;

    if (std.c.getenv("ANTFLY_REQUIRE_LOW_FD_HTTP_RATCHET") != null) {
        const nofile = try std.posix.getrlimit(.NOFILE);
        try std.testing.expectEqual(@as(u64, 256), nofile.cur);
    }

    const App = struct {
        fn executor(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{ .status = 200, .body = try alloc.dupe(u8, "ok") };
        }
    };

    const batch_size = 48;
    const rounds = 4;
    const thread_slack = 4;
    const fd_slack = 8;
    var app = App{};
    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 64,
        .header_read_timeout_ms = 30_000,
    }, app.executor());
    defer listener.deinit();
    try listener.start();
    const bound_addr = listener.boundAddress().?;
    const client_io = std.Io.Threaded.global_single_threaded.io();

    // Let the accept and observer owners reach their steady idle state before
    // measuring the process. The accept loop reserves one connection slot
    // while blocked in accept(); no client socket is active at this point.
    sleepMs(100);
    const baseline_threads = try countLinuxProcEntries("/proc/self/task");
    const baseline_fds = try countLinuxProcEntries("/proc/self/fd");
    const hard_thread_ceiling = baseline_threads + @as(usize, listener.cfg.max_connection_threads) + thread_slack;
    var warmed_thread_ceiling: ?usize = null;

    for (0..rounds) |_| {
        var clients = [_]?std.Io.net.Stream{null} ** batch_size;
        defer for (&clients) |*maybe_stream| {
            if (maybe_stream.*) |*stream| stream.close(client_io);
        };

        for (&clients) |*slot| slot.* = try bound_addr.connect(client_io, .{ .mode = .stream });

        var loaded = false;
        for (0..400) |_| {
            const stats = listener.runtimeStats();
            if (stats.active_deadline_observers >= batch_size and
                stats.active_connection_threads >= batch_size)
            {
                loaded = true;
                break;
            }
            sleepMs(5);
        }
        try std.testing.expect(loaded);
        const loaded_threads = try countLinuxProcEntries("/proc/self/task");
        try std.testing.expect(loaded_threads <= hard_thread_ceiling);

        for (&clients) |*maybe_stream| {
            if (maybe_stream.*) |*stream| {
                stream.shutdown(client_io, .both) catch {};
                stream.close(client_io);
                maybe_stream.* = null;
            }
        }

        var recovered = false;
        var recovered_threads: usize = 0;
        for (0..2_000) |_| {
            const stats = listener.runtimeStats();
            const threads = try countLinuxProcEntries("/proc/self/task");
            const fds = try countLinuxProcEntries("/proc/self/fd");
            if (stats.active_deadline_observers == 0 and
                stats.active_peer_observers == 0 and
                stats.active_connection_threads <= 1 and
                fds <= baseline_fds + fd_slack)
            {
                recovered = true;
                recovered_threads = threads;
                break;
            }
            sleepMs(5);
        }
        try std.testing.expect(recovered);
        try std.testing.expect(recovered_threads <= hard_thread_ceiling);
        if (warmed_thread_ceiling) |ceiling| {
            try std.testing.expect(loaded_threads <= ceiling);
            try std.testing.expect(recovered_threads <= ceiling);
        } else {
            warmed_thread_ceiling = @max(loaded_threads, recovered_threads) + thread_slack;
        }
    }
}

test "std http listener stop interrupts accepted header read" {
    const App = struct {
        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    const StopThread = struct {
        listener: *StdHttpListener,
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.listener.stop();
            self.done.store(true, .release);
        }
    };

    var app = App{};
    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 1,
        .header_read_timeout_ms = 60_000,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const bound_addr = listener.boundAddress() orelse return error.TestUnexpectedResult;
    const idle_io = std.Io.Threaded.global_single_threaded.io();
    var idle_stream = try bound_addr.connect(idle_io, .{ .mode = .stream });
    defer idle_stream.close(idle_io);

    var saw_slot = false;
    for (0..10_000) |_| {
        if (listener.active_connection_threads.load(.acquire) == 1) {
            saw_slot = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(saw_slot);

    var stop_thread_state = StopThread{ .listener = &listener };
    const stop_thread = try std.Thread.spawn(.{}, StopThread.run, .{&stop_thread_state});
    defer stop_thread.join();

    var stopped = false;
    for (0..10_000) |_| {
        if (stop_thread_state.done.load(.acquire)) {
            stopped = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(stopped);
}

test "std http listener stop interrupts accepted body read" {
    const App = struct {
        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(_: *anyopaque, alloc: std.mem.Allocator, _: common.HttpRequest) !common.HttpResponse {
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    const StopThread = struct {
        listener: *StdHttpListener,
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.listener.stop();
            self.done.store(true, .release);
        }
    };

    var app = App{};
    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 1,
        .header_read_timeout_ms = 60_000,
        .body_read_timeout_ms = 60_000,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const bound_addr = listener.boundAddress() orelse return error.TestUnexpectedResult;
    const idle_io = std.Io.Threaded.global_single_threaded.io();
    var idle_stream = try bound_addr.connect(idle_io, .{ .mode = .stream });
    defer idle_stream.close(idle_io);

    var write_buffer: [256]u8 = undefined;
    var writer = idle_stream.writer(idle_io, &write_buffer);
    try writer.interface.writeAll(
        "POST / HTTP/1.1\r\n" ++
            "Host: localhost\r\n" ++
            "Content-Length: 100\r\n" ++
            "\r\n",
    );
    try writer.interface.flush();

    var saw_slot = false;
    for (0..10_000) |_| {
        if (listener.active_connection_threads.load(.acquire) == 1) {
            saw_slot = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(saw_slot);

    var stop_thread_state = StopThread{ .listener = &listener };
    const stop_thread = try std.Thread.spawn(.{}, StopThread.run, .{&stop_thread_state});
    defer stop_thread.join();

    var stopped = false;
    for (0..10_000) |_| {
        if (stop_thread_state.done.load(.acquire)) {
            stopped = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(stopped);
}

test "std http listener stop returns while saturated with a headerless connection queued" {
    const std_http_executor = @import("std_http_executor.zig");

    const App = struct {
        entered_slow: std.atomic.Value(bool) = .init(false),
        release_slow: std.atomic.Value(bool) = .init(false),

        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            _ = req;
            self.entered_slow.store(true, .release);
            while (!self.release_slow.load(.acquire)) sleepMs(1);
            return .{
                .status = 200,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "ok"),
            };
        }
    };

    const RequestThread = struct {
        uri: []const u8,
        executor: common.RequestExecutor,
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = self.uri,
            }) catch {
                self.done.store(true, .release);
                return;
            };
            response.deinit(std.heap.page_allocator);
            self.done.store(true, .release);
        }
    };

    const StopThread = struct {
        listener: *StdHttpListener,
        done: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            self.listener.stop();
            self.done.store(true, .release);
        }
    };

    var app = App{};
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const request_executor = executor.executor();

    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
        .max_connection_threads = 1,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);
    const slow_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/slow", .{base_uri});
    defer std.testing.allocator.free(slow_uri);

    var slow_req = RequestThread{ .uri = slow_uri, .executor = request_executor };
    const slow_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&slow_req});
    defer slow_thread.join();
    defer app.release_slow.store(true, .release);

    var saw_slow = false;
    for (0..10_000) |_| {
        if (app.entered_slow.load(.acquire)) {
            saw_slow = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(saw_slow);

    // A connection that never sends headers while the listener is
    // saturated: it must not be accepted (and must not wedge serve/stop).
    const bound_addr = listener.boundAddress() orelse return error.TestUnexpectedResult;
    const idle_io = std.Io.Threaded.global_single_threaded.io();
    var idle_stream = try bound_addr.connect(idle_io, .{ .mode = .stream });
    defer idle_stream.close(idle_io);
    sleepMs(20);

    var stop_thread_state = StopThread{ .listener = &listener };
    const stop_thread = try std.Thread.spawn(.{}, StopThread.run, .{&stop_thread_state});
    defer stop_thread.join();

    // stop() waits for in-flight requests; release the slow one and the
    // whole shutdown must then complete promptly even though the headerless
    // connection is still open.
    sleepMs(20);
    app.release_slow.store(true, .release);

    var stopped = false;
    for (0..10_000) |_| {
        if (stop_thread_state.done.load(.acquire)) {
            stopped = true;
            break;
        }
        sleepMs(1);
    }
    try std.testing.expect(stopped);
}

test "std http executor runs timed requests concurrently" {
    const std_http_executor = @import("std_http_executor.zig");
    const request_timeout_ms = 20_000;
    const admission_timeout_ms = 10_000;

    const App = struct {
        io: std.Io,
        entered_slow: std.Io.Event = .unset,
        entered_fast: std.Io.Event = .unset,
        release_slow: std.Io.Event = .unset,

        fn executor(self: *@This()) common.RequestExecutor {
            return .{
                .ptr = self,
                .vtable = &.{
                    .execute = execute,
                },
            };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            if (std.mem.eql(u8, req.uri, "/slow")) {
                self.entered_slow.set(self.io);
                self.release_slow.waitUncancelable(self.io);
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                    .body = try alloc.dupe(u8, "slow"),
                };
            }
            if (std.mem.eql(u8, req.uri, "/fast")) {
                self.entered_fast.set(self.io);
                return .{
                    .status = 200,
                    .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                    .body = try alloc.dupe(u8, "fast"),
                };
            }
            return .{
                .status = 404,
                .content_type = try alloc.dupe(u8, "text/plain; charset=utf-8"),
                .body = try alloc.dupe(u8, "missing"),
            };
        }
    };

    const RequestThread = struct {
        uri: []const u8,
        executor: common.RequestExecutor,
        status: std.atomic.Value(u16) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = self.uri,
                .timeout_ms = request_timeout_ms,
            }) catch {
                self.failed.store(true, .release);
                return;
            };
            defer response.deinit(std.heap.page_allocator);

            self.status.store(response.status, .release);
        }
    };

    const Wait = struct {
        fn event(evt: *std.Io.Event, io: std.Io) bool {
            const deadline = std.Io.Clock.Timestamp.fromNow(io, .{
                .raw = std.Io.Duration.fromMilliseconds(admission_timeout_ms),
                .clock = .awake,
            });
            while (!evt.isSet()) {
                evt.waitTimeout(io, .{ .deadline = deadline }) catch |err| switch (err) {
                    error.Timeout => {
                        if (std.Io.Clock.Timestamp.now(io, .awake).compare(.gte, deadline)) return false;
                        continue;
                    },
                    error.Canceled => return false,
                };
            }
            return true;
        }
    };

    var event_io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer event_io_impl.deinit();
    const event_io = event_io_impl.io();
    var app = App{ .io = event_io };
    var executor = std_http_executor.StdHttpExecutor.init(std.heap.page_allocator, .{});
    defer executor.deinit();
    const request_executor = executor.executor();

    var listener = StdHttpListener.init(std.heap.page_allocator, .{
        .serve_in_connection_threads = true,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const base_uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(base_uri);

    const slow_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/slow", .{base_uri});
    defer std.testing.allocator.free(slow_uri);
    const fast_uri = try std.fmt.allocPrint(std.testing.allocator, "{s}/fast", .{base_uri});
    defer std.testing.allocator.free(fast_uri);

    var slow_req = RequestThread{ .uri = slow_uri, .executor = request_executor };
    var slow_thread: ?std.Thread = try std.Thread.spawn(.{}, RequestThread.run, .{&slow_req});
    var fast_thread: ?std.Thread = null;
    defer {
        app.release_slow.set(event_io);
        if (fast_thread) |thread| thread.join();
        if (slow_thread) |thread| thread.join();
    }
    try std.testing.expect(Wait.event(&app.entered_slow, event_io));

    var fast_req = RequestThread{ .uri = fast_uri, .executor = request_executor };
    fast_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&fast_req});
    const fast_admitted = Wait.event(&app.entered_fast, event_io);

    app.release_slow.set(event_io);
    fast_thread.?.join();
    fast_thread = null;
    slow_thread.?.join();
    slow_thread = null;

    try std.testing.expect(fast_admitted);
    try std.testing.expect(!fast_req.failed.load(.acquire));
    try std.testing.expectEqual(@as(u16, 200), fast_req.status.load(.acquire));
    try std.testing.expect(!slow_req.failed.load(.acquire));
    try std.testing.expectEqual(@as(u16, 200), slow_req.status.load(.acquire));
}
