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
const httpx = @import("httpx");
const common = @import("http_common.zig");
const std_http_listener = @import("std_http_listener.zig");
const threaded_connect_io = @import("threaded_connect_io.zig");

const cancellation_poll_interval_ms: i64 = 25;

pub const StdHttpExecutorConfig = struct {
    read_buffer_size: usize = 8 * 1024,
    write_buffer_size: usize = 1024,
    max_response_bytes: usize = 4 << 20,
    thread_stack_size: usize = std_http_listener.default_request_stack_size,
    /// Hard ceiling for retained workers used by timeout/cancellation-aware
    /// requests. Callers normally stay well below this through listener and
    /// query admission; the finite limit prevents an abnormal fan-out from
    /// permanently growing the owned Threaded executor without bound.
    io_concurrent_limit: u32 = std_http_listener.default_process_io_concurrent_limit,
    keep_alive: bool = false,
    /// Resolve host names before opening a concrete IP socket. Zig 0.16's
    /// `HostName.connect` can corrupt its nested lookup future when a winning
    /// connection cancels the remaining attempts. This transport avoids that
    /// cancellation path and is required for long-lived HA replication loops.
    resolve_before_connect: bool = false,
    /// Retain a successfully connected DNS result across requests. Keep this
    /// disabled for Kubernetes headless Services: an old Pod can remain
    /// connectable after the Service authority has moved to a new endpoint.
    cache_resolved_addresses: bool = false,
    /// Default end-to-end request deadline, including DNS lookup and connect,
    /// when the individual request does not provide a tighter deadline. Zero
    /// preserves the caller's unbounded behavior.
    request_timeout_ms: u32 = 0,
    /// Proactively retire pooled HTTP/1.1 connections before a server-side
    /// keep-alive cap closes them. 0 means unlimited client-side reuse.
    max_requests_per_connection: u32 = 32,
};

pub const StdHttpExecutor = struct {
    const IoOwner = enum {
        owned,
        shared,
    };

    alloc: std.mem.Allocator,
    cfg: StdHttpExecutorConfig,
    io_impl: *std.Io.Threaded,
    io_vtable: *std.Io.VTable,
    io_owner: IoOwner,
    client: std.http.Client,
    resolved_client: httpx.Client,
    resolved_client_mutex: std.Io.Mutex,
    lifecycle_mutex: std.Io.Mutex,
    idle_cond: std.Io.Condition,
    closing: bool,
    in_flight: usize,
    client_mutex: std.Io.Mutex,
    reuse_mutex: std.Io.Mutex,
    requests_on_current_connection: u32,

    pub fn initInPlace(self: *StdHttpExecutor, alloc: std.mem.Allocator, cfg: StdHttpExecutorConfig) void {
        const io_impl = alloc.create(std.Io.Threaded) catch @panic("OOM");
        io_impl.* = std.Io.Threaded.init(alloc, .{
            .stack_size = cfg.thread_stack_size,
            .concurrent_limit = .limited(cfg.io_concurrent_limit),
        });
        const io_vtable = threaded_connect_io.createVTable(alloc, io_impl) catch @panic("OOM");
        self.* = .{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = io_impl,
            .io_vtable = io_vtable,
            .io_owner = .owned,
            .client = undefined,
            .resolved_client = undefined,
            .resolved_client_mutex = .init,
            .lifecycle_mutex = .init,
            .idle_cond = .init,
            .closing = false,
            .in_flight = 0,
            .client_mutex = .init,
            .reuse_mutex = .init,
            .requests_on_current_connection = 0,
        };
        self.client = .{
            .allocator = alloc,
            .io = threaded_connect_io.io(io_impl, io_vtable),
            .read_buffer_size = cfg.read_buffer_size,
            .write_buffer_size = cfg.write_buffer_size,
        };
        self.resolved_client = httpx.Client.initWithConfig(alloc, io_impl.io(), resolvedClientConfig(cfg));
    }

    pub fn initSharedInPlace(self: *StdHttpExecutor, alloc: std.mem.Allocator, cfg: StdHttpExecutorConfig, io_impl: *std.Io.Threaded) void {
        const io_vtable = threaded_connect_io.createVTable(alloc, io_impl) catch @panic("OOM");
        self.* = .{
            .alloc = alloc,
            .cfg = cfg,
            .io_impl = io_impl,
            .io_vtable = io_vtable,
            .io_owner = .shared,
            .client = undefined,
            .resolved_client = undefined,
            .resolved_client_mutex = .init,
            .lifecycle_mutex = .init,
            .idle_cond = .init,
            .closing = false,
            .in_flight = 0,
            .client_mutex = .init,
            .reuse_mutex = .init,
            .requests_on_current_connection = 0,
        };
        self.client = .{
            .allocator = alloc,
            .io = threaded_connect_io.io(io_impl, io_vtable),
            .read_buffer_size = cfg.read_buffer_size,
            .write_buffer_size = cfg.write_buffer_size,
        };
        self.resolved_client = httpx.Client.initWithConfig(alloc, io_impl.io(), resolvedClientConfig(cfg));
    }

    pub fn init(alloc: std.mem.Allocator, cfg: StdHttpExecutorConfig) StdHttpExecutor {
        var self: StdHttpExecutor = undefined;
        self.initInPlace(alloc, cfg);
        return self;
    }

    pub fn deinit(self: *StdHttpExecutor) void {
        const io = self.io_impl.io();
        self.lifecycle_mutex.lockUncancelable(io);
        self.closing = true;
        while (self.in_flight != 0) {
            self.idle_cond.waitUncancelable(io, &self.lifecycle_mutex);
        }
        self.lifecycle_mutex.unlock(io);

        self.client.deinit();
        self.resolved_client.deinit();
        self.alloc.destroy(self.io_vtable);
        if (self.io_owner == .owned) {
            self.io_impl.deinit();
            self.alloc.destroy(self.io_impl);
        }
        self.* = undefined;
    }

    pub fn executor(self: *StdHttpExecutor) common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{
                .execute = execute,
            },
        };
    }

    fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const self: *StdHttpExecutor = @ptrCast(@alignCast(ptr));
        if (req.delivery_tracker) |tracker| tracker.markNotSent();
        try self.beginRequest();
        defer self.endRequest();

        var effective_req = req;
        if (effective_req.timeout_ms == null and self.cfg.request_timeout_ms > 0) {
            effective_req.timeout_ms = self.cfg.request_timeout_ms;
        }
        if (effective_req.cancellation) |cancellation| {
            if (cancellation.isCancelled()) return error.Cancelled;
        }
        if (self.cfg.resolve_before_connect) return try self.executeResolved(alloc, effective_req);
        if (effective_req.timeout_ms != null or effective_req.cancellation != null)
            return try self.executeWithControl(alloc, effective_req);
        return try self.executeDirect(alloc, effective_req);
    }

    fn executeResolved(self: *StdHttpExecutor, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const io = self.io_impl.io();
        self.resolved_client_mutex.lockUncancelable(io);
        defer self.resolved_client_mutex.unlock(io);

        const extra_count = @as(usize, @intFromBool(req.content_type != null)) +
            @as(usize, @intFromBool(req.authorization != null));
        const header_pairs = try alloc.alloc([2][]const u8, req.headers.len + extra_count);
        defer alloc.free(header_pairs);
        var header_index: usize = 0;
        if (req.content_type) |content_type| {
            header_pairs[header_index] = .{ "content-type", content_type };
            header_index += 1;
        }
        if (req.authorization) |authorization| {
            header_pairs[header_index] = .{ "authorization", authorization };
            header_index += 1;
        }
        for (req.headers) |header| {
            header_pairs[header_index] = .{ header.name, header.value };
            header_index += 1;
        }

        const method: httpx.Method = switch (req.method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
        };
        var response = try self.resolved_client.request(method, req.uri, .{
            .headers = header_pairs,
            .body = if (req.body.len == 0) null else req.body,
            .timeout_ms = if (req.timeout_ms) |timeout_ms| timeout_ms else null,
            .cancellation = if (req.cancellation) |cancellation| blk: {
                const token_value = cancellation.token();
                break :blk httpx.CancellationToken.fromCallback(token_value.ptr, token_value.is_cancelled_fn);
            } else null,
            .follow_redirects = false,
        });
        defer response.deinit();

        const content_type = if (response.contentType()) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (content_type) |value| alloc.free(value);

        var header_count: usize = 0;
        for (response.headers.iterator()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            header_count += 1;
        }
        const headers: []common.Header = if (header_count > 0)
            try alloc.alloc(common.Header, header_count)
        else
            @constCast((&[_]common.Header{})[0..]);
        var copied_headers: usize = 0;
        errdefer {
            for (headers[0..copied_headers]) |*header| header.deinit(alloc);
            if (header_count > 0) alloc.free(headers);
        }
        for (response.headers.iterator()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            const owned_name = try alloc.dupe(u8, header.name);
            errdefer alloc.free(owned_name);
            headers[copied_headers] = .{
                .name = owned_name,
                .value = try alloc.dupe(u8, header.value),
            };
            copied_headers += 1;
        }

        const body = if (response.body) |value|
            if (value.len > 0) try alloc.dupe(u8, value) else @constCast((&[_]u8{})[0..])
        else
            @constCast((&[_]u8{})[0..]);
        return .{
            .status = response.status.code,
            .content_type = content_type,
            .headers = headers,
            .body = body,
        };
    }

    fn resolvedClientConfig(cfg: StdHttpExecutorConfig) httpx.ClientConfig {
        return .{
            .timeouts = .{
                .connect_ms = 30_000,
                .read_ms = 30_000,
                .write_ms = 30_000,
                .request_ms = cfg.request_timeout_ms,
            },
            .retry_policy = .{ .max_retries = 0 },
            .redirect_policy = .{ .follow_redirects = false },
            .max_response_size = cfg.max_response_bytes,
            .keep_alive = false,
            .cache_resolved_addresses = cfg.cache_resolved_addresses,
        };
    }

    fn executeWithControl(self: *StdHttpExecutor, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        if (req.timeout_ms != null and req.timeout_ms.? == 0) return error.Timeout;

        const RequestResult = anyerror!common.HttpResponse;
        const RequestState = struct {
            result: RequestResult = error.Canceled,
            done: std.Io.Event = .unset,
        };

        const Task = struct {
            fn requestTask(
                state: *RequestState,
                task_io: std.Io,
                http_executor: *StdHttpExecutor,
                request_alloc: std.mem.Allocator,
                request: common.HttpRequest,
            ) std.Io.Cancelable!void {
                state.result = http_executor.executeDirect(request_alloc, request);
                state.done.set(task_io);
            }

            fn drainResult(state: *RequestState, response_alloc: std.mem.Allocator) void {
                if (!state.done.isSet()) return;
                if (state.result) |response_value| {
                    var response = response_value;
                    response.deinit(response_alloc);
                } else |_| {}
            }

            fn cancelAndDrain(
                group: *std.Io.Group,
                state: *RequestState,
                task_io: std.Io,
                response_alloc: std.mem.Allocator,
            ) void {
                // Group cancellation joins the network task. executeDirect's
                // request/client defers retire an interrupted connection before
                // any request-owned pointers are allowed to leave this scope.
                group.cancel(task_io);
                drainResult(state, response_alloc);
            }
        };

        const io = self.io_impl.io();
        const deadline = if (req.timeout_ms) |value|
            std.Io.Clock.Timestamp.fromNow(io, .{
                .raw = std.Io.Duration.fromMilliseconds(@intCast(value)),
                .clock = .awake,
            })
        else
            null;
        var state = RequestState{};
        var group: std.Io.Group = .init;
        try group.concurrent(io, Task.requestTask, .{
            &state,
            io,
            self,
            alloc,
            req,
        });
        var group_active = true;
        defer if (group_active) group.cancel(io);

        while (!state.done.isSet()) {
            if (req.cancellation) |cancellation| {
                if (cancellation.isCancelled()) {
                    Task.cancelAndDrain(&group, &state, io, alloc);
                    group_active = false;
                    return error.Cancelled;
                }
            }
            if (deadline) |value| {
                if (std.Io.Clock.Timestamp.now(io, .awake).compare(.gte, value)) {
                    Task.cancelAndDrain(&group, &state, io, alloc);
                    group_active = false;
                    return error.Timeout;
                }
            }

            // The network completion event wakes this caller immediately. A
            // bounded timeout is needed only to sample the caller-owned
            // semantic cancellation token; it does not allocate a polling task
            // or a second OS thread per outbound request.
            const wait_deadline = if (req.cancellation != null) blk: {
                const poll_deadline = std.Io.Clock.Timestamp.fromNow(io, .{
                    .raw = std.Io.Duration.fromMilliseconds(cancellation_poll_interval_ms),
                    .clock = .awake,
                });
                if (deadline) |value| {
                    break :blk if (value.compare(.lt, poll_deadline)) value else poll_deadline;
                }
                break :blk poll_deadline;
            } else deadline.?;
            state.done.waitTimeout(io, .{ .deadline = wait_deadline }) catch |err| switch (err) {
                error.Timeout => continue,
                error.Canceled => {
                    Task.cancelAndDrain(&group, &state, io, alloc);
                    group_active = false;
                    return error.Canceled;
                },
            };
        }

        group.await(io) catch |err| {
            group_active = false;
            Task.drainResult(&state, alloc);
            return err;
        };
        group_active = false;
        var response = try state.result;
        if (req.cancellation) |cancellation| {
            if (cancellation.isCancelled()) {
                response.deinit(alloc);
                return error.Cancelled;
            }
        }
        return response;
    }

    fn executeDirect(self: *StdHttpExecutor, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
        const io = threaded_connect_io.io(self.io_impl, self.io_vtable);
        // A controlled request queued behind the serialized keep-alive client
        // must remain interruptible before it acquires a socket of its own.
        if (self.cfg.keep_alive) try self.client_mutex.lock(io);
        defer if (self.cfg.keep_alive) self.client_mutex.unlock(io);

        // std.http.Client owns mutable connection and resolver state. A pooled
        // client must be serialized; non-persistent requests use request-local
        // state so callers can execute concurrently without sacrificing reuse.
        var local_client: std.http.Client = .{
            .allocator = self.alloc,
            .io = io,
            .read_buffer_size = self.cfg.read_buffer_size,
            .write_buffer_size = self.cfg.write_buffer_size,
        };
        defer if (!self.cfg.keep_alive) local_client.deinit();
        const client = if (self.cfg.keep_alive) &self.client else &local_client;

        const uri = try std.Uri.parse(req.uri);
        const method = switch (req.method) {
            .GET => std.http.Method.GET,
            .POST => std.http.Method.POST,
            .PUT => std.http.Method.PUT,
            .DELETE => std.http.Method.DELETE,
        };

        var extra_headers = std.ArrayListUnmanaged(std.http.Header).empty;
        defer extra_headers.deinit(alloc);
        if (req.content_type) |content_type| {
            try extra_headers.append(alloc, .{
                .name = "content-type",
                .value = content_type,
            });
        }
        if (req.authorization) |authorization| {
            try extra_headers.append(alloc, .{
                .name = "authorization",
                .value = authorization,
            });
        }
        for (req.headers) |header| {
            if (!shouldForwardRequestHeader(req.headers, header.name)) continue;
            try extra_headers.append(alloc, .{
                .name = header.name,
                .value = header.value,
            });
        }

        const request_keep_alive = self.reserveRequestKeepAlive();
        var request = try std.http.Client.request(client, method, uri, .{
            .extra_headers = extra_headers.items,
            .keep_alive = request_keep_alive,
        });
        defer request.deinit();

        // Everything above this point is local request preparation or
        // connection establishment. From the first send operation onward, an
        // error cannot prove that the peer did not receive the request.
        if (req.delivery_tracker) |tracker| tracker.markMayHaveBeenSent();
        if (req.body.len > 0 or method.requestHasBody()) {
            request.transfer_encoding = .{ .content_length = req.body.len };
            var body_buffer: [16 * 1024]u8 = undefined;
            var body_writer = try request.sendBodyUnflushed(&body_buffer);
            if (req.body.len > 0) {
                try body_writer.writer.writeAll(req.body);
            }
            try body_writer.end();
            try request.connection.?.flush();
        } else {
            try request.sendBodiless();
        }

        var response = try request.receiveHead(&.{});
        const content_type = if (response.head.content_type) |value|
            try alloc.dupe(u8, value)
        else
            null;
        errdefer if (content_type) |value| alloc.free(value);

        var header_count: usize = 0;
        var header_it = response.head.iterateHeaders();
        while (header_it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            header_count += 1;
        }
        var headers: []common.Header = if (header_count > 0)
            try alloc.alloc(common.Header, header_count)
        else
            @constCast((&[_]common.Header{})[0..]);
        var header_index: usize = 0;
        errdefer {
            for (headers[0..header_index]) |*header| header.deinit(alloc);
            if (header_count > 0) alloc.free(headers);
        }

        header_it = response.head.iterateHeaders();
        while (header_it.next()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            headers[header_index] = .{
                .name = try alloc.dupe(u8, header.name),
                .value = try alloc.dupe(u8, header.value),
            };
            header_index += 1;
        }

        var transfer_buffer: [512]u8 = undefined;
        const body = try response.reader(&transfer_buffer).allocRemaining(alloc, .limited(self.cfg.max_response_bytes));

        const connection_closing = if (request.connection) |connection| connection.closing else true;
        self.recordCompletedRequest(request_keep_alive, connection_closing);
        return .{
            .status = @intFromEnum(response.head.status),
            .content_type = content_type,
            .headers = headers,
            .body = body,
        };
    }

    fn beginRequest(self: *StdHttpExecutor) !void {
        const io = self.io_impl.io();
        self.lifecycle_mutex.lockUncancelable(io);
        defer self.lifecycle_mutex.unlock(io);

        if (self.closing) return error.ExecutorShuttingDown;
        self.in_flight += 1;
    }

    fn endRequest(self: *StdHttpExecutor) void {
        const io = self.io_impl.io();
        self.lifecycle_mutex.lockUncancelable(io);
        defer self.lifecycle_mutex.unlock(io);

        self.in_flight -= 1;
        if (self.in_flight == 0) self.idle_cond.broadcast(io);
    }

    fn reserveRequestKeepAlive(self: *StdHttpExecutor) bool {
        const io = self.io_impl.io();
        self.reuse_mutex.lockUncancelable(io);
        defer self.reuse_mutex.unlock(io);

        if (!self.cfg.keep_alive) return false;
        const max_requests = self.cfg.max_requests_per_connection;
        if (max_requests == 0) return true;
        if (self.requests_on_current_connection + 1 >= max_requests) {
            self.requests_on_current_connection = 0;
            return false;
        }
        self.requests_on_current_connection += 1;
        return true;
    }

    fn recordCompletedRequest(self: *StdHttpExecutor, request_keep_alive: bool, connection_closing: bool) void {
        const io = self.io_impl.io();
        self.reuse_mutex.lockUncancelable(io);
        defer self.reuse_mutex.unlock(io);

        if (!request_keep_alive) {
            self.requests_on_current_connection = 0;
            return;
        }
        if (connection_closing) {
            self.requests_on_current_connection = 0;
            return;
        }
    }
};

fn shouldForwardRequestHeader(headers: []const common.RequestHeader, name: []const u8) bool {
    // The new client request owns framing, routing, connection lifecycle, and
    // the two canonical headers represented separately on HttpRequest.
    const transport_owned = [_][]const u8{
        "authorization",
        "connection",
        "content-length",
        "content-type",
        "expect",
        "host",
        "keep-alive",
        "proxy-authenticate",
        "proxy-authorization",
        "proxy-connection",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    };
    for (transport_owned) |blocked| {
        if (std.ascii.eqlIgnoreCase(name, blocked)) return false;
    }

    // RFC 9110 permits Connection to nominate additional hop-by-hop fields.
    // Strip those tokens as well rather than forwarding connection-specific
    // state to a different socket.
    for (headers) |header| {
        if (!std.ascii.eqlIgnoreCase(header.name, "connection")) continue;
        var tokens = std.mem.splitScalar(u8, header.value, ',');
        while (tokens.next()) |token| {
            if (std.ascii.eqlIgnoreCase(name, std.mem.trim(u8, token, " \t"))) return false;
        }
    }
    return true;
}

test "std http executor module compiles" {
    _ = StdHttpExecutorConfig;
    _ = StdHttpExecutor;
}

test "resolved executor does not retain DNS authority unless explicitly enabled" {
    try std.testing.expect(!StdHttpExecutor.resolvedClientConfig(.{
        .resolve_before_connect = true,
    }).cache_resolved_addresses);
    try std.testing.expect(StdHttpExecutor.resolvedClientConfig(.{
        .resolve_before_connect = true,
        .cache_resolved_addresses = true,
    }).cache_resolved_addresses);
}

test "executor request deadline bounds resolved transport by default" {
    const cfg = StdHttpExecutorConfig{
        .resolve_before_connect = true,
        .request_timeout_ms = 7_500,
    };
    try std.testing.expectEqual(@as(u64, 7_500), StdHttpExecutor.resolvedClientConfig(cfg).timeouts.request_ms);
}

test "std http executor owns a finite controlled request worker budget" {
    var executor = StdHttpExecutor.init(std.testing.allocator, .{
        .io_concurrent_limit = 7,
    });
    defer executor.deinit();

    try std.testing.expectEqual(std.Io.Limit.limited(7), executor.io_impl.concurrent_limit);
}

test "std http executor forwards only end-to-end request headers" {
    const headers = [_]common.RequestHeader{
        .{ .name = "Host", .value = "source.invalid" },
        .{ .name = "Content-Length", .value = "42" },
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "Authorization", .value = "Bearer token" },
        .{ .name = "Connection", .value = "keep-alive, X-Hop" },
        .{ .name = "X-Hop", .value = "socket state" },
        .{ .name = "X-Antfly-Trusted-Principal", .value = "principal-token" },
        .{ .name = "X-Request-Id", .value = "request-1" },
    };
    for (headers[0..6]) |header| {
        try std.testing.expect(!shouldForwardRequestHeader(&headers, header.name));
    }
    try std.testing.expect(shouldForwardRequestHeader(&headers, headers[6].name));
    try std.testing.expect(shouldForwardRequestHeader(&headers, headers[7].name));
}

test "std http executor retires pooled connection before configured cap" {
    var executor = StdHttpExecutor.init(std.testing.allocator, .{
        .keep_alive = true,
        .max_requests_per_connection = 3,
    });
    defer executor.deinit();

    try std.testing.expect(executor.reserveRequestKeepAlive());
    executor.recordCompletedRequest(true, false);
    try std.testing.expectEqual(@as(u32, 1), executor.requests_on_current_connection);

    try std.testing.expect(executor.reserveRequestKeepAlive());
    executor.recordCompletedRequest(true, false);
    try std.testing.expectEqual(@as(u32, 2), executor.requests_on_current_connection);

    try std.testing.expect(!executor.reserveRequestKeepAlive());
    executor.recordCompletedRequest(false, true);
    try std.testing.expectEqual(@as(u32, 0), executor.requests_on_current_connection);
}

test "std http executor resets reuse count when server closes connection" {
    var executor = StdHttpExecutor.init(std.testing.allocator, .{ .max_requests_per_connection = 32 });
    defer executor.deinit();

    executor.requests_on_current_connection = 7;

    executor.recordCompletedRequest(true, true);
    try std.testing.expectEqual(@as(u32, 0), executor.requests_on_current_connection);
}

fn sleepTestMs(io: std.Io, ms: u64) void {
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromMilliseconds(@intCast(ms)),
    }, io) catch {};
}

test "std http executor cancellation interrupts a request queued for the pooled client" {
    const RequestThread = struct {
        executor: common.RequestExecutor,
        cancellation: *const common.RequestCancellation,
        outcome: std.atomic.Value(u8) = .init(0),
        delivery_tracker: common.RequestDeliveryTracker = .{},

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = "http://127.0.0.1:1/never-reached",
                .cancellation = self.cancellation,
                .delivery_tracker = &self.delivery_tracker,
            }) catch |err| {
                self.outcome.store(if (err == error.Cancelled) 1 else 2, .release);
                return;
            };
            response.deinit(std.heap.page_allocator);
            self.outcome.store(3, .release);
        }
    };

    var executor = StdHttpExecutor.init(std.testing.allocator, .{ .keep_alive = true });
    defer executor.deinit();
    const io = threaded_connect_io.io(executor.io_impl, executor.io_vtable);
    executor.client_mutex.lockUncancelable(io);
    var client_locked = true;
    defer if (client_locked) executor.client_mutex.unlock(io);

    var cancellation: common.RequestCancellation = .{};
    var request_state = RequestThread{
        .executor = executor.executor(),
        .cancellation = &cancellation,
    };
    const request_thread = try std.Thread.spawn(.{}, RequestThread.run, .{&request_state});
    var request_joined = false;
    defer if (!request_joined) {
        if (client_locked) {
            executor.client_mutex.unlock(io);
            client_locked = false;
        }
        request_thread.join();
    };

    var admitted = false;
    for (0..1_000) |_| {
        executor.lifecycle_mutex.lockUncancelable(io);
        admitted = executor.in_flight == 1;
        executor.lifecycle_mutex.unlock(io);
        if (admitted) break;
        sleepTestMs(io, 1);
    }
    try std.testing.expect(admitted);
    // Allow the network task to reach the deliberately held client mutex.
    sleepTestMs(io, 25);

    cancellation.cancel();
    for (0..1_000) |_| {
        if (request_state.outcome.load(.acquire) != 0) break;
        sleepTestMs(io, 1);
    }
    const cancelled_while_queued = request_state.outcome.load(.acquire) == 1;

    executor.client_mutex.unlock(io);
    client_locked = false;
    request_thread.join();
    request_joined = true;
    try std.testing.expect(cancelled_while_queued);
    try std.testing.expectEqual(@as(u8, 1), request_state.outcome.load(.acquire));
    try std.testing.expectEqual(.not_sent, request_state.delivery_tracker.load());
}

test "std http executor cancellation interrupts an active response wait" {
    const App = struct {
        entered: std.atomic.Value(bool) = .init(false),
        exited: std.atomic.Value(bool) = .init(false),

        fn executor(self: *@This()) common.RequestExecutor {
            return .{ .ptr = self, .vtable = &.{ .execute = execute } };
        }

        fn execute(ptr: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            const cancellation = req.cancellation orelse return error.TestExpectedCancellation;
            self.entered.store(true, .release);
            while (!cancellation.isCancelled()) {
                sleepTestMs(std.Io.Threaded.global_single_threaded.io(), 1);
            }
            self.exited.store(true, .release);
            return .{ .status = 200, .body = try alloc.dupe(u8, "cancelled") };
        }
    };

    const RequestTask = struct {
        executor: common.RequestExecutor,
        uri: []const u8,
        cancellation: *const common.RequestCancellation,
        outcome: std.atomic.Value(u8) = .init(0),
        delivery_tracker: common.RequestDeliveryTracker = .{},

        fn run(self: *@This()) void {
            var response = self.executor.execute(std.heap.page_allocator, .{
                .method = .GET,
                .uri = self.uri,
                .cancellation = self.cancellation,
                .delivery_tracker = &self.delivery_tracker,
            }) catch |err| {
                self.outcome.store(if (err == error.Cancelled) 1 else 2, .release);
                return;
            };
            response.deinit(std.heap.page_allocator);
            self.outcome.store(3, .release);
        }
    };

    var app = App{};
    var listener = std_http_listener.StdHttpListener.init(std.testing.allocator, .{
        .serve_in_connection_threads = true,
    }, app.executor());
    defer listener.deinit();
    try listener.start();

    const uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(uri);
    var executor = StdHttpExecutor.init(std.testing.allocator, .{});
    defer executor.deinit();
    var cancellation = common.RequestCancellation{};
    var task = RequestTask{
        .executor = executor.executor(),
        .uri = uri,
        .cancellation = &cancellation,
    };
    var task_io = std.Io.Threaded.init(std.testing.allocator, .{
        .concurrent_limit = .limited(1),
    });
    defer task_io.deinit();
    var group: std.Io.Group = .init;
    try group.concurrent(task_io.io(), RequestTask.run, .{&task});
    var group_active = true;
    defer if (group_active) group.cancel(task_io.io());

    for (0..2_000) |_| {
        if (app.entered.load(.acquire)) break;
        sleepTestMs(std.Io.Threaded.global_single_threaded.io(), 1);
    }
    try std.testing.expect(app.entered.load(.acquire));
    cancellation.cancel();
    for (0..2_000) |_| {
        if (task.outcome.load(.acquire) != 0 and app.exited.load(.acquire)) break;
        sleepTestMs(std.Io.Threaded.global_single_threaded.io(), 1);
    }
    try std.testing.expectEqual(@as(u8, 1), task.outcome.load(.acquire));
    try std.testing.expectEqual(.may_have_been_sent, task.delivery_tracker.load());
    try std.testing.expect(app.exited.load(.acquire));
    try group.await(task_io.io());
    group_active = false;
}
