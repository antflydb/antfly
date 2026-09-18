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

//! HTTP request execution over a caller-owned `std.Io`.
//!
//! Unlike `StdHttpExecutor`, this owner does not create or depend on
//! `std.Io.Threaded`. Production, integration, and VOPR callers can therefore
//! put clients and listeners under the same scheduler and clock.

const std = @import("std");
const httpx = @import("httpx");
const common = @import("http_common.zig");

pub const IoHttpExecutorConfig = struct {
    max_response_bytes: usize = 4 << 20,
    connect_timeout_ms: u64 = 30_000,
    read_timeout_ms: u64 = 30_000,
    write_timeout_ms: u64 = 30_000,
    keep_alive: bool = false,
    pool_max_connections: u32 = 20,
    pool_max_per_host: u32 = 5,
};

pub const IoHttpExecutor = struct {
    alloc: std.mem.Allocator,
    io: std.Io,
    client: httpx.Client,
    max_response_bytes: usize,

    pub fn init(
        alloc: std.mem.Allocator,
        io: std.Io,
        cfg: IoHttpExecutorConfig,
    ) IoHttpExecutor {
        const client_config: httpx.ClientConfig = .{
            .timeouts = .{
                .connect_ms = cfg.connect_timeout_ms,
                .read_ms = cfg.read_timeout_ms,
                .write_ms = cfg.write_timeout_ms,
            },
            .retry_policy = .{ .max_retries = 0 },
            .redirect_policy = .{ .follow_redirects = false },
            // Streaming sinks own their output budget and backpressure. The
            // buffered path applies its ceiling per request below.
            .max_response_size = std.math.maxInt(usize),
            .keep_alive = cfg.keep_alive,
            .pool_max_connections = cfg.pool_max_connections,
            .pool_max_per_host = cfg.pool_max_per_host,
            .cache_resolved_addresses = true,
            .cancel_in_flight_on_shutdown = true,
        };
        return .{
            .alloc = alloc,
            .io = io,
            .client = httpx.Client.initWithConfig(alloc, io, client_config),
            .max_response_bytes = cfg.max_response_bytes,
        };
    }

    pub fn deinit(self: *IoHttpExecutor) void {
        self.client.deinit();
        self.* = undefined;
    }

    pub fn activeRequestCount(self: *const IoHttpExecutor) usize {
        return self.client.activeRequestCount();
    }

    pub fn beginShutdown(self: *IoHttpExecutor) void {
        self.client.beginShutdown();
    }

    pub fn drainShutdown(self: *IoHttpExecutor) void {
        self.client.drainShutdown();
    }

    pub fn executor(self: *IoHttpExecutor) common.RequestExecutor {
        return .{
            .ptr = self,
            .vtable = &.{ .execute = execute, .execute_stream = executeStream },
            .realtime_ns_fn = realtimeNs,
            .clock_io = @import("../../runtime_io_abi.zig").Borrow.init(&self.io),
        };
    }

    fn realtimeNs(ptr: *anyopaque) i128 {
        const self: *IoHttpExecutor = @ptrCast(@alignCast(ptr));
        return @intCast(std.Io.Clock.real.now(self.io).nanoseconds);
    }

    fn checkRequest(req: common.HttpRequest) !void {
        if (req.delivery_tracker) |tracker| tracker.markNotSent();
        if (req.timeout_ms != null and req.timeout_ms.? == 0) return error.Timeout;
        if (req.cancellation) |cancellation| {
            if (cancellation.isCancelled()) return error.Cancelled;
        }
    }

    fn requestHeaders(self: *IoHttpExecutor, req: common.HttpRequest) ![][2][]const u8 {
        const extra_count = @as(usize, @intFromBool(req.content_type != null)) +
            @as(usize, @intFromBool(req.authorization != null));
        const header_pairs = try self.alloc.alloc([2][]const u8, req.headers.len + extra_count);
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

        return header_pairs;
    }

    fn requestOptions(req: common.HttpRequest, headers: []const [2][]const u8) httpx.RequestOptions {
        return .{
            .delivery_observer = if (req.delivery_tracker) |tracker| .{
                .context = tracker,
                .before_send = struct {
                    fn mark(context: *anyopaque) void {
                        const observer: *common.RequestDeliveryTracker = @ptrCast(@alignCast(context));
                        observer.markMayHaveBeenSent();
                    }
                }.mark,
            } else null,
            .headers = headers,
            .body = if (req.body.len == 0) null else req.body,
            .timeout_ms = if (req.timeout_ms) |timeout_ms| timeout_ms else null,
            .cancellation = if (req.cancellation) |cancellation| blk: {
                const token = cancellation.token();
                break :blk httpx.CancellationToken.fromCallback(token.ptr, token.is_cancelled_fn);
            } else null,
            .follow_redirects = false,
        };
    }

    fn requestMethod(method: common.Method) httpx.Method {
        return switch (method) {
            .GET => .GET,
            .POST => .POST,
            .PUT => .PUT,
            .DELETE => .DELETE,
        };
    }

    fn executeStream(ptr: *anyopaque, response_alloc: std.mem.Allocator, req: common.HttpRequest, downstream: common.StreamWriter) !bool {
        const self: *IoHttpExecutor = @ptrCast(@alignCast(ptr));
        try checkRequest(req);
        const headers = try self.requestHeaders(req);
        defer self.alloc.free(headers);
        const Adapter = struct {
            alloc: std.mem.Allocator,
            downstream: common.StreamWriter,

            pub fn startResponse(adapter: *@This(), response: httpx.Response) !void {
                var count: usize = 0;
                for (response.headers.iterator()) |_| count += 1;
                const response_headers = try adapter.alloc.alloc(common.RequestHeader, count);
                defer adapter.alloc.free(response_headers);
                for (response.headers.iterator(), 0..) |header, i|
                    response_headers[i] = .{ .name = header.name, .value = header.value };
                // Header slices are borrowed only for this synchronous call,
                // including the routed-read protocol acknowledgement.
                try adapter.downstream.start(adapter.alloc, .{
                    .status = response.status.code,
                    .content_type = response.contentType(),
                    .headers = response_headers,
                });
            }

            pub fn writeAll(adapter: *@This(), bytes: []const u8) !void {
                try adapter.downstream.writeAll(bytes);
            }
        };
        var adapter = Adapter{ .alloc = response_alloc, .downstream = downstream };
        var options = requestOptions(req, headers);
        options.max_response_size = if (req.max_response_bytes != null) req.responseLimit(self.max_response_bytes) else null;
        var response = try self.client.requestToWriter(requestMethod(req.method), req.uri, options, &adapter, null, null);
        defer response.deinit();
        try downstream.flush();
        return true;
    }

    fn execute(
        ptr: *anyopaque,
        response_alloc: std.mem.Allocator,
        req: common.HttpRequest,
    ) !common.HttpResponse {
        const self: *IoHttpExecutor = @ptrCast(@alignCast(ptr));
        try checkRequest(req);
        const headers_in = try self.requestHeaders(req);
        defer self.alloc.free(headers_in);
        var options = requestOptions(req, headers_in);
        options.max_response_size = req.responseLimit(self.max_response_bytes);
        var response = try self.client.request(requestMethod(req.method), req.uri, options);
        defer response.deinit();

        const content_type = if (response.contentType()) |value|
            try response_alloc.dupe(u8, value)
        else
            null;
        errdefer if (content_type) |value| response_alloc.free(value);

        var header_count: usize = 0;
        for (response.headers.iterator()) |header| {
            if (!std.ascii.eqlIgnoreCase(header.name, "content-type")) header_count += 1;
        }
        const headers: []common.Header = if (header_count == 0)
            @constCast((&[_]common.Header{})[0..])
        else
            try response_alloc.alloc(common.Header, header_count);
        var copied_headers: usize = 0;
        errdefer {
            for (headers[0..copied_headers]) |*header| header.deinit(response_alloc);
            if (header_count > 0) response_alloc.free(headers);
        }
        for (response.headers.iterator()) |header| {
            if (std.ascii.eqlIgnoreCase(header.name, "content-type")) continue;
            const owned_name = try response_alloc.dupe(u8, header.name);
            errdefer response_alloc.free(owned_name);
            headers[copied_headers] = .{
                .name = owned_name,
                .value = try response_alloc.dupe(u8, header.value),
            };
            copied_headers += 1;
        }

        const body = if (response.body) |value|
            if (value.len == 0)
                @constCast((&[_]u8{})[0..])
            else
                try response_alloc.dupe(u8, value)
        else
            @constCast((&[_]u8{})[0..]);
        return .{
            .status = response.status.code,
            .content_type = content_type,
            .headers = headers,
            .body = body,
        };
    }
};

test "I/O HTTP executor rejects an already-cancelled request without transport work" {
    var cancelled = std.atomic.Value(bool).init(true);
    const cancellation = common.RequestCancellation{ .borrowed = &cancelled };
    var tracker: common.RequestDeliveryTracker = .{};
    var executor = IoHttpExecutor.init(std.testing.allocator, std.testing.io, .{});
    defer executor.deinit();

    try std.testing.expectError(error.Cancelled, executor.executor().execute(std.testing.allocator, .{
        .method = .GET,
        .uri = "http://127.0.0.1:1/never-sent",
        .cancellation = &cancellation,
        .delivery_tracker = &tracker,
    }));
    try std.testing.expectEqual(.not_sent, tracker.load());
}

test "I/O HTTP executor preserves not-sent proof when concurrency admission fails" {
    // One task permits the watchdog but rejects the network task. Neither
    // partial task admission nor its cancellation may lose not-sent proof.
    var runtime = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(1) });
    defer runtime.deinit();
    var executor = IoHttpExecutor.init(std.testing.allocator, runtime.io(), .{});
    defer executor.deinit();
    var tracker: common.RequestDeliveryTracker = .{};
    try std.testing.expectError(error.ConcurrencyUnavailable, executor.executor().execute(std.testing.allocator, .{
        .method = .POST,
        .uri = "http://127.0.0.1:1/never-sent",
        .body = "{}",
        .timeout_ms = 1000,
        .delivery_tracker = &tracker,
    }));
    try std.testing.expectEqual(.not_sent, tracker.load());
}

test "I/O HTTP executor streams routed response headers and bounded chunks beyond the buffered ceiling" {
    const App = struct {
        fn execute(_: *anyopaque, alloc: std.mem.Allocator, req: common.HttpRequest) !common.HttpResponse {
            try std.testing.expectEqual(common.Method.POST, req.method);
            try std.testing.expectEqualStrings("{\"scan\":true}", req.body);
            try std.testing.expectEqualStrings("Bearer stream-test", req.authorization.?);
            const body = try alloc.alloc(u8, 128 * 1024);
            errdefer alloc.free(body);
            @memset(body, 'x');
            const headers = try alloc.alloc(common.Header, 1);
            errdefer alloc.free(headers);
            const name = try alloc.dupe(u8, "x-antfly-catalog-route-fence-ack");
            errdefer alloc.free(name);
            const value = try alloc.dupe(u8, "supported");
            errdefer alloc.free(value);
            headers[0] = .{ .name = name, .value = value };
            return .{ .status = 200, .content_type = try alloc.dupe(u8, "application/x-ndjson"), .headers = headers, .body = body };
        }
    };
    const Sink = struct {
        starts: usize = 0,
        writes: usize = 0,
        bytes: usize = 0,
        flushes: usize = 0,
        reject: bool = false,

        fn start(raw: *anyopaque, _: std.mem.Allocator, response: common.StreamingResponse) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.starts += 1;
            try std.testing.expectEqual(@as(u16, 200), response.status);
            try std.testing.expectEqualStrings("application/x-ndjson", response.content_type.?);
            var found = false;
            for (response.headers) |header| {
                if (std.ascii.eqlIgnoreCase(header.name, "x-antfly-catalog-route-fence-ack")) {
                    try std.testing.expectEqualStrings("supported", header.value);
                    found = true;
                }
            }
            try std.testing.expect(found);
        }

        fn write(raw: *anyopaque, bytes: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try std.testing.expectEqual(@as(usize, 1), self.starts);
            self.writes += 1;
            if (self.reject) return error.TestSinkRejected;
            try std.testing.expect(std.mem.allEqual(u8, bytes, 'x'));
            self.bytes += bytes.len;
        }

        fn flush(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.flushes += 1;
        }

        fn writer(self: *@This()) common.StreamWriter {
            return .{ .ptr = self, .vtable = &.{ .start = start, .write_all = write, .flush = flush } };
        }
    };
    var app: App = .{};
    var listener = @import("std_http_listener.zig").StdHttpListener.init(std.testing.allocator, .{}, .{ .ptr = &app, .vtable = &.{ .execute = App.execute } });
    defer listener.deinit();
    try listener.start();
    const uri = try listener.baseUri(std.testing.allocator);
    defer std.testing.allocator.free(uri);
    var executor = IoHttpExecutor.init(std.testing.allocator, std.testing.io, .{ .max_response_bytes = 16, .keep_alive = true });
    defer executor.deinit();
    const request: common.HttpRequest = .{ .method = .POST, .uri = uri, .body = "{\"scan\":true}", .authorization = "Bearer stream-test", .content_type = "application/json", .timeout_ms = 5000 };

    // A sink failure releases the request lease and does not poison the next
    // stream or silently retry a partially consumed snapshot.
    var rejected: Sink = .{ .reject = true };
    try std.testing.expectError(error.TestSinkRejected, executor.executor().executeStream(std.testing.allocator, request, rejected.writer()));
    try std.testing.expectEqual(@as(usize, 0), executor.activeRequestCount());
    try std.testing.expectEqual(@as(usize, 0), rejected.flushes);
    var sink: Sink = .{};
    try std.testing.expect((try executor.executor().executeStream(std.testing.allocator, request, sink.writer())).?);
    try std.testing.expectEqual(@as(usize, 128 * 1024), sink.bytes);
    try std.testing.expect(sink.writes > 1);
    try std.testing.expectEqual(@as(usize, 1), sink.flushes);
    try std.testing.expectEqual(@as(usize, 0), executor.activeRequestCount());
    try std.testing.expectError(error.ResponseTooLarge, executor.executor().execute(std.testing.allocator, request));
    var capped = request;
    capped.max_response_bytes = 256 * 1024;
    var cap_sink: Sink = .{};
    // A caller cannot raise the configured ceiling, even for an explicitly
    // bounded stream. Null above preserves the existing sink-owned budget.
    try std.testing.expectError(error.ResponseTooLarge, executor.executor().executeStream(std.testing.allocator, capped, cap_sink.writer()));
    try std.testing.expectEqual(@as(usize, 0), cap_sink.bytes);
    var larger = IoHttpExecutor.init(std.testing.allocator, std.testing.io, .{ .max_response_bytes = 256 * 1024 });
    defer larger.deinit();
    capped.max_response_bytes = 64;
    try std.testing.expectError(error.ResponseTooLarge, larger.executor().execute(std.testing.allocator, capped));
    cap_sink = .{};
    try std.testing.expectError(error.ResponseTooLarge, larger.executor().executeStream(std.testing.allocator, capped, cap_sink.writer()));
    try std.testing.expectEqual(@as(usize, 0), cap_sink.bytes);
    try std.testing.expectEqual(@as(usize, 0), larger.activeRequestCount());
}

test "I/O HTTP executor streaming cancellation and zero deadline retain not-sent proof" {
    var executor = IoHttpExecutor.init(std.testing.allocator, std.testing.io, .{});
    defer executor.deinit();
    var cancellation: common.RequestCancellation = .{};
    cancellation.cancel();
    var tracker: common.RequestDeliveryTracker = .{};
    const unused: common.StreamWriter = undefined;
    var request: common.HttpRequest = .{ .method = .GET, .uri = "http://127.0.0.1:1/never-sent", .cancellation = &cancellation, .delivery_tracker = &tracker };
    try std.testing.expectError(error.Cancelled, executor.executor().executeStream(std.testing.allocator, request, unused));
    try std.testing.expectEqual(.not_sent, tracker.load());
    request.cancellation = null;
    request.timeout_ms = 0;
    try std.testing.expectError(error.Timeout, executor.executor().executeStream(std.testing.allocator, request, unused));
    try std.testing.expectEqual(.not_sent, tracker.load());
    try std.testing.expectEqual(@as(usize, 0), executor.activeRequestCount());
}
