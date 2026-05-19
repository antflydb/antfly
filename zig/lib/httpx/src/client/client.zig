//! HTTP Client Implementation for httpx.zig
//!
//! HTTP/1.1 and HTTP/2 client over TCP with optional TLS (HTTPS).
//!
//! ## HTTP/2 Support
//!
//! Set `http2_enabled` or `force_http2` in `ClientConfig`. The client uses
//! "prior knowledge" mode (RFC 7540 §3.4), sending the h2 connection preface
//! directly. ALPN negotiation is not available (Zig stdlib limitation).
//!
//! One h2 connection is maintained per host:port in `h2_conns`. When the Io
//! backend supports fibers, a background receive-loop fiber pumps frames
//! continuously, enabling true stream multiplexing — multiple request fibers
//! can share the connection, with writes serialized via `write_mutex` and
//! per-stream completion signaled via `Io.Event`. Without fiber support,
//! frames are pumped inline (one request at a time).

const std = @import("std");
const array_list_writer_mod = @import("../util/array_list_writer.zig");
const arrayListWriter = array_list_writer_mod.arrayListWriter;
const serializeToSlice = array_list_writer_mod.serializeToSlice;
const mem = std.mem;
const Allocator = mem.Allocator;
const Io = std.Io;

const types = @import("../core/types.zig");
const meta = @import("../core/meta.zig");
const Headers = @import("../core/headers.zig").Headers;
const HeaderName = @import("../core/headers.zig").HeaderName;
const Uri = @import("../core/uri.zig").Uri;
const Request = @import("../core/request.zig").Request;
const Response = @import("../core/response.zig").Response;
const Status = @import("../core/status.zig").Status;
const socket_mod = @import("../net/socket.zig");
const Socket = socket_mod.Socket;
const Address = socket_mod.Address;
const resolveAddress = socket_mod.resolveAddress;
const SocketIoReader = socket_mod.SocketIoReader;
const SocketIoWriter = socket_mod.SocketIoWriter;
const SliceIoReader = socket_mod.SliceIoReader;
const PrefixedReader = socket_mod.PrefixedReader;
const ContentLengthReader = socket_mod.ContentLengthReader;
const ChunkedBodyReader = socket_mod.ChunkedBodyReader;
const IoReaderHelpers = socket_mod.IoReaderHelpers;
const Parser = @import("../protocol/parser.zig").Parser;
const TlsConfig = @import("../tls/tls.zig").TlsConfig;
const TlsSession = @import("../tls/tls.zig").TlsSession;
const ConnectionPool = @import("pool.zig").ConnectionPool;
const TlsPool = @import("pool.zig").TlsPool;
const TlsConnection = @import("pool.zig").TlsConnection;
const common = @import("../util/common.zig");
const flate = std.compress.flate;
const h2_mod = @import("../protocol/h2_connection.zig");
const H2Connection = h2_mod.H2Connection;
const hpack = @import("../protocol/hpack.zig");
const Stream = @import("../protocol/stream.zig").Stream;

/// HTTP client configuration.
pub const ClientConfig = struct {
    base_url: ?[]const u8 = null,
    timeouts: types.Timeouts = .{},
    retry_policy: types.RetryPolicy = .{},
    redirect_policy: types.RedirectPolicy = .{},
    default_headers: ?[]const [2][]const u8 = null,
    user_agent: []const u8 = meta.default_user_agent,
    max_response_size: usize = types.default_max_body_size,
    max_response_headers: usize = 256,
    verify_ssl: bool = true,
    /// Enable HTTP/2 via "prior knowledge" mode (RFC 7540 §3.4).
    /// The client sends the h2 preface directly without ALPN negotiation.
    /// When Zig's stdlib exposes ALPN, this will additionally negotiate h2.
    http2_enabled: bool = false,
    /// Alias for `http2_enabled`. When true, the client always speaks HTTP/2
    /// to every host, regardless of ALPN (which the stdlib doesn't support yet).
    force_http2: bool = false,
    http3_enabled: bool = false,
    keep_alive: bool = true,
    pool_max_connections: u32 = 20,
    pool_max_per_host: u32 = 5,
    max_cookies: usize = 1000,
    /// Send a PING health check after this many ms of no frames received on
    /// an H2 connection. 0 = disabled. Similar to Go's http2.Transport.ReadIdleTimeout.
    h2_read_idle_timeout_ms: u64 = 30_000,
    /// How long to wait for a PING ACK before declaring the connection dead (ms).
    /// Similar to Go's http2.Transport.PingTimeout. Only used when read_idle_timeout > 0.
    h2_ping_timeout_ms: u64 = 15_000,
};

/// Per-request options.
pub const RequestOptions = struct {
    headers: ?[]const [2][]const u8 = null,
    body: ?[]const u8 = null,
    json: ?[]const u8 = null,
    timeout_ms: ?u64 = null,
    follow_redirects: ?bool = null,
};

pub const WriterProgress = struct {
    bytes_written: u64,
    total_bytes: ?u64,
};

pub const WriterProgressCallback = *const fn (progress: WriterProgress, context: ?*anyopaque) void;

/// Request interceptor function type.
pub const RequestInterceptor = *const fn (*Request, ?*anyopaque) anyerror!void;

/// Response interceptor function type.
pub const ResponseInterceptor = *const fn (*Response, ?*anyopaque) anyerror!void;

/// Interceptor with context.
pub const Interceptor = struct {
    request_fn: ?RequestInterceptor = null,
    response_fn: ?ResponseInterceptor = null,
    context: ?*anyopaque = null,
};

/// A pooled HTTP/2 connection: socket + optional TLS + H2Connection state.
/// Heap-allocated for pointer stability (TlsSession stores internal pointers).
/// When fiber support is available, a background receive-loop fiber pumps
/// frames continuously, enabling true stream multiplexing on one connection.
const H2PoolEntry = struct {
    socket: Socket,
    session: TlsSession,
    h2: H2Connection,
    is_tls: bool,
    broken: bool = false,
    /// Tracks the background receive-loop fiber (if spawned).
    recv_group: Io.Group = Io.Group.init,
    /// True when a receive-loop fiber is actively pumping frames.
    recv_running: bool = false,
    /// Tracks the PING health-check fiber.
    ping_group: Io.Group = Io.Group.init,
    /// Monotonic timestamp (ms) of the last received frame, updated by the
    /// receive loop fiber and read by the ping timer fiber.
    last_frame_ts: i64 = 0,
    /// Client config for timeout values (set during creation).
    read_idle_timeout_ms: u64 = 0,
    ping_timeout_ms: u64 = 15_000,
    io: Io = undefined,
};

const TlsSessionIoReader = struct {
    inner: *TlsSession,
    max_read: usize,
    reader_iface: Io.Reader,
    const max_empty_reads: usize = 5000;

    fn init(inner: *TlsSession, max_read: usize, buffer: []u8) TlsSessionIoReader {
        return .{
            .inner = inner,
            .max_read = max_read,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *TlsSessionIoReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        var iovecs_buffer: [8][]u8 = undefined;
        const dest_n, const data_size = try r.writableVector(&iovecs_buffer, bufs);
        const dest = iovecs_buffer[0..dest_n];
        if (dest.len == 0 or dest[0].len == 0) return 0;

        const read_buf = dest[0][0..@min(dest[0].len, p.max_read)];
        var empty_read_policy = EmptyReadPolicy.init(max_empty_reads);

        while (true) {
            const n = p.inner.read(read_buf) catch return error.ReadFailed;

            switch (empty_read_policy.observe(n)) {
                .done => {
                    if (n > data_size) {
                        r.end += n - data_size;
                        return data_size;
                    }
                    return n;
                },
                .retry => sleepAfterEmptyRead(p.inner.io),
                .eof => return error.EndOfStream,
            }
        }
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

const EmptyReadPolicy = struct {
    empty_reads: usize = 0,
    max_empty_reads: usize,

    const Decision = enum {
        done,
        retry,
        eof,
    };

    fn init(max_empty_reads: usize) EmptyReadPolicy {
        return .{ .max_empty_reads = max_empty_reads };
    }

    fn observe(self: *EmptyReadPolicy, n: usize) Decision {
        if (n != 0) {
            self.empty_reads = 0;
            return .done;
        }
        self.empty_reads += 1;
        if (self.empty_reads >= self.max_empty_reads) return .eof;
        return .retry;
    }
};

fn sleepAfterEmptyRead(io: Io) void {
    // Zero-duration sleep yields to the Io scheduler without the 1ms penalty
    // that was throttling large TLS downloads to ~2 KiB/s. TLS reads can
    // return 0 bytes between record boundaries, triggering thousands of sleeps.
    io.sleep(Io.Duration.zero, .awake) catch {};
}

/// HTTP Client.
pub const Client = struct {
    allocator: Allocator,
    io: Io,
    config: ClientConfig,
    interceptors: std.ArrayListUnmanaged(Interceptor) = .empty,
    cookies: std.StringHashMapUnmanaged([]const u8) = .{},
    cookie_mutex: Io.Mutex = Io.Mutex.init,
    pool: ConnectionPool,
    tls_pool: TlsPool,
    /// Cached HTTP/2 connections keyed by "host:port".
    h2_conns: std.StringHashMapUnmanaged(*H2PoolEntry) = .{},
    /// Protects h2_conns from concurrent fiber access during connection
    /// creation (getOrCreateH2Conn yields on DNS, connect, TLS handshake).
    h2_mutex: Io.Mutex = Io.Mutex.init,

    const Self = @This();

    /// Creates a new HTTP client with default configuration.
    pub fn init(allocator: Allocator, io: Io) Self {
        return initWithConfig(allocator, io, .{});
    }

    /// Creates a new HTTP client with custom configuration.
    pub fn initWithConfig(allocator: Allocator, io: Io, config: ClientConfig) Self {
        const pool_cfg = @import("pool.zig").PoolConfig{
            .max_connections = config.pool_max_connections,
            .max_per_host = config.pool_max_per_host,
        };
        return .{
            .allocator = allocator,
            .io = io,
            .config = config,
            .pool = ConnectionPool.initWithConfig(allocator, io, pool_cfg, {}),
            .tls_pool = TlsPool.initWithConfig(allocator, io, pool_cfg, .{ .verify_ssl = config.verify_ssl }),
        };
    }

    /// Releases all allocated resources.
    pub fn deinit(self: *Self) void {
        self.interceptors.deinit(self.allocator);
        var it = self.cookies.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.cookies.deinit(self.allocator);
        self.pool.deinit();
        self.tls_pool.deinit();

        // Clean up cached HTTP/2 connections.
        var h2_it = self.h2_conns.iterator();
        while (h2_it.next()) |entry| {
            const e = entry.value_ptr.*;
            // Close the socket first so the receive loop's blocking read
            // returns ConnectionClosed, allowing the fiber to exit cleanly.
            // This also causes the ping timer's waitTimeout to fail.
            e.socket.close();
            // Await recv_group first: the receive loop sets entry.broken=true
            // on exit, which causes the ping fiber's while(!broken) loop to
            // terminate without waiting for a full ping timeout cycle.
            e.recv_group.await(self.io) catch {};
            e.ping_group.await(self.io) catch {};
            e.h2.deinit();
            e.session.deinit();
            self.allocator.destroy(e);
            self.allocator.free(entry.key_ptr.*);
        }
        self.h2_conns.deinit(self.allocator);
    }

    /// Adds an interceptor to the client.
    pub fn addInterceptor(self: *Self, interceptor: Interceptor) !void {
        try self.interceptors.append(self.allocator, interceptor);
    }

    /// Makes an HTTP request.
    pub fn request(self: *Self, method: types.Method, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.requestInternal(method, url, reqOpts, 0);
    }

    /// Makes an HTTP request and streams the response body to `writer`.
    /// The returned response contains status and headers; the body is not retained.
    pub fn requestToWriter(
        self: *Self,
        method: types.Method,
        url: []const u8,
        reqOpts: RequestOptions,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !Response {
        return self.requestToWriterInternal(method, url, reqOpts, writer, progress_cb, progress_ctx, 0);
    }

    pub fn getToWriter(
        self: *Self,
        url: []const u8,
        reqOpts: RequestOptions,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !Response {
        return self.requestToWriter(.GET, url, reqOpts, writer, progress_cb, progress_ctx);
    }

    fn requestInternal(self: *Self, method: types.Method, url: []const u8, reqOpts: RequestOptions, depth: u32) !Response {
        const owned_url = if (self.config.base_url) |base|
            try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ base, url })
        else
            null;
        defer if (owned_url) |u| self.allocator.free(u);
        const full_url = owned_url orelse url;

        var req = try Request.init(self.allocator, method, full_url);
        defer req.deinit();

        try req.headers.set(HeaderName.USER_AGENT, self.config.user_agent);

        if (self.config.default_headers) |hdrs| {
            for (hdrs) |h| {
                try req.headers.set(h[0], h[1]);
            }
        }

        if (reqOpts.headers) |hdrs| {
            for (hdrs) |h| {
                try req.headers.set(h[0], h[1]);
            }
        }

        if (!self.config.keep_alive and !req.headers.contains(HeaderName.CONNECTION)) {
            try req.headers.set(HeaderName.CONNECTION, "close");
        }

        if (reqOpts.body) |body| {
            try req.setBody(body);
        }

        if (reqOpts.json) |json_body| {
            try req.setJson(json_body);
        }

        // Request compressed responses unless the caller already set Accept-Encoding.
        if (!req.headers.contains(HeaderName.ACCEPT_ENCODING)) {
            try req.headers.set(HeaderName.ACCEPT_ENCODING, "gzip, deflate");
        }

        try self.attachCookies(&req);

        for (self.interceptors.items) |interceptor| {
            if (interceptor.request_fn) |f| {
                try f(&req, interceptor.context);
            }
        }

        var response = try self.executeRequest(&req, reqOpts.timeout_ms);
        errdefer response.deinit();

        try self.storeCookies(&response);

        for (self.interceptors.items) |interceptor| {
            if (interceptor.response_fn) |f| {
                try f(&response, interceptor.context);
            }
        }

        const should_follow = reqOpts.follow_redirects orelse
            self.config.redirect_policy.follow_redirects;
        if (should_follow and response.isRedirect()) {
            if (depth >= self.config.redirect_policy.max_redirects) {
                response.deinit();
                return error.TooManyRedirects;
            }

            const location = response.headers.get(HeaderName.LOCATION) orelse {
                response.deinit();
                return error.InvalidResponse;
            };

            const next_url = try self.resolveRedirectUrl(req.uri, location);
            defer self.allocator.free(next_url);

            const next_method = self.config.redirect_policy.getRedirectMethod(response.status.code, req.method);
            response.deinit();
            return self.requestInternal(next_method, next_url, reqOpts, depth + 1);
        }

        return response;
    }

    fn requestToWriterInternal(
        self: *Self,
        method: types.Method,
        url: []const u8,
        reqOpts: RequestOptions,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
        depth: u32,
    ) !Response {
        const owned_url = if (self.config.base_url) |base|
            try std.fmt.allocPrint(self.allocator, "{s}{s}", .{ base, url })
        else
            null;
        defer if (owned_url) |u| self.allocator.free(u);
        const full_url = owned_url orelse url;

        var req = try Request.init(self.allocator, method, full_url);
        defer req.deinit();

        try req.headers.set(HeaderName.USER_AGENT, self.config.user_agent);

        if (self.config.default_headers) |hdrs| {
            for (hdrs) |h| {
                try req.headers.set(h[0], h[1]);
            }
        }

        if (reqOpts.headers) |hdrs| {
            for (hdrs) |h| {
                try req.headers.set(h[0], h[1]);
            }
        }

        if (!self.config.keep_alive and !req.headers.contains(HeaderName.CONNECTION)) {
            try req.headers.set(HeaderName.CONNECTION, "close");
        }

        if (reqOpts.body) |body| {
            try req.setBody(body);
        }

        if (reqOpts.json) |json_body| {
            try req.setJson(json_body);
        }

        if (!req.headers.contains(HeaderName.ACCEPT_ENCODING)) {
            try req.headers.set(HeaderName.ACCEPT_ENCODING, "gzip, deflate");
        }

        try self.attachCookies(&req);

        for (self.interceptors.items) |interceptor| {
            if (interceptor.request_fn) |f| {
                try f(&req, interceptor.context);
            }
        }

        var response = try self.executeRequestToWriter(&req, reqOpts.timeout_ms, writer, progress_cb, progress_ctx);
        errdefer response.deinit();

        try self.storeCookies(&response);

        for (self.interceptors.items) |interceptor| {
            if (interceptor.response_fn) |f| {
                try f(&response, interceptor.context);
            }
        }

        const should_follow = reqOpts.follow_redirects orelse
            self.config.redirect_policy.follow_redirects;
        if (should_follow and response.isRedirect()) {
            if (depth >= self.config.redirect_policy.max_redirects) {
                response.deinit();
                return error.TooManyRedirects;
            }

            const location = response.headers.get(HeaderName.LOCATION) orelse {
                response.deinit();
                return error.InvalidResponse;
            };

            const next_url = try self.resolveRedirectUrl(req.uri, location);
            defer self.allocator.free(next_url);

            const next_method = self.config.redirect_policy.getRedirectMethod(response.status.code, req.method);
            response.deinit();
            return self.requestToWriterInternal(next_method, next_url, reqOpts, writer, progress_cb, progress_ctx, depth + 1);
        }

        return response;
    }

    /// Executes the actual HTTP request.
    fn executeRequest(self: *Self, req: *Request, timeout_override_ms: ?u64) !Response {
        const policy = self.config.retry_policy;
        const can_retry_method = (!policy.retry_only_idempotent) or req.method.isIdempotent();

        var attempt: u32 = 0;
        while (true) {
            var res = self.executeRequestOnce(req, timeout_override_ms) catch |err| {
                // RFC 7540 §6.8: Streams refused via GOAWAY were never
                // processed and are always safe to retry on a new connection.
                const is_goaway_refused = (err == error.GoawayRefused);
                const is_max_streams = (err == error.MaxConcurrentStreamsExceeded);
                if ((is_goaway_refused or is_max_streams or (policy.retry_on_connection_error and can_retry_method)) and attempt < policy.max_retries) {
                    attempt += 1;
                    const delay_ms = policy.calculateDelay(attempt);
                    if (delay_ms > 0) {
                        self.io.sleep(Io.Duration.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};
                    }
                    continue;
                }
                return err;
            };

            if (can_retry_method and attempt < policy.max_retries and policy.shouldRetryStatus(res.status.code)) {
                res.deinit();
                attempt += 1;
                const delay_ms = policy.calculateDelay(attempt);
                if (delay_ms > 0) {
                    self.io.sleep(Io.Duration.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};
                }
                continue;
            }

            return res;
        }
    }

    fn executeRequestToWriter(
        self: *Self,
        req: *Request,
        timeout_override_ms: ?u64,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !Response {
        const policy = self.config.retry_policy;
        const can_retry_method = (!policy.retry_only_idempotent) or req.method.isIdempotent();

        var attempt: u32 = 0;
        while (true) {
            var res = self.executeRequestToWriterOnce(req, timeout_override_ms, writer, progress_cb, progress_ctx) catch |err| {
                const is_goaway_refused = (err == error.GoawayRefused);
                const is_max_streams = (err == error.MaxConcurrentStreamsExceeded);
                if ((is_goaway_refused or is_max_streams or (policy.retry_on_connection_error and can_retry_method)) and attempt < policy.max_retries) {
                    attempt += 1;
                    const delay_ms = policy.calculateDelay(attempt);
                    if (delay_ms > 0) {
                        self.io.sleep(Io.Duration.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};
                    }
                    continue;
                }
                return err;
            };

            if (can_retry_method and attempt < policy.max_retries and policy.shouldRetryStatus(res.status.code)) {
                res.deinit();
                attempt += 1;
                const delay_ms = policy.calculateDelay(attempt);
                if (delay_ms > 0) {
                    self.io.sleep(Io.Duration.fromMilliseconds(@intCast(delay_ms)), .awake) catch {};
                }
                continue;
            }

            return res;
        }
    }

    fn applyTimeouts(socket: *Socket, recv_ms: u64, send_ms: u64) !void {
        if (recv_ms > 0) try socket.setRecvTimeout(recv_ms);
        if (send_ms > 0) try socket.setSendTimeout(send_ms);
    }

    fn executeRequestOnce(self: *Self, req: *Request, timeout_override_ms: ?u64) !Response {
        const host = req.uri.host orelse return error.InvalidUri;
        const port = req.uri.effectivePort();
        // Use request_ms as a fallback ceiling for socket timeouts when the
        // specific read/write timeouts are not set via per-request override.
        const timeout_ms = timeout_override_ms orelse blk: {
            if (self.config.timeouts.request_ms > 0) break :blk self.config.timeouts.request_ms;
            break :blk self.config.timeouts.read_ms;
        };
        const write_timeout_ms = timeout_override_ms orelse blk: {
            if (self.config.timeouts.request_ms > 0) break :blk self.config.timeouts.request_ms;
            break :blk self.config.timeouts.write_ms;
        };

        // HTTP/2 "prior knowledge" path (RFC 7540 §3.4).
        // Reuses a pooled connection per host when available.
        if (self.config.http2_enabled or self.config.force_http2) {
            const is_tls = req.uri.isTls();
            const entry = try self.getOrCreateH2Conn(host, port, is_tls);
            const result = self.executeH2OnPooled(entry, req) catch |err| {
                // Only mark the connection broken for transport/framing errors.
                // Stream-level errors (MaxConcurrentStreamsExceeded, ContentLengthMismatch,
                // StreamDataOverflow) don't indicate a bad connection — other streams
                // may still be healthy.
                switch (err) {
                    error.MaxConcurrentStreamsExceeded,
                    error.ContentLengthMismatch,
                    error.StreamDataOverflow,
                    => {},
                    else => entry.broken = true,
                }
                return err;
            };
            // Mark broken if peer sent GOAWAY — next request gets a fresh connection.
            if (entry.h2.goaway_received) entry.broken = true;
            return result;
        }

        if (req.uri.isTls()) {
            if (self.config.keep_alive) {
                var tls_conn = try self.tls_pool.getConnection(host, port);
                var ok = false;
                defer {
                    if (ok) self.tls_pool.releaseConnection(tls_conn) else self.tls_pool.evictConnection(tls_conn);
                }

                try applyTimeouts(&tls_conn.socket, timeout_ms, write_timeout_ms);
                return self.executeOnTls(&tls_conn.session, req, &ok);
            }

            // Non-pooled TLS fallback (keep_alive disabled).
            const addr = try resolveAddress(self.io, host, port);
            var socket = try Socket.connect(addr, self.io);
            defer socket.close();
            try applyTimeouts(&socket, timeout_ms, write_timeout_ms);
            return self.executeOnNewTls(&socket, host, req);
        }

        if (self.config.keep_alive) {
            var conn = try self.pool.getConnection(host, port);
            var ok = false;
            defer {
                if (ok) self.pool.releaseConnection(conn) else self.pool.evictConnection(conn);
            }

            try applyTimeouts(&conn.socket, timeout_ms, write_timeout_ms);
            return self.executeOnSocket(&conn.socket, req, &ok);
        }

        const addr = try resolveAddress(self.io, host, port);
        var socket = try Socket.connect(addr, self.io);
        defer socket.close();
        try applyTimeouts(&socket, timeout_ms, write_timeout_ms);
        return self.executeOnSocket(&socket, req, null);
    }

    fn executeRequestToWriterOnce(
        self: *Self,
        req: *Request,
        timeout_override_ms: ?u64,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !Response {
        const host = req.uri.host orelse return error.InvalidUri;
        const port = req.uri.effectivePort();
        const timeout_ms = timeout_override_ms orelse blk: {
            if (self.config.timeouts.request_ms > 0) break :blk self.config.timeouts.request_ms;
            break :blk self.config.timeouts.read_ms;
        };
        const write_timeout_ms = timeout_override_ms orelse blk: {
            if (self.config.timeouts.request_ms > 0) break :blk self.config.timeouts.request_ms;
            break :blk self.config.timeouts.write_ms;
        };

        if (self.config.http2_enabled or self.config.force_http2) {
            var res = try self.executeRequestOnce(req, timeout_override_ms);
            errdefer res.deinit();
            try writeBufferedBody(&res, writer, progress_cb, progress_ctx);
            return res;
        }

        if (req.uri.isTls()) {
            if (self.config.keep_alive) {
                var tls_conn = try self.tls_pool.getConnection(host, port);
                var ok = false;
                defer {
                    if (ok) self.tls_pool.releaseConnection(tls_conn) else self.tls_pool.evictConnection(tls_conn);
                }

                try applyTimeouts(&tls_conn.socket, timeout_ms, write_timeout_ms);
                return self.executeOnTlsToWriter(&tls_conn.session, req, writer, progress_cb, progress_ctx, &ok);
            }

            const addr = try resolveAddress(self.io, host, port);
            var socket = try Socket.connect(addr, self.io);
            defer socket.close();
            try applyTimeouts(&socket, timeout_ms, write_timeout_ms);
            return self.executeOnNewTlsToWriter(&socket, host, req, writer, progress_cb, progress_ctx);
        }

        if (self.config.keep_alive) {
            var conn = try self.pool.getConnection(host, port);
            var ok = false;
            defer {
                if (ok) self.pool.releaseConnection(conn) else self.pool.evictConnection(conn);
            }

            try applyTimeouts(&conn.socket, timeout_ms, write_timeout_ms);
            return self.executeOnSocketToWriter(&conn.socket, req, writer, progress_cb, progress_ctx, &ok);
        }

        const addr = try resolveAddress(self.io, host, port);
        var socket = try Socket.connect(addr, self.io);
        defer socket.close();
        try applyTimeouts(&socket, timeout_ms, write_timeout_ms);
        return self.executeOnSocketToWriter(&socket, req, writer, progress_cb, progress_ctx, null);
    }

    /// Sends request and reads response over a plain TCP socket.
    /// If `keep_alive_out` is non-null, sets it based on the response's keep-alive header.
    fn executeOnSocket(self: *Self, socket: *Socket, req: *Request, keep_alive_out: ?*bool) !Response {
        const bytes = try serializeToSlice(self.allocator, req);
        defer self.allocator.free(bytes);
        try socket.sendAll(bytes);
        var res = try self.readResponse(socket, req.method);
        if (keep_alive_out) |out| {
            out.* = res.headers.isKeepAlive(.HTTP_1_1);
        }
        return res;
    }

    fn executeOnSocketToWriter(
        self: *Self,
        socket: *Socket,
        req: *Request,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
        keep_alive_out: ?*bool,
    ) !Response {
        const bytes = try serializeToSlice(self.allocator, req);
        defer self.allocator.free(bytes);
        try socket.sendAll(bytes);
        var res = try self.readResponseToWriter(socket, req.method, writer, progress_cb, progress_ctx);
        if (keep_alive_out) |out| {
            out.* = res.headers.isKeepAlive(.HTTP_1_1);
        }
        return res;
    }

    /// Sends request and reads response over an established TLS session.
    /// If `keep_alive_out` is non-null, sets it based on the response's keep-alive header.
    fn executeOnTls(self: *Self, session: *TlsSession, req: *Request, keep_alive_out: ?*bool) !Response {
        const bytes = try serializeToSlice(self.allocator, req);
        defer self.allocator.free(bytes);
        const w = try session.getWriter();
        try w.writeAll(bytes);
        try session.flush();
        var res = try self.readResponse(session, req.method);
        if (keep_alive_out) |out| {
            out.* = res.headers.isKeepAlive(.HTTP_1_1);
        }
        return res;
    }

    fn executeOnTlsToWriter(
        self: *Self,
        session: *TlsSession,
        req: *Request,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
        keep_alive_out: ?*bool,
    ) !Response {
        const bytes = try serializeToSlice(self.allocator, req);
        defer self.allocator.free(bytes);
        const w = try session.getWriter();
        try w.writeAll(bytes);
        try session.flush();
        var res = try self.readResponseToWriter(session, req.method, writer, progress_cb, progress_ctx);
        if (keep_alive_out) |out| {
            out.* = res.headers.isKeepAlive(.HTTP_1_1);
        }
        return res;
    }

    /// Creates a new TLS session on a socket and executes a request.
    fn executeOnNewTls(self: *Self, socket: *Socket, host: []const u8, req: *Request) !Response {
        const tls_cfg = if (self.config.verify_ssl) TlsConfig.init(self.allocator) else TlsConfig.insecure(self.allocator);
        var session = TlsSession.init(tls_cfg, self.io);
        defer session.deinit();
        session.attachSocket(socket);
        try session.handshake(host);
        return self.executeOnTls(&session, req, null);
    }

    fn executeOnNewTlsToWriter(
        self: *Self,
        socket: *Socket,
        host: []const u8,
        req: *Request,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !Response {
        const tls_cfg = if (self.config.verify_ssl) TlsConfig.init(self.allocator) else TlsConfig.insecure(self.allocator);
        var session = TlsSession.init(tls_cfg, self.io);
        defer session.deinit();
        session.attachSocket(socket);
        try session.handshake(host);
        return self.executeOnTlsToWriter(&session, req, writer, progress_cb, progress_ctx, null);
    }

    /// Gets or creates a pooled HTTP/2 connection for the given host:port.
    /// The connection preface and SETTINGS exchange happen once on creation;
    /// subsequent requests reuse the same TCP/TLS + H2Connection state.
    ///
    /// The mutex is held only for map lookups and insertion — not across
    /// blocking I/O (DNS, TCP connect, TLS handshake, SETTINGS exchange).
    /// If two fibers race to create the same host:port, the loser discards
    /// its connection and uses the winner's.
    fn getOrCreateH2Conn(self: *Self, host: []const u8, port: u16, is_tls: bool) !*H2PoolEntry {
        var key_buf: [280]u8 = undefined;
        const key = std.fmt.bufPrint(&key_buf, "{s}:{d}", .{ host, port }) catch return error.InvalidUri;

        // --- Phase 1: Check map under lock ---
        {
            self.h2_mutex.lockUncancelable(self.io);
            defer self.h2_mutex.unlock(self.io);

            if (self.h2_conns.get(key)) |entry| {
                if (!entry.broken and !entry.h2.goaway_received) return entry;
                if (entry.h2.goaway_received) entry.broken = true;
            }
        }

        // --- Phase 2: Create connection without lock (blocking I/O) ---
        const entry = try self.allocator.create(H2PoolEntry);
        entry.recv_running = false;
        errdefer self.allocator.destroy(entry);

        const addr = try resolveAddress(self.io, host, port);
        entry.socket = try Socket.connect(addr, self.io);
        // Guard socket close only until fibers take ownership (recv_running).
        // After fibers start, the errdefer at line ~521 handles shutdown.
        errdefer if (!entry.recv_running) entry.socket.close();
        entry.socket.setNoDelay(true) catch {};
        entry.socket.setKeepAlive(true) catch {};

        entry.is_tls = is_tls;
        entry.broken = false;

        if (is_tls) {
            var tls_cfg = if (self.config.verify_ssl) TlsConfig.init(self.allocator) else TlsConfig.insecure(self.allocator);
            tls_cfg.alpn_protocols = &.{ "h2", "http/1.1" };
            entry.session = TlsSession.init(tls_cfg, self.io);
            errdefer entry.session.deinit();
            entry.session.attachSocket(&entry.socket);
            try entry.session.handshake(host);
        } else {
            // Initialize a dummy session (deinit is safe on un-handshaked session).
            entry.session = TlsSession.init(TlsConfig.init(self.allocator), self.io);
        }
        // Session errdefer at function scope — the block-scoped errdefs above
        // only cover the TLS handshake. This one covers everything after.
        errdefer entry.session.deinit();

        entry.h2 = H2Connection.initClient(self.allocator, self.io);
        entry.h2.max_stream_data_size = self.config.max_response_size;
        entry.io = self.io;
        entry.last_frame_ts = common.milliTimestamp(self.io);
        entry.read_idle_timeout_ms = self.config.h2_read_idle_timeout_ms;
        entry.ping_timeout_ms = self.config.h2_ping_timeout_ms;
        errdefer entry.h2.deinit();

        // Perform h2 handshake: preface + SETTINGS exchange.
        if (is_tls) {
            const w = try entry.session.getWriter();
            try entry.h2.sendClientPreface(w);
            const r = try entry.session.getReader();
            try self.exchangeH2Settings(&entry.h2, r, w);
        } else {
            try entry.h2.sendClientPreface(&entry.socket);
            try self.exchangeH2Settings(&entry.h2, &entry.socket, &entry.socket);
        }

        // Spawn background fibers before taking the lock for insertion.
        if (entry.recv_group.concurrent(self.io, h2RecvLoopFiber, .{entry})) {
            entry.recv_running = true;
            if (self.config.h2_read_idle_timeout_ms > 0) {
                entry.ping_group.concurrent(self.io, h2PingTimerFiber, .{entry}) catch {};
            }
        } else |_| {}

        errdefer if (entry.recv_running) {
            entry.socket.close();
            entry.recv_group.await(self.io) catch {};
            entry.ping_group.await(self.io) catch {};
        };

        // --- Phase 3: Re-check and insert under lock ---
        // Collect entries to tear down outside the lock (await yields the
        // fiber, so we must not hold h2_mutex during teardown).
        var race_winner: ?*H2PoolEntry = null;
        var stale_removed: ?std.StringHashMapUnmanaged(*H2PoolEntry).KV = null;
        // Use defer so stale entry is cleaned up even if allocPrint/put fail.
        defer if (stale_removed) |removed| self.destroyH2EntryKeyed(removed);

        {
            self.h2_mutex.lockUncancelable(self.io);
            defer self.h2_mutex.unlock(self.io);

            // Another fiber may have raced us and inserted a connection.
            if (self.h2_conns.get(key)) |existing| {
                if (!existing.broken and !existing.h2.goaway_received) {
                    race_winner = existing;
                }
            }

            if (race_winner == null) {
                // Remove stale/broken entry if present.
                stale_removed = self.h2_conns.fetchRemove(key);

                const owned_key = try std.fmt.allocPrint(self.allocator, "{s}:{d}", .{ host, port });
                errdefer self.allocator.free(owned_key);
                try self.h2_conns.put(self.allocator, owned_key, entry);
            }
        }

        if (race_winner) |existing| {
            // Loser: tear down our connection, use the winner's.
            self.destroyH2Entry(entry);
            return existing;
        }

        return entry;
    }

    /// Tears down an H2PoolEntry that was never inserted into h2_conns.
    fn destroyH2Entry(self: *Self, entry: *H2PoolEntry) void {
        if (entry.recv_running) {
            entry.socket.close();
            entry.recv_group.await(self.io) catch {};
            entry.ping_group.await(self.io) catch {};
        } else {
            entry.socket.close();
        }
        entry.h2.deinit();
        entry.session.deinit();
        self.allocator.destroy(entry);
    }

    /// Tears down an H2PoolEntry that was removed from h2_conns via fetchRemove.
    fn destroyH2EntryKeyed(self: *Self, removed: std.StringHashMapUnmanaged(*H2PoolEntry).KV) void {
        const e = removed.value;
        e.socket.close();
        e.recv_group.await(self.io) catch {};
        e.ping_group.await(self.io) catch {};
        e.h2.deinit();
        e.session.deinit();
        self.allocator.destroy(e);
        self.allocator.free(removed.key);
    }

    /// Reads frames until the server's initial SETTINGS has been received and ACKed.
    /// Gives up after 64 non-SETTINGS frames to prevent hangs against misbehaving peers.
    fn exchangeH2Settings(self: *Self, h2: *H2Connection, reader: anytype, writer: anytype) !void {
        var settings_received = false;
        var frame_count: u32 = 0;
        while (!settings_received) {
            if (frame_count >= 64) return error.ProtocolError;
            var frame = try h2.readFrame(reader);
            defer frame.deinit(self.allocator);
            frame_count += 1;
            if (frame.header.frame_type == .settings and frame.header.flags & H2Connection.FLAG_ACK == 0) {
                try h2.handleSettings(&frame, writer);
                settings_received = true;
            } else {
                _ = try h2.dispatchFrame(&frame, writer);
            }
        }
    }

    /// Background receive-loop fiber for multiplexed H2 connections.
    /// Continuously pumps frames until GOAWAY or connection error.
    /// Updates `last_frame_ts` on each frame so the ping timer fiber
    /// can detect idle connections.
    fn h2RecvLoopFiber(entry: *H2PoolEntry) Io.Cancelable!void {
        if (entry.is_tls) {
            const r = entry.session.getReader() catch {
                entry.broken = true;
                return;
            };
            const w = entry.session.getWriter() catch {
                entry.broken = true;
                return;
            };
            h2RecvLoop(entry, r, w);
        } else {
            h2RecvLoop(entry, &entry.socket, &entry.socket);
        }
        // h2RecvLoop's defer already sets entry.broken = true
    }

    fn h2RecvLoop(entry: *H2PoolEntry, reader: anytype, writer: anytype) void {
        // Mark broken BEFORE signaling streams so that retrying request fibers
        // (woken by signalAllStreams) see entry.broken=true and don't reuse
        // this dead connection.
        var last_err: anyerror = error.ConnectionClosed;
        defer entry.h2.signalAllStreams(last_err);
        defer {
            entry.broken = true;
        }
        while (!entry.h2.goaway_received) {
            _ = entry.h2.processOneFrameLocked(reader, writer) catch |err| switch (err) {
                error.ConnectionClosed => return,
                else => {
                    // Send GOAWAY so the server knows we're closing.
                    entry.h2.sendGoaway(writer, .protocol_error) catch {};
                    last_err = err;
                    return;
                },
            };
            entry.last_frame_ts = common.milliTimestamp(entry.io);
        }
    }

    /// PING health-check fiber. Runs alongside the receive loop.
    /// Sleeps for `read_idle_timeout_ms`, then checks if any frames were
    /// received since the last check. If not, sends a PING and waits for
    /// ACK within `ping_timeout_ms`. On timeout, closes the socket to
    /// tear down the connection (similar to Go's http2.Transport health check).
    fn h2PingTimerFiber(entry: *H2PoolEntry) Io.Cancelable!void {
        const idle_ms = entry.read_idle_timeout_ms;
        const ping_ms = entry.ping_timeout_ms;
        if (idle_ms == 0) return;

        while (!entry.broken) {
            // Sleep for the idle timeout period.
            entry.io.sleep(Io.Duration.fromMilliseconds(@intCast(idle_ms)), .awake) catch return;

            if (entry.broken) return;

            // Check if we received any frame during the sleep period.
            const now = common.milliTimestamp(entry.io);
            const since_last = now - entry.last_frame_ts;
            if (since_last < @as(i64, @intCast(idle_ms))) continue;

            // No frames received — send a PING to probe liveness.
            {
                entry.h2.write_mutex.lockUncancelable(entry.io);
                defer entry.h2.write_mutex.unlock(entry.io);
                // Reset inside mutex so a stale ACK from a prior cycle
                // (processed between reset and send) can't satisfy this wait.
                entry.h2.ping_ack_event.reset();
                if (entry.is_tls) {
                    if (entry.session.getWriter()) |w|
                        entry.h2.sendPing(w, .{ 0x68, 0x32, 0x70, 0x69, 0x6e, 0x67, 0x00, 0x00 }) catch {}
                    else |_| {}
                } else {
                    entry.h2.sendPing(&entry.socket, .{ 0x68, 0x32, 0x70, 0x69, 0x6e, 0x67, 0x00, 0x00 }) catch {};
                }
            }

            // Wait for PING ACK with timeout.
            const timeout: Io.Timeout = .{ .duration = .{
                .raw = Io.Duration.fromMilliseconds(@intCast(ping_ms)),
                .clock = .awake,
            } };
            entry.h2.ping_ack_event.waitTimeout(entry.io, timeout) catch {
                // Timeout or canceled — connection is dead.
                entry.broken = true;
                entry.socket.close();
                return;
            };
            // ACK received — connection is alive. Update timestamp.
            entry.last_frame_ts = common.milliTimestamp(entry.io);
        }
    }

    /// Executes a request on a pooled H2 connection. Creates a stream, sends
    /// the request, pumps frames until complete, and extracts the response.
    ///
    /// In multiplexed mode (recv_running=true), the background receive fiber
    /// pumps frames while this fiber waits on a per-stream event.
    /// In fallback mode, frames are pumped inline via awaitStreamComplete.
    fn executeH2OnPooled(self: *Self, entry: *H2PoolEntry, req: *Request) !Response {
        const h2 = &entry.h2;
        if (h2.goaway_received) {
            entry.broken = true;
            return error.ConnectionClosed;
        }
        const stream = try h2.stream_manager.createStream();
        const stream_id = stream.id;
        errdefer h2.stream_manager.removeStream(stream_id);

        // Build request pseudo-headers.
        const method_str = if (req.method == .CUSTOM)
            req.custom_method orelse "CUSTOM"
        else
            req.method.toString();
        const scheme = req.uri.scheme orelse "http";
        const authority = req.uri.host orelse return error.InvalidUri;
        var path_buf = std.ArrayListUnmanaged(u8).empty;
        defer path_buf.deinit(self.allocator);
        const alw = arrayListWriter(&path_buf, self.allocator);
        try alw.writeAll(req.uri.path);
        if (req.uri.query) |q| {
            try alw.writeAll("?");
            try alw.writeAll(q);
        }
        const path = path_buf.items;

        var extra = std.ArrayListUnmanaged(hpack.HeaderEntry).empty;
        defer extra.deinit(self.allocator);
        for (req.headers.entries.items) |he| {
            if (std.ascii.eqlIgnoreCase(he.name, "host")) continue;
            if (std.ascii.eqlIgnoreCase(he.name, "connection")) continue;
            if (std.ascii.eqlIgnoreCase(he.name, "transfer-encoding")) continue;
            if (std.ascii.eqlIgnoreCase(he.name, "upgrade")) continue;
            try extra.append(self.allocator, .{ .name = he.name, .value = he.value });
        }

        const h2_headers = try H2Connection.buildRequestHeaders(
            method_str, path, scheme, authority, extra.items, self.allocator,
        );
        defer self.allocator.free(h2_headers);

        const has_body = req.body != null;

        if (entry.recv_running) {
            // Multiplexed mode: background fiber pumps frames; we wait on a
            // per-stream completion semaphore that the receive loop posts.
            // Heap-allocated for pointer stability: the receive loop may post
            // after this function returns on a write error path, so a stack-
            // allocated semaphore would be use-after-free.
            const sem = try self.allocator.create(Io.Semaphore);
            sem.* = .{ .permits = 0 };
            stream.completion_sem = sem;
            // Re-fetch the stream pointer for cleanup since the backing
            // map may have rehashed while we were blocked on I/O.
            defer {
                if (h2.stream_manager.getStream(stream_id)) |s| {
                    s.completion_sem = null;
                }
                self.allocator.destroy(sem);
            }

            // Serialize frame writes via the connection's write mutex.
            {
                h2.write_mutex.lockUncancelable(self.io);
                defer h2.write_mutex.unlock(self.io);
                if (entry.is_tls) {
                    const w = try entry.session.getWriter();
                    try h2.sendHeaders(w, stream_id, h2_headers, !has_body);
                    if (req.body) |body| try h2.writeDataBlocking(w, stream_id, body, true);
                } else {
                    try h2.sendHeaders(&entry.socket, stream_id, h2_headers, !has_body);
                    if (req.body) |body| try h2.writeDataBlocking(&entry.socket, stream_id, body, true);
                }
            }

            // Wait for the receive loop to deliver the response.
            sem.waitUncancelable(self.io);
        } else {
            // Fallback mode: pump frames inline (no fiber support).
            if (entry.is_tls) {
                const r = try entry.session.getReader();
                const w = try entry.session.getWriter();
                try h2.sendHeaders(w, stream_id, h2_headers, !has_body);
                if (req.body) |body| try h2.writeData(w, stream_id, body, true);
                try h2.awaitStreamComplete(r, w, stream_id);
            } else {
                try h2.sendHeaders(&entry.socket, stream_id, h2_headers, !has_body);
                if (req.body) |body| try h2.writeData(&entry.socket, stream_id, body, true);
                try h2.awaitStreamComplete(&entry.socket, &entry.socket, stream_id);
            }
        }

        // Extract response from mailbox.
        const s = h2.stream_manager.getStream(stream_id) orelse return error.InvalidResponse;
        defer h2.stream_manager.removeStream(stream_id);
        if (s.stream_error) |err| return err;

        // Headers were decoded in deliverToMailbox (receive loop) to avoid
        // concurrent HPACK decode races on the shared hpack_ctx.
        const decoded_headers = s.request_headers orelse return error.InvalidResponse;

        var status_code: ?u16 = null;
        var response_headers = Headers.init(self.allocator);
        errdefer response_headers.deinit();

        for (decoded_headers) |h| {
            if (mem.eql(u8, h.name, ":status")) {
                status_code = std.fmt.parseInt(u16, h.value, 10) catch return error.InvalidResponse;
            } else if (h.name.len > 0 and h.name[0] != ':') {
                try response_headers.append(h.name, h.value);
            }
        }

        const code = status_code orelse return error.InvalidResponse;
        var res = Response.init(self.allocator, code);
        res.version = .HTTP_2;
        res.headers.deinit();
        res.headers = response_headers;

        if (s.data_buf.items.len > 0) {
            if (s.data_buf.items.len > self.config.max_response_size) return error.ResponseTooLarge;
            res.body = try self.allocator.dupe(u8, s.data_buf.items);
            res.body_owned = true;
        }

        return res;
    }

    /// Incremental reader for an HTTP/2 response body.
    /// Reads DATA frame payloads as they arrive from the receive loop.
    pub const H2StreamReader = struct {
        stream: *Stream,
        io: Io,
        h2: *H2Connection,
        entry: *H2PoolEntry,
        data_event: *Io.Event,
        allocator: Allocator,
        read_timeout: Io.Timeout = .none,

        /// Reads up to `buf.len` bytes. Blocks until data is available.
        /// Returns 0 at EOF. Returns error.Timeout if no data arrives
        /// within the configured read_timeout.
        pub fn read(self: *H2StreamReader, buf: []u8) !usize {
            while (true) {
                const avail = self.stream.data_buf.items.len - self.stream.read_offset;
                if (avail > 0) {
                    const n = @min(avail, buf.len);
                    const start = self.stream.read_offset;
                    @memcpy(buf[0..n], self.stream.data_buf.items[start..][0..n]);
                    self.stream.read_offset += n;
                    if (self.stream.read_offset >= Stream.compact_threshold) {
                        self.stream.compactDataBuf();
                    }
                    return n;
                }
                if (self.stream.stream_error) |err| return err;
                if (self.stream.completed) return 0;

                // Reset event, re-check buffer (handles race with receive loop),
                // then wait with timeout.
                self.data_event.reset();
                const avail2 = self.stream.data_buf.items.len - self.stream.read_offset;
                if (avail2 > 0) continue;
                if (self.stream.stream_error) |err| return err;
                if (self.stream.completed) return 0;

                self.data_event.waitTimeout(self.io, self.read_timeout) catch |err| switch (err) {
                    error.Timeout => return error.Timeout,
                    error.Canceled => return error.Canceled,
                };
            }
        }

        /// Releases the stream. Sends RST_STREAM(CANCEL) if the stream
        /// hasn't completed, telling the server to stop sending DATA frames.
        pub fn close(self: *H2StreamReader) void {
            const stream_id = self.stream.id;
            const completed = self.stream.completed;
            self.stream.data_event = null;
            self.stream.completion_sem = null;

            if (!completed) {
                self.h2.write_mutex.lockUncancelable(self.io);
                defer self.h2.write_mutex.unlock(self.io);
                if (self.entry.is_tls) {
                    if (self.entry.session.getWriter()) |w|
                        self.h2.sendRstStream(w, stream_id, .cancel) catch {}
                    else |_| {}
                } else {
                    self.h2.sendRstStream(&self.entry.socket, stream_id, .cancel) catch {};
                }
            }

            self.h2.stream_manager.removeStream(stream_id);
            self.allocator.destroy(self.data_event);
        }
    };

    /// Response from a streaming H2 request. Contains response headers
    /// and an incremental reader for the response body.
    pub const H2StreamResponse = struct {
        status_code: u16,
        headers: Headers,
        reader: H2StreamReader,

        pub fn deinit(self: *H2StreamResponse) void {
            self.reader.close();
            self.headers.deinit();
        }
    };

    /// Sends an HTTP/2 request and returns response headers + an incremental
    /// body reader. The caller reads DATA frames as they arrive without
    /// waiting for END_STREAM. Requires multiplexed mode (recv_running=true).
    pub fn requestStream(self: *Self, req: *Request) !H2StreamResponse {
        const host = req.uri.host orelse return error.InvalidUri;
        const is_tls = req.uri.isTls();
        const port = req.uri.effectivePort();
        const entry = try self.getOrCreateH2Conn(host, port, is_tls);
        if (!entry.recv_running) return error.MultiplexingRequired;

        const h2 = &entry.h2;
        if (h2.goaway_received) {
            entry.broken = true;
            return error.ConnectionClosed;
        }
        const stream = try h2.stream_manager.createStream();
        const stream_id = stream.id;
        errdefer h2.stream_manager.removeStream(stream_id);

        // Heap-allocate event for pointer stability and timed waits.
        const data_event = try self.allocator.create(Io.Event);
        data_event.* = .unset;
        stream.data_event = data_event;
        errdefer {
            stream.data_event = null;
            self.allocator.destroy(data_event);
        }

        // Build request pseudo-headers.
        const method_str = if (req.method == .CUSTOM)
            req.custom_method orelse "CUSTOM"
        else
            req.method.toString();
        const scheme = req.uri.scheme orelse "http";
        const authority = host;
        var path_buf = std.ArrayListUnmanaged(u8).empty;
        defer path_buf.deinit(self.allocator);
        const alw = arrayListWriter(&path_buf, self.allocator);
        try alw.writeAll(req.uri.path);
        if (req.uri.query) |q| {
            try alw.writeAll("?");
            try alw.writeAll(q);
        }
        const path = path_buf.items;

        var extra = std.ArrayListUnmanaged(hpack.HeaderEntry).empty;
        defer extra.deinit(self.allocator);
        for (req.headers.entries.items) |he| {
            if (std.ascii.eqlIgnoreCase(he.name, "host")) continue;
            if (std.ascii.eqlIgnoreCase(he.name, "connection")) continue;
            if (std.ascii.eqlIgnoreCase(he.name, "transfer-encoding")) continue;
            if (std.ascii.eqlIgnoreCase(he.name, "upgrade")) continue;
            try extra.append(self.allocator, .{ .name = he.name, .value = he.value });
        }

        const h2_headers = try H2Connection.buildRequestHeaders(
            method_str, path, scheme, authority, extra.items, self.allocator,
        );
        defer self.allocator.free(h2_headers);

        const has_body = req.body != null;

        // Send request frames under write mutex.
        {
            h2.write_mutex.lockUncancelable(self.io);
            defer h2.write_mutex.unlock(self.io);
            if (entry.is_tls) {
                const w = try entry.session.getWriter();
                try h2.sendHeaders(w, stream_id, h2_headers, !has_body);
                if (req.body) |body| try h2.writeDataBlocking(w, stream_id, body, true);
            } else {
                try h2.sendHeaders(&entry.socket, stream_id, h2_headers, !has_body);
                if (req.body) |body| try h2.writeDataBlocking(&entry.socket, stream_id, body, true);
            }
        }

        // Wait for HEADERS response (not END_STREAM) with timeout.
        const header_timeout: Io.Timeout = if (self.config.timeouts.read_ms > 0)
            .{ .duration = .{
                .raw = Io.Duration.fromMilliseconds(@intCast(self.config.timeouts.read_ms)),
                .clock = .awake,
            } }
        else
            .none;
        while (!stream.got_headers and !stream.completed) {
            data_event.reset();
            if (stream.got_headers or stream.completed) break;
            data_event.waitTimeout(self.io, header_timeout) catch |err| switch (err) {
                error.Timeout => return error.Timeout,
                error.Canceled => return error.Canceled,
            };
        }
        if (stream.stream_error) |err| return err;

        // Headers were decoded in deliverToMailbox (receive loop) to avoid
        // concurrent HPACK decode races on the shared hpack_ctx.
        const decoded_headers = stream.request_headers orelse return error.InvalidResponse;

        var status_code: ?u16 = null;
        var response_headers = Headers.init(self.allocator);
        errdefer response_headers.deinit();

        for (decoded_headers) |h| {
            if (mem.eql(u8, h.name, ":status")) {
                status_code = std.fmt.parseInt(u16, h.value, 10) catch
                    return error.InvalidResponse;
            } else if (h.name.len > 0 and h.name[0] != ':') {
                try response_headers.append(h.name, h.value);
            }
        }

        return .{
            .status_code = status_code orelse return error.InvalidResponse,
            .headers = response_headers,
            .reader = .{
                .stream = stream,
                .io = self.io,
                .h2 = h2,
                .entry = entry,
                .data_event = data_event,
                .allocator = self.allocator,
            },
        };
    }

    /// Unified response reader parameterized on the read source.
    /// `ReadSource` is either `*Socket` (TCP) or `*TlsSession` (HTTPS).
    ///
    /// Streaming pipeline: parse headers only → build Io.Reader chain
    /// (leftover → socket/TLS → content-length/chunked → decompress) → read into output.
    /// Only one copy of the body is ever in memory at a time.
    fn readResponse(self: *Self, source: anytype, req_method: types.Method) !Response {
        var parser = Parser.initResponse(self.allocator);
        defer parser.deinit();
        parser.max_body_size = self.config.max_response_size;
        parser.max_headers = self.config.max_response_headers;
        parser.headers_only = true;

        var buf: [16 * 1024]u8 = undefined;
        var total_read: usize = 0;
        var leftover: usize = 0;
        var informational_count: u8 = 0;

        while (true) {
            while (!parser.isComplete()) {
                if (leftover > 0) {
                    const consumed = try parser.feed(buf[0..leftover]);
                    if (consumed < leftover) {
                        std.mem.copyForwards(u8, buf[0 .. leftover - consumed], buf[consumed..leftover]);
                    }
                    leftover -= consumed;
                    continue;
                }

                if (leftover >= buf.len) return error.InvalidResponse;
                const n = recvFrom(source, buf[leftover..]) catch |err| {
                    if (@TypeOf(source) == *TlsSession) {
                        if (err == error.ReadFailed) continue;
                    }
                    return err;
                };
                if (n == 0) break;
                total_read += n;
                if (total_read > self.config.max_response_size) return error.ResponseTooLarge;
                const total = leftover + n;
                const consumed = try parser.feed(buf[0..total]);
                leftover = total - consumed;
                if (consumed > 0 and leftover > 0) {
                    std.mem.copyForwards(u8, buf[0..leftover], buf[consumed..total]);
                }
            }

            parser.finishEof();
            if (!parser.isComplete()) return error.InvalidResponse;

            if (parser.status_code) |code| {
                if (code >= 100 and code < 200) {
                    informational_count += 1;
                    if (informational_count > 20) return error.TooManyInformationalResponses;
                    parser.reset();
                    parser.headers_only = true;
                    total_read = 0;
                    continue;
                }
            }
            break;
        }

        return self.buildStreamingResponse(&parser, source, buf[0..leftover], req_method);
    }

    fn readResponseToWriter(
        self: *Self,
        source: anytype,
        req_method: types.Method,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !Response {
        var parser = Parser.initResponse(self.allocator);
        defer parser.deinit();
        parser.max_body_size = self.config.max_response_size;
        parser.max_headers = self.config.max_response_headers;
        parser.headers_only = true;

        var buf: [16 * 1024]u8 = undefined;
        var total_read: usize = 0;
        var leftover: usize = 0;
        var informational_count: u8 = 0;

        while (true) {
            while (!parser.isComplete()) {
                if (leftover > 0) {
                    const consumed = try parser.feed(buf[0..leftover]);
                    if (consumed < leftover) {
                        std.mem.copyForwards(u8, buf[0 .. leftover - consumed], buf[consumed..leftover]);
                    }
                    leftover -= consumed;
                    continue;
                }

                if (leftover >= buf.len) return error.InvalidResponse;
                const n = recvFrom(source, buf[leftover..]) catch |err| {
                    if (@TypeOf(source) == *TlsSession) {
                        if (err == error.ReadFailed) continue;
                    }
                    return err;
                };
                if (n == 0) break;
                total_read += n;
                if (total_read > self.config.max_response_size) return error.ResponseTooLarge;
                const total = leftover + n;
                const consumed = try parser.feed(buf[0..total]);
                leftover = total - consumed;
                if (consumed > 0 and leftover > 0) {
                    std.mem.copyForwards(u8, buf[0..leftover], buf[consumed..total]);
                }
            }

            parser.finishEof();
            if (!parser.isComplete()) return error.InvalidResponse;

            if (parser.status_code) |code| {
                if (code >= 100 and code < 200) {
                    informational_count += 1;
                    if (informational_count > 20) return error.TooManyInformationalResponses;
                    parser.reset();
                    parser.headers_only = true;
                    total_read = 0;
                    continue;
                }
            }
            break;
        }

        return self.writeStreamingResponse(&parser, source, buf[0..leftover], req_method, writer, progress_cb, progress_ctx);
    }

    /// Read bytes from either a Socket or a TlsSession into `buf`.
    fn recvFrom(source: anytype, buf: []u8) !usize {
        const Source = @TypeOf(source);
        if (Source == *Socket) {
            return source.recv(buf);
        } else if (Source == *TlsSession) {
            var empty_read_policy = EmptyReadPolicy.init(TlsSessionIoReader.max_empty_reads);
            while (true) {
                const n = source.read(buf) catch return error.ReadFailed;
                switch (empty_read_policy.observe(n)) {
                    .done => return n,
                    .retry => sleepAfterEmptyRead(source.io),
                    .eof => return 0,
                }
            }
        } else {
            @compileError("recvFrom: unsupported source type " ++ @typeName(Source));
        }
    }

    /// Wraps `source` (Socket or TlsSession) into a uniform `Io.Reader` for the reader chain.
    fn sourceToIoReader(source: anytype, socket_reader: *SocketIoReader, tls_reader: *TlsSessionIoReader, io_buf: []u8) !*Io.Reader {
        const Source = @TypeOf(source);
        if (Source == *Socket) {
            socket_reader.* = SocketIoReader.init(source, io_buf);
            return &socket_reader.reader_iface;
        } else if (Source == *TlsSession) {
            tls_reader.* = TlsSessionIoReader.init(source, io_buf.len, io_buf);
            return &tls_reader.reader_iface;
        } else {
            @compileError("sourceToIoReader: unsupported source type " ++ @typeName(Source));
        }
    }

    fn readSomeOnce(reader: *Io.Reader, buf: []u8) !usize {
        var iov = [_][]u8{buf};
        return reader.readVec(&iov);
    }

    /// Builds a Response by streaming the body through an Io.Reader chain.
    /// After headers are parsed, the chain is: leftover bytes → network → framing → decompress → output.
    fn buildStreamingResponse(self: *Self, parser: *Parser, source: anytype, leftover: []const u8, req_method: types.Method) !Response {
        const code = parser.status_code orelse return error.InvalidResponse;
        var res = Response.init(parser.allocator, code);
        errdefer res.deinit();
        // Move headers ownership from parser to response.
        res.headers.deinit();
        res.headers = parser.headers;
        parser.headers = Headers.init(parser.allocator);

        // RFC 7230 §3.3: Responses to HEAD and 1xx/204/304 status codes
        // MUST NOT contain a message body regardless of headers.
        const no_body_status = (code >= 100 and code < 200) or code == 204 or code == 304;
        const has_body = !no_body_status and req_method != .HEAD and
            (parser.chunked or (parser.content_length orelse 1) > 0);
        if (!has_body) return res;

        // Detect Content-Encoding for transparent decompression.
        const container: ?flate.Container = if (res.headers.get(HeaderName.CONTENT_ENCODING)) |raw_enc| blk: {
            const enc = std.mem.trim(u8, raw_enc, " \t");
            if (std.ascii.eqlIgnoreCase(enc, "gzip")) break :blk .gzip;
            if (std.ascii.eqlIgnoreCase(enc, "deflate")) break :blk .raw;
            break :blk null;
        } else null;

        // --- Build the Io.Reader chain (all stack-allocated) ---

        var source_io_buf: [8192]u8 = undefined;
        var socket_reader: SocketIoReader = undefined;
        var tls_reader: TlsSessionIoReader = undefined;
        const raw_reader = try sourceToIoReader(source, &socket_reader, &tls_reader, &source_io_buf);

        // Layer 1: Prepend any leftover bytes from header parsing.
        var prefix_buf: [8192]u8 = undefined;
        var prefixed = PrefixedReader.init(leftover, raw_reader, &prefix_buf);
        const body_source: *Io.Reader = if (leftover.len > 0)
            &prefixed.reader_iface
        else
            raw_reader;

        // Layer 2: Apply transfer framing (Content-Length limit or chunked decode).
        var cl_buf: [8192]u8 = undefined;
        var cl_reader: ContentLengthReader = undefined;
        var chunked_buf: [8192]u8 = undefined;
        var chunked_reader: ChunkedBodyReader = undefined;

        const framed_reader: *Io.Reader = if (parser.chunked) blk: {
            chunked_reader = ChunkedBodyReader.init(body_source, &chunked_buf);
            break :blk &chunked_reader.reader_iface;
        } else if (parser.content_length) |len| blk: {
            if (len > std.math.maxInt(usize)) return error.ResponseTooLarge;
            cl_reader = ContentLengthReader.init(body_source, @intCast(len), &cl_buf);
            break :blk &cl_reader.reader_iface;
        } else blk: {
            // No Content-Length, not chunked — body delimited by connection close.
            break :blk body_source;
        };

        const close_delimited_body = !parser.chunked and parser.content_length == null;

        // --- Read from the chain into a single output buffer ---
        var result = std.ArrayListUnmanaged(u8).empty;
        errdefer result.deinit(self.allocator);

        // Pre-allocate hint: use content_length if known (and not compressed).
        if (container == null) {
            if (parser.content_length) |len| {
                if (len <= std.math.maxInt(usize)) {
                    try result.ensureTotalCapacity(self.allocator, @intCast(len));
                }
            }
        }

        const max_size = self.config.max_response_size;
        var read_buf: [16 * 1024]u8 = undefined;

        if (container) |ctr| {
            var compressed = std.ArrayListUnmanaged(u8).empty;
            defer compressed.deinit(self.allocator);

            while (true) {
                const n = readSomeOnce(framed_reader, &read_buf) catch |err| {
                    if (err == error.EndOfStream) break;
                    if (err == error.ReadFailed and close_delimited_body) break;
                    return error.InvalidResponse;
                };
                if (n == 0) break;
                if (compressed.items.len + n > max_size) return error.ResponseTooLarge;
                try compressed.appendSlice(self.allocator, read_buf[0..n]);
            }

            var compressed_reader: Io.Reader = .fixed(compressed.items);
            var decompress_window: [flate.max_window_len]u8 = undefined;
            var decompressor = flate.Decompress.init(&compressed_reader, ctr, &decompress_window);

            while (true) {
                const n = decompressor.reader.readSliceShort(&read_buf) catch |err| {
                    if (err == error.EndOfStream) break;
                    return error.DecompressionFailed;
                };
                if (n == 0) break;
                if (result.items.len + n > max_size) return error.ResponseTooLarge;
                try result.appendSlice(self.allocator, read_buf[0..n]);
            }
        } else {
            while (true) {
                const n = readSomeOnce(framed_reader, &read_buf) catch |err| {
                    if (err == error.EndOfStream) break;
                    if (err == error.ReadFailed and close_delimited_body) break;
                    return error.InvalidResponse;
                };
                if (n == 0) break;
                if (result.items.len + n > max_size) return error.ResponseTooLarge;
                try result.appendSlice(self.allocator, read_buf[0..n]);
            }
        }

        if (result.items.len > 0) {
            res.body = try result.toOwnedSlice(self.allocator);
            res.body_owned = true;
        }

        // Remove encoding/length headers when decompression was applied.
        if (container != null) {
            _ = res.headers.remove(HeaderName.CONTENT_ENCODING);
            _ = res.headers.remove(HeaderName.CONTENT_LENGTH);
        }

        return res;
    }

    fn writeStreamingResponse(
        self: *Self,
        parser: *Parser,
        source: anytype,
        leftover: []const u8,
        req_method: types.Method,
        writer: anytype,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !Response {
        const code = parser.status_code orelse return error.InvalidResponse;
        var res = Response.init(parser.allocator, code);
        errdefer res.deinit();
        res.headers.deinit();
        res.headers = parser.headers;
        parser.headers = Headers.init(parser.allocator);

        const no_body_status = (code >= 100 and code < 200) or code == 204 or code == 304;
        const has_body = !no_body_status and req_method != .HEAD and
            (parser.chunked or (parser.content_length orelse 1) > 0);
        if (!has_body) return res;

        const container: ?flate.Container = if (res.headers.get(HeaderName.CONTENT_ENCODING)) |raw_enc| blk: {
            const enc = std.mem.trim(u8, raw_enc, " \t");
            if (std.ascii.eqlIgnoreCase(enc, "gzip")) break :blk .gzip;
            if (std.ascii.eqlIgnoreCase(enc, "deflate")) break :blk .raw;
            break :blk null;
        } else null;

        var source_io_buf: [8192]u8 = undefined;
        var socket_reader: SocketIoReader = undefined;
        var tls_reader: TlsSessionIoReader = undefined;
        const raw_reader = try sourceToIoReader(source, &socket_reader, &tls_reader, &source_io_buf);

        var prefix_buf: [8192]u8 = undefined;
        var prefixed = PrefixedReader.init(leftover, raw_reader, &prefix_buf);
        const body_source: *Io.Reader = if (leftover.len > 0)
            &prefixed.reader_iface
        else
            raw_reader;

        var cl_buf: [8192]u8 = undefined;
        var cl_reader: ContentLengthReader = undefined;
        var chunked_buf: [8192]u8 = undefined;
        var chunked_reader: ChunkedBodyReader = undefined;

        const framed_reader: *Io.Reader = if (parser.chunked) blk: {
            chunked_reader = ChunkedBodyReader.init(body_source, &chunked_buf);
            break :blk &chunked_reader.reader_iface;
        } else if (parser.content_length) |len| blk: {
            if (len > std.math.maxInt(usize)) return error.ResponseTooLarge;
            cl_reader = ContentLengthReader.init(body_source, @intCast(len), &cl_buf);
            break :blk &cl_reader.reader_iface;
        } else blk: {
            break :blk body_source;
        };

        const close_delimited_body = !parser.chunked and parser.content_length == null;
        const is_redirect = code >= 300 and code < 400;
        const progress_total = if (container == null and !parser.chunked) parser.content_length else null;
        var written_total: u64 = 0;

        var read_buf: [16 * 1024]u8 = undefined;

        if (is_redirect) {
            while (true) {
                const n = readSomeOnce(framed_reader, &read_buf) catch |err| {
                    if (err == error.EndOfStream) break;
                    if (err == error.ReadFailed and close_delimited_body) break;
                    return error.InvalidResponse;
                };
                if (n == 0) break;
                written_total += n;
                if (written_total > self.config.max_response_size) return error.ResponseTooLarge;
            }
        } else if (container) |ctr| {
            var decompress_window: [flate.max_window_len]u8 = undefined;
            var decompressor = flate.Decompress.init(framed_reader, ctr, &decompress_window);

            while (true) {
                const n = decompressor.reader.readSliceShort(&read_buf) catch |err| {
                    if (err == error.EndOfStream) break;
                    if (err == error.ReadFailed and close_delimited_body) break;
                    return error.DecompressionFailed;
                };
                if (n == 0) break;
                if (written_total + n > self.config.max_response_size) return error.ResponseTooLarge;
                try writer.writeAll(read_buf[0..n]);
                written_total += n;
                if (progress_cb) |cb| cb(.{ .bytes_written = written_total, .total_bytes = null }, progress_ctx);
            }
        } else {
            while (true) {
                const n = readSomeOnce(framed_reader, &read_buf) catch |err| {
                    if (err == error.EndOfStream) break;
                    if (err == error.ReadFailed and close_delimited_body) break;
                    return error.InvalidResponse;
                };
                if (n == 0) break;
                try writer.writeAll(read_buf[0..n]);
                written_total += n;
                if (progress_cb) |cb| cb(.{ .bytes_written = written_total, .total_bytes = progress_total }, progress_ctx);
            }
        }

        if (container != null) {
            _ = res.headers.remove(HeaderName.CONTENT_ENCODING);
            _ = res.headers.remove(HeaderName.CONTENT_LENGTH);
        }

        return res;
    }

    fn writeBufferedBody(res: *Response, writer: anytype, progress_cb: ?WriterProgressCallback, progress_ctx: ?*anyopaque) !void {
        if (res.body) |body| {
            try writer.writeAll(body);
            if (progress_cb) |cb| cb(.{ .bytes_written = body.len, .total_bytes = res.contentLength() }, progress_ctx);
            if (res.body_owned) {
                res.allocator.free(body);
                res.body_owned = false;
            }
            res.body = null;
        }
    }

    fn readChunkedTlsBody(
        self: *Self,
        source: *TlsSession,
        leftover: []const u8,
        progress_cb: ?WriterProgressCallback,
        progress_ctx: ?*anyopaque,
    ) !?[]u8 {
        var framed = std.ArrayListUnmanaged(u8).empty;
        defer framed.deinit(self.allocator);
        var empty_read_policy = EmptyReadPolicy.init(TlsSessionIoReader.max_empty_reads);

        if (leftover.len > 0) {
            try framed.appendSlice(self.allocator, leftover);
            if (framed.items.len > self.config.max_response_size) return error.ResponseTooLarge;
            if (progress_cb) |cb| cb(.{ .bytes_written = framed.items.len, .total_bytes = null }, progress_ctx);
        }

        while (true) {
            if (findChunkedMessageEnd(framed.items)) |end| {
                return try self.allocator.dupe(u8, framed.items[0..end]);
            }

            var read_buf: [2048]u8 = undefined;
            const n = recvFrom(source, &read_buf) catch |err| {
                if (err == error.ReadFailed) continue;
                return error.InvalidResponse;
            };
            switch (empty_read_policy.observe(n)) {
                .done => {},
                .retry => {
                    sleepAfterEmptyRead(source.io);
                    continue;
                },
                .eof => return error.InvalidResponse,
            }
            if (framed.items.len + n > self.config.max_response_size) return error.ResponseTooLarge;
            try framed.appendSlice(self.allocator, read_buf[0..n]);
            if (progress_cb) |cb| cb(.{ .bytes_written = framed.items.len, .total_bytes = null }, progress_ctx);
        }
    }

    fn findChunkedMessageEnd(data: []const u8) ?usize {
        var pos: usize = 0;
        while (true) {
            const line_rel = std.mem.indexOf(u8, data[pos..], "\r\n") orelse return null;
            const line = data[pos .. pos + line_rel];
            pos += line_rel + 2;

            const hex_end = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
            const hex = std.mem.trim(u8, line[0..hex_end], " \t");
            const chunk_len = std.fmt.parseInt(usize, hex, 16) catch return null;

            if (chunk_len == 0) {
                while (true) {
                    const trailer_rel = std.mem.indexOf(u8, data[pos..], "\r\n") orelse return null;
                    if (trailer_rel == 0) return pos + 2;
                    pos += trailer_rel + 2;
                }
            }

            if (data.len < pos + chunk_len + 2) return null;
            if (!std.mem.eql(u8, data[pos + chunk_len .. pos + chunk_len + 2], "\r\n")) return null;
            pos += chunk_len + 2;
        }
    }

    fn decodeChunkedBody(allocator: Allocator, data: []const u8, max_size: usize) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(allocator);

        var pos: usize = 0;
        while (true) {
            const line_rel = std.mem.indexOf(u8, data[pos..], "\r\n") orelse return error.InvalidResponse;
            const line = data[pos .. pos + line_rel];
            pos += line_rel + 2;

            const hex_end = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
            const hex = std.mem.trim(u8, line[0..hex_end], " \t");
            const chunk_len = std.fmt.parseInt(usize, hex, 16) catch return error.InvalidResponse;

            if (chunk_len == 0) {
                while (true) {
                    const trailer_rel = std.mem.indexOf(u8, data[pos..], "\r\n") orelse return error.InvalidResponse;
                    pos += trailer_rel + 2;
                    if (trailer_rel == 0) return out.toOwnedSlice(allocator);
                }
            }

            if (data.len < pos + chunk_len + 2) return error.InvalidResponse;
            if (!std.mem.eql(u8, data[pos + chunk_len .. pos + chunk_len + 2], "\r\n")) return error.InvalidResponse;
            if (out.items.len + chunk_len > max_size) return error.ResponseTooLarge;
            try out.appendSlice(allocator, data[pos .. pos + chunk_len]);
            pos += chunk_len + 2;
        }
    }

    fn resolveRedirectUrl(self: *Self, base: Uri, location: []const u8) ![]u8 {
        // Absolute URL.
        if (mem.indexOf(u8, location, "://") != null) {
            // Only allow http/https schemes to prevent open redirects (e.g. file://, ftp://).
            // Use case-insensitive comparison per RFC 3986 §3.1 (schemes are case-insensitive).
            const has_http = location.len >= 7 and std.ascii.eqlIgnoreCase(location[0..7], "http://");
            const has_https = location.len >= 8 and std.ascii.eqlIgnoreCase(location[0..8], "https://");
            if (!has_http and !has_https) {
                return error.UnsafeRedirect;
            }
            return self.allocator.dupe(u8, location);
        }

        const scheme = base.scheme orelse "http";
        const host = base.host orelse return error.InvalidUri;
        const port = base.effectivePort();

        if (location.len > 0 and location[0] == '/') {
            return std.fmt.allocPrint(self.allocator, "{s}://{s}:{d}{s}", .{ scheme, host, port, location });
        }

        // Relative to current path.
        const base_path = base.path;
        const slash = mem.lastIndexOfScalar(u8, base_path, '/') orelse 0;
        const prefix = base_path[0 .. slash + 1];
        return std.fmt.allocPrint(self.allocator, "{s}://{s}:{d}{s}{s}", .{ scheme, host, port, prefix, location });
    }

    fn attachCookies(self: *Self, req: *Request) !void {
        self.cookie_mutex.lockUncancelable(self.io);
        defer self.cookie_mutex.unlock(self.io);
        if (self.cookies.count() == 0) return;

        var list = std.ArrayListUnmanaged(u8).empty;
        defer list.deinit(self.allocator);
        const writer = arrayListWriter(&list, self.allocator);

        var it = self.cookies.iterator();
        var first = true;
        while (it.next()) |entry| {
            if (!first) try writer.writeAll("; ");
            first = false;
            try writer.print("{s}={s}", .{ entry.key_ptr.*, entry.value_ptr.* });
        }

        if (list.items.len > 0) {
            try req.headers.set(HeaderName.COOKIE, list.items);
        }
    }

    fn storeCookies(self: *Self, res: *const Response) !void {
        // Collect Set-Cookie values outside the lock to minimize contention.
        for (res.headers.entries.items) |entry| {
            if (!std.ascii.eqlIgnoreCase(entry.name, HeaderName.SET_COOKIE)) continue;
            const pair = common.parseSetCookiePair(entry.value) orelse continue;
            self.cookie_mutex.lockUncancelable(self.io);
            defer self.cookie_mutex.unlock(self.io);
            try self.setCookieLocked(pair.name, pair.value);
        }
    }

    const containsCrLf = @import("../core/headers.zig").containsCrLf;

    /// Returns true if a cookie name or value contains characters that could
    /// enable header injection (CR, LF) or malform the Cookie header (;).
    fn isSafeCookieToken(s: []const u8) bool {
        if (containsCrLf(s)) return false;
        return std.mem.indexOfScalar(u8, s, ';') == null;
    }

    fn setCookieLocked(self: *Self, name: []const u8, value: []const u8) !void {
        // Reject cookies with characters that enable header injection (CRLF)
        // or malform the Cookie header (semicolon in value).
        if (!isSafeCookieToken(name) or !isSafeCookieToken(value)) return;

        const gop = try self.cookies.getOrPut(self.allocator, name);

        if (gop.found_existing) {
            // Allocate new value BEFORE freeing old to avoid dangling pointer on OOM.
            const new_value = try self.allocator.dupe(u8, value);
            self.allocator.free(gop.value_ptr.*);
            gop.value_ptr.* = new_value;
            return;
        }

        // New entry path.
        // count() includes the newly reserved slot, so >= max means at capacity.
        if (self.cookies.count() >= self.config.max_cookies) {
            // Undo the slot reservation.
            self.cookies.removeByPtr(gop.key_ptr);
            return;
        }
        gop.key_ptr.* = try self.allocator.dupe(u8, name);
        errdefer {
            // If the value dupe below fails, undo the partial insertion
            // so the map doesn't hold a key with an uninitialized value.
            self.allocator.free(gop.key_ptr.*);
            self.cookies.removeByPtr(gop.key_ptr);
        }

        gop.value_ptr.* = try self.allocator.dupe(u8, value);
    }

    /// Adds or replaces a cookie in the in-memory client cookie jar.
    pub fn setCookie(self: *Self, name: []const u8, value: []const u8) !void {
        self.cookie_mutex.lockUncancelable(self.io);
        defer self.cookie_mutex.unlock(self.io);
        return self.setCookieLocked(name, value);
    }

    /// Returns a cookie value from the in-memory cookie jar.
    pub fn getCookie(self: *Self, name: []const u8) ?[]const u8 {
        self.cookie_mutex.lockUncancelable(self.io);
        defer self.cookie_mutex.unlock(self.io);
        return self.cookies.get(name);
    }

    /// Removes a cookie from the in-memory cookie jar.
    pub fn removeCookie(self: *Self, name: []const u8) bool {
        self.cookie_mutex.lockUncancelable(self.io);
        defer self.cookie_mutex.unlock(self.io);
        if (self.cookies.fetchRemove(name)) |removed| {
            self.allocator.free(removed.key);
            self.allocator.free(removed.value);
            return true;
        }
        return false;
    }

    /// Clears all cookies from the in-memory cookie jar.
    pub fn clearCookies(self: *Self) void {
        self.cookie_mutex.lockUncancelable(self.io);
        defer self.cookie_mutex.unlock(self.io);
        var it = self.cookies.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.cookies.clearRetainingCapacity();
    }

    /// Returns true if a cookie with the given name exists in the jar.
    pub fn hasCookie(self: *Self, name: []const u8) bool {
        self.cookie_mutex.lockUncancelable(self.io);
        defer self.cookie_mutex.unlock(self.io);
        return self.cookies.contains(name);
    }

    /// Returns the number of cookies currently stored in the jar.
    pub fn cookieCount(self: *Self) usize {
        self.cookie_mutex.lockUncancelable(self.io);
        defer self.cookie_mutex.unlock(self.io);
        return self.cookies.count();
    }

    /// GET request convenience method.
    pub fn get(self: *Self, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.request(.GET, url, reqOpts);
    }

    /// POST request convenience method.
    pub fn post(self: *Self, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.request(.POST, url, reqOpts);
    }

    /// PUT request convenience method.
    pub fn put(self: *Self, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.request(.PUT, url, reqOpts);
    }

    /// DELETE request convenience method.
    pub fn delete(self: *Self, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.request(.DELETE, url, reqOpts);
    }

    /// PATCH request convenience method.
    pub fn patch(self: *Self, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.request(.PATCH, url, reqOpts);
    }

    /// HEAD request convenience method.
    pub fn head(self: *Self, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.request(.HEAD, url, reqOpts);
    }

    /// OPTIONS request convenience method.
    pub fn options(self: *Self, url: []const u8, reqOpts: RequestOptions) !Response {
        return self.request(.OPTIONS, url, reqOpts);
    }
};


test "Client initialization" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    try std.testing.expectEqualStrings(meta.default_user_agent, client.config.user_agent);
}

test "Client with config" {
    const allocator = std.testing.allocator;
    var client = Client.initWithConfig(allocator, std.testing.io, .{
        .base_url = "https://api.example.com",
        .user_agent = "TestClient/1.0",
    });
    defer client.deinit();

    try std.testing.expectEqualStrings("https://api.example.com", client.config.base_url.?);
}

test "Response parsing" {
    const allocator = std.testing.allocator;
    const data = "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 15\r\n\r\n{\"status\":\"ok\"}";

    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    _ = try parser.feed(data);
    try std.testing.expect(parser.isComplete());

    const code = parser.status_code orelse return error.InvalidResponse;
    try std.testing.expectEqual(@as(u16, 200), code);
    try std.testing.expectEqualStrings("application/json", parser.headers.get("Content-Type").?);
}

test "Client stores Set-Cookie headers" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    var response = Response.init(allocator, 200);
    defer response.deinit();

    try response.headers.append("Set-Cookie", "session=abc123; Path=/; HttpOnly");
    try response.headers.append("Set-Cookie", "theme=dark; Path=/");

    try client.storeCookies(&response);

    try std.testing.expectEqualStrings("abc123", client.cookies.get("session").?);
    try std.testing.expectEqualStrings("dark", client.cookies.get("theme").?);
}

test "Client attaches Cookie header from jar" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    try client.setCookie("session", "abc123");
    try client.setCookie("theme", "dark");

    var request = try Request.init(allocator, .GET, "https://example.com/");
    defer request.deinit();

    try client.attachCookies(&request);

    const cookie_header = request.headers.get("Cookie") orelse return error.TestUnexpectedResult;
    try std.testing.expect(mem.indexOf(u8, cookie_header, "session=abc123") != null);
    try std.testing.expect(mem.indexOf(u8, cookie_header, "theme=dark") != null);
}

test "Client cookie jar public API" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    try client.setCookie("session", "abc123");
    try std.testing.expectEqualStrings("abc123", client.getCookie("session").?);

    const removed = client.removeCookie("session");
    try std.testing.expect(removed);
    try std.testing.expect(client.getCookie("session") == null);

    try client.setCookie("theme", "dark");
    try client.setCookie("lang", "en");
    client.clearCookies();
    try std.testing.expectEqual(@as(usize, 0), client.cookies.count());
}

test "Client method convenience functions" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    // Compile-time check that convenience methods exist.
    const request_ptr: *const fn (*Client, types.Method, []const u8, RequestOptions) anyerror!Response = Client.request;
    const options_ptr: *const fn (*Client, []const u8, RequestOptions) anyerror!Response = Client.options;
    _ = request_ptr;
    _ = options_ptr;
}

test "Client hasCookie and cookieCount" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    try std.testing.expectEqual(@as(usize, 0), client.cookieCount());
    try std.testing.expect(!client.hasCookie("session"));

    try client.setCookie("session", "abc123");
    try std.testing.expectEqual(@as(usize, 1), client.cookieCount());
    try std.testing.expect(client.hasCookie("session"));
}

test "Client response size limit" {
    const config = ClientConfig{};
    try std.testing.expectEqual(@as(usize, 100 * 1024 * 1024), config.max_response_size);
}

test "Client config retry policy defaults" {
    const config = ClientConfig{};
    try std.testing.expectEqual(@as(u32, 3), config.retry_policy.max_retries);
}

test "Client config redirect policy defaults" {
    const config = ClientConfig{};
    try std.testing.expectEqual(@as(u32, 10), config.redirect_policy.max_redirects);
}

test "Client config keep alive default" {
    const config = ClientConfig{};
    try std.testing.expect(config.keep_alive);
}

test "Client interceptor management" {
    const allocator = std.testing.allocator;
    var client = Client.init(allocator, std.testing.io);
    defer client.deinit();

    try client.addInterceptor(.{
        .request_fn = null,
        .response_fn = null,
        .context = null,
    });

    try std.testing.expectEqual(@as(usize, 1), client.interceptors.items.len);
}

test "Client config base URL" {
    const allocator = std.testing.allocator;
    var client = Client.initWithConfig(allocator, std.testing.io, .{
        .base_url = "https://api.example.com",
    });
    defer client.deinit();

    try std.testing.expect(client.config.base_url != null);
    try std.testing.expectEqualStrings("https://api.example.com", client.config.base_url.?);
}

test "Client HTTP version default" {
    const config = ClientConfig{};
    try std.testing.expect(!config.http2_enabled);
    try std.testing.expect(!config.http3_enabled);
}

test "Client config max_response_headers default" {
    const config = ClientConfig{};
    try std.testing.expectEqual(@as(usize, 256), config.max_response_headers);
}

test "Client config limits are customizable" {
    const allocator = std.testing.allocator;
    var client = Client.initWithConfig(allocator, std.testing.io, .{
        .max_response_size = 1024,
        .max_response_headers = 32,
    });
    defer client.deinit();

    try std.testing.expectEqual(@as(usize, 1024), client.config.max_response_size);
    try std.testing.expectEqual(@as(usize, 32), client.config.max_response_headers);
}

test "SliceIoReader reads slice data" {
    const data = "hello, world!";
    var buf: [64]u8 = undefined;
    var reader = SliceIoReader.init(data, &buf);

    var out: [64]u8 = undefined;
    var iov = [_][]u8{out[0..]};
    const n = try reader.reader_iface.readVec(&iov);
    try std.testing.expectEqualStrings(data, out[0..n]);

    // Second read should return EndOfStream.
    var iov2 = [_][]u8{out[0..]};
    try std.testing.expectError(error.EndOfStream, reader.reader_iface.readVec(&iov2));
}

test "EmptyReadPolicy retries transient empty reads and resets after progress" {
    var policy = EmptyReadPolicy.init(3);

    try std.testing.expectEqual(EmptyReadPolicy.Decision.retry, policy.observe(0));
    try std.testing.expectEqual(@as(usize, 1), policy.empty_reads);

    try std.testing.expectEqual(EmptyReadPolicy.Decision.retry, policy.observe(0));
    try std.testing.expectEqual(@as(usize, 2), policy.empty_reads);

    try std.testing.expectEqual(EmptyReadPolicy.Decision.done, policy.observe(128));
    try std.testing.expectEqual(@as(usize, 0), policy.empty_reads);

    try std.testing.expectEqual(EmptyReadPolicy.Decision.retry, policy.observe(0));
    try std.testing.expectEqual(@as(usize, 1), policy.empty_reads);
}

test "EmptyReadPolicy reaches eof after max transient empty reads" {
    var policy = EmptyReadPolicy.init(2);

    try std.testing.expectEqual(EmptyReadPolicy.Decision.retry, policy.observe(0));
    try std.testing.expectEqual(EmptyReadPolicy.Decision.eof, policy.observe(0));
    try std.testing.expectEqual(@as(usize, 2), policy.empty_reads);
}

test "findChunkedMessageEnd finds terminal chunk" {
    const body = "5\r\nhello\r\n0\r\n\r\nextra";
    try std.testing.expectEqual(@as(?usize, 15), Client.findChunkedMessageEnd(body));
}

test "findChunkedMessageEnd handles trailers" {
    const body = "5\r\nhello\r\n0\r\nX-Test: ok\r\n\r\n";
    try std.testing.expectEqual(@as(?usize, body.len), Client.findChunkedMessageEnd(body));
}

test "findChunkedMessageEnd returns null for incomplete body" {
    try std.testing.expectEqual(@as(?usize, null), Client.findChunkedMessageEnd("5\r\nhello\r\n0\r\n"));
}

test "decodeChunkedBody decodes payload" {
    const allocator = std.testing.allocator;
    const decoded = try Client.decodeChunkedBody(allocator, "5\r\nhello\r\n6\r\n world\r\n0\r\n\r\n", 64);
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("hello world", decoded);
}

test "decodeChunkedBody handles trailers" {
    const allocator = std.testing.allocator;
    const decoded = try Client.decodeChunkedBody(allocator, "5\r\nhello\r\n0\r\nX-Test: ok\r\n\r\n", 64);
    defer allocator.free(decoded);
    try std.testing.expectEqualStrings("hello", decoded);
}

test "client resolveAddress falls back to hostname lookup" {
    const addr = try resolveAddress(std.testing.io, "localhost", 443);
    switch (addr) {
        .ip4 => |ip4| try std.testing.expectEqual(@as(u16, 443), ip4.port),
        .ip6 => |ip6| try std.testing.expectEqual(@as(u16, 443), ip6.port),
    }
}

test "H2StreamReader reads pre-buffered data and returns EOF" {
    const allocator = std.testing.allocator;

    // Set up an H2Connection with a stream that has pre-buffered data.
    var h2 = H2Connection.initClient(allocator, std.testing.io);
    defer h2.deinit();

    const stream = try h2.stream_manager.createStream();
    const stream_id = stream.id;

    // Simulate receive loop having delivered data.
    try stream.data_buf.appendSlice(allocator, "hello world");
    stream.completed = true; // END_STREAM already received

    // Create a heap-allocated event (as requestStream would).
    const data_event = try allocator.create(Io.Event);
    data_event.* = .unset;
    stream.data_event = data_event;

    var reader = Client.H2StreamReader{
        .stream = stream,
        .io = std.testing.io,
        .h2 = &h2,
        .entry = undefined, // Not dereferenced: stream.completed=true so close() skips RST_STREAM.
        .data_event = data_event,
        .allocator = allocator,
    };

    // Read first chunk.
    var buf: [5]u8 = undefined;
    const n1 = try reader.read(&buf);
    try std.testing.expectEqual(@as(usize, 5), n1);
    try std.testing.expectEqualStrings("hello", &buf);

    // Read second chunk.
    var buf2: [10]u8 = undefined;
    const n2 = try reader.read(&buf2);
    try std.testing.expectEqual(@as(usize, 6), n2);
    try std.testing.expectEqualStrings(" world", buf2[0..n2]);

    // Next read should return 0 (EOF) since completed=true and no more data.
    const n3 = try reader.read(&buf2);
    try std.testing.expectEqual(@as(usize, 0), n3);

    // Clean up — close frees the event and removes the stream.
    reader.close();

    // Verify stream was removed.
    try std.testing.expect(h2.stream_manager.getStream(stream_id) == null);
}

const test_tls_cert_pem =
    "-----BEGIN CERTIFICATE-----\n" ++
    "MIIDCTCCAfGgAwIBAgIUGjCWCIDqTRw/vKwQVR5QhFvTtqswDQYJKoZIhvcNAQEL\n" ++
    "BQAwFDESMBAGA1UEAwwJbG9jYWxob3N0MB4XDTI2MDQwMTA1MzAzNFoXDTI2MDQw\n" ++
    "MjA1MzAzNFowFDESMBAGA1UEAwwJbG9jYWxob3N0MIIBIjANBgkqhkiG9w0BAQEF\n" ++
    "AAOCAQ8AMIIBCgKCAQEAlcbhuVCGNinrY/u0i9CraIMPPbUUuidxw6oDcxU2dBmf\n" ++
    "5dZgbMWIOM6ugs2bg0dVLld/epE3AKbK1O2wzmPtziyE+i+aUze1EddHgOm91bDc\n" ++
    "jRE/Poy74X0Nh9kpqdG5JTgqrGU8HFWgVAZJ72cQTTHQrLG4V4cZUaklga2b3EWx\n" ++
    "AtKuhn25aG3c5NO9gdZRJCs9YZ/q7WKX1xI0uwnJXrS54/e7uLgE2HnQZADXujCd\n" ++
    "62fTUaa6NJBjqpYcrmsRT5RU0ZymJQB6megB8GiyAWAtvb0qP9LIyYDo0krW4Q6y\n" ++
    "d4hKFktK6x2yMhsYb71085OiH5I1lvcPMimG3kEdQQIDAQABo1MwUTAdBgNVHQ4E\n" ++
    "FgQU1ju/opd9LtCdJJgvwJIFyrUGCxwwHwYDVR0jBBgwFoAU1ju/opd9LtCdJJgv\n" ++
    "wJIFyrUGCxwwDwYDVR0TAQH/BAUwAwEB/zANBgkqhkiG9w0BAQsFAAOCAQEAeNEq\n" ++
    "DCRtgQY3DXmIv4DBb6hef/5pULMwmb0Rsv5cQfExE0TLYT+m5jTPUbl34kyvFDew\n" ++
    "IAL+6IQf1Wug8ohRPZPxtNFhf8LT+Z2DjfmX1+rMNRNKGeENbj7kDhOE0U7On+iH\n" ++
    "mFtDm068CNlMtUO39BF8dYNFZLyTNPZagmoS+InUSVNwVKGop4TKeN1oHkiMBf9u\n" ++
    "ZSLs84DHU59GbIgSJWBi5MihRgbpauZs/VhvfciLU9KlcwLY1XgbImFggSJdcyqm\n" ++
    "7PlORVQMy2bnuImpzdGywVdMjH9ka7wuEuoXQOSAhMJvycfuY4jls7+abBrhlV/l\n" ++
    "36+7YT0LzE5Z9pLwyA==\n" ++
    "-----END CERTIFICATE-----\n";

const test_tls_key_pem =
    "-----BEGIN PRIVATE KEY-----\n" ++
    "MIIEvAIBADANBgkqhkiG9w0BAQEFAASCBKYwggSiAgEAAoIBAQCVxuG5UIY2Ketj\n" ++
    "+7SL0Ktogw89tRS6J3HDqgNzFTZ0GZ/l1mBsxYg4zq6CzZuDR1UuV396kTcApsrU\n" ++
    "7bDOY+3OLIT6L5pTN7UR10eA6b3VsNyNET8+jLvhfQ2H2Smp0bklOCqsZTwcVaBU\n" ++
    "BknvZxBNMdCssbhXhxlRqSWBrZvcRbEC0q6Gfblobdzk072B1lEkKz1hn+rtYpfX\n" ++
    "EjS7CcletLnj97u4uATYedBkANe6MJ3rZ9NRpro0kGOqlhyuaxFPlFTRnKYlAHqZ\n" ++
    "6AHwaLIBYC29vSo/0sjJgOjSStbhDrJ3iEoWS0rrHbIyGxhvvXTzk6IfkjWW9w8y\n" ++
    "KYbeQR1BAgMBAAECggEAQsS7uJBwnDG4yUQamt+FohwWzcPtPwU5fmfKjOGOelg4\n" ++
    "A047gxHV5bkha5c79dx1WSjRX/Lfaa9xKVXipUc/6lLHXv6cle92DUOCkTHiGiJz\n" ++
    "V4GyR3CWivFj+ETzgUxIdJKi12Jz1w/G3t5E1HAGANutsmaxjndf7prwaOxbWGic\n" ++
    "RCPwL5T/UTen6KknrMWASJSJFVhrjszvsbz6lea/EF9OrmmKSu8/NaQH+fOF5xix\n" ++
    "pdRVCzkiFq9ox1R59k30hjMx2VJRRN53ErauMxWzogJVB7vrCxgdw3qxAr35tKTv\n" ++
    "YRQBeCRDu0nDw05DBo+fMGN8Fe0f3tke7XiKk0sgvwKBgQDL2UMX74KIMjlSFvBE\n" ++
    "6kgO3AJGwItxHnyUcpkUTpYyxGmZ8ZUAw8SG1CBBgBz9161gytTlyQcw0M779xUe\n" ++
    "RXDaclhk8x1PDjnrjlX1WltOPc6f/Qafk1XvzHP5UGI65E2rGXRIrpejF/eiecoM\n" ++
    "SD8TV9FuzBq3R8Te8XbeaCYsLwKBgQC8GEZukAft6x/OAVh6UIIOgzHCbYaklau7\n" ++
    "7bj9NVQKEJRGABRYA/73J579r/stl3cybrRm0IfmzJk2QcBp/AaKTJF0MyiyMfRq\n" ++
    "7LqQLtlJQFDYzqH4KAhjCBiUu7OeZV0SF7ds2EesH1EF+tmOHK8SeFbTTIQriy38\n" ++
    "BfD/jZHBjwKBgGNYH6WTmRbM+zhxa2j6kGGFgSp//bUEOYyTCN1nqzVUmW5n2MkF\n" ++
    "n0piKNIjIH3pVVqdnwHZZcK5kJYlBUq6ZtRe84tHHBqCAWI1/NhUz7ii0IcR5d9x\n" ++
    "C2mRR1fSf/zZdKyU/CHLzKS0MoAhQIGZ1/uSScPofoCh3mUUYmzjbu8LAoGAXv+z\n" ++
    "suuz1YpHSfiMA1reFQ5V92jx8/ZUAlqSb/CbPWoaOTCZFcsO3y13s5FKP0Ccxy/6\n" ++
    "lWMFAKCdUTXsRJsxgnAhlpqwFy/7znU51NCUldaR/q5+R6OQeNQB9jzG/10aoKSx\n" ++
    "05t4t4opleeYMZpzIdT9pUKkDooA86Tcj3WlBCkCgYAOJKlKAjp2ZwHQ/ktkX1hK\n" ++
    "tHbXZMXM40RjrujdimrIJxK87o73jp1s6cIuuHpNCVN15rI30Eb0WGy7ER2J2q2T\n" ++
    "Bk1Pw3B3fAHZpbU7U8YgKW1KTNU7zpOwZjPCTBh2f4jhwwN0Zk4iltu8D2y2KvsC\n" ++
    "xm8hZP/ZhT5VKqRD/ot8fw==\n" ++
    "-----END PRIVATE KEY-----\n";

const python_tls_server_script =
    "import pathlib\n" ++
    "import socket\n" ++
    "import ssl\n" ++
    "import time\n" ++
    "import sys\n" ++
    "\n" ++
    "port = int(sys.argv[1])\n" ++
    "cert = sys.argv[2]\n" ++
    "key = sys.argv[3]\n" ++
    "request_out = pathlib.Path(sys.argv[4])\n" ++
    "\n" ++
    "listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n" ++
    "listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" ++
    "listener.bind(('127.0.0.1', port))\n" ++
    "listener.listen(1)\n" ++
    "ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\n" ++
    "ctx.load_cert_chain(certfile=cert, keyfile=key)\n" ++
    "while True:\n" ++
    "    conn, _ = listener.accept()\n" ++
    "    with conn:\n" ++
    "        try:\n" ++
    "            with ctx.wrap_socket(conn, server_side=True) as tls_conn:\n" ++
    "                data = b''\n" ++
    "                while b'\\r\\n\\r\\n' not in data:\n" ++
    "                    chunk = tls_conn.recv(4096)\n" ++
    "                    if not chunk:\n" ++
    "                        break\n" ++
    "                    data += chunk\n" ++
    "                if not data:\n" ++
    "                    continue\n" ++
    "                request_out.write_bytes(data)\n" ++
    "                tls_conn.sendall(b'HTTP/1.1 200 OK\\r\\nContent-Length: 2\\r\\nConnection: close\\r\\n\\r\\nok')\n" ++
    "                break\n" ++
    "        except ssl.SSLError:\n" ++
    "            continue\n" ++
    "listener.close()\n";

const python_tls_chunked_gzip_server_script =
    "import gzip\n" ++
    "import socket\n" ++
    "import ssl\n" ++
    "import sys\n" ++
    "\n" ++
    "port = int(sys.argv[1])\n" ++
    "cert = sys.argv[2]\n" ++
    "key = sys.argv[3]\n" ++
    "payload = gzip.compress(b'{\"ok\":true}\\n')\n" ++
    "listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n" ++
    "listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" ++
    "listener.bind(('127.0.0.1', port))\n" ++
    "listener.listen(1)\n" ++
    "ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\n" ++
    "ctx.load_cert_chain(certfile=cert, keyfile=key)\n" ++
    "while True:\n" ++
    "    conn, _ = listener.accept()\n" ++
    "    with conn:\n" ++
    "        try:\n" ++
    "            with ctx.wrap_socket(conn, server_side=True) as tls_conn:\n" ++
    "                data = b''\n" ++
    "                while b'\\r\\n\\r\\n' not in data:\n" ++
    "                    chunk = tls_conn.recv(4096)\n" ++
    "                    if not chunk:\n" ++
    "                        break\n" ++
    "                    data += chunk\n" ++
    "                if not data:\n" ++
    "                    continue\n" ++
    "                tls_conn.sendall(\n" ++
    "                    b'HTTP/1.1 200 OK\\r\\n'\n" ++
    "                    b'Content-Type: application/json\\r\\n'\n" ++
    "                    b'Transfer-Encoding: chunked\\r\\n'\n" ++
    "                    b'Content-Encoding: gzip\\r\\n'\n" ++
    "                    b'Connection: close\\r\\n\\r\\n'\n" ++
    "                )\n" ++
    "                tls_conn.sendall(format(len(payload), 'x').encode() + b'\\r\\n')\n" ++
    "                tls_conn.sendall(payload + b'\\r\\n0\\r\\n\\r\\n')\n" ++
    "                break\n" ++
    "        except ssl.SSLError:\n" ++
    "            continue\n" ++
    "listener.close()\n";

const python_redirect_writer_server_script =
    "import socketserver\n" ++
    "import sys\n" ++
    "from http.server import BaseHTTPRequestHandler\n" ++
    "\n" ++
    "port = int(sys.argv[1])\n" ++
    "redirect_body = b'redirect-body'\n" ++
    "final_body = b'final-payload'\n" ++
    "\n" ++
    "class Handler(BaseHTTPRequestHandler):\n" ++
    "    protocol_version = 'HTTP/1.1'\n" ++
    "    def do_GET(self):\n" ++
    "        if self.path == '/redirect':\n" ++
    "            self.send_response(307)\n" ++
    "            self.send_header('Location', '/final')\n" ++
    "            self.send_header('Content-Length', str(len(redirect_body)))\n" ++
    "            self.send_header('Connection', 'close')\n" ++
    "            self.end_headers()\n" ++
    "            self.wfile.write(redirect_body)\n" ++
    "            return\n" ++
    "        if self.path == '/final':\n" ++
    "            self.send_response(200)\n" ++
    "            self.send_header('Content-Length', str(len(final_body)))\n" ++
    "            self.send_header('Connection', 'close')\n" ++
    "            self.end_headers()\n" ++
    "            self.wfile.write(final_body)\n" ++
    "            return\n" ++
    "        self.send_response(404)\n" ++
    "        self.send_header('Content-Length', '0')\n" ++
    "        self.send_header('Connection', 'close')\n" ++
    "        self.end_headers()\n" ++
    "    def log_message(self, fmt, *args):\n" ++
    "        pass\n" ++
    "\n" ++
    "class ReuseTCPServer(socketserver.TCPServer):\n" ++
    "    allow_reuse_address = True\n" ++
    "\n" ++
    "with ReuseTCPServer(('127.0.0.1', port), Handler) as httpd:\n" ++
    "    httpd.serve_forever()\n";

const python_tls_fixed_keepalive_server_script =
    "import socket\n" ++
    "import ssl\n" ++
    "import sys\n" ++
    "import time\n" ++
    "\n" ++
    "port = int(sys.argv[1])\n" ++
    "cert = sys.argv[2]\n" ++
    "key = sys.argv[3]\n" ++
    "payload = (b'0123456789abcdef' * 16384)\n" ++
    "listener = socket.socket(socket.AF_INET, socket.SOCK_STREAM)\n" ++
    "listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)\n" ++
    "listener.bind(('127.0.0.1', port))\n" ++
    "listener.listen(1)\n" ++
    "ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)\n" ++
    "ctx.load_cert_chain(certfile=cert, keyfile=key)\n" ++
    "while True:\n" ++
    "    conn, _ = listener.accept()\n" ++
    "    with conn:\n" ++
    "        try:\n" ++
    "            with ctx.wrap_socket(conn, server_side=True) as tls_conn:\n" ++
    "                data = b''\n" ++
    "                while b'\\r\\n\\r\\n' not in data:\n" ++
    "                    chunk = tls_conn.recv(4096)\n" ++
    "                    if not chunk:\n" ++
    "                        break\n" ++
    "                    data += chunk\n" ++
    "                if not data:\n" ++
    "                    continue\n" ++
    "                hdr = (\n" ++
    "                    b'HTTP/1.1 200 OK\\r\\n'\n" ++
    "                    b'Content-Type: text/plain\\r\\n'\n" ++
    "                    b'Content-Length: ' + str(len(payload)).encode() + b'\\r\\n'\n" ++
    "                    b'Connection: keep-alive\\r\\n\\r\\n'\n" ++
    "                )\n" ++
    "                tls_conn.sendall(hdr)\n" ++
    "                tls_conn.sendall(payload)\n" ++
    "                time.sleep(2.0)\n" ++
    "                break\n" ++
    "        except ssl.SSLError:\n" ++
    "            continue\n" ++
    "listener.close()\n";

fn reserveEphemeralPort(io: Io) !u16 {
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try socket_mod.TcpListener.init(listen_addr, io);
    defer listener.deinit();
    return listener.getLocalAddress().ip4.port;
}

fn getWithRetry(client: *Client, io: Io, url: []const u8, max_attempts: usize) !Response {
    var attempts: usize = 0;
    while (attempts < max_attempts) : (attempts += 1) {
        return client.get(url, .{}) catch |err| {
            if (err != error.ConnectionRefused or attempts + 1 >= max_attempts) return err;
            io.sleep(Io.Duration.fromMilliseconds(100), .awake) catch {};
            continue;
        };
    }
    unreachable;
}

test "HTTPS client round trip via local TLS server" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const port = try reserveEphemeralPort(io);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "cert.pem", .data = test_tls_cert_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "key.pem", .data = test_tls_key_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_tls_server_script });

    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{
            "python3",
            "server.py",
            port_arg,
            "cert.pem",
            "key.pem",
            "request.bin",
        },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);

    io.sleep(Io.Duration.fromMilliseconds(1000), .awake) catch {};

    const url = try std.fmt.allocPrint(allocator, "https://127.0.0.1:{d}/", .{port});
    defer allocator.free(url);

    var client = Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .verify_ssl = false,
        .retry_policy = .{ .max_retries = 0 },
        .timeouts = .{ .request_ms = 2_000, .read_ms = 2_000, .write_ms = 2_000 },
    });
    defer client.deinit();

    var resp = try getWithRetry(&client, io, url, 50);
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expectEqualStrings("ok", resp.body orelse "");

    const request = try tmp.dir.readFileAlloc(io, "request.bin", allocator, .limited(4096));
    defer allocator.free(request);
    const expected_host = try std.fmt.allocPrint(allocator, "Host: 127.0.0.1:{d}\r\n", .{port});
    defer allocator.free(expected_host);
    try std.testing.expect(std.mem.startsWith(u8, request, "GET / HTTP/1.1\r\n"));
    try std.testing.expect(std.mem.indexOf(u8, request, expected_host) != null);
    try std.testing.expect(std.mem.indexOf(u8, request, "Connection: close\r\n") != null);
}


test "HTTPS client handles chunked gzip body via local TLS server" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const port = try reserveEphemeralPort(io);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "cert.pem", .data = test_tls_cert_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "key.pem", .data = test_tls_key_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_tls_chunked_gzip_server_script });

    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{
            "python3",
            "server.py",
            port_arg,
            "cert.pem",
            "key.pem",
        },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);

    io.sleep(Io.Duration.fromMilliseconds(1000), .awake) catch {};

    const url = try std.fmt.allocPrint(allocator, "https://127.0.0.1:{d}/", .{port});
    defer allocator.free(url);

    var client = Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .verify_ssl = false,
        .retry_policy = .{ .max_retries = 0 },
        .timeouts = .{ .request_ms = 2_000, .read_ms = 2_000, .write_ms = 2_000 },
    });
    defer client.deinit();

    var resp = try getWithRetry(&client, io, url, 50);
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expectEqualStrings("{\"ok\":true}\n", resp.body orelse "");
}

test "HTTPS client streams chunked gzip body to writer via local TLS server" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const port = try reserveEphemeralPort(io);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "cert.pem", .data = test_tls_cert_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "key.pem", .data = test_tls_key_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_tls_chunked_gzip_server_script });

    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{
            "python3",
            "server.py",
            port_arg,
            "cert.pem",
            "key.pem",
        },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);

    io.sleep(Io.Duration.fromMilliseconds(1000), .awake) catch {};

    const url = try std.fmt.allocPrint(allocator, "https://127.0.0.1:{d}/", .{port});
    defer allocator.free(url);

    var client = Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .verify_ssl = false,
        .retry_policy = .{ .max_retries = 0 },
        .timeouts = .{ .request_ms = 2_000, .read_ms = 2_000, .write_ms = 2_000 },
    });
    defer client.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    var resp = try client.getToWriter(url, .{}, arrayListWriter(&out, allocator), null, null);
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expect(resp.body == null);
    try std.testing.expectEqualStrings("{\"ok\":true}\n", out.items);
}

test "requestToWriter follows redirects without streaming intermediate redirect body" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const port = try reserveEphemeralPort(io);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_redirect_writer_server_script });

    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{
            "python3",
            "server.py",
            port_arg,
        },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);

    io.sleep(Io.Duration.fromMilliseconds(500), .awake) catch {};

    const url = try std.fmt.allocPrint(allocator, "http://127.0.0.1:{d}/redirect", .{port});
    defer allocator.free(url);

    var client = Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .retry_policy = .{ .max_retries = 0 },
        .timeouts = .{ .request_ms = 2_000, .read_ms = 2_000, .write_ms = 2_000 },
    });
    defer client.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    var resp = try client.getToWriter(url, .{}, arrayListWriter(&out, allocator), null, null);
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expect(resp.body == null);
    try std.testing.expectEqualStrings("final-payload", out.items);
}

test "HTTPS client streams fixed content-length body with keep-alive to writer" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;

    const port = try reserveEphemeralPort(io);

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(io, .{ .sub_path = "cert.pem", .data = test_tls_cert_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "key.pem", .data = test_tls_key_pem });
    try tmp.dir.writeFile(io, .{ .sub_path = "server.py", .data = python_tls_fixed_keepalive_server_script });

    var port_buf: [16]u8 = undefined;
    const port_arg = try std.fmt.bufPrint(&port_buf, "{d}", .{port});

    var child = std.process.spawn(io, .{
        .argv = &.{
            "python3",
            "server.py",
            port_arg,
            "cert.pem",
            "key.pem",
        },
        .cwd = .{ .dir = tmp.dir },
        .stdin = .ignore,
        .stdout = .inherit,
        .stderr = .inherit,
    }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer child.kill(io);

    io.sleep(Io.Duration.fromMilliseconds(500), .awake) catch {};

    const url = try std.fmt.allocPrint(allocator, "https://127.0.0.1:{d}/", .{port});
    defer allocator.free(url);

    var client = Client.initWithConfig(allocator, io, .{
        .keep_alive = false,
        .verify_ssl = false,
        .retry_policy = .{ .max_retries = 0 },
        .timeouts = .{ .request_ms = 5_000, .read_ms = 5_000, .write_ms = 5_000 },
    });
    defer client.deinit();

    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);

    var resp = try client.getToWriter(url, .{}, arrayListWriter(&out, allocator), null, null);
    defer resp.deinit();

    try std.testing.expectEqual(@as(u16, 200), resp.status.code);
    try std.testing.expectEqual(@as(usize, 16 * 16384), out.items.len);
    try std.testing.expect(resp.body == null);
}

// Tests for decompressBody and responseFromParser were removed — these methods
// were replaced by the streaming decompression pipeline in buildStreamingResponse.
