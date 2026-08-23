//! HTTP Server Implementation for httpx.zig
//!
//! Production-ready HTTP server with comprehensive features:
//!
//! - Pattern-based routing with path parameters
//! - Middleware stack support
//! - Context-based request handling
//! - JSON response helpers
//! - Static file serving
//! - HTTP/2 with two entry paths:
//!   1. **Prior knowledge** (RFC 7540 §3.4): client sends h2 preface directly;
//!      the server detects it from the first bytes of the connection.
//!   2. **h2c upgrade** (RFC 7540 §3.2): client sends an HTTP/1.1 request with
//!      `Upgrade: h2c`; the server responds with 101 and switches to h2.
//! - Cross-platform (Linux, Windows, macOS)
//!
//! ## TLS / HTTPS
//!
//! Zig 0.16 `std.crypto.tls` only provides a `Client` — there is no
//! server-side TLS implementation yet. For HTTPS, deploy behind a TLS-
//! terminating reverse proxy (e.g. nginx, Caddy, envoy) that forwards
//! plaintext HTTP/2 (h2c) or HTTP/1.1 to this server. The `tls_cert_path`
//! and `tls_key_path` fields in `ServerConfig` are reserved for future
//! direct TLS support.

const std = @import("std");
const builtin = @import("builtin");
const array_list_writer_mod = @import("../util/array_list_writer.zig");
const arrayListWriter = array_list_writer_mod.arrayListWriter;
const serializeToSlice = array_list_writer_mod.serializeToSlice;
const mem = std.mem;
const posix = std.posix;
const Allocator = mem.Allocator;
const Io = std.Io;
const milliTimestamp = @import("../util/common.zig").milliTimestamp;
const encoding = @import("../util/encoding.zig");

const types = @import("../core/types.zig");
const Request = @import("../core/request.zig").Request;
const Response = @import("../core/response.zig").Response;
const ResponseBuilder = @import("../core/response.zig").ResponseBuilder;
const Headers = @import("../core/headers.zig").Headers;
const HeaderName = @import("../core/headers.zig").HeaderName;
const containsCrLf = @import("../core/headers.zig").containsCrLf;
const Parser = @import("../protocol/parser.zig").Parser;
const http = @import("../protocol/http.zig");
const Socket = @import("../net/socket.zig").Socket;
const Address = @import("../net/socket.zig").Address;
const TcpListener = @import("../net/socket.zig").TcpListener;
const Router = @import("router.zig").Router;
const RouteParam = @import("router.zig").RouteParam;
const middleware_mod = @import("middleware.zig");
const Middleware = middleware_mod.Middleware;
const common = @import("../util/common.zig");
const h2_mod = @import("../protocol/h2_connection.zig");
const H2Connection = h2_mod.H2Connection;
const hpack = @import("../protocol/hpack.zig");
const stream_mod = @import("../protocol/stream.zig");
const Stream = stream_mod.Stream;
const SharedBodyBudget = @import("../protocol/body_budget.zig").SharedBodyBudget;
const HttpRuntime = @import("http_runtime.zig").HttpRuntime;
const CancellationObserver = @import("cancellation_observer.zig").Observer;

fn deadlineAfter(io: Io, timeout_ms: u64) i64 {
    if (timeout_ms == 0) return 0;
    const bounded_ms: i64 = @intCast(@min(timeout_ms, @as(u64, std.math.maxInt(i64))));
    return milliTimestamp(io) +| bounded_ms;
}

fn applyReadDeadline(sock: *Socket, io: Io, deadline_ms: i64) !void {
    if (deadline_ms == 0) return sock.setRecvTimeout(0);
    const remaining_ms = deadline_ms - milliTimestamp(io);
    if (remaining_ms <= 0) return error.Timeout;
    try sock.setRecvTimeout(@intCast(remaining_ms));
}

pub const CookieOptions = common.CookieOptions;
pub const SameSite = common.SameSite;

/// SSE event payload used by `Context.sse`.
pub const SseEvent = struct {
    data: []const u8,
    event: ?[]const u8 = null,
    id: ?[]const u8 = null,
    retry_ms: ?u32 = null,
};

/// Pre-route hook called after parsing the request and before route matching.
pub const PreRouteHook = *const fn (*Context) anyerror!void;

pub const H1DisconnectCancellation = enum {
    /// Every active HTTP/1 request must be registered with HttpRuntime. If the
    /// observer is unavailable, fail the request closed before dispatch.
    required,
    /// Retain shutdown cancellation but do not observe peer disconnects.
    /// Intended only for bounded control listeners such as health/readiness,
    /// which must remain able to report an unhealthy shared HttpRuntime.
    disabled,
};

/// Backend-neutral hard-disconnect observation for virtual or embedded
/// transports whose socket handles are not native process descriptors.
/// Orderly half-close must return false; only a reset/abort is cancellation.
pub const H1DisconnectProbe = struct {
    ptr: ?*const anyopaque,
    is_hard_disconnected: *const fn (?*const anyopaque, std.Io.net.Socket.Handle) bool,

    pub fn requested(self: H1DisconnectProbe, handle: std.Io.net.Socket.Handle) bool {
        return self.is_hard_disconnected(self.ptr, handle);
    }
};

pub const ConnectionExecution = enum {
    /// Every accepted connection is submitted to HttpRuntime's bounded
    /// connection lane. Saturation rejects it without blocking accept.
    concurrent,
    /// Deliberately serve one connection at a time on the calling thread.
    /// This is an explicit embedding mode, never a saturation fallback.
    serial,
};

/// Server configuration.
pub const ServerConfig = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 8080,
    max_body_size: usize = 10 * 1024 * 1024,
    max_headers: usize = 100,
    max_file_size: usize = types.default_max_body_size,
    /// Absolute wall-clock limit for receiving one request's header block.
    /// Zero disables this phase deadline.
    header_read_timeout_ms: u64 = 30_000,
    /// Absolute wall-clock limit for receiving one request body after headers.
    /// Zero disables this phase deadline.
    body_read_timeout_ms: u64 = 120_000,
    /// Per-write socket timeout for response delivery. Zero disables it.
    response_write_timeout_ms: u64 = 30_000,
    keep_alive_timeout_ms: u64 = 60_000,
    max_connections: u32 = 1000,
    /// Aggregate handler tasks reserved from `HttpRuntime`. Zero uses
    /// `max_connections`. HTTP/1, HTTP/2, and h2c all share this bound;
    /// saturation rejects before application work begins.
    max_request_tasks: u32 = 0,
    connection_execution: ConnectionExecution = .concurrent,
    /// Optional transport runtime shared by multiple listeners. When absent,
    /// the Server owns a private runtime whose listener, connection, and
    /// request-task lanes are sized to this configuration.
    http_runtime: ?*HttpRuntime = null,
    h1_disconnect_cancellation: H1DisconnectCancellation = .required,
    /// Required when disconnect cancellation runs on borrowed std.Io lanes.
    /// Native runtimes leave this null and use their descriptor observer.
    h1_disconnect_probe: ?H1DisconnectProbe = null,
    /// Maximum H1 requests allowed to wait for a body after headers have been
    /// parsed. 0 inherits max_connections. This preserves connection capacity
    /// for control/recovery traffic during slow uploads.
    max_h1_inflight_bodies: u32 = 0,
    /// Exponential accept-error backoff. Resource exhaustion otherwise turns
    /// EMFILE/ENFILE into a CPU-burning, unbounded log loop.
    accept_error_backoff_initial_ms: u32 = 5,
    accept_error_backoff_max_ms: u32 = 1_000,
    /// Permit rebinding an address that has recently been used without allowing
    /// two live listeners to share the same bind tuple.
    reuse_address: bool = true,
    /// Explicit multi-listener load balancing via SO_REUSEPORT. Production
    /// listeners remain exclusive unless this is deliberately enabled.
    reuse_port: bool = false,
    keep_alive: bool = true,
    max_requests_per_connection: u32 = 1000,
    /// Idle timeout for HTTP/2 connections (ms). The server initiates graceful
    /// shutdown (GOAWAY) when no streams are active for this duration. 0 = no timeout.
    h2_idle_timeout_ms: u64 = 300_000,
    /// Maximum total streams processed on a single H2 connection before sending
    /// GOAWAY. 0 = unlimited. Prevents unbounded HPACK/state growth.
    h2_max_requests: u32 = 10_000,
    /// HTTP/2 initial window size advertised in SETTINGS. Controls how much DATA
    /// a client can send per stream before waiting for WINDOW_UPDATE. The default
    /// 1MB allows large request bodies without excessive round-trips. RFC 7540
    /// default is 65535 which forces ~160 round-trips for a 10MB upload.
    h2_initial_window_size: u32 = 1_048_576,
    /// HTTP/2 max concurrent streams advertised in SETTINGS. Controls how many
    /// in-flight requests a client can have on one connection. 0 = use default (100).
    h2_max_concurrent_streams: u32 = 100,
    /// Aggregate request-body bytes retained across every HTTP/1 and HTTP/2
    /// connection. Values below max_body_size are raised to it so one valid
    /// request can always complete; 0 selects that minimum.
    request_body_buffer_budget_bytes: usize = 64 * 1024 * 1024,
    /// Reserved for future server-side TLS support. Zig 0.16 only provides
    /// `std.crypto.tls.Client`; there is no server TLS implementation yet.
    /// Use a TLS-terminating reverse proxy in the meantime.
    tls_cert_path: ?[]const u8 = null,
    tls_key_path: ?[]const u8 = null,

    /// Resolves every sentinel/default and enforces dependent bounds. Shared
    /// HttpRuntime owners must size their lanes from this value so the runtime
    /// and Server cannot interpret the same listener configuration differently.
    pub fn normalized(config: @This()) @This() {
        var resolved = config;
        if (resolved.max_connections == 0) resolved.max_connections = 1000;
        if (resolved.max_request_tasks == 0) resolved.max_request_tasks = resolved.max_connections;
        if (resolved.h2_max_concurrent_streams == 0) resolved.h2_max_concurrent_streams = 100;
        // Do not advertise more simultaneous streams on one connection than
        // this listener can ever publish to its bounded handler lane.
        resolved.h2_max_concurrent_streams = @min(resolved.h2_max_concurrent_streams, resolved.max_request_tasks);
        resolved.request_body_buffer_budget_bytes = @max(resolved.request_body_buffer_budget_bytes, resolved.max_body_size);
        resolved.accept_error_backoff_initial_ms = @max(resolved.accept_error_backoff_initial_ms, 1);
        resolved.accept_error_backoff_max_ms = @max(resolved.accept_error_backoff_max_ms, resolved.accept_error_backoff_initial_ms);
        return resolved;
    }
};

const H1DisconnectProbeContext = struct {
    probe: H1DisconnectProbe,
    handle: std.Io.net.Socket.Handle,
    cancellation: *std.atomic.Value(bool),
    runtime: *HttpRuntime,
    recorded: bool = false,

    fn requested(raw: ?*const anyopaque) bool {
        const self: *@This() = @ptrCast(@alignCast(@constCast(raw orelse return false)));
        if (!self.probe.requested(self.handle)) return false;
        self.cancellation.store(true, .release);
        if (!self.recorded) {
            self.runtime.recordBorrowedH1HardDisconnect();
            self.recorded = true;
        }
        return true;
    }
};

/// Maps an application error to an HTTP response. Cancellation has no status:
/// its transport or shutdown source already made response delivery invalid.
fn routeErrorStatus(err: anyerror) ?u16 {
    return switch (err) {
        error.Canceled, error.Cancelled => null,
        error.Timeout => 408,
        error.DeadlineExceeded => 504,
        error.BodyTooLarge, error.StreamTooLong, error.ValueTooLong => 413,
        error.BodyCapacityExceeded => 429,
        error.EndOfStream,
        error.SyntaxError,
        error.UnexpectedToken,
        error.MissingField,
        error.UnknownField,
        error.InvalidRequest,
        => 400,
        else => 500,
    };
}

fn routeErrorBody(code: u16) []const u8 {
    return switch (code) {
        400 => "{\"error\":\"INVALID_REQUEST\",\"message\":\"invalid request\"}",
        404 => "{\"error\":\"NOT_FOUND\",\"message\":\"resource not found\"}",
        408 => "{\"error\":\"REQUEST_TIMEOUT\",\"message\":\"request timed out\"}",
        413 => "{\"error\":\"PAYLOAD_TOO_LARGE\",\"message\":\"request payload is too large\"}",
        429 => "{\"error\":\"TOO_MANY_REQUESTS\",\"message\":\"request body capacity exhausted\"}",
        431 => "{\"error\":\"REQUEST_HEADER_FIELDS_TOO_LARGE\",\"message\":\"request headers are too large\"}",
        else => "{\"error\":\"INTERNAL_ERROR\",\"message\":\"internal server error\"}",
    };
}

/// Reports whether `input` begins with a complete HTTP/1 request under the
/// same parser limits as this server. `input` is the connection loop's
/// unconsumed suffix after dispatching the current request, so non-empty data
/// alone is not enough: it may be a partial or malformed next request.
fn hasCompleteBufferedH1Request(allocator: Allocator, input: []const u8, config: ServerConfig) bool {
    if (input.len == 0) return false;

    var probe = Parser.init(allocator);
    defer probe.deinit();
    // This parser is only a framing probe for the next pipelined request.
    // In particular, do not reserve the declared Content-Length before the
    // complete body is known to be buffered.
    probe.store_body = false;
    probe.max_body_size = config.max_body_size;
    probe.max_headers = config.max_headers;
    _ = probe.feed(input) catch return false;
    return probe.isComplete();
}

test "buffered H1 probe frames bodies without retaining them" {
    const allocator = std.testing.allocator;
    const config = ServerConfig{};

    // A complete header block alone is not a complete fixed-length request.
    try std.testing.expect(!hasCompleteBufferedH1Request(
        allocator,
        "POST /upload HTTP/1.1\r\nContent-Length: 10485760\r\n\r\n",
        config,
    ));
    try std.testing.expect(hasCompleteBufferedH1Request(
        allocator,
        "POST /upload HTTP/1.1\r\nContent-Length: 3\r\n\r\nabc",
        config,
    ));

    // Chunk framing must remain incremental: an unfinished terminal chunk is
    // not a pipelined request, while its completed form is.
    try std.testing.expect(!hasCompleteBufferedH1Request(
        allocator,
        "POST /upload HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n",
        config,
    ));
    try std.testing.expect(hasCompleteBufferedH1Request(
        allocator,
        "POST /upload HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n0\r\n\r\n",
        config,
    ));
}

/// Request context passed to handlers.
pub const Context = struct {
    allocator: Allocator,
    io: Io,
    request: *Request,
    response: ResponseBuilder,
    params: []const RouteParam = &.{},
    /// Borrowed opaque value associated with the matched route.
    route_data: ?*anyopaque = null,
    data: ?std.StringHashMap(DataEntry) = null,
    decoded_query_values: std.ArrayListUnmanaged([]u8) = .empty,
    max_file_size: usize = types.default_max_body_size,

    // H1 streaming field (set by the server, null for HTTP/2).
    h1_sock: ?*Socket = null,
    /// True when this HTTP/1.1 request was parsed with bytes for a following
    /// request already held in the connection loop's private buffer. Handlers
    /// that observe the peer socket must not mistake its EOF for cancellation
    /// of the current request: the peer may have half-closed after pipelining
    /// its next request.
    h1_has_buffered_input: bool = false,
    /// Set to true when a streaming response has been sent via `streamResponse()`.
    /// When true, the connection loop skips the normal response serialization.
    h1_stream_sent: bool = false,

    // H2 streaming fields (set by the server for H2 streams, null for HTTP/1.1).
    h2: ?*H2Connection = null,
    h2_sock: ?*Socket = null,
    h2_stream_id: u31 = 0,
    h2_stream_sent: bool = false,
    /// Borrowed transport-owned cancellation signal. HTTP/2 supplies the
    /// stream reset state; HTTP/1 supplies the active connection request state.
    cancellation: ?*const std.atomic.Value(bool) = null,
    /// Optional transport-neutral cancellation source installed by adapters
    /// that cannot expose their concrete listener state to the handler.
    cancellation_probe: ?CancellationProbe = null,

    /// Optional transport-neutral streaming sink. Linked runtime adapters use
    /// this to preserve incremental response delivery without sharing socket
    /// or HTTP/2 connection representations across a code-generation ABI.
    stream_delegate: ?StreamDelegate = null,

    /// Optional transport-neutral lazy request body. Linked runtime adapters
    /// use this to retain transport ownership while application admission runs
    /// before a streaming upload is buffered.
    body_delegate: ?BodyDelegate = null,

    /// Streaming body reader for HTTP/2 requests where the body arrives
    /// incrementally (dispatch-on-HEADERS mode). Null for HTTP/1.1 or
    /// for H2 requests that completed before the handler was dispatched.
    h2_body_reader: ?*H2StreamReader = null,

    /// Entry in the context data map with an optional destructor for cleanup.
    pub const DataEntry = struct {
        ptr: *anyopaque,
        dtor: ?*const fn (*anyopaque) void = null,
    };

    const Self = @This();

    pub const CancellationProbe = struct {
        ptr: ?*const anyopaque,
        is_cancelled: *const fn (?*const anyopaque) bool,

        pub fn requested(self: CancellationProbe) bool {
            return self.is_cancelled(self.ptr);
        }
    };

    pub const StreamDelegate = struct {
        ptr: ?*anyopaque,
        start: *const fn (?*anyopaque, u16) anyerror!void,
        write: *const fn (?*anyopaque, []const u8) anyerror!void,
        close: *const fn (?*anyopaque) anyerror!void,
    };

    pub const BodyDelegate = struct {
        ptr: ?*anyopaque,
        read_all: *const fn (?*anyopaque) anyerror!?[]const u8,
        streaming: bool,
    };

    /// Creates a new context for a request.
    pub fn init(allocator: Allocator, io: Io, req: *Request) Self {
        return .{
            .allocator = allocator,
            .io = io,
            .request = req,
            .response = ResponseBuilder.init(allocator),
        };
    }

    /// Releases context resources. Calls destructors for data entries that have them.
    pub fn deinit(self: *Self) void {
        for (self.decoded_query_values.items) |value| self.allocator.free(value);
        self.decoded_query_values.deinit(self.allocator);
        if (self.data) |*data| {
            var it = data.iterator();
            while (it.next()) |entry| {
                if (entry.value_ptr.dtor) |dtor| dtor(entry.value_ptr.ptr);
            }
            data.deinit();
        }
        self.response.deinit();
    }

    /// Stores a pointer in the context data map with an optional destructor.
    /// If replacing an existing entry, its destructor is called first.
    pub fn setData(self: *Self, key: []const u8, ptr: *anyopaque, dtor: ?*const fn (*anyopaque) void) !void {
        if (self.data == null) {
            self.data = std.StringHashMap(DataEntry).init(self.allocator);
        }
        if (self.data.?.get(key)) |existing| {
            if (existing.dtor) |d| d(existing.ptr);
        }
        try self.data.?.put(key, .{ .ptr = ptr, .dtor = dtor });
    }

    /// Retrieves a stored pointer by key.
    pub fn getData(self: *const Self, key: []const u8) ?*anyopaque {
        const data = self.data orelse return null;
        return if (data.get(key)) |entry| entry.ptr else null;
    }

    /// Returns a URL parameter by name.
    pub fn param(self: *const Self, name: []const u8) ?[]const u8 {
        for (self.params) |p| {
            if (std.mem.eql(u8, p.name, name)) return p.value;
        }
        return null;
    }

    /// Returns a query parameter by name.
    pub fn query(self: *const Self, name: []const u8) ?[]const u8 {
        const query_str = self.request.uri.query orelse return null;
        return common.queryValue(query_str, name);
    }

    /// Returns one application/x-www-form-urlencoded query value, decoded
    /// exactly once into request-owned memory.
    pub fn queryDecoded(self: *Self, name: []const u8) !?[]const u8 {
        const raw = self.query(name) orelse return null;
        const decoded = try encoding.PercentEncoding.decodeFormData(self.allocator, raw);
        errdefer self.allocator.free(decoded);
        try self.decoded_query_values.append(self.allocator, decoded);
        return decoded;
    }

    /// Returns a request header by name.
    pub fn header(self: *const Self, name: []const u8) ?[]const u8 {
        return self.request.headers.get(name);
    }

    /// Returns a parsed cookie value by name from the request Cookie header.
    pub fn cookie(self: *const Self, name: []const u8) ?[]const u8 {
        const cookie_header = self.request.headers.get(HeaderName.COOKIE) orelse return null;
        return common.cookieValue(cookie_header, name);
    }

    /// Sets the response status code.
    pub fn status(self: *Self, code: u16) *Self {
        _ = self.response.status(code);
        return self;
    }

    /// Sets a response header.
    pub fn setHeader(self: *Self, name: []const u8, value: []const u8) !void {
        _ = try self.response.header(name, value);
    }

    /// Appends a Set-Cookie header with common cookie attributes.
    /// Returns error.HeaderContainsCrLf if the name or value contains CR or LF.
    pub fn setCookie(self: *Self, name: []const u8, value: []const u8, options: CookieOptions) !void {
        if (containsCrLf(name) or containsCrLf(value))
            return error.HeaderContainsCrLf;
        const set_cookie = try common.buildSetCookieHeader(self.allocator, name, value, options);
        try self.appendOwnedSetCookie(set_cookie);
    }

    /// Appends a Set-Cookie header that removes a cookie via Max-Age=0.
    /// Returns error.HeaderContainsCrLf if the name contains CR or LF.
    pub fn removeCookie(self: *Self, name: []const u8, options: CookieOptions) !void {
        if (containsCrLf(name))
            return error.HeaderContainsCrLf;
        var remove_options = options;
        remove_options.max_age = 0;
        const remove_value = try common.buildSetCookieHeader(self.allocator, name, "", remove_options);
        try self.appendOwnedSetCookie(remove_value);
    }

    /// Appends a Set-Cookie header with an already-allocated value.
    /// On error the value is freed.
    fn appendOwnedSetCookie(self: *Self, value: []u8) !void {
        errdefer self.allocator.free(value);
        // Validate the fully-composed value (covers options.path, .domain, etc.)
        if (containsCrLf(value)) return error.HeaderContainsCrLf;
        const owned_name = try self.allocator.dupe(u8, HeaderName.SET_COOKIE);
        errdefer self.allocator.free(owned_name);
        try self.response.headers.entries.append(self.allocator, .{
            .name = owned_name,
            .value = value,
            .owned = true,
        });
    }

    /// Sends a plain text response.
    pub fn text(self: *Self, data: []const u8) !Response {
        _ = try self.response.header(HeaderName.CONTENT_TYPE, "text/plain; charset=utf-8");
        _ = self.response.body(data);
        return self.response.build();
    }

    /// Sends an HTML response.
    pub fn html(self: *Self, data: []const u8) !Response {
        _ = try self.response.header(HeaderName.CONTENT_TYPE, "text/html; charset=utf-8");
        _ = self.response.body(data);
        return self.response.build();
    }

    /// Sends a file response.
    pub fn file(self: *Self, path: []const u8) !Response {
        // Reject path traversal: check for ".." segments in raw and
        // percent-decoded form. Also reject absolute paths and null bytes.
        if (containsTraversal(path)) {
            return self.status(403).text("Forbidden");
        }

        const f = Io.Dir.cwd().openFile(self.io, path, .{}) catch return self.status(404).text("Not Found");
        defer f.close(self.io);

        const stat = f.stat(self.io) catch return self.status(404).text("Not Found");
        if (stat.size > self.max_file_size) {
            return self.status(413).text("File Too Large");
        }

        const content = try self.allocator.alloc(u8, @intCast(stat.size));
        errdefer self.allocator.free(content);
        _ = f.readPositionalAll(self.io, content, 0) catch return self.status(500).text("Read Error");

        _ = try self.response.header(HeaderName.CONTENT_TYPE, common.mimeTypeFromPath(path));
        self.response.body_data = content;
        self.response.body_owned = true;
        return self.response.build();
    }

    /// Sends chunked transfer-encoded payload with optional trailers.
    pub fn chunked(self: *Self, data: []const u8, trailers: ?*const Headers) !Response {
        const encoded = try http.encodeChunkedBody(data, trailers, self.allocator);
        errdefer self.allocator.free(encoded);

        _ = try self.response.header(HeaderName.TRANSFER_ENCODING, "chunked");
        if (trailers) |trailer_headers| {
            const trailer_names = try trailerHeaderNames(self.allocator, trailer_headers);
            defer self.allocator.free(trailer_names);
            _ = try self.response.header(HeaderName.TRAILER, trailer_names);
        }
        // Transfer ownership to the builder to avoid a second allocation in build().
        self.response.body_data = encoded;
        self.response.body_owned = true;
        return self.response.build();
    }

    /// Sends one-shot Server-Sent Events payload.
    pub fn sse(self: *Self, events: []const SseEvent) !Response {
        var payload = std.ArrayListUnmanaged(u8).empty;
        defer payload.deinit(self.allocator);
        const writer = arrayListWriter(&payload, self.allocator);

        for (events) |evt| {
            if (evt.id) |id| {
                if (mem.indexOfAny(u8, id, "\r\n") != null) return error.InvalidSseField;
                try writer.print("id: {s}\n", .{id});
            }
            if (evt.event) |name| {
                if (mem.indexOfAny(u8, name, "\r\n") != null) return error.InvalidSseField;
                try writer.print("event: {s}\n", .{name});
            }
            if (evt.retry_ms) |retry_ms| try writer.print("retry: {d}\n", .{retry_ms});

            var lines = mem.splitScalar(u8, evt.data, '\n');
            while (lines.next()) |line| {
                try writer.print("data: {s}\n", .{line});
            }
            try writer.writeAll("\n");
        }

        _ = try self.response.header(HeaderName.CONTENT_TYPE, "text/event-stream; charset=utf-8");
        _ = try self.response.header(HeaderName.CACHE_CONTROL, "no-cache");
        _ = try self.response.header(HeaderName.CONNECTION, "keep-alive");
        _ = self.response.body(payload.items);
        return self.response.build();
    }

    /// Sends a JSON response.
    pub fn json(self: *Self, value: anytype) !Response {
        _ = try self.response.json(value);
        return self.response.build();
    }

    /// Sends a redirect response.
    pub fn redirect(self: *Self, url: []const u8, code: u16) !Response {
        _ = self.response.status(code);
        _ = try self.response.header(HeaderName.LOCATION, url);
        return self.response.build();
    }

    /// Reader for server-side HTTP/2 streaming request bodies.
    /// Reads incrementally from the stream's data_buf mailbox, waiting
    /// on data_event when no data is available. Returns 0 (EOF) when
    /// END_STREAM has been received and all data has been consumed.
    pub const H2StreamReader = struct {
        h2_stream: *Stream,
        io: Io,
        data_event: *Io.Event,
        /// One absolute body deadline shared by every read. A duration timeout
        /// here would renew after each DATA frame and permit an endless
        /// trickle upload to retain application admission.
        deadline_ms: i64 = 0,

        /// Reads available body data into `buf`. Returns 0 on EOF.
        /// Returns error.Timeout once the shared body deadline expires.
        pub fn read(self: *H2StreamReader, buf: []u8) !usize {
            while (true) {
                const avail = self.h2_stream.data_buf.items.len - self.h2_stream.read_offset;
                if (avail > 0) {
                    const n = @min(avail, buf.len);
                    const start = self.h2_stream.read_offset;
                    @memcpy(buf[0..n], self.h2_stream.data_buf.items[start..][0..n]);
                    self.h2_stream.read_offset += n;
                    if (self.h2_stream.read_offset >= Stream.compact_threshold) {
                        self.h2_stream.compactDataBuf();
                    }
                    return n;
                }
                if (self.h2_stream.stream_error) |err| return err;
                if (self.h2_stream.completed) return 0;

                // Reset event, re-check (race guard), then wait with timeout.
                self.data_event.reset();
                const avail2 = self.h2_stream.data_buf.items.len - self.h2_stream.read_offset;
                if (avail2 > 0) continue;
                if (self.h2_stream.stream_error) |err| return err;
                if (self.h2_stream.completed) return 0;

                const timeout: Io.Timeout = if (self.deadline_ms > 0) blk: {
                    const remaining_ms = self.deadline_ms - milliTimestamp(self.io);
                    if (remaining_ms <= 0) return error.Timeout;
                    break :blk .{ .duration = .{
                        .raw = Io.Duration.fromMilliseconds(@intCast(remaining_ms)),
                        .clock = .awake,
                    } };
                } else .none;
                self.data_event.waitTimeout(self.io, timeout) catch |err| switch (err) {
                    error.Timeout => return error.Timeout,
                    error.Canceled => return error.Canceled,
                };
            }
        }

        /// Reads all remaining body data into a single owned slice.
        pub fn readAll(self: *H2StreamReader, allocator: Allocator) ![]u8 {
            var result = std.ArrayListUnmanaged(u8).empty;
            errdefer result.deinit(allocator);
            var buf: [8192]u8 = undefined;
            while (true) {
                const n = try self.read(&buf);
                if (n == 0) break;
                try result.appendSlice(allocator, buf[0..n]);
            }
            return result.toOwnedSlice(allocator);
        }
    };

    /// Returns the request body, buffering from the streaming H2 reader if needed.
    /// For HTTP/1.1 this returns request.body directly. For HTTP/2 streaming,
    /// reads the entire body on first call.
    pub fn body(self: *Self) !?[]const u8 {
        if (self.request.body != null) return self.request.body;
        if (self.body_delegate) |delegate| {
            const data = try delegate.read_all(delegate.ptr);
            self.request.body = data;
            self.request.body_owned = false;
            self.body_delegate = null;
            return data;
        }
        if (self.h2_body_reader) |reader| {
            const data = reader.readAll(self.allocator) catch |err| switch (err) {
                error.EndOfStream => {
                    if (self.bodyFramingRequiresEndStream()) return err;
                    self.h2_body_reader = null;
                    return null;
                },
                else => return err,
            };
            self.request.body = data;
            self.request.body_owned = true;
            self.h2_body_reader = null;
            return self.request.body;
        }
        return null;
    }

    /// True when reading the body can still block on peer-controlled input.
    /// Application admission uses this instead of inspecting a concrete H2
    /// reader so linked and direct transports have identical semantics.
    pub fn hasStreamingRequestBody(self: *const Self) bool {
        if (self.h2_body_reader != null) return true;
        return if (self.body_delegate) |delegate| delegate.streaming else false;
    }

    pub fn isCancellationRequested(self: *const Self) bool {
        if (self.cancellation) |signal| {
            if (signal.load(.acquire)) return true;
        }
        if (self.cancellation_probe) |probe| return probe.requested();
        return false;
    }

    fn bodyFramingRequiresEndStream(self: *const Self) bool {
        if (self.request.headers.get(HeaderName.TRANSFER_ENCODING) != null) return true;
        const content_length = self.request.headers.getContentLength() orelse return false;
        return content_length != 0;
    }

    /// Reads the request body and hands the raw bytes to a caller-supplied parser.
    /// Returns null if no body is present.
    pub fn parseBody(self: *Self, comptime Parsed: type, parser: anytype) !?Parsed {
        const raw = (try self.body()) orelse return null;
        return try parser(self.allocator, raw);
    }

    /// Reads the request body and parses it as JSON into the given type.
    /// Returns null if no body is present. Caller must call .deinit() on result.
    pub fn parseJson(self: *Self, comptime T: type) !?std.json.Parsed(T) {
        const JsonParser = struct {
            fn parse(allocator: Allocator, raw: []const u8) !std.json.Parsed(T) {
                return try std.json.parseFromSlice(T, allocator, raw, .{
                    .ignore_unknown_fields = true,
                });
            }
        };
        return try self.parseBody(std.json.Parsed(T), JsonParser.parse);
    }

    /// Writer for server-side HTTP/2 streaming responses.
    /// Acquired via `ctx.streamH2()`. Each `write()` sends DATA frame(s)
    /// without END_STREAM. Call `close()` to send END_STREAM.
    pub const H2StreamWriter = struct {
        h2: *H2Connection,
        sock: *Socket,
        stream_id: u31,
        io: Io,
        closed: bool = false,

        /// Sends data as DATA frames without END_STREAM.
        /// Blocks if the flow-control window is exhausted, resuming
        /// when WINDOW_UPDATE frames are received from the peer.
        pub fn write(self: *H2StreamWriter, data: []const u8) !void {
            if (self.closed) return error.StreamClosed;
            self.h2.write_mutex.lockUncancelable(self.io);
            defer self.h2.write_mutex.unlock(self.io);
            try self.h2.writeDataBlocking(self.sock, self.stream_id, data, false);
        }

        /// Sends END_STREAM and marks the writer done.
        pub fn close(self: *H2StreamWriter) !void {
            if (self.closed) return;
            self.closed = true;
            self.h2.write_mutex.lockUncancelable(self.io);
            defer self.h2.write_mutex.unlock(self.io);
            try self.h2.writeData(self.sock, self.stream_id, &.{}, true);
        }

        /// Sends trailing HEADERS with END_STREAM (RFC 7540 §8.1).
        /// Used for gRPC status trailers, content digests, etc.
        /// Closes the writer — no further writes are allowed.
        pub fn sendTrailers(self: *H2StreamWriter, trailers: []const hpack.HeaderEntry) !void {
            if (self.closed) return error.StreamClosed;
            self.closed = true;
            self.h2.write_mutex.lockUncancelable(self.io);
            defer self.h2.write_mutex.unlock(self.io);
            try self.h2.sendHeaders(self.sock, self.stream_id, trailers, true);
        }
    };

    /// Sends HEADERS (without END_STREAM) and returns a writer for incremental
    /// DATA frames. The handler must call `writer.close()` when done.
    /// Only available for HTTP/2 streams.
    pub fn streamH2(self: *Self, status_code: u16, extra_headers: []const hpack.HeaderEntry) !H2StreamWriter {
        const h2 = self.h2 orelse return error.NotH2;
        const sock = self.h2_sock orelse return error.NotH2;

        var status_buf: [3]u8 = undefined;
        const h2_headers = try H2Connection.buildResponseHeaders(
            status_code,
            extra_headers,
            &status_buf,
            self.allocator,
        );
        defer self.allocator.free(h2_headers);

        h2.write_mutex.lockUncancelable(self.io);
        defer h2.write_mutex.unlock(self.io);
        try h2.sendHeaders(sock, self.h2_stream_id, h2_headers, false);

        self.h2_stream_sent = true;
        return .{ .h2 = h2, .sock = sock, .stream_id = self.h2_stream_id, .io = self.io };
    }

    /// Unified streaming response writer for both HTTP/1.1 and HTTP/2.
    /// Each `write()` sends data immediately to the client:
    /// - HTTP/1.1: chunked transfer encoding frames
    /// - HTTP/2: DATA frames (via H2StreamWriter)
    /// The handler must call `close()` when done to send the terminating
    /// chunk (HTTP/1.1) or END_STREAM (HTTP/2).
    pub const StreamWriter = struct {
        h1_sock: ?*Socket,
        h2_writer: ?H2StreamWriter,
        delegate: ?StreamDelegate = null,
        closed: bool = false,

        /// Sends a chunk of data to the client.
        pub fn write(self: *StreamWriter, data: []const u8) !void {
            if (self.closed) return error.StreamClosed;
            if (data.len == 0) return;

            if (self.delegate) |delegate| {
                try delegate.write(delegate.ptr, data);
            } else if (self.h2_writer) |*w| {
                try w.write(data);
            } else if (self.h1_sock) |sock| {
                // Write one HTTP/1.1 chunked frame: hex-size CRLF data CRLF
                var size_buf: [18]u8 = undefined; // max "FFFFFFFFFFFFFFFF\r\n"
                const size_str = std.fmt.bufPrint(&size_buf, "{x}\r\n", .{data.len}) catch unreachable;
                try sock.sendAll(size_str);
                try sock.sendAll(data);
                try sock.sendAll("\r\n");
            } else return error.StreamClosed;
        }

        /// Sends the terminating chunk / END_STREAM.
        pub fn close(self: *StreamWriter) !void {
            if (self.closed) return;
            self.closed = true;

            if (self.delegate) |delegate| {
                try delegate.close(delegate.ptr);
            } else if (self.h2_writer) |*w| {
                try w.close();
            } else if (self.h1_sock) |sock| {
                // Terminating chunk: "0\r\n\r\n"
                try sock.sendAll("0\r\n\r\n");
            }
        }

        /// Convenience: write a complete SSE event (formats `event:`, `data:`, trailing newline).
        pub fn writeEvent(self: *StreamWriter, event_name: ?[]const u8, data: []const u8) !void {
            return writeEventTo(self, event_name, data);
        }

        fn writeEventTo(writer: anytype, event_name: ?[]const u8, data: []const u8) !void {
            var required: usize = 1;
            if (event_name) |name| {
                required = std.math.add(usize, required, "event: ".len + 1) catch return error.EventTooLarge;
                required = std.math.add(usize, required, name.len) catch return error.EventTooLarge;
            }
            var size_lines = mem.splitScalar(u8, data, '\n');
            while (size_lines.next()) |line| {
                required = std.math.add(usize, required, "data: ".len + 1) catch return error.EventTooLarge;
                required = std.math.add(usize, required, line.len) catch return error.EventTooLarge;
            }

            // Keep the normal per-token path to one transport write. Oversized
            // completion/tool events fall back to equivalent incremental SSE
            // framing instead of failing an otherwise successful stream.
            var buf: [8192]u8 = undefined;
            if (required <= buf.len) {
                var pos: usize = 0;
                if (event_name) |name| {
                    @memcpy(buf[pos..][0.."event: ".len], "event: ");
                    pos += "event: ".len;
                    @memcpy(buf[pos..][0..name.len], name);
                    pos += name.len;
                    buf[pos] = '\n';
                    pos += 1;
                }
                var lines = mem.splitScalar(u8, data, '\n');
                while (lines.next()) |line| {
                    @memcpy(buf[pos..][0.."data: ".len], "data: ");
                    pos += "data: ".len;
                    @memcpy(buf[pos..][0..line.len], line);
                    pos += line.len;
                    buf[pos] = '\n';
                    pos += 1;
                }
                buf[pos] = '\n';
                pos += 1;
                std.debug.assert(pos == required);
                return writer.write(buf[0..pos]);
            }

            if (event_name) |name| {
                try writer.write("event: ");
                try writer.write(name);
                try writer.write("\n");
            }

            var lines = mem.splitScalar(u8, data, '\n');
            while (lines.next()) |line| {
                try writer.write("data: ");
                try writer.write(line);
                try writer.write("\n");
            }
            try writer.write("\n");
        }
    };

    /// Sends response headers and returns a `StreamWriter` for incremental body data.
    /// Works for both HTTP/1.1 (chunked transfer encoding) and HTTP/2 (DATA frames).
    /// The handler must call `writer.close()` when done.
    ///
    /// Usage:
    /// ```
    /// var writer = try ctx.streamResponse(200);
    /// try writer.writeEvent(null, "{\"token\": \"hello\"}");
    /// try writer.writeEvent(null, "[DONE]");
    /// try writer.close();
    /// return ctx.response.build(); // return value is ignored for streams
    /// ```
    pub fn streamResponse(self: *Self, status_code: u16) !StreamWriter {
        if (self.stream_delegate) |delegate| {
            try delegate.start(delegate.ptr, status_code);
            return .{ .h1_sock = null, .h2_writer = null, .delegate = delegate };
        }
        if (self.h2 != null) {
            // HTTP/2 path — delegate to existing streamH2
            const h2w = try self.streamH2(status_code, &.{
                .{ .name = "content-type", .value = "text/event-stream; charset=utf-8" },
                .{ .name = "cache-control", .value = "no-cache" },
            });
            return .{ .h1_sock = null, .h2_writer = h2w };
        }

        // HTTP/1.1 path — send headers with Transfer-Encoding: chunked
        const sock = self.h1_sock orelse return error.NoSocket;

        // Build and send headers-only response
        var resp = Response.init(self.allocator, status_code);
        defer resp.deinit();
        try resp.headers.set(HeaderName.CONTENT_TYPE, "text/event-stream; charset=utf-8");
        try resp.headers.set(HeaderName.CACHE_CONTROL, "no-cache");
        try resp.headers.set(HeaderName.CONNECTION, "keep-alive");
        try resp.headers.set(HeaderName.TRANSFER_ENCODING, "chunked");

        // Serialize headers only (no body)
        const header_bytes = try serializeToSlice(self.allocator, &resp);
        defer self.allocator.free(header_bytes);
        try sock.sendAll(header_bytes);

        self.h1_stream_sent = true;
        return .{ .h1_sock = sock, .h2_writer = null };
    }
};

/// A route handler with optional instance context.
///
/// Bound handlers make route ownership explicit and allow multiple server
/// instances to use the same generated router type without process-global
/// state. Plain functions remain supported for stateless handlers.
pub const Handler = union(enum) {
    function: *const fn (*Context) anyerror!Response,
    bound: Bound,
    wrapped: Wrapped,

    pub const Bound = struct {
        ptr: *anyopaque,
        call: *const fn (ptr: *anyopaque, ctx: *Context) anyerror!Response,
    };

    /// Non-recursive representation of the two base handler forms. Keeping
    /// Handler out of the stored wrapper and callback signature prevents each
    /// generated router from recursively expanding the handler type graph.
    const Inner = union(enum) {
        function: *const fn (*Context) anyerror!Response,
        bound: Bound,

        fn handler(self: Inner) Handler {
            return switch (self) {
                .function => |function| .{ .function = function },
                .bound => |bound| .{ .bound = bound },
            };
        }
    };

    pub const Wrapped = struct {
        ptr: *anyopaque,
        inner: Inner,
        call: *const fn (ptr: *anyopaque, inner: Inner, ctx: *Context) anyerror!Response,
    };

    /// Converts either an existing Handler or a plain handler function into
    /// the canonical stored representation.
    pub fn from(handler: anytype) Handler {
        if (@TypeOf(handler) == Handler) return handler;
        return .{ .function = handler };
    }

    /// Binds a handler method to an explicitly owned instance.
    pub fn bind(instance: anytype, comptime method: anytype) Handler {
        const Instance = @TypeOf(instance);
        comptime {
            switch (@typeInfo(Instance)) {
                .pointer => |pointer| {
                    if (pointer.size != .one) @compileError("httpx.Handler.bind requires a single-item pointer");
                    if (pointer.is_const) @compileError("httpx.Handler.bind currently requires a mutable instance pointer");
                },
                else => @compileError("httpx.Handler.bind requires an instance pointer"),
            }
        }

        const Adapter = struct {
            fn call(raw: *anyopaque, ctx: *Context) anyerror!Response {
                const typed: Instance = @ptrCast(@alignCast(raw));
                return @call(.never_inline, method, .{ typed, ctx });
            }
        };

        return .{ .bound = .{
            .ptr = @ptrCast(instance),
            .call = Adapter.call,
        } };
    }

    /// Wraps an existing handler without replacing or re-deriving its target.
    /// The wrapper instance must outlive every router that stores the result.
    pub fn wrap(instance: anytype, inner: Handler, comptime method: anytype) Handler {
        const Instance = @TypeOf(instance);
        comptime {
            switch (@typeInfo(Instance)) {
                .pointer => |pointer| {
                    if (pointer.size != .one) @compileError("httpx.Handler.wrap requires a single-item pointer");
                    if (pointer.is_const) @compileError("httpx.Handler.wrap currently requires a mutable instance pointer");
                },
                else => @compileError("httpx.Handler.wrap requires an instance pointer"),
            }
        }

        const Adapter = struct {
            fn call(raw: *anyopaque, wrapped: Inner, ctx: *Context) anyerror!Response {
                const typed: Instance = @ptrCast(@alignCast(raw));
                return @call(.never_inline, method, .{ typed, wrapped.handler(), ctx });
            }
        };

        return switch (inner) {
            .function => |function| .{ .wrapped = .{
                .ptr = @ptrCast(instance),
                .inner = .{ .function = function },
                .call = Adapter.call,
            } },
            .bound => |bound| .{ .wrapped = .{
                .ptr = @ptrCast(instance),
                .inner = .{ .bound = bound },
                .call = Adapter.call,
            } },
            .wrapped => @panic("httpx.Handler.wrap does not support nested wrappers"),
        };
    }

    pub fn invoke(self: Handler, ctx: *Context) anyerror!Response {
        return switch (self) {
            // Handler is the intentional type-erasure/code-generation
            // boundary for routers. Inlining through it duplicates large
            // generated and inference handlers for every wrapper and prefix.
            .function => |function| @call(.never_inline, function, .{ctx}),
            .bound => |bound| @call(.never_inline, bound.call, .{ bound.ptr, ctx }),
            .wrapped => |wrapped| @call(.never_inline, wrapped.call, .{ wrapped.ptr, wrapped.inner, ctx }),
        };
    }
};

/// HTTP Server.
pub const Server = struct {
    pub const RuntimeStats = struct {
        max_connections: u32,
        active_connections: usize,
        peak_active_connections: usize,
        active_requests: usize,
        peak_active_requests: usize,
        accept_errors_total: u64,
        connection_timeouts_total: u64,
        connection_dispatch_rejections_total: u64,
        request_dispatch_rejections_total: u64,
        h2_stream_dispatch_rejections_total: u64,
        request_cancellations_total: u64,
        body_buffer_capacity_bytes: usize,
        body_buffer_in_use_bytes: usize,
        body_buffer_peak_bytes: usize,
        body_buffer_rejected_total: u64,
    };

    allocator: Allocator,
    io: Io,
    config: ServerConfig,
    router: Router,
    middleware: std.ArrayListUnmanaged(Middleware) = .empty,
    pre_route_hooks: std.ArrayListUnmanaged(PreRouteHook) = .empty,
    global_handler: ?Handler = null,
    listener: ?TcpListener = null,
    running: bool = false,
    /// Cross-thread shutdown requests are published atomically. The listener
    /// thread remains the sole owner of `listener`, `running`, and the
    /// connection group; requestStop only wakes its accept loop.
    /// 0 = running, 1 = graceful drain, 2 = immediate cancellation. Immediate
    /// shutdown is terminal and wins races with graceful requests.
    shutdown_mode: std.atomic.Value(u8) = .init(0),
    graceful_timeout_ms: std.atomic.Value(u64) = .init(0),
    listen_started: std.atomic.Value(bool) = .init(false),
    /// Actual bound port, published for cross-thread wakeups when config.port
    /// is zero and the kernel selects an ephemeral port.
    wake_port: std.atomic.Value(u16) = .init(0),
    active_connections: std.atomic.Value(usize) = .init(0),
    peak_active_connections: std.atomic.Value(usize) = .init(0),
    active_requests: std.atomic.Value(usize) = .init(0),
    peak_active_requests: std.atomic.Value(usize) = .init(0),
    accept_errors_total: std.atomic.Value(u64) = .init(0),
    connection_timeouts_total: std.atomic.Value(u64) = .init(0),
    connection_dispatch_rejections_total: std.atomic.Value(u64) = .init(0),
    request_dispatch_rejections_total: std.atomic.Value(u64) = .init(0),
    h2_stream_dispatch_rejections_total: std.atomic.Value(u64) = .init(0),
    request_permits: std.atomic.Value(u32),
    request_cancellations_total: std.atomic.Value(u64) = .init(0),
    connection_controls_mutex: std.atomic.Mutex = .unlocked,
    connection_controls: std.ArrayListUnmanaged(*ConnectionControl) = .empty,
    connections: Io.Group = Io.Group.init,
    conn_semaphore: Io.Semaphore,
    h1_body_budget: SharedBodyBudget,
    waiting_for_connection_permit: std.atomic.Value(bool) = .init(false),
    body_budget: SharedBodyBudget,
    owned_http_runtime: HttpRuntime,
    /// Acquired before publishing a bound socket so transport cancellation is
    /// startable whenever callers advertise the listener as ready.
    http_runtime_lease: HttpRuntime.ListenerLease = .{},

    const Self = @This();

    /// Structured owner for a long-lived `Server.listen` task.
    ///
    /// The caller owns both the Server and the executor behind its Io value.
    /// A ListenerTask owns the submitted future and its separately allocated
    /// run state: it binds before returning from `start`, publishes shutdown to
    /// the listener, and makes joining explicit before the server or executor
    /// can be destroyed. The Future captures only the stable RunState pointer,
    /// so the ListenerTask handle itself remains safe to return or move after
    /// start.
    pub const ListenerTask = struct {
        server: *Self,
        io: Io,
        future: ?Io.Future(anyerror!void) = null,
        run_state: ?*RunState = null,
        state: State = .initialized,
        terminal_runtime_state: RuntimeState = .initialized,
        terminal_failure: ?anyerror = null,

        const RunState = struct {
            server: *Self,
            runtime_state: std.atomic.Value(RuntimeState) = .init(.running),
            failure: anyerror = error.Unexpected,
        };

        pub const State = enum {
            initialized,
            running,
            joined,
        };

        pub const RuntimeState = enum(u8) {
            initialized,
            running,
            stopped,
            failed,
        };

        pub fn init(server: *Self) ListenerTask {
            return .{ .server = server, .io = server.io };
        }

        pub fn start(self: *ListenerTask) !void {
            if (self.state != .initialized) return error.ListenerTaskAlreadyStarted;
            try self.server.bind();
            self.io = self.server.listenerIo();
            const run_state = try self.server.allocator.create(RunState);
            run_state.* = .{ .server = self.server };
            self.run_state = run_state;
            self.state = .running;
            self.terminal_runtime_state = .running;
            self.terminal_failure = null;
            self.future = self.io.concurrent(run, .{run_state}) catch |err| {
                self.server.allocator.destroy(run_state);
                self.run_state = null;
                self.state = .initialized;
                self.terminal_runtime_state = .initialized;
                return err;
            };
        }

        pub fn requestStop(self: *ListenerTask) void {
            if (self.state == .running) self.server.requestStop();
        }

        pub fn shutdown(self: *ListenerTask, timeout_ms: u64) void {
            if (self.state == .running) self.server.shutdown(timeout_ms);
        }

        pub fn join(self: *ListenerTask) !void {
            if (self.future) |*future| {
                defer {
                    self.future = null;
                    self.state = .joined;
                    const run_state = self.run_state.?;
                    self.terminal_runtime_state = run_state.runtime_state.load(.acquire);
                    self.server.allocator.destroy(run_state);
                    self.run_state = null;
                }
                future.await(self.io) catch |err| {
                    self.terminal_failure = err;
                    return err;
                };
                return;
            }
            if (self.state == .initialized) self.state = .joined;
            if (self.terminal_runtime_state == .initialized)
                self.terminal_runtime_state = .stopped;
        }

        pub fn isRunning(self: *const ListenerTask) bool {
            return self.state == .running;
        }

        /// Publish an unexpected terminal listener error without consuming the
        /// Future. The owner still must call `join` before deinitialization.
        pub fn runtimeFailure(self: *const ListenerTask) ?anyerror {
            if (self.run_state) |run_state| {
                return if (run_state.runtime_state.load(.acquire) == .failed) run_state.failure else null;
            }
            return if (self.terminal_runtime_state == .failed) self.terminal_failure else null;
        }

        pub fn runtimeState(self: *const ListenerTask) RuntimeState {
            if (self.run_state) |run_state| return run_state.runtime_state.load(.acquire);
            return self.terminal_runtime_state;
        }

        fn run(run_state: *RunState) anyerror!void {
            run_state.server.listen() catch |err| {
                run_state.failure = err;
                run_state.runtime_state.store(.failed, .release);
                return err;
            };
            run_state.runtime_state.store(.stopped, .release);
        }
    };

    const ConnectionControl = struct {
        socket: *Socket,
        h1_request_cancellation: *std.atomic.Value(bool),
        h2: ?*H2Connection = null,
        interrupted: std.atomic.Value(bool) = .init(false),

        /// Interrupts in-flight I/O without releasing the descriptor. The
        /// connection fiber is the sole descriptor owner and closes it after
        /// its handler has unwound.
        fn interrupt(self: *@This(), graceful: bool) void {
            self.h1_request_cancellation.store(true, .release);
            if (graceful) {
                if (self.h2) |h2| {
                    h2.write_mutex.lockUncancelable(h2.io);
                    h2.sendGoaway(self.socket, .no_error) catch {};
                    h2.write_mutex.unlock(h2.io);
                }
            }
            if (!self.interrupted.swap(true, .acq_rel)) {
                self.socket.shutdown();
            }
        }
    };

    const ConnectionContext = struct {
        socket: Socket,
        control: ConnectionControl,
        h1_request_cancellation: std.atomic.Value(bool) = .init(false),
    };

    /// Creates a server with default configuration.
    pub fn init(allocator: Allocator, io: Io) Self {
        return initWithConfig(allocator, io, .{});
    }

    /// Creates a server with custom configuration.
    pub fn initWithConfig(allocator: Allocator, io: Io, config: ServerConfig) Self {
        const cfg = config.normalized();

        return .{
            .allocator = allocator,
            .io = io,
            .config = cfg,
            .router = Router.init(allocator),
            .conn_semaphore = .{ .permits = cfg.max_connections },
            .request_permits = .init(cfg.max_request_tasks),
            .h1_body_budget = SharedBodyBudget.init(if (cfg.max_h1_inflight_bodies == 0) cfg.max_connections else cfg.max_h1_inflight_bodies),
            .body_budget = SharedBodyBudget.init(cfg.request_body_buffer_budget_bytes),
            .owned_http_runtime = HttpRuntime.init(allocator, .{
                .max_active_h1_requests = cfg.max_connections,
                .max_active_connections = cfg.max_connections,
                .max_active_requests = cfg.max_request_tasks,
            }),
        };
    }

    /// Lock-free snapshot suitable for health and metrics endpoints. The
    /// configured limit is immutable after initialization and the remaining
    /// fields are atomically maintained by the accept/request paths.
    pub fn runtimeStats(self: *const Self) RuntimeStats {
        const body = self.body_budget.stats();
        return .{
            .max_connections = self.config.max_connections,
            .active_connections = self.active_connections.load(.acquire),
            .peak_active_connections = self.peak_active_connections.load(.acquire),
            .active_requests = self.active_requests.load(.acquire),
            .peak_active_requests = self.peak_active_requests.load(.acquire),
            .accept_errors_total = self.accept_errors_total.load(.acquire),
            .connection_timeouts_total = self.connection_timeouts_total.load(.acquire),
            .connection_dispatch_rejections_total = self.connection_dispatch_rejections_total.load(.acquire),
            .request_dispatch_rejections_total = self.request_dispatch_rejections_total.load(.acquire),
            .h2_stream_dispatch_rejections_total = self.h2_stream_dispatch_rejections_total.load(.acquire),
            .request_cancellations_total = self.request_cancellations_total.load(.acquire),
            .body_buffer_capacity_bytes = body.capacity,
            .body_buffer_in_use_bytes = body.in_use,
            .body_buffer_peak_bytes = body.peak_in_use,
            .body_buffer_rejected_total = body.rejected_total,
        };
    }

    /// Process/runtime-scoped transport statistics. Unlike `runtimeStats`,
    /// these values are shared by every Server injected with the same
    /// HttpRuntime and must be exported exactly once by that runtime's owner.
    pub fn httpRuntimeStats(self: *const Self) HttpRuntime.Stats {
        return (self.config.http_runtime orelse @constCast(&self.owned_http_runtime)).stats();
    }

    /// Releases all server resources.
    pub fn deinit(self: *Self) void {
        if (self.listener) |*l| l.deinit();
        self.listener = null;
        self.http_runtime_lease.release();
        self.owned_http_runtime.deinit();
        self.router.deinit();
        self.middleware.deinit(self.allocator);
        self.pre_route_hooks.deinit(self.allocator);
        self.connection_controls.deinit(self.allocator);
    }

    /// Adds middleware to the server.
    pub fn use(self: *Self, mw: Middleware) !void {
        try self.middleware.append(self.allocator, mw);
    }

    /// Adds a pre-route hook executed before route matching.
    pub fn preRoute(self: *Self, hook: PreRouteHook) !void {
        try self.pre_route_hooks.append(self.allocator, hook);
    }

    /// Registers a global fallback handler for unmatched routes.
    pub fn global(self: *Self, handler: anytype) void {
        self.global_handler = Handler.from(handler);
    }

    /// Registers a route handler.
    pub fn route(self: *Self, method: types.Method, path: []const u8, handler: anytype) !void {
        try self.router.add(method, path, handler);
    }

    /// Registers a route with borrowed opaque data copied into Context.
    pub fn routeWithData(self: *Self, method: types.Method, path: []const u8, handler: anytype, data: *anyopaque) !void {
        try self.router.addWithData(method, path, handler, data);
    }

    /// Registers a GET route.
    pub fn get(self: *Self, path: []const u8, handler: anytype) !void {
        try self.route(.GET, path, handler);
    }

    /// Registers a POST route.
    pub fn post(self: *Self, path: []const u8, handler: anytype) !void {
        try self.route(.POST, path, handler);
    }

    /// Registers a PUT route.
    pub fn put(self: *Self, path: []const u8, handler: anytype) !void {
        try self.route(.PUT, path, handler);
    }

    /// Registers a DELETE route.
    pub fn delete(self: *Self, path: []const u8, handler: anytype) !void {
        try self.route(.DELETE, path, handler);
    }

    /// Registers a PATCH route.
    pub fn patch(self: *Self, path: []const u8, handler: anytype) !void {
        try self.route(.PATCH, path, handler);
    }

    /// Registers a HEAD route.
    pub fn head(self: *Self, path: []const u8, handler: anytype) !void {
        try self.route(.HEAD, path, handler);
    }

    /// Registers an OPTIONS route.
    pub fn options(self: *Self, path: []const u8, handler: anytype) !void {
        try self.route(.OPTIONS, path, handler);
    }

    /// Registers a handler for all standard HTTP methods on a path.
    pub fn any(self: *Self, path: []const u8, handler: anytype) !void {
        const normalized = Handler.from(handler);
        try self.route(.GET, path, normalized);
        try self.route(.POST, path, normalized);
        try self.route(.PUT, path, normalized);
        try self.route(.DELETE, path, normalized);
        try self.route(.PATCH, path, normalized);
        try self.route(.HEAD, path, normalized);
        try self.route(.OPTIONS, path, normalized);
        try self.route(.TRACE, path, normalized);
        try self.route(.CONNECT, path, normalized);
    }

    /// Binds the server socket without starting the accept loop.
    /// Call `boundAddress()` after this to get the actual address
    /// (useful when port 0 is used for OS-assigned ports).
    pub fn bind(self: *Self) !void {
        if (self.listener != null) return;
        if ((self.config.tls_cert_path == null) != (self.config.tls_key_path == null))
            return error.InvalidServerTlsConfiguration;
        // Direct server TLS is not implemented. Reject it before reserving
        // runtime or socket capacity so deployments cannot accidentally serve
        // plaintext on a port configured as HTTPS. The supported boundary is
        // explicit TLS termination in a reverse proxy/load balancer.
        if (self.config.tls_cert_path != null) return error.ServerTlsUnsupported;
        const observer_capacity = if (self.config.h1_disconnect_cancellation == .required and
            self.config.h1_disconnect_probe == null)
            self.config.max_connections
        else
            0;
        var http_runtime_lease = try (self.config.http_runtime orelse &self.owned_http_runtime).acquireListener(.{
            .max_h1_requests = observer_capacity,
            .max_connections = if (self.config.connection_execution == .concurrent) self.config.max_connections else 0,
            .max_requests = self.config.max_request_tasks,
        });
        errdefer http_runtime_lease.release();
        const addr = try Address.parse(self.config.host, self.config.port);
        const backlog_u32: u32 = @max(self.config.max_connections, 1);
        const backlog: u31 = @intCast(@min(backlog_u32, @as(u32, std.math.maxInt(u31))));
        var listener = try TcpListener.initWithOptions(addr, http_runtime_lease.connectionIo(), .{
            .kernel_backlog = backlog,
            .reuse_address = self.config.reuse_address,
            .reuse_port = self.config.reuse_port,
        });
        errdefer listener.deinit();
        const bound = listener.getLocalAddress();
        const port = switch (bound) {
            .ip4 => |ip4| ip4.port,
            .ip6 => |ip6| ip6.port,
        };
        self.wake_port.store(port, .release);
        self.http_runtime_lease = http_runtime_lease;
        self.listener = listener;
    }

    /// Returns the bound listener address, or null if not yet bound.
    pub fn boundAddress(self: *Self) ?Address {
        var l = self.listener orelse return null;
        return l.getLocalAddress();
    }

    /// Starts the server and begins accepting connections.
    /// Uses Io.Group.concurrent to spawn a fiber per connection. Executor
    /// saturation rejects that connection and leaves the accept loop live.
    /// Callers that require serial serving must select `.serial` explicitly.
    pub fn listen(self: *Self) !void {
        // A server is single-use once stopped. In particular, do not let a
        // startup/shutdown race re-bind a listener after its owner has begun
        // tearing down the handler state referenced by this server.
        if (self.shutdown_mode.load(.acquire) != 0) return;
        if (self.listener != null and self.http_runtime_lease.runtime == null)
            return error.ServerAlreadyListened;
        if (self.listener == null) try self.bind();
        defer self.http_runtime_lease.release();
        if (self.shutdown_mode.load(.acquire) != 0) return;
        self.running = true;
        self.listen_started.store(true, .release);
        defer self.listen_started.store(false, .release);
        if (self.shutdown_mode.load(.acquire) != 0) {
            self.running = false;
            return;
        }

        var accept_error_backoff_ms = self.config.accept_error_backoff_initial_ms;
        while (self.running and self.shutdown_mode.load(.acquire) == 0) {
            // Block accept loop when at max concurrent connections.
            // Gate before accept so we don't hold open sockets while waiting.
            self.waiting_for_connection_permit.store(true, .release);
            if (self.shutdown_mode.load(.acquire) != 0) {
                // If stop won the announcement handshake it published exactly
                // one wake permit. Consume that permit before leaving so a
                // later listen cycle cannot exceed max_connections.
                if (!self.waiting_for_connection_permit.swap(false, .acq_rel)) {
                    self.conn_semaphore.waitUncancelable(self.connectionIo());
                }
                break;
            }
            self.conn_semaphore.waitUncancelable(self.connectionIo());
            const stop_published_wake = !self.waiting_for_connection_permit.swap(false, .acq_rel);
            if (self.shutdown_mode.load(.acquire) != 0) {
                // Without a published wake, the wait consumed a real capacity
                // permit; restore it. With a wake, the net permit count is
                // already unchanged even if another permit was also available.
                if (!stop_published_wake) self.conn_semaphore.post(self.connectionIo());
                break;
            }

            const conn = self.listener.?.accept() catch |err| {
                self.conn_semaphore.post(self.connectionIo());
                if (!self.running or self.shutdown_mode.load(.acquire) != 0 or self.listener == null) break;
                _ = self.accept_errors_total.fetchAdd(1, .monotonic);
                std.log.warn("httpx accept failed; backing off delay_ms={d} err={s}", .{ accept_error_backoff_ms, @errorName(err) });
                self.io.sleep(Io.Duration.fromMilliseconds(accept_error_backoff_ms), .awake) catch {};
                accept_error_backoff_ms = @min(self.config.accept_error_backoff_max_ms, accept_error_backoff_ms *| 2);
                continue;
            };
            accept_error_backoff_ms = self.config.accept_error_backoff_initial_ms;
            if (self.shutdown_mode.load(.acquire) != 0) {
                var wake_socket = conn.socket;
                wake_socket.close();
                self.conn_semaphore.post(self.connectionIo());
                break;
            }

            // Spawn a lightweight fiber to handle this connection concurrently.
            const connection = self.allocator.create(ConnectionContext) catch {
                var rejected_socket = conn.socket;
                rejected_socket.close();
                self.conn_semaphore.post(self.connectionIo());
                continue;
            };
            connection.* = .{
                .socket = conn.socket,
                .control = .{
                    .socket = &connection.socket,
                    .h1_request_cancellation = &connection.h1_request_cancellation,
                },
            };
            if ((self.config.http_runtime orelse &self.owned_http_runtime).supportsNativeSocketTimeouts())
                connection.socket.enableNativeTimeouts();
            self.registerConnection(&connection.control) catch {
                connection.socket.close();
                self.allocator.destroy(connection);
                self.conn_semaphore.post(self.connectionIo());
                continue;
            };
            const active = self.active_connections.fetchAdd(1, .acq_rel) + 1;
            updateAtomicMax(&self.peak_active_connections, active);
            switch (self.config.connection_execution) {
                .serial => self.handleConnection(connection) catch |err| {
                    self.recordConnectionError(err);
                    logConnectionError(err);
                },
                .concurrent => self.connections.concurrent(self.connectionIo(), handleConnectionFiber, .{ self, connection }) catch {
                    _ = self.connection_dispatch_rejections_total.fetchAdd(1, .monotonic);
                    _ = self.active_connections.fetchSub(1, .acq_rel);
                    self.unregisterConnection(&connection.control);
                    connection.socket.close();
                    self.allocator.destroy(connection);
                    self.conn_semaphore.post(self.connectionIo());
                },
            }
        }

        self.running = false;
        if (self.shutdown_mode.load(.acquire) == 1) self.drainRequests(self.graceful_timeout_ms.load(.acquire));
        self.closeConnections();
        if (self.active_connections.load(.acquire) != 0) self.connections.cancel(self.connectionIo());
        // Wait for all in-flight connections to finish before returning.
        self.connections.await(self.connectionIo()) catch {};
        self.shutdown_mode.store(0, .release);
    }

    fn listenerIo(self: *const Self) Io {
        return self.http_runtime_lease.listenerIo();
    }

    fn connectionIo(self: *const Self) Io {
        return self.http_runtime_lease.connectionIo();
    }

    fn requestIo(self: *const Self) Io {
        return self.http_runtime_lease.requestIo();
    }

    fn drainRequests(self: *Self, timeout_ms: u64) void {
        if (timeout_ms == 0 or self.active_requests.load(.acquire) == 0) return;
        const started_ns = Io.Clock.awake.now(self.io).nanoseconds;
        const timeout_ns: i128 = @as(i128, timeout_ms) * std.time.ns_per_ms;
        while (self.active_requests.load(.acquire) != 0) {
            if (self.shutdown_mode.load(.acquire) == 2) return;
            const now_ns = Io.Clock.awake.now(self.io).nanoseconds;
            if (now_ns - started_ns >= timeout_ns) return;
            const remaining_ms: u64 = @intCast(@max(@as(i128, 1), @divFloor(timeout_ns - (now_ns - started_ns), std.time.ns_per_ms)));
            self.io.sleep(Io.Duration.fromMilliseconds(@intCast(@min(remaining_ms, 10))), .awake) catch return;
        }
    }

    /// Requests shutdown from another OS thread without touching listener or
    /// connection-group state. A loopback connection wakes a blocked accept;
    /// posting the semaphore also wakes a listener blocked at its connection
    /// limit. All mutable server teardown remains on the listener thread.
    pub fn requestStop(self: *Self) void {
        var observed = self.shutdown_mode.load(.acquire);
        while (observed != 2) {
            observed = self.shutdown_mode.cmpxchgWeak(observed, 2, .acq_rel, .acquire) orelse break;
        }
        self.wakeListener();
    }

    fn wakeListener(self: *Self) void {
        // Before listen() publishes ownership there is no accept/admission wait
        // to wake and the runtime lease may not exist yet (or may already have
        // been released after listen returned). A concurrent stop still wins:
        // listen() checks shutdown_mode on both sides of bind/startup.
        if (!self.listen_started.load(.acquire)) return;

        // Only publish a permit when the listener has announced that it may
        // block in the admission gate. The listener consumes this permit before
        // leaving, so repeated stop/listen cycles cannot inflate capacity.
        if (self.waiting_for_connection_permit.swap(false, .acq_rel)) {
            self.conn_semaphore.post(self.connectionIo());
        }

        const published_port = self.wake_port.load(.acquire);
        const port = if (published_port != 0) published_port else self.config.port;
        if (port == 0) return;
        var addr = Address.parse(self.config.host, port) catch return;
        switch (addr) {
            .ip4 => |ip4| if (std.mem.allEqual(u8, &ip4.bytes, 0)) {
                addr = .{ .ip4 = .loopback(ip4.port) };
            },
            .ip6 => |ip6| if (std.mem.allEqual(u8, &ip6.bytes, 0)) {
                addr = .{ .ip6 = .loopback(ip6.port) };
            },
        }
        var wake_socket = Socket.connect(addr, self.listenerIo()) catch return;
        wake_socket.close();
    }

    /// Requests immediate server shutdown. This method is safe to call from a
    /// different OS thread; listener and connection teardown are performed by
    /// the listener thread before listen() returns.
    pub fn stop(self: *Self) void {
        self.requestStop();
    }

    /// Publishes a graceful shutdown request from any ordinary OS thread or
    /// fiber. The listener thread stops accepting, drains active connections
    /// for at most `timeout_ms`, then cancels the remainder before `listen()`
    /// returns. Callers that need synchronous completion should join the thread
    /// running `listen()`. Signal handlers must still notify ordinary code.
    pub fn shutdown(self: *Self, timeout_ms: u64) void {
        self.graceful_timeout_ms.store(timeout_ms, .release);
        _ = self.shutdown_mode.cmpxchgStrong(0, 1, .acq_rel, .acquire);
        self.wakeListener();
    }

    /// Fiber entry point for concurrent connection handling.
    /// Signature returns `Io.Cancelable!void` as required by Group.concurrent.
    fn handleConnectionFiber(self: *Self, connection: *ConnectionContext) Io.Cancelable!void {
        self.handleConnection(connection) catch |err| {
            self.recordConnectionError(err);
            logConnectionError(err);
        };
    }

    fn recordConnectionError(self: *Self, err: anyerror) void {
        if (err == error.Timeout) _ = self.connection_timeouts_total.fetchAdd(1, .monotonic);
    }

    fn routeErrorResponseStatus(self: *Self, err: anyerror) ?u16 {
        const status = routeErrorStatus(err);
        if (status == null) _ = self.request_cancellations_total.fetchAdd(1, .monotonic);
        return status;
    }

    fn cancelH2Stream(h2: *H2Connection, sock: *Socket, stream_id: u31) void {
        h2.write_mutex.lockUncancelable(h2.io);
        defer h2.write_mutex.unlock(h2.io);
        const stream = h2.stream_manager.getStream(stream_id) orelse return;
        if (stream.state == .idle or stream.state == .closed) return;
        h2.sendRstStream(sock, stream_id, .cancel) catch {};
        stream.reset();
    }

    fn logConnectionError(err: anyerror) void {
        switch (err) {
            // Normal peer-abandonment and shutdown races are accounted for by
            // admission/cancellation metrics. Logging one line per abandoned
            // socket recreates the overload log storm this server is meant to
            // contain and obscures actionable listener failures.
            error.Timeout,
            error.Canceled,
            error.EndOfStream,
            error.ConnectionResetByPeer,
            error.BrokenPipe,
            error.RecvFailed,
            error.SendFailed,
            error.InvalidSocketOption,
            => {},
            else => std.debug.print("Connection error: {}\n", .{err}),
        }
    }

    const H1ApplicationResult = struct {
        response: ?Response = null,
        status: ?u16 = null,
        suppress_body: bool = false,
    };

    /// Executes transport-independent HTTP/1 application work on the bounded
    /// request lane. The connection task remains the sole parser and response
    /// lifecycle owner and waits for this result before reusing the socket.
    fn executeH1Application(self: *Self, ctx: *Context, req: *Request) anyerror!H1ApplicationResult {
        for (self.pre_route_hooks.items) |hook| try hook(ctx);

        var suppress_body = false;
        var params_buf: [16]RouteParam = undefined;
        var route_result = self.router.find(req.method, req.uri.path, &params_buf);
        if (route_result == null and req.method == .HEAD) {
            route_result = self.router.find(.GET, req.uri.path, &params_buf);
            suppress_body = route_result != null;
        }

        if (route_result) |matched_route| {
            ctx.params = matched_route.params;
            ctx.route_data = matched_route.data;
            return .{
                .response = try self.executeMiddleware(ctx, matched_route.handler),
                .suppress_body = suppress_body,
            };
        }

        var allow_methods: [16]types.Method = undefined;
        const allow_count = self.router.allowedMethods(req.uri.path, &allow_methods);
        if (req.method == .OPTIONS and allow_count > 0) {
            var response = Response.init(self.allocator, 204);
            errdefer response.deinit();
            try self.setAllowHeader(&response.headers, allow_methods[0..allow_count]);
            return .{ .response = response };
        }
        if (allow_count > 0) {
            var response = Response.init(self.allocator, 405);
            errdefer response.deinit();
            try self.setAllowHeader(&response.headers, allow_methods[0..allow_count]);
            return .{ .response = response };
        }
        if (self.global_handler) |global_handler| {
            return .{ .response = try self.executeMiddleware(ctx, global_handler) };
        }
        return .{ .status = 404 };
    }

    /// Handles a single connection.
    fn handleConnection(self: *Self, connection: *ConnectionContext) !void {
        defer _ = self.active_connections.fetchSub(1, .acq_rel);
        defer self.conn_semaphore.post(self.connectionIo());
        defer self.allocator.destroy(connection);
        defer connection.socket.close();
        defer self.unregisterConnection(&connection.control);
        var sock = connection.socket;

        // Set initial timeout once; only update when transitioning to keep-alive.
        if (self.config.header_read_timeout_ms > 0) {
            try sock.setRecvTimeout(self.config.header_read_timeout_ms);
        }
        if (self.config.response_write_timeout_ms > 0) {
            try sock.setSendTimeout(self.config.response_write_timeout_ms);
        }

        // Peek at the first bytes to detect HTTP/2 "prior knowledge" (RFC 7540 §3.4).
        // The h2 preface is "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n" (24 bytes).
        var peek_buf: [8192]u8 = undefined;
        const first_header_deadline_ms = deadlineAfter(self.io, self.config.header_read_timeout_ms);
        try applyReadDeadline(&sock, self.io, first_header_deadline_ms);
        const first_n = try sock.recv(&peek_buf);
        if (first_n == 0) return;

        if (first_n >= 24 and mem.eql(u8, peek_buf[0..24], http.HTTP2_PREFACE)) {
            return self.handleH2Connection(&connection.control, &sock, peek_buf[24..first_n]);
        }

        // HTTP/1.1 path — feed the already-read bytes to the parser.
        var parser = Parser.init(self.allocator);
        defer parser.deinit();
        parser.max_body_size = self.config.max_body_size;
        parser.max_headers = self.config.max_headers;
        parser.body_budget = &self.body_budget;

        var first_request = true;
        var request_active = false;
        defer if (request_active) self.finishRequest();
        var request_count: u32 = 0;
        var first_recv_done = true; // We already did the first recv.
        var buffer: [8192]u8 = undefined;
        var leftover: usize = 0;
        while (self.running and self.shutdown_mode.load(.acquire) == 0) {
            parser.reset();
            var h1_body_reserved = false;
            defer if (h1_body_reserved) self.h1_body_budget.release(1);

            // Keep-alive idle, header ingress, and body ingress are separate
            // absolute phases. Bytes cannot renew any of these deadlines.
            var waiting_for_request_bytes = !first_request and leftover == 0;
            var body_deadline_started = false;
            var deadline_ms = if (first_request)
                first_header_deadline_ms
            else
                deadlineAfter(
                    self.io,
                    if (waiting_for_request_bytes) self.config.keep_alive_timeout_ms else self.config.header_read_timeout_ms,
                );

            // Feed any leftover bytes from the previous request (pipelining)
            // before reading from the socket.
            if (leftover > 0) {
                const consumed = parser.feed(buffer[0..leftover]) catch |err| switch (err) {
                    error.BodyTooLarge => {
                        try self.sendError(&sock, 413);
                        return;
                    },
                    error.BodyCapacityExceeded => {
                        try self.sendError(&sock, 429);
                        return;
                    },
                    error.HeaderTooLarge, error.TooManyHeaders => {
                        try self.sendError(&sock, 431);
                        return;
                    },
                    error.InvalidHeader, error.InvalidChunkEncoding => {
                        try self.sendError(&sock, 400);
                        return;
                    },
                    else => return err,
                };
                if (consumed < leftover) {
                    std.mem.copyForwards(u8, buffer[0 .. leftover - consumed], buffer[consumed..leftover]);
                }
                leftover -= consumed;
                if (parser.isError()) {
                    try self.sendError(&sock, 400);
                    return;
                }
                if (!self.reserveH1BodyAfterHeaders(&parser, &h1_body_reserved)) {
                    try self.sendError(&sock, 429);
                    return;
                }
                if (!body_deadline_started and parser.hasCompleteHeaders() and !parser.isComplete()) {
                    body_deadline_started = true;
                    deadline_ms = deadlineAfter(self.io, self.config.body_read_timeout_ms);
                }
            }

            while (!parser.isComplete()) {
                if (deadline_ms > 0 and milliTimestamp(self.io) >= deadline_ms) {
                    if (!waiting_for_request_bytes) try self.sendError(&sock, 408);
                    return;
                }
                // On the very first iteration, feed the bytes we already read
                // during protocol detection instead of doing another recv.
                const n = if (first_recv_done) blk: {
                    @memcpy(buffer[0..first_n], peek_buf[0..first_n]);
                    first_recv_done = false;
                    break :blk first_n;
                } else blk: {
                    applyReadDeadline(&sock, self.io, deadline_ms) catch {
                        if (!waiting_for_request_bytes) try self.sendError(&sock, 408);
                        return;
                    };
                    const received = sock.recv(&buffer) catch |err| switch (err) {
                        error.Timeout => {
                            if (!waiting_for_request_bytes) try self.sendError(&sock, 408);
                            return;
                        },
                        else => return err,
                    };
                    if (waiting_for_request_bytes and received > 0) {
                        waiting_for_request_bytes = false;
                        deadline_ms = deadlineAfter(self.io, self.config.header_read_timeout_ms);
                    }
                    break :blk received;
                };
                if (n == 0) return;
                const consumed = parser.feed(buffer[0..n]) catch |err| switch (err) {
                    error.BodyTooLarge => {
                        try self.sendError(&sock, 413);
                        return;
                    },
                    error.BodyCapacityExceeded => {
                        try self.sendError(&sock, 429);
                        return;
                    },
                    error.HeaderTooLarge, error.TooManyHeaders => {
                        try self.sendError(&sock, 431);
                        return;
                    },
                    error.InvalidHeader, error.InvalidChunkEncoding => {
                        try self.sendError(&sock, 400);
                        return;
                    },
                    else => return err,
                };
                // Track unconsumed bytes for h2c upgrade or pipelining.
                if (consumed < n) {
                    std.mem.copyForwards(u8, buffer[0 .. n - consumed], buffer[consumed..n]);
                }
                leftover = n - consumed;
                if (parser.isError()) {
                    try self.sendError(&sock, 400);
                    return;
                }
                if (!self.reserveH1BodyAfterHeaders(&parser, &h1_body_reserved)) {
                    try self.sendError(&sock, 429);
                    return;
                }
                if (!body_deadline_started and parser.hasCompleteHeaders() and !parser.isComplete()) {
                    body_deadline_started = true;
                    deadline_ms = deadlineAfter(self.io, self.config.body_read_timeout_ms);
                }
            }

            if (h1_body_reserved) {
                self.h1_body_budget.release(1);
                h1_body_reserved = false;
            }

            if (!self.tryStartRequest()) {
                self.recordRequestDispatchRejection();
                try self.sendError(&sock, 503);
                return;
            }
            request_active = true;

            var req = try Request.init(
                self.allocator,
                parser.method orelse .GET,
                parser.path orelse "/",
            );
            defer req.deinit();
            req.version = parser.version;

            // Borrow headers and body from the parser without copying.
            // The parser outlives the request within this loop iteration:
            // req is deinitialized via `defer req.deinit()` before
            // `parser.reset()` at the top of the next iteration.
            for (parser.headers.iterator()) |h| {
                try req.headers.appendBorrowed(h.name, h.value);
            }

            if (parser.getBody().len > 0) {
                req.body = parser.getBody();
                req.body_owned = false;
            }

            // RFC 7231 §5.1.1: Respond to Expect: 100-continue so the
            // client knows it's safe to send the body.
            if (req.headers.get(HeaderName.EXPECT)) |expect| {
                if (std.ascii.eqlIgnoreCase(expect, "100-continue")) {
                    // Reject before telling client to send, if Content-Length exceeds limit.
                    if (parser.content_length) |cl| {
                        if (cl > self.config.max_body_size) {
                            try sock.sendAll("HTTP/1.1 413 Content Too Large\r\n\r\n");
                            return;
                        }
                    }
                    try sock.sendAll("HTTP/1.1 100 Continue\r\n\r\n");
                }
            }

            // RFC 7540 §3.2: h2c upgrade — switch to HTTP/2 over cleartext.
            // Pass any bytes beyond the parsed request (H2 preface pipelined
            // in the same TCP segment) as initial_data for the H2 reader.
            if (first_request and http.isH2cUpgradeRequest(&req.headers)) {
                // Transfer request accounting to the H2 upgrade handler. It
                // completes stream 1 before entering the long-lived H2 loop.
                // Transfer the already-counted request permit to stream 1;
                // the upgrade handler releases it as soon as that request
                // finishes, before entering the long-lived frame loop.
                request_active = false;
                return self.handleH2cUpgrade(&connection.control, &sock, &req, buffer[0..leftover]);
            }

            var ctx = Context.init(self.allocator, self.io, &req);
            ctx.max_file_size = self.config.max_file_size;
            ctx.h1_sock = &sock;
            // A non-empty suffix is not necessarily a pipelined request: it
            // can be a partial or malformed request line. Preserve this fact
            // for handlers without mutating the live parser or buffer.
            ctx.h1_has_buffered_input = hasCompleteBufferedH1Request(self.allocator, buffer[0..leftover], self.config);
            connection.h1_request_cancellation.store(false, .release);
            ctx.cancellation = &connection.h1_request_cancellation;
            var cancellation_registration: CancellationObserver.Registration = .{};
            var disconnect_probe_context: H1DisconnectProbeContext = undefined;
            if (self.config.h1_disconnect_cancellation == .required) {
                if (self.config.h1_disconnect_probe) |probe| {
                    disconnect_probe_context = .{
                        .probe = probe,
                        .handle = sock.handle,
                        .cancellation = &connection.h1_request_cancellation,
                        .runtime = self.config.http_runtime orelse &self.owned_http_runtime,
                    };
                    ctx.cancellation_probe = .{
                        .ptr = &disconnect_probe_context,
                        .is_cancelled = H1DisconnectProbeContext.requested,
                    };
                } else {
                    cancellation_registration = (self.config.http_runtime orelse &self.owned_http_runtime).registerH1Request(
                        sock.handle,
                        &connection.h1_request_cancellation,
                    ) catch {
                        try self.sendError(&sock, 503);
                        return;
                    };
                }
            }
            defer cancellation_registration.deinit();
            defer ctx.deinit();

            var request_future = self.requestIo().concurrent(executeH1Application, .{ self, &ctx, &req }) catch {
                self.recordRequestDispatchRejection();
                self.finishRequest();
                request_active = false;
                try self.sendError(&sock, 503);
                return;
            };
            const application = request_future.await(self.requestIo()) catch |err| {
                const status = self.routeErrorResponseStatus(err) orelse return;
                std.debug.print("HTTP/1 application handler error: {}\n", .{err});
                // Once streaming headers are committed, another HTTP
                // response would corrupt the connection. Closing the
                // connection is the only valid terminal action.
                if (ctx.h1_stream_sent) return;
                return self.sendError(&sock, status);
            };
            if (application.status) |status| return self.sendError(&sock, status);
            var response = application.response.?;
            defer response.deinit();
            const suppress_body = application.suppress_body;

            // If the handler used streamResponse(), the response was already
            // sent directly on the socket (chunked transfer encoding).
            // The handler should have called writer.close() which sends the
            // terminating "0\r\n\r\n" chunk, leaving the connection in a
            // clean state for the next request.
            if (ctx.h1_stream_sent) {
                ctx.h1_stream_sent = false;
                self.finishRequest();
                request_active = false;
                // Check if the client wants keep-alive
                const stream_keep_alive = self.config.keep_alive and
                    req.headers.isKeepAlive(req.version) and
                    self.shutdown_mode.load(.acquire) == 0;
                if (!stream_keep_alive) return;

                request_count += 1;
                if (self.config.max_requests_per_connection > 0 and
                    request_count >= self.config.max_requests_per_connection)
                    return;

                if (first_request) first_request = false;
                self.io.sleep(Io.Duration.zero, .awake) catch {};
                continue;
            }

            if (suppress_body) {
                if (response.body_owned) {
                    if (response.body) |body| self.allocator.free(body);
                    response.body_owned = false;
                }
                response.body = null;
            }

            const request_wants_keep_alive = req.headers.isKeepAlive(req.version);
            // Handlers may deliberately shed an overloaded request and ask the
            // peer to reconnect later. Honor a response-side Connection: close
            // instead of advertising closure while retaining the socket and its
            // connection-admission permit until the keep-alive timeout.
            const response_wants_keep_alive = response.headers.isKeepAlive(req.version);
            const reaches_request_limit = self.config.max_requests_per_connection > 0 and
                request_count + 1 >= self.config.max_requests_per_connection;
            const keep_alive = self.config.keep_alive and request_wants_keep_alive and response_wants_keep_alive and
                !reaches_request_limit and self.shutdown_mode.load(.acquire) == 0;
            if (!keep_alive) {
                try response.headers.set(HeaderName.CONNECTION, "close");
            }

            try ensureContentLengthHeader(&response);
            try ensureDateHeader(self.io, &response);

            try sendBuffered(self.allocator, &sock, &response);

            self.finishRequest();
            request_active = false;

            if (!keep_alive) return;

            // Drain any unread request body before reusing the connection
            // for the next request, similar to Go's net/http finishRequest.
            if (parser.content_length) |cl| {
                const body_read = parser.getBody().len;
                var remaining: u64 = if (cl > body_read) cl - body_read else 0;
                const max_drain: u64 = 256 * 1024; // Match Go's 256 KB limit.
                if (remaining > max_drain) return; // Too much to drain; close.
                var drain_buf: [8192]u8 = undefined;
                while (remaining > 0) {
                    const to_read = @min(remaining, drain_buf.len);
                    const n = sock.recv(drain_buf[0..@intCast(to_read)]) catch return;
                    if (n == 0) return;
                    remaining -= n;
                }
            }

            request_count += 1;
            if (self.config.max_requests_per_connection > 0 and
                request_count >= self.config.max_requests_per_connection)
                return;

            if (first_request) {
                first_request = false;
            }
            self.io.sleep(Io.Duration.zero, .awake) catch {};
        }
    }

    /// Admit an HTTP/1 request body as soon as headers identify it, rather
    /// than after the parser has waited for an attacker-controlled upload.
    fn reserveH1BodyAfterHeaders(self: *Self, parser: *const Parser, reserved: *bool) bool {
        if (reserved.* or !parser.hasCompleteHeaders() or parser.isComplete()) return true;
        if (!self.h1_body_budget.tryReserve(1)) return false;
        reserved.* = true;
        return true;
    }

    /// Handles an HTTP/1.1 → HTTP/2 upgrade (h2c, RFC 7540 §3.2).
    /// Sends 101 Switching Protocols, handles the original request as stream 1,
    /// then enters the normal H2 receive loop for subsequent requests.
    /// `initial_h2_data` contains any bytes pipelined beyond the upgrade request.
    fn handleH2cUpgrade(self: *Self, control: *ConnectionControl, sock: *Socket, original_req: *Request, initial_h2_data: []const u8) !void {
        var stream1_request_active = true;
        defer if (stream1_request_active) self.finishRequest();
        // 1. Send 101 Switching Protocols.
        try sock.sendAll("HTTP/1.1 101 Switching Protocols\r\nConnection: Upgrade\r\nUpgrade: h2c\r\n\r\n");

        // 2. Decode the HTTP2-Settings header (base64url → SETTINGS payload).
        const settings_b64 = original_req.headers.get(HeaderName.HTTP2_SETTINGS) orelse
            return error.MissingH2cSettings;
        const settings_payload = try http.decodeH2cSettings(settings_b64, self.allocator);
        defer self.allocator.free(settings_payload);

        // 3. Create H2 connection and apply peer settings.
        var h2 = H2Connection.initServer(self.allocator, self.io);
        defer h2.deinit();
        self.setH2Control(control, &h2);
        defer self.clearH2Control(control);
        h2.max_stream_data_size = self.config.max_body_size;
        h2.recv_data_budget = &self.body_budget;
        h2.local_settings.initial_window_size = self.config.h2_initial_window_size;
        h2.local_settings.max_concurrent_streams = self.config.h2_max_concurrent_streams;
        try h2.applyPeerSettings(settings_payload);

        // 4. Send server SETTINGS.
        try h2.sendSettings(sock);

        // Increase connection-level recv window (same as handleH2Connection).
        if (self.config.h2_initial_window_size > 65535) {
            const delta: u31 = @intCast(self.config.h2_initial_window_size - 65535);
            try h2.sendWindowUpdate(sock, 0, delta);
            try h2.stream_manager.updateConnectionRecvWindow(@intCast(delta));
        }

        // 5. Read client h2 preface + SETTINGS before handling stream 1.
        // RFC 7540 §3.2: The client sends its connection preface immediately
        // after the 101 response. We must process it before sending any
        // stream-level frames so flow control settings are applied correctly.
        var h2c_reader = H2SocketReader{ .socket = sock, .initial = initial_h2_data, .initial_pos = 0 };

        try h2.readClientPreface(&h2c_reader);

        // Read client's SETTINGS frame.
        var settings_frame = try h2.readFrame(&h2c_reader);
        defer settings_frame.deinit(self.allocator);
        // RFC 7540 §3.5: First client frame MUST be a non-ACK SETTINGS.
        if (settings_frame.header.frame_type != .settings) return error.ProtocolError;
        if (settings_frame.header.flags & H2Connection.FLAG_ACK != 0) return error.ProtocolError;
        try h2.handleSettings(&settings_frame, sock);

        // 6. Handle the original HTTP/1.1 request as stream 1.
        _ = try h2.stream_manager.getOrCreateStream(1);
        original_req.version = .HTTP_2;
        var stream1_processed = false;
        var stream1_future = self.requestIo().concurrent(
            routeAndRespondH2,
            .{ self, &h2, sock, 1, original_req, null },
        ) catch null;
        if (stream1_future) |*future| {
            const result = future.await(self.requestIo());
            self.finishRequest();
            stream1_request_active = false;
            result catch |err| {
                // RFC 7540 §6.8: Send GOAWAY before closing so the client
                // knows stream 1 was processed (or at least attempted).
                h2.sendGoaway(sock, .no_error) catch {};
                h2.stream_manager.removeStream(1);
                return err;
            };
            stream1_processed = true;
        } else {
            self.recordH2StreamDispatchRejection();
            self.finishRequest();
            stream1_request_active = false;
            h2.write_mutex.lockUncancelable(h2.io);
            h2.sendRstStream(sock, 1, .refused_stream) catch {};
            h2.stream_manager.removeStream(1);
            h2.write_mutex.unlock(h2.io);
        }
        if (stream1_processed) {
            h2.stream_manager.removeStream(1);
            // Stream 1 was fully processed; record it so GOAWAY advertises the
            // correct last_stream_id and clients don't retry it (RFC 7540 §6.8).
            h2.last_processed_stream_id = 1;
        }

        // 7. Enter the normal H2 receive loop for subsequent requests.

        // Set socket recv timeout for idle detection (same as handleH2Connection).
        if (self.config.h2_idle_timeout_ms > 0) {
            sock.setRecvTimeout(self.config.h2_idle_timeout_ms) catch {};
        }

        // Run the standard H2 frame loop.
        var stream_fibers = Io.Group.init;
        defer {
            stream_fibers.await(self.requestIo()) catch {};
            h2.sendGoaway(sock, .no_error) catch {};
        }
        // Wake all blocked handler fibers before awaiting them (reverse defer order).
        defer h2.signalAllStreams(error.ConnectionClosed);

        // Count stream 1 only if application processing actually began.
        var h2c_request_count: u32 = @intFromBool(stream1_processed);
        var last_activity: i64 = milliTimestamp(self.io);
        while (!h2.goaway_received and self.shouldContinueH2()) {
            if (self.shutdown_mode.load(.acquire) == 1 and !h2.goaway_sent) {
                h2.write_mutex.lockUncancelable(h2.io);
                h2.sendGoaway(sock, .no_error) catch {};
                h2.write_mutex.unlock(h2.io);
            }
            // H2 idle timeout (mirrors handleH2Connection).
            if (self.config.h2_idle_timeout_ms > 0 and
                h2ActiveStreamCount(&h2) == 0)
            {
                const idle_ms = milliTimestamp(self.io) - last_activity;
                if (idle_ms >= @as(i64, @intCast(self.config.h2_idle_timeout_ms))) break;
            }
            if (self.config.h2_max_requests > 0 and h2c_request_count >= self.config.h2_max_requests) {
                break;
            }

            const maybe_sid = h2.processOneFrameLocked(&h2c_reader, sock) catch |err| switch (err) {
                error.ConnectionClosed => break,
                error.RecvFailed => continue,
                else => {
                    // RFC 7540 §5.4.1: Send GOAWAY with the correct error
                    // code before closing on connection-level errors.
                    h2.sendGoaway(sock, .protocol_error) catch {};
                    return err;
                },
            };

            const sid = maybe_sid orelse continue;
            last_activity = milliTimestamp(self.io);
            const data_event = self.allocator.create(Io.Event) catch {
                h2.write_mutex.lockUncancelable(h2.io);
                h2.sendRstStream(sock, sid, .internal_error) catch {};
                h2.stream_manager.removeStream(sid);
                h2.write_mutex.unlock(h2.io);
                continue;
            };
            data_event.* = .unset;
            if (!self.claimH2StreamForHandler(&h2, sock, sid, data_event)) {
                self.allocator.destroy(data_event);
                continue;
            }
            h2c_request_count += 1;

            // Reserve drain ownership before publishing the handler fiber. A
            // concurrent graceful shutdown must not observe an accepted stream
            // as idle during the scheduler handoff.
            if (!self.tryStartRequest()) {
                self.rejectH2StreamDispatch(&h2, sock, sid, data_event, false);
                continue;
            }
            stream_fibers.concurrent(self.requestIo(), handleH2StreamFiber, .{ self, &h2, sock, sid, data_event }) catch {
                self.rejectH2StreamDispatch(&h2, sock, sid, data_event, true);
            };
        }
    }

    /// Handles an HTTP/2 connection after the 24-byte preface has been consumed.
    /// `initial_data` contains any bytes read beyond the preface from the first recv.
    ///
    /// Runs a receive loop that reads frames and delivers them to per-stream
    /// mailboxes. When a stream is complete, a handler task is submitted to
    /// the bounded H2 lane. Saturation resets the stream with REFUSED_STREAM;
    /// it must never run inline on the sole frame pump.
    fn handleH2Connection(self: *Self, control: *ConnectionControl, sock: *Socket, initial_data: []const u8) !void {
        var h2 = H2Connection.initServer(self.allocator, self.io);
        defer h2.deinit();
        self.setH2Control(control, &h2);
        defer self.clearH2Control(control);
        h2.max_stream_data_size = self.config.max_body_size;
        h2.recv_data_budget = &self.body_budget;
        h2.local_settings.initial_window_size = self.config.h2_initial_window_size;
        h2.local_settings.max_concurrent_streams = self.config.h2_max_concurrent_streams;

        // Set socket recv timeout so the receive loop unblocks periodically,
        // allowing the idle timeout check to re-evaluate. Without this,
        // processOneFrameLocked blocks indefinitely on I/O and the idle
        // check at the top of the loop never re-executes.
        if (self.config.h2_idle_timeout_ms > 0) {
            sock.setRecvTimeout(self.config.h2_idle_timeout_ms) catch {};
        }

        // Wrap socket in a reader that first yields `initial_data`, then reads from socket.
        var h2_reader = H2SocketReader{ .socket = sock, .initial = initial_data, .initial_pos = 0 };

        // RFC 7540 §3.5: Server SETTINGS MUST be the first frame the server sends.
        // Send our SETTINGS before reading/ACKing the client's.
        try h2.sendSettings(sock);

        // RFC 7540 §6.9.1: SETTINGS INITIAL_WINDOW_SIZE only affects stream-level
        // windows. Increase the connection-level recv window to match so large
        // request bodies don't stall on connection-level flow control.
        if (self.config.h2_initial_window_size > 65535) {
            const delta: u31 = @intCast(self.config.h2_initial_window_size - 65535);
            try h2.sendWindowUpdate(sock, 0, delta);
            try h2.stream_manager.updateConnectionRecvWindow(@intCast(delta));
        }

        // Now read and ACK the client's SETTINGS frame (follows the preface).
        var settings_frame = try h2.readFrame(&h2_reader);
        defer settings_frame.deinit(self.allocator);
        // RFC 7540 §3.5: First client frame MUST be a non-ACK SETTINGS.
        if (settings_frame.header.frame_type != .settings) return error.ProtocolError;
        if (settings_frame.header.flags & H2Connection.FLAG_ACK != 0) return error.ProtocolError;
        try h2.handleSettings(&settings_frame, sock);

        // Per-stream handler fibers. Awaited before h2 is deinitialized.
        var stream_fibers = Io.Group.init;
        defer {
            // Wait for all in-flight handler fibers to finish.
            stream_fibers.await(self.requestIo()) catch {};
            // Send GOAWAY with no_error if we haven't already, so the peer
            // can distinguish a clean close from a truncation.
            if (!h2.goaway_sent) {
                h2.sendGoaway(sock, .no_error) catch {};
            }
        }
        // Wake all blocked handler fibers before awaiting them (reverse defer order).
        defer h2.signalAllStreams(error.ConnectionClosed);

        // Receive loop: reads frames, handles connection-level traffic, and
        // delivers stream-level frames to per-stream mailboxes. Uses the
        // locked variant so SETTINGS ACK / PING / WINDOW_UPDATE writes are
        // serialized with handler fibers' response writes.
        var h2_request_count: u32 = 0;
        var last_activity: i64 = milliTimestamp(self.io);
        while (!h2.goaway_received and self.shouldContinueH2()) {
            if (self.shutdown_mode.load(.acquire) == 1 and !h2.goaway_sent) {
                h2.write_mutex.lockUncancelable(h2.io);
                h2.sendGoaway(sock, .no_error) catch {};
                h2.write_mutex.unlock(h2.io);
            }
            // H2 idle timeout: initiate graceful shutdown if no streams are
            // active and idle threshold is exceeded.
            if (self.config.h2_idle_timeout_ms > 0 and
                h2ActiveStreamCount(&h2) == 0)
            {
                const idle_ms = milliTimestamp(self.io) - last_activity;
                if (idle_ms >= @as(i64, @intCast(self.config.h2_idle_timeout_ms))) break;
            }

            // H2 max requests: send GOAWAY when limit is reached.
            if (self.config.h2_max_requests > 0 and h2_request_count >= self.config.h2_max_requests) {
                break;
            }

            const maybe_sid = h2.processOneFrameLocked(&h2_reader, sock) catch |err| switch (err) {
                error.ConnectionClosed => break,
                // Socket recv timeout (from h2_idle_timeout_ms) surfaces as
                // RecvFailed. Re-enter the loop so the idle check runs.
                error.RecvFailed => continue,
                else => {
                    // RFC 7540 §5.4.1: Send GOAWAY with the correct error
                    // code before closing on connection-level errors.
                    h2.sendGoaway(sock, .protocol_error) catch {};
                    return err;
                },
            };

            const sid = maybe_sid orelse continue;
            // Only update idle timer on stream-level frames. Connection-level
            // frames (PING, SETTINGS ACK, WINDOW_UPDATE on stream 0) must not
            // reset the timer — otherwise a client can hold a connection open
            // indefinitely by sending periodic PINGs without opening streams.
            last_activity = milliTimestamp(self.io);
            const data_event = self.allocator.create(Io.Event) catch {
                // Alloc failure: reject the stream to avoid a leak.
                h2.write_mutex.lockUncancelable(h2.io);
                h2.sendRstStream(sock, sid, .internal_error) catch {};
                h2.stream_manager.removeStream(sid);
                h2.write_mutex.unlock(h2.io);
                continue;
            };
            data_event.* = .unset;
            if (!self.claimH2StreamForHandler(&h2, sock, sid, data_event)) {
                self.allocator.destroy(data_event);
                continue;
            }
            h2_request_count += 1;

            // The frame pump must remain the sole reader. If the bounded H2
            // task lane is saturated, reject before application execution;
            // running inline here can deadlock a streaming body waiting for
            // DATA that only this loop can receive.
            if (!self.tryStartRequest()) {
                self.rejectH2StreamDispatch(&h2, sock, sid, data_event, false);
                continue;
            }
            stream_fibers.concurrent(self.requestIo(), handleH2StreamFiber, .{ self, &h2, sock, sid, data_event }) catch {
                self.rejectH2StreamDispatch(&h2, sock, sid, data_event, true);
            };
        }
    }

    /// Fiber entry point for per-stream HTTP/2 request handling.
    fn handleH2StreamFiber(self: *Self, h2: *H2Connection, sock: *Socket, stream_id: u31, data_event: *Io.Event) Io.Cancelable!void {
        self.handleH2Stream(h2, sock, stream_id, data_event) catch |err| {
            std.debug.print("H2 stream handler error: {}\n", .{err});
        };
    }

    /// Handles a single HTTP/2 stream: reads pre-decoded headers from the
    /// mailbox, routes the request, and sends the response. Dispatched as
    /// soon as HEADERS arrive — the body may still be streaming.
    fn handleH2Stream(self: *Self, h2: *H2Connection, sock: *Socket, stream_id: u31, data_event: *Io.Event) !void {
        defer self.finishRequest();
        // Ensure cleanup: detach event from stream, remove stream, free event.
        // All stream map mutations happen under write_mutex so the receive
        // loop (which holds write_mutex in processOneFrameLocked) cannot
        // concurrently access data_buf while it's being freed. Removing
        // the stream atomically with clearing data_event also prevents the
        // TOCTOU race where the receive loop sees data_event==null on a
        // still-present stream and double-dispatches a handler fiber.
        defer {
            {
                h2.write_mutex.lockUncancelable(h2.io);
                defer h2.write_mutex.unlock(h2.io);
                if (h2.stream_manager.getStream(stream_id)) |s| {
                    // A response can end locally while the peer is still
                    // uploading (for example a 429 from body admission). Reset
                    // every stream whose remote side remains open so rejected
                    // bodies stop consuming socket and flow-control capacity.
                    if (s.state != .idle and s.state != .closed and !s.end_stream_received) {
                        const is_body_capacity = if (s.stream_error) |err| err == error.BodyCapacityExceeded else false;
                        const code: http.Http2ErrorCode = if (is_body_capacity)
                            .enhance_your_calm
                        else
                            .cancel;
                        h2.sendRstStream(sock, stream_id, code) catch {};
                    }
                    s.data_event = null;
                }
                h2.stream_manager.removeStream(stream_id);
            }
            self.allocator.destroy(data_event);
        }

        // The receive loop mutates StreamManager while holding write_mutex.
        // Snapshot the separately allocated Stream pointer under that lock,
        // then release it before parsing or routing so DATA delivery can run.
        const stream = getH2Stream(h2, stream_id) orelse return;

        // Headers were decoded in the receive loop's deliverToMailbox to
        // avoid concurrent HPACK decode races on the shared hpack_ctx.
        const decoded_headers = stream.request_headers orelse return;

        // Extract pseudo-headers → build a Request.
        // RFC 7540 §8.1.2.1: pseudo-headers MUST appear before regular
        // headers, MUST NOT be duplicated, :method and :path are required.
        var method_str: ?[]const u8 = null;
        var path: ?[]const u8 = null;
        var authority: ?[]const u8 = null;
        var scheme: ?[]const u8 = null;
        var past_pseudo = false;

        var extra_headers = Headers.init(self.allocator);
        defer extra_headers.deinit();

        for (decoded_headers) |h| {
            if (h.name.len > 0 and h.name[0] == ':') {
                // RFC 7540 §8.1.2.1: pseudo-headers after regular headers
                // is a stream error (PROTOCOL_ERROR).
                if (past_pseudo) {
                    try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                    return;
                }
                if (mem.eql(u8, h.name, ":method")) {
                    if (method_str != null) {
                        try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                        return;
                    }
                    method_str = h.value;
                } else if (mem.eql(u8, h.name, ":path")) {
                    if (path != null) {
                        try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                        return;
                    }
                    path = h.value;
                } else if (mem.eql(u8, h.name, ":authority")) {
                    if (authority != null) {
                        try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                        return;
                    }
                    authority = h.value;
                } else if (mem.eql(u8, h.name, ":scheme")) {
                    if (scheme != null) {
                        try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                        return;
                    }
                    scheme = h.value;
                } else {
                    // Unknown pseudo-header → stream error.
                    try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                    return;
                }
            } else {
                past_pseudo = true;
                // RFC 7540 §8.1.2.2: Reject connection-specific headers.
                if (isH2ForbiddenHeader(h.name, h.value)) {
                    try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                    return;
                }
                try extra_headers.append(h.name, h.value);
            }
        }

        // RFC 7540 §8.1.2.3: :method and :path are required for all
        // request methods except CONNECT.
        const m_str = method_str orelse {
            try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
            return;
        };
        const is_connect = mem.eql(u8, m_str, "CONNECT");
        if (is_connect) {
            // RFC 7540 §8.3: CONNECT MUST have :authority, MUST NOT have :path or :scheme.
            if (authority == null or path != null or scheme != null) {
                try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
                return;
            }
        } else if (path == null) {
            try self.sendH2ErrorLocked(h2, sock, stream_id, 400);
            return;
        }

        const req_method = types.Method.fromString(m_str) orelse .GET;
        var req = try Request.init(self.allocator, req_method, path orelse "/");
        defer req.deinit();
        req.version = .HTTP_2;

        for (extra_headers.entries.items) |entry| {
            try req.headers.appendBorrowed(entry.name, entry.value);
        }
        if (authority) |auth| {
            try req.headers.appendBorrowed("host", auth);
        }

        // DATA admission may fail after the handler fiber is dispatched but
        // before it installs a streaming body reader. Never route a partial
        // request body: return the retryable overload response while the
        // stream is still owned by this handler. Errors that originate at the
        // peer or in protocol validation remain terminal and must not produce
        // additional response frames.
        if (stream.stream_error) |err| {
            if (err == error.BodyCapacityExceeded) {
                try self.sendH2ErrorLocked(h2, sock, stream_id, 429);
                return;
            }
            return err;
        }

        // If the body already arrived (stream completed before handler ran or
        // headers-only request), set it directly. Otherwise provide a streaming reader.
        var body_reader: ?Context.H2StreamReader = null;
        if (stream.completed) {
            if (stream.data_buf.items.len > 0) {
                req.body = stream.data_buf.items;
                req.body_owned = false;
            }
        } else {
            body_reader = .{
                .h2_stream = stream,
                .io = self.io,
                .data_event = data_event,
                .deadline_ms = deadlineAfter(self.io, self.config.body_read_timeout_ms),
            };
        }

        try self.routeAndRespondH2(h2, sock, stream_id, &req, if (body_reader != null) &body_reader.? else null);
    }

    /// Claims this listener's request quota before publishing application
    /// work to the shared runtime. Runtime reservations guarantee aggregate
    /// executor capacity; this local permit prevents one listener from using
    /// capacity reserved for another.
    fn tryStartRequest(self: *Self) bool {
        var observed = self.request_permits.load(.acquire);
        while (observed != 0) {
            if (self.request_permits.cmpxchgWeak(observed, observed - 1, .acq_rel, .acquire)) |actual| {
                observed = actual;
                continue;
            }
            const active = self.active_requests.fetchAdd(1, .acq_rel) + 1;
            updateAtomicMax(&self.peak_active_requests, active);
            return true;
        }
        return false;
    }

    fn finishRequest(self: *Self) void {
        const previous_active = self.active_requests.fetchSub(1, .acq_rel);
        std.debug.assert(previous_active > 0);
        const previous_permits = self.request_permits.fetchAdd(1, .release);
        std.debug.assert(previous_permits < self.config.max_request_tasks);
    }

    fn recordRequestDispatchRejection(self: *Self) void {
        _ = self.request_dispatch_rejections_total.fetchAdd(1, .monotonic);
    }

    fn recordH2StreamDispatchRejection(self: *Self) void {
        self.recordRequestDispatchRejection();
        _ = self.h2_stream_dispatch_rejections_total.fetchAdd(1, .monotonic);
    }

    fn shouldContinueH2(self: *Self) bool {
        const mode = self.shutdown_mode.load(.acquire);
        return mode != 2 and (self.running or (mode == 1 and self.active_requests.load(.acquire) != 0));
    }

    fn updateAtomicMax(counter: *std.atomic.Value(usize), value: usize) void {
        var observed = counter.load(.acquire);
        while (observed < value) {
            if (counter.cmpxchgWeak(observed, value, .acq_rel, .acquire) == null) return;
            observed = counter.load(.acquire);
        }
    }

    fn registerConnection(self: *Self, control: *ConnectionControl) !void {
        self.lockConnectionControls();
        defer self.connection_controls_mutex.unlock();
        try self.connection_controls.append(self.allocator, control);
    }

    fn unregisterConnection(self: *Self, control: *ConnectionControl) void {
        self.lockConnectionControls();
        defer self.connection_controls_mutex.unlock();
        for (self.connection_controls.items, 0..) |candidate, index| {
            if (candidate == control) {
                _ = self.connection_controls.swapRemove(index);
                return;
            }
        }
    }

    fn closeConnections(self: *Self) void {
        self.lockConnectionControls();
        defer self.connection_controls_mutex.unlock();
        const graceful = self.shutdown_mode.load(.acquire) == 1;
        for (self.connection_controls.items) |control| control.interrupt(graceful);
    }

    fn setH2Control(self: *Self, control: *ConnectionControl, h2: *H2Connection) void {
        self.lockConnectionControls();
        defer self.connection_controls_mutex.unlock();
        control.h2 = h2;
    }

    fn clearH2Control(self: *Self, control: *ConnectionControl) void {
        self.lockConnectionControls();
        defer self.connection_controls_mutex.unlock();
        control.h2 = null;
    }

    fn lockConnectionControls(self: *Self) void {
        while (!self.connection_controls_mutex.tryLock()) std.atomic.spinLoopHint();
    }

    /// Routes a request through middleware and sends the H2 response.
    /// Shared between normal H2 streams (from HPACK) and h2c upgrade (from HTTP/1.1).
    fn routeAndRespondH2(self: *Self, h2: *H2Connection, sock: *Socket, stream_id: u31, req: *Request, body_reader: ?*Context.H2StreamReader) !void {
        var ctx = Context.init(self.allocator, self.io, req);
        ctx.max_file_size = self.config.max_file_size;
        ctx.h2 = h2;
        ctx.h2_sock = sock;
        ctx.h2_body_reader = body_reader;
        ctx.h2_stream_id = stream_id;
        if (getH2Stream(h2, stream_id)) |stream| {
            ctx.cancellation = &stream.cancellation;
        }
        defer ctx.deinit();

        for (self.pre_route_hooks.items) |hook| {
            hook(&ctx) catch |err| {
                const status = self.routeErrorResponseStatus(err) orelse {
                    cancelH2Stream(h2, sock, stream_id);
                    return;
                };
                if (!ctx.h2_stream_sent) try self.sendH2ErrorLocked(h2, sock, stream_id, status);
                return;
            };
        }

        var suppress_body = false;
        var params_buf: [16]RouteParam = undefined;
        var route_result = self.router.find(req.method, req.uri.path, &params_buf);

        if (route_result == null and req.method == .HEAD) {
            route_result = self.router.find(.GET, req.uri.path, &params_buf);
            suppress_body = route_result != null;
        }

        if (route_result) |r| {
            ctx.params = r.params;
            ctx.route_data = r.data;
        }

        var response: Response = undefined;
        if (route_result) |r| {
            response = self.executeMiddleware(&ctx, r.handler) catch |err| {
                const status = self.routeErrorResponseStatus(err) orelse {
                    cancelH2Stream(h2, sock, stream_id);
                    return;
                };
                if (!ctx.h2_stream_sent) try self.sendH2ErrorLocked(h2, sock, stream_id, status);
                return;
            };
        } else {
            // Mirror the HTTP/1.1 path: 405 with Allow header for known paths,
            // 204 for OPTIONS, 404 otherwise.
            var allow_methods: [16]types.Method = undefined;
            const allow_count = self.router.allowedMethods(req.uri.path, &allow_methods);
            if (req.method == .OPTIONS and allow_count > 0) {
                response = Response.init(self.allocator, 204);
                errdefer response.deinit();
                try self.setAllowHeader(&response.headers, allow_methods[0..allow_count]);
            } else if (allow_count > 0) {
                response = Response.init(self.allocator, 405);
                errdefer response.deinit();
                try self.setAllowHeader(&response.headers, allow_methods[0..allow_count]);
            } else if (self.global_handler) |global_handler| {
                response = self.executeMiddleware(&ctx, global_handler) catch |err| {
                    const status = self.routeErrorResponseStatus(err) orelse {
                        cancelH2Stream(h2, sock, stream_id);
                        return;
                    };
                    if (!ctx.h2_stream_sent) try self.sendH2ErrorLocked(h2, sock, stream_id, status);
                    return;
                };
            } else {
                response = Response.init(self.allocator, 404);
            }
        }

        // If the handler used streamH2(), it already sent HEADERS+DATA.
        if (ctx.h2_stream_sent) {
            response.deinit();
            return;
        }
        defer response.deinit();

        if (suppress_body) {
            if (response.body_owned) {
                if (response.body) |b| self.allocator.free(b);
                response.body_owned = false;
            }
            response.body = null;
        }

        // RFC 7231 §7.1.1.2: origin servers SHOULD include a Date header.
        try ensureDateHeader(self.io, &response);

        // Build response headers outside the lock.
        var resp_extra = std.ArrayListUnmanaged(hpack.HeaderEntry).empty;
        defer resp_extra.deinit(self.allocator);

        try appendH2ResponseHeaders(self.allocator, &resp_extra, &response.headers);

        var status_buf: [3]u8 = undefined;
        const h2_headers = try H2Connection.buildResponseHeaders(
            response.status.code,
            resp_extra.items,
            &status_buf,
            self.allocator,
        );
        defer self.allocator.free(h2_headers);

        const has_body = response.body != null and response.body.?.len > 0;

        // Acquire write mutex for HPACK encoding + frame serialization.
        h2.write_mutex.lockUncancelable(h2.io);
        defer h2.write_mutex.unlock(h2.io);

        h2.sendHeaders(sock, stream_id, h2_headers, !has_body) catch |err| {
            // Mark stream closed so activeStreamCount doesn't keep counting it.
            if (h2.stream_manager.getStream(stream_id)) |s| s.reset();
            return err;
        };

        if (has_body) {
            h2.writeDataBlocking(sock, stream_id, response.body.?, true) catch |err| {
                if (h2.stream_manager.getStream(stream_id)) |s| s.reset();
                return err;
            };
        }
    }

    /// Sends the same safe JSON error envelope used by HTTP/1.
    fn sendH2Error(self: *Self, h2: *H2Connection, writer: anytype, stream_id: u31, code: u16) !void {
        const body = routeErrorBody(code);
        var length_buf: [32]u8 = undefined;
        const length = try std.fmt.bufPrint(&length_buf, "{d}", .{body.len});
        var extra = [_]hpack.HeaderEntry{
            .{ .name = "content-type", .value = "application/json" },
            .{ .name = "content-length", .value = length },
            .{ .name = "retry-after", .value = "1" },
        };
        const extra_len: usize = if (code == 429) extra.len else extra.len - 1;
        var status_buf: [3]u8 = undefined;
        const h2_headers = try H2Connection.buildResponseHeaders(code, extra[0..extra_len], &status_buf, self.allocator);
        defer self.allocator.free(h2_headers);
        try h2.sendHeaders(writer, stream_id, h2_headers, false);
        try h2.writeDataBlocking(writer, stream_id, body, true);
    }

    /// Like `sendH2Error` but acquires the write mutex first.
    fn sendH2ErrorLocked(self: *Self, h2: *H2Connection, writer: anytype, stream_id: u31, code: u16) !void {
        h2.write_mutex.lockUncancelable(h2.io);
        defer h2.write_mutex.unlock(h2.io);
        try self.sendH2Error(h2, writer, stream_id, code);
    }

    /// StreamManager stores Stream allocations separately from its hash map.
    /// Synchronize the lookup with frame-pump map mutations; the returned
    /// pointer remains stable until handler cleanup removes it under the same
    /// mutex.
    fn getH2Stream(h2: *H2Connection, stream_id: u31) ?*Stream {
        h2.write_mutex.lockUncancelable(h2.io);
        defer h2.write_mutex.unlock(h2.io);
        return h2.stream_manager.getStream(stream_id);
    }

    /// Atomically claims a received H2 stream for one handler fiber. The
    /// frame pump and handler cleanup both mutate StreamManager, so map lookup,
    /// admission, reset, removal, and data_event publication share its mutex.
    fn claimH2StreamForHandler(self: *Self, h2: *H2Connection, sock: *Socket, stream_id: u31, data_event: *Io.Event) bool {
        h2.write_mutex.lockUncancelable(h2.io);
        defer h2.write_mutex.unlock(h2.io);

        const stream = h2.stream_manager.getStream(stream_id) orelse return false;
        if (!stream.got_headers or stream.data_event != null) return false;
        if (stream.stream_error != null) {
            h2.stream_manager.removeStream(stream_id);
            return false;
        }
        if (self.shutdown_mode.load(.acquire) != 0 or
            h2.stream_manager.activeStreamCount() > h2.local_max_concurrent_streams)
        {
            h2.sendRstStream(sock, stream_id, .refused_stream) catch {};
            h2.stream_manager.removeStream(stream_id);
            return false;
        }
        stream.data_event = data_event;
        return true;
    }

    /// Unwinds a stream which was claimed and counted but could not be
    /// published to the bounded handler lane. REFUSED_STREAM tells the client
    /// that application processing did not begin and the request is safe to
    /// retry on another connection (RFC 7540 section 8.1.4).
    fn rejectH2StreamDispatch(
        self: *Self,
        h2: *H2Connection,
        sock: anytype,
        stream_id: u31,
        data_event: *Io.Event,
        request_started: bool,
    ) void {
        self.recordH2StreamDispatchRejection();
        if (request_started) self.finishRequest();
        h2.write_mutex.lockUncancelable(h2.io);
        if (h2.stream_manager.getStream(stream_id)) |stream| {
            stream.cancellation.store(true, .release);
            stream.data_event = null;
            h2.sendRstStream(sock, stream_id, .refused_stream) catch {};
            h2.stream_manager.removeStream(stream_id);
        }
        h2.write_mutex.unlock(h2.io);
        self.allocator.destroy(data_event);
    }

    fn h2ActiveStreamCount(h2: *H2Connection) usize {
        h2.write_mutex.lockUncancelable(h2.io);
        defer h2.write_mutex.unlock(h2.io);
        return h2.stream_manager.activeStreamCount();
    }

    /// Sends an error response.
    fn sendError(self: *Self, socket: *Socket, code: u16) !void {
        var resp = Response.init(self.allocator, code);
        defer resp.deinit();

        resp.body = routeErrorBody(code);
        try resp.headers.set(HeaderName.CONTENT_TYPE, "application/json");
        // Every sendError caller terminates the HTTP/1 connection. Make that
        // lifecycle explicit so clients do not return the socket to a pool.
        try resp.headers.set(HeaderName.CONNECTION, "close");
        if (code == 429) try resp.headers.set("Retry-After", "1");
        try ensureContentLengthHeader(&resp);
        try ensureDateHeader(self.io, &resp);

        try sendBuffered(self.allocator, socket, &resp);
    }

    /// Serializes a response to memory, then sends in a single writeAll call
    /// to avoid per-header syscalls through the unbuffered SocketWriter.
    fn sendBuffered(allocator: Allocator, socket: *Socket, resp: *Response) !void {
        const bytes = try serializeToSlice(allocator, resp);
        defer allocator.free(bytes);
        try socket.sendAll(bytes);
    }

    /// RFC 7231 §7.1.1.2: Origin servers MUST send a Date header field
    /// in IMF-fixdate format: e.g. "Sun, 06 Nov 1994 08:49:37 GMT".
    fn ensureDateHeader(io: Io, response: *Response) !void {
        if (response.headers.get(HeaderName.DATE) != null) return;
        const epoch = std.time.epoch;
        const now = Io.Clock.real.now(io);
        const ts: u64 = @intCast(@max(@divFloor(now.nanoseconds, std.time.ns_per_s), 0));
        const es = epoch.EpochSeconds{ .secs = ts };
        const day_secs = es.getDaySeconds();
        const epoch_day = es.getEpochDay();
        const year_day = epoch_day.calculateYearDay();
        const month_day = year_day.calculateMonthDay();

        // Day of week: 1970-01-01 was Thursday (index 4).
        const dow_names = [7][]const u8{ "Thu", "Fri", "Sat", "Sun", "Mon", "Tue", "Wed" };
        const mon_names = [12][]const u8{ "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec" };
        const dow = dow_names[epoch_day.day % 7];
        const mon = mon_names[month_day.month.numeric() - 1];

        var buf: [30]u8 = undefined;
        const date_str = std.fmt.bufPrint(&buf, "{s}, {d:0>2} {s} {d} {d:0>2}:{d:0>2}:{d:0>2} GMT", .{
            dow,
            month_day.day_index + 1,
            mon,
            year_day.year,
            day_secs.getHoursIntoDay(),
            day_secs.getMinutesIntoHour(),
            day_secs.getSecondsIntoMinute(),
        }) catch return;
        try response.headers.set("Date", date_str);
    }

    fn ensureContentLengthHeader(response: *Response) !void {
        if (response.headers.get(HeaderName.CONTENT_LENGTH) != null) return;
        if (response.headers.isChunked()) return;

        const body_len: usize = if (response.body) |b| b.len else 0;
        try response.headers.setContentLength(body_len);
    }

    /// Sets the `Allow` header for automatic OPTIONS and 405 responses.
    fn setAllowHeader(_: *Self, headers: *Headers, methods: []const types.Method) !void {
        var buf: [256]u8 = undefined;
        var pos: usize = 0;
        var has_options = false;

        for (methods) |m| {
            if (m == .OPTIONS) has_options = true;
            const name = m.toString();
            if (pos + name.len + 2 > buf.len) break;
            if (pos > 0) {
                @memcpy(buf[pos..][0..2], ", ");
                pos += 2;
            }
            @memcpy(buf[pos..][0..name.len], name);
            pos += name.len;
        }

        if (!has_options) {
            const opt = "OPTIONS";
            if (pos + opt.len + 2 <= buf.len) {
                if (pos > 0) {
                    @memcpy(buf[pos..][0..2], ", ");
                    pos += 2;
                }
                @memcpy(buf[pos..][0..opt.len], opt);
                pos += opt.len;
            }
        }

        try headers.set(HeaderName.ALLOW, buf[0..pos]);
    }

    /// Middleware execution state kept on the stack frame of executeMiddleware.
    /// Uses `@fieldParentPtr` through the embedded `Next` to carry state
    /// without exposing internal fields on Context.
    const MiddlewareExecState = struct {
        server: *Self,
        route_handler: Handler,
        index: usize = 0,
        next: middleware_mod.Next = .{ ._call = trampoline },

        fn trampoline(next_ptr: *middleware_mod.Next, ctx: *Context) anyerror!Response {
            const state: *MiddlewareExecState = @fieldParentPtr("next", next_ptr);
            return advance(ctx, state);
        }

        fn advance(ctx: *Context, state: *MiddlewareExecState) anyerror!Response {
            if (state.index < state.server.middleware.items.len) {
                const mw = state.server.middleware.items[state.index];
                state.index += 1;
                return mw.invoke(ctx, &state.next);
            }
            return state.route_handler.invoke(ctx);
        }
    };

    fn executeMiddleware(self: *Self, ctx: *Context, route_handler: Handler) !Response {
        var state = MiddlewareExecState{
            .server = self,
            .route_handler = route_handler,
        };
        return MiddlewareExecState.advance(ctx, &state);
    }
};

/// RFC 7540 §8.1.2.2: Connection-specific headers are forbidden in HTTP/2.
/// Returns true if the header must be rejected.
fn isH2ForbiddenHeader(name: []const u8, value: []const u8) bool {
    const forbidden = [_][]const u8{
        "connection",
        "keep-alive",
        "proxy-connection",
        "transfer-encoding",
        "upgrade",
    };
    for (forbidden) |f| {
        if (std.ascii.eqlIgnoreCase(name, f)) return true;
    }
    // "te" is allowed only with value "trailers".
    if (std.ascii.eqlIgnoreCase(name, "te")) {
        return !std.ascii.eqlIgnoreCase(value, "trailers");
    }
    return false;
}

fn appendH2ResponseHeaders(
    allocator: Allocator,
    destination: *std.ArrayListUnmanaged(hpack.HeaderEntry),
    source: *const Headers,
) !void {
    for (source.entries.items) |entry| {
        // RFC 9113 section 8.2.2: connection-specific fields are an H1
        // concern and MUST NOT be generated in an H2 response. Filtering
        // centrally keeps arbitrary middleware and application handlers from
        // accidentally producing a malformed stream.
        if (isH2ForbiddenHeader(entry.name, entry.value)) continue;
        try destination.append(allocator, .{ .name = entry.name, .value = entry.value });
    }
}

/// Returns true if `path` contains traversal sequences (`..`), null bytes,
/// or starts with `/` (absolute). Checks both raw and common percent-encoded
/// variants (`%2e`, `%2E`).
fn containsTraversal(path: []const u8) bool {
    // Reject null bytes — can bypass C-based filesystem APIs.
    if (mem.indexOfScalar(u8, path, 0) != null) return true;
    // Reject absolute paths.
    if (path.len > 0 and path[0] == '/') return true;
    // Reject backslashes — Windows path separators can bypass unix-only checks.
    if (mem.indexOfScalar(u8, path, '\\') != null) return true;

    // Check for ".." in raw form.
    if (mem.indexOf(u8, path, "..") != null) return true;

    // Check for percent-encoded slash (%2f, %2F) — can bypass directory checks.
    {
        var j: usize = 0;
        while (j + 2 < path.len) : (j += 1) {
            if (path[j] == '%' and path[j + 1] == '2' and (path[j + 2] == 'f' or path[j + 2] == 'F')) return true;
        }
    }

    // Check for percent-encoded dot variants: %2e and %2E.
    var i: usize = 0;
    while (i < path.len) {
        if (isEncodedDot(path, i)) {
            // Check if followed by another dot (raw or encoded).
            const next = i + 3;
            if (next < path.len and path[next] == '.') return true;
            if (isEncodedDot(path, next)) return true;
            // Check if preceded by a raw dot.
            if (i > 0 and path[i - 1] == '.') return true;
        }
        i += 1;
    }
    return false;
}

fn isEncodedDot(path: []const u8, i: usize) bool {
    if (i + 2 >= path.len) return false;
    if (path[i] != '%') return false;
    if (path[i + 1] != '2') return false;
    return path[i + 2] == 'e' or path[i + 2] == 'E';
}

/// Duck-typed reader for h2 connection handling: yields `initial` bytes first,
/// then reads from the underlying socket via `recv`.
const H2SocketReader = struct {
    socket: *Socket,
    initial: []const u8,
    initial_pos: usize,

    pub fn read(self: *H2SocketReader, buf: []u8) !usize {
        if (self.initial_pos < self.initial.len) {
            const avail = self.initial.len - self.initial_pos;
            const n = @min(avail, buf.len);
            @memcpy(buf[0..n], self.initial[self.initial_pos .. self.initial_pos + n]);
            self.initial_pos += n;
            return n;
        }
        return self.socket.recv(buf);
    }
};

fn trailerHeaderNames(allocator: Allocator, headers: *const Headers) ![]u8 {
    const items = headers.iterator();
    const names = try allocator.alloc([]const u8, items.len);
    defer allocator.free(names);
    for (items, 0..) |h, i| names[i] = h.name;
    return std.mem.join(allocator, ", ", names);
}

test "Server initialization" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();

    try std.testing.expectEqual(@as(u16, 8080), server.config.port);
}

test "bound handlers keep independent instance context" {
    const StatefulHandler = struct {
        status_code: u16,
        calls: usize = 0,

        fn handle(self: *@This(), ctx: *Context) anyerror!Response {
            self.calls += 1;
            return Response.init(ctx.allocator, self.status_code);
        }
    };

    const allocator = std.testing.allocator;
    var first_state = StatefulHandler{ .status_code = 201 };
    var second_state = StatefulHandler{ .status_code = 202 };
    var first_router = Router.init(allocator);
    defer first_router.deinit();
    var second_router = Router.init(allocator);
    defer second_router.deinit();

    try first_router.add(.GET, "/state", Handler.bind(&first_state, StatefulHandler.handle));
    try second_router.add(.GET, "/state", Handler.bind(&second_state, StatefulHandler.handle));

    var request = try Request.init(allocator, .GET, "/state");
    defer request.deinit();
    var ctx = Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();
    var params: [16]RouteParam = undefined;

    var first_response = try first_router.find(.GET, "/state", &params).?.handler.invoke(&ctx);
    defer first_response.deinit();
    var second_response = try second_router.find(.GET, "/state", &params).?.handler.invoke(&ctx);
    defer second_response.deinit();

    try std.testing.expectEqual(@as(u16, 201), first_response.status.code);
    try std.testing.expectEqual(@as(u16, 202), second_response.status.code);
    try std.testing.expectEqual(@as(usize, 1), first_state.calls);
    try std.testing.expectEqual(@as(usize, 1), second_state.calls);
}

test "wrapped handlers preserve the supplied handler target" {
    const StatefulHandler = struct {
        calls: usize = 0,

        fn handle(self: *@This(), ctx: *Context) anyerror!Response {
            self.calls += 1;
            return Response.init(ctx.allocator, 207);
        }
    };
    const Wrapper = struct {
        calls: usize = 0,

        fn handle(self: *@This(), inner: Handler, ctx: *Context) anyerror!Response {
            self.calls += 1;
            return inner.invoke(ctx);
        }
    };

    const allocator = std.testing.allocator;
    var state: StatefulHandler = .{};
    var wrapper: Wrapper = .{};
    const handler = Handler.wrap(&wrapper, Handler.bind(&state, StatefulHandler.handle), Wrapper.handle);

    var request = try Request.init(allocator, .GET, "/wrapped");
    defer request.deinit();
    var ctx = Context.init(allocator, std.testing.io, &request);
    defer ctx.deinit();
    var response = try handler.invoke(&ctx);
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 207), response.status.code);
    try std.testing.expectEqual(@as(usize, 1), state.calls);
    try std.testing.expectEqual(@as(usize, 1), wrapper.calls);
}

test "Context response helpers" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/test");
    defer req.deinit();

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    _ = ctx.status(201);
    try std.testing.expectEqual(@as(u16, 201), ctx.response.status_code);
}

test "Server with config" {
    const allocator = std.testing.allocator;
    var server = Server.initWithConfig(allocator, std.testing.io, .{
        .host = "0.0.0.0",
        .port = 3000,
    });
    defer server.deinit();

    try std.testing.expectEqual(@as(u16, 3000), server.config.port);
    try std.testing.expectEqualStrings("0.0.0.0", server.config.host);
}

test "Context query parsing" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/search?q=zig&lang=en");
    defer req.deinit();

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    try std.testing.expectEqualStrings("zig", ctx.query("q").?);
    try std.testing.expectEqualStrings("en", ctx.query("lang").?);
    try std.testing.expect(ctx.query("missing") == null);
}

test "Context cookie helpers" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();
    try req.headers.set(HeaderName.COOKIE, "session=abc123; theme=dark");

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    try std.testing.expectEqualStrings("abc123", ctx.cookie("session").?);
    try std.testing.expectEqualStrings("dark", ctx.cookie("theme").?);
    try std.testing.expect(ctx.cookie("missing") == null);

    try ctx.setCookie("session", "next", .{ .path = "/", .http_only = true, .same_site = .lax });
    const set_cookie = ctx.response.headers.get(HeaderName.SET_COOKIE).?;
    try std.testing.expect(mem.indexOf(u8, set_cookie, "session=next") != null);

    try ctx.removeCookie("session", .{ .path = "/" });
    const all_set_cookies = try ctx.response.headers.getAll(HeaderName.SET_COOKIE, allocator);
    defer allocator.free(all_set_cookies);
    try std.testing.expect(all_set_cookies.len >= 2);
}

test "Router allowed methods for path" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();

    const handler = struct {
        fn h(_: *Context) anyerror!Response {
            return error.TestUnexpectedResult;
        }
    }.h;

    try server.get("/users/:id", handler);
    try server.put("/users/:id", handler);
    try server.delete("/users/:id", handler);

    var methods: [16]types.Method = undefined;
    const count = server.router.allowedMethods("/users/42", &methods);

    try std.testing.expect(count >= 3);

    var has_get = false;
    var has_put = false;
    var has_delete = false;
    for (methods[0..count]) |m| {
        if (m == .GET) has_get = true;
        if (m == .PUT) has_put = true;
        if (m == .DELETE) has_delete = true;
    }

    try std.testing.expect(has_get);
    try std.testing.expect(has_put);
    try std.testing.expect(has_delete);
}

test "Server any() registers all methods" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();

    const handler = struct {
        fn h(_: *Context) anyerror!Response {
            return error.TestUnexpectedResult;
        }
    }.h;

    try server.any("/wild", handler);

    var pbuf: [16]RouteParam = undefined;
    try std.testing.expect(server.router.find(.GET, "/wild", &pbuf) != null);
    try std.testing.expect(server.router.find(.POST, "/wild", &pbuf) != null);
    try std.testing.expect(server.router.find(.TRACE, "/wild", &pbuf) != null);
    try std.testing.expect(server.router.find(.CONNECT, "/wild", &pbuf) != null);
}

test "ServerConfig defaults" {
    const config = ServerConfig{};
    try std.testing.expectEqual(@as(usize, 10 * 1024 * 1024), config.max_body_size);
    try std.testing.expectEqual(@as(usize, 64 * 1024 * 1024), config.request_body_buffer_budget_bytes);
    try std.testing.expectEqual(@as(usize, 100), config.max_headers);
    try std.testing.expectEqual(@as(usize, 100 * 1024 * 1024), config.max_file_size);
    try std.testing.expectEqual(@as(u64, 30_000), config.header_read_timeout_ms);
    try std.testing.expectEqual(@as(u64, 120_000), config.body_read_timeout_ms);
    try std.testing.expectEqual(@as(u64, 30_000), config.response_write_timeout_ms);

    const normalized = (ServerConfig{
        .max_connections = 64,
        .max_request_tasks = 0,
        .h2_max_concurrent_streams = 0,
        .max_body_size = 4096,
        .request_body_buffer_budget_bytes = 0,
        .accept_error_backoff_initial_ms = 0,
        .accept_error_backoff_max_ms = 0,
    }).normalized();
    try std.testing.expectEqual(@as(u32, 64), normalized.max_request_tasks);
    try std.testing.expectEqual(@as(u32, 64), normalized.h2_max_concurrent_streams);
    try std.testing.expectEqual(@as(usize, 4096), normalized.request_body_buffer_budget_bytes);
    try std.testing.expectEqual(@as(u32, 1), normalized.accept_error_backoff_initial_ms);
    try std.testing.expectEqual(@as(u32, 1), normalized.accept_error_backoff_max_ms);

    var server = Server.initWithConfig(std.testing.allocator, std.testing.io, .{
        .header_read_timeout_ms = 0,
        .body_read_timeout_ms = 0,
        .response_write_timeout_ms = 0,
        .keep_alive_timeout_ms = 0,
    });
    defer server.deinit();
    try std.testing.expectEqual(@as(u64, 0), server.config.header_read_timeout_ms);
    try std.testing.expectEqual(@as(u64, 0), server.config.body_read_timeout_ms);
    try std.testing.expectEqual(@as(u64, 0), server.config.response_write_timeout_ms);
    try std.testing.expectEqual(@as(u64, 0), server.config.keep_alive_timeout_ms);

    var bounded_h2 = Server.initWithConfig(std.testing.allocator, std.testing.io, .{
        .max_connections = 64,
        .max_request_tasks = 8,
        .h2_max_concurrent_streams = 100,
    });
    defer bounded_h2.deinit();
    try std.testing.expectEqual(@as(u32, 8), bounded_h2.config.h2_max_concurrent_streams);
}

test "routeErrorStatus maps oversized route errors to payload too large" {
    try std.testing.expectEqual(@as(?u16, 400), routeErrorStatus(error.SyntaxError));
    try std.testing.expectEqual(@as(?u16, 400), routeErrorStatus(error.MissingField));
    try std.testing.expectEqual(@as(?u16, 413), routeErrorStatus(error.ValueTooLong));
    try std.testing.expectEqual(@as(?u16, 413), routeErrorStatus(error.StreamTooLong));
    try std.testing.expectEqual(@as(?u16, 413), routeErrorStatus(error.BodyTooLarge));
    try std.testing.expectEqual(@as(?u16, 429), routeErrorStatus(error.BodyCapacityExceeded));
    try std.testing.expectEqual(@as(?u16, 408), routeErrorStatus(error.Timeout));
    try std.testing.expectEqual(@as(?u16, 504), routeErrorStatus(error.DeadlineExceeded));
    try std.testing.expectEqual(@as(?u16, 500), routeErrorStatus(error.UnexpectedRouteFailure));
    try std.testing.expectEqual(@as(?u16, null), routeErrorStatus(error.Canceled));
    try std.testing.expectEqual(@as(?u16, null), routeErrorStatus(error.Cancelled));
    try std.testing.expectEqualStrings(
        "{\"error\":\"INVALID_REQUEST\",\"message\":\"invalid request\"}",
        routeErrorBody(400),
    );
    try std.testing.expectEqualStrings(
        "{\"error\":\"NOT_FOUND\",\"message\":\"resource not found\"}",
        routeErrorBody(404),
    );
    try std.testing.expectEqualStrings(
        "{\"error\":\"REQUEST_TIMEOUT\",\"message\":\"request timed out\"}",
        routeErrorBody(408),
    );
    try std.testing.expectEqualStrings(
        "{\"error\":\"PAYLOAD_TOO_LARGE\",\"message\":\"request payload is too large\"}",
        routeErrorBody(413),
    );
    try std.testing.expectEqualStrings(
        "{\"error\":\"REQUEST_HEADER_FIELDS_TOO_LARGE\",\"message\":\"request headers are too large\"}",
        routeErrorBody(431),
    );
    try std.testing.expectEqualStrings(
        "{\"error\":\"INTERNAL_ERROR\",\"message\":\"internal server error\"}",
        routeErrorBody(500),
    );
}

test "Context queryDecoded decodes percent escapes exactly once" {
    const allocator = std.testing.allocator;
    var req = try Request.init(
        allocator,
        .GET,
        "/backup?location=s3%3A%2F%2Fbucket%2Fa%2520b&plus=a+b",
    );
    defer req.deinit();

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const location = (try ctx.queryDecoded("location")).?;
    try std.testing.expectEqualStrings("s3://bucket/a%20b", location);
    const plus = (try ctx.queryDecoded("plus")).?;
    try std.testing.expectEqualStrings("a b", plus);
    const location_again = (try ctx.queryDecoded("location")).?;
    try std.testing.expectEqualStrings(location, location_again);
    try std.testing.expectEqual(@as(usize, 3), ctx.decoded_query_values.items.len);
}

test "Context max_file_size default and override" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    // Default should match ServerConfig default.
    try std.testing.expectEqual(@as(usize, 100 * 1024 * 1024), ctx.max_file_size);

    // Can be overridden (as server does in handleConnection).
    ctx.max_file_size = 1024;
    try std.testing.expectEqual(@as(usize, 1024), ctx.max_file_size);
}

test "Context file rejects path traversal" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    var response = try ctx.file("../etc/passwd");
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 403), response.status.code);
}

test "Parser max_headers is configurable" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();
    parser.max_headers = 2;

    // Feed a request with 3 headers — should trigger TooManyHeaders error.
    const result = parser.feed("GET / HTTP/1.1\r\nA: 1\r\nB: 2\r\nC: 3\r\n\r\n");
    try std.testing.expectError(error.TooManyHeaders, result);
}

test "Parser max_body_size is configurable" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();
    parser.max_body_size = 4;

    _ = try parser.feed("POST / HTTP/1.1\r\nContent-Length: 10\r\n\r\n");
    // Content-Length 10 > max_body_size 4, should be error state.
    try std.testing.expect(parser.isError());
}

test "Context.streamH2 returns NotH2 for HTTP/1.1 context" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    // h2 fields are null by default (HTTP/1.1 context).
    try std.testing.expectError(error.NotH2, ctx.streamH2(200, &.{}));
}

test "H2StreamWriter write and close" {
    const allocator = std.testing.allocator;

    var h2 = H2Connection.initServer(allocator, std.testing.io);
    defer h2.deinit();

    // Create a stream so writeData can find it.
    _ = try h2.stream_manager.getOrCreateStream(1);

    // Write to an in-memory buffer via a TestWriter.
    var buf = std.ArrayListUnmanaged(u8).empty;
    defer buf.deinit(allocator);
    const TestWriter = struct {
        list: *std.ArrayListUnmanaged(u8),
        alloc: Allocator,
        pub fn writeAll(self: @This(), data: []const u8) !void {
            try self.list.appendSlice(self.alloc, data);
        }
    };
    var sock_stub = TestWriter{ .list = &buf, .alloc = allocator };

    // Cast the stub to a Socket pointer isn't feasible, so test the
    // underlying H2Connection.writeData directly (what H2StreamWriter calls).
    // Send DATA without END_STREAM.
    try h2.writeData(&sock_stub, 1, "chunk1", false);
    const first_len = buf.items.len;
    try std.testing.expect(first_len > 6); // 9-byte frame header + "chunk1"

    try h2.writeData(&sock_stub, 1, "chunk2", false);
    try std.testing.expect(buf.items.len > first_len);

    // Send END_STREAM.
    try h2.writeData(&sock_stub, 1, &.{}, true);

    // Verify last frame has END_STREAM flag. The frame header is 9 bytes;
    // the last frame is an empty DATA frame with END_STREAM.
    // Find the last 9-byte frame header.
    const last_frame_start = buf.items.len - 9; // empty payload, just header
    const flags = buf.items[last_frame_start + 4];
    try std.testing.expect(flags & H2Connection.FLAG_END_STREAM != 0);
}

test "H2StreamReader reads pre-buffered data and returns EOF" {
    const allocator = std.testing.allocator;

    var s = Stream.init(1);
    defer s.deinit(allocator);

    // Pre-populate data as if DATA frames already arrived.
    try s.data_buf.appendSlice(allocator, "hello world");
    s.completed = true;
    s.got_headers = true;

    var event = Io.Event.unset;
    var reader = Context.H2StreamReader{
        .h2_stream = &s,
        .io = std.testing.io,
        .data_event = &event,
    };

    var buf: [32]u8 = undefined;
    const n = try reader.read(&buf);
    try std.testing.expectEqualStrings("hello world", buf[0..n]);

    // Second read should return EOF.
    const n2 = try reader.read(&buf);
    try std.testing.expectEqual(@as(usize, 0), n2);
}

test "H2StreamReader.readAll buffers entire body" {
    const allocator = std.testing.allocator;

    var s = Stream.init(1);
    defer s.deinit(allocator);

    try s.data_buf.appendSlice(allocator, "part1");
    try s.data_buf.appendSlice(allocator, "part2");
    s.completed = true;
    s.got_headers = true;

    var event = Io.Event.unset;
    var reader = Context.H2StreamReader{
        .h2_stream = &s,
        .io = std.testing.io,
        .data_event = &event,
    };

    const body_data = try reader.readAll(allocator);
    defer allocator.free(body_data);
    try std.testing.expectEqualStrings("part1part2", body_data);
}

test "H2StreamReader body deadline cannot be renewed by trickle data" {
    const allocator = std.testing.allocator;
    var stream = Stream.init(1);
    defer stream.deinit(allocator);
    try stream.data_buf.append(allocator, 'x');

    var event = Io.Event.unset;
    var reader = Context.H2StreamReader{
        .h2_stream = &stream,
        .io = std.testing.io,
        .data_event = &event,
        .deadline_ms = milliTimestamp(std.testing.io) - 1,
    };
    var byte: [1]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 1), try reader.read(&byte));
    try std.testing.expectEqual(@as(u8, 'x'), byte[0]);
    try std.testing.expectError(error.Timeout, reader.read(&byte));
}

test "H1 body deadline starts after headers and rejects a stalled upload" {
    const State = struct {
        var handled = std.atomic.Value(bool).init(false);

        fn handler(ctx: *Context) anyerror!Response {
            handled.store(true, .release);
            return ctx.text("unexpected");
        }
    };
    State.handled.store(false, .release);

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .header_read_timeout_ms = 1_000,
        .body_read_timeout_ms = 25,
        .response_write_timeout_ms = 1_000,
        .h1_disconnect_cancellation = .disabled,
    });
    defer server.deinit();
    try server.post("/upload", State.handler);
    try server.bind();

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("deadline listener failed: {}", .{err});
        }
    }.run, .{&server});
    defer {
        server.stop();
        listener_thread.join();
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(server.boundAddress().?, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    try client.sendAll(
        "POST /upload HTTP/1.1\r\n" ++
            "Host: test\r\n" ++
            "Content-Length: 3\r\n" ++
            "Connection: close\r\n\r\n" ++
            "x",
    );

    var response: [512]u8 = undefined;
    const n = try client.recv(&response);
    try std.testing.expect(mem.indexOf(u8, response[0..n], " 408 ") != null);
    try std.testing.expect(!State.handled.load(.acquire));
}

test "H1 handler failure after stream commit closes without a second response" {
    const State = struct {
        fn handler(ctx: *Context) anyerror!Response {
            var writer = try ctx.streamResponse(200);
            try writer.write("partial");
            return error.TestStreamFailure;
        }
    };

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .h1_disconnect_cancellation = .disabled,
    });
    defer server.deinit();
    try server.get("/stream", State.handler);
    try server.bind();

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("stream failure listener failed: {}", .{err});
        }
    }.run, .{&server});
    defer {
        server.stop();
        listener_thread.join();
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(server.boundAddress().?, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    try client.sendAll("GET /stream HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");

    var response: [1024]u8 = undefined;
    var response_len: usize = 0;
    while (true) {
        const n = try client.recv(response[response_len..]);
        if (n == 0) break;
        response_len += n;
        if (response_len == response.len) return error.TestUnexpectedResult;
    }
    const bytes = response[0..response_len];
    try std.testing.expectEqual(@as(usize, 1), mem.count(u8, bytes, "HTTP/1.1"));
    try std.testing.expect(mem.indexOf(u8, bytes, "7\r\npartial\r\n") != null);
    try std.testing.expect(mem.indexOf(u8, bytes, " 500 ") == null);
}

test "H2StreamReader reports terminal stream errors instead of truncated EOF" {
    const allocator = std.testing.allocator;
    var s = Stream.init(1);
    defer s.deinit(allocator);
    s.completed = true;
    s.stream_error = error.BodyCapacityExceeded;

    var event = Io.Event.unset;
    var reader = Context.H2StreamReader{
        .h2_stream = &s,
        .io = std.testing.io,
        .data_event = &event,
    };
    var buf: [1]u8 = undefined;
    try std.testing.expectError(error.BodyCapacityExceeded, reader.read(&buf));
}

test "H2 body admission exhaustion writes retryable 429 before stream reset" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();
    var h2 = H2Connection.initServer(allocator, std.testing.io);
    defer h2.deinit();
    const stream = try h2.stream_manager.getOrCreateStream(1);
    try stream.open();
    stream.stream_error = error.BodyCapacityExceeded;
    stream.completed = true;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const TestWriter = struct {
        list: *std.ArrayListUnmanaged(u8),
        alloc: Allocator,
        pub fn writeAll(self: @This(), data: []const u8) !void {
            try self.list.appendSlice(self.alloc, data);
        }
    };
    var writer = TestWriter{ .list = &wire, .alloc = allocator };
    try server.sendH2Error(&h2, &writer, 1, 429);

    try std.testing.expect(wire.items.len >= 18);
    const headers_len: usize = std.mem.readInt(u24, wire.items[0..3], .big);
    try std.testing.expectEqual(@intFromEnum(http.Http2FrameType.headers), wire.items[3]);
    try std.testing.expect(wire.items[4] & H2Connection.FLAG_END_HEADERS != 0);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();
    const decoded = try client.decodeFrameHeaders(wire.items[9..][0..headers_len], wire.items[4]);
    defer stream_mod.freeDecodedHeaders(allocator, decoded.headers);
    var saw_status = false;
    var saw_retry_after = false;
    for (decoded.headers) |header| {
        if (mem.eql(u8, header.name, ":status") and mem.eql(u8, header.value, "429")) saw_status = true;
        if (mem.eql(u8, header.name, "retry-after") and mem.eql(u8, header.value, "1")) saw_retry_after = true;
    }
    try std.testing.expect(saw_status);
    try std.testing.expect(saw_retry_after);

    const data_offset = 9 + headers_len;
    const data_len: usize = std.mem.readInt(u24, wire.items[data_offset..][0..3], .big);
    try std.testing.expectEqual(@intFromEnum(http.Http2FrameType.data), wire.items[data_offset + 3]);
    try std.testing.expect(wire.items[data_offset + 4] & H2Connection.FLAG_END_STREAM != 0);
    try std.testing.expectEqualStrings(routeErrorBody(429), wire.items[data_offset + 9 ..][0..data_len]);
    try std.testing.expect(stream.end_stream_sent);
}

test "Context.body() returns request.body for HTTP/1.1" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .POST, "/");
    defer req.deinit();

    const body_content = "request body";
    req.body = body_content;

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const result = try ctx.body();
    try std.testing.expectEqualStrings(body_content, result.?);
}

test "Context.body() buffers from H2StreamReader" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .POST, "/");
    defer req.deinit();

    var s = Stream.init(1);
    defer s.deinit(allocator);
    try s.data_buf.appendSlice(allocator, "streamed body");
    s.completed = true;

    var event = Io.Event.unset;
    var body_reader = Context.H2StreamReader{
        .h2_stream = &s,
        .io = std.testing.io,
        .data_event = &event,
    };

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();
    ctx.h2_body_reader = &body_reader;

    const result = try ctx.body();
    try std.testing.expectEqualStrings("streamed body", result.?);

    // Second call should return the buffered body directly.
    const result2 = try ctx.body();
    try std.testing.expectEqualStrings("streamed body", result2.?);

    // h2_body_reader should be consumed (set to null).
    try std.testing.expect(ctx.h2_body_reader == null);
}

test "Context.body() treats unframed H2 EndOfStream as absent body" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();

    var s = Stream.init(1);
    defer s.deinit(allocator);
    s.stream_error = error.EndOfStream;

    var event = Io.Event.unset;
    var body_reader = Context.H2StreamReader{
        .h2_stream = &s,
        .io = std.testing.io,
        .data_event = &event,
    };

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();
    ctx.h2_body_reader = &body_reader;

    const result = try ctx.body();
    try std.testing.expect(result == null);
    try std.testing.expect(ctx.h2_body_reader == null);
}

test "Context.body() preserves H2 EndOfStream when content length promised bytes" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .POST, "/");
    defer req.deinit();
    try req.setHeader(HeaderName.CONTENT_LENGTH, "5");

    var s = Stream.init(1);
    defer s.deinit(allocator);
    s.stream_error = error.EndOfStream;

    var event = Io.Event.unset;
    var body_reader = Context.H2StreamReader{
        .h2_stream = &s,
        .io = std.testing.io,
        .data_event = &event,
    };

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();
    ctx.h2_body_reader = &body_reader;

    try std.testing.expectError(error.EndOfStream, ctx.body());
}

test "Context.parseBody() returns null without a body" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .POST, "/");
    defer req.deinit();

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const BodyParser = struct {
        fn parse(_: Allocator, _: []const u8) !usize {
            return 0;
        }
    };

    const result = try ctx.parseBody(usize, BodyParser.parse);
    try std.testing.expect(result == null);
}

test "Context.parseBody() delegates raw bytes to a custom parser" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .POST, "/");
    defer req.deinit();
    req.body = "custom payload";

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const Parsed = struct {
        len: usize,
        starts_with: u8,
    };
    const BodyParser = struct {
        fn parse(_: Allocator, raw: []const u8) !Parsed {
            return .{
                .len = raw.len,
                .starts_with = raw[0],
            };
        }
    };

    const result = (try ctx.parseBody(Parsed, BodyParser.parse)).?;
    try std.testing.expectEqual(@as(usize, "custom payload".len), result.len);
    try std.testing.expectEqual(@as(u8, 'c'), result.starts_with);
}

test "Context.parseJson() parses through the shared body parser path" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .POST, "/");
    defer req.deinit();
    req.body = "{\"name\":\"termite\"}";

    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const Payload = struct {
        name: []const u8,
    };

    var parsed = (try ctx.parseJson(Payload)).?;
    defer parsed.deinit();
    try std.testing.expectEqualStrings("termite", parsed.value.name);
}

test "H2 request rejection resets an unprocessed stream and unwinds ownership" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();
    var h2 = H2Connection.initServer(allocator, std.testing.io);
    defer h2.deinit();

    const stream = try h2.stream_manager.getOrCreateStream(1);
    try stream.open();
    const data_event = try allocator.create(Io.Event);
    data_event.* = .unset;
    stream.data_event = data_event;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const TestWriter = struct {
        list: *std.ArrayListUnmanaged(u8),
        alloc: Allocator,
        pub fn writeAll(self: @This(), data: []const u8) !void {
            try self.list.appendSlice(self.alloc, data);
        }
    };
    var writer = TestWriter{ .list = &wire, .alloc = allocator };

    try std.testing.expect(server.tryStartRequest());
    server.rejectH2StreamDispatch(&h2, &writer, 1, data_event, true);

    try std.testing.expectEqual(@as(usize, 0), server.runtimeStats().active_requests);
    try std.testing.expectEqual(@as(u64, 1), server.runtimeStats().request_dispatch_rejections_total);
    try std.testing.expectEqual(@as(u64, 1), server.runtimeStats().h2_stream_dispatch_rejections_total);
    try std.testing.expect(h2.stream_manager.getStream(1) == null);
    try std.testing.expectEqual(@as(usize, 13), wire.items.len);
    try std.testing.expectEqual(@intFromEnum(http.Http2FrameType.rst_stream), wire.items[3]);
    try std.testing.expectEqual(@as(u32, 1), std.mem.readInt(u32, wire.items[5..9], .big));
    try std.testing.expectEqual(
        @as(u32, @intFromEnum(http.Http2ErrorCode.refused_stream)),
        std.mem.readInt(u32, wire.items[9..13], .big),
    );
}

test "h2c applyPeerSettings propagates INITIAL_WINDOW_SIZE and HPACK table size" {
    // Verifies the fix: handleH2cUpgrade now calls h2.applyPeerSettings()
    // (not raw applySettingsPayload), so HPACK encoder and stream windows
    // are correctly synced with the peer's HTTP2-Settings header.
    const allocator = std.testing.allocator;

    // Build a SETTINGS payload: INITIAL_WINDOW_SIZE=32768, HEADER_TABLE_SIZE=2048.
    var settings_payload: [12]u8 = undefined;
    std.mem.writeInt(u16, settings_payload[0..2], @intFromEnum(http.Http2SettingId.initial_window_size), .big);
    std.mem.writeInt(u32, settings_payload[2..6], 32768, .big);
    std.mem.writeInt(u16, settings_payload[6..8], @intFromEnum(http.Http2SettingId.header_table_size), .big);
    std.mem.writeInt(u32, settings_payload[8..12], 2048, .big);

    // Base64url encode (what the HTTP2-Settings header carries).
    var encoded_buf: [32]u8 = undefined;
    const encoded = std.base64.url_safe_no_pad.Encoder.encode(&encoded_buf, &settings_payload);

    // Decode via the same path handleH2cUpgrade uses.
    const decoded = try http.decodeH2cSettings(encoded, allocator);
    defer allocator.free(decoded);

    var h2 = H2Connection.initServer(allocator, std.testing.io);
    defer h2.deinit();

    // applyPeerSettings (not raw applySettingsPayload) applies side effects.
    try h2.applyPeerSettings(decoded);

    // Peer's INITIAL_WINDOW_SIZE must be reflected.
    try std.testing.expectEqual(@as(u32, 32768), h2.peer_settings.initial_window_size);
    // HPACK encoder table must be updated.
    try std.testing.expectEqual(@as(?usize, 2048), h2.stream_manager.hpack_encode_ctx.pending_table_size_update);
    try std.testing.expectEqual(@as(usize, 2048), h2.stream_manager.hpack_encode_ctx.dynamic_table.max_size);
    // max_concurrent_streams propagated to stream manager.
    try std.testing.expectEqual(h2.peer_settings.max_concurrent_streams, h2.stream_manager.max_concurrent_streams);
}

test "handleH2Stream cleanup skips RST_STREAM on already-closed stream" {
    // Verifies the fix: when a stream is in .closed state (peer already
    // sent RST_STREAM), the server's defer block should NOT send RST_STREAM
    // back (RFC 7540 §5.4.2 violation).
    const allocator = std.testing.allocator;

    var h2 = H2Connection.initServer(allocator, std.testing.io);
    defer h2.deinit();

    // Create stream 1 and put it in closed state (as if peer sent RST_STREAM).
    const stream = try h2.stream_manager.getOrCreateStream(1);
    stream.state = .closed;
    stream.end_stream_sent = false; // server hasn't sent END_STREAM yet

    // The guard from handleH2Stream's defer:
    // "if (!s.end_stream_sent and s.state != .idle and s.state != .closed)"
    // Should be false for .closed state.
    const should_rst = !stream.end_stream_sent and stream.state != .idle and stream.state != .closed;
    try std.testing.expect(!should_rst);

    // Compare: an open stream that hasn't sent END_STREAM SHOULD get RST.
    const stream3 = try h2.stream_manager.getOrCreateStream(3);
    stream3.state = .open;
    stream3.end_stream_sent = false;
    const should_rst_open = !stream3.end_stream_sent and stream3.state != .idle and stream3.state != .closed;
    try std.testing.expect(should_rst_open);
}

test "H2 handler stream lookups share the receive mutex" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();
    var h2 = H2Connection.initServer(allocator, std.testing.io);
    defer h2.deinit();

    h2.write_mutex.lockUncancelable(std.testing.io);
    const stream = try h2.stream_manager.getOrCreateStream(1);
    h2.write_mutex.unlock(std.testing.io);

    // The handler snapshot shares the receive loop's map mutex and retains
    // the stable Stream allocation rather than any map storage.
    const handler_stream = Server.getH2Stream(&h2, 1) orelse return error.TestUnexpectedResult;
    try std.testing.expect(handler_stream == stream);

    h2.write_mutex.lockUncancelable(std.testing.io);
    h2.stream_manager.removeStream(1);
    h2.write_mutex.unlock(std.testing.io);
    try std.testing.expect(Server.getH2Stream(&h2, 1) == null);
}

test "isH2ForbiddenHeader rejects connection-specific headers" {
    // RFC 7540 §8.1.2.2: connection-specific headers are forbidden.
    try std.testing.expect(isH2ForbiddenHeader("connection", "keep-alive"));
    try std.testing.expect(isH2ForbiddenHeader("Connection", "keep-alive"));
    try std.testing.expect(isH2ForbiddenHeader("keep-alive", "timeout=5"));
    try std.testing.expect(isH2ForbiddenHeader("proxy-connection", "keep-alive"));
    try std.testing.expect(isH2ForbiddenHeader("transfer-encoding", "chunked"));
    try std.testing.expect(isH2ForbiddenHeader("Transfer-Encoding", "chunked"));
    try std.testing.expect(isH2ForbiddenHeader("upgrade", "h2c"));

    // "te" with value "trailers" is allowed.
    try std.testing.expect(!isH2ForbiddenHeader("te", "trailers"));
    try std.testing.expect(!isH2ForbiddenHeader("TE", "trailers"));
    // "te" with other values is forbidden.
    try std.testing.expect(isH2ForbiddenHeader("te", "gzip"));
    try std.testing.expect(isH2ForbiddenHeader("te", "chunked"));

    // Normal headers are allowed.
    try std.testing.expect(!isH2ForbiddenHeader("content-type", "text/html"));
    try std.testing.expect(!isH2ForbiddenHeader("accept", "*/*"));
}

test "H2 response serialization strips connection-specific headers" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();
    try headers.set("Connection", "close");
    try headers.set("Keep-Alive", "timeout=5");
    try headers.set("Transfer-Encoding", "chunked");
    try headers.set("TE", "trailers");
    try headers.set("Retry-After", "1");

    var encoded = std.ArrayListUnmanaged(hpack.HeaderEntry).empty;
    defer encoded.deinit(allocator);
    try appendH2ResponseHeaders(allocator, &encoded, &headers);

    try std.testing.expectEqual(@as(usize, 2), encoded.items.len);
    try std.testing.expectEqualStrings("TE", encoded.items[0].name);
    try std.testing.expectEqualStrings("trailers", encoded.items[0].value);
    try std.testing.expectEqualStrings("Retry-After", encoded.items[1].name);
    try std.testing.expectEqualStrings("1", encoded.items[1].value);
}

test "shutdown publishes graceful listener-thread work" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();

    server.running = true;
    server.shutdown(25);
    try std.testing.expect(server.running);
    try std.testing.expect(server.listener == null);
    try std.testing.expectEqual(@as(u8, 1), server.shutdown_mode.load(.acquire));
    try std.testing.expectEqual(@as(u64, 25), server.graceful_timeout_ms.load(.acquire));
}

test "stop publishes synchronized listener-thread shutdown" {
    const allocator = std.testing.allocator;
    var server = Server.init(allocator, std.testing.io);
    defer server.deinit();

    server.running = true;
    server.stop();
    try std.testing.expect(server.running);
    try std.testing.expectEqual(@as(u8, 2), server.shutdown_mode.load(.acquire));
    try std.testing.expect(server.listener == null);

    // The running flag belongs to an already-started listener and is cleared
    // when that listener observes the stop. Exercise the distinct
    // stop-before-listen race with a server that has not started yet.
    var not_started = Server.init(allocator, std.testing.io);
    defer not_started.deinit();
    not_started.stop();

    // A concurrent owner may call stop before the listener thread reaches
    // listen(). That late listen must not resurrect the server.
    try not_started.listen();
    try std.testing.expect(!not_started.running);
    try std.testing.expect(not_started.listener == null);
}

test "requestStop only publishes synchronized listener-thread work" {
    const allocator = std.testing.allocator;
    var server = Server.initWithConfig(allocator, std.testing.io, .{ .host = "127.0.0.1", .port = 1 });
    defer server.deinit();

    server.running = true;
    server.requestStop();
    try std.testing.expect(server.running);
    try std.testing.expectEqual(@as(u8, 2), server.shutdown_mode.load(.acquire));
    try std.testing.expect(server.listener == null);
}

test "repeated stop requests do not inflate connection admission permits" {
    var server = Server.initWithConfig(std.testing.allocator, std.testing.io, .{
        .host = "127.0.0.1",
        .port = 1,
        .max_connections = 3,
    });
    defer server.deinit();
    server.requestStop();
    server.requestStop();
    try std.testing.expectEqual(@as(usize, 3), server.conn_semaphore.permits);
}

test "accept backoff is normalized and bounded" {
    var server = Server.initWithConfig(std.testing.allocator, std.testing.io, .{
        .accept_error_backoff_initial_ms = 0,
        .accept_error_backoff_max_ms = 0,
    });
    defer server.deinit();
    try std.testing.expectEqual(@as(u32, 1), server.config.accept_error_backoff_initial_ms);
    try std.testing.expectEqual(@as(u32, 1), server.config.accept_error_backoff_max_ms);

    var delay = server.config.accept_error_backoff_initial_ms;
    for (0..64) |_| delay = @min(server.config.accept_error_backoff_max_ms, delay *| 2);
    try std.testing.expectEqual(server.config.accept_error_backoff_max_ms, delay);

    server.active_connections.store(3, .release);
    server.peak_active_connections.store(5, .release);
    server.active_requests.store(2, .release);
    server.peak_active_requests.store(4, .release);
    server.accept_errors_total.store(7, .release);
    server.connection_timeouts_total.store(11, .release);
    server.connection_dispatch_rejections_total.store(13, .release);
    server.request_dispatch_rejections_total.store(14, .release);
    server.h2_stream_dispatch_rejections_total.store(15, .release);
    server.request_cancellations_total.store(17, .release);
    const stats = server.runtimeStats();
    try std.testing.expectEqual(@as(u32, 1000), stats.max_connections);
    try std.testing.expectEqual(@as(usize, 3), stats.active_connections);
    try std.testing.expectEqual(@as(usize, 5), stats.peak_active_connections);
    try std.testing.expectEqual(@as(usize, 2), stats.active_requests);
    try std.testing.expectEqual(@as(usize, 4), stats.peak_active_requests);
    try std.testing.expectEqual(@as(u64, 7), stats.accept_errors_total);
    try std.testing.expectEqual(@as(u64, 11), stats.connection_timeouts_total);
    try std.testing.expectEqual(@as(u64, 13), stats.connection_dispatch_rejections_total);
    try std.testing.expectEqual(@as(u64, 14), stats.request_dispatch_rejections_total);
    try std.testing.expectEqual(@as(u64, 15), stats.h2_stream_dispatch_rejections_total);
    try std.testing.expectEqual(@as(u64, 17), stats.request_cancellations_total);
}

test "cross-thread stop wakes an ephemeral listener" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();
    try server.bind();
    try std.testing.expect(server.wake_port.load(.acquire) != 0);

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("ephemeral listener failed: {}", .{err});
        }
    }.run, .{&server});
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};
    server.stop();
    listener_thread.join();
    try std.testing.expect(!server.running);
}

test "multiple listeners share one HTTP runtime lifecycle" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var http_runtime = HttpRuntime.init(allocator, .{ .max_active_h1_requests = 8 });
    defer http_runtime.deinit();

    var first = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 4,
        .http_runtime = &http_runtime,
    });
    defer first.deinit();
    var second = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 4,
        .http_runtime = &http_runtime,
    });
    defer second.deinit();

    const first_thread = try std.Thread.spawn(.{}, struct {
        fn run(server: *Server) void {
            server.listen() catch |err| std.debug.panic("first shared-runtime listener failed: {}", .{err});
        }
    }.run, .{&first});
    const second_thread = try std.Thread.spawn(.{}, struct {
        fn run(server: *Server) void {
            server.listen() catch |err| std.debug.panic("second shared-runtime listener failed: {}", .{err});
        }
    }.run, .{&second});
    while (!first.listen_started.load(.acquire) or !second.listen_started.load(.acquire))
        std.Thread.yield() catch {};

    try std.testing.expectEqual(@as(usize, 2), http_runtime.stats().active_listener_leases);
    first.stop();
    second.stop();
    first_thread.join();
    second_thread.join();
    try std.testing.expectEqual(@as(usize, 0), http_runtime.stats().active_listener_leases);
}

test "shared HTTP runtime preserves each listener request reservation" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var http_runtime = HttpRuntime.init(allocator, .{
        .max_active_h1_requests = 0,
        .max_active_connections = 2,
        .max_active_requests = 2,
    });
    defer http_runtime.deinit();

    var first = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 1,
        .max_request_tasks = 1,
        .http_runtime = &http_runtime,
        .h1_disconnect_cancellation = .disabled,
    });
    defer first.deinit();
    var second = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 1,
        .max_request_tasks = 1,
        .http_runtime = &http_runtime,
        .h1_disconnect_cancellation = .disabled,
    });
    defer second.deinit();
    try first.bind();
    try second.bind();

    try std.testing.expect(first.tryStartRequest());
    defer first.finishRequest();
    try std.testing.expect(!first.tryStartRequest());
    try std.testing.expect(second.tryStartRequest());
    defer second.finishRequest();

    const runtime_stats = http_runtime.stats();
    try std.testing.expectEqual(@as(usize, 2), runtime_stats.request_capacity);
    try std.testing.expectEqual(@as(usize, 2), runtime_stats.reserved_request_capacity);
    try std.testing.expectEqual(@as(usize, 1), first.runtimeStats().active_requests);
    try std.testing.expectEqual(@as(usize, 1), second.runtimeStats().active_requests);
}

test "bind establishes HTTP runtime ownership before publishing an address" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var http_runtime = HttpRuntime.init(allocator, .{ .max_active_h1_requests = 1 });
    defer http_runtime.deinit();

    {
        var server = Server.initWithConfig(allocator, io_impl.io(), .{
            .host = "127.0.0.1",
            .port = 0,
            .max_connections = 1,
            .http_runtime = &http_runtime,
        });
        defer server.deinit();
        try server.bind();
        try std.testing.expect(server.boundAddress() != null);
        try std.testing.expectEqual(@as(usize, 1), http_runtime.stats().active_listener_leases);
        try std.testing.expectEqual(@as(usize, 1), http_runtime.stats().reserved_h1_request_capacity);
    }
    try std.testing.expectEqual(@as(usize, 0), http_runtime.stats().active_listener_leases);
    try std.testing.expectEqual(@as(usize, 0), http_runtime.stats().reserved_h1_request_capacity);
}

test "bind rejects a connection limit larger than shared HTTP runtime capacity" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var http_runtime = HttpRuntime.init(allocator, .{
        .max_active_h1_requests = 2,
        .max_active_connections = 1,
        .max_active_requests = 2,
    });
    defer http_runtime.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 2,
        .http_runtime = &http_runtime,
    });
    defer server.deinit();
    try std.testing.expectError(error.HttpRuntimeConnectionCapacityExceeded, server.bind());
    try std.testing.expectEqual(@as(usize, 0), server.httpRuntimeStats().active_listener_leases);
}

test "bounded control listener serves without H1 observer capacity" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var http_runtime = HttpRuntime.init(allocator, .{ .max_active_h1_requests = 0 });
    defer http_runtime.deinit();

    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 1,
        .http_runtime = &http_runtime,
        .h1_disconnect_cancellation = .disabled,
    });
    defer server.deinit();
    try server.get("/readyz", struct {
        fn handle(ctx: *Context) anyerror!Response {
            return ctx.status(503).text("not ready");
        }
    }.handle);
    try server.bind();

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("control listener failed: {}", .{err});
        }
    }.run, .{&server});
    defer {
        server.stop();
        listener_thread.join();
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(server.boundAddress().?, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    try client.sendAll("GET /readyz HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    var response: [1024]u8 = undefined;
    const response_len = try client.recv(&response);
    try std.testing.expect(mem.indexOf(u8, response[0..response_len], "503") != null);
    try std.testing.expectEqual(@as(usize, 0), http_runtime.stats().reserved_h1_request_capacity);
}

test "listener task binds synchronously and joins before executor teardown" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();

    var task = Server.ListenerTask.init(&server);
    try task.start();
    try std.testing.expect(task.isRunning());
    try std.testing.expect(server.boundAddress() != null);
    try std.testing.expectError(error.ListenerTaskAlreadyStarted, task.start());

    task.shutdown(1000);
    try task.join();
    try std.testing.expectEqual(Server.ListenerTask.State.joined, task.state);
    try std.testing.expect(!server.running);
    // Joining is idempotent for cleanup paths.
    try task.join();
}

test "HTTP runtime tasks do not consume the nested-operation executor" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{ .concurrent_limit = .limited(1) });
    defer io_impl.deinit();
    var application_task_started = std.atomic.Value(bool).init(false);
    var release_application_task = std.atomic.Value(bool).init(false);
    var application_task = try io_impl.io().concurrent(struct {
        fn run(started: *std.atomic.Value(bool), release: *const std.atomic.Value(bool)) void {
            started.store(true, .release);
            while (!release.load(.acquire)) std.Thread.yield() catch {};
        }
    }.run, .{ &application_task_started, &release_application_task });
    defer {
        release_application_task.store(true, .release);
        application_task.await(io_impl.io());
    }
    while (!application_task_started.load(.acquire)) std.Thread.yield() catch {};

    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 1,
        .h1_disconnect_cancellation = .disabled,
    });
    defer server.deinit();
    try server.get("/ok", struct {
        fn handle(ctx: *Context) anyerror!Response {
            return ctx.text("ok");
        }
    }.handle);

    var task = Server.ListenerTask.init(&server);
    try task.start();
    defer {
        task.requestStop();
        task.join() catch {};
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(server.boundAddress().?, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    try client.sendAll("GET /ok HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    var response: [1024]u8 = undefined;
    const response_len = try client.recv(&response);
    try std.testing.expect(mem.indexOf(u8, response[0..response_len], "200") != null);
    try std.testing.expectEqual(@as(u64, 0), server.runtimeStats().connection_dispatch_rejections_total);
}

test "HTTP/1 and h2c request saturation reject before application work" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const State = struct {
        var handler_calls = std.atomic.Value(u32).init(0);
        var first_started = std.atomic.Value(bool).init(false);
        var release_first = std.atomic.Value(bool).init(false);

        fn handler(ctx: *Context) anyerror!Response {
            _ = handler_calls.fetchAdd(1, .acq_rel);
            first_started.store(true, .release);
            while (!release_first.load(.acquire)) std.Thread.yield() catch {};
            return ctx.text("complete");
        }
    };
    State.handler_calls.store(0, .release);
    State.first_started.store(false, .release);
    State.release_first.store(false, .release);

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 3,
        .max_request_tasks = 1,
        .h1_disconnect_cancellation = .disabled,
    });
    defer server.deinit();
    try server.get("/work", State.handler);

    var task = Server.ListenerTask.init(&server);
    try task.start();
    defer {
        task.requestStop();
        task.join() catch {};
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};
    defer State.release_first.store(true, .release);

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var first_client = try Socket.connect(server.boundAddress().?, client_io);
    defer first_client.close();
    try first_client.setRecvTimeout(5_000);
    try first_client.sendAll("GET /work HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    while (!State.first_started.load(.acquire)) std.Thread.yield() catch {};

    var second_client = try Socket.connect(server.boundAddress().?, client_io);
    defer second_client.close();
    try second_client.setRecvTimeout(5_000);
    try second_client.sendAll("GET /work HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    var rejected_response: [1024]u8 = undefined;
    const rejected_len = try second_client.recv(&rejected_response);

    try std.testing.expect(mem.indexOf(u8, rejected_response[0..rejected_len], " 503 ") != null);
    try std.testing.expect(mem.indexOf(u8, rejected_response[0..rejected_len], "Connection: close\r\n") != null);

    // An h2c upgrade is still an admitted request. Saturation must reject it
    // before switching protocols, otherwise h2c could bypass the universal
    // request lane through the upgrade path.
    var h2c_client = try Socket.connect(server.boundAddress().?, client_io);
    defer h2c_client.close();
    try h2c_client.setRecvTimeout(5_000);
    try h2c_client.sendAll(
        "GET /work HTTP/1.1\r\n" ++
            "Host: test\r\n" ++
            "Connection: Upgrade, HTTP2-Settings\r\n" ++
            "Upgrade: h2c\r\n" ++
            "HTTP2-Settings: AAMAAABkAAQAAP__\r\n\r\n",
    );
    var h2c_rejected_response: [1024]u8 = undefined;
    const h2c_rejected_len = try h2c_client.recv(&h2c_rejected_response);
    try std.testing.expect(mem.indexOf(u8, h2c_rejected_response[0..h2c_rejected_len], " 503 ") != null);
    try std.testing.expect(mem.indexOf(u8, h2c_rejected_response[0..h2c_rejected_len], "Connection: close\r\n") != null);
    try std.testing.expect(mem.indexOf(u8, h2c_rejected_response[0..h2c_rejected_len], " 101 ") == null);

    try std.testing.expectEqual(@as(u32, 1), State.handler_calls.load(.acquire));
    try std.testing.expectEqual(@as(u64, 2), server.runtimeStats().request_dispatch_rejections_total);
    try std.testing.expectEqual(@as(u64, 0), server.runtimeStats().h2_stream_dispatch_rejections_total);

    State.release_first.store(true, .release);
    var completed_response: [1024]u8 = undefined;
    const completed_len = try first_client.recv(&completed_response);
    try std.testing.expect(mem.indexOf(u8, completed_response[0..completed_len], " 200 ") != null);
}

test "canceled route is a response-free terminal transport outcome" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
        .max_connections = 1,
        .h1_disconnect_cancellation = .disabled,
    });
    defer server.deinit();
    try server.get("/cancel", struct {
        fn handle(_: *Context) anyerror!Response {
            return error.Canceled;
        }
    }.handle);

    var task = Server.ListenerTask.init(&server);
    try task.start();
    defer {
        task.requestStop();
        task.join() catch {};
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(server.boundAddress().?, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    try client.sendAll("GET /cancel HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    var response: [128]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try client.recv(&response));
    try std.testing.expectEqual(@as(u64, 1), server.runtimeStats().request_cancellations_total);
}

test "listener task remains valid after its owning handle moves" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();

    const Factory = struct {
        fn start(s: *Server) !Server.ListenerTask {
            var task = Server.ListenerTask.init(s);
            try task.start();
            return task;
        }
    };
    var task = try Factory.start(&server);
    try std.testing.expectEqual(Server.ListenerTask.RuntimeState.running, task.runtimeState());
    task.shutdown(1000);
    try task.join();
    try std.testing.expectEqual(Server.ListenerTask.RuntimeState.stopped, task.runtimeState());
}

test "reuse address preserves exclusive live listener ownership" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var first = Server.initWithConfig(allocator, io, .{
        .host = "127.0.0.1",
        .port = 0,
        .reuse_address = true,
    });
    defer first.deinit();
    try first.bind();
    const port = first.boundAddress().?.ip4.port;

    var second = Server.initWithConfig(allocator, io, .{
        .host = "127.0.0.1",
        .port = port,
        .reuse_address = true,
    });
    defer second.deinit();
    try std.testing.expectError(error.AddressInUse, second.bind());
}

test "reuse address permits an immediate listener restart" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var first = Server.initWithConfig(allocator, io, .{
        .host = "127.0.0.1",
        .port = 0,
        .reuse_address = true,
    });
    try first.bind();
    const port = first.boundAddress().?.ip4.port;
    // Exercise an accepted connection so the replacement covers the socket
    // states for which SO_REUSEADDR is needed, not merely a never-used bind.
    var client = try Socket.connect(first.boundAddress().?, io);
    var accepted = try first.listener.?.accept();
    accepted.socket.close();
    client.close();
    first.deinit();

    var replacement = Server.initWithConfig(allocator, io, .{
        .host = "127.0.0.1",
        .port = port,
        .reuse_address = true,
    });
    defer replacement.deinit();
    try replacement.bind();
}

test "ephemeral listeners remain independently bindable" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var first = Server.initWithConfig(allocator, io, .{ .host = "127.0.0.1", .port = 0 });
    defer first.deinit();
    var second = Server.initWithConfig(allocator, io, .{ .host = "127.0.0.1", .port = 0 });
    defer second.deinit();
    try first.bind();
    try second.bind();
    try std.testing.expect(first.boundAddress().?.ip4.port != second.boundAddress().?.ip4.port);
}

test "reuse port is an explicit live listener opt in" {
    if (comptime builtin.os.tag == .windows or builtin.os.tag == .wasi or
        builtin.os.tag == .freestanding or !@hasDecl(posix.SO, "REUSEPORT")) return error.SkipZigTest;

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    var first = Server.initWithConfig(allocator, io, .{
        .host = "127.0.0.1",
        .port = 0,
        .reuse_address = true,
        .reuse_port = true,
    });
    defer first.deinit();
    try first.bind();

    var second = Server.initWithConfig(allocator, io, .{
        .host = "127.0.0.1",
        .port = first.boundAddress().?.ip4.port,
        .reuse_address = true,
        .reuse_port = true,
    });
    defer second.deinit();
    try second.bind();
}

test "cross-thread graceful shutdown is listener-owned" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();
    try server.bind();
    const address = server.boundAddress().?;

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("graceful listener failed: {}", .{err});
        }
    }.run, .{&server});
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};
    // An idle keep-alive connection is not an active request and must not
    // consume the graceful request deadline.
    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(address, client_io);
    defer client.close();
    while (server.active_connections.load(.acquire) == 0) std.Thread.yield() catch {};
    const started = Io.Clock.awake.now(client_io).nanoseconds;
    server.shutdown(5000);
    listener_thread.join();
    const elapsed = Io.Clock.awake.now(client_io).nanoseconds - started;
    try std.testing.expect(!server.running);
    try std.testing.expectEqual(@as(usize, 0), server.active_connections.load(.acquire));
    try std.testing.expect(elapsed < std.time.ns_per_s);
}

test "H1 context preserves buffered pipeline input across client SHUT_WR" {
    if (builtin.os.tag == .windows) return;

    const State = struct {
        var first_saw_buffered_input = std.atomic.Value(bool).init(false);
        var second_handled = std.atomic.Value(bool).init(false);

        fn handler(ctx: *Context) anyerror!Response {
            if (mem.eql(u8, ctx.request.uri.path, "/a")) {
                first_saw_buffered_input.store(ctx.h1_has_buffered_input, .release);
                return ctx.text("A");
            }
            second_handled.store(true, .release);
            return ctx.text("B");
        }
    };
    State.first_saw_buffered_input.store(false, .release);
    State.second_handled.store(false, .release);

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer server.deinit();
    try server.get("/a", State.handler);
    try server.get("/b", State.handler);
    try server.bind();
    const address = server.boundAddress().?;

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("pipeline listener failed: {}", .{err});
        }
    }.run, .{&server});
    defer {
        server.stop();
        listener_thread.join();
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(address, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    // A and B deliberately share one write. The client half-closes only after
    // both complete requests are in flight, so EOF must not cancel A while B
    // is already held by the server's connection buffer.
    try client.sendAll(
        "GET /a HTTP/1.1\r\nHost: test\r\n\r\n" ++
            "GET /b HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n",
    );
    try client_io.vtable.netShutdown(client_io.userdata, client.handle, .send);

    var response: [1024]u8 = undefined;
    var response_len: usize = 0;
    while (true) {
        const n = try client.recv(response[response_len..]);
        if (n == 0) break;
        response_len += n;
        if (response_len == response.len) return error.TestUnexpectedResult;
    }

    try std.testing.expect(State.first_saw_buffered_input.load(.acquire));
    try std.testing.expect(State.second_handled.load(.acquire));
    try std.testing.expect(mem.indexOf(u8, response[0..response_len], "\r\n\r\nA") != null);
    try std.testing.expect(mem.indexOf(u8, response[0..response_len], "\r\n\r\nB") != null);
}

test "H1 orderly half close does not cancel an active response" {
    if (builtin.os.tag == .windows) return;

    const State = struct {
        var canceled = std.atomic.Value(bool).init(true);

        fn handler(ctx: *Context) anyerror!Response {
            try ctx.io.sleep(Io.Duration.fromMilliseconds(100), .awake);
            canceled.store(ctx.isCancellationRequested(), .release);
            return ctx.text("complete");
        }
    };
    State.canceled.store(true, .release);

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer server.deinit();
    try server.get("/slow", State.handler);
    try server.bind();

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("half-close listener failed: {}", .{err});
        }
    }.run, .{&server});
    defer {
        server.stop();
        listener_thread.join();
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(server.boundAddress().?, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    try client.sendAll("GET /slow HTTP/1.1\r\nHost: test\r\nConnection: close\r\n\r\n");
    try client_io.vtable.netShutdown(client_io.userdata, client.handle, .send);

    var response: [1024]u8 = undefined;
    var response_len: usize = 0;
    while (true) {
        const n = try client.recv(response[response_len..]);
        if (n == 0) break;
        response_len += n;
    }
    try std.testing.expect(mem.indexOf(u8, response[0..response_len], "complete") != null);
    try std.testing.expect(!State.canceled.load(.acquire));
    try std.testing.expectEqual(@as(u64, 0), server.httpRuntimeStats().h1_hard_disconnect_cancellations_total);
}

test "H1 hard disconnect remains observable behind pipelined input" {
    if (builtin.os.tag == .windows or builtin.os.tag == .freestanding) return;

    const State = struct {
        var started = std.atomic.Value(bool).init(false);
        var canceled = std.atomic.Value(bool).init(false);

        fn handler(ctx: *Context) anyerror!Response {
            started.store(true, .release);
            while (!ctx.isCancellationRequested())
                try ctx.io.sleep(Io.Duration.fromMilliseconds(1), .awake);
            canceled.store(true, .release);
            return error.Canceled;
        }
    };
    State.started.store(false, .release);
    State.canceled.store(false, .release);

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer server.deinit();
    try server.get("/slow", State.handler);
    try server.bind();

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("hard-disconnect listener failed: {}", .{err});
        }
    }.run, .{&server});
    defer {
        server.stop();
        listener_thread.join();
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(server.boundAddress().?, client_io);
    var client_open = true;
    defer if (client_open) client.close();
    try client.sendAll("GET /slow HTTP/1.1\r\nHost: test\r\n\r\n");
    while (!State.started.load(.acquire)) std.Thread.yield() catch {};
    while (server.httpRuntimeStats().active_h1_cancellation_observers != 1) std.Thread.yield() catch {};

    // Leave a partial next request unread while the active handler owns the
    // connection, then abort. Readability must be suppressed without dropping
    // the descriptor's hard-error observation.
    try client.sendAll("G");
    var observation_delay = std.posix.timespec{ .sec = 0, .nsec = 100 * std.time.ns_per_ms };
    _ = std.posix.system.nanosleep(&observation_delay, &observation_delay);
    var linger = std.posix.linger{ .onoff = 1, .linger = 0 };
    try std.posix.setsockopt(
        client.handle,
        std.posix.SOL.SOCKET,
        std.posix.SO.LINGER,
        std.mem.asBytes(&linger),
    );
    client.close();
    client_open = false;

    for (0..10_000) |_| {
        if (State.canceled.load(.acquire) and server.httpRuntimeStats().active_h1_cancellation_observers == 0) break;
        var delay = std.posix.timespec{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = std.posix.system.nanosleep(&delay, &delay);
    }
    try std.testing.expect(State.canceled.load(.acquire));
    try std.testing.expectEqual(@as(usize, 0), server.httpRuntimeStats().active_h1_cancellation_observers);
    try std.testing.expectEqual(@as(u64, 1), server.httpRuntimeStats().h1_hard_disconnect_cancellations_total);
}

test "H1 context does not treat a partial pipeline suffix as buffered input" {
    if (builtin.os.tag == .windows) return;

    const State = struct {
        var first_saw_buffered_input = std.atomic.Value(bool).init(true);

        fn handler(ctx: *Context) anyerror!Response {
            first_saw_buffered_input.store(ctx.h1_has_buffered_input, .release);
            return ctx.text("A");
        }
    };
    State.first_saw_buffered_input.store(true, .release);

    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{
        .host = "127.0.0.1",
        .port = 0,
    });
    defer server.deinit();
    try server.get("/a", State.handler);
    try server.bind();
    const address = server.boundAddress().?;

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("partial-pipeline listener failed: {}", .{err});
        }
    }.run, .{&server});
    defer {
        server.stop();
        listener_thread.join();
    }
    while (!server.listen_started.load(.acquire)) std.Thread.yield() catch {};

    const client_io = std.Io.Threaded.global_single_threaded.io();
    var client = try Socket.connect(address, client_io);
    defer client.close();
    try client.setRecvTimeout(5_000);
    // The trailing G starts a second request but cannot complete one. The
    // parser must report that distinction even though an orderly FIN remains
    // non-cancelling for the response already in progress.
    try client.sendAll("GET /a HTTP/1.1\r\nHost: test\r\n\r\nG");
    try client_io.vtable.netShutdown(client_io.userdata, client.handle, .send);

    var response: [1024]u8 = undefined;
    while (try client.recv(&response) != 0) {}

    try std.testing.expect(!State.first_saw_buffered_input.load(.acquire));
}

test "connection interruption preserves the fiber-owned descriptor" {
    if (builtin.os.tag == .windows) return;

    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();

    var client = try Socket.connect(listener.getLocalAddress(), io);
    defer client.close();
    var accepted = try listener.accept();
    defer accepted.socket.close();

    var h1_request_cancellation = std.atomic.Value(bool).init(false);
    var control = Server.ConnectionControl{
        .socket = &accepted.socket,
        .h1_request_cancellation = &h1_request_cancellation,
    };
    control.interrupt(false);

    const rc = posix.system.fcntl(accepted.socket.handle, posix.F.GETFD, @as(usize, 0));
    try std.testing.expectEqual(posix.E.SUCCESS, posix.errno(rc));
}

test "immediate stop preempts graceful request drain" {
    const allocator = std.testing.allocator;
    var io_impl = std.Io.Threaded.init(allocator, .{});
    defer io_impl.deinit();
    var server = Server.initWithConfig(allocator, io_impl.io(), .{ .host = "127.0.0.1", .port = 0 });
    defer server.deinit();
    try server.bind();
    server.active_requests.store(1, .release);
    server.shutdown(10_000);

    const listener_thread = try std.Thread.spawn(.{}, struct {
        fn run(s: *Server) void {
            s.listen() catch |err| std.debug.panic("draining listener failed: {}", .{err});
        }
    }.run, .{&server});
    const caller_io = std.Io.Threaded.global_single_threaded.io();
    caller_io.sleep(Io.Duration.fromMilliseconds(20), .awake) catch {};
    const started = Io.Clock.awake.now(caller_io).nanoseconds;
    server.stop();
    listener_thread.join();
    const elapsed = Io.Clock.awake.now(caller_io).nanoseconds - started;
    server.active_requests.store(0, .release);
    try std.testing.expect(elapsed < std.time.ns_per_s);
}

test "containsTraversal rejects double-encoded dot-dot" {
    // %252e%252e decodes to %2e%2e (first decode), then .. (second decode).
    // containsTraversal checks percent-encoded dots directly.
    try std.testing.expect(containsTraversal("%2e%2e"));
    try std.testing.expect(containsTraversal("%2E%2E"));
    try std.testing.expect(containsTraversal("%2e."));
    try std.testing.expect(containsTraversal(".%2e"));
    try std.testing.expect(containsTraversal(".%2E"));
}

test "containsTraversal rejects percent-encoded slashes" {
    try std.testing.expect(containsTraversal("foo%2fbar"));
    try std.testing.expect(containsTraversal("foo%2Fbar"));
}

test "containsTraversal rejects backslashes" {
    try std.testing.expect(containsTraversal("foo\\bar"));
    try std.testing.expect(containsTraversal("..\\etc"));
}

test "containsTraversal rejects null bytes" {
    try std.testing.expect(containsTraversal("foo\x00bar"));
}

test "containsTraversal rejects absolute paths" {
    try std.testing.expect(containsTraversal("/etc/passwd"));
}

test "containsTraversal allows safe paths" {
    try std.testing.expect(!containsTraversal("assets/style.css"));
    try std.testing.expect(!containsTraversal("images/photo.jpg"));
    try std.testing.expect(!containsTraversal("file.txt"));
}

test "stream writer preserves small SSE write density and supports oversized events" {
    const Capture = struct {
        bytes: std.ArrayListUnmanaged(u8) = .empty,
        writes: usize = 0,

        fn write(self: *@This(), data: []const u8) !void {
            try self.bytes.appendSlice(std.testing.allocator, data);
            self.writes += 1;
        }
    };

    var capture = Capture{};
    defer capture.bytes.deinit(std.testing.allocator);
    try Context.StreamWriter.writeEventTo(&capture, null, "small");
    try std.testing.expectEqual(@as(usize, 1), capture.writes);
    try std.testing.expectEqualStrings("data: small\n\n", capture.bytes.items);

    capture.bytes.clearRetainingCapacity();
    capture.writes = 0;
    const large = [_]u8{'x'} ** 9000;
    try Context.StreamWriter.writeEventTo(&capture, "message", &large);
    try std.testing.expect(capture.writes > 1);
    try std.testing.expect(std.mem.startsWith(u8, capture.bytes.items, "event: message\ndata: "));
    try std.testing.expect(std.mem.endsWith(u8, capture.bytes.items, "\n\n"));
    try std.testing.expectEqual(@as(usize, "event: message\ndata: ".len + large.len + 2), capture.bytes.items.len);
}

test "SSE rejects CR in id field" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/sse");
    defer req.deinit();
    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const events = [_]SseEvent{.{ .data = "data", .id = "bad\rid", .event = null, .retry_ms = null }};
    const result = ctx.sse(&events);
    try std.testing.expectError(error.InvalidSseField, result);
}

test "SSE rejects LF in event field" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/sse");
    defer req.deinit();
    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const events = [_]SseEvent{.{ .data = "data", .id = null, .event = "bad\nevent", .retry_ms = null }};
    const result = ctx.sse(&events);
    try std.testing.expectError(error.InvalidSseField, result);
}

test "setCookie rejects CR/LF in name" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();
    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const result = ctx.setCookie("bad\rname", "value", .{});
    try std.testing.expectError(error.HeaderContainsCrLf, result);
}

test "setCookie rejects CR/LF in value" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();
    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const result = ctx.setCookie("name", "bad\nvalue", .{});
    try std.testing.expectError(error.HeaderContainsCrLf, result);
}

test "removeCookie rejects CR/LF in name" {
    const allocator = std.testing.allocator;
    var req = try Request.init(allocator, .GET, "/");
    defer req.deinit();
    var ctx = Context.init(allocator, std.testing.io, &req);
    defer ctx.deinit();

    const result = ctx.removeCookie("bad\nname", .{});
    try std.testing.expectError(error.HeaderContainsCrLf, result);
}
