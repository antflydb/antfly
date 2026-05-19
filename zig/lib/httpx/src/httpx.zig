//! httpx.zig - Production-Ready HTTP Library for Zig
//!
//! A comprehensive HTTP client and server library with production-ready HTTP/1.x
//! runtime support and HTTP/2/HTTP/3 protocol primitives.
//!
//! ## Important Note
//!
//! **httpx.zig implements HTTP/2 and HTTP/3 from scratch.** Zig's standard library
//! does not provide HTTP/2, HTTP/3, or QUIC support. This library contains complete
//! custom implementations of these protocols:
//!
//! - **HTTP/2**: HPACK header compression (RFC 7541), stream multiplexing, flow control (RFC 7540)
//! - **HTTP/3**: QPACK header compression (RFC 9204), HTTP/3 framing (RFC 9114)
//! - **QUIC**: Transport framing, packet structures, variable-length integers (RFC 9000)
//!
//! ## Supported Protocols
//!
//! - **HTTP/1.0**: Basic request-response semantics
//! - **HTTP/1.1**: Persistent connections, chunked transfer, pipelining
//! - **HTTP/2**: HPACK compression and framing primitives for advanced integrations
//! - **HTTP/3**: QPACK compression and QUIC framing primitives for advanced integrations
//!
//! ## Platform Support
//!
//! - Linux (x86, x86_64, aarch64, arm)
//! - Windows (x86, x86_64, aarch64, arm)
//! - macOS (x86, x86_64, aarch64, arm)
//! - FreeBSD, NetBSD, OpenBSD
//!
//! ## Features
//!
//! ### Client Features
//! - Connection pooling with keep-alive
//! - Automatic retry with exponential backoff
//! - Redirect following with configurable policies
//! - Request/response interceptors
//! - Concurrent request execution
//! - TLS/SSL support (HTTPS)
//! - Timeout configuration
//! - Cookie handling
//!
//! ### Server Features
//! - Pattern-based routing with path parameters
//! - Middleware stack (CORS, logging, rate limiting, etc.)
//! - Static file serving
//! - JSON response helpers
//! - Request context with user data
//!
//! ### Protocol Features
//! - HTTP/2 HPACK header compression (RFC 7541)
//! - HTTP/2 stream state machine and flow control
//! - HTTP/3 QPACK header compression (RFC 9204)
//! - QUIC transport framing (RFC 9000)
//!
//! ## Quick Start
//!
//! ```zig
//! const httpx = @import("httpx");
//!
//! // Client usage
//! var client = httpx.Client.init(allocator);
//! defer client.deinit();
//! const response = try client.get("https://api.example.com/users", .{});
//!
//! // Server usage
//! var server = httpx.Server.init(allocator);
//! try server.get("/hello", helloHandler);
//! try server.listen();
//! ```

const std = @import("std");

pub const types = @import("core/types.zig");
pub const meta = @import("core/meta.zig");
pub const headers = @import("core/headers.zig");
pub const uri = @import("core/uri.zig");
pub const status = @import("core/status.zig");
pub const request = @import("core/request.zig");
pub const response = @import("core/response.zig");

pub const http = @import("protocol/http.zig");
pub const parser = @import("protocol/parser.zig");
pub const hpack = @import("protocol/hpack.zig");
pub const stream = @import("protocol/stream.zig");
pub const qpack = @import("protocol/qpack.zig");
pub const quic = @import("protocol/quic.zig");
pub const h2_connection = @import("protocol/h2_connection.zig");

pub const socket = @import("net/socket.zig");
pub const address = @import("net/address.zig");

pub const tls = @import("tls/tls.zig");

pub const client_mod = @import("client/client.zig");
pub const pool = @import("client/pool.zig");

pub const server_mod = @import("server/server.zig");
pub const router = @import("server/router.zig");
pub const middleware = @import("server/middleware.zig");

pub const buffer = @import("util/buffer.zig");
pub const encoding = @import("util/encoding.zig");
pub const json = @import("util/json.zig");
pub const common = @import("util/common.zig");

pub const testing_mod = @import("testing.zig");

pub const executor = @import("concurrency/executor.zig");
pub const concurrency = @import("concurrency/pool.zig");

pub const RequestSpec = concurrency.RequestSpec;
pub const RequestResult = concurrency.RequestResult;
pub const BatchBuilder = concurrency.BatchBuilder;

pub const Executor = executor.Executor;
pub const Task = executor.Task;

pub const Method = types.Method;
pub const Version = types.Version;
pub const HttpError = types.HttpError;
pub const ContentType = types.ContentType;
pub const Timeouts = types.Timeouts;
pub const RetryPolicy = types.RetryPolicy;
pub const RedirectPolicy = types.RedirectPolicy;
pub const Http2Settings = types.Http2Settings;
pub const Http3Settings = types.Http3Settings;

pub const Headers = headers.Headers;
pub const HeaderName = headers.HeaderName;
pub const Header = headers.Header;

pub const Uri = uri.Uri;

pub const Status = status.Status;
pub const StatusCode = status.StatusCode;

pub const Request = request.Request;
pub const RequestBuilder = request.RequestBuilder;

pub const Response = response.Response;
pub const ResponseBuilder = response.ResponseBuilder;

pub const Socket = socket.Socket;
pub const TcpListener = socket.TcpListener;
pub const UdpSocket = socket.UdpSocket;

pub const Parser = parser.Parser;

pub const Http2ConnectionSettings = http.Http2ConnectionSettings;
pub const Http2FrameType = http.Http2FrameType;
pub const Http2FrameHeader = http.Http2FrameHeader;
pub const Http2ErrorCode = http.Http2ErrorCode;
pub const Http3FrameType = http.Http3FrameType;
pub const Http3ErrorCode = http.Http3ErrorCode;
pub const AlpnProtocol = http.AlpnProtocol;
pub const NegotiatedProtocol = http.NegotiatedProtocol;

// HTTP/2 HPACK exports
pub const HpackContext = hpack.HpackContext;
pub const HpackStaticTable = hpack.StaticTable;
pub const HpackDynamicTable = hpack.DynamicTable;
pub const encodeHpackHeaders = hpack.encodeHeaders;
pub const decodeHpackHeaders = hpack.decodeHeaders;

// HTTP/2 Connection exports
pub const H2Connection = h2_connection.H2Connection;
pub const H2Frame = h2_connection.Frame;

// HTTP/2 Stream exports
pub const Stream = stream.Stream;
pub const StreamState = stream.StreamState;
pub const StreamManager = stream.StreamManager;
pub const StreamPriority = stream.StreamPriority;

// HTTP/3 QPACK exports
pub const QpackContext = qpack.QpackContext;
pub const QpackStaticTable = qpack.StaticTable;
pub const encodeQpackHeaders = qpack.encodeHeaders;
pub const decodeQpackHeaders = qpack.decodeHeaders;

// QUIC exports
pub const QuicVersion = quic.Version;
pub const QuicLongHeader = quic.LongHeader;
pub const QuicShortHeader = quic.ShortHeader;
pub const QuicConnectionId = quic.ConnectionId;
pub const QuicFrameType = quic.FrameType;
pub const QuicTransportError = quic.TransportError;
pub const QuicStreamFrame = quic.StreamFrame;
pub const QuicCryptoFrame = quic.CryptoFrame;
pub const QuicAckFrame = quic.AckFrame;
pub const QuicTransportParameters = quic.TransportParameters;

pub const encodeChunkedBody = http.encodeChunkedBody;
pub const isH2cUpgradeRequest = http.isH2cUpgradeRequest;
pub const negotiateVersion = http.negotiateVersion;

pub const Client = client_mod.Client;
pub const ClientConfig = client_mod.ClientConfig;
pub const RequestOptions = client_mod.RequestOptions;
pub const WriterProgress = client_mod.WriterProgress;
pub const WriterProgressCallback = client_mod.WriterProgressCallback;
pub const Interceptor = client_mod.Interceptor;
pub const RequestInterceptor = client_mod.RequestInterceptor;
pub const ResponseInterceptor = client_mod.ResponseInterceptor;

pub const ConnectionPool = pool.ConnectionPool;
pub const PoolConfig = pool.PoolConfig;
pub const Connection = pool.Connection;
pub const PoolStats = pool.PoolStats;

pub const Server = server_mod.Server;
pub const ServerConfig = server_mod.ServerConfig;
pub const Context = server_mod.Context;
pub const Handler = server_mod.Handler;
pub const CookieOptions = server_mod.CookieOptions;
pub const SameSite = server_mod.SameSite;
pub const SseEvent = server_mod.SseEvent;
pub const StreamWriter = server_mod.Context.StreamWriter;
pub const PreRouteHook = server_mod.PreRouteHook;

pub const Router = router.Router;
pub const RouteGroup = router.RouteGroup;
pub const RouteMatch = router.RouteMatch;

pub const Middleware = middleware.Middleware;
pub const Next = middleware.Next;
pub const cors = middleware.cors;
pub const logger = middleware.logger;
pub const helmet = middleware.helmet;

pub const RingBuffer = buffer.RingBuffer;
pub const FixedBuffer = buffer.FixedBuffer;

pub const Base64 = encoding.Base64;
pub const Hex = encoding.Hex;
pub const PercentEncoding = encoding.PercentEncoding;
pub const CookiePair = common.CookiePair;

pub const TlsConfig = tls.TlsConfig;
pub const TlsSession = tls.TlsSession;

pub const TestServer = testing_mod.TestServer;
pub const TestRoute = testing_mod.Route;
pub const TestResponseSpec = testing_mod.ResponseSpec;

pub const VERSION = meta.version;
pub const DEFAULT_USER_AGENT = meta.default_user_agent;

/// Returns a query parameter value from a raw query string.
pub const queryValue = common.queryValue;

/// Parses the first name/value pair from a Set-Cookie header value.
pub const parseSetCookiePair = common.parseSetCookiePair;

/// Executes all requests in parallel and returns a result per request.
pub fn all(allocator: std.mem.Allocator, client: *Client, specs: []const RequestSpec) ![]RequestResult {
    return concurrency.all(allocator, client, specs);
}

/// Executes all requests in parallel and returns the first 2xx response (if any).
pub fn any(allocator: std.mem.Allocator, client: *Client, specs: []const RequestSpec) !?Response {
    return concurrency.any(allocator, client, specs);
}

/// Executes all requests in parallel and returns the first completion (success or error).
pub fn race(allocator: std.mem.Allocator, client: *Client, specs: []const RequestSpec) !RequestResult {
    return concurrency.race(allocator, client, specs);
}


/// Convenience function to create a GET request.
pub fn get(allocator: std.mem.Allocator, io: std.Io, url: []const u8) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.get(url, .{});
}

/// Convenience function to create a one-shot request with an explicit method.
pub fn sendRequest(
    allocator: std.mem.Allocator,
    io: std.Io,
    method: Method,
    url: []const u8,
    req_options: RequestOptions,
) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.request(method, url, req_options);
}

/// Convenience function to create a POST request with JSON body.
pub fn postJson(allocator: std.mem.Allocator, io: std.Io, url: []const u8, body: []const u8) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.post(url, .{ .json = body });
}

/// Convenience function to create a POST request.
pub fn post(allocator: std.mem.Allocator, io: std.Io, url: []const u8, req_options: RequestOptions) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.post(url, req_options);
}

/// Convenience function to create a PUT request.
pub fn put(allocator: std.mem.Allocator, io: std.Io, url: []const u8, req_options: RequestOptions) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.put(url, req_options);
}

/// Convenience function to create a DELETE request.
pub fn del(allocator: std.mem.Allocator, io: std.Io, url: []const u8, req_options: RequestOptions) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.delete(url, req_options);
}

/// Convenience function to create a PATCH request.
pub fn patch(allocator: std.mem.Allocator, io: std.Io, url: []const u8, req_options: RequestOptions) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.patch(url, req_options);
}

/// Convenience function to create a HEAD request.
pub fn head(allocator: std.mem.Allocator, io: std.Io, url: []const u8, req_options: RequestOptions) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.head(url, req_options);
}

/// Convenience function to create an OPTIONS request.
pub fn options(allocator: std.mem.Allocator, io: std.Io, url: []const u8, options_in: RequestOptions) !Response {
    var c = Client.init(allocator, io);
    defer c.deinit();
    return c.options(url, options_in);
}

test "core types" {
    _ = types;
}

test "headers" {
    _ = headers;
}

test "uri" {
    _ = uri;
}

test "status" {
    _ = status;
}

test "request" {
    _ = request;
}

test "response" {
    _ = response;
}

test "http protocol" {
    _ = http;
}

test "hpack" {
    _ = hpack;
}

test "stream" {
    _ = stream;
}

test "qpack" {
    _ = qpack;
}

test "quic" {
    _ = quic;
}

test "h2 connection" {
    _ = h2_connection;
}

test "parser" {
    _ = parser;
}

test "fuzz" {
    _ = @import("protocol/fuzz.zig");
}

test "buffer" {
    _ = buffer;
}

test "encoding" {
    _ = encoding;
}

test "json" {
    _ = json;
}

test "common" {
    _ = common;
}

test "socket" {
    _ = socket;
}

test "address" {
    _ = address;
}

test "tls" {
    _ = tls;
}

test "server" {
    _ = server_mod;
}

test "router" {
    _ = router;
}

test "middleware" {
    _ = middleware;
}

test "client" {
    _ = client_mod;
}

test "pool" {
    _ = pool;
}

test "concurrency" {
    _ = concurrency;
}

test "testing" {
    _ = testing_mod;
}
