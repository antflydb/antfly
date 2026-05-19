//! Core HTTP Types for httpx.zig
//!
//! This module provides fundamental HTTP type definitions including methods,
//! protocol versions (HTTP/1.0, HTTP/1.1, HTTP/2, HTTP/3), error types,
//! content types, and configuration structures for timeouts, retries, and redirects.
//!
//! All types are designed for zero-allocation operation where possible and
//! provide compile-time string conversion for maximum performance.

const std = @import("std");

/// HTTP request methods as defined in RFC 7231 and RFC 5789.
/// Supports all standard methods plus a CUSTOM variant for extensions.
pub const Method = enum {
    GET,
    POST,
    PUT,
    DELETE,
    PATCH,
    HEAD,
    OPTIONS,
    TRACE,
    CONNECT,
    CUSTOM,

    /// Converts the method to its canonical string representation.
    pub fn toString(self: Method) []const u8 {
        return switch (self) {
            .GET => "GET",
            .POST => "POST",
            .PUT => "PUT",
            .DELETE => "DELETE",
            .PATCH => "PATCH",
            .HEAD => "HEAD",
            .OPTIONS => "OPTIONS",
            .TRACE => "TRACE",
            .CONNECT => "CONNECT",
            .CUSTOM => "CUSTOM",
        };
    }

    /// Parses a string into a Method enum value.
    /// Returns null for unrecognized method strings.
    pub fn fromString(str: []const u8) ?Method {
        const map = std.StaticStringMap(Method).initComptime(.{
            .{ "GET", .GET },
            .{ "POST", .POST },
            .{ "PUT", .PUT },
            .{ "DELETE", .DELETE },
            .{ "PATCH", .PATCH },
            .{ "HEAD", .HEAD },
            .{ "OPTIONS", .OPTIONS },
            .{ "TRACE", .TRACE },
            .{ "CONNECT", .CONNECT },
        });
        return map.get(str);
    }

    /// Returns true if the method is idempotent per RFC 7231.
    /// Idempotent methods can be safely retried without side effects.
    pub fn isIdempotent(self: Method) bool {
        return switch (self) {
            .GET, .HEAD, .PUT, .DELETE, .OPTIONS, .TRACE => true,
            .POST, .PATCH, .CONNECT, .CUSTOM => false,
        };
    }

    /// Returns true if the method is considered safe per RFC 7231.
    /// Safe methods should not cause side effects on the server.
    pub fn isSafe(self: Method) bool {
        return switch (self) {
            .GET, .HEAD, .OPTIONS, .TRACE => true,
            else => false,
        };
    }

    /// Returns true if the method typically includes a request body.
    pub fn hasRequestBody(self: Method) bool {
        return switch (self) {
            .POST, .PUT, .PATCH => true,
            else => false,
        };
    }

    /// Returns true if the method expects a response body.
    pub fn hasResponseBody(self: Method) bool {
        return switch (self) {
            .HEAD => false,
            else => true,
        };
    }
};

/// HTTP protocol versions including HTTP/2 and HTTP/3 (QUIC).
pub const Version = enum {
    HTTP_1_0,
    HTTP_1_1,
    HTTP_2,
    HTTP_3,

    /// Returns the canonical string representation of the version.
    pub fn toString(self: Version) []const u8 {
        return switch (self) {
            .HTTP_1_0 => "HTTP/1.0",
            .HTTP_1_1 => "HTTP/1.1",
            .HTTP_2 => "HTTP/2",
            .HTTP_3 => "HTTP/3",
        };
    }

    /// Parses a version string into a Version enum.
    pub fn fromString(str: []const u8) ?Version {
        const map = std.StaticStringMap(Version).initComptime(.{
            .{ "HTTP/1.0", .HTTP_1_0 },
            .{ "HTTP/1.1", .HTTP_1_1 },
            .{ "HTTP/2", .HTTP_2 },
            .{ "HTTP/2.0", .HTTP_2 },
            .{ "HTTP/3", .HTTP_3 },
            .{ "HTTP/3.0", .HTTP_3 },
        });
        return map.get(str);
    }

    /// Returns true if the version supports multiplexing.
    pub fn supportsMultiplexing(self: Version) bool {
        return self == .HTTP_2 or self == .HTTP_3;
    }

    /// Returns true if the version supports server push.
    pub fn supportsServerPush(self: Version) bool {
        return self == .HTTP_2;
    }

    /// Returns true if the version uses QUIC transport.
    pub fn usesQuic(self: Version) bool {
        return self == .HTTP_3;
    }

    /// Returns true if the version requires TLS by specification.
    pub fn requiresTls(self: Version) bool {
        return self == .HTTP_2 or self == .HTTP_3;
    }
};

/// HTTP error types with context information for debugging.
pub const HttpError = error{
    ConnectionFailed,
    ConnectionReset,
    ConnectionTimeout,
    InvalidUri,
    InvalidResponse,
    InvalidHeader,
    InvalidChunkSize,
    TooManyRedirects,
    TlsHandshakeFailed,
    TlsCertificateError,
    TlsError,
    ResponseTooLarge,
    RequestTooLarge,
    Timeout,
    HostUnreachable,
    DnsResolutionFailed,
    ProtocolError,
    StreamError,
    FlowControlError,
    FrameError,
    CompressionError,
    Http2Error,
    Http3Error,
    QuicError,
    OutOfMemory,
};

/// Common MIME content types for HTTP messages.
pub const ContentType = enum {
    text_plain,
    text_html,
    text_css,
    text_javascript,
    application_json,
    application_xml,
    application_octet_stream,
    application_form_urlencoded,
    multipart_form_data,
    image_png,
    image_jpeg,
    image_gif,
    image_webp,
    image_svg,

    /// Returns the MIME type string.
    pub fn toString(self: ContentType) []const u8 {
        return switch (self) {
            .text_plain => "text/plain",
            .text_html => "text/html",
            .text_css => "text/css",
            .text_javascript => "text/javascript",
            .application_json => "application/json",
            .application_xml => "application/xml",
            .application_octet_stream => "application/octet-stream",
            .application_form_urlencoded => "application/x-www-form-urlencoded",
            .multipart_form_data => "multipart/form-data",
            .image_png => "image/png",
            .image_jpeg => "image/jpeg",
            .image_gif => "image/gif",
            .image_webp => "image/webp",
            .image_svg => "image/svg+xml",
        };
    }

    /// Parses a MIME type string into a ContentType enum.
    /// Handles parameters (e.g. "application/json; charset=utf-8") by
    /// extracting the base type before the semicolon.
    pub fn fromString(str: []const u8) ?ContentType {
        const map = std.StaticStringMap(ContentType).initComptime(.{
            .{ "text/plain", .text_plain },
            .{ "text/html", .text_html },
            .{ "text/css", .text_css },
            .{ "text/javascript", .text_javascript },
            .{ "application/json", .application_json },
            .{ "application/xml", .application_xml },
            .{ "application/octet-stream", .application_octet_stream },
            .{ "application/x-www-form-urlencoded", .application_form_urlencoded },
            .{ "multipart/form-data", .multipart_form_data },
            .{ "image/png", .image_png },
            .{ "image/jpeg", .image_jpeg },
            .{ "image/gif", .image_gif },
            .{ "image/webp", .image_webp },
            .{ "image/svg+xml", .image_svg },
        });
        // Strip parameters (e.g. "; charset=utf-8") before lookup.
        const base = if (std.mem.indexOfScalar(u8, str, ';')) |i|
            std.mem.trim(u8, str[0..i], " \t")
        else
            str;
        return map.get(base);
    }
};

/// Transfer encoding types for HTTP message bodies.
pub const TransferEncoding = enum {
    identity,
    chunked,
    gzip,
    deflate,
    br,

    pub fn toString(self: TransferEncoding) []const u8 {
        return switch (self) {
            .identity => "identity",
            .chunked => "chunked",
            .gzip => "gzip",
            .deflate => "deflate",
            .br => "br",
        };
    }
};

/// Timeout configuration for HTTP operations in milliseconds.
pub const Timeouts = struct {
    connect_ms: u64 = 30_000,
    read_ms: u64 = 30_000,
    write_ms: u64 = 30_000,
    keep_alive_ms: u64 = 60_000,
    idle_ms: u64 = 120_000,
    request_ms: u64 = 0,

    /// Creates a timeout configuration with all values set uniformly.
    pub fn uniform(ms: u64) Timeouts {
        return .{
            .connect_ms = ms,
            .read_ms = ms,
            .write_ms = ms,
            .keep_alive_ms = ms * 2,
            .idle_ms = ms * 4,
        };
    }

    /// Creates timeouts optimized for fast operations.
    pub fn fast() Timeouts {
        return uniform(5_000);
    }

    /// Creates timeouts for long-running operations.
    pub fn slow() Timeouts {
        return uniform(120_000);
    }

    /// Disables all timeouts (use with caution).
    pub fn none() Timeouts {
        return .{
            .connect_ms = 0,
            .read_ms = 0,
            .write_ms = 0,
            .keep_alive_ms = 0,
            .idle_ms = 0,
        };
    }
};

/// Retry policy configuration with exponential backoff support.
pub const RetryPolicy = struct {
    max_retries: u32 = 3,
    initial_delay_ms: u64 = 1000,
    max_delay_ms: u64 = 30_000,
    backoff_multiplier: f64 = 2.0,
    retry_on_status: []const u16 = &[_]u16{ 429, 500, 502, 503, 504 },
    retry_on_connection_error: bool = true,
    retry_only_idempotent: bool = true,

    /// Calculates the delay for a given retry attempt using exponential backoff.
    pub fn calculateDelay(self: RetryPolicy, attempt: u32) u64 {
        if (attempt == 0) return 0;
        const multiplier = std.math.pow(f64, self.backoff_multiplier, @as(f64, @floatFromInt(attempt - 1)));
        const delay = @as(u64, @intFromFloat(@as(f64, @floatFromInt(self.initial_delay_ms)) * multiplier));
        return @min(delay, self.max_delay_ms);
    }

    /// Returns true if the given status code should trigger a retry.
    pub fn shouldRetryStatus(self: RetryPolicy, status: u16) bool {
        for (self.retry_on_status) |s| {
            if (s == status) return true;
        }
        return false;
    }

    /// Creates a policy that never retries.
    pub fn noRetry() RetryPolicy {
        return .{ .max_retries = 0 };
    }

    /// Creates an aggressive retry policy for critical requests.
    pub fn aggressive() RetryPolicy {
        return .{
            .max_retries = 5,
            .initial_delay_ms = 500,
            .backoff_multiplier = 1.5,
        };
    }
};

/// Redirect policy configuration for HTTP clients.
pub const RedirectPolicy = struct {
    max_redirects: u32 = 10,
    follow_redirects: bool = true,
    preserve_method: bool = false,
    preserve_headers: bool = true,
    allow_cross_origin: bool = true,

    /// Returns the appropriate method to use after a redirect.
    pub fn getRedirectMethod(self: RedirectPolicy, status: u16, original: Method) Method {
        if (self.preserve_method) return original;
        return switch (status) {
            301, 302 => .GET,
            303 => .GET,
            307, 308 => original,
            else => original,
        };
    }

    /// Creates a policy that doesn't follow redirects.
    pub fn noFollow() RedirectPolicy {
        return .{ .follow_redirects = false };
    }

    /// Creates a strict policy that preserves method on redirects.
    pub fn strict() RedirectPolicy {
        return .{ .preserve_method = true };
    }
};

/// Default maximum body/response size (100 MB).
pub const default_max_body_size: usize = 100 * 1024 * 1024;

/// HTTP/2 specific settings as defined in RFC 7540.
pub const Http2Settings = struct {
    header_table_size: u32 = 4096,
    enable_push: bool = false,
    max_concurrent_streams: u32 = 100,
    initial_window_size: u32 = 65535,
    max_frame_size: u32 = 16384,
    max_header_list_size: u32 = 8192,
};

/// HTTP/3 and QUIC specific settings.
pub const Http3Settings = struct {
    max_field_section_size: u64 = 8192,
    qpack_max_table_capacity: u64 = 4096,
    qpack_blocked_streams: u64 = 100,
    enable_connect_protocol: bool = true,
    enable_datagrams: bool = false,
};

test "Method.fromString" {
    try std.testing.expectEqual(Method.GET, Method.fromString("GET").?);
    try std.testing.expectEqual(Method.POST, Method.fromString("POST").?);
    try std.testing.expect(Method.fromString("INVALID") == null);
}

test "Method properties" {
    try std.testing.expect(Method.GET.isIdempotent());
    try std.testing.expect(Method.GET.isSafe());
    try std.testing.expect(!Method.POST.isIdempotent());
    try std.testing.expect(Method.POST.hasRequestBody());
}

test "Version.fromString" {
    try std.testing.expectEqual(Version.HTTP_1_1, Version.fromString("HTTP/1.1").?);
    try std.testing.expectEqual(Version.HTTP_2, Version.fromString("HTTP/2").?);
    try std.testing.expectEqual(Version.HTTP_2, Version.fromString("HTTP/2.0").?);
    try std.testing.expectEqual(Version.HTTP_3, Version.fromString("HTTP/3").?);
    try std.testing.expectEqual(Version.HTTP_3, Version.fromString("HTTP/3.0").?);
    try std.testing.expect(Version.fromString("INVALID") == null);
}

test "Version properties" {
    try std.testing.expect(Version.HTTP_2.supportsMultiplexing());
    try std.testing.expect(Version.HTTP_3.usesQuic());
    try std.testing.expect(!Version.HTTP_1_1.supportsMultiplexing());
}

test "ContentType.fromString" {
    try std.testing.expectEqual(ContentType.application_json, ContentType.fromString("application/json").?);
    try std.testing.expectEqual(ContentType.text_html, ContentType.fromString("text/html; charset=utf-8").?);
}

test "RetryPolicy.calculateDelay" {
    const policy = RetryPolicy{};
    try std.testing.expectEqual(@as(u64, 0), policy.calculateDelay(0));
    try std.testing.expectEqual(@as(u64, 1000), policy.calculateDelay(1));
    try std.testing.expectEqual(@as(u64, 2000), policy.calculateDelay(2));
}

test "RetryPolicy calculateDelay exponential backoff" {
    const policy = RetryPolicy{};
    try std.testing.expectEqual(@as(u64, 0), policy.calculateDelay(0));
    try std.testing.expectEqual(@as(u64, 1000), policy.calculateDelay(1));
    try std.testing.expectEqual(@as(u64, 2000), policy.calculateDelay(2));
    try std.testing.expectEqual(@as(u64, 4000), policy.calculateDelay(3));
}

test "RetryPolicy calculateDelay caps at max" {
    const policy = RetryPolicy{};
    const delay = policy.calculateDelay(10);
    try std.testing.expect(delay <= 30000);
    try std.testing.expectEqual(@as(u64, 30000), delay);
}

test "RetryPolicy shouldRetryStatus defaults" {
    const policy = RetryPolicy{};
    // These should trigger retry
    try std.testing.expect(policy.shouldRetryStatus(429));
    try std.testing.expect(policy.shouldRetryStatus(500));
    try std.testing.expect(policy.shouldRetryStatus(502));
    try std.testing.expect(policy.shouldRetryStatus(503));
    try std.testing.expect(policy.shouldRetryStatus(504));
    // These should not trigger retry
    try std.testing.expect(!policy.shouldRetryStatus(200));
    try std.testing.expect(!policy.shouldRetryStatus(301));
    try std.testing.expect(!policy.shouldRetryStatus(400));
    try std.testing.expect(!policy.shouldRetryStatus(401));
    try std.testing.expect(!policy.shouldRetryStatus(403));
    try std.testing.expect(!policy.shouldRetryStatus(404));
}

test "RetryPolicy noRetry" {
    const policy = RetryPolicy.noRetry();
    try std.testing.expectEqual(@as(u32, 0), policy.max_retries);
}

test "RetryPolicy aggressive" {
    const policy = RetryPolicy.aggressive();
    try std.testing.expectEqual(@as(u32, 5), policy.max_retries);
    try std.testing.expectEqual(@as(u64, 500), policy.initial_delay_ms);
}

test "RetryPolicy custom status codes" {
    const policy = RetryPolicy{ .retry_on_status = &[_]u16{ 418, 503 } };
    try std.testing.expect(policy.shouldRetryStatus(418));
    try std.testing.expect(policy.shouldRetryStatus(503));
    try std.testing.expect(!policy.shouldRetryStatus(500));
}

test "RedirectPolicy.getRedirectMethod" {
    const policy = RedirectPolicy{};
    try std.testing.expectEqual(Method.GET, policy.getRedirectMethod(301, .POST));
    try std.testing.expectEqual(Method.POST, policy.getRedirectMethod(307, .POST));
}

test "RedirectPolicy 301 converts POST to GET" {
    const policy = RedirectPolicy{};
    try std.testing.expectEqual(Method.GET, policy.getRedirectMethod(301, .POST));
}

test "RedirectPolicy 302 converts POST to GET" {
    const policy = RedirectPolicy{};
    try std.testing.expectEqual(Method.GET, policy.getRedirectMethod(302, .POST));
}

test "RedirectPolicy 307 preserves method" {
    const policy = RedirectPolicy{};
    try std.testing.expectEqual(Method.POST, policy.getRedirectMethod(307, .POST));
}

test "RedirectPolicy 308 preserves method" {
    const policy = RedirectPolicy{};
    try std.testing.expectEqual(Method.PUT, policy.getRedirectMethod(308, .PUT));
}

test "RedirectPolicy 303 always GET" {
    const policy = RedirectPolicy{};
    try std.testing.expectEqual(Method.GET, policy.getRedirectMethod(303, .DELETE));
}

test "RedirectPolicy strict preserves all" {
    const policy = RedirectPolicy.strict();
    try std.testing.expectEqual(Method.POST, policy.getRedirectMethod(301, .POST));
}

test "RedirectPolicy noFollow" {
    const policy = RedirectPolicy.noFollow();
    try std.testing.expectEqual(false, policy.follow_redirects);
}

test "RedirectPolicy defaults" {
    const policy = RedirectPolicy{};
    try std.testing.expectEqual(@as(u32, 10), policy.max_redirects);
    try std.testing.expectEqual(true, policy.follow_redirects);
    try std.testing.expectEqual(false, policy.preserve_method);
}
