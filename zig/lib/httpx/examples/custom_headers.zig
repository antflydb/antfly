//! Custom Headers Example
//!
//! Demonstrates working with HTTP headers.

const std = @import("std");
const httpx = @import("httpx");

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("=== Custom Headers Example ===\n\n", .{});

    var headers = httpx.Headers.init(allocator);
    defer headers.deinit();

    try headers.set(httpx.HeaderName.CONTENT_TYPE, "application/json");
    try headers.set(httpx.HeaderName.AUTHORIZATION, "Bearer token123");
    try headers.set(httpx.HeaderName.ACCEPT, "application/json");
    try headers.set(httpx.HeaderName.USER_AGENT, "httpx.zig/1.0");
    try headers.append("X-Custom-Header", "custom-value");
    try headers.append("X-Request-ID", "req-12345");

    std.debug.print("Headers ({d} total):\n", .{headers.count()});
    std.debug.print("-----------------\n", .{});
    for (headers.iterator()) |h| {
        std.debug.print("  {s}: {s}\n", .{ h.name, h.value });
    }

    std.debug.print("\nHeader lookups (case-insensitive):\n", .{});
    std.debug.print("  content-type: {s}\n", .{headers.get("content-type") orelse "not found"});
    std.debug.print("  AUTHORIZATION: {s}\n", .{headers.get("AUTHORIZATION") orelse "not found"});

    std.debug.print("\nContent-Length parsing:\n", .{});
    try headers.set(httpx.HeaderName.CONTENT_LENGTH, "1234");
    if (headers.getContentLength()) |len| {
        std.debug.print("  Parsed length: {d}\n", .{len});
    }

    std.debug.print("\nChunked encoding check:\n", .{});
    try headers.set(httpx.HeaderName.TRANSFER_ENCODING, "chunked");
    std.debug.print("  Is chunked: {}\n", .{headers.isChunked()});

    std.debug.print("\nKeep-alive detection:\n", .{});
    std.debug.print("  HTTP/1.1 keep-alive: {}\n", .{headers.isKeepAlive(.HTTP_1_1)});
    std.debug.print("  HTTP/1.0 keep-alive: {}\n", .{headers.isKeepAlive(.HTTP_1_0)});

    std.debug.print("\nCommon header name constants:\n", .{});
    std.debug.print("  CONTENT_TYPE: {s}\n", .{httpx.HeaderName.CONTENT_TYPE});
    std.debug.print("  AUTHORIZATION: {s}\n", .{httpx.HeaderName.AUTHORIZATION});
    std.debug.print("  CACHE_CONTROL: {s}\n", .{httpx.HeaderName.CACHE_CONTROL});
}
