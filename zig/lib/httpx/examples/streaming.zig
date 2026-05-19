//! Streaming Example
//!
//! Demonstrates streaming HTTP responses and chunked transfer encoding.

const std = @import("std");
const httpx = @import("httpx");

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("=== Streaming Example ===\n\n", .{});

    std.debug.print("Chunked Transfer Encoding:\n", .{});
    std.debug.print("--------------------------\n", .{});

    var headers = httpx.Headers.init(allocator);
    defer headers.deinit();

    try headers.set(httpx.HeaderName.TRANSFER_ENCODING, "chunked");
    std.debug.print("Transfer-Encoding: chunked\n", .{});
    std.debug.print("  Is chunked: {}\n", .{headers.isChunked()});

    std.debug.print("\nChunked Response Format:\n", .{});
    const chunks = [_][]const u8{
        "Hello, ",
        "this is ",
        "chunked ",
        "data!",
    };

    for (chunks) |chunk| {
        std.debug.print("  {x}\r\n", .{chunk.len});
        std.debug.print("  {s}\r\n", .{chunk});
    }
    std.debug.print("  0\r\n", .{});
    std.debug.print("  \r\n", .{});

    std.debug.print("\nBuffer utilities for streaming:\n", .{});

    var ring = try httpx.buffer.RingBuffer.init(allocator, 1024);
    defer ring.deinit();

    _ = try ring.writeBytes("Streaming data...");
    std.debug.print("  Ring buffer capacity: {d}\n", .{ring.getCapacity()});
    std.debug.print("  Bytes available: {d}\n", .{ring.getAvailable()});
    std.debug.print("  Free space: {d}\n", .{ring.getFreeSpace()});

    var fixed = httpx.buffer.FixedBuffer(256){};
    try fixed.append("Fixed buffer data");
    std.debug.print("\n  Fixed buffer length: {d}\n", .{fixed.len});
    std.debug.print("  Fixed buffer remaining: {d}\n", .{fixed.remaining()});

    std.debug.print("\nStreaming use cases:\n", .{});
    std.debug.print("  - Large file downloads\n", .{});
    std.debug.print("  - Server-Sent Events (SSE)\n", .{});
    std.debug.print("  - Real-time data feeds\n", .{});
    std.debug.print("  - Video/audio streaming\n", .{});
}
