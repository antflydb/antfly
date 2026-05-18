//! HTTP/2 Protocol Example for httpx.zig
//!
//! This example demonstrates httpx.zig's custom HTTP/2 implementation:
//! - HPACK header compression (RFC 7541)
//! - HTTP/2 stream management and state machine
//! - HTTP/2 framing
//! - Flow control with WINDOW_UPDATE
//!
//! Note: httpx.zig implements HTTP/2 from scratch as Zig's standard library
//! does not provide HTTP/2 support. This is a complete custom implementation
//! following RFC 7540 and RFC 7541.

const std = @import("std");
const httpx = @import("httpx");

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("\n=== httpx.zig HTTP/2 Example ===\n\n", .{});

    // Example 1: HPACK Header Compression
    try hpackExample(allocator);

    // Example 2: HTTP/2 Stream Management
    try streamExample(allocator);

    // Example 3: HTTP/2 Framing
    try framingExample(allocator);

    // Example 4: Flow Control
    try flowControlExample(allocator);

    std.debug.print("\n=== All HTTP/2 examples completed ===\n", .{});
}

/// Demonstrates HPACK header compression.
fn hpackExample(allocator: std.mem.Allocator) !void {
    std.debug.print("--- HPACK Header Compression ---\n", .{});

    // Initialize HPACK context
    var ctx = httpx.HpackContext.init(allocator);
    defer ctx.deinit();

    // Define headers to encode (typical HTTP/2 request headers)
    const headers = [_]httpx.hpack.HeaderEntry{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":path", .value = "/api/users" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":authority", .value = "api.example.com" },
        .{ .name = "accept", .value = "application/json" },
        .{ .name = "user-agent", .value = "httpx.zig/1.0" },
    };

    // Encode headers using HPACK
    const encoded = try httpx.hpack.encodeHeaders(&ctx, &headers, allocator);
    defer allocator.free(encoded);

    std.debug.print("Original headers: {d} fields\n", .{headers.len});
    std.debug.print("Encoded size: {d} bytes\n", .{encoded.len});

    // Calculate compression ratio
    var original_size: usize = 0;
    for (headers) |h| {
        original_size += h.name.len + h.value.len + 4; // +4 for ": " and CRLF
    }
    const ratio = @as(f32, @floatFromInt(encoded.len)) / @as(f32, @floatFromInt(original_size)) * 100;
    std.debug.print("Compression ratio: {d:.1}%\n", .{ratio});

    // Decode headers back
    var decode_ctx = httpx.HpackContext.init(allocator);
    defer decode_ctx.deinit();

    const decoded = try httpx.hpack.decodeHeaders(&decode_ctx, encoded, allocator);
    defer {
        for (decoded) |h| h.deinit(allocator);
        allocator.free(decoded);
    }

    std.debug.print("Decoded {d} headers:\n", .{decoded.len});
    for (decoded) |h| {
        std.debug.print("  {s}: {s}\n", .{ h.name, h.value });
    }

    // Demonstrate integer encoding (used in HPACK)
    var buf: [10]u8 = undefined;
    const len = try httpx.hpack.encodeInteger(1337, 5, &buf);
    std.debug.print("Integer 1337 encoded in {d} bytes with 5-bit prefix\n", .{len});

    // Decode it back
    const result = try httpx.hpack.decodeInteger(buf[0..len], 5);
    std.debug.print("Decoded back to: {d}\n\n", .{result.value});
}

/// Demonstrates HTTP/2 stream state management.
fn streamExample(allocator: std.mem.Allocator) !void {
    std.debug.print("--- HTTP/2 Stream Management ---\n", .{});

    // Create a stream manager (client-side: uses odd stream IDs)
    var manager = httpx.StreamManager.init(allocator, true);
    defer manager.deinit();

    // Create streams for multiple concurrent requests
    const stream1 = try manager.createStream();
    std.debug.print("Created stream {d} (state: idle)\n", .{stream1.id});

    const stream2 = try manager.createStream();
    std.debug.print("Created stream {d} (state: idle)\n", .{stream2.id});

    const stream3 = try manager.createStream();
    std.debug.print("Created stream {d} (state: idle)\n", .{stream3.id});

    // Open streams (simulate sending HEADERS)
    try stream1.open();
    std.debug.print("Stream {d} opened (state: open)\n", .{stream1.id});

    // Demonstrate state transitions
    stream1.sendEndStream();
    std.debug.print("Stream {d} sent END_STREAM (state: half_closed_local)\n", .{stream1.id});

    stream1.receiveEndStream();
    std.debug.print("Stream {d} received END_STREAM (state: closed)\n", .{stream1.id});

    // Check active streams
    std.debug.print("Active streams: {d}\n", .{manager.activeStreamCount()});

    // Stream priority
    const priority = httpx.StreamPriority{
        .dependency = 0, // Root
        .weight = 32,
        .exclusive = false,
    };
    try stream2.open();
    stream2.priority = priority;
    std.debug.print("Stream {d} priority: weight={d}, dependency={d}\n", .{
        stream2.id,
        stream2.priority.weight,
        stream2.priority.dependency,
    });

    std.debug.print("\n", .{});
}

/// Demonstrates HTTP/2 frame construction.
fn framingExample(allocator: std.mem.Allocator) !void {
    std.debug.print("--- HTTP/2 Framing ---\n", .{});

    // Use a StreamManager for HPACK context
    var stream_manager = httpx.StreamManager.init(allocator, true);
    defer stream_manager.deinit();

    const headers_result = try httpx.stream.buildHeadersFramePayload(
        &stream_manager,
        &[_]httpx.hpack.HeaderEntry{
            .{ .name = ":method", .value = "POST" },
            .{ .name = ":path", .value = "/api/data" },
            .{ .name = "content-type", .value = "application/json" },
        },
        null, // No priority
        allocator,
    );
    defer allocator.free(headers_result.payload);

    std.debug.print("HEADERS frame payload: {d} bytes\n", .{headers_result.payload.len});
    std.debug.print("HEADERS flags: 0x{x}\n", .{headers_result.flags});

    // Build RST_STREAM frame
    const rst_payload = httpx.stream.buildRstStreamPayload(.no_error);
    std.debug.print("RST_STREAM frame payload: {d} bytes\n", .{rst_payload.len});

    // Build WINDOW_UPDATE frame
    const window_update = httpx.stream.buildWindowUpdatePayload(32768);
    std.debug.print("WINDOW_UPDATE frame payload: {d} bytes (increment: 32768)\n", .{window_update.len});

    // Build GOAWAY frame
    const goaway = try httpx.stream.buildGoawayPayload(0, .no_error, null, allocator);
    defer allocator.free(goaway);
    std.debug.print("GOAWAY frame payload: {d} bytes\n", .{goaway.len});

    // Build PING frame
    const ping = httpx.stream.buildPingPayload(.{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });
    std.debug.print("PING frame payload: {d} bytes\n", .{ping.len});

    // HTTP/2 frame header
    const frame_header = httpx.Http2FrameHeader{
        .length = @intCast(headers_result.payload.len),
        .frame_type = .headers,
        .flags = headers_result.flags,
        .stream_id = 1,
    };
    const serialized = frame_header.serialize();
    std.debug.print("Frame header encoded: {d} bytes (type=HEADERS, stream=1)\n", .{serialized.len});

    // HTTP/2 frame types
    std.debug.print("\nHTTP/2 frame types:\n", .{});
    inline for (@typeInfo(httpx.Http2FrameType).@"enum".fields) |field| {
        std.debug.print("  0x{x:0>2}: {s}\n", .{ field.value, field.name });
    }

    std.debug.print("\n", .{});
}

/// Demonstrates HTTP/2 flow control.
fn flowControlExample(allocator: std.mem.Allocator) !void {
    std.debug.print("--- HTTP/2 Flow Control ---\n", .{});

    var manager = httpx.StreamManager.init(allocator, true);
    defer manager.deinit();

    const stream = try manager.createStream();
    try stream.open();

    // Initial window sizes (RFC 7540 default: 65535)
    std.debug.print("Initial send window: {d}\n", .{stream.send_window});
    std.debug.print("Initial recv window: {d}\n", .{stream.recv_window});
    std.debug.print("Connection send window: {d}\n", .{manager.connection_send_window});
    std.debug.print("Connection recv window: {d}\n", .{manager.connection_recv_window});

    // Simulate sending data (decreases send window)
    const data_size: i32 = 16384;
    stream.send_window -= data_size;
    manager.connection_send_window -= data_size;
    std.debug.print("\nAfter sending {d} bytes:\n", .{data_size});
    std.debug.print("Stream send window: {d}\n", .{stream.send_window});
    std.debug.print("Connection send window: {d}\n", .{manager.connection_send_window});

    // Receive WINDOW_UPDATE (increases send window)
    const increment: i32 = 32768;
    stream.send_window += increment;
    manager.connection_send_window += increment;
    std.debug.print("\nAfter WINDOW_UPDATE ({d}):\n", .{increment});
    std.debug.print("Stream send window: {d}\n", .{stream.send_window});
    std.debug.print("Connection send window: {d}\n", .{manager.connection_send_window});

    // Parse a WINDOW_UPDATE payload
    const wu_payload = httpx.stream.buildWindowUpdatePayload(65535);
    const parsed_increment = try httpx.stream.parseWindowUpdatePayload(&wu_payload);
    std.debug.print("\nParsed WINDOW_UPDATE increment: {d}\n", .{parsed_increment});

    // HTTP/2 error codes
    std.debug.print("\nHTTP/2 error codes:\n", .{});
    inline for (@typeInfo(httpx.Http2ErrorCode).@"enum".fields) |field| {
        std.debug.print("  0x{x}: {s}\n", .{ field.value, field.name });
    }

    std.debug.print("\n", .{});
}
