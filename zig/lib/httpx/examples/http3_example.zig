//! HTTP/3 Protocol Example for httpx.zig
//!
//! This example demonstrates httpx.zig's custom HTTP/3 implementation:
//! - QPACK header compression (RFC 9204)
//! - QUIC transport framing (RFC 9000)
//! - QUIC packet structures
//! - Variable-length integer encoding
//!
//! Note: httpx.zig implements HTTP/3 and QUIC from scratch as Zig's standard
//! library does not provide QUIC or HTTP/3 support. This is a complete custom
//! implementation following RFC 9000, RFC 9114, and RFC 9204.

const std = @import("std");
const httpx = @import("httpx");

pub fn main() !void {
    const allocator = std.heap.smp_allocator;

    std.debug.print("\n=== httpx.zig HTTP/3 Example ===\n\n", .{});

    // Example 1: QPACK Header Compression
    try qpackExample(allocator);

    // Example 2: QUIC Packet Structure
    try quicPacketExample(allocator);

    // Example 3: QUIC Frames
    try quicFrameExample(allocator);

    // Example 4: Variable-Length Integers
    try varintExample();

    // Example 5: HTTP/3 Frame Types
    try http3FrameExample(allocator);

    std.debug.print("\n=== All HTTP/3 examples completed ===\n", .{});
}

/// Demonstrates QPACK header compression (HTTP/3's header compression).
fn qpackExample(allocator: std.mem.Allocator) !void {
    std.debug.print("--- QPACK Header Compression ---\n", .{});

    // Initialize QPACK context
    var ctx = httpx.QpackContext.init(allocator);
    defer ctx.deinit();

    // QPACK has a larger static table than HPACK (99 vs 61 entries)
    std.debug.print("QPACK static table size: {d} entries\n", .{httpx.qpack.StaticTable.entries.len});
    std.debug.print("HPACK static table size: {d} entries\n", .{httpx.hpack.StaticTable.entries.len});

    // Define headers to encode (typical HTTP/3 request headers)
    const headers = [_]httpx.qpack.HeaderEntry{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":path", .value = "/api/v3/resources" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":authority", .value = "api.example.com" },
        .{ .name = "accept", .value = "application/json" },
        .{ .name = "accept-encoding", .value = "gzip, deflate, br" },
    };

    // Encode headers using QPACK
    const encoded = try httpx.qpack.encodeHeaders(&ctx, &headers, allocator);
    defer allocator.free(encoded);

    std.debug.print("Original headers: {d} fields\n", .{headers.len});
    std.debug.print("Encoded size: {d} bytes\n", .{encoded.len});

    // Decode headers back
    var decode_ctx = httpx.QpackContext.init(allocator);
    defer decode_ctx.deinit();

    const decoded = try httpx.qpack.decodeHeaders(&decode_ctx, encoded, allocator);
    defer {
        for (decoded) |h| {
            allocator.free(h.name);
            allocator.free(h.value);
        }
        allocator.free(decoded);
    }

    std.debug.print("Decoded {d} headers:\n", .{decoded.len});
    for (decoded) |h| {
        std.debug.print("  {s}: {s}\n", .{ h.name, h.value });
    }

    // Static table lookups
    if (httpx.qpack.StaticTable.get(17)) |entry| {
        std.debug.print("Static table index 17: {s}={s}\n", .{ entry.name, entry.value });
    }
    if (httpx.qpack.StaticTable.findNameValue(":method", "POST")) |idx| {
        std.debug.print("Found :method=POST at index {d}\n", .{idx});
    }

    // Demonstrate encoder stream instructions
    var encoder_stream = std.ArrayListUnmanaged(u8).empty;
    defer encoder_stream.deinit(allocator);

    try httpx.qpack.encodeSetCapacity(4096, &encoder_stream, allocator);
    std.debug.print("Encoder stream: Set Dynamic Table Capacity (4096)\n", .{});

    try httpx.qpack.encodeInsertNameRef(true, 17, "custom-value", &encoder_stream, allocator);
    std.debug.print("Encoder stream: Insert With Name Reference\n", .{});

    std.debug.print("Encoder stream total size: {d} bytes\n\n", .{encoder_stream.items.len});
}

/// Demonstrates QUIC packet structure.
fn quicPacketExample(allocator: std.mem.Allocator) !void {
    _ = allocator;
    std.debug.print("--- QUIC Packet Structure ---\n", .{});

    // Create connection IDs
    const dcid = try httpx.quic.ConnectionId.init(&[_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08 });
    const scid = try httpx.quic.ConnectionId.init(&[_]u8{ 0x11, 0x12, 0x13, 0x14 });

    std.debug.print("Destination CID: {d} bytes\n", .{dcid.len});
    std.debug.print("Source CID: {d} bytes\n", .{scid.len});

    // Create a long header (used in Initial, Handshake, 0-RTT packets)
    const long_header = httpx.quic.LongHeader{
        .packet_type = .initial,
        .version = .v1,
        .dcid = dcid,
        .scid = scid,
    };

    var header_buf: [128]u8 = undefined;
    const header_len = try long_header.encode(&header_buf);

    std.debug.print("Long header encoded: {d} bytes\n", .{header_len});
    std.debug.print("  Packet type: Initial\n", .{});
    std.debug.print("  Version: QUIC v1 (0x00000001)\n", .{});

    // Decode it back
    const decoded = try httpx.quic.LongHeader.decode(header_buf[0..header_len]);
    std.debug.print("Decoded header:\n", .{});
    std.debug.print("  Packet type: {s}\n", .{@tagName(decoded.header.packet_type)});
    std.debug.print("  DCID length: {d}\n", .{decoded.header.dcid.len});
    std.debug.print("  SCID length: {d}\n", .{decoded.header.scid.len});

    // Short header (used after connection established)
    const short_header = httpx.quic.ShortHeader{
        .dcid = dcid,
        .spin_bit = 0,
        .key_phase = 0,
    };

    var short_buf: [32]u8 = undefined;
    const short_len = try short_header.encode(&short_buf);
    std.debug.print("\nShort header encoded: {d} bytes\n", .{short_len});
    std.debug.print("  (Used for 1-RTT packets after handshake)\n\n", .{});
}

/// Demonstrates QUIC frame construction.
fn quicFrameExample(_: std.mem.Allocator) !void {
    std.debug.print("--- QUIC Frames ---\n", .{});

    // STREAM frame (carries application data)
    const stream_frame = httpx.quic.StreamFrame{
        .stream_id = 4, // Client-initiated bidirectional stream
        .offset = 0,
        .data = "Hello, HTTP/3!",
        .fin = false,
    };

    var stream_buf: [128]u8 = undefined;
    const stream_len = try stream_frame.encode(&stream_buf);
    std.debug.print("STREAM frame: {d} bytes (stream_id={d})\n", .{ stream_len, stream_frame.stream_id });

    // Decode STREAM frame
    const decoded_stream = try httpx.quic.StreamFrame.decode(stream_buf[0..stream_len]);
    std.debug.print("  Decoded data: \"{s}\"\n", .{decoded_stream.frame.data});

    // CRYPTO frame (carries TLS handshake data)
    const crypto_frame = httpx.quic.CryptoFrame{
        .offset = 0,
        .data = &[_]u8{ 0x01, 0x00, 0x00, 0x05, 'h', 'e', 'l', 'l', 'o' },
    };

    var crypto_buf: [64]u8 = undefined;
    const crypto_len = try crypto_frame.encode(&crypto_buf);
    std.debug.print("CRYPTO frame: {d} bytes\n", .{crypto_len});

    // ACK frame
    const ack_frame = httpx.quic.AckFrame{
        .largest_acknowledged = 42,
        .ack_delay = 100,
        .first_ack_range = 10,
        .ack_ranges = &.{},
    };

    var ack_buf: [64]u8 = undefined;
    const ack_len = try ack_frame.encode(&ack_buf);
    std.debug.print("ACK frame: {d} bytes (largest_ack={d})\n", .{ ack_len, ack_frame.largest_acknowledged });

    // CONNECTION_CLOSE frame
    const close_frame = httpx.quic.ConnectionCloseFrame{
        .error_code = @intFromEnum(httpx.quic.TransportError.no_error),
        .frame_type = null,
        .reason_phrase = "graceful shutdown",
    };

    var close_buf: [64]u8 = undefined;
    const close_len = try close_frame.encode(false, &close_buf);
    std.debug.print("CONNECTION_CLOSE frame: {d} bytes\n", .{close_len});

    // Frame types
    std.debug.print("\nQUIC frame types:\n", .{});
    inline for (@typeInfo(httpx.quic.FrameType).@"enum".fields) |field| {
        std.debug.print("  0x{x:0>2}: {s}\n", .{ field.value, field.name });
    }

    std.debug.print("\n", .{});
}

/// Demonstrates QUIC variable-length integer encoding.
fn varintExample() !void {
    std.debug.print("--- QUIC Variable-Length Integers ---\n", .{});

    // QUIC uses variable-length integers that can be 1, 2, 4, or 8 bytes
    const test_values = [_]u64{ 0, 37, 15293, 494878333 };

    for (test_values) |value| {
        var buf: [8]u8 = undefined;
        const len = try httpx.quic.encodeVarInt(value, &buf);
        std.debug.print("Value {d}: {d} byte(s) encoded\n", .{ value, len });

        // Decode it back
        const decoded = try httpx.quic.decodeVarInt(&buf);
        std.debug.print("  Decoded: {d}\n", .{decoded.value});
    }

    // Show encoding ranges
    std.debug.print("\nQUIC varint encoding ranges:\n", .{});
    std.debug.print("  1 byte:  0 - 63\n", .{});
    std.debug.print("  2 bytes: 64 - 16383\n", .{});
    std.debug.print("  4 bytes: 16384 - 1073741823\n", .{});
    std.debug.print("  8 bytes: 1073741824 - 4611686018427387903\n\n", .{});
}

/// Demonstrates HTTP/3 frame types.
fn http3FrameExample(allocator: std.mem.Allocator) !void {
    _ = allocator;
    std.debug.print("--- HTTP/3 Frame Types ---\n", .{});

    // HTTP/3 uses QUIC streams, with special unidirectional streams
    std.debug.print("HTTP/3 unidirectional stream types:\n", .{});
    inline for (@typeInfo(httpx.quic.Http3StreamType).@"enum".fields) |field| {
        std.debug.print("  0x{x:0>2}: {s}\n", .{ field.value, field.name });
    }

    // HTTP/3 frame types (from http.zig)
    std.debug.print("\nHTTP/3 frame types:\n", .{});
    std.debug.print("  0x00: DATA\n", .{});
    std.debug.print("  0x01: HEADERS\n", .{});
    std.debug.print("  0x03: CANCEL_PUSH\n", .{});
    std.debug.print("  0x04: SETTINGS\n", .{});
    std.debug.print("  0x05: PUSH_PROMISE\n", .{});
    std.debug.print("  0x07: GOAWAY\n", .{});
    std.debug.print("  0x0d: MAX_PUSH_ID\n", .{});

    // HTTP/3 SETTINGS
    std.debug.print("\nHTTP/3 settings identifiers:\n", .{});
    std.debug.print("  0x01: QPACK_MAX_TABLE_CAPACITY\n", .{});
    std.debug.print("  0x06: MAX_FIELD_SECTION_SIZE\n", .{});
    std.debug.print("  0x07: QPACK_BLOCKED_STREAMS\n", .{});

    // Transport parameters
    std.debug.print("\nQUIC transport parameters:\n", .{});
    inline for (@typeInfo(httpx.quic.TransportParameter).@"enum".fields[0..10]) |field| {
        std.debug.print("  0x{x:0>2}: {s}\n", .{ field.value, field.name });
    }

    std.debug.print("\n", .{});
}
