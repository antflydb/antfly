//! HTTP Protocol Implementation for httpx.zig
//!
//! Unified HTTP protocol support for multiple versions.
//!
//! This module focuses on protocol wire-format framing/types and helpers:
//!
//! - HTTP/1.x: chunked transfer encoding helpers.
//! - HTTP/2: frame header + SETTINGS payload helpers, and basic frame IO.
//! - HTTP/3: QUIC varint + HTTP/3 frame header helpers.
//!
//! Note: HTTP/1.1 request/response serialization lives in core/request.zig
//! and core/response.zig (Request.serialize / Response.serialize).

const std = @import("std");
const arrayListWriter = @import("../util/array_list_writer.zig").arrayListWriter;
const mem = std.mem;
const Allocator = mem.Allocator;
const builtin = @import("builtin");

const types = @import("../core/types.zig");
const headers_mod = @import("../core/headers.zig");
const Headers = headers_mod.Headers;
const HeaderName = headers_mod.HeaderName;
const containsToken = headers_mod.containsToken;
const Request = @import("../core/request.zig").Request;
const Response = @import("../core/response.zig").Response;
const Status = @import("../core/status.zig").Status;

/// HTTP protocol version negotiation result.
pub const NegotiatedProtocol = types.Version;

/// Standard Application-Layer Protocol Negotiation (ALPN) identifiers.
///
/// These strings obey the IANA registry for ALPN protocol IDs used in TLS handshakes:
/// - "http/1.1": HTTP/1.1
/// - "h2": HTTP/2 over TLS
/// - "h3": HTTP/3 over QUIC
pub const AlpnProtocol = struct {
    pub const HTTP_1_1 = "http/1.1";
    pub const HTTP_2 = "h2";
    pub const HTTP_3 = "h3";
};

/// HTTP/2 frame types as defined in RFC 7540.
pub const Http2FrameType = enum(u8) {
    data = 0x0,
    headers = 0x1,
    priority = 0x2,
    rst_stream = 0x3,
    settings = 0x4,
    push_promise = 0x5,
    ping = 0x6,
    goaway = 0x7,
    window_update = 0x8,
    continuation = 0x9,
    _, // RFC 7540 §4.1: unknown frame types MUST be ignored
};

/// Represents the 9-byte header standard for all HTTP/2 frames.
pub const Http2FrameHeader = struct {
    length: u24,
    frame_type: Http2FrameType,
    flags: u8,
    stream_id: u31,

    /// Encodes the frame header into wire format.
    pub fn serialize(self: Http2FrameHeader) [9]u8 {
        var buf: [9]u8 = undefined;
        std.mem.writeInt(u24, buf[0..3], self.length, .big);
        buf[3] = @intFromEnum(self.frame_type);
        buf[4] = self.flags;
        std.mem.writeInt(u32, buf[5..9], self.stream_id, .big);
        return buf;
    }

    /// Decodes a frame header from wire format.
    pub fn parse(data: [9]u8) Http2FrameHeader {
        return .{
            .length = std.mem.readInt(u24, data[0..3], .big),
            .frame_type = @enumFromInt(data[3]),
            .flags = data[4],
            .stream_id = @intCast(std.mem.readInt(u32, data[5..9], .big) & 0x7FFFFFFF),
        };
    }
};

/// The standard connection preface sent by the client to initiate HTTP/2.
pub const HTTP2_PREFACE = "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n";

/// Setting identifiers for HTTP/2 SETTINGS frames (RFC 7540 §6.5.2).
pub const Http2SettingId = enum(u16) {
    header_table_size = 0x1,
    enable_push = 0x2,
    max_concurrent_streams = 0x3,
    initial_window_size = 0x4,
    max_frame_size = 0x5,
    max_header_list_size = 0x6,
};

/// Standard error codes for HTTP/2 stream and connection termination.
pub const Http2ErrorCode = enum(u32) {
    no_error = 0x0,
    protocol_error = 0x1,
    internal_error = 0x2,
    flow_control_error = 0x3,
    settings_timeout = 0x4,
    stream_closed = 0x5,
    frame_size_error = 0x6,
    refused_stream = 0x7,
    cancel = 0x8,
    compression_error = 0x9,
    connect_error = 0xa,
    enhance_your_calm = 0xb,
    inadequate_security = 0xc,
    http_1_1_required = 0xd,
};

pub const Http2ConnectionSettings = types.Http2Settings;

pub fn encodeSettingsPayload(settings: Http2ConnectionSettings, allocator: Allocator, out: *std.ArrayListUnmanaged(u8)) !void {
    // Each setting is 6 bytes: 16-bit ID + 32-bit value.
    var buf: [6]u8 = undefined;

    const settings_list = [_]struct { id: Http2SettingId, val: u32 }{
        .{ .id = .header_table_size, .val = settings.header_table_size },
        .{ .id = .enable_push, .val = if (settings.enable_push) 1 else 0 },
        .{ .id = .max_concurrent_streams, .val = settings.max_concurrent_streams },
        .{ .id = .initial_window_size, .val = settings.initial_window_size },
        .{ .id = .max_frame_size, .val = settings.max_frame_size },
        .{ .id = .max_header_list_size, .val = settings.max_header_list_size },
    };
    for (settings_list) |s| {
        std.mem.writeInt(u16, buf[0..2], @intFromEnum(s.id), .big);
        std.mem.writeInt(u32, buf[2..6], s.val, .big);
        try out.appendSlice(allocator, &buf);
    }
}

/// Stack-buffer variant of encodeSettingsPayload.
/// buf must be at least 6 * 6 = 36 bytes.
/// Returns the slice of buf that was written.
pub fn encodeSettingsPayloadBuf(settings: Http2ConnectionSettings, buf: *[6 * 6]u8) []u8 {
    const settings_list = [_]struct { id: Http2SettingId, val: u32 }{
        .{ .id = .header_table_size, .val = settings.header_table_size },
        .{ .id = .enable_push, .val = if (settings.enable_push) 1 else 0 },
        .{ .id = .max_concurrent_streams, .val = settings.max_concurrent_streams },
        .{ .id = .initial_window_size, .val = settings.initial_window_size },
        .{ .id = .max_frame_size, .val = settings.max_frame_size },
        .{ .id = .max_header_list_size, .val = settings.max_header_list_size },
    };
    var offset: usize = 0;
    for (settings_list) |s| {
        std.mem.writeInt(u16, buf[offset..][0..2], @intFromEnum(s.id), .big);
        std.mem.writeInt(u32, buf[offset..][2..6], s.val, .big);
        offset += 6;
    }
    return buf[0..offset];
}

pub fn applySettingsPayload(settings: *Http2ConnectionSettings, payload: []const u8) !void {
    if (payload.len % 6 != 0) return error.InvalidSettingsPayload;

    var i: usize = 0;
    while (i < payload.len) : (i += 6) {
        const id = std.mem.readInt(u16, payload[i..][0..2], .big);
        const value = std.mem.readInt(u32, payload[i..][2..6], .big);

        const setting = std.enums.fromInt(Http2SettingId, id) orelse continue;
        switch (setting) {
            .header_table_size => settings.header_table_size = value,
            .enable_push => {
                if (value > 1) return error.ProtocolError;
                settings.enable_push = (value != 0);
            },
            .max_concurrent_streams => settings.max_concurrent_streams = value,
            .initial_window_size => {
                // RFC 7540 §6.5.2: MUST be [0, 2^31-1].
                if (value > 0x7FFFFFFF) return error.FlowControlError;
                settings.initial_window_size = value;
            },
            .max_frame_size => {
                // RFC 7540 §6.5.2: MUST be [16384, 16777215].
                if (value < 16384 or value > 16777215) return error.ProtocolError;
                settings.max_frame_size = value;
            },
            .max_header_list_size => settings.max_header_list_size = value,
        }
    }
}


/// HTTP/3 frame types as defined in RFC 9114.
pub const Http3FrameType = enum(u64) {
    data = 0x00,
    headers = 0x01,
    cancel_push = 0x03,
    settings = 0x04,
    push_promise = 0x05,
    goaway = 0x07,
    max_push_id = 0x0D,
};

/// Configuration parameters for HTTP/3 connections.
/// Standard error codes for HTTP/3 stream and connection errors.
pub const Http3ErrorCode = enum(u64) {
    no_error = 0x100,
    general_protocol_error = 0x101,
    internal_error = 0x102,
    stream_creation_error = 0x103,
    closed_critical_stream = 0x104,
    frame_unexpected = 0x105,
    frame_error = 0x106,
    excessive_load = 0x107,
    id_error = 0x108,
    settings_error = 0x109,
    missing_settings = 0x10a,
    request_rejected = 0x10b,
    request_cancelled = 0x10c,
    request_incomplete = 0x10d,
    message_error = 0x10e,
    connect_error = 0x10f,
    version_fallback = 0x110,
};

/// Encodes an integer into the Variable-Length Integer format specified in RFC 9000.
pub fn encodeVarInt(value: u64, dest: []u8) !usize {
    if (value < 64) {
        if (dest.len < 1) return error.BufferTooSmall;
        dest[0] = @as(u8, @intCast(value));
        return 1;
    } else if (value < 16384) {
        if (dest.len < 2) return error.BufferTooSmall;
        dest[0] = @as(u8, @intCast((value >> 8) | 0x40));
        dest[1] = @as(u8, @intCast(value & 0xFF));
        return 2;
    } else if (value < 1073741824) {
        if (dest.len < 4) return error.BufferTooSmall;
        dest[0] = @as(u8, @intCast((value >> 24) | 0x80));
        dest[1] = @as(u8, @intCast((value >> 16) & 0xFF));
        dest[2] = @as(u8, @intCast((value >> 8) & 0xFF));
        dest[3] = @as(u8, @intCast(value & 0xFF));
        return 4;
    } else if (value < 4611686018427387904) {
        if (dest.len < 8) return error.BufferTooSmall;
        dest[0] = @as(u8, @intCast((value >> 56) | 0xC0));
        dest[1] = @as(u8, @intCast((value >> 48) & 0xFF));
        dest[2] = @as(u8, @intCast((value >> 40) & 0xFF));
        dest[3] = @as(u8, @intCast((value >> 32) & 0xFF));
        dest[4] = @as(u8, @intCast((value >> 24) & 0xFF));
        dest[5] = @as(u8, @intCast((value >> 16) & 0xFF));
        dest[6] = @as(u8, @intCast((value >> 8) & 0xFF));
        dest[7] = @as(u8, @intCast(value & 0xFF));
        return 8;
    }
    return error.ValueTooLarge;
}

/// Decodes an integer from the Variable-Length Integer format.
pub fn decodeVarInt(data: []const u8) !struct { value: u64, len: usize } {
    if (data.len == 0) return error.UnexpectedEof;
    const first = data[0];
    const prefix = first >> 6;
    const len: usize = @as(usize, 1) << @as(u3, @intCast(prefix));

    if (data.len < len) return error.UnexpectedEof;

    var value: u64 = @as(u64, first & 0x3F);
    var i: usize = 1;
    while (i < len) : (i += 1) {
        value = (value << 8) | data[i];
    }
    return .{ .value = value, .len = len };
}

/// HTTP/3 frame header (type + length), encoded as two QUIC varints.
pub const Http3FrameHeader = struct {
    frame_type: u64,
    length: u64,

    pub fn encode(self: Http3FrameHeader, out: []u8) !usize {
        var offset: usize = 0;
        offset += try encodeVarInt(self.frame_type, out[offset..]);
        offset += try encodeVarInt(self.length, out[offset..]);
        return offset;
    }

    pub fn decode(data: []const u8) !struct { header: Http3FrameHeader, len: usize } {
        const t = try decodeVarInt(data);
        const l = try decodeVarInt(data[t.len..]);
        return .{
            .header = .{ .frame_type = t.value, .length = l.value },
            .len = t.len + l.len,
        };
    }
};

/// Writes payload using HTTP/1.1 chunked transfer format with optional trailers
/// directly to the given writer, avoiding heap materialization.
pub fn writeChunkedBody(writer: anytype, body: []const u8, trailers: ?*const Headers) !void {
    const chunk_size: usize = 4096;
    var offset: usize = 0;
    while (offset < body.len) {
        const len = @min(chunk_size, body.len - offset);
        try writer.print("{x}\r\n", .{len});
        try writer.writeAll(body[offset .. offset + len]);
        try writer.writeAll("\r\n");
        offset += len;
    }

    try writer.writeAll("0\r\n");

    if (trailers) |trailer_headers| {
        for (trailer_headers.iterator()) |h| {
            try writer.print("{s}: {s}\r\n", .{ h.name, h.value });
        }
    }

    try writer.writeAll("\r\n");
}

/// Encodes payload using HTTP/1.1 chunked transfer format with optional trailers.
/// Prefer `writeChunkedBody` for streaming directly to a socket writer.
pub fn encodeChunkedBody(body: []const u8, trailers: ?*const Headers, allocator: Allocator) ![]u8 {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);
    const writer = arrayListWriter(&out, allocator);
    try writeChunkedBody(writer, body, trailers);
    return out.toOwnedSlice(allocator);
}

/// Returns true if request headers represent an HTTP/1.1 h2c upgrade attempt.
pub fn isH2cUpgradeRequest(headers: *const Headers) bool {
    const upgrade = headers.get(HeaderName.UPGRADE) orelse return false;
    if (!std.ascii.eqlIgnoreCase(upgrade, "h2c")) return false;

    const connection = headers.get(HeaderName.CONNECTION) orelse return false;
    if (!containsToken(connection, "Upgrade")) return false;

    return headers.get(HeaderName.HTTP2_SETTINGS) != null;
}

/// Decodes the base64url-encoded HTTP2-Settings header value (RFC 7540 §3.2.1)
/// into a raw SETTINGS frame payload suitable for `applySettingsPayload`.
pub fn decodeH2cSettings(encoded: []const u8, allocator: Allocator) ![]u8 {
    const decoder = std.base64.url_safe_no_pad.Decoder;
    const out_len = decoder.calcSizeForSlice(encoded) catch return error.InvalidH2cSettings;
    const out = try allocator.alloc(u8, out_len);
    errdefer allocator.free(out);
    decoder.decode(out, encoded) catch return error.InvalidH2cSettings;
    return out;
}

/// Determines the highest supported HTTP version based on ALPN negotiation string.
pub fn negotiateVersion(alpn: ?[]const u8) types.Version {
    if (alpn) |protocol| {
        if (mem.eql(u8, protocol, AlpnProtocol.HTTP_3)) return .HTTP_3;
        if (mem.eql(u8, protocol, AlpnProtocol.HTTP_2)) return .HTTP_2;
        if (mem.eql(u8, protocol, AlpnProtocol.HTTP_1_1)) return .HTTP_1_1;
    }
    return .HTTP_1_1;
}

test "HTTP/2 frame header serialization" {
    const header = Http2FrameHeader{
        .length = 256,
        .frame_type = .data,
        .flags = 0x01,
        .stream_id = 1,
    };
    const serialized = header.serialize();
    const parsed = Http2FrameHeader.parse(serialized);

    try std.testing.expectEqual(header.length, parsed.length);
    try std.testing.expectEqual(header.frame_type, parsed.frame_type);
    try std.testing.expectEqual(header.stream_id, parsed.stream_id);
}

test "Protocol negotiation" {
    try std.testing.expectEqual(types.Version.HTTP_2, negotiateVersion("h2"));
    try std.testing.expectEqual(types.Version.HTTP_3, negotiateVersion("h3"));
    try std.testing.expectEqual(types.Version.HTTP_1_1, negotiateVersion("http/1.1"));
    try std.testing.expectEqual(types.Version.HTTP_1_1, negotiateVersion(null));
}

test "VarInt encoding" {
    var buf: [8]u8 = undefined;

    // 1 byte (0-63)
    var len = try encodeVarInt(25, &buf);
    try std.testing.expectEqual(@as(usize, 1), len);
    try std.testing.expectEqual(@as(u8, 25), buf[0]);
    var decoded = try decodeVarInt(buf[0..len]);
    try std.testing.expectEqual(@as(u64, 25), decoded.value);

    // 2 bytes (64-16383)
    len = try encodeVarInt(15293, &buf);
    try std.testing.expectEqual(@as(usize, 2), len);
    decoded = try decodeVarInt(buf[0..len]);
    try std.testing.expectEqual(@as(u64, 15293), decoded.value);

    // 4 bytes
    len = try encodeVarInt(494878333, &buf);
    try std.testing.expectEqual(@as(usize, 4), len);
    decoded = try decodeVarInt(buf[0..len]);
    try std.testing.expectEqual(@as(u64, 494878333), decoded.value);
}

test "HTTP/2 SETTINGS payload encode/decode" {
    const allocator = std.testing.allocator;

    const settings_in = Http2ConnectionSettings{
        .header_table_size = 4096,
        .enable_push = false,
        .max_concurrent_streams = 123,
        .initial_window_size = 65535,
        .max_frame_size = 16384,
        .max_header_list_size = 9000,
    };

    var payload = std.ArrayListUnmanaged(u8).empty;
    defer payload.deinit(allocator);
    try encodeSettingsPayload(settings_in, allocator, &payload);

    var settings_out = Http2ConnectionSettings{};
    try applySettingsPayload(&settings_out, payload.items);

    try std.testing.expectEqual(settings_in.header_table_size, settings_out.header_table_size);
    try std.testing.expectEqual(settings_in.enable_push, settings_out.enable_push);
    try std.testing.expectEqual(settings_in.max_concurrent_streams, settings_out.max_concurrent_streams);
    try std.testing.expectEqual(settings_in.initial_window_size, settings_out.initial_window_size);
    try std.testing.expectEqual(settings_in.max_frame_size, settings_out.max_frame_size);
    try std.testing.expectEqual(settings_in.max_header_list_size, settings_out.max_header_list_size);
}

test "HTTP/3 frame header encode/decode" {
    var buf: [32]u8 = undefined;
    const hdr = Http3FrameHeader{ .frame_type = 0x01, .length = 1234 };
    const n = try hdr.encode(&buf);
    const decoded = try Http3FrameHeader.decode(buf[0..n]);
    try std.testing.expectEqual(hdr.frame_type, decoded.header.frame_type);
    try std.testing.expectEqual(hdr.length, decoded.header.length);
}

test "encodeChunkedBody includes final chunk and trailers" {
    const allocator = std.testing.allocator;

    var trailers = Headers.init(allocator);
    defer trailers.deinit();
    try trailers.set("X-Checksum", "abc123");

    const chunked = try encodeChunkedBody("hello", &trailers, allocator);
    defer allocator.free(chunked);

    try std.testing.expect(mem.indexOf(u8, chunked, "5\r\nhello\r\n") != null);
    try std.testing.expect(mem.endsWith(u8, chunked, "0\r\nX-Checksum: abc123\r\n\r\n"));
}

test "isH2cUpgradeRequest detects valid h2c headers" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.set(HeaderName.UPGRADE, "h2c");
    try headers.set(HeaderName.CONNECTION, "Upgrade, HTTP2-Settings");
    try headers.set(HeaderName.HTTP2_SETTINGS, "AAMAAABkAAQCAAAAAAIAAAAA");

    try std.testing.expect(isH2cUpgradeRequest(&headers));
}

test "decodeH2cSettings decodes base64url SETTINGS payload" {
    const allocator = std.testing.allocator;
    // This is a valid HTTP2-Settings value encoding three settings:
    // MAX_CONCURRENT_STREAMS=100, INITIAL_WINDOW_SIZE=33554432, ENABLE_PUSH=0
    const encoded = "AAMAAABkAAQCAAAAAAIAAAAA";
    const payload = try decodeH2cSettings(encoded, allocator);
    defer allocator.free(payload);

    // Each setting is 6 bytes (2-byte ID + 4-byte value), so 3 settings = 18 bytes.
    try std.testing.expectEqual(@as(usize, 18), payload.len);

    // Verify we can apply it as a valid SETTINGS payload.
    var settings = Http2ConnectionSettings{};
    try applySettingsPayload(&settings, payload);
    try std.testing.expectEqual(@as(u32, 100), settings.max_concurrent_streams);
}

test "applySettingsPayload rejects invalid ENABLE_PUSH" {
    var settings = Http2ConnectionSettings{};
    // ENABLE_PUSH (id=2) with value=2 (must be 0 or 1).
    var payload: [6]u8 = undefined;
    std.mem.writeInt(u16, payload[0..2], @intFromEnum(Http2SettingId.enable_push), .big);
    std.mem.writeInt(u32, payload[2..6], 2, .big);
    try std.testing.expectError(error.ProtocolError, applySettingsPayload(&settings, &payload));
}

test "applySettingsPayload rejects invalid INITIAL_WINDOW_SIZE" {
    var settings = Http2ConnectionSettings{};
    // INITIAL_WINDOW_SIZE (id=4) with value=0x80000000 (exceeds 2^31-1).
    var payload: [6]u8 = undefined;
    std.mem.writeInt(u16, payload[0..2], @intFromEnum(Http2SettingId.initial_window_size), .big);
    std.mem.writeInt(u32, payload[2..6], 0x80000000, .big);
    try std.testing.expectError(error.FlowControlError, applySettingsPayload(&settings, &payload));
}

test "applySettingsPayload rejects invalid MAX_FRAME_SIZE" {
    var settings = Http2ConnectionSettings{};
    // MAX_FRAME_SIZE (id=5) with value=0 (below 16384 minimum).
    var payload: [6]u8 = undefined;
    std.mem.writeInt(u16, payload[0..2], @intFromEnum(Http2SettingId.max_frame_size), .big);
    std.mem.writeInt(u32, payload[2..6], 0, .big);
    try std.testing.expectError(error.ProtocolError, applySettingsPayload(&settings, &payload));

    // Also reject values above 16777215.
    std.mem.writeInt(u32, payload[2..6], 16777216, .big);
    try std.testing.expectError(error.ProtocolError, applySettingsPayload(&settings, &payload));
}
