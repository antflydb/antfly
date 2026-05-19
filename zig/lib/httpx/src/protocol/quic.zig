//! QUIC Transport Protocol for HTTP/3
//!
//! Implements core QUIC protocol functionality based on RFC 9000
//!
//! QUIC is the transport layer for HTTP/3, providing:
//! - Multiplexed streams over UDP
//! - Built-in TLS 1.3 encryption
//! - Connection migration
//! - Low-latency connection establishment (0-RTT)

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const posix = std.posix;

const http = @import("http.zig");

/// QUIC version identifiers
pub const Version = enum(u32) {
    /// QUIC version 1 (RFC 9000)
    v1 = 0x00000001,
    /// QUIC version 2 (RFC 9369)
    v2 = 0x6b3343cf,
    /// Version negotiation
    negotiation = 0x00000000,
};

/// QUIC long header packet types
pub const LongPacketType = enum(u2) {
    initial = 0,
    zero_rtt = 1,
    handshake = 2,
    retry = 3,
};

/// QUIC frame types
pub const FrameType = enum(u64) {
    padding = 0x00,
    ping = 0x01,
    ack = 0x02,
    ack_ecn = 0x03,
    reset_stream = 0x04,
    stop_sending = 0x05,
    crypto = 0x06,
    new_token = 0x07,
    stream = 0x08, // 0x08-0x0f
    max_data = 0x10,
    max_stream_data = 0x11,
    max_streams_bidi = 0x12,
    max_streams_uni = 0x13,
    data_blocked = 0x14,
    stream_data_blocked = 0x15,
    streams_blocked_bidi = 0x16,
    streams_blocked_uni = 0x17,
    new_connection_id = 0x18,
    retire_connection_id = 0x19,
    path_challenge = 0x1a,
    path_response = 0x1b,
    connection_close = 0x1c,
    connection_close_app = 0x1d,
    handshake_done = 0x1e,

    pub fn isStream(frame_type: u64) bool {
        return frame_type >= 0x08 and frame_type <= 0x0f;
    }
};

/// QUIC transport error codes
pub const TransportError = enum(u64) {
    no_error = 0x00,
    internal_error = 0x01,
    connection_refused = 0x02,
    flow_control_error = 0x03,
    stream_limit_error = 0x04,
    stream_state_error = 0x05,
    final_size_error = 0x06,
    frame_encoding_error = 0x07,
    transport_parameter_error = 0x08,
    connection_id_limit_error = 0x09,
    protocol_violation = 0x0a,
    invalid_token = 0x0b,
    application_error = 0x0c,
    crypto_buffer_exceeded = 0x0d,
    key_update_error = 0x0e,
    aead_limit_reached = 0x0f,
    no_viable_path = 0x10,
    // 0x100-0x1ff: crypto errors (TLS alerts + 0x100)
};

/// QUIC transport parameters
pub const TransportParameter = enum(u64) {
    original_destination_connection_id = 0x00,
    max_idle_timeout = 0x01,
    stateless_reset_token = 0x02,
    max_udp_payload_size = 0x03,
    initial_max_data = 0x04,
    initial_max_stream_data_bidi_local = 0x05,
    initial_max_stream_data_bidi_remote = 0x06,
    initial_max_stream_data_uni = 0x07,
    initial_max_streams_bidi = 0x08,
    initial_max_streams_uni = 0x09,
    ack_delay_exponent = 0x0a,
    max_ack_delay = 0x0b,
    disable_active_migration = 0x0c,
    preferred_address = 0x0d,
    active_connection_id_limit = 0x0e,
    initial_source_connection_id = 0x0f,
    retry_source_connection_id = 0x10,
};

/// Connection ID (variable length, 0-20 bytes)
pub const ConnectionId = struct {
    data: [20]u8 = undefined,
    len: u8 = 0,

    pub fn init(bytes: []const u8) !ConnectionId {
        if (bytes.len > 20) return error.ConnectionIdTooLong;
        var cid = ConnectionId{};
        @memcpy(cid.data[0..bytes.len], bytes);
        cid.len = @intCast(bytes.len);
        return cid;
    }

    pub fn slice(self: *const ConnectionId) []const u8 {
        return self.data[0..self.len];
    }

    pub fn random() ConnectionId {
        var cid = ConnectionId{ .len = 8 };
        std.crypto.random.bytes(cid.data[0..8]);
        return cid;
    }
};

/// QUIC packet header (long form)
pub const LongHeader = struct {
    /// Header form (1 = long, always for this struct)
    form: u1 = 1,
    /// Fixed bit (must be 1)
    fixed_bit: u1 = 1,
    /// Long packet type
    packet_type: LongPacketType = .initial,
    /// Type-specific bits
    type_specific: u4 = 0,
    /// Version
    version: Version = .v1,
    /// Destination connection ID
    dcid: ConnectionId = .{},
    /// Source connection ID
    scid: ConnectionId = .{},

    /// Encodes the header to wire format.
    pub fn encode(self: LongHeader, out: []u8) !usize {
        if (out.len < 7 + self.dcid.len + self.scid.len) return error.BufferTooSmall;

        var offset: usize = 0;

        // First byte: form(1) | fixed_bit(1) | type(2) | type_specific(4)
        out[offset] = (@as(u8, self.form) << 7) |
            (@as(u8, self.fixed_bit) << 6) |
            (@as(u8, @intFromEnum(self.packet_type)) << 4) |
            self.type_specific;
        offset += 1;

        // Version (4 bytes, big-endian)
        const ver = @intFromEnum(self.version);
        out[offset] = @intCast((ver >> 24) & 0xFF);
        out[offset + 1] = @intCast((ver >> 16) & 0xFF);
        out[offset + 2] = @intCast((ver >> 8) & 0xFF);
        out[offset + 3] = @intCast(ver & 0xFF);
        offset += 4;

        // DCID length + DCID
        out[offset] = self.dcid.len;
        offset += 1;
        @memcpy(out[offset .. offset + self.dcid.len], self.dcid.slice());
        offset += self.dcid.len;

        // SCID length + SCID
        out[offset] = self.scid.len;
        offset += 1;
        @memcpy(out[offset .. offset + self.scid.len], self.scid.slice());
        offset += self.scid.len;

        return offset;
    }

    /// Decodes a long header from wire format.
    pub fn decode(data: []const u8) !struct { header: LongHeader, len: usize } {
        if (data.len < 7) return error.UnexpectedEof;

        var header = LongHeader{};
        var offset: usize = 0;

        // First byte
        const first = data[offset];
        header.form = @intCast((first >> 7) & 1);
        header.fixed_bit = @intCast((first >> 6) & 1);
        header.packet_type = @enumFromInt((first >> 4) & 3);
        header.type_specific = @intCast(first & 0x0F);
        offset += 1;

        // Version
        const ver = (@as(u32, data[offset]) << 24) |
            (@as(u32, data[offset + 1]) << 16) |
            (@as(u32, data[offset + 2]) << 8) |
            data[offset + 3];
        header.version = @enumFromInt(ver);
        offset += 4;

        // DCID
        const dcid_len = data[offset];
        offset += 1;
        if (dcid_len > 20 or data.len < offset + dcid_len) return error.InvalidConnectionId;
        header.dcid = try ConnectionId.init(data[offset .. offset + dcid_len]);
        offset += dcid_len;

        // SCID
        if (data.len < offset + 1) return error.UnexpectedEof;
        const scid_len = data[offset];
        offset += 1;
        if (scid_len > 20 or data.len < offset + scid_len) return error.InvalidConnectionId;
        header.scid = try ConnectionId.init(data[offset .. offset + scid_len]);
        offset += scid_len;

        return .{ .header = header, .len = offset };
    }
};

/// QUIC packet header (short form, for 1-RTT packets)
pub const ShortHeader = struct {
    /// Header form (0 = short)
    form: u1 = 0,
    /// Fixed bit (must be 1)
    fixed_bit: u1 = 1,
    /// Spin bit (for latency measurement)
    spin_bit: u1 = 0,
    /// Reserved bits
    reserved: u2 = 0,
    /// Key phase
    key_phase: u1 = 0,
    /// Packet number length (encoded as 0-3, actual length 1-4)
    pn_len: u2 = 0,
    /// Destination connection ID
    dcid: ConnectionId,

    /// Encodes the header to wire format.
    pub fn encode(self: ShortHeader, out: []u8) !usize {
        if (out.len < 1 + self.dcid.len) return error.BufferTooSmall;

        var offset: usize = 0;

        // First byte
        out[offset] = (@as(u8, self.form) << 7) |
            (@as(u8, self.fixed_bit) << 6) |
            (@as(u8, self.spin_bit) << 5) |
            (@as(u8, self.reserved) << 3) |
            (@as(u8, self.key_phase) << 2) |
            self.pn_len;
        offset += 1;

        // DCID (length is known from connection context)
        @memcpy(out[offset .. offset + self.dcid.len], self.dcid.slice());
        offset += self.dcid.len;

        return offset;
    }

    /// Decodes a short header from wire format.
    pub fn decode(data: []const u8, dcid_len: u8) !struct { header: ShortHeader, len: usize } {
        if (data.len < 1 + dcid_len) return error.UnexpectedEof;

        var header = ShortHeader{};
        var offset: usize = 0;

        // First byte
        const first = data[offset];
        header.form = @intCast((first >> 7) & 1);
        header.fixed_bit = @intCast((first >> 6) & 1);
        header.spin_bit = @intCast((first >> 5) & 1);
        header.reserved = @intCast((first >> 3) & 3);
        header.key_phase = @intCast((first >> 2) & 1);
        header.pn_len = @intCast(first & 3);
        offset += 1;

        // DCID
        header.dcid = try ConnectionId.init(data[offset .. offset + dcid_len]);
        offset += dcid_len;

        return .{ .header = header, .len = offset };
    }
};

/// Variable-length integer encoding (same as HTTP/3)
pub const encodeVarInt = http.encodeVarInt;
pub const decodeVarInt = http.decodeVarInt;

/// STREAM frame structure
pub const StreamFrame = struct {
    stream_id: u64,
    offset: u64 = 0,
    length: ?u64 = null,
    fin: bool = false,
    data: []const u8,

    /// Encodes a STREAM frame.
    pub fn encode(self: StreamFrame, out: []u8) !usize {
        var offset: usize = 0;

        // Frame type: 0x08 + flags
        var frame_type: u8 = 0x08;
        if (self.offset > 0) frame_type |= 0x04; // OFF bit
        if (self.length != null) frame_type |= 0x02; // LEN bit
        if (self.fin) frame_type |= 0x01; // FIN bit

        out[offset] = frame_type;
        offset += 1;

        // Stream ID
        offset += try encodeVarInt(self.stream_id, out[offset..]);

        // Offset (if present)
        if (self.offset > 0) {
            offset += try encodeVarInt(self.offset, out[offset..]);
        }

        // Length (if present)
        if (self.length) |len| {
            offset += try encodeVarInt(len, out[offset..]);
        }

        // Data
        if (out.len < offset + self.data.len) return error.BufferTooSmall;
        @memcpy(out[offset .. offset + self.data.len], self.data);
        offset += self.data.len;

        return offset;
    }

    /// Decodes a STREAM frame.
    pub fn decode(data: []const u8) !struct { frame: StreamFrame, len: usize } {
        if (data.len < 2) return error.UnexpectedEof;

        var offset: usize = 0;
        const frame_type = data[offset];
        offset += 1;

        const has_offset = (frame_type & 0x04) != 0;
        const has_length = (frame_type & 0x02) != 0;
        const fin = (frame_type & 0x01) != 0;

        // Stream ID
        const sid = try decodeVarInt(data[offset..]);
        offset += sid.len;

        // Offset
        var stream_offset: u64 = 0;
        if (has_offset) {
            const off = try decodeVarInt(data[offset..]);
            stream_offset = off.value;
            offset += off.len;
        }

        // Length
        var length: ?u64 = null;
        var data_len: usize = undefined;
        if (has_length) {
            const len = try decodeVarInt(data[offset..]);
            length = len.value;
            data_len = @intCast(len.value);
            offset += len.len;
        } else {
            data_len = data.len - offset;
        }

        if (data.len < offset + data_len) return error.UnexpectedEof;

        return .{
            .frame = .{
                .stream_id = sid.value,
                .offset = stream_offset,
                .length = length,
                .fin = fin,
                .data = data[offset .. offset + data_len],
            },
            .len = offset + data_len,
        };
    }
};

/// CRYPTO frame structure (used for TLS handshake data)
pub const CryptoFrame = struct {
    offset: u64,
    data: []const u8,

    /// Encodes a CRYPTO frame.
    pub fn encode(self: CryptoFrame, out: []u8) !usize {
        var offset: usize = 0;

        // Frame type: 0x06
        out[offset] = 0x06;
        offset += 1;

        // Offset
        offset += try encodeVarInt(self.offset, out[offset..]);

        // Length
        offset += try encodeVarInt(self.data.len, out[offset..]);

        // Data
        if (out.len < offset + self.data.len) return error.BufferTooSmall;
        @memcpy(out[offset .. offset + self.data.len], self.data);
        offset += self.data.len;

        return offset;
    }

    /// Decodes a CRYPTO frame.
    pub fn decode(data: []const u8) !struct { frame: CryptoFrame, len: usize } {
        if (data.len < 3) return error.UnexpectedEof;

        var offset: usize = 0;

        // Frame type
        if (data[offset] != 0x06) return error.InvalidFrameType;
        offset += 1;

        // Offset
        const off = try decodeVarInt(data[offset..]);
        offset += off.len;

        // Length
        const len = try decodeVarInt(data[offset..]);
        offset += len.len;

        const data_len: usize = @intCast(len.value);
        if (data.len < offset + data_len) return error.UnexpectedEof;

        return .{
            .frame = .{
                .offset = off.value,
                .data = data[offset .. offset + data_len],
            },
            .len = offset + data_len,
        };
    }
};

/// ACK frame structure
pub const AckFrame = struct {
    largest_acknowledged: u64,
    ack_delay: u64,
    first_ack_range: u64,
    ack_ranges: []const AckRange = &.{},

    pub const AckRange = struct {
        gap: u64,
        length: u64,
    };

    /// Encodes an ACK frame.
    pub fn encode(self: AckFrame, out: []u8) !usize {
        var offset: usize = 0;

        // Frame type: 0x02
        out[offset] = 0x02;
        offset += 1;

        // Largest Acknowledged
        offset += try encodeVarInt(self.largest_acknowledged, out[offset..]);

        // ACK Delay
        offset += try encodeVarInt(self.ack_delay, out[offset..]);

        // ACK Range Count
        offset += try encodeVarInt(self.ack_ranges.len, out[offset..]);

        // First ACK Range
        offset += try encodeVarInt(self.first_ack_range, out[offset..]);

        // Additional ACK Ranges
        for (self.ack_ranges) |range| {
            offset += try encodeVarInt(range.gap, out[offset..]);
            offset += try encodeVarInt(range.length, out[offset..]);
        }

        return offset;
    }

    /// Decodes an ACK frame.
    pub fn decode(data: []const u8, allocator: Allocator) !struct { frame: AckFrame, len: usize } {
        if (data.len < 1) return error.UnexpectedEof;
        if (data[0] != 0x02 and data[0] != 0x03) return error.InvalidFrameType;

        var offset: usize = 1;

        const largest = try decodeVarInt(data[offset..]);
        offset += largest.len;

        const delay = try decodeVarInt(data[offset..]);
        offset += delay.len;

        const range_count = try decodeVarInt(data[offset..]);
        offset += range_count.len;

        const first_range = try decodeVarInt(data[offset..]);
        offset += first_range.len;

        const max_ack_ranges = 256;
        const ranges_len: usize = @intCast(range_count.value);
        if (ranges_len > max_ack_ranges) return error.AckRangeCountTooLarge;
        const ranges = try allocator.alloc(AckFrame.AckRange, ranges_len);
        errdefer allocator.free(ranges);

        for (ranges, 0..) |*range, i| {
            _ = i;
            const gap = try decodeVarInt(data[offset..]);
            offset += gap.len;
            const len = try decodeVarInt(data[offset..]);
            offset += len.len;
            range.* = .{ .gap = gap.value, .length = len.value };
        }

        return .{
            .frame = .{
                .largest_acknowledged = largest.value,
                .ack_delay = delay.value,
                .first_ack_range = first_range.value,
                .ack_ranges = ranges,
            },
            .len = offset,
        };
    }
};

/// CONNECTION_CLOSE frame structure
pub const ConnectionCloseFrame = struct {
    error_code: u64,
    frame_type: ?u64 = null, // Only for transport errors
    reason_phrase: []const u8 = &.{},

    /// Encodes a CONNECTION_CLOSE frame.
    pub fn encode(self: ConnectionCloseFrame, is_app: bool, out: []u8) !usize {
        var offset: usize = 0;

        // Frame type: 0x1c (transport) or 0x1d (application)
        out[offset] = if (is_app) 0x1d else 0x1c;
        offset += 1;

        // Error Code
        offset += try encodeVarInt(self.error_code, out[offset..]);

        // Frame Type (only for transport close)
        if (!is_app) {
            offset += try encodeVarInt(self.frame_type orelse 0, out[offset..]);
        }

        // Reason Phrase Length + Reason Phrase
        offset += try encodeVarInt(self.reason_phrase.len, out[offset..]);
        if (self.reason_phrase.len > 0) {
            if (out.len < offset + self.reason_phrase.len) return error.BufferTooSmall;
            @memcpy(out[offset .. offset + self.reason_phrase.len], self.reason_phrase);
            offset += self.reason_phrase.len;
        }

        return offset;
    }

    /// Decodes a CONNECTION_CLOSE frame.
    pub fn decode(data: []const u8) !struct { frame: ConnectionCloseFrame, is_app: bool, len: usize } {
        if (data.len < 2) return error.UnexpectedEof;

        const frame_type = data[0];
        if (frame_type != 0x1c and frame_type != 0x1d) return error.InvalidFrameType;
        const is_app = frame_type == 0x1d;

        var offset: usize = 1;

        const err_code = try decodeVarInt(data[offset..]);
        offset += err_code.len;

        var related_frame_type: ?u64 = null;
        if (!is_app) {
            const ft = try decodeVarInt(data[offset..]);
            offset += ft.len;
            related_frame_type = ft.value;
        }

        const reason_len = try decodeVarInt(data[offset..]);
        offset += reason_len.len;

        const phrase_len: usize = @intCast(reason_len.value);
        if (data.len < offset + phrase_len) return error.UnexpectedEof;

        return .{
            .frame = .{
                .error_code = err_code.value,
                .frame_type = related_frame_type,
                .reason_phrase = data[offset .. offset + phrase_len],
            },
            .is_app = is_app,
            .len = offset + phrase_len,
        };
    }
};

/// QUIC transport parameters for connection setup
pub const TransportParameters = struct {
    max_idle_timeout: u64 = 30000, // 30 seconds
    max_udp_payload_size: u64 = 65527,
    initial_max_data: u64 = 10 * 1024 * 1024, // 10 MB
    initial_max_stream_data_bidi_local: u64 = 1024 * 1024,
    initial_max_stream_data_bidi_remote: u64 = 1024 * 1024,
    initial_max_stream_data_uni: u64 = 1024 * 1024,
    initial_max_streams_bidi: u64 = 100,
    initial_max_streams_uni: u64 = 100,
    ack_delay_exponent: u64 = 3,
    max_ack_delay: u64 = 25,
    disable_active_migration: bool = false,
    active_connection_id_limit: u64 = 2,

    /// Encodes transport parameters.
    pub fn encode(self: TransportParameters, allocator: Allocator) ![]u8 {
        var out = std.ArrayListUnmanaged(u8).empty;
        errdefer out.deinit(allocator);

        inline for (@typeInfo(TransportParameters).@"struct".fields) |field| {
            const param_id = @intFromEnum(@field(TransportParameter, field.name));
            const value = @field(self, field.name);

            if (field.type == bool) {
                if (value) {
                    var buf: [16]u8 = undefined;
                    const id_len = try encodeVarInt(param_id, &buf);
                    try out.appendSlice(allocator, buf[0..id_len]);
                    try out.append(allocator, 0); // Zero-length value
                }
            } else {
                var buf: [32]u8 = undefined;
                const id_len = try encodeVarInt(param_id, &buf);
                try out.appendSlice(allocator, buf[0..id_len]);

                const val_len = try encodeVarInt(value, buf[16..]);
                const len_len = try encodeVarInt(val_len, buf[8..16]);
                try out.appendSlice(allocator, buf[8 .. 8 + len_len]);
                try out.appendSlice(allocator, buf[16 .. 16 + val_len]);
            }
        }

        return out.toOwnedSlice(allocator);
    }

    /// Decodes transport parameters.
    pub fn decode(data: []const u8) !TransportParameters {
        var params = TransportParameters{};
        var offset: usize = 0;

        while (offset < data.len) {
            const id_result = try decodeVarInt(data[offset..]);
            offset += id_result.len;

            const len_result = try decodeVarInt(data[offset..]);
            offset += len_result.len;

            const value_len: usize = @intCast(len_result.value);
            if (data.len < offset + value_len) return error.UnexpectedEof;
            const value = data[offset .. offset + value_len];
            offset += value_len;

            const param: TransportParameter = @enumFromInt(id_result.value);

            const parsed_value = if (value.len == 0)
                null
            else blk: {
                const v = try decodeVarInt(value);
                if (v.len != value.len) return error.InvalidTransportParameter;
                break :blk v.value;
            };

            switch (param) {
                .max_idle_timeout => params.max_idle_timeout = parsed_value orelse 0,
                .max_udp_payload_size => params.max_udp_payload_size = parsed_value orelse return error.InvalidTransportParameter,
                .initial_max_data => params.initial_max_data = parsed_value orelse return error.InvalidTransportParameter,
                .initial_max_stream_data_bidi_local => params.initial_max_stream_data_bidi_local = parsed_value orelse return error.InvalidTransportParameter,
                .initial_max_stream_data_bidi_remote => params.initial_max_stream_data_bidi_remote = parsed_value orelse return error.InvalidTransportParameter,
                .initial_max_stream_data_uni => params.initial_max_stream_data_uni = parsed_value orelse return error.InvalidTransportParameter,
                .initial_max_streams_bidi => params.initial_max_streams_bidi = parsed_value orelse return error.InvalidTransportParameter,
                .initial_max_streams_uni => params.initial_max_streams_uni = parsed_value orelse return error.InvalidTransportParameter,
                .ack_delay_exponent => params.ack_delay_exponent = parsed_value orelse return error.InvalidTransportParameter,
                .max_ack_delay => params.max_ack_delay = parsed_value orelse return error.InvalidTransportParameter,
                .disable_active_migration => {
                    if (value_len != 0) return error.InvalidTransportParameter;
                    params.disable_active_migration = true;
                },
                .active_connection_id_limit => params.active_connection_id_limit = parsed_value orelse return error.InvalidTransportParameter,
                else => {
                    // Preserve forward-compatibility by skipping unknown/unhandled parameters.
                },
            }
        }

        return params;
    }
};

/// Stream type indicators for QUIC streams
pub const StreamType = enum(u2) {
    /// Client-initiated bidirectional
    client_bidi = 0,
    /// Server-initiated bidirectional
    server_bidi = 1,
    /// Client-initiated unidirectional
    client_uni = 2,
    /// Server-initiated unidirectional
    server_uni = 3,

    pub fn fromId(stream_id: u64) StreamType {
        return @enumFromInt(stream_id & 0x03);
    }

    pub fn isBidirectional(self: StreamType) bool {
        return self == .client_bidi or self == .server_bidi;
    }

    pub fn isClientInitiated(self: StreamType) bool {
        return self == .client_bidi or self == .client_uni;
    }
};

/// HTTP/3 unidirectional stream types
pub const Http3StreamType = enum(u64) {
    /// Control stream
    control = 0x00,
    /// Push stream
    push = 0x01,
    /// QPACK encoder stream
    qpack_encoder = 0x02,
    /// QPACK decoder stream
    qpack_decoder = 0x03,
};

test "ConnectionId" {
    const cid = try ConnectionId.init(&[_]u8{ 0x01, 0x02, 0x03, 0x04 });
    try std.testing.expectEqual(@as(u8, 4), cid.len);
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x01, 0x02, 0x03, 0x04 }, cid.slice());
}

test "LongHeader encode/decode" {
    const header = LongHeader{
        .packet_type = .initial,
        .version = .v1,
        .dcid = try ConnectionId.init(&[_]u8{ 0x01, 0x02, 0x03, 0x04 }),
        .scid = try ConnectionId.init(&[_]u8{ 0x05, 0x06 }),
    };

    var buf: [64]u8 = undefined;
    const len = try header.encode(&buf);

    const decoded = try LongHeader.decode(buf[0..len]);
    try std.testing.expectEqual(header.packet_type, decoded.header.packet_type);
    try std.testing.expectEqual(header.version, decoded.header.version);
    try std.testing.expectEqualSlices(u8, header.dcid.slice(), decoded.header.dcid.slice());
    try std.testing.expectEqualSlices(u8, header.scid.slice(), decoded.header.scid.slice());
}

test "STREAM frame encode/decode" {
    const frame = StreamFrame{
        .stream_id = 4,
        .offset = 100,
        .fin = true,
        .data = "Hello, World!",
    };

    var buf: [64]u8 = undefined;
    const len = try frame.encode(&buf);

    const decoded = try StreamFrame.decode(buf[0..len]);
    try std.testing.expectEqual(frame.stream_id, decoded.frame.stream_id);
    try std.testing.expectEqual(frame.offset, decoded.frame.offset);
    try std.testing.expectEqual(frame.fin, decoded.frame.fin);
    try std.testing.expectEqualStrings(frame.data, decoded.frame.data);
}

test "CRYPTO frame encode/decode" {
    const frame = CryptoFrame{
        .offset = 0,
        .data = "TLS ClientHello",
    };

    var buf: [64]u8 = undefined;
    const len = try frame.encode(&buf);

    const decoded = try CryptoFrame.decode(buf[0..len]);
    try std.testing.expectEqual(frame.offset, decoded.frame.offset);
    try std.testing.expectEqualStrings(frame.data, decoded.frame.data);
}

test "StreamType" {
    try std.testing.expectEqual(StreamType.client_bidi, StreamType.fromId(0));
    try std.testing.expectEqual(StreamType.server_bidi, StreamType.fromId(1));
    try std.testing.expectEqual(StreamType.client_uni, StreamType.fromId(2));
    try std.testing.expectEqual(StreamType.server_uni, StreamType.fromId(3));
    try std.testing.expectEqual(StreamType.client_bidi, StreamType.fromId(4));
}

test "ACK frame encode/decode" {
    const allocator = std.testing.allocator;
    const ranges = [_]AckFrame.AckRange{.{ .gap = 1, .length = 2 }};
    const frame = AckFrame{
        .largest_acknowledged = 42,
        .ack_delay = 3,
        .first_ack_range = 4,
        .ack_ranges = &ranges,
    };

    var buf: [128]u8 = undefined;
    const len = try frame.encode(&buf);

    const decoded = try AckFrame.decode(buf[0..len], allocator);
    defer allocator.free(decoded.frame.ack_ranges);

    try std.testing.expectEqual(frame.largest_acknowledged, decoded.frame.largest_acknowledged);
    try std.testing.expectEqual(frame.ack_delay, decoded.frame.ack_delay);
    try std.testing.expectEqual(frame.first_ack_range, decoded.frame.first_ack_range);
    try std.testing.expectEqual(@as(usize, 1), decoded.frame.ack_ranges.len);
    try std.testing.expectEqual(frame.ack_ranges[0].gap, decoded.frame.ack_ranges[0].gap);
    try std.testing.expectEqual(frame.ack_ranges[0].length, decoded.frame.ack_ranges[0].length);
}

test "CONNECTION_CLOSE encode/decode" {
    const frame = ConnectionCloseFrame{
        .error_code = @intFromEnum(TransportError.protocol_violation),
        .frame_type = @intFromEnum(FrameType.stream),
        .reason_phrase = "bad stream state",
    };

    var buf: [128]u8 = undefined;
    const len = try frame.encode(false, &buf);

    const decoded = try ConnectionCloseFrame.decode(buf[0..len]);
    try std.testing.expect(!decoded.is_app);
    try std.testing.expectEqual(frame.error_code, decoded.frame.error_code);
    try std.testing.expectEqual(frame.frame_type.?, decoded.frame.frame_type.?);
    try std.testing.expectEqualStrings(frame.reason_phrase, decoded.frame.reason_phrase);
}

test "TransportParameters encode/decode" {
    const allocator = std.testing.allocator;
    const params = TransportParameters{
        .max_idle_timeout = 15000,
        .max_udp_payload_size = 1200,
        .initial_max_data = 1_000_000,
        .disable_active_migration = true,
        .active_connection_id_limit = 4,
    };

    const encoded = try params.encode(allocator);
    defer allocator.free(encoded);

    const decoded = try TransportParameters.decode(encoded);
    try std.testing.expectEqual(params.max_idle_timeout, decoded.max_idle_timeout);
    try std.testing.expectEqual(params.max_udp_payload_size, decoded.max_udp_payload_size);
    try std.testing.expectEqual(params.initial_max_data, decoded.initial_max_data);
    try std.testing.expectEqual(params.disable_active_migration, decoded.disable_active_migration);
    try std.testing.expectEqual(params.active_connection_id_limit, decoded.active_connection_id_limit);
}
