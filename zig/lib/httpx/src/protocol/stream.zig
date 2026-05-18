//! HTTP/2 Stream Management for httpx.zig
//!
//! Implements RFC 7540 - Hypertext Transfer Protocol Version 2 (HTTP/2)
//!
//! Features:
//! - Stream state machine (idle, open, half-closed, closed)
//! - Stream prioritization and dependency handling
//! - Flow control (connection and stream level)
//! - Stream multiplexing support
//! - WINDOW_UPDATE frame handling
//! - RST_STREAM handling

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Io = std.Io;

const http = @import("http.zig");
const hpack = @import("hpack.zig");

/// Overflow-guarded window delta addition per RFC 7540 §6.9.1.
fn addWindowDelta(current: i32, delta: i32) error{FlowControlError}!i32 {
    const next = @as(i64, current) + delta;
    if (next > std.math.maxInt(i32) or next < std.math.minInt(i32)) return error.FlowControlError;
    return @intCast(next);
}

/// HTTP/2 Stream States as per RFC 7540 Section 5.1
pub const StreamState = enum {
    /// Stream has not been opened yet. Reserved stream IDs are in this state.
    idle,
    /// Reserved stream created by sending or receiving PUSH_PROMISE.
    reserved_local,
    /// Reserved stream created by peer's PUSH_PROMISE.
    reserved_remote,
    /// Stream is open for sending and receiving.
    open,
    /// Stream is half-closed (local): we cannot send, but can receive.
    half_closed_local,
    /// Stream is half-closed (remote): peer cannot send, but can receive.
    half_closed_remote,
    /// Stream is fully closed.
    closed,
};

/// Priority information for a stream.
pub const StreamPriority = struct {
    /// The stream this stream depends on (0 for root).
    dependency: u31 = 0,
    /// Relative weight (1-256).
    weight: u8 = 16,
    /// Exclusive dependency flag.
    exclusive: bool = false,
};

/// Represents an HTTP/2 stream.
pub const Stream = struct {
    id: u31,
    state: StreamState = .idle,
    priority: StreamPriority = .{},

    /// Local send window (how much we can send).
    send_window: i32 = 65535,
    /// Local receive window (how much peer can send to us).
    recv_window: i32 = 65535,

    /// Whether we've sent END_STREAM.
    end_stream_sent: bool = false,
    /// Whether we've received END_STREAM.
    end_stream_received: bool = false,

    /// Request headers (decoded). On client connections, this also holds
    /// response headers (the receive loop stores all decoded HEADERS here).
    request_headers: ?[]hpack.DecodedHeader = null,
    /// Trailing headers (RFC 7540 §8.1), decoded from a second HEADERS frame
    /// received after DATA. Stored separately to avoid overwriting request_headers.
    trailer_headers: ?[]hpack.DecodedHeader = null,

    // -- Per-stream mailbox for multiplexed I/O --
    // The receive loop writes here; request fibers read.

    /// Flags from the HEADERS frame (contains END_STREAM, etc).
    headers_flags: u8 = 0,
    /// Accumulated DATA frame payloads for this stream.
    data_buf: std.ArrayListUnmanaged(u8) = .empty,
    /// Error from RST_STREAM or connection error, set by the receive loop.
    stream_error: ?anyerror = null,
    /// True once the receive loop has delivered HEADERS for this stream.
    got_headers: bool = false,
    /// True once all frames for this stream have been received (END_STREAM or error).
    completed: bool = false,
    /// Optional semaphore posted by the receive loop when this stream completes.
    /// Owned by the requesting fiber (stack-allocated); the receive loop only calls post().
    completion_sem: ?*Io.Semaphore = null,
    /// Semaphore posted by the receive loop on every DATA frame and on HEADERS.
    /// Consumer fibers wait on this to read data incrementally.
    /// Owned by the consumer fiber; the receive loop only calls post().
    data_event: ?*Io.Event = null,
    /// Read cursor into data_buf for incremental consumption.
    read_offset: usize = 0,

    /// Expected total DATA bytes from content-length header (RFC 7540 §8.1.2.6).
    /// null = no content-length. Checked against total_data_received at END_STREAM.
    content_length: ?u64 = null,

    /// Total DATA bytes received on this stream (unaffected by compaction).
    /// Used for content-length validation instead of data_buf.items.len,
    /// which shrinks when compactDataBuf() discards consumed bytes.
    total_data_received: u64 = 0,

    /// Accumulated received DATA bytes not yet acknowledged via WINDOW_UPDATE.
    pending_window_update: u32 = 0,

    /// Compaction threshold: shift consumed bytes to the front when
    /// read_offset exceeds this to bound memory usage on long-lived streams.
    pub const compact_threshold: usize = 64 * 1024;

    const Self = @This();

    pub fn init(id: u31) Self {
        return .{ .id = id };
    }

    pub fn deinit(self: *Self, allocator: Allocator) void {
        self.data_buf.deinit(allocator);
        freeDecodedHeaders(allocator, self.request_headers);
        freeDecodedHeaders(allocator, self.trailer_headers);
    }

    /// Checks if sending data is allowed in current state.
    pub fn canSend(self: *const Self) bool {
        return switch (self.state) {
            .open, .half_closed_remote => true,
            else => false,
        };
    }

    /// Checks if receiving data is allowed in current state.
    pub fn canReceive(self: *const Self) bool {
        return switch (self.state) {
            .open, .half_closed_local => true,
            else => false,
        };
    }

    /// Transitions state after sending END_STREAM.
    pub fn sendEndStream(self: *Self) void {
        self.end_stream_sent = true;
        switch (self.state) {
            .open => self.state = .half_closed_local,
            .half_closed_remote => self.state = .closed,
            else => {},
        }
    }

    /// Transitions state after receiving END_STREAM.
    pub fn receiveEndStream(self: *Self) void {
        self.end_stream_received = true;
        switch (self.state) {
            .open => self.state = .half_closed_remote,
            .half_closed_local => self.state = .closed,
            else => {},
        }
    }

    /// Opens the stream (transitions from idle to open).
    pub fn open(self: *Self) !void {
        if (self.state != .idle) return error.InvalidStreamState;
        self.state = .open;
    }

    /// Closes the stream due to RST_STREAM or error.
    pub fn reset(self: *Self) void {
        self.state = .closed;
    }

    /// Discards consumed bytes (before read_offset) from data_buf.
    /// Shifts remaining bytes to front and resets read_offset to 0.
    pub fn compactDataBuf(self: *Self) void {
        if (self.read_offset == 0) return;
        const remaining = self.data_buf.items.len - self.read_offset;
        if (remaining > 0) {
            std.mem.copyForwards(u8, self.data_buf.items[0..remaining],
                self.data_buf.items[self.read_offset..]);
        }
        self.data_buf.items.len = remaining;
        self.read_offset = 0;
    }

    /// Updates the send window by delta (can be negative for data sent).
    pub fn updateSendWindow(self: *Self, delta: i32) !void {
        self.send_window = try addWindowDelta(self.send_window, delta);
    }

    /// Updates the receive window by delta.
    pub fn updateRecvWindow(self: *Self, delta: i32) !void {
        self.recv_window = try addWindowDelta(self.recv_window, delta);
    }
};

/// Frees a slice of HPACK decoded headers and their owned name/value strings.
pub fn freeDecodedHeaders(allocator: Allocator, headers: ?[]hpack.DecodedHeader) void {
    const hdrs = headers orelse return;
    for (hdrs) |h| h.deinit(allocator);
    allocator.free(hdrs);
}

/// Manages all streams for an HTTP/2 connection.
///
/// Streams are heap-allocated for pointer stability: `*Stream` pointers
/// remain valid across map mutations (put/remove) that may rehash the
/// backing table. This is critical for multiplexed connections where the
/// receive loop fiber may insert new peer-initiated streams while request
/// fibers hold `*Stream` pointers.
pub const StreamManager = struct {
    allocator: Allocator,
    streams: std.AutoHashMapUnmanaged(u31, *Stream) = .{},

    /// Next stream ID to use for client-initiated streams (odd numbers).
    next_client_stream_id: u31 = 1,
    /// Next stream ID to use for server-initiated streams (even numbers).
    next_server_stream_id: u31 = 2,

    /// Whether this is a client (initiates odd stream IDs) or server (even).
    is_client: bool = true,

    /// Connection-level send window.
    connection_send_window: i32 = 65535,
    /// Connection-level receive window.
    connection_recv_window: i32 = 65535,

    /// Maximum concurrent streams allowed (from SETTINGS).
    max_concurrent_streams: u32 = 100,

    /// Highest stream ID that has been removed. Used to distinguish late
    /// frames for closed streams from truly invalid stream IDs.
    max_closed_stream_id: u31 = 0,


    /// Separate HPACK contexts for encoder and decoder (RFC 7541 §2.2).
    /// Each endpoint maintains two independent dynamic tables: one for
    /// encoding outbound headers and one for decoding inbound headers.
    hpack_encode_ctx: hpack.HpackContext,
    hpack_decode_ctx: hpack.HpackContext,

    const Self = @This();

    pub fn init(allocator: Allocator, is_client: bool) Self {
        return .{
            .allocator = allocator,
            .is_client = is_client,
            .hpack_encode_ctx = hpack.HpackContext.init(allocator),
            .hpack_decode_ctx = hpack.HpackContext.init(allocator),
        };
    }

    pub fn deinit(self: *Self) void {
        var it = self.streams.iterator();
        while (it.next()) |entry| {
            entry.value_ptr.*.deinit(self.allocator);
            self.allocator.destroy(entry.value_ptr.*);
        }
        self.streams.deinit(self.allocator);
        self.hpack_encode_ctx.deinit();
        self.hpack_decode_ctx.deinit();
    }

    /// Creates a new stream with the next available ID.
    /// The stream is immediately transitioned to `.open` state since locally
    /// initiated streams are open by definition at creation (RFC 7540 §5.1:
    /// sending HEADERS transitions idle → open).
    /// Returns error.MaxConcurrentStreamsExceeded if the peer's
    /// max_concurrent_streams limit would be violated (RFC 7540 §6.5.2).
    pub fn createStream(self: *Self) !*Stream {
        if (self.activeStreamCount() >= self.max_concurrent_streams) {
            return error.MaxConcurrentStreamsExceeded;
        }
        const id = if (self.is_client) blk: {
            const id = self.next_client_stream_id;
            if (id > std.math.maxInt(u31) - 2) return error.StreamIdExhausted;
            self.next_client_stream_id += 2;
            break :blk id;
        } else blk: {
            const id = self.next_server_stream_id;
            if (id > std.math.maxInt(u31) - 2) return error.StreamIdExhausted;
            self.next_server_stream_id += 2;
            break :blk id;
        };

        const stream = try self.allocator.create(Stream);
        stream.* = Stream.init(id);
        stream.state = .open;
        errdefer self.allocator.destroy(stream);
        try self.streams.put(self.allocator, id, stream);
        return stream;
    }

    /// Gets an existing stream by ID.
    pub fn getStream(self: *Self, id: u31) ?*Stream {
        return self.streams.get(id);
    }

    /// Gets or creates a stream (for handling incoming frames).
    /// Validates that peer-initiated stream IDs are monotonically increasing
    /// per RFC 7540 §5.1.1.
    pub fn getOrCreateStream(self: *Self, id: u31) !*Stream {
        if (self.streams.get(id)) |stream| {
            return stream;
        }

        // Validate stream ID based on initiator
        const is_client_stream = (id % 2 == 1);
        if (self.is_client and is_client_stream) {
            return error.InvalidStreamId; // Server cannot create client streams
        }
        if (!self.is_client and !is_client_stream) {
            return error.InvalidStreamId; // Client cannot create server streams
        }

        // Track highest peer-initiated stream ID for monotonicity enforcement.
        // Use maxInt(u31) as sentinel when the ID space is exhausted so that
        // deliverToMailbox's `sid < last_seen` check rejects any subsequent
        // attempt to reuse or exceed the last valid stream ID (RFC 7540 §5.1.1).
        if (is_client_stream and !self.is_client) {
            // Server receiving client stream — advance tracker past this ID.
            if (id >= self.next_client_stream_id) {
                self.next_client_stream_id = if (id <= std.math.maxInt(u31) - 2) id + 2 else std.math.maxInt(u31);
            }
        } else if (!is_client_stream and self.is_client) {
            // Client receiving server stream — advance tracker past this ID.
            if (id >= self.next_server_stream_id) {
                self.next_server_stream_id = if (id <= std.math.maxInt(u31) - 2) id + 2 else std.math.maxInt(u31);
            }
        }

        const stream = try self.allocator.create(Stream);
        stream.* = Stream.init(id);
        errdefer self.allocator.destroy(stream);
        try self.streams.put(self.allocator, id, stream);
        return stream;
    }

    /// Removes a closed stream and updates the closed-stream watermark.
    pub fn removeStream(self: *Self, id: u31) void {
        if (self.streams.fetchRemove(id)) |kv| {
            if (id > self.max_closed_stream_id) self.max_closed_stream_id = id;
            kv.value.deinit(self.allocator);
            self.allocator.destroy(kv.value);
        }
    }

    /// Returns the number of currently active streams. Streams are active
    /// from creation until removal via removeStream(). O(1).
    pub fn activeStreamCount(self: *const Self) usize {
        return self.streams.count();
    }

    /// Updates connection-level send window.
    pub fn updateConnectionSendWindow(self: *Self, delta: i32) !void {
        self.connection_send_window = try addWindowDelta(self.connection_send_window, delta);
    }

    /// Updates connection-level receive window.
    pub fn updateConnectionRecvWindow(self: *Self, delta: i32) !void {
        self.connection_recv_window = try addWindowDelta(self.connection_recv_window, delta);
    }

    /// Applies initial window size change from SETTINGS to open/half-closed-remote
    /// streams per RFC 7540 §6.9.2. Skips streams in states where send window is
    /// irrelevant. Individual stream overflow is a stream error, not connection error.
    pub fn applyInitialWindowSizeChange(self: *Self, old_size: u32, new_size: u32) !void {
        if (new_size > std.math.maxInt(i32) or old_size > std.math.maxInt(i32)) return error.FlowControlError;
        const delta = @as(i32, @intCast(new_size)) - @as(i32, @intCast(old_size));
        var it = self.streams.iterator();
        while (it.next()) |entry| {
            const s = entry.value_ptr.*;
            // Only adjust streams where we can still send data.
            if (!s.canSend()) continue;
            s.updateSendWindow(delta) catch {
                // RFC 7540 §6.9.2: overflow on a single stream is a
                // stream error (FLOW_CONTROL_ERROR), not connection error.
                s.stream_error = error.FlowControlError;
                s.completed = true;
            };
        }
    }
};

/// Builds a HEADERS frame payload with optional priority.
pub fn buildHeadersFramePayload(
    stream_manager: *StreamManager,
    headers: []const hpack.HeaderEntry,
    priority: ?StreamPriority,
    allocator: Allocator,
) !struct { payload: []u8, flags: u8 } {
    var out = std.ArrayListUnmanaged(u8).empty;
    errdefer out.deinit(allocator);

    var flags: u8 = 0;

    // Optional priority block (5 bytes)
    if (priority) |p| {
        flags |= 0x20; // PRIORITY flag

        var dep: u32 = p.dependency;
        if (p.exclusive) dep |= 0x80000000;

        var dep_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &dep_buf, dep, .big);
        try out.appendSlice(allocator, &dep_buf);
        try out.append(allocator, p.weight -% 1); // Weight is 1-256, encoded as 0-255
    }

    // HPACK-encoded headers
    const encoded_headers = try hpack.encodeHeaders(&stream_manager.hpack_encode_ctx, headers, allocator);
    defer allocator.free(encoded_headers);
    try out.appendSlice(allocator, encoded_headers);

    // END_HEADERS flag (we don't use CONTINUATION for now)
    flags |= 0x04;

    return .{ .payload = try out.toOwnedSlice(allocator), .flags = flags };
}

/// Parses a HEADERS frame payload.
pub fn parseHeadersFramePayload(
    stream_manager: *StreamManager,
    payload: []const u8,
    flags: u8,
    allocator: Allocator,
) !struct { headers: []hpack.DecodedHeader, priority: ?StreamPriority } {
    return parseHeadersFramePayloadWithOptions(stream_manager, payload, flags, allocator, .{});
}

/// Parses a HEADERS frame payload with configurable HPACK decode options.
pub fn parseHeadersFramePayloadWithOptions(
    stream_manager: *StreamManager,
    payload: []const u8,
    flags: u8,
    allocator: Allocator,
    hpack_options: hpack.DecodeHeadersOptions,
) !struct { headers: []hpack.DecodedHeader, priority: ?StreamPriority } {
    var offset: usize = 0;
    var priority: ?StreamPriority = null;

    // Check for PADDED flag (0x08)
    var pad_length: usize = 0;
    if (flags & 0x08 != 0) {
        if (payload.len < 1) return error.InvalidFrame;
        pad_length = payload[0];
        offset += 1;
    }

    // Check for PRIORITY flag (0x20)
    if (flags & 0x20 != 0) {
        if (payload.len < offset + 5) return error.InvalidFrame;
        const dep_raw = std.mem.readInt(u32, payload[offset..][0..4], .big);
        priority = .{
            .exclusive = (dep_raw & 0x80000000) != 0,
            .dependency = @intCast(dep_raw & 0x7FFFFFFF),
            .weight = payload[offset + 4] +% 1,
        };
        offset += 5;
    }

    // Remaining is HPACK block (minus padding).
    // Bounds-check before subtraction to prevent usize underflow.
    if (pad_length > payload.len - offset) return error.InvalidFrame;
    const header_block_len = payload.len - offset - pad_length;

    const headers = try hpack.decodeHeadersWithOptions(
        &stream_manager.hpack_decode_ctx,
        payload[offset .. offset + header_block_len],
        allocator,
        hpack_options,
    );

    return .{ .headers = headers, .priority = priority };
}

/// Builds a DATA frame payload.
pub fn buildDataFramePayload(data: []const u8, allocator: Allocator) ![]u8 {
    return allocator.dupe(u8, data);
}

/// Builds a WINDOW_UPDATE frame payload.
pub fn buildWindowUpdatePayload(increment: u31) [4]u8 {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, increment, .big);
    return buf;
}

/// Parses a WINDOW_UPDATE frame payload.
pub fn parseWindowUpdatePayload(payload: []const u8) !u31 {
    if (payload.len != 4) return error.InvalidFrame;
    const raw = std.mem.readInt(u32, payload[0..4], .big);
    const increment = raw & 0x7FFFFFFF;
    if (increment == 0) return error.ProtocolError; // WINDOW_UPDATE with 0 is protocol error
    return @intCast(increment);
}

/// Builds an RST_STREAM frame payload.
pub fn buildRstStreamPayload(error_code: http.Http2ErrorCode) [4]u8 {
    var buf: [4]u8 = undefined;
    std.mem.writeInt(u32, &buf, @intFromEnum(error_code), .big);
    return buf;
}

/// Parses an RST_STREAM frame payload.
pub fn parseRstStreamPayload(payload: []const u8) !http.Http2ErrorCode {
    if (payload.len != 4) return error.InvalidFrame;
    return @enumFromInt(std.mem.readInt(u32, payload[0..4], .big));
}

/// Builds a PRIORITY frame payload.
pub fn buildPriorityPayload(priority: StreamPriority) [5]u8 {
    var buf: [5]u8 = undefined;
    var dep: u32 = priority.dependency;
    if (priority.exclusive) dep |= 0x80000000;
    std.mem.writeInt(u32, buf[0..4], dep, .big);
    buf[4] = priority.weight -% 1;
    return buf;
}

/// Parses a PRIORITY frame payload.
pub fn parsePriorityPayload(payload: []const u8) !StreamPriority {
    if (payload.len != 5) return error.InvalidFrame;
    const dep_raw = std.mem.readInt(u32, payload[0..4], .big);
    return .{
        .exclusive = (dep_raw & 0x80000000) != 0,
        .dependency = @intCast(dep_raw & 0x7FFFFFFF),
        .weight = payload[4] +% 1,
    };
}

/// Builds a GOAWAY frame payload.
pub fn buildGoawayPayload(last_stream_id: u31, error_code: http.Http2ErrorCode, debug_data: ?[]const u8, allocator: Allocator) ![]u8 {
    const code = @intFromEnum(error_code);
    const debug_len = if (debug_data) |d| d.len else 0;
    const payload = try allocator.alloc(u8, 8 + debug_len);
    errdefer allocator.free(payload);

    std.mem.writeInt(u32, payload[0..4], last_stream_id, .big);
    std.mem.writeInt(u32, payload[4..8], code, .big);

    if (debug_data) |d| {
        @memcpy(payload[8..], d);
    }

    return payload;
}

/// Parses a GOAWAY frame payload.
pub fn parseGoawayPayload(payload: []const u8, allocator: Allocator) !struct {
    last_stream_id: u31,
    error_code: http.Http2ErrorCode,
    debug_data: ?[]u8,
} {
    if (payload.len < 8) return error.InvalidFrame;

    const last_stream_id: u31 = @intCast(std.mem.readInt(u32, payload[0..4], .big) & 0x7FFFFFFF);
    const error_code: http.Http2ErrorCode = @enumFromInt(std.mem.readInt(u32, payload[4..8], .big));

    const debug_data = if (payload.len > 8)
        try allocator.dupe(u8, payload[8..])
    else
        null;

    return .{
        .last_stream_id = last_stream_id,
        .error_code = error_code,
        .debug_data = debug_data,
    };
}

/// Builds a PING frame payload.
pub fn buildPingPayload(opaque_data: [8]u8) [8]u8 {
    return opaque_data;
}

test "Stream state transitions" {
    const allocator = std.testing.allocator;
    var stream = Stream.init(1);
    defer stream.deinit(allocator);

    try stream.open();
    try std.testing.expectEqual(StreamState.open, stream.state);

    stream.sendEndStream();
    try std.testing.expectEqual(StreamState.half_closed_local, stream.state);

    stream.receiveEndStream();
    try std.testing.expectEqual(StreamState.closed, stream.state);
}

test "Stream manager create and get" {
    const allocator = std.testing.allocator;
    var manager = StreamManager.init(allocator, true);
    defer manager.deinit();

    const stream1 = try manager.createStream();
    try std.testing.expectEqual(@as(u31, 1), stream1.id);

    const stream2 = try manager.createStream();
    try std.testing.expectEqual(@as(u31, 3), stream2.id);

    const got = manager.getStream(1).?;
    try std.testing.expectEqual(@as(u31, 1), got.id);
}

test "Flow control window update" {
    const allocator = std.testing.allocator;
    var stream = Stream.init(1);
    defer stream.deinit(allocator);

    try std.testing.expectEqual(@as(i32, 65535), stream.send_window);

    try stream.updateSendWindow(-1000);
    try std.testing.expectEqual(@as(i32, 64535), stream.send_window);

    try stream.updateSendWindow(500);
    try std.testing.expectEqual(@as(i32, 65035), stream.send_window);
}

test "WINDOW_UPDATE payload" {
    const payload = buildWindowUpdatePayload(32768);
    const increment = try parseWindowUpdatePayload(&payload);
    try std.testing.expectEqual(@as(u31, 32768), increment);
}

test "RST_STREAM payload" {
    const payload = buildRstStreamPayload(.cancel);
    const error_code = try parseRstStreamPayload(&payload);
    try std.testing.expectEqual(http.Http2ErrorCode.cancel, error_code);
}

test "compactDataBuf shifts remaining data to front" {
    const allocator = std.testing.allocator;
    var s = Stream.init(1);
    defer s.deinit(allocator);

    // Append 128KB of data.
    const chunk = "A" ** 1024;
    for (0..128) |_| {
        try s.data_buf.appendSlice(allocator, chunk);
    }
    try std.testing.expectEqual(@as(usize, 128 * 1024), s.data_buf.items.len);

    // Simulate reading 96KB.
    s.read_offset = 96 * 1024;
    s.compactDataBuf();

    // Remaining 32KB should be at offset 0.
    try std.testing.expectEqual(@as(usize, 0), s.read_offset);
    try std.testing.expectEqual(@as(usize, 32 * 1024), s.data_buf.items.len);
    // All remaining bytes should be 'A'.
    for (s.data_buf.items) |b| {
        try std.testing.expectEqual(@as(u8, 'A'), b);
    }
}

test "compactDataBuf is no-op when read_offset is zero" {
    const allocator = std.testing.allocator;
    var s = Stream.init(1);
    defer s.deinit(allocator);

    try s.data_buf.appendSlice(allocator, "hello");
    s.compactDataBuf();
    try std.testing.expectEqual(@as(usize, 5), s.data_buf.items.len);
    try std.testing.expectEqual(@as(usize, 0), s.read_offset);
}

test "PRIORITY payload" {
    const priority = StreamPriority{
        .dependency = 5,
        .weight = 128,
        .exclusive = true,
    };
    const payload = buildPriorityPayload(priority);
    const parsed = try parsePriorityPayload(&payload);

    try std.testing.expectEqual(priority.dependency, parsed.dependency);
    try std.testing.expectEqual(priority.weight, parsed.weight);
    try std.testing.expectEqual(priority.exclusive, parsed.exclusive);
}

test "addWindowDelta rejects overflow" {
    try std.testing.expectError(error.FlowControlError, addWindowDelta(std.math.maxInt(i32), 1));
}

test "addWindowDelta rejects underflow" {
    try std.testing.expectError(error.FlowControlError, addWindowDelta(std.math.minInt(i32), -1));
}

test "addWindowDelta accepts valid delta" {
    const result = try addWindowDelta(1000, -500);
    try std.testing.expectEqual(@as(i32, 500), result);
}
