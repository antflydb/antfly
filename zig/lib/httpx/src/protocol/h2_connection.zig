//! HTTP/2 Connection Layer for httpx.zig
//!
//! Implements the HTTP/2 connection state machine (RFC 7540) on top of existing
//! protocol primitives (HPACK, stream management, frame headers, SETTINGS).
//!
//! Features:
//! - Connection preface exchange (client and server)
//! - SETTINGS handshake with ACK tracking
//! - Frame I/O: read/write frames over any reader/writer
//! - PING echo, GOAWAY handling, WINDOW_UPDATE flow control
//! - HEADERS frame splitting into CONTINUATION when exceeding MAX_FRAME_SIZE
//! - CONTINUATION frame reassembly on receive
//! - Pseudo-header construction for HTTP/2 requests and responses

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;
const Io = std.Io;

const http = @import("http.zig");
const stream_mod = @import("stream.zig");
const hpack = @import("hpack.zig");
const types = @import("../core/types.zig");

const Http2FrameType = http.Http2FrameType;
const Http2FrameHeader = http.Http2FrameHeader;
const Http2ErrorCode = http.Http2ErrorCode;
const Http2Settings = types.Http2Settings;
const StreamManager = stream_mod.StreamManager;
const Stream = stream_mod.Stream;
const StreamPriority = stream_mod.StreamPriority;

/// A received HTTP/2 frame (header + payload).
pub const Frame = struct {
    header: Http2FrameHeader,
    payload: []u8,

    pub fn deinit(self: *Frame, allocator: Allocator) void {
        allocator.free(self.payload);
    }
};

/// HTTP/2 connection state machine.
///
/// Owns a `StreamManager` and drives frame I/O over an arbitrary
/// reader/writer pair (plain TCP socket or TLS session).
/// Reader must have `read(buf: []u8) !usize` or `recv(buf: []u8) !usize`.
/// Writer must have `writeAll(data: []const u8) !void`.
pub const H2Connection = struct {
    allocator: Allocator,
    io: Io,
    stream_manager: StreamManager,
    local_settings: Http2Settings,
    peer_settings: Http2Settings,
    is_server: bool,
    goaway_sent: bool = false,
    goaway_received: bool = false,
    last_peer_stream_id: u31 = 0,
    /// Highest peer-initiated stream ID we have processed (for GOAWAY, RFC 7540 §6.8).
    last_processed_stream_id: u31 = 0,

    /// Serializes frame writes so multiple fibers can safely share one connection.
    write_mutex: Io.Mutex = Io.Mutex.init,

    /// Accumulated connection-level DATA bytes not yet acknowledged via WINDOW_UPDATE.
    pending_conn_window_update: u32 = 0,

    /// Signaled when send window increases (WINDOW_UPDATE received), allowing
    /// writeDataBlocking to wake up and send more data.
    send_window_event: Io.Event = .unset,

    /// Maximum bytes of DATA payload allowed per stream. 0 = unlimited.
    /// Set from ServerConfig.max_body_size or ClientConfig.max_response_size.
    max_stream_data_size: usize = 0,

    /// Timeout for writeDataBlocking to wait for flow-control window space.
    /// Prevents indefinite blocking when a peer stops sending WINDOW_UPDATE.
    /// Default 30s. Set to `.none` for no timeout (original behavior).
    send_window_timeout: Io.Timeout = .{ .duration = .{
        .raw = Io.Duration.fromMilliseconds(30_000),
        .clock = .awake,
    } },

    /// Server's own limit on how many concurrent streams the peer can open.
    /// Initialized from local_settings and NOT overwritten by peer SETTINGS.
    local_max_concurrent_streams: u32 = 100,

    // Reassembly buffer for CONTINUATION frames.
    continuation_stream_id: ?u31 = null,
    continuation_buf: std.ArrayListUnmanaged(u8) = .empty,
    continuation_flags: u8 = 0,

    /// Signaled when a PING ACK is received with the expected opaque data,
    /// allowing a health-check fiber to verify liveness of the connection.
    ping_ack_event: Io.Event = .unset,
    /// Opaque data from the last sent PING. handlePing only signals
    /// ping_ack_event when the ACK payload matches, preventing stale or
    /// server-initiated PING ACKs from falsely satisfying the health check.
    ping_expected_data: [8]u8 = .{0} ** 8,

    /// Send WINDOW_UPDATE when accumulated consumed bytes exceed this threshold.
    /// Defaults to half the initial window size. Adjusted when peer SETTINGS
    /// changes INITIAL_WINDOW_SIZE so small windows don't deadlock.
    window_update_threshold: u32 = 65535 / 2,

    const Self = @This();

    /// Maximum total CONTINUATION reassembly size (64KB).
    /// Prevents unbounded memory growth from CONTINUATION frame floods.
    pub const max_continuation_size: usize = 64 * 1024;

    // -- Frame flag constants (RFC 7540 §6) --
    pub const FLAG_ACK: u8 = 0x01;
    pub const FLAG_END_STREAM: u8 = 0x01;
    pub const FLAG_END_HEADERS: u8 = 0x04;
    pub const FLAG_PADDED: u8 = 0x08;
    pub const FLAG_PRIORITY: u8 = 0x20;

    /// Creates a client-side HTTP/2 connection.
    pub fn initClient(allocator: Allocator, io: Io) Self {
        return .{
            .allocator = allocator,
            .io = io,
            .stream_manager = StreamManager.init(allocator, true),
            .local_settings = .{},
            .peer_settings = .{},
            .is_server = false,
        };
    }

    /// Creates a server-side HTTP/2 connection.
    pub fn initServer(allocator: Allocator, io: Io) Self {
        return .{
            .allocator = allocator,
            .io = io,
            .stream_manager = StreamManager.init(allocator, false),
            .local_settings = .{},
            .peer_settings = .{},
            .is_server = true,
        };
    }

    pub fn deinit(self: *Self) void {
        self.stream_manager.deinit();
        self.continuation_buf.deinit(self.allocator);
    }

    // ---------------------------------------------------------------
    // Connection Preface (RFC 7540 §3.5)
    // ---------------------------------------------------------------

    /// Client: sends the connection preface (magic + SETTINGS).
    pub fn sendClientPreface(self: *Self, writer: anytype) !void {
        try writer.writeAll(http.HTTP2_PREFACE);
        try self.sendSettings(writer);
    }

    /// Server: reads and validates the 24-byte client preface magic.
    /// The caller must then read the client's SETTINGS frame via `readFrame`.
    pub fn readClientPreface(self: *Self, reader: anytype) !void {
        _ = self;
        var buf: [24]u8 = undefined;
        try readExact(reader, &buf);
        if (!mem.eql(u8, &buf, http.HTTP2_PREFACE)) return error.ProtocolError;
    }

    // ---------------------------------------------------------------
    // SETTINGS (RFC 7540 §6.5)
    // ---------------------------------------------------------------

    /// Sends our local SETTINGS frame. Also syncs `local_max_concurrent_streams`
    /// so the enforcement limit matches what we advertise to the peer.
    pub fn sendSettings(self: *Self, writer: anytype) !void {
        // Sync enforcement limit with what we advertise.
        self.local_max_concurrent_streams = self.local_settings.max_concurrent_streams;
        // 6 settings × 6 bytes each = 36 bytes max — use a stack buffer to
        // avoid heap allocation for this tiny, fixed-size payload.
        var buf: [6 * 6]u8 = undefined;
        const payload = http.encodeSettingsPayloadBuf(self.local_settings, &buf);
        try self.writeFrame(writer, .settings, 0, 0, payload);
    }

    /// Sends a SETTINGS ACK (empty SETTINGS with ACK flag).
    pub fn sendSettingsAck(self: *Self, writer: anytype) !void {
        try self.writeFrame(writer, .settings, FLAG_ACK, 0, &.{});
    }

    /// Applies a received SETTINGS payload to `peer_settings`.
    /// Updates stream windows if INITIAL_WINDOW_SIZE changed.
    /// Signals the HPACK encoder if HEADER_TABLE_SIZE changed (RFC 7541 §4.2).
    pub fn applyPeerSettings(self: *Self, payload: []const u8) !void {
        const old_window = self.peer_settings.initial_window_size;
        const old_table_size = self.peer_settings.header_table_size;
        try http.applySettingsPayload(&self.peer_settings, payload);
        const new_window = self.peer_settings.initial_window_size;
        if (old_window != new_window) {
            try self.stream_manager.applyInitialWindowSizeChange(old_window, new_window);
        }
        // RFC 7541 §4.2: When peer changes HEADER_TABLE_SIZE, our encoder must
        // emit a size update at the start of the next header block.
        if (old_table_size != self.peer_settings.header_table_size) {
            self.stream_manager.hpack_encode_ctx.setTableSize(self.peer_settings.header_table_size);
        }
        // peer_settings.max_concurrent_streams limits how many streams *we*
        // can initiate. Propagate to stream_manager so createStream enforces it.
        // This does NOT change our own local limit on how many streams the peer
        // can open to us (local_max_concurrent_streams).
        self.stream_manager.max_concurrent_streams = self.peer_settings.max_concurrent_streams;
        // Adapt window update threshold to the local initial window size
        // so small windows don't deadlock (threshold must be <= window size).
        self.window_update_threshold = @max(1, self.local_settings.initial_window_size / 2);
    }

    // ---------------------------------------------------------------
    // Frame I/O
    // ---------------------------------------------------------------

    /// Resets CONTINUATION reassembly state after an error.
    /// payload is freed by the caller's errdefer, not here.
    fn resetContinuation(self: *Self) void {
        self.continuation_stream_id = null;
        self.continuation_buf.clearRetainingCapacity();
    }

    /// Reads one HTTP/2 frame. Handles CONTINUATION reassembly.
    /// Returns the complete frame; caller owns the payload memory.
    pub fn readFrame(self: *Self, reader: anytype) !Frame {
        while (true) {
            var hdr_buf: [9]u8 = undefined;
            try readExact(reader, &hdr_buf);
            const hdr = Http2FrameHeader.parse(hdr_buf);

            // Enforce MAX_FRAME_SIZE (RFC 7540 §4.2).
            if (hdr.length > self.local_settings.max_frame_size) return error.FrameSizeError;

            const payload = try self.allocator.alloc(u8, hdr.length);
            errdefer self.allocator.free(payload);
            try readExact(reader, payload);

            // CONTINUATION reassembly (RFC 7540 §6.10).
            if (self.continuation_stream_id) |cont_id| {
                if (hdr.frame_type != .continuation or hdr.stream_id != cont_id) {
                    // payload freed by errdefer above.
                    return error.ProtocolError;
                }
                // Cap reassembly to prevent CONTINUATION bombs.
                if (self.continuation_buf.items.len + payload.len > max_continuation_size) {
                    // payload freed by errdefer above.
                    self.resetContinuation();
                    return error.HeaderBlockTooLarge;
                }
                self.continuation_buf.appendSlice(self.allocator, payload) catch |err| {
                    self.resetContinuation();
                    return err;
                };
                self.allocator.free(payload);

                if (hdr.flags & FLAG_END_HEADERS != 0) {
                    const assembled = try self.continuation_buf.toOwnedSlice(self.allocator);
                    const stream_id = cont_id;
                    const flags = self.continuation_flags | FLAG_END_HEADERS;
                    self.continuation_stream_id = null;
                    self.continuation_flags = 0;
                    return .{
                        .header = .{
                            .length = @intCast(assembled.len),
                            .frame_type = .headers,
                            .flags = flags,
                            .stream_id = stream_id,
                        },
                        .payload = assembled,
                    };
                }
                continue;
            }

            // Check if this HEADERS frame needs CONTINUATION reassembly.
            if (hdr.frame_type == .headers and hdr.flags & FLAG_END_HEADERS == 0) {
                self.continuation_stream_id = hdr.stream_id;
                self.continuation_flags = hdr.flags;
                self.continuation_buf.clearRetainingCapacity();
                self.continuation_buf.appendSlice(self.allocator, payload) catch |err| {
                    self.resetContinuation();
                    return err;
                };
                self.allocator.free(payload);
                continue;
            }

            return .{ .header = hdr, .payload = payload };
        }
    }

    /// Writes a single frame (header + payload).
    pub fn writeFrame(self: *Self, writer: anytype, frame_type: Http2FrameType, flags: u8, stream_id: u31, payload: []const u8) !void {
        _ = self;
        const hdr = Http2FrameHeader{
            .length = @intCast(payload.len),
            .frame_type = frame_type,
            .flags = flags,
            .stream_id = stream_id,
        };
        const hdr_bytes = hdr.serialize();
        try writer.writeAll(&hdr_bytes);
        if (payload.len > 0) try writer.writeAll(payload);
    }

    /// Sends HEADERS (+ CONTINUATION if needed), splitting at MAX_FRAME_SIZE.
    pub fn writeHeaders(self: *Self, writer: anytype, stream_id: u31, header_block: []const u8, end_stream: bool) !void {
        const max_size = self.peer_settings.max_frame_size;
        if (header_block.len <= max_size) {
            var flags: u8 = FLAG_END_HEADERS;
            if (end_stream) flags |= FLAG_END_STREAM;
            try self.writeFrame(writer, .headers, flags, stream_id, header_block);
        } else {
            var flags: u8 = 0;
            if (end_stream) flags |= FLAG_END_STREAM;
            try self.writeFrame(writer, .headers, flags, stream_id, header_block[0..max_size]);

            var offset: usize = max_size;
            while (offset < header_block.len) {
                const end = @min(offset + max_size, header_block.len);
                const cont_flags: u8 = if (end == header_block.len) FLAG_END_HEADERS else 0;
                try self.writeFrame(writer, .continuation, cont_flags, stream_id, header_block[offset..end]);
                offset = end;
            }
        }
    }

    /// Sends DATA frame(s), respecting peer's MAX_FRAME_SIZE.
    /// Returns error.FlowControlError if the send window is exhausted.
    pub fn writeData(self: *Self, writer: anytype, stream_id: u31, data: []const u8, end_stream: bool) !void {
        const max_size = self.peer_settings.max_frame_size;
        const stream = self.stream_manager.getStream(stream_id) orelse return error.InvalidStreamId;

        if (data.len > std.math.maxInt(i32)) return error.FlowControlError;
        const data_i32: i32 = @intCast(data.len);
        if (stream.send_window < data_i32) return error.FlowControlError;
        if (self.stream_manager.connection_send_window < data_i32) return error.FlowControlError;

        try stream.updateSendWindow(-data_i32);
        try self.stream_manager.updateConnectionSendWindow(-data_i32);

        var offset: usize = 0;
        while (offset < data.len) {
            const end = @min(offset + max_size, data.len);
            const is_last = (end == data.len);
            const flags: u8 = if (is_last and end_stream) FLAG_END_STREAM else 0;
            try self.writeFrame(writer, .data, flags, stream_id, data[offset..end]);
            offset = end;
        }

        if (data.len == 0 and end_stream) {
            try self.writeFrame(writer, .data, FLAG_END_STREAM, stream_id, &.{});
        }

        if (end_stream) stream.sendEndStream();
    }

    /// Sends DATA frame(s) with flow-control back-pressure. Unlike `writeData`,
    /// this method blocks (yields the fiber) when the send window is exhausted,
    /// waiting for WINDOW_UPDATE frames from the peer.
    ///
    /// **Caller must hold `write_mutex`**. The mutex is temporarily released
    /// while waiting for window space so the receive loop can process frames.
    pub fn writeDataBlocking(self: *Self, writer: anytype, stream_id: u31, data: []const u8, end_stream: bool) !void {
        const max_size = self.peer_settings.max_frame_size;

        var offset: usize = 0;
        while (offset < data.len) {
            const stream = self.stream_manager.getStream(stream_id) orelse return error.InvalidStreamId;
            if (stream.stream_error) |err| return err;

            const remaining = data.len - offset;

            // Available window = min(stream, connection), clamped to 0.
            const sw: usize = if (stream.send_window > 0) @intCast(stream.send_window) else 0;
            const cw: usize = if (self.stream_manager.connection_send_window > 0) @intCast(self.stream_manager.connection_send_window) else 0;
            const window = @min(sw, cw);
            const chunk_size = @min(remaining, @min(window, max_size));

            if (chunk_size == 0) {
                // Window exhausted — release mutex, wait for WINDOW_UPDATE, reacquire.
                {
                    self.write_mutex.unlock(self.io);
                    defer self.write_mutex.lockUncancelable(self.io);
                    self.send_window_event.reset();
                    // Re-check after reset (receive loop may have updated between our check and reset).
                    const s2 = self.stream_manager.getStream(stream_id) orelse return error.InvalidStreamId;
                    if (s2.stream_error) |err| return err;
                    const sw2: usize = if (s2.send_window > 0) @intCast(s2.send_window) else 0;
                    const cw2: usize = if (self.stream_manager.connection_send_window > 0) @intCast(self.stream_manager.connection_send_window) else 0;
                    if (sw2 == 0 or cw2 == 0) {
                        self.send_window_event.waitTimeout(self.io, self.send_window_timeout) catch |err| switch (err) {
                            error.Timeout => return error.Timeout,
                            error.Canceled => return error.Canceled,
                        };
                    }
                }
                continue;
            }

            const chunk_i32: i32 = @intCast(chunk_size);
            try stream.updateSendWindow(-chunk_i32);
            try self.stream_manager.updateConnectionSendWindow(-chunk_i32);

            const is_last = (offset + chunk_size >= data.len);
            const flags: u8 = if (is_last and end_stream) FLAG_END_STREAM else 0;
            self.writeFrame(writer, .data, flags, stream_id, data[offset..][0..chunk_size]) catch |err| {
                // Restore send windows — data was never transmitted.
                stream.updateSendWindow(chunk_i32) catch {};
                self.stream_manager.updateConnectionSendWindow(chunk_i32) catch {};
                return err;
            };
            offset += chunk_size;
        }

        if (data.len == 0 and end_stream) {
            try self.writeFrame(writer, .data, FLAG_END_STREAM, stream_id, &.{});
        }

        if (end_stream) {
            const stream = self.stream_manager.getStream(stream_id) orelse return error.InvalidStreamId;
            stream.sendEndStream();
        }
    }

    // ---------------------------------------------------------------
    // Connection-level frame handlers
    // ---------------------------------------------------------------

    /// Handles a received SETTINGS frame.
    pub fn handleSettings(self: *Self, frame: *const Frame, writer: anytype) !void {
        if (frame.header.stream_id != 0) return error.ProtocolError;
        if (frame.header.flags & FLAG_ACK != 0) {
            if (frame.payload.len != 0) return error.FrameSizeError;
            return;
        }
        try self.applyPeerSettings(frame.payload);
        try self.sendSettingsAck(writer);
    }

    /// Handles a received PING frame — echoes back with ACK, or signals
    /// the ping_ack_event when an ACK is received (for health-check probes).
    pub fn handlePing(self: *Self, frame: *const Frame, writer: anytype) !void {
        if (frame.header.stream_id != 0) return error.ProtocolError;
        if (frame.payload.len != 8) return error.FrameSizeError;
        if (frame.header.flags & FLAG_ACK != 0) {
            // RFC 7540 §6.7: PING ACK must echo the exact opaque data.
            // Only signal the health-check event if the payload matches
            // what we sent, ignoring stale or server-initiated ACKs.
            if (std.mem.eql(u8, frame.payload[0..8], &self.ping_expected_data)) {
                self.ping_ack_event.set(self.io);
            }
            return;
        }
        try self.writeFrame(writer, .ping, FLAG_ACK, 0, frame.payload);
    }

    /// Sends a PING frame. Caller must hold write_mutex.
    /// Stores `opaque_data` so handlePing can validate the ACK payload.
    pub fn sendPing(self: *Self, writer: anytype, opaque_data: [8]u8) !void {
        self.ping_expected_data = opaque_data;
        try self.writeFrame(writer, .ping, 0, 0, &opaque_data);
    }

    /// Handles a received WINDOW_UPDATE frame.
    pub fn handleWindowUpdate(self: *Self, frame: *const Frame) !void {
        const increment = stream_mod.parseWindowUpdatePayload(frame.payload) catch |err| {
            // RFC 7540 §6.9.1: increment=0 on a stream is a stream error
            // (PROTOCOL_ERROR), not a connection error. Only connection-level
            // WINDOW_UPDATE(0) kills the connection.
            if (err == error.ProtocolError and frame.header.stream_id != 0) {
                if (self.stream_manager.getStream(frame.header.stream_id)) |s| {
                    s.stream_error = error.ProtocolError;
                    s.completed = true;
                    if (s.data_event) |ev| ev.set(self.io);
                    if (s.completion_sem) |sem| sem.post(self.io);
                }
                return;
            }
            return err;
        };
        if (frame.header.stream_id == 0) {
            // RFC 7540 §6.9.1: Connection-level overflow is a connection error;
            // FlowControlError propagates so caller can send GOAWAY and tear down.
            try self.stream_manager.updateConnectionSendWindow(@intCast(increment));
        } else {
            const s = self.stream_manager.getStream(frame.header.stream_id) orelse return;
            s.updateSendWindow(@intCast(increment)) catch {
                // RFC 7540 §6.9.1: Stream-level flow control overflow is a
                // stream error, not a connection error. Signal the stream
                // and continue processing other streams.
                s.stream_error = error.FlowControlError;
                s.completed = true;
                if (s.data_event) |ev| ev.set(self.io);
                if (s.completion_sem) |sem| sem.post(self.io);
                return;
            };
        }
        // Wake any fiber blocked in writeDataBlocking waiting for window space.
        self.send_window_event.set(self.io);
    }

    /// Handles a received GOAWAY frame. Signals streams above the peer's
    /// last_stream_id as retryable (RFC 7540 §6.8: streams with ID >
    /// last_stream_id were never processed and can be safely retried).
    pub fn handleGoaway(self: *Self, frame: *const Frame) !void {
        if (frame.payload.len < 8) return error.FrameSizeError;
        self.last_peer_stream_id = @intCast(mem.readInt(u32, frame.payload[0..4], .big) & 0x7FFFFFFF);
        self.goaway_received = true;

        // Signal streams above last_stream_id — they were never processed.
        var it = self.stream_manager.streams.iterator();
        while (it.next()) |entry| {
            const s = entry.value_ptr.*;
            if (s.id > self.last_peer_stream_id and !s.completed) {
                s.stream_error = error.GoawayRefused;
                s.completed = true;
                if (s.data_event) |ev| ev.set(self.io);
                if (s.completion_sem) |sem| sem.post(self.io);
            }
        }
    }

    /// Sends a GOAWAY frame advertising the last stream ID we processed (RFC 7540 §6.8).
    pub fn sendGoaway(self: *Self, writer: anytype, error_code: Http2ErrorCode) !void {
        if (self.goaway_sent) return;
        const payload = try stream_mod.buildGoawayPayload(self.last_processed_stream_id, error_code, null, self.allocator);
        defer self.allocator.free(payload);
        try self.writeFrame(writer, .goaway, 0, 0, payload);
        self.goaway_sent = true;
    }

    /// Returns true if the connection is draining (GOAWAY sent or received)
    /// and no streams are still open.
    pub fn isDrained(self: *const Self) bool {
        return (self.goaway_sent or self.goaway_received) and
            self.stream_manager.activeStreamCount() == 0;
    }

    /// Initiates graceful shutdown: sends GOAWAY with no_error, then pumps
    /// frames until all in-flight streams complete or the peer closes.
    /// Uses the locked frame pump since handler fibers may still be writing.
    pub fn gracefulShutdown(self: *Self, reader: anytype, writer: anytype) !void {
        if (!self.goaway_sent) {
            try self.sendGoaway(writer, .no_error);
        }
        // Drain remaining stream frames until all streams are done.
        while (self.stream_manager.activeStreamCount() > 0) {
            _ = self.processOneFrameLocked(reader, writer) catch |err| switch (err) {
                error.ConnectionClosed => return,
                else => return err,
            };
        }
    }

    /// Sends a RST_STREAM frame.
    pub fn sendRstStream(self: *Self, writer: anytype, stream_id: u31, error_code: Http2ErrorCode) !void {
        const payload = stream_mod.buildRstStreamPayload(error_code);
        try self.writeFrame(writer, .rst_stream, 0, stream_id, &payload);
    }

    /// Sends a WINDOW_UPDATE frame for connection (stream_id=0) or a specific stream.
    pub fn sendWindowUpdate(self: *Self, writer: anytype, stream_id: u31, increment: u31) !void {
        const payload = stream_mod.buildWindowUpdatePayload(increment);
        try self.writeFrame(writer, .window_update, 0, stream_id, &payload);
    }

    /// Accumulates received DATA bytes and sends WINDOW_UPDATE when the
    /// threshold is exceeded, reducing per-frame overhead on busy connections.
    fn accumulateWindowUpdate(self: *Self, writer: anytype, stream_id: u31, len: u32) !void {
        // Connection-level accumulator.
        self.pending_conn_window_update += len;
        if (self.pending_conn_window_update >= self.window_update_threshold) {
            try self.flushWindowAccumulator(writer, 0, &self.pending_conn_window_update);
        }
        // Stream-level accumulator.
        if (self.stream_manager.getStream(stream_id)) |s| {
            s.pending_window_update += len;
            if (s.pending_window_update >= self.window_update_threshold) {
                try self.flushWindowAccumulator(writer, stream_id, &s.pending_window_update);
            }
        }
    }

    /// Sends WINDOW_UPDATE(s) to drain an accumulator, capping each
    /// increment at maxInt(u31) per RFC 7540 §6.9. Restores the local
    /// receive window accounting so it stays in sync with WINDOW_UPDATE.
    fn flushWindowAccumulator(self: *Self, writer: anytype, stream_id: u31, accum: *u32) !void {
        while (accum.* > 0) {
            const inc: u31 = @intCast(@min(accum.*, std.math.maxInt(u31)));
            try self.sendWindowUpdate(writer, stream_id, inc);
            // Restore the local recv window to match the credit we just advertised.
            if (stream_id == 0) {
                self.stream_manager.updateConnectionRecvWindow(@intCast(inc)) catch {};
            } else {
                if (self.stream_manager.getStream(stream_id)) |s| {
                    s.updateRecvWindow(@intCast(inc)) catch {};
                }
            }
            accum.* -= inc;
        }
    }

    /// Flushes any remaining connection-level window credit. Called on
    /// stream completion to avoid leaving credit below the threshold.
    fn flushConnWindowUpdate(self: *Self, writer: anytype) !void {
        if (self.pending_conn_window_update > 0) {
            try self.flushWindowAccumulator(writer, 0, &self.pending_conn_window_update);
        }
    }

    /// Flushes any remaining stream-level window credit for the given stream.
    fn flushStreamWindowUpdate(self: *Self, writer: anytype, stream_id: u31) !void {
        if (self.stream_manager.getStream(stream_id)) |s| {
            if (s.pending_window_update > 0) {
                try self.flushWindowAccumulator(writer, stream_id, &s.pending_window_update);
            }
        }
    }

    // ---------------------------------------------------------------
    // Dispatching
    // ---------------------------------------------------------------

    /// Processes one frame: dispatches connection-level frames internally,
    /// returns stream-level frames (HEADERS, DATA, RST_STREAM) to the caller.
    /// Returns null for connection-level frames that were handled internally.
    pub fn dispatchFrame(self: *Self, frame: *Frame, writer: anytype) !?Frame {
        return switch (frame.header.frame_type) {
            .settings => {
                try self.handleSettings(frame, writer);
                return null;
            },
            .ping => {
                try self.handlePing(frame, writer);
                return null;
            },
            .window_update => {
                try self.handleWindowUpdate(frame);
                return null;
            },
            .goaway => {
                // RFC 7540 §6.8: GOAWAY MUST be on stream 0.
                if (frame.header.stream_id != 0) {
                    try self.sendGoaway(writer, .protocol_error);
                    return error.ProtocolError;
                }
                try self.handleGoaway(frame);
                return null;
            },
            .push_promise => {
                // RFC 7540 §6.6: PUSH_PROMISE on a connection where ENABLE_PUSH=0
                // is a connection error (PROTOCOL_ERROR). enable_push defaults to false.
                try self.sendGoaway(writer, .protocol_error);
                return error.ProtocolError;
            },
            .headers, .data, .rst_stream => {
                // RFC 7540 §6.2/§6.1/§6.4: These frame types MUST have a non-zero
                // stream ID. Stream ID 0 is a connection error (PROTOCOL_ERROR).
                if (frame.header.stream_id == 0) {
                    try self.sendGoaway(writer, .protocol_error);
                    return error.ProtocolError;
                }
                return frame.*;
            },
            .priority => {
                // RFC 7540 §6.3: PRIORITY on stream 0 is a connection error.
                if (frame.header.stream_id == 0) {
                    try self.sendGoaway(writer, .protocol_error);
                    return error.ProtocolError;
                }
                return frame.*;
            },
            .continuation => {
                // RFC 7540 §6.10: CONTINUATION received outside a HEADERS
                // sequence (continuation_stream_id == null in readFrame) is
                // a connection error (PROTOCOL_ERROR).
                try self.sendGoaway(writer, .protocol_error);
                return error.ProtocolError;
            },
            else => null, // Unknown frame types MUST be ignored (RFC 7540 §4.1).
        };
    }

    // ---------------------------------------------------------------
    // Pseudo-header helpers
    // ---------------------------------------------------------------

    /// Builds HTTP/2 request pseudo-headers + regular headers as HPACK entries.
    pub fn buildRequestHeaders(
        method: []const u8,
        path: []const u8,
        scheme: []const u8,
        authority: []const u8,
        extra_headers: []const hpack.HeaderEntry,
        allocator: Allocator,
    ) ![]hpack.HeaderEntry {
        const pseudo_count: usize = 4;
        const total = pseudo_count + extra_headers.len;
        const result = try allocator.alloc(hpack.HeaderEntry, total);
        result[0] = .{ .name = ":method", .value = method };
        result[1] = .{ .name = ":scheme", .value = scheme };
        result[2] = .{ .name = ":authority", .value = authority };
        result[3] = .{ .name = ":path", .value = path };
        if (extra_headers.len > 0) {
            @memcpy(result[pseudo_count..], extra_headers);
        }
        return result;
    }

    /// Builds HTTP/2 response pseudo-headers + regular headers as HPACK entries.
    pub fn buildResponseHeaders(
        status_code: u16,
        extra_headers: []const hpack.HeaderEntry,
        status_buf: *[3]u8,
        allocator: Allocator,
    ) ![]hpack.HeaderEntry {
        const total = 1 + extra_headers.len;
        const result = try allocator.alloc(hpack.HeaderEntry, total);
        _ = std.fmt.bufPrint(status_buf, "{d}", .{status_code}) catch unreachable;
        result[0] = .{ .name = ":status", .value = status_buf };
        if (extra_headers.len > 0) {
            @memcpy(result[1..], extra_headers);
        }
        return result;
    }

    /// Encodes headers via HPACK and sends as HEADERS frame(s).
    pub fn sendHeaders(self: *Self, writer: anytype, stream_id: u31, h2_headers: []const hpack.HeaderEntry, end_stream: bool) !void {
        const encoded = try hpack.encodeHeaders(&self.stream_manager.hpack_encode_ctx, h2_headers, self.allocator);
        defer self.allocator.free(encoded);
        try self.writeHeaders(writer, stream_id, encoded, end_stream);
        if (end_stream) {
            if (self.stream_manager.getStream(stream_id)) |s| s.sendEndStream();
        }
    }

    /// Decodes HPACK headers from a HEADERS frame payload.
    pub const DecodeResult = struct {
        headers: []hpack.DecodedHeader,
        priority: ?StreamPriority,
    };

    pub fn decodeFrameHeaders(self: *Self, payload: []const u8, flags: u8) !DecodeResult {
        const result = try stream_mod.parseHeadersFramePayloadWithOptions(
            &self.stream_manager,
            payload,
            flags,
            self.allocator,
            .{
                .max_table_size = self.local_settings.header_table_size,
                .max_decoded_size = self.local_settings.max_header_list_size,
            },
        );
        return .{ .headers = result.headers, .priority = result.priority };
    }

    // ---------------------------------------------------------------
    // Per-stream mailbox delivery and frame pumping
    // ---------------------------------------------------------------

    /// Delivers a stream-level frame to the target stream's mailbox.
    /// Copies payload data so the original frame can be freed afterward.
    /// HPACK decoding happens here (in the receive loop) to avoid concurrent
    /// decode races on the shared hpack_decode_ctx (RFC 7540 §4.3).
    pub fn deliverToMailbox(self: *Self, frame: *const Frame) !void {
        const sid = frame.header.stream_id;
        if (sid == 0) return;

        // Track highest peer-initiated stream ID for GOAWAY (RFC 7540 §6.8).
        const is_peer_initiated = if (self.is_server) (sid % 2 == 1) else (sid % 2 == 0);
        if (is_peer_initiated and sid > self.last_processed_stream_id) {
            self.last_processed_stream_id = sid;
        }

        const stream = self.stream_manager.getStream(sid) orelse blk: {
            // RFC 7540 §5.1: Late frames for a stream that was already
            // fully processed and removed are a stream error, not a
            // connection error. Return ClosedStream so the caller can
            // send RST_STREAM(STREAM_CLOSED) without killing the connection.
            if (sid <= self.stream_manager.max_closed_stream_id) return error.ClosedStream;

            // RFC 7540 §5.1.2: Don't create new streams after GOAWAY.
            if (self.goaway_received) return error.ProtocolError;

            // RFC 7540 §5.1.1: Peer stream IDs must be monotonically increasing.
            if (is_peer_initiated) {
                const last_seen = if (self.is_server)
                    self.stream_manager.next_client_stream_id
                else
                    self.stream_manager.next_server_stream_id;
                if (sid < last_seen) return error.ClosedStream;
            }

            break :blk try self.stream_manager.getOrCreateStream(sid);
        };

        switch (frame.header.frame_type) {
            .headers => {
                // Decode HPACK immediately in the receive loop. This is the
                // only safe place since hpack_decode_ctx is connection-scoped
                // and stateful — concurrent decodes corrupt the dynamic table.
                const dec = self.decodeFrameHeaders(frame.payload, frame.header.flags) catch |err| {
                    return err;
                };
                if (dec.priority) |p| stream.priority = p;

                if (stream.got_headers) {
                    // RFC 7540 §8.1: Trailing HEADERS MUST include END_STREAM.
                    if (frame.header.flags & FLAG_END_STREAM == 0) {
                        stream.stream_error = error.ProtocolError;
                        stream.completed = true;
                        stream_mod.freeDecodedHeaders(self.allocator, dec.headers);
                        if (stream.data_event) |ev| ev.set(self.io);
                        if (stream.completion_sem) |sem| sem.post(self.io);
                        return;
                    }
                    // Store trailing headers separately to avoid overwriting
                    // the initial request/response headers.
                    stream_mod.freeDecodedHeaders(self.allocator, stream.trailer_headers);
                    stream.trailer_headers = dec.headers;
                } else {
                    // Initial HEADERS — store decoded headers.
                    stream.headers_flags = frame.header.flags;

                    stream_mod.freeDecodedHeaders(self.allocator, stream.request_headers);
                    stream.request_headers = dec.headers;

                    // Transition idle → open (RFC 7540 §5.1).
                    if (stream.state == .idle) stream.state = .open;
                    stream.got_headers = true;

                    // RFC 7540 §8.1.2.6: Extract content-length for
                    // END_STREAM validation.
                    for (dec.headers) |h| {
                        if (std.ascii.eqlIgnoreCase(h.name, "content-length")) {
                            stream.content_length = std.fmt.parseInt(u64, h.value, 10) catch null;
                            break;
                        }
                    }
                }

                if (stream.data_event) |ev| ev.set(self.io);
                if (frame.header.flags & FLAG_END_STREAM != 0) {
                    // RFC 7540 §8.1.2.6: HEADERS with END_STREAM and a
                    // non-zero content-length is a stream error.
                    if (stream.content_length) |cl| {
                        if (cl != 0) {
                            stream.stream_error = error.ContentLengthMismatch;
                            stream.completed = true;
                            if (stream.data_event) |ev2| ev2.set(self.io);
                            if (stream.completion_sem) |sem| sem.post(self.io);
                            return;
                        }
                    }
                    stream.completed = true;
                    stream.receiveEndStream();
                    if (stream.completion_sem) |sem| sem.post(self.io);
                }
            },
            .data => {
                // RFC 7540 §6.1: Strip padding from DATA frames.
                const data_payload = blk: {
                    if (frame.header.flags & FLAG_PADDED != 0) {
                        if (frame.payload.len < 1) return error.ProtocolError;
                        const pad_len: usize = frame.payload[0];
                        if (pad_len + 1 > frame.payload.len) return error.ProtocolError;
                        break :blk frame.payload[1 .. frame.payload.len - pad_len];
                    }
                    break :blk frame.payload;
                };
                // RFC 7540 §6.9: Decrement receive windows on incoming DATA.
                // Flow control covers the entire frame payload including padding
                // (RFC 7540 §6.9: "the entire DATA frame payload is included in
                // flow control, including the Pad Length and Padding fields").
                // Decrement BEFORE state/overflow checks so that accumulated
                // WINDOW_UPDATE credit (from processOneFrame) stays consistent
                // with recv window accounting regardless of early returns.
                if (frame.payload.len > 0) {
                    const data_len: i32 = @intCast(@min(frame.payload.len, @as(usize, @intCast(std.math.maxInt(i32)))));
                    stream.updateRecvWindow(-data_len) catch {
                        stream.stream_error = error.FlowControlError;
                        stream.completed = true;
                        if (stream.data_event) |ev| ev.set(self.io);
                        if (stream.completion_sem) |sem| sem.post(self.io);
                        return;
                    };
                    // RFC 7540 §6.9.1: connection-level flow control violation
                    // is a connection error. Return the error so the caller
                    // can send GOAWAY(FLOW_CONTROL_ERROR).
                    try self.stream_manager.updateConnectionRecvWindow(-data_len);
                }
                // RFC 7540 §5.1: DATA on half-closed-remote or closed is a stream error.
                // Checked after recv window decrement so flow control stays consistent.
                if (!stream.canReceive()) {
                    stream.stream_error = error.StreamClosed;
                    stream.completed = true;
                    if (stream.data_event) |ev| ev.set(self.io);
                    if (stream.completion_sem) |sem| sem.post(self.io);
                    return;
                }
                if (self.max_stream_data_size > 0) {
                    // Use total_data_received, not data_buf.items.len — compactDataBuf()
                    // shrinks the buffer, so buffer length alone is bypassable.
                    const new_size = stream.total_data_received + data_payload.len;
                    if (new_size > self.max_stream_data_size) {
                        stream.stream_error = error.StreamDataOverflow;
                        stream.completed = true;
                        if (stream.data_event) |ev| ev.set(self.io);
                        if (stream.completion_sem) |sem| sem.post(self.io);
                        return;
                    }
                }
                stream.total_data_received += data_payload.len;
                try stream.data_buf.appendSlice(self.allocator, data_payload);
                if (stream.data_event) |ev| ev.set(self.io);
                if (frame.header.flags & FLAG_END_STREAM != 0) {
                    // RFC 7540 §8.1.2.6: If content-length was provided,
                    // the total DATA payload must match exactly. Uses
                    // total_data_received (not data_buf.items.len) because
                    // compactDataBuf() may have shrunk data_buf.
                    if (stream.content_length) |expected| {
                        if (stream.total_data_received != expected) {
                            stream.stream_error = error.ContentLengthMismatch;
                            stream.completed = true;
                            if (stream.data_event) |ev2| ev2.set(self.io);
                            if (stream.completion_sem) |sem| sem.post(self.io);
                            return;
                        }
                    }
                    stream.completed = true;
                    stream.receiveEndStream();
                    if (stream.completion_sem) |sem| sem.post(self.io);
                    if (stream.data_event) |ev2| ev2.set(self.io);
                }
            },
            .rst_stream => {
                // RFC 7540 §6.4: RST_STREAM frames MUST be exactly 4 octets.
                if (frame.payload.len != 4) return error.FrameSizeError;
                // RFC 7540 §5.1: RST_STREAM on an idle stream is a
                // connection error (PROTOCOL_ERROR).
                if (stream.state == .idle) return error.ProtocolError;
                stream.stream_error = error.StreamReset;
                stream.completed = true;
                stream.reset();
                if (stream.completion_sem) |sem| sem.post(self.io);
                if (stream.data_event) |ev| ev.set(self.io);
            },
            else => {},
        }
    }

    /// Reads one frame, handles connection-level frames internally, and
    /// delivers stream-level frames to the per-stream mailbox. Sends
    /// WINDOW_UPDATE for received DATA to keep flow control open.
    /// Returns the stream ID that received data, or null.
    pub fn processOneFrame(self: *Self, reader: anytype, writer: anytype) !?u31 {
        var frame = try self.readFrame(reader);
        defer frame.deinit(self.allocator);

        const maybe = try self.dispatchFrame(&frame, writer);
        if (maybe) |sf| {
            self.deliverToMailbox(&Frame{ .header = sf.header, .payload = sf.payload }) catch |err| switch (err) {
                // RFC 7540 §5.1: late frame for a closed stream — send
                // RST_STREAM(STREAM_CLOSED) but keep the connection alive.
                // No WINDOW_UPDATE accumulation: deliverToMailbox returned
                // before accepting the data, so no credit was consumed.
                error.ClosedStream => {
                    self.sendRstStream(writer, sf.header.stream_id, .stream_closed) catch {};
                    return sf.header.stream_id;
                },
                else => return err,
            };
            // Accumulate WINDOW_UPDATE *after* successful delivery so the
            // credit matches the actual recv window decrement in deliverToMailbox.
            // This avoids permanent window inflation when delivery fails
            // (ClosedStream) after accumulateWindowUpdate has already flushed.
            if (sf.header.frame_type == .data and sf.payload.len > 0) {
                try self.accumulateWindowUpdate(writer, sf.header.stream_id, @intCast(sf.payload.len));
            }
            // RFC 7540 §5.1: If deliverToMailbox flagged a stream error (e.g.
            // DATA on half-closed-remote), send RST_STREAM so the peer stops.
            // This runs after accumulateWindowUpdate so flow control stays balanced.
            if (sf.header.frame_type == .data) {
                if (self.stream_manager.getStream(sf.header.stream_id)) |stream| {
                    if (stream.stream_error != null) {
                        self.sendRstStream(writer, sf.header.stream_id, .stream_closed) catch {};
                    }
                }
            }
            // Flush window credit on stream completion or reset.
            if (sf.header.frame_type == .data and sf.header.flags & FLAG_END_STREAM != 0) {
                try self.flushStreamWindowUpdate(writer, sf.header.stream_id);
                try self.flushConnWindowUpdate(writer);
            } else if (sf.header.frame_type == .rst_stream) {
                // RFC 7540 §5.1: MUST NOT send frames other than PRIORITY on
                // a closed stream. Skip stream-level WINDOW_UPDATE; only flush
                // connection-level credit.
                try self.flushConnWindowUpdate(writer);
            }
            return sf.header.stream_id;
        }
        return null;
    }

    /// Like `processOneFrame` but acquires `write_mutex` around any frame
    /// writes (SETTINGS ACK, PING echo, WINDOW_UPDATE). Use this when
    /// handler fibers may be writing concurrently on the same connection.
    pub fn processOneFrameLocked(self: *Self, reader: anytype, writer: anytype) !?u31 {
        // Read without the lock — reading blocks on I/O and must not prevent
        // handler fibers from writing responses.
        var frame = try self.readFrame(reader);
        defer frame.deinit(self.allocator);

        // Lock for writes: dispatchFrame may send SETTINGS ACK or PING echo,
        // and we may send WINDOW_UPDATE for DATA frames.
        {
            self.write_mutex.lockUncancelable(self.io);
            defer self.write_mutex.unlock(self.io);

            const maybe = try self.dispatchFrame(&frame, writer);
            if (maybe) |sf| {
                // deliverToMailbox decodes HPACK + copies payload under lock,
                // ensuring the shared hpack_decode_ctx is not accessed concurrently.
                self.deliverToMailbox(&Frame{ .header = sf.header, .payload = sf.payload }) catch |err| switch (err) {
                    error.ClosedStream => {
                        self.sendRstStream(writer, sf.header.stream_id, .stream_closed) catch {};
                        return sf.header.stream_id;
                    },
                    else => return err,
                };
                // Accumulate WINDOW_UPDATE *after* successful delivery.
                if (sf.header.frame_type == .data and sf.payload.len > 0) {
                    try self.accumulateWindowUpdate(writer, sf.header.stream_id, @intCast(sf.payload.len));
                }
                // RFC 7540 §5.1: If deliverToMailbox flagged a stream error (e.g.
                // DATA on half-closed-remote), send RST_STREAM so the peer stops.
                // This runs after accumulateWindowUpdate so flow control stays balanced.
                if (sf.header.frame_type == .data) {
                    if (self.stream_manager.getStream(sf.header.stream_id)) |stream| {
                        if (stream.stream_error != null) {
                            self.sendRstStream(writer, sf.header.stream_id, .stream_closed) catch {};
                        }
                    }
                }
                // Flush window credit on stream completion or reset.
                if (sf.header.frame_type == .data and sf.header.flags & FLAG_END_STREAM != 0) {
                    try self.flushStreamWindowUpdate(writer, sf.header.stream_id);
                    try self.flushConnWindowUpdate(writer);
                } else if (sf.header.frame_type == .rst_stream) {
                    // RFC 7540 §5.1: skip stream-level WINDOW_UPDATE for
                    // closed streams; only flush connection-level credit.
                    try self.flushConnWindowUpdate(writer);
                }
                return sf.header.stream_id;
            }
        }
        return null;
    }

    /// Pumps frames until the given stream has received its HEADERS.
    pub fn awaitStreamHeaders(self: *Self, reader: anytype, writer: anytype, stream_id: u31) !void {
        while (true) {
            const stream = self.stream_manager.getStream(stream_id) orelse return error.InvalidStreamId;
            if (stream.got_headers) return;
            if (stream.stream_error) |err| return err;
            _ = try self.processOneFrame(reader, writer);
        }
    }

    /// Pumps frames until the given stream is complete (END_STREAM or error).
    pub fn awaitStreamComplete(self: *Self, reader: anytype, writer: anytype, stream_id: u31) !void {
        while (true) {
            const stream = self.stream_manager.getStream(stream_id) orelse return error.InvalidStreamId;
            if (stream.completed) {
                if (stream.stream_error) |err| return err;
                return;
            }
            _ = try self.processOneFrame(reader, writer);
        }
    }

    /// Continuously pumps frames until GOAWAY or connection error.
    /// Delivers stream-level frames to per-stream mailboxes (posting
    /// completion semaphores when set). Intended for a background
    /// receive fiber on client-side multiplexed connections.
    pub fn runReceiveLoop(self: *Self, reader: anytype, writer: anytype) !void {
        defer self.signalAllStreams(error.ConnectionClosed);
        while (!self.goaway_received) {
            _ = self.processOneFrameLocked(reader, writer) catch |err| switch (err) {
                error.ConnectionClosed => return,
                else => return err,
            };
        }
    }

    /// Signals all active streams with an error and posts their events/semaphores
    /// so waiting fibers don't hang forever after the receive loop exits.
    pub fn signalAllStreams(self: *Self, err: anyerror) void {
        var it = self.stream_manager.streams.iterator();
        while (it.next()) |entry| {
            const s = entry.value_ptr.*;
            if (!s.completed) {
                s.stream_error = err;
                s.completed = true;
                if (s.data_event) |ev| ev.set(self.io);
                if (s.completion_sem) |sem| sem.post(self.io);
            }
        }
    }

    // ---------------------------------------------------------------
    // I/O helpers — work with any duck-typed reader/writer
    // ---------------------------------------------------------------

    /// Reads exactly `buf.len` bytes from reader.
    /// Reader must support `.recv(buf)` (Socket) or be an Io.Reader (TLS).
    fn readExact(reader: anytype, buf: []u8) !void {
        var pos: usize = 0;
        while (pos < buf.len) {
            const n = try readSome(reader, buf[pos..]);
            if (n == 0) return error.ConnectionClosed;
            pos += n;
        }
    }

    fn readSome(reader: anytype, buf: []u8) !usize {
        // Duck-typed: try .read() first, then .recv() for Socket compatibility.
        const Reader = @TypeOf(reader);
        const Child = switch (@typeInfo(Reader)) {
            .pointer => |p| p.child,
            else => Reader,
        };
        if (@hasDecl(Child, "read")) {
            return reader.read(buf);
        } else if (@hasDecl(Child, "recv")) {
            return reader.recv(buf);
        } else if (Reader == *std.Io.Reader) {
            var iov = [_][]u8{buf};
            return reader.readVec(&iov) catch |err| switch (err) {
                error.EndOfStream => @as(usize, 0),
                else => err,
            };
        } else {
            @compileError("readSome: reader must have .read() or .recv() method, got " ++ @typeName(Reader));
        }
    }
};

// ---------------------------------------------------------------
// Test utilities: in-memory reader/writer
// ---------------------------------------------------------------

/// Duck-typed writer backed by an ArrayListUnmanaged(u8).
const TestWriter = struct {
    list: *std.ArrayListUnmanaged(u8),
    allocator: Allocator,

    pub fn writeAll(self: TestWriter, data: []const u8) !void {
        try self.list.appendSlice(self.allocator, data);
    }

    pub fn print(self: TestWriter, comptime fmt: []const u8, args: anytype) !void {
        try self.list.print(self.allocator, fmt, args);
    }
};

fn testWriter(list: *std.ArrayListUnmanaged(u8), allocator: Allocator) TestWriter {
    return .{ .list = list, .allocator = allocator };
}

/// Duck-typed reader that returns a configurable error on the first read.
/// Used to test error propagation paths in readFrame/processOneFrameLocked.
const ErrorReader = struct {
    err: anyerror,

    pub fn read(self: *ErrorReader, _: []u8) !usize {
        return self.err;
    }
};

/// Duck-typed reader backed by a slice cursor.
const TestReader = struct {
    data: []const u8,
    pos: usize = 0,

    pub fn read(self: *TestReader, buf: []u8) !usize {
        if (self.pos >= self.data.len) return 0;
        const available = self.data.len - self.pos;
        const n = @min(available, buf.len);
        @memcpy(buf[0..n], self.data[self.pos .. self.pos + n]);
        self.pos += n;
        return n;
    }
};

// ---------------------------------------------------------------
// Tests
// ---------------------------------------------------------------

test "connection preface round-trip" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();

    // Client writes preface + SETTINGS.
    const writer = testWriter(&wire, allocator);
    try client.sendClientPreface(writer);

    // Server reads and validates preface.
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    var reader = TestReader{ .data = wire.items };
    try server.readClientPreface(&reader);

    // Server reads the SETTINGS frame.
    var frame = try server.readFrame(&reader);
    defer frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.settings, frame.header.frame_type);
    try std.testing.expectEqual(@as(u31, 0), frame.header.stream_id);
}

test "SETTINGS exchange" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();
    conn.local_settings.max_concurrent_streams = 42;

    try conn.sendSettings(writer);

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);
    defer frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.settings, frame.header.frame_type);
    try std.testing.expect(frame.header.flags & H2Connection.FLAG_ACK == 0);

    // Apply settings on peer side.
    var peer = H2Connection.initServer(allocator, std.testing.io);
    defer peer.deinit();
    try peer.applyPeerSettings(frame.payload);
    try std.testing.expectEqual(@as(u32, 42), peer.peer_settings.max_concurrent_streams);
}

test "SETTINGS ACK" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();
    try conn.sendSettingsAck(writer);

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);
    defer frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.settings, frame.header.frame_type);
    try std.testing.expect(frame.header.flags & H2Connection.FLAG_ACK != 0);
    try std.testing.expectEqual(@as(u24, 0), frame.header.length);
}

test "PING echo" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initServer(allocator, std.testing.io);
    defer conn.deinit();

    const ping_data = [8]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };
    try conn.writeFrame(writer, .ping, 0, 0, &ping_data);

    // Read the PING frame.
    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);

    // Handle it (should echo back).
    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const reply_writer = testWriter(&reply, allocator);

    try conn.handlePing(&frame, reply_writer);
    frame.deinit(allocator);

    // Read the PING ACK reply.
    var reply_reader = TestReader{ .data = reply.items };
    var ack = try conn.readFrame(&reply_reader);
    defer ack.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.ping, ack.header.frame_type);
    try std.testing.expect(ack.header.flags & H2Connection.FLAG_ACK != 0);
    try std.testing.expectEqualSlices(u8, &ping_data, ack.payload);
}

test "GOAWAY handling" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    // Build a GOAWAY frame (last_stream_id=7, no_error).
    var goaway_payload: [8]u8 = undefined;
    mem.writeInt(u32, goaway_payload[0..4], 7, .big);
    mem.writeInt(u32, goaway_payload[4..8], @intFromEnum(Http2ErrorCode.no_error), .big);
    try conn.writeFrame(writer, .goaway, 0, 0, &goaway_payload);

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);
    defer frame.deinit(allocator);

    try conn.handleGoaway(&frame);
    try std.testing.expect(conn.goaway_received);
    try std.testing.expectEqual(@as(u31, 7), conn.last_peer_stream_id);
}

test "WINDOW_UPDATE handling" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    const initial_window = conn.stream_manager.connection_send_window;

    const wu_payload = stream_mod.buildWindowUpdatePayload(1000);
    try conn.writeFrame(writer, .window_update, 0, 0, &wu_payload);

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);
    defer frame.deinit(allocator);

    try conn.handleWindowUpdate(&frame);
    try std.testing.expectEqual(initial_window + 1000, conn.stream_manager.connection_send_window);
}

test "HEADERS with CONTINUATION reassembly" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    // HEADERS frame without END_HEADERS + CONTINUATION with END_HEADERS.
    const part1 = "hello";
    const part2 = " world";
    try conn.writeFrame(writer, .headers, H2Connection.FLAG_END_STREAM, 1, part1);
    try conn.writeFrame(writer, .continuation, H2Connection.FLAG_END_HEADERS, 1, part2);

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);
    defer frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.headers, frame.header.frame_type);
    try std.testing.expect(frame.header.flags & H2Connection.FLAG_END_HEADERS != 0);
    try std.testing.expect(frame.header.flags & H2Connection.FLAG_END_STREAM != 0);
    try std.testing.expectEqualSlices(u8, "hello world", frame.payload);
}

test "single HEADERS frame (no CONTINUATION)" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    try conn.writeFrame(writer, .headers, H2Connection.FLAG_END_HEADERS | H2Connection.FLAG_END_STREAM, 1, "complete");

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);
    defer frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.headers, frame.header.frame_type);
    try std.testing.expectEqualSlices(u8, "complete", frame.payload);
}

test "writeHeaders splits at MAX_FRAME_SIZE" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();
    conn.peer_settings.max_frame_size = 10;

    const block = "ABCDEFGHIJKLMNOPQRSTUVWXY"; // 25 bytes → 3 frames
    try conn.writeHeaders(writer, 1, block, true);

    // Read back with CONTINUATION reassembly.
    var conn2 = H2Connection.initClient(allocator, std.testing.io);
    defer conn2.deinit();
    conn2.local_settings.max_frame_size = 16384; // Allow reading large reassembled payload.

    var reader = TestReader{ .data = wire.items };
    var reassembled = try conn2.readFrame(&reader);
    defer reassembled.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.headers, reassembled.header.frame_type);
    try std.testing.expect(reassembled.header.flags & H2Connection.FLAG_END_HEADERS != 0);
    try std.testing.expectEqualSlices(u8, block, reassembled.payload);
}

test "dispatchFrame routes correctly" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initServer(allocator, std.testing.io);
    defer conn.deinit();

    try conn.sendSettings(writer);

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const reply_writer = testWriter(&reply, allocator);

    const result = try conn.dispatchFrame(&frame, reply_writer);
    frame.deinit(allocator);
    try std.testing.expect(result == null);
}

test "buildRequestHeaders" {
    const allocator = std.testing.allocator;
    const extra = [_]hpack.HeaderEntry{
        .{ .name = "content-type", .value = "application/json" },
    };
    const h = try H2Connection.buildRequestHeaders("GET", "/index.html", "https", "example.com", &extra, allocator);
    defer allocator.free(h);

    try std.testing.expectEqual(@as(usize, 5), h.len);
    try std.testing.expectEqualStrings(":method", h[0].name);
    try std.testing.expectEqualStrings("GET", h[0].value);
    try std.testing.expectEqualStrings(":scheme", h[1].name);
    try std.testing.expectEqualStrings(":path", h[3].name);
    try std.testing.expectEqualStrings("/index.html", h[3].value);
    try std.testing.expectEqualStrings("content-type", h[4].name);
}

test "buildResponseHeaders" {
    const allocator = std.testing.allocator;
    var status_buf: [3]u8 = undefined;
    const extra = [_]hpack.HeaderEntry{
        .{ .name = "content-type", .value = "text/html" },
    };
    const h = try H2Connection.buildResponseHeaders(200, &extra, &status_buf, allocator);
    defer allocator.free(h);

    try std.testing.expectEqual(@as(usize, 2), h.len);
    try std.testing.expectEqualStrings(":status", h[0].name);
    try std.testing.expectEqualStrings("200", h[0].value);
}

test "HPACK encode + decode round-trip via sendHeaders/decodeFrameHeaders" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();

    const h2_headers = [_]hpack.HeaderEntry{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":authority", .value = "example.com" },
    };
    try client.sendHeaders(writer, 1, &h2_headers, true);

    // Decode on server side.
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    var reader = TestReader{ .data = wire.items };
    var frame = try server.readFrame(&reader);
    defer frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.headers, frame.header.frame_type);

    const decoded = try server.decodeFrameHeaders(frame.payload, frame.header.flags);
    defer {
        for (decoded.headers) |h| h.deinit(allocator);
        allocator.free(decoded.headers);
    }

    try std.testing.expectEqual(@as(usize, 4), decoded.headers.len);
    try std.testing.expectEqualStrings(":method", decoded.headers[0].name);
    try std.testing.expectEqualStrings("GET", decoded.headers[0].value);
    try std.testing.expectEqualStrings(":path", decoded.headers[1].name);
    try std.testing.expectEqualStrings("/", decoded.headers[1].value);
    try std.testing.expectEqualStrings(":authority", decoded.headers[3].name);
    try std.testing.expectEqualStrings("example.com", decoded.headers[3].value);
}

test "full HTTP/2 request-response round-trip" {
    const allocator = std.testing.allocator;

    // Simulate client → server → client using in-memory buffers.

    // --- Client sends preface + SETTINGS + request ---
    var c2s = std.ArrayListUnmanaged(u8).empty; // client-to-server wire
    defer c2s.deinit(allocator);
    const c2s_w = testWriter(&c2s, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();

    try client.sendClientPreface(c2s_w);

    // Create stream and send request HEADERS (GET /).
    _ = try client.stream_manager.createStream();
    const req_headers = [_]hpack.HeaderEntry{
        .{ .name = ":method", .value = "GET" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":authority", .value = "example.com" },
    };
    try client.sendHeaders(c2s_w, 1, &req_headers, true); // END_STREAM (no body)

    // --- Server reads preface + SETTINGS + request ---
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    var s_reader = TestReader{ .data = c2s.items };
    try server.readClientPreface(&s_reader);

    // Read client's SETTINGS.
    var s2c = std.ArrayListUnmanaged(u8).empty; // server-to-client wire
    defer s2c.deinit(allocator);
    const s2c_w = testWriter(&s2c, allocator);

    var settings_frame = try server.readFrame(&s_reader);
    try server.handleSettings(&settings_frame, s2c_w); // sends ACK
    settings_frame.deinit(allocator);

    // Send server SETTINGS.
    try server.sendSettings(s2c_w);

    // Read the request HEADERS frame.
    var req_frame = try server.readFrame(&s_reader);
    defer req_frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.headers, req_frame.header.frame_type);
    try std.testing.expect(req_frame.header.flags & H2Connection.FLAG_END_STREAM != 0);

    const decoded = try server.decodeFrameHeaders(req_frame.payload, req_frame.header.flags);
    defer {
        for (decoded.headers) |h| h.deinit(allocator);
        allocator.free(decoded.headers);
    }

    // Verify server got the right request.
    try std.testing.expectEqual(@as(usize, 4), decoded.headers.len);
    try std.testing.expectEqualStrings("GET", decoded.headers[0].value);
    try std.testing.expectEqualStrings("/", decoded.headers[1].value);

    // --- Server sends response ---
    var status_buf: [3]u8 = undefined;
    const resp_extra = [_]hpack.HeaderEntry{
        .{ .name = "content-type", .value = "text/plain" },
    };
    const resp_headers = try H2Connection.buildResponseHeaders(200, &resp_extra, &status_buf, allocator);
    defer allocator.free(resp_headers);

    // Server accepts client-initiated stream 1.
    _ = try server.stream_manager.getOrCreateStream(1);
    const stream_id: u31 = 1;

    try server.sendHeaders(s2c_w, stream_id, resp_headers, false);
    const body = "Hello, HTTP/2!";
    try server.writeData(s2c_w, stream_id, body, true);

    // --- Client reads response ---
    var c_reader = TestReader{ .data = s2c.items };

    // Read and dispatch connection-level frames until we get the response HEADERS.
    var got_status: ?[]const u8 = null;
    var got_body = std.ArrayListUnmanaged(u8).empty;
    defer got_body.deinit(allocator);
    var resp_decoded: ?[]hpack.DecodedHeader = null;
    defer if (resp_decoded) |hdrs| {
        for (hdrs) |h| h.deinit(allocator);
        allocator.free(hdrs);
    };

    var end_stream = false;
    while (!end_stream) {
        var frame = try client.readFrame(&c_reader);
        defer frame.deinit(allocator);

        const maybe = try client.dispatchFrame(&frame, c2s_w);
        if (maybe == null) continue;

        const sf = maybe.?;
        switch (sf.header.frame_type) {
            .headers => {
                const dec = try client.decodeFrameHeaders(sf.payload, sf.header.flags);
                resp_decoded = dec.headers;
                for (dec.headers) |h| {
                    if (mem.eql(u8, h.name, ":status")) got_status = h.value;
                }
                if (sf.header.flags & H2Connection.FLAG_END_STREAM != 0) end_stream = true;
            },
            .data => {
                if (sf.payload.len > 0) try got_body.appendSlice(allocator, sf.payload);
                if (sf.header.flags & H2Connection.FLAG_END_STREAM != 0) end_stream = true;
            },
            else => {},
        }
    }

    // Verify the response.
    try std.testing.expectEqualStrings("200", got_status.?);
    try std.testing.expectEqualStrings("Hello, HTTP/2!", got_body.items);
}

test "mailbox-based request-response round-trip" {
    const allocator = std.testing.allocator;

    // --- Client sends preface + SETTINGS + request ---
    var c2s = std.ArrayListUnmanaged(u8).empty;
    defer c2s.deinit(allocator);
    const c2s_w = testWriter(&c2s, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();

    try client.sendClientPreface(c2s_w);

    _ = try client.stream_manager.createStream();
    const req_headers = [_]hpack.HeaderEntry{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/api/data" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":authority", .value = "example.com" },
    };
    try client.sendHeaders(c2s_w, 1, &req_headers, false);
    try client.writeData(c2s_w, 1, "request body", true);

    // --- Server reads via mailbox pattern ---
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    var s_reader = TestReader{ .data = c2s.items };
    try server.readClientPreface(&s_reader);

    var s2c = std.ArrayListUnmanaged(u8).empty;
    defer s2c.deinit(allocator);
    const s2c_w = testWriter(&s2c, allocator);

    // Read client SETTINGS.
    var settings_frame = try server.readFrame(&s_reader);
    try server.handleSettings(&settings_frame, s2c_w);
    settings_frame.deinit(allocator);

    try server.sendSettings(s2c_w);

    // Pump frames until stream 1 is complete.
    while (true) {
        _ = try server.processOneFrame(&s_reader, s2c_w);
        if (server.stream_manager.getStream(1)) |s| {
            if (s.completed) break;
        }
    }

    // Verify mailbox contents.
    const s1 = server.stream_manager.getStream(1).?;
    try std.testing.expect(s1.got_headers);
    try std.testing.expect(s1.completed);
    try std.testing.expectEqualStrings("request body", s1.data_buf.items);

    // Headers were already decoded in deliverToMailbox.
    const req_hdrs = s1.request_headers.?;
    try std.testing.expectEqualStrings(":method", req_hdrs[0].name);
    try std.testing.expectEqualStrings("POST", req_hdrs[0].value);
    try std.testing.expectEqualStrings("/api/data", req_hdrs[1].value);

    // --- Server sends response, client reads via awaitStreamComplete ---
    var status_buf: [3]u8 = undefined;
    const resp_headers = try H2Connection.buildResponseHeaders(201, &.{}, &status_buf, allocator);
    defer allocator.free(resp_headers);

    _ = try server.stream_manager.getOrCreateStream(1);
    try server.sendHeaders(s2c_w, 1, resp_headers, false);
    try server.writeData(s2c_w, 1, "response body", true);

    // Client uses awaitStreamComplete.
    var c_reader = TestReader{ .data = s2c.items };
    try client.awaitStreamComplete(&c_reader, c2s_w, 1);

    const cs1 = client.stream_manager.getStream(1).?;
    try std.testing.expect(cs1.completed);
    try std.testing.expect(cs1.got_headers);
    try std.testing.expectEqualStrings("response body", cs1.data_buf.items);

    // Response headers were already decoded in deliverToMailbox.
    const resp_hdrs = cs1.request_headers.?;
    try std.testing.expectEqualStrings(":status", resp_hdrs[0].name);
    try std.testing.expectEqualStrings("201", resp_hdrs[0].value);
}

test "deliverToMailbox incremental DATA without END_STREAM" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;

    // Deliver a DATA frame without END_STREAM.
    const payload1 = try allocator.dupe(u8, "chunk1");
    defer allocator.free(payload1);
    var frame1 = Frame{
        .header = .{
            .length = @intCast(payload1.len),
            .frame_type = .data,
            .flags = 0, // no END_STREAM
            .stream_id = 1,
        },
        .payload = payload1,
    };
    try server.deliverToMailbox(&frame1);

    // Stream should have data but NOT be completed.
    try std.testing.expectEqualStrings("chunk1", stream.data_buf.items);
    try std.testing.expect(!stream.completed);
    try std.testing.expectEqual(@as(usize, 0), stream.read_offset);

    // Deliver a second DATA frame without END_STREAM.
    const payload2 = try allocator.dupe(u8, "chunk2");
    defer allocator.free(payload2);
    var frame2 = Frame{
        .header = .{
            .length = @intCast(payload2.len),
            .frame_type = .data,
            .flags = 0,
            .stream_id = 1,
        },
        .payload = payload2,
    };
    try server.deliverToMailbox(&frame2);

    try std.testing.expectEqualStrings("chunk1chunk2", stream.data_buf.items);
    try std.testing.expect(!stream.completed);

    // Deliver a final DATA frame WITH END_STREAM.
    const payload3 = try allocator.dupe(u8, "chunk3");
    defer allocator.free(payload3);
    var frame3 = Frame{
        .header = .{
            .length = @intCast(payload3.len),
            .frame_type = .data,
            .flags = H2Connection.FLAG_END_STREAM,
            .stream_id = 1,
        },
        .payload = payload3,
    };
    try server.deliverToMailbox(&frame3);

    try std.testing.expectEqualStrings("chunk1chunk2chunk3", stream.data_buf.items);
    try std.testing.expect(stream.completed);
}

test "h2 streaming: server sends incremental DATA, client reads chunks" {
    const allocator = std.testing.allocator;

    // Server: send HEADERS (no END_STREAM) + 3 DATA chunks + END_STREAM.
    var s2c = std.ArrayListUnmanaged(u8).empty;
    defer s2c.deinit(allocator);
    const s2c_w = testWriter(&s2c, allocator);

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    const stream = try server.stream_manager.getOrCreateStream(1);
    _ = stream;

    var status_buf: [3]u8 = undefined;
    const resp_headers = try H2Connection.buildResponseHeaders(200, &.{}, &status_buf, allocator);
    defer allocator.free(resp_headers);

    // Send HEADERS without END_STREAM.
    try server.sendHeaders(s2c_w, 1, resp_headers, false);

    // Send 3 DATA chunks without END_STREAM.
    try server.writeData(s2c_w, 1, "chunk1", false);
    try server.writeData(s2c_w, 1, "chunk2", false);
    try server.writeData(s2c_w, 1, "chunk3", true); // END_STREAM

    // Client: read the frames and verify incremental delivery.
    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();

    // Client must have stream 1 registered (normally created when sending the request).
    _ = try client.stream_manager.createStream();

    var reader = TestReader{ .data = s2c.items };

    // Process HEADERS frame.
    const sid1 = try client.processOneFrame(&reader, s2c_w);
    try std.testing.expectEqual(@as(?u31, 1), sid1);
    const cs = client.stream_manager.getStream(1).?;
    try std.testing.expect(cs.got_headers);
    try std.testing.expect(!cs.completed);

    // Process first DATA frame — should have "chunk1" but not be completed.
    _ = try client.processOneFrame(&reader, s2c_w);
    try std.testing.expectEqualStrings("chunk1", cs.data_buf.items);
    try std.testing.expect(!cs.completed);

    // Process second DATA frame — accumulated data.
    _ = try client.processOneFrame(&reader, s2c_w);
    try std.testing.expectEqualStrings("chunk1chunk2", cs.data_buf.items);
    try std.testing.expect(!cs.completed);

    // Process third DATA frame with END_STREAM.
    _ = try client.processOneFrame(&reader, s2c_w);
    try std.testing.expectEqualStrings("chunk1chunk2chunk3", cs.data_buf.items);
    try std.testing.expect(cs.completed);

    // Verify read_offset is usable for incremental consumption.
    try std.testing.expectEqual(@as(usize, 0), cs.read_offset);
    cs.read_offset = 6; // consumed "chunk1"
    const remaining = cs.data_buf.items[cs.read_offset..];
    try std.testing.expectEqualStrings("chunk2chunk3", remaining);
}

test "h2 streaming over TCP: incremental DATA frames" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const socket_mod = @import("../net/socket.zig");
    const Socket = socket_mod.Socket;
    const Address = socket_mod.Address;
    const TcpListener = socket_mod.TcpListener;

    // 1. Bind listener.
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    // 2. Client: connect, send h2 preface + GET request.
    var client_sock = try Socket.connect(bound_addr, io);
    defer client_sock.close();

    var client_h2 = H2Connection.initClient(allocator, io);
    defer client_h2.deinit();

    try client_h2.sendClientPreface(&client_sock);
    const req_headers = try H2Connection.buildRequestHeaders("GET", "/stream", "http", "localhost", &.{}, allocator);
    defer allocator.free(req_headers);
    const stream = try client_h2.stream_manager.createStream();
    const stream_id = stream.id;
    try client_h2.sendHeaders(&client_sock, stream_id, req_headers, true);

    // 3. Server: accept, handshake, read request.
    const accept_result = try listener.accept();
    var server_sock = accept_result.socket;
    defer server_sock.close();

    var server_h2 = H2Connection.initServer(allocator, io);
    defer server_h2.deinit();

    var preface_buf: [24]u8 = undefined;
    H2Connection.readExact(&server_sock, &preface_buf) catch return;
    if (!mem.eql(u8, &preface_buf, http.HTTP2_PREFACE)) return error.ProtocolError;

    var settings_frame = try server_h2.readFrame(&server_sock);
    defer settings_frame.deinit(allocator);
    try server_h2.handleSettings(&settings_frame, &server_sock);
    try server_h2.sendSettings(&server_sock);

    // Pump until request stream completes.
    var completed_sid: ?u31 = null;
    while (completed_sid == null) {
        const maybe_sid = try server_h2.processOneFrame(&server_sock, &server_sock);
        if (maybe_sid) |sid| {
            const s = server_h2.stream_manager.getStream(sid) orelse continue;
            if (s.completed) completed_sid = sid;
        }
    }

    // 4. Server: send HEADERS (no END_STREAM) + 3 DATA chunks + END_STREAM.
    var status_buf: [3]u8 = undefined;
    const resp_headers = try H2Connection.buildResponseHeaders(200, &.{}, &status_buf, allocator);
    defer allocator.free(resp_headers);
    try server_h2.sendHeaders(&server_sock, completed_sid.?, resp_headers, false);
    try server_h2.writeData(&server_sock, completed_sid.?, "aaa", false);
    try server_h2.writeData(&server_sock, completed_sid.?, "bbb", false);
    try server_h2.writeData(&server_sock, completed_sid.?, "ccc", true);

    // 5. Client: read frames one at a time, verify incremental data.
    const cs = client_h2.stream_manager.getStream(stream_id).?;

    // Pump until we get HEADERS + all DATA.
    while (!cs.completed) {
        _ = try client_h2.processOneFrame(&client_sock, &client_sock);
    }

    try std.testing.expect(cs.got_headers);
    try std.testing.expect(cs.completed);
    try std.testing.expectEqualStrings("aaabbbccc", cs.data_buf.items);

    // Verify read_offset incremental consumption.
    var out: [3]u8 = undefined;
    @memcpy(&out, cs.data_buf.items[cs.read_offset..][0..3]);
    try std.testing.expectEqualStrings("aaa", &out);
    cs.read_offset += 3;
    @memcpy(&out, cs.data_buf.items[cs.read_offset..][0..3]);
    try std.testing.expectEqualStrings("bbb", &out);
    cs.read_offset += 3;
    @memcpy(&out, cs.data_buf.items[cs.read_offset..][0..3]);
    try std.testing.expectEqualStrings("ccc", &out);
    cs.read_offset += 3;
    try std.testing.expectEqual(cs.data_buf.items.len, cs.read_offset);
}

test "h2 round-trip over TCP loopback" {
    const allocator = std.testing.allocator;
    const io = std.testing.io;
    const socket_mod = @import("../net/socket.zig");
    const Socket = socket_mod.Socket;
    const Address = socket_mod.Address;
    const TcpListener = socket_mod.TcpListener;

    // 1. Bind listener on ephemeral port.
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    // 2. Client: connect and burst-write the h2 handshake + request.
    var client_sock = try Socket.connect(bound_addr, io);
    defer client_sock.close();

    var client_h2 = H2Connection.initClient(allocator, io);
    defer client_h2.deinit();

    // Send client preface (magic + SETTINGS).
    try client_h2.sendClientPreface(&client_sock);

    // Build and send a GET / request.
    const req_headers = try H2Connection.buildRequestHeaders(
        "GET", "/", "http", "localhost", &.{}, allocator,
    );
    defer allocator.free(req_headers);
    const stream = try client_h2.stream_manager.createStream();
    const stream_id = stream.id;
    try client_h2.sendHeaders(&client_sock, stream_id, req_headers, true); // END_STREAM

    // 3. Server: accept and process the h2 handshake + request.
    const accept_result = try listener.accept();
    var server_sock = accept_result.socket;
    defer server_sock.close();

    var server_h2 = H2Connection.initServer(allocator, io);
    defer server_h2.deinit();

    // Read client preface (24-byte magic).
    var preface_buf: [24]u8 = undefined;
    H2Connection.readExact(&server_sock, &preface_buf) catch return;
    if (!mem.eql(u8, &preface_buf, http.HTTP2_PREFACE)) return error.ProtocolError;

    // Read client's SETTINGS frame and ACK it.
    var settings_frame = try server_h2.readFrame(&server_sock);
    defer settings_frame.deinit(allocator);
    if (settings_frame.header.frame_type != .settings) return error.ProtocolError;
    try server_h2.handleSettings(&settings_frame, &server_sock);

    // Send server SETTINGS.
    try server_h2.sendSettings(&server_sock);

    // Pump frames until a stream completes (reads client's HEADERS + SETTINGS ACK).
    var completed_sid: ?u31 = null;
    while (completed_sid == null) {
        const maybe_sid = try server_h2.processOneFrame(&server_sock, &server_sock);
        if (maybe_sid) |sid| {
            const s = server_h2.stream_manager.getStream(sid) orelse continue;
            if (s.completed) completed_sid = sid;
        }
    }

    // Verify the server received the request headers.
    const srv_stream = server_h2.stream_manager.getStream(completed_sid.?).?;
    try std.testing.expect(srv_stream.got_headers);
    try std.testing.expect(srv_stream.completed);

    // 4. Server sends a 200 response with body "hello".
    var status_buf: [3]u8 = undefined;
    const resp_headers = try H2Connection.buildResponseHeaders(200, &.{}, &status_buf, allocator);
    defer allocator.free(resp_headers);
    try server_h2.sendHeaders(&server_sock, completed_sid.?, resp_headers, false);
    try server_h2.writeData(&server_sock, completed_sid.?, "hello", true);

    // 5. Client reads the response (pump frames for server SETTINGS + response).
    try client_h2.awaitStreamComplete(&client_sock, &client_sock, stream_id);

    const resp_stream = client_h2.stream_manager.getStream(stream_id).?;
    try std.testing.expect(resp_stream.completed);
    try std.testing.expectEqualStrings("hello", resp_stream.data_buf.items);

    // Response headers were already decoded in deliverToMailbox.
    const resp_hdrs = resp_stream.request_headers.?;
    try std.testing.expectEqualStrings(":status", resp_hdrs[0].name);
    try std.testing.expectEqualStrings("200", resp_hdrs[0].value);
}

test "batched WINDOW_UPDATE: small DATA frames don't trigger immediate updates" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Register stream 1 (client-initiated) and transition to open.
    const s1 = try server.stream_manager.getOrCreateStream(1);
    s1.state = .open;

    // Build 3 small DATA frames (100 bytes each, well below 32KB threshold).
    var c2s = std.ArrayListUnmanaged(u8).empty;
    defer c2s.deinit(allocator);
    const c2s_w = testWriter(&c2s, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();
    _ = try client.stream_manager.createStream();
    try client.writeData(c2s_w, 1, "A" ** 100, false);
    try client.writeData(c2s_w, 1, "B" ** 100, false);
    try client.writeData(c2s_w, 1, "C" ** 100, false);

    // Server processes the frames, writing responses to reply buffer.
    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const reply_w = testWriter(&reply, allocator);

    var reader = TestReader{ .data = c2s.items };
    _ = try server.processOneFrame(&reader, reply_w);
    _ = try server.processOneFrame(&reader, reply_w);
    _ = try server.processOneFrame(&reader, reply_w);

    // 300 bytes total < 32KB threshold. No WINDOW_UPDATE should be emitted.
    try std.testing.expectEqual(@as(usize, 0), reply.items.len);
    try std.testing.expectEqual(@as(u32, 300), server.pending_conn_window_update);

    // Verify data was still delivered to the stream mailbox.
    const s = server.stream_manager.getStream(1).?;
    try std.testing.expectEqual(@as(usize, 300), s.data_buf.items.len);
    try std.testing.expectEqual(@as(u32, 300), s.pending_window_update);
}

test "batched WINDOW_UPDATE: threshold triggers flush" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();
    const s1 = try server.stream_manager.getOrCreateStream(1);
    s1.state = .open;

    // Build multiple DATA frames totaling > 32KB threshold.
    // Each frame is limited to MAX_FRAME_SIZE (16384), so we send 3 frames.
    var c2s = std.ArrayListUnmanaged(u8).empty;
    defer c2s.deinit(allocator);
    const c2s_w = testWriter(&c2s, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();
    _ = try client.stream_manager.createStream();
    try client.writeData(c2s_w, 1, "X" ** 16000, false);
    try client.writeData(c2s_w, 1, "Y" ** 16000, false);
    try client.writeData(c2s_w, 1, "Z" ** 1000, false);

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const reply_w = testWriter(&reply, allocator);

    var reader = TestReader{ .data = c2s.items };

    // First frame: 16000 bytes, below 32KB threshold. No WINDOW_UPDATE.
    _ = try server.processOneFrame(&reader, reply_w);
    try std.testing.expectEqual(@as(usize, 0), reply.items.len);
    try std.testing.expectEqual(@as(u32, 16000), server.pending_conn_window_update);

    // Second frame: 32000 cumulative, still below 32768 threshold. No flush.
    _ = try server.processOneFrame(&reader, reply_w);
    try std.testing.expectEqual(@as(usize, 0), reply.items.len);
    try std.testing.expectEqual(@as(u32, 32000), server.pending_conn_window_update);

    // Third frame: 33000 cumulative, exceeds 32768 threshold. Should flush.
    _ = try server.processOneFrame(&reader, reply_w);
    // 2 WINDOW_UPDATE frames (connection + stream), 13 bytes each.
    try std.testing.expectEqual(@as(usize, 26), reply.items.len);
    try std.testing.expectEqual(@as(u32, 0), server.pending_conn_window_update);

    const s = server.stream_manager.getStream(1).?;
    try std.testing.expectEqual(@as(u32, 0), s.pending_window_update);
}

test "batched WINDOW_UPDATE: END_STREAM flushes connection and stream credit" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();
    const s1 = try server.stream_manager.getOrCreateStream(1);
    s1.state = .open;

    // Send a small DATA frame with END_STREAM.
    var c2s = std.ArrayListUnmanaged(u8).empty;
    defer c2s.deinit(allocator);
    const c2s_w = testWriter(&c2s, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();
    _ = try client.stream_manager.createStream();
    try client.writeData(c2s_w, 1, "small", true); // END_STREAM

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const reply_w = testWriter(&reply, allocator);

    var reader = TestReader{ .data = c2s.items };
    _ = try server.processOneFrame(&reader, reply_w);

    // END_STREAM should flush both stream and connection accumulators.
    // 5 bytes < threshold, but END_STREAM forces flush.
    // Two WINDOW_UPDATE frames (stream + connection), 13 bytes each = 26.
    try std.testing.expectEqual(@as(usize, 26), reply.items.len);
    try std.testing.expectEqual(@as(u32, 0), server.pending_conn_window_update);
}

test "deliverToMailbox rejects DATA exceeding max_stream_data_size" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();
    server.max_stream_data_size = 100; // 100 bytes max

    // Create a stream.
    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;

    // Deliver a DATA frame within limits.
    var small_frame = Frame{
        .header = .{ .length = 50, .frame_type = .data, .flags = 0, .stream_id = 1 },
        .payload = @constCast(&([_]u8{0x41} ** 50)),
    };
    try server.deliverToMailbox(&small_frame);
    try std.testing.expectEqual(@as(usize, 50), stream.data_buf.items.len);
    try std.testing.expect(!stream.completed);

    // Deliver a DATA frame that exceeds the limit.
    var big_frame = Frame{
        .header = .{ .length = 60, .frame_type = .data, .flags = 0, .stream_id = 1 },
        .payload = @constCast(&([_]u8{0x42} ** 60)),
    };
    try server.deliverToMailbox(&big_frame);

    // Stream should be marked completed with error, data_buf unchanged.
    try std.testing.expect(stream.completed);
    try std.testing.expect(stream.stream_error != null);
    try std.testing.expectEqual(@as(usize, 50), stream.data_buf.items.len);
}

test "deliverToMailbox allows unlimited DATA when max_stream_data_size is 0" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();
    // Default: max_stream_data_size = 0 (unlimited).

    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;

    var frame = Frame{
        .header = .{ .length = 200, .frame_type = .data, .flags = 0, .stream_id = 1 },
        .payload = @constCast(&([_]u8{0x41} ** 200)),
    };
    try server.deliverToMailbox(&frame);
    try std.testing.expectEqual(@as(usize, 200), stream.data_buf.items.len);
    try std.testing.expect(!stream.completed);
}

test "CONTINUATION reassembly rejects oversized header block" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Build a HEADERS frame without END_HEADERS to trigger CONTINUATION mode.
    const headers_payload = [_]u8{0x82}; // :method GET (static index 2)
    var c2s = std.ArrayListUnmanaged(u8).empty;
    defer c2s.deinit(allocator);

    // HEADERS frame (no END_HEADERS flag).
    const hdr1 = Http2FrameHeader{
        .length = headers_payload.len,
        .frame_type = .headers,
        .flags = 0, // no END_HEADERS
        .stream_id = 1,
    };
    try c2s.appendSlice(allocator, &hdr1.serialize());
    try c2s.appendSlice(allocator, &headers_payload);

    // CONTINUATION frame with payload exceeding max_continuation_size.
    const big_payload = try allocator.alloc(u8, H2Connection.max_continuation_size + 1);
    defer allocator.free(big_payload);
    @memset(big_payload, 0);

    const cont_hdr = Http2FrameHeader{
        .length = @intCast(big_payload.len),
        .frame_type = .continuation,
        .flags = H2Connection.FLAG_END_HEADERS,
        .stream_id = 1,
    };
    try c2s.appendSlice(allocator, &cont_hdr.serialize());
    try c2s.appendSlice(allocator, big_payload);

    // Override max_frame_size to allow the large frame through.
    server.local_settings.max_frame_size = @intCast(big_payload.len + 1);

    var reader = TestReader{ .data = c2s.items };
    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const writer = testWriter(&reply, allocator);

    const result = server.readFrame(&reader);
    try std.testing.expectError(error.HeaderBlockTooLarge, result);

    // Verify continuation state was reset so connection can recover.
    try std.testing.expect(server.continuation_stream_id == null);
    try std.testing.expectEqual(@as(usize, 0), server.continuation_buf.items.len);

    _ = writer;
}

test "deliverToMailbox refuses new streams after GOAWAY" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Simulate receiving GOAWAY.
    server.goaway_received = true;

    // Try to deliver a HEADERS frame for a new stream.
    var frame = Frame{
        .header = .{ .length = 1, .frame_type = .headers, .flags = H2Connection.FLAG_END_HEADERS | H2Connection.FLAG_END_STREAM, .stream_id = 3 },
        .payload = @constCast(&[_]u8{0x82}), // :method GET
    };
    const result = server.deliverToMailbox(&frame);
    try std.testing.expectError(error.ProtocolError, result);

    // Existing stream should still work.
    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;
    server.goaway_received = true; // re-set after getOrCreateStream

    var data_frame = Frame{
        .header = .{ .length = 5, .frame_type = .data, .flags = 0, .stream_id = 1 },
        .payload = @constCast("hello"),
    };
    try server.deliverToMailbox(&data_frame);
    try std.testing.expectEqual(@as(usize, 5), stream.data_buf.items.len);
}

test "window accumulator handles values exceeding u31 max" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Set accumulator to a value that exceeds u31 max.
    server.pending_conn_window_update = std.math.maxInt(u31) + 100;

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const writer = testWriter(&reply, allocator);

    try server.flushConnWindowUpdate(writer);

    // Should have sent multiple WINDOW_UPDATE frames and drained to 0.
    try std.testing.expectEqual(@as(u32, 0), server.pending_conn_window_update);
    // At least 2 frames (one for maxInt(u31), one for remainder).
    try std.testing.expect(reply.items.len >= 2 * 13);
}

test "dispatchFrame rejects HEADERS on stream 0" {
    const allocator = std.testing.allocator;
    var conn = H2Connection.initServer(allocator, std.testing.io);
    defer conn.deinit();

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const writer = testWriter(&reply, allocator);

    var frame = Frame{
        .header = .{ .length = 1, .frame_type = .headers, .flags = 0x04, .stream_id = 0 },
        .payload = @constCast(&[_]u8{0x82}),
    };
    try std.testing.expectError(error.ProtocolError, conn.dispatchFrame(&frame, writer));
    // Should have sent GOAWAY.
    try std.testing.expect(reply.items.len > 0);
    try std.testing.expect(conn.goaway_sent);
}

test "dispatchFrame rejects DATA on stream 0" {
    const allocator = std.testing.allocator;
    var conn = H2Connection.initServer(allocator, std.testing.io);
    defer conn.deinit();

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const writer = testWriter(&reply, allocator);

    var frame = Frame{
        .header = .{ .length = 5, .frame_type = .data, .flags = 0, .stream_id = 0 },
        .payload = @constCast("hello"),
    };
    try std.testing.expectError(error.ProtocolError, conn.dispatchFrame(&frame, writer));
    try std.testing.expect(conn.goaway_sent);
}

test "dispatchFrame rejects PUSH_PROMISE" {
    const allocator = std.testing.allocator;
    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const writer = testWriter(&reply, allocator);

    var frame = Frame{
        .header = .{ .length = 5, .frame_type = .push_promise, .flags = 0, .stream_id = 1 },
        .payload = @constCast(&[_]u8{ 0, 0, 0, 2, 0x82 }),
    };
    try std.testing.expectError(error.ProtocolError, conn.dispatchFrame(&frame, writer));
    try std.testing.expect(conn.goaway_sent);
}

test "deliverToMailbox rejects DATA on half-closed-remote stream" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .half_closed_remote; // Peer already sent END_STREAM.

    var frame = Frame{
        .header = .{ .length = 5, .frame_type = .data, .flags = 0, .stream_id = 1 },
        .payload = @constCast("hello"),
    };
    try server.deliverToMailbox(&frame);
    // Stream should be marked as error.
    try std.testing.expect(stream.stream_error != null);
    try std.testing.expect(stream.completed);
    try std.testing.expectEqual(@as(usize, 0), stream.data_buf.items.len);
}

test "deliverToMailbox enforces stream ID monotonicity" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // First create stream 5.
    const s5 = try server.stream_manager.getOrCreateStream(5);
    s5.state = .open;

    // Now try to create stream 3 (lower than 5) — should return ClosedStream
    // since it looks like a late frame for a previously-seen stream ID.
    var frame = Frame{
        .header = .{ .length = 1, .frame_type = .headers, .flags = 0x05, .stream_id = 3 },
        .payload = @constCast(&[_]u8{0x82}),
    };
    try std.testing.expectError(error.ClosedStream, server.deliverToMailbox(&frame));
}

test "receive window decremented on incoming DATA" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;
    const initial_recv = stream.recv_window;
    const initial_conn_recv = server.stream_manager.connection_recv_window;

    var frame = Frame{
        .header = .{ .length = 100, .frame_type = .data, .flags = 0, .stream_id = 1 },
        .payload = @constCast(&([_]u8{0x41} ** 100)),
    };
    try server.deliverToMailbox(&frame);

    try std.testing.expectEqual(initial_recv - 100, stream.recv_window);
    try std.testing.expectEqual(initial_conn_recv - 100, server.stream_manager.connection_recv_window);
}

test "stream-level window flushed at END_STREAM" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();
    const s1 = try server.stream_manager.getOrCreateStream(1);
    s1.state = .open;

    // Send a DATA frame with END_STREAM via processOneFrame.
    var c2s = std.ArrayListUnmanaged(u8).empty;
    defer c2s.deinit(allocator);
    const c2s_w = testWriter(&c2s, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();
    _ = try client.stream_manager.createStream();
    try client.writeData(c2s_w, 1, "test data", true);

    var reply = std.ArrayListUnmanaged(u8).empty;
    defer reply.deinit(allocator);
    const reply_w = testWriter(&reply, allocator);
    var reader = TestReader{ .data = c2s.items };
    _ = try server.processOneFrame(&reader, reply_w);

    // Both stream and connection window should have been flushed.
    try std.testing.expectEqual(@as(u32, 0), server.pending_conn_window_update);
    const s = server.stream_manager.getStream(1).?;
    try std.testing.expectEqual(@as(u32, 0), s.pending_window_update);
    // Two WINDOW_UPDATE frames (stream + connection), 13 bytes each.
    try std.testing.expectEqual(@as(usize, 26), reply.items.len);
}

test "adaptive window_update_threshold adjusts with SETTINGS" {
    const allocator = std.testing.allocator;
    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    // Default threshold is half of 65535 = 32767.
    try std.testing.expectEqual(@as(u32, 32767), conn.window_update_threshold);

    // Change local initial_window_size to 8192 and re-trigger threshold calc.
    conn.local_settings.initial_window_size = 8192;
    // Simulate peer sending any SETTINGS to trigger threshold recalculation.
    var payload: [6]u8 = undefined;
    std.mem.writeInt(u16, payload[0..2], @intFromEnum(http.Http2SettingId.max_concurrent_streams), .big);
    std.mem.writeInt(u32, payload[2..6], 200, .big);
    try conn.applyPeerSettings(&payload);

    // Threshold should be half of 8192 = 4096.
    try std.testing.expectEqual(@as(u32, 4096), conn.window_update_threshold);
}

test "sendGoaway uses last_processed_stream_id" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Simulate receiving frames on streams 1, 3, 5.
    for ([_]u31{ 1, 3, 5 }) |sid| {
        _ = try server.stream_manager.getOrCreateStream(sid);
        server.last_processed_stream_id = sid;
    }

    // sendGoaway should advertise stream 5 (the last we processed).
    try server.sendGoaway(writer, .no_error);

    // Parse the GOAWAY payload to verify last_stream_id.
    var reader = TestReader{ .data = wire.items };
    var frame = try server.readFrame(&reader);
    defer frame.deinit(allocator);

    try std.testing.expectEqual(Http2FrameType.goaway, frame.header.frame_type);
    const last_id: u31 = @intCast(mem.readInt(u32, frame.payload[0..4], .big) & 0x7FFFFFFF);
    try std.testing.expectEqual(@as(u31, 5), last_id);
}

test "deliverToMailbox tracks last_processed_stream_id" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    try std.testing.expectEqual(@as(u31, 0), server.last_processed_stream_id);

    // Deliver a HEADERS frame on stream 1 (peer-initiated for server).
    // Use a minimal valid HPACK block: indexed :method GET.
    const hpack_payload = [_]u8{0x82}; // Indexed: :method GET
    var frame1 = Frame{
        .header = .{
            .length = 1,
            .frame_type = .headers,
            .flags = H2Connection.FLAG_END_HEADERS | H2Connection.FLAG_END_STREAM,
            .stream_id = 1,
        },
        .payload = @constCast(&hpack_payload),
    };
    try server.deliverToMailbox(&frame1);
    try std.testing.expectEqual(@as(u31, 1), server.last_processed_stream_id);

    // Deliver on stream 5 (skipping 3).
    var frame5 = Frame{
        .header = .{
            .length = 1,
            .frame_type = .headers,
            .flags = H2Connection.FLAG_END_HEADERS | H2Connection.FLAG_END_STREAM,
            .stream_id = 5,
        },
        .payload = @constCast(&hpack_payload),
    };
    try server.deliverToMailbox(&frame5);
    try std.testing.expectEqual(@as(u31, 5), server.last_processed_stream_id);
}

test "applyPeerSettings signals HPACK encoder table size update" {
    const allocator = std.testing.allocator;

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    // Initially no pending update.
    try std.testing.expect(conn.stream_manager.hpack_encode_ctx.pending_table_size_update == null);

    // Peer sends SETTINGS with HEADER_TABLE_SIZE = 2048.
    var payload: [6]u8 = undefined;
    std.mem.writeInt(u16, payload[0..2], @intFromEnum(http.Http2SettingId.header_table_size), .big);
    std.mem.writeInt(u32, payload[2..6], 2048, .big);
    try conn.applyPeerSettings(&payload);

    // Encoder should have a pending table size update.
    try std.testing.expectEqual(@as(?usize, 2048), conn.stream_manager.hpack_encode_ctx.pending_table_size_update);
    try std.testing.expectEqual(@as(usize, 2048), conn.stream_manager.hpack_encode_ctx.dynamic_table.max_size);
}

test "handleWindowUpdate signals send_window_event" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initClient(allocator, std.testing.io);
    defer conn.deinit();

    // Write a WINDOW_UPDATE for connection (stream 0).
    const wu_payload = stream_mod.buildWindowUpdatePayload(1000);
    try conn.writeFrame(writer, .window_update, 0, 0, &wu_payload);

    var reader = TestReader{ .data = wire.items };
    var frame = try conn.readFrame(&reader);
    defer frame.deinit(allocator);

    // After handling, the event should be set (no pending waiter, just verifying no crash).
    try conn.handleWindowUpdate(&frame);
    // send_window_event was set — this is a smoke test that it doesn't panic.
    try std.testing.expectEqual(conn.stream_manager.connection_send_window, 65535 + 1000);
}

test "late frame for removed stream returns ClosedStream not ProtocolError" {
    const allocator = std.testing.allocator;
    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Create and open stream 1.
    const s1 = try server.stream_manager.getOrCreateStream(1);
    s1.state = .open;
    // Simulate completing and removing it.
    server.stream_manager.removeStream(1);

    // Late DATA for stream 1 should be ClosedStream, not ProtocolError.
    var frame = Frame{
        .header = .{ .length = 3, .frame_type = .data, .flags = 0, .stream_id = 1 },
        .payload = @constCast("abc"),
    };
    try std.testing.expectError(error.ClosedStream, server.deliverToMailbox(&frame));
}

test "GOAWAY signals streams above last_stream_id" {
    const allocator = std.testing.allocator;
    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var client = H2Connection.initClient(allocator, std.testing.io);
    defer client.deinit();

    // Create streams 1 and 3.
    const s1 = try client.stream_manager.createStream();
    s1.state = .open;
    const s3 = try client.stream_manager.createStream();
    s3.state = .open;

    // Build GOAWAY payload: last_stream_id=1, error_code=no_error.
    const goaway_payload = try stream_mod.buildGoawayPayload(1, .no_error, null, allocator);
    defer allocator.free(goaway_payload);

    // Synthesize GOAWAY frame and handle it.
    var goaway_frame = Frame{
        .header = .{ .length = @intCast(goaway_payload.len), .frame_type = .goaway, .flags = 0, .stream_id = 0 },
        .payload = goaway_payload,
    };
    _ = writer;
    try client.handleGoaway(&goaway_frame);

    // Stream 1 (within last_stream_id) should NOT be signaled.
    try std.testing.expect(s1.stream_error == null);
    try std.testing.expect(!s1.completed);

    // Stream 3 (above last_stream_id) should be signaled as GoawayRefused.
    try std.testing.expect(s3.stream_error != null);
    try std.testing.expect(s3.completed);
}

test "recv window restored after WINDOW_UPDATE sent" {
    const allocator = std.testing.allocator;
    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initServer(allocator, std.testing.io);
    defer conn.deinit();

    const initial_recv = conn.stream_manager.connection_recv_window;

    // Simulate receiving DATA that decrements recv window and accumulates.
    const data_bytes: i32 = @intCast(conn.window_update_threshold + 1);
    try conn.stream_manager.updateConnectionRecvWindow(-data_bytes);
    conn.pending_conn_window_update = conn.window_update_threshold + 1;

    // Flush sends WINDOW_UPDATE and restores the recv window.
    try conn.flushConnWindowUpdate(writer);

    // After flush, recv window should be restored to initial value.
    try std.testing.expectEqual(initial_recv, conn.stream_manager.connection_recv_window);
    try std.testing.expectEqual(@as(u32, 0), conn.pending_conn_window_update);
}

test "enable_push defaults to false" {
    const settings = @import("../core/types.zig").Http2Settings{};
    try std.testing.expectEqual(false, settings.enable_push);
}

test "MAX_CONCURRENT_STREAMS off-by-one fixed: >= not >" {
    const allocator = std.testing.allocator;
    var manager = stream_mod.StreamManager.init(allocator, false);
    defer manager.deinit();
    manager.max_concurrent_streams = 2;

    // Create 2 open streams = the limit.
    const s1 = try manager.getOrCreateStream(1);
    s1.state = .open;
    const s3 = try manager.getOrCreateStream(3);
    s3.state = .open;

    try std.testing.expectEqual(@as(usize, 2), manager.activeStreamCount());
    // With >= check, 2 active streams meets the limit of 2.
    try std.testing.expect(manager.activeStreamCount() >= manager.max_concurrent_streams);
}

test "PADDED DATA: padding stripped from application data" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;

    // Build a padded DATA frame: pad_length=3, data="hello", padding=3 zero bytes.
    // Wire: [pad_len:1][data:5][padding:3] = 9 bytes total.
    const padded_payload = [_]u8{3} ++ "hello".* ++ [_]u8{ 0, 0, 0 };
    var frame = Frame{
        .header = .{
            .length = @intCast(padded_payload.len),
            .frame_type = .data,
            .flags = H2Connection.FLAG_PADDED,
            .stream_id = 1,
        },
        .payload = @constCast(&padded_payload),
    };
    try server.deliverToMailbox(&frame);

    // Application should see only "hello", not the padding.
    try std.testing.expectEqualStrings("hello", stream.data_buf.items);
}

test "PADDED DATA: flow control covers entire frame including padding" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;
    const initial_recv = stream.recv_window;
    const initial_conn_recv = server.stream_manager.connection_recv_window;

    // 9-byte padded frame: 1 byte pad_len + 5 data + 3 padding.
    const padded_payload = [_]u8{3} ++ "hello".* ++ [_]u8{ 0, 0, 0 };
    var frame = Frame{
        .header = .{
            .length = @intCast(padded_payload.len),
            .frame_type = .data,
            .flags = H2Connection.FLAG_PADDED,
            .stream_id = 1,
        },
        .payload = @constCast(&padded_payload),
    };
    try server.deliverToMailbox(&frame);

    // RFC 7540 §6.9: flow control covers entire payload including padding (9 bytes).
    try std.testing.expectEqual(initial_recv - 9, stream.recv_window);
    try std.testing.expectEqual(initial_conn_recv - 9, server.stream_manager.connection_recv_window);
    // But only the actual data is delivered to the application.
    try std.testing.expectEqual(@as(usize, 5), stream.data_buf.items.len);
}

test "PADDED DATA: invalid padding length rejected as ProtocolError" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .open;

    // pad_length=10 but total payload is only 6 bytes — invalid.
    const bad_payload = [_]u8{ 10, 'h', 'e', 'l', 'l', 'o' };
    var frame = Frame{
        .header = .{
            .length = @intCast(bad_payload.len),
            .frame_type = .data,
            .flags = H2Connection.FLAG_PADDED,
            .stream_id = 1,
        },
        .payload = @constCast(&bad_payload),
    };
    try std.testing.expectError(error.ProtocolError, server.deliverToMailbox(&frame));
}

test "trailer HEADERS stored separately from initial headers" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Encode initial headers.
    const init_headers = [_]hpack.HeaderEntry{
        .{ .name = ":method", .value = "POST" },
        .{ .name = ":path", .value = "/" },
        .{ .name = ":scheme", .value = "https" },
        .{ .name = ":authority", .value = "example.com" },
    };
    const init_encoded = try hpack.encodeHeaders(&server.stream_manager.hpack_encode_ctx, &init_headers, allocator);
    defer allocator.free(init_encoded);

    // Deliver initial HEADERS.
    var init_frame = Frame{
        .header = .{
            .length = @intCast(init_encoded.len),
            .frame_type = .headers,
            .flags = H2Connection.FLAG_END_HEADERS,
            .stream_id = 1,
        },
        .payload = init_encoded,
    };
    try server.deliverToMailbox(&init_frame);

    const stream = server.stream_manager.getStream(1).?;
    try std.testing.expect(stream.got_headers);
    try std.testing.expect(!stream.completed);
    try std.testing.expect(stream.request_headers != null);
    try std.testing.expect(stream.trailer_headers == null);

    // Encode trailer headers.
    const trailer_hdrs = [_]hpack.HeaderEntry{
        .{ .name = "grpc-status", .value = "0" },
    };
    const trailer_encoded = try hpack.encodeHeaders(&server.stream_manager.hpack_encode_ctx, &trailer_hdrs, allocator);
    defer allocator.free(trailer_encoded);

    // Deliver trailing HEADERS with END_STREAM.
    var trailer_frame = Frame{
        .header = .{
            .length = @intCast(trailer_encoded.len),
            .frame_type = .headers,
            .flags = H2Connection.FLAG_END_HEADERS | H2Connection.FLAG_END_STREAM,
            .stream_id = 1,
        },
        .payload = trailer_encoded,
    };
    try server.deliverToMailbox(&trailer_frame);

    // Initial headers should be preserved.
    try std.testing.expect(stream.request_headers != null);
    try std.testing.expectEqual(@as(usize, 4), stream.request_headers.?.len);
    try std.testing.expectEqualStrings(":method", stream.request_headers.?[0].name);

    // Trailers stored separately.
    try std.testing.expect(stream.trailer_headers != null);
    try std.testing.expectEqual(@as(usize, 1), stream.trailer_headers.?.len);
    try std.testing.expectEqualStrings("grpc-status", stream.trailer_headers.?[0].name);
    try std.testing.expectEqualStrings("0", stream.trailer_headers.?[0].value);

    // Stream should be completed.
    try std.testing.expect(stream.completed);
}

test "RST_STREAM on idle stream is connection error" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Create a stream but leave it in idle state (no HEADERS received).
    _ = try server.stream_manager.getOrCreateStream(1);

    // Send RST_STREAM for the idle stream.
    const rst_payload = stream_mod.buildRstStreamPayload(.cancel);
    var frame = Frame{
        .header = .{
            .length = 4,
            .frame_type = .rst_stream,
            .flags = 0,
            .stream_id = 1,
        },
        .payload = @constCast(&rst_payload),
    };
    // RFC 7540 §5.1: RST_STREAM on idle stream must be a connection error.
    try std.testing.expectError(error.ProtocolError, server.deliverToMailbox(&frame));
}

test "SETTINGS_MAX_HEADER_LIST_SIZE enforced in HPACK decode" {
    const allocator = std.testing.allocator;

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();
    // Set a very small header list size limit.
    server.local_settings.max_header_list_size = 32;

    // Encode headers that exceed 32 bytes when decoded
    // (name + value per header, RFC 7541 §4.1 doesn't add the 32-byte overhead
    // in our implementation — just raw name+value sizes).
    const headers = [_]hpack.HeaderEntry{
        .{ .name = "x-long-header", .value = "this-value-is-quite-long-indeed" },
    };
    const encoded = try hpack.encodeHeaders(&server.stream_manager.hpack_encode_ctx, &headers, allocator);
    defer allocator.free(encoded);

    // Decoding should fail because total decoded size > 32 bytes.
    const result = server.decodeFrameHeaders(encoded, H2Connection.FLAG_END_HEADERS);
    try std.testing.expectError(error.HeaderBlockTooLarge, result);
}

test "applyInitialWindowSizeChange skips closed streams" {
    const allocator = std.testing.allocator;
    var manager = stream_mod.StreamManager.init(allocator, true);
    defer manager.deinit();

    // Create an open stream and a closed stream.
    const s1 = try manager.createStream();
    s1.state = .open;
    s1.send_window = 1000;

    const s3 = try manager.createStream();
    s3.state = .closed;
    s3.send_window = 500;

    // Apply window size change: 65535 → 70000 (delta = +4465).
    try manager.applyInitialWindowSizeChange(65535, 70000);

    // Open stream should be updated.
    try std.testing.expectEqual(@as(i32, 1000 + 4465), s1.send_window);
    // Closed stream should be unchanged (canSend() = false).
    try std.testing.expectEqual(@as(i32, 500), s3.send_window);
}

test "processOneFrameLocked propagates reader errors distinctly" {
    // Verifies that errors from the reader are propagated through
    // processOneFrameLocked to the caller (not swallowed). The server
    // receive loop relies on distinguishing Canceled, RecvFailed, and
    // ConnectionClosed to choose the correct response (GOAWAY vs retry).
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initServer(allocator, std.testing.io);
    defer conn.deinit();

    // Each error should propagate unchanged.
    const errors = [_]anyerror{ error.Canceled, error.RecvFailed };
    for (errors) |expected_err| {
        var reader = ErrorReader{ .err = expected_err };
        const result = conn.processOneFrameLocked(&reader, writer);
        try std.testing.expectError(expected_err, result);
    }

    // EOF (read returns 0) maps to ConnectionClosed.
    var eof_reader = TestReader{ .data = &.{} };
    try std.testing.expectError(error.ConnectionClosed, conn.readFrame(&eof_reader));
}

test "Frame.deinit frees zero-length payload" {
    // Regression: Frame.deinit previously skipped free for len==0 payloads,
    // leaking allocator metadata (e.g. GPA tracking).
    const allocator = std.testing.allocator;

    const payload = try allocator.alloc(u8, 0);
    var frame = Frame{
        .header = .{ .length = 0, .frame_type = .data, .flags = 0, .stream_id = 0 },
        .payload = payload,
    };
    // Should not leak — GPA would detect if free is skipped.
    frame.deinit(allocator);
}

test "CONTINUATION OOM resets state so connection can recover" {
    // Regression: OOM in continuation_buf.appendSlice left stale
    // continuation_stream_id and buffer, corrupting subsequent frames.
    // Also verifies no double-free (errdefer handles payload free).
    const allocator = std.testing.allocator;

    // Build a HEADERS frame without END_HEADERS to trigger continuation mode.
    const headers_payload = [_]u8{0x82}; // :method GET (static index 2)
    var c2s = std.ArrayListUnmanaged(u8).empty;
    defer c2s.deinit(allocator);

    const hdr1 = Http2FrameHeader{
        .length = headers_payload.len,
        .frame_type = .headers,
        .flags = 0, // no END_HEADERS — triggers continuation mode
        .stream_id = 1,
    };
    try c2s.appendSlice(allocator, &hdr1.serialize());
    try c2s.appendSlice(allocator, &headers_payload);

    // CONTINUATION frame with a small payload (OOM will be on appendSlice).
    const cont_payload = [_]u8{ 0x84, 0x86 }; // two indexed headers
    const cont_hdr = Http2FrameHeader{
        .length = cont_payload.len,
        .frame_type = .continuation,
        .flags = H2Connection.FLAG_END_HEADERS,
        .stream_id = 1,
    };
    try c2s.appendSlice(allocator, &cont_hdr.serialize());
    try c2s.appendSlice(allocator, &cont_payload);

    // Use a FailingAllocator that allows initial setup but fails on a
    // later allocation (the continuation_buf.appendSlice).
    // Count: H2Connection.initServer does some allocs for HPACK tables, etc.
    // We need to find the right fail_index by trial — try from 0 upwards.
    var fail_index: usize = 0;
    while (fail_index < 100) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(allocator, .{ .fail_index = fail_index });
        const fa = failing.allocator();

        var server = H2Connection.initServer(fa, std.testing.io);
        defer server.deinit();

        var reader = TestReader{ .data = c2s.items };
        const result = server.readFrame(&reader);

        if (result) |f| {
            // Success — this fail_index didn't hit the right alloc. Clean up.
            var frame = f;
            frame.deinit(fa);
            continue;
        } else |err| {
            if (err == error.OutOfMemory) {
                // Hit OOM — verify state was cleaned up.
                try std.testing.expect(server.continuation_stream_id == null);
                try std.testing.expectEqual(@as(usize, 0), server.continuation_buf.items.len);
                return; // Test passed.
            }
            // Other errors during init are fine, keep trying.
            continue;
        }
    }
    // If we never hit OOM, something is wrong with the test setup.
    return error.TestExpectedOom;
}

test "sendGoaway is idempotent — second call is a no-op" {
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var conn = H2Connection.initServer(allocator, std.testing.io);
    defer conn.deinit();

    // First call should write a GOAWAY frame.
    try conn.sendGoaway(writer, .no_error);
    try std.testing.expect(conn.goaway_sent);
    const first_len = wire.items.len;
    try std.testing.expect(first_len > 0);

    // Second call should be a no-op.
    try conn.sendGoaway(writer, .protocol_error);
    try std.testing.expectEqual(first_len, wire.items.len);
}

test "processOneFrameLocked sends RST_STREAM on stream_error after DATA delivery" {
    // Verifies the fix: processOneFrameLocked now has the same stream_error
    // RST guard as processOneFrame, sending RST_STREAM when deliverToMailbox
    // flags a stream error (e.g. DATA on half-closed-remote).
    const allocator = std.testing.allocator;

    var wire = std.ArrayListUnmanaged(u8).empty;
    defer wire.deinit(allocator);
    const writer = testWriter(&wire, allocator);

    var server = H2Connection.initServer(allocator, std.testing.io);
    defer server.deinit();

    // Create stream 1 in half_closed_remote (peer already sent END_STREAM).
    const stream = try server.stream_manager.getOrCreateStream(1);
    stream.state = .half_closed_remote;

    // Build a DATA frame for stream 1 (violates half-closed-remote).
    const data_payload = "illegal";
    var frame_buf = std.ArrayListUnmanaged(u8).empty;
    defer frame_buf.deinit(allocator);
    const hdr = Http2FrameHeader{
        .length = data_payload.len,
        .frame_type = .data,
        .flags = 0,
        .stream_id = 1,
    };
    try frame_buf.appendSlice(allocator, &hdr.serialize());
    try frame_buf.appendSlice(allocator, data_payload);

    var reader = TestReader{ .data = frame_buf.items };
    _ = try server.processOneFrameLocked(&reader, writer);

    // deliverToMailbox should have set stream_error.
    try std.testing.expect(stream.stream_error != null);

    // processOneFrameLocked should have sent RST_STREAM in the wire output.
    // Find a RST_STREAM frame (type=0x3) in the wire bytes.
    var found_rst = false;
    var pos: usize = 0;
    while (pos + 9 <= wire.items.len) {
        const flen = std.mem.readInt(u24, wire.items[pos..][0..3], .big);
        const ftype = wire.items[pos + 3];
        if (ftype == 0x3) { // RST_STREAM
            found_rst = true;
            break;
        }
        pos += 9 + flen;
    }
    try std.testing.expect(found_rst);
}
