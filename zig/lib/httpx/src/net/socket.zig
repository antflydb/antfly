//! Cross-Platform Socket Abstraction for httpx.zig
//!
//! Pure-Zig networking via std.Io (no libc dependency):
//!
//! - TCP client and server socket operations via std.Io.net
//! - UDP datagram sockets
//! - Configurable socket options (via posix.setsockopt)
//! - Io.Reader/Io.Writer adapters for TLS integration

const std = @import("std");
const posix = std.posix;
const Io = std.Io;
const net = Io.net;
const Allocator = std.mem.Allocator;
const builtin = @import("builtin");

const is_windows = builtin.os.tag == .windows;

/// Network address type (std.Io.net.IpAddress).
pub const Address = net.IpAddress;

const HostName = net.HostName;

/// Resolves a host string to an address, trying IP literal parsing first
/// and falling back to DNS lookup.
pub fn resolveAddress(io: Io, host: []const u8, port: u16) !Address {
    if (Address.resolve(io, host, port)) |addr| return addr else |_| {}

    const host_name = try HostName.init(host);
    var canonical_name_buffer: [HostName.max_len]u8 = undefined;
    var lookup_buffer: [32]HostName.LookupResult = undefined;
    var lookup_queue: Io.Queue(HostName.LookupResult) = .init(&lookup_buffer);
    try HostName.lookup(host_name, io, &lookup_queue, .{
        .port = port,
        .canonical_name_buffer = &canonical_name_buffer,
    });
    while (true) {
        const result = lookup_queue.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            else => return err,
        };
        switch (result) {
            .address => |address| return address,
            .canonical_name => {},
        }
    }
    return error.UnknownHostName;
}

fn firstNonEmptyBuffer(bufs: [][]u8) ?struct { index: usize, buf: []u8 } {
    for (bufs, 0..) |buf, i| {
        if (buf.len != 0) return .{ .index = i, .buf = buf };
    }
    return null;
}

fn readVecOnce(reader: *Io.Reader, buf: []u8) Io.Reader.Error!usize {
    var iov = [_][]u8{buf};
    return reader.readVec(&iov);
}

fn readAtLeastOne(reader: *Io.Reader, buf: []u8) Io.Reader.Error!usize {
    while (true) {
        const n = try readVecOnce(reader, buf);
        if (n != 0) return n;
    }
}

/// TCP socket abstraction backed by std.Io.net.
pub const Socket = struct {
    handle: net.Socket.Handle,
    io: Io,

    const Self = @This();

    pub const AcceptResult = struct {
        socket: Socket,
        addr: Address,
    };

    /// Connects to the given address and returns a connected TCP socket.
    pub fn connect(addr: Address, io: Io) !Self {
        const stream = try addr.connect(io, .{ .mode = .stream });
        return .{ .handle = stream.socket.handle, .io = io };
    }

    /// Creates a socket from a raw handle (e.g. from accept).
    pub fn fromHandle(handle: net.Socket.Handle, io: Io) Self {
        return .{ .handle = handle, .io = io };
    }

    /// Closes the socket.
    pub fn close(self: *Self) void {
        self.io.vtable.netClose(self.io.userdata, @ptrCast((&self.handle)[0..1]));
    }

    /// Sends data, returning the number of bytes written.
    pub fn send(self: *Self, data: []const u8) !usize {
        return self.io.vtable.netWrite(self.io.userdata, self.handle, "", &.{data}, 1) catch return error.SendFailed;
    }

    /// Sends all data, blocking until complete.
    pub fn sendAll(self: *Self, data: []const u8) !void {
        var sent: usize = 0;
        while (sent < data.len) {
            sent += try self.send(data[sent..]);
        }
    }

    /// Alias for sendAll — provides the `writeAll` interface expected by
    /// duck-typed writers (e.g. H2Connection).
    pub fn writeAll(self: *Self, data: []const u8) !void {
        return self.sendAll(data);
    }

    /// Receives data into the buffer, returning bytes received (0 = EOF).
    pub fn recv(self: *Self, buffer: []u8) !usize {
        if (buffer.len == 0) return 0;
        if (!is_windows) {
            return posix.read(self.handle, buffer) catch |err| switch (err) {
                error.WouldBlock => return error.Timeout,
                else => return error.RecvFailed,
            };
        }
        var bufs = [_][]u8{buffer};
        return self.io.vtable.netRead(self.io.userdata, self.handle, &bufs) catch return error.RecvFailed;
    }

    /// Enables or disables TCP_NODELAY (Nagle's algorithm).
    pub fn setNoDelay(self: *Self, enable: bool) !void {
        const value: u32 = if (enable) 1 else 0;
        try posix.setsockopt(self.handle, posix.IPPROTO.TCP, posix.TCP.NODELAY, std.mem.asBytes(&value));
    }

    /// Sets the receive timeout in milliseconds.
    pub fn setRecvTimeout(self: *Self, ms: u64) !void {
        try self.setTimeout(posix.SO.RCVTIMEO, ms);
    }

    /// Sets the send timeout in milliseconds.
    pub fn setSendTimeout(self: *Self, ms: u64) !void {
        try self.setTimeout(posix.SO.SNDTIMEO, ms);
    }

    fn setTimeout(self: *Self, opt: u32, ms: u64) !void {
        if (is_windows) {
            const value_ms: u32 = @intCast(@min(ms, @as(u64, std.math.maxInt(u32))));
            try posix.setsockopt(self.handle, posix.SOL.SOCKET, opt, std.mem.asBytes(&value_ms));
        } else {
            const tv = posix.timeval{
                .sec = @intCast(ms / 1000),
                .usec = @intCast((ms % 1000) * 1000),
            };
            try posix.setsockopt(self.handle, posix.SOL.SOCKET, opt, std.mem.asBytes(&tv));
        }
    }

    /// Enables or disables keep-alive probes.
    pub fn setKeepAlive(self: *Self, enable: bool) !void {
        const value: u32 = if (enable) 1 else 0;
        try posix.setsockopt(self.handle, posix.SOL.SOCKET, posix.SO.KEEPALIVE, std.mem.asBytes(&value));
    }

    /// Returns a reader interface for the socket.
    pub fn reader(self: *Self) SocketReader {
        return .{ .socket = self };
    }

    /// Returns a writer interface for the socket.
    pub fn writer(self: *Self) SocketWriter {
        return .{ .socket = self };
    }

    pub const SocketReader = struct {
        socket: *Socket,

        pub fn read(self: SocketReader, buffer: []u8) !usize {
            return self.socket.recv(buffer);
        }
    };

    pub const SocketWriter = struct {
        socket: *Socket,

        pub fn writeAll(self: SocketWriter, data: []const u8) !void {
            var sent: usize = 0;
            while (sent < data.len) {
                sent += try self.socket.send(data[sent..]);
            }
        }

        pub fn print(self: SocketWriter, comptime fmt: []const u8, args: anytype) !void {
            // 64 KB stack buffer handles all realistic HTTP header/status lines.
            // If the formatted output somehow exceeds this, bufPrint returns
            // NoSpaceLeft and we propagate it rather than silently truncating.
            var buf: [65536]u8 = undefined;
            const slice = try std.fmt.bufPrint(&buf, fmt, args);
            try self.writeAll(slice);
        }
    };
};

/// Shared Io.Reader VTable helpers for custom reader adapters.
/// These generic implementations call through the vtable's `readVec` and
/// can be reused by any adapter that only needs to provide `readVec`.
pub const IoReaderHelpers = struct {
    pub fn stream(r: *Io.Reader, w: *Io.Writer, limit: Io.Limit) Io.Reader.StreamError!usize {
        var total: usize = 0;
        const max_limit = limit.toInt() orelse std.math.maxInt(usize);

        while (total < max_limit) {
            const max_to_read = @min(r.buffer.len, max_limit - total);
            var iov = [_][]u8{r.buffer[0..max_to_read]};
            const n = r.vtable.readVec(r, &iov) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            if (n == 0) break;

            try w.writeAll(r.buffer[0..n]);
            total += n;
        }

        return total;
    }

    pub fn discard(r: *Io.Reader, limit: Io.Limit) error{ EndOfStream, ReadFailed }!usize {
        var total: usize = 0;
        const max_limit = limit.toInt() orelse std.math.maxInt(usize);

        while (total < max_limit) {
            const max_to_read = @min(r.buffer.len, max_limit - total);
            var iov = [_][]u8{r.buffer[0..max_to_read]};
            const n = r.vtable.readVec(r, &iov) catch |err| switch (err) {
                error.EndOfStream => break,
                else => return err,
            };
            if (n == 0) break;
            total += n;
        }

        return total;
    }

    pub fn rebase(r: *Io.Reader, capacity: usize) Io.Reader.RebaseError!void {
        try std.Io.Reader.defaultRebase(r, capacity);
    }

    /// Builds a standard Io.Reader.VTable using the shared helpers and a
    /// custom readVec implementation. Avoids repeating the same four-field
    /// literal in every Io.Reader adapter.
    pub fn makeVTable(comptime readVecFn: *const fn (*Io.Reader, [][]u8) Io.Reader.Error!usize) Io.Reader.VTable {
        return .{
            .stream = stream,
            .discard = discard,
            .readVec = readVecFn,
            .rebase = rebase,
        };
    }
};

/// Adapter that exposes a `std.Io.Reader` backed by a connected `Socket`.
///
/// This is primarily used to integrate with `std.crypto.tls.Client`.
pub const SocketIoReader = struct {
    socket: *Socket,
    reader_iface: Io.Reader,

    pub fn init(socket: *Socket, buffer: []u8) SocketIoReader {
        return .{
            .socket = socket,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *SocketIoReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        var iovecs_buffer: [8][]u8 = undefined;
        const dest_n, const data_size = try r.writableVector(&iovecs_buffer, bufs);
        const dest = iovecs_buffer[0..dest_n];
        if (dest.len == 0 or dest[0].len == 0) return 0;
        const n = if (is_windows)
            p.socket.io.vtable.netRead(p.socket.io.userdata, p.socket.handle, dest) catch return error.ReadFailed
        else
            posix.read(p.socket.handle, dest[0]) catch |err| switch (err) {
                error.WouldBlock => return error.ReadFailed,
                else => return error.ReadFailed,
            };
        if (n == 0) return error.EndOfStream;
        if (n > data_size) {
            r.end += n - data_size;
            return data_size;
        }
        return n;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that exposes a `std.Io.Reader` backed by a `[]const u8` slice.
///
/// Used to feed in-memory data (e.g. compressed bytes) to APIs that require
/// an `Io.Reader`, such as `std.compress.flate.Decompress`.
pub const SliceIoReader = struct {
    data: []const u8,
    pos: usize = 0,
    reader_iface: Io.Reader,

    pub fn init(data: []const u8, buffer: []u8) SliceIoReader {
        return .{
            .data = data,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *SliceIoReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        const entry = firstNonEmptyBuffer(bufs) orelse return 0;
        const buf = entry.buf;
        const remaining = p.data[p.pos..];
        if (remaining.len == 0) return error.EndOfStream;
        const n = @min(buf.len, remaining.len);
        @memcpy(buf[0..n], remaining[0..n]);
        p.pos += n;
        return n;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that exposes a `std.Io.Writer` backed by a connected `Socket`.
///
/// This is primarily used to integrate with `std.crypto.tls.Client`.
pub const SocketIoWriter = struct {
    socket: *Socket,
    writer_iface: Io.Writer,

    pub fn init(socket: *Socket, buffer: []u8) SocketIoWriter {
        return .{
            .socket = socket,
            .writer_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .end = 0,
            },
        };
    }

    fn parent(w: *Io.Writer) *SocketIoWriter {
        return @fieldParentPtr("writer_iface", w);
    }

    fn drain(w: *Io.Writer, bufs: []const []const u8, splat: usize) Io.Writer.Error!usize {
        const p = parent(w);
        if (bufs.len == 0) {
            const buffered = w.buffered();
            if (buffered.len == 0) return 0;
            p.socket.sendAll(buffered) catch return error.WriteFailed;
            return w.consumeAll();
        }
        const n = p.socket.io.vtable.netWrite(p.socket.io.userdata, p.socket.handle, w.buffered(), bufs, splat) catch return error.WriteFailed;
        return w.consume(n);
    }

    fn sendFile(w: *Io.Writer, file_reader: *Io.File.Reader, limit: Io.Limit) Io.Writer.FileError!usize {
        const p = parent(w);

        var total: usize = 0;
        const max_limit = limit.toInt() orelse std.math.maxInt(usize);
        while (total < max_limit) {
            const remaining = max_limit - total;
            const chunk_len = @min(w.buffer.len, remaining);
            if (chunk_len == 0) break;

            const n_read = file_reader.file.readStreaming(file_reader.io, &.{w.buffer[0..chunk_len]}) catch return error.ReadFailed;
            if (n_read == 0) break;

            p.socket.sendAll(w.buffer[0..n_read]) catch return error.WriteFailed;
            total += n_read;
        }

        return total;
    }

    fn flush(w: *Io.Writer) Io.Writer.Error!void {
        const p = parent(w);
        while (w.end != 0) {
            const buffered = w.buffered();
            p.socket.sendAll(buffered) catch return error.WriteFailed;
            _ = w.consumeAll();
        }
    }

    fn rebase(w: *Io.Writer, preserve: usize, capacity: usize) Io.Writer.Error!void {
        if (w.buffer.len - w.end >= capacity) return;

        const preserved_len = @min(preserve, w.end);
        const to_flush = w.end - preserved_len;
        if (to_flush > 0) {
            const p = parent(w);
            p.socket.sendAll(w.buffer[0..to_flush]) catch return error.WriteFailed;
        }
        if (preserved_len > 0 and to_flush > 0) {
            @memmove(w.buffer[0..preserved_len], w.buffer[to_flush..][0..preserved_len]);
        }
        w.end = preserved_len;
        if (w.buffer.len - w.end < capacity) return error.WriteFailed;
    }

    const vtable: Io.Writer.VTable = .{
        .drain = drain,
        .sendFile = sendFile,
        .flush = flush,
        .rebase = rebase,
    };
};

/// Adapter that first serves bytes from a prefix slice, then delegates to an
/// inner `Io.Reader`. Used to drain leftover header-parse bytes before
/// switching to the network stream for the body.
pub const PrefixedReader = struct {
    prefix: []const u8,
    prefix_pos: usize = 0,
    inner: *Io.Reader,
    reader_iface: Io.Reader,

    pub fn init(prefix: []const u8, inner: *Io.Reader, buffer: []u8) PrefixedReader {
        return .{
            .prefix = prefix,
            .inner = inner,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *PrefixedReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        const entry = firstNonEmptyBuffer(bufs) orelse return 0;
        const buf = entry.buf;

        // Drain prefix first.
        const remaining = p.prefix[p.prefix_pos..];
        if (remaining.len > 0) {
            const n = @min(buf.len, remaining.len);
            @memcpy(buf[0..n], remaining[0..n]);
            p.prefix_pos += n;
            return n;
        }

        // Prefix exhausted — delegate to inner reader.
        return readVecOnce(p.inner, buf);
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that wraps an `Io.Reader` and limits reads to exactly `limit` bytes.
///
/// Used to enforce Content-Length boundaries when streaming HTTP response bodies.
/// After `limit` bytes have been delivered, further reads return `EndOfStream`.
pub const ContentLengthReader = struct {
    inner: *Io.Reader,
    remaining: usize,
    reader_iface: Io.Reader,

    pub fn init(inner: *Io.Reader, limit: usize, buffer: []u8) ContentLengthReader {
        return .{
            .inner = inner,
            .remaining = limit,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *ContentLengthReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        if (p.remaining == 0) return error.EndOfStream;
        const entry = firstNonEmptyBuffer(bufs) orelse return 0;

        // Clamp the caller's buffer to our remaining byte budget.
        const orig_buf = bufs[entry.index];
        const clamped_len = @min(orig_buf.len, p.remaining);
        bufs[entry.index] = orig_buf[0..clamped_len];
        defer bufs[entry.index] = orig_buf; // restore original slice for caller

        const n = readVecOnce(p.inner, bufs[entry.index]) catch |err| switch (err) {
            error.EndOfStream => return error.ReadFailed,
            error.ReadFailed => return error.ReadFailed,
        };
        p.remaining -= n;
        return n;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// Adapter that wraps an `Io.Reader` and decodes HTTP chunked transfer encoding.
///
/// Reads chunk-size lines, delivers chunk data, consumes inter-chunk CRLFs,
/// and returns `EndOfStream` after the terminal `0\r\n\r\n` chunk.
///
/// Uses an internal read-ahead buffer to reduce per-byte syscalls when
/// parsing chunk-size lines and inter-chunk delimiters.
pub const ChunkedBodyReader = struct {
    inner: *Io.Reader,
    chunk_remaining: usize = 0,
    state: ChunkState = .chunk_size,
    line_buf: [32]u8 = undefined,
    line_len: usize = 0,
    // Read-ahead buffer for reducing syscalls during chunk-size parsing.
    ahead_buf: [512]u8 = undefined,
    ahead_start: usize = 0,
    ahead_end: usize = 0,
    reader_iface: Io.Reader,

    const ChunkState = enum {
        chunk_size,
        chunk_data,
        chunk_crlf,
        trailer,
        done,
    };

    pub fn init(inner: *Io.Reader, buffer: []u8) ChunkedBodyReader {
        return .{
            .inner = inner,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *ChunkedBodyReader {
        return @fieldParentPtr("reader_iface", r);
    }

    /// Reads a single byte, consuming from the read-ahead buffer first,
    /// then falling back to a bulk read from the inner reader.
    fn readOneByte(p: *ChunkedBodyReader) Io.Reader.Error!u8 {
        if (p.ahead_start < p.ahead_end) {
            const b = p.ahead_buf[p.ahead_start];
            p.ahead_start += 1;
            return b;
        }
        // Refill the read-ahead buffer in bulk.
        try p.fillAhead();
        if (p.ahead_start >= p.ahead_end) return error.EndOfStream;
        const b = p.ahead_buf[p.ahead_start];
        p.ahead_start += 1;
        return b;
    }

    /// Fills the read-ahead buffer from the inner reader.
    /// Compacts remaining bytes to the front first.
    fn fillAhead(p: *ChunkedBodyReader) Io.Reader.Error!void {
        // Compact: move unconsumed bytes to front.
        const remaining = p.ahead_end - p.ahead_start;
        if (remaining > 0 and p.ahead_start > 0) {
            std.mem.copyForwards(u8, p.ahead_buf[0..remaining], p.ahead_buf[p.ahead_start..p.ahead_end]);
        }
        p.ahead_start = 0;
        p.ahead_end = remaining;

        if (p.ahead_end >= p.ahead_buf.len) return; // buffer full
        const n = readAtLeastOne(p.inner, p.ahead_buf[p.ahead_end..]) catch |err| return err;
        if (n == 0) return error.EndOfStream;
        p.ahead_end += n;
    }

    /// Returns the number of buffered bytes available in the read-ahead buffer.
    fn aheadAvailable(p: *ChunkedBodyReader) usize {
        return p.ahead_end - p.ahead_start;
    }

    fn readVec(r: *Io.Reader, bufs: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        var iovecs_buffer: [8][]u8 = undefined;
        const dest_n, const data_size = try r.writableVector(&iovecs_buffer, bufs);
        const dest = iovecs_buffer[0..dest_n];
        const entry = firstNonEmptyBuffer(dest) orelse return 0;

        while (true) {
            switch (p.state) {
                .done => return error.EndOfStream,

                .chunk_size => {
                    // Read into read-ahead buffer and scan for newline.
                    while (true) {
                        // Scan buffered data for newline.
                        const buffered = p.ahead_buf[p.ahead_start..p.ahead_end];
                        if (std.mem.indexOfScalar(u8, buffered, '\n')) |nl_pos| {
                            // Accumulate line content (excluding \r and \n) into line_buf.
                            for (buffered[0..nl_pos]) |byte| {
                                if (byte == '\r') continue;
                                if (p.line_len >= p.line_buf.len) return error.ReadFailed;
                                p.line_buf[p.line_len] = byte;
                                p.line_len += 1;
                            }
                            p.ahead_start += nl_pos + 1; // consume through newline
                            break;
                        }
                        // No newline yet — accumulate all buffered bytes into line_buf.
                        for (buffered) |byte| {
                            if (byte == '\r') continue;
                            if (p.line_len >= p.line_buf.len) return error.ReadFailed;
                            p.line_buf[p.line_len] = byte;
                            p.line_len += 1;
                        }
                        p.ahead_start = p.ahead_end; // consume all

                        // Read more data in bulk.
                        try p.fillAhead();
                    }

                    // Parse hex chunk size (ignore extensions after ';').
                    const line = p.line_buf[0..p.line_len];
                    p.line_len = 0;
                    const hex_end = std.mem.indexOfScalar(u8, line, ';') orelse line.len;
                    const hex = std.mem.trim(u8, line[0..hex_end], " \t");
                    p.chunk_remaining = std.fmt.parseInt(usize, hex, 16) catch return error.ReadFailed;

                    if (p.chunk_remaining == 0) {
                        // Terminal chunk. Consume trailer lines until empty line.
                        p.state = .trailer;
                        continue;
                    }
                    p.state = .chunk_data;
                    continue;
                },

                .chunk_data => {
                    // First drain any read-ahead bytes that belong to this chunk.
                    const ahead_avail = p.aheadAvailable();
                    if (ahead_avail > 0) {
                        const orig_buf = dest[entry.index];
                        const n = @min(@min(orig_buf.len, p.chunk_remaining), ahead_avail);
                        @memcpy(orig_buf[0..n], p.ahead_buf[p.ahead_start..][0..n]);
                        p.ahead_start += n;
                        p.chunk_remaining -= n;
                        if (p.chunk_remaining == 0) {
                            p.state = .chunk_crlf;
                        }
                        if (n > data_size) {
                            r.end += n - data_size;
                            return data_size;
                        }
                        return n;
                    }

                    // Read-ahead empty — read directly from inner reader.
                    const orig_buf = dest[entry.index];
                    const clamped_len = @min(orig_buf.len, p.chunk_remaining);
                    dest[entry.index] = orig_buf[0..clamped_len];
                    defer dest[entry.index] = orig_buf;

                    const n = readVecOnce(p.inner, dest[entry.index]) catch |err| return err;
                    p.chunk_remaining -= n;
                    if (p.chunk_remaining == 0) {
                        p.state = .chunk_crlf;
                    }
                    if (n > data_size) {
                        r.end += n - data_size;
                        return data_size;
                    }
                    return n;
                },

                .chunk_crlf => {
                    // Consume the \r\n after chunk data.
                    const b1 = p.readOneByte() catch |err| return err;
                    if (b1 == '\r') {
                        const b2 = p.readOneByte() catch |err| return err;
                        if (b2 != '\n') return error.ReadFailed;
                    } else if (b1 != '\n') {
                        return error.ReadFailed;
                    }
                    p.state = .chunk_size;
                    continue;
                },

                .trailer => {
                    // Read trailer lines until we see an empty line (\r\n or \n).
                    var saw_content = false;
                    while (true) {
                        const byte = p.readOneByte() catch |err| return err;
                        if (byte == '\n') {
                            if (!saw_content) {
                                p.state = .done;
                                return error.EndOfStream;
                            }
                            saw_content = false;
                            continue;
                        }
                        if (byte != '\r') saw_content = true;
                    }
                },
            }
        }
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

/// TCP listener for accepting incoming connections.
pub const TcpListener = struct {
    server: net.Server,
    io: Io,

    const Self = @This();

    /// Creates and binds a TCP listener to the address.
    pub fn init(addr: Address, io: Io) !Self {
        return initWithOptions(addr, io, .{ .reuse_address = true });
    }

    /// Creates and binds with explicit options.
    pub fn initWithOptions(addr: Address, io: Io, options: Address.ListenOptions) !Self {
        const server = try addr.listen(io, options);
        return .{ .server = server, .io = io };
    }

    /// Closes the listener.
    pub fn deinit(self: *Self) void {
        self.server.deinit(self.io);
    }

    /// Accepts an incoming connection.
    pub fn accept(self: *Self) !Socket.AcceptResult {
        const stream = try self.server.accept(self.io);
        return .{
            .socket = Socket.fromHandle(stream.socket.handle, self.io),
            .addr = stream.socket.address,
        };
    }

    /// Returns the local address the listener is bound to.
    pub fn getLocalAddress(self: *Self) Address {
        return self.server.socket.address;
    }
};

/// UDP datagram socket abstraction backed by std.Io.net.
pub const UdpSocket = struct {
    socket: net.Socket,
    io: Io,

    const Self = @This();

    /// Creates a UDP socket bound to the given address.
    pub fn bind(addr: Address, io: Io) !Self {
        const socket = try addr.bind(io, .{ .mode = .dgram });
        return .{ .socket = socket, .io = io };
    }

    /// Closes the socket.
    pub fn close(self: *Self) void {
        self.socket.close(self.io);
    }

    /// Sends a datagram to a specific address.
    /// UDP sends are all-or-nothing; returns data.len on success.
    pub fn sendTo(self: *Self, dest: Address, data: []const u8) !usize {
        self.socket.send(self.io, &dest, data) catch return error.SendFailed;
        return data.len;
    }

    /// Receives a datagram and returns the source address.
    pub fn recvFrom(self: *Self, buffer: []u8) !struct { n: usize, addr: Address } {
        const msg = self.socket.receive(self.io, buffer) catch return error.RecvFailed;
        return .{ .n = msg.data.len, .addr = msg.from };
    }
};

const BufferOnlyReader = struct {
    remaining: []const u8,
    chunk_size: usize,
    reader_iface: Io.Reader,

    fn init(data: []const u8, chunk_size: usize, buffer: []u8) BufferOnlyReader {
        return .{
            .remaining = data,
            .chunk_size = chunk_size,
            .reader_iface = .{
                .vtable = &vtable,
                .buffer = buffer,
                .seek = 0,
                .end = 0,
            },
        };
    }

    fn parent(r: *Io.Reader) *BufferOnlyReader {
        return @fieldParentPtr("reader_iface", r);
    }

    fn readVec(r: *Io.Reader, _: [][]u8) Io.Reader.Error!usize {
        const p = parent(r);
        if (p.remaining.len == 0) return error.EndOfStream;
        const available = r.buffer.len - r.end;
        if (available == 0) return 0;

        const n = @min(@min(available, p.remaining.len), p.chunk_size);
        @memcpy(r.buffer[r.end..][0..n], p.remaining[0..n]);
        r.end += n;
        p.remaining = p.remaining[n..];
        return 0;
    }

    const vtable = IoReaderHelpers.makeVTable(readVec);
};

test "Socket connect and close" {
    const io = std.testing.io;

    // Listen on a random port
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();

    const bound_addr = listener.getLocalAddress();
    try std.testing.expect(bound_addr.getPort() != 0);

    // Connect
    var socket = try Socket.connect(bound_addr, io);
    defer socket.close();
}

test "TcpListener accept" {
    const io = std.testing.io;

    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();

    const bound_addr = listener.getLocalAddress();

    // Connect client
    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    // Accept server side
    var result = try listener.accept();
    defer result.socket.close();
}

test "Socket.writer writeAll and print over TCP" {
    const io = std.testing.io;
    const allocator = std.testing.allocator;

    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    // Client writes using SocketWriter.
    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    const w = client.writer();
    try w.writeAll("GET / HTTP/1.1\r\n");
    try w.print("Host: {s}\r\n", .{"localhost"});
    try w.writeAll("\r\n");

    // Server reads and verifies.
    var result = try listener.accept();
    defer result.socket.close();

    var buf: [256]u8 = undefined;
    var total: usize = 0;
    while (total < "GET / HTTP/1.1\r\nHost: localhost\r\n\r\n".len) {
        const n = try result.socket.recv(buf[total..]);
        if (n == 0) break;
        total += n;
    }
    try std.testing.expectEqualStrings("GET / HTTP/1.1\r\nHost: localhost\r\n\r\n", buf[0..total]);

    // Also verify Request.serialize works through SocketWriter.
    const Request = @import("../core/request.zig").Request;
    var req = try Request.init(allocator, .GET, "/test");
    defer req.deinit();
    try req.headers.set("Host", "example.com");

    var client2 = try Socket.connect(bound_addr, io);
    defer client2.close();
    const w2 = client2.writer();
    try req.serialize(w2);

    var result2 = try listener.accept();
    defer result2.socket.close();

    var buf2: [512]u8 = undefined;
    var total2: usize = 0;
    while (total2 < 4) { // read at least request line
        const n = try result2.socket.recv(buf2[total2..]);
        if (n == 0) break;
        total2 += n;
    }
    // Verify the request line was serialized correctly.
    try std.testing.expect(std.mem.startsWith(u8, buf2[0..total2], "GET /test"));
}

test "UdpSocket send/recv localhost" {
    const io = std.testing.io;

    const bind_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var recv_sock = try UdpSocket.bind(bind_addr, io);
    defer recv_sock.close();

    const recv_addr = recv_sock.socket.address;

    var send_sock = try UdpSocket.bind(bind_addr, io);
    defer send_sock.close();

    const msg = "ping";
    _ = try send_sock.sendTo(recv_addr, msg);

    var buf: [32]u8 = undefined;
    const got = try recv_sock.recvFrom(&buf);
    try std.testing.expectEqualStrings(msg, buf[0..got.n]);
}

test "SocketIoWriter flush sends buffered bytes" {
    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    var write_buf: [64]u8 = undefined;
    var writer = SocketIoWriter.init(&client, &write_buf);
    try writer.writer_iface.writeAll("hello");
    try writer.writer_iface.flush();

    var result = try listener.accept();
    defer result.socket.close();

    var recv_buf: [16]u8 = undefined;
    const got = try result.socket.recv(&recv_buf);
    try std.testing.expectEqualStrings("hello", recv_buf[0..got]);
}

test "Socket recv timeout returns error.Timeout" {
    if (is_windows) return;

    const io = std.testing.io;
    const listen_addr = Address{ .ip4 = .{ .bytes = .{ 127, 0, 0, 1 }, .port = 0 } };
    var listener = try TcpListener.init(listen_addr, io);
    defer listener.deinit();
    const bound_addr = listener.getLocalAddress();

    var client = try Socket.connect(bound_addr, io);
    defer client.close();

    var accepted = try listener.accept();
    defer accepted.socket.close();

    try accepted.socket.setRecvTimeout(50);

    var recv_buf: [8]u8 = undefined;
    try std.testing.expectError(error.Timeout, accepted.socket.recv(&recv_buf));
}

test "SliceIoReader skips leading empty buffers" {
    const data = "hello";
    var read_buf: [32]u8 = undefined;
    var reader = SliceIoReader.init(data, &read_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try reader.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, data.len), got);
    try std.testing.expectEqualStrings(data, out[0..got]);
}

test "PrefixedReader skips leading empty buffers" {
    var inner_buf: [32]u8 = undefined;
    var inner = SliceIoReader.init("world", &inner_buf);
    var prefixed_buf: [32]u8 = undefined;
    var prefixed = PrefixedReader.init("hello", &inner.reader_iface, &prefixed_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try prefixed.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, 5), got);
    try std.testing.expectEqualStrings("hello", out[0..got]);
}

test "ContentLengthReader skips leading empty buffers" {
    var inner_buf: [32]u8 = undefined;
    var inner = SliceIoReader.init("abcdef", &inner_buf);
    var cl_buf: [32]u8 = undefined;
    var limited = ContentLengthReader.init(&inner.reader_iface, 3, &cl_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try limited.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, 3), got);
    try std.testing.expectEqualStrings("abc", out[0..got]);
}

test "ContentLengthReader handles inner reader that buffers before producing bytes" {
    var inner_buf: [16]u8 = undefined;
    var inner = BufferOnlyReader.init("abcdef", 2, &inner_buf);
    var cl_buf: [16]u8 = undefined;
    var limited = ContentLengthReader.init(&inner.reader_iface, 4, &cl_buf);

    var out: [8]u8 = undefined;
    const got = try limited.reader_iface.readSliceShort(out[0..4]);
    try std.testing.expectEqual(@as(usize, 4), got);
    try std.testing.expectEqualStrings("abcd", out[0..got]);
}

test "ContentLengthReader reports premature EOF as ReadFailed" {
    var inner_buf: [16]u8 = undefined;
    var inner = SliceIoReader.init("abc", &inner_buf);
    var cl_buf: [16]u8 = undefined;
    var limited = ContentLengthReader.init(&inner.reader_iface, 4, &cl_buf);

    var out: [8]u8 = undefined;
    try std.testing.expectError(error.ReadFailed, limited.reader_iface.readSliceShort(out[0..4]));
}

test "ChunkedBodyReader skips leading empty buffers" {
    var inner_buf: [64]u8 = undefined;
    var inner = SliceIoReader.init("5\r\nhello\r\n0\r\n\r\n", &inner_buf);
    var chunked_buf: [64]u8 = undefined;
    var chunked = ChunkedBodyReader.init(&inner.reader_iface, &chunked_buf);

    var empty: [0]u8 = .{};
    var out: [8]u8 = undefined;
    var iov = [_][]u8{ empty[0..], out[0..] };
    const got = try chunked.reader_iface.readVec(&iov);
    try std.testing.expectEqual(@as(usize, 5), got);
    try std.testing.expectEqualStrings("hello", out[0..got]);
}

test "ChunkedBodyReader handles inner reader that buffers before producing bytes" {
    var inner_buf: [64]u8 = undefined;
    var inner = BufferOnlyReader.init("5\r\nhello\r\n0\r\n\r\n", 3, &inner_buf);
    var chunked_buf: [64]u8 = undefined;
    var chunked = ChunkedBodyReader.init(&inner.reader_iface, &chunked_buf);

    var out: [8]u8 = undefined;
    const got = try chunked.reader_iface.readSliceShort(out[0..5]);
    try std.testing.expectEqual(@as(usize, 5), got);
    try std.testing.expectEqualStrings("hello", out[0..got]);
}
