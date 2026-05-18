//! Incremental HTTP Message Parser for httpx.zig
//!
//! State-machine based parser for HTTP/1.x messages supporting:
//!
//! - Incremental parsing (feed data as it arrives)
//! - Request and response parsing
//! - Chunked transfer encoding
//! - Header limits for security
//! - Cross-platform compatible

const std = @import("std");
const mem = std.mem;
const Allocator = mem.Allocator;

const types = @import("../core/types.zig");
const headers_mod = @import("../core/headers.zig");
const Headers = headers_mod.Headers;
const containsToken = headers_mod.containsToken;
const Status = @import("../core/status.zig").Status;

/// Parser state machine states.
pub const ParserState = enum {
    start,
    request_line,
    status_line,
    headers,
    body,
    chunk_size,
    chunk_data,
    chunk_crlf,
    chunk_trailer,
    complete,
    err,
};

/// Parser mode - request or response.
pub const ParserMode = enum {
    request,
    response,
};

/// Reason for the parser entering the error state.
pub const ErrorReason = enum {
    none,
    header_too_large,
    too_many_headers,
    body_too_large,
    invalid_header,
    invalid_chunk_encoding,
    malformed_request_line,
    malformed_status_line,
    malformed_chunk_size,
    smuggling_detected,
};

/// Incremental HTTP message parser.
pub const Parser = struct {
    allocator: Allocator,
    state: ParserState = .start,
    mode: ParserMode = .request,
    error_reason: ErrorReason = .none,
    method: ?types.Method = null,
    path: ?[]const u8 = null,
    version: types.Version = .HTTP_1_1,
    status_code: ?u16 = null,
    headers: Headers,
    body_buffer: std.ArrayListUnmanaged(u8) = .empty,
    content_length: ?u64 = null,
    chunked: bool = false,
    current_chunk_size: usize = 0,
    bytes_read: usize = 0,
    chunk_crlf_read: u2 = 0,
    line_buffer: std.ArrayListUnmanaged(u8) = .empty,
    max_header_size: usize = 8192,
    max_headers: usize = 100,
    max_body_size: usize = 100 * 1024 * 1024, // 100 MB
    header_bytes: usize = 0,
    header_count: usize = 0,
    total_body_bytes: usize = 0,
    /// When true, the path was allocated via dupe and must be freed on reset/deinit.
    /// When false, path points into `line_buffer` and is valid until reset.
    path_owned: bool = false,
    /// When true, the parser marks the message as complete after headers are
    /// parsed and does not consume any body bytes. The caller is responsible
    /// for reading the body using `content_length` / `chunked` metadata.
    headers_only: bool = false,

    const Self = @This();

    /// Creates a new parser instance.
    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .headers = Headers.init(allocator),
        };
    }

    /// Creates a parser for parsing responses.
    pub fn initResponse(allocator: Allocator) Self {
        var p = init(allocator);
        p.mode = .response;
        p.state = .status_line;
        return p;
    }

    /// Releases all allocated memory.
    pub fn deinit(self: *Self) void {
        self.headers.deinit();
        self.body_buffer.deinit(self.allocator);
        self.line_buffer.deinit(self.allocator);
        if (self.path_owned) {
            if (self.path) |p| self.allocator.free(p);
        }
    }

    /// Finalizes parsing when the underlying stream has reached EOF.
    ///
    /// For HTTP/1.x responses with neither `Content-Length` nor `Transfer-Encoding: chunked`,
    /// the body is delimited by connection close. In that case, reaching EOF means the
    /// message is complete.
    pub fn finishEof(self: *Self) void {
        if (self.state == .body and self.mode == .response and self.content_length == null and !self.chunked) {
            self.state = .complete;
        }
    }

    /// Feeds data to the parser, returning the number of bytes consumed.
    pub fn feed(self: *Self, data: []const u8) !usize {
        var consumed: usize = 0;

        while (consumed < data.len and self.state != .complete and self.state != .err) {
            const remaining = data[consumed..];
            consumed += switch (self.state) {
                .start => self.parseStart(remaining),
                .request_line => try self.parseRequestLine(remaining),
                .status_line => try self.parseStatusLine(remaining),
                .headers => try self.parseHeaders(remaining),
                .body => try self.parseBody(remaining),
                .chunk_size => try self.parseChunkSize(remaining),
                .chunk_data => try self.parseChunkData(remaining),
                .chunk_crlf => try self.parseChunkCrlf(remaining),
                .chunk_trailer => try self.parseChunkTrailer(remaining),
                .complete, .err => break,
            };
        }

        return consumed;
    }

    /// Returns true if parsing is complete.
    pub fn isComplete(self: *const Self) bool {
        return self.state == .complete;
    }

    /// Returns true if parsing encountered an error.
    pub fn isError(self: *const Self) bool {
        return self.state == .err;
    }

    /// Returns the parsed body.
    pub fn getBody(self: *const Self) []const u8 {
        return self.body_buffer.items;
    }

    /// Returns the parsed status.
    pub fn getStatus(self: *const Self) ?Status {
        if (self.status_code) |code| {
            return Status.fromCode(code);
        }
        return null;
    }

    /// Returns the reason for the parser entering the error state.
    pub fn getErrorReason(self: *const Self) ErrorReason {
        return self.error_reason;
    }

    /// Maximum retained buffer capacity after reset. Buffers that grew beyond
    /// this threshold (e.g. due to a single large request) are released to
    /// prevent permanent per-connection memory inflation on keep-alive connections.
    const max_retained_capacity: usize = 64 * 1024;

    /// Resets the parser for reuse.
    pub fn reset(self: *Self) void {
        self.state = .start;
        self.error_reason = .none;
        self.method = null;
        if (self.path_owned) {
            if (self.path) |p| self.allocator.free(p);
        }
        self.path = null;
        self.path_owned = false;
        self.status_code = null;
        self.headers.clear();
        // Release oversized buffers to prevent permanent memory inflation
        // from occasional large requests on long-lived keep-alive connections.
        if (self.body_buffer.capacity > max_retained_capacity) {
            self.body_buffer.deinit(self.allocator);
            self.body_buffer = .empty;
        } else {
            self.body_buffer.clearRetainingCapacity();
        }
        if (self.line_buffer.capacity > max_retained_capacity) {
            self.line_buffer.deinit(self.allocator);
            self.line_buffer = .empty;
        } else {
            self.line_buffer.clearRetainingCapacity();
        }
        self.content_length = null;
        self.chunked = false;
        self.current_chunk_size = 0;
        self.bytes_read = 0;
        self.chunk_crlf_read = 0;
        self.header_bytes = 0;
        self.header_count = 0;
        self.total_body_bytes = 0;
    }

    fn checkLineBufferLimit(self: *Self) !void {
        if (self.line_buffer.items.len > self.max_header_size) {
            self.state = .err;
            self.error_reason = .header_too_large;
            return error.HeaderTooLarge;
        }
    }

    /// Shared CRLF line-reading logic used by all line-oriented parser states.
    /// Returns the completed line and bytes consumed, or null if CRLF not yet received
    /// (in which case `data.len` bytes were buffered and the caller should return that).
    const LineResult = struct { line: []const u8, consumed: usize };
    fn readLine(self: *Self, data: []const u8) !?LineResult {
        if (self.line_buffer.items.len > 0 and self.line_buffer.items[self.line_buffer.items.len - 1] == '\r') {
            if (data.len == 0) return null;
            if (data[0] == '\n') {
                return .{
                    .line = self.line_buffer.items[0 .. self.line_buffer.items.len - 1],
                    .consumed = 1,
                };
            }
        }

        const line_end = mem.indexOf(u8, data, "\r\n") orelse {
            try self.line_buffer.appendSlice(self.allocator, data);
            try self.checkLineBufferLimit();
            return null;
        };

        const line = if (self.line_buffer.items.len > 0) blk: {
            try self.line_buffer.appendSlice(self.allocator, data[0..line_end]);
            break :blk self.line_buffer.items;
        } else data[0..line_end];

        return .{ .line = line, .consumed = line_end + 2 };
    }

    fn bumpHeaderBytes(self: *Self, line_len: usize) !void {
        // Account for CRLF too.
        self.header_bytes += line_len + 2;
        if (self.header_bytes > self.max_header_size) {
            self.state = .err;
            self.error_reason = .header_too_large;
            return error.HeaderTooLarge;
        }
    }

    fn parseStart(self: *Self, data: []const u8) usize {
        if (data.len == 0) return 0;

        if (self.mode == .response) {
            self.state = .status_line;
        } else {
            self.state = .request_line;
        }
        return 0;
    }

    fn parseRequestLine(self: *Self, data: []const u8) !usize {
        const lr = (try self.readLine(data)) orelse return data.len;
        const line = lr.line;

        var parts = mem.splitScalar(u8, line, ' ');

        const method_str = parts.next() orelse {
            self.state = .err;
            self.error_reason = .malformed_request_line;
            return lr.consumed;
        };
        self.method = types.Method.fromString(method_str) orelse .CUSTOM;

        const path = parts.next() orelse {
            self.state = .err;
            self.error_reason = .malformed_request_line;
            return lr.consumed;
        };
        // Always dupe the path. The caller's buffer may be reused for
        // subsequent recv() calls during body parsing, which would corrupt
        // a borrowed path slice.
        self.path = try self.allocator.dupe(u8, path);
        self.path_owned = true;

        const version_str = parts.next() orelse {
            self.state = .err;
            self.error_reason = .malformed_request_line;
            return lr.consumed;
        };
        self.version = types.Version.fromString(version_str) orelse .HTTP_1_1;

        try self.bumpHeaderBytes(line.len);

        self.line_buffer.clearRetainingCapacity();
        self.state = .headers;
        return lr.consumed;
    }

    fn parseStatusLine(self: *Self, data: []const u8) !usize {
        const lr = (try self.readLine(data)) orelse return data.len;
        const line = lr.line;

        var parts = mem.splitScalar(u8, line, ' ');

        const version_str = parts.next() orelse {
            self.state = .err;
            self.error_reason = .malformed_status_line;
            return lr.consumed;
        };
        self.version = types.Version.fromString(version_str) orelse .HTTP_1_1;

        const status_str = parts.next() orelse {
            self.state = .err;
            self.error_reason = .malformed_status_line;
            return lr.consumed;
        };
        self.status_code = std.fmt.parseInt(u16, status_str, 10) catch {
            self.state = .err;
            self.error_reason = .malformed_status_line;
            return lr.consumed;
        };

        try self.bumpHeaderBytes(line.len);

        self.line_buffer.clearRetainingCapacity();
        self.state = .headers;
        return lr.consumed;
    }

    fn parseHeaders(self: *Self, data: []const u8) !usize {
        const lr = (try self.readLine(data)) orelse return data.len;
        const line = lr.line;

        if (line.len == 0) {
            self.line_buffer.clearRetainingCapacity();
            try self.bumpHeaderBytes(0);
            self.determineBodyState();
            return lr.consumed;
        }

        try self.bumpHeaderBytes(line.len);

        if (mem.indexOf(u8, line, ":")) |sep| {
            if (self.header_count >= self.max_headers) {
                self.state = .err;
                self.error_reason = .too_many_headers;
                return error.TooManyHeaders;
            }
            const name = mem.trim(u8, line[0..sep], " \t");
            const value = mem.trim(u8, line[sep + 1 ..], " \t");

            // Reject header injection via embedded CR/LF.
            if (mem.indexOfAny(u8, name, "\r\n") != null or
                mem.indexOfAny(u8, value, "\r\n") != null)
            {
                self.state = .err;
                self.error_reason = .invalid_header;
                return error.InvalidHeader;
            }

            try self.headers.append(name, value);
            self.header_count += 1;

            if (std.ascii.eqlIgnoreCase(name, "content-length")) {
                // RFC 9112 §8.6: reject duplicate Content-Length with different values.
                if (self.content_length) |existing| {
                    const new_cl = std.fmt.parseInt(u64, value, 10) catch null;
                    if (new_cl) |cl| {
                        if (cl != existing) {
                            self.state = .err;
                            self.error_reason = .smuggling_detected;
                            return error.InvalidHeader;
                        }
                    }
                } else {
                    self.content_length = std.fmt.parseInt(u64, value, 10) catch null;
                }
            } else if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
                // Token-list match per RFC 7230 §3.3.1 — prevent "chunkedx" bypass.
                if (containsToken(value, "chunked")) {
                    self.chunked = true;
                }
            }
        }

        self.line_buffer.clearRetainingCapacity();
        return lr.consumed;
    }

    fn determineBodyState(self: *Self) void {
        // RFC 7230 §3.3.3: reject messages with both Content-Length and
        // Transfer-Encoding to prevent request smuggling.
        if (self.chunked and self.content_length != null) {
            self.state = .err;
            self.error_reason = .smuggling_detected;
            return;
        }

        // In headers-only mode the caller will read the body externally
        // (e.g. via a streaming Io.Reader chain). Mark complete now so
        // any unconsumed bytes remain available to the caller.
        if (self.headers_only) {
            self.state = .complete;
            return;
        }

        if (self.chunked) {
            self.state = .chunk_size;
        } else if (self.content_length) |len| {
            if (len > self.max_body_size) {
                self.state = .err;
                self.error_reason = .body_too_large;
                return;
            }
            if (len > 0) {
                // Pre-allocate the body buffer to avoid repeated reallocs
                // during incremental parsing of fixed-length bodies.
                self.body_buffer.ensureTotalCapacity(self.allocator, @intCast(len)) catch {};
                self.state = .body;
            } else {
                self.state = .complete;
            }
        } else if (self.mode == .response) {
            self.state = .body;
        } else {
            self.state = .complete;
        }
    }

    fn parseBody(self: *Self, data: []const u8) !usize {
        if (self.content_length) |len| {
            if (self.bytes_read >= len) {
                self.state = .complete;
                return 0;
            }
            const remaining = len - self.bytes_read;
            const to_read = @min(data.len, @as(usize, @intCast(remaining)));
            try self.body_buffer.appendSlice(self.allocator, data[0..to_read]);
            self.bytes_read += to_read;

            if (self.bytes_read >= len) {
                self.state = .complete;
            }
            return to_read;
        }

        self.total_body_bytes += data.len;
        if (self.total_body_bytes > self.max_body_size) {
            self.state = .err;
            self.error_reason = .body_too_large;
            return error.BodyTooLarge;
        }
        try self.body_buffer.appendSlice(self.allocator, data);
        return data.len;
    }

    fn parseChunkSize(self: *Self, data: []const u8) !usize {
        const lr = (try self.readLine(data)) orelse return data.len;
        const line = lr.line;

        const size_part = if (mem.indexOfScalar(u8, line, ';')) |semi|
            mem.trim(u8, line[0..semi], " \t")
        else
            mem.trim(u8, line, " \t");

        self.current_chunk_size = std.fmt.parseInt(usize, size_part, 16) catch {
            self.state = .err;
            self.error_reason = .malformed_chunk_size;
            return lr.consumed;
        };

        if (self.current_chunk_size > self.max_body_size) {
            self.state = .err;
            self.error_reason = .body_too_large;
            return lr.consumed;
        }

        self.line_buffer.clearRetainingCapacity();
        self.bytes_read = 0;
        self.chunk_crlf_read = 0;

        if (self.current_chunk_size == 0) {
            self.state = .chunk_trailer;
        } else {
            self.state = .chunk_data;
        }

        return lr.consumed;
    }

    fn parseChunkData(self: *Self, data: []const u8) !usize {
        const remaining = self.current_chunk_size - self.bytes_read;
        const to_read = @min(data.len, remaining);

        self.total_body_bytes += to_read;
        if (self.total_body_bytes > self.max_body_size) {
            self.state = .err;
            self.error_reason = .body_too_large;
            return error.BodyTooLarge;
        }

        try self.body_buffer.appendSlice(self.allocator, data[0..to_read]);
        self.bytes_read += to_read;

        if (self.bytes_read >= self.current_chunk_size) {
            self.state = .chunk_crlf;
        }

        return to_read;
    }

    fn parseChunkCrlf(self: *Self, data: []const u8) !usize {
        if (data.len == 0) return 0;

        var consumed: usize = 0;
        while (consumed < data.len and self.chunk_crlf_read < 2) {
            const b = data[consumed];
            switch (self.chunk_crlf_read) {
                0 => if (b != '\r') {
                    self.state = .err;
                    self.error_reason = .invalid_chunk_encoding;
                    return error.InvalidChunkEncoding;
                },
                1 => if (b != '\n') {
                    self.state = .err;
                    self.error_reason = .invalid_chunk_encoding;
                    return error.InvalidChunkEncoding;
                },
                else => {},
            }
            self.chunk_crlf_read += 1;
            consumed += 1;
        }

        if (self.chunk_crlf_read == 2) {
            self.chunk_crlf_read = 0;
            self.state = .chunk_size;
        }

        return consumed;
    }

    fn parseChunkTrailer(self: *Self, data: []const u8) !usize {
        const lr = (try self.readLine(data)) orelse return data.len;

        // Ignore trailer fields but consume them until the terminating empty line.
        if (lr.line.len == 0) {
            self.line_buffer.clearRetainingCapacity();
            self.state = .complete;
            return lr.consumed;
        }

        self.line_buffer.clearRetainingCapacity();
        return lr.consumed;
    }
};

test "Parser request line" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const data = "GET /api/users HTTP/1.1\r\nHost: example.com\r\n\r\n";
    _ = try parser.feed(data);

    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqual(types.Method.GET, parser.method.?);
    try std.testing.expectEqualStrings("/api/users", parser.path.?);
}

test "Parser response" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    const data = "HTTP/1.1 200 OK\r\nContent-Length: 5\r\n\r\nHello";
    _ = try parser.feed(data);

    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqual(@as(?u16, 200), parser.status_code);
    try std.testing.expectEqualStrings("Hello", parser.getBody());
}

test "Parser chunked encoding" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    const data = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\nHello\r\n0\r\n\r\n";
    _ = try parser.feed(data);

    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("Hello", parser.getBody());
}

test "Parser response body by close (finishEof)" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    const data = "HTTP/1.1 200 OK\r\n\r\nHello";
    _ = try parser.feed(data);
    try std.testing.expect(!parser.isComplete());
    parser.finishEof();
    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("Hello", parser.getBody());
}

test "Parser chunked with extension and split CRLF" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    _ = try parser.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    _ = try parser.feed("5;foo=bar\r\nHel");
    _ = try parser.feed("lo\r");
    _ = try parser.feed("\n0\r\n\r\n");

    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("Hello", parser.getBody());
}

test "Parser headers" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const data = "GET / HTTP/1.1\r\nHost: example.com\r\nUser-Agent: test\r\n\r\n";
    _ = try parser.feed(data);

    try std.testing.expectEqualStrings("example.com", parser.headers.get("Host").?);
    try std.testing.expectEqualStrings("test", parser.headers.get("User-Agent").?);
}

test "Parser reset" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    _ = try parser.feed("GET / HTTP/1.1\r\n\r\n");
    try std.testing.expect(parser.isComplete());

    parser.reset();
    try std.testing.expect(!parser.isComplete());
    try std.testing.expect(parser.method == null);
}

test "multi-feed parsing" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Feed the message in several separate calls to exercise incremental parsing.
    _ = try parser.feed("GET /index.html HTTP/1.1\r\n");
    try std.testing.expect(!parser.isComplete());

    _ = try parser.feed("Host: localhost\r\n");
    _ = try parser.feed("Content-Length: 3\r\n");
    _ = try parser.feed("\r\n");
    try std.testing.expect(!parser.isComplete());

    _ = try parser.feed("ab");
    try std.testing.expect(!parser.isComplete());

    _ = try parser.feed("c");
    try std.testing.expect(parser.isComplete());

    try std.testing.expectEqual(types.Method.GET, parser.method.?);
    try std.testing.expectEqualStrings("/index.html", parser.path.?);
    try std.testing.expectEqualStrings("localhost", parser.headers.get("Host").?);
    try std.testing.expectEqualStrings("abc", parser.getBody());
}

test "header size limit returns HeaderTooLarge" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Build a request with a header value exceeding 8192 bytes.
    const prefix = "GET / HTTP/1.1\r\nX-Big: ";
    const suffix = "\r\n\r\n";
    const value_len = 8200;
    var buf: [prefix.len + value_len + suffix.len]u8 = undefined;
    @memcpy(buf[0..prefix.len], prefix);
    @memset(buf[prefix.len..][0..value_len], 'A');
    @memcpy(buf[prefix.len + value_len ..], suffix);

    const result = parser.feed(&buf);
    try std.testing.expectError(error.HeaderTooLarge, result);
    try std.testing.expect(parser.isError());
}

test "too many headers returns TooManyHeaders" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Feed the request line first.
    _ = try parser.feed("GET / HTTP/1.1\r\n");

    // Feed 100 headers (the maximum allowed).
    for (0..100) |i| {
        var hdr_buf: [64]u8 = undefined;
        const hdr = std.fmt.bufPrint(&hdr_buf, "X-H-{d}: val\r\n", .{i}) catch unreachable;
        _ = try parser.feed(hdr);
    }

    // The 101st header should trigger TooManyHeaders.
    const result = parser.feed("X-Overflow: boom\r\n");
    try std.testing.expectError(error.TooManyHeaders, result);
    try std.testing.expect(parser.isError());
}

test "invalid chunk size puts parser in error state" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    _ = try parser.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    // Feed a non-hex chunk size.
    _ = try parser.feed("xyz\r\n");

    try std.testing.expect(parser.isError());
}

test "missing CRLF after chunk data returns InvalidChunkEncoding" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    _ = try parser.feed("HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n");
    _ = try parser.feed("5\r\nHello");
    // After 5 bytes of chunk data the parser expects \r\n but gets "XX".
    const result = parser.feed("XX");
    try std.testing.expectError(error.InvalidChunkEncoding, result);
    try std.testing.expect(parser.isError());
}

test "empty request line parts puts parser in error state" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Request line with method only, missing path and version.
    _ = try parser.feed("GET\r\n");

    try std.testing.expect(parser.isError());
}

test "zero content-length completes with empty body" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    _ = try parser.feed("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");

    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("", parser.getBody());
}

test "multiple chunks of varying sizes" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    const data =
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n" ++
        "3\r\nabc\r\n" ++
        "A\r\n0123456789\r\n" ++
        "1\r\nZ\r\n" ++
        "0\r\n\r\n";
    _ = try parser.feed(data);

    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("abc0123456789Z", parser.getBody());
}

test "split headers across feeds parses correctly" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Split in the middle of the header name "Content-Length".
    _ = try parser.feed("GET / HTTP/1.1\r\nCont");
    _ = try parser.feed("ent-Length: 4\r\n\r\ndata");

    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("4", parser.headers.get("Content-Length").?);
    try std.testing.expectEqualStrings("data", parser.getBody());
}

test "response with no body signals uses finishEof" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();

    // Response with neither Content-Length nor Transfer-Encoding.
    _ = try parser.feed("HTTP/1.1 200 OK\r\n\r\n");
    _ = try parser.feed("some body ");
    _ = try parser.feed("content here");

    try std.testing.expect(!parser.isComplete());
    parser.finishEof();
    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("some body content here", parser.getBody());
}

test "reject request smuggling: CL + TE both present" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    _ = try parser.feed("POST / HTTP/1.1\r\nContent-Length: 5\r\nTransfer-Encoding: chunked\r\n\r\n");

    // Parser should be in error state due to CL+TE conflict.
    try std.testing.expect(parser.isError());
}

test "reject body too large via Content-Length" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Set a small max for testing.
    parser.max_body_size = 16;

    _ = try parser.feed("POST / HTTP/1.1\r\nContent-Length: 100\r\n\r\n");

    // Parser should reject because 100 > 16.
    try std.testing.expect(parser.isError());
}

test "reject body too large during chunked transfer" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    parser.max_body_size = 8;

    // Chunk size 0x10 = 16 exceeds max_body_size of 8.
    _ = try parser.feed("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n10\r\nabcdefghijklmnop\r\n0\r\n\r\n");
    try std.testing.expect(parser.isError());
}

test "reject body too large accumulated across chunks" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    parser.max_body_size = 8;

    // Two small chunks (4+5=9) that together exceed max_body_size of 8.
    const result = parser.feed("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n4\r\nabcd\r\n5\r\nefghi\r\n0\r\n\r\n");
    try std.testing.expectError(error.BodyTooLarge, result);
}

test "reject header injection via CR in value" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const result = parser.feed("GET / HTTP/1.1\r\nX-Bad: val\rue\r\n\r\n");
    try std.testing.expectError(error.InvalidHeader, result);
}

test "reject header injection via LF in name" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const result = parser.feed("GET / HTTP/1.1\r\nX-Ba\nd: value\r\n\r\n");
    try std.testing.expectError(error.InvalidHeader, result);
}

test "headers_only mode completes after headers" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();
    parser.headers_only = true;

    const data = "GET /stream HTTP/1.1\r\nContent-Length: 1000\r\n\r\nBODY_DATA";
    const consumed = try parser.feed(data);
    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqual(@as(?u64, 1000), parser.content_length);
    try std.testing.expectEqualStrings("", parser.getBody());
    // Unconsumed body bytes remain after the consumed headers.
    try std.testing.expect(consumed < data.len);
}

test "headers_only mode with chunked encoding" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();
    parser.headers_only = true;

    _ = try parser.feed("POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n");
    try std.testing.expect(parser.isComplete());
    try std.testing.expect(parser.chunked);
    try std.testing.expectEqualStrings("", parser.getBody());
}

test "reject duplicate Content-Length with different values" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    const result = parser.feed("GET / HTTP/1.1\r\nContent-Length: 10\r\nContent-Length: 20\r\n\r\n");
    try std.testing.expectError(error.InvalidHeader, result);
    try std.testing.expectEqual(ErrorReason.smuggling_detected, parser.getErrorReason());
}

test "allow duplicate Content-Length with same value" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    _ = try parser.feed("GET / HTTP/1.1\r\nContent-Length: 10\r\nContent-Length: 10\r\n\r\n0123456789");
    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqual(@as(?u64, 10), parser.content_length);
}

test "path borrowing safe when request line spans feeds" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    // Feed request line in two parts so path comes from line_buffer and gets duped.
    _ = try parser.feed("GET /my/pa");
    _ = try parser.feed("th HTTP/1.1\r\nHost: example.com\r\n\r\n");
    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("/my/path", parser.path.?);
    try std.testing.expect(parser.path_owned);
}

test "path borrowing from data when request line fits in one feed" {
    const allocator = std.testing.allocator;
    var parser = Parser.init(allocator);
    defer parser.deinit();

    _ = try parser.feed("GET /single HTTP/1.1\r\nHost: example.com\r\n\r\n");
    try std.testing.expect(parser.isComplete());
    try std.testing.expectEqualStrings("/single", parser.path.?);
    try std.testing.expect(parser.path_owned);
}

test "response headers parse when CRLF splits across feeds" {
    const allocator = std.testing.allocator;
    var parser = Parser.initResponse(allocator);
    defer parser.deinit();
    parser.headers_only = true;

    _ = try parser.feed("HTTP/1.1 200 OK\r\nContent-Type: application/json\r");
    const second = "\nTransfer-Encoding: chunked\r\n\r\n5\r\nhello\r\n";
    const consumed = try parser.feed(second);

    try std.testing.expect(parser.isComplete());
    try std.testing.expect(consumed > 0);
    try std.testing.expect(consumed < second.len);
    try std.testing.expectEqual(@as(?u16, 200), parser.status_code);
    try std.testing.expectEqualStrings("application/json", parser.headers.get("Content-Type").?);
    try std.testing.expect(parser.chunked);
}
