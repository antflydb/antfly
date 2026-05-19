//! HTTP Response Representation for httpx.zig
//!
//! Provides the Response structure and ResponseBuilder for handling
//! HTTP responses. Features include:
//!
//! - Status code and reason phrase management
//! - Header access with common helpers
//! - Body handling with JSON parsing support
//! - Response building for servers

const std = @import("std");
const ant_json = @import("antfly-json");
const serializeToSlice = @import("../util/array_list_writer.zig").serializeToSlice;
const mem = std.mem;
const Allocator = mem.Allocator;

const types = @import("types.zig");
const Headers = @import("headers.zig").Headers;
const HeaderName = @import("headers.zig").HeaderName;
const Status = @import("status.zig").Status;
const Json = @import("../util/json.zig").Json;
/// HTTP response representation.
pub const Response = struct {
    allocator: Allocator,
    version: types.Version = .HTTP_1_1,
    status: Status,
    headers: Headers,
    body: ?[]const u8 = null,
    body_owned: bool = false,

    const Self = @This();

    /// Creates a new response with the given status code.
    pub fn init(allocator: Allocator, status_code: u16) Self {
        return .{
            .allocator = allocator,
            .status = Status.fromCode(status_code),
            .headers = Headers.init(allocator),
        };
    }

    /// Releases all allocated memory.
    pub fn deinit(self: *Self) void {
        self.headers.deinit();
        if (self.body_owned) {
            if (self.body) |b| {
                self.allocator.free(b);
            }
        }
    }

    /// Returns true if the response indicates success (2xx).
    pub fn ok(self: *const Self) bool {
        return self.status.isSuccess();
    }

    /// Returns true if the response is a redirect (3xx).
    pub fn isRedirect(self: *const Self) bool {
        return self.status.isRedirect();
    }

    /// Returns true if the response is an error (4xx or 5xx).
    pub fn isError(self: *const Self) bool {
        return self.status.isError();
    }

    /// Returns the response body as text.
    pub fn text(self: *const Self) ?[]const u8 {
        return self.body;
    }

    /// Parses the response body as JSON into the given type.
    /// Caller must call `.deinit()` on the returned `Parsed(T)` to free the arena.
    pub fn json(self: *const Self, comptime T: type) !ant_json.Parsed(T) {
        const body = self.body orelse return error.NoBody;
        return ant_json.parseFromSlice(T, self.allocator, body, .{});
    }

    /// Returns the Location header value for redirects.
    pub fn location(self: *const Self) ?[]const u8 {
        return self.headers.get(HeaderName.LOCATION);
    }

    /// Returns the Content-Type header value.
    pub fn contentType(self: *const Self) ?[]const u8 {
        return self.headers.get(HeaderName.CONTENT_TYPE);
    }

    /// Returns the Content-Length header value.
    pub fn contentLength(self: *const Self) ?u64 {
        return self.headers.getContentLength();
    }

    /// Returns true if the response uses chunked transfer encoding.
    pub fn isChunked(self: *const Self) bool {
        return self.headers.isChunked();
    }

    /// Returns a specific header value.
    pub fn header(self: *const Self, name: []const u8) ?[]const u8 {
        return self.headers.get(name);
    }

    /// Creates a redirect response and sets the `Location` header.
    pub fn redirect(allocator: Allocator, status_code: u16, redirect_to: []const u8) !Self {
        var resp = Self.init(allocator, status_code);
        try resp.headers.set(HeaderName.LOCATION, redirect_to);
        return resp;
    }

    /// Creates a plain-text response with Content-Type and Content-Length set.
    pub fn fromText(allocator: Allocator, status_code: u16, text_body: []const u8) !Self {
        var resp = Self.init(allocator, status_code);
        errdefer resp.deinit();
        try resp.headers.set(HeaderName.CONTENT_TYPE, "text/plain; charset=utf-8");
        resp.body = try allocator.dupe(u8, text_body);
        resp.body_owned = true;
        try resp.headers.setContentLength(text_body.len);
        return resp;
    }

    /// Creates a JSON response from a serializable value.
    pub fn fromJson(allocator: Allocator, status_code: u16, value: anytype) !Self {
        var resp = Self.init(allocator, status_code);
        errdefer resp.deinit();
        try resp.headers.set(HeaderName.CONTENT_TYPE, "application/json");
        resp.body = try Json.stringify(allocator, value);
        resp.body_owned = true;

        if (resp.body) |b| {
            try resp.headers.setContentLength(b.len);
        }
        return resp;
    }

    /// Serializes the response to HTTP/1.1 wire format.
    pub fn serialize(self: *const Self, writer: anytype) !void {
        try writer.print("{s} {d} {s}\r\n", .{
            self.version.toString(),
            self.status.code,
            self.status.phrase,
        });

        try self.headers.serialize(writer);
        try writer.writeAll("\r\n");

        if (self.body) |body| {
            try writer.writeAll(body);
        }
    }

    /// Serializes to an allocated buffer.
    pub fn toSlice(self: *const Self, allocator: Allocator) ![]u8 {
        return serializeToSlice(allocator, self);
    }
};

/// Fluent builder for constructing responses (server-side).
pub const ResponseBuilder = struct {
    allocator: Allocator,
    status_code: u16 = 200,
    headers: Headers,
    body_data: ?[]const u8 = null,
    body_owned: bool = false,

    const Self = @This();

    /// Creates a new response builder.
    pub fn init(allocator: Allocator) Self {
        return .{
            .allocator = allocator,
            .headers = Headers.init(allocator),
        };
    }

    /// Releases builder resources.
    pub fn deinit(self: *Self) void {
        self.freeOwnedBody();
        self.headers.deinit();
    }

    fn freeOwnedBody(self: *Self) void {
        if (self.body_owned) {
            if (self.body_data) |b| {
                self.allocator.free(b);
            }
            self.body_owned = false;
            self.body_data = null;
        }
    }

    /// Sets the status code.
    pub fn status(self: *Self, code: u16) *Self {
        self.status_code = code;
        return self;
    }

    /// Sets a response header (replaces any existing value for the name).
    pub fn header(self: *Self, name: []const u8, value: []const u8) !*Self {
        try self.headers.set(name, value);
        return self;
    }

    /// Sets the response body.
    pub fn body(self: *Self, data: []const u8) *Self {
        self.freeOwnedBody();
        self.body_data = data;
        return self;
    }

    /// Sets a JSON body with appropriate Content-Type.
    pub fn json(self: *Self, value: anytype) !*Self {
        _ = try self.header(HeaderName.CONTENT_TYPE, "application/json");
        self.freeOwnedBody();
        const serialized = try Json.stringify(self.allocator, value);
        self.body_data = serialized;
        self.body_owned = true;
        return self;
    }

    /// Sets an HTML body with appropriate Content-Type.
    pub fn html(self: *Self, content: []const u8) !*Self {
        _ = try self.header(HeaderName.CONTENT_TYPE, "text/html; charset=utf-8");
        self.freeOwnedBody();
        self.body_data = content;
        return self;
    }

    /// Sets a plain text body with appropriate Content-Type.
    pub fn text(self: *Self, content: []const u8) !*Self {
        _ = try self.header(HeaderName.CONTENT_TYPE, "text/plain; charset=utf-8");
        self.freeOwnedBody();
        self.body_data = content;
        return self;
    }

    /// Builds the final response.
    pub fn build(self: *Self) !Response {
        var response = Response.init(self.allocator, self.status_code);
        errdefer response.deinit();

        // Transfer header ownership directly instead of copying each entry.
        response.headers.deinit();
        response.headers.entries = self.headers.entries;
        self.headers.entries = .empty;

        if (self.body_data) |b| {
            const body_len = b.len;
            if (self.body_owned) {
                // Transfer ownership instead of copying.
                response.body = b;
                self.body_data = null;
                self.body_owned = false;
            } else {
                response.body = try self.allocator.dupe(u8, b);
            }
            response.body_owned = true;

            if (!response.headers.isChunked()) {
                try response.headers.setContentLength(body_len);
            }
        }

        return response;
    }
};

test "Response initialization" {
    const allocator = std.testing.allocator;
    var response = Response.init(allocator, 200);
    defer response.deinit();

    try std.testing.expect(response.ok());
    try std.testing.expectEqual(@as(u16, 200), response.status.code);
}

test "Response status checks" {
    const allocator = std.testing.allocator;

    var ok = Response.init(allocator, 200);
    defer ok.deinit();
    try std.testing.expect(ok.ok());
    try std.testing.expect(!ok.isError());

    var redirect = Response.init(allocator, 301);
    defer redirect.deinit();
    try std.testing.expect(redirect.isRedirect());

    var error_resp = Response.init(allocator, 404);
    defer error_resp.deinit();
    try std.testing.expect(error_resp.isError());
}

test "ResponseBuilder" {
    const allocator = std.testing.allocator;
    var builder = ResponseBuilder.init(allocator);
    defer builder.deinit();

    _ = builder.status(201);
    _ = try builder.header("X-Custom", "value");
    _ = builder.body("test content");

    var response = try builder.build();
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 201), response.status.code);
    try std.testing.expect(response.body != null);
}

test "Response serialization" {
    const allocator = std.testing.allocator;
    var response = Response.init(allocator, 200);
    defer response.deinit();

    const serialized = try response.toSlice(allocator);
    defer allocator.free(serialized);

    try std.testing.expect(mem.startsWith(u8, serialized, "HTTP/1.1 200 OK\r\n"));
}

test "Response serialization includes headers and body" {
    const allocator = std.testing.allocator;
    var response = Response.init(allocator, 200);
    defer response.deinit();

    try response.headers.set("Content-Type", "text/plain");
    response.body = "Hello, world!";

    const serialized = try response.toSlice(allocator);
    defer allocator.free(serialized);

    try std.testing.expect(mem.startsWith(u8, serialized, "HTTP/1.1 200 OK\r\n"));
    try std.testing.expect(mem.indexOf(u8, serialized, "Content-Type: text/plain\r\n") != null);
    try std.testing.expect(mem.endsWith(u8, serialized, "\r\n\r\nHello, world!"));
}

test "Response redirect constructor" {
    const allocator = std.testing.allocator;
    var response = try Response.redirect(allocator, 302, "https://example.com/new");
    defer response.deinit();

    try std.testing.expectEqual(@as(u16, 302), response.status.code);
    try std.testing.expectEqualStrings("https://example.com/new", response.location().?);
}

test "Response fromText and fromJson constructors" {
    const allocator = std.testing.allocator;

    var text_resp = try Response.fromText(allocator, 200, "hello");
    defer text_resp.deinit();
    try std.testing.expectEqualStrings("text/plain; charset=utf-8", text_resp.contentType().?);
    try std.testing.expectEqualStrings("hello", text_resp.text().?);

    var json_resp = try Response.fromJson(allocator, 201, .{ .ok = true });
    defer json_resp.deinit();
    try std.testing.expectEqualStrings("application/json", json_resp.contentType().?);
    try std.testing.expect(json_resp.text() != null);
}

test "ResponseBuilder json replaces previous owned body without leak" {
    const allocator = std.testing.allocator;
    var builder = ResponseBuilder.init(allocator);
    defer builder.deinit();

    // Set a JSON body, then replace it. The first allocation must be freed.
    _ = try builder.json(.{ .first = true });
    _ = try builder.json(.{ .second = true });

    var response = try builder.build();
    defer response.deinit();

    // The built response should contain the second JSON body.
    try std.testing.expect(response.body != null);
    try std.testing.expect(mem.indexOf(u8, response.body.?, "second") != null);
}

test "ResponseBuilder body clears owned json body without leak" {
    const allocator = std.testing.allocator;
    var builder = ResponseBuilder.init(allocator);
    defer builder.deinit();

    // Set a JSON body (owned), then replace with a plain body (not owned).
    _ = try builder.json(.{ .allocated = true });
    _ = builder.body("plain text");

    var response = try builder.build();
    defer response.deinit();

    try std.testing.expectEqualStrings("plain text", response.body.?);
}
