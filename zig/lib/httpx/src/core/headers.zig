//! HTTP Headers Implementation for httpx.zig
//!
//! Provides a high-performance, case-insensitive HTTP header storage with
//! multi-value support per RFC 7230. Features include:
//!
//! - Case-insensitive header name lookups
//! - Multiple values per header name (e.g., Set-Cookie)
//! - Efficient serialization for wire format
//! - Common header name constants for compile-time optimization
//! - Memory-safe ownership model with automatic cleanup

const std = @import("std");
const array_list_writer_mod = @import("../util/array_list_writer.zig");
const serializeToSlice = array_list_writer_mod.serializeToSlice;
const mem = std.mem;
const Allocator = mem.Allocator;
const types = @import("types.zig");

/// Standard HTTP header name constants.
/// Using these constants enables compile-time string interning.
pub const HeaderName = struct {
    pub const ACCEPT = "Accept";
    pub const ACCEPT_CHARSET = "Accept-Charset";
    pub const ACCEPT_ENCODING = "Accept-Encoding";
    pub const ACCEPT_LANGUAGE = "Accept-Language";
    pub const AUTHORIZATION = "Authorization";
    pub const CACHE_CONTROL = "Cache-Control";
    pub const CONNECTION = "Connection";
    pub const CONTENT_DISPOSITION = "Content-Disposition";
    pub const CONTENT_ENCODING = "Content-Encoding";
    pub const CONTENT_LENGTH = "Content-Length";
    pub const CONTENT_TYPE = "Content-Type";
    pub const COOKIE = "Cookie";
    pub const DATE = "Date";
    pub const ETAG = "ETag";
    pub const EXPECT = "Expect";
    pub const EXPIRES = "Expires";
    pub const HOST = "Host";
    pub const IF_MATCH = "If-Match";
    pub const IF_MODIFIED_SINCE = "If-Modified-Since";
    pub const IF_NONE_MATCH = "If-None-Match";
    pub const LAST_MODIFIED = "Last-Modified";
    pub const LOCATION = "Location";
    pub const ORIGIN = "Origin";
    pub const PRAGMA = "Pragma";
    pub const PROXY_AUTHORIZATION = "Proxy-Authorization";
    pub const RANGE = "Range";
    pub const REFERER = "Referer";
    pub const RETRY_AFTER = "Retry-After";
    pub const SERVER = "Server";
    pub const SET_COOKIE = "Set-Cookie";
    pub const STRICT_TRANSPORT_SECURITY = "Strict-Transport-Security";
    pub const TRANSFER_ENCODING = "Transfer-Encoding";
    pub const UPGRADE = "Upgrade";
    pub const USER_AGENT = "User-Agent";
    pub const VARY = "Vary";
    pub const WWW_AUTHENTICATE = "WWW-Authenticate";
    pub const X_CONTENT_TYPE_OPTIONS = "X-Content-Type-Options";
    pub const X_FRAME_OPTIONS = "X-Frame-Options";
    pub const X_XSS_PROTECTION = "X-XSS-Protection";
    pub const ALLOW = "Allow";
    pub const TRAILER = "Trailer";
    pub const HTTP2_SETTINGS = "HTTP2-Settings";
    pub const ACCESS_CONTROL_ALLOW_ORIGIN = "Access-Control-Allow-Origin";
    pub const ACCESS_CONTROL_ALLOW_METHODS = "Access-Control-Allow-Methods";
    pub const ACCESS_CONTROL_ALLOW_HEADERS = "Access-Control-Allow-Headers";
    pub const ACCESS_CONTROL_EXPOSE_HEADERS = "Access-Control-Expose-Headers";
    pub const ACCESS_CONTROL_ALLOW_CREDENTIALS = "Access-Control-Allow-Credentials";
    pub const ACCESS_CONTROL_MAX_AGE = "Access-Control-Max-Age";
    pub const REFERRER_POLICY = "Referrer-Policy";
    pub const X_REQUEST_ID = "X-Request-ID";
};

/// Represents a single HTTP header entry.
pub const Header = struct {
    name: []const u8,
    value: []const u8,
    owned: bool = false,
};

/// HTTP headers collection with case-insensitive lookups.
pub const Headers = struct {
    allocator: Allocator,
    entries: std.ArrayListUnmanaged(Header) = .empty,

    const Self = @This();

    /// Creates a new empty Headers instance.
    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Releases all allocated memory.
    pub fn deinit(self: *Self) void {
        for (self.entries.items) |entry| {
            if (entry.owned) {
                self.allocator.free(entry.name);
                self.allocator.free(entry.value);
            }
        }
        self.entries.deinit(self.allocator);
    }

    /// Appends a header, duplicating both name and value.
    /// Returns error.HeaderContainsCrLf if the name or value contains CR or LF.
    pub fn append(self: *Self, name: []const u8, value: []const u8) !void {
        if (containsCrLf(name) or containsCrLf(value)) return error.HeaderContainsCrLf;
        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);
        const owned_value = try self.allocator.dupe(u8, value);
        try self.entries.append(self.allocator, .{
            .name = owned_name,
            .value = owned_value,
            .owned = true,
        });
    }

    /// Appends a header without copying. The caller must ensure the name and
    /// value slices outlive this Headers instance. Used on hot paths where the
    /// source lifetime is guaranteed (e.g. parser → request in the server loop).
    pub fn appendBorrowed(self: *Self, name: []const u8, value: []const u8) !void {
        if (containsCrLf(name) or containsCrLf(value)) return error.HeaderContainsCrLf;
        try self.entries.append(self.allocator, .{
            .name = name,
            .value = value,
            .owned = false,
        });
    }

    /// Sets a header, replacing any existing values with the same name.
    /// Optimized: updates value in-place when exactly one match exists,
    /// avoiding a full scan + realloc for the common single-valued case.
    pub fn set(self: *Self, name: []const u8, value: []const u8) !void {
        if (containsCrLf(name) or containsCrLf(value)) return error.HeaderContainsCrLf;

        // Find first match and check if there are duplicates.
        var first_idx: ?usize = null;
        var has_duplicates = false;
        for (self.entries.items, 0..) |entry, i| {
            if (eqlIgnoreCase(entry.name, name)) {
                if (first_idx == null) {
                    first_idx = i;
                } else {
                    has_duplicates = true;
                    break;
                }
            }
        }

        if (first_idx) |idx| {
            if (!has_duplicates) {
                // Fast path: single existing entry — update value in-place.
                const owned_value = try self.allocator.dupe(u8, value);
                errdefer self.allocator.free(owned_value);
                const entry = &self.entries.items[idx];
                if (entry.owned) {
                    self.allocator.free(entry.value);
                    // Keep the existing owned name.
                } else {
                    // Upgrade borrowed entry to owned.
                    entry.name = try self.allocator.dupe(u8, entry.name);
                    entry.owned = true;
                }
                entry.value = owned_value;
                return;
            }
            // Slow path: multiple entries — remove all and re-add.
            self.removeAll(name);
        }

        try self.append(name, value);
    }

    /// Retrieves the first value for a header name (case-insensitive).
    pub fn get(self: *const Self, name: []const u8) ?[]const u8 {
        for (self.entries.items) |entry| {
            if (eqlIgnoreCase(entry.name, name)) return entry.value;
        }
        return null;
    }

    /// Returns all values for a header name.
    pub fn getAll(self: *const Self, name: []const u8, allocator: Allocator) ![][]const u8 {
        var values = std.ArrayListUnmanaged([]const u8).empty;
        for (self.entries.items) |entry| {
            if (eqlIgnoreCase(entry.name, name)) {
                try values.append(allocator, entry.value);
            }
        }
        return values.toOwnedSlice(allocator);
    }

    /// Returns true if the header exists.
    pub fn contains(self: *const Self, name: []const u8) bool {
        return self.get(name) != null;
    }

    /// Removes the first occurrence of a header.
    pub fn remove(self: *Self, name: []const u8) bool {
        for (self.entries.items, 0..) |entry, i| {
            if (eqlIgnoreCase(entry.name, name)) {
                if (entry.owned) {
                    self.allocator.free(entry.name);
                    self.allocator.free(entry.value);
                }
                _ = self.entries.orderedRemove(i);
                return true;
            }
        }
        return false;
    }

    /// Removes all occurrences of a header, preserving insertion order.
    /// Uses two-pointer compaction for O(N) instead of O(N*K) with orderedRemove.
    pub fn removeAll(self: *Self, name: []const u8) void {
        var write: usize = 0;
        for (self.entries.items) |entry| {
            if (eqlIgnoreCase(entry.name, name)) {
                if (entry.owned) {
                    self.allocator.free(entry.name);
                    self.allocator.free(entry.value);
                }
            } else {
                self.entries.items[write] = entry;
                write += 1;
            }
        }
        self.entries.items.len = write;
    }

    /// Returns the number of headers.
    pub fn count(self: *const Self) usize {
        return self.entries.items.len;
    }

    /// Returns an iterator over all headers.
    pub fn iterator(self: *const Self) []const Header {
        return self.entries.items;
    }

    /// Clears all headers.
    pub fn clear(self: *Self) void {
        for (self.entries.items) |entry| {
            if (entry.owned) {
                self.allocator.free(entry.name);
                self.allocator.free(entry.value);
            }
        }
        self.entries.clearRetainingCapacity();
    }

    /// Creates a deep copy of the headers.
    pub fn clone(self: *const Self, allocator: Allocator) !Headers {
        var new_headers = Headers.init(allocator);
        for (self.entries.items) |entry| {
            try new_headers.append(entry.name, entry.value);
        }
        return new_headers;
    }

    /// Sets the Content-Length header from a numeric value.
    pub fn setContentLength(self: *Self, len: usize) !void {
        var buf: [32]u8 = undefined;
        const str = std.fmt.bufPrint(&buf, "{d}", .{len}) catch unreachable;
        try self.set(HeaderName.CONTENT_LENGTH, str);
    }

    /// Parses Content-Length header value.
    pub fn getContentLength(self: *const Self) ?u64 {
        const value = self.get(HeaderName.CONTENT_LENGTH) orelse return null;
        return std.fmt.parseInt(u64, value, 10) catch null;
    }

    /// Returns true if Transfer-Encoding includes chunked.
    /// Uses token-list parsing per RFC 7230 §3.3.1.
    pub fn isChunked(self: *const Self) bool {
        const value = self.get(HeaderName.TRANSFER_ENCODING) orelse return false;
        return containsToken(value, "chunked");
    }

    /// Determines if connection should be kept alive based on headers and version.
    /// Parses the Connection header as a comma-separated token list per RFC 7230 §6.1.
    pub fn isKeepAlive(self: *const Self, version: types.Version) bool {
        const conn = self.get(HeaderName.CONNECTION);
        if (conn) |c| {
            if (containsToken(c, "close")) return false;
            if (containsToken(c, "keep-alive")) return true;
        }
        return version == .HTTP_1_1 or version == .HTTP_2 or version == .HTTP_3;
    }

    /// Serializes headers to HTTP wire format.
    /// CRLF injection is prevented at append/set time, so serialize is safe.
    pub fn serialize(self: *const Self, writer: anytype) !void {
        for (self.entries.items) |entry| {
            try writer.writeAll(entry.name);
            try writer.writeAll(": ");
            try writer.writeAll(entry.value);
            try writer.writeAll("\r\n");
        }
    }

    /// Serializes headers to an allocated string.
    pub fn toSlice(self: *const Self, allocator: Allocator) ![]u8 {
        return serializeToSlice(allocator, self);
    }
};

/// Case-insensitive string comparison for ASCII.
const eqlIgnoreCase = std.ascii.eqlIgnoreCase;

/// Returns true if `header_value` contains `token` as a comma-separated,
/// whitespace-trimmed, case-insensitive token per RFC 7230 §3.2.6.
pub fn containsToken(header_value: []const u8, token: []const u8) bool {
    var it = mem.splitScalar(u8, header_value, ',');
    while (it.next()) |part| {
        const trimmed = mem.trim(u8, part, " \t");
        if (eqlIgnoreCase(trimmed, token)) return true;
    }
    return false;
}

pub fn containsCrLf(s: []const u8) bool {
    return mem.indexOfScalar(u8, s, '\r') != null or mem.indexOfScalar(u8, s, '\n') != null;
}

test "Headers basic operations" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("Content-Type", "application/json");
    try std.testing.expectEqualStrings("application/json", headers.get("Content-Type").?);
}

test "Headers case insensitivity" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("Content-Type", "text/html");
    try std.testing.expectEqualStrings("text/html", headers.get("content-type").?);
    try std.testing.expectEqualStrings("text/html", headers.get("CONTENT-TYPE").?);
}

test "Headers set replaces existing" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("X-Test", "value1");
    try headers.set("X-Test", "value2");
    try std.testing.expectEqualStrings("value2", headers.get("X-Test").?);
    try std.testing.expectEqual(@as(usize, 1), headers.count());
}

test "Headers multiple values" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("Set-Cookie", "cookie1=value1");
    try headers.append("Set-Cookie", "cookie2=value2");
    try std.testing.expectEqual(@as(usize, 2), headers.count());
}

test "Headers Content-Length parsing" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.set("Content-Length", "12345");
    try std.testing.expectEqual(@as(u64, 12345), headers.getContentLength().?);
}

test "Headers serialize handles values exceeding 4096 bytes" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    // Create a header value larger than the old 4096-byte print buffer.
    const large_value = try allocator.alloc(u8, 8192);
    defer allocator.free(large_value);
    @memset(large_value, 'x');

    try headers.set("X-Large", large_value);

    // Serialize to an in-memory buffer to verify correctness.
    var out = std.ArrayListUnmanaged(u8).empty;
    defer out.deinit(allocator);
    const writer = array_list_writer_mod.arrayListWriter(&out, allocator);

    try headers.serialize(writer);

    // Should contain the full header line.
    try std.testing.expect(out.items.len > 8192);
    try std.testing.expect(std.mem.startsWith(u8, out.items, "X-Large: "));
    try std.testing.expect(std.mem.endsWith(u8, out.items, "\r\n"));
}

test "Headers keep-alive detection" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try std.testing.expect(headers.isKeepAlive(.HTTP_1_1));
    try std.testing.expect(!headers.isKeepAlive(.HTTP_1_0));

    try headers.set("Connection", "keep-alive");
    try std.testing.expect(headers.isKeepAlive(.HTTP_1_0));
}

test "Headers getAll returns all values" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("Set-Cookie", "a=1");
    try headers.append("Set-Cookie", "b=2");
    try headers.append("Set-Cookie", "c=3");

    const values = try headers.getAll("Set-Cookie", allocator);
    defer allocator.free(values);

    try std.testing.expectEqual(@as(usize, 3), values.len);
    try std.testing.expectEqualStrings("a=1", values[0]);
    try std.testing.expectEqualStrings("b=2", values[1]);
    try std.testing.expectEqualStrings("c=3", values[2]);
}

test "Headers remove first occurrence" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("X-Test", "first");
    try headers.append("X-Test", "second");
    try std.testing.expect(headers.remove("X-Test"));
    try std.testing.expectEqual(@as(usize, 1), headers.count());
    try std.testing.expectEqualStrings("second", headers.get("X-Test").?);
}

test "Headers removeAll removes all occurrences" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("X-Multi", "a");
    try headers.append("Keep-Me", "yes");
    try headers.append("X-Multi", "b");
    try headers.append("X-Multi", "c");

    headers.removeAll("X-Multi");
    try std.testing.expectEqual(@as(usize, 1), headers.count());
    try std.testing.expectEqualStrings("yes", headers.get("Keep-Me").?);
    try std.testing.expect(headers.get("X-Multi") == null);
}

test "Headers clone creates independent copy" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("Content-Type", "text/html");
    try headers.append("X-Custom", "value");

    var cloned = try headers.clone(allocator);
    defer cloned.deinit();

    // Modify original — clone should be unaffected.
    try headers.set("Content-Type", "application/json");
    try std.testing.expectEqualStrings("text/html", cloned.get("Content-Type").?);
    try std.testing.expectEqual(@as(usize, 2), cloned.count());
}

test "Headers contains" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("Authorization", "Bearer token");
    try std.testing.expect(headers.contains("authorization"));
    try std.testing.expect(!headers.contains("X-Missing"));
}

test "Headers clear removes all entries" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("A", "1");
    try headers.append("B", "2");
    headers.clear();
    try std.testing.expectEqual(@as(usize, 0), headers.count());
}

test "Headers appendBorrowed does not own memory" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    // Static strings — appendBorrowed should not copy.
    try headers.appendBorrowed("Host", "example.com");
    try std.testing.expectEqualStrings("example.com", headers.get("Host").?);
    try std.testing.expectEqual(@as(usize, 1), headers.count());
    // Verify the entry is not owned.
    try std.testing.expect(!headers.entries.items[0].owned);
}

test "Headers set slow path with multiple duplicates" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.append("X-Dup", "a");
    try headers.append("X-Dup", "b");
    try headers.append("X-Dup", "c");
    try std.testing.expectEqual(@as(usize, 3), headers.count());

    // set should remove all three and add one new entry.
    try headers.set("X-Dup", "final");
    try std.testing.expectEqual(@as(usize, 1), headers.count());
    try std.testing.expectEqualStrings("final", headers.get("X-Dup").?);
}

test "Headers reject CRLF in append" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try std.testing.expectError(error.HeaderContainsCrLf, headers.append("X-Bad\r", "val"));
    try std.testing.expectError(error.HeaderContainsCrLf, headers.append("X-Ok", "val\nue"));
    try std.testing.expectError(error.HeaderContainsCrLf, headers.appendBorrowed("X-Bad\n", "val"));
}

test "Headers isChunked token matching" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.set("Transfer-Encoding", "gzip, chunked");
    try std.testing.expect(headers.isChunked());

    try headers.set("Transfer-Encoding", "chunkedx");
    try std.testing.expect(!headers.isChunked());
}

test "Headers Connection close detection" {
    const allocator = std.testing.allocator;
    var headers = Headers.init(allocator);
    defer headers.deinit();

    try headers.set("Connection", "close");
    try std.testing.expect(!headers.isKeepAlive(.HTTP_1_1));
}
