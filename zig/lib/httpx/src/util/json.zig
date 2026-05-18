//! JSON Utilities for httpx.zig
//!
//! Provides JSON handling utilities for HTTP message bodies:
//!
//! - Type-safe parsing using the local antfly JSON facade
//! - Dynamic JSON building with JsonBuilder
//! - Common JSON operations for APIs

const std = @import("std");
const json = @import("antfly-json");
const Allocator = std.mem.Allocator;
const arrayListWriter = @import("array_list_writer.zig").arrayListWriter;

fn stringifyJsonAlloc(allocator: Allocator, value: anytype, options: json.Stringify.Options) ![]u8 {
    return json.Stringify.valueAlloc(allocator, value, options);
}

/// JSON utility functions.
pub const Json = struct {
    /// Parses a JSON string into the specified type.
    pub fn parse(comptime T: type, allocator: Allocator, data: []const u8) !json.Parsed(T) {
        return json.parseFromSlice(T, allocator, data, .{});
    }

    /// Serializes a value to a JSON string.
    pub fn stringify(allocator: Allocator, value: anytype) ![]u8 {
        return stringifyJsonAlloc(allocator, value, .{});
    }

    /// Serializes a value to a JSON string with pretty formatting.
    pub fn stringifyPretty(allocator: Allocator, value: anytype) ![]u8 {
        return stringifyJsonAlloc(allocator, value, .{ .whitespace = .indent_2 });
    }

    /// Validates that a string is valid JSON.
    pub fn validate(allocator: Allocator, data: []const u8) bool {
        var scanner = json.Scanner.initCompleteInput(allocator, data);
        defer scanner.deinit();

        while (true) {
            const token = scanner.next() catch return false;
            if (token == .end_of_document) return true;
        }
    }
};

/// Dynamic JSON builder for constructing JSON objects.
pub const JsonBuilder = struct {
    allocator: Allocator,
    buffer: std.ArrayListUnmanaged(u8) = .empty,
    depth: usize = 0,
    needs_comma: bool = false,

    const Self = @This();

    /// Creates a new JSON builder.
    pub fn init(allocator: Allocator) Self {
        return .{ .allocator = allocator };
    }

    /// Releases builder resources.
    pub fn deinit(self: *Self) void {
        self.buffer.deinit(self.allocator);
    }

    /// Starts a JSON object.
    pub fn beginObject(self: *Self) !void {
        try self.maybeComma();
        try self.buffer.append(self.allocator, '{');
        self.depth += 1;
        self.needs_comma = false;
    }

    /// Ends the current JSON object.
    pub fn endObject(self: *Self) !void {
        if (self.depth == 0) return error.UnmatchedEnd;
        try self.buffer.append(self.allocator, '}');
        self.depth -= 1;
        self.needs_comma = true;
    }

    /// Starts a JSON array.
    pub fn beginArray(self: *Self) !void {
        try self.maybeComma();
        try self.buffer.append(self.allocator, '[');
        self.depth += 1;
        self.needs_comma = false;
    }

    /// Ends the current JSON array.
    pub fn endArray(self: *Self) !void {
        if (self.depth == 0) return error.UnmatchedEnd;
        try self.buffer.append(self.allocator, ']');
        self.depth -= 1;
        self.needs_comma = true;
    }

    /// Writes an object key.
    pub fn key(self: *Self, name: []const u8) !void {
        try self.maybeComma();
        try self.writeString(name);
        try self.buffer.append(self.allocator, ':');
        self.needs_comma = false;
    }

    /// Writes a string value.
    pub fn string(self: *Self, value: []const u8) !void {
        try self.maybeComma();
        try self.writeString(value);
        self.needs_comma = true;
    }

    /// Writes an integer value.
    pub fn number(self: *Self, value: anytype) !void {
        try self.maybeComma();
        try self.buffer.print(self.allocator, "{d}", .{value});
        self.needs_comma = true;
    }

    /// Writes a boolean value.
    pub fn boolean(self: *Self, value: bool) !void {
        try self.maybeComma();
        const str = if (value) "true" else "false";
        try self.buffer.appendSlice(self.allocator, str);
        self.needs_comma = true;
    }

    /// Writes a null value.
    pub fn nullValue(self: *Self) !void {
        try self.maybeComma();
        try self.buffer.appendSlice(self.allocator, "null");
        self.needs_comma = true;
    }

    /// Returns the built JSON string.
    pub fn toSlice(self: *const Self) []const u8 {
        return self.buffer.items;
    }

    /// Returns ownership of the JSON string.
    pub fn toOwnedSlice(self: *Self) ![]u8 {
        return self.buffer.toOwnedSlice(self.allocator);
    }

    fn maybeComma(self: *Self) !void {
        if (self.needs_comma) {
            try self.buffer.append(self.allocator, ',');
        }
    }

    fn writeString(self: *Self, str: []const u8) !void {
        try self.buffer.append(self.allocator, '"');
        for (str) |c| {
            switch (c) {
                '"' => try self.buffer.appendSlice(self.allocator, "\\\""),
                '\\' => try self.buffer.appendSlice(self.allocator, "\\\\"),
                '\n' => try self.buffer.appendSlice(self.allocator, "\\n"),
                '\r' => try self.buffer.appendSlice(self.allocator, "\\r"),
                '\t' => try self.buffer.appendSlice(self.allocator, "\\t"),
                0x00...0x08, 0x0B, 0x0C, 0x0E...0x1F => {
                    // JSON requires all control characters to be escaped.
                    var buf: [6]u8 = undefined;
                    _ = std.fmt.bufPrint(&buf, "\\u{X:0>4}", .{c}) catch unreachable;
                    try self.buffer.appendSlice(self.allocator, &buf);
                },
                else => try self.buffer.append(self.allocator, c),
            }
        }
        try self.buffer.append(self.allocator, '"');
    }
};

test "JsonBuilder object" {
    const allocator = std.testing.allocator;
    var builder = JsonBuilder.init(allocator);
    defer builder.deinit();

    try builder.beginObject();
    try builder.key("name");
    try builder.string("test");
    try builder.key("count");
    try builder.number(42);
    try builder.endObject();

    const result = builder.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, result, "\"name\":\"test\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"count\":42") != null);
}

test "JsonBuilder array" {
    const allocator = std.testing.allocator;
    var builder = JsonBuilder.init(allocator);
    defer builder.deinit();

    try builder.beginArray();
    try builder.number(1);
    try builder.number(2);
    try builder.number(3);
    try builder.endArray();

    try std.testing.expectEqualStrings("[1,2,3]", builder.toSlice());
}

test "JsonBuilder nested" {
    const allocator = std.testing.allocator;
    var builder = JsonBuilder.init(allocator);
    defer builder.deinit();

    try builder.beginObject();
    try builder.key("items");
    try builder.beginArray();
    try builder.beginObject();
    try builder.key("id");
    try builder.number(1);
    try builder.endObject();
    try builder.endArray();
    try builder.endObject();

    const result = builder.toSlice();
    try std.testing.expect(std.mem.startsWith(u8, result, "{\"items\":[{\"id\":1}]}"));
}

test "JsonBuilder boolean and null" {
    const allocator = std.testing.allocator;
    var builder = JsonBuilder.init(allocator);
    defer builder.deinit();

    try builder.beginObject();
    try builder.key("active");
    try builder.boolean(true);
    try builder.key("deleted");
    try builder.boolean(false);
    try builder.key("data");
    try builder.nullValue();
    try builder.endObject();

    const result = builder.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, result, "\"active\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\"data\":null") != null);
}

test "JsonBuilder string escaping" {
    const allocator = std.testing.allocator;
    var builder = JsonBuilder.init(allocator);
    defer builder.deinit();

    try builder.beginObject();
    try builder.key("text");
    try builder.string("line1\nline2\ttab");
    try builder.endObject();

    const result = builder.toSlice();
    try std.testing.expect(std.mem.indexOf(u8, result, "\\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, result, "\\t") != null);
}
