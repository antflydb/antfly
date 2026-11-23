// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

const std = @import("std");

/// Bounded, owned SQL lookup path. Copying it pins every namespace for a
/// prepared statement, portal, transaction savepoint, or retained cursor.
pub const Namespace = struct {
    bytes: [128]u8 = @splat(0),
    len: u8 = 0,

    pub fn init(text: []const u8) !Namespace {
        if (text.len == 0 or text.len > 128 or (!std.ascii.isAlphabetic(text[0]) and text[0] != '_')) return error.InvalidParameter;
        for (text[1..]) |ch| if (!std.ascii.isAlphanumeric(ch) and ch != '_' and ch != '-') return error.UnsupportedSqlShape;
        var value: Namespace = .{ .len = @intCast(text.len) };
        @memcpy(value.bytes[0..text.len], text);
        return value;
    }

    pub fn slice(self: *const Namespace) []const u8 {
        return self.bytes[0..self.len];
    }
};

pub const Path = struct {
    pub const max_namespaces = 8;
    entries: [max_namespaces]Namespace = @splat(.{}),
    len: u8 = 0,

    pub fn validate(self: Path) !void {
        if (self.len > max_namespaces) return error.InvalidParameter;
        for (self.entries[0..self.len], 0..) |*entry, i| {
            if (entry.len == 0 or entry.len > entry.bytes.len) return error.InvalidParameter;
            _ = try Namespace.init(entry.slice());
            for (self.entries[0..i]) |*prior| if (std.mem.eql(u8, prior.slice(), entry.slice())) return error.InvalidParameter;
        }
    }

    pub fn jsonStringify(self: Path, writer: anytype) !void {
        try writer.beginArray();
        for (self.entries[0..self.len]) |*entry| try writer.write(entry.slice());
        try writer.endArray();
    }

    pub fn jsonParse(a: std.mem.Allocator, source: anytype, options: std.json.ParseOptions) !Path {
        const names = try std.json.innerParse([]const []const u8, a, source, options);
        var result: Path = .{};
        for (names) |name| result.append(Namespace.init(name) catch return error.UnexpectedToken) catch return error.UnexpectedToken;
        result.validate() catch return error.UnexpectedToken;
        return result;
    }

    pub fn jsonParseFromValue(_: std.mem.Allocator, value: std.json.Value, _: std.json.ParseOptions) !Path {
        if (value != .array) return error.UnexpectedToken;
        var result: Path = .{};
        for (value.array.items) |name| {
            if (name != .string) return error.UnexpectedToken;
            result.append(Namespace.init(name.string) catch return error.UnexpectedToken) catch return error.UnexpectedToken;
        }
        result.validate() catch return error.UnexpectedToken;
        return result;
    }

    pub fn append(self: *Path, name: Namespace) !void {
        if (self.len == max_namespaces) return error.ProgramLimitExceeded;
        self.entries[self.len] = name;
        self.len += 1;
    }

    pub fn first(self: *const Path) []const u8 {
        std.debug.assert(self.len != 0);
        return self.entries[0].slice();
    }

    pub fn display(self: *const Path, alloc: std.mem.Allocator) ![]const u8 {
        var out: std.Io.Writer.Allocating = .init(alloc);
        for (self.entries[0..self.len], 0..) |*entry, index| {
            if (index != 0) try out.writer.writeAll(", ");
            try out.writer.writeAll(entry.slice());
        }
        return out.written();
    }
};

test "SQL durable search paths encode compact names own decoded bytes and reject invalid state" {
    const a = std.testing.allocator;
    var path: Path = .{};
    try path.append(try Namespace.init("tenant"));
    try path.append(try Namespace.init("public"));
    const bytes = try std.json.Stringify.valueAlloc(a, path, .{});
    defer a.free(bytes);
    try std.testing.expectEqualStrings("[\"tenant\",\"public\"]", bytes);
    var parsed = try std.json.parseFromSlice(Path, a, bytes, .{});
    const owned = parsed.value;
    parsed.deinit();
    try std.testing.expectEqualDeep(path, owned);
    var value = try std.json.parseFromSlice(std.json.Value, a, bytes, .{});
    defer value.deinit();
    var restored = try std.json.parseFromValue(Path, a, value.value, .{});
    defer restored.deinit();
    try std.testing.expectEqualDeep(path, restored.value);
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(Path, a, "[\"public\",\"public\"]", .{}));
    try std.testing.expectError(error.UnexpectedToken, std.json.parseFromSlice(Path, a, "[\"bad/path\"]", .{}));
}
