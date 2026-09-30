// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
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
