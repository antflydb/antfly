//! Writer adapter for ArrayListUnmanaged(u8) in Zig 0.16.
//!
//! Zig 0.16 removed `.writer(allocator)` from ArrayListUnmanaged.
//! This provides a duck-typed writer with .print(), .writeAll(), .writeByte()
//! that can be passed to functions expecting `writer: anytype`.

const std = @import("std");

pub fn ArrayListWriter(comptime List: type) type {
    return struct {
        list: *List,
        allocator: std.mem.Allocator,

        const Self = @This();

        pub fn print(self: Self, comptime fmt: []const u8, args: anytype) !void {
            try self.list.print(self.allocator, fmt, args);
        }

        pub fn writeAll(self: Self, data: []const u8) !void {
            try self.list.appendSlice(self.allocator, data);
        }

        pub fn writeByte(self: Self, byte: u8) !void {
            try self.list.append(self.allocator, byte);
        }
    };
}

pub fn arrayListWriter(list: *std.ArrayListUnmanaged(u8), allocator: std.mem.Allocator) ArrayListWriter(std.ArrayListUnmanaged(u8)) {
    return .{ .list = list, .allocator = allocator };
}

/// Serializes any object with a `serialize(writer) !void` method to an owned slice.
pub fn serializeToSlice(allocator: std.mem.Allocator, obj: anytype) ![]u8 {
    var buffer = std.ArrayListUnmanaged(u8).empty;
    errdefer buffer.deinit(allocator);
    const writer = arrayListWriter(&buffer, allocator);
    try obj.serialize(writer);
    return buffer.toOwnedSlice(allocator);
}
