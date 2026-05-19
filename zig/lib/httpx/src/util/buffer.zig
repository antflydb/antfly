//! Buffer Utilities for httpx.zig
//!
//! Provides high-performance buffer implementations for HTTP message handling:
//!
//! - `RingBuffer`: Circular buffer for streaming data
//! - `FixedBuffer`: Stack-allocated fixed-size buffer

const std = @import("std");
const Allocator = std.mem.Allocator;

/// Circular buffer for streaming data with fixed capacity.
pub const RingBuffer = struct {
    allocator: Allocator,
    data: []u8,
    read_pos: usize = 0,
    write_pos: usize = 0,
    count: usize = 0,

    const Self = @This();

    /// Creates a new ring buffer with the specified capacity.
    pub fn init(allocator: Allocator, capacity: usize) !Self {
        return .{
            .allocator = allocator,
            .data = try allocator.alloc(u8, capacity),
        };
    }

    /// Releases the buffer memory.
    pub fn deinit(self: *Self) void {
        self.allocator.free(self.data);
    }

    /// Writes bytes to the buffer, returning the number written.
    pub fn writeBytes(self: *Self, bytes: []const u8) !usize {
        const space = self.data.len - self.count;
        const to_write = @min(bytes.len, space);
        const src = bytes[0..to_write];

        // Up to two memcpy calls: one to end of ring, one from start.
        const first = @min(to_write, self.data.len - self.write_pos);
        @memcpy(self.data[self.write_pos..][0..first], src[0..first]);
        if (first < to_write) {
            @memcpy(self.data[0 .. to_write - first], src[first..to_write]);
        }
        self.write_pos = (self.write_pos + to_write) % self.data.len;
        self.count += to_write;

        return to_write;
    }

    /// Reads bytes from the buffer into the provided slice.
    pub fn readBytes(self: *Self, buf: []u8) usize {
        const to_read = @min(buf.len, self.count);

        const first = @min(to_read, self.data.len - self.read_pos);
        @memcpy(buf[0..first], self.data[self.read_pos..][0..first]);
        if (first < to_read) {
            @memcpy(buf[first..to_read], self.data[0 .. to_read - first]);
        }
        self.read_pos = (self.read_pos + to_read) % self.data.len;
        self.count -= to_read;

        return to_read;
    }

    /// Returns the number of bytes available to read.
    pub fn getAvailable(self: *const Self) usize {
        return self.count;
    }

    /// Returns the total buffer capacity.
    pub fn getCapacity(self: *const Self) usize {
        return self.data.len;
    }

    /// Returns the amount of free space in the buffer.
    pub fn getFreeSpace(self: *const Self) usize {
        return self.data.len - self.count;
    }

    /// Returns true if the buffer is empty.
    pub fn isEmpty(self: *const Self) bool {
        return self.count == 0;
    }

    /// Returns true if the buffer is full.
    pub fn isFull(self: *const Self) bool {
        return self.count == self.data.len;
    }

    /// Clears all data from the buffer.
    pub fn clear(self: *Self) void {
        self.read_pos = 0;
        self.write_pos = 0;
        self.count = 0;
    }
};

/// Fixed-size buffer for stack allocation without heap usage.
pub fn FixedBuffer(comptime size: usize) type {
    return struct {
        data: [size]u8 = undefined,
        len: usize = 0,

        const Self = @This();

        /// Appends bytes to the buffer.
        pub fn append(self: *Self, bytes: []const u8) !void {
            if (self.len + bytes.len > size) return error.BufferOverflow;
            @memcpy(self.data[self.len..][0..bytes.len], bytes);
            self.len += bytes.len;
        }

        /// Appends a single byte.
        pub fn appendByte(self: *Self, byte: u8) !void {
            if (self.len >= size) return error.BufferOverflow;
            self.data[self.len] = byte;
            self.len += 1;
        }

        /// Returns the current buffer contents.
        pub fn slice(self: *const Self) []const u8 {
            return self.data[0..self.len];
        }

        /// Clears the buffer.
        pub fn clear(self: *Self) void {
            self.len = 0;
        }

        /// Returns the remaining capacity.
        pub fn remaining(self: *const Self) usize {
            return size - self.len;
        }
    };
}

test "RingBuffer operations" {
    const allocator = std.testing.allocator;
    var ring = try RingBuffer.init(allocator, 8);
    defer ring.deinit();

    _ = try ring.writeBytes("hello");
    try std.testing.expectEqual(@as(usize, 5), ring.getAvailable());

    var out: [10]u8 = undefined;
    const n = ring.readBytes(&out);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("hello", out[0..n]);
}

test "RingBuffer wraparound" {
    const allocator = std.testing.allocator;
    var ring = try RingBuffer.init(allocator, 4);
    defer ring.deinit();

    _ = try ring.writeBytes("ab");
    var out: [2]u8 = undefined;
    _ = ring.readBytes(&out);
    _ = try ring.writeBytes("cdef");

    try std.testing.expectEqual(@as(usize, 4), ring.getAvailable());
}

test "FixedBuffer operations" {
    var buf = FixedBuffer(32){};

    try buf.append("test");
    try std.testing.expectEqualStrings("test", buf.slice());
    try std.testing.expectEqual(@as(usize, 28), buf.remaining());
}

test "FixedBuffer overflow" {
    var buf = FixedBuffer(4){};

    try buf.append("test");
    try std.testing.expectError(error.BufferOverflow, buf.append("x"));
}
