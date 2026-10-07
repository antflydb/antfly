// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

/// Bounded staging for platforms without the POSIX atomic writer. The caller
/// owns the file and publication; this type never allocates or closes it.
pub fn StagedFile(comptime Crc32: type) type {
    return struct {
        const Self = @This();
        pub const buffer_size = 64 * 1024;
        io: std.Io,
        file: std.Io.File,
        persisted: usize = 0,
        buffered: usize = 0,
        buffer: [buffer_size]u8 = undefined,
        failure: ?anyerror = null,

        pub fn len(self: *const Self) usize {
            return self.persisted + self.buffered;
        }

        pub fn append(self: *Self, bytes: []const u8) !void {
            if (self.failure) |err| return err;
            if (bytes.len > std.math.maxInt(usize) - self.len()) return error.FileTooBig;
            var offset: usize = 0;
            while (offset < bytes.len) {
                const n = @min(self.buffer.len - self.buffered, bytes.len - offset);
                @memcpy(self.buffer[self.buffered..][0..n], bytes[offset..][0..n]);
                self.buffered += n;
                offset += n;
                if (self.buffered == self.buffer.len) try self.flush();
            }
        }

        pub fn flush(self: *Self) !void {
            if (self.failure) |err| return err;
            if (self.buffered == 0) return;
            self.file.writePositionalAll(self.io, self.buffer[0..self.buffered], self.persisted) catch |err| {
                self.failure = err;
                return err;
            };
            self.persisted += self.buffered;
            self.buffered = 0;
        }

        pub fn writeAt(self: *Self, offset: usize, bytes: []const u8) !void {
            if (self.failure) |err| return err;
            if (offset > self.len() or bytes.len > self.len() - offset) return error.InvalidAtomicWriteOffset;
            const on_disk = if (offset < self.persisted) @min(bytes.len, self.persisted - offset) else 0;
            if (on_disk > 0) self.file.writePositionalAll(self.io, bytes[0..on_disk], offset) catch |err| {
                self.failure = err;
                return err;
            };
            if (on_disk < bytes.len) @memcpy(self.buffer[offset + on_disk - self.persisted ..][0 .. bytes.len - on_disk], bytes[on_disk..]);
        }

        pub fn crc32Range(self: *Self, offset: usize, range_len: usize) !u32 {
            if (self.failure) |err| return err;
            if (offset > self.len() or range_len > self.len() - offset) return error.InvalidAtomicWriteOffset;
            var crc = Crc32.init();
            var scratch: [buffer_size]u8 = undefined;
            const end = offset + range_len;
            var pos = offset;
            while (pos < @min(end, self.persisted)) {
                const n = @min(scratch.len, @min(end, self.persisted) - pos);
                const read = self.file.readPositionalAll(self.io, scratch[0..n], pos) catch |err| {
                    self.failure = err;
                    return err;
                };
                if (read != n) {
                    self.failure = error.EndOfStream;
                    return error.EndOfStream;
                }
                crc.update(scratch[0..n]);
                pos += n;
            }
            if (pos < end) crc.update(self.buffer[pos - self.persisted .. end - self.persisted]);
            return crc.final();
        }

        pub fn sync(self: *Self) !void {
            try self.flush();
            self.file.sync(self.io) catch |err| {
                self.failure = err;
                return err;
            };
        }
    };
}

test "staged output is bounded and patches and checksums cross the disk buffer boundary" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(io, "staging", .{ .read = true, .exclusive = true });
    defer file.close(io);
    var staged: StagedFile(std.hash.Crc32) = .{ .io = io, .file = file };
    var chunk: [64 * 1024]u8 = undefined;
    for (&chunk, 0..) |*byte, i| byte.* = @truncate(i);
    var expected = std.hash.Crc32.init();
    for (0..128) |_| {
        try staged.append(&chunk);
        expected.update(&chunk);
    }
    try std.testing.expectEqual(@as(usize, 8 * 1024 * 1024), staged.len());
    try std.testing.expectEqual(@as(usize, 0), staged.buffered);
    try std.testing.expectEqual(expected.final(), try staged.crc32Range(0, staged.len()));
    try staged.append("tail");
    try staged.writeAt(staged.persisted - 2, "PATCH");
    try std.testing.expectEqual(std.hash.Crc32.hash("PATCHl"), try staged.crc32Range(staged.persisted - 2, 6));
    try std.testing.expectEqual(std.hash.Crc32.hash(""), try staged.crc32Range(staged.len(), 0));
    try std.testing.expectError(error.InvalidAtomicWriteOffset, staged.writeAt(staged.len(), "x"));
    try std.testing.expectError(error.InvalidAtomicWriteOffset, staged.crc32Range(staged.len(), 1));
    try staged.sync();
    var patch: [6]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 6), try file.readPositionalAll(io, &patch, staged.len() - 6));
    try std.testing.expectEqualStrings("PATCHl", &patch);
    try std.testing.expectEqual(staged.len(), (try file.stat(io)).size);
}

test "a failed staging read prevents later publication" {
    const io = std.testing.io;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const file = try tmp.dir.createFile(io, "staging", .{ .read = true });
    defer file.close(io);
    var staged: StagedFile(std.hash.Crc32) = .{ .io = io, .file = file };
    try staged.append("data");
    try staged.flush();
    try file.setLength(io, 0);
    try std.testing.expectError(error.EndOfStream, staged.crc32Range(0, 4));
    try std.testing.expectError(error.EndOfStream, staged.append("more"));
    try std.testing.expectError(error.EndOfStream, staged.sync());
}
