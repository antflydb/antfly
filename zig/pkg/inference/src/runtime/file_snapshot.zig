// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Bounded immutable file input. Read and validate the same descriptor, with
//! nonblocking open so malformed FIFO/device inputs cannot stall admission.
const std = @import("std");
const builtin = @import("builtin");
const Control = @import("../execution_control.zig").InferenceExecutionControl;

pub fn openRegular(io: std.Io, directory: std.Io.Dir, path: []const u8, control: ?Control) !std.Io.File {
    if (control) |active| try active.check();
    if (builtin.os.tag != .linux and builtin.os.tag != .macos) return error.UnsupportedSnapshotPlatform;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidSnapshotPath;
    var buffer: [std.fs.max_path_bytes + 1]u8 = undefined;
    const terminated = try std.fmt.bufPrintZ(&buffer, "{s}", .{path});
    const fd = try std.posix.openatZ(directory.handle, terminated, .{ .ACCMODE = .RDONLY, .CLOEXEC = true, .NONBLOCK = true, .NOCTTY = true }, 0);
    const file = std.Io.File{ .handle = fd, .flags = .{ .nonblocking = true } };
    errdefer file.close(io);
    if ((try file.stat(io)).kind != .file) return error.InvalidSnapshotFile;
    return file;
}

pub fn read(a: std.mem.Allocator, io: std.Io, directory: std.Io.Dir, path: []const u8, max_bytes: usize, control: ?Control) ![]u8 {
    const file = try openRegular(io, directory, path, control);
    defer file.close(io);
    return readOpened(a, io, file, max_bytes, control);
}

/// The caller retains the descriptor; header admission can precede this read
/// without reopening a path that may have been replaced in the meantime.
pub fn readOpened(a: std.mem.Allocator, io: std.Io, file: std.Io.File, max_bytes: usize, control: ?Control) ![]u8 {
    if (control) |active| try active.check();
    const initial = try file.stat(io);
    if (initial.kind != .file) return error.InvalidSnapshotFile;
    const size = std.math.cast(usize, initial.size) orelse return error.SnapshotLimitExceeded;
    if (size > max_bytes) return error.SnapshotLimitExceeded;
    const bytes = try a.alloc(u8, size);
    errdefer a.free(bytes);
    var offset: usize = 0;
    while (offset < size) {
        if (control) |active| try active.check();
        const end = @min(size, offset +| (256 * 1024));
        if (try file.readPositionalAll(io, bytes[offset..end], offset) != end - offset) return error.SnapshotChanged;
        offset = end;
    }
    var extra: [1]u8 = undefined;
    if (try file.readPositionalAll(io, &extra, size) != 0) return error.SnapshotChanged;
    const final = try file.stat(io);
    if (final.kind != .file or final.size != initial.size or final.inode != initial.inode or !std.meta.eql(final.mtime, initial.mtime) or !std.meta.eql(final.ctime, initial.ctime)) return error.SnapshotChanged;
    if (control) |active| try active.check();
    return bytes;
}

pub const Digest = struct { size_bytes: u64, sha256: [32]u8 };
pub fn digest(io: std.Io, directory: std.Io.Dir, path: []const u8, max_bytes: u64, control: ?Control) !Digest {
    const file = try openRegular(io, directory, path, control);
    defer file.close(io);
    const initial = try file.stat(io);
    if (initial.size > max_bytes) return error.SnapshotLimitExceeded;
    var hash = std.crypto.hash.sha2.Sha256.init(.{});
    var buffer: [64 * 1024]u8 = undefined;
    var offset: u64 = 0;
    while (offset < initial.size) {
        if (control) |active| try active.check();
        const count: usize = @intCast(@min(initial.size - offset, buffer.len));
        if (try file.readPositionalAll(io, buffer[0..count], offset) != count) return error.SnapshotChanged;
        hash.update(buffer[0..count]);
        offset += count;
    }
    if (try file.readPositionalAll(io, buffer[0..1], offset) != 0) return error.SnapshotChanged;
    const final = try file.stat(io);
    if (final.kind != .file or final.size != initial.size or final.inode != initial.inode or !std.meta.eql(final.mtime, initial.mtime) or !std.meta.eql(final.ctime, initial.ctime)) return error.SnapshotChanged;
    if (control) |active| try active.check();
    return .{ .size_bytes = initial.size, .sha256 = hash.finalResult() };
}

test "file snapshot rejects FIFO and bounds allocation before reads" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    var temporary = std.testing.tmpDir(.{});
    defer temporary.cleanup();
    const file = try temporary.dir.createFile(io, "input", .{});
    try file.writeStreamingAll(io, "immutable");
    file.close(io);
    try std.testing.expectError(error.SnapshotLimitExceeded, read(a, io, temporary.dir, "input", 3, null));
    const bytes = try read(a, io, temporary.dir, "input", 16, null);
    defer a.free(bytes);
    try std.testing.expectEqualStrings("immutable", bytes);
    var expected: [32]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(bytes, &expected, .{});
    try std.testing.expectEqual(Digest{ .size_bytes = bytes.len, .sha256 = expected }, try digest(io, temporary.dir, "input", 16, null));
    try std.testing.expectError(error.InvalidSnapshotFile, read(a, io, temporary.dir, ".", 16, null));
    if (builtin.os.tag == .macos or builtin.os.tag == .linux) {
        const C = struct {
            extern "c" fn mkfifoat(c_int, [*:0]const u8, c_uint) c_int;
        };
        try std.testing.expectEqual(@as(c_int, 0), C.mkfifoat(temporary.dir.handle, "fifo", 0o600));
        try std.testing.expectError(error.InvalidSnapshotFile, read(a, io, temporary.dir, "fifo", 16, null));
    }
}
