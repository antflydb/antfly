// Copyright 2026 Antfly, Inc.
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

// Shared C file I/O helpers for Zig 0.16.
//
// Zig 0.16 removed std.fs.cwd(), std.fs.openFileAbsolute(), etc.
// This module wraps C library calls (open, read, mmap) for absolute
// path access that all modules share.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");

pub const link_libc = build_options.link_libc;

pub const c = if (build_options.link_libc) @cImport({
    @cInclude("fcntl.h");
    @cInclude("unistd.h");
    @cInclude("sys/stat.h");
    @cInclude("sys/mman.h");
    @cInclude("dirent.h");
}) else struct {};

/// Memory-mapped file region. The mapped bytes are valid until `deinit()` is called.
pub const MmapRegion = struct {
    data: []align(std.heap.page_size_min) u8,
    fd: std.posix.fd_t,

    /// Memory-map an entire file read-only. Returns borrowed bytes backed by the OS page cache.
    pub fn init(allocator: std.mem.Allocator, path: []const u8) !MmapRegion {
        const path_z = try allocator.dupeSentinel(u8, path, 0);
        defer allocator.free(path_z);

        const fd = try openReadOnlyZ(path_z);
        errdefer closeFd(fd);

        const size = fileSizeFromFd(fd) catch return error.StatFailed;
        if (size == 0) {
            return error.EmptyFile;
        }

        const mapped = try std.posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
        return .{ .data = mapped, .fd = fd };
    }

    /// Hint sequential access for the first `len` bytes (the header region).
    /// Only the header is read sequentially; the rest of the file (tensor data)
    /// is left with default advice so the kernel doesn't eagerly page in
    /// multi-GB of tensor data.
    pub fn adviseSequentialPrefix(self: *MmapRegion, len: usize) void {
        const clamped = @min(len, self.data.len);
        if (clamped > 0) {
            advise(self.data.ptr, clamped, .sequential);
        }
        // Mark the remainder as random-access so the kernel avoids readahead
        // into the (potentially multi-GB) tensor data region.
        if (clamped < self.data.len) {
            advise(self.data.ptr + clamped, self.data.len - clamped, .random);
        }
    }

    /// Switch the entire region to random-access advice.
    pub fn adviseRandom(self: *MmapRegion) void {
        advise(self.data.ptr, self.data.len, .random);
    }

    pub fn deinit(self: *MmapRegion) void {
        std.posix.munmap(self.data);
        closeFd(self.fd);
    }
};

/// Read an entire file into an allocated buffer.
/// Max size is configurable (default 100MB for SafeTensors weights).
pub fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    return readFileMax(allocator, path, 100 * 1024 * 1024);
}

/// Read an entire file with a custom max size limit.
pub fn readFileMax(allocator: std.mem.Allocator, path: []const u8, max_size: usize) ![]u8 {
    const path_z = try allocator.dupeSentinel(u8, path, 0);
    defer allocator.free(path_z);

    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);

    const size = try fileSizeFromFd(fd);
    if (size > max_size) return error.FileTooLarge;

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var total: usize = 0;
    while (total < size) {
        const n = readAt(fd, buf[total..], total) catch break;
        if (n == 0) break;
        total += n;
    }

    if (total != size) return error.IncompleteRead;
    return buf;
}

/// Return the byte size of a file.
pub fn fileSize(allocator: std.mem.Allocator, path: []const u8) !u64 {
    const path_z = try allocator.dupeSentinel(u8, path, 0);
    defer allocator.free(path_z);

    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);

    return try fileSizeFromFd(fd);
}

/// Read a byte range from a file using pread.
pub fn readRegion(allocator: std.mem.Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const path_z = try allocator.dupeSentinel(u8, path, 0);
    defer allocator.free(path_z);

    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);

    const size: u64 = @intCast(try fileSizeFromFd(fd));
    const end = try std.math.add(u64, offset, len);
    if (end > size) return error.RegionOutOfBounds;

    const buf = try allocator.alloc(u8, len);
    errdefer allocator.free(buf);

    var total: usize = 0;
    while (total < len) {
        const read_off = try std.math.add(u64, offset, total);
        const n = readAt(fd, buf[total..], read_off) catch break;
        if (n == 0) break;
        total += n;
    }

    if (total != len) return error.IncompleteRead;
    return buf;
}

/// Check if a file exists at the given path.
pub fn fileExists(allocator: std.mem.Allocator, path: []const u8) bool {
    const path_z = allocator.dupeSentinel(u8, path, 0) catch return false;
    defer allocator.free(path_z);
    return fileExistsZ(path_z);
}

/// Check if a file exists (null-terminated path, no allocation).
pub fn fileExistsZ(path_z: [:0]const u8) bool {
    const fd = openReadOnlyZ(path_z) catch return false;
    closeFd(fd);
    return true;
}

fn fileSizeFromFd(fd: std.posix.fd_t) !usize {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        while (true) {
            var statx = std.mem.zeroes(linux.Statx);
            switch (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .SIZE = true }, &statx))) {
                .SUCCESS => {
                    if (!statx.mask.SIZE) return error.StatFailed;
                    return @intCast(statx.size);
                },
                .INTR => continue,
                else => return error.StatFailed,
            }
        }
    } else if (comptime build_options.link_libc) {
        var stat_buf: c.struct_stat = undefined;
        if (c.fstat(fd, &stat_buf) != 0) return error.StatFailed;
        return @intCast(stat_buf.st_size);
    } else {
        const file: std.Io.File = .{ .handle = fd, .flags = .{ .nonblocking = false } };
        const stat = try file.stat(std.Options.debug_io);
        return @intCast(stat.size);
    }
}

const Advice = enum { sequential, random };

fn openReadOnlyZ(path_z: [:0]const u8) !std.posix.fd_t {
    if (comptime build_options.link_libc) {
        const fd = c.open(path_z.ptr, c.O_RDONLY);
        if (fd < 0) return error.FileNotFound;
        return @intCast(fd);
    }
    return std.posix.openatZ(std.posix.AT.FDCWD, path_z.ptr, .{ .ACCMODE = .RDONLY }, 0) catch error.FileNotFound;
}

fn closeFd(fd: std.posix.fd_t) void {
    if (comptime build_options.link_libc) {
        _ = c.close(fd);
    } else {
        _ = std.posix.system.close(fd);
    }
}

fn readAt(fd: std.posix.fd_t, buf: []u8, offset: u64) !usize {
    if (comptime build_options.link_libc) {
        const n = c.pread(fd, buf.ptr, buf.len, @intCast(offset));
        if (n < 0) return error.ReadFailed;
        return @intCast(n);
    }
    if (builtin.os.tag == .linux) {
        while (true) {
            const rc = std.os.linux.pread(fd, buf.ptr, buf.len, @intCast(offset));
            switch (std.os.linux.errno(rc)) {
                .SUCCESS => return @intCast(rc),
                .INTR => continue,
                else => return error.ReadFailed,
            }
        }
    }
    return error.ReadFailed;
}

fn advise(ptr: [*]u8, len: usize, advice: Advice) void {
    if (comptime build_options.link_libc) {
        const c_advice = switch (advice) {
            .sequential => c.MADV_SEQUENTIAL,
            .random => c.MADV_RANDOM,
        };
        _ = c.madvise(ptr, len, c_advice);
    }
}

/// Join a directory and filename, then check existence.
pub fn fileExistsInDir(allocator: std.mem.Allocator, dir: []const u8, name: []const u8) bool {
    const path = std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir, name }) catch return false;
    defer allocator.free(path);
    return fileExists(allocator, path);
}

/// Join a directory and filename, then read the file.
pub fn readFileFromDir(allocator: std.mem.Allocator, dir: []const u8, name: []const u8) ![]u8 {
    const path = try std.fmt.allocPrint(allocator, "{s}/{s}", .{ dir, name });
    defer allocator.free(path);
    return readFile(allocator, path);
}

test "fileExistsZ on nonexistent" {
    try std.testing.expect(!fileExistsZ("/tmp/this_file_should_not_exist_termite_zig_test"));
}

test "MmapRegion maps file data correctly and adviseRandom does not crash" {
    const allocator = std.testing.allocator;

    // Write a temp file with known content.
    const path_buf = try std.fmt.allocPrint(allocator, "/tmp/termite_mmap_test_data_{d}", .{std.posix.system.getpid()});
    defer allocator.free(path_buf);
    const payload = "Hello, mmap! This is test data for the MmapRegion verification test.";
    {
        var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path_buf, .{ .truncate = true });
        defer file.close(std.testing.io);
        try file.writeStreamingAll(std.testing.io, payload);
    }
    defer std.Io.Dir.deleteFileAbsolute(std.testing.io, path_buf) catch {};

    // mmap and verify contents
    var region = try MmapRegion.init(allocator, path_buf);
    defer region.deinit();

    try std.testing.expectEqual(payload.len, region.data.len);
    try std.testing.expectEqualSlices(u8, payload, region.data[0..payload.len]);

    // adviseRandom should not fail
    region.adviseRandom();

    // Data should still be readable after advice change
    try std.testing.expectEqualSlices(u8, payload, region.data[0..payload.len]);
}
