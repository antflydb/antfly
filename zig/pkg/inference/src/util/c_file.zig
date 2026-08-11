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

pub const c = if (build_options.link_libc) PosixC else struct {};

const PosixC = struct {
    pub const DIR = std.c.DIR;
    pub const mode_t = std.c.mode_t;
    pub const struct_stat = std.c.Stat;

    pub const struct_dirent = switch (builtin.os.tag) {
        .linux => extern struct {
            ino: std.c.ino_t,
            off: std.c.off_t,
            reclen: c_ushort,
            type: u8,
            d_name: [256]u8,
        },
        .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => extern struct {
            ino: u64,
            seekoff: u64,
            reclen: u16,
            namlen: u16,
            type: u8,
            d_name: [1024]u8,
        },
        else => std.c.dirent,
    };

    fn flag(o: std.c.O) c_int {
        return @bitCast(o);
    }

    pub const O_RDONLY = flag(.{ .ACCMODE = .RDONLY });
    pub const O_WRONLY = flag(.{ .ACCMODE = .WRONLY });
    pub const O_RDWR = flag(.{ .ACCMODE = .RDWR });
    pub const O_CREAT = flag(.{ .CREAT = true });
    pub const O_EXCL = flag(.{ .EXCL = true });
    pub const O_TRUNC = flag(.{ .TRUNC = true });

    pub const MADV_RANDOM = std.c.MADV.RANDOM;
    pub const MADV_SEQUENTIAL = std.c.MADV.SEQUENTIAL;

    pub const POSIX_FADV_NORMAL = std.os.linux.POSIX_FADV.NORMAL;
    pub const POSIX_FADV_SEQUENTIAL = std.os.linux.POSIX_FADV.SEQUENTIAL;
    pub const POSIX_FADV_RANDOM = std.os.linux.POSIX_FADV.RANDOM;
    pub const POSIX_FADV_WILLNEED = std.os.linux.POSIX_FADV.WILLNEED;
    pub const POSIX_FADV_DONTNEED = std.os.linux.POSIX_FADV.DONTNEED;
    pub const POSIX_FADV_NOREUSE = std.os.linux.POSIX_FADV.NOREUSE;

    pub extern "c" fn open(path: [*:0]const u8, oflag: c_int, ...) c_int;
    pub extern "c" fn lstat(path: [*:0]const u8, buf: *struct_stat) c_int;
    pub extern "c" fn posix_fadvise(fd: std.c.fd_t, offset: std.c.off_t, len: std.c.off_t, advice: c_int) c_int;

    pub const close = std.c.close;
    pub const fstat = std.c.fstat;
    pub const ftruncate = std.c.ftruncate;
    pub const getcwd = std.c.getcwd;
    pub const getpid = std.c.getpid;
    pub const link = std.c.link;
    pub const madvise = std.c.madvise;
    pub const mkdir = std.c.mkdir;
    pub const pread = std.c.pread;
    pub const pwrite = std.c.pwrite;
    pub const symlink = std.c.symlink;
    pub const unlink = std.c.unlink;
    pub const write = std.c.write;

    pub fn opendir(pathname: [*:0]const u8) ?*DIR {
        return std.c.opendir(pathname);
    }

    pub fn closedir(dp: ?*DIR) c_int {
        return std.c.closedir(dp.?);
    }

    pub fn readdir(dp: ?*DIR) ?*struct_dirent {
        const entry = std.c.readdir(dp.?) orelse return null;
        return @ptrCast(@alignCast(entry));
    }
};

var mmap_temp_counter: std.atomic.Value(u64) = .init(0);

/// Memory-mapped file region. The mapped bytes are valid until `deinit()` is called.
pub const MmapRegion = struct {
    data: []align(std.heap.page_size_min) u8,
    fd: std.posix.fd_t,

    /// Memory-map an entire file read-only. Returns borrowed bytes backed by the OS page cache.
    pub fn init(allocator: std.mem.Allocator, path: []const u8) !MmapRegion {
        const path_z = try allocator.dupeZ(u8, path);
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

pub fn mmapTempCopy(allocator: std.mem.Allocator, prefix: []const u8, bytes: []const u8) !MmapRegion {
    if (!comptime build_options.link_libc) return error.UnsupportedPlatform;
    if (bytes.len == 0) return error.EmptyFile;

    const nonce = mmap_temp_counter.fetchAdd(1, .monotonic);
    const path = try std.fmt.allocPrint(
        allocator,
        "/tmp/{s}-{d}-{d}.bin",
        .{ prefix, std.posix.system.getpid(), nonce },
    );
    defer allocator.free(path);
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    const fd = c.open(path_z.ptr, c.O_RDWR | c.O_CREAT | c.O_EXCL, @as(c.mode_t, 0o600));
    if (fd < 0) return error.CreateFailed;
    errdefer closeFd(@intCast(fd));
    var temp_unlinked = false;
    errdefer {
        if (!temp_unlinked) _ = c.unlink(path_z.ptr);
    }
    if (c.unlink(path_z.ptr) != 0) return error.UnlinkFailed;
    temp_unlinked = true;
    if (c.ftruncate(fd, @intCast(bytes.len)) != 0) return error.TruncateFailed;
    try writeAllAt(@intCast(fd), bytes, 0);

    const mapped = try std.posix.mmap(null, bytes.len, .{ .READ = true }, .{ .TYPE = .SHARED }, @intCast(fd), 0);
    var region = MmapRegion{ .data = mapped, .fd = @intCast(fd) };
    region.adviseRandom();
    return region;
}

/// Read an entire file into an allocated buffer.
/// Max size is configurable (default 100MB for SafeTensors weights).
pub fn readFile(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    return readFileMax(allocator, path, 100 * 1024 * 1024);
}

/// Read an entire file with a custom max size limit.
pub fn readFileMax(allocator: std.mem.Allocator, path: []const u8, max_size: usize) ![]u8 {
    const path_z = try allocator.dupeZ(u8, path);
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
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);

    return try fileSizeFromFd(fd);
}

/// Read a byte range from a file using pread.
pub fn readRegion(allocator: std.mem.Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);

    const size: u64 = @intCast(try fileSizeFromFd(fd));
    const end = try std.math.add(u64, offset, len);
    if (end > size) return error.RegionOutOfBounds;

    const buf = try allocator.alloc(u8, len);
    errdefer allocator.free(buf);

    try readRegionFromFd(fd, buf, offset);
    return buf;
}

/// Read a byte range from a file into an existing buffer using pread.
pub fn readRegionInto(allocator: std.mem.Allocator, path: []const u8, offset: u64, buf: []u8) !void {
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);

    const size: u64 = @intCast(try fileSizeFromFd(fd));
    const end = try std.math.add(u64, offset, buf.len);
    if (end > size) return error.RegionOutOfBounds;

    try readRegionFromFd(fd, buf, offset);
}

fn readRegionFromFd(fd: std.posix.fd_t, buf: []u8, offset: u64) !void {
    var total: usize = 0;
    while (total < buf.len) {
        const read_off = try std.math.add(u64, offset, total);
        const n = readAt(fd, buf[total..], read_off) catch break;
        if (n == 0) break;
        total += n;
    }
    if (total != buf.len) return error.IncompleteRead;
}

pub const FileAdvice = enum { normal, sequential, random, will_need, dont_need, no_reuse };

pub fn adviseFileRange(allocator: std.mem.Allocator, path: []const u8, offset: u64, len: usize, advice: FileAdvice) void {
    if (!comptime build_options.link_libc) return;
    if (comptime builtin.os.tag != .linux) return;
    const path_z = allocator.dupeZ(u8, path) catch return;
    defer allocator.free(path_z);
    const fd = openReadOnlyZ(path_z) catch return;
    defer closeFd(fd);
    const c_advice: c_int = switch (advice) {
        .normal => c.POSIX_FADV_NORMAL,
        .sequential => c.POSIX_FADV_SEQUENTIAL,
        .random => c.POSIX_FADV_RANDOM,
        .will_need => c.POSIX_FADV_WILLNEED,
        .dont_need => c.POSIX_FADV_DONTNEED,
        .no_reuse => c.POSIX_FADV_NOREUSE,
    };
    _ = c.posix_fadvise(fd, @intCast(offset), @intCast(len), c_advice);
}

/// Check if a file exists at the given path.
pub fn fileExists(allocator: std.mem.Allocator, path: []const u8) bool {
    const path_z = allocator.dupeZ(u8, path) catch return false;
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
        return @intCast(statSize(stat_buf));
    } else {
        const file: std.Io.File = .{ .handle = fd, .flags = .{ .nonblocking = false } };
        const stat = try file.stat(std.Options.debug_io);
        return @intCast(stat.size);
    }
}

fn statSize(stat: c.struct_stat) std.c.off_t {
    if (@hasField(c.struct_stat, "st_size")) return stat.st_size;
    return stat.size;
}

const Advice = enum { sequential, random };

fn openReadOnlyZ(path_z: [:0]const u8) !std.posix.fd_t {
    // std.posix.openatZ preserves actionable failures such as AccessDenied,
    // NotDir, and descriptor exhaustion while still retrying EINTR. Flattening
    // every failure to FileNotFound makes callers misreport operational faults
    // as absent model metadata.
    return std.posix.openatZ(std.posix.AT.FDCWD, path_z.ptr, .{ .ACCMODE = .RDONLY }, 0);
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

fn writeAllAt(fd: std.posix.fd_t, bytes: []const u8, offset: u64) !void {
    var total: usize = 0;
    while (total < bytes.len) {
        const write_off = try std.math.add(u64, offset, total);
        const n = if (comptime build_options.link_libc) blk: {
            const rc = c.pwrite(fd, bytes.ptr + total, bytes.len - total, @intCast(write_off));
            if (rc < 0) return error.WriteFailed;
            break :blk @as(usize, @intCast(rc));
        } else blk: {
            if (builtin.os.tag == .linux) {
                while (true) {
                    const rc = std.os.linux.pwrite(fd, bytes.ptr + total, bytes.len - total, @intCast(write_off));
                    switch (std.os.linux.errno(rc)) {
                        .SUCCESS => break :blk @as(usize, @intCast(rc)),
                        .INTR => continue,
                        else => return error.WriteFailed,
                    }
                }
            }
            return error.WriteFailed;
        };
        if (n == 0) return error.IncompleteWrite;
        total += n;
    }
}

fn advise(ptr: [*]u8, len: usize, advice: Advice) void {
    if (comptime build_options.link_libc) {
        const c_advice: u32 = switch (advice) {
            .sequential => c.MADV_SEQUENTIAL,
            .random => c.MADV_RANDOM,
        };
        const page_size = std.heap.page_size_min;
        const start = @intFromPtr(ptr);
        const aligned_start = std.mem.alignBackward(usize, start, page_size);
        const prefix = start - aligned_start;
        const advised_len = len + prefix;
        const aligned_ptr: *align(page_size) anyopaque = @ptrFromInt(aligned_start);
        _ = c.madvise(aligned_ptr, advised_len, c_advice);
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

test "readFile preserves non-missing open failures" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    try tmp.dir.writeFile(std.testing.io, .{ .sub_path = "not-a-directory", .data = "file" });

    const path = try std.fs.path.join(allocator, &.{
        ".zig-cache",
        "tmp",
        tmp.sub_path[0..],
        "not-a-directory",
        "metadata.json",
    });
    defer allocator.free(path);

    try std.testing.expectError(error.NotDir, readFile(allocator, path));
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

test "mmapTempCopy maps unlinked temp data" {
    const payload = "temporary mapped payload";
    var region = try mmapTempCopy(std.testing.allocator, "termite-mmap-temp-test", payload);
    defer region.deinit();

    try std.testing.expectEqual(payload.len, region.data.len);
    try std.testing.expectEqualSlices(u8, payload, region.data[0..payload.len]);
}
