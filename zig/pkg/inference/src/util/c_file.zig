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

/// This advisory-I/O implementation is currently enabled only on Linux;
/// Darwin does not export `posix_fadvise`. Keep capability selection in one
/// compile-time constant so another libc target cannot accidentally retain an
/// unresolved reference.
pub const supports_posix_file_advice = build_options.link_libc and builtin.os.tag == .linux;

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
    pub const MADV_DONTNEED = std.c.MADV.DONTNEED;

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

/// Return the byte offset of a borrowed slice inside its complete mmap.
/// Callers use this to order independent tensor reads by physical file order.
pub fn mappedSliceOffset(full: []const u8, slice: []const u8) ?usize {
    const full_start = @intFromPtr(full.ptr);
    const slice_start = @intFromPtr(slice.ptr);
    if (slice_start < full_start) return null;
    const offset = slice_start - full_start;
    if (offset > full.len or slice.len > full.len - offset) return null;
    return offset;
}

/// Memory-mapped file region. The mapped bytes are valid until `deinit()` is called.
pub const MmapRegion = struct {
    data: []align(std.heap.page_size_min) u8,
    fd: std.posix.fd_t,
    /// Whether deinit should evict clean pages from the kernel page cache.
    /// Model stores default to the historical memory-conservative behavior;
    /// CUDA full-residency admission can explicitly retain the cache so a
    /// replacement worker shares the already-read checkpoint pages.
    discard_on_deinit: bool = true,

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

    /// Override a prior random-access hint for one borrowed mmap slice whose
    /// consumer is about to scan every byte in ascending order. This is used
    /// by resident GPU uploads: applying the hint to the exact tensor span
    /// restores kernel readahead without faulting unrelated model weights.
    pub fn adviseBytesSequential(bytes: []const u8) void {
        if (bytes.len == 0) return;
        advise(@constCast(bytes.ptr), bytes.len, .sequential);
    }

    /// Release clean pages for a consumed file-backed range. The mapping stays
    /// valid: a later access faults the bytes back from the file. This is an
    /// advisory Linux optimization and deliberately cannot fail allocation or
    /// inference when a kernel/filesystem declines either hint.
    pub fn discardFileRange(self: *MmapRegion, offset: usize, len: usize) void {
        if (comptime !supports_posix_file_advice) return;
        if (offset >= self.data.len or len == 0) return;

        const clamped_len = @min(len, self.data.len - offset);
        advise(self.data.ptr + offset, clamped_len, .dont_need);
        _ = c.posix_fadvise(
            self.fd,
            @intCast(offset),
            @intCast(clamped_len),
            c.POSIX_FADV_DONTNEED,
        );
    }

    /// Keep clean file pages eligible for reuse after this mapping is closed.
    /// The pages remain reclaimable by the kernel and are never anonymous
    /// process memory. This is useful for rolling restarts and prefetching.
    pub fn preserveFileCacheOnDeinit(self: *MmapRegion) void {
        self.discard_on_deinit = false;
    }

    pub fn deinit(self: *MmapRegion) void {
        const mapped_len = self.data.len;
        const fd = self.fd;
        // Model eviction must release both the process mapping and its clean
        // page-cache residency. Repeatedly mapping different multi-GiB weight
        // files can otherwise leave recently active file pages charged to the
        // cgroup after munmap(), temporarily exhausting an explicit process
        // envelope even though the evicted model owns no live memory. Both
        // hints are best effort and affect only clean file-backed pages:
        // anonymous, dirty, writeback, and still-shared pages remain charged.
        if (self.discard_on_deinit and comptime supports_posix_file_advice) {
            advise(self.data.ptr, mapped_len, .dont_need);
        }
        std.posix.munmap(self.data);
        if (self.discard_on_deinit and comptime supports_posix_file_advice) {
            _ = c.posix_fadvise(
                fd,
                0,
                @intCast(mapped_len),
                c.POSIX_FADV_DONTNEED,
            );
        }
        closeFd(fd);
        self.* = undefined;
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

pub const FileIdentity = struct {
    size: u64,
    inode: u64,
    mtime_seconds: i64,
    mtime_nanoseconds: u32,
    device_major: u32,
    device_minor: u32,
    quick_fingerprint_sha256: [32]u8,
};

const file_identity_sample_bytes: u64 = 64 * 1024;
const file_identity_sample_points: u64 = 16;

fn fileIdentitySampleOffset(size: u64, sample_len: u64, point: u64, points: u64) u64 {
    if (size <= sample_len or points <= 1) return 0;
    const span = size - sample_len;
    return @intCast((@as(u128, span) * point) / (points - 1));
}

test "file identity sampling spans interior ranges" {
    const size: u64 = 16 * 1024 * 1024;
    const first = fileIdentitySampleOffset(size, file_identity_sample_bytes, 0, file_identity_sample_points);
    const middle = fileIdentitySampleOffset(size, file_identity_sample_bytes, 8, file_identity_sample_points);
    const last = fileIdentitySampleOffset(size, file_identity_sample_bytes, 15, file_identity_sample_points);
    try std.testing.expectEqual(@as(u64, 0), first);
    try std.testing.expect(middle > file_identity_sample_bytes);
    try std.testing.expect(middle < size - file_identity_sample_bytes);
    try std.testing.expectEqual(size - file_identity_sample_bytes, last);
}

/// Stable local identity for immutable deployment artifacts. A prepared model
/// pack binds to this tuple so admission can reject stale sidecars without
/// hashing a multi-gigabyte checkpoint on every process start.
pub fn fileIdentity(allocator: std.mem.Allocator, path: []const u8) !FileIdentity {
    if (comptime builtin.os.tag != .linux) return error.UnsupportedPlatform;
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);
    const linux = std.os.linux;
    var statx = std.mem.zeroes(linux.Statx);
    while (true) {
        switch (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{
            .SIZE = true,
            .INO = true,
            .MTIME = true,
        }, &statx))) {
            .SUCCESS => break,
            .INTR => continue,
            else => return error.StatFailed,
        }
    }
    if (!statx.mask.SIZE or !statx.mask.INO or !statx.mask.MTIME) return error.StatFailed;
    const sample_bytes: usize = @intCast(@min(statx.size, file_identity_sample_bytes));
    const sample = try allocator.alloc(u8, sample_bytes);
    defer allocator.free(sample);
    var hasher = std.crypto.hash.sha2.Sha256.init(.{});
    hasher.update("antfly-file-identity-sampled-v2\x00");
    var size_le: [@sizeOf(u64)]u8 = undefined;
    std.mem.writeInt(u64, &size_le, statx.size, .little);
    hasher.update(&size_le);
    const points: u64 = if (statx.size <= sample_bytes) 1 else file_identity_sample_points;
    for (0..points) |point| {
        const offset = fileIdentitySampleOffset(
            statx.size,
            sample_bytes,
            point,
            points,
        );
        var offset_le: [@sizeOf(u64)]u8 = undefined;
        std.mem.writeInt(u64, &offset_le, offset, .little);
        hasher.update(&offset_le);
        try readRegionFromFd(fd, sample, offset);
        hasher.update(sample);
    }
    var quick_fingerprint: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    hasher.final(&quick_fingerprint);
    return .{
        .size = statx.size,
        .inode = statx.ino,
        .mtime_seconds = statx.mtime.sec,
        .mtime_nanoseconds = statx.mtime.nsec,
        .device_major = statx.dev_major,
        .device_minor = statx.dev_minor,
        .quick_fingerprint_sha256 = quick_fingerprint,
    };
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
    if (comptime !supports_posix_file_advice) return;
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

pub const FilePrefetchResult = struct {
    bytes: u64,
    workers: u8,
};

/// Populate the OS page cache with bounded parallel pread streams. This does
/// not retain userspace buffers and never changes the file. It is intended for
/// server startup prefetch, where CUDA admission happens later and consumes
/// the warmed pages through the normal validated loader.
pub fn prefetchFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    requested_workers: u8,
) !FilePrefetchResult {
    if (comptime !supports_posix_file_advice) return error.UnsupportedPlatform;
    const workers: usize = std.math.clamp(@as(usize, requested_workers), 1, 8);
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    const fd = try openReadOnlyZ(path_z);
    defer closeFd(fd);
    const file_size = try fileSizeFromFd(fd);
    if (file_size == 0) return .{ .bytes = 0, .workers = @intCast(workers) };
    _ = c.posix_fadvise(fd, 0, @intCast(file_size), c.POSIX_FADV_WILLNEED);

    const Worker = struct {
        fd: std.posix.fd_t,
        start: u64,
        end: u64,
        bytes: u64 = 0,
        failure: ?anyerror = null,

        fn run(self: *@This()) void {
            const buffer = std.heap.page_allocator.alloc(u8, 8 * 1024 * 1024) catch {
                self.failure = error.OutOfMemory;
                return;
            };
            defer std.heap.page_allocator.free(buffer);
            var offset = self.start;
            while (offset < self.end) {
                const remaining: usize = @intCast(@min(
                    self.end - offset,
                    @as(u64, buffer.len),
                ));
                const n = readAt(self.fd, buffer[0..remaining], offset) catch |err| {
                    self.failure = err;
                    return;
                };
                if (n == 0) {
                    self.failure = error.IncompleteRead;
                    return;
                }
                offset += n;
                self.bytes += n;
            }
        }
    };

    if (workers == 1) {
        var state = Worker{ .fd = fd, .start = 0, .end = file_size };
        state.run();
        if (state.failure) |err| return err;
        if (state.bytes != file_size) return error.IncompleteRead;
        return .{ .bytes = state.bytes, .workers = 1 };
    }

    const states = try allocator.alloc(Worker, workers);
    defer allocator.free(states);
    const threads = try allocator.alloc(std.Thread, workers);
    defer allocator.free(threads);
    var spawned: usize = 0;
    defer for (threads[0..spawned]) |thread| thread.join();
    for (states, 0..) |*state, index| {
        const start = (@as(u64, file_size) * @as(u64, @intCast(index))) /
            @as(u64, @intCast(workers));
        const end = (@as(u64, file_size) * @as(u64, @intCast(index + 1))) /
            @as(u64, @intCast(workers));
        state.* = .{ .fd = fd, .start = start, .end = end };
        threads[index] = try std.Thread.spawn(.{}, Worker.run, .{state});
        spawned += 1;
    }
    for (threads[0..spawned]) |thread| thread.join();
    spawned = 0;

    var total: u64 = 0;
    for (states) |state| {
        if (state.failure) |err| return err;
        total += state.bytes;
    }
    if (total != file_size) return error.IncompleteRead;
    return .{ .bytes = total, .workers = @intCast(workers) };
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

/// Atomically publish a path without replacing an existing destination.
/// Prepared checkpoint directories use this after fully syncing a sibling
/// temporary directory, so concurrent creators cannot clobber each other.
pub fn renameNoReplace(allocator: std.mem.Allocator, old_path: []const u8, new_path: []const u8) !void {
    if (comptime builtin.os.tag != .linux) return error.UnsupportedPlatform;
    const old_z = try allocator.dupeZ(u8, old_path);
    defer allocator.free(old_z);
    const new_z = try allocator.dupeZ(u8, new_path);
    defer allocator.free(new_z);
    const linux = std.os.linux;
    switch (linux.errno(linux.renameat2(
        linux.AT.FDCWD,
        old_z.ptr,
        linux.AT.FDCWD,
        new_z.ptr,
        .{ .NOREPLACE = true },
    ))) {
        .SUCCESS => {},
        .EXIST => return error.PathAlreadyExists,
        .NOENT => return error.FileNotFound,
        .XDEV => return error.RenameAcrossMountPoints,
        .NOTDIR => return error.NotDir,
        .ACCES, .PERM => return error.AccessDenied,
        else => return error.RenameFailed,
    }
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

const Advice = enum { sequential, random, dont_need };

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
    // Prefer the raw Linux syscall even in libc-linked builds so the hot
    // prefetch/read path can distinguish and retry EINTR deterministically.
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
    if (comptime build_options.link_libc) {
        const n = c.pread(fd, buf.ptr, buf.len, @intCast(offset));
        if (n < 0) return error.ReadFailed;
        return @intCast(n);
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
            .dont_need => c.MADV_DONTNEED,
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

test "MmapRegion advice preserves readable mapped data" {
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

    // DONTNEED only releases clean page-cache residency. It must not invalidate
    // the mapping or change the file-backed contents.
    region.discardFileRange(7, 13);
    try std.testing.expectEqualSlices(u8, payload, region.data[0..payload.len]);
    try std.testing.expect(region.discard_on_deinit);
    region.preserveFileCacheOnDeinit();
    try std.testing.expect(!region.discard_on_deinit);
}

test "mmapTempCopy maps unlinked temp data" {
    const payload = "temporary mapped payload";
    var region = try mmapTempCopy(std.testing.allocator, "termite-mmap-temp-test", payload);
    defer region.deinit();

    try std.testing.expectEqual(payload.len, region.data.len);
    try std.testing.expectEqualSlices(u8, payload, region.data[0..payload.len]);
}

test "prefetchFile reads every byte with bounded workers" {
    if (!comptime build_options.link_libc or builtin.os.tag != .linux)
        return error.SkipZigTest;
    const allocator = std.testing.allocator;
    const path = try std.fmt.allocPrint(
        allocator,
        "/tmp/antfly-prefetch-test-{d}.bin",
        .{std.posix.system.getpid()},
    );
    defer allocator.free(path);
    defer std.Io.Dir.deleteFileAbsolute(std.testing.io, path) catch {};
    const payload = try allocator.alloc(u8, 1024 * 1024 + 37);
    defer allocator.free(payload);
    for (payload, 0..) |*byte, index| byte.* = @truncate(index);
    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{ .truncate = true });
    try file.writeStreamingAll(std.testing.io, payload);
    file.close(std.testing.io);

    const result = try prefetchFile(allocator, path, 4);
    try std.testing.expectEqual(@as(u64, payload.len), result.bytes);
    try std.testing.expectEqual(@as(u8, 4), result.workers);
}
