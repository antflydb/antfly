// Copyright 2026 Antfly, Inc.
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");

fn fsPathDebugEnabled() bool {
    if (builtin.os.tag == .freestanding) return false;
    return platform.env.getenv("ANTFLY_FS_PATH_DEBUG") != null;
}

fn logPathDebug(comptime event: []const u8, path: []const u8) void {
    if (!fsPathDebugEnabled()) return;
    const ptr_int = @intFromPtr(path.ptr);
    std.log.info("fs_paths {s} ptr=0x{x} len={d}", .{
        event,
        ptr_int,
        path.len,
    });
}

pub fn createDirPathPortable(io: anytype, path: []const u8) !void {
    if (path.len == 0) return;
    // `Dir.createDirPath` accepts relative and absolute paths. Keeping this on
    // the borrowed interface is essential: a VoprIo directory handle is not a
    // kernel descriptor and must never escape into a raw POSIX mkdir path.
    try std.Io.Dir.cwd().createDirPath(io, path);
}

pub fn createFilePortable(io: anytype, path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    // `Dir.createFile` likewise accepts both path forms and dispatches through
    // std.Io, preserving virtual filesystem identity and fault injection.
    return try std.Io.Dir.cwd().createFile(io, path, flags);
}

pub fn syncDirPortable(io: anytype, path: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
        return error.DurableDirectorySyncUnsupported;

    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, if (path.len == 0) "." else path, .{ .iterate = true });
    defer dir.close(io);
    try syncDirectoryWithIo(io, dir);
}

pub fn syncFileFdPortable(fd: std.posix.fd_t) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
        return error.DurableFileSyncUnsupported;
    while (true) switch (std.posix.errno(std.posix.system.fsync(fd))) {
        .SUCCESS => return,
        .INTR => continue,
        .INVAL => return error.DurableFileSyncUnsupported,
        .BADF => return error.InvalidFileDescriptor,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return std.posix.unexpectedErrno(err),
    };
}

pub fn syncDirectoryFdPortable(fd: std.posix.fd_t) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
        return error.DurableDirectorySyncUnsupported;
    while (true) switch (std.posix.errno(std.posix.system.fsync(fd))) {
        .SUCCESS => return,
        .INTR => continue,
        .INVAL => return error.DurableDirectorySyncUnsupported,
        .BADF => return error.InvalidFileDescriptor,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return std.posix.unexpectedErrno(err),
    };
}

/// Syncs a directory represented by a `std.Io.Dir`, including traversal-only
/// handles. On Linux, `openDir` uses `O_PATH` unless iteration is requested,
/// and `Dir.cwd()` is represented by `AT_FDCWD`; neither may be passed directly
/// to `fsync`. Reopening `.` through the held directory keeps resolution bound
/// to the same directory while obtaining a sync-capable descriptor.
pub fn syncDirectoryHandlePortable(io: anytype, dir: std.Io.Dir) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding)
        return error.DurableDirectorySyncUnsupported;

    var sync_dir = try dir.openDir(io, ".", .{
        .iterate = true,
        .follow_symlinks = false,
    });
    defer sync_dir.close(io);
    try syncDirectoryWithIo(io, sync_dir);
}

fn syncDirectoryWithIo(io: anytype, dir: std.Io.Dir) !void {
    // std.Io exposes sync on File rather than Dir. Directory and file handles
    // share the same backend handle type, so this remains a normal vtable call
    // for both Threaded and VoprIo runtimes.
    const file = std.Io.File{
        .handle = dir.handle,
        .flags = .{ .nonblocking = false },
    };
    try file.sync(io);
}

pub fn syncFilePortable(io: anytype, path: []const u8) !void {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    try file.sync(io);
}

pub fn syncFileAndParentPortable(io: anytype, path: []const u8) !void {
    try syncFilePortable(io, path);
    const parent = std.fs.path.dirname(path) orelse if (std.fs.path.isAbsolute(path)) "/" else ".";
    try syncDirPortable(io, parent);
}

pub fn pathsReferToSameExistingFile(allocator: std.mem.Allocator, io: anytype, a: []const u8, b: []const u8) !bool {
    const a_real = realPathAlloc(allocator, io, a) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(a_real);

    const b_real = realPathAlloc(allocator, io, b) catch |err| switch (err) {
        error.FileNotFound, error.NotDir => return false,
        else => return err,
    };
    defer allocator.free(b_real);

    return std.mem.eql(u8, a_real, b_real);
}

fn realPathAlloc(allocator: std.mem.Allocator, io: anytype, path: []const u8) ![:0]u8 {
    if (std.fs.path.isAbsolute(path)) {
        return try std.Io.Dir.realPathFileAbsoluteAlloc(io, path, allocator);
    }
    return try std.Io.Dir.cwd().realPathFileAlloc(io, path, allocator);
}

test "syncDirPortable opens a real directory fd" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(path);

    try syncDirPortable(io_impl.io(), path);
}

test "syncDirectoryHandlePortable supports cwd and traversal-only handles" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    const io = io_impl.io();

    try syncDirectoryHandlePortable(io, std.Io.Dir.cwd());

    const path = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(path);
    var traversal_dir = try std.Io.Dir.cwd().openDir(io, path, .{
        .follow_symlinks = false,
    });
    defer traversal_dir.close(io);
    try syncDirectoryHandlePortable(io, traversal_dir);
}

test "createDirPathPortable creates absolute nested directories" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();

    const cwd_path = try std.Io.Dir.cwd().realPathFileAlloc(io_impl.io(), ".zig-cache/tmp", std.testing.allocator);
    defer std.testing.allocator.free(cwd_path);
    const path = try std.fmt.allocPrint(std.testing.allocator, "{s}/{s}/abs/a/b/c", .{ cwd_path, tmp.sub_path });
    defer std.testing.allocator.free(path);

    try createDirPathPortable(io_impl.io(), path);

    var dir = try std.Io.Dir.openDirAbsolute(io_impl.io(), path, .{});
    defer dir.close(io_impl.io());
}

test "pathsReferToSameExistingFile resolves aliases" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();

    const relative = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer std.testing.allocator.free(relative);
    const absolute = try std.Io.Dir.cwd().realPathFileAlloc(io_impl.io(), relative, std.testing.allocator);
    defer std.testing.allocator.free(absolute);
    const aliased = try std.fmt.allocPrint(std.testing.allocator, "./{s}", .{relative});
    defer std.testing.allocator.free(aliased);

    try std.testing.expect(try pathsReferToSameExistingFile(std.testing.allocator, io_impl.io(), absolute, aliased));
    try std.testing.expect(!try pathsReferToSameExistingFile(std.testing.allocator, io_impl.io(), absolute, "missing"));
}

fn createDirAbsolutePortable(path: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) {
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        std.Io.Dir.createDirAbsolute(io_impl.io(), path, .default_dir) catch |err| switch (err) {
            error.PathAlreadyExists => return,
            else => return err,
        };
        return;
    }

    const allocator = std.heap.page_allocator;
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    while (true) {
        const rc = std.posix.system.mkdir(path_z, std.Io.File.Permissions.default_dir.toMode());
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .EXIST, .ISDIR => return,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .DQUOT, .NOSPC => return error.NoSpaceLeft,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .INVAL => {
                if (std.fs.path.isAbsolute(path) and dirAlreadyExistsPosix(path)) return;
                return error.BadPathName;
            },
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn mkdirPathIgnoreExistingPosix(dir_fd: std.posix.fd_t, path: []const u8) !void {
    const allocator = std.heap.page_allocator;
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    while (true) {
        switch (std.posix.errno(std.posix.system.mkdirat(dir_fd, path_z, std.Io.File.Permissions.default_dir.toMode()))) {
            .SUCCESS, .EXIST, .ISDIR => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .DQUOT, .NOSPC => return error.NoSpaceLeft,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .INVAL => {
                if (dirAlreadyExistsAtPosix(dir_fd, path)) return;
                return error.BadPathName;
            },
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn openDirAtPathPosix(dir_fd: std.posix.fd_t, path: []const u8) !std.posix.fd_t {
    const allocator = std.heap.page_allocator;
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);
    return try std.posix.openatZ(dir_fd, path_z.ptr, .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
}

fn dirAlreadyExistsAtPosix(dir_fd: std.posix.fd_t, path: []const u8) bool {
    const fd = openDirAtPathPosix(dir_fd, path) catch return false;
    _ = std.posix.system.close(fd);
    return true;
}

fn dirAlreadyExistsPosix(path: []const u8) bool {
    const fd = openDirAtPathPosix(std.posix.AT.FDCWD, path) catch return false;
    _ = std.posix.system.close(fd);
    return true;
}

fn mkdirAbsoluteIgnoreExistingPosix(path: []const u8) !void {
    const allocator = std.heap.page_allocator;
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    while (true) {
        switch (std.posix.errno(std.posix.system.mkdir(path_z, std.Io.File.Permissions.default_dir.toMode()))) {
            .SUCCESS, .EXIST, .ISDIR => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .DQUOT, .NOSPC => return error.NoSpaceLeft,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .INVAL => {
                if (std.fs.path.isAbsolute(path) and dirAlreadyExistsPosix(path)) return;
                return error.BadPathName;
            },
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn createAbsoluteDirPathPosix(path: []const u8) !void {
    if (path.len == 0) return;
    if (!std.fs.path.isAbsolute(path)) return error.BadPathName;
    if (path.len == 1 and path[0] == std.fs.path.sep) return;
    logPathDebug("create_absolute_dir_path_begin", path);

    var current_fd = try std.posix.openatZ(std.posix.AT.FDCWD, "/", .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
    defer _ = std.posix.system.close(current_fd);

    var idx: usize = 1;
    while (idx < path.len) {
        while (idx < path.len and path[idx] == std.fs.path.sep) : (idx += 1) {}
        if (idx >= path.len) break;

        const component_start = idx;
        while (idx < path.len and path[idx] != std.fs.path.sep) : (idx += 1) {}
        const component = path[component_start..idx];
        logPathDebug("create_absolute_dir_path_component", component);
        mkdirPathIgnoreExistingPosix(current_fd, component) catch |err| switch (err) {
            error.BadPathName => try mkdirAbsoluteIgnoreExistingPosix(path[0..idx]),
            else => return err,
        };

        const next_fd = try openDirAtPathPosix(current_fd, component);
        _ = std.posix.system.close(current_fd);
        current_fd = next_fd;
    }
}

fn createAbsoluteFilePortable(io: anytype, path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    return createAbsoluteFileViaParentDir(path, flags) catch |err| switch (err) {
        error.BadPathName => createAbsoluteFileViaAbsolutePath(path, flags) catch |fallback_err| switch (fallback_err) {
            error.BadPathName => std.Io.Dir.createFileAbsolute(io, path, flags),
            else => return fallback_err,
        },
        else => return err,
    };
}

fn absoluteCreatePosixFlags(flags: std.Io.Dir.CreateFileOptions) std.posix.O {
    return .{
        .ACCMODE = if (flags.read) .RDWR else .WRONLY,
        .CREAT = true,
        .CLOEXEC = true,
        .TRUNC = flags.truncate,
        .APPEND = !flags.truncate,
        .EXCL = flags.exclusive,
    };
}

fn createAbsoluteFileViaParentDir(path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    const parent_path = std.fs.path.dirname(path) orelse return error.FileNotFound;
    const base_name = std.fs.path.basename(path);
    if (base_name.len == 0) return error.BadPathName;

    const parent_fd = try std.posix.openat(std.posix.AT.FDCWD, parent_path, .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
    const file_fd = std.posix.openat(parent_fd, base_name, absoluteCreatePosixFlags(flags), std.Io.File.Permissions.default_file.toMode()) catch |err| {
        _ = std.posix.system.close(parent_fd);
        return err;
    };
    _ = std.posix.system.close(parent_fd);
    return .{
        .handle = file_fd,
        .flags = .{ .nonblocking = false },
    };
}

fn createAbsoluteFileViaAbsolutePath(path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    const file_fd = try std.posix.openat(
        std.posix.AT.FDCWD,
        path,
        absoluteCreatePosixFlags(flags),
        std.Io.File.Permissions.default_file.toMode(),
    );
    return .{
        .handle = file_fd,
        .flags = .{ .nonblocking = false },
    };
}
