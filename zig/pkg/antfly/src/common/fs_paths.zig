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

fn fsPathDebugEnabled() bool {
    if (builtin.os.tag == .freestanding) return false;
    return std.c.getenv("ANTFLY_FS_PATH_DEBUG") != null;
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
    if (!std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.cwd().createDirPath(io, path);
        return;
    }

    if (builtin.os.tag != .windows and builtin.os.tag != .wasi and builtin.os.tag != .freestanding) {
        try createAbsoluteDirPathPosix(path);
        return;
    }

    var idx: usize = 1;
    while (idx < path.len) : (idx += 1) {
        if (path[idx] != std.fs.path.sep) continue;
        if (idx == 1) continue;
        try createDirAbsolutePortable(path[0..idx]);
    }
    try createDirAbsolutePortable(path);
}

pub fn createFilePortable(io: anytype, path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    const base_name = std.fs.path.basename(path);
    if (!std.fs.path.isAbsolute(path)) {
        if (std.fs.path.dirname(path)) |parent_path| {
            var parent = try std.Io.Dir.cwd().openDir(io, parent_path, .{});
            defer parent.close(io);
            return try parent.createFile(io, base_name, flags);
        }
        return try std.Io.Dir.cwd().createFile(io, base_name, flags);
    }

    return try createAbsoluteFilePortable(io, path, flags);
}

pub fn syncDirPortable(io: anytype, path: []const u8) !void {
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return;

    var dir = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openDirAbsolute(io, path, .{ .iterate = true })
    else
        try std.Io.Dir.cwd().openDir(io, if (path.len == 0) "." else path, .{ .iterate = true });
    defer dir.close(io);

    while (true) switch (std.posix.errno(std.posix.system.fsync(dir.handle))) {
        .SUCCESS => return,
        .INTR => continue,
        .INVAL => return,
        .BADF => return error.InvalidFileDescriptor,
        .IO => return error.InputOutput,
        .NOSPC => return error.NoSpaceLeft,
        .DQUOT => return error.DiskQuota,
        else => |err| return std.posix.unexpectedErrno(err),
    };
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
