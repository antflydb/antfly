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

pub const Error = std.posix.OpenError || error{
    Corrupted,
    PathTooLong,
    WriterLocked,
    Unexpected,
};

const lock_magic: u32 = 0x574c434b; // WLCK
const lock_version: u32 = 1;

const LockState = extern struct {
    magic: u32,
    version: u32,
    owner_pid: u32,
    reserved: u32,
};

pub const WriterLock = struct {
    fd: std.posix.fd_t,

    pub fn release(self: *WriterLock) void {
        clearOwner(self.fd) catch {};
        unlock(self.fd);
        _ = std.posix.system.close(self.fd);
        self.* = undefined;
    }
};

pub fn acquire(data_path: []const u8) Error!WriterLock {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const lock_path = try lockPath(&path_buf, data_path);
    const fd = try std.posix.openat(std.posix.AT.FDCWD, lock_path, .{
        .ACCMODE = .RDWR,
        .CREAT = true,
    }, 0o600);
    errdefer _ = std.posix.system.close(fd);

    try lockExclusiveNonBlocking(fd);
    errdefer unlock(fd);

    try ensureInitialized(fd);
    var state = try readState(fd);
    const pid: u32 = @intCast(std.posix.system.getpid());

    if (state.owner_pid != 0 and !processAlive(state.owner_pid)) {
        state.owner_pid = 0;
    }
    if (state.owner_pid != 0) return error.WriterLocked;

    state.owner_pid = pid;
    try writeState(fd, state);
    return .{ .fd = fd };
}

fn lockPath(buf: []u8, data_path: []const u8) Error![]const u8 {
    if (std.mem.endsWith(u8, data_path, "/data.mdb")) {
        const dir_path = data_path[0 .. data_path.len - "/data.mdb".len];
        return std.fmt.bufPrint(buf, "{s}/writer.mdb", .{dir_path}) catch error.PathTooLong;
    }
    return std.fmt.bufPrint(buf, "{s}-writer", .{data_path}) catch error.PathTooLong;
}

fn lockExclusiveNonBlocking(fd: std.posix.fd_t) Error!void {
    while (true) switch (std.posix.errno(std.posix.system.flock(fd, std.posix.LOCK.EX | std.posix.LOCK.NB))) {
        .SUCCESS => return,
        .INTR => continue,
        .AGAIN => return error.WriterLocked,
        else => return error.Unexpected,
    };
}

fn unlock(fd: std.posix.fd_t) void {
    while (true) switch (std.posix.errno(std.posix.system.flock(fd, std.posix.LOCK.UN))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return,
    };
}

fn ensureInitialized(fd: std.posix.fd_t) Error!void {
    const current_size = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (current_size < 0) return error.Unexpected;
    if (@as(usize, @intCast(current_size)) == 0) {
        try writeState(fd, .{
            .magic = lock_magic,
            .version = lock_version,
            .owner_pid = 0,
            .reserved = 0,
        });
        return;
    }
    if (@as(usize, @intCast(current_size)) != @sizeOf(LockState)) return error.Corrupted;
    const state = try readState(fd);
    if (state.magic != lock_magic or state.version != lock_version) return error.Corrupted;
}

fn readState(fd: std.posix.fd_t) Error!LockState {
    var state: LockState = undefined;
    try readAllAtOffset(fd, std.mem.asBytes(&state), 0);
    return state;
}

fn writeState(fd: std.posix.fd_t, state: LockState) Error!void {
    try writeAllAtOffset(fd, std.mem.asBytes(&state), 0);
    while (true) switch (std.posix.errno(std.posix.system.fsync(fd))) {
        .SUCCESS => return,
        .INTR => continue,
        else => return error.Unexpected,
    };
}

fn clearOwner(fd: std.posix.fd_t) Error!void {
    try ensureInitialized(fd);
    var state = try readState(fd);
    const pid: u32 = @intCast(std.posix.system.getpid());
    if (state.owner_pid == pid) {
        state.owner_pid = 0;
        try writeState(fd, state);
    }
}

fn readAllAtOffset(fd: std.posix.fd_t, bytes: []u8, offset: usize) Error!void {
    var read_len: usize = 0;
    while (read_len < bytes.len) {
        const rc = std.posix.system.pread(fd, bytes.ptr + read_len, bytes.len - read_len, @intCast(offset + read_len));
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const n: usize = @intCast(rc);
                if (n == 0) return error.Corrupted;
                read_len += n;
            },
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn writeAllAtOffset(fd: std.posix.fd_t, bytes: []const u8, offset: usize) Error!void {
    var written: usize = 0;
    while (written < bytes.len) {
        const rc = std.posix.system.pwrite(fd, bytes.ptr + written, bytes.len - written, @intCast(offset + written));
        switch (std.posix.errno(rc)) {
            .SUCCESS => written += @intCast(rc),
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn processAlive(pid: u32) bool {
    if (pid == 0) return false;
    switch (std.posix.errno(std.posix.system.kill(@intCast(pid), @enumFromInt(0)))) {
        .SUCCESS => return true,
        .SRCH => return false,
        .PERM => return true,
        else => return true,
    }
}

fn makePipe() Error![2]std.posix.fd_t {
    var fds: [2]std.posix.fd_t = undefined;
    while (true) switch (std.posix.errno(std.posix.system.pipe(&fds))) {
        .SUCCESS => return fds,
        .INTR => continue,
        else => return error.Unexpected,
    };
}

fn writeSignal(fd: std.posix.fd_t) Error!void {
    const byte: [1]u8 = .{1};
    while (true) {
        const rc = std.posix.system.write(fd, &byte, byte.len);
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                if (rc != 1) return error.Unexpected;
                return;
            },
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}

fn waitSignal(fd: std.posix.fd_t) Error!void {
    var byte: [1]u8 = undefined;
    while (true) {
        const rc = std.posix.system.read(fd, &byte, byte.len);
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                if (rc != 1) return error.Unexpected;
                return;
            },
            .INTR => continue,
            else => return error.Unexpected,
        }
    }
}
test "writer lock rejects a second same-process writer" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const data_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/writer_lock.mdb", .{tmp.sub_path});

    var first = try acquire(data_path);
    defer first.release();
    try std.testing.expectError(error.WriterLocked, acquire(data_path));
}

test "writer lock clears stale owner and can be reacquired" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const data_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/writer_stale.mdb", .{tmp.sub_path});

    var lock_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try lockPath(&lock_path_buf, data_path);
    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDWR,
        .CREAT = true,
    }, 0o600);
    defer _ = std.posix.system.close(fd);

    try writeState(fd, .{
        .magic = lock_magic,
        .version = lock_version,
        .owner_pid = 999_999,
        .reserved = 0,
    });

    var lock = try acquire(data_path);
    defer lock.release();
}

test "writer lock rejects acquisition from another process while held" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const data_path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/writer_fork.mdb", .{tmp.sub_path});

    const child_ready = try makePipe();
    defer {
        _ = std.posix.system.close(child_ready[0]);
        _ = std.posix.system.close(child_ready[1]);
    }
    const child_release = try makePipe();
    defer {
        _ = std.posix.system.close(child_release[0]);
        _ = std.posix.system.close(child_release[1]);
    }

    const pid = std.posix.system.fork();
    if (pid == 0) {
        _ = std.posix.system.close(child_ready[0]);
        _ = std.posix.system.close(child_release[1]);
        var lock = acquire(data_path) catch std.posix.system.exit(1);
        writeSignal(child_ready[1]) catch std.posix.system.exit(2);
        waitSignal(child_release[0]) catch std.posix.system.exit(3);
        lock.release();
        std.posix.system.exit(0);
    }

    try waitSignal(child_ready[0]);
    try std.testing.expectError(error.WriterLocked, acquire(data_path));
    try writeSignal(child_release[1]);

    const WaitStatus = if (builtin.link_libc) c_int else u32;
    var status: WaitStatus = 0;
    while (true) switch (std.posix.errno(std.posix.system.waitpid(@intCast(pid), &status, 0))) {
        .SUCCESS => break,
        .INTR => continue,
        else => return error.Unexpected,
    };
    try std.testing.expectEqual(@as(WaitStatus, 0), status);
}
