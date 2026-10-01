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
const format = @import("format.zig");

pub const Error = std.posix.OpenError || error{
    Corrupted,
    PathTooLong,
    ReadersFull,
    Unexpected,
};

pub const SlotId = usize;

pub const Registry = struct {
    fd: std.posix.fd_t,
    slot_id: ?SlotId = null,

    pub fn open(path: []const u8) Error!Registry {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const readers_path = try readersPath(&path_buf, path);
        const fd = try openTable(readers_path);
        return .{ .fd = fd };
    }

    pub fn close(self: *Registry) void {
        self.deactivate();
        _ = std.posix.system.close(self.fd);
        self.* = undefined;
    }

    pub fn activate(self: *Registry, txnid: format.Txnid) Error!SlotId {
        try lockExclusive(self.fd);
        defer unlock(self.fd);

        var slots = try readSlots(self.fd);
        const pid: u32 = @intCast(std.posix.system.getpid());

        if (self.slot_id) |slot_id| {
            if (slot_id < max_readers) {
                const slot = &slots[slot_id];
                if (slot.pid == 0 or slot.pid == pid or !processAlive(slot.pid)) {
                    slot.* = .{ .pid = pid, .txnid = txnid };
                    try writeSlots(self.fd, &slots);
                    return slot_id;
                }
            }
            self.slot_id = null;
        }

        var first_empty: ?usize = null;
        for (&slots, 0..) |*slot, i| {
            if (slot.pid != 0 and !processAlive(slot.pid)) {
                slot.* = .{ .pid = 0, .txnid = 0 };
            }
            if (slot.pid == 0 and first_empty == null) first_empty = i;
        }

        const slot_index = first_empty orelse return error.ReadersFull;
        slots[slot_index] = .{ .pid = pid, .txnid = txnid };
        try writeSlots(self.fd, &slots);
        self.slot_id = slot_index;
        return slot_index;
    }

    pub fn deactivate(self: *Registry) void {
        const slot_id = self.slot_id orelse return;
        if (slot_id >= max_readers) return;

        lockExclusive(self.fd) catch return;
        defer unlock(self.fd);

        var slots = readSlots(self.fd) catch return;
        const pid: u32 = @intCast(std.posix.system.getpid());
        if (slots[slot_id].pid == pid) {
            slots[slot_id] = .{ .pid = 0, .txnid = 0 };
            writeSlots(self.fd, &slots) catch return;
        }
    }
};

const table_magic: u32 = 0x52445253; // RDRS
const table_version: u32 = 1;
const max_readers: usize = 128;

const TableHeader = extern struct {
    magic: u32,
    version: u32,
    slot_count: u32,
    reserved: u32,
};

const ReaderSlot = extern struct {
    pid: u32,
    txnid: format.Txnid,
};

pub fn register(path: []const u8, txnid: format.Txnid) Error!SlotId {
    var registry = try Registry.open(path);
    defer registry.close();
    const slot = try registry.activate(txnid);
    registry.slot_id = null;
    return slot;
}

pub fn unregister(path: []const u8, slot_id: SlotId) void {
    if (slot_id >= max_readers) return;

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const readers_path = readersPath(&path_buf, path) catch return;
    const fd = openTable(readers_path) catch return;
    defer _ = std.posix.system.close(fd);

    lockExclusive(fd) catch return;
    defer unlock(fd);

    var slots = readSlots(fd) catch return;
    const pid: u32 = @intCast(std.posix.system.getpid());
    if (slots[slot_id].pid == pid) {
        slots[slot_id] = .{ .pid = 0, .txnid = 0 };
        writeSlots(fd, &slots) catch {};
    }
}

pub fn oldest(path: []const u8) Error!?format.Txnid {
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const readers_path = try readersPath(&path_buf, path);
    const fd = try openTable(readers_path);
    defer _ = std.posix.system.close(fd);

    try lockExclusive(fd);
    defer unlock(fd);

    var slots = try readSlots(fd);
    var changed = false;
    var result: ?format.Txnid = null;
    for (&slots) |*slot| {
        if (slot.pid == 0) continue;
        if (!processAlive(slot.pid)) {
            slot.* = .{ .pid = 0, .txnid = 0 };
            changed = true;
            continue;
        }
        if (result == null or slot.txnid < result.?) result = slot.txnid;
    }
    if (changed) try writeSlots(fd, &slots);
    return result;
}

fn readersPath(buf: []u8, path: []const u8) Error![]const u8 {
    return std.fmt.bufPrint(buf, "{s}.readers", .{path}) catch error.PathTooLong;
}

fn openTable(path: []const u8) Error!std.posix.fd_t {
    return std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDWR,
        .CREAT = true,
    }, 0o600);
}

fn lockExclusive(fd: std.posix.fd_t) Error!void {
    while (true) switch (std.posix.errno(std.posix.system.flock(fd, std.posix.LOCK.EX))) {
        .SUCCESS => return,
        .INTR => continue,
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

fn tableSize() usize {
    return @sizeOf(TableHeader) + max_readers * @sizeOf(ReaderSlot);
}

fn readSlots(fd: std.posix.fd_t) Error![max_readers]ReaderSlot {
    try ensureTableInitialized(fd);

    var slots: [max_readers]ReaderSlot = undefined;
    const bytes = std.mem.asBytes(&slots);
    try readAllAtOffset(fd, bytes, @sizeOf(TableHeader));
    return slots;
}

fn writeSlots(fd: std.posix.fd_t, slots: *const [max_readers]ReaderSlot) Error!void {
    try ensureTableInitialized(fd);
    try writeAllAtOffset(fd, std.mem.asBytes(slots), @sizeOf(TableHeader));
}

fn ensureTableInitialized(fd: std.posix.fd_t) Error!void {
    const current_size = std.posix.system.lseek(fd, 0, std.posix.SEEK.END);
    if (current_size < 0) return error.Unexpected;

    if (@as(usize, @intCast(current_size)) == 0) {
        var header = TableHeader{
            .magic = table_magic,
            .version = table_version,
            .slot_count = max_readers,
            .reserved = 0,
        };
        var slots = std.mem.zeroes([max_readers]ReaderSlot);
        try writeAllAtOffset(fd, std.mem.asBytes(&header), 0);
        try writeAllAtOffset(fd, std.mem.asBytes(&slots), @sizeOf(TableHeader));
        return;
    }

    if (@as(usize, @intCast(current_size)) != tableSize()) return error.Corrupted;

    var header: TableHeader = undefined;
    try readAllAtOffset(fd, std.mem.asBytes(&header), 0);
    if (header.magic != table_magic or header.version != table_version or header.slot_count != max_readers) {
        return error.Corrupted;
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
test "reader registry persists oldest snapshot by path in shared table" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reader_registry.mdb", .{tmp.sub_path});

    const first = try register(path, 9);
    const second = try register(path, 4);
    try std.testing.expectEqual(@as(?format.Txnid, 4), try oldest(path));

    unregister(path, second);
    try std.testing.expectEqual(@as(?format.Txnid, 9), try oldest(path));

    unregister(path, first);
    try std.testing.expectEqual(@as(?format.Txnid, null), try oldest(path));
}

test "reader registry cleans stale slots" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reader_stale.mdb", .{tmp.sub_path});

    var readers_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const readers_path = try readersPath(&readers_path_buf, path);
    const fd = try openTable(readers_path);
    defer _ = std.posix.system.close(fd);

    var slots = std.mem.zeroes([max_readers]ReaderSlot);
    slots[0] = .{ .pid = 999_999, .txnid = 3 };
    try lockExclusive(fd);
    try writeSlots(fd, &slots);
    unlock(fd);

    try std.testing.expectEqual(@as(?format.Txnid, null), try oldest(path));
}

test "reader registry oldest ignores stale slots when live readers exist" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reader_mixed.mdb", .{tmp.sub_path});

    const first = try register(path, 12);
    const second = try register(path, 7);

    var readers_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const readers_path = try readersPath(&readers_path_buf, path);
    const fd = try openTable(readers_path);
    defer _ = std.posix.system.close(fd);

    try lockExclusive(fd);
    var slots = try readSlots(fd);
    slots[5] = .{ .pid = 999_999, .txnid = 1 };
    try writeSlots(fd, &slots);
    unlock(fd);

    try std.testing.expectEqual(@as(?format.Txnid, 7), try oldest(path));

    unregister(path, second);
    try std.testing.expectEqual(@as(?format.Txnid, 12), try oldest(path));
    unregister(path, first);
}

test "reader registry tracks oldest snapshot across processes" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/reader_fork.mdb", .{tmp.sub_path});

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
        const slot = register(path, 4) catch std.posix.system.exit(1);
        writeSignal(child_ready[1]) catch std.posix.system.exit(2);
        waitSignal(child_release[0]) catch std.posix.system.exit(3);
        unregister(path, slot);
        std.posix.system.exit(0);
    }

    try waitSignal(child_ready[0]);
    const parent_slot = try register(path, 9);
    defer unregister(path, parent_slot);

    try std.testing.expectEqual(@as(?format.Txnid, 4), try oldest(path));
    try writeSignal(child_release[1]);

    const WaitStatus = if (builtin.link_libc) c_int else u32;
    var status: WaitStatus = 0;
    while (true) switch (std.posix.errno(std.posix.system.waitpid(@intCast(pid), &status, 0))) {
        .SUCCESS => break,
        .INTR => continue,
        else => return error.Unexpected,
    };
    try std.testing.expectEqual(@as(WaitStatus, 0), status);
    try std.testing.expectEqual(@as(?format.Txnid, 9), try oldest(path));
}
