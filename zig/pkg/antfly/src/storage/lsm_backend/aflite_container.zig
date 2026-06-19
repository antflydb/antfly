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

//! Single-file storage container for Antfly Lite.
//!
//! This is the first production-shaped `.aflite` storage primitive. It presents
//! the existing LSM `Storage` interface over one physical file by replaying an
//! append-only stream of logical-file records into an in-memory name index.
//! Later work can add free-space reuse and checkpoint/vacuum without changing
//! the higher-level LSM storage contract.

const std = @import("std");
const builtin = @import("builtin");
const fs_paths = @import("../../common/fs_paths.zig");
const platform_sync = @import("antfly_platform").sync;
const storage_io = @import("storage_io.zig");

const Allocator = std.mem.Allocator;
const AtomicWriteSink = storage_io.AtomicWriteSink;
const Storage = storage_io.Storage;

const file_magic = "AFLITE\x00\x01";
const record_magic = "AFLR";
const record_header_no_crc_size: usize = 21;
const record_header_size: usize = 25;
const max_record_payload_bytes: usize = std.math.maxInt(u32);

const RecordKind = enum(u8) {
    put = 1,
    delete = 2,
    rename = 3,
    delete_tree = 4,
};

pub const ContainerStorage = struct {
    pub const Options = struct {
        read_only: bool = false,
    };

    allocator: Allocator,
    io_impl: std.Io.Threaded,
    path: []u8,
    lock_file: ?std.Io.File = null,
    read_only: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    files: std.StringHashMapUnmanaged([]u8) = .empty,

    const vtable: Storage.VTable = .{
        .create_dir_path = createDirPath,
        .read_file_alloc = readFileAlloc,
        .read_file_range_alloc = readFileRangeAlloc,
        .file_size = fileSize,
        .read_file_trailer_alloc = readFileTrailerAlloc,
        .write_file_absolute = writeFileAbsolute,
        .append_file_absolute = appendFileAbsolute,
        .begin_atomic_write = beginAtomicWrite,
        .rename_absolute = renameAbsolute,
        .delete_file_absolute = deleteFileAbsolute,
        .delete_tree = deleteTree,
        .now_ns = nowNs,
    };

    pub fn open(allocator: Allocator, path: []const u8) !ContainerStorage {
        return try openWithOptions(allocator, path, .{});
    }

    pub fn openReadOnly(allocator: Allocator, path: []const u8) !ContainerStorage {
        return try openWithOptions(allocator, path, .{ .read_only = true });
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, options: Options) !ContainerStorage {
        var io_impl = std.Io.Threaded.init(allocator, .{});
        errdefer io_impl.deinit();

        const owned_path = try allocator.dupe(u8, path);
        var owned_path_transferred = false;
        errdefer if (!owned_path_transferred) allocator.free(owned_path);

        if (!options.read_only) {
            try ensureParentDir(io_impl.io(), owned_path);
        }

        var self = ContainerStorage{
            .allocator = allocator,
            .io_impl = io_impl,
            .path = owned_path,
            .read_only = options.read_only,
        };
        owned_path_transferred = true;
        errdefer self.deinit();

        self.lock_file = try openLockFile(self.io_impl.io(), self.path, options.read_only);
        try self.loadOrCreate(!options.read_only);
        return self;
    }

    pub fn deinit(self: *ContainerStorage) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.files.deinit(self.allocator);
        if (self.lock_file) |file| {
            file.unlock(self.io_impl.io());
            file.close(self.io_impl.io());
        }
        self.allocator.free(self.path);
        self.io_impl.deinit();
        self.* = undefined;
    }

    pub fn storage(self: *ContainerStorage) Storage {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn loadOrCreate(self: *ContainerStorage, create_if_missing: bool) !void {
        const raw = std.Io.Dir.cwd().readFileAlloc(
            self.io_impl.io(),
            self.path,
            self.allocator,
            .limited(std.math.maxInt(usize)),
        ) catch |err| switch (err) {
            error.FileNotFound => {
                if (!create_if_missing) return error.FileNotFound;
                try writeFile(self.io_impl.io(), self.path, file_magic);
                return;
            },
            else => return err,
        };
        defer self.allocator.free(raw);

        if (raw.len == 0 and create_if_missing) {
            try writeFile(self.io_impl.io(), self.path, file_magic);
            return;
        }
        if (raw.len < file_magic.len or !std.mem.eql(u8, raw[0..file_magic.len], file_magic)) {
            return error.InvalidAfliteContainer;
        }

        var offset: usize = file_magic.len;
        while (offset < raw.len) {
            const next = try self.applyRecord(raw, offset) orelse break;
            offset = next;
        }
    }

    fn applyRecord(self: *ContainerStorage, raw: []const u8, offset: usize) !?usize {
        if (raw.len - offset < record_header_size) return null;
        const header = raw[offset .. offset + record_header_size];
        if (!std.mem.eql(u8, header[0..record_magic.len], record_magic)) return error.InvalidAfliteContainer;

        const kind_raw = header[4];
        const path_len = std.mem.readInt(u32, header[5..9], .little);
        const aux_len = std.mem.readInt(u32, header[9..13], .little);
        const value_len = std.mem.readInt(u64, header[13..21], .little);
        const expected_crc = std.mem.readInt(u32, header[21..25], .little);
        const payload_len_u64 = @as(u64, path_len) + @as(u64, aux_len) + value_len;
        if (payload_len_u64 > max_record_payload_bytes) return error.RecordTooLarge;
        const payload_len: usize = @intCast(payload_len_u64);
        const end = offset + record_header_size + payload_len;
        if (end > raw.len) return null;

        var crc = std.hash.Crc32.init();
        crc.update(header[0..record_header_no_crc_size]);
        crc.update(raw[offset + record_header_size .. end]);
        if (crc.final() != expected_crc) return null;

        const payload = raw[offset + record_header_size .. end];
        const path = payload[0..path_len];
        const aux = payload[path.len .. path.len + aux_len];
        const value = payload[path.len + aux.len ..];

        const kind: RecordKind = switch (kind_raw) {
            @intFromEnum(RecordKind.put) => .put,
            @intFromEnum(RecordKind.delete) => .delete,
            @intFromEnum(RecordKind.rename) => .rename,
            @intFromEnum(RecordKind.delete_tree) => .delete_tree,
            else => return error.InvalidAfliteContainer,
        };

        switch (kind) {
            .put => try self.putInMemory(path, value),
            .delete => self.deleteInMemory(path),
            .rename => try self.renameInMemory(path, aux),
            .delete_tree => try self.deleteTreeInMemory(path),
        }
        return end;
    }

    fn appendRecord(self: *ContainerStorage, kind: RecordKind, path: []const u8, aux: []const u8, value: []const u8) !void {
        if (path.len > std.math.maxInt(u32) or aux.len > std.math.maxInt(u32)) return error.RecordTooLarge;
        const payload_len_u64 = @as(u64, path.len) + @as(u64, aux.len) + @as(u64, value.len);
        if (payload_len_u64 > max_record_payload_bytes) return error.RecordTooLarge;

        const record_len = record_header_size + @as(usize, @intCast(payload_len_u64));
        const record = try self.allocator.alloc(u8, record_len);
        defer self.allocator.free(record);

        @memcpy(record[0..4], record_magic);
        record[4] = @intFromEnum(kind);
        std.mem.writeInt(u32, record[5..9], @intCast(path.len), .little);
        std.mem.writeInt(u32, record[9..13], @intCast(aux.len), .little);
        std.mem.writeInt(u64, record[13..21], value.len, .little);
        @memcpy(record[record_header_size..][0..path.len], path);
        @memcpy(record[record_header_size + path.len ..][0..aux.len], aux);
        @memcpy(record[record_header_size + path.len + aux.len ..][0..value.len], value);

        var crc = std.hash.Crc32.init();
        crc.update(record[0..record_header_no_crc_size]);
        crc.update(record[record_header_size..]);
        std.mem.writeInt(u32, record[21..25], crc.final(), .little);

        try appendFile(self.io_impl.io(), self.path, record, true);
    }

    fn putDurable(self: *ContainerStorage, path: []const u8, value: []const u8) !void {
        try self.appendRecord(.put, path, "", value);
        try self.putInMemory(path, value);
    }

    fn putInMemory(self: *ContainerStorage, path: []const u8, value: []const u8) !void {
        const owned_path = try self.allocator.dupe(u8, path);
        errdefer self.allocator.free(owned_path);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);

        const gop = try self.files.getOrPut(self.allocator, owned_path);
        if (gop.found_existing) {
            self.allocator.free(owned_path);
            self.allocator.free(gop.value_ptr.*);
            gop.value_ptr.* = owned_value;
        } else {
            gop.value_ptr.* = owned_value;
        }
    }

    fn deleteDurable(self: *ContainerStorage, path: []const u8) !void {
        if (!self.files.contains(path)) return error.FileNotFound;
        try self.appendRecord(.delete, path, "", "");
        self.deleteInMemory(path);
    }

    fn deleteInMemory(self: *ContainerStorage, path: []const u8) void {
        const removed = self.files.fetchRemove(path) orelse return;
        self.allocator.free(removed.key);
        self.allocator.free(removed.value);
    }

    fn renameDurable(self: *ContainerStorage, old_path: []const u8, new_path: []const u8) !void {
        if (!self.files.contains(old_path)) return error.FileNotFound;
        try self.appendRecord(.rename, old_path, new_path, "");
        try self.renameInMemory(old_path, new_path);
    }

    fn renameInMemory(self: *ContainerStorage, old_path: []const u8, new_path: []const u8) !void {
        const removed = self.files.fetchRemove(old_path) orelse return;
        const old_key = removed.key;
        const value = removed.value;

        const new_key = try self.allocator.dupe(u8, new_path);
        errdefer self.allocator.free(new_key);
        const gop = try self.files.getOrPut(self.allocator, new_key);
        if (gop.found_existing) {
            self.allocator.free(new_key);
            self.allocator.free(gop.value_ptr.*);
            gop.value_ptr.* = value;
        } else {
            gop.value_ptr.* = value;
        }
        self.allocator.free(old_key);
    }

    fn deleteTreeDurable(self: *ContainerStorage, path: []const u8) !void {
        try self.appendRecord(.delete_tree, path, "", "");
        try self.deleteTreeInMemory(path);
    }

    fn deleteTreeInMemory(self: *ContainerStorage, path: []const u8) !void {
        var doomed = std.ArrayListUnmanaged([]const u8).empty;
        defer doomed.deinit(self.allocator);

        var it = self.files.iterator();
        while (it.next()) |entry| {
            if (!pathContains(path, entry.key_ptr.*)) continue;
            try doomed.append(self.allocator, entry.key_ptr.*);
        }

        for (doomed.items) |doomed_key| {
            const removed = self.files.fetchRemove(doomed_key) orelse continue;
            self.allocator.free(removed.key);
            self.allocator.free(removed.value);
        }
    }
};

fn createDirPath(_: *anyopaque, _: []const u8) !void {}

fn ensureWritable(self: *ContainerStorage) !void {
    if (self.read_only) return error.ReadOnly;
}

fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    if (stored.len > max_bytes) return error.FileTooBig;
    return try allocator.dupe(u8, stored);
}

fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    const start: usize = @intCast(offset);
    if (start > stored.len or stored.len - start < len) return error.EndOfStream;
    return try allocator.dupe(u8, stored[start .. start + len]);
}

fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    return stored.len;
}

fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    if (stored.len < len) return error.EndOfStream;
    return try allocator.dupe(u8, stored[stored.len - len ..]);
}

fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    try ensureWritable(self);
    try self.putDurable(path, contents);
}

fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
    _ = sync;
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    try ensureWritable(self);

    if (self.files.get(path)) |old| {
        const joined = try self.allocator.alloc(u8, old.len + contents.len);
        defer self.allocator.free(joined);
        @memcpy(joined[0..old.len], old);
        @memcpy(joined[old.len..], contents);
        try self.putDurable(path, joined);
        return;
    }
    try self.putDurable(path, contents);
}

fn beginAtomicWrite(ptr: *anyopaque, allocator: Allocator, path: []const u8) !AtomicWriteSink {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    try ensureWritable(self);
    return try ContainerAtomicWriteSink.create(allocator, self, path);
}

fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    try ensureWritable(self);
    try self.renameDurable(old_path, new_path);
}

fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    try ensureWritable(self);
    try self.deleteDurable(path);
}

fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    try ensureWritable(self);
    try self.deleteTreeDurable(path);
}

fn nowNs(ptr: *anyopaque) u64 {
    const self: *ContainerStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    const now = std.Io.Timestamp.now(self.io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

const ContainerAtomicWriteSink = struct {
    allocator: Allocator,
    storage: *ContainerStorage,
    path: []u8,
    out: std.ArrayListUnmanaged(u8) = .empty,

    const vtable: AtomicWriteSink.VTable = .{
        .len = len,
        .append_slice = appendSlice,
        .write_at = writeAt,
        .crc32_prefix = crc32Prefix,
        .finish = finish,
        .abort = abort,
    };

    fn create(allocator: Allocator, storage: *ContainerStorage, path: []const u8) !AtomicWriteSink {
        const self = try allocator.create(ContainerAtomicWriteSink);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .storage = storage,
            .path = try allocator.dupe(u8, path),
        };
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }

    fn deinit(self: *ContainerAtomicWriteSink) void {
        self.out.deinit(self.allocator);
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *ContainerAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *ContainerAtomicWriteSink = @ptrCast(@alignCast(ptr));
        try self.out.appendSlice(self.allocator, bytes);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *ContainerAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        @memcpy(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *ContainerAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidAtomicWriteOffset;
        return std.hash.Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *ContainerAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();

        const locked = lockAtomic(&self.storage.mutex);
        defer if (locked) self.storage.mutex.unlock();
        try ensureWritable(self.storage);
        try self.storage.putDurable(self.path, self.out.items);
    }

    fn abort(ptr: *anyopaque) void {
        const self: *ContainerAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

fn lockAtomic(mutex: *std.atomic.Mutex) bool {
    if (builtin.os.tag == .freestanding) return false;
    platform_sync.lockYielding(mutex);
    return true;
}

fn pathContains(prefix: []const u8, path: []const u8) bool {
    if (!std.mem.startsWith(u8, path, prefix)) return false;
    if (path.len == prefix.len) return true;
    return path[prefix.len] == '/';
}

fn ensureParentDir(io: std.Io, path: []const u8) !void {
    const parent = std.fs.path.dirname(path) orelse return;
    if (parent.len == 0) return;
    try fs_paths.createDirPathPortable(io, parent);
}

fn openLockFile(io: std.Io, path: []const u8, read_only: bool) !?std.Io.File {
    return openLockedFile(io, path, read_only) catch |err| switch (err) {
        error.FileLocksUnsupported => {
            try prepareUnlockedFile(io, path, read_only);
            return null;
        },
        else => return err,
    };
}

fn openLockedFile(io: std.Io, path: []const u8, read_only: bool) !std.Io.File {
    const lock: std.Io.File.Lock = if (read_only) .shared else .exclusive;
    if (read_only) {
        var file = try openFilePortable(io, path, .{
            .mode = .read_only,
            .lock = lock,
            .lock_nonblocking = true,
        });
        errdefer file.close(io);
        return file;
    }

    var file = try fs_paths.createFilePortable(io, path, .{
        .read = true,
        .truncate = false,
        .lock = lock,
        .lock_nonblocking = true,
    });
    errdefer file.close(io);
    return file;
}

fn prepareUnlockedFile(io: std.Io, path: []const u8, read_only: bool) !void {
    if (read_only) {
        var file = try openFilePortable(io, path, .{ .mode = .read_only });
        defer file.close(io);
        return;
    }

    var file = try fs_paths.createFilePortable(io, path, .{
        .read = true,
        .truncate = false,
    });
    defer file.close(io);
}

fn openFilePortable(io: std.Io, path: []const u8, flags: std.Io.Dir.OpenFileOptions) !std.Io.File {
    const base_name = std.fs.path.basename(path);
    if (!std.fs.path.isAbsolute(path)) {
        if (std.fs.path.dirname(path)) |parent_path| {
            var parent = try std.Io.Dir.cwd().openDir(io, parent_path, .{});
            defer parent.close(io);
            return try parent.openFile(io, base_name, flags);
        }
        return try std.Io.Dir.cwd().openFile(io, base_name, flags);
    }

    return try std.Io.Dir.openFileAbsolute(io, path, flags);
}

fn writeFile(io: std.Io, path: []const u8, contents: []const u8) !void {
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = true });
    defer file.close(io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &file_buf);
    try writer.interface.writeAll(contents);
    try writer.end();
    try file.sync(io);
}

fn appendFile(io: std.Io, path: []const u8, contents: []const u8, sync: bool) !void {
    var file = try fs_paths.createFilePortable(io, path, .{ .truncate = false });
    defer file.close(io);

    const size = (try file.stat(io)).size;
    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &file_buf);
    try writer.seekTo(size);
    try writer.interface.writeAll(contents);
    try writer.end();
    if (sync) try file.sync(io);
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "aflite container storage persists logical files across reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "app.aflite");
    defer alloc.free(path);

    {
        var container = try ContainerStorage.open(alloc, path);
        defer container.deinit();
        const storage = container.storage();
        try storage.createDirPath("/lsm");
        try storage.writeFileAbsolute("/lsm/a.table", "hello world");
        try storage.appendFileAbsolute(alloc, "/lsm/a.table", "!", true);
        const ell = try storage.readFileRangeAlloc(alloc, "/lsm/a.table", 1, 3);
        defer alloc.free(ell);
        try std.testing.expectEqualStrings("ell", ell);
        try std.testing.expectEqual(@as(u64, 12), try storage.fileSize("/lsm/a.table"));
    }

    {
        var reopened = try ContainerStorage.open(alloc, path);
        defer reopened.deinit();
        const storage = reopened.storage();
        const got = try storage.readFileAlloc(alloc, "/lsm/a.table", 64);
        defer alloc.free(got);
        try std.testing.expectEqualStrings("hello world!", got);
        const trailer = try storage.readFileTrailerAlloc(alloc, "/lsm/a.table", 6);
        defer alloc.free(trailer);
        try std.testing.expectEqualStrings("world!", trailer);
    }
}

test "aflite container storage supports rename delete tree and atomic writer" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "ops.aflite");
    defer alloc.free(path);

    var container = try ContainerStorage.open(alloc, path);
    defer container.deinit();
    const storage = container.storage();

    try storage.writeFileAbsolute("/root/a", "alpha");
    try storage.renameAbsolute("/root/a", "/root/b");
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(alloc, "/root/a", 64));
    const renamed = try storage.readFileAlloc(alloc, "/root/b", 64);
    defer alloc.free(renamed);
    try std.testing.expectEqualStrings("alpha", renamed);

    var writer = try storage.beginAtomicWrite(alloc, "/root/sub/c");
    try writer.appendSlice("hello _____");
    try writer.writeAt(6, "world");
    try std.testing.expectEqual(std.hash.Crc32.hash("hello world"), try writer.crc32Prefix(writer.len()));
    try writer.finish();

    const atomic = try storage.readFileAlloc(alloc, "/root/sub/c", 64);
    defer alloc.free(atomic);
    try std.testing.expectEqualStrings("hello world", atomic);

    try storage.deleteTree("/root/sub");
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(alloc, "/root/sub/c", 64));
}

test "aflite container storage ignores truncated tail record on reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "truncated.aflite");
    defer alloc.free(path);

    {
        var container = try ContainerStorage.open(alloc, path);
        defer container.deinit();
        try container.storage().writeFileAbsolute("/good", "value");
    }

    var io_impl = std.Io.Threaded.init(alloc, .{});
    defer io_impl.deinit();
    try appendFile(io_impl.io(), path, "partial-record", true);

    var reopened = try ContainerStorage.open(alloc, path);
    defer reopened.deinit();
    const got = try reopened.storage().readFileAlloc(alloc, "/good", 64);
    defer alloc.free(got);
    try std.testing.expectEqualStrings("value", got);
}

test "aflite container read-only open requires existing file and rejects writes" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "readonly.aflite");
    defer alloc.free(path);

    try std.testing.expectError(error.FileNotFound, ContainerStorage.openReadOnly(alloc, path));

    {
        var container = try ContainerStorage.open(alloc, path);
        defer container.deinit();
        try container.storage().writeFileAbsolute("/doc", "value");
    }

    var readonly = try ContainerStorage.openReadOnly(alloc, path);
    defer readonly.deinit();
    const storage = readonly.storage();

    const got = try storage.readFileAlloc(alloc, "/doc", 64);
    defer alloc.free(got);
    try std.testing.expectEqualStrings("value", got);
    try std.testing.expectError(error.ReadOnly, storage.writeFileAbsolute("/doc", "new"));
    try std.testing.expectError(error.ReadOnly, storage.appendFileAbsolute(alloc, "/doc", "!", true));
    try std.testing.expectError(error.ReadOnly, storage.renameAbsolute("/doc", "/other"));
    try std.testing.expectError(error.ReadOnly, storage.deleteFileAbsolute("/doc"));
    try std.testing.expectError(error.ReadOnly, storage.deleteTree("/"));
    try std.testing.expectError(error.ReadOnly, storage.beginAtomicWrite(alloc, "/atomic"));
}
