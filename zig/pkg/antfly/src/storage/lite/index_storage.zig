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

//! LSM `Storage` adapter backed by native `.aflite` index catalog records.
//!
//! This is an incremental Lite-native index backend: existing Antfly index
//! implementations can still use their LSM storage contract, but their logical
//! files are stored under the dedicated native `.aflite` index checkpoint root
//! rather than in the internal bridge container.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const docstore = @import("docstore.zig");
const native = @import("native.zig");
const storage_io = @import("../lsm_backend/storage_io.zig");

const Allocator = std.mem.Allocator;
const AtomicWriteSink = storage_io.AtomicWriteSink;
const StorageIo = storage_io.Storage;

pub const Store = struct {
    allocator: Allocator,
    docs: *docstore.Store,

    const vtable: StorageIo.VTable = .{
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

    pub fn init(allocator: Allocator, docs: *docstore.Store) Store {
        return .{
            .allocator = allocator,
            .docs = docs,
        };
    }

    pub fn storage(self: *Store) StorageIo {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }
};

fn createDirPath(_: *anyopaque, _: []const u8) !void {}

fn ensureWritable(self: *Store) !void {
    if (self.docs.read_only) return error.ReadOnly;
}

fn lockStore(store: *docstore.Store) void {
    platform_sync.lockYielding(&store.mutex);
}

fn pathContains(prefix: []const u8, path: []const u8) bool {
    if (!std.mem.startsWith(u8, path, prefix)) return false;
    if (path.len == prefix.len) return true;
    return path[prefix.len] == '/';
}

fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const stored = (try self.docs.file.getIndexCatalogRecordAlloc(allocator, path)) orelse return error.FileNotFound;
    errdefer allocator.free(stored);
    if (stored.len > max_bytes) return error.FileTooBig;
    return stored;
}

fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const stored = (try self.docs.file.getIndexCatalogRecordAlloc(allocator, path)) orelse return error.FileNotFound;
    defer allocator.free(stored);
    if (offset > std.math.maxInt(usize)) return error.EndOfStream;
    const start: usize = @intCast(offset);
    if (start > stored.len or stored.len - start < len) return error.EndOfStream;
    return try allocator.dupe(u8, stored[start .. start + len]);
}

fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const stored = (try self.docs.file.getIndexCatalogRecordAlloc(self.allocator, path)) orelse return error.FileNotFound;
    defer self.allocator.free(stored);
    return stored.len;
}

fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const stored = (try self.docs.file.getIndexCatalogRecordAlloc(allocator, path)) orelse return error.FileNotFound;
    defer allocator.free(stored);
    if (stored.len < len) return error.EndOfStream;
    return try allocator.dupe(u8, stored[stored.len - len ..]);
}

fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try ensureWritable(self);

    lockStore(self.docs);
    defer self.docs.mutex.unlock();
    try self.docs.file.putIndexCatalogRecord(path, contents);
}

fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
    _ = sync;
    const self: *Store = @ptrCast(@alignCast(ptr));
    try ensureWritable(self);

    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const existing = try self.docs.file.getIndexCatalogRecordAlloc(self.allocator, path);
    defer if (existing) |bytes| self.allocator.free(bytes);
    if (existing) |old| {
        const joined = try self.allocator.alloc(u8, old.len + contents.len);
        defer self.allocator.free(joined);
        @memcpy(joined[0..old.len], old);
        @memcpy(joined[old.len..], contents);
        try self.docs.file.putIndexCatalogRecord(path, joined);
        return;
    }
    try self.docs.file.putIndexCatalogRecord(path, contents);
}

fn beginAtomicWrite(ptr: *anyopaque, allocator: Allocator, path: []const u8) !AtomicWriteSink {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try ensureWritable(self);
    return try NativeAtomicWriteSink.create(allocator, self, path);
}

fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try ensureWritable(self);

    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const stored = (try self.docs.file.getIndexCatalogRecordAlloc(self.allocator, old_path)) orelse return;
    defer self.allocator.free(stored);
    try self.docs.file.putIndexCatalogBatch(&.{
        .{ .key = new_path, .value = stored },
        .{ .key = old_path, .is_delete = true },
    });
}

fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try ensureWritable(self);

    lockStore(self.docs);
    defer self.docs.mutex.unlock();
    try self.docs.file.deleteIndexCatalogRecord(path);
}

fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try ensureWritable(self);

    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const index_records = try self.docs.file.snapshotIndexCatalogRecordsAlloc(self.allocator);
    defer native.NativeFile.freeSnapshotCatalogRecords(self.allocator, index_records);

    var mutations = std.ArrayListUnmanaged(native.CatalogMutation).empty;
    defer {
        for (mutations.items) |mutation| self.allocator.free(mutation.key);
        mutations.deinit(self.allocator);
    }

    for (index_records) |record| {
        if (!pathContains(path, record.key)) continue;
        const key = try self.allocator.dupe(u8, record.key);
        errdefer self.allocator.free(key);
        try mutations.append(self.allocator, .{ .key = key, .is_delete = true });
    }

    try self.docs.file.putIndexCatalogBatch(mutations.items);
}

fn nowNs(_: *anyopaque) u64 {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    const now = std.Io.Timestamp.now(io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

const NativeAtomicWriteSink = struct {
    allocator: Allocator,
    storage: *Store,
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

    fn create(allocator: Allocator, storage: *Store, path: []const u8) !AtomicWriteSink {
        const self = try allocator.create(NativeAtomicWriteSink);
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

    fn deinit(self: *NativeAtomicWriteSink) void {
        self.out.deinit(self.allocator);
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        try self.out.appendSlice(self.allocator, bytes);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        @memcpy(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidAtomicWriteOffset;
        return std.hash.Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();
        try writeFileAbsolute(self.storage, self.path, self.out.items);
    }

    fn abort(ptr: *anyopaque) void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite native index storage persists logical files across reopen" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-storage.aflite");
    defer allocator.free(path);

    {
        var docs = try docstore.Store.open(allocator, path, false);
        defer docs.close();
        var index_store = Store.init(allocator, &docs);
        const storage = index_store.storage();

        try storage.createDirPath("/indexes/ft");
        try storage.writeFileAbsolute("/indexes/ft/a.tbl", "hello");
        try storage.appendFileAbsolute(allocator, "/indexes/ft/a.tbl", " world", true);

        var writer = try storage.beginAtomicWrite(allocator, "/indexes/ft/b.tbl");
        try writer.appendSlice("abc_____");
        try writer.writeAt(3, "def");
        try std.testing.expectEqual(std.hash.Crc32.hash("abcdef__"), try writer.crc32Prefix(writer.len()));
        try writer.finish();

        const checkpoint = docs.file.activeCheckpoint();
        try std.testing.expectEqual(@as(u64, 0), checkpoint.catalog_root_page);
        try std.testing.expect(checkpoint.index_catalog_root_page != 0);
        const records = try docs.file.snapshotIndexCatalogRecordsAlloc(allocator);
        defer native.NativeFile.freeSnapshotCatalogRecords(allocator, records);
        try std.testing.expectEqual(@as(usize, 2), records.len);
    }

    {
        var docs = try docstore.Store.open(allocator, path, true);
        defer docs.close();
        var index_store = Store.init(allocator, &docs);
        const storage = index_store.storage();

        const got = try storage.readFileAlloc(allocator, "/indexes/ft/a.tbl", 64);
        defer allocator.free(got);
        try std.testing.expectEqualStrings("hello world", got);

        const range = try storage.readFileRangeAlloc(allocator, "/indexes/ft/b.tbl", 2, 4);
        defer allocator.free(range);
        try std.testing.expectEqualStrings("cdef", range);
    }
}

test "lite native index storage handles large files rename and delete tree" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-storage-large.aflite");
    defer allocator.free(path);

    const large = try allocator.alloc(u8, native.default_page_size * 3);
    defer allocator.free(large);
    for (large, 0..) |*byte, i| byte.* = @intCast(i % 251);

    var docs = try docstore.Store.open(allocator, path, false);
    defer docs.close();
    var index_store = Store.init(allocator, &docs);
    const storage = index_store.storage();

    try storage.writeFileAbsolute("/dense/a/blob", large);
    try std.testing.expectEqual(@as(u64, @intCast(large.len)), try storage.fileSize("/dense/a/blob"));
    try storage.renameAbsolute("/dense/a/blob", "/dense/a/blob2");
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/dense/a/blob", 8));

    const trailer = try storage.readFileTrailerAlloc(allocator, "/dense/a/blob2", 17);
    defer allocator.free(trailer);
    try std.testing.expectEqualSlices(u8, large[large.len - 17 ..], trailer);

    try storage.writeFileAbsolute("/dense/a/sub/file", "child");
    try storage.deleteTree("/dense/a");
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/dense/a/blob2", 8));
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/dense/a/sub/file", 8));
}
