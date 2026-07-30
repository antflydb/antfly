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

//! Internal index `Storage` adapter backed by native `.aflite` catalog pages.
//!
//! This is an incremental Lite-native index backend: existing Antfly index
//! implementations can still use their LSM storage contract, but their logical
//! files are stored under the dedicated native `.aflite` index checkpoint root
//! rather than in the internal bridge container. Public Lite status reports the
//! native catalog-page layout; this adapter is not a user-visible file format.

const std = @import("std");
const platform_sync = @import("antfly_platform").sync;
const byte_copy = @import("../../common/byte_copy.zig");
const docstore = @import("docstore.zig");
const native = @import("native.zig");
const storage_io = @import("../lsm_backend/storage_io.zig");

const Allocator = std.mem.Allocator;
const AtomicWriteSink = storage_io.AtomicWriteSink;
const StorageIo = storage_io.Storage;

pub const Store = struct {
    allocator: Allocator,
    docs: *docstore.Store,
    namespace_prefix: []const u8,

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
        .sync_contents_absolute = syncContentsAbsolute,
        .sync_parent_absolute = syncParentAbsolute,
        .now_ns = nowNs,
        .root_identity_alloc = rootIdentityAlloc,
        .rename_is_atomic = true,
    };

    pub fn init(allocator: Allocator, docs: *docstore.Store) Store {
        return initWithNamespace(allocator, docs, "");
    }

    pub fn initWithNamespace(allocator: Allocator, docs: *docstore.Store, namespace_prefix: []const u8) Store {
        return .{
            .allocator = allocator,
            .docs = docs,
            .namespace_prefix = namespace_prefix,
        };
    }

    pub fn storage(self: *Store) StorageIo {
        return .{
            .ptr = self,
            .vtable = &vtable,
        };
    }
};

fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
}

fn lockStore(store: *docstore.Store) void {
    platform_sync.lockYielding(&store.mutex);
}

fn pinCheckpoint(store: *docstore.Store) native.CheckpointSlot {
    lockStore(store);
    defer store.mutex.unlock();
    return store.file.activeCheckpoint();
}

fn pathContains(prefix: []const u8, path: []const u8) bool {
    if (prefix.len == 0) return true;
    if (!std.mem.startsWith(u8, path, prefix)) return false;
    if (path.len == prefix.len) return true;
    return path[prefix.len] == '/';
}

fn validateIndexPath(self: *const Store, path: []const u8) !void {
    if (path.len == 0) return error.InvalidNativeIndexPath;
    if (std.mem.indexOfScalar(u8, path, 0) != null) return error.InvalidNativeIndexPath;
    if (!pathContains(self.namespace_prefix, path)) return error.InvalidNativeIndexPath;
    if (self.namespace_prefix.len != 0) {
        if (path.len == self.namespace_prefix.len) return;
        var it = std.mem.splitScalar(u8, path[self.namespace_prefix.len + 1 ..], '/');
        while (it.next()) |segment| {
            if (segment.len == 0) return error.InvalidNativeIndexPath;
            if (std.mem.eql(u8, segment, ".") or std.mem.eql(u8, segment, "..")) return error.InvalidNativeIndexPath;
        }
    }
}

fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    const io = self.docs.file.io_impl.io();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);

    const stored = (try self.docs.file.getIndexCatalogRecordAtCheckpointAlloc(allocator, path, checkpoint)) orelse return error.FileNotFound;
    errdefer allocator.free(stored);
    if (stored.len > max_bytes) return error.FileTooBig;
    return stored;
}

fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    const io = self.docs.file.io_impl.io();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);

    return (try self.docs.file.getIndexCatalogRecordRangeAtCheckpointAlloc(allocator, path, offset, len, checkpoint)) orelse return error.FileNotFound;
}

fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    const io = self.docs.file.io_impl.io();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);

    const size = (try self.docs.file.getIndexCatalogRecordSizeAtCheckpoint(path, checkpoint)) orelse return error.FileNotFound;
    return @intCast(size);
}

fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    const io = self.docs.file.io_impl.io();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);

    const size = (try self.docs.file.getIndexCatalogRecordSizeAtCheckpoint(path, checkpoint)) orelse return error.FileNotFound;
    if (size < len) return error.EndOfStream;
    return .{
        .bytes = (try self.docs.file.getIndexCatalogRecordRangeAtCheckpointAlloc(allocator, path, @intCast(size - len), len, checkpoint)) orelse return error.FileNotFound,
        .file_size = size,
    };
}

fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    try writeFileReserved(self, path, contents);
}

fn writeFileReserved(self: *Store, path: []const u8, contents: []const u8) !void {
    lockStore(self.docs);
    defer self.docs.mutex.unlock();
    try self.docs.file.putIndexCatalogRecord(path, contents);
}

fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
    _ = sync;
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    try self.docs.file.appendIndexCatalogRecord(path, contents);
}

fn beginAtomicWrite(ptr: *anyopaque, allocator: Allocator, path: []const u8) !AtomicWriteSink {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    if (self.docs.read_only) return error.ReadOnly;
    return try NativeAtomicWriteSink.create(allocator, self, path);
}

fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, old_path);
    try validateIndexPath(self, new_path);
    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    try self.docs.file.renameIndexCatalogRecord(old_path, new_path);
}

fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    lockStore(self.docs);
    defer self.docs.mutex.unlock();
    try self.docs.file.deleteIndexCatalogRecord(path);
}

fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    lockStore(self.docs);
    defer self.docs.mutex.unlock();

    const index_records = try self.docs.file.snapshotIndexCatalogKeysAlloc(self.allocator);
    defer native.NativeFile.freeSnapshotCatalogKeys(self.allocator, index_records);

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

fn syncContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    lockStore(self.docs);
    defer self.docs.mutex.unlock();
    try self.docs.file.sync();
}

fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
    // A logical rename and its namespace update publish through one native
    // checkpoint. Syncing the container is therefore both the file-content
    // and logical-parent durability barrier.
    return try syncContentsAbsolute(ptr, path);
}

fn nowNs(ptr: *anyopaque) u64 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    const now = std.Io.Timestamp.now(self.docs.file.io_impl.io(), .awake);
    return @intCast(now.toNanoseconds());
}

fn rootIdentityAlloc(
    ptr: *anyopaque,
    allocator: Allocator,
    root_dir: []const u8,
) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    const canonical = try storage_io.nativeRealPathAlloc(allocator, self.docs.file.path);
    defer allocator.free(canonical);
    return try std.fmt.allocPrint(
        allocator,
        "aflite-native:{s}\x00{s}",
        .{ canonical, root_dir },
    );
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
        .crc32_range = crc32Range,
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
        try byte_copy.appendSlicePossiblyAliased(&self.out, self.allocator, bytes);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        byte_copy.copyPossiblyAliased(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidAtomicWriteOffset;
        return std.hash.Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn crc32Range(ptr: *anyopaque, offset: usize, range_len: usize) !u32 {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or range_len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        return std.hash.Crc32.hash(self.out.items[offset..][0..range_len]);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();
        try writeFileReserved(self.storage, self.path, self.out.items);
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
        var docs = try docstore.Store.create(allocator, path, true);
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

test "lite native index storage root identity is physical and namespaced" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-identity.aflite");
    defer allocator.free(path);

    var first_identity: []u8 = undefined;
    {
        var docs = try docstore.Store.create(allocator, path, true);
        defer docs.close();
        var indexes = Store.init(allocator, &docs);
        const storage = indexes.storage();

        first_identity = try storage.rootIdentityAlloc(
            allocator,
            "__antfly_lite/tables/a/index_repair.checkpoint",
        );
        const other_namespace = try storage.rootIdentityAlloc(
            allocator,
            "__antfly_lite/tables/b/index_repair.checkpoint",
        );
        defer allocator.free(other_namespace);
        try std.testing.expect(!std.mem.eql(u8, first_identity, other_namespace));
    }
    defer allocator.free(first_identity);

    var reopened_docs = try docstore.Store.open(allocator, path, true);
    defer reopened_docs.close();
    var reopened_indexes = Store.init(allocator, &reopened_docs);
    const reopened_identity = try reopened_indexes.storage().rootIdentityAlloc(
        allocator,
        "__antfly_lite/tables/a/index_repair.checkpoint",
    );
    defer allocator.free(reopened_identity);
    try std.testing.expectEqualStrings(first_identity, reopened_identity);
}

test "lite native index reads remain pinned while newer checkpoints publish" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/index-pinned-checkpoint.aflite", .{tmp.sub_path});
    defer allocator.free(path);

    var docs = try docstore.Store.create(allocator, path, true);
    defer docs.close();
    var indexes = Store.init(allocator, &docs);
    try writeFileAbsolute(&indexes, "segments/current", "generation-one");
    const pinned = docs.file.activeCheckpoint();
    try writeFileAbsolute(&indexes, "segments/current", "generation-two");

    const old = (try docs.file.getIndexCatalogRecordAtCheckpointAlloc(allocator, "segments/current", pinned)) orelse return error.TestUnexpectedResult;
    defer allocator.free(old);
    try std.testing.expectEqualStrings("generation-one", old);

    const current = try readFileAlloc(&indexes, allocator, "segments/current", 1024);
    defer allocator.free(current);
    try std.testing.expectEqualStrings("generation-two", current);
}

test "lite native index storage can be scoped to the Lite index namespace" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-storage-namespace.aflite");
    defer allocator.free(path);

    var docs = try docstore.Store.create(allocator, path, true);
    defer docs.close();
    var index_store = Store.initWithNamespace(allocator, &docs, "__antfly_lite");
    const storage = index_store.storage();

    try storage.createDirPath("__antfly_lite/indexes/ft");
    try storage.writeFileAbsolute("__antfly_lite/indexes/ft/a.tbl", "scoped");
    const got = try storage.readFileAlloc(allocator, "__antfly_lite/indexes/ft/a.tbl", 64);
    defer allocator.free(got);
    try std.testing.expectEqualStrings("scoped", got);

    try std.testing.expectError(error.InvalidNativeIndexPath, storage.createDirPath("__antfly_lite_other/indexes/ft"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.writeFileAbsolute("__antfly_lite_other/indexes/ft/a.tbl", "bad"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.readFileAlloc(allocator, "__antfly_lite_other/indexes/ft/a.tbl", 64));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.beginAtomicWrite(allocator, "__antfly_lite_other/indexes/ft/a.tbl"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.renameAbsolute("__antfly_lite/indexes/ft/a.tbl", "__antfly_lite_other/indexes/ft/a.tbl"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.deleteTree("__antfly_lite_other"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.writeFileAbsolute("", "bad"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.writeFileAbsolute("__antfly_lite/indexes/ft/\x00bad", "bad"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.writeFileAbsolute("__antfly_lite/../outside", "bad"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.writeFileAbsolute("__antfly_lite/./indexes/ft/a.tbl", "bad"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.writeFileAbsolute("__antfly_lite/indexes//ft/a.tbl", "bad"));
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

    var docs = try docstore.Store.create(allocator, path, true);
    defer docs.close();
    var index_store = Store.init(allocator, &docs);
    const storage = index_store.storage();

    try storage.writeFileAbsolute("/dense/a/blob", large);
    try std.testing.expectEqual(@as(u64, @intCast(large.len)), try storage.fileSize("/dense/a/blob"));

    const append_suffix = " native append keeps old pages streaming";
    const before_append_page_count = docs.file.activeCheckpoint().page_count;
    try storage.appendFileAbsolute(allocator, "/dense/a/blob", append_suffix, true);
    try std.testing.expectEqual(before_append_page_count + 7, docs.file.activeCheckpoint().page_count);
    try std.testing.expectEqual(@as(u64, @intCast(large.len + append_suffix.len)), try storage.fileSize("/dense/a/blob"));

    const before_rename_page_count = docs.file.activeCheckpoint().page_count;
    try storage.renameAbsolute("/dense/a/blob", "/dense/a/blob2");
    try std.testing.expectEqual(before_rename_page_count + 3, docs.file.activeCheckpoint().page_count);
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/dense/a/blob", 8));
    const after_rename_check = try docs.file.check();
    try std.testing.expect(after_rename_check.valid);

    const range_offset = native.default_page_size + 19;
    const range = try storage.readFileRangeAlloc(allocator, "/dense/a/blob2", range_offset, 41);
    defer allocator.free(range);
    try std.testing.expectEqualSlices(u8, large[range_offset..][0..41], range);

    var trailer = try storage.readFileTrailerAlloc(allocator, "/dense/a/blob2", 17);
    defer trailer.deinit(allocator);
    try std.testing.expectEqualSlices(u8, append_suffix[append_suffix.len - 17 ..], trailer.bytes);
    try std.testing.expectEqual(@as(u64, large.len + append_suffix.len), trailer.file_size);
    try std.testing.expectError(error.EndOfStream, storage.readFileRangeAlloc(allocator, "/dense/a/blob2", large.len + append_suffix.len - 4, 8));

    try storage.writeFileAbsolute("/dense/a/sub/file", "child");
    try storage.deleteTree("/dense/a");
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/dense/a/blob2", 8));
    try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/dense/a/sub/file", 8));

    const keys = try docs.file.snapshotIndexCatalogKeysAlloc(allocator);
    defer native.NativeFile.freeSnapshotCatalogKeys(allocator, keys);
    try std.testing.expectEqual(@as(usize, 0), keys.len);
}

test "lite native index storage aborts atomic writes without publishing partial files" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-storage-atomic-abort.aflite");
    defer allocator.free(path);

    {
        var docs = try docstore.Store.create(allocator, path, true);
        defer docs.close();
        var index_store = Store.init(allocator, &docs);
        const storage = index_store.storage();

        try storage.writeFileAbsolute("/indexes/ft/stable.tbl", "stable");

        var replace_writer = try storage.beginAtomicWrite(allocator, "/indexes/ft/stable.tbl");
        try replace_writer.appendSlice("partial replacement");
        replace_writer.abort();

        const stable = try storage.readFileAlloc(allocator, "/indexes/ft/stable.tbl", 64);
        defer allocator.free(stable);
        try std.testing.expectEqualStrings("stable", stable);

        var new_writer = try storage.beginAtomicWrite(allocator, "/indexes/ft/new.tbl");
        try new_writer.appendSlice("partial new file");
        new_writer.abort();

        try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/indexes/ft/new.tbl", 64));
    }

    {
        var reopened_docs = try docstore.Store.open(allocator, path, true);
        defer reopened_docs.close();
        var reopened_index_store = Store.init(allocator, &reopened_docs);
        const storage = reopened_index_store.storage();

        const stable = try storage.readFileAlloc(allocator, "/indexes/ft/stable.tbl", 64);
        defer allocator.free(stable);
        try std.testing.expectEqualStrings("stable", stable);
        try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/indexes/ft/new.tbl", 64));
    }
}

test "lite native index storage read-only open rejects mutations" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-storage-readonly.aflite");
    defer allocator.free(path);

    {
        var docs = try docstore.Store.create(allocator, path, true);
        defer docs.close();
        var index_store = Store.init(allocator, &docs);
        const storage = index_store.storage();

        try storage.writeFileAbsolute("/indexes/ft/stable.tbl", "stable");
    }

    {
        var docs = try docstore.Store.open(allocator, path, true);
        defer docs.close();
        var index_store = Store.init(allocator, &docs);
        const storage = index_store.storage();

        const stable = try storage.readFileAlloc(allocator, "/indexes/ft/stable.tbl", 64);
        defer allocator.free(stable);
        try std.testing.expectEqualStrings("stable", stable);

        try std.testing.expectError(error.ReadOnly, storage.writeFileAbsolute("/indexes/ft/new.tbl", "new"));
        try std.testing.expectError(error.ReadOnly, storage.appendFileAbsolute(allocator, "/indexes/ft/stable.tbl", "!", true));
        try std.testing.expectError(error.ReadOnly, storage.renameAbsolute("/indexes/ft/stable.tbl", "/indexes/ft/renamed.tbl"));
        try std.testing.expectError(error.ReadOnly, storage.deleteFileAbsolute("/indexes/ft/stable.tbl"));
        try std.testing.expectError(error.ReadOnly, storage.deleteTree("/indexes/ft"));

        try std.testing.expectError(error.ReadOnly, storage.beginAtomicWrite(allocator, "/indexes/ft/atomic.tbl"));
    }

    {
        var reopened_docs = try docstore.Store.open(allocator, path, true);
        defer reopened_docs.close();
        var reopened_index_store = Store.init(allocator, &reopened_docs);
        const storage = reopened_index_store.storage();

        const stable = try storage.readFileAlloc(allocator, "/indexes/ft/stable.tbl", 64);
        defer allocator.free(stable);
        try std.testing.expectEqualStrings("stable", stable);
        try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/indexes/ft/new.tbl", 64));
        try std.testing.expectError(error.FileNotFound, storage.readFileAlloc(allocator, "/indexes/ft/atomic.tbl", 64));
    }
}

test "lite native index storage recovers previous checkpoint after interrupted update" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-storage-crash-recovery.aflite");
    defer allocator.free(path);

    {
        var docs = try docstore.Store.create(allocator, path, true);
        defer docs.close();
        var index_store = Store.init(allocator, &docs);
        const storage = index_store.storage();

        try storage.writeFileAbsolute("/indexes/ft/stable.tbl", "stable");

        var writer = try storage.beginAtomicWrite(allocator, "/indexes/ft/stable.tbl");
        try writer.appendSlice("replacement");
        try writer.finish();

        const active_slot = docs.file.header.active_checkpoint;
        const previous_slot: u8 = if (active_slot == 0) 1 else 0;
        const previous = docs.file.header.checkpoints[previous_slot];
        try std.testing.expect(previous.commit_sequence > 0);
        try std.testing.expect(docs.file.activeCheckpoint().commit_sequence > previous.commit_sequence);

        try docs.file.file.setLength(docs.file.io_impl.io(), previous.page_count * @as(u64, docs.file.header.page_size));
        try docs.file.file.sync(docs.file.io_impl.io());
    }

    {
        var reopened_docs = try docstore.Store.open(allocator, path, true);
        defer reopened_docs.close();
        var reopened_index_store = Store.init(allocator, &reopened_docs);
        const storage = reopened_index_store.storage();

        const stable = try storage.readFileAlloc(allocator, "/indexes/ft/stable.tbl", 64);
        defer allocator.free(stable);
        try std.testing.expectEqualStrings("stable", stable);
    }
}

test "lite native index storage serializes physical writes without taking document writer slot" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-index-storage-single-writer.aflite");
    defer allocator.free(path);

    var docs = try docstore.Store.create(allocator, path, true);
    defer docs.close();
    var index_store = Store.init(allocator, &docs);
    const storage = index_store.storage();

    var writer = try storage.beginAtomicWrite(allocator, "/indexes/ft/a.tbl");
    try writer.appendSlice("pending");

    var doc_writer = try docs.beginWrite();
    defer doc_writer.abort();
    writer.abort();

    try storage.writeFileAbsolute("/indexes/ft/b.tbl", "released");

    try std.testing.expectError(error.FileBusy, docs.beginWrite());
}
