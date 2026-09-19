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
const builtin = @import("builtin");
const Crc32 = @import("antfly_hash").Crc32;
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
        .list_file_names_alloc = listFileNamesAlloc,
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
    const io = self.docs.file.runtime();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);

    return (try self.docs.file.getIndexCatalogRecordLimitedAtCheckpointAlloc(allocator, path, max_bytes, checkpoint)) orelse error.FileNotFound;
}

fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    const io = self.docs.file.runtime();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);

    return (try self.docs.file.getIndexCatalogRecordRangeAtCheckpointAlloc(allocator, path, offset, len, checkpoint)) orelse return error.FileNotFound;
}

fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    const io = self.docs.file.runtime();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);

    const size = (try self.docs.file.getIndexCatalogRecordSizeAtCheckpoint(path, checkpoint)) orelse return error.FileNotFound;
    return @intCast(size);
}

fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) !storage_io.FileTrailer {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    const io = self.docs.file.runtime();
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

const CatalogWrite = struct {
    kind: enum { put, append, rename, delete, sync },
    path: []const u8,
    value: []const u8 = "",
    fn apply(ptr: *anyopaque, file: *native.NativeFile) !void {
        const self: *@This() = @ptrCast(@alignCast(ptr));
        switch (self.kind) {
            .put => try file.putIndexCatalogRecord(self.path, self.value),
            .append => try file.appendIndexCatalogRecord(self.path, self.value),
            .rename => try file.renameIndexCatalogRecord(self.path, self.value),
            .delete => try file.deleteIndexCatalogRecord(self.path),
            .sync => try file.sync(),
        }
    }
};

fn writeFileReserved(self: *Store, path: []const u8, contents: []const u8) !void {
    var mutation = CatalogWrite{ .kind = .put, .path = path, .value = contents };
    try self.docs.submitMutation(&mutation, CatalogWrite.apply);
}

fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    var mutation = CatalogWrite{ .kind = .append, .path = path, .value = contents };
    try self.docs.submitMutationWithDurability(&mutation, CatalogWrite.apply, sync);
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
    var mutation = CatalogWrite{ .kind = .rename, .path = old_path, .value = new_path };
    try self.docs.submitMutation(&mutation, CatalogWrite.apply);
}

fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    var mutation = CatalogWrite{ .kind = .delete, .path = path };
    try self.docs.submitMutation(&mutation, CatalogWrite.apply);
}

fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    const directory = if (std.mem.eql(u8, path, "/")) path else std.mem.trimEnd(u8, path, "/");
    try validateIndexPath(self, directory);
    const Context = struct {
        store: *Store,
        path: []const u8,
        fn apply(context: *anyopaque, _: *native.NativeFile) !void {
            const request: *@This() = @ptrCast(@alignCast(context));
            try deleteTreeLocked(request.store, request.path);
        }
    };
    var request = Context{ .store = self, .path = directory };
    try self.docs.submitMutation(&request, Context.apply);
}

// Subtree deletion stays one atomic native transaction. Each private flush
// releases keys and editor scratch; the cursor retains the original immutable
// root so subsequent batches cannot skip entries as the active tree shrinks.
const CatalogDeleteBatch = struct {
    const max_keys = 64;
    const max_key_bytes = 16 * 1024;
    allocator: Allocator,
    file: *native.NativeFile,
    mutations: std.ArrayListUnmanaged(native.CatalogMutation) = .empty,
    key_bytes: usize = 0,

    fn clear(self: *@This()) void {
        for (self.mutations.items) |mutation| self.allocator.free(mutation.key);
        self.mutations.clearRetainingCapacity();
        self.key_bytes = 0;
    }

    fn deinit(self: *@This()) void {
        self.clear();
        self.mutations.deinit(self.allocator);
    }

    fn add(self: *@This(), key: []const u8) !void {
        if (self.mutations.items.len != 0 and
            (self.mutations.items.len >= max_keys or self.key_bytes + key.len > max_key_bytes)) try self.flush();
        const owned = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned);
        try self.mutations.append(self.allocator, .{ .key = owned, .is_delete = true });
        self.key_bytes += owned.len;
    }

    fn flush(self: *@This()) !void {
        if (self.mutations.items.len == 0) return;
        try self.file.putIndexCatalogBatch(self.mutations.items);
        _ = try self.file.materializeTransactionCheckpoint();
        self.clear();
    }
};

fn deleteTreeLocked(self: *Store, path: []const u8) !void {
    const directory = if (std.mem.eql(u8, path, "/")) path else std.mem.trimEnd(u8, path, "/");
    try validateIndexPath(self, directory);
    const file = &self.docs.file;
    // Include earlier callbacks in this group before pinning the scan root.
    const checkpoint = try file.materializeTransactionCheckpoint();
    var batch = CatalogDeleteBatch{ .allocator = self.allocator, .file = file };
    defer batch.deinit();
    // Include the exact file, but never a neighboring name such as /a-other.
    if (try file.getIndexCatalogRecordSizeAtCheckpoint(directory, checkpoint) != null) try batch.add(directory);
    const prefix = if (std.mem.eql(u8, directory, "/"))
        try self.allocator.dupe(u8, "/")
    else
        try std.fmt.allocPrint(self.allocator, "{s}/", .{directory});
    defer self.allocator.free(prefix);
    var cursor = try file.indexCatalogCursor(checkpoint, prefix);
    defer cursor.deinit();
    while (try cursor.next()) |record| {
        defer file.allocator.free(record.key);
        if (!std.mem.eql(u8, record.key, directory)) try batch.add(record.key);
    }
    try batch.flush();
}

fn listFileNamesAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8) ![][]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    const directory = if (std.mem.eql(u8, path, "/")) path else std.mem.trimEnd(u8, path, "/");
    try validateIndexPath(self, directory);
    const io = self.docs.file.runtime();
    self.docs.generation_lock.lockSharedUncancelable(io);
    defer self.docs.generation_lock.unlockShared(io);
    const checkpoint = pinCheckpoint(self.docs);
    const prefix = if (std.mem.eql(u8, directory, "/"))
        try allocator.dupe(u8, "/")
    else
        try std.fmt.allocPrint(allocator, "{s}/", .{directory});
    defer allocator.free(prefix);
    // Only scoped stores validate every path component. The unscoped adapter
    // also accepts repeated/trailing separators, whose dirname semantics can
    // make a raw descendant key an immediate file. Preserve those keys through
    // the general prefix cursor rather than skipping their byte ranges.
    var cursor = if (self.namespace_prefix.len != 0)
        try self.docs.file.indexCatalogDirectoryCursor(checkpoint, prefix)
    else
        try self.docs.file.indexCatalogCursor(checkpoint, prefix);
    defer cursor.deinit();
    var names = std.ArrayListUnmanaged([]u8).empty;
    errdefer {
        for (names.items) |name| allocator.free(name);
        names.deinit(allocator);
    }
    while (try cursor.next()) |record| {
        defer self.docs.file.allocator.free(record.key);
        const parent = std.fs.path.dirname(record.key) orelse continue;
        if (!std.mem.eql(u8, parent, directory)) continue;
        const name = try allocator.dupe(u8, std.fs.path.basename(record.key));
        errdefer allocator.free(name);
        try names.append(allocator, name);
    }
    return try names.toOwnedSlice(allocator);
}

fn syncContentsAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *Store = @ptrCast(@alignCast(ptr));
    try validateIndexPath(self, path);
    var mutation = CatalogWrite{ .kind = .sync, .path = path };
    try self.docs.submitMutation(&mutation, CatalogWrite.apply);
}

fn syncParentAbsolute(ptr: *anyopaque, path: []const u8) !void {
    // A logical rename and its namespace update publish through one native
    // checkpoint. Syncing the container is therefore both the file-content
    // and logical-parent durability barrier.
    return try syncContentsAbsolute(ptr, path);
}

fn nowNs(ptr: *anyopaque) u64 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    const now = std.Io.Timestamp.now(self.docs.file.runtime(), .awake);
    return @intCast(now.toNanoseconds());
}

fn rootIdentityAlloc(
    ptr: *anyopaque,
    allocator: Allocator,
    root_dir: []const u8,
) ![]u8 {
    const self: *Store = @ptrCast(@alignCast(ptr));
    const io = self.docs.file.runtime();
    const canonical = if (std.fs.path.isAbsolute(self.docs.file.path))
        try std.Io.Dir.realPathFileAbsoluteAlloc(io, self.docs.file.path, allocator)
    else
        try std.Io.Dir.cwd().realPathFileAlloc(io, self.docs.file.path, allocator);
    defer allocator.free(canonical);
    return try std.fmt.allocPrint(
        allocator,
        "aflite-native:{s}\x00{s}",
        .{ canonical, root_dir },
    );
}

/// A fixed write buffer backed by a private staging file. Builders may patch
/// headers and checksum any range without keeping their output in memory.
/// Staging never reserves the document writer slot or pins a file generation;
/// finish imports it into the current generation under the publication mutex.
const NativeAtomicWriteSink = struct {
    const buffer_size = 64 * 1024;
    allocator: Allocator,
    storage: *Store,
    path: []u8,
    file: ?std.Io.File = null,
    tmp_path: ?[]u8 = null,
    persisted: usize = 0,
    buffered: usize = 0,
    buffer: [buffer_size]u8 = undefined,
    failure: ?anyerror = null,
    write_options: native.WriteOptions = .{},

    const vtable: AtomicWriteSink.VTable = .{
        .len = len,
        .append_slice = appendSlice,
        .write_at = writeAt,
        .crc32_prefix = crc32Prefix,
        .crc32_range = crc32Range,
        .finish = finish,
        .abort = abort,
        .set_cache_intent = setCacheIntent,
    };

    fn create(allocator: Allocator, storage: *Store, path: []const u8) !AtomicWriteSink {
        const self = try allocator.create(NativeAtomicWriteSink);
        errdefer allocator.destroy(self);
        self.* = .{ .allocator = allocator, .storage = storage, .path = try allocator.dupe(u8, path) };
        return .{ .ptr = self, .vtable = &vtable };
    }

    fn deinit(self: *NativeAtomicWriteSink) void {
        const io = self.storage.docs.file.runtime();
        if (self.file) |file| file.close(io);
        if (self.tmp_path) |path| {
            std.Io.Dir.cwd().deleteFile(io, path) catch {};
            self.allocator.free(path);
        }
        self.allocator.free(self.path);
        self.allocator.destroy(self);
    }

    fn ensureFile(self: *NativeAtomicWriteSink) !std.Io.File {
        if (self.file) |file| return file;
        const io = self.storage.docs.file.runtime();
        var random: [16]u8 = undefined;
        try io.randomSecure(&random);
        // Keep the basename independent of the database name so every valid
        // database basename can spill. Retain sibling placement and exclusive
        // creation; the random name is private to this writer.
        const basename = try std.fmt.allocPrint(self.allocator, ".aflite-write-{x}", .{random});
        defer self.allocator.free(basename);
        const parent = std.fs.path.dirname(self.storage.docs.file.path) orelse ".";
        const path = try std.fs.path.join(self.allocator, &.{ parent, basename });
        errdefer self.allocator.free(path);
        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .exclusive = true, .permissions = .fromMode(0o600) });
        self.file = file;
        self.tmp_path = path;
        // On POSIX the descriptor owns the staging file after unlink. Abort,
        // cancellation, and process death then reclaim it without a scavenger.
        if (comptime builtin.os.tag != .windows) {
            std.Io.Dir.cwd().deleteFile(io, path) catch return file;
            self.allocator.free(path);
            self.tmp_path = null;
        }
        return file;
    }

    fn flush(self: *NativeAtomicWriteSink) !void {
        if (self.buffered == 0) return;
        const file = try self.ensureFile();
        try file.writePositionalAll(self.storage.docs.file.runtime(), self.buffer[0..self.buffered], self.persisted);
        self.persisted += self.buffered;
        self.buffered = 0;
    }

    fn len(ptr: *anyopaque) usize {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.persisted + self.buffered;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (self.failure) |err| return err;
        if (bytes.len > std.math.maxInt(u32) - len(ptr)) return error.RecordTooLarge;
        errdefer |err| self.failure = err;
        var offset: usize = 0;
        while (offset < bytes.len) {
            const n = @min(self.buffer.len - self.buffered, bytes.len - offset);
            @memcpy(self.buffer[self.buffered..][0..n], bytes[offset..][0..n]);
            self.buffered += n;
            offset += n;
            if (self.buffered == self.buffer.len) try self.flush();
        }
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (self.failure) |err| return err;
        if (offset > len(ptr) or bytes.len > len(ptr) - offset) return error.InvalidAtomicWriteOffset;
        errdefer |err| self.failure = err;
        const on_disk = if (offset < self.persisted) @min(bytes.len, self.persisted - offset) else 0;
        if (on_disk > 0) try self.file.?.writePositionalAll(self.storage.docs.file.runtime(), bytes[0..on_disk], offset);
        if (on_disk < bytes.len) @memcpy(self.buffer[offset + on_disk - self.persisted ..][0 .. bytes.len - on_disk], bytes[on_disk..]);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        return try crc32Range(ptr, 0, len_prefix);
    }

    fn crc32Range(ptr: *anyopaque, offset: usize, range_len: usize) !u32 {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (self.failure) |err| return err;
        if (offset > len(ptr) or range_len > len(ptr) - offset) return error.InvalidAtomicWriteOffset;
        errdefer |err| self.failure = err;
        var crc = Crc32.init();
        var scratch: [buffer_size]u8 = undefined;
        var pos = offset;
        const end = offset + range_len;
        while (pos < @min(end, self.persisted)) {
            const n = @min(scratch.len, @min(end, self.persisted) - pos);
            if (try self.file.?.readPositionalAll(self.storage.docs.file.runtime(), scratch[0..n], pos) != n)
                return error.EndOfStream;
            crc.update(scratch[0..n]);
            pos += n;
        }
        if (pos < end) crc.update(self.buffer[pos - self.persisted .. end - self.persisted]);
        return crc.final();
    }

    fn setCacheIntent(ptr: *anyopaque, intent: storage_io.AtomicWriteCacheIntent) void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.write_options.payload_cache = switch (intent) {
            .normal => .normal,
            .cold_sequential => .cold_sequential,
        };
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();
        if (self.failure) |err| return err;
        if (self.file != null) try self.flush();
        try self.storage.docs.submitMutation(self, publish);
    }

    fn publish(ptr: *anyopaque, destination: *native.NativeFile) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (self.file) |file| {
            try destination.putIndexCatalogRecordFromFile(self.path, file, self.persisted, self.write_options);
        } else {
            try destination.putIndexCatalogRecordWithOptions(self.path, self.buffer[0..self.buffered], self.write_options);
        }
    }

    fn abort(ptr: *anyopaque) void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.deinit();
    }
};

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite native repeated WAL reset does not publish unchanged control records" {
    const wal = @import("../lsm_backend/wal.zig");
    const State = @import("../lsm_backend/state.zig").State;
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-wal-reset.aflite");
    defer allocator.free(path);
    var docs = try docstore.Store.create(allocator, path, true);
    defer docs.close();
    var indexes = Store.init(allocator, &docs);
    const storage = indexes.storage();
    const root = "/indexes/test";
    try wal.reset(storage, allocator, root);
    const empty = docs.file.activeCheckpoint();
    for (0..3) |_| try wal.reset(storage, allocator, root);
    try std.testing.expectEqualDeep(empty, docs.file.activeCheckpoint());

    var state: State = .{};
    defer state.deinit(allocator);
    try state.upsert(allocator, .{ .name = "docs" }, "a", "A", false);
    _ = try wal.appendStateWithOptions(storage, allocator, root, &state, false, .{ .segment_bytes = 32 });
    const retained = try wal.snapshotRetention(storage, allocator, root);
    try std.testing.expect(retained.bytes > 0);
    try wal.reset(storage, allocator, root);
    const after = try wal.snapshotRetention(storage, allocator, root);
    try std.testing.expectEqual(@as(u64, 0), after.bytes);
    const reset = docs.file.activeCheckpoint();
    try std.testing.expect(reset.commit_sequence > empty.commit_sequence);
    try wal.reset(storage, allocator, root);
    try std.testing.expectEqualDeep(reset, docs.file.activeCheckpoint());
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
        try std.testing.expectEqual(Crc32.hash("abcdef__"), try writer.crc32Prefix(writer.len()));
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

test "lite native index storage lists immediate live files within its namespace" {
    const alloc = std.testing.allocator;
    var fixture = try @import("../../common/test_directory.zig").TestDirectory.init("listing.aflite");
    defer fixture.cleanup();
    {
        var docs = try docstore.Store.create(alloc, fixture.path(), true);
        defer docs.close();
        var indexes = Store.init(alloc, &docs);
        const storage = indexes.storage();
        for ([_][]const u8{ "/a/blocks/live", "/a/blocks/deleted", "/a/blocks/renamed", "/a/blocks/nested/child", "/a/blocks-other/sibling", "/b/blocks/other" }) |path|
            try storage.writeFileAbsolute(path, "data");
        try storage.writeFileAbsolute("/a/blocks/live", "replacement");
        try storage.deleteFileAbsolute("/a/blocks/deleted");
        try storage.renameAbsolute("/a/blocks/renamed", "/a/blocks/moved");
    }
    // Listing works on a read-only checkpoint and does not resurrect old keys.
    var docs = try docstore.Store.open(alloc, fixture.path(), true);
    defer docs.close();
    const before = docs.file.activeCheckpoint().commit_sequence;
    var indexes = Store.initWithNamespace(alloc, &docs, "/a");
    const storage = indexes.storage();
    const Check = struct {
        fn run(a: Allocator, s: StorageIo) !void {
            const names = try s.listFileNamesAlloc(a, "/a/blocks/");
            defer StorageIo.freeFileNames(a, names);
            try std.testing.expectEqual(@as(usize, 2), names.len);
            try std.testing.expectEqualStrings("live", names[0]);
            try std.testing.expectEqualStrings("moved", names[1]);
        }
    };
    try std.testing.checkAllAllocationFailures(alloc, Check.run, .{storage});
    const empty = try storage.listFileNamesAlloc(alloc, "/a/missing");
    defer StorageIo.freeFileNames(alloc, empty);
    try std.testing.expectEqual(@as(usize, 0), empty.len);
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.listFileNamesAlloc(alloc, "/b/blocks"));
    try std.testing.expectError(error.InvalidNativeIndexPath, storage.listFileNamesAlloc(alloc, "/a/../b"));
    try std.testing.expectEqual(before, docs.file.activeCheckpoint().commit_sequence);
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
    try std.testing.expectEqual(before_append_page_count + 6, docs.file.activeCheckpoint().page_count);
    try std.testing.expectEqual(@as(u64, @intCast(large.len + append_suffix.len)), try storage.fileSize("/dense/a/blob"));

    const before_rename_page_count = docs.file.activeCheckpoint().page_count;
    try storage.renameAbsolute("/dense/a/blob", "/dense/a/blob2");
    // Two packed records share one page, plus tree node, descriptor and free map.
    try std.testing.expectEqual(before_rename_page_count + 4, docs.file.activeCheckpoint().page_count);
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

        try docs.file.file.setLength(docs.file.runtime(), previous.page_count * @as(u64, docs.file.header.page_size));
        try docs.file.file.sync(docs.file.runtime());
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

test "lite native directory operations seek bounded prefixes independent of catalog history" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "catalog-directory-seek.aflite");
    defer alloc.free(path);
    var docs = try docstore.Store.create(alloc, path, true);
    defer docs.close();
    var indexes = Store.init(alloc, &docs);
    const storage = indexes.storage();
    for (0..1000) |i| {
        var key: [32]u8 = undefined;
        try storage.writeFileAbsolute(try std.fmt.bufPrint(&key, "/unrelated/{d:0>8}", .{i}), "x");
        try storage.writeFileAbsolute("/unrelated/repeated", try std.fmt.bufPrint(&key, "{d}", .{i}));
    }
    try storage.writeFileAbsolute("/a", "exact");
    try storage.writeFileAbsolute("/a/one", "child");
    try storage.writeFileAbsolute("/a/sub/two", "nested");
    try storage.writeFileAbsolute("/a/deleted", "old");
    try storage.deleteFileAbsolute("/a/deleted");
    try storage.writeFileAbsolute("/a-other/keep", "neighbor");
    const before = docs.file.test_page_reads.load(.monotonic);
    const names = try storage.listFileNamesAlloc(alloc, "/a/");
    defer StorageIo.freeFileNames(alloc, names);
    try std.testing.expectEqual(@as(usize, 1), names.len);
    try std.testing.expectEqualStrings("one", names[0]);
    try std.testing.expect(docs.file.test_page_reads.load(.monotonic) - before <= 12);
    const before_missing = docs.file.test_page_reads.load(.monotonic);
    const missing = try storage.listFileNamesAlloc(alloc, "/absent");
    defer StorageIo.freeFileNames(alloc, missing);
    try std.testing.expectEqual(@as(usize, 0), missing.len);
    try std.testing.expect(docs.file.test_page_reads.load(.monotonic) - before_missing <= 6);
    const before_delete = docs.file.test_page_reads.load(.monotonic);
    try storage.deleteTree("/a/");
    try std.testing.expect(docs.file.test_page_reads.load(.monotonic) - before_delete <= 40);
    try std.testing.expectError(error.FileNotFound, storage.fileSize("/a"));
    try std.testing.expectError(error.FileNotFound, storage.fileSize("/a/one"));
    try std.testing.expectError(error.FileNotFound, storage.fileSize("/a/sub/two"));
    try std.testing.expectEqual(@as(u64, 8), try storage.fileSize("/a-other/keep"));
    try std.testing.expectEqual(@as(u64, 1), try storage.fileSize("/unrelated/00000000"));
    try std.testing.expect((try docs.file.check()).valid);
}

test "lite native staged atomic writes bound heap and survive concurrent commits and vacuum" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "staged-atomic.aflite");
    defer alloc.free(path);
    const expected = try alloc.alloc(u8, 8 * 1024 * 1024 + 137);
    defer alloc.free(expected);
    for (expected, 0..) |*byte, i| byte.* = @intCast(i % 251);
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = alloc, .limit = 512 * 1024 };
    {
        var docs = try docstore.Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        docs.file.page_cache_enabled.store(false, .monotonic);
        var indexes = Store.init(budget.allocator(), &docs);
        const storage = indexes.storage();
        try storage.writeFileAbsolute("/block", "old");
        var writer = try storage.beginAtomicWrite(budget.allocator(), "/block");
        var active = true;
        defer if (active) writer.abort();
        try writer.appendSlice(expected[0..33]);
        try writer.appendSlice(expected[33..]);
        try std.testing.expectEqual(expected.len, writer.len());
        const impl: *NativeAtomicWriteSink = @ptrCast(@alignCast(writer.ptr));
        try std.testing.expect(impl.file != null);
        if (comptime builtin.os.tag != .windows) try std.testing.expect(impl.tmp_path == null);
        const patch_offsets = [_]usize{ 3, 65530, impl.persisted - 3, expected.len - 8 };
        for (patch_offsets) |offset| {
            const patch = "PATCHED!";
            try writer.writeAt(offset, patch);
            @memcpy(expected[offset..][0..patch.len], patch);
        }
        try std.testing.expectEqual(Crc32.hash(expected), try writer.crc32Prefix(expected.len));
        try std.testing.expectEqual(Crc32.hash(expected[65530..66530]), try writer.crc32Range(65530, 1000));
        try std.testing.expectEqual(Crc32.hash(expected[expected.len - 10 ..]), try writer.crc32Range(expected.len - 10, 10));
        try std.testing.expectEqual(Crc32.hash(""), try writer.crc32Range(expected.len, 0));
        try std.testing.expectError(error.InvalidAtomicWriteOffset, writer.writeAt(expected.len, "x"));
        try std.testing.expectError(error.InvalidAtomicWriteOffset, writer.crc32Range(expected.len, 1));
        const old = try storage.readFileAlloc(alloc, "/block", 10);
        defer alloc.free(old);
        try std.testing.expectEqualStrings("old", old);

        // A second staged writer and a document transaction can coexist. The
        // first sink is private and must not depend on the original generation.
        {
            var txn = try docs.beginWrite();
            errdefer txn.abort();
            try txn.put("doc", "committed");
            var other = try storage.beginAtomicWrite(budget.allocator(), "/other");
            try other.appendSlice(expected[0..70000]);
            try other.finish();
            try txn.commit();
        }
        _ = try docs.vacuum();
        const before = docs.file.activeCheckpoint();
        active = false;
        try writer.finish();
        try std.testing.expectEqual(before.commit_sequence + 1, docs.file.activeCheckpoint().commit_sequence);
        try std.testing.expect(budget.peak <= budget.limit);
        const actual = try storage.readFileAlloc(alloc, "/block", expected.len);
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, expected, actual);
        // Full integrity checking maintains a separate page reachability set.
        budget.limit = std.math.maxInt(usize);
        try std.testing.expect((try docs.file.check()).valid);
    }
    try std.testing.expectEqual(@as(usize, 0), budget.live);
    var reopened = try docstore.Store.open(alloc, path, true);
    defer reopened.close();
    const value = (try reopened.file.getIndexCatalogRecordAlloc(alloc, "/block")).?;
    defer alloc.free(value);
    try std.testing.expectEqualSlices(u8, expected, value);
    const document = (try reopened.file.getDocumentAlloc(alloc, "doc")).?;
    defer alloc.free(document);
    try std.testing.expectEqualStrings("committed", document);
}

test "lite native staged atomic writes discard failed imports and poisoned sources" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "failed-staging.aflite");
    defer alloc.free(path);
    var docs = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
    defer docs.close();
    var indexes = Store.init(alloc, &docs);
    const storage = indexes.storage();
    try storage.writeFileAbsolute("/stable", "old");
    const checkpoint = docs.file.activeCheckpoint();
    const bytes: [128 * 1024]u8 = @splat('v');
    for ([_]bool{ false, true }) |checksum_first| {
        var writer = try storage.beginAtomicWrite(alloc, "/stable");
        try writer.appendSlice(&bytes);
        const impl: *NativeAtomicWriteSink = @ptrCast(@alignCast(writer.ptr));
        try impl.file.?.setLength(std.testing.io, 65536);
        if (checksum_first) {
            try std.testing.expectError(error.EndOfStream, writer.crc32Prefix(bytes.len));
            try std.testing.expectError(error.EndOfStream, writer.appendSlice("must not recover silently"));
        }
        try std.testing.expectError(error.EndOfStream, writer.finish());
        try std.testing.expect(std.meta.eql(checkpoint, docs.file.activeCheckpoint()));
        const value = try storage.readFileAlloc(alloc, "/stable", 10);
        defer alloc.free(value);
        try std.testing.expectEqualStrings("old", value);
    }
    var aborted = try storage.beginAtomicWrite(alloc, "/stable");
    try aborted.appendSlice(&bytes);
    aborted.abort();
    try std.testing.expect(std.meta.eql(checkpoint, docs.file.activeCheckpoint()));
    var empty = try storage.beginAtomicWrite(alloc, "/empty");
    try empty.finish();
    try std.testing.expectEqual(@as(u64, 0), try storage.fileSize("/empty"));
    var retry = try storage.beginAtomicWrite(alloc, "/stable");
    try retry.appendSlice(&bytes);
    try retry.finish();
    const value = try storage.readFileAlloc(alloc, "/stable", bytes.len);
    defer alloc.free(value);
    try std.testing.expectEqualSlices(u8, &bytes, value);
    try std.testing.expect((try docs.file.check()).valid);
}

test "lite native atomic writes spill with long database basenames" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    // Valid under NAME_MAX=255, including the writer lock's .lock suffix.
    // Appending the former 50-byte staging suffix would exceed that limit.
    const name = "x" ** 213 ++ ".aflite";
    const path = try testPath(alloc, tmp, name);
    defer alloc.free(path);
    const bytes: [128 * 1024 + 17]u8 = @splat('s');
    {
        var docs = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        var indexes = Store.init(alloc, &docs);
        const storage = indexes.storage();
        var writer = try storage.beginAtomicWrite(alloc, "/long-name");
        var active = true;
        defer if (active) writer.abort();
        try writer.appendSlice(&bytes);
        active = false;
        try writer.finish();
        {
            var aborted = try storage.beginAtomicWrite(alloc, "/aborted");
            defer aborted.abort();
            try aborted.appendSlice(&bytes);
        }
        try std.testing.expectError(error.FileNotFound, storage.fileSize("/aborted"));
    }
    var reopened = try docstore.Store.open(alloc, path, true);
    defer reopened.close();
    const actual = (try reopened.file.getIndexCatalogRecordAlloc(alloc, "/long-name")).?;
    defer alloc.free(actual);
    try std.testing.expectEqualSlices(u8, &bytes, actual);
    try std.testing.expect((try reopened.file.check()).valid);
}

test "lite native cold atomic writes preserve hot pages with bounded cache admission" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "cold-atomic.aflite");
    defer alloc.free(path);
    const bytes = try alloc.alloc(u8, 2 * 1024 * 1024 + 17);
    defer alloc.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @intCast(i % 251);
    {
        var docs = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        docs.file.page_cache.limit_bytes = 256 * 1024;
        var indexes = Store.init(alloc, &docs);
        const storage = indexes.storage();
        try storage.writeFileAbsolute("/hot", bytes[0..12000]);
        const hot = try storage.readFileAlloc(alloc, "/hot", 12000);
        defer alloc.free(hot);
        try std.testing.expectEqualSlices(u8, bytes[0..12000], hot);
        var hot_pages = std.ArrayList(u64).empty;
        defer hot_pages.deinit(alloc);
        var cached = docs.file.page_cache.pages.iterator();
        while (cached.next()) |entry| {
            if (entry.value_ptr.bytes[4] == @intFromEnum(native.PageKind.value)) try hot_pages.append(alloc, entry.key_ptr.*);
        }
        try std.testing.expect(hot_pages.items.len > 0);

        // Cover both the in-memory external-value path and the file importer.
        // The spilled payload is eight times the entire page-cache capacity.
        const cases = .{ .{ "/cold-buffered", @as(usize, 32000) }, .{ "/cold-staged", bytes.len } };
        inline for (cases) |case| {
            var writer = try storage.beginAtomicWrite(alloc, case[0]);
            var active = true;
            defer if (active) writer.abort();
            writer.setCacheIntent(.cold_sequential);
            try writer.appendSlice(bytes[0..case[1]]);
            active = false;
            try writer.finish();
            for (hot_pages.items) |id| try std.testing.expect(docs.file.page_cache.pages.contains(id));
            cached = docs.file.page_cache.pages.iterator();
            var payload_pages: usize = 0;
            while (cached.next()) |entry| {
                if (entry.value_ptr.bytes[4] == @intFromEnum(native.PageKind.value)) payload_pages += 1;
            }
            try std.testing.expectEqual(hot_pages.items.len, payload_pages);
            try std.testing.expect(docs.file.page_cache.pages.contains(docs.file.activeCheckpoint().index_catalog_root_page));
        }

        // Policy is local to the sink, and can be changed before publication.
        var normal = try storage.beginAtomicWrite(alloc, "/normal");
        var active = true;
        defer if (active) normal.abort();
        normal.setCacheIntent(.cold_sequential);
        try normal.appendSlice(bytes[0..32000]);
        normal.setCacheIntent(.normal);
        active = false;
        try normal.finish();
        var payload_pages: usize = 0;
        cached = docs.file.page_cache.pages.iterator();
        while (cached.next()) |entry| {
            if (entry.value_ptr.bytes[4] == @intFromEnum(native.PageKind.value)) payload_pages += 1;
        }
        try std.testing.expect(payload_pages > hot_pages.items.len);
        for (hot_pages.items) |id| try std.testing.expect(docs.file.page_cache.pages.contains(id));
    }
    var reopened = try docstore.Store.open(alloc, path, true);
    defer reopened.close();
    inline for (.{ .{ "/hot", @as(usize, 12000) }, .{ "/cold-buffered", @as(usize, 32000) }, .{ "/cold-staged", bytes.len }, .{ "/normal", @as(usize, 32000) } }) |case| {
        const actual = (try reopened.file.getIndexCatalogRecordAlloc(alloc, case[0])).?;
        defer alloc.free(actual);
        try std.testing.expectEqualSlices(u8, bytes[0..case[1]], actual);
    }
    try std.testing.expect((try reopened.file.check()).valid);
}

test "lite native directory cursor skips large subtrees and preserves boundary files" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "directory-subtree-skip.aflite");
    defer alloc.free(path);
    {
        var docs = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        var indexes = Store.initWithNamespace(alloc, &docs, "/a");
        const storage = indexes.storage();
        var arena = std.heap.ArenaAllocator.init(alloc);
        defer arena.deinit();
        const scratch = arena.allocator();
        var mutations = std.ArrayList(native.CatalogMutation).empty;
        for (0..4096) |i| {
            const dir = if (i < 2048) "sub" else "other";
            try mutations.append(scratch, .{ .key = try std.fmt.allocPrint(scratch, "/a/{s}/deep/{d:0>8}", .{ dir, i }), .value = "nested" });
        }
        for ([_][]const u8{ "/a", "/a/sub", "/a/sub.", "/a/sub0", "/a/submarine", "/a/other0", "/a/z", "/top" }) |key|
            try mutations.append(scratch, .{ .key = key, .value = "direct" });
        try docs.file.putIndexCatalogBatch(mutations.items);
        const pinned = docs.file.activeCheckpoint();
        const reads = docs.file.test_page_reads.load(.monotonic);
        const names = try storage.listFileNamesAlloc(alloc, "/a/");
        defer StorageIo.freeFileNames(alloc, names);
        const expected = [_][]const u8{ "other0", "sub", "sub.", "sub0", "submarine", "z" };
        try std.testing.expectEqual(expected.len, names.len);
        for (expected, names) |want, got| try std.testing.expectEqualStrings(want, got);
        try std.testing.expect(docs.file.test_page_reads.load(.monotonic) - reads <= 40);
        var unscoped = Store.init(alloc, &docs);
        const root_names = try unscoped.storage().listFileNamesAlloc(alloc, "/");
        defer StorageIo.freeFileNames(alloc, root_names);
        try std.testing.expectEqual(@as(usize, 2), root_names.len);
        try std.testing.expectEqualStrings("a", root_names[0]);
        try std.testing.expectEqualStrings("top", root_names[1]);
        // The cursor remains in its original checkpoint even after live
        // deletion. Seek-past must not silently rebind to the latest root.
        var cursor = try docs.file.indexCatalogDirectoryCursor(pinned, "/a/");
        defer cursor.deinit();
        try storage.deleteFileAbsolute("/a/sub0");
        var count: usize = 0;
        while (try cursor.next()) |entry| {
            defer alloc.free(entry.key);
            try std.testing.expectEqualStrings(expected[count], std.fs.path.basename(entry.key));
            count += 1;
        }
        try std.testing.expectEqual(expected.len, count);
    }
    var reopened = try docstore.Store.open(alloc, path, true);
    defer reopened.close();
    var indexes = Store.initWithNamespace(alloc, &reopened, "/a");
    const names = try indexes.storage().listFileNamesAlloc(alloc, "/a");
    defer StorageIo.freeFileNames(alloc, names);
    try std.testing.expectEqual(@as(usize, 5), names.len);
    try std.testing.expect((try reopened.file.check()).valid);
}

test "lite native atomic imports batch writes and preserve publication on failed flush" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "batched-import.aflite");
    defer alloc.free(path);
    const bytes = try alloc.alloc(u8, 2 * 1024 * 1024 + 17);
    defer alloc.free(bytes);
    for (bytes, 0..) |*byte, i| byte.* = @intCast(i % 251);
    {
        var docs = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        var indexes = Store.init(alloc, &docs);
        const storage = indexes.storage();
        try storage.writeFileAbsolute("/value", "old");
        const old = docs.file.activeCheckpoint();
        // Fail after one complete page batch reached disk, and on the first
        // batch. Neither path may publish the unfinished replacement.
        for ([_]usize{ 0, 1 }) |after| {
            var writer = try storage.beginAtomicWrite(alloc, "/value");
            var active = true;
            defer if (active) writer.abort();
            try writer.appendSlice(bytes);
            docs.file.test_page_write_fail_after = after;
            active = false;
            try std.testing.expectError(error.TestPageWriteFailure, writer.finish());
            docs.file.test_page_write_fail_after = null;
            try std.testing.expect(std.meta.eql(old, docs.file.activeCheckpoint()));
            const value = try storage.readFileAlloc(alloc, "/value", 10);
            defer alloc.free(value);
            try std.testing.expectEqualStrings("old", value);
            var pinned = try docstore.Store.open(alloc, path, true);
            defer pinned.close();
            const disk = (try pinned.file.getIndexCatalogRecordAlloc(alloc, "/value")).?;
            defer alloc.free(disk);
            try std.testing.expectEqualStrings("old", disk);
        }
        var writer = try storage.beginAtomicWrite(alloc, "/value");
        var active = true;
        defer if (active) writer.abort();
        writer.setCacheIntent(.cold_sequential);
        try writer.appendSlice(bytes);
        const writes = docs.file.test_page_write_calls.load(.monotonic);
        const pages = docs.file.test_page_writes.load(.monotonic);
        active = false;
        try writer.finish();
        try std.testing.expect(docs.file.test_page_writes.load(.monotonic) - pages > 500);
        try std.testing.expect(docs.file.test_page_write_calls.load(.monotonic) - writes <= 40);
        // Buffered external values use the same batch writer.
        const buffered_writes = docs.file.test_page_write_calls.load(.monotonic);
        var buffered = try storage.beginAtomicWrite(alloc, "/buffered");
        var buffered_active = true;
        defer if (buffered_active) buffered.abort();
        try buffered.appendSlice(bytes[0..32000]);
        buffered_active = false;
        try buffered.finish();
        try std.testing.expect(docs.file.test_page_write_calls.load(.monotonic) - buffered_writes <= 5);
    }
    var reopened = try docstore.Store.open(alloc, path, true);
    defer reopened.close();
    const value = (try reopened.file.getIndexCatalogRecordAlloc(alloc, "/value")).?;
    defer alloc.free(value);
    try std.testing.expectEqualSlices(u8, bytes, value);
    const buffered = (try reopened.file.getIndexCatalogRecordAlloc(alloc, "/buffered")).?;
    defer alloc.free(buffered);
    try std.testing.expectEqualSlices(u8, bytes[0..32000], buffered);
    try std.testing.expect((try reopened.file.check()).valid);
}

test "lite native unscoped listings preserve accepted non-normalized keys" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(alloc, tmp, "unscoped-path-listing.aflite");
    defer alloc.free(path);
    const keys = [_][]const u8{
        "/a//file",
        "/a/file/",
        // '!' sorts before '/', so a descendant can precede the trailing
        // separator alias. Merely trimming the first encountered key would
        // still skip the alias when jumping over this subtree.
        "/a/dir/!nested",
        "/a/dir//",
        "/a/dir/nested",
        "/a/dir0",
        "/a/normal",
    };
    const expected = [_][]const u8{ "file", "dir", "dir0", "file", "renamed" };
    {
        var docs = try docstore.Store.createWithOptions(alloc, path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        var indexes = Store.init(alloc, &docs);
        const storage = indexes.storage();
        for (keys) |key| try storage.writeFileAbsolute(key, key);
        try storage.renameAbsolute("/a/normal", "/a/renamed/");
        var writer = try storage.beginAtomicWrite(alloc, "/a//file");
        var active = true;
        defer if (active) writer.abort();
        try writer.appendSlice("updated");
        active = false;
        try writer.finish();
        const names = try storage.listFileNamesAlloc(alloc, "/a/");
        defer StorageIo.freeFileNames(alloc, names);
        try std.testing.expectEqual(expected.len, names.len);
        for (expected, names) |want, got| try std.testing.expectEqualStrings(want, got);
        const renamed = try storage.readFileAlloc(alloc, "/a/renamed/", 100);
        defer alloc.free(renamed);
        try std.testing.expectEqualStrings("/a/normal", renamed);
        // The production namespace contract rejects these aliases; it is
        // what makes subtree skipping safe for the scoped adapter.
        var scoped = Store.initWithNamespace(alloc, &docs, "/a");
        for ([_][]const u8{ "/a//other", "/a/other/", "/a/other//" }) |key|
            try std.testing.expectError(error.InvalidNativeIndexPath, scoped.storage().writeFileAbsolute(key, "invalid"));
    }
    var reopened = try docstore.Store.open(alloc, path, true);
    defer reopened.close();
    var indexes = Store.init(alloc, &reopened);
    const storage = indexes.storage();
    const names = try storage.listFileNamesAlloc(alloc, "/a");
    defer StorageIo.freeFileNames(alloc, names);
    try std.testing.expectEqual(expected.len, names.len);
    for (expected, names) |want, got| try std.testing.expectEqualStrings(want, got);
    const updated = try storage.readFileAlloc(alloc, "/a//file", 100);
    defer alloc.free(updated);
    try std.testing.expectEqualStrings("updated", updated);
    const trailing = try storage.readFileAlloc(alloc, "/a/file/", 100);
    defer alloc.free(trailing);
    try std.testing.expectEqualStrings("/a/file/", trailing);
    try std.testing.expect((try reopened.file.check()).valid);
}

test "lite index read limits reject from pinned metadata before payload allocation" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/limited-read.aflite", .{tmp.sub_path});
    defer a.free(path);
    var docs = try docstore.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
    defer docs.close();
    docs.file.page_cache_enabled.store(false, .monotonic);
    const large = try a.alloc(u8, 4 * 1024 * 1024);
    defer a.free(large);
    @memset(large, 'v');
    try docs.file.putIndexCatalogRecord("/scope/large", large);
    try docs.file.putIndexCatalogRecord("/scope/inline", "small");
    try docs.file.putIndexCatalogRecord("/scope/empty", "");
    var store = Store.initWithNamespace(a, &docs, "/scope");
    var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 0 };
    const before = docs.file.test_page_reads.load(.monotonic);
    try std.testing.expectError(error.FileTooBig, store.storage().readFileAlloc(budget.allocator(), "/scope/large", 1024));
    try std.testing.expectError(error.FileTooBig, store.storage().readFileAlloc(budget.allocator(), "/scope/inline", 4));
    try std.testing.expectError(error.FileNotFound, store.storage().readFileAlloc(budget.allocator(), "/scope/missing", 0));
    const empty = try store.storage().readFileAlloc(budget.allocator(), "/scope/empty", 0);
    budget.allocator().free(empty);
    try std.testing.expectEqual(@as(usize, 0), budget.peak);
    try std.testing.expect(docs.file.test_page_reads.load(.monotonic) - before <= 16);
    const checkpoint = docs.file.activeCheckpoint();
    try docs.file.putIndexCatalogRecord("/scope/large", "short");
    try std.testing.expectError(error.FileTooBig, docs.file.getIndexCatalogRecordLimitedAtCheckpointAlloc(budget.allocator(), "/scope/large", 1024, checkpoint));
    budget.limit = std.math.maxInt(usize);
    const current = try store.storage().readFileAlloc(budget.allocator(), "/scope/large", 5);
    defer budget.allocator().free(current);
    try std.testing.expectEqualStrings("short", current);
    const exact = (try docs.file.getIndexCatalogRecordLimitedAtCheckpointAlloc(a, "/scope/large", large.len, checkpoint)).?;
    defer a.free(exact);
    try std.testing.expectEqualSlices(u8, large, exact);
}

test "lite subtree deletion bounds heap across private batches and preserves snapshots" {
    const a = std.testing.allocator;
    for ([_]struct { count: usize, key_len: usize }{
        .{ .count = 4096, .key_len = 128 },
        .{ .count = 16384, .key_len = 128 },
        .{ .count = 4096, .key_len = 1024 },
    }) |case| {
        const count = case.count;
        var tmp = std.testing.tmpDir(.{});
        defer tmp.cleanup();
        const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/bounded-delete.aflite", .{tmp.sub_path});
        defer a.free(path);
        var budget = @import("test_allocator.zig").BudgetAllocator{ .backing = a };
        var docs = try docstore.Store.createWithOptions(budget.allocator(), path, .{ .no_sync = true, .io = std.testing.io });
        defer docs.close();
        docs.file.page_cache_enabled.store(false, .monotonic);
        var arena = std.heap.ArenaAllocator.init(a);
        defer arena.deinit();
        const mutations = try arena.allocator().alloc(native.CatalogMutation, count);
        for (mutations, 0..) |*mutation, i| {
            const key = try arena.allocator().alloc(u8, case.key_len);
            @memset(key, 'x');
            _ = try std.fmt.bufPrint(key[0..15], "/scope/{d:0>8}", .{i});
            mutation.* = .{ .key = key, .value = "" };
        }
        try docs.file.putIndexCatalogBatch(mutations);
        try docs.file.putIndexCatalogRecord("/scope", "exact");
        try docs.file.putIndexCatalogRecord("/scope-other/keep", "neighbor");
        const pinned = docs.file.activeCheckpoint();
        var store = Store.initWithNamespace(budget.allocator(), &docs, "/scope");
        const baseline = budget.live;
        budget.peak = baseline;
        budget.limit = baseline + 512 * 1024;
        try store.storage().deleteTree("/scope");
        try std.testing.expect(budget.peak - baseline <= 512 * 1024);
        budget.limit = std.math.maxInt(usize);
        try std.testing.expectEqual(pinned.commit_sequence + 1, docs.file.activeCheckpoint().commit_sequence);
        try std.testing.expect((try docs.file.getIndexCatalogRecordSize("/scope")) == null);
        var cursor = try docs.file.indexCatalogCursor(docs.file.activeCheckpoint(), "/scope/");
        defer cursor.deinit();
        try std.testing.expect((try cursor.next()) == null);
        const old = (try docs.file.getIndexCatalogRecordAtCheckpointAlloc(a, "/scope", pinned)).?;
        defer a.free(old);
        try std.testing.expectEqualStrings("exact", old);
        try std.testing.expectEqual(@as(?usize, 0), try docs.file.getIndexCatalogRecordSizeAtCheckpoint(mutations[count / 2].key, pinned));
        const neighbor = (try docs.file.getIndexCatalogRecordAlloc(a, "/scope-other/keep")).?;
        defer a.free(neighbor);
        try std.testing.expectEqualStrings("neighbor", neighbor);
        try std.testing.expect((try docs.file.check()).valid);
    }
}

test "lite subtree deletion rolls back earlier private batches on failure" {
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/delete-rollback.aflite", .{tmp.sub_path});
    defer a.free(path);
    var docs = try docstore.Store.createWithOptions(a, path, .{ .no_sync = true, .io = std.testing.io });
    defer docs.close();
    docs.file.page_cache_enabled.store(false, .monotonic);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const mutations = try arena.allocator().alloc(native.CatalogMutation, 2048);
    for (mutations, 0..) |*mutation, i| mutation.* = .{
        .key = try std.fmt.allocPrint(arena.allocator(), "/scope/{d:0>8}", .{i}),
        .value = "original",
    };
    try docs.file.putIndexCatalogBatch(mutations);
    const pinned = docs.file.activeCheckpoint();
    const size = (try docs.file.file.stat(std.testing.io)).size;
    var store = Store.initWithNamespace(a, &docs, "/scope");
    const before = docs.file.test_page_writes.load(.monotonic);
    docs.file.test_page_write_fail_after = 30;
    try std.testing.expectError(error.TestPageWriteFailure, store.storage().deleteTree("/scope"));
    docs.file.test_page_write_fail_after = null;
    try std.testing.expect(docs.file.test_page_writes.load(.monotonic) > before);
    try std.testing.expectEqual(pinned.index_catalog_root_page, docs.file.activeCheckpoint().index_catalog_root_page);
    try std.testing.expectEqual(size, (try docs.file.file.stat(std.testing.io)).size);
    try std.testing.expect((try docs.file.check()).valid);
    const retained = (try docs.file.getIndexCatalogRecordAlloc(a, mutations[0].key)).?;
    defer a.free(retained);
    try std.testing.expectEqualStrings("original", retained);
    // Fail allocations both before scanning and after a private batch spills.
    for ([_]usize{ 0, 16, 512, 2048 }) |fail_index| {
        var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = fail_index });
        store.allocator = failing.allocator();
        docs.file.allocator = failing.allocator();
        const result = store.storage().deleteTree("/scope");
        store.allocator = a;
        docs.file.allocator = a;
        try std.testing.expectError(error.OutOfMemory, result);
        try std.testing.expectEqual(pinned.index_catalog_root_page, docs.file.activeCheckpoint().index_catalog_root_page);
        try std.testing.expectEqual(size, (try docs.file.file.stat(std.testing.io)).size);
        try std.testing.expect((try docs.file.check()).valid);
    }
    try store.storage().deleteTree("/scope");
    try std.testing.expectEqual(pinned.commit_sequence + 1, docs.file.activeCheckpoint().commit_sequence);
    var cursor = try docs.file.indexCatalogCursor(docs.file.activeCheckpoint(), "/scope/");
    defer cursor.deinit();
    try std.testing.expect((try cursor.next()) == null);
    try std.testing.expect((try docs.file.check()).valid);
}
