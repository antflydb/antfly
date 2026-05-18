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
const fs_paths = @import("../../common/fs_paths.zig");

const Allocator = std.mem.Allocator;
const CounterU64 = platform.atomic.Value(u64);
const supports_native_storage = builtin.os.tag != .freestanding;
const supports_posix_fd_cache = supports_native_storage and
    builtin.os.tag != .windows and
    builtin.os.tag != .wasi and
    (builtin.os.tag == .linux or builtin.link_libc) and
    @hasDecl(std.posix.system, "pread");
// TODO: Re-enable Linux evented storage once std.Io.Evented/std.Io.Uring is
// stable enough for this code path. In Zig 0.16, instantiating std.Io.Uring
// trips stdlib error-set mismatches: std/Io/Uring.zig's dirOpenDir and
// dirRealPathFile propagate openat's error.ReadOnlyFileSystem into std/Io/Dir.zig
// error sets that do not include it.
const supports_evented_runtime = false;
const max_cached_native_fds: usize = 1024;
const fd_cache_shard_count: usize = if (builtin.os.tag == .freestanding) 1 else 16;
const max_posix_io_chunk: usize = 64 * 1024 * 1024;

pub const RuntimeKind = enum {
    threaded,
    evented,
};

pub const NativeStorageStats = struct {
    fd_cache_entries: usize = 0,
    fd_cache_capacity: usize = 0,
};

pub const AtomicWriteSink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        len: *const fn (*anyopaque) usize,
        append_slice: *const fn (*anyopaque, []const u8) anyerror!void,
        write_at: *const fn (*anyopaque, usize, []const u8) anyerror!void,
        crc32_prefix: *const fn (*anyopaque, usize) anyerror!u32,
        finish: *const fn (*anyopaque) anyerror!void,
        abort: *const fn (*anyopaque) void,
    };

    pub fn len(self: *const AtomicWriteSink) usize {
        return self.vtable.len(self.ptr);
    }

    pub fn appendSlice(self: *AtomicWriteSink, bytes: []const u8) !void {
        try self.vtable.append_slice(self.ptr, bytes);
    }

    pub fn appendByte(self: *AtomicWriteSink, byte: u8) !void {
        const one = [1]u8{byte};
        try self.appendSlice(&one);
    }

    pub fn writeAt(self: *AtomicWriteSink, offset: usize, bytes: []const u8) !void {
        try self.vtable.write_at(self.ptr, offset, bytes);
    }

    pub fn crc32Prefix(self: *AtomicWriteSink, len_prefix: usize) !u32 {
        return try self.vtable.crc32_prefix(self.ptr, len_prefix);
    }

    /// Atomically publish the written bytes at the requested destination.
    /// Consumes the sink whether publishing succeeds or fails.
    pub fn finish(self: *AtomicWriteSink) !void {
        try self.vtable.finish(self.ptr);
    }

    /// Discard all bytes and remove any temporary object.
    /// Consumes the sink.
    pub fn abort(self: *AtomicWriteSink) void {
        self.vtable.abort(self.ptr);
    }
};

pub const Storage = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        create_dir_path: *const fn (*anyopaque, []const u8) anyerror!void,
        read_file_alloc: *const fn (*anyopaque, Allocator, []const u8, usize) anyerror![]u8,
        read_file_range_alloc: *const fn (*anyopaque, Allocator, []const u8, u64, usize) anyerror![]u8,
        read_file_range_into: ?*const fn (*anyopaque, []const u8, u64, []u8) anyerror!void = null,
        read_file_range_at_most_into: ?*const fn (*anyopaque, []const u8, u64, []u8) anyerror!usize = null,
        file_size: *const fn (*anyopaque, []const u8) anyerror!u64,
        read_file_trailer_alloc: ?*const fn (*anyopaque, Allocator, []const u8, usize) anyerror![]u8 = null,
        write_file_absolute: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        append_file_absolute: ?*const fn (*anyopaque, []const u8, []const u8, bool) anyerror!void = null,
        begin_atomic_write: ?*const fn (*anyopaque, Allocator, []const u8) anyerror!AtomicWriteSink = null,
        rename_absolute: *const fn (*anyopaque, []const u8, []const u8) anyerror!void,
        delete_file_absolute: *const fn (*anyopaque, []const u8) anyerror!void,
        delete_tree: *const fn (*anyopaque, []const u8) anyerror!void,
        now_ns: *const fn (*anyopaque) u64,
    };

    pub fn createDirPath(self: Storage, path: []const u8) !void {
        return self.vtable.create_dir_path(self.ptr, path);
    }

    pub fn readFileAlloc(self: Storage, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
        return self.vtable.read_file_alloc(self.ptr, allocator, path, max_bytes);
    }

    pub fn readFileRangeAlloc(self: Storage, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
        return self.vtable.read_file_range_alloc(self.ptr, allocator, path, offset, len);
    }

    pub fn readFileRangeInto(self: Storage, allocator: Allocator, path: []const u8, offset: u64, out: []u8) !void {
        if (self.vtable.read_file_range_into) |read_file_range_into| {
            return read_file_range_into(self.ptr, path, offset, out);
        }
        const loaded = try self.readFileRangeAlloc(allocator, path, offset, out.len);
        defer allocator.free(loaded);
        if (loaded.len != out.len) return error.EndOfStream;
        @memcpy(out, loaded);
    }

    pub fn readFileRangeAtMostInto(self: Storage, allocator: Allocator, path: []const u8, offset: u64, out: []u8) !usize {
        if (self.vtable.read_file_range_at_most_into) |read_file_range_at_most_into| {
            return try read_file_range_at_most_into(self.ptr, path, offset, out);
        }
        const size = self.fileSize(path) catch |err| switch (err) {
            error.FileNotFound => return 0,
            else => return err,
        };
        if (offset >= size) return 0;
        const len: usize = @intCast(@min(size - offset, out.len));
        try self.readFileRangeInto(allocator, path, offset, out[0..len]);
        return len;
    }

    pub fn fileSize(self: Storage, path: []const u8) !u64 {
        return self.vtable.file_size(self.ptr, path);
    }

    pub fn readFileTrailerAlloc(self: Storage, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
        if (self.vtable.read_file_trailer_alloc) |read_file_trailer_alloc| {
            return read_file_trailer_alloc(self.ptr, allocator, path, len);
        }

        const size = try self.fileSize(path);
        if (size < len) return error.EndOfStream;
        return try self.readFileRangeAlloc(allocator, path, size - len, len);
    }

    pub fn writeFileAbsolute(self: Storage, path: []const u8, contents: []const u8) !void {
        return self.vtable.write_file_absolute(self.ptr, path, contents);
    }

    pub fn appendFileAbsolute(self: Storage, allocator: Allocator, path: []const u8, contents: []const u8, sync: bool) !void {
        if (self.vtable.append_file_absolute) |append_file_absolute| {
            return append_file_absolute(self.ptr, path, contents, sync);
        }

        const existing = self.readFileAlloc(allocator, path, std.math.maxInt(usize)) catch |err| switch (err) {
            error.FileNotFound => {
                try self.writeFileAbsolute(path, contents);
                return;
            },
            else => return err,
        };
        defer allocator.free(existing);

        const joined = try allocator.alloc(u8, existing.len + contents.len);
        defer allocator.free(joined);
        @memcpy(joined[0..existing.len], existing);
        @memcpy(joined[existing.len..], contents);
        try self.writeFileAbsolute(path, joined);
    }

    pub fn beginAtomicWrite(self: Storage, allocator: Allocator, path: []const u8) !AtomicWriteSink {
        if (self.vtable.begin_atomic_write) |begin_atomic_write| {
            return try begin_atomic_write(self.ptr, allocator, path);
        }
        return try BufferedAtomicWriteSink.create(allocator, self, path);
    }

    pub fn renameAbsolute(self: Storage, old_path: []const u8, new_path: []const u8) !void {
        return self.vtable.rename_absolute(self.ptr, old_path, new_path);
    }

    pub fn deleteFileAbsolute(self: Storage, path: []const u8) !void {
        return self.vtable.delete_file_absolute(self.ptr, path);
    }

    pub fn deleteTree(self: Storage, path: []const u8) !void {
        return self.vtable.delete_tree(self.ptr, path);
    }

    pub fn nowNs(self: Storage) u64 {
        return self.vtable.now_ns(self.ptr);
    }
};

pub fn createDirPathPortable(io: anytype, path: []const u8) !void {
    return fs_paths.createDirPathPortable(io, path);
}

/// Thin wrapper for host-provided storage callbacks.
/// Intended for embedders that want durable LSM semantics without native fs access,
/// such as wasm or foreign host runtimes.
pub const HostStorage = struct {
    ptr: *anyopaque,
    vtable: *const Storage.VTable,

    pub fn init(ptr: *anyopaque, vtable: *const Storage.VTable) HostStorage {
        return .{
            .ptr = ptr,
            .vtable = vtable,
        };
    }

    pub fn storage(self: HostStorage) Storage {
        return .{
            .ptr = self.ptr,
            .vtable = self.vtable,
        };
    }
};

const BufferedAtomicWriteSink = struct {
    allocator: Allocator,
    storage: Storage,
    final_path: []u8,
    tmp_path: []u8,
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn create(allocator: Allocator, storage: Storage, path: []const u8) !AtomicWriteSink {
        const self = try allocator.create(BufferedAtomicWriteSink);
        errdefer allocator.destroy(self);

        const final_path = try allocator.dupe(u8, path);
        errdefer allocator.free(final_path);

        const tmp_path = try tempSiblingPath(allocator, path);
        errdefer allocator.free(tmp_path);

        self.* = .{
            .allocator = allocator,
            .storage = storage,
            .final_path = final_path,
            .tmp_path = tmp_path,
        };
        return .{
            .ptr = self,
            .vtable = &buffered_atomic_write_sink_vtable,
        };
    }

    fn deinit(self: *BufferedAtomicWriteSink) void {
        self.out.deinit(self.allocator);
        self.allocator.free(self.final_path);
        self.allocator.free(self.tmp_path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        try self.out.appendSlice(self.allocator, bytes);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        @memcpy(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidAtomicWriteOffset;
        return std.hash.Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();

        self.storage.writeFileAbsolute(self.tmp_path, self.out.items) catch |err| {
            self.storage.deleteFileAbsolute(self.tmp_path) catch {};
            return err;
        };
        self.storage.renameAbsolute(self.tmp_path, self.final_path) catch |err| {
            self.storage.deleteFileAbsolute(self.tmp_path) catch {};
            return err;
        };
    }

    fn abort(ptr: *anyopaque) void {
        const self: *BufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        self.storage.deleteFileAbsolute(self.tmp_path) catch {};
        self.deinit();
    }
};

const buffered_atomic_write_sink_vtable: AtomicWriteSink.VTable = .{
    .len = BufferedAtomicWriteSink.len,
    .append_slice = BufferedAtomicWriteSink.appendSlice,
    .write_at = BufferedAtomicWriteSink.writeAt,
    .crc32_prefix = BufferedAtomicWriteSink.crc32Prefix,
    .finish = BufferedAtomicWriteSink.finish,
    .abort = BufferedAtomicWriteSink.abort,
};

const FdCache = if (!supports_posix_fd_cache)
    struct {
        pub fn init(_: Allocator) FdCache {
            return .{};
        }

        pub fn deinit(_: *FdCache) void {}

        pub fn snapshotStats(_: *const FdCache) NativeStorageStats {
            return .{};
        }

        pub fn readRangeAlloc(_: *FdCache, _: Allocator, _: []const u8, _: u64, _: usize) ![]u8 {
            return error.UnsupportedNativeStorageRuntime;
        }

        pub fn fileSize(_: *FdCache, _: []const u8) !u64 {
            return error.UnsupportedNativeStorageRuntime;
        }

        pub fn readTrailerAlloc(_: *FdCache, _: Allocator, _: []const u8, _: usize) ![]u8 {
            return error.UnsupportedNativeStorageRuntime;
        }

        pub fn invalidatePath(_: *FdCache, _: []const u8) void {}
        pub fn invalidateTree(_: *FdCache, _: []const u8) void {}
        pub fn invalidateRename(_: *FdCache, _: []const u8, _: []const u8) void {}
    }
else
    struct {
        const BucketMap = std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(*Entry));

        const Entry = struct {
            path_hash: u64,
            path: [:0]u8,
            fd: std.posix.fd_t,
            ref_count: usize = 0,
            last_access: u64 = 0,
            invalidated: bool = false,
            lru_prev: ?*Entry = null,
            lru_next: ?*Entry = null,

            fn deinit(self: *Entry, allocator: Allocator) void {
                closeFd(self.fd);
                allocator.free(self.path);
                self.* = undefined;
            }
        };

        const Shard = struct {
            mutex: std.atomic.Mutex = .unlocked,
            entries: BucketMap = .empty,
            lru_head: ?*Entry = null,
            lru_tail: ?*Entry = null,
        };

        allocator: Allocator,
        shards: []Shard,
        entry_count: std.atomic.Value(usize) = .init(0),
        access_clock: CounterU64 = .init(0),
        evict_cursor: std.atomic.Value(usize) = .init(0),
        evict_mutex: std.atomic.Mutex = .unlocked,

        fn init(allocator: Allocator) FdCache {
            const shards = allocator.alloc(Shard, fd_cache_shard_count) catch @panic("OOM");
            @memset(shards, .{});
            return .{
                .allocator = allocator,
                .shards = shards,
            };
        }

        fn deinit(self: *FdCache) void {
            for (self.shards) |*shard| {
                var current = shard.lru_head;
                while (current) |entry| {
                    const next = entry.lru_next;
                    entry.deinit(self.allocator);
                    self.allocator.destroy(entry);
                    current = next;
                }

                var it = shard.entries.iterator();
                while (it.next()) |bucket| {
                    bucket.value_ptr.deinit(self.allocator);
                }
                shard.entries.deinit(self.allocator);
            }
            self.allocator.free(self.shards);
            self.* = undefined;
        }

        fn snapshotStats(self: *const FdCache) NativeStorageStats {
            return .{
                .fd_cache_entries = self.entry_count.load(.monotonic),
                .fd_cache_capacity = max_cached_native_fds,
            };
        }

        fn readRangeAlloc(self: *FdCache, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const entry = try self.retain(path);
            defer self.release(entry);

            const out = try allocator.alloc(u8, len);
            errdefer allocator.free(out);
            try readAllAtOffset(entry.fd, out, offset);
            return out;
        }

        fn readRangeInto(self: *FdCache, path: []const u8, offset: u64, out: []u8) !void {
            const entry = try self.retain(path);
            defer self.release(entry);
            try readAllAtOffset(entry.fd, out, offset);
        }

        fn readRangeAtMostInto(self: *FdCache, path: []const u8, offset: u64, out: []u8) !usize {
            const entry = try self.retain(path);
            defer self.release(entry);
            return try readAtMostAtOffset(entry.fd, out, offset);
        }

        fn fileSize(self: *FdCache, path: []const u8) !u64 {
            const entry = try self.retain(path);
            defer self.release(entry);
            return try fileSizeFromFd(entry.fd);
        }

        fn readTrailerAlloc(self: *FdCache, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const entry = try self.retain(path);
            defer self.release(entry);

            const size = try fileSizeFromFd(entry.fd);
            if (size < len) return error.EndOfStream;

            const out = try allocator.alloc(u8, len);
            errdefer allocator.free(out);
            try readAllAtOffset(entry.fd, out, size - len);
            return out;
        }

        fn invalidatePath(self: *FdCache, path: []const u8) void {
            const path_hash = hashPath(path);
            const shard = self.shardForHash(path_hash);
            const locked = lockAtomic(&shard.mutex);
            defer if (locked) shard.mutex.unlock();

            var matched_any = false;
            if (shard.entries.getPtr(path_hash)) |bucket| {
                for (bucket.items) |entry| {
                    if (!std.mem.eql(u8, entry.path, path)) continue;
                    entry.invalidated = true;
                    matched_any = true;
                }
            }

            if (!matched_any) return;

            while (self.findInvalidatedEntryLocked(shard, path_hash, path)) |entry| {
                if (entry.ref_count != 0) break;
                self.removeEntryLocked(shard, entry);
            }
        }

        fn invalidateTree(self: *FdCache, path: []const u8) void {
            for (self.shards) |*shard| {
                const locked = lockAtomic(&shard.mutex);

                var current = shard.lru_head;
                while (current) |entry| {
                    const next = entry.lru_next;
                    if (pathContains(path, entry.path)) {
                        entry.invalidated = true;
                        if (entry.ref_count == 0) self.removeEntryLocked(shard, entry);
                    }
                    current = next;
                }
                if (locked) shard.mutex.unlock();
            }
        }

        fn invalidateRename(self: *FdCache, old_path: []const u8, new_path: []const u8) void {
            self.invalidatePath(old_path);
            self.invalidatePath(new_path);
        }

        fn retain(self: *FdCache, path: []const u8) !*Entry {
            const path_hash = hashPath(path);
            const shard = self.shardForHash(path_hash);
            {
                const locked = lockAtomic(&shard.mutex);
                defer if (locked) shard.mutex.unlock();

                if (self.findEntryLocked(shard, path_hash, path)) |entry| {
                    entry.ref_count += 1;
                    entry.last_access = self.nextAccessLocked();
                    self.touchEntryLocked(shard, entry);
                    return entry;
                }
            }

            const owned_path = try self.allocator.dupeZ(u8, path);
            errdefer self.allocator.free(owned_path);

            const fd = try std.posix.openatZ(std.posix.AT.FDCWD, owned_path, .{
                .ACCMODE = .RDONLY,
                .CLOEXEC = true,
            }, 0);
            errdefer closeFd(fd);

            const entry = try self.allocator.create(Entry);
            errdefer self.allocator.destroy(entry);
            entry.* = .{
                .path_hash = path_hash,
                .path = owned_path,
                .fd = fd,
                .ref_count = 1,
                .last_access = self.nextAccessLocked(),
            };

            {
                const locked = lockAtomic(&shard.mutex);
                errdefer if (locked) shard.mutex.unlock();

                if (self.findEntryLocked(shard, path_hash, owned_path)) |existing| {
                    existing.ref_count += 1;
                    existing.last_access = self.nextAccessLocked();
                    self.touchEntryLocked(shard, existing);
                    if (locked) shard.mutex.unlock();
                    entry.deinit(self.allocator);
                    self.allocator.destroy(entry);
                    return existing;
                }

                const bucket = try shard.entries.getOrPut(self.allocator, path_hash);
                if (!bucket.found_existing) bucket.value_ptr.* = .empty;
                try bucket.value_ptr.append(self.allocator, entry);
                self.linkEntryLocked(shard, entry);
                _ = self.entry_count.fetchAdd(1, .monotonic);
                if (locked) shard.mutex.unlock();
            }

            self.evictToBudget();
            return entry;
        }

        fn release(self: *FdCache, entry: *Entry) void {
            const shard = self.shardForHash(entry.path_hash);
            const locked = lockAtomic(&shard.mutex);

            std.debug.assert(entry.ref_count > 0);
            entry.ref_count -= 1;
            entry.last_access = self.nextAccessLocked();
            self.touchEntryLocked(shard, entry);
            if (entry.ref_count == 0 and entry.invalidated) {
                self.removeEntryLocked(shard, entry);
                if (locked) shard.mutex.unlock();
                return;
            }
            if (locked) shard.mutex.unlock();
            self.evictToBudget();
        }

        fn evictToBudget(self: *FdCache) void {
            const locked = lockAtomic(&self.evict_mutex);
            defer if (locked) self.evict_mutex.unlock();

            while (self.entry_count.load(.monotonic) >= max_cached_native_fds and self.evictOne()) {}
        }

        fn evictOne(self: *FdCache) bool {
            const start = self.evict_cursor.fetchAdd(1, .monotonic);
            for (0..self.shards.len) |offset| {
                const shard = &self.shards[(start + offset) % self.shards.len];
                const locked = lockAtomic(&shard.mutex);

                var current = shard.lru_head;
                while (current) |entry| {
                    if (entry.ref_count == 0) {
                        self.removeEntryLocked(shard, entry);
                        if (locked) shard.mutex.unlock();
                        return true;
                    }
                    current = entry.lru_next;
                }
                if (locked) shard.mutex.unlock();
            }
            return false;
        }

        fn findEntryLocked(self: *FdCache, shard: *Shard, path_hash: u64, path: []const u8) ?*Entry {
            _ = self;
            const bucket = shard.entries.getPtr(path_hash) orelse return null;
            var i = bucket.items.len;
            while (i > 0) {
                i -= 1;
                const entry = bucket.items[i];
                if (!entry.invalidated and std.mem.eql(u8, entry.path, path)) return entry;
            }
            return null;
        }

        fn findInvalidatedEntryLocked(self: *FdCache, shard: *Shard, path_hash: u64, path: []const u8) ?*Entry {
            _ = self;
            const bucket = shard.entries.getPtr(path_hash) orelse return null;
            var i = bucket.items.len;
            while (i > 0) {
                i -= 1;
                const entry = bucket.items[i];
                if (entry.invalidated and std.mem.eql(u8, entry.path, path)) return entry;
            }
            return null;
        }

        fn removeEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            const bucket = shard.entries.getPtr(entry.path_hash) orelse unreachable;
            for (bucket.items, 0..) |bucket_entry, i| {
                if (bucket_entry != entry) continue;
                _ = bucket.orderedRemove(i);
                if (bucket.items.len == 0) {
                    var removed = shard.entries.fetchRemove(entry.path_hash) orelse unreachable;
                    removed.value.deinit(self.allocator);
                }
                self.unlinkEntryLocked(shard, entry);
                std.debug.assert(entry.ref_count == 0);
                _ = self.entry_count.fetchSub(1, .monotonic);
                entry.deinit(self.allocator);
                self.allocator.destroy(entry);
                return;
            }
            unreachable;
        }

        fn shardForHash(self: *FdCache, path_hash: u64) *Shard {
            return &self.shards[@intCast(path_hash % self.shards.len)];
        }

        fn linkEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            _ = self;
            entry.lru_prev = shard.lru_tail;
            entry.lru_next = null;
            if (entry.lru_prev) |prev| {
                prev.lru_next = entry;
            } else {
                shard.lru_head = entry;
            }
            shard.lru_tail = entry;
        }

        fn unlinkEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            _ = self;
            if (entry.lru_prev) |prev| {
                prev.lru_next = entry.lru_next;
            } else {
                shard.lru_head = entry.lru_next;
            }
            if (entry.lru_next) |next| {
                next.lru_prev = entry.lru_prev;
            } else {
                shard.lru_tail = entry.lru_prev;
            }
            entry.lru_prev = null;
            entry.lru_next = null;
        }

        fn touchEntryLocked(self: *FdCache, shard: *Shard, entry: *Entry) void {
            if (shard.lru_tail == entry) return;
            self.unlinkEntryLocked(shard, entry);
            self.linkEntryLocked(shard, entry);
        }

        fn nextAccessLocked(self: *FdCache) u64 {
            return self.access_clock.fetchAdd(1, .monotonic) + 1;
        }
    };

const NativeStorageState = struct {
    // Storage handles may be copied into background work. Keep the runtime and
    // fd cache behind a ref-counted state so late operations never dereference a
    // NativeStorage wrapper that backend close has already destroyed.
    allocator: Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    closing: std.atomic.Value(bool) = .init(false),
    threaded: std.Io.Threaded,
    fd_cache: FdCache,

    fn create(allocator: Allocator, kind: RuntimeKind) !*NativeStorageState {
        if (kind != .threaded) return error.UnsupportedEventedIoRuntime;
        const state = try allocator.create(NativeStorageState);
        errdefer allocator.destroy(state);
        var threaded = std.Io.Threaded.init(allocator, .{});
        errdefer threaded.deinit();
        state.* = .{
            .allocator = allocator,
            .threaded = threaded,
            .fd_cache = FdCache.init(allocator),
        };
        return state;
    }

    fn retain(self: *NativeStorageState) !*NativeStorageState {
        while (true) {
            if (self.closing.load(.acquire)) return error.StorageClosed;
            const current = self.refs.load(.acquire);
            if (current == 0) return error.StorageClosed;
            if (self.refs.cmpxchgWeak(current, current + 1, .acq_rel, .acquire)) |_| {
                continue;
            }
            if (self.closing.load(.acquire)) {
                self.release();
                return error.StorageClosed;
            }
            return self;
        }
    }

    fn closeStorageRef(self: *NativeStorageState) void {
        self.closing.store(true, .release);
        self.release();
    }

    fn release(self: *NativeStorageState) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.allocator;
        self.fd_cache.deinit();
        self.threaded.deinit();
        self.* = undefined;
        allocator.destroy(self);
    }

    fn invalidatePath(self: *NativeStorageState, path: []const u8) void {
        self.fd_cache.invalidatePath(path);
    }

    fn invalidateRename(self: *NativeStorageState, old_path: []const u8, new_path: []const u8) void {
        self.fd_cache.invalidateRename(old_path, new_path);
    }

    fn invalidateTree(self: *NativeStorageState, path: []const u8) void {
        self.fd_cache.invalidateTree(path);
    }
};

pub const NativeStorage = if (!supports_native_storage)
    struct {
        pub fn init(_: Allocator, _: RuntimeKind) !NativeStorage {
            return error.UnsupportedNativeStorageRuntime;
        }

        pub fn deinit(_: *NativeStorage) void {}

        pub fn snapshotStats(_: *const NativeStorage) NativeStorageStats {
            return .{};
        }

        pub fn storage(_: *NativeStorage) Storage {
            @panic("native storage is unavailable on this target");
        }
    }
else blk: {
    if (supports_evented_runtime) {
        break :blk struct {
            runtime: union(RuntimeKind) {
                threaded: std.Io.Threaded,
                evented: std.Io.Evented,
            },
            state: *NativeStorageState,

            const native_vtable: Storage.VTable = .{
                .create_dir_path = createDirPath,
                .read_file_alloc = readFileAlloc,
                .read_file_range_alloc = readFileRangeAlloc,
                .read_file_range_into = readFileRangeInto,
                .read_file_range_at_most_into = readFileRangeAtMostInto,
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

            pub fn init(allocator: Allocator, kind: RuntimeKind) !NativeStorage {
                var runtime = switch (kind) {
                    .threaded => .{ .threaded = std.Io.Threaded.init(allocator, .{}) },
                    .evented => blk2: {
                        var evented: std.Io.Evented = undefined;
                        try std.Io.Evented.init(&evented, allocator, .{});
                        break :blk2 .{ .evented = evented };
                    },
                };
                errdefer switch (runtime) {
                    .threaded => |*threaded| threaded.deinit(),
                    .evented => |*evented| std.Io.Evented.deinit(evented),
                };
                return .{
                    .runtime = runtime,
                    .state = try NativeStorageState.create(allocator),
                };
            }

            pub fn deinit(self: *NativeStorage) void {
                self.state.closeStorageRef();
                switch (self.runtime) {
                    .threaded => |*threaded| threaded.deinit(),
                    .evented => |*evented| std.Io.Evented.deinit(evented),
                }
                self.* = undefined;
            }

            pub fn snapshotStats(self: *const NativeStorage) NativeStorageStats {
                return self.state.fd_cache.snapshotStats();
            }

            pub fn storage(self: *NativeStorage) Storage {
                return .{
                    .ptr = self,
                    .vtable = &native_vtable,
                };
            }

            fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                switch (self.runtime) {
                    .threaded => |*threaded| try createDirPathPortable(threaded.io(), path),
                    .evented => |*evented| try createDirPathPortable(evented.io(), path),
                }
            }

            fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                return switch (self.runtime) {
                    .threaded => |*threaded| try std.Io.Dir.cwd().readFileAlloc(threaded.io(), path, allocator, .limited(max_bytes)),
                    .evented => |*evented| try std.Io.Dir.cwd().readFileAlloc(evented.io(), path, allocator, .limited(max_bytes)),
                };
            }

            fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                if (comptime supports_posix_fd_cache) {
                    const state = try self.state.retain();
                    defer state.release();
                    return try state.fd_cache.readRangeAlloc(allocator, path, offset, len);
                }
                return switch (self.runtime) {
                    .threaded => |*threaded| try readFileRangeWithIo(threaded.io(), allocator, path, offset, len),
                    .evented => |*evented| try readFileRangeWithIo(evented.io(), allocator, path, offset, len),
                };
            }

            fn readFileRangeInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                if (comptime supports_posix_fd_cache) {
                    const state = try self.state.retain();
                    defer state.release();
                    return try state.fd_cache.readRangeInto(path, offset, out);
                }
                return switch (self.runtime) {
                    .threaded => |*threaded| try readFileRangeWithIoInto(threaded.io(), path, offset, out),
                    .evented => |*evented| try readFileRangeWithIoInto(evented.io(), path, offset, out),
                };
            }

            fn readFileRangeAtMostInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !usize {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                if (comptime supports_posix_fd_cache) {
                    const state = try self.state.retain();
                    defer state.release();
                    return try state.fd_cache.readRangeAtMostInto(path, offset, out);
                }
                return switch (self.runtime) {
                    .threaded => |*threaded| try readFileRangeWithIoAtMostInto(threaded.io(), path, offset, out),
                    .evented => |*evented| try readFileRangeWithIoAtMostInto(evented.io(), path, offset, out),
                };
            }

            fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                if (comptime supports_posix_fd_cache) {
                    const state = try self.state.retain();
                    defer state.release();
                    return try state.fd_cache.fileSize(path);
                }
                return switch (self.runtime) {
                    .threaded => |*threaded| try fileSizeWithIo(threaded.io(), path),
                    .evented => |*evented| try fileSizeWithIo(evented.io(), path),
                };
            }

            fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                if (comptime supports_posix_fd_cache) {
                    const state = try self.state.retain();
                    defer state.release();
                    return try state.fd_cache.readTrailerAlloc(allocator, path, len);
                }
                return switch (self.runtime) {
                    .threaded => |*threaded| try readFileTrailerWithIo(threaded.io(), allocator, path, len),
                    .evented => |*evented| try readFileTrailerWithIo(evented.io(), allocator, path, len),
                };
            }

            fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                self.state.invalidatePath(path);
                switch (self.runtime) {
                    .threaded => |*threaded| try writeFileAbsoluteWithIo(threaded.io(), path, contents),
                    .evented => |*evented| try writeFileAbsoluteWithIo(evented.io(), path, contents),
                }
            }

            fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                self.state.invalidatePath(path);
                switch (self.runtime) {
                    .threaded => |*threaded| try appendFileAbsoluteWithIo(threaded.io(), path, contents, sync),
                    .evented => |*evented| try appendFileAbsoluteWithIo(evented.io(), path, contents, sync),
                }
            }

            fn beginAtomicWrite(ptr: *anyopaque, allocator: Allocator, path: []const u8) !AtomicWriteSink {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                return try NativeAtomicWriteSink.create(allocator, path, self.state);
            }

            fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                self.state.invalidateRename(old_path, new_path);
                switch (self.runtime) {
                    .threaded => |*threaded| try renamePathWithIo(threaded.io(), old_path, new_path),
                    .evented => |*evented| try renamePathWithIo(evented.io(), old_path, new_path),
                }
            }

            fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                self.state.invalidatePath(path);
                switch (self.runtime) {
                    .threaded => |*threaded| try deleteFilePathWithIo(threaded.io(), path),
                    .evented => |*evented| try deleteFilePathWithIo(evented.io(), path),
                }
            }

            fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                self.state.invalidateTree(path);
                switch (self.runtime) {
                    .threaded => |*threaded| try std.Io.Dir.cwd().deleteTree(threaded.io(), path),
                    .evented => |*evented| try std.Io.Dir.cwd().deleteTree(evented.io(), path),
                }
            }

            fn nowNs(ptr: *anyopaque) u64 {
                const self: *NativeStorage = @ptrCast(@alignCast(ptr));
                return switch (self.runtime) {
                    .threaded => |*threaded| blk2: {
                        const now = std.Io.Timestamp.now(threaded.io(), .awake);
                        break :blk2 @intCast(now.toNanoseconds());
                    },
                    .evented => |*evented| blk2: {
                        const now = std.Io.Timestamp.now(evented.io(), .awake);
                        break :blk2 @intCast(now.toNanoseconds());
                    },
                };
            }
        };
    }

    break :blk struct {
        state: *NativeStorageState,

        const threaded_only_vtable: Storage.VTable = .{
            .create_dir_path = createDirPath,
            .read_file_alloc = readFileAlloc,
            .read_file_range_alloc = readFileRangeAlloc,
            .read_file_range_into = readFileRangeInto,
            .read_file_range_at_most_into = readFileRangeAtMostInto,
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

        pub fn init(allocator: Allocator, kind: RuntimeKind) !NativeStorage {
            return .{ .state = try NativeStorageState.create(allocator, kind) };
        }

        pub fn deinit(self: *NativeStorage) void {
            self.state.closeStorageRef();
            self.* = undefined;
        }

        pub fn snapshotStats(self: *const NativeStorage) NativeStorageStats {
            return self.state.fd_cache.snapshotStats();
        }

        pub fn storage(self: *NativeStorage) Storage {
            return .{
                .ptr = self.state,
                .vtable = &threaded_only_vtable,
            };
        }

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            try createDirPathPortable(retained.threaded.io(), path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            return try std.Io.Dir.cwd().readFileAlloc(retained.threaded.io(), path, allocator, .limited(max_bytes));
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            if (comptime supports_posix_fd_cache) {
                return try retained.fd_cache.readRangeAlloc(allocator, path, offset, len);
            }
            return try readFileRangeWithIo(retained.threaded.io(), allocator, path, offset, len);
        }

        fn readFileRangeInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            if (comptime supports_posix_fd_cache) {
                return try retained.fd_cache.readRangeInto(path, offset, out);
            }
            return try readFileRangeWithIoInto(retained.threaded.io(), path, offset, out);
        }

        fn readFileRangeAtMostInto(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !usize {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            if (comptime supports_posix_fd_cache) {
                return try retained.fd_cache.readRangeAtMostInto(path, offset, out);
            }
            return try readFileRangeWithIoAtMostInto(retained.threaded.io(), path, offset, out);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            if (comptime supports_posix_fd_cache) {
                return try retained.fd_cache.fileSize(path);
            }
            return try fileSizeWithIo(retained.threaded.io(), path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            if (comptime supports_posix_fd_cache) {
                return try retained.fd_cache.readTrailerAlloc(allocator, path, len);
            }
            return try readFileTrailerWithIo(retained.threaded.io(), allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            retained.invalidatePath(path);
            try writeFileAbsoluteWithIo(retained.threaded.io(), path, contents);
        }

        fn appendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            retained.invalidatePath(path);
            try appendFileAbsoluteWithIo(retained.threaded.io(), path, contents, sync);
        }

        fn beginAtomicWrite(ptr: *anyopaque, allocator: Allocator, path: []const u8) !AtomicWriteSink {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            return try NativeAtomicWriteSink.create(allocator, path, state);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            retained.invalidateRename(old_path, new_path);
            try renamePathWithIo(retained.threaded.io(), old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            retained.invalidatePath(path);
            try deleteFilePathWithIo(retained.threaded.io(), path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = try state.retain();
            defer retained.release();
            retained.invalidateTree(path);
            try std.Io.Dir.cwd().deleteTree(retained.threaded.io(), path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const state: *NativeStorageState = @ptrCast(@alignCast(ptr));
            const retained = state.retain() catch return 0;
            defer retained.release();
            const now = std.Io.Timestamp.now(retained.threaded.io(), .awake);
            return @intCast(now.toNanoseconds());
        }
    };
};

fn writeFileAbsoluteWithIo(io: anytype, path: []const u8, contents: []const u8) !void {
    var file = openFilePathForWriteWithIo(io, path, .{ .truncate = true }) catch |err| {
        std.log.err("lsm writeFileAbsolute create failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        return err;
    };
    defer file.close(io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &file_buf);
    writer.interface.writeAll(contents) catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm writeFileAbsolute write failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
    writer.end() catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm writeFileAbsolute finish failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
}

fn appendFileAbsoluteWithIo(io: anytype, path: []const u8, contents: []const u8, sync: bool) !void {
    var file = openFilePathForWriteWithIo(io, path, .{ .truncate = false }) catch |err| {
        std.log.err("lsm appendFileAbsolute create failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        return err;
    };
    defer file.close(io);

    const size = (try file.stat(io)).size;
    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(io, &file_buf);
    writer.seekTo(size) catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm appendFileAbsolute seek failed path={s} offset={} err={s}", .{ path, size, @errorName(actual) });
        return actual;
    };
    writer.interface.writeAll(contents) catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm appendFileAbsolute write failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
    writer.end() catch |err| {
        const actual = if (err == error.WriteFailed) writer.err orelse err else err;
        std.log.err("lsm appendFileAbsolute finish failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(actual) });
        return actual;
    };
    if (sync) file.sync(io) catch |err| {
        std.log.err("lsm appendFileAbsolute sync failed path={s} bytes={} err={s}", .{ path, contents.len, @errorName(err) });
        return err;
    };
}

fn openFilePathForWriteWithIo(io: anytype, path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    return try fs_paths.createFilePortable(io, path, flags);
}

fn readFileRangeWithIo(io: anytype, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(offset);
    const out = try allocator.alloc(u8, len);
    errdefer allocator.free(out);
    try reader.interface.readSliceAll(out);
    return out;
}

fn readFileRangeWithIoInto(io: anytype, path: []const u8, offset: u64, out: []u8) !void {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(offset);
    try reader.interface.readSliceAll(out);
}

fn readFileRangeWithIoAtMostInto(io: anytype, path: []const u8, offset: u64, out: []u8) !usize {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    var reader = file.reader(io, &.{});
    try reader.seekTo(offset);
    return try reader.interface.readSliceShort(out);
}

fn fileSizeWithIo(io: anytype, path: []const u8) !u64 {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);
    return (try file.stat(io)).size;
}

fn readFileTrailerWithIo(io: anytype, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.openFileAbsolute(io, path, .{})
    else
        try std.Io.Dir.cwd().openFile(io, path, .{});
    defer file.close(io);

    const size = (try file.stat(io)).size;
    if (size < len) return error.EndOfStream;

    var reader = file.reader(io, &.{});
    try reader.seekTo(size - len);
    const out = try allocator.alloc(u8, len);
    errdefer allocator.free(out);
    try reader.interface.readSliceAll(out);
    return out;
}

fn renamePathWithIo(io: anytype, old_path: []const u8, new_path: []const u8) !void {
    if (std.fs.path.isAbsolute(old_path) and std.fs.path.isAbsolute(new_path)) {
        try renameAbsoluteWithIo(io, old_path, new_path);
        return;
    }
    try std.Io.Dir.rename(std.Io.Dir.cwd(), old_path, std.Io.Dir.cwd(), new_path, io);
}

fn renameAbsoluteWithIo(io: anytype, old_path: []const u8, new_path: []const u8) !void {
    if (builtin.os.tag != .windows and builtin.os.tag != .wasi and builtin.os.tag != .freestanding) {
        return try renameAbsolutePosix(old_path, new_path);
    }

    const old_parent_path = std.fs.path.dirname(old_path) orelse return error.FileNotFound;
    const new_parent_path = std.fs.path.dirname(new_path) orelse return error.FileNotFound;
    const old_base_name = std.fs.path.basename(old_path);
    const new_base_name = std.fs.path.basename(new_path);

    var old_parent = try std.Io.Dir.openDirAbsolute(io, old_parent_path, .{});
    defer old_parent.close(io);
    var new_parent = try std.Io.Dir.openDirAbsolute(io, new_parent_path, .{});
    defer new_parent.close(io);

    try std.Io.Dir.rename(old_parent, old_base_name, new_parent, new_base_name, io);
}

fn renameAbsolutePosix(old_path: []const u8, new_path: []const u8) !void {
    const old_parent_path = std.fs.path.dirname(old_path) orelse return error.FileNotFound;
    const new_parent_path = std.fs.path.dirname(new_path) orelse return error.FileNotFound;
    const old_base_name = std.fs.path.basename(old_path);
    const new_base_name = std.fs.path.basename(new_path);

    const old_parent_fd = try std.posix.openat(std.posix.AT.FDCWD, old_parent_path, .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
    defer closeFd(old_parent_fd);
    const new_parent_fd = try std.posix.openat(std.posix.AT.FDCWD, new_parent_path, .{
        .ACCMODE = .RDONLY,
        .DIRECTORY = true,
        .CLOEXEC = true,
    }, 0);
    defer closeFd(new_parent_fd);

    const allocator = std.heap.page_allocator;
    const old_base_name_z = try allocator.dupeZ(u8, old_base_name);
    defer allocator.free(old_base_name_z);
    const new_base_name_z = try allocator.dupeZ(u8, new_base_name);
    defer allocator.free(new_base_name_z);

    while (true) {
        const rc = std.posix.system.renameat(old_parent_fd, old_base_name_z, new_parent_fd, new_base_name_z);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .BUSY => return error.FileBusy,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .MLINK => return error.LinkQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .NOTEMPTY, .EXIST => return error.PathAlreadyExists,
            .XDEV => return error.RenameAcrossMountPoints,
            .INVAL => return renameAbsoluteDirectPosix(old_path, new_path),
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn renameAbsoluteDirectPosix(old_path: []const u8, new_path: []const u8) !void {
    const allocator = std.heap.page_allocator;
    const old_path_z = try allocator.dupeZ(u8, old_path);
    defer allocator.free(old_path_z);
    const new_path_z = try allocator.dupeZ(u8, new_path);
    defer allocator.free(new_path_z);

    while (true) {
        const rc = std.posix.system.rename(old_path_z, new_path_z);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .BUSY => return error.FileBusy,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .MLINK => return error.LinkQuotaExceeded,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .NOMEM => return error.SystemResources,
            .NOSPC => return error.NoSpaceLeft,
            .NOTEMPTY, .EXIST => return error.PathAlreadyExists,
            .XDEV => return error.RenameAcrossMountPoints,
            .INVAL => {
                std.log.err("lsm rename invalid old={s} new={s}", .{ old_path, new_path });
                return error.InvalidArgument;
            },
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn deleteFilePathWithIo(io: anytype, path: []const u8) !void {
    if (!std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.cwd().deleteFile(io, path);
        return;
    }

    const parent_path = std.fs.path.dirname(path) orelse return error.FileNotFound;
    const base_name = std.fs.path.basename(path);
    var parent = try std.Io.Dir.openDirAbsolute(io, parent_path, .{});
    defer parent.close(io);
    try parent.deleteFile(io, base_name);
}

fn createAtomicWriteFdPosix(path: []const u8) !std.posix.fd_t {
    return try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDWR,
        .CREAT = true,
        .CLOEXEC = true,
        .TRUNC = true,
    }, std.Io.File.Permissions.default_file.toMode());
}

fn deleteFilePathPosix(path: []const u8) !void {
    const allocator = std.heap.page_allocator;
    const path_z = try allocator.dupeZ(u8, path);
    defer allocator.free(path_z);

    while (true) {
        const rc = std.posix.system.unlink(path_z);
        switch (std.posix.errno(rc)) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES, .PERM, .ROFS => return error.AccessDenied,
            .BUSY => return error.FileBusy,
            .IO => return error.InputOutput,
            .ISDIR => return error.IsDir,
            .LOOP => return error.SymLinkLoop,
            .NAMETOOLONG => return error.NameTooLong,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            else => |err| return std.posix.unexpectedErrno(err),
        }
    }
}

fn readAllAtOffset(fd: std.posix.fd_t, bytes: []u8, offset: u64) !void {
    var read_len: usize = 0;
    while (read_len < bytes.len) {
        const chunk_len = @min(max_posix_io_chunk, bytes.len - read_len);
        const rc = std.posix.system.pread(fd, bytes.ptr + read_len, chunk_len, @intCast(offset + read_len));
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const n: usize = @intCast(rc);
                if (n == 0) return error.EndOfStream;
                read_len += n;
            },
            .INTR => continue,
            else => |err| return posixReadError(err),
        }
    }
}

fn readAtMostAtOffset(fd: std.posix.fd_t, bytes: []u8, offset: u64) !usize {
    var read_len: usize = 0;
    while (read_len < bytes.len) {
        const chunk_len = @min(max_posix_io_chunk, bytes.len - read_len);
        const rc = std.posix.system.pread(fd, bytes.ptr + read_len, chunk_len, @intCast(offset + read_len));
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const n: usize = @intCast(rc);
                if (n == 0) return read_len;
                read_len += n;
            },
            .INTR => continue,
            else => |err| return posixReadError(err),
        }
    }
    return read_len;
}

fn writeAllAtOffset(fd: std.posix.fd_t, bytes: []const u8, offset: u64) !void {
    var written: usize = 0;
    while (written < bytes.len) {
        const chunk_len = @min(max_posix_io_chunk, bytes.len - written);
        const rc = std.posix.system.pwrite(fd, bytes.ptr + written, chunk_len, @intCast(offset + written));
        switch (std.posix.errno(rc)) {
            .SUCCESS => {
                const n: usize = @intCast(rc);
                if (n == 0) return error.WriteZero;
                written += n;
            },
            .INTR => continue,
            else => |err| return posixWriteError(err),
        }
    }
}

fn closeFd(fd: std.posix.fd_t) void {
    _ = std.posix.system.close(fd);
}

fn fileSizeFromFd(fd: std.posix.fd_t) !u64 {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        while (true) {
            var statx = std.mem.zeroes(linux.Statx);
            switch (linux.errno(linux.statx(fd, "", linux.AT.EMPTY_PATH, .{ .SIZE = true }, &statx))) {
                .SUCCESS => {
                    if (!statx.mask.SIZE) return error.Unexpected;
                    return statx.size;
                },
                .INTR => continue,
                else => |err| return posixStatError(err),
            }
        }
    } else {
        var stat: std.posix.Stat = undefined;
        while (true) {
            const rc = std.posix.system.fstat(fd, &stat);
            switch (std.posix.errno(rc)) {
                .SUCCESS => return @bitCast(stat.size),
                .INTR => continue,
                else => |err| return posixStatError(err),
            }
        }
    }
}

fn posixReadError(err: std.posix.E) anyerror {
    return switch (err) {
        .AGAIN => error.WouldBlock,
        .BADF => error.InvalidFileDescriptor,
        .FAULT => error.InvalidAddress,
        .INVAL => error.InvalidArgument,
        .IO => error.InputOutput,
        .NXIO => error.NoDevice,
        .OVERFLOW => error.FileTooBig,
        else => std.posix.unexpectedErrno(err),
    };
}

fn posixWriteError(err: std.posix.E) anyerror {
    return switch (err) {
        .ACCES, .PERM, .ROFS => error.AccessDenied,
        .AGAIN => error.WouldBlock,
        .BADF => error.InvalidFileDescriptor,
        .DQUOT => error.DiskQuota,
        .FBIG, .OVERFLOW => error.FileTooBig,
        .FAULT => error.InvalidAddress,
        .INVAL => error.InvalidArgument,
        .IO => error.InputOutput,
        .NOSPC => error.NoSpaceLeft,
        .NXIO => error.NoDevice,
        .PIPE => error.BrokenPipe,
        else => std.posix.unexpectedErrno(err),
    };
}

fn posixStatError(err: std.posix.E) anyerror {
    return switch (err) {
        .ACCES, .PERM => error.AccessDenied,
        .BADF => error.InvalidFileDescriptor,
        .FAULT => error.InvalidAddress,
        .IO => error.InputOutput,
        .LOOP => error.SymLinkLoop,
        .NAMETOOLONG => error.NameTooLong,
        .NOENT => error.FileNotFound,
        .NOMEM => error.SystemResources,
        .NOTDIR => error.NotDir,
        .OVERFLOW => error.FileTooBig,
        else => std.posix.unexpectedErrno(err),
    };
}

const NativeBufferedAtomicWriteSink = struct {
    allocator: Allocator,
    state: *NativeStorageState,
    final_path: []u8,
    tmp_path: []u8,
    out: std.ArrayListUnmanaged(u8) = .empty,

    fn create(allocator: Allocator, path: []const u8, state: *NativeStorageState) !AtomicWriteSink {
        const retained_state = try state.retain();
        errdefer retained_state.release();

        const self = try allocator.create(NativeBufferedAtomicWriteSink);
        errdefer allocator.destroy(self);

        const final_path = try allocator.dupe(u8, path);
        errdefer allocator.free(final_path);

        const tmp_path = try tempSiblingPath(allocator, path);
        errdefer allocator.free(tmp_path);

        self.* = .{
            .allocator = allocator,
            .state = retained_state,
            .final_path = final_path,
            .tmp_path = tmp_path,
        };
        return .{
            .ptr = self,
            .vtable = &native_buffered_atomic_write_sink_vtable,
        };
    }

    fn deinit(self: *NativeBufferedAtomicWriteSink) void {
        self.state.release();
        self.out.deinit(self.allocator);
        self.allocator.free(self.final_path);
        self.allocator.free(self.tmp_path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.out.items.len;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        try self.out.appendSlice(self.allocator, bytes);
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.out.items.len or bytes.len > self.out.items.len - offset) return error.InvalidAtomicWriteOffset;
        @memcpy(self.out.items[offset..][0..bytes.len], bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.out.items.len) return error.InvalidAtomicWriteOffset;
        return std.hash.Crc32.hash(self.out.items[0..len_prefix]);
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();

        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();

        self.state.invalidatePath(self.tmp_path);
        writeFileAbsoluteWithIo(io_impl.io(), self.tmp_path, self.out.items) catch |err| {
            deleteFilePathWithIo(io_impl.io(), self.tmp_path) catch {};
            return err;
        };
        self.state.invalidateRename(self.tmp_path, self.final_path);
        renamePathWithIo(io_impl.io(), self.tmp_path, self.final_path) catch |err| {
            deleteFilePathWithIo(io_impl.io(), self.tmp_path) catch {};
            return err;
        };
    }

    fn abort(ptr: *anyopaque) void {
        const self: *NativeBufferedAtomicWriteSink = @ptrCast(@alignCast(ptr));
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        self.state.invalidatePath(self.tmp_path);
        deleteFilePathWithIo(io_impl.io(), self.tmp_path) catch {};
        self.deinit();
    }
};

const native_buffered_atomic_write_sink_vtable: AtomicWriteSink.VTable = .{
    .len = NativeBufferedAtomicWriteSink.len,
    .append_slice = NativeBufferedAtomicWriteSink.appendSlice,
    .write_at = NativeBufferedAtomicWriteSink.writeAt,
    .crc32_prefix = NativeBufferedAtomicWriteSink.crc32Prefix,
    .finish = NativeBufferedAtomicWriteSink.finish,
    .abort = NativeBufferedAtomicWriteSink.abort,
};

const NativeAtomicWriteSink = struct {
    allocator: Allocator,
    state: *NativeStorageState,
    final_path: []u8,
    tmp_path: []u8,
    fd: std.posix.fd_t,
    bytes_written: usize = 0,

    fn create(allocator: Allocator, path: []const u8, state: *NativeStorageState) !AtomicWriteSink {
        if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) {
            return try NativeBufferedAtomicWriteSink.create(allocator, path, state);
        }

        const retained_state = try state.retain();
        errdefer retained_state.release();

        const final_path = try allocator.dupe(u8, path);
        errdefer allocator.free(final_path);

        const tmp_path = try tempSiblingPath(allocator, path);
        errdefer allocator.free(tmp_path);

        const fd = try createAtomicWriteFdPosix(tmp_path);
        errdefer closeFd(fd);

        const self = try allocator.create(NativeAtomicWriteSink);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .state = retained_state,
            .final_path = final_path,
            .tmp_path = tmp_path,
            .fd = fd,
        };
        return .{
            .ptr = self,
            .vtable = &native_atomic_write_sink_vtable,
        };
    }

    fn deinit(self: *NativeAtomicWriteSink) void {
        self.state.release();
        self.allocator.free(self.final_path);
        self.allocator.free(self.tmp_path);
        self.allocator.destroy(self);
    }

    fn len(ptr: *anyopaque) usize {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        return self.bytes_written;
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        try writeAllAtOffset(self.fd, bytes, @intCast(self.bytes_written));
        self.bytes_written += bytes.len;
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (offset > self.bytes_written or bytes.len > self.bytes_written - offset) return error.InvalidAtomicWriteOffset;
        try writeAllAtOffset(self.fd, bytes, @intCast(offset));
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (len_prefix > self.bytes_written) return error.InvalidAtomicWriteOffset;

        var crc = std.hash.Crc32.init();
        var offset: usize = 0;
        var buf: [64 * 1024]u8 = undefined;
        while (offset < len_prefix) {
            const n = @min(buf.len, len_prefix - offset);
            try readAllAtOffset(self.fd, buf[0..n], @intCast(offset));
            crc.update(buf[0..n]);
            offset += n;
        }
        return crc.final();
    }

    fn finish(ptr: *anyopaque) !void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        defer self.deinit();

        closeFd(self.fd);
        self.fd = -1;

        self.state.invalidateRename(self.tmp_path, self.final_path);
        renameAbsoluteDirectPosix(self.tmp_path, self.final_path) catch |err| {
            deleteFilePathPosix(self.tmp_path) catch {};
            return err;
        };
    }

    fn abort(ptr: *anyopaque) void {
        const self: *NativeAtomicWriteSink = @ptrCast(@alignCast(ptr));
        if (self.fd >= 0) closeFd(self.fd);
        self.state.invalidatePath(self.tmp_path);
        deleteFilePathPosix(self.tmp_path) catch {};
        self.deinit();
    }
};

const native_atomic_write_sink_vtable: AtomicWriteSink.VTable = .{
    .len = NativeAtomicWriteSink.len,
    .append_slice = NativeAtomicWriteSink.appendSlice,
    .write_at = NativeAtomicWriteSink.writeAt,
    .crc32_prefix = NativeAtomicWriteSink.crc32Prefix,
    .finish = NativeAtomicWriteSink.finish,
    .abort = NativeAtomicWriteSink.abort,
};

pub const MemoryStorage = struct {
    allocator: Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    files: std.StringHashMapUnmanaged([]u8) = .empty,
    tick: u64 = 1,

    pub fn init(allocator: Allocator) MemoryStorage {
        return .{
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *MemoryStorage) void {
        var it = self.files.iterator();
        while (it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            self.allocator.free(entry.value_ptr.*);
        }
        self.files.deinit(self.allocator);
        self.* = undefined;
    }

    pub fn storage(self: *MemoryStorage) Storage {
        return .{
            .ptr = self,
            .vtable = &memory_vtable,
        };
    }
};

fn lockAtomic(mutex: *std.atomic.Mutex) bool {
    if (builtin.os.tag == .freestanding) return false;
    while (!mutex.tryLock()) std.atomic.spinLoopHint();
    return true;
}

const memory_vtable: Storage.VTable = .{
    .create_dir_path = memoryCreateDirPath,
    .read_file_alloc = memoryReadFileAlloc,
    .read_file_range_alloc = memoryReadFileRangeAlloc,
    .file_size = memoryFileSize,
    .read_file_trailer_alloc = memoryReadFileTrailerAlloc,
    .write_file_absolute = memoryWriteFileAbsolute,
    .append_file_absolute = memoryAppendFileAbsolute,
    .rename_absolute = memoryRenameAbsolute,
    .delete_file_absolute = memoryDeleteFileAbsolute,
    .delete_tree = memoryDeleteTree,
    .now_ns = memoryNowNs,
};

fn memoryCreateDirPath(_: *anyopaque, _: []const u8) !void {}

fn memoryReadFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    if (stored.len > max_bytes) return error.FileTooBig;
    return try allocator.dupe(u8, stored);
}

fn memoryReadFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    const start: usize = @intCast(offset);
    if (start > stored.len or stored.len - start < len) return error.EndOfStream;
    return try allocator.dupe(u8, stored[start .. start + len]);
}

fn memoryFileSize(ptr: *anyopaque, path: []const u8) !u64 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    return stored.len;
}

fn memoryReadFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const stored = self.files.get(path) orelse return error.FileNotFound;
    if (stored.len < len) return error.EndOfStream;
    return try allocator.dupe(u8, stored[stored.len - len ..]);
}

fn memoryWriteFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const owned_path = try self.allocator.dupe(u8, path);
    errdefer self.allocator.free(owned_path);
    const owned_contents = try self.allocator.dupe(u8, contents);
    errdefer self.allocator.free(owned_contents);

    const gop = try self.files.getOrPut(self.allocator, owned_path);
    if (gop.found_existing) {
        self.allocator.free(owned_path);
        self.allocator.free(gop.value_ptr.*);
        gop.value_ptr.* = owned_contents;
    } else {
        gop.value_ptr.* = owned_contents;
    }
}

fn memoryAppendFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8, sync: bool) !void {
    _ = sync;
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    if (self.files.getPtr(path)) |value_ptr| {
        const old = value_ptr.*;
        const joined = try self.allocator.alloc(u8, old.len + contents.len);
        errdefer self.allocator.free(joined);
        @memcpy(joined[0..old.len], old);
        @memcpy(joined[old.len..], contents);
        self.allocator.free(old);
        value_ptr.* = joined;
        return;
    }

    const owned_path = try self.allocator.dupe(u8, path);
    errdefer self.allocator.free(owned_path);
    const owned_contents = try self.allocator.dupe(u8, contents);
    errdefer self.allocator.free(owned_contents);
    try self.files.putNoClobber(self.allocator, owned_path, owned_contents);
}

fn memoryRenameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const removed = self.files.fetchRemove(old_path) orelse return error.FileNotFound;
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

fn memoryDeleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

    const removed = self.files.fetchRemove(path) orelse return error.FileNotFound;
    self.allocator.free(removed.key);
    self.allocator.free(removed.value);
}

fn memoryDeleteTree(ptr: *anyopaque, path: []const u8) !void {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();

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

fn memoryNowNs(ptr: *anyopaque) u64 {
    const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
    const locked = lockAtomic(&self.mutex);
    defer if (locked) self.mutex.unlock();
    const current = self.tick;
    self.tick += 1;
    return current;
}

fn pathContains(prefix: []const u8, path: []const u8) bool {
    if (!std.mem.startsWith(u8, path, prefix)) return false;
    if (path.len == prefix.len) return true;
    return path[prefix.len] == '/';
}

fn tempSiblingPath(allocator: Allocator, path: []const u8) ![]u8 {
    const nonce = atomic_write_nonce.fetchAdd(1, .monotonic);
    return try std.fmt.allocPrint(allocator, "{s}.tmp-{d}", .{ path, nonce });
}

var atomic_write_nonce: CounterU64 = .init(0);

fn hashPath(path: []const u8) u64 {
    return std.hash.Wyhash.hash(0x6d3b7a1db6f9c24f, path);
}

test "host storage delegates through callbacks" {
    var backing = MemoryStorage.init(std.testing.allocator);
    defer backing.deinit();

    const HostContext = struct {
        backing: *MemoryStorage,
        trailer_reads: usize = 0,

        fn createDirPath(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().createDirPath(path);
        }

        fn readFileAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileAlloc(allocator, path, max_bytes);
        }

        fn readFileRangeAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, offset: u64, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().readFileRangeAlloc(allocator, path, offset, len);
        }

        fn fileSize(ptr: *anyopaque, path: []const u8) !u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().fileSize(path);
        }

        fn readFileTrailerAlloc(ptr: *anyopaque, allocator: Allocator, path: []const u8, len: usize) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.trailer_reads += 1;
            return self.backing.storage().readFileTrailerAlloc(allocator, path, len);
        }

        fn writeFileAbsolute(ptr: *anyopaque, path: []const u8, contents: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().writeFileAbsolute(path, contents);
        }

        fn renameAbsolute(ptr: *anyopaque, old_path: []const u8, new_path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().renameAbsolute(old_path, new_path);
        }

        fn deleteFileAbsolute(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteFileAbsolute(path);
        }

        fn deleteTree(ptr: *anyopaque, path: []const u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().deleteTree(path);
        }

        fn nowNs(ptr: *anyopaque) u64 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            return self.backing.storage().nowNs();
        }
    };

    const host_vtable: Storage.VTable = .{
        .create_dir_path = HostContext.createDirPath,
        .read_file_alloc = HostContext.readFileAlloc,
        .read_file_range_alloc = HostContext.readFileRangeAlloc,
        .file_size = HostContext.fileSize,
        .read_file_trailer_alloc = HostContext.readFileTrailerAlloc,
        .write_file_absolute = HostContext.writeFileAbsolute,
        .rename_absolute = HostContext.renameAbsolute,
        .delete_file_absolute = HostContext.deleteFileAbsolute,
        .delete_tree = HostContext.deleteTree,
        .now_ns = HostContext.nowNs,
    };

    var host_ctx = HostContext{ .backing = &backing };
    const host = HostStorage.init(&host_ctx, &host_vtable).storage();

    try host.createDirPath("/host");
    try host.writeFileAbsolute("/host/a.txt", "hello");
    const hello = try host.readFileAlloc(std.testing.allocator, "/host/a.txt", 32);
    defer std.testing.allocator.free(hello);
    try std.testing.expectEqualStrings("hello", hello);
    const ell = try host.readFileRangeAlloc(std.testing.allocator, "/host/a.txt", 1, 3);
    defer std.testing.allocator.free(ell);
    try std.testing.expectEqualStrings("ell", ell);
    const llo = try host.readFileTrailerAlloc(std.testing.allocator, "/host/a.txt", 3);
    defer std.testing.allocator.free(llo);
    try std.testing.expectEqualStrings("llo", llo);
    try std.testing.expectEqual(@as(usize, 1), host_ctx.trailer_reads);

    try host.renameAbsolute("/host/a.txt", "/host/b.txt");
    try std.testing.expectError(error.FileNotFound, host.readFileAlloc(std.testing.allocator, "/host/a.txt", 32));
    const renamed = try host.readFileAlloc(std.testing.allocator, "/host/b.txt", 32);
    defer std.testing.allocator.free(renamed);
    try std.testing.expectEqualStrings("hello", renamed);

    try host.writeFileAbsolute("/host/sub/c.txt", "world");
    try host.deleteTree("/host/sub");
    try std.testing.expectError(error.FileNotFound, host.readFileAlloc(std.testing.allocator, "/host/sub/c.txt", 32));

    try host.deleteFileAbsolute("/host/b.txt");
    try std.testing.expectError(error.FileNotFound, host.readFileAlloc(std.testing.allocator, "/host/b.txt", 32));

    const t0 = host.nowNs();
    const t1 = host.nowNs();
    try std.testing.expect(t1 > t0);
}

test "native atomic write sink supports patching and crc before finish" {
    if (!supports_native_storage) return error.SkipZigTest;

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    defer native.deinit();

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/antfly-storage-atomic-{d}", .{atomic_write_nonce.fetchAdd(1, .monotonic)});
    defer native.storage().deleteFileAbsolute(path) catch {};

    var writer = try native.storage().beginAtomicWrite(std.testing.allocator, path);
    var active = true;
    defer if (active) writer.abort();

    try writer.appendSlice("hello _____");
    try writer.writeAt(6, "world");
    try std.testing.expectEqual(std.hash.Crc32.hash("hello world"), try writer.crc32Prefix(writer.len()));

    active = false;
    try writer.finish();

    const written = try native.storage().readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("hello world", written);
}

test "native atomic write sink retains invalidation state past storage deinit" {
    if (!supports_native_storage) return error.SkipZigTest;
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/antfly-storage-atomic-lease-{d}", .{atomic_write_nonce.fetchAdd(1, .monotonic)});

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    var writer = try native.storage().beginAtomicWrite(std.testing.allocator, path);
    var active = true;
    defer if (active) writer.abort();

    try writer.appendSlice("leased");
    native.deinit();

    active = false;
    try writer.finish();

    var verifier = try NativeStorage.init(std.testing.allocator, .threaded);
    defer verifier.deinit();
    defer verifier.storage().deleteFileAbsolute(path) catch {};

    const written = try verifier.storage().readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("leased", written);
}

test "native buffered atomic write sink retains invalidation state past storage deinit" {
    if (!supports_native_storage) return error.SkipZigTest;
    if (builtin.os.tag == .windows or builtin.os.tag == .wasi or builtin.os.tag == .freestanding) return error.SkipZigTest;

    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "/tmp/antfly-storage-buffered-atomic-lease-{d}", .{atomic_write_nonce.fetchAdd(1, .monotonic)});

    var native = try NativeStorage.init(std.testing.allocator, .threaded);
    var writer = try NativeBufferedAtomicWriteSink.create(std.testing.allocator, path, native.state);
    var active = true;
    defer if (active) writer.abort();

    try writer.appendSlice("buffered _____");
    try writer.writeAt(9, "lease");
    try std.testing.expectEqual(std.hash.Crc32.hash("buffered lease"), try writer.crc32Prefix(writer.len()));
    native.deinit();

    active = false;
    try writer.finish();

    var verifier = try NativeStorage.init(std.testing.allocator, .threaded);
    defer verifier.deinit();
    defer verifier.storage().deleteFileAbsolute(path) catch {};

    const written = try verifier.storage().readFileAlloc(std.testing.allocator, path, 64);
    defer std.testing.allocator.free(written);
    try std.testing.expectEqualStrings("buffered lease", written);
}
