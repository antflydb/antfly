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

//! Persistent full-text index catalog with WAL for crash safety.
//!
//! Wraps IndexWriter (in-memory) with durable metadata. Full-text segment bytes
//! are immutable AFSM/zapx-style files when a host file store is available; the
//! metadata backend tracks active segment ids, ranges, deletions, and committed
//! WAL state. The WAL ensures that in-flight batches survive crashes. On
//! recovery, the WAL is replayed to reconstruct any batches that were written
//! but not yet published.
//!
//! Architecture:
//!   PersistentIndex
//!   ├── IndexWriter (existing, in-memory snapshot management)
//!   ├── Segment files (<path>/segments/*.seg) — immutable full-text segments
//!   ├── Main backend (<path>/index/) — metadata/catalog only
//!   └── WAL backend (<path>/wal/) — pending batches for crash recovery
//!
//! Write flow:
//!   1. WAL.append(serialized_batch) → returns LSN (durable)
//!   2. Publish immutable segment file atomically
//!   3. Metadata txn: update active segments/ranges + committed_lsn = LSN
//!   4. IndexWriter.addSegmentWithId(segment) → new in-memory snapshot
//!   4. WAL.truncate(LSN)

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const supports_main_lmdb = builtin.os.tag != .freestanding;
const lmdb = if (supports_main_lmdb) @import("lmdb.zig") else struct {
    pub const CommitBackend = enum {
        sync,
        worker_thread,
        async_io,
        adaptive,
    };

    pub const CommitStats = struct {};
};
const lmdb_backend = if (supports_main_lmdb) @import("lmdb_backend.zig") else struct {
    pub const Backend = struct {
        pub fn open(_: Allocator, _: [*:0]const u8, _: anytype) !@This() {
            return error.UnsupportedPlatform;
        }

        pub fn close(_: *@This()) void {}

        pub fn sync(_: *@This(), _: bool) !void {
            return error.UnsupportedPlatform;
        }

        pub fn commitStatsSnapshot(_: *@This()) ?lmdb.CommitStats {
            return null;
        }

        pub fn runtimeNamespaceStore(_: *@This(), _: Allocator) !backend_erased.NamespaceStore {
            return error.UnsupportedPlatform;
        }
    };
};
const mem_backend = @import("mem_backend.zig");
const lsm_backend = @import("lsm_backend/mod.zig");
const storage_io = lsm_backend.storage_io;
const platform_time = @import("../platform/time.zig");
const wal_mod = if (builtin.os.tag == .freestanding) @import("portable_wal.zig") else @import("wal.zig");
const storage_sim = @import("sim_runtime.zig");
const sim_fixture = @import("sim_fixture.zig");
const persistent_sim_fixture = @import("persistent_sim_fixture.zig");
const zig_lmdb = if (builtin.is_test) @import("lmdb_engine") else struct {
    pub const storage_sim_soak = false;
    pub const is_zig_backend = false;
    pub const sim = struct {};
};
const index_mod = @import("../index.zig");
const segment_mod = @import("../segment.zig");
const inverted_mod = @import("../section/inverted.zig");
const introducer_mod = @import("../introducer.zig");
const roaring = @import("../encoding/roaring.zig");
const storage_sim_soak = zig_lmdb.storage_sim_soak;

var bench_persistent_publish_cache: std.atomic.Value(u8) = .init(0);

fn getenv(name: [*:0]const u8) ?[*:0]u8 {
    if (!builtin.link_libc) return null;
    return std.c.getenv(name);
}

fn envEnabled(name: [*:0]const u8) bool {
    const raw_z = getenv(name) orelse return false;
    const raw = std.mem.span(raw_z);
    return !(std.mem.eql(u8, raw, "0") or
        std.ascii.eqlIgnoreCase(raw, "false") or
        std.ascii.eqlIgnoreCase(raw, "no"));
}

fn benchPersistentPublishEnabled() bool {
    switch (bench_persistent_publish_cache.load(.monotonic)) {
        1 => return false,
        2 => return true,
        else => {},
    }
    const enabled = envEnabled("ANTFLY_BENCH_PERSISTENT_PUBLISH") or
        envEnabled("ANTFLY_BENCH_TEXT_METRICS") or
        envEnabled("ANTFLY_BENCH_TEXT_PROFILE");
    bench_persistent_publish_cache.store(if (enabled) 2 else 1, .monotonic);
    return enabled;
}

fn nsToMs(ns: u64) u64 {
    return ns / std.time.ns_per_ms;
}

pub const PersistentIndexOptions = struct {
    path: [*:0]const u8,
    main_backend: MainBackend = .lsm,
    wal_backend: ?wal_mod.StorageBackend = null,
    main_lsm_storage: ?lsm_backend.Storage = null,
    wal_storage: ?lsm_backend.Storage = null,
    main_lsm_options: lsm_backend.Options = .{ .flush_threshold = 1 },
    wal_lsm_options: lsm_backend.Options = .{},
    lsm_cache: ?*lsm_backend.Cache = null,
    lsm_root_generation: u64 = 0,
    read_only: bool = false,
    main_map_size: usize = 256 * 1024 * 1024, // 256MB default
    wal_map_size: usize = 64 * 1024 * 1024, // 64MB default
    main_no_sync: bool = false,
    main_no_meta_sync: bool = false,
    wal_no_sync: bool = false,
    main_commit_backend: lmdb.CommitBackend = .sync,
    wal_commit_backend: lmdb.CommitBackend = .adaptive,
    wal_group_commit_window_ns: u64 = 0,
    wal_group_commit_max_requests: usize = 64,
    wal_clock: storage_sim.Clock = storage_sim.real_clock,
    wal_commit_scheduler: storage_sim.CompletionScheduler = storage_sim.real_completion_scheduler,
    model_wal_commit_backend_completions: bool = false,

    pub fn resolvedWalBackend(self: PersistentIndexOptions) wal_mod.StorageBackend {
        return self.wal_backend orelse switch (self.main_backend) {
            .lsm_memory, .mem => .lsm_memory,
            .lmdb, .lsm => .lsm,
        };
    }
};

fn resolvedMainLsmOptions(opts: PersistentIndexOptions, memory_only: bool) lsm_backend.Options {
    var lsm_options = opts.main_lsm_options;
    lsm_options.backend.read_only = opts.read_only;
    if (opts.read_only) lsm_options.backend.create_if_missing = false;
    lsm_options.backend.durability = if (memory_only or opts.main_no_sync) .none else lsm_options.backend.durability;
    if (!memory_only) lsm_options.storage = opts.main_lsm_storage orelse lsm_options.storage;
    lsm_options.cache = opts.lsm_cache orelse lsm_options.cache;
    if (opts.lsm_root_generation != 0 and lsm_options.root_generation == 0) {
        lsm_options.root_generation = opts.lsm_root_generation;
    }
    return lsm_options;
}

test "persistent index defaults to lsm main backend" {
    const opts = PersistentIndexOptions{ .path = "" };
    try std.testing.expectEqual(MainBackend.lsm, opts.main_backend);
}

test "persistent index defaults to lsm wal backend" {
    const opts = PersistentIndexOptions{ .path = "" };
    try std.testing.expectEqual(wal_mod.StorageBackend.lsm, opts.resolvedWalBackend());
}

test "persistent index uses in-memory lsm wal backend for in-memory main stores" {
    try std.testing.expectEqual(wal_mod.StorageBackend.lsm_memory, (PersistentIndexOptions{
        .path = "",
        .main_backend = .mem,
    }).resolvedWalBackend());
    try std.testing.expectEqual(wal_mod.StorageBackend.lsm_memory, (PersistentIndexOptions{
        .path = "",
        .main_backend = .lsm_memory,
    }).resolvedWalBackend());
}

test "persistent index routes main lsm profile options" {
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(std.testing.allocator, .{
        .path = path,
        .main_backend = .lsm_memory,
        .main_lsm_options = .{ .flush_threshold = 77 },
    });
    defer idx.close();

    switch (idx.main_store_owner) {
        .lsm => |handle| try std.testing.expectEqual(@as(usize, 77), handle.backend.options.flush_threshold),
        else => return error.TestUnexpectedResult,
    }
}

test "persistent index routes wal lsm profile options" {
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(std.testing.allocator, .{
        .path = path,
        .main_backend = .lsm_memory,
        .wal_lsm_options = .{ .flush_threshold = 88 },
    });
    defer idx.close();

    switch (idx.wal.store_owner) {
        .lsm => |handle| try std.testing.expectEqual(@as(usize, 88), handle.backend.options.flush_threshold),
        else => return error.TestUnexpectedResult,
    }
}

pub const MainBackend = enum {
    lmdb,
    mem,
    lsm_memory,
    lsm,
};

pub const PersistentIndexStats = struct {
    wal: wal_mod.WalStats,
    main_commit: ?lmdb.CommitStats,
};

const SegmentFileStore = struct {
    allocator: Allocator,
    root_dir: []u8,
    storage_owner: ?*storage_io.NativeStorage = null,
    storage: storage_io.Storage,

    fn open(
        allocator: Allocator,
        root_dir: []const u8,
        storage: ?storage_io.Storage,
        create_if_missing: bool,
        io_runtime: storage_io.RuntimeKind,
    ) !SegmentFileStore {
        const owned_root = try allocator.dupe(u8, root_dir);
        errdefer allocator.free(owned_root);

        if (storage) |provided| {
            if (create_if_missing) try provided.createDirPath(owned_root);
            return .{
                .allocator = allocator,
                .root_dir = owned_root,
                .storage = provided,
            };
        }

        const owner = try allocator.create(storage_io.NativeStorage);
        errdefer allocator.destroy(owner);
        owner.* = try storage_io.NativeStorage.init(allocator, io_runtime);
        errdefer owner.deinit();
        if (create_if_missing) try owner.storage().createDirPath(owned_root);

        return .{
            .allocator = allocator,
            .root_dir = owned_root,
            .storage_owner = owner,
            .storage = owner.storage(),
        };
    }

    fn close(self: *SegmentFileStore) void {
        if (self.storage_owner) |owner| {
            owner.deinit();
            self.allocator.destroy(owner);
        }
        self.allocator.free(self.root_dir);
        self.* = undefined;
    }

    fn pathAlloc(self: *const SegmentFileStore, seg_id: u64) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "{s}/{d}.seg", .{ self.root_dir, seg_id });
    }

    fn publish(self: *SegmentFileStore, seg_id: u64, bytes: []const u8) !index_mod.SegmentData {
        const path = try self.pathAlloc(seg_id);
        defer self.allocator.free(path);

        var writer = try self.storage.beginAtomicWrite(self.allocator, path);
        var active = true;
        defer if (active) writer.abort();
        try writer.appendSlice(bytes);
        active = false;
        try writer.finish();

        if (self.storage_owner == null) {
            return .fromOwnedHeap(try self.allocator.dupe(u8, bytes));
        }
        return .fromMapped(try mapSegmentFile(path));
    }

    fn delete(self: *SegmentFileStore, seg_id: u64) void {
        const path = self.pathAlloc(seg_id) catch return;
        defer self.allocator.free(path);
        self.storage.deleteFileAbsolute(path) catch {};
    }
};

const AtomicSegmentSink = struct {
    writer: *storage_io.AtomicWriteSink,

    fn sink(self: *AtomicSegmentSink) segment_mod.SegmentSink {
        return .{
            .ptr = self,
            .vtable = &atomic_segment_sink_vtable,
        };
    }

    fn len(ptr: *anyopaque) usize {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        return self.writer.len();
    }

    fn appendSlice(ptr: *anyopaque, bytes: []const u8) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        try self.writer.appendSlice(bytes);
    }

    fn appendByte(ptr: *anyopaque, byte: u8) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        try self.writer.appendByte(byte);
    }

    fn appendNTimes(ptr: *anyopaque, byte: u8, count: usize) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        var buf: [4096]u8 = undefined;
        @memset(&buf, byte);
        var remaining = count;
        while (remaining > 0) {
            const n = @min(remaining, buf.len);
            try self.writer.appendSlice(buf[0..n]);
            remaining -= n;
        }
    }

    fn writeAt(ptr: *anyopaque, offset: usize, bytes: []const u8) !void {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        try self.writer.writeAt(offset, bytes);
    }

    fn crc32Prefix(ptr: *anyopaque, len_prefix: usize) !u32 {
        const self: *AtomicSegmentSink = @ptrCast(@alignCast(ptr));
        return try self.writer.crc32Prefix(len_prefix);
    }
};

const atomic_segment_sink_vtable = segment_mod.SegmentSink.VTable{
    .len = AtomicSegmentSink.len,
    .append_slice = AtomicSegmentSink.appendSlice,
    .append_byte = AtomicSegmentSink.appendByte,
    .append_ntimes = AtomicSegmentSink.appendNTimes,
    .write_at = AtomicSegmentSink.writeAt,
    .crc32_prefix = AtomicSegmentSink.crc32Prefix,
};

fn walCommitBackendForOptions(backend: lmdb.CommitBackend) wal_mod.CommitBackend {
    return switch (backend) {
        .sync => .sync,
        .worker_thread => .worker_thread,
        .async_io => .async_io,
        .adaptive => .adaptive,
    };
}

pub const SegmentKeyRange = struct {
    seg_id: u64,
    min_doc_key: []u8,
    max_doc_key: []u8,

    pub fn deinit(self: *SegmentKeyRange, alloc: Allocator) void {
        alloc.free(self.min_doc_key);
        alloc.free(self.max_doc_key);
        self.* = undefined;
    }
};

pub const SegmentSplitClass = enum {
    left_only,
    right_only,
    mixed,
};

pub const SegmentSplitPlanEntry = struct {
    seg_id: u64,
    min_doc_key: []u8,
    max_doc_key: []u8,
    class: SegmentSplitClass,

    pub fn deinit(self: *SegmentSplitPlanEntry, alloc: Allocator) void {
        alloc.free(self.min_doc_key);
        alloc.free(self.max_doc_key);
        self.* = undefined;
    }
};

pub const SegmentHandoffResult = struct {
    transferred_segments: usize,
    doc_keys: [][]u8,

    pub fn deinit(self: *SegmentHandoffResult, alloc: Allocator) void {
        for (self.doc_keys) |key| alloc.free(key);
        alloc.free(self.doc_keys);
        self.* = undefined;
    }
};

pub const StoredSegment = struct {
    seg_id: u64,
    segment_bytes: []u8,
    deletion_bitmap_bytes: ?[]u8,

    pub fn deinit(self: *StoredSegment, alloc: Allocator) void {
        alloc.free(self.segment_bytes);
        if (self.deletion_bitmap_bytes) |bytes| alloc.free(bytes);
        self.* = undefined;
    }
};

/// Meta keys in the LMDB metadata database.
const meta_committed_lsn = "committed_lsn";
const meta_next_seg_id = "next_seg_id";
const meta_active_segments = "active_segments";
const meta_active_segment_prefix = "active_segment:";
const meta_segment_range_prefix = "segment_range:";
const segments_db_name = "segments";
const meta_db_name = "meta";
const deletions_db_name = "deletions";
var global_storage_mu: std.atomic.Mutex = .unlocked;

fn lockPersistentStorage() void {
    while (!global_storage_mu.tryLock()) std.atomic.spinLoopHint();
}

fn unlockPersistentStorage() void {
    global_storage_mu.unlock();
}

const MainKeyspace = enum {
    segments,
    meta,
    deletions,
};

fn mapRuntimeNamespace(namespace: backend_types.Namespace) !MainKeyspace {
    if (namespace.name == null) return .meta;
    if (std.mem.eql(u8, namespace.name.?, "segments")) return .segments;
    if (std.mem.eql(u8, namespace.name.?, "meta")) return .meta;
    if (std.mem.eql(u8, namespace.name.?, "deletions")) return .deletions;
    return error.InvalidNamespace;
}

fn runtimeNamespace(keyspace: MainKeyspace) backend_types.Namespace {
    return switch (keyspace) {
        .segments => .{ .name = "segments" },
        .meta => .{ .name = "meta" },
        .deletions => .{ .name = "deletions" },
    };
}

const MainStoreOwner = union(enum) {
    lmdb: *lmdb_backend.Backend,
    mem: *mem_backend.Backend,
    lsm: lsm_backend.BackendHandle,

    fn close(self: *MainStoreOwner, alloc: Allocator) void {
        switch (self.*) {
            .lmdb => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .mem => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*handle| handle.close(),
        }
        self.* = undefined;
    }

    fn sync(self: *MainStoreOwner, force: bool) !void {
        switch (self.*) {
            .lmdb => |backend| try backend.sync(force),
            .mem => {},
            .lsm => |*handle| try handle.backend.sync(force),
        }
    }

    fn lsmMaintenanceScore(self: *const MainStoreOwner) u64 {
        return switch (self.*) {
            .lmdb, .mem => 0,
            .lsm => |handle| handle.backend.maintenanceScore(),
        };
    }

    fn snapshotLsmMaintenanceStats(self: *const MainStoreOwner) ?lsm_backend.Backend.MaintenanceStats {
        return switch (self.*) {
            .lmdb, .mem => null,
            .lsm => |handle| handle.backend.snapshotMaintenanceStats(),
        };
    }

    fn snapshotLsmNativeStorageStats(self: *const MainStoreOwner) ?lsm_backend.NativeStorageStats {
        return switch (self.*) {
            .lmdb, .mem => null,
            .lsm => |handle| handle.backend.snapshotNativeStorageStats(),
        };
    }

    fn runLsmMaintenanceStep(self: *MainStoreOwner) !bool {
        return switch (self.*) {
            .lmdb, .mem => false,
            .lsm => |*handle| try handle.backend.runMaintenanceStep(),
        };
    }

    fn runLsmMaintenanceStepBestEffort(self: *MainStoreOwner) !bool {
        return switch (self.*) {
            .lmdb, .mem => false,
            .lsm => |*handle| try handle.backend.runMaintenanceStepBestEffort(),
        };
    }

    fn commitStatsSnapshot(self: *MainStoreOwner) ?lmdb.CommitStats {
        return switch (self.*) {
            .lmdb => |backend| backend.commitStatsSnapshot(),
            .mem, .lsm => null,
        };
    }
};

const OpenedMainStore = struct {
    store: backend_erased.NamespaceStore,
    owner: MainStoreOwner,
};

pub const PersistentIndex = struct {
    alloc: Allocator,
    writer: index_mod.IndexWriter,
    main_store: backend_erased.NamespaceStore,
    main_store_owner: MainStoreOwner,
    segment_files: ?SegmentFileStore,
    wal: wal_mod.WAL,
    committed_lsn: u64,
    read_only: bool = false,

    pub const BackendStore = backend_adapter.Store(PersistentIndex, MainTxn, MainTxn, MainTxn, .{
        .capabilities = backendCapabilities,
        .begin_read = beginReadMainTxn,
        .begin_write = beginWriteMainTxn,
        .begin_batch = beginWriteMainTxn,
    });

    pub const SegmentSinkBuildFn = *const fn (*anyopaque, *segment_mod.SegmentSink) anyerror!void;

    const MainTxn = struct {
        read: ?backend_erased.NamespaceReadTxn = null,
        write: ?backend_erased.NamespaceWriteTxn = null,

        const ReadAdapter = backend_adapter.NamespaceReadTxn(MainTxn, MainKeyspace, .{
            .abort = abort,
            .get = get,
        });
        const WriteAdapter = backend_adapter.NamespaceWriteTxn(MainTxn, MainKeyspace, .{
            .abort = abort,
            .commit = commit,
            .get = get,
            .put = put,
            .delete = delete,
        });

        fn open(store: *backend_erased.NamespaceStore, read_only: bool) !MainTxn {
            if (read_only) {
                return .{ .read = try store.beginRead() };
            }
            return .{ .write = try store.beginWrite() };
        }

        pub fn abort(self: *MainTxn) void {
            if (self.read) |*txn| {
                txn.abort();
                self.read = null;
            }
            if (self.write) |*txn| {
                txn.abort();
                self.write = null;
            }
        }

        pub fn commit(self: *MainTxn) !void {
            if (self.write) |*txn| {
                try txn.commit();
                self.write = null;
                return;
            }
            return error.ReadOnly;
        }

        pub fn get(self: *MainTxn, keyspace: MainKeyspace, key: []const u8) ![]const u8 {
            const namespace = runtimeNamespace(keyspace);
            if (self.read) |*txn| return try txn.get(namespace, key);
            if (self.write) |*txn| return try txn.get(namespace, key);
            return error.TransactionClosed;
        }

        pub fn put(self: *MainTxn, keyspace: MainKeyspace, key: []const u8, value: []const u8) !void {
            if (self.write) |*txn| {
                try txn.put(runtimeNamespace(keyspace), key, value);
                return;
            }
            return error.ReadOnly;
        }

        pub fn delete(self: *MainTxn, keyspace: MainKeyspace, key: []const u8) !void {
            if (self.write) |*txn| {
                try txn.delete(runtimeNamespace(keyspace), key);
                return;
            }
            return error.ReadOnly;
        }

        pub fn openCursor(self: *MainTxn, keyspace: MainKeyspace) !backend_erased.Cursor {
            const namespace = runtimeNamespace(keyspace);
            if (self.read) |*txn| return try txn.openCursor(namespace);
            if (self.write) |*txn| return try txn.openCursor(namespace);
            return error.TransactionClosed;
        }

        fn readAdapter(self: *MainTxn) ReadAdapter {
            return ReadAdapter.init(self);
        }

        fn writeAdapter(self: *MainTxn) WriteAdapter {
            return WriteAdapter.init(self);
        }
    };

    fn beginReadMainTxn(self: *PersistentIndex) !MainTxn {
        return try MainTxn.open(&self.main_store, true);
    }

    fn beginWriteMainTxn(self: *PersistentIndex) !MainTxn {
        return try MainTxn.open(&self.main_store, false);
    }

    fn backendCapabilities(_: *PersistentIndex) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = true,
            .cursors = true,
            .native_namespaces = true,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn backendStore(self: *PersistentIndex) BackendStore {
        return BackendStore.init(self);
    }

    fn openMainStore(alloc: Allocator, index_path_z: [*:0]const u8, index_path: []const u8, opts: PersistentIndexOptions) !OpenedMainStore {
        switch (opts.main_backend) {
            .lmdb => {
                if (!supports_main_lmdb) return error.UnsupportedPlatform;
                const backend = try alloc.create(lmdb_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = try lmdb_backend.Backend.open(alloc, index_path_z, .{
                    .backend = .{
                        .durability = if (opts.main_no_sync) .none else .full,
                    },
                    .env = .{
                        .max_dbs = 3,
                        .map_size = opts.main_map_size,
                        .no_sync = opts.main_no_sync,
                        .no_meta_sync = opts.main_no_meta_sync,
                        .commit_backend = opts.main_commit_backend,
                    },
                });
                errdefer backend.close();

                var store = try backend.runtimeNamespaceStore(alloc);
                errdefer store.deinit();
                return .{
                    .store = store,
                    .owner = .{ .lmdb = backend },
                };
            },
            .mem => {
                const backend = try alloc.create(mem_backend.Backend);
                errdefer alloc.destroy(backend);
                backend.* = mem_backend.Backend.init(alloc, .{});
                errdefer backend.close();

                var store = try backend.runtimeNamespaceStore(alloc);
                errdefer store.deinit();
                return .{
                    .store = store,
                    .owner = .{ .mem = backend },
                };
            },
            .lsm_memory => {
                var handle = try lsm_backend.BackendHandle.init(alloc, resolvedMainLsmOptions(opts, true));
                errdefer handle.close();

                var store = try handle.backend.runtimeNamespaceStore(alloc);
                errdefer store.deinit();
                return .{
                    .store = store,
                    .owner = .{ .lsm = handle },
                };
            },
            .lsm => {
                var handle = try lsm_backend.BackendHandle.open(alloc, index_path, resolvedMainLsmOptions(opts, false));
                errdefer handle.close();

                var store = try handle.backend.runtimeNamespaceStore(alloc);
                errdefer store.deinit();
                return .{
                    .store = store,
                    .owner = .{ .lsm = handle },
                };
            },
        }
    }

    /// Open or create a persistent index. Recovers existing state + replays WAL.
    pub fn open(alloc: Allocator, opts: PersistentIndexOptions) !PersistentIndex {
        lockPersistentStorage();
        defer unlockPersistentStorage();

        const path_span = std.mem.span(opts.path);
        const wal_storage = opts.wal_storage orelse opts.main_lsm_storage;
        const needs_host_dirs =
            opts.main_backend == .lmdb or
            (opts.main_backend == .lsm and opts.main_lsm_storage == null) or
            opts.resolvedWalBackend() == .lmdb or
            (opts.resolvedWalBackend() == .lsm and wal_storage == null);

        // Create subdirectories
        var index_buf: [512]u8 = undefined;
        const index_path = std.fmt.bufPrintZ(&index_buf, "{s}/index", .{path_span}) catch return error.PathTooLong;
        const index_path_span = index_path[0..index_path.len];
        if (builtin.os.tag != .freestanding and needs_host_dirs) {
            var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
            defer io_impl.deinit();
            try storage_io.createDirPathPortable(io_impl.io(), path_span);
            try storage_io.createDirPathPortable(io_impl.io(), index_path_span);
        }

        var wal_buf: [512]u8 = undefined;
        const wal_path = std.fmt.bufPrintZ(&wal_buf, "{s}/wal", .{path_span}) catch return error.PathTooLong;

        var segment_files: ?SegmentFileStore = null;
        if (supports_main_lmdb) {
            var segments_buf: [512]u8 = undefined;
            const segments_path = std.fmt.bufPrint(&segments_buf, "{s}/segments", .{path_span}) catch return error.PathTooLong;
            segment_files = try SegmentFileStore.open(
                alloc,
                segments_path,
                opts.main_lsm_storage,
                !opts.read_only,
                opts.main_lsm_options.io_runtime,
            );
        }
        errdefer if (segment_files) |*store| store.close();

        // Open WAL
        var wal = try wal_mod.WAL.open(wal_path.ptr, .{
            .map_size = opts.wal_map_size,
            .no_sync = opts.wal_no_sync,
            .commit_backend = walCommitBackendForOptions(opts.wal_commit_backend),
            .group_commit_window_ns = opts.wal_group_commit_window_ns,
            .group_commit_max_requests = opts.wal_group_commit_max_requests,
            .backend = opts.resolvedWalBackend(),
            .read_only = opts.read_only,
            .storage = wal_storage,
            .lsm_options = opts.wal_lsm_options,
            .clock = opts.wal_clock,
            .commit_scheduler = opts.wal_commit_scheduler,
            .model_commit_backend_completions = opts.model_wal_commit_backend_completions,
        });
        errdefer wal.close();

        // Open main backing store
        var opened_main = try openMainStore(alloc, index_path.ptr, index_path_span, opts);
        errdefer {
            opened_main.store.deinit();
            opened_main.owner.close(alloc);
        }

        // Open namespaces eagerly on writable backends
        if (!opts.read_only) {
            var setup_txn = try MainTxn.open(&opened_main.store, false);
            errdefer setup_txn.abort();
            try setup_txn.commit();
        }

        var writer = try index_mod.IndexWriter.init(alloc);
        errdefer writer.deinit();
        var stale_active_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_active_ids.deinit(alloc);

        // Recovery: load existing segments from metadata plus immutable files.
        var committed_lsn: u64 = 0;
        {
            var read_txn = try MainTxn.open(&opened_main.store, true);
            defer read_txn.abort();

            // Read committed_lsn
            const lsn_bytes = read_txn.get(.meta, meta_committed_lsn) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (lsn_bytes) |lb| {
                if (lb.len >= 8) committed_lsn = std.mem.readInt(u64, lb[0..8], .little);
            }

            const active_ids = try loadActiveSegmentIdsFromTxn(&read_txn, alloc);
            defer alloc.free(active_ids);
            for (active_ids) |seg_id| {
                const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
                const seg_bytes = read_txn.get(.segments, &seg_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                var segment_data = if (seg_bytes) |bytes| blk: {
                    break :blk if (segment_files) |*store|
                        try store.publish(seg_id, bytes)
                    else
                        index_mod.SegmentData.fromOwnedHeap(try alloc.dupe(u8, bytes));
                } else if (segment_files) |*store| blk: {
                    const segment_path = try store.pathAlloc(seg_id);
                    defer store.allocator.free(segment_path);
                    if (store.storage_owner != null) {
                        break :blk index_mod.SegmentData.fromMapped(mapSegmentFile(segment_path) catch |err| switch (err) {
                            error.FileNotFound => {
                                try stale_active_ids.append(alloc, seg_id);
                                continue;
                            },
                            else => return err,
                        });
                    }
                    const loaded = store.storage.readFileAlloc(alloc, segment_path, std.math.maxInt(usize)) catch |err| switch (err) {
                        error.FileNotFound => {
                            try stale_active_ids.append(alloc, seg_id);
                            continue;
                        },
                        else => return err,
                    };
                    break :blk index_mod.SegmentData.fromOwnedHeap(loaded);
                } else {
                    try stale_active_ids.append(alloc, seg_id);
                    continue;
                };
                errdefer segment_data.deinit(alloc);
                writer.addSegmentWithIdData(seg_id, segment_data) catch |err| {
                    if (isStaleActiveSegmentDataError(err)) {
                        segment_data.deinit(alloc);
                        try stale_active_ids.append(alloc, seg_id);
                        continue;
                    }
                    return err;
                };

                const deletion_bytes = read_txn.get(.deletions, &seg_key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (deletion_bytes) |bytes| {
                    var bitmap = try roaring.RoaringBitmap.fromBytes(alloc, bytes);
                    if (!bitmap.isEmpty()) {
                        writer.setDeletionBitmap(seg_id, bitmap);
                    } else {
                        bitmap.deinit();
                    }
                }
            }
        }

        // Replay WAL entries after committed_lsn
        const wal_entries = try wal.iterateFrom(alloc, committed_lsn + 1);
        defer {
            for (wal_entries) |e| alloc.free(@constCast(e.data));
            alloc.free(wal_entries);
        }

        var pi = PersistentIndex{
            .alloc = alloc,
            .writer = writer,
            .main_store = opened_main.store,
            .main_store_owner = opened_main.owner,
            .segment_files = segment_files,
            .wal = wal,
            .committed_lsn = committed_lsn,
            .read_only = opts.read_only,
        };

        // Replay each WAL entry
        if (!opts.read_only) {
            for (wal_entries) |entry| {
                try pi.replayWalEntry(entry.lsn, entry.data);
            }
        }

        if (!opts.read_only and stale_active_ids.items.len > 0) {
            std.log.warn("persistent index pruning {d} stale active segment references path={s}", .{
                stale_active_ids.items.len,
                path_span,
            });
            try pi.pruneMissingActiveSegmentsLocked(stale_active_ids.items);
        }
        if (!opts.read_only) {
            try pi.pruneActiveSegmentsMissingRangesOrDataLocked(path_span);
        }

        return pi;
    }

    fn lockStorage(self: *PersistentIndex) void {
        _ = self;
        lockPersistentStorage();
    }

    fn unlockStorage(self: *PersistentIndex) void {
        _ = self;
        unlockPersistentStorage();
    }

    pub fn close(self: *PersistentIndex) void {
        self.lockStorage();
        self.writer.deinit();
        self.wal.close();
        self.main_store.deinit();
        self.main_store_owner.close(self.alloc);
        if (self.segment_files) |*store| store.close();
        self.unlockStorage();
        self.* = undefined;
    }

    pub fn sync(self: *PersistentIndex, force: bool) !void {
        self.lockStorage();
        defer self.unlockStorage();
        try self.main_store_owner.sync(force);
        try self.wal.sync(force);
    }

    pub fn syncMain(self: *PersistentIndex, force: bool) !void {
        self.lockStorage();
        defer self.unlockStorage();
        try self.main_store_owner.sync(force);
    }

    pub fn lsmMaintenanceScore(self: *const PersistentIndex) u64 {
        return self.main_store_owner.lsmMaintenanceScore();
    }

    pub fn lsmMaintenanceDebtHint(self: *const PersistentIndex) u64 {
        return switch (self.main_store_owner) {
            .lmdb, .mem => 0,
            .lsm => |handle| handle.backend.maintenanceDebtHint(),
        };
    }

    pub fn refreshLsmMaintenanceDebtHint(self: *PersistentIndex) void {
        switch (self.main_store_owner) {
            .lmdb, .mem => {},
            .lsm => |handle| handle.backend.refreshMaintenanceDebtHint(),
        }
    }

    pub fn snapshotLsmMaintenanceStats(self: *const PersistentIndex) ?lsm_backend.Backend.MaintenanceStats {
        return self.main_store_owner.snapshotLsmMaintenanceStats();
    }

    pub fn snapshotLsmNativeStorageStats(self: *const PersistentIndex) ?lsm_backend.NativeStorageStats {
        return self.main_store_owner.snapshotLsmNativeStorageStats();
    }

    pub fn runLsmMaintenanceStep(self: *PersistentIndex) !bool {
        self.lockStorage();
        defer self.unlockStorage();
        return try self.main_store_owner.runLsmMaintenanceStep();
    }

    pub fn runLsmMaintenanceStepBestEffort(self: *PersistentIndex) !bool {
        self.lockStorage();
        defer self.unlockStorage();
        return try self.main_store_owner.runLsmMaintenanceStepBestEffort();
    }

    /// Index a pre-built segment (WAL-protected, durable after return).
    pub fn indexSegment(self: *PersistentIndex, segment_bytes: []const u8) !void {
        const owned = try self.alloc.dupe(u8, segment_bytes);
        try self.indexSegmentOwned(owned);
    }

    /// Like indexSegment(), but takes ownership of segment_bytes.
    pub fn indexSegmentOwned(self: *PersistentIndex, segment_bytes: []u8) !void {
        self.lockStorage();
        defer self.unlockStorage();

        var owned: ?[]u8 = segment_bytes;
        defer if (owned) |buf| self.alloc.free(buf);

        const profile_enabled = benchPersistentPublishEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var wal_append_ns: u64 = 0;
        var reserve_id_ns: u64 = 0;
        var materialize_ns: u64 = 0;
        var persist_ns: u64 = 0;
        var writer_publish_ns: u64 = 0;
        const begin_write_ns: u64 = 0;
        const build_sink_ns: u64 = 0;
        const finish_write_ns: u64 = 0;
        const map_segment_ns: u64 = 0;
        const key_range_ns: u64 = 0;
        var wal_truncate_ns: u64 = 0;
        const uses_segment_wal = self.segment_files == null;

        // 1. Write inline segment bytes to WAL. File-backed segments are
        // atomically published before the active metadata commit, so replay can
        // recover from metadata alone and orphan pre-commit files are harmless.
        const lsn = if (uses_segment_wal) blk: {
            const wal_append_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            const appended_lsn = try self.wal.append(owned.?);
            if (profile_enabled) wal_append_ns = platform_time.monotonicNs() - wal_append_start_ns;
            break :blk appended_lsn;
        } else self.committed_lsn;

        // 2. Allocate segment ID under writer lock to prevent races
        const reserve_id_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        self.writer.lockMutex();
        const seg_id = self.writer.next_segment_id;
        self.writer.next_segment_id += 1;
        self.writer.mu.unlock();
        if (profile_enabled) reserve_id_ns = platform_time.monotonicNs() - reserve_id_start_ns;

        // 3. Publish immutable file-backed segment bytes before metadata becomes visible.
        var segment_data: ?index_mod.SegmentData = null;
        if (self.segment_files != null) {
            const materialize_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            segment_data = try self.materializeSegmentData(seg_id, owned.?);
            if (profile_enabled) materialize_ns = platform_time.monotonicNs() - materialize_start_ns;
        }
        errdefer {
            if (segment_data) |*data| data.deinit(self.alloc);
            if (self.segment_files != null) self.deleteSegmentFile(seg_id);
        }

        // 4. Persist metadata.
        const persist_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        try self.persistSegment(seg_id, owned.?, lsn);
        if (profile_enabled) persist_ns = platform_time.monotonicNs() - persist_start_ns;

        if (self.segment_files == null) {
            segment_data = index_mod.SegmentData.fromOwnedHeap(owned.?);
            owned = null;
        }

        // 5. Add to in-memory writer (addSegmentWithIdData will acquire the lock itself)
        const writer_publish_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        try self.writer.addSegmentWithIdData(seg_id, segment_data.?);
        segment_data = null;
        if (profile_enabled) writer_publish_ns = platform_time.monotonicNs() - writer_publish_start_ns;

        // 6. Truncate WAL
        if (uses_segment_wal) {
            const wal_truncate_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
            try self.wal.truncate(lsn);
            if (profile_enabled) wal_truncate_ns = platform_time.monotonicNs() - wal_truncate_start_ns;
        }

        if (profile_enabled) {
            std.log.info(
                "antfly_bench_text_publish seg_id={d} bytes={d} total_ms={d} wal_append_ms={d} reserve_id_ms={d} materialize_ms={d} begin_write_ms={d} build_sink_ms={d} finish_write_ms={d} map_segment_ms={d} key_range_ms={d} persist_ms={d} writer_publish_ms={d} wal_truncate_ms={d} file_backed={} uses_segment_wal={}",
                .{
                    seg_id,
                    segment_bytes.len,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    nsToMs(wal_append_ns),
                    nsToMs(reserve_id_ns),
                    nsToMs(materialize_ns),
                    nsToMs(begin_write_ns),
                    nsToMs(build_sink_ns),
                    nsToMs(finish_write_ns),
                    nsToMs(map_segment_ns),
                    nsToMs(key_range_ns),
                    nsToMs(persist_ns),
                    nsToMs(writer_publish_ns),
                    nsToMs(wal_truncate_ns),
                    self.segment_files != null,
                    uses_segment_wal,
                },
            );
        }
    }

    /// Build and publish one segment directly into the persistent segment
    /// artifact when native segment files are available.
    ///
    /// Memory-only and synthetic storage backends fall back to the heap-backed
    /// `indexSegmentOwned` path because they cannot mmap the final segment file.
    pub fn indexSegmentFromSinkBuilder(
        self: *PersistentIndex,
        ctx: *anyopaque,
        build_fn: SegmentSinkBuildFn,
    ) !usize {
        if (self.segment_files == null or self.segment_files.?.storage_owner == null) {
            var sink_impl = segment_mod.MemorySegmentSink.init(self.alloc);
            errdefer sink_impl.deinit();
            var sink = sink_impl.sink();
            try build_fn(ctx, &sink);
            const segment_len = sink.len();
            const owned = try sink_impl.finishOwned();
            try self.indexSegmentOwned(owned);
            return segment_len;
        }

        const profile_enabled = benchPersistentPublishEnabled();
        const total_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var reserve_id_ns: u64 = 0;
        var materialize_ns: u64 = 0;
        var begin_write_ns: u64 = 0;
        var build_sink_ns: u64 = 0;
        var finish_write_ns: u64 = 0;
        var map_segment_ns: u64 = 0;
        var key_range_ns: u64 = 0;
        var persist_ns: u64 = 0;
        var writer_publish_ns: u64 = 0;

        const reserve_id_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const seg_id = self.reserveSegmentId();
        if (profile_enabled) reserve_id_ns = platform_time.monotonicNs() - reserve_id_start_ns;

        const store = &self.segment_files.?;
        const path = try store.pathAlloc(seg_id);
        defer store.allocator.free(path);

        const materialize_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        const begin_write_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var writer = try store.storage.beginAtomicWrite(self.alloc, path);
        if (profile_enabled) begin_write_ns = platform_time.monotonicNs() - begin_write_start_ns;
        var writer_active = true;
        errdefer if (writer_active) writer.abort();

        var sink_adapter = AtomicSegmentSink{ .writer = &writer };
        var sink = sink_adapter.sink();
        const build_sink_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        try build_fn(ctx, &sink);
        const segment_len = sink.len();
        if (profile_enabled) build_sink_ns = platform_time.monotonicNs() - build_sink_start_ns;

        writer_active = false;
        const finish_write_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        try writer.finish();
        if (profile_enabled) finish_write_ns = platform_time.monotonicNs() - finish_write_start_ns;
        if (profile_enabled) materialize_ns = platform_time.monotonicNs() - materialize_start_ns;

        const map_segment_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var segment_data: ?index_mod.SegmentData = .fromMapped(try mapSegmentFile(path));
        if (profile_enabled) map_segment_ns = platform_time.monotonicNs() - map_segment_start_ns;
        errdefer {
            if (segment_data) |*data| data.deinit(self.alloc);
            self.deleteSegmentFile(seg_id);
        }

        const key_range_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        var key_range = try extractSegmentKeyRange(self.alloc, segment_data.?.bytes());
        if (profile_enabled) key_range_ns = platform_time.monotonicNs() - key_range_start_ns;
        defer key_range.deinit(self.alloc);

        const persist_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        self.lockStorage();
        defer self.unlockStorage();
        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();
        try self.saveSegmentRange(&txn, seg_id, key_range);
        try self.updateActiveSegments(&txn, seg_id, .add);
        const lsn_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, self.committed_lsn));
        try txn.put(.meta, meta_committed_lsn, &lsn_bytes);
        try txn.commit();
        if (profile_enabled) persist_ns = platform_time.monotonicNs() - persist_start_ns;

        const writer_publish_start_ns = if (profile_enabled) platform_time.monotonicNs() else 0;
        try self.writer.addSegmentWithIdData(seg_id, segment_data.?);
        segment_data = null;
        if (profile_enabled) writer_publish_ns = platform_time.monotonicNs() - writer_publish_start_ns;

        if (profile_enabled) {
            std.log.info(
                "antfly_bench_text_publish seg_id={d} bytes={d} total_ms={d} wal_append_ms={d} reserve_id_ms={d} materialize_ms={d} begin_write_ms={d} build_sink_ms={d} finish_write_ms={d} map_segment_ms={d} key_range_ms={d} persist_ms={d} writer_publish_ms={d} wal_truncate_ms={d} file_backed={} uses_segment_wal={}",
                .{
                    seg_id,
                    segment_len,
                    nsToMs(platform_time.monotonicNs() - total_start_ns),
                    0,
                    nsToMs(reserve_id_ns),
                    nsToMs(materialize_ns),
                    nsToMs(begin_write_ns),
                    nsToMs(build_sink_ns),
                    nsToMs(finish_write_ns),
                    nsToMs(map_segment_ns),
                    nsToMs(key_range_ns),
                    nsToMs(persist_ns),
                    nsToMs(writer_publish_ns),
                    0,
                    true,
                    false,
                },
            );
        }
        return segment_len;
    }

    /// Get current snapshot (lock-free, delegates to IndexWriter).
    pub fn snapshot(self: *PersistentIndex) *index_mod.IndexSnapshot {
        return self.writer.snapshot();
    }

    pub fn acquireSnapshot(self: *PersistentIndex) *index_mod.IndexSnapshot {
        return self.writer.acquireSnapshot();
    }

    pub fn walStatsSnapshot(self: *PersistentIndex) wal_mod.WalStats {
        return self.wal.statsSnapshot();
    }

    pub fn commitStatsSnapshot(self: *PersistentIndex) ?lmdb.CommitStats {
        return self.main_store_owner.commitStatsSnapshot();
    }

    pub fn statsSnapshot(self: *PersistentIndex) PersistentIndexStats {
        return .{
            .wal = self.wal.statsSnapshot(),
            .main_commit = self.main_store_owner.commitStatsSnapshot(),
        };
    }

    pub fn activeSegmentRanges(self: *PersistentIndex, alloc: Allocator) ![]SegmentKeyRange {
        return self.activeSegmentRangesRepairing(alloc, true, false);
    }

    fn activeSegmentRangesRepairing(
        self: *PersistentIndex,
        alloc: Allocator,
        repair_stale: bool,
        storage_locked: bool,
    ) ![]SegmentKeyRange {
        var did_repair = false;
        while (true) {
            const ranges = self.loadActiveSegmentRangesSkippingMissing(alloc, repair_stale, storage_locked) catch |err| switch (err) {
                error.StaleActiveSegments => {
                    if (did_repair) return err;
                    did_repair = true;
                    continue;
                },
                else => return err,
            };
            return ranges;
        }
    }

    fn loadActiveSegmentRangesSkippingMissing(
        self: *PersistentIndex,
        alloc: Allocator,
        repair_stale: bool,
        storage_locked: bool,
    ) ![]SegmentKeyRange {
        var txn = try self.beginReadMainTxn();
        var txn_open = true;
        defer if (txn_open) txn.abort();

        const active_ids = try self.loadActiveSegmentIds(&txn, alloc);
        defer alloc.free(active_ids);

        var ranges = std.ArrayListUnmanaged(SegmentKeyRange).empty;
        errdefer {
            for (ranges.items) |*range| range.deinit(alloc);
            ranges.deinit(alloc);
        }
        var stale_ids = std.ArrayListUnmanaged(u64).empty;
        defer stale_ids.deinit(alloc);

        for (active_ids) |seg_id| {
            const range = self.loadSegmentRange(&txn, alloc, seg_id) catch |err| switch (err) {
                error.NotFound => {
                    if (!repair_stale) return err;
                    try stale_ids.append(alloc, seg_id);
                    continue;
                },
                else => return err,
            };
            try ranges.append(alloc, range);
        }
        if (stale_ids.items.len > 0) {
            txn.abort();
            txn_open = false;
            std.log.warn("persistent index pruning {d} active segment references with missing range/data", .{
                stale_ids.items.len,
            });
            if (storage_locked) {
                try self.pruneMissingActiveSegmentsLocked(stale_ids.items);
            } else {
                try self.pruneMissingActiveSegments(stale_ids.items);
            }
            return error.StaleActiveSegments;
        }
        return try ranges.toOwnedSlice(alloc);
    }

    pub fn classifyActiveSegmentsForSplit(
        self: *PersistentIndex,
        alloc: Allocator,
        split_key: []const u8,
    ) ![]SegmentSplitPlanEntry {
        const ranges = try self.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }

        var plan = try alloc.alloc(SegmentSplitPlanEntry, ranges.len);
        errdefer {
            for (plan) |*entry| {
                if (entry.min_doc_key.len > 0) alloc.free(entry.min_doc_key);
                if (entry.max_doc_key.len > 0) alloc.free(entry.max_doc_key);
            }
            alloc.free(plan);
        }
        for (plan) |*entry| {
            entry.* = .{
                .seg_id = 0,
                .min_doc_key = &.{},
                .max_doc_key = &.{},
                .class = .mixed,
            };
        }

        for (ranges, 0..) |range, i| {
            plan[i] = .{
                .seg_id = range.seg_id,
                .min_doc_key = try alloc.dupe(u8, range.min_doc_key),
                .max_doc_key = try alloc.dupe(u8, range.max_doc_key),
                .class = classifySegmentRange(range.min_doc_key, range.max_doc_key, split_key),
            };
        }
        return plan;
    }

    pub fn handoffRightOnlySegmentsToChild(self: *PersistentIndex, dest: *PersistentIndex, split_key: []const u8) !usize {
        var result = try self.handoffRightOnlySegmentsToChildDetailed(dest, split_key, self.alloc, true);
        defer result.deinit(self.alloc);
        return result.transferred_segments;
    }

    pub fn handoffRightOnlySegmentsToChildDetailed(
        self: *PersistentIndex,
        dest: *PersistentIndex,
        split_key: []const u8,
        alloc: Allocator,
        collect_doc_keys: bool,
    ) !SegmentHandoffResult {
        const plan = try self.classifyActiveSegmentsForSplit(alloc, split_key);
        defer {
            for (plan) |*entry| entry.deinit(self.alloc);
            alloc.free(plan);
        }

        var source_txn = try self.beginReadMainTxn();
        defer source_txn.abort();

        var dest_txn = try dest.beginWriteMainTxn();
        errdefer dest_txn.abort();

        const existing_dest_ids = try dest.loadActiveSegmentIds(&dest_txn, alloc);
        defer alloc.free(existing_dest_ids);
        var existing_dest = std.AutoHashMapUnmanaged(u64, void).empty;
        defer existing_dest.deinit(alloc);
        try existing_dest.ensureTotalCapacity(alloc, @intCast(existing_dest_ids.len));
        for (existing_dest_ids) |seg_id| {
            existing_dest.putAssumeCapacity(seg_id, {});
        }

        var copied: usize = 0;
        var doc_keys = std.ArrayListUnmanaged([]u8).empty;
        errdefer {
            for (doc_keys.items) |key| alloc.free(key);
            doc_keys.deinit(alloc);
        }
        for (plan) |entry| {
            if (entry.class != .right_only) continue;
            if (existing_dest.contains(entry.seg_id)) continue;

            const seg_bytes = try self.readSegmentBytesAlloc(&source_txn, alloc, entry.seg_id);
            defer alloc.free(seg_bytes);
            var segment_data: ?index_mod.SegmentData = try dest.materializeSegmentData(entry.seg_id, seg_bytes);
            defer if (segment_data) |*data| data.deinit(alloc);

            const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, entry.seg_id));
            if (dest.segment_files == null) try dest_txn.put(.segments, &seg_key, seg_bytes);
            const deletion_bytes = source_txn.get(.deletions, &seg_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (deletion_bytes) |bytes| {
                try dest_txn.put(.deletions, &seg_key, bytes);
            }

            try dest.saveSegmentRange(&dest_txn, entry.seg_id, .{
                .seg_id = entry.seg_id,
                .min_doc_key = entry.min_doc_key,
                .max_doc_key = entry.max_doc_key,
            });
            try dest.updateActiveSegments(&dest_txn, entry.seg_id, .add);
            try existing_dest.put(alloc, entry.seg_id, {});
            copied += 1;
        }
        try dest_txn.commit();

        for (plan) |entry| {
            if (entry.class != .right_only) continue;

            const seg_bytes = try self.readSegmentBytesAlloc(&source_txn, alloc, entry.seg_id);
            defer alloc.free(seg_bytes);
            try dest.writer.addSegmentWithIdData(entry.seg_id, try dest.materializeSegmentData(entry.seg_id, seg_bytes));

            const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, entry.seg_id));
            const deletion_bytes = source_txn.get(.deletions, &seg_key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
            if (deletion_bytes) |bytes| {
                var bitmap = try roaring.RoaringBitmap.fromBytes(self.alloc, bytes);
                if (!bitmap.isEmpty()) {
                    dest.writer.setDeletionBitmap(entry.seg_id, bitmap);
                } else {
                    bitmap.deinit();
                }
            }

            if (collect_doc_keys) {
                var reader = try segment_mod.SegmentReader.init(alloc, seg_bytes);
                defer reader.deinit();
                for (0..reader.doc_count) |doc_idx| {
                    const stored = reader.storedDoc(@intCast(doc_idx)) orelse continue;
                    try doc_keys.append(alloc, try alloc.dupe(u8, stored.id));
                }
            }
        }

        return .{
            .transferred_segments = copied,
            .doc_keys = try doc_keys.toOwnedSlice(alloc),
        };
    }

    pub fn readStoredSegment(self: *PersistentIndex, alloc: Allocator, seg_id: u64) !StoredSegment {
        var txn = try self.beginReadMainTxn();
        defer txn.abort();

        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
        const segment_bytes = try self.readSegmentBytesAlloc(&txn, alloc, seg_id);
        errdefer alloc.free(segment_bytes);

        const deletion_bitmap_bytes = txn.get(.deletions, &seg_key) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };

        return .{
            .seg_id = seg_id,
            .segment_bytes = segment_bytes,
            .deletion_bitmap_bytes = if (deletion_bitmap_bytes) |bytes| try alloc.dupe(u8, bytes) else null,
        };
    }

    /// Tombstone a document by external ID and persist the segment deletion bitmap.
    pub fn deleteById(self: *PersistentIndex, doc_id: []const u8) !bool {
        const delete_info = (try self.writer.deleteByIdTracked(self.alloc, doc_id)) orelse return false;
        defer self.alloc.free(delete_info.bitmap_bytes);

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, delete_info.seg_id));
        try txn.put(.deletions, &seg_key, delete_info.bitmap_bytes);
        try txn.commit();
        return true;
    }

    /// Atomically replace old segments with a newly merged segment.
    pub fn replaceSegments(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []const u8) !void {
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        _ = try self.replaceSegmentsWithReservedId(old_seg_ids, new_seg_id, segment_bytes, false);
    }

    /// Like replaceSegments(), but takes ownership of segment_bytes.
    pub fn replaceSegmentsOwned(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []u8) !void {
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        _ = try self.replaceSegmentsWithReservedIdOwned(old_seg_ids, new_seg_id, segment_bytes, false);
    }

    /// Like replaceSegments, but skips the merge if the planned source segment
    /// IDs are no longer all active. Merge schedulers use this after building
    /// against a retained snapshot while concurrent writes may have advanced.
    pub fn replaceSegmentsIfActive(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []const u8) !bool {
        if (old_seg_ids.len == 0) return false;
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        return try self.replaceSegmentsWithReservedId(old_seg_ids, new_seg_id, segment_bytes, true);
    }

    /// Like replaceSegmentsIfActive(), but consumes segment_bytes regardless of
    /// whether the merge is applied.
    pub fn replaceSegmentsIfActiveOwned(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []u8) !bool {
        if (old_seg_ids.len == 0) {
            self.alloc.free(segment_bytes);
            return false;
        }
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_id = self.reserveSegmentId();
        return try self.replaceSegmentsWithReservedIdOwned(old_seg_ids, new_seg_id, segment_bytes, true);
    }

    /// Atomically replace old segments with zero or more newly merged segments.
    /// Consumes segment_bytes_list and every segment buffer regardless of
    /// whether the replacement is applied.
    pub fn replaceSegmentsIfActiveManyOwned(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes_list: [][]u8) !bool {
        if (old_seg_ids.len == 0) {
            freeOwnedSegmentList(self.alloc, segment_bytes_list);
            return false;
        }
        if (segment_bytes_list.len == 0) {
            return try self.removeSegmentsIfActive(old_seg_ids);
        }
        self.lockStorage();
        defer self.unlockStorage();

        const new_seg_ids = try self.reserveSegmentIds(segment_bytes_list.len);
        defer self.alloc.free(new_seg_ids);
        return try self.replaceSegmentsWithReservedIdsOwned(old_seg_ids, new_seg_ids, segment_bytes_list, true);
    }

    pub fn removeSegmentsIfActive(self: *PersistentIndex, old_seg_ids: []const u64) !bool {
        if (old_seg_ids.len == 0) return false;
        self.lockStorage();
        defer self.unlockStorage();

        if (!try self.allSegmentsActive(old_seg_ids)) return false;
        try self.removeSegmentsLocked(old_seg_ids);
        return true;
    }

    fn allSegmentsActive(self: *PersistentIndex, old_seg_ids: []const u64) !bool {
        var txn = try self.beginReadMainTxn();
        defer txn.abort();
        const active_ids = try self.loadActiveSegmentIds(&txn, self.alloc);
        defer self.alloc.free(active_ids);
        for (old_seg_ids) |old_id| {
            if (!containsSegmentId(active_ids, old_id)) return false;
        }
        return true;
    }

    fn reserveSegmentId(self: *PersistentIndex) u64 {
        self.writer.lockMutex();
        const seg_id = self.writer.next_segment_id;
        self.writer.next_segment_id += 1;
        self.writer.mu.unlock();
        return seg_id;
    }

    fn reserveSegmentIds(self: *PersistentIndex, count: usize) ![]u64 {
        const seg_ids = try self.alloc.alloc(u64, count);
        self.writer.lockMutex();
        defer self.writer.mu.unlock();
        for (seg_ids, 0..) |*seg_id, i| {
            seg_id.* = self.writer.next_segment_id + i;
        }
        self.writer.next_segment_id += count;
        return seg_ids;
    }

    fn materializeSegmentData(self: *PersistentIndex, seg_id: u64, segment_bytes: []const u8) !index_mod.SegmentData {
        if (self.segment_files) |*store| {
            return try store.publish(seg_id, segment_bytes);
        }
        return index_mod.SegmentData.fromOwnedHeap(try self.alloc.dupe(u8, segment_bytes));
    }

    fn readSegmentBytesAlloc(self: *PersistentIndex, txn: *MainTxn, alloc: Allocator, seg_id: u64) ![]u8 {
        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
        if (txn.get(.segments, &seg_key)) |segment_bytes| {
            return try alloc.dupe(u8, segment_bytes);
        } else |err| switch (err) {
            error.NotFound => {},
            else => return err,
        }

        const store = self.segment_files orelse return error.NotFound;
        if (comptime builtin.os.tag == .freestanding) return error.NotFound;

        const segment_path = try store.pathAlloc(seg_id);
        defer store.allocator.free(segment_path);

        return store.storage.readFileAlloc(
            alloc,
            segment_path,
            std.math.maxInt(usize),
        ) catch |file_err| switch (file_err) {
            error.FileNotFound => error.NotFound,
            else => file_err,
        };
    }

    fn deleteSegmentFile(self: *PersistentIndex, seg_id: u64) void {
        if (self.segment_files) |*store| store.delete(seg_id);
    }

    fn pruneActiveSegmentsMissingRangesOrDataLocked(self: *PersistentIndex, log_path: []const u8) !void {
        const ranges = self.activeSegmentRangesRepairing(self.alloc, true, true) catch |err| switch (err) {
            error.StaleActiveSegments => return error.NotFound,
            else => return err,
        };
        defer {
            for (ranges) |*range| range.deinit(self.alloc);
            self.alloc.free(ranges);
        }
        _ = log_path;
    }

    fn pruneMissingActiveSegments(self: *PersistentIndex, stale_seg_ids: []const u64) !void {
        self.lockStorage();
        defer self.unlockStorage();
        try self.pruneMissingActiveSegmentsLocked(stale_seg_ids);
    }

    fn pruneMissingActiveSegmentsLocked(self: *PersistentIndex, stale_seg_ids: []const u64) !void {
        if (stale_seg_ids.len == 0) return;

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        for (stale_seg_ids) |seg_id| {
            const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
            try self.updateActiveSegments(&txn, seg_id, .remove);
            txn.delete(.segments, &seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, seg_id);
        }

        try txn.commit();
    }

    fn replaceSegmentsWithReservedId(self: *PersistentIndex, old_seg_ids: []const u64, new_seg_id: u64, segment_bytes: []const u8, require_active: bool) !bool {
        const owned = try self.alloc.dupe(u8, segment_bytes);
        return try self.replaceSegmentsWithReservedIdOwned(old_seg_ids, new_seg_id, owned, require_active);
    }

    fn replaceSegmentsWithReservedIdOwned(self: *PersistentIndex, old_seg_ids: []const u64, new_seg_id: u64, segment_bytes: []u8, require_active: bool) !bool {
        const owned = segment_bytes;
        defer self.alloc.free(owned);

        var key_range = try extractSegmentKeyRange(self.alloc, owned);
        defer key_range.deinit(self.alloc);

        var segment_data: ?index_mod.SegmentData = try self.materializeSegmentData(new_seg_id, owned);
        errdefer {
            if (segment_data) |*data| data.deinit(self.alloc);
            self.deleteSegmentFile(new_seg_id);
        }

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        if (require_active) {
            const active_ids = try self.loadActiveSegmentIds(&txn, self.alloc);
            defer self.alloc.free(active_ids);
            for (old_seg_ids) |old_id| {
                if (!containsSegmentId(active_ids, old_id)) {
                    txn.abort();
                    if (segment_data) |*data| data.deinit(self.alloc);
                    self.deleteSegmentFile(new_seg_id);
                    return false;
                }
            }
        }

        const new_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, new_seg_id));
        if (self.segment_files == null) try txn.put(.segments, &new_seg_key, owned);

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        try self.saveSegmentRange(&txn, new_seg_id, key_range);
        try self.updateActiveSegments(&txn, new_seg_id, .add);
        try txn.commit();

        try self.writer.replaceSegmentsData(old_seg_ids, new_seg_id, segment_data.?);
        segment_data = null;
        for (old_seg_ids) |old_id| self.deleteSegmentFile(old_id);
        return true;
    }

    fn replaceSegmentsCatalogOnlyForTest(self: *PersistentIndex, old_seg_ids: []const u64, segment_bytes: []const u8) !void {
        if (!builtin.is_test) return error.Unsupported;

        const new_seg_id = self.reserveSegmentId();
        var key_range = try extractSegmentKeyRange(self.alloc, segment_bytes);
        defer key_range.deinit(self.alloc);

        var segment_data: ?index_mod.SegmentData = try self.materializeSegmentData(new_seg_id, segment_bytes);
        defer if (segment_data) |*data| data.deinit(self.alloc);
        errdefer self.deleteSegmentFile(new_seg_id);

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        const new_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, new_seg_id));
        if (self.segment_files == null) try txn.put(.segments, &new_seg_key, segment_bytes);

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        try self.saveSegmentRange(&txn, new_seg_id, key_range);
        try self.updateActiveSegments(&txn, new_seg_id, .add);
        try txn.commit();

        // Simulates a process crash after durable catalog publish but before
        // the volatile IndexWriter snapshot observes the replacement.
        segment_data.?.deinit(self.alloc);
        segment_data = null;
    }

    fn replaceSegmentsWithReservedIdsOwned(
        self: *PersistentIndex,
        old_seg_ids: []const u64,
        new_seg_ids: []const u64,
        segment_bytes_list: [][]u8,
        require_active: bool,
    ) !bool {
        std.debug.assert(new_seg_ids.len == segment_bytes_list.len);
        defer freeOwnedSegmentList(self.alloc, segment_bytes_list);

        var key_ranges = try self.alloc.alloc(SegmentKeyRange, segment_bytes_list.len);
        var key_ranges_initialized: usize = 0;
        defer {
            for (key_ranges[0..key_ranges_initialized]) |*range| range.deinit(self.alloc);
            self.alloc.free(key_ranges);
        }

        var replacements = try self.alloc.alloc(index_mod.ReplacementSegmentData, segment_bytes_list.len);
        var replacements_initialized: usize = 0;
        var published_to_writer = false;
        defer {
            if (!published_to_writer) {
                for (replacements[0..replacements_initialized]) |*replacement| {
                    replacement.data.deinit(self.alloc);
                    self.deleteSegmentFile(replacement.id);
                }
            }
            self.alloc.free(replacements);
        }

        for (segment_bytes_list, 0..) |segment_bytes, i| {
            key_ranges[i] = try extractSegmentKeyRange(self.alloc, segment_bytes);
            key_ranges[i].seg_id = new_seg_ids[i];
            key_ranges_initialized += 1;

            replacements[i] = .{
                .id = new_seg_ids[i],
                .data = try self.materializeSegmentData(new_seg_ids[i], segment_bytes),
            };
            replacements_initialized += 1;
        }

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        if (require_active) {
            const active_ids = try self.loadActiveSegmentIds(&txn, self.alloc);
            defer self.alloc.free(active_ids);
            for (old_seg_ids) |old_id| {
                if (!containsSegmentId(active_ids, old_id)) {
                    txn.abort();
                    return false;
                }
            }
        }

        for (segment_bytes_list, 0..) |segment_bytes, i| {
            const new_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, new_seg_ids[i]));
            if (self.segment_files == null) try txn.put(.segments, &new_seg_key, segment_bytes);
        }

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        for (key_ranges[0..key_ranges_initialized]) |key_range| {
            try self.saveSegmentRange(&txn, key_range.seg_id, key_range);
            try self.updateActiveSegments(&txn, key_range.seg_id, .add);
        }
        try txn.commit();

        try self.writer.replaceSegmentsManyData(old_seg_ids, replacements);
        published_to_writer = true;
        for (old_seg_ids) |old_id| self.deleteSegmentFile(old_id);
        return true;
    }

    pub fn removeSegments(self: *PersistentIndex, old_seg_ids: []const u64) !void {
        if (old_seg_ids.len == 0) return;
        self.lockStorage();
        defer self.unlockStorage();
        try self.removeSegmentsLocked(old_seg_ids);
    }

    fn removeSegmentsLocked(self: *PersistentIndex, old_seg_ids: []const u64) !void {
        if (old_seg_ids.len == 0) return;

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        for (old_seg_ids) |old_id| {
            const old_seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, old_id));
            try self.updateActiveSegments(&txn, old_id, .remove);
            txn.delete(.segments, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            txn.delete(.deletions, &old_seg_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
            try self.deleteSegmentRange(&txn, old_id);
        }

        try txn.commit();
        try self.writer.removeSegments(old_seg_ids);
        for (old_seg_ids) |old_id| self.deleteSegmentFile(old_id);
    }

    fn persistSegment(self: *PersistentIndex, seg_id: u64, segment_bytes: []const u8, lsn: u64) !void {
        var key_range = try extractSegmentKeyRange(self.alloc, segment_bytes);
        defer key_range.deinit(self.alloc);

        var txn = try self.beginWriteMainTxn();
        errdefer txn.abort();

        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
        if (self.segment_files == null) try txn.put(.segments, &seg_key, segment_bytes);

        try self.saveSegmentRange(&txn, seg_id, key_range);

        // Update active segments list
        try self.updateActiveSegments(&txn, seg_id, .add);

        // Update committed_lsn
        const lsn_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, lsn));
        try txn.put(.meta, meta_committed_lsn, &lsn_bytes);

        try txn.commit();
        self.committed_lsn = lsn;
    }

    fn updateActiveSegments(self: *PersistentIndex, txn: *MainTxn, seg_id: u64, op: enum { add, remove }) !void {
        _ = self;
        const marker_key = activeSegmentMetaKey(seg_id);
        switch (op) {
            .add => try txn.put(.meta, &marker_key, &.{1}),
            .remove => txn.delete(.meta, &marker_key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            },
        }
        txn.delete(.meta, meta_active_segments) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn replayWalEntry(self: *PersistentIndex, lsn: u64, segment_bytes: []const u8) !void {
        const seg_id = self.writer.next_segment_id;
        var segment_data: ?index_mod.SegmentData = try self.materializeSegmentData(seg_id, segment_bytes);
        errdefer {
            if (segment_data) |*data| data.deinit(self.alloc);
            self.deleteSegmentFile(seg_id);
        }
        try self.persistSegment(seg_id, segment_bytes, lsn);
        try self.writer.addSegmentWithIdData(seg_id, segment_data.?);
        segment_data = null;
        try self.wal.truncate(lsn);
    }

    fn saveSegmentRange(
        self: *PersistentIndex,
        txn: *MainTxn,
        seg_id: u64,
        key_range: SegmentKeyRange,
    ) !void {
        const range_key = segmentRangeMetaKey(seg_id);
        var buf = try self.alloc.alloc(u8, 8 + key_range.min_doc_key.len + key_range.max_doc_key.len);
        defer self.alloc.free(buf);
        buf[0..4].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(key_range.min_doc_key.len))));
        @memcpy(buf[4..][0..key_range.min_doc_key.len], key_range.min_doc_key);
        const max_off = 4 + key_range.min_doc_key.len;
        buf[max_off..][0..4].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(key_range.max_doc_key.len))));
        @memcpy(buf[max_off + 4 ..][0..key_range.max_doc_key.len], key_range.max_doc_key);
        try txn.put(.meta, &range_key, buf);
    }

    fn deleteSegmentRange(self: *PersistentIndex, txn: *MainTxn, seg_id: u64) !void {
        _ = self;
        const range_key = segmentRangeMetaKey(seg_id);
        txn.delete(.meta, &range_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
    }

    fn loadActiveSegmentIds(
        self: *PersistentIndex,
        txn: *MainTxn,
        alloc: Allocator,
    ) ![]u64 {
        _ = self;
        return try loadActiveSegmentIdsFromTxn(txn, alloc);
    }

    fn loadSegmentRange(
        self: *PersistentIndex,
        txn: *MainTxn,
        alloc: Allocator,
        seg_id: u64,
    ) !SegmentKeyRange {
        const range_key = segmentRangeMetaKey(seg_id);
        const encoded = txn.get(.meta, &range_key) catch |err| switch (err) {
            error.NotFound => {
                const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
                if (txn.get(.segments, &seg_key)) |segment_bytes| {
                    var rebuilt = try extractSegmentKeyRange(alloc, segment_bytes);
                    rebuilt.seg_id = seg_id;
                    return rebuilt;
                } else |segment_err| switch (segment_err) {
                    error.NotFound => {
                        if (self.segment_files) |*store| {
                            if (comptime builtin.os.tag == .freestanding) {
                                return error.NotFound;
                            }

                            const segment_path = try store.pathAlloc(seg_id);
                            defer self.alloc.free(segment_path);

                            const file_bytes = store.storage.readFileAlloc(
                                alloc,
                                segment_path,
                                std.math.maxInt(usize),
                            ) catch |file_err| switch (file_err) {
                                error.FileNotFound => return error.NotFound,
                                else => return file_err,
                            };
                            defer alloc.free(file_bytes);

                            var rebuilt = try extractSegmentKeyRange(alloc, file_bytes);
                            rebuilt.seg_id = seg_id;
                            return rebuilt;
                        }
                        return error.NotFound;
                    },
                    else => return segment_err,
                }
            },
            else => return err,
        };
        return try decodeSegmentRange(alloc, seg_id, encoded);
    }
};

fn loadActiveSegmentIdsFromTxn(txn: *PersistentIndex.MainTxn, alloc: Allocator) ![]u64 {
    var cursor = txn.openCursor(.meta) catch |err| switch (err) {
        error.NotFound => return try alloc.alloc(u64, 0),
        else => return err,
    };
    defer cursor.close();

    var seg_ids = std.ArrayListUnmanaged(u64).empty;
    errdefer seg_ids.deinit(alloc);

    var entry = try cursor.seekAtOrAfter(meta_active_segment_prefix);
    while (entry) |item| : (entry = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, meta_active_segment_prefix)) break;
        if (item.key.len != meta_active_segment_prefix.len + 8) continue;
        try seg_ids.append(alloc, std.mem.readInt(u64, item.key[meta_active_segment_prefix.len..][0..8], .big));
    }
    return try seg_ids.toOwnedSlice(alloc);
}

fn segmentRangeMetaKey(seg_id: u64) [meta_segment_range_prefix.len + 8]u8 {
    var key: [meta_segment_range_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..meta_segment_range_prefix.len], meta_segment_range_prefix);
    key[meta_segment_range_prefix.len..][0..8].* = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
    return key;
}

fn activeSegmentMetaKey(seg_id: u64) [meta_active_segment_prefix.len + 8]u8 {
    var key: [meta_active_segment_prefix.len + 8]u8 = undefined;
    @memcpy(key[0..meta_active_segment_prefix.len], meta_active_segment_prefix);
    key[meta_active_segment_prefix.len..][0..8].* = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));
    return key;
}

fn isStaleActiveSegmentDataError(err: anyerror) bool {
    return switch (err) {
        error.CorruptInput,
        error.CrcMismatch,
        error.InvalidMagic,
        error.InvalidSegment,
        error.UnsupportedVersion,
        => true,
        else => false,
    };
}

fn extractSegmentKeyRange(alloc: Allocator, segment_bytes: []const u8) !SegmentKeyRange {
    var reader = try segment_mod.SegmentReader.init(alloc, segment_bytes);
    defer reader.deinit();

    if (reader.doc_count == 0) return error.EmptySegment;

    var min_key: ?[]u8 = null;
    errdefer if (min_key) |key| alloc.free(key);
    var max_key: ?[]u8 = null;
    errdefer if (max_key) |key| alloc.free(key);

    for (0..reader.doc_count) |doc_idx| {
        const stored = reader.storedDoc(@intCast(doc_idx)) orelse continue;
        if (min_key == null or std.mem.order(u8, stored.id, min_key.?) == .lt) {
            if (min_key) |key| alloc.free(key);
            min_key = try alloc.dupe(u8, stored.id);
        }
        if (max_key == null or std.mem.order(u8, stored.id, max_key.?) == .gt) {
            if (max_key) |key| alloc.free(key);
            max_key = try alloc.dupe(u8, stored.id);
        }
    }

    if (min_key == null or max_key == null) return error.EmptySegment;
    return .{
        .seg_id = 0,
        .min_doc_key = min_key.?,
        .max_doc_key = max_key.?,
    };
}

fn decodeSegmentRange(alloc: Allocator, seg_id: u64, encoded: []const u8) !SegmentKeyRange {
    if (encoded.len < 8) return error.CorruptEnvironment;
    const min_len = std.mem.readInt(u32, encoded[0..4], .little);
    const min_end = 4 + min_len;
    if (min_end + 4 > encoded.len) return error.CorruptEnvironment;
    const max_len = std.mem.readInt(u32, encoded[min_end..][0..4], .little);
    const max_start = min_end + 4;
    const max_end = max_start + max_len;
    if (max_end > encoded.len) return error.CorruptEnvironment;

    return .{
        .seg_id = seg_id,
        .min_doc_key = try alloc.dupe(u8, encoded[4..min_end]),
        .max_doc_key = try alloc.dupe(u8, encoded[max_start..max_end]),
    };
}

fn classifySegmentRange(min_doc_key: []const u8, max_doc_key: []const u8, split_key: []const u8) SegmentSplitClass {
    if (std.mem.order(u8, max_doc_key, split_key) == .lt) return .left_only;
    if (std.mem.order(u8, min_doc_key, split_key) != .lt) return .right_only;
    return .mixed;
}

fn containsSegmentId(segment_ids: []const u64, target: u64) bool {
    for (segment_ids) |seg_id| {
        if (seg_id == target) return true;
    }
    return false;
}

fn freeOwnedSegmentList(alloc: Allocator, segments: [][]u8) void {
    for (segments) |segment| alloc.free(segment);
    if (segments.len > 0) alloc.free(segments);
}

fn mapSegmentFile(path: []const u8) anyerror![]align(std.heap.page_size_min) u8 {
    if (builtin.os.tag == .freestanding or builtin.os.tag == .windows or builtin.os.tag == .wasi) {
        return error.UnsupportedPlatform;
    }

    const fd = try std.posix.openat(std.posix.AT.FDCWD, path, .{
        .ACCMODE = .RDONLY,
        .CLOEXEC = true,
    }, 0);
    defer _ = std.posix.system.close(fd);

    const len = try fileSizeFromFd(fd);
    if (len == 0) return error.EmptySegment;

    return try std.posix.mmap(
        null,
        len,
        .{ .READ = true },
        .{ .TYPE = .SHARED },
        fd,
        0,
    );
}

fn fileSizeFromFd(fd: std.posix.fd_t) !usize {
    if (builtin.os.tag == .linux) {
        const linux = std.os.linux;
        const empty_path: [*:0]const u8 = "";
        while (true) {
            var statx = std.mem.zeroes(linux.Statx);
            switch (linux.errno(linux.statx(fd, empty_path, linux.AT.EMPTY_PATH, .{ .SIZE = true }, &statx))) {
                .SUCCESS => {
                    if (!statx.mask.SIZE) return error.Unexpected;
                    return @intCast(statx.size);
                },
                .INTR => continue,
                else => return error.Unexpected,
            }
        }
    } else {
        var stat: std.posix.Stat = undefined;
        while (true) {
            const rc = std.posix.system.fstat(fd, &stat);
            switch (std.posix.errno(rc)) {
                .SUCCESS => return @intCast(stat.size),
                .INTR => continue,
                else => return error.Unexpected,
            }
        }
    }
}

// ============================================================================
// Tests
// ============================================================================

const scorer_mod = @import("../search/scorer.zig");

fn buildSimpleSegment(alloc: Allocator, doc_id: []const u8, term: []const u8) ![]u8 {
    var inv_builder = inverted_mod.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    try inv_builder.addDocument(0, &.{.{ .term = term, .freq = 1, .norm = 10 }});
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    try seg_writer.addStoredDoc(doc_id, "{}");

    return seg_writer.build();
}

fn buildMultiDocSegment(alloc: Allocator, docs: []const struct { doc_id: []const u8, term: []const u8 }) ![]u8 {
    var inv_builder = inverted_mod.InvertedIndexBuilder.init(alloc, .{});
    defer inv_builder.deinit();
    for (docs, 0..) |doc, i| {
        try inv_builder.addDocument(@intCast(i), &.{.{ .term = doc.term, .freq = 1, .norm = 10 }});
    }
    const inv_data = try inv_builder.build();
    defer alloc.free(inv_data);

    var seg_writer = segment_mod.SegmentWriter.init(alloc);
    defer seg_writer.deinit();
    const field_idx = try seg_writer.addField("body");
    try seg_writer.addSection(field_idx, .inverted_text, inv_data);
    for (docs) |doc| {
        try seg_writer.addStoredDoc(doc.doc_id, "{}");
    }
    return seg_writer.build();
}

var persist_tmp_nonce: u64 = 0;

fn persistTmpPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-persist-test-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &persist_tmp_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupPersistDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "persistent index write and read" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg = try buildSimpleSegment(alloc, "doc1", "hello");
        defer alloc.free(seg);
        try pi.indexSegment(seg);

        const snap = pi.snapshot();
        try std.testing.expectEqual(@as(u32, 1), snap.global_doc_count);
    }
}

test "persistent index reopen recovery" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    // Write data
    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg1 = try buildSimpleSegment(alloc, "doc1", "hello");
        defer alloc.free(seg1);
        try pi.indexSegment(seg1);

        const seg2 = try buildSimpleSegment(alloc, "doc2", "world");
        defer alloc.free(seg2);
        try pi.indexSegment(seg2);

        try std.testing.expectEqual(@as(u32, 2), pi.snapshot().global_doc_count);
    }

    // Reopen and verify data survived
    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        try std.testing.expectEqual(@as(u32, 2), pi.snapshot().global_doc_count);

        // Verify search works
        const snap = pi.snapshot();
        const results = try snap.search(alloc, "body", &.{"hello"}, 10);
        defer alloc.free(results.hits);
        try std.testing.expect(results.hits.len >= 1);
    }
}

test "persistent index exposes wal and lmdb commit stats" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{
        .path = path,
        .wal_group_commit_window_ns = 5 * std.time.ns_per_ms,
        .wal_group_commit_max_requests = 4,
    });
    defer pi.close();

    const seg = try buildSimpleSegment(alloc, "doc1", "hello");
    defer alloc.free(seg);
    try pi.indexSegment(seg);

    const stats = pi.statsSnapshot();
    if (supports_main_lmdb) {
        try std.testing.expectEqual(@as(u64, 0), stats.wal.append_calls);
    } else {
        try std.testing.expectEqual(@as(u64, 1), stats.wal.append_calls);
        try std.testing.expect(stats.wal.physical_commits >= 1);
    }
    if (stats.main_commit) |commit| {
        try std.testing.expect(commit.publish_calls >= 1);
        try std.testing.expect(commit.full_publish_calls >= 1);
        try std.testing.expect(commit.page_images_written > 0);
        try std.testing.expect(commit.bytes_written > 0);
        try std.testing.expect(commit.total_publish_ns > 0);
    }
}

test "persistent index persists segment key ranges across reopen" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg_a = try buildSimpleSegment(alloc, "doc:c", "alpha");
        defer alloc.free(seg_a);
        try pi.indexSegment(seg_a);

        const seg_b = try buildMultiDocSegment(alloc, &.{
            .{ .doc_id = "doc:m", .term = "middle" },
            .{ .doc_id = "doc:z", .term = "omega" },
        });
        defer alloc.free(seg_b);
        try pi.indexSegment(seg_b);

        const ranges = try pi.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }
        try std.testing.expectEqual(@as(usize, 2), ranges.len);
        try std.testing.expectEqualStrings("doc:c", ranges[0].min_doc_key);
        try std.testing.expectEqualStrings("doc:c", ranges[0].max_doc_key);
        try std.testing.expectEqualStrings("doc:m", ranges[1].min_doc_key);
        try std.testing.expectEqualStrings("doc:z", ranges[1].max_doc_key);
    }

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const ranges = try pi.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }
        try std.testing.expectEqual(@as(usize, 2), ranges.len);
        try std.testing.expectEqualStrings("doc:c", ranges[0].min_doc_key);
        try std.testing.expectEqualStrings("doc:c", ranges[0].max_doc_key);
        try std.testing.expectEqualStrings("doc:m", ranges[1].min_doc_key);
        try std.testing.expectEqualStrings("doc:z", ranges[1].max_doc_key);
    }
}

test "persistent index replaceSegments updates segment key range metadata" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();

    const seg_a = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg_a);
    try pi.indexSegment(seg_a);

    const seg_b = try buildSimpleSegment(alloc, "doc:z", "omega");
    defer alloc.free(seg_b);
    try pi.indexSegment(seg_b);

    const merged = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:z", .term = "omega" },
    });
    defer alloc.free(merged);
    try pi.replaceSegments(&.{ 1, 2 }, merged);

    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqual(@as(u64, 3), ranges[0].seg_id);
    try std.testing.expectEqualStrings("doc:a", ranges[0].min_doc_key);
    try std.testing.expectEqualStrings("doc:z", ranges[0].max_doc_key);
}

test "persistent index replaceSegmentsIfActiveManyOwned publishes bounded merge outputs atomically" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();

    const seg_a = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg_a);
    try pi.indexSegment(seg_a);

    const seg_b = try buildSimpleSegment(alloc, "doc:z", "omega");
    defer alloc.free(seg_b);
    try pi.indexSegment(seg_b);

    const replacements = try alloc.alloc([]u8, 2);
    replacements[0] = try buildSimpleSegment(alloc, "doc:a", "alpha");
    replacements[1] = try buildSimpleSegment(alloc, "doc:z", "omega");
    const applied = try pi.replaceSegmentsIfActiveManyOwned(&.{ 1, 2 }, replacements);
    try std.testing.expect(applied);

    const snap = pi.snapshot();
    try std.testing.expectEqual(@as(usize, 2), snap.segments.len);
    try std.testing.expectEqual(@as(u64, 3), snap.segments[0].id);
    try std.testing.expectEqual(@as(u64, 4), snap.segments[1].id);

    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 2), ranges.len);
    try std.testing.expectEqualStrings("doc:a", ranges[0].min_doc_key);
    try std.testing.expectEqualStrings("doc:z", ranges[1].min_doc_key);
}

test "persistent index open prunes stale active segment references" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
        defer alloc.free(seg);
        try pi.indexSegment(seg);

        var txn = try pi.beginWriteMainTxn();
        errdefer txn.abort();

        const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, @as(u64, 1)));
        txn.delete(.segments, &seg_key) catch |err| switch (err) {
            error.NotFound => {},
            else => return err,
        };
        try pi.deleteSegmentRange(&txn, 1);
        try txn.commit();
        pi.deleteSegmentFile(1);
    }

    {
        var pi = try PersistentIndex.open(alloc, .{ .path = path });
        defer pi.close();

        const ranges = try pi.activeSegmentRanges(alloc);
        defer {
            for (ranges) |*range| range.deinit(alloc);
            alloc.free(ranges);
        }
        try std.testing.expectEqual(@as(usize, 0), ranges.len);

        var txn = try pi.beginReadMainTxn();
        defer txn.abort();
        const active_ids = try pi.loadActiveSegmentIds(&txn, alloc);
        defer alloc.free(active_ids);
        try std.testing.expectEqual(@as(usize, 0), active_ids.len);
    }
}

test "persistent index classifies active segments for split" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var pi = try PersistentIndex.open(alloc, .{ .path = path });
    defer pi.close();

    const left_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:c", .term = "charlie" },
    });
    defer alloc.free(left_seg);
    try pi.indexSegment(left_seg);

    const right_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:m", .term = "middle" },
        .{ .doc_id = "doc:z", .term = "omega" },
    });
    defer alloc.free(right_seg);
    try pi.indexSegment(right_seg);

    const mixed_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:f", .term = "foxtrot" },
        .{ .doc_id = "doc:t", .term = "tango" },
    });
    defer alloc.free(mixed_seg);
    try pi.indexSegment(mixed_seg);

    const plan = try pi.classifyActiveSegmentsForSplit(alloc, "doc:m");
    defer {
        for (plan) |*entry| entry.deinit(alloc);
        alloc.free(plan);
    }

    try std.testing.expectEqual(@as(usize, 3), plan.len);
    try std.testing.expectEqual(SegmentSplitClass.left_only, plan[0].class);
    try std.testing.expectEqual(SegmentSplitClass.right_only, plan[1].class);
    try std.testing.expectEqual(SegmentSplitClass.mixed, plan[2].class);
}

test "persistent index hands off right-only segments to child index" {
    const alloc = std.testing.allocator;
    var src_path_buf: [256]u8 = undefined;
    const src_path = persistTmpPath(&src_path_buf);
    defer cleanupPersistDir(src_path);

    var dest_path_buf: [256]u8 = undefined;
    const dest_path = persistTmpPath(&dest_path_buf);
    defer cleanupPersistDir(dest_path);

    var src = try PersistentIndex.open(alloc, .{ .path = src_path });
    defer src.close();

    const left_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "alpha" },
        .{ .doc_id = "doc:c", .term = "charlie" },
    });
    defer alloc.free(left_seg);
    try src.indexSegment(left_seg);

    const right_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:m", .term = "middle" },
        .{ .doc_id = "doc:z", .term = "omega" },
    });
    defer alloc.free(right_seg);
    try src.indexSegment(right_seg);
    try std.testing.expect(try src.deleteById("doc:z"));

    const mixed_seg = try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:f", .term = "foxtrot" },
        .{ .doc_id = "doc:t", .term = "tango" },
    });
    defer alloc.free(mixed_seg);
    try src.indexSegment(mixed_seg);

    var dest = try PersistentIndex.open(alloc, .{ .path = dest_path });
    defer dest.close();

    const copied = try src.handoffRightOnlySegmentsToChild(&dest, "doc:m");
    try std.testing.expectEqual(@as(usize, 1), copied);

    const ranges = try dest.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }
    try std.testing.expectEqual(@as(usize, 1), ranges.len);
    try std.testing.expectEqualStrings("doc:m", ranges[0].min_doc_key);
    try std.testing.expectEqualStrings("doc:z", ranges[0].max_doc_key);

    const snap = dest.snapshot();
    try std.testing.expectEqual(@as(u32, 1), snap.global_doc_count);

    const middle_results = try snap.search(alloc, "body", &.{"middle"}, 10);
    defer alloc.free(middle_results.hits);
    try std.testing.expectEqual(@as(u32, 1), middle_results.total_count);

    const omega_results = try snap.search(alloc, "body", &.{"omega"}, 10);
    defer alloc.free(omega_results.hits);
    try std.testing.expectEqual(@as(u32, 0), omega_results.total_count);

    const alpha_results = try snap.search(alloc, "body", &.{"alpha"}, 10);
    defer alloc.free(alpha_results.hits);
    try std.testing.expectEqual(@as(u32, 0), alpha_results.total_count);
}

test "persistent namespace adapters expose multi-partition txn operations" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    defer idx.close();

    const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, 7));

    {
        var txn = try idx.beginWriteMainTxn();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put(.meta, meta_committed_lsn, &std.mem.toBytes(@as(u64, 11)));
        try write.put(.segments, &seg_key, "seg7");
        try write.put(.deletions, &seg_key, "del7");
        try write.commit();
    }

    {
        var txn = try idx.beginReadMainTxn();
        defer txn.abort();
        var read = txn.readAdapter();
        const lsn_bytes = try read.get(.meta, meta_committed_lsn);
        try std.testing.expectEqual(@as(u64, 11), std.mem.readInt(u64, lsn_bytes[0..8], .little));
        try std.testing.expectEqualStrings("seg7", try read.get(.segments, &seg_key));
        try std.testing.expectEqualStrings("del7", try read.get(.deletions, &seg_key));
    }
}

test "persistent backend store opens concrete txn handles" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    defer idx.close();

    const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, 8));
    var backend = idx.backendStore();
    try std.testing.expect(backend.capabilities().native_namespaces);

    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put(.segments, &seg_key, "seg8");
        try write.commit();
    }

    {
        var txn = try backend.beginRead();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("seg8", try read.get(.segments, &seg_key));
    }

    {
        var batch = try backend.beginBatch();
        errdefer batch.abort();
        var write = batch.writeAdapter();
        try write.put(.meta, meta_committed_lsn, &std.mem.toBytes(@as(u64, 13)));
        try write.commit();
    }
}

test "persistent backend runtime erases namespace store handles" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    var idx = try PersistentIndex.open(alloc, .{ .path = path });
    defer idx.close();

    var runtime = try backend_erased.namespaceStoreFrom(
        std.testing.allocator,
        idx.backendStore(),
        MainKeyspace,
        mapRuntimeNamespace,
    );
    defer runtime.deinit();
    try std.testing.expect(runtime.capabilities().native_namespaces);

    const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, 12));

    {
        var txn = try runtime.beginWrite();
        try txn.put(.{ .name = "segments" }, &seg_key, "seg12");
        try txn.put(.{}, meta_committed_lsn, &std.mem.toBytes(@as(u64, 21)));
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("seg12", try txn.get(.{ .name = "segments" }, &seg_key));
        const lsn = try txn.get(.{}, meta_committed_lsn);
        try std.testing.expectEqual(@as(u64, 21), std.mem.readInt(u64, lsn[0..8], .little));
    }
}

test "persistent index reopens with durable lsm main backend" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg);

    {
        var idx = try PersistentIndex.open(alloc, .{
            .path = path,
            .main_backend = .lsm,
        });
        defer idx.close();
        try idx.indexSegment(seg);
        try idx.syncMain(true);
        try std.testing.expect(idx.commitStatsSnapshot() == null);
    }

    {
        var idx = try PersistentIndex.open(alloc, .{
            .path = path,
            .main_backend = .lsm,
        });
        defer idx.close();

        const snap = idx.snapshot();
        try std.testing.expectEqual(@as(u32, 1), snap.global_doc_count);
        const results = try snap.search(alloc, "body", &.{"alpha"}, 10);
        defer alloc.free(results.hits);
        try std.testing.expectEqual(@as(u32, 1), results.total_count);
    }
}

test "persistent index lsm maintenance debt hint tracks segment store writes" {
    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg);

    var idx = try PersistentIndex.open(alloc, .{
        .path = path,
        .main_backend = .lsm,
        .main_lsm_options = .{ .flush_threshold = 1024 },
    });
    defer idx.close();

    try std.testing.expectEqual(@as(u64, 0), idx.lsmMaintenanceDebtHint());

    try idx.indexSegment(seg);
    try std.testing.expect(idx.lsmMaintenanceDebtHint() > 0);

    while (try idx.runLsmMaintenanceStep()) {}
    try std.testing.expectEqual(@as(u64, 0), idx.lsmMaintenanceDebtHint());
}

test "persistent index snapshots use mapped segment files when native storage is available" {
    if (!supports_main_lmdb) return error.SkipZigTest;

    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = persistTmpPath(&path_buf);
    defer cleanupPersistDir(path);

    const seg = try buildSimpleSegment(alloc, "doc:a", "alpha");
    defer alloc.free(seg);

    var idx = try PersistentIndex.open(alloc, .{
        .path = path,
        .main_backend = .lsm,
    });
    defer idx.close();

    try idx.indexSegment(seg);

    const snap = idx.snapshot();
    try std.testing.expectEqual(@as(usize, 1), snap.segments.len);
    try std.testing.expect(snap.segments[0].data.isFileBacked());
    try std.testing.expectEqualStrings("doc:a", snap.storedDoc(0).?.id);
}

const PersistentSimAction = persistent_sim_fixture.Action;
const PersistentCrashOutcome = persistent_sim_fixture.CrashOutcome;
const PersistentSimSegmentSpec = persistent_sim_fixture.SegmentSpec;

const PersistentSimSummary = struct {
    doc_count: u32,
    segment_count: usize,
    alpha_hits: usize,
    beta_hits: usize,
    gamma_hits: usize,
};

fn persistentSimOptionsToIndexOptions(
    path: [*:0]const u8,
    opts: persistent_sim_fixture.Options,
) PersistentIndexOptions {
    return .{
        .path = path,
        .main_no_sync = opts.main_no_sync,
        .main_no_meta_sync = opts.main_no_meta_sync,
        .wal_no_sync = opts.wal_no_sync,
        .main_commit_backend = switch (opts.main_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
        .wal_commit_backend = switch (opts.wal_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
    };
}

fn persistentModeledOptionsToIndexOptions(
    path: [*:0]const u8,
    opts: persistent_sim_fixture.Options,
    device_model: *storage_sim.ModeledDevice,
    runtime: *storage_sim.Runtime,
) PersistentIndexOptions {
    var index_opts = persistentSimOptionsToIndexOptions(path, opts);
    index_opts.main_backend = .lsm;
    index_opts.wal_backend = .lsm;
    index_opts.main_lsm_storage = device_model.storage();
    index_opts.wal_storage = device_model.storage();
    index_opts.wal_clock = runtime.clock();
    index_opts.wal_commit_scheduler = runtime.completionScheduler();
    index_opts.model_wal_commit_backend_completions = true;
    return index_opts;
}

fn fixtureOptionsFromPersistentOptions(opts: PersistentIndexOptions) persistent_sim_fixture.Options {
    return .{
        .main_no_sync = opts.main_no_sync,
        .main_no_meta_sync = opts.main_no_meta_sync,
        .wal_no_sync = opts.wal_no_sync,
        .main_commit_backend = switch (opts.main_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
        .wal_commit_backend = switch (opts.wal_commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
    };
}

fn persistentSimSummaryFromIndex(alloc: Allocator, pi: *PersistentIndex) !PersistentSimSummary {
    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }

    const snap = pi.snapshot();
    return .{
        .doc_count = snap.global_doc_count,
        .segment_count = ranges.len,
        .alpha_hits = try persistentSearchHitCount(alloc, snap, "alpha"),
        .beta_hits = try persistentSearchHitCount(alloc, snap, "beta"),
        .gamma_hits = try persistentSearchHitCount(alloc, snap, "gamma"),
    };
}

fn persistentSearchHitCount(alloc: Allocator, snap: *index_mod.IndexSnapshot, term: []const u8) !usize {
    const results = try snap.search(alloc, "body", &.{term}, 32);
    defer alloc.free(results.hits);
    return @intCast(results.total_count);
}

fn expectedPersistentSummary(actions: []const PersistentSimAction) PersistentSimSummary {
    var summary: PersistentSimSummary = .{
        .doc_count = 0,
        .segment_count = 0,
        .alpha_hits = 0,
        .beta_hits = 0,
        .gamma_hits = 0,
    };
    for (actions) |action| {
        switch (action) {
            .reopen => {},
            .index_segment => |spec| switch (spec) {
                .alpha => {
                    summary.doc_count += 1;
                    summary.segment_count += 1;
                    summary.alpha_hits += 1;
                },
                .beta => {
                    summary.doc_count += 1;
                    summary.segment_count += 1;
                    summary.beta_hits += 1;
                },
                .gamma_pair => {
                    summary.doc_count += 2;
                    summary.segment_count += 1;
                    summary.gamma_hits += 2;
                },
                .alpha_beta_pair => {
                    summary.doc_count += 2;
                    summary.segment_count += 1;
                    summary.alpha_hits += 1;
                    summary.beta_hits += 1;
                },
            },
        }
    }
    return summary;
}

fn expectPersistentSummaryFields(
    fixture_name: []const u8,
    opts: persistent_sim_fixture.Options,
    summary: PersistentSimSummary,
) !void {
    if (opts.expected_doc_count) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_doc_count", expected, summary.doc_count);
    }
    if (opts.expected_segment_count) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_segment_count", expected, summary.segment_count);
    }
    if (opts.expected_alpha_hits) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_alpha_hits", expected, summary.alpha_hits);
    }
    if (opts.expected_beta_hits) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_beta_hits", expected, summary.beta_hits);
    }
    if (opts.expected_gamma_hits) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_gamma_hits", expected, summary.gamma_hits);
    }
}

fn expectPersistentSummariesEqual(
    fixture_name: []const u8,
    expected: PersistentSimSummary,
    actual: PersistentSimSummary,
) !void {
    try sim_fixture.expectFieldEqual(fixture_name, "expected_doc_count", expected.doc_count, actual.doc_count);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_segment_count", expected.segment_count, actual.segment_count);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_alpha_hits", expected.alpha_hits, actual.alpha_hits);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_beta_hits", expected.beta_hits, actual.beta_hits);
    try sim_fixture.expectFieldEqual(fixture_name, "expected_gamma_hits", expected.gamma_hits, actual.gamma_hits);
}

fn expectPersistentCrashOutcome(
    fixture_name: []const u8,
    opts: persistent_sim_fixture.Options,
    outcome: PersistentCrashOutcome,
) !void {
    if (opts.expected_outcome) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_outcome", expected, outcome);
    }
}

fn classifyPersistentCrashSummary(
    before: PersistentSimSummary,
    after: PersistentSimSummary,
    actual: PersistentSimSummary,
    phase: lmdb.CommitPublishPhase,
) !PersistentCrashOutcome {
    _ = before;
    _ = phase;
    try expectPersistentSummariesEqual("persistent-crash-committed", after, actual);
    return .committed;
}

fn persistentSimSegment(alloc: Allocator, spec: PersistentSimSegmentSpec, step: usize) ![]u8 {
    return switch (spec) {
        .alpha => blk: {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:alpha:{d}", .{step});
            defer alloc.free(doc_id);
            break :blk try buildSimpleSegment(alloc, doc_id, "alpha");
        },
        .beta => blk: {
            const doc_id = try std.fmt.allocPrint(alloc, "doc:beta:{d}", .{step});
            defer alloc.free(doc_id);
            break :blk try buildSimpleSegment(alloc, doc_id, "beta");
        },
        .gamma_pair => blk: {
            const doc_id_a = try std.fmt.allocPrint(alloc, "doc:gamma:{d}:0", .{step});
            defer alloc.free(doc_id_a);
            const doc_id_b = try std.fmt.allocPrint(alloc, "doc:gamma:{d}:1", .{step});
            defer alloc.free(doc_id_b);
            break :blk try buildMultiDocSegment(alloc, &.{
                .{ .doc_id = doc_id_a, .term = "gamma" },
                .{ .doc_id = doc_id_b, .term = "gamma" },
            });
        },
        .alpha_beta_pair => blk: {
            const doc_id_a = try std.fmt.allocPrint(alloc, "doc:mix:{d}:a", .{step});
            defer alloc.free(doc_id_a);
            const doc_id_b = try std.fmt.allocPrint(alloc, "doc:mix:{d}:b", .{step});
            defer alloc.free(doc_id_b);
            break :blk try buildMultiDocSegment(alloc, &.{
                .{ .doc_id = doc_id_a, .term = "alpha" },
                .{ .doc_id = doc_id_b, .term = "beta" },
            });
        },
    };
}

fn applyPersistentReplayAction(pi: *PersistentIndex, alloc: Allocator, action: PersistentSimAction, step: usize) !void {
    switch (action) {
        .reopen => return,
        .index_segment => |spec| {
            const segment = try persistentSimSegment(alloc, spec, step);
            defer alloc.free(segment);
            try pi.indexSegment(segment);
        },
    }
}

fn replayPersistentSimActionsAtPath(
    alloc: Allocator,
    path: [*:0]const u8,
    opts: PersistentIndexOptions,
    actions: []const PersistentSimAction,
) !PersistentSimSummary {
    _ = path;
    var pi = try PersistentIndex.open(alloc, opts);
    var pi_open = true;
    defer if (pi_open) pi.close();

    for (actions, 0..) |action, step| {
        switch (action) {
            .reopen => {
                pi.close();
                pi_open = false;
                pi = try PersistentIndex.open(alloc, opts);
                pi_open = true;
            },
            else => try applyPersistentReplayAction(&pi, alloc, action, step),
        }
    }

    pi.close();
    pi_open = false;
    pi = try PersistentIndex.open(alloc, opts);
    pi_open = true;
    return try persistentSimSummaryFromIndex(alloc, &pi);
}

fn persistSegmentAtPhaseForTest(
    self: *PersistentIndex,
    seg_id: u64,
    segment_bytes: []const u8,
    lsn: u64,
    phase: lmdb.CommitPublishPhase,
) !void {
    const backend = switch (self.main_store_owner) {
        .lmdb => |backend| backend,
        else => return error.Unsupported,
    };

    var key_range = try extractSegmentKeyRange(self.alloc, segment_bytes);
    defer key_range.deinit(self.alloc);

    var raw = try backend.env.begin(.{});
    defer raw.abort();
    const segments_dbi = try raw.openDb(segments_db_name, .{ .create = true });
    const meta_dbi = try raw.openDb(meta_db_name, .{ .create = true });

    const seg_key = std.mem.toBytes(std.mem.nativeToBig(u64, seg_id));

    try raw.put(segments_dbi, &seg_key, segment_bytes, .{});

    const range_key = segmentRangeMetaKey(seg_id);
    var buf = try self.alloc.alloc(u8, 8 + key_range.min_doc_key.len + key_range.max_doc_key.len);
    defer self.alloc.free(buf);
    buf[0..4].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(key_range.min_doc_key.len))));
    @memcpy(buf[4..][0..key_range.min_doc_key.len], key_range.min_doc_key);
    const max_off = 4 + key_range.min_doc_key.len;
    buf[max_off..][0..4].* = @bitCast(std.mem.nativeToLittle(u32, @as(u32, @intCast(key_range.max_doc_key.len))));
    @memcpy(buf[max_off + 4 ..][0..key_range.max_doc_key.len], key_range.max_doc_key);
    try raw.put(meta_dbi, &range_key, buf, .{});

    const active_key = activeSegmentMetaKey(seg_id);
    try raw.put(meta_dbi, &active_key, &.{1}, .{});

    const lsn_bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, lsn));
    try raw.put(meta_dbi, meta_committed_lsn, &lsn_bytes, .{});
    try raw.publishCommitPhaseForTest(phase);
}

pub fn indexSegmentPublishPhaseForTest(
    self: *PersistentIndex,
    segment_bytes: []const u8,
    phase: lmdb.CommitPublishPhase,
) !void {
    const lsn = try self.wal.append(segment_bytes);

    self.writer.lockMutex();
    const seg_id = self.writer.next_segment_id;
    self.writer.next_segment_id += 1;
    self.writer.mu.unlock();

    try persistSegmentAtPhaseForTest(self, seg_id, segment_bytes, lsn, phase);
}

fn applyCommittedPersistentActionAtPath(
    alloc: Allocator,
    path: [*:0]const u8,
    opts: PersistentIndexOptions,
    step: usize,
    action: PersistentSimAction,
) !void {
    _ = path;
    var pi = try PersistentIndex.open(alloc, opts);
    defer pi.close();
    try applyPersistentReplayAction(&pi, alloc, action, step);
}

fn applyPersistentCrashActionAtPath(
    alloc: Allocator,
    path: [*:0]const u8,
    opts: PersistentIndexOptions,
    step: usize,
    action: PersistentSimAction,
    phase: lmdb.CommitPublishPhase,
) !void {
    _ = path;
    var pi = try PersistentIndex.open(alloc, opts);
    defer pi.close();

    switch (action) {
        .reopen => return error.InvalidFixture,
        .index_segment => |spec| {
            const segment = try persistentSimSegment(alloc, spec, step);
            defer alloc.free(segment);
            try indexSegmentPublishPhaseForTest(&pi, segment, phase);
        },
    }
}

fn replayPersistentCrashWorkload(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    prelude_actions: []const PersistentSimAction,
    crash_action: PersistentSimAction,
    phase: lmdb.CommitPublishPhase,
) !PersistentCrashOutcome {
    var committed_path_buf: [256]u8 = undefined;
    const committed_path = persistTmpPathWithSuffix(&committed_path_buf, "sim-crash-committed");
    defer cleanupPersistDir(committed_path);

    var crash_path_buf: [256]u8 = undefined;
    const crash_path = persistTmpPathWithSuffix(&crash_path_buf, "sim-crash-phase");
    defer cleanupPersistDir(crash_path);

    _ = try replayPersistentSimActionsAtPath(alloc, committed_path, persistentSimOptionsAtPath(committed_path, opts), prelude_actions);
    _ = try replayPersistentSimActionsAtPath(alloc, crash_path, persistentSimOptionsAtPath(crash_path, opts), prelude_actions);

    const before = try replayPersistentSimActionsAtPath(alloc, committed_path, persistentSimOptionsAtPath(committed_path, opts), &.{});

    try applyCommittedPersistentActionAtPath(alloc, committed_path, persistentSimOptionsAtPath(committed_path, opts), prelude_actions.len, crash_action);
    const after = try replayPersistentSimActionsAtPath(alloc, committed_path, persistentSimOptionsAtPath(committed_path, opts), &.{});

    try applyPersistentCrashActionAtPath(alloc, crash_path, persistentSimOptionsAtPath(crash_path, opts), prelude_actions.len, crash_action, phase);
    const actual = try replayPersistentSimActionsAtPath(alloc, crash_path, persistentSimOptionsAtPath(crash_path, opts), &.{});

    _ = case_label;
    return try classifyPersistentCrashSummary(before, after, actual, phase);
}

fn persistentSimOptionsAtPath(path: [*:0]const u8, opts: PersistentIndexOptions) PersistentIndexOptions {
    var copy = opts;
    copy.path = path;
    return copy;
}

fn persistTmpPathWithSuffix(buf: []u8, suffix: []const u8) [*:0]const u8 {
    const base = "/tmp/antfly-persist-test-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &persist_tmp_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}\x00", .{ base, ts, nonce, suffix }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn persistentReplayArtifactPath(buf: []u8, suffix: []const u8) []const u8 {
    const base = "/tmp/antfly-persistent-replay-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &persist_tmp_nonce, .Add, 1, .monotonic);
    return std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}.fixture", .{ base, ts, nonce, suffix }) catch unreachable;
}

fn writePersistentReplayArtifactFile(path: []const u8, contents: []const u8) !void {
    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
    defer file.close(std.testing.io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &file_buf);
    try writer.interface.writeAll(contents);
    try writer.end();
}

fn writePersistentReplayFixtureArtifact(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    summary: PersistentSimSummary,
    actions: []const PersistentSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = persistentReplayArtifactPath(&path_buf, case_label);
    const path = try alloc.dupe(u8, artifact_path);
    errdefer alloc.free(path);

    const normalized = try persistent_sim_fixture.renderReplayArtifact(
        alloc,
        blk: {
            var fixture_opts = fixtureOptionsFromPersistentOptions(opts);
            fixture_opts.expected_doc_count = summary.doc_count;
            fixture_opts.expected_segment_count = summary.segment_count;
            fixture_opts.expected_alpha_hits = summary.alpha_hits;
            fixture_opts.expected_beta_hits = summary.beta_hits;
            fixture_opts.expected_gamma_hits = summary.gamma_hits;
            break :blk fixture_opts;
        },
        case_label,
        seed,
        expectation_note,
        actions,
    );
    defer alloc.free(normalized);

    try writePersistentReplayArtifactFile(path, normalized);
    return path;
}

fn writePersistentCrashFixtureArtifact(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    phase: lmdb.CommitPublishPhase,
    expectation_note: []const u8,
    expected_outcome: PersistentCrashOutcome,
    prelude_actions: []const PersistentSimAction,
    crash_action: PersistentSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = persistentReplayArtifactPath(&path_buf, case_label);
    const path = try alloc.dupe(u8, artifact_path);
    errdefer alloc.free(path);

    const normalized = try persistent_sim_fixture.renderCrashArtifact(
        alloc,
        blk: {
            var fixture_opts = fixtureOptionsFromPersistentOptions(opts);
            fixture_opts.expected_outcome = expected_outcome;
            break :blk fixture_opts;
        },
        case_label,
        seed,
        @tagName(phase),
        expectation_note,
        prelude_actions,
        crash_action,
    );
    defer alloc.free(normalized);

    try writePersistentReplayArtifactFile(path, normalized);
    return path;
}

fn printPersistentAction(action: PersistentSimAction) !void {
    const line = try persistent_sim_fixture.renderAction(std.testing.allocator, action);
    defer std.testing.allocator.free(line);
    std.debug.print("    {s}\n", .{line});
}

fn reportReducedPersistentSchedule(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    actions: []const PersistentSimAction,
) !void {
    const Replayer = struct {
        alloc: Allocator,
        opts: PersistentIndexOptions,
        case_label: []const u8,

        pub fn replay(self: @This(), candidate: []const PersistentSimAction) !void {
            const expected = expectedPersistentSummary(candidate);
            var path_buf: [256]u8 = undefined;
            const tmp_path = persistTmpPathWithSuffix(&path_buf, "sim-reduce");
            defer cleanupPersistDir(tmp_path);
            const actual = try replayPersistentSimActionsAtPath(self.alloc, tmp_path, persistentSimOptionsAtPath(tmp_path, self.opts), candidate);
            try expectPersistentSummariesEqual(self.case_label, expected, actual);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(
        PersistentSimAction,
        alloc,
        actions,
        Replayer{ .alloc = alloc, .opts = opts, .case_label = case_label },
    );
    defer alloc.free(reduced);

    const summary = expectedPersistentSummary(reduced);
    const artifact_path = writePersistentReplayFixtureArtifact(
        alloc,
        opts,
        case_label,
        seed,
        "expected persistent index replay to preserve WAL-backed segments across reopen cycles",
        summary,
        reduced,
    ) catch |err| blk: {
        std.debug.print("failed to write persistent replay artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| alloc.free(path);

    std.debug.print("reduced failing persistent schedule ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
    for (reduced) |action| try printPersistentAction(action);
}

fn reportReducedPersistentCrashSchedule(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    phase: lmdb.CommitPublishPhase,
    prelude_actions: []const PersistentSimAction,
    crash_action: PersistentSimAction,
) !void {
    const Replayer = struct {
        alloc: Allocator,
        opts: PersistentIndexOptions,
        case_label: []const u8,
        phase: lmdb.CommitPublishPhase,
        crash_action: PersistentSimAction,

        pub fn replay(self: @This(), candidate: []const PersistentSimAction) !void {
            _ = try replayPersistentCrashWorkload(self.alloc, self.opts, self.case_label, candidate, self.crash_action, self.phase);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(
        PersistentSimAction,
        alloc,
        prelude_actions,
        Replayer{
            .alloc = alloc,
            .opts = opts,
            .case_label = case_label,
            .phase = phase,
            .crash_action = crash_action,
        },
    );
    defer alloc.free(reduced);

    var expected_path_buf: [256]u8 = undefined;
    const expected_path = persistTmpPathWithSuffix(&expected_path_buf, "sim-crash-expected");
    defer cleanupPersistDir(expected_path);

    _ = try replayPersistentSimActionsAtPath(alloc, expected_path, persistentSimOptionsAtPath(expected_path, opts), reduced);
    const before = try replayPersistentSimActionsAtPath(alloc, expected_path, persistentSimOptionsAtPath(expected_path, opts), &.{});
    try applyCommittedPersistentActionAtPath(alloc, expected_path, persistentSimOptionsAtPath(expected_path, opts), reduced.len, crash_action);
    const after = try replayPersistentSimActionsAtPath(alloc, expected_path, persistentSimOptionsAtPath(expected_path, opts), &.{});

    var actual_path_buf: [256]u8 = undefined;
    const actual_path = persistTmpPathWithSuffix(&actual_path_buf, "sim-crash-actual");
    defer cleanupPersistDir(actual_path);

    _ = try replayPersistentSimActionsAtPath(alloc, actual_path, persistentSimOptionsAtPath(actual_path, opts), reduced);
    try applyPersistentCrashActionAtPath(alloc, actual_path, persistentSimOptionsAtPath(actual_path, opts), reduced.len, crash_action, phase);
    const actual = try replayPersistentSimActionsAtPath(alloc, actual_path, persistentSimOptionsAtPath(actual_path, opts), &.{});
    const expected_outcome = try classifyPersistentCrashSummary(before, after, actual, phase);

    const artifact_path = writePersistentCrashFixtureArtifact(
        alloc,
        opts,
        case_label,
        seed,
        phase,
        "expected persistent reopen to recover the committed snapshot once the WAL append has completed",
        expected_outcome,
        reduced,
        crash_action,
    ) catch |err| blk: {
        std.debug.print("failed to write persistent crash artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| alloc.free(path);

    std.debug.print("reduced failing persistent crash prelude ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
}

fn randomPersistentAction(random: std.Random) PersistentSimAction {
    if (random.uintLessThan(u8, 5) == 0) return .reopen;
    const spec_index = random.uintLessThan(u8, 4);
    return .{ .index_segment = @enumFromInt(spec_index) };
}

fn runPersistentReplayCase(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var actions = std.ArrayListUnmanaged(PersistentSimAction).empty;
    defer actions.deinit(alloc);

    for (0..steps) |_| {
        try actions.append(alloc, randomPersistentAction(random));
    }

    var path_buf: [256]u8 = undefined;
    const path = persistTmpPathWithSuffix(&path_buf, "sim-replay");
    defer cleanupPersistDir(path);

    const actual = replayPersistentSimActionsAtPath(alloc, path, persistentSimOptionsAtPath(path, opts), actions.items) catch |err| {
        reportReducedPersistentSchedule(alloc, opts, case_label, seed, actions.items) catch {};
        return err;
    };
    try expectPersistentSummariesEqual(case_label, expectedPersistentSummary(actions.items), actual);
}

fn randomPersistentCrashAction(random: std.Random) PersistentSimAction {
    return .{ .index_segment = @enumFromInt(random.uintLessThan(u8, 4)) };
}

fn runPersistentCrashCase(
    alloc: Allocator,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    if (!zig_lmdb.is_zig_backend) return;

    const phases = [_]lmdb.CommitPublishPhase{
        .before_data_sync,
        .after_data_sync_before_meta,
        .after_meta_write_before_meta_sync,
        .fully_published,
    };

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var prelude = std.ArrayListUnmanaged(PersistentSimAction).empty;
    defer prelude.deinit(alloc);
    for (0..steps) |_| {
        try prelude.append(alloc, randomPersistentAction(random));
    }

    for (phases, 0..) |phase, phase_index| {
        const crash_action = randomPersistentCrashAction(random);
        _ = replayPersistentCrashWorkload(alloc, opts, case_label, prelude.items, crash_action, phase) catch |err| {
            reportReducedPersistentCrashSchedule(alloc, opts, case_label, seed + phase_index, phase, prelude.items, crash_action) catch {};
            return err;
        };
    }
}

fn replayPersistentFixtureFile(alloc: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "pkg/antfly/src/storage/persistent_sim_fixtures/{s}", .{name});
    defer alloc.free(path);

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(64 * 1024));
    defer alloc.free(contents);

    var fixture = try persistent_sim_fixture.parseFixture(alloc, contents);
    defer fixture.deinit(alloc);

    var path_buf: [256]u8 = undefined;
    const tmp_path = persistTmpPathWithSuffix(&path_buf, "fixture");
    defer cleanupPersistDir(tmp_path);

    var opts = persistentSimOptionsToIndexOptions(tmp_path, fixture.opts);
    if (fixture.mode == .crash) opts.main_backend = .lmdb;
    switch (fixture.mode) {
        .replay => {
            const summary = try replayPersistentSimActionsAtPath(alloc, tmp_path, opts, fixture.actions);
            try expectPersistentSummaryFields(fixture.case_label orelse fixture.label orelse name, fixture.opts, summary);
        },
        .crash => {
            if (!zig_lmdb.is_zig_backend) return;
            const outcome = try replayPersistentCrashWorkload(
                alloc,
                opts,
                fixture.case_label orelse fixture.label orelse name,
                fixture.prelude_actions,
                fixture.crash_action orelse return error.InvalidFixture,
                std.meta.stringToEnum(lmdb.CommitPublishPhase, fixture.phase orelse return error.InvalidFixture) orelse return error.InvalidFixture,
            );
            try expectPersistentCrashOutcome(fixture.case_label orelse fixture.label orelse name, fixture.opts, outcome);
        },
    }
}

fn replayModeledPersistentFixtureFile(alloc: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(alloc, "pkg/antfly/src/storage/persistent_sim_fixtures/{s}", .{name});
    defer alloc.free(path);

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, alloc, .limited(64 * 1024));
    defer alloc.free(contents);

    var fixture = try persistent_sim_fixture.parseFixture(alloc, contents);
    defer fixture.deinit(alloc);

    var runtime = storage_sim.Runtime.init(alloc);
    defer runtime.deinit();
    var device_model = storage_sim.ModeledDevice.init(alloc);
    defer device_model.deinit();

    const modeled_path: [*:0]const u8 = "/persistent-modeled-fixture";
    const opts = persistentModeledOptionsToIndexOptions(modeled_path, fixture.opts, &device_model, &runtime);
    switch (fixture.mode) {
        .replay => {
            const summary = try replayPersistentSimActionsAtPath(alloc, modeled_path, opts, fixture.actions);
            try expectPersistentSummaryFields(fixture.case_label orelse fixture.label orelse name, fixture.opts, summary);
        },
        .crash => {
            const outcome = try replayModeledPersistentCrashFixture(
                alloc,
                modeled_path,
                opts,
                fixture.case_label orelse fixture.label orelse name,
                fixture.prelude_actions,
                fixture.crash_action orelse return error.InvalidFixture,
                &device_model,
            );
            try expectPersistentCrashOutcome(fixture.case_label orelse fixture.label orelse name, fixture.opts, outcome);
        },
    }
}

fn replayModeledPersistentCrashFixture(
    alloc: Allocator,
    path: [*:0]const u8,
    opts: PersistentIndexOptions,
    case_label: []const u8,
    prelude_actions: []const PersistentSimAction,
    crash_action: PersistentSimAction,
    device_model: *storage_sim.ModeledDevice,
) !PersistentCrashOutcome {
    _ = try replayPersistentSimActionsAtPath(alloc, path, opts, prelude_actions);
    try applyCommittedPersistentActionAtPath(alloc, path, opts, prelude_actions.len, crash_action);
    try device_model.device().crash();

    const actual = try replayPersistentSimActionsAtPath(alloc, path, opts, &.{});
    const full_actions = try alloc.alloc(PersistentSimAction, prelude_actions.len + 1);
    defer alloc.free(full_actions);
    @memcpy(full_actions[0..prelude_actions.len], prelude_actions);
    full_actions[prelude_actions.len] = crash_action;
    try expectPersistentSummariesEqual(case_label, expectedPersistentSummary(full_actions), actual);
    return .committed;
}

fn runPersistentReplayFixtures(alloc: Allocator) !void {
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly/src/storage/persistent_sim_fixtures", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer fixtures_dir.close(std.testing.io);

    var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (fixture_names.items) |name| alloc.free(name);
        fixture_names.deinit(alloc);
    }

    var walker = try fixtures_dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_names.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_names.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (fixture_names.items) |name| try replayPersistentFixtureFile(alloc, name);
}

fn runModeledPersistentFixtures(alloc: Allocator) !void {
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly/src/storage/persistent_sim_fixtures", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer fixtures_dir.close(std.testing.io);

    var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (fixture_names.items) |name| alloc.free(name);
        fixture_names.deinit(alloc);
    }

    var walker = try fixtures_dir.walk(alloc);
    defer walker.deinit();

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_names.append(alloc, try alloc.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_names.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (fixture_names.items) |name| try replayModeledPersistentFixtureFile(alloc, name);
}

fn runModeledPersistentReplayCase(
    alloc: Allocator,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    var runtime = storage_sim.Runtime.init(alloc);
    defer runtime.deinit();
    var device_model = storage_sim.ModeledDevice.init(alloc);
    defer device_model.deinit();

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();

    var actions = std.ArrayListUnmanaged(PersistentSimAction).empty;
    defer actions.deinit(alloc);
    for (0..steps) |_| {
        try actions.append(alloc, randomPersistentAction(random));
    }

    const path: [*:0]const u8 = "/persistent-modeled-sim";
    const opts = persistentModeledOptionsToIndexOptions(path, .{
        .main_commit_backend = .async_io,
        .wal_commit_backend = .worker_thread,
    }, &device_model, &runtime);
    const actual = try replayPersistentSimActionsAtPath(alloc, path, opts, actions.items);
    try device_model.device().crash();
    const reopened = try replayPersistentSimActionsAtPath(alloc, path, opts, &.{});
    try expectPersistentSummariesEqual(case_label, actual, reopened);
    try expectPersistentSummariesEqual(case_label, expectedPersistentSummary(actions.items), reopened);
}

fn crashReopenModeledPersistentIndex(
    alloc: Allocator,
    pi: *PersistentIndex,
    device_model: *storage_sim.ModeledDevice,
    opts: PersistentIndexOptions,
) !void {
    pi.close();
    try device_model.device().crash();
    pi.* = try PersistentIndex.open(alloc, opts);
}

fn buildCompactedTextSegment(alloc: Allocator) ![]u8 {
    return try buildMultiDocSegment(alloc, &.{
        .{ .doc_id = "doc:a", .term = "fresh" },
        .{ .doc_id = "doc:b", .term = "beta" },
    });
}

fn seedTextCompactionInputs(pi: *PersistentIndex, alloc: Allocator) !void {
    const stale = try buildSimpleSegment(alloc, "doc:a", "stale");
    defer alloc.free(stale);
    try pi.indexSegment(stale);

    const beta = try buildSimpleSegment(alloc, "doc:b", "beta");
    defer alloc.free(beta);
    try pi.indexSegment(beta);
}

fn expectPersistentTextCompactionView(
    alloc: Allocator,
    pi: *PersistentIndex,
    expected_segments: usize,
    stale_hits: usize,
    fresh_hits: usize,
    beta_hits: usize,
) !void {
    const ranges = try pi.activeSegmentRanges(alloc);
    defer {
        for (ranges) |*range| range.deinit(alloc);
        alloc.free(ranges);
    }

    const snap = pi.snapshot();
    try std.testing.expectEqual(@as(u32, 2), snap.global_doc_count);
    try std.testing.expectEqual(expected_segments, ranges.len);
    try std.testing.expectEqual(stale_hits, try persistentSearchHitCount(alloc, snap, "stale"));
    try std.testing.expectEqual(fresh_hits, try persistentSearchHitCount(alloc, snap, "fresh"));
    try std.testing.expectEqual(beta_hits, try persistentSearchHitCount(alloc, snap, "beta"));
}

fn expectModeledPersistentReplaceError(result: anyerror!void) !void {
    result catch |err| switch (err) {
        error.InjectedWriteFault, error.InjectedSyncFault => return,
        else => return err,
    };
    return error.TestExpectedError;
}

test "persistent modeled full-text compaction publish faults stay green" {
    const alloc = std.testing.allocator;

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-segment-write-fault";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 2, 1, 0, 1);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        try device_model.injectWriteFailureForPathContains(".seg");
        try expectModeledPersistentReplaceError(pi.replaceSegments(&.{ 1, 2 }, compacted));
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 2, 1, 0, 1);
    }

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-catalog-sync-fault";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        try device_model.injectSyncFailureForPathContains("/index/");
        try expectModeledPersistentReplaceError(pi.replaceSegments(&.{ 1, 2 }, compacted));
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 2, 1, 0, 1);
    }

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-after-catalog-publish";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        try pi.replaceSegmentsCatalogOnlyForTest(&.{ 1, 2 }, compacted);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 1, 0, 1, 1);
    }

    {
        var runtime = storage_sim.Runtime.init(alloc);
        defer runtime.deinit();
        var device_model = storage_sim.ModeledDevice.init(alloc);
        defer device_model.deinit();

        const path: [*:0]const u8 = "/persistent-text-compaction-cleanup-fault";
        const opts = persistentModeledOptionsToIndexOptions(path, .{}, &device_model, &runtime);
        var pi = try PersistentIndex.open(alloc, opts);
        defer pi.close();

        try seedTextCompactionInputs(&pi, alloc);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);

        const compacted = try buildCompactedTextSegment(alloc);
        defer alloc.free(compacted);
        try device_model.injectDeleteFailureForPathContains("1.seg");
        try pi.replaceSegments(&.{ 1, 2 }, compacted);
        try crashReopenModeledPersistentIndex(alloc, &pi, &device_model, opts);
        try expectPersistentTextCompactionView(alloc, &pi, 1, 0, 1, 1);
    }
}

fn runPersistentSoak(alloc: Allocator) !void {
    try runPersistentReplayCase(alloc, .{ .path = "" }, "persistent-soak-default-a", 0xA17F_B201, 72);
    try runPersistentReplayCase(alloc, .{ .path = "" }, "persistent-soak-default-b", 0xA17F_B202, 72);
    try runPersistentReplayCase(
        alloc,
        .{ .path = "", .main_commit_backend = .async_io, .wal_commit_backend = .worker_thread },
        "persistent-soak-async-main",
        0xA17F_B203,
        60,
    );
    try runPersistentCrashCase(alloc, .{ .path = "", .main_backend = .lmdb }, "persistent-soak-crash-default", 0xA17F_B204, 16);
    try runPersistentCrashCase(
        alloc,
        .{ .path = "", .main_backend = .lmdb, .main_commit_backend = .async_io },
        "persistent-soak-crash-async-main",
        0xA17F_B205,
        16,
    );
}

test "persistent sim workloads stay green" {
    const alloc = std.testing.allocator;
    try runPersistentReplayCase(alloc, .{ .path = "" }, "persistent-default", 0xA17F_B001, 16);
    try runPersistentReplayCase(alloc, .{ .path = "", .main_commit_backend = .async_io, .wal_commit_backend = .worker_thread }, "persistent-async-main", 0xA17F_B002, 14);
    try runPersistentCrashCase(alloc, .{ .path = "", .main_backend = .lmdb }, "persistent-crash-default", 0xA17F_B101, 6);
    try runPersistentCrashCase(alloc, .{ .path = "", .main_backend = .lmdb, .main_commit_backend = .async_io }, "persistent-crash-async-main", 0xA17F_B102, 6);
}

test "persistent replay fixtures stay green" {
    try runPersistentReplayFixtures(std.testing.allocator);
}

test "persistent modeled replay fixtures stay green" {
    try runModeledPersistentFixtures(std.testing.allocator);
}

test "persistent modeled sim workload stays green" {
    try runModeledPersistentReplayCase(std.testing.allocator, "persistent-modeled-sim", 0xA17F_B501, 18);
}

test "persistent sim soak stays green" {
    if (!storage_sim_soak) return;
    try runPersistentSoak(std.testing.allocator);
}
