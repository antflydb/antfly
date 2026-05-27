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

//! Generic write-ahead log (WAL) over ordered durable storage.
//!
//! A reusable, append-only log with LSN ordering, CRC32 integrity,
//! truncation, and replay iteration. Knows nothing about search — just
//! appends opaque byte entries. Usable for search persistence, Raft
//! consensus log, KV storage, or any ordered-write-ahead pattern.
//!
//! Key encoding: LSN as u64 big-endian (LMDB sorts lexicographically).
//! Value format: [data_len: u32 LE][data: ...][CRC32: u32 LE]
//! CRC covers data_len + data bytes.

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const fs_paths = @import("../common/fs_paths.zig");
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const lsm_backend = @import("lsm_backend/mod.zig");
const lsm_storage = @import("lsm_backend/storage_io.zig");
const lmdb_backend = @import("lmdb_backend.zig");
const platform_time = @import("../platform/time.zig");
const lmdb = @import("lmdb.zig");
const storage_sim = @import("sim_runtime.zig");
const sim_fixture = @import("sim_fixture.zig");
const wal_sim_fixture = @import("wal_sim_fixture.zig");
const zig_lmdb = @import("lmdb_engine");
const large_entry_threshold = 4096;
const storage_sim_soak = zig_lmdb.storage_sim_soak;
var wal_tmp_nonce: u64 = 0;

fn nextWalTmpNonce() u64 {
    return @atomicRmw(u64, &wal_tmp_nonce, .Add, 1, .seq_cst);
}

pub const CommitStats = lmdb.CommitStats;
pub const CommitBackend = lmdb.CommitBackend;
pub const StorageBackend = enum {
    lmdb,
    lsm,
    lsm_memory,
};

pub const WalOptions = struct {
    map_size: usize = 64 * 1024 * 1024, // 64MB default
    no_sync: bool = false,
    artificial_sync_delay_ns: u64 = 0,
    group_commit_window_ns: u64 = 0,
    group_commit_max_requests: usize = 64,
    commit_backend: CommitBackend = .adaptive,
    backend: ?StorageBackend = null,
    read_only: bool = false,
    storage: ?lsm_storage.Storage = null,
    lsm_options: lsm_backend.Options = .{},
    clock: storage_sim.Clock = storage_sim.real_clock,
    commit_scheduler: storage_sim.CompletionScheduler = storage_sim.real_completion_scheduler,
    model_commit_backend_completions: bool = false,

    pub fn resolvedBackend(self: WalOptions) StorageBackend {
        return self.backend orelse .lsm;
    }
};

pub const WalEntry = struct {
    lsn: u64,
    data: []const u8,
};

pub const BatchAppendResult = struct {
    first_lsn: u64,
    count: usize,

    pub fn isEmpty(self: BatchAppendResult) bool {
        return self.count == 0;
    }

    pub fn lsnAt(self: BatchAppendResult, index: usize) u64 {
        std.debug.assert(index < self.count);
        return self.first_lsn + index;
    }

    pub fn lastLsn(self: BatchAppendResult) ?u64 {
        if (self.count == 0) return null;
        return self.first_lsn + self.count - 1;
    }
};

pub const WalStats = struct {
    append_calls: u64 = 0,
    append_batch_calls: u64 = 0,
    logical_entries: u64 = 0,
    physical_commits: u64 = 0,
    grouped_commits: u64 = 0,
    grouped_requests: u64 = 0,
    max_requests_per_commit: u64 = 0,
    max_entries_per_commit: u64 = 0,
    total_wait_ns: u64 = 0,
    total_coalesce_ns: u64 = 0,
    total_txn_open_ns: u64 = 0,
    total_put_ns: u64 = 0,
    total_commit_ns: u64 = 0,
};

pub const FullStats = struct {
    wal: WalStats,
    commit: ?lmdb.CommitStats,
};

const CommitBatch = struct {
    head: ?*AppendRequest = null,
    tail: ?*AppendRequest = null,
    request_count: usize = 0,
    entry_count: usize = 0,
};

const CommitRunStats = struct {
    request_count: usize = 0,
    entry_count: usize = 0,
    txn_open_ns: u64 = 0,
    put_ns: u64 = 0,
    commit_ns: u64 = 0,
};

const CommitBatchResult = union(enum) {
    success: BatchAppendResult,
    failure: anyerror,
};

const CommitAttempt = struct {
    result: CommitBatchResult,
    stats: CommitRunStats,
};

const AppendRequest = struct {
    next: ?*AppendRequest = null,
    entries: []const []const u8,
    result: BatchAppendResult = .{ .first_lsn = 0, .count = 0 },
    err: ?anyerror = null,
    done: bool = false,
};

const default_namespace: backend_types.Namespace = .{};

const StoreOwner = union(enum) {
    lmdb: *lmdb_backend.Backend,
    lsm: lsm_backend.BackendHandle,

    fn close(self: *StoreOwner, alloc: Allocator) void {
        switch (self.*) {
            .lmdb => |backend| {
                backend.close();
                alloc.destroy(backend);
            },
            .lsm => |*handle| handle.close(),
        }
        self.* = undefined;
    }

    fn sync(self: *StoreOwner, force: bool) !void {
        switch (self.*) {
            .lmdb => |backend| try backend.sync(force),
            .lsm => |*handle| try handle.backend.sync(force),
        }
    }

    fn commitStatsSnapshot(self: *StoreOwner) ?lmdb.CommitStats {
        return switch (self.*) {
            .lmdb => |backend| backend.commitStatsSnapshot(),
            .lsm => null,
        };
    }

    fn runtimeNamespaceStore(self: StoreOwner, allocator: Allocator) !backend_erased.NamespaceStore {
        return switch (self) {
            .lmdb => |backend| try backend.runtimeNamespaceStore(allocator),
            .lsm => |handle| try handle.backend.runtimeNamespaceStore(allocator),
        };
    }

    fn lmdbEnv(self: *StoreOwner) ?*lmdb.Environment {
        return switch (self.*) {
            .lmdb => |backend| &backend.env,
            .lsm => null,
        };
    }
};

pub const WAL = struct {
    store: backend_erased.NamespaceStore,
    store_owner: StoreOwner,
    next_lsn: u64,
    group_commit_window_ns: u64,
    group_commit_max_requests: usize,
    commit_completion_delay_ns: u64,
    sync_after_commit: bool,
    clock: storage_sim.Clock,
    commit_scheduler: storage_sim.CompletionScheduler,
    mutex: std.atomic.Mutex = .unlocked,
    coordinator_active: bool = false,
    pending_head: ?*AppendRequest = null,
    pending_tail: ?*AppendRequest = null,
    stats: WalStats = .{},

    pub const BackendStore = backend_adapter.Store(WAL, Txn, Txn, Txn, .{
        .capabilities = backendCapabilities,
        .begin_read = beginReadTxn,
        .begin_write = beginWriteTxn,
        .begin_batch = beginWriteTxn,
    });

    const Txn = struct {
        inner: union(enum) {
            read: backend_erased.NamespaceReadTxn,
            write: backend_erased.NamespaceWriteTxn,
        },

        const CursorAdapter = backend_erased.Cursor;
        const ReadAdapter = backend_adapter.ReadTxn(Txn, CursorAdapter, .{
            .abort = abort,
            .get = get,
            .open_cursor = openCursorAdapter,
        });
        const WriteAdapter = backend_adapter.WriteTxn(Txn, CursorAdapter, .{
            .abort = abort,
            .commit = commit,
            .get = get,
            .put = put,
            .delete = delete,
            .open_cursor = openCursorAdapter,
        });

        fn abort(self: *Txn) void {
            switch (self.inner) {
                .read => |*txn| txn.abort(),
                .write => |*txn| txn.abort(),
            }
            self.* = undefined;
        }

        fn commit(self: *Txn) !void {
            switch (self.inner) {
                .read => return error.ReadOnlyTransaction,
                .write => |*txn| try txn.commit(),
            }
            self.* = undefined;
        }

        fn get(self: *Txn, key: []const u8) ![]const u8 {
            return switch (self.inner) {
                .read => |*txn| try txn.get(default_namespace, key),
                .write => |*txn| try txn.get(default_namespace, key),
            };
        }

        fn put(self: *Txn, key: []const u8, value: []const u8) !void {
            switch (self.inner) {
                .read => return error.ReadOnlyTransaction,
                .write => |*txn| try txn.put(default_namespace, key, value),
            }
        }

        fn appendPut(self: *Txn, key: []const u8, value: []const u8) !void {
            switch (self.inner) {
                .read => return error.ReadOnlyTransaction,
                .write => |*txn| try txn.appendPut(default_namespace, key, value),
            }
        }

        fn delete(self: *Txn, key: []const u8) !void {
            switch (self.inner) {
                .read => return error.ReadOnlyTransaction,
                .write => |*txn| try txn.delete(default_namespace, key),
            }
        }

        fn cursor(self: *Txn) !CursorAdapter {
            return try self.openCursorAdapter();
        }

        fn openCursorAdapter(self: *Txn) !CursorAdapter {
            return switch (self.inner) {
                .read => |*txn| try txn.openCursor(default_namespace),
                .write => |*txn| try txn.openCursor(default_namespace),
            };
        }

        fn readAdapter(self: *Txn) ReadAdapter {
            return ReadAdapter.init(self);
        }

        fn writeAdapter(self: *Txn) WriteAdapter {
            return WriteAdapter.init(self);
        }
    };

    fn beginReadTxn(self: *WAL) !Txn {
        return .{
            .inner = .{
                .read = try self.store.beginRead(),
            },
        };
    }

    fn beginWriteTxn(self: *WAL) !Txn {
        return .{
            .inner = .{
                .write = try self.store.beginWrite(),
            },
        };
    }

    fn beginLmdbFixtureTxn(self: *WAL) !FixtureTxn {
        const env = self.store_owner.lmdbEnv() orelse return error.Unsupported;

        var raw = try env.begin(.{});
        errdefer raw.abort();
        const dbi = try raw.openDb(null, .{ .create = true });
        return .{
            .raw = raw,
            .dbi = dbi,
        };
    }

    /// Open or create a WAL at the given path.
    pub fn open(path: [*:0]const u8, opts: WalOptions) !WAL {
        var store_owner = try openStoreOwner(std.heap.page_allocator, path, opts);
        errdefer store_owner.close(std.heap.page_allocator);

        var store = try store_owner.runtimeNamespaceStore(std.heap.page_allocator);
        errdefer store.deinit();

        // Scan for the highest existing LSN
        var read_txn = Txn{
            .inner = .{
                .read = try store.beginRead(),
            },
        };
        defer read_txn.abort();

        var next_lsn: u64 = 1;
        if (try readNextLsnMeta(&read_txn)) |stored_next_lsn| {
            next_lsn = stored_next_lsn;
        }
        var cur = try read_txn.cursor();
        defer cur.close();

        const last = try cur.last();

        if (last) |entry| {
            if (entry.key.len == 8) {
                const lsn = std.mem.readInt(u64, entry.key[0..8], .big);
                next_lsn = @max(next_lsn, lsn + 1);
            }
        }

        return .{
            .store = store,
            .store_owner = store_owner,
            .next_lsn = next_lsn,
            .group_commit_window_ns = opts.group_commit_window_ns,
            .group_commit_max_requests = @max(@as(usize, 1), opts.group_commit_max_requests),
            .commit_completion_delay_ns = resolvedCommitCompletionDelayNs(opts),
            .sync_after_commit = opts.resolvedBackend() != .lmdb and !opts.no_sync,
            .clock = opts.clock,
            .commit_scheduler = opts.commit_scheduler,
        };
    }

    pub fn close(self: *WAL) void {
        self.store.deinit();
        self.store_owner.close(std.heap.page_allocator);
        self.* = undefined;
    }

    fn backendCapabilities(_: *WAL) backend_types.Capabilities {
        return .{
            .ordered_ranges = true,
            .reverse_ranges = true,
            .cursors = true,
            .native_namespaces = false,
            .write_batches = .atomic,
            .single_writer = true,
            .read_snapshots = .snapshot,
        };
    }

    pub fn backendStore(self: *WAL) BackendStore {
        return BackendStore.init(self);
    }

    /// Append opaque data. Returns assigned LSN. Durable after return.
    pub fn append(self: *WAL, data: []const u8) !u64 {
        const result = try self.appendBatch(&.{data});
        return result.first_lsn;
    }

    /// Append multiple entries in one durable transaction. Returns the assigned
    /// contiguous LSN range.
    pub fn appendBatch(self: *WAL, entries: []const []const u8) !BatchAppendResult {
        if (entries.len == 0) {
            return .{ .first_lsn = self.next_lsn, .count = 0 };
        }

        const wait_started = self.nowNs();
        var request = AppendRequest{ .entries = entries };

        lockAtomic(&self.mutex);

        if (entries.len == 1) {
            self.stats.append_calls += 1;
        } else {
            self.stats.append_batch_calls += 1;
        }
        self.stats.logical_entries += entries.len;

        self.enqueueAppendRequestLocked(&request);
        const leader = if (!self.coordinator_active) blk: {
            self.coordinator_active = true;
            break :blk true;
        } else false;
        self.mutex.unlock();

        if (leader) {
            self.driveAppendCoordinator();
        }

        lockAtomic(&self.mutex);
        defer self.mutex.unlock();

        while (!request.done) {
            self.mutex.unlock();
            std.Thread.yield() catch {};
            lockAtomic(&self.mutex);
        }

        self.stats.total_wait_ns += self.elapsedSince(wait_started);
        if (request.err) |err| return err;
        return request.result;
    }

    pub fn statsSnapshot(self: *WAL) WalStats {
        lockAtomic(&self.mutex);
        defer self.mutex.unlock();
        return self.stats;
    }

    pub fn commitStatsSnapshot(self: *WAL) ?lmdb.CommitStats {
        return self.store_owner.commitStatsSnapshot();
    }

    pub fn fullStatsSnapshot(self: *WAL) FullStats {
        return .{
            .wal = self.statsSnapshot(),
            .commit = self.commitStatsSnapshot(),
        };
    }

    pub fn sync(self: *WAL, force: bool) !void {
        try self.store_owner.sync(force);
    }

    /// Truncate all entries with LSN <= the given value.
    pub fn truncate(self: *WAL, up_to_lsn: u64) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();

        var to_delete = std.ArrayListUnmanaged([8]u8).empty;
        defer to_delete.deinit(std.heap.page_allocator);

        {
            var cur = try txn.cursor();
            defer cur.close();

            var maybe_entry = try cur.first();
            if (maybe_entry == null) {
                try txn.commit();
                return;
            }

            while (maybe_entry) |entry| {
                if (entry.key.len != 8) {
                    maybe_entry = try cur.next();
                    continue;
                }
                const lsn = std.mem.readInt(u64, entry.key[0..8], .big);
                if (lsn > up_to_lsn) break;

                var key: [8]u8 = undefined;
                @memcpy(&key, entry.key[0..8]);
                try to_delete.append(std.heap.page_allocator, key);

                maybe_entry = try cur.next();
            }
        }

        for (to_delete.items) |key| {
            txn.delete(&key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
        }

        try txn.commit();
    }

    /// Iterate entries from a given LSN (inclusive). Caller owns returned slice.
    pub fn iterateFrom(self: *WAL, alloc: Allocator, from_lsn: u64) ![]WalEntry {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var entries = std.ArrayListUnmanaged(WalEntry).empty;
        defer entries.deinit(alloc);

        var cur = try txn.cursor();
        defer cur.close();

        const start_key = std.mem.toBytes(std.mem.nativeToBig(u64, from_lsn));
        var maybe_entry = try cur.seekAtOrAfter(&start_key);
        if (maybe_entry == null) return try alloc.dupe(WalEntry, entries.items);

        while (maybe_entry) |entry| {
            if (entry.key.len != 8) break;
            const lsn = std.mem.readInt(u64, entry.key[0..8], .big);
            const decoded = decodeWalValue(entry.value);
            if (decoded) |data| {
                try entries.append(alloc, .{
                    .lsn = lsn,
                    .data = try alloc.dupe(u8, data),
                });
            } else |err| switch (err) {
                error.CorruptWal => {},
            }

            maybe_entry = try cur.next();
        }

        return try alloc.dupe(WalEntry, entries.items);
    }

    /// Import ScanAction from docstore for consistent API.
    pub const ScanAction = @import("docstore.zig").DocStore.ScanAction;

    /// Streaming callback-based iteration from a given LSN. Constant memory.
    /// Callback receives entries with data pointing into LMDB mmap (valid only during call).
    pub fn iterateFromStreaming(
        self: *WAL,
        from_lsn: u64,
        callback: *const fn (entry: WalEntry) anyerror!ScanAction,
    ) !void {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.cursor();
        defer cur.close();

        const start_key = std.mem.toBytes(std.mem.nativeToBig(u64, from_lsn));
        var maybe_entry = try cur.seekAtOrAfter(&start_key);
        if (maybe_entry == null) return;

        while (maybe_entry) |entry| {
            if (entry.key.len != 8) return;
            const lsn = std.mem.readInt(u64, entry.key[0..8], .big);
            const decoded = decodeWalValue(entry.value);
            if (decoded) |data| {
                const action = try callback(.{ .lsn = lsn, .data = data });
                if (action == .stop) return;
            } else |err| switch (err) {
                error.CorruptWal => {},
            }

            maybe_entry = try cur.next();
        }
    }

    pub fn iterateFromStreamingWithContext(
        self: *WAL,
        from_lsn: u64,
        context: anytype,
        comptime callback: fn (@TypeOf(context), WalEntry) anyerror!ScanAction,
    ) !void {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.cursor();
        defer cur.close();

        const start_key = std.mem.toBytes(std.mem.nativeToBig(u64, from_lsn));
        var maybe_entry = try cur.seekAtOrAfter(&start_key);
        if (maybe_entry == null) return;

        while (maybe_entry) |entry| {
            if (entry.key.len != 8) return;
            const lsn = std.mem.readInt(u64, entry.key[0..8], .big);
            const decoded = decodeWalValue(entry.value);
            if (decoded) |data| {
                const action = try callback(context, .{ .lsn = lsn, .data = data });
                if (action == .stop) return;
            } else |err| switch (err) {
                error.CorruptWal => {},
            }

            maybe_entry = try cur.next();
        }
    }

    /// Current highest LSN (0 if empty).
    pub fn lastLsn(self: *const WAL) u64 {
        if (self.next_lsn <= 1) return 0;
        return self.next_lsn - 1;
    }

    fn enqueueAppendRequestLocked(self: *WAL, request: *AppendRequest) void {
        request.next = null;
        if (self.pending_tail) |tail| {
            tail.next = request;
        } else {
            self.pending_head = request;
        }
        self.pending_tail = request;
    }

    fn drainPendingRequestsLocked(self: *WAL) CommitBatch {
        var batch = CommitBatch{};
        while (self.pending_head) |request| {
            self.pending_head = request.next;
            if (self.pending_head == null) self.pending_tail = null;

            request.next = null;
            if (batch.tail) |tail| {
                tail.next = request;
            } else {
                batch.head = request;
            }
            batch.tail = request;
            batch.request_count += 1;
            batch.entry_count += request.entries.len;

            if (batch.request_count >= self.group_commit_max_requests) break;
        }
        return batch;
    }

    fn driveAppendCoordinator(self: *WAL) void {
        while (true) {
            var coalesce_ns: u64 = 0;
            lockAtomic(&self.mutex);
            const should_wait = self.shouldCoalesceWaitLocked();
            const wait_ns = if (should_wait) self.effectiveCoalesceWindowNsLocked() else 0;
            self.mutex.unlock();

            if (wait_ns > 0) {
                const coalesce_started = self.nowNs();
                self.sleepNs(wait_ns);
                coalesce_ns = self.elapsedSince(coalesce_started);
            }

            lockAtomic(&self.mutex);
            const batch = self.drainPendingRequestsLocked();
            self.mutex.unlock();

            if (batch.head == null) {
                lockAtomic(&self.mutex);
                self.coordinator_active = false;
                self.mutex.unlock();
                return;
            }

            const attempt = self.commitAppendBatch(batch);

            lockAtomic(&self.mutex);
            self.recordCommitStatsLocked(batch, attempt.stats, coalesce_ns);

            var current = batch.head;
            while (current) |request| {
                current = request.next;
                request.done = true;
                switch (attempt.result) {
                    .success => |success| {
                        request.result = .{
                            .first_lsn = success.first_lsn + requestOffsetInBatch(batch.head, request),
                            .count = request.entries.len,
                        };
                        request.err = null;
                    },
                    .failure => |err| {
                        request.err = err;
                    },
                }
            }

            if (self.pending_head == null) {
                self.coordinator_active = false;
            }
            const should_continue = self.coordinator_active;
            self.mutex.unlock();

            if (!should_continue) return;
        }
    }

    fn shouldCoalesceWaitLocked(self: *WAL) bool {
        if (self.group_commit_window_ns == 0) return false;
        const head = self.pending_head orelse return false;
        return head.next == null;
    }

    fn effectiveCoalesceWindowNsLocked(self: *WAL) u64 {
        if (self.group_commit_window_ns == 0) return 0;
        const avg_commit_ns = if (self.stats.physical_commits == 0)
            0
        else
            self.stats.total_commit_ns / self.stats.physical_commits;
        const baseline_ns = if (avg_commit_ns > 0)
            avgCommitWindowNs(avg_commit_ns)
        else
            initialCommitWindowNs(self.group_commit_window_ns);
        return @min(self.group_commit_window_ns, baseline_ns);
    }

    fn commitAppendBatch(self: *WAL, batch: CommitBatch) CommitAttempt {
        var stats = CommitRunStats{
            .request_count = batch.request_count,
            .entry_count = batch.entry_count,
        };
        const first_lsn = self.next_lsn;
        var lsn = first_lsn;

        const txn_open_started = self.nowNs();
        var txn = self.beginWriteTxn() catch |err| {
            stats.txn_open_ns = self.elapsedSince(txn_open_started);
            return .{
                .result = .{ .failure = err },
                .stats = stats,
            };
        };
        stats.txn_open_ns = self.elapsedSince(txn_open_started);
        errdefer txn.abort();

        var request = batch.head;
        while (request) |current| : (request = current.next) {
            for (current.entries) |data| {
                const put_started = self.nowNs();
                putEncodedEntry(&txn, lsn, data) catch |err| {
                    txn.abort();
                    stats.put_ns += self.elapsedSince(put_started);
                    return .{
                        .result = .{ .failure = err },
                        .stats = stats,
                    };
                };
                stats.put_ns += self.elapsedSince(put_started);
                lsn += 1;
            }
        }

        putNextLsnMeta(&txn, lsn) catch |err| {
            txn.abort();
            return .{
                .result = .{ .failure = err },
                .stats = stats,
            };
        };

        const commit_started = self.nowNs();
        txn.commit() catch |err| {
            stats.commit_ns = self.elapsedSince(commit_started);
            return .{
                .result = .{ .failure = err },
                .stats = stats,
            };
        };
        self.next_lsn = lsn;
        if (self.sync_after_commit) {
            self.store_owner.sync(true) catch |err| {
                stats.commit_ns = self.elapsedSince(commit_started);
                return .{
                    .result = .{ .failure = err },
                    .stats = stats,
                };
            };
        }
        self.waitForCommitCompletion(self.commit_completion_delay_ns) catch |err| {
            stats.commit_ns = self.elapsedSince(commit_started);
            return .{
                .result = .{ .failure = err },
                .stats = stats,
            };
        };
        stats.commit_ns = self.elapsedSince(commit_started);

        return .{
            .result = .{
                .success = .{
                    .first_lsn = first_lsn,
                    .count = stats.entry_count,
                },
            },
            .stats = stats,
        };
    }

    fn recordCommitStatsLocked(self: *WAL, batch: CommitBatch, stats: CommitRunStats, coalesce_ns: u64) void {
        _ = batch;
        self.stats.total_coalesce_ns += coalesce_ns;
        self.stats.physical_commits += 1;
        if (stats.request_count > 1) {
            self.stats.grouped_commits += 1;
            self.stats.grouped_requests += stats.request_count;
        }
        self.stats.max_requests_per_commit = @max(self.stats.max_requests_per_commit, stats.request_count);
        self.stats.max_entries_per_commit = @max(self.stats.max_entries_per_commit, stats.entry_count);
        self.stats.total_txn_open_ns += stats.txn_open_ns;
        self.stats.total_put_ns += stats.put_ns;
        self.stats.total_commit_ns += stats.commit_ns;
    }

    fn nowNs(self: *const WAL) u64 {
        return self.clock.nowNs();
    }

    fn elapsedSince(self: *const WAL, started: u64) u64 {
        return self.nowNs() - started;
    }

    fn sleepNs(self: *const WAL, ns: u64) void {
        self.clock.sleepNs(ns);
    }

    fn waitForCommitCompletion(self: *const WAL, ns: u64) !void {
        try self.commit_scheduler.waitNs(ns);
    }
};

fn openStoreOwner(alloc: Allocator, path: [*:0]const u8, opts: WalOptions) !StoreOwner {
    return switch (opts.resolvedBackend()) {
        .lmdb => blk: {
            var io_impl = std.Io.Threaded.init(alloc, .{});
            defer io_impl.deinit();
            try fs_paths.createDirPathPortable(io_impl.io(), std.mem.span(path));

            const backend = try alloc.create(lmdb_backend.Backend);
            errdefer alloc.destroy(backend);
            backend.* = try lmdb_backend.Backend.open(alloc, path, .{
                .backend = .{
                    .durability = if (opts.no_sync) .none else .full,
                },
                .env = .{
                    .max_dbs = 1,
                    .map_size = opts.map_size,
                    .no_sync = opts.no_sync,
                    .artificial_sync_delay_ns = opts.artificial_sync_delay_ns,
                    .commit_backend = opts.commit_backend,
                },
            });
            errdefer backend.close();
            break :blk .{ .lmdb = backend };
        },
        .lsm => blk: {
            if (opts.storage == null) {
                var io_impl = std.Io.Threaded.init(alloc, .{});
                defer io_impl.deinit();
                try fs_paths.createDirPathPortable(io_impl.io(), std.mem.span(path));
            }

            var lsm_options = opts.lsm_options;
            lsm_options.backend.read_only = opts.read_only;
            if (opts.read_only) lsm_options.backend.create_if_missing = false;
            lsm_options.backend.durability = if (opts.no_sync) .none else lsm_options.backend.durability;
            lsm_options.storage = opts.storage orelse lsm_options.storage;
            var handle = try lsm_backend.BackendHandle.open(alloc, std.mem.span(path), lsm_options);
            errdefer handle.close();
            break :blk .{ .lsm = handle };
        },
        .lsm_memory => blk: {
            var lsm_options = opts.lsm_options;
            lsm_options.backend.durability = .none;
            lsm_options.storage = opts.storage orelse lsm_options.storage;
            var handle = try lsm_backend.BackendHandle.init(alloc, lsm_options);
            errdefer handle.close();
            break :blk .{ .lsm = handle };
        },
    };
}

fn resolvedCommitCompletionDelayNs(opts: WalOptions) u64 {
    if (opts.resolvedBackend() == .lmdb) return 0;
    if (opts.artificial_sync_delay_ns > 0) return opts.artificial_sync_delay_ns;
    if (!opts.model_commit_backend_completions) return 0;
    return modeledCommitBackendCompletionNs(opts.commit_backend);
}

fn modeledCommitBackendCompletionNs(commit_backend: CommitBackend) u64 {
    return switch (commit_backend) {
        .sync, .adaptive => 0,
        .worker_thread => 75 * std.time.ns_per_us,
        .async_io => 50 * std.time.ns_per_us,
    };
}

const FixtureTxn = struct {
    raw: lmdb.Transaction,
    dbi: lmdb.Dbi,

    fn abort(self: *FixtureTxn) void {
        self.raw.abort();
        self.* = undefined;
    }

    fn commit(self: *FixtureTxn) !void {
        try self.raw.commit();
        self.* = undefined;
    }

    fn publishCommitPhaseForTest(self: *FixtureTxn, phase: zig_lmdb.commit_support.CommitPublishPhase) !void {
        try self.raw.publishCommitPhaseForTest(phase);
    }

    fn get(self: *FixtureTxn, key: []const u8) ![]const u8 {
        return try self.raw.get(self.dbi, key);
    }

    fn put(self: *FixtureTxn, key: []const u8, value: []const u8) !void {
        try self.raw.put(self.dbi, key, value, .{});
    }

    fn appendPut(self: *FixtureTxn, key: []const u8, value: []const u8) !void {
        try self.raw.put(self.dbi, key, value, .{ .append = true });
    }
};

fn requestOffsetInBatch(head: ?*AppendRequest, target: *AppendRequest) usize {
    var offset: usize = 0;
    var current = head;
    while (current) |request| : (current = request.next) {
        if (request == target) return offset;
        offset += request.entries.len;
    }
    unreachable;
}

fn nowNs() u64 {
    _ = builtin;
    return platform_time.monotonicNs();
}

fn elapsedSince(started: u64) u64 {
    const now = nowNs();
    return now - started;
}

fn sleepNs(ns: u64) void {
    if (ns == 0) return;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Clock.Duration.sleep(.{
        .clock = .awake,
        .raw = .fromNanoseconds(@intCast(ns)),
    }, io_impl.io()) catch {};
}

fn avgCommitWindowNs(avg_commit_ns: u64) u64 {
    const min_window = 25 * std.time.ns_per_us;
    const max_window = 250 * std.time.ns_per_us;
    return std.math.clamp(avg_commit_ns / 2, min_window, max_window);
}

fn initialCommitWindowNs(configured_window_ns: u64) u64 {
    const max_initial_window = 2 * std.time.ns_per_ms;
    return @min(configured_window_ns, max_initial_window);
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) {
        std.Thread.yield() catch {};
    }
}

fn putWalValue(txn: anytype, key: []const u8, value: []const u8) !void {
    if (comptime @TypeOf(txn.*) == FixtureTxn) {
        try txn.appendPut(key, value);
        return;
    }

    txn.appendPut(key, value) catch |err| switch (err) {
        error.Unsupported => try txn.put(key, value),
        else => return err,
    };
}

fn putEncodedEntry(txn: anytype, lsn: u64, data: []const u8) !void {
    const key = std.mem.toBytes(std.mem.nativeToBig(u64, lsn));
    const data_len: u32 = @intCast(data.len);
    const value_len = 4 + data.len + 4;
    const checksum = checksumForEntry(data_len, data);

    if (value_len <= large_entry_threshold) {
        var buf: [large_entry_threshold]u8 = undefined;
        encodeEntryValue(buf[0..value_len], data_len, data, checksum);
        try putWalValue(txn, &key, buf[0..value_len]);
        return;
    }

    const buf = try std.heap.page_allocator.alloc(u8, value_len);
    defer std.heap.page_allocator.free(buf);

    encodeEntryValue(buf, data_len, data, checksum);
    try putWalValue(txn, &key, buf);
}

const meta_next_lsn_key = [_]u8{0};

fn putNextLsnMeta(txn: anytype, next_lsn: u64) !void {
    const bytes = std.mem.toBytes(std.mem.nativeToLittle(u64, next_lsn));
    try txn.put(&meta_next_lsn_key, &bytes);
}

fn readNextLsnMeta(txn: anytype) !?u64 {
    const bytes = txn.get(&meta_next_lsn_key) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len < 8) return error.Corrupted;
    return std.mem.readInt(u64, bytes[0..8], .little);
}

fn decodeWalValue(value: []const u8) error{CorruptWal}![]const u8 {
    if (value.len < 8) return error.CorruptWal;

    const data_len: usize = std.mem.readInt(u32, value[0..4], .little);
    const data_end = std.math.add(usize, 4, data_len) catch return error.CorruptWal;
    const crc_end = std.math.add(usize, data_end, 4) catch return error.CorruptWal;
    if (value.len < crc_end) return error.CorruptWal;

    const data = value[4..data_end];
    const stored_crc = std.mem.readInt(u32, value[data_end..][0..4], .little);

    var crc = std.hash.Crc32.init();
    crc.update(value[0..4]);
    crc.update(data);
    if (crc.final() != stored_crc) return error.CorruptWal;

    return data;
}

fn encodeEntryValue(dst: []u8, data_len: u32, data: []const u8, checksum: u32) void {
    dst[0..4].* = std.mem.toBytes(std.mem.nativeToLittle(u32, data_len));
    @memcpy(dst[4..][0..data.len], data);
    dst[4 + data.len ..][0..4].* = std.mem.toBytes(std.mem.nativeToLittle(u32, checksum));
}

fn checksumForEntry(data_len: u32, data: []const u8) u32 {
    var crc = std.hash.Crc32.init();
    crc.update(&std.mem.toBytes(std.mem.nativeToLittle(u32, data_len)));
    crc.update(data);
    return crc.final();
}

// ============================================================================
// Tests
// ============================================================================

fn walTmpPathWithSuffix(buf: []u8, suffix: []const u8) [*:0]const u8 {
    const base = "/tmp/antfly-wal-test-";
    const pid: u32 = @intCast(std.posix.system.getpid());
    const ts = nowNs();
    const nonce = nextWalTmpNonce();
    const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}-{d}-{s}\x00", .{ base, pid, ts, nonce, suffix }) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn walTmpPath(buf: []u8) [*:0]const u8 {
    return walTmpPathWithSuffix(buf, "default");
}

const WalSimEntry = struct {
    lsn: u64,
    data: []u8,
};

const WalSimCase = struct {
    label: []const u8,
    opts: WalOptions,
    seed: u64,
    steps: usize,
    allow_concurrent_pair: bool = false,
};

const WalSimAction = wal_sim_fixture.Action;
const WalCrashOutcome = wal_sim_fixture.CrashOutcome;

const WalReplaySummary = struct {
    visible_entries: usize,
    last_lsn: u64,
};

fn deinitWalSimModel(allocator: Allocator, model: *std.ArrayListUnmanaged(WalSimEntry)) void {
    for (model.items) |entry| allocator.free(entry.data);
    model.deinit(allocator);
}

fn appendWalSimEntry(
    allocator: Allocator,
    model: *std.ArrayListUnmanaged(WalSimEntry),
    lsn: u64,
    data: []u8,
) !void {
    try model.append(allocator, .{ .lsn = lsn, .data = data });
}

fn truncateWalSimModel(
    allocator: Allocator,
    model: *std.ArrayListUnmanaged(WalSimEntry),
    up_to_lsn: u64,
) void {
    var kept: usize = 0;
    for (model.items) |entry| {
        if (entry.lsn <= up_to_lsn) {
            allocator.free(entry.data);
            continue;
        }
        model.items[kept] = entry;
        kept += 1;
    }
    model.items.len = kept;
}

fn walSimLastLsn(next_lsn: u64) u64 {
    return if (next_lsn <= 1) 0 else next_lsn - 1;
}

fn walSimPayload(
    allocator: Allocator,
    case_label: []const u8,
    step: usize,
    slot: usize,
) ![]u8 {
    return std.fmt.allocPrint(allocator, "{s}-step-{d}-slot-{d}", .{
        case_label,
        step,
        slot,
    });
}

fn walSimVerifyFrom(random: std.Random, next_lsn: u64) u64 {
    const max = next_lsn + 1;
    return if (max <= 1) 1 else random.uintLessThanBiased(u64, max) + 1;
}

fn nextWalSimAction(
    random: std.Random,
    model: []const WalSimEntry,
    runtime_next_lsn: u64,
    allow_concurrent_pair: bool,
) WalSimAction {
    const action = random.uintLessThanBiased(u8, 100);

    if (action < 24 or runtime_next_lsn == 1) return .append;
    if (action < 48) return .{ .append_batch = @intCast(2 + random.uintLessThanBiased(usize, 3)) };
    if (allow_concurrent_pair and action < 62) return .concurrent_pair;
    if (action < 76) return .{ .reopen_and_verify_from = walSimVerifyFrom(random, runtime_next_lsn) };
    if (action < 88 and model.len != 0) {
        const up_to_lsn = model[random.uintLessThanBiased(usize, model.len)].lsn;
        return .{ .truncate_and_verify_from = .{
            .up_to_lsn = up_to_lsn,
            .from_lsn = walSimVerifyFrom(random, runtime_next_lsn),
        } };
    }
    return .{ .verify_from = walSimVerifyFrom(random, runtime_next_lsn) };
}

fn nextWalCrashPreludeAction(random: std.Random, runtime_next_lsn: u64) WalSimAction {
    const action = random.uintLessThanBiased(u8, 100);
    if (action < 35 or runtime_next_lsn == 1) return .append;
    if (action < 65) return .{ .append_batch = @intCast(2 + random.uintLessThanBiased(usize, 2)) };
    if (action < 82) return .{ .reopen_and_verify_from = walSimVerifyFrom(random, runtime_next_lsn) };
    return .{ .verify_from = walSimVerifyFrom(random, runtime_next_lsn) };
}

fn walCrashActionForCase(case_index: usize) WalSimAction {
    return if ((case_index % 2) == 0)
        .append
    else
        .{ .append_batch = 2 };
}

fn verifyWalSimState(
    allocator: Allocator,
    wal: *WAL,
    model: []const WalSimEntry,
    next_lsn: u64,
    from_lsn: u64,
) !void {
    try std.testing.expectEqual(walSimLastLsn(next_lsn), wal.lastLsn());

    const entries = try wal.iterateFrom(allocator, from_lsn);
    defer {
        for (entries) |entry| allocator.free(@constCast(entry.data));
        allocator.free(entries);
    }

    var expected_count: usize = 0;
    for (model) |entry| {
        if (entry.lsn >= from_lsn) expected_count += 1;
    }
    try std.testing.expectEqual(expected_count, entries.len);

    var actual_index: usize = 0;
    for (model) |entry| {
        if (entry.lsn < from_lsn) continue;
        try std.testing.expectEqual(entry.lsn, entries[actual_index].lsn);
        try std.testing.expectEqualStrings(entry.data, entries[actual_index].data);
        actual_index += 1;
    }
}

fn reopenWalSim(wal: *WAL, wal_open: *bool, path: [*:0]const u8, opts: WalOptions) !void {
    wal.close();
    wal_open.* = false;
    wal.* = try WAL.open(path, opts);
    wal_open.* = true;
}

fn applyWalSimAction(
    allocator: Allocator,
    wal: *WAL,
    wal_open: *bool,
    path: [*:0]const u8,
    opts: WalOptions,
    case_label: []const u8,
    step: usize,
    model: *std.ArrayListUnmanaged(WalSimEntry),
    runtime_next_lsn: *u64,
    action: WalSimAction,
) !void {
    switch (action) {
        .append => {
            const payload = try walSimPayload(allocator, case_label, step, 0);
            const lsn = wal.append(payload) catch |err| {
                allocator.free(payload);
                return err;
            };
            try std.testing.expectEqual(runtime_next_lsn.*, lsn);
            try appendWalSimEntry(allocator, model, lsn, payload);
            runtime_next_lsn.* += 1;
        },
        .append_batch => |batch_len| {
            var payloads: [4][]u8 = undefined;
            var batch: [4][]const u8 = undefined;
            var made: usize = 0;
            errdefer {
                for (payloads[0..made]) |payload| allocator.free(payload);
            }

            while (made < batch_len) : (made += 1) {
                payloads[made] = try walSimPayload(allocator, case_label, step, made);
                batch[made] = payloads[made];
            }

            const result = try wal.appendBatch(batch[0..batch_len]);
            try std.testing.expectEqual(runtime_next_lsn.*, result.first_lsn);
            try std.testing.expectEqual(batch_len, result.count);
            for (payloads[0..batch_len], 0..) |payload, idx| {
                try appendWalSimEntry(allocator, model, result.lsnAt(idx), payload);
            }
            runtime_next_lsn.* += batch_len;
        },
        .concurrent_pair => {
            const StartBarrier = struct {
                mutex: std.atomic.Mutex = .unlocked,
                waiting: usize = 0,
                open: bool = false,

                fn wait(self: *@This(), total: usize) void {
                    var registered = false;
                    while (true) {
                        lockAtomic(&self.mutex);
                        if (!registered) {
                            self.waiting += 1;
                            registered = true;
                            if (self.waiting == total) self.open = true;
                        }
                        const ready = self.open;
                        self.mutex.unlock();
                        if (ready) return;
                        std.Thread.yield() catch {};
                    }
                }
            };

            const Worker = struct {
                wal: *WAL,
                barrier: *StartBarrier,
                payload: []const u8,
                result: u64 = 0,
                err: ?anyerror = null,

                fn run(self: *@This()) void {
                    self.barrier.wait(2);
                    self.result = self.wal.append(self.payload) catch |err| {
                        self.err = err;
                        return;
                    };
                }
            };

            const left_payload = try walSimPayload(allocator, case_label, step, 0);
            errdefer allocator.free(left_payload);
            const right_payload = try walSimPayload(allocator, case_label, step, 1);
            errdefer allocator.free(right_payload);

            var barrier = StartBarrier{};
            var left = Worker{ .wal = wal, .barrier = &barrier, .payload = left_payload };
            var right = Worker{ .wal = wal, .barrier = &barrier, .payload = right_payload };

            const left_thread = try std.Thread.spawn(.{}, Worker.run, .{&left});
            const right_thread = try std.Thread.spawn(.{}, Worker.run, .{&right});
            left_thread.join();
            right_thread.join();

            if (left.err) |err| return err;
            if (right.err) |err| return err;

            if (left.result < right.result) {
                try appendWalSimEntry(allocator, model, left.result, left_payload);
                try appendWalSimEntry(allocator, model, right.result, right_payload);
            } else {
                try appendWalSimEntry(allocator, model, right.result, right_payload);
                try appendWalSimEntry(allocator, model, left.result, left_payload);
            }
            try std.testing.expectEqual(runtime_next_lsn.*, @min(left.result, right.result));
            try std.testing.expectEqual(runtime_next_lsn.* + 1, @max(left.result, right.result));
            runtime_next_lsn.* += 2;
        },
        .reopen_and_verify_from => |from_lsn| {
            try reopenWalSim(wal, wal_open, path, opts);
            try verifyWalSimState(allocator, wal, model.items, runtime_next_lsn.*, from_lsn);
        },
        .truncate_and_verify_from => |payload| {
            if (payload.up_to_lsn > 0) {
                try wal.truncate(payload.up_to_lsn);
                truncateWalSimModel(allocator, model, payload.up_to_lsn);
            }
            try verifyWalSimState(allocator, wal, model.items, runtime_next_lsn.*, payload.from_lsn);
        },
        .verify_from => |from_lsn| {
            try verifyWalSimState(allocator, wal, model.items, runtime_next_lsn.*, from_lsn);
        },
    }
}

fn replayWalSimActionsAtPath(
    allocator: Allocator,
    path: [*:0]const u8,
    opts: WalOptions,
    case_label: []const u8,
    actions: []const WalSimAction,
    starting_step: usize,
) !WalReplaySummary {
    var wal = try WAL.open(path, opts);
    var wal_open = true;
    defer if (wal_open) wal.close();

    var model: std.ArrayListUnmanaged(WalSimEntry) = .empty;
    defer deinitWalSimModel(allocator, &model);

    var runtime_next_lsn: u64 = 1;
    for (actions, 0..) |action, step| {
        try applyWalSimAction(
            allocator,
            &wal,
            &wal_open,
            path,
            opts,
            case_label,
            starting_step + step,
            &model,
            &runtime_next_lsn,
            action,
        );
    }

    try reopenWalSim(&wal, &wal_open, path, opts);
    try verifyWalSimState(allocator, &wal, model.items, runtime_next_lsn, 1);
    return .{
        .visible_entries = model.items.len,
        .last_lsn = walSimLastLsn(runtime_next_lsn),
    };
}

fn replayWalSimActions(
    allocator: Allocator,
    opts: WalOptions,
    case_label: []const u8,
    actions: []const WalSimAction,
) !WalReplaySummary {
    var path_buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&path_buf, case_label);
    defer cleanupWalDir(path);
    return try replayWalSimActionsAtPath(allocator, path, opts, case_label, actions, 0);
}

fn replayModeledWalSimActions(
    allocator: Allocator,
    case_label: []const u8,
    actions: []const WalSimAction,
) !WalReplaySummary {
    var runtime = storage_sim.Runtime.init(allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(allocator);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/wal-modeled-replay";
    return try replayWalSimActionsAtPath(
        allocator,
        path,
        .{
            .backend = .lsm,
            .storage = device_model.storage(),
            .clock = runtime.clock(),
            .commit_scheduler = runtime.completionScheduler(),
            .group_commit_window_ns = 1 * std.time.ns_per_ms,
            .model_commit_backend_completions = true,
        },
        case_label,
        actions,
        0,
    );
}

fn takeWalSnapshot(allocator: Allocator, path: [*:0]const u8, opts: WalOptions) ![]WalSimEntry {
    var wal = try WAL.open(path, opts);
    defer wal.close();

    const entries = try wal.iterateFrom(allocator, 1);
    defer allocator.free(entries);

    const snapshot = try allocator.alloc(WalSimEntry, entries.len);
    errdefer {
        for (snapshot[0..entries.len]) |entry| allocator.free(entry.data);
        allocator.free(snapshot);
    }
    for (entries, 0..) |entry, idx| {
        snapshot[idx] = .{
            .lsn = entry.lsn,
            .data = @constCast(entry.data),
        };
    }
    return snapshot;
}

fn freeWalSnapshot(allocator: Allocator, snapshot: []WalSimEntry) void {
    for (snapshot) |entry| allocator.free(entry.data);
    allocator.free(snapshot);
}

fn expectWalSnapshotsEqual(expected: []const WalSimEntry, actual: []const WalSimEntry) !void {
    try std.testing.expectEqual(expected.len, actual.len);
    for (expected, actual) |expected_entry, actual_entry| {
        try std.testing.expectEqual(expected_entry.lsn, actual_entry.lsn);
        try std.testing.expectEqualStrings(expected_entry.data, actual_entry.data);
    }
}

fn walSnapshotsEqual(expected: []const WalSimEntry, actual: []const WalSimEntry) bool {
    if (expected.len != actual.len) return false;
    for (expected, actual) |expected_entry, actual_entry| {
        if (expected_entry.lsn != actual_entry.lsn) return false;
        if (!std.mem.eql(u8, expected_entry.data, actual_entry.data)) return false;
    }
    return true;
}

fn classifyWalCrashSnapshot(
    before: []const WalSimEntry,
    after: []const WalSimEntry,
    actual: []const WalSimEntry,
    phase: zig_lmdb.commit_support.CommitPublishPhase,
) !WalCrashOutcome {
    switch (phase) {
        .before_data_sync, .after_data_sync_before_meta => {
            try expectWalSnapshotsEqual(before, actual);
            return .previous;
        },
        .after_meta_write_before_meta_sync => {
            if (walSnapshotsEqual(before, actual)) return .previous_or_committed;
            if (walSnapshotsEqual(after, actual)) return .previous_or_committed;
            try expectWalSnapshotsEqual(after, actual);
            return .previous_or_committed;
        },
        .fully_published => {
            try expectWalSnapshotsEqual(after, actual);
            return .committed;
        },
    }
}

fn applyWalCrashActionAtPath(
    allocator: Allocator,
    path: [*:0]const u8,
    opts: WalOptions,
    case_label: []const u8,
    step: usize,
    action: WalSimAction,
    phase: lmdb.CommitPublishPhase,
) !void {
    var wal = try WAL.open(path, opts);
    defer wal.close();

    const next_lsn = wal.lastLsn() + 1;
    var txn = try wal.beginLmdbFixtureTxn();
    defer txn.abort();

    switch (action) {
        .append => {
            const payload = try walSimPayload(allocator, case_label, step, 0);
            defer allocator.free(payload);
            try putEncodedEntry(&txn, next_lsn, payload);
            try putNextLsnMeta(&txn, next_lsn + 1);
        },
        .append_batch => |count| {
            var lsn = next_lsn;
            for (0..count) |slot| {
                const payload = try walSimPayload(allocator, case_label, step, slot);
                defer allocator.free(payload);
                try putEncodedEntry(&txn, lsn, payload);
                lsn += 1;
            }
            try putNextLsnMeta(&txn, lsn);
        },
        else => return error.InvalidFixture,
    }

    try txn.publishCommitPhaseForTest(phase);
}

fn applyCommittedWalCrashActionAtPath(
    allocator: Allocator,
    path: [*:0]const u8,
    opts: WalOptions,
    case_label: []const u8,
    step: usize,
    action: WalSimAction,
) !void {
    var wal = try WAL.open(path, opts);
    defer wal.close();

    const next_lsn = wal.lastLsn() + 1;
    var txn = try wal.beginLmdbFixtureTxn();
    errdefer txn.abort();

    switch (action) {
        .append => {
            const payload = try walSimPayload(allocator, case_label, step, 0);
            defer allocator.free(payload);
            try putEncodedEntry(&txn, next_lsn, payload);
            try putNextLsnMeta(&txn, next_lsn + 1);
        },
        .append_batch => |count| {
            var lsn = next_lsn;
            for (0..count) |slot| {
                const payload = try walSimPayload(allocator, case_label, step, slot);
                defer allocator.free(payload);
                try putEncodedEntry(&txn, lsn, payload);
                lsn += 1;
            }
            try putNextLsnMeta(&txn, lsn);
        },
        else => return error.InvalidFixture,
    }

    try txn.commit();
}

fn applyPublicWalCrashAction(
    allocator: Allocator,
    wal: *WAL,
    case_label: []const u8,
    step: usize,
    action: WalSimAction,
    model: *std.ArrayListUnmanaged(WalSimEntry),
    runtime_next_lsn: *u64,
) !void {
    switch (action) {
        .append => {
            const payload = try walSimPayload(allocator, case_label, step, 0);
            const lsn = wal.append(payload) catch |err| {
                allocator.free(payload);
                return err;
            };
            try std.testing.expectEqual(runtime_next_lsn.*, lsn);
            try appendWalSimEntry(allocator, model, lsn, payload);
            runtime_next_lsn.* += 1;
        },
        .append_batch => |count| {
            if (count > 4) return error.InvalidFixture;
            var payloads: [4][]u8 = undefined;
            var batch: [4][]const u8 = undefined;
            var made: usize = 0;
            errdefer {
                for (payloads[0..made]) |payload| allocator.free(payload);
            }
            while (made < count) : (made += 1) {
                payloads[made] = try walSimPayload(allocator, case_label, step, made);
                batch[made] = payloads[made];
            }

            const result = try wal.appendBatch(batch[0..count]);
            try std.testing.expectEqual(runtime_next_lsn.*, result.first_lsn);
            try std.testing.expectEqual(@as(usize, count), result.count);
            for (payloads[0..count], 0..) |payload, idx| {
                try appendWalSimEntry(allocator, model, result.lsnAt(idx), payload);
            }
            runtime_next_lsn.* += count;
        },
        else => return error.InvalidFixture,
    }
}

fn expectPublicWalCrashActionSyncFailure(
    allocator: Allocator,
    wal: *WAL,
    case_label: []const u8,
    step: usize,
    action: WalSimAction,
) !void {
    switch (action) {
        .append => {
            const payload = try walSimPayload(allocator, case_label, step, 0);
            defer allocator.free(payload);
            if (wal.append(payload)) |_| return error.ExpectedSyncFailure else |_| return;
        },
        .append_batch => |count| {
            if (count > 4) return error.InvalidFixture;
            var payloads: [4][]u8 = undefined;
            var batch: [4][]const u8 = undefined;
            var made: usize = 0;
            defer {
                for (payloads[0..made]) |payload| allocator.free(payload);
            }
            while (made < count) : (made += 1) {
                payloads[made] = try walSimPayload(allocator, case_label, step, made);
                batch[made] = payloads[made];
            }
            if (wal.appendBatch(batch[0..count])) |_| return error.ExpectedSyncFailure else |_| return;
        },
        else => return error.InvalidFixture,
    }
}

fn replayModeledWalCrashAfterAck(
    allocator: Allocator,
    case_label: []const u8,
    prelude_actions: []const WalSimAction,
    crash_action: WalSimAction,
) !void {
    var runtime = storage_sim.Runtime.init(allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(allocator);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/wal-modeled-crash-after-ack";
    const opts = WalOptions{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .group_commit_window_ns = 1 * std.time.ns_per_ms,
        .model_commit_backend_completions = true,
    };

    var wal = try WAL.open(path, opts);
    var wal_open = true;
    var model: std.ArrayListUnmanaged(WalSimEntry) = .empty;
    defer deinitWalSimModel(allocator, &model);

    var runtime_next_lsn: u64 = 1;
    for (prelude_actions, 0..) |action, step| {
        try applyWalSimAction(
            allocator,
            &wal,
            &wal_open,
            path,
            opts,
            case_label,
            step,
            &model,
            &runtime_next_lsn,
            action,
        );
    }

    try applyPublicWalCrashAction(
        allocator,
        &wal,
        case_label,
        prelude_actions.len,
        crash_action,
        &model,
        &runtime_next_lsn,
    );
    try device_model.device().crash();

    var reopened = try WAL.open(path, opts);
    defer reopened.close();
    try verifyWalSimState(allocator, &reopened, model.items, runtime_next_lsn, 1);

    wal_open = false;
    wal = undefined;
}

fn replayModeledWalCrashFixture(
    allocator: Allocator,
    fixture: wal_sim_fixture.ReplayFixture,
) !WalCrashOutcome {
    var runtime = storage_sim.Runtime.init(allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(allocator);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/wal-modeled-crash-fixture";
    const opts = modeledWalOptionsFromFixtureOptions(fixture.opts, &device_model, &runtime);
    const case_label = fixture.case_label orelse fixture.label orelse "wal-modeled-crash";
    const phase = fixture.phase orelse return error.InvalidFixture;
    const crash_action = fixture.crash_action orelse return error.InvalidFixture;

    var wal = try WAL.open(path, opts);
    var wal_open = true;
    var model: std.ArrayListUnmanaged(WalSimEntry) = .empty;
    defer deinitWalSimModel(allocator, &model);

    var runtime_next_lsn: u64 = 1;
    for (fixture.prelude_actions, 0..) |action, step| {
        try applyWalSimAction(
            allocator,
            &wal,
            &wal_open,
            path,
            opts,
            case_label,
            step,
            &model,
            &runtime_next_lsn,
            action,
        );
    }

    const outcome: WalCrashOutcome = switch (phase) {
        .before_data_sync, .after_data_sync_before_meta => blk: {
            device_model.injectSyncFailure();
            try expectPublicWalCrashActionSyncFailure(
                allocator,
                &wal,
                case_label,
                fixture.prelude_actions.len,
                crash_action,
            );
            break :blk .previous;
        },
        .after_meta_write_before_meta_sync, .fully_published => blk: {
            try applyPublicWalCrashAction(
                allocator,
                &wal,
                case_label,
                fixture.prelude_actions.len,
                crash_action,
                &model,
                &runtime_next_lsn,
            );
            break :blk if (phase == .fully_published) .committed else .previous_or_committed;
        },
    };

    try device_model.device().crash();
    wal_open = false;
    wal = undefined;

    var reopened = try WAL.open(path, opts);
    defer reopened.close();
    try verifyWalSimState(allocator, &reopened, model.items, runtime_next_lsn, 1);
    return outcome;
}

fn replayWalCrashWorkload(
    allocator: Allocator,
    opts: WalOptions,
    case_label: []const u8,
    prelude_actions: []const WalSimAction,
    crash_action: WalSimAction,
    phase: zig_lmdb.commit_support.CommitPublishPhase,
) !WalCrashOutcome {
    if (!zig_lmdb.is_zig_backend) return .previous_or_committed;
    var fixture_opts = opts;
    fixture_opts.backend = .lmdb;
    fixture_opts.storage = null;
    fixture_opts.lsm_options = .{};

    var committed_path_buf: [256]u8 = undefined;
    const committed_path = walTmpPathWithSuffix(&committed_path_buf, "crash-committed");
    defer cleanupWalDir(committed_path);

    var crash_path_buf: [256]u8 = undefined;
    const crash_path = walTmpPathWithSuffix(&crash_path_buf, "crash-phase");
    defer cleanupWalDir(crash_path);

    _ = try replayWalSimActionsAtPath(allocator, committed_path, fixture_opts, case_label, prelude_actions, 0);
    _ = try replayWalSimActionsAtPath(allocator, crash_path, fixture_opts, case_label, prelude_actions, 0);

    const before_snapshot = try takeWalSnapshot(allocator, committed_path, fixture_opts);
    defer freeWalSnapshot(allocator, before_snapshot);

    try applyCommittedWalCrashActionAtPath(
        allocator,
        committed_path,
        fixture_opts,
        case_label,
        prelude_actions.len,
        crash_action,
    );
    const after_snapshot = try takeWalSnapshot(allocator, committed_path, fixture_opts);
    defer freeWalSnapshot(allocator, after_snapshot);

    try applyWalCrashActionAtPath(
        allocator,
        crash_path,
        fixture_opts,
        case_label,
        prelude_actions.len,
        crash_action,
        phase,
    );

    const zig_reopened_snapshot = try takeWalSnapshot(allocator, crash_path, fixture_opts);
    defer freeWalSnapshot(allocator, zig_reopened_snapshot);

    return try classifyWalCrashSnapshot(before_snapshot, after_snapshot, zig_reopened_snapshot, phase);
}

fn walReplayArtifactPath(buf: []u8, suffix: []const u8) []const u8 {
    const base = "/tmp/antfly-wal-replay-";
    const ts = nowNs();
    const nonce = nextWalTmpNonce();
    return std.fmt.bufPrint(buf, "{s}{d}-{d}-{s}.fixture", .{ base, ts, nonce, suffix }) catch unreachable;
}

fn writeWalReplayFixtureArtifact(
    allocator: Allocator,
    opts: WalOptions,
    case_label: []const u8,
    seed: u64,
    expectation_note: []const u8,
    summary: WalReplaySummary,
    actions: []const WalSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = walReplayArtifactPath(&path_buf, case_label);
    const path = try allocator.dupe(u8, artifact_path);
    errdefer allocator.free(path);

    const normalized = try wal_sim_fixture.renderReplayArtifact(
        allocator,
        fixtureOptionsFromWalOptions(opts),
        case_label,
        seed,
        expectation_note,
        summary.visible_entries,
        summary.last_lsn,
        actions,
    );
    defer allocator.free(normalized);

    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
    defer file.close(std.testing.io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &file_buf);
    try writer.interface.writeAll(normalized);
    try writer.end();

    return path;
}

fn writeWalCrashFixtureArtifact(
    allocator: Allocator,
    opts: WalOptions,
    case_label: []const u8,
    seed: u64,
    phase: zig_lmdb.commit_support.CommitPublishPhase,
    expectation_note: []const u8,
    expected_outcome: WalCrashOutcome,
    prelude_actions: []const WalSimAction,
    crash_action: WalSimAction,
) !?[]u8 {
    var path_buf: [256]u8 = undefined;
    const artifact_path = walReplayArtifactPath(&path_buf, case_label);
    const path = try allocator.dupe(u8, artifact_path);
    errdefer allocator.free(path);

    const normalized = try wal_sim_fixture.renderCrashArtifact(
        allocator,
        fixtureOptionsFromWalOptions(opts),
        case_label,
        seed,
        fixturePhaseFromCommitPhase(phase),
        expectation_note,
        expected_outcome,
        prelude_actions,
        crash_action,
    );
    defer allocator.free(normalized);

    var file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
    defer file.close(std.testing.io);

    var file_buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &file_buf);
    try writer.interface.writeAll(normalized);
    try writer.end();

    return path;
}

fn reportReducedWalSchedule(
    allocator: Allocator,
    opts: WalOptions,
    case_label: []const u8,
    seed: u64,
    actions: []const WalSimAction,
) !void {
    const Replayer = struct {
        allocator: Allocator,
        opts: WalOptions,
        case_label: []const u8,

        pub fn replay(self: @This(), candidate: []const WalSimAction) !void {
            _ = try replayWalSimActions(self.allocator, self.opts, self.case_label, candidate);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(
        WalSimAction,
        allocator,
        actions,
        Replayer{
            .allocator = allocator,
            .opts = opts,
            .case_label = case_label,
        },
    );
    defer allocator.free(reduced);

    const summary = replayWalSimActions(allocator, opts, case_label, reduced) catch |err| {
        std.debug.print("failed to recompute WAL replay summary for {s}: {s}\n", .{ case_label, @errorName(err) });
        return;
    };

    const artifact_path = writeWalReplayFixtureArtifact(
        allocator,
        opts,
        case_label,
        seed,
        "expected WAL state to survive replayed reopen and truncate schedules",
        summary,
        reduced,
    ) catch |err| blk: {
        std.debug.print("failed to write WAL replay artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| allocator.free(path);

    std.debug.print("reduced failing WAL schedule ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
}

fn fixtureOptionsFromWalOptions(opts: WalOptions) wal_sim_fixture.Options {
    return .{
        .no_sync = opts.no_sync,
        .commit_backend = switch (opts.commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
        .group_commit_window_ns = opts.group_commit_window_ns,
        .group_commit_max_requests = opts.group_commit_max_requests,
    };
}

fn walOptionsFromFixtureOptions(opts: wal_sim_fixture.Options) WalOptions {
    return .{
        .no_sync = opts.no_sync,
        .commit_backend = switch (opts.commit_backend) {
            .sync => .sync,
            .worker_thread => .worker_thread,
            .async_io => .async_io,
            .adaptive => .adaptive,
        },
        .group_commit_window_ns = opts.group_commit_window_ns,
        .group_commit_max_requests = opts.group_commit_max_requests,
    };
}

fn modeledWalOptionsFromFixtureOptions(
    opts: wal_sim_fixture.Options,
    device_model: *storage_sim.ModeledDevice,
    runtime: *storage_sim.Runtime,
) WalOptions {
    var wal_opts = walOptionsFromFixtureOptions(opts);
    wal_opts.backend = .lsm;
    wal_opts.storage = device_model.storage();
    wal_opts.clock = runtime.clock();
    wal_opts.commit_scheduler = runtime.completionScheduler();
    wal_opts.model_commit_backend_completions = true;
    return wal_opts;
}

fn expectWalReplaySummary(
    fixture_name: []const u8,
    opts: wal_sim_fixture.Options,
    summary: WalReplaySummary,
) !void {
    if (opts.expected_visible_entries) |expected| {
        try sim_fixture.expectFieldEqual(
            fixture_name,
            "expected_visible_entries",
            expected,
            summary.visible_entries,
        );
    }
    if (opts.expected_last_lsn) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_last_lsn", expected, summary.last_lsn);
    }
}

fn expectWalCrashOutcome(
    fixture_name: []const u8,
    opts: wal_sim_fixture.Options,
    outcome: WalCrashOutcome,
) !void {
    if (opts.expected_outcome) |expected| {
        try sim_fixture.expectFieldEqual(fixture_name, "expected_outcome", expected, outcome);
    }
}

fn fixturePhaseFromCommitPhase(phase: zig_lmdb.commit_support.CommitPublishPhase) wal_sim_fixture.CommitPhase {
    return switch (phase) {
        .before_data_sync => .before_data_sync,
        .after_data_sync_before_meta => .after_data_sync_before_meta,
        .after_meta_write_before_meta_sync => .after_meta_write_before_meta_sync,
        .fully_published => .fully_published,
    };
}

fn commitPhaseFromFixturePhase(phase: wal_sim_fixture.CommitPhase) zig_lmdb.commit_support.CommitPublishPhase {
    return switch (phase) {
        .before_data_sync => .before_data_sync,
        .after_data_sync_before_meta => .after_data_sync_before_meta,
        .after_meta_write_before_meta_sync => .after_meta_write_before_meta_sync,
        .fully_published => .fully_published,
    };
}

fn walCrashExpectationNoteForPhase(phase: zig_lmdb.commit_support.CommitPublishPhase) []const u8 {
    return switch (phase) {
        .before_data_sync, .after_data_sync_before_meta => "expected WAL reopen to preserve the previous committed snapshot",
        .after_meta_write_before_meta_sync => "expected WAL reopen to match either the previous or newly committed snapshot",
        .fully_published => "expected WAL reopen to preserve the newly committed snapshot",
    };
}

fn reportReducedWalCrashSchedule(
    allocator: Allocator,
    opts: WalOptions,
    case_label: []const u8,
    seed: u64,
    phase: zig_lmdb.commit_support.CommitPublishPhase,
    prelude_actions: []const WalSimAction,
    crash_action: WalSimAction,
) !void {
    const Replayer = struct {
        allocator: Allocator,
        opts: WalOptions,
        case_label: []const u8,
        phase: zig_lmdb.commit_support.CommitPublishPhase,
        crash_action: WalSimAction,

        pub fn replay(self: @This(), candidate: []const WalSimAction) !void {
            _ = try replayWalCrashWorkload(self.allocator, self.opts, self.case_label, candidate, self.crash_action, self.phase);
        }
    };

    const reduced = try zig_lmdb.sim.reduceFailingSequence(
        WalSimAction,
        allocator,
        prelude_actions,
        Replayer{
            .allocator = allocator,
            .opts = opts,
            .case_label = case_label,
            .phase = phase,
            .crash_action = crash_action,
        },
    );
    defer allocator.free(reduced);

    const expected_outcome: WalCrashOutcome = switch (phase) {
        .before_data_sync, .after_data_sync_before_meta => .previous,
        .after_meta_write_before_meta_sync => .previous_or_committed,
        .fully_published => .committed,
    };

    const artifact_path = writeWalCrashFixtureArtifact(
        allocator,
        opts,
        case_label,
        seed,
        phase,
        walCrashExpectationNoteForPhase(phase),
        expected_outcome,
        reduced,
        crash_action,
    ) catch |err| blk: {
        std.debug.print("failed to write WAL crash artifact for {s}: {s}\n", .{ case_label, @errorName(err) });
        break :blk null;
    };
    defer if (artifact_path) |path| allocator.free(path);

    std.debug.print("reduced failing WAL crash prelude ({d} actions):\n", .{reduced.len});
    if (artifact_path) |path| std.debug.print("replay fixture: {s}\n", .{path});
}

fn replayWalFixtureFile(allocator: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(allocator, "pkg/antfly/src/storage/wal_sim_fixtures/{s}", .{name});
    defer allocator.free(path);

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(64 * 1024));
    defer allocator.free(contents);

    var fixture = try wal_sim_fixture.parseFixture(allocator, contents);
    defer fixture.deinit(allocator);

    switch (fixture.mode) {
        .replay => {
            const summary = try replayWalSimActions(
                allocator,
                walOptionsFromFixtureOptions(fixture.opts),
                fixture.case_label orelse fixture.label orelse "wal-replay",
                fixture.actions,
            );
            try expectWalReplaySummary(fixture.case_label orelse fixture.label orelse name, fixture.opts, summary);
        },
        .crash => {
            if (!zig_lmdb.is_zig_backend) return;
            const outcome = try replayWalCrashWorkload(
                allocator,
                walOptionsFromFixtureOptions(fixture.opts),
                fixture.case_label orelse fixture.label orelse "wal-crash",
                fixture.prelude_actions,
                fixture.crash_action orelse return error.InvalidFixture,
                commitPhaseFromFixturePhase(fixture.phase orelse return error.InvalidFixture),
            );
            try expectWalCrashOutcome(fixture.case_label orelse fixture.label orelse name, fixture.opts, outcome);
        },
    }
}

fn replayModeledWalFixtureFile(allocator: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(allocator, "pkg/antfly/src/storage/wal_sim_fixtures/{s}", .{name});
    defer allocator.free(path);

    const contents = try std.Io.Dir.cwd().readFileAlloc(std.testing.io, path, allocator, .limited(64 * 1024));
    defer allocator.free(contents);

    var fixture = try wal_sim_fixture.parseFixture(allocator, contents);
    defer fixture.deinit(allocator);

    switch (fixture.mode) {
        .replay => {
            var runtime = storage_sim.Runtime.init(allocator);
            defer runtime.deinit();
            var device_model = storage_sim.ModeledDevice.init(allocator);
            defer device_model.deinit();

            const wal_path: [*:0]const u8 = "/wal-modeled-fixture";
            const summary = try replayWalSimActionsAtPath(
                allocator,
                wal_path,
                modeledWalOptionsFromFixtureOptions(fixture.opts, &device_model, &runtime),
                fixture.case_label orelse fixture.label orelse "wal-modeled-replay",
                fixture.actions,
                0,
            );
            try expectWalReplaySummary(fixture.case_label orelse fixture.label orelse name, fixture.opts, summary);
        },
        .crash => {
            const outcome = try replayModeledWalCrashFixture(allocator, fixture);
            try expectWalCrashOutcome(fixture.case_label orelse fixture.label orelse name, fixture.opts, outcome);
        },
    }
}

fn runWalReplayFixtures(allocator: Allocator) !void {
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly/src/storage/wal_sim_fixtures", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer fixtures_dir.close(std.testing.io);

    var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (fixture_names.items) |name| allocator.free(name);
        fixture_names.deinit(allocator);
    }

    var walker = try fixtures_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_names.append(allocator, try allocator.dupe(u8, entry.path));
    }

    std.mem.sort([]u8, fixture_names.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (fixture_names.items) |name| {
        try replayWalFixtureFile(allocator, name);
    }
}

fn runModeledWalReplayFixtures(allocator: Allocator) !void {
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly/src/storage/wal_sim_fixtures/replay", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer fixtures_dir.close(std.testing.io);

    var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (fixture_names.items) |name| allocator.free(name);
        fixture_names.deinit(allocator);
    }

    var walker = try fixtures_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_names.append(allocator, try std.fmt.allocPrint(allocator, "replay/{s}", .{entry.path}));
    }

    std.mem.sort([]u8, fixture_names.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (fixture_names.items) |name| {
        try replayModeledWalFixtureFile(allocator, name);
    }
}

fn runModeledWalCrashFixtures(allocator: Allocator) !void {
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly/src/storage/wal_sim_fixtures/crash", .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return,
        else => return err,
    };
    defer fixtures_dir.close(std.testing.io);

    var fixture_names: std.ArrayListUnmanaged([]u8) = .empty;
    defer {
        for (fixture_names.items) |name| allocator.free(name);
        fixture_names.deinit(allocator);
    }

    var walker = try fixtures_dir.walk(allocator);
    defer walker.deinit();

    while (try walker.next(std.testing.io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.path, ".fixture")) continue;
        try fixture_names.append(allocator, try std.fmt.allocPrint(allocator, "crash/{s}", .{entry.path}));
    }

    std.mem.sort([]u8, fixture_names.items, {}, struct {
        fn lessThan(_: void, lhs: []u8, rhs: []u8) bool {
            return std.mem.lessThan(u8, lhs, rhs);
        }
    }.lessThan);

    for (fixture_names.items) |name| {
        try replayModeledWalFixtureFile(allocator, name);
    }
}

fn runWalSimCase(allocator: Allocator, case: WalSimCase) !void {
    var prng = std.Random.DefaultPrng.init(case.seed);
    const random = prng.random();
    var schedule: std.ArrayListUnmanaged(WalSimAction) = .empty;
    defer schedule.deinit(allocator);

    var simulated_model: std.ArrayListUnmanaged(WalSimEntry) = .empty;
    defer deinitWalSimModel(allocator, &simulated_model);
    var simulated_runtime_next_lsn: u64 = 1;

    var path_buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&path_buf, case.label);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, case.opts);
    var wal_open = true;
    defer if (wal_open) wal.close();
    for (0..case.steps) |step| {
        const action = nextWalSimAction(random, simulated_model.items, simulated_runtime_next_lsn, case.allow_concurrent_pair);
        try schedule.append(allocator, action);

        applyWalSimAction(
            allocator,
            &wal,
            &wal_open,
            path,
            case.opts,
            case.label,
            step,
            &simulated_model,
            &simulated_runtime_next_lsn,
            action,
        ) catch |err| {
            reportReducedWalSchedule(allocator, case.opts, case.label, case.seed, schedule.items) catch {};
            return err;
        };
    }

    try reopenWalSim(&wal, &wal_open, path, case.opts);
    verifyWalSimState(allocator, &wal, simulated_model.items, simulated_runtime_next_lsn, 1) catch |err| {
        reportReducedWalSchedule(allocator, case.opts, case.label, case.seed, schedule.items) catch {};
        return err;
    };
}

fn runModeledWalSimCase(
    allocator: Allocator,
    label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    var runtime = storage_sim.Runtime.init(allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(allocator);
    defer device_model.deinit();

    var path_buf: [256]u8 = undefined;
    const path_slice = try std.fmt.bufPrint(&path_buf, "/wal-modeled-vopr-{s}\x00", .{label});
    const path: [*:0]const u8 = @ptrCast(path_slice.ptr);
    const opts = WalOptions{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .group_commit_window_ns = 1 * std.time.ns_per_ms,
        .model_commit_backend_completions = true,
    };

    var prng = std.Random.DefaultPrng.init(seed);
    const random = prng.random();
    var schedule: std.ArrayListUnmanaged(WalSimAction) = .empty;
    defer schedule.deinit(allocator);

    var model: std.ArrayListUnmanaged(WalSimEntry) = .empty;
    defer deinitWalSimModel(allocator, &model);
    var runtime_next_lsn: u64 = 1;

    var wal = try WAL.open(path, opts);
    var wal_open = true;
    defer if (wal_open) wal.close();

    for (0..steps) |step| {
        const action = nextWalSimAction(random, model.items, runtime_next_lsn, false);
        try schedule.append(allocator, action);
        applyWalSimAction(
            allocator,
            &wal,
            &wal_open,
            path,
            opts,
            label,
            step,
            &model,
            &runtime_next_lsn,
            action,
        ) catch |err| {
            std.debug.print("modeled WAL VOPR failed label={s} seed={d} step={d} actions={d}\n", .{
                label,
                seed,
                step,
                schedule.items.len,
            });
            return err;
        };
    }

    try device_model.device().crash();
    try reopenWalSim(&wal, &wal_open, path, opts);
    try verifyWalSimState(allocator, &wal, model.items, runtime_next_lsn, 1);
}

fn runWalCrashCase(
    allocator: Allocator,
    opts: WalOptions,
    case_label: []const u8,
    seed: u64,
    steps: usize,
) !void {
    const phases = [_]zig_lmdb.commit_support.CommitPublishPhase{
        .before_data_sync,
        .after_data_sync_before_meta,
        .after_meta_write_before_meta_sync,
    };

    for (phases, 0..) |phase, phase_index| {
        var prng = std.Random.DefaultPrng.init(seed + phase_index);
        const random = prng.random();
        var prelude: std.ArrayListUnmanaged(WalSimAction) = .empty;
        defer prelude.deinit(allocator);

        var simulated_runtime_next_lsn: u64 = 1;
        for (0..steps) |_| {
            const action = nextWalCrashPreludeAction(random, simulated_runtime_next_lsn);
            try prelude.append(allocator, action);
            switch (action) {
                .append => simulated_runtime_next_lsn += 1,
                .append_batch => |count| simulated_runtime_next_lsn += count,
                .reopen_and_verify_from => {},
                .verify_from => {},
                else => unreachable,
            }
        }

        const crash_action = walCrashActionForCase(phase_index + case_label.len);
        _ = replayWalCrashWorkload(allocator, opts, case_label, prelude.items, crash_action, phase) catch |err| {
            reportReducedWalCrashSchedule(allocator, opts, case_label, seed, phase, prelude.items, crash_action) catch {};
            return err;
        };
    }
}

fn runWalSoak(allocator: Allocator) !void {
    const sim_cases = [_]WalSimCase{
        .{ .label = "soak-adaptive-a", .opts = .{}, .seed = 0xA17F_A001, .steps = 120 },
        .{ .label = "soak-adaptive-b", .opts = .{}, .seed = 0xA17F_A002, .steps = 120 },
        .{
            .label = "soak-async-io-grouped",
            .opts = .{
                .commit_backend = .async_io,
                .group_commit_window_ns = 2 * std.time.ns_per_ms,
            },
            .seed = 0xA17F_A003,
            .steps = 96,
            .allow_concurrent_pair = true,
        },
    };
    for (sim_cases) |case| {
        try runWalSimCase(allocator, case);
    }

    try runWalCrashCase(allocator, .{}, "soak-crash-default", 0xA17F_A101, 12);
    try runWalCrashCase(allocator, .{ .commit_backend = .async_io }, "soak-crash-async-io", 0xA17F_A102, 12);
}

test "wal defaults to adaptive commit backend" {
    const opts = WalOptions{};
    try std.testing.expectEqual(CommitBackend.adaptive, opts.commit_backend);
}

test "wal sim workload survives reopen cycles across commit backends" {
    const allocator = std.testing.allocator;
    const cases = [_]WalSimCase{
        .{ .label = "adaptive", .opts = .{}, .seed = 0xA17F_1001, .steps = 40 },
        .{ .label = "worker-thread", .opts = .{ .commit_backend = .worker_thread }, .seed = 0xA17F_1002, .steps = 40 },
        .{ .label = "async-io", .opts = .{ .commit_backend = .async_io }, .seed = 0xA17F_1003, .steps = 40 },
        .{
            .label = "async-io-grouped",
            .opts = .{
                .commit_backend = .async_io,
                .group_commit_window_ns = 2 * std.time.ns_per_ms,
            },
            .seed = 0xA17F_1004,
            .steps = 32,
            .allow_concurrent_pair = true,
        },
    };

    for (cases) |case| {
        try runWalSimCase(allocator, case);
    }
}

test "wal group commit uses injected virtual clock" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{
        .backend = .lsm_memory,
        .clock = runtime.clock(),
        .group_commit_window_ns = 2 * std.time.ns_per_ms,
    });
    defer wal.close();

    const lsn = try wal.append("virtual-time");
    try std.testing.expectEqual(@as(u64, 1), lsn);
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), runtime.clock().nowNs());

    const stats = wal.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), stats.total_coalesce_ns);
    try std.testing.expectEqual(@as(u64, 2 * std.time.ns_per_ms), stats.total_wait_ns);
}

test "wal can reopen on modeled storage device" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/wal-modeled-storage";
    const opts = WalOptions{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .group_commit_window_ns = 1 * std.time.ns_per_ms,
        .model_commit_backend_completions = true,
    };

    {
        var wal = try WAL.open(path, opts);
        defer wal.close();

        try std.testing.expectEqual(@as(u64, 1), try wal.append("alpha"));
        const batch = try wal.appendBatch(&.{ "beta", "gamma" });
        try std.testing.expectEqual(@as(u64, 2), batch.first_lsn);
        try std.testing.expectEqual(@as(usize, 2), batch.count);
    }

    {
        var wal = try WAL.open(path, opts);
        defer wal.close();

        try std.testing.expectEqual(@as(u64, 4), wal.next_lsn);
        const entries = try wal.iterateFrom(std.testing.allocator, 1);
        defer {
            for (entries) |entry| std.testing.allocator.free(entry.data);
            std.testing.allocator.free(entries);
        }
        try std.testing.expectEqual(@as(usize, 3), entries.len);
        try std.testing.expectEqual(@as(u64, 1), entries[0].lsn);
        try std.testing.expectEqualStrings("alpha", entries[0].data);
        try std.testing.expectEqual(@as(u64, 2), entries[1].lsn);
        try std.testing.expectEqualStrings("beta", entries[1].data);
        try std.testing.expectEqual(@as(u64, 3), entries[2].lsn);
        try std.testing.expectEqualStrings("gamma", entries[2].data);
    }
}

test "wal modeled storage survives crash before close after acknowledged append" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/wal-modeled-crash-before-close";
    const opts = WalOptions{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .model_commit_backend_completions = true,
    };

    var crashed_wal = try WAL.open(path, opts);
    try std.testing.expectEqual(@as(u64, 1), try crashed_wal.append("committed-before-crash"));
    try device_model.device().crash();

    var reopened = try WAL.open(path, opts);
    defer reopened.close();

    try std.testing.expectEqual(@as(u64, 2), reopened.next_lsn);
    const entries = try reopened.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(entry.data);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].lsn);
    try std.testing.expectEqualStrings("committed-before-crash", entries[0].data);

    crashed_wal = undefined;
}

test "wal modeled replay runner uses virtual storage and time" {
    const actions = [_]WalSimAction{
        .append,
        .{ .append_batch = 2 },
        .{ .verify_from = 1 },
        .{ .reopen_and_verify_from = 2 },
        .{ .truncate_and_verify_from = .{ .up_to_lsn = 1, .from_lsn = 1 } },
        .append,
        .{ .reopen_and_verify_from = 1 },
    };

    const summary = try replayModeledWalSimActions(
        std.testing.allocator,
        "modeled-replay",
        &actions,
    );
    try std.testing.expectEqual(@as(usize, 3), summary.visible_entries);
    try std.testing.expectEqual(@as(u64, 4), summary.last_lsn);
}

test "wal modeled crash runner preserves acknowledged public append" {
    const prelude = [_]WalSimAction{
        .append,
        .{ .append_batch = 2 },
        .{ .reopen_and_verify_from = 1 },
    };

    try replayModeledWalCrashAfterAck(
        std.testing.allocator,
        "modeled-crash-after-ack",
        &prelude,
        .append,
    );
}

test "wal modeled VOPR campaign stays green" {
    try runModeledWalSimCase(std.testing.allocator, "seed-a", 0xA17F_5001, 24);
    try runModeledWalSimCase(std.testing.allocator, "seed-b", 0xA17F_5002, 24);
}

test "wal modeled replay fixtures stay green" {
    try runModeledWalReplayFixtures(std.testing.allocator);
}

test "wal modeled crash fixtures stay green" {
    try runModeledWalCrashFixtures(std.testing.allocator);
}

test "wal modeled commit backend completion uses scheduled virtual time" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    var side_effect = struct {
        count: u32 = 0,

        fn mark(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
        }
    }{};
    try runtime.schedule(25 * std.time.ns_per_us, &side_effect, @TypeOf(side_effect).mark);

    const path: [*:0]const u8 = "/wal-modeled-commit-backend";
    var wal = try WAL.open(path, .{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .commit_backend = .async_io,
        .model_commit_backend_completions = true,
    });
    defer wal.close();

    try std.testing.expectEqual(@as(u64, 1), try wal.append("async-modeled"));
    try std.testing.expectEqual(@as(u64, 50 * std.time.ns_per_us), runtime.clock().nowNs());
    try std.testing.expectEqual(@as(u32, 1), side_effect.count);

    const stats = wal.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 50 * std.time.ns_per_us), stats.total_commit_ns);
}

test "wal modeled storage commit delay uses injected virtual clock" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();
    var side_effect = struct {
        count: u32 = 0,

        fn mark(ptr: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.count += 1;
        }
    }{};
    try runtime.schedule(1 * std.time.ns_per_ms, &side_effect, @TypeOf(side_effect).mark);

    const path: [*:0]const u8 = "/wal-modeled-commit-delay";
    var wal = try WAL.open(path, .{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .artificial_sync_delay_ns = 3 * std.time.ns_per_ms,
    });
    defer wal.close();

    try std.testing.expectEqual(@as(u64, 1), try wal.append("delayed"));
    try std.testing.expectEqual(@as(u64, 3 * std.time.ns_per_ms), runtime.clock().nowNs());
    try std.testing.expectEqual(@as(u32, 1), side_effect.count);

    const stats = wal.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 3 * std.time.ns_per_ms), stats.total_commit_ns);
    try std.testing.expectEqual(@as(u64, 3 * std.time.ns_per_ms), stats.total_wait_ns);
}

test "wal crash publish phases survive reopen" {
    if (!zig_lmdb.is_zig_backend) return;
    const allocator = std.testing.allocator;
    try runWalCrashCase(allocator, .{}, "crash-default", 0xA17F_3001, 6);
    try runWalCrashCase(allocator, .{ .commit_backend = .async_io }, "crash-async-io", 0xA17F_3002, 6);
}

test "wal replay fixtures stay green" {
    try runWalReplayFixtures(std.testing.allocator);
}

test "wal sim soak stays green" {
    if (!storage_sim_soak) return;
    try runWalSoak(std.testing.allocator);
}

test "wal append and last lsn" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{});
    defer wal.close();

    try std.testing.expectEqual(@as(u64, 0), wal.lastLsn());

    const lsn1 = try wal.append("hello");
    try std.testing.expectEqual(@as(u64, 1), lsn1);
    try std.testing.expectEqual(@as(u64, 1), wal.lastLsn());

    const lsn2 = try wal.append("world");
    try std.testing.expectEqual(@as(u64, 2), lsn2);
    try std.testing.expectEqual(@as(u64, 2), wal.lastLsn());
}

test "wal append batch assigns contiguous lsns" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{});
    defer wal.close();

    const batch = [_][]const u8{ "alpha", "beta", "gamma" };
    const result = try wal.appendBatch(&batch);

    try std.testing.expectEqual(@as(u64, 1), result.first_lsn);
    try std.testing.expectEqual(@as(usize, 3), result.count);
    try std.testing.expectEqual(@as(u64, 1), result.lsnAt(0));
    try std.testing.expectEqual(@as(u64, 3), result.lastLsn().?);
    try std.testing.expectEqual(@as(u64, 3), wal.lastLsn());

    const entries = try wal.iterateFrom(alloc, 1);
    defer {
        for (entries) |entry| alloc.free(@constCast(entry.data));
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 3), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].lsn);
    try std.testing.expectEqualStrings("alpha", entries[0].data);
    try std.testing.expectEqual(@as(u64, 2), entries[1].lsn);
    try std.testing.expectEqualStrings("beta", entries[1].data);
    try std.testing.expectEqual(@as(u64, 3), entries[2].lsn);
    try std.testing.expectEqualStrings("gamma", entries[2].data);
}

test "wal append batch and single append maintain lsn continuity across reopen" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    {
        var wal = try WAL.open(path, .{});
        defer wal.close();

        const batch = [_][]const u8{ "one", "two" };
        const result = try wal.appendBatch(&batch);
        try std.testing.expectEqual(@as(u64, 1), result.first_lsn);
        try std.testing.expectEqual(@as(u64, 2), result.lastLsn().?);

        const lsn3 = try wal.append("three");
        try std.testing.expectEqual(@as(u64, 3), lsn3);
    }

    {
        var wal = try WAL.open(path, .{});
        defer wal.close();

        try std.testing.expectEqual(@as(u64, 3), wal.lastLsn());

        const batch = [_][]const u8{ "four", "five" };
        const result = try wal.appendBatch(&batch);
        try std.testing.expectEqual(@as(u64, 4), result.first_lsn);
        try std.testing.expectEqual(@as(u64, 5), result.lastLsn().?);

        const entries = try wal.iterateFrom(alloc, 1);
        defer {
            for (entries) |entry| alloc.free(@constCast(entry.data));
            alloc.free(entries);
        }

        try std.testing.expectEqual(@as(usize, 5), entries.len);
        try std.testing.expectEqualStrings("one", entries[0].data);
        try std.testing.expectEqualStrings("two", entries[1].data);
        try std.testing.expectEqualStrings("three", entries[2].data);
        try std.testing.expectEqualStrings("four", entries[3].data);
        try std.testing.expectEqualStrings("five", entries[4].data);
    }
}

test "wal append batch supports large entries" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{});
    defer wal.close();

    const large = try alloc.alloc(u8, large_entry_threshold * 2);
    defer alloc.free(large);
    @memset(large, 'x');

    const batch = [_][]const u8{ "small", large };
    const result = try wal.appendBatch(&batch);
    try std.testing.expectEqual(@as(u64, 1), result.first_lsn);
    try std.testing.expectEqual(@as(u64, 2), result.lastLsn().?);

    const entries = try wal.iterateFrom(alloc, 1);
    defer {
        for (entries) |entry| alloc.free(@constCast(entry.data));
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("small", entries[0].data);
    try std.testing.expectEqualStrings(large, entries[1].data);
}

test "wal stats track append and commit buckets" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();

    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .group_commit_window_ns = 1 * std.time.ns_per_ms,
        .model_commit_backend_completions = true,
    });
    defer wal.close();

    _ = try wal.append("one");
    const batch = [_][]const u8{ "two", "three" };
    _ = try wal.appendBatch(&batch);

    const stats = wal.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 1), stats.append_calls);
    try std.testing.expectEqual(@as(u64, 1), stats.append_batch_calls);
    try std.testing.expectEqual(@as(u64, 3), stats.logical_entries);
    try std.testing.expectEqual(@as(u64, 2), stats.physical_commits);
    try std.testing.expect(stats.total_wait_ns > 0);
}

test "wal post-commit completion failure does not reuse durable lsn" {
    const FailingCompletion = struct {
        calls: u32 = 0,

        fn scheduler(self: *@This()) storage_sim.CompletionScheduler {
            return .{
                .ctx = self,
                .wait_ns_fn = wait,
            };
        }

        fn wait(ctx: ?*anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(ctx.?));
            self.calls += 1;
            if (self.calls == 1) return error.InjectedCompletionFailure;
        }
    };

    var failing_completion = FailingCompletion{};

    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{
        .backend = .lsm_memory,
        .commit_scheduler = failing_completion.scheduler(),
        .artificial_sync_delay_ns = 1,
    });
    defer wal.close();

    try std.testing.expectError(error.InjectedCompletionFailure, wal.append("first"));

    const second_lsn = try wal.append("second");
    try std.testing.expectEqual(@as(u64, 2), second_lsn);

    const entries = try wal.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(entry.data);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqual(@as(u64, 1), entries[0].lsn);
    try std.testing.expectEqualStrings("first", entries[0].data);
    try std.testing.expectEqual(@as(u64, 2), entries[1].lsn);
    try std.testing.expectEqualStrings("second", entries[1].data);
}

test "wal group commit coalesces concurrent appends" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    const path: [*:0]const u8 = "/wal-group-commit-coalesce";
    var wal = try WAL.open(path, .{
        .backend = .lsm_memory,
        .clock = runtime.clock(),
        .group_commit_window_ns = 10 * std.time.ns_per_ms,
    });
    defer wal.close();

    const alpha_entries = [_][]const u8{"alpha"};
    const beta_entries = [_][]const u8{"beta"};
    var request_a = AppendRequest{ .entries = &alpha_entries };
    var request_b = AppendRequest{ .entries = &beta_entries };

    lockAtomic(&wal.mutex);
    wal.stats.append_calls += 2;
    wal.stats.logical_entries += 2;
    wal.enqueueAppendRequestLocked(&request_a);
    wal.enqueueAppendRequestLocked(&request_b);
    wal.coordinator_active = true;
    wal.mutex.unlock();

    wal.driveAppendCoordinator();

    try std.testing.expect(request_a.done);
    try std.testing.expect(request_b.done);
    if (request_a.err) |err| return err;
    if (request_b.err) |err| return err;

    try std.testing.expectEqual(@as(u64, 1), request_a.result.first_lsn);
    try std.testing.expectEqual(@as(usize, 1), request_a.result.count);
    try std.testing.expectEqual(@as(u64, 2), request_b.result.first_lsn);
    try std.testing.expectEqual(@as(usize, 1), request_b.result.count);

    const stats = wal.statsSnapshot();
    try std.testing.expectEqual(@as(u64, 2), stats.append_calls);
    try std.testing.expectEqual(@as(u64, 2), stats.logical_entries);
    try std.testing.expectEqual(@as(u64, 1), stats.physical_commits);
    try std.testing.expectEqual(@as(u64, 1), stats.grouped_commits);
    try std.testing.expectEqual(@as(u64, 2), stats.grouped_requests);
    try std.testing.expectEqual(@as(u64, 2), stats.max_requests_per_commit);

    const entries = try wal.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(entry.data);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("alpha", entries[0].data);
    try std.testing.expectEqualStrings("beta", entries[1].data);
}

test "wal async-io group commit coalesces concurrent appends" {
    const StartBarrier = struct {
        mutex: std.atomic.Mutex = .unlocked,
        waiting: usize = 0,
        open: bool = false,

        fn wait(self: *@This(), total: usize) void {
            var registered = false;
            while (true) {
                lockAtomic(&self.mutex);
                if (!registered) {
                    self.waiting += 1;
                    registered = true;
                    if (self.waiting == total) self.open = true;
                }
                const ready = self.open;
                self.mutex.unlock();
                if (ready) return;
                std.Thread.yield() catch {};
            }
        }
    };

    const Worker = struct {
        wal: *WAL,
        barrier: *StartBarrier,
        payload: []const u8,
        result: u64 = 0,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            self.barrier.wait(2);
            self.result = self.wal.append(self.payload) catch |err| {
                self.err = err;
                return;
            };
        }
    };

    var attempt: usize = 0;
    while (attempt < 8) : (attempt += 1) {
        var buf: [256]u8 = undefined;
        const path = walTmpPath(&buf);
        defer cleanupWalDir(path);

        var wal = try WAL.open(path, .{
            .commit_backend = .async_io,
            .artificial_sync_delay_ns = 5 * std.time.ns_per_ms,
            .group_commit_window_ns = 50 * std.time.ns_per_ms,
        });
        defer wal.close();

        var barrier = StartBarrier{};
        var worker_a = Worker{ .wal = &wal, .barrier = &barrier, .payload = "alpha" };
        var worker_b = Worker{ .wal = &wal, .barrier = &barrier, .payload = "beta" };

        const thread_a = try std.Thread.spawn(.{}, Worker.run, .{&worker_a});
        const thread_b = try std.Thread.spawn(.{}, Worker.run, .{&worker_b});
        thread_a.join();
        thread_b.join();

        if (worker_a.err) |err| return err;
        if (worker_b.err) |err| return err;

        const min_lsn = @min(worker_a.result, worker_b.result);
        const max_lsn = @max(worker_a.result, worker_b.result);
        try std.testing.expectEqual(@as(u64, 1), min_lsn);
        try std.testing.expectEqual(@as(u64, 2), max_lsn);

        const stats = wal.statsSnapshot();
        try std.testing.expectEqual(@as(u64, 2), stats.append_calls);
        try std.testing.expectEqual(@as(u64, 2), stats.logical_entries);
        if (stats.grouped_commits > 0) {
            try std.testing.expectEqual(@as(u64, 1), stats.physical_commits);
            try std.testing.expectEqual(@as(u64, 1), stats.grouped_commits);
            try std.testing.expectEqual(@as(u64, 2), stats.grouped_requests);
            try std.testing.expect(stats.max_requests_per_commit >= 2);
            return;
        }
    }

    return error.TestExpectedEqual;
}

test "wal exposes underlying lmdb commit stats when available" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{});
    defer wal.close();

    _ = try wal.append("hello");

    const full = wal.fullStatsSnapshot();
    try std.testing.expectEqual(@as(u64, 1), full.wal.append_calls);
    if (full.commit) |commit| {
        try std.testing.expect(commit.publish_calls >= 1);
        try std.testing.expect(commit.full_publish_calls >= 1);
        try std.testing.expect(commit.page_images_written > 0);
        try std.testing.expect(commit.bytes_written > 0);
        try std.testing.expect(commit.total_publish_ns > 0);
    }
}

test "wal async-io commit backend survives repeated append and reopen" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    {
        var wal = try WAL.open(path, .{ .commit_backend = .async_io });
        defer wal.close();

        var i: usize = 0;
        while (i < 8) : (i += 1) {
            var payload_buf: [32]u8 = undefined;
            const payload = try std.fmt.bufPrint(&payload_buf, "entry-{d}", .{i});
            try std.testing.expectEqual(@as(u64, @intCast(i + 1)), try wal.append(payload));
        }
    }

    {
        var wal = try WAL.open(path, .{ .commit_backend = .async_io });
        defer wal.close();
        try std.testing.expectEqual(@as(u64, 8), wal.lastLsn());
    }
}

test "wal async-io survives concurrent append burst" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{ .commit_backend = .async_io });
    defer wal.close();

    const payload = try alloc.alloc(u8, 256);
    defer alloc.free(payload);
    @memset(payload, 'x');

    const StartBarrier = struct {
        mutex: std.atomic.Mutex = .unlocked,
        waiting: usize = 0,
        open: bool = false,

        fn wait(self: *@This(), total: usize) void {
            var registered = false;
            while (true) {
                lockAtomic(&self.mutex);
                if (!registered) {
                    self.waiting += 1;
                    registered = true;
                    if (self.waiting == total) self.open = true;
                }
                const ready = self.open;
                self.mutex.unlock();
                if (ready) return;
                std.Thread.yield() catch {};
            }
        }
    };

    const Worker = struct {
        wal: *WAL,
        barrier: *StartBarrier,
        payload: []const u8,
        appends: usize,
        err: ?anyerror = null,

        fn run(self: *@This(), total: usize) void {
            self.barrier.wait(total);
            var i: usize = 0;
            while (i < self.appends) : (i += 1) {
                _ = self.wal.append(self.payload) catch |err| {
                    self.err = err;
                    return;
                };
            }
        }
    };

    var barrier = StartBarrier{};
    var workers = [_]Worker{
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
    };

    var threads: [workers.len]std.Thread = undefined;
    for (&workers, 0..) |*worker, idx| {
        threads[idx] = try std.Thread.spawn(.{}, Worker.run, .{ worker, workers.len });
    }
    for (threads) |thread| thread.join();
    for (workers) |worker| {
        if (worker.err) |err| return err;
    }

    try std.testing.expectEqual(@as(u64, 256), wal.lastLsn());
}

test "wal async-io survives grouped concurrent append burst" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{
        .commit_backend = .async_io,
        .group_commit_window_ns = 10 * std.time.ns_per_ms,
    });
    defer wal.close();

    const payload = try alloc.alloc(u8, 256);
    defer alloc.free(payload);
    @memset(payload, 'x');

    const StartBarrier = struct {
        mutex: std.atomic.Mutex = .unlocked,
        waiting: usize = 0,
        open: bool = false,

        fn wait(self: *@This(), total: usize) void {
            var registered = false;
            while (true) {
                lockAtomic(&self.mutex);
                if (!registered) {
                    self.waiting += 1;
                    registered = true;
                    if (self.waiting == total) self.open = true;
                }
                const ready = self.open;
                self.mutex.unlock();
                if (ready) return;
                std.Thread.yield() catch {};
            }
        }
    };

    const Worker = struct {
        wal: *WAL,
        barrier: *StartBarrier,
        payload: []const u8,
        appends: usize,
        err: ?anyerror = null,

        fn run(self: *@This(), total: usize) void {
            self.barrier.wait(total);
            var i: usize = 0;
            while (i < self.appends) : (i += 1) {
                _ = self.wal.append(self.payload) catch |err| {
                    self.err = err;
                    return;
                };
            }
        }
    };

    var barrier = StartBarrier{};
    var workers = [_]Worker{
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
    };

    var threads: [workers.len]std.Thread = undefined;
    for (&workers, 0..) |*worker, idx| {
        threads[idx] = try std.Thread.spawn(.{}, Worker.run, .{ worker, workers.len });
    }
    for (threads) |thread| thread.join();
    for (workers) |worker| {
        if (worker.err) |err| return err;
    }

    try std.testing.expectEqual(@as(u64, 256), wal.lastLsn());
    const stats = wal.statsSnapshot();
    try std.testing.expect(stats.grouped_commits > 0);
}

test "wal async-io survives plain then grouped concurrent runs in one process" {
    const runBurst = struct {
        fn run(path: [*:0]const u8, grouped: bool) !void {
            const alloc = std.testing.allocator;
            var wal = try WAL.open(path, .{
                .commit_backend = .async_io,
                .group_commit_window_ns = if (grouped) 10 * std.time.ns_per_ms else 0,
            });
            defer wal.close();

            const payload = try alloc.alloc(u8, 256);
            defer alloc.free(payload);
            @memset(payload, 'x');

            const StartBarrier = struct {
                mutex: std.atomic.Mutex = .unlocked,
                waiting: usize = 0,
                open: bool = false,

                fn wait(self: *@This(), total: usize) void {
                    var registered = false;
                    while (true) {
                        lockAtomic(&self.mutex);
                        if (!registered) {
                            self.waiting += 1;
                            registered = true;
                            if (self.waiting == total) self.open = true;
                        }
                        const ready = self.open;
                        self.mutex.unlock();
                        if (ready) return;
                        std.Thread.yield() catch {};
                    }
                }
            };

            const Worker = struct {
                wal: *WAL,
                barrier: *StartBarrier,
                payload: []const u8,
                appends: usize,
                err: ?anyerror = null,

                fn run(self: *@This(), total: usize) void {
                    self.barrier.wait(total);
                    var i: usize = 0;
                    while (i < self.appends) : (i += 1) {
                        _ = self.wal.append(self.payload) catch |err| {
                            self.err = err;
                            return;
                        };
                    }
                }
            };

            var barrier = StartBarrier{};
            var workers = [_]Worker{
                .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
                .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
                .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
                .{ .wal = &wal, .barrier = &barrier, .payload = payload, .appends = 64 },
            };

            var threads: [workers.len]std.Thread = undefined;
            for (&workers, 0..) |*worker, idx| {
                threads[idx] = try std.Thread.spawn(.{}, Worker.run, .{ worker, workers.len });
            }
            for (threads) |thread| thread.join();
            for (workers) |worker| {
                if (worker.err) |err| return err;
            }
        }
    }.run;

    var plain_buf: [256]u8 = undefined;
    const plain_path = walTmpPath(&plain_buf);
    defer cleanupWalDir(plain_path);
    try runBurst(plain_path, false);

    var grouped_buf: [256]u8 = undefined;
    const grouped_path = walTmpPath(&grouped_buf);
    defer cleanupWalDir(grouped_path);
    try runBurst(grouped_path, true);
}

test "wal truncate" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{});
    defer wal.close();

    _ = try wal.append("a");
    _ = try wal.append("b");
    _ = try wal.append("c");

    try wal.truncate(2);

    // After truncation, only LSN 3 should remain
    try std.testing.expectEqual(@as(u64, 3), wal.lastLsn());
}

test "wal reopen preserves state" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    {
        var w = try WAL.open(path, .{});
        defer w.close();
        _ = try w.append("first");
        _ = try w.append("second");
    }

    {
        var w = try WAL.open(path, .{});
        defer w.close();
        try std.testing.expectEqual(@as(u64, 2), w.lastLsn());
        const lsn3 = try w.append("third");
        try std.testing.expectEqual(@as(u64, 3), lsn3);
    }
}

test "wal truncate then reopen keeps monotonic lsns" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    {
        var w = try WAL.open(path, .{});
        defer w.close();
        _ = try w.append("first");
        try w.truncate(1);
    }

    {
        var w = try WAL.open(path, .{});
        defer w.close();
        const lsn = try w.append("second");
        try std.testing.expectEqual(@as(u64, 2), lsn);
        try std.testing.expectEqual(@as(u64, 2), w.lastLsn());
    }
}

test "wal iterate from" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var w = try WAL.open(path, .{});
    defer w.close();

    _ = try w.append("alpha");
    _ = try w.append("beta");
    _ = try w.append("gamma");

    // Iterate from LSN 2
    const entries = try w.iterateFrom(alloc, 2);
    defer {
        for (entries) |e| alloc.free(@constCast(e.data));
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqual(@as(u64, 2), entries[0].lsn);
    try std.testing.expectEqualStrings("beta", entries[0].data);
    try std.testing.expectEqual(@as(u64, 3), entries[1].lsn);
    try std.testing.expectEqualStrings("gamma", entries[1].data);
}

test "wal truncate then iterate" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var w = try WAL.open(path, .{});
    defer w.close();

    _ = try w.append("a");
    _ = try w.append("b");
    _ = try w.append("c");

    try w.truncate(2);

    // Only LSN 3 should remain
    const entries = try w.iterateFrom(alloc, 1);
    defer {
        for (entries) |e| alloc.free(@constCast(e.data));
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqual(@as(u64, 3), entries[0].lsn);
    try std.testing.expectEqualStrings("c", entries[0].data);
}

test "wal streaming iteration" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var w = try WAL.open(path, .{});
    defer w.close();

    _ = try w.append("alpha");
    _ = try w.append("beta");
    _ = try w.append("gamma");

    // Streaming from LSN 2
    const StreamCtx = struct {
        var lsns: [3]u64 = undefined;
        var count: usize = 0;
        fn cb(entry: WalEntry) anyerror!WAL.ScanAction {
            lsns[count] = entry.lsn;
            count += 1;
            return .@"continue";
        }
    };
    StreamCtx.count = 0;

    try w.iterateFromStreaming(2, &StreamCtx.cb);
    try std.testing.expectEqual(@as(usize, 2), StreamCtx.count);
    try std.testing.expectEqual(@as(u64, 2), StreamCtx.lsns[0]);
    try std.testing.expectEqual(@as(u64, 3), StreamCtx.lsns[1]);

    // Verify matches non-streaming
    const entries = try w.iterateFrom(alloc, 2);
    defer {
        for (entries) |e| alloc.free(@constCast(e.data));
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
}

test "wal supports large entries" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var w = try WAL.open(path, .{});
    defer w.close();

    const payload = try alloc.alloc(u8, large_entry_threshold * 2);
    defer alloc.free(payload);
    @memset(payload, 'x');

    const lsn = try w.append(payload);
    try std.testing.expectEqual(@as(u64, 1), lsn);

    const entries = try w.iterateFrom(alloc, 1);
    defer {
        for (entries) |e| alloc.free(@constCast(e.data));
        alloc.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings(payload, entries[0].data);
}

test "wal iterateFrom replays many large entries after reopen" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    const entry_count = 256;
    const payload = try alloc.alloc(u8, large_entry_threshold * 2);
    defer alloc.free(payload);
    for (payload, 0..) |*byte, idx| byte.* = @intCast(idx % 251);

    {
        var w = try WAL.open(path, .{});
        defer w.close();

        var i: usize = 0;
        while (i < entry_count) : (i += 1) {
            payload[0] = @intCast(i % 251);
            _ = try w.append(payload);
        }
    }

    {
        var w = try WAL.open(path, .{});
        defer w.close();

        const entries = try w.iterateFrom(alloc, 129);
        defer {
            for (entries) |e| alloc.free(@constCast(e.data));
            alloc.free(entries);
        }

        try std.testing.expectEqual(@as(usize, entry_count - 128), entries.len);
        try std.testing.expectEqual(@as(u64, 129), entries[0].lsn);
        try std.testing.expectEqual(@as(u64, entry_count), entries[entries.len - 1].lsn);
        try std.testing.expectEqual(@as(usize, payload.len), entries[0].data.len);
        try std.testing.expectEqual(@as(usize, payload.len), entries[entries.len - 1].data.len);
        try std.testing.expectEqual(@as(u8, 128 % 251), entries[0].data[0]);
        try std.testing.expectEqual(@as(u8, (entry_count - 1) % 251), entries[entries.len - 1].data[0]);
    }
}

test "wal replay skips corrupt tail entries" {
    const alloc = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    {
        var w = try WAL.open(path, .{});
        defer w.close();

        _ = try w.append("alpha");
        _ = try w.append("beta");

        var txn = try w.beginWriteTxn();
        errdefer txn.abort();
        const corrupt_key = std.mem.toBytes(std.mem.nativeToBig(u64, 2));
        try txn.put(&corrupt_key, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 });
        try txn.commit();
    }

    {
        var w = try WAL.open(path, .{});
        defer w.close();

        const entries = try w.iterateFrom(alloc, 1);
        defer {
            for (entries) |e| alloc.free(@constCast(e.data));
            alloc.free(entries);
        }

        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqual(@as(u64, 1), entries[0].lsn);
        try std.testing.expectEqualStrings("alpha", entries[0].data);

        const Recorder = struct {
            lsns: std.ArrayListUnmanaged(u64) = .empty,
            var recorder: @This() = .{};

            fn callback(entry: WalEntry) !WAL.ScanAction {
                try recorder.lsns.append(std.testing.allocator, entry.lsn);
                try std.testing.expectEqualStrings("alpha", entry.data);
                return .@"continue";
            }
        };

        Recorder.recorder = .{};
        defer Recorder.recorder.lsns.deinit(alloc);
        try w.iterateFromStreaming(1, Recorder.callback);
        try std.testing.expectEqual(@as(usize, 1), Recorder.recorder.lsns.items.len);
        try std.testing.expectEqual(@as(u64, 1), Recorder.recorder.lsns.items[0]);
    }
}

test "decodeWalValue rejects oversized declared payload length" {
    var value = [_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 };
    value[0..4].* = std.mem.toBytes(std.mem.nativeToLittle(u32, std.math.maxInt(u32)));
    try std.testing.expectError(error.CorruptWal, decodeWalValue(&value));
}

test "wal backend adapters expose txn cursor operations" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{});
    defer wal.close();

    const key = std.mem.toBytes(std.mem.nativeToBig(u64, 7));

    {
        var txn = try wal.beginWriteTxn();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put(&key, "value");
        var cur = try write.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings(&key, (try cur.start(.{})).?.key);
        try write.commit();
    }

    {
        var txn = try wal.beginReadTxn();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("value", try read.get(&key));
    }
}

test "wal backend store opens concrete txn handles" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{});
    defer wal.close();

    const key = std.mem.toBytes(std.mem.nativeToBig(u64, 8));
    var backend = wal.backendStore();
    try std.testing.expectEqual(backend_types.WriteBatchMode.atomic, backend.capabilities().write_batches);

    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put(&key, "value2");
        try write.commit();
    }

    {
        var txn = try backend.beginRead();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("value2", try read.get(&key));
    }

    {
        var batch = try backend.beginBatch();
        errdefer batch.abort();
        var write = batch.writeAdapter();
        try write.put(&key, "value3");
        try write.commit();
    }
}

test "wal can use lsm backend for append reopen and truncate" {
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);

    {
        var wal = try WAL.open(path, .{ .backend = .lsm });
        defer wal.close();

        try std.testing.expectEqual(StorageBackend.lsm, (WalOptions{ .backend = .lsm }).resolvedBackend());
        try std.testing.expectEqual(@as(u64, 1), try wal.append("alpha"));
        const batch = try wal.appendBatch(&.{ "beta", "gamma" });
        try std.testing.expectEqual(@as(u64, 2), batch.first_lsn);
        try std.testing.expectEqual(@as(usize, 2), batch.count);
        try wal.truncate(2);
    }

    {
        var wal = try WAL.open(path, .{ .backend = .lsm });
        defer wal.close();

        try std.testing.expectEqual(@as(u64, 3), wal.lastLsn());
        const entries = try wal.iterateFrom(std.testing.allocator, 1);
        defer {
            for (entries) |entry| std.testing.allocator.free(@constCast(entry.data));
            std.testing.allocator.free(entries);
        }

        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqual(@as(u64, 3), entries[0].lsn);
        try std.testing.expectEqualStrings("gamma", entries[0].data);
    }
}

test "wal can use in-memory lsm backend without creating durable state" {
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "lsm-memory");

    var wal = try WAL.open(path, .{ .backend = .lsm_memory });
    defer wal.close();

    try std.testing.expectEqual(@as(u64, 1), try wal.append("alpha"));
    const entries = try wal.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(@constCast(entry.data));
        std.testing.allocator.free(entries);
    }

    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("alpha", entries[0].data);
    try wal.truncate(1);
    const remaining = try wal.iterateFrom(std.testing.allocator, 1);
    defer std.testing.allocator.free(remaining);
    try std.testing.expectEqual(@as(usize, 0), remaining.len);

    try std.testing.expectEqual(StorageBackend.lsm, (WalOptions{}).resolvedBackend());
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().openDir(std.testing.io, std.mem.span(path), .{}));
}

test "wal routes lsm profile options" {
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "lsm-options");

    var wal = try WAL.open(path, .{
        .backend = .lsm_memory,
        .lsm_options = .{ .flush_threshold = 37 },
    });
    defer wal.close();

    switch (wal.store_owner) {
        .lsm => |handle| try std.testing.expectEqual(@as(usize, 37), handle.backend.options.flush_threshold),
        else => return error.TestUnexpectedResult,
    }
}

fn cleanupWalDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}
