// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Generic write-ahead log (WAL) over ordered durable storage.
//!
//! A reusable, append-only log with LSN ordering, CRC32 integrity,
//! truncation, and replay iteration. Knows nothing about search — just
//! appends opaque byte entries. Usable for search persistence, Raft
//! consensus log, KV storage, or any ordered-write-ahead pattern.
//!
//! Key encoding: LSN as u64 big-endian for lexicographic ordering.
//! Value format: [data_len: u32 LE][data: ...][CRC32: u32 LE]
//! CRC covers data_len + data bytes.

const std = @import("std");
const Crc32 = @import("antfly_hash").Crc32;
const builtin = @import("builtin");
const build_options = @import("build_options");
const Allocator = std.mem.Allocator;
const fs_paths = @import("antfly_runtime_fs").fs_paths;
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const lsm_backend = @import("lsm_backend/mod.zig");
const lsm_storage = @import("lsm_backend/storage_io.zig");
const platform = @import("antfly_platform");
const platform_time = @import("antfly_platform").time;
const storage_sim = @import("sim_runtime.zig");
const sim_fixture = @import("sim_fixture.zig");
const wal_sim_fixture = @import("wal_sim_fixture.zig");
const zig_lmdb = if (builtin.is_test) @import("lmdb_engine") else struct {
    pub const storage_sim_soak = false;
    pub const is_zig_backend = false;
};
const large_entry_threshold = 4096;
const storage_sim_soak = zig_lmdb.storage_sim_soak;
var wal_tmp_nonce: u64 = 0;
// Volatile owner identities are unique within this process, including across
// statically linked source copies (each copy has a different origin address).
var recovery_owner_counter: std.atomic.Value(u64) = .init(1);

fn allocateRecoveryOwner(counter: *std.atomic.Value(u64)) !u64 {
    var current = counter.load(.monotonic);
    while (true) {
        if (current == 0) return error.Overflow;
        if (counter.cmpxchgWeak(current, current +% 1, .monotonic, .monotonic)) |changed| current = changed else return current;
    }
}

/// An ephemeral position in one open WAL. Never persist it across process
/// lifetimes. origin/owner are opaque identities, not dereferenceable handles.
pub const RecoveryPosition = extern struct {
    origin: u64 = 0,
    owner: u64 = 0,
    revision: u64 = 0,
    next_lsn: u64 = 0,

    pub fn valid(self: @This()) bool {
        return self.origin != 0 and self.owner != 0 and self.revision != 0 and self.next_lsn != 0;
    }
};

pub const RecoveryAppendResult = union(enum) {
    changed,
    busy,
    appended: u64,
};

fn lsmOpenDebugLogsEnabled() bool {
    return platform.env.getenv("ANTFLY_LSM_OPEN_DEBUG") != null;
}

fn nextWalTmpNonce() u64 {
    return @atomicRmw(u64, &wal_tmp_nonce, .Add, 1, .seq_cst);
}

pub const CommitStats = struct {};
pub const CommitBackend = enum { sync, worker_thread, async_io, adaptive };
pub const StorageBackend = enum {
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

/// Result of an append guarded by a durable idempotency key. The key and
/// digest live in the same transaction and namespace as the WAL entry, so a
/// crash can expose neither half of the publication boundary.
pub const IdempotentAppendResult = struct {
    lsn: u64,
    appended: bool,
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
    inner_segment_syncs: u64 = 0,
    inner_index_syncs: u64 = 0,
    post_commit_segment_syncs: u64 = 0,
    post_commit_index_syncs: u64 = 0,
};

pub const FullStats = struct {
    wal: WalStats,
    commit: ?CommitStats,
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
    commit_attempted: bool = false,
    inner_segment_syncs: u64 = 0,
    inner_index_syncs: u64 = 0,
    post_commit_segment_syncs: u64 = 0,
    post_commit_index_syncs: u64 = 0,
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
    encoded_entries: ?[]const []const u8 = null,
    result: BatchAppendResult = .{ .first_lsn = 0, .count = 0 },
    err: ?anyerror = null,
    done: bool = false,
};

const DurabilitySyncStats = struct {
    segment_syncs: u64 = 0,
    index_syncs: u64 = 0,
};

const default_namespace: backend_types.Namespace = .{};

const StoreOwner = union(enum) {
    lsm: lsm_backend.BackendHandle,

    fn close(self: *StoreOwner, _: Allocator) void {
        switch (self.*) {
            .lsm => |*handle| handle.close(),
        }
        self.* = undefined;
    }

    fn abandonAfterCrash(self: *StoreOwner, _: Allocator) void {
        switch (self.*) {
            .lsm => |*handle| handle.abandonAfterCrash(),
        }
        self.* = undefined;
    }

    pub fn sync(self: *StoreOwner, force: bool) !void {
        switch (self.*) {
            .lsm => |*handle| try handle.backend.sync(force),
        }
    }

    fn checkpointLsmWalAfterDurableBoundary(self: *StoreOwner) !void {
        switch (self.*) {
            .lsm => |*handle| try handle.backend.checkpointWalAfterDurableBoundary(),
        }
    }

    fn pinNativeCheckpoint(self: *StoreOwner) !lsm_backend.Backend.NativeCheckpoint {
        return switch (self.*) {
            .lsm => |*handle| try handle.backend.pinNativeCheckpoint(),
        };
    }

    fn syncCommitDurability(self: *StoreOwner) !DurabilitySyncStats {
        return switch (self.*) {
            // The LSM transaction has already appended and fsynced its replay
            // WAL. Sync that durable boundary without forcing an unrelated
            // memtable flush and compaction build on every outer WAL append.
            .lsm => |*handle| blk: {
                const stats = try handle.backend.syncReplayStateWithStats();
                break :blk .{
                    .segment_syncs = stats.segment_syncs,
                    .index_syncs = stats.index_syncs,
                };
            },
        };
    }

    fn commitProvidesDurability(self: *const StoreOwner) bool {
        return switch (self.*) {
            .lsm => |*handle| handle.backend.commitProvidesDurability(),
        };
    }

    fn lsmWriteStatsSnapshot(self: *const StoreOwner) ?lsm_backend.Backend.WriteStats {
        return switch (self.*) {
            .lsm => |*handle| handle.backend.snapshotWriteStats(),
        };
    }

    fn commitStatsSnapshot(self: *StoreOwner) ?CommitStats {
        return switch (self.*) {
            .lsm => null,
        };
    }

    fn runtimeNamespaceStore(self: StoreOwner, allocator: Allocator) !backend_erased.NamespaceStore {
        return switch (self) {
            .lsm => |handle| try handle.backend.runtimeNamespaceStore(allocator),
        };
    }
};

pub const WAL = struct {
    /// Must outlive this WAL and every joined backend operation.
    allocator: Allocator = std.heap.page_allocator,
    store: backend_erased.NamespaceStore,
    store_owner: StoreOwner,
    next_lsn: u64,
    recovery_owner: u64 = 0,
    destructive_revision: std.atomic.Value(u64) = .init(1),
    // A failed storage commit can have applied data before reporting failure.
    // Reopen must reconstruct the durable cursor before any further mutation.
    recovery_poisoned: std.atomic.Value(bool) = .init(false),
    group_commit_window_ns: u64,
    group_commit_max_requests: usize,
    commit_completion_delay_ns: u64,
    sync_after_commit: bool,
    clock: storage_sim.Clock,
    commit_scheduler: storage_sim.CompletionScheduler,
    // WAL's synchronous API has no scheduling dependency. This context parks
    // callers without starting executor workers; all condition users share it.
    sync_io: std.Io = if (@import("builtin").os.tag == .freestanding)
        .failing
    else
        std.Io.Threaded.global_single_threaded.io(),
    mutex: std.Io.Mutex = .init,
    completed: std.Io.Condition = .init,
    coordinator_active: bool = false,
    pending_head: ?*AppendRequest = null,
    pending_tail: ?*AppendRequest = null,
    stats: WalStats = .{},

    owned_read_mutex: std.atomic.Mutex = .unlocked,
    owned_read_closing: bool = false,
    owned_read_starting: usize = 0,
    owned_read_retiring: usize = 0,
    owned_reads: [owned_read_capacity]OwnedReadSlot = @splat(.{}),

    pub const owned_read_capacity: usize = 16;
    const ReadPort = @import("../common/read_admission.zig").Port;
    const OwnedReadSlot = struct {
        owner: ?*WAL = null,
        operation: ?*OwnedRead = null,
        starting: bool = false,
        claimed: bool = false,
        retiring: bool = false,
    };

    /// Caller-allocator-owned handle. WAL close retires its data and snapshot,
    /// detaches the parent, and leaves this closed handle safe to destroy. The
    /// supplied allocator context must outlive the handle, independently of WAL.
    pub const OwnedRead = struct {
        pub const Status = enum { pending, ready };
        pub const Chunk = struct { len: usize, done: bool };
        const Phase = enum { lookup, header, checksum, ready, failed, closed };
        allocator: Allocator,
        mutex: std.atomic.Mutex = .unlocked,
        closing: std.atomic.Value(bool) = .init(false),
        slot: ?*OwnedReadSlot = null,
        lookup: ?backend_erased.OwnedValueRead = null,
        value: ?backend_erased.OwnedValue = null,
        phase: Phase = .lookup,
        failure: ?anyerror = null,
        data_len: usize = 0,
        checksum_offset: usize = 0,
        crc: Crc32 = .init(),
        expected_crc: u32 = 0,

        pub fn step(self: *@This(), quantum: usize, control: ReadPort) !Status {
            if (quantum == 0 or quantum > 64 * 1024) return error.InvalidArgument;
            if (self.closing.load(.acquire)) return error.TransactionClosed;
            if (!self.mutex.tryLock()) return error.Backpressured;
            defer self.mutex.unlock();
            if (self.closing.load(.acquire)) return error.TransactionClosed;
            if (self.failure) |err| return err;
            return self.advance(quantum, control) catch |err| {
                const cause = blk: {
                    control.checkpoint() catch |original| break :blk original;
                    break :blk err;
                };
                self.retire();
                self.phase = .failed;
                self.failure = cause;
                return cause;
            };
        }

        fn advance(self: *@This(), quantum: usize, control: ReadPort) !Status {
            try control.checkpoint();
            switch (self.phase) {
                .lookup => if (try self.lookup.?.step(quantum, control) == .ready) {
                    self.value = try self.lookup.?.takeOutput();
                    self.lookup = null;
                    self.phase = .header;
                },
                .header => {
                    const bytes = self.value.?.bytes();
                    if (bytes.len < 8) return error.CorruptWal;
                    self.data_len = std.mem.readInt(u32, bytes[0..4], .little);
                    const end = std.math.add(usize, 4, self.data_len) catch return error.CorruptWal;
                    const crc_end = std.math.add(usize, end, 4) catch return error.CorruptWal;
                    if (bytes.len < crc_end) return error.CorruptWal;
                    self.expected_crc = std.mem.readInt(u32, bytes[end..][0..4], .little);
                    self.phase = .checksum;
                },
                .checksum => {
                    const end = 4 + self.data_len;
                    const take = @min(quantum, end - self.checksum_offset);
                    self.crc.update(self.value.?.bytes()[self.checksum_offset..][0..take]);
                    self.checksum_offset += take;
                    if (self.checksum_offset == end) {
                        if (self.crc.final() != self.expected_crc) return error.CorruptWal;
                        self.phase = .ready;
                    }
                },
                .ready => {},
                .closed => return error.TransactionClosed,
                .failed => return self.failure.?,
            }
            try control.checkpoint();
            return if (self.phase == .ready) .ready else .pending;
        }

        /// Copy at most one caller-funded window. No result or borrowed span
        /// escapes the operation lock, so close cannot invalidate caller bytes.
        pub fn readChunk(self: *@This(), offset: usize, out: []u8, control: ReadPort) !Chunk {
            if (out.len > 64 * 1024) return error.InvalidArgument;
            if (self.closing.load(.acquire)) return error.TransactionClosed;
            if (!self.mutex.tryLock()) return error.Backpressured;
            defer self.mutex.unlock();
            if (self.closing.load(.acquire)) return error.TransactionClosed;
            if (self.failure) |err| return err;
            if (self.phase != .ready or offset > self.data_len or (out.len == 0 and offset < self.data_len)) return error.InvalidArgument;
            return self.copyChunk(offset, out, control) catch |err| {
                self.retire();
                self.phase = .failed;
                self.failure = err;
                return err;
            };
        }

        fn copyChunk(self: *@This(), offset: usize, out: []u8, control: ReadPort) !Chunk {
            try control.checkpoint();
            const take = @min(out.len, self.data_len - offset);
            @memcpy(out[0..take], self.value.?.bytes()[4 + offset ..][0..take]);
            try control.checkpoint();
            return .{ .len = take, .done = offset + take == self.data_len };
        }

        fn retire(self: *@This()) void {
            if (self.lookup) |*lookup| lookup.deinit();
            self.lookup = null;
            if (self.value) |*value| value.deinit();
            self.value = null;
        }

        pub fn destroy(self: *@This()) void {
            self.closing.store(true, .release);
            while (true) {
                platform.sync.lockYielding(&self.mutex);
                if (self.slot) |slot| {
                    const parent = slot.owner.?;
                    platform.sync.lockYielding(&parent.owned_read_mutex);
                    if (slot.claimed) {
                        // Close owns this slot and needs the operation lock.
                        // Keep caller metadata/allocator alive until detachment.
                        parent.owned_read_mutex.unlock();
                        self.mutex.unlock();
                        platform.time.yieldBriefly();
                        continue;
                    }
                    slot.operation = null;
                    slot.retiring = true;
                    self.slot = null;
                    parent.owned_read_retiring += 1;
                    parent.owned_read_mutex.unlock();
                    self.retire();
                    // The parent cannot tear down store/allocator while this
                    // grant-free retirement is still using either of them.
                    platform.sync.lockYielding(&parent.owned_read_mutex);
                    slot.retiring = false;
                    parent.owned_read_retiring -= 1;
                    parent.owned_read_mutex.unlock();
                } else self.retire();
                const allocation = self.allocator;
                self.mutex.unlock();
                allocation.destroy(self);
                return;
            }
        }
    };

    pub fn beginOwnedRead(self: *WAL, allocator: Allocator, lsn: u64, control: ReadPort) !*OwnedRead {
        try control.checkpoint();
        if (lsn == 0) return error.InvalidLsn;
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.owned_read_mutex, control);
        if (self.owned_read_closing) {
            self.owned_read_mutex.unlock();
            return error.TransactionClosed;
        }
        if (self.store.vtable.begin_owned_value_read == null) {
            self.owned_read_mutex.unlock();
            return error.ExecutionWaitUnsupported;
        }
        const slot = for (&self.owned_reads) |*candidate| {
            if (candidate.operation == null and !candidate.starting and !candidate.claimed and !candidate.retiring) break candidate;
        } else {
            self.owned_read_mutex.unlock();
            return error.Backpressured;
        };
        slot.* = .{ .owner = self, .starting = true };
        self.owned_read_starting += 1;
        self.owned_read_mutex.unlock();
        var registered = false;
        defer {
            platform.sync.lockYielding(&self.owned_read_mutex);
            self.owned_read_starting -= 1;
            slot.starting = false;
            if (!registered) slot.operation = null;
            self.owned_read_mutex.unlock();
        }
        const operation = try allocator.create(OwnedRead);
        operation.* = .{ .allocator = allocator };
        errdefer {
            operation.retire();
            allocator.destroy(operation);
        }
        const key = std.mem.toBytes(std.mem.nativeToBig(u64, lsn));
        operation.lookup = try self.store.beginOwnedValueRead(allocator, default_namespace, &key, control);
        try control.checkpoint();
        try @import("../common/bounded_mutex.zig").lockAtomic(&self.owned_read_mutex, control);
        defer self.owned_read_mutex.unlock();
        if (self.owned_read_closing) return error.TransactionClosed;
        operation.slot = slot;
        slot.operation = operation;
        registered = true;
        return operation;
    }

    fn closeOwnedReads(self: *WAL) void {
        platform.sync.lockYielding(&self.owned_read_mutex);
        self.owned_read_closing = true;
        self.owned_read_mutex.unlock();
        while (true) {
            platform.sync.lockYielding(&self.owned_read_mutex);
            const slot = for (&self.owned_reads) |*candidate| {
                if (candidate.operation != null and !candidate.claimed) break candidate;
            } else null;
            if (slot) |claimed| {
                claimed.claimed = true;
                const operation = claimed.operation.?;
                operation.closing.store(true, .release);
                self.owned_read_mutex.unlock();
                platform.sync.lockYielding(&operation.mutex);
                operation.retire();
                operation.phase = .closed;
                operation.slot = null;
                platform.sync.lockYielding(&self.owned_read_mutex);
                claimed.operation = null;
                claimed.claimed = false;
                self.owned_read_mutex.unlock();
                // No further operation access after unlocking: caller destroy
                // can now free its independent metadata immediately.
                operation.mutex.unlock();
            } else {
                const finished = self.owned_read_starting == 0 and self.owned_read_retiring == 0;
                self.owned_read_mutex.unlock();
                if (finished) return;
                platform.time.yieldBriefly();
            }
        }
    }

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

        fn getWithAdmission(self: *Txn, key: []const u8, control: @import("../common/read_admission.zig").Port) ![]const u8 {
            return switch (self.inner) {
                .read => |*txn| try txn.getWithAdmission(default_namespace, key, control),
                .write => error.ReadOnlyTransaction,
            };
        }

        fn put(self: *Txn, key: []const u8, value: []const u8) !void {
            switch (self.inner) {
                .read => return error.ReadOnlyTransaction,
                .write => |*txn| try txn.put(default_namespace, key, value),
            }
        }

        pub fn appendPut(self: *Txn, key: []const u8, value: []const u8) !void {
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

    fn beginReadTxnWithAdmission(self: *WAL, control: @import("../common/read_admission.zig").Port) !Txn {
        return .{ .inner = .{ .read = try self.store.beginReadWithAdmission(control) } };
    }

    fn beginWriteTxn(self: *WAL) !Txn {
        return .{
            .inner = .{
                .write = try self.store.beginWrite(),
            },
        };
    }

    /// Open or create a WAL at the given path.
    pub fn open(path: [*:0]const u8, opts: WalOptions) !WAL {
        return openWithAllocator(std.heap.page_allocator, path, opts);
    }

    pub fn openWithAllocator(allocator: Allocator, path: [*:0]const u8, opts: WalOptions) !WAL {
        var store_owner = try openStoreOwner(allocator, path, opts);
        errdefer store_owner.close(allocator);

        var store = try store_owner.runtimeNamespaceStore(allocator);
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
        const sync_after_commit = !opts.no_sync and !store_owner.commitProvidesDurability();

        return .{
            .allocator = allocator,
            .store = store,
            .store_owner = store_owner,
            .next_lsn = next_lsn,
            .recovery_owner = try allocateRecoveryOwner(&recovery_owner_counter),
            .group_commit_window_ns = opts.group_commit_window_ns,
            .group_commit_max_requests = @max(@as(usize, 1), opts.group_commit_max_requests),
            .commit_completion_delay_ns = resolvedCommitCompletionDelayNs(opts),
            .sync_after_commit = sync_after_commit,
            .clock = opts.clock,
            .commit_scheduler = opts.commit_scheduler,
        };
    }

    pub fn close(self: *WAL) void {
        self.closeOwnedReads();
        self.store.deinit();
        self.store_owner.close(self.allocator);
        self.* = undefined;
    }

    /// Releases in-memory ownership without flushing or finalizing storage.
    /// Crash simulators must use this after rolling storage back to its last
    /// durable state; a normal close would let the stale pre-crash owner write
    /// into the recovered generation.
    pub fn abandonAfterCrash(self: *WAL) void {
        self.closeOwnedReads();
        self.store.deinit();
        self.store_owner.abandonAfterCrash(self.allocator);
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

    /// Owned WAL encoding funded before the caller's irreversible decision.
    /// The stable WAL and its allocator owner outlive this grant. Join publish
    /// before destroying it. No construction control or LSN is retained.
    pub const PreparedAppend = struct {
        allocator: Allocator,
        wal: *WAL,
        issuer: u64,
        encoded: []u8,
        prefix_len: usize,
        tail_crc: u32,
        shift: CrcShift,
        attempted: std.atomic.Value(bool) = .init(false),

        pub fn publish(self: *PreparedAppend, prefix: []const u8) !u64 {
            const encoded = try self.seal(prefix);
            var request: AppendRequest = .{
                .entries = &.{encoded[4 .. encoded.len - 4]},
                .encoded_entries = &.{encoded},
            };
            return (try self.wal.appendRequest(&request)).first_lsn;
        }

        fn seal(self: *PreparedAppend, prefix: []const u8) ![]const u8 {
            if (self.attempted.cmpxchgStrong(false, true, .acq_rel, .acquire) != null) return error.InvalidArgument;
            if (prefix.len != self.prefix_len or self.issuer == 0 or self.wal.recovery_owner != self.issuer) return error.InvalidArgument;
            @memcpy(self.encoded[4..][0..prefix.len], prefix);
            const crc = self.shift.apply(Crc32.hash(self.encoded[0 .. 4 + prefix.len])) ^ self.tail_crc;
            std.mem.writeInt(u32, self.encoded[self.encoded.len - 4 ..][0..4], crc, .little);
            return self.encoded;
        }

        pub fn deinit(self: *PreparedAppend) void {
            self.allocator.free(self.encoded);
            self.* = undefined;
        }
    };

    pub fn prepareAppend(self: *WAL, prefix_len: usize, tail: []const u8, control: ?@import("../common/read_admission.zig").Port) !PreparedAppend {
        if (control) |port| try port.checkpoint();
        if (prefix_len > 4096 or self.recovery_owner == 0) return error.InvalidArgument;
        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        const data_len = std.math.add(usize, prefix_len, tail.len) catch return error.InvalidArgument;
        const wire_len = std.math.cast(u32, data_len) orelse return error.InvalidArgument;
        const total = std.math.add(usize, data_len, 8) catch return error.InvalidArgument;
        const encoded = self.allocator.alloc(u8, total) catch |cause| {
            if (control) |port| try port.checkpoint();
            return cause;
        };
        errdefer self.allocator.free(encoded);
        std.mem.writeInt(u32, encoded[0..4], wire_len, .little);
        var crc = Crc32.init();
        var offset: usize = 0;
        while (offset < tail.len) {
            if (control) |port| try port.checkpoint();
            const end = offset + @min(tail.len - offset, 4096);
            @memcpy(encoded[4 + prefix_len + offset .. 4 + prefix_len + end], tail[offset..end]);
            crc.update(tail[offset..end]);
            offset = end;
        }
        const shift = try CrcShift.init(tail.len, control);
        if (control) |port| try port.checkpoint();
        return .{ .allocator = self.allocator, .wal = self, .issuer = self.recovery_owner, .encoded = encoded, .prefix_len = prefix_len, .tail_crc = crc.final(), .shift = shift };
    }

    /// Bootstrap an empty logical log under the same serialization as append.
    /// A completion error after commit must retain the committed append cursor.
    pub fn appendAt(self: *WAL, expected_lsn: u64, data: []const u8) !u64 {
        if (expected_lsn == 0) return error.InvalidArgument;
        _ = try std.math.add(u64, expected_lsn, 1);
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (self.coordinator_active or self.pending_head != null) self.completed.waitUncancelable(self.sync_io, &self.mutex);
        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        if (self.next_lsn != 1) return error.WalLsnMismatch;
        self.next_lsn = expected_lsn;
        errdefer if (self.next_lsn == expected_lsn) {
            self.next_lsn = 1;
        };
        return try self.commitSingleLocked(data, null);
    }

    /// Try one coherent position without waiting behind a writer/coordinator.
    /// The caller retires every retained dependency before retrying busy.
    pub fn tryRecoveryPosition(self: *WAL, original: ?@import("../common/read_admission.zig").Port) !?RecoveryPosition {
        var retained: @import("../common/read_admission.zig").RetainedDependency = .{};
        const control = retained.bind(original);
        if (control) |port| try port.checkpoint();
        if (!self.mutex.tryLock()) return null;
        defer self.mutex.unlock(self.sync_io);
        if (control) |port| try port.checkpoint();
        // The normal coordinator publishes next_lsn outside this mutex while
        // active. Never observe that mutable field during its commit.
        if (self.coordinator_active or self.pending_head != null) return null;
        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        const revision = self.destructive_revision.load(.acquire);
        if (revision == 0 or self.recovery_owner == 0) return error.Overflow;
        return .{ .origin = @intFromPtr(&recovery_owner_counter), .owner = self.recovery_owner, .revision = revision, .next_lsn = self.next_lsn };
    }

    /// Check the full absence witness and append while retaining writer
    /// serialization. No lifetime check may replace committed completion.
    pub fn appendIfRecoveryPosition(self: *WAL, expected: RecoveryPosition, data: []const u8, original: ?@import("../common/read_admission.zig").Port) !RecoveryAppendResult {
        if (!expected.valid()) return error.InvalidArgument;
        var retained: @import("../common/read_admission.zig").RetainedDependency = .{};
        const control = retained.bind(original);
        if (control) |port| try port.checkpoint();
        if (!self.mutex.tryLock()) return .busy;
        defer self.mutex.unlock(self.sync_io);
        if (control) |port| try port.checkpoint();
        if (self.coordinator_active or self.pending_head != null) return .busy;
        if (self.recovery_poisoned.load(.acquire)) return .changed;
        if (expected.origin != @intFromPtr(&recovery_owner_counter) or expected.owner != self.recovery_owner or
            expected.revision != self.destructive_revision.load(.acquire) or expected.next_lsn != self.next_lsn) return .changed;
        return .{ .appended = try self.commitSingleLocked(data, control) };
    }

    fn commitSingleLocked(self: *WAL, data: []const u8, control: ?@import("../common/read_admission.zig").Port) !u64 {
        var request: AppendRequest = .{ .entries = &.{data} };
        const batch: CommitBatch = .{ .head = &request, .tail = &request, .request_count = 1, .entry_count = 1 };
        self.stats.append_calls += 1;
        self.stats.logical_entries += 1;
        const attempt = self.commitAppendBatch(batch, control);
        self.recordCommitStatsLocked(batch, attempt.stats, 0);
        return switch (attempt.result) {
            .success => |result| result.first_lsn,
            .failure => |err| err,
        };
    }

    fn invalidateRecoveryPositionsLocked(self: *WAL) void {
        const old = self.destructive_revision.load(.monotonic);
        // Exhaustion permanently disables witnesses for this open owner.
        self.destructive_revision.store(if (old == 0 or old == std.math.maxInt(u64)) 0 else old + 1, .release);
    }

    /// Append multiple entries in one durable transaction. Returns the assigned
    /// contiguous LSN range.
    pub fn appendBatch(self: *WAL, entries: []const []const u8) !BatchAppendResult {
        if (entries.len == 0) {
            return .{ .first_lsn = self.next_lsn, .count = 0 };
        }

        var request = AppendRequest{ .entries = entries };
        return self.appendRequest(&request);
    }

    fn appendRequest(self: *WAL, request: *AppendRequest) !BatchAppendResult {
        const wait_started = self.nowNs();

        self.mutex.lockUncancelable(self.sync_io);
        if (self.recovery_poisoned.load(.acquire)) {
            self.mutex.unlock(self.sync_io);
            return error.CorruptWal;
        }

        if (request.entries.len == 1) {
            self.stats.append_calls += 1;
        } else {
            self.stats.append_batch_calls += 1;
        }
        self.stats.logical_entries += request.entries.len;

        self.enqueueAppendRequestLocked(request);
        const leader = if (!self.coordinator_active) blk: {
            self.coordinator_active = true;
            break :blk true;
        } else false;
        self.mutex.unlock(self.sync_io);

        if (leader) {
            self.driveAppendCoordinator();
        }

        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);

        while (!request.done) {
            self.completed.waitUncancelable(self.sync_io, &self.mutex);
        }

        self.stats.total_wait_ns += self.elapsedSince(wait_started);
        if (request.err) |err| return err;
        return request.result;
    }

    /// Append one entry exactly once for `idempotency_key`.
    ///
    /// A retry with the same digest returns the original LSN without appending.
    /// Reusing the key for different bytes fails closed. The compact key index
    /// is deliberately retained when old WAL payloads are truncated, preserving
    /// replay safety without retaining the unbounded receipt bodies.
    pub fn appendIdempotent(
        self: *WAL,
        idempotency_key: []const u8,
        digest: []const u8,
        data: []const u8,
    ) !IdempotentAppendResult {
        if (idempotency_key.len == 0 or idempotency_key.len > 1024 or digest.len == 0 or digest.len > 1024)
            return error.InvalidIdempotencyKey;
        const metadata_key = try idempotencyMetadataKeyAlloc(self.allocator, idempotency_key);
        defer self.allocator.free(metadata_key);

        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (self.coordinator_active or self.pending_head != null) {
            self.completed.waitUncancelable(self.sync_io, &self.mutex);
        }

        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        const lsn = committed: {
            var txn = try self.beginWriteTxn();
            errdefer txn.abort();
            if (txn.get(metadata_key)) |raw| {
                const existing = try decodeIdempotencyMetadata(raw);
                if (!std.mem.eql(u8, existing.digest, digest)) return error.IdempotencyConflict;
                txn.abort();
                return .{ .lsn = existing.lsn, .appended = false };
            } else |err| switch (err) {
                error.NotFound => {},
                else => return err,
            }

            const assigned_lsn = self.next_lsn;
            const next_lsn = try std.math.add(u64, assigned_lsn, 1);
            try putEncodedEntry(self.allocator, &txn, assigned_lsn, data);
            try putNextLsnMeta(&txn, next_lsn);
            const metadata = try encodeIdempotencyMetadataAlloc(self.allocator, digest, assigned_lsn);
            defer self.allocator.free(metadata);
            try txn.put(metadata_key, metadata);
            txn.commit() catch |err| {
                self.recovery_poisoned.store(true, .release);
                return err;
            };
            break :committed assigned_lsn;
        };
        self.next_lsn = lsn + 1;
        if (self.sync_after_commit) _ = try self.store_owner.syncCommitDurability();
        try self.waitForCommitCompletion(self.commit_completion_delay_ns);
        return .{ .lsn = lsn, .appended = true };
    }

    /// Test-only corruption hook used by higher-level durable protocols to
    /// prove that a missing/torn tail is detected rather than silently skipped.
    pub fn injectCorruptEntryForTest(self: *WAL, lsn: u64) !void {
        if (!builtin.is_test) return error.Unsupported;
        if (lsn == 0 or lsn > self.lastLsn()) return error.InvalidLsn;
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (self.coordinator_active or self.pending_head != null) {
            self.completed.waitUncancelable(self.sync_io, &self.mutex);
        }
        {
            var txn = try self.beginWriteTxn();
            errdefer txn.abort();
            const key = std.mem.toBytes(std.mem.nativeToBig(u64, lsn));
            try txn.put(&key, &[_]u8{ 0, 0, 0, 0, 0, 0, 0, 0 });
            self.invalidateRecoveryPositionsLocked();
            txn.commit() catch |err| {
                self.recovery_poisoned.store(true, .release);
                return err;
            };
        }
        if (self.sync_after_commit) _ = try self.store_owner.syncCommitDurability();
    }

    pub fn statsSnapshot(self: *WAL) WalStats {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        return self.stats;
    }

    pub fn commitStatsSnapshot(self: *WAL) ?CommitStats {
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

    /// Manifest the physical LSM generation which backs this logical WAL.
    /// Native capture uses the resulting immutable runs and deliberately omits
    /// the appendable physical WAL payloads.
    pub fn checkpointLsmWalAfterDurableBoundary(self: *WAL) !void {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (self.coordinator_active or self.pending_head != null) {
            self.completed.waitUncancelable(self.sync_io, &self.mutex);
        }
        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        try self.store_owner.checkpointLsmWalAfterDurableBoundary();
    }

    /// Retains an immutable physical generation after all logical WAL writes
    /// admitted before the caller's revision fence have completed.
    pub fn pinNativeCheckpoint(self: *WAL) !lsm_backend.Backend.NativeCheckpoint {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        // Native capture reaches this method only after backend-runtime replay
        // admission and the DB revision fence have drained prior append work.
        // Never spin or sleep an OS thread if that contract is violated.
        if (self.coordinator_active or self.pending_head != null)
            return error.WalAppendInProgress;
        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        return try self.store_owner.pinNativeCheckpoint();
    }

    /// Truncate all entries with LSN <= the given value.
    pub fn truncate(self: *WAL, up_to_lsn: u64) !void {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        while (self.coordinator_active or self.pending_head != null) self.completed.waitUncancelable(self.sync_io, &self.mutex);
        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();

        var to_delete = std.ArrayListUnmanaged([8]u8).empty;
        defer to_delete.deinit(self.allocator);

        {
            var cur = try txn.cursor();
            defer cur.close();

            var maybe_entry = try cur.first();
            if (maybe_entry == null) {
                txn.commit() catch |err| {
                    self.recovery_poisoned.store(true, .release);
                    return err;
                };
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
                try to_delete.append(self.allocator, key);

                maybe_entry = try cur.next();
            }
        }

        for (to_delete.items) |key| {
            txn.delete(&key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
        }

        if (to_delete.items.len != 0) self.invalidateRecoveryPositionsLocked();
        txn.commit() catch |err| {
            self.recovery_poisoned.store(true, .release);
            return err;
        };
    }

    /// Truncate all entries with LSN > the given value and move the append
    /// cursor back to keep_lsn + 1. Used by timeline rewind workflows that must
    /// discard divergent suffix records before following a promoted timeline.
    pub fn truncateAfter(self: *WAL, keep_lsn: u64) !void {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);

        while (self.coordinator_active or self.pending_head != null) {
            self.completed.waitUncancelable(self.sync_io, &self.mutex);
        }

        if (self.recovery_poisoned.load(.acquire)) return error.CorruptWal;
        const next_lsn = try std.math.add(u64, keep_lsn, 1);
        if (next_lsn >= self.next_lsn) return;

        var txn = try self.beginWriteTxn();
        errdefer txn.abort();

        var to_delete = std.ArrayListUnmanaged([8]u8).empty;
        defer to_delete.deinit(self.allocator);

        {
            var cur = try txn.cursor();
            defer cur.close();

            const start_key = std.mem.toBytes(std.mem.nativeToBig(u64, next_lsn));
            var maybe_entry = try cur.seekAtOrAfter(&start_key);
            while (maybe_entry) |entry| {
                if (entry.key.len != 8) break;

                var key: [8]u8 = undefined;
                @memcpy(&key, entry.key[0..8]);
                try to_delete.append(self.allocator, key);

                maybe_entry = try cur.next();
            }
        }

        for (to_delete.items) |key| {
            txn.delete(&key) catch |err| switch (err) {
                error.NotFound => continue,
                else => return err,
            };
        }
        try putNextLsnMeta(&txn, next_lsn);
        self.invalidateRecoveryPositionsLocked();
        txn.commit() catch |err| {
            self.recovery_poisoned.store(true, .release);
            return err;
        };
        self.next_lsn = next_lsn;
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

    /// Read one retained entry by exact LSN. Caller owns the returned data.
    pub fn readAt(self: *WAL, alloc: Allocator, lsn: u64) !?WalEntry {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        const key = std.mem.toBytes(std.mem.nativeToBig(u64, lsn));
        const value = txn.get(&key) catch |err| switch (err) {
            error.NotFound => return null,
            else => return err,
        };
        const decoded = decodeWalValue(value) catch |err| switch (err) {
            error.CorruptWal => return error.CorruptWal,
        };
        return .{
            .lsn = lsn,
            .data = try alloc.dupe(u8, decoded),
        };
    }

    /// Visit one exact, checksum-verified entry without copying its payload.
    /// Entry bytes are borrowed only through the synchronous callback. The
    /// callback must not retain them or reenter a write on this WAL while the
    /// read transaction is held. Callback failure still retires the snapshot.
    pub fn withEntryAt(self: *WAL, lsn: u64, context: anytype, comptime callback: fn (@TypeOf(context), WalEntry) anyerror!void) !bool {
        return self.withEntryAtWithAdmission(lsn, null, context, callback);
    }

    /// Bounded checksum quanta with lifetime checks around snapshot access.
    /// Namespace snapshot creation/get still need physical admission. A snapshot
    /// is a retained dependency: preserve lifetime without transferring CPU.
    pub fn withEntryAtWithAdmission(self: *WAL, lsn: u64, original: ?@import("../common/read_admission.zig").Port, context: anytype, comptime callback: fn (@TypeOf(context), WalEntry) anyerror!void) !bool {
        var retained: @import("../common/read_admission.zig").RetainedDependency = .{};
        const control = retained.bind(original);
        if (control) |port| try port.checkpoint();
        var txn = if (control) |port| try self.beginReadTxnWithAdmission(port) else try self.beginReadTxn();
        defer txn.abort();
        if (control) |port| try port.checkpoint();
        const key = std.mem.toBytes(std.mem.nativeToBig(u64, lsn));
        const fetched = if (control) |port| txn.getWithAdmission(&key, port) else txn.get(&key);
        const value = fetched catch |err| switch (err) {
            error.NotFound => {
                if (control) |port| try port.checkpoint();
                return false;
            },
            else => return err,
        };
        const data = try decodeWalValueWithAdmission(value, control);
        if (control) |port| try port.checkpoint();
        try callback(context, .{ .lsn = lsn, .data = data });
        if (control) |port| try port.checkpoint();
        return true;
    }

    /// Import ScanAction from docstore for consistent API.
    pub const ScanAction = @import("docstore.zig").DocStore.ScanAction;

    /// Streaming callback-based iteration from a given LSN. Constant memory.
    /// Callback receives borrowed entry data (valid only during the call).
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

    /// Thread-safe snapshot for compiled owners whose callers cannot borrow
    /// the in-process WAL fields directly.
    pub fn lastLsnSnapshot(self: *WAL) u64 {
        self.mutex.lockUncancelable(self.sync_io);
        defer self.mutex.unlock(self.sync_io);
        return self.lastLsn();
    }

    /// Synchronous snapshot under the original caller's authority. Progress
    /// occurs only before acquisition; rejection racing acquisition releases it.
    pub fn lastLsnSnapshotWithAdmission(self: *WAL, control: @import("../common/read_admission.zig").Port) !u64 {
        try @import("../common/bounded_mutex.zig").lock(&self.mutex, self.sync_io, control);
        defer self.mutex.unlock(self.sync_io);
        const lsn = self.lastLsn();
        try control.checkpoint();
        return lsn;
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
            self.mutex.lockUncancelable(self.sync_io);
            const should_wait = self.shouldCoalesceWaitLocked();
            const wait_ns = if (should_wait) self.effectiveCoalesceWindowNsLocked() else 0;
            self.mutex.unlock(self.sync_io);

            if (wait_ns > 0) {
                const coalesce_started = self.nowNs();
                self.sleepNs(wait_ns);
                coalesce_ns = self.elapsedSince(coalesce_started);
            }

            self.mutex.lockUncancelable(self.sync_io);
            const batch = self.drainPendingRequestsLocked();
            self.mutex.unlock(self.sync_io);

            if (batch.head == null) {
                self.mutex.lockUncancelable(self.sync_io);
                self.coordinator_active = false;
                self.completed.broadcast(self.sync_io);
                self.mutex.unlock(self.sync_io);
                return;
            }

            const attempt = self.commitAppendBatch(batch, null);

            self.mutex.lockUncancelable(self.sync_io);
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
            self.completed.broadcast(self.sync_io);
            self.mutex.unlock(self.sync_io);

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

    fn commitAppendBatch(self: *WAL, batch: CommitBatch, control: ?@import("../common/read_admission.zig").Port) CommitAttempt {
        var stats = CommitRunStats{
            .request_count = batch.request_count,
            .entry_count = batch.entry_count,
        };
        if (self.recovery_poisoned.load(.acquire)) return .{ .result = .{ .failure = error.CorruptWal }, .stats = stats };
        if (control) |port| port.checkpoint() catch |err| return .{ .result = .{ .failure = err }, .stats = stats };
        const first_lsn = self.next_lsn;
        const entry_count = std.math.cast(u64, batch.entry_count) orelse return .{
            .result = .{ .failure = error.Overflow },
            .stats = stats,
        };
        _ = std.math.add(u64, first_lsn, entry_count) catch return .{
            .result = .{ .failure = error.Overflow },
            .stats = stats,
        };
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

        var request = batch.head;
        while (request) |current| : (request = current.next) {
            for (current.entries, 0..) |data, index| {
                const put_started = self.nowNs();
                const put = if (current.encoded_entries) |encoded| blk: {
                    const key = std.mem.toBytes(std.mem.nativeToBig(u64, lsn));
                    break :blk putWalValue(&txn, &key, encoded[index]);
                } else putEncodedEntry(self.allocator, &txn, lsn, data);
                put catch |err| {
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

        if (control) |port| port.checkpoint() catch |err| {
            txn.abort();
            return .{ .result = .{ .failure = err }, .stats = stats };
        };
        const lsm_stats_before = self.store_owner.lsmWriteStatsSnapshot();
        const commit_started = self.nowNs();
        stats.commit_attempted = true;
        txn.commit() catch |err| {
            self.recovery_poisoned.store(true, .release);
            txn.abort();
            stats.commit_ns = self.elapsedSince(commit_started);
            return .{
                .result = .{ .failure = err },
                .stats = stats,
            };
        };
        if (lsm_stats_before) |before| {
            const after = self.store_owner.lsmWriteStatsSnapshot().?;
            stats.inner_segment_syncs = after.wal_segment_syncs -| before.wal_segment_syncs;
            stats.inner_index_syncs = after.wal_index_syncs -| before.wal_index_syncs;
        }
        self.next_lsn = lsn;
        if (self.sync_after_commit) {
            const sync_stats = self.store_owner.syncCommitDurability() catch |err| {
                stats.commit_ns = self.elapsedSince(commit_started);
                return .{
                    .result = .{ .failure = err },
                    .stats = stats,
                };
            };
            stats.post_commit_segment_syncs += sync_stats.segment_syncs;
            stats.post_commit_index_syncs += sync_stats.index_syncs;
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
        self.stats.total_txn_open_ns += stats.txn_open_ns;
        self.stats.total_put_ns += stats.put_ns;
        if (!stats.commit_attempted) return;
        self.stats.physical_commits += 1;
        if (stats.request_count > 1) {
            self.stats.grouped_commits += 1;
            self.stats.grouped_requests += stats.request_count;
        }
        self.stats.max_requests_per_commit = @max(self.stats.max_requests_per_commit, stats.request_count);
        self.stats.max_entries_per_commit = @max(self.stats.max_entries_per_commit, stats.entry_count);

        self.stats.total_commit_ns += stats.commit_ns;
        self.stats.inner_segment_syncs += stats.inner_segment_syncs;
        self.stats.inner_index_syncs += stats.inner_index_syncs;
        self.stats.post_commit_segment_syncs += stats.post_commit_segment_syncs;
        self.stats.post_commit_index_syncs += stats.post_commit_index_syncs;
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
        .lsm => blk: {
            const path_slice = std.mem.span(path);
            const path_owned = try alloc.dupe(u8, path_slice);
            defer alloc.free(path_owned);
            const debug_open = lsmOpenDebugLogsEnabled();
            if (debug_open) {
                std.log.info(
                    "wal lsm open begin path={s} path_ptr=0x{x} owned_ptr=0x{x} storage_provided={any} read_only={any} no_sync={any}",
                    .{
                        path_owned,
                        @intFromPtr(path),
                        @intFromPtr(path_owned.ptr),
                        opts.storage != null,
                        opts.read_only,
                        opts.no_sync,
                    },
                );
            }
            if (opts.storage == null and !opts.read_only) {
                var io_impl = std.Io.Threaded.init(alloc, .{});
                defer io_impl.deinit();
                try fs_paths.createDirPathPortable(io_impl.io(), path_owned);
                if (debug_open) std.log.info("wal lsm open ensured dir path={s}", .{path_owned});
            }

            var lsm_options = opts.lsm_options;
            lsm_options.backend.read_only = opts.read_only;
            if (opts.read_only) lsm_options.backend.create_if_missing = false;
            lsm_options.backend.durability = if (opts.no_sync) .none else lsm_options.backend.durability;
            lsm_options.storage = opts.storage orelse lsm_options.storage;
            lsm_options.background_executor = null;
            if (debug_open) {
                std.log.info(
                    "wal lsm backend handle open begin path={s} lsm_storage_provided={any} create_if_missing={any} durability={s}",
                    .{
                        path_owned,
                        lsm_options.storage != null,
                        lsm_options.backend.create_if_missing,
                        @tagName(lsm_options.backend.durability),
                    },
                );
            }
            var handle = try lsm_backend.BackendHandle.open(alloc, path_owned, lsm_options);
            if (debug_open) std.log.info("wal lsm backend handle open done path={s}", .{path_owned});
            errdefer handle.close();
            break :blk .{ .lsm = handle };
        },
        .lsm_memory => blk: {
            var lsm_options = opts.lsm_options;
            lsm_options.backend.durability = .none;
            lsm_options.storage = opts.storage orelse lsm_options.storage;
            lsm_options.background_executor = null;
            var handle = try lsm_backend.BackendHandle.init(alloc, lsm_options);
            errdefer handle.close();
            break :blk .{ .lsm = handle };
        },
    };
}

fn resolvedCommitCompletionDelayNs(opts: WalOptions) u64 {
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
    return platform_time.monotonicNs();
}

fn elapsedSince(started: u64) u64 {
    return nowNs() - started;
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
    @import("antfly_platform").sync.lockYielding(mutex);
}

fn putWalValue(txn: anytype, key: []const u8, value: []const u8) !void {
    txn.appendPut(key, value) catch |err| switch (err) {
        error.Unsupported => try txn.put(key, value),
        else => return err,
    };
}

fn putEncodedEntry(allocator: Allocator, txn: anytype, lsn: u64, data: []const u8) !void {
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

    const buf = try allocator.alloc(u8, value_len);
    defer allocator.free(buf);

    encodeEntryValue(buf, data_len, data, checksum);
    try putWalValue(txn, &key, buf);
}

const meta_next_lsn_key = [_]u8{0};
const idempotency_metadata_prefix = "\x00\x01wal-idempotency/";

const IdempotencyMetadata = struct {
    digest: []const u8,
    lsn: u64,
};

fn idempotencyMetadataKeyAlloc(allocator: Allocator, key: []const u8) ![]u8 {
    const out = try allocator.alloc(u8, idempotency_metadata_prefix.len + key.len);
    @memcpy(out[0..idempotency_metadata_prefix.len], idempotency_metadata_prefix);
    @memcpy(out[idempotency_metadata_prefix.len..], key);
    return out;
}

fn encodeIdempotencyMetadataAlloc(allocator: Allocator, digest: []const u8, lsn: u64) ![]u8 {
    const digest_len: u16 = std.math.cast(u16, digest.len) orelse return error.InvalidIdempotencyKey;
    const out = try allocator.alloc(u8, 2 + digest.len + 8);
    out[0..2].* = std.mem.toBytes(std.mem.nativeToLittle(u16, digest_len));
    @memcpy(out[2..][0..digest.len], digest);
    out[2 + digest.len ..][0..8].* = std.mem.toBytes(std.mem.nativeToLittle(u64, lsn));
    return out;
}

fn decodeIdempotencyMetadata(raw: []const u8) !IdempotencyMetadata {
    if (raw.len < 10) return error.Corrupted;
    const digest_len: usize = std.mem.readInt(u16, raw[0..2], .little);
    const lsn_offset = std.math.add(usize, 2, digest_len) catch return error.Corrupted;
    if (lsn_offset + 8 != raw.len) return error.Corrupted;
    const lsn = std.mem.readInt(u64, raw[lsn_offset..][0..8], .little);
    if (lsn == 0) return error.Corrupted;
    return .{ .digest = raw[2..lsn_offset], .lsn = lsn };
}

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
    return decodeWalValueWithAdmission(value, null) catch return error.CorruptWal;
}

fn decodeWalValueWithAdmission(value: []const u8, control: ?@import("../common/read_admission.zig").Port) ![]const u8 {
    if (control) |port| try port.checkpoint();
    if (value.len < 8) return error.CorruptWal;

    const data_len: usize = std.mem.readInt(u32, value[0..4], .little);
    const data_end = std.math.add(usize, 4, data_len) catch return error.CorruptWal;
    const crc_end = std.math.add(usize, data_end, 4) catch return error.CorruptWal;
    if (value.len < crc_end) return error.CorruptWal;

    const data = value[4..data_end];
    const stored_crc = std.mem.readInt(u32, value[data_end..][0..4], .little);

    var crc = Crc32.init();
    crc.update(value[0..4]);
    if (control) |port| {
        var offset: usize = 0;
        while (offset < data.len) {
            try port.checkpoint();
            const end = offset + @min(64 * 1024, data.len - offset);
            crc.update(data[offset..end]);
            offset = end;
        }
        try port.checkpoint();
    } else crc.update(data);
    if (crc.final() != stored_crc) return error.CorruptWal;

    return data;
}

fn encodeEntryValue(dst: []u8, data_len: u32, data: []const u8, checksum: u32) void {
    dst[0..4].* = std.mem.toBytes(std.mem.nativeToLittle(u32, data_len));
    @memcpy(dst[4..][0..data.len], data);
    dst[4 + data.len ..][0..4].* = std.mem.toBytes(std.mem.nativeToLittle(u32, checksum));
}

fn checksumForEntry(data_len: u32, data: []const u8) u32 {
    var crc = Crc32.init();
    crc.update(&std.mem.toBytes(std.mem.nativeToLittle(u32, data_len)));
    crc.update(data);
    return crc.final();
}

/// IEEE CRC concatenation, specialized to the immutable tail length during
/// preparation. Publication applies one 32-column matrix to the short prefix
/// CRC and XORs the saved tail CRC; it never revisits payload bytes.
const CrcShift = struct {
    columns: [32]u32,

    fn times(matrix: *const [32]u32, value: u32) u32 {
        var bits = value;
        var result: u32 = 0;
        var index: usize = 0;
        while (bits != 0) : (index += 1) {
            if (bits & 1 != 0) result ^= matrix[index];
            bits >>= 1;
        }
        return result;
    }

    fn compose(left: *const [32]u32, right: *const [32]u32) [32]u32 {
        var result: [32]u32 = undefined;
        for (&result, 0..) |*column, i| column.* = times(left, right[i]);
        return result;
    }

    fn init(length: usize, control: ?@import("../common/read_admission.zig").Port) !CrcShift {
        var identity: [32]u32 = undefined;
        for (&identity, 0..) |*column, i| column.* = @as(u32, 1) << @as(u5, @intCast(i));
        var power: [32]u32 = undefined;
        power[0] = 0xedb88320;
        for (power[1..], 0..) |*column, i| column.* = @as(u32, 1) << @as(u5, @intCast(i));
        // The reflected polynomial matrix advances one bit; square to one byte.
        for (0..3) |_| power = compose(&power, &power);
        var remaining = length;
        while (remaining != 0) {
            if (control) |port| try port.checkpoint();
            if (remaining & 1 != 0) identity = compose(&power, &identity);
            remaining >>= 1;
            if (remaining != 0) power = compose(&power, &power);
        }
        return .{ .columns = identity };
    }

    fn apply(self: *const CrcShift, prefix_crc: u32) u32 {
        return times(&self.columns, prefix_crc);
    }
};

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

fn abandonWalSimAfterModeledCrash(wal: *WAL, wal_open: *bool) void {
    if (!wal_open.*) return;
    wal.abandonAfterCrash();
    wal_open.* = false;
}

fn reopenWalSimAfterModeledCrash(wal: *WAL, wal_open: *bool, path: [*:0]const u8, opts: WalOptions) !void {
    abandonWalSimAfterModeledCrash(wal, wal_open);
    wal.* = try WAL.open(path, opts);
    wal_open.* = true;
}

const ConcurrentAppendWorker = struct {
    wal: *WAL,
    payload: []const u8,
    appends: usize = 1,
    result: u64 = 0,
    err: ?anyerror = null,

    fn run(self: *@This()) void {
        for (0..self.appends) |_| {
            self.result = self.wal.append(self.payload) catch |err| {
                self.err = err;
                return;
            };
        }
    }
};

/// Test workload ownership: no append starts until every worker has arrived.
/// If any launch fails, cancel the barrier waits and drain the admitted tasks
/// before returning, while the WAL and payloads are still alive.
fn runConcurrentAppends(io: std.Io, workers: []ConcurrentAppendWorker) !void {
    const Cohort = struct {
        remaining: std.atomic.Value(usize),
        ready: std.Io.Event = .unset,

        fn run(self: *@This(), task_io: std.Io, worker: *ConcurrentAppendWorker) void {
            if (self.remaining.fetchSub(1, .acq_rel) == 1) self.ready.set(task_io);
            self.ready.wait(task_io) catch return;
            worker.run();
        }
    };
    var cohort = Cohort{ .remaining = .init(workers.len) };
    var group: std.Io.Group = .init;
    defer group.cancel(io);
    for (workers) |*worker| try group.concurrent(io, Cohort.run, .{ &cohort, io, worker });
    try group.await(io);
}

test "wal concurrent append startup failure drains workers without writing" {
    for (0..3) |capacity| {
        var io_impl = std.Io.Threaded.init(std.testing.allocator, .{
            .async_limit = .nothing,
            .concurrent_limit = .limited(capacity),
        });
        defer io_impl.deinit();
        var buf: [256]u8 = undefined;
        const path = walTmpPath(&buf);
        defer cleanupWalDir(path);
        var wal = try WAL.open(path, .{});
        defer wal.close();
        var workers = [_]ConcurrentAppendWorker{
            .{ .wal = &wal, .payload = "alpha" },
            .{ .wal = &wal, .payload = "beta" },
            .{ .wal = &wal, .payload = "gamma" },
        };
        try std.testing.expectError(error.ConcurrencyUnavailable, runConcurrentAppends(io_impl.io(), &workers));
        try std.testing.expectEqual(@as(u64, 0), wal.lastLsn());
        for (workers) |worker| {
            try std.testing.expectEqual(@as(u64, 0), worker.result);
            try std.testing.expect(worker.err == null);
        }
        // Reuse the same WAL and payloads after every partial startup point.
        try runConcurrentAppends(std.testing.io, &workers);
        try std.testing.expectEqual(@as(u64, 3), wal.lastLsn());
        for (workers) |worker| {
            try std.testing.expect(worker.err == null);
            try std.testing.expect(worker.result > 0);
        }
    }
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
            const Worker = ConcurrentAppendWorker;

            const left_payload = try walSimPayload(allocator, case_label, step, 0);
            errdefer allocator.free(left_payload);
            const right_payload = try walSimPayload(allocator, case_label, step, 1);
            errdefer allocator.free(right_payload);

            var workers = [_]Worker{
                .{ .wal = wal, .payload = left_payload },
                .{ .wal = wal, .payload = right_payload },
            };
            try runConcurrentAppends(std.testing.io, &workers);
            const left = workers[0];
            const right = workers[1];

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
    abandonWalSimAfterModeledCrash(&wal, &wal_open);

    var reopened = try WAL.open(path, opts);
    defer reopened.close();
    try verifyWalSimState(allocator, &reopened, model.items, runtime_next_lsn, 1);
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
    abandonWalSimAfterModeledCrash(&wal, &wal_open);

    var reopened = try WAL.open(path, opts);
    defer reopened.close();
    try verifyWalSimState(allocator, &reopened, model.items, runtime_next_lsn, 1);
    return outcome;
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

fn replayWalFixtureFile(allocator: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(allocator, "pkg/antfly-embedded/src/storage/wal_sim_fixtures/{s}", .{name});
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
        .crash => try replayModeledWalFixtureFile(allocator, name),
    }
}

fn replayModeledWalFixtureFile(allocator: Allocator, name: []const u8) !void {
    const path = try std.fmt.allocPrint(allocator, "pkg/antfly-embedded/src/storage/wal_sim_fixtures/{s}", .{name});
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
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly-embedded/src/storage/wal_sim_fixtures", .{ .iterate = true }) catch |err| switch (err) {
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
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly-embedded/src/storage/wal_sim_fixtures/replay", .{ .iterate = true }) catch |err| switch (err) {
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
    var fixtures_dir = std.Io.Dir.cwd().openDir(std.testing.io, "pkg/antfly-embedded/src/storage/wal_sim_fixtures/crash", .{ .iterate = true }) catch |err| switch (err) {
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
    try reopenWalSimAfterModeledCrash(&wal, &wal_open, path, opts);
    try verifyWalSimState(allocator, &wal, model.items, runtime_next_lsn, 1);
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
    crashed_wal.abandonAfterCrash();

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
}

test "wal modeled storage preserves acknowledged append across segment rotation crash" {
    var runtime = storage_sim.Runtime.init(std.testing.allocator);
    defer runtime.deinit();

    var device_model = storage_sim.ModeledDevice.init(std.testing.allocator);
    defer device_model.deinit();

    const path: [*:0]const u8 = "/wal-modeled-rotation-crash";
    const opts = WalOptions{
        .backend = .lsm,
        .storage = device_model.storage(),
        .clock = runtime.clock(),
        .commit_scheduler = runtime.completionScheduler(),
        .model_commit_backend_completions = true,
        .lsm_options = .{ .wal_segment_bytes = 32 },
    };

    var crashed_wal = try WAL.open(path, opts);
    try std.testing.expectEqual(@as(u64, 1), try crashed_wal.append("first-segment-record"));
    try std.testing.expectEqual(@as(u64, 2), try crashed_wal.append("rotated-segment-record"));
    try std.testing.expect((try device_model.fileSize("/wal-modeled-rotation-crash/wal/00000000000000000002.log")) > 0);

    try device_model.device().crash();
    crashed_wal.abandonAfterCrash();

    var reopened = try WAL.open(path, opts);
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 3), reopened.next_lsn);

    const entries = try reopened.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(entry.data);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqualStrings("first-segment-record", entries[0].data);
    try std.testing.expectEqualStrings("rotated-segment-record", entries[1].data);
}

test "wal lsm sync boundary failures recover a valid acknowledged prefix" {
    const sync_failure_needles = [_][]const u8{
        "/wal/00000000000000000001.log",
        "/wal/index",
    };

    for (sync_failure_needles, 0..) |needle, case_index| {
        var runtime = storage_sim.Runtime.init(std.testing.allocator);
        defer runtime.deinit();

        var device_model = storage_sim.ModeledDevice.init(std.testing.allocator);
        defer device_model.deinit();

        const path = try std.fmt.allocPrintSentinel(
            std.testing.allocator,
            "/wal-modeled-sync-failure-{d}",
            .{case_index},
            0,
        );
        defer std.testing.allocator.free(path);
        const opts = WalOptions{
            .backend = .lsm,
            .storage = device_model.storage(),
            .clock = runtime.clock(),
            .commit_scheduler = runtime.completionScheduler(),
            .lsm_options = .{
                // Index publication is segment-scoped, so force the index
                // boundary case through a real rotation.
                .wal_segment_bytes = if (case_index == 1) 32 else 64 * 1024 * 1024,
            },
        };

        var crashed_wal = try WAL.open(path, opts);
        try std.testing.expectEqual(@as(u64, 1), try crashed_wal.append("committed"));
        try device_model.injectSyncFailureForPathContains(needle);
        try std.testing.expectError(error.InjectedSyncFault, crashed_wal.append("uncertain"));
        try device_model.device().crash();
        crashed_wal.abandonAfterCrash();

        var reopened = try WAL.open(path, opts);
        defer reopened.close();
        const entries = try reopened.iterateFrom(std.testing.allocator, 1);
        defer {
            for (entries) |entry| std.testing.allocator.free(entry.data);
            std.testing.allocator.free(entries);
        }
        // A failure may happen after the new record reached durable storage but
        // before the final durability boundary completed. Recovery may therefore
        // include the uncertain record, but it must never lose the previously
        // acknowledged prefix or expose anything other than that valid suffix.
        try std.testing.expect(entries.len == 1 or entries.len == 2);
        try std.testing.expectEqualStrings("committed", entries[0].data);
        if (entries.len == 2) {
            try std.testing.expectEqual(@as(u64, 2), entries[1].lsn);
            try std.testing.expectEqualStrings("uncertain", entries[1].data);
        }

        const recovered_lsn = try reopened.append("after-recovery");
        try std.testing.expectEqual(@as(u64, @intCast(entries.len + 1)), recovered_lsn);
    }
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

test "wal replay fixtures stay green" {
    try runWalReplayFixtures(std.testing.allocator);
}

test "wal sim soak stays green" {
    if (!storage_sim_soak) return;
    try runWalSoak(std.testing.allocator);
}

test "wal append prepared CRC transform matches full wire checksum for arbitrary partitions" {
    var bytes: [16385]u8 = undefined;
    for (&bytes, 0..) |*byte, i| byte.* = @truncate(i *% 73 +% 41);
    for ([_]usize{ 0, 1, 2, 7, 31, 100, 4096 }) |prefix_len| {
        for ([_]usize{ 0, 1, 2, 3, 31, 127, 4095, 4096, 8193 }) |tail_len| {
            const prefix = bytes[0..prefix_len];
            const tail = bytes[prefix_len..][0..tail_len];
            const shift = try CrcShift.init(tail.len, null);
            const actual = shift.apply(Crc32.hash(prefix)) ^ Crc32.hash(tail);
            try std.testing.expectEqual(Crc32.hash(bytes[0 .. prefix_len + tail_len]), actual);
        }
    }
    // Repeated squaring handles large lengths without allocating the suffix.
    const once = try CrcShift.init(1 << 30, null);
    const twice = try CrcShift.init(@as(usize, 1) << 31, null);
    try std.testing.expectEqual(twice.apply(0x12345678), once.apply(once.apply(0x12345678)));
}

test "wal append prepared encoding preserves owned bytes and rejects reuse without spending an LSN" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&path_buf, "prepared-encoding");
    defer cleanupWalDir(path);
    var wal = try WAL.openWithAllocator(a, path, .{ .no_sync = true, .commit_backend = .sync });
    defer wal.close();
    var input: [8193]u8 = @splat(51);
    const original = input;
    var abandoned = try wal.prepareAppend(4, &input, null);
    abandoned.deinit();
    try std.testing.expectEqual(@as(u64, 0), wal.lastLsn());
    var entry = try wal.prepareAppend(4, &input, null);
    defer entry.deinit();
    @memset(&input, 17);
    var empty: [0]u8 = .{};
    var deny = std.heap.FixedBufferAllocator.init(&empty);
    wal.allocator = deny.allocator();
    defer wal.allocator = a;
    try std.testing.expectError(error.OutOfMemory, wal.append(&input));
    try std.testing.expectEqual(@as(u64, 1), try entry.publish("head"));
    try std.testing.expectError(error.InvalidArgument, entry.publish("head"));
    wal.allocator = a;
    const stored = (try wal.readAt(a, 1)).?;
    defer a.free(stored.data);
    try std.testing.expectEqualSlices(u8, "head", stored.data[0..4]);
    try std.testing.expectEqualSlices(u8, &original, stored.data[4..]);
    try std.testing.expectEqual(@as(u64, 1), wal.lastLsn());
}

test "wal append prepared construction preserves original refusal and unwinds allocation failures" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&path_buf, "prepared-refusal");
    defer cleanupWalDir(path);
    var wal = try WAL.openWithAllocator(a, path, .{ .no_sync = true });
    defer wal.close();
    const read = @import("../common/read_admission.zig");
    const State = struct {
        checks: usize = 0,
        reject: usize = std.math.maxInt(usize),
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject) return error.DeadlineExceeded;
        }
    };
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check } };
    const input: [8193]u8 = @splat(7);
    var grant = try wal.prepareAppend(100, &input, read.Port.init(&payload));
    grant.deinit();
    const checks = state.checks;
    for (1..checks + 1) |reject| {
        state = .{ .reject = reject };
        try std.testing.expectError(error.DeadlineExceeded, wal.prepareAppend(100, &input, read.Port.init(&payload)));
        try std.testing.expectEqual(reject, state.checks);
        try std.testing.expectEqual(@as(u64, 0), wal.lastLsn());
    }
    const Fixture = struct {
        fn run(allocator: Allocator, target: *WAL) !void {
            const previous = target.allocator;
            target.allocator = allocator;
            defer target.allocator = previous;
            var prepared = try target.prepareAppend(100, &input, null);
            prepared.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{&wal});
    try std.testing.expectEqual(@as(u64, 0), wal.lastLsn());
}

test "wal append prepared and ordinary entries share one physical coordinator commit" {
    const a = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&path_buf, "prepared-coalesce");
    defer cleanupWalDir(path);
    var wal = try WAL.openWithAllocator(a, path, .{ .no_sync = true, .commit_backend = .sync });
    defer wal.close();
    const input: [8193]u8 = @splat(29);
    var prepared = try wal.prepareAppend(4, &input, null);
    defer prepared.deinit();
    const encoded = try prepared.seal("head");
    var ordinary: AppendRequest = .{ .entries = &.{"ordinary"} };
    var funded: AppendRequest = .{ .entries = &.{encoded[4 .. encoded.len - 4]}, .encoded_entries = &.{encoded}, .next = &ordinary };
    const batch: CommitBatch = .{ .head = &funded, .tail = &ordinary, .request_count = 2, .entry_count = 2 };
    var empty: [0]u8 = .{};
    var deny = std.heap.FixedBufferAllocator.init(&empty);
    wal.allocator = deny.allocator();
    defer wal.allocator = a;
    wal.mutex.lockUncancelable(wal.sync_io);
    const attempt = wal.commitAppendBatch(batch, null);
    wal.mutex.unlock(wal.sync_io);
    switch (attempt.result) {
        .success => |result| try std.testing.expectEqual(@as(u64, 1), result.first_lsn),
        .failure => |cause| return cause,
    }
    wal.allocator = a;
    try std.testing.expectEqual(@as(u64, 2), wal.lastLsn());
    const first = (try wal.readAt(a, 1)).?;
    defer a.free(first.data);
    const second = (try wal.readAt(a, 2)).?;
    defer a.free(second.data);
    try std.testing.expectEqualSlices(u8, "head", first.data[0..4]);
    try std.testing.expectEqualSlices(u8, &input, first.data[4..]);
    try std.testing.expectEqualSlices(u8, "ordinary", second.data);
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

test "wal post-commit idempotent completion failure preserves receipt and append cursor" {
    const Completion = struct {
        calls: usize = 0,
        fn wait(raw: ?*anyopaque, _: u64) !void {
            const self: *@This() = @ptrCast(@alignCast(raw.?));
            self.calls += 1;
            if (self.calls == 1) return error.InjectedCompletionFailure;
        }
    };
    var completion: Completion = .{};
    var buf: [256]u8 = undefined;
    const path = walTmpPath(&buf);
    defer cleanupWalDir(path);
    {
        var wal = try WAL.open(path, .{
            .commit_scheduler = .{ .ctx = &completion, .wait_ns_fn = Completion.wait },
            .artificial_sync_delay_ns = 1,
        });
        defer wal.close();
        try std.testing.expectError(error.InjectedCompletionFailure, wal.appendIdempotent("first", "digest", "committed"));
        try std.testing.expectEqual(@as(u64, 2), wal.next_lsn);
        try std.testing.expect(!wal.recovery_poisoned.load(.acquire));
        const retry = try wal.appendIdempotent("first", "digest", "must not replace");
        try std.testing.expectEqualDeep(IdempotentAppendResult{ .lsn = 1, .appended = false }, retry);
        try std.testing.expectEqual(@as(usize, 1), completion.calls);
        try std.testing.expectError(error.IdempotencyConflict, wal.appendIdempotent("first", "other digest", "conflict"));
        const second = try wal.appendIdempotent("second", "digest", "second");
        try std.testing.expectEqualDeep(IdempotentAppendResult{ .lsn = 2, .appended = true }, second);
        try std.testing.expectEqual(@as(u64, 3), try wal.append("ordinary"));
    }
    var reopened = try WAL.open(path, .{});
    defer reopened.close();
    try std.testing.expectEqual(@as(u64, 4), reopened.next_lsn);
    try std.testing.expectEqualDeep(IdempotentAppendResult{ .lsn = 1, .appended = false }, try reopened.appendIdempotent("first", "digest", "must not replace"));
    try std.testing.expectEqualDeep(IdempotentAppendResult{ .lsn = 2, .appended = false }, try reopened.appendIdempotent("second", "digest", "must not replace"));
    const entries = try reopened.iterateFrom(std.testing.allocator, 1);
    defer {
        for (entries) |entry| std.testing.allocator.free(entry.data);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 3), entries.len);
    try std.testing.expectEqualStrings("committed", entries[0].data);
    try std.testing.expectEqualStrings("second", entries[1].data);
    try std.testing.expectEqualStrings("ordinary", entries[2].data);
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

    wal.mutex.lockUncancelable(wal.sync_io);
    wal.stats.append_calls += 2;
    wal.stats.logical_entries += 2;
    wal.enqueueAppendRequestLocked(&request_a);
    wal.enqueueAppendRequestLocked(&request_b);
    wal.coordinator_active = true;
    wal.mutex.unlock(wal.sync_io);

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
    const Worker = ConcurrentAppendWorker;

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

        var workers = [_]Worker{
            .{ .wal = &wal, .payload = "alpha" },
            .{ .wal = &wal, .payload = "beta" },
        };
        try runConcurrentAppends(std.testing.io, &workers);
        const worker_a = workers[0];
        const worker_b = workers[1];

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

    const Worker = ConcurrentAppendWorker;

    var workers = [_]Worker{
        .{ .wal = &wal, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .payload = payload, .appends = 64 },
    };

    try runConcurrentAppends(std.testing.io, &workers);
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

    const Worker = ConcurrentAppendWorker;

    var workers = [_]Worker{
        .{ .wal = &wal, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .payload = payload, .appends = 64 },
        .{ .wal = &wal, .payload = payload, .appends = 64 },
    };

    try runConcurrentAppends(std.testing.io, &workers);
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

            const Worker = ConcurrentAppendWorker;

            var workers = [_]Worker{
                .{ .wal = &wal, .payload = payload, .appends = 64 },
                .{ .wal = &wal, .payload = payload, .appends = 64 },
                .{ .wal = &wal, .payload = payload, .appends = 64 },
                .{ .wal = &wal, .payload = payload, .appends = 64 },
            };

            try runConcurrentAppends(std.testing.io, &workers);
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
        {
            // Cursors must close before commit: committing with an open
            // cursor fails closed with error.TransactionCursorActive.
            var cur = try write.openCursor();
            defer cur.close();
            try std.testing.expectEqualStrings(&key, (try cur.start(.{})).?.key);
        }
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

test "wal full durability does not repeat an already durable lsm commit" {
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "lsm-single-durable-boundary");
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{ .backend = .lsm });
    defer wal.close();

    _ = try wal.append("warmup");
    const before = wal.statsSnapshot();
    _ = try wal.append("measured");
    const after = wal.statsSnapshot();

    try std.testing.expectEqual(@as(u64, 1), after.inner_segment_syncs - before.inner_segment_syncs);
    try std.testing.expectEqual(@as(u64, 0), after.inner_index_syncs - before.inner_index_syncs);
    try std.testing.expectEqual(@as(u64, 0), after.post_commit_segment_syncs - before.post_commit_segment_syncs);
    try std.testing.expectEqual(@as(u64, 0), after.post_commit_index_syncs - before.post_commit_index_syncs);
}

test "wal retains post commit sync when lsm commit is not durable" {
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "lsm-post-commit-durable-boundary");
    defer cleanupWalDir(path);

    var wal = try WAL.open(path, .{
        .backend = .lsm,
        .lsm_options = .{
            .backend = .{ .durability = .none },
            .wal_sync_on_commit = false,
        },
    });
    defer wal.close();

    _ = try wal.append("warmup");
    const before = wal.statsSnapshot();
    _ = try wal.append("measured");
    const after = wal.statsSnapshot();

    try std.testing.expectEqual(@as(u64, 0), after.inner_segment_syncs - before.inner_segment_syncs);
    try std.testing.expectEqual(@as(u64, 0), after.inner_index_syncs - before.inner_index_syncs);
    try std.testing.expectEqual(@as(u64, 1), after.post_commit_segment_syncs - before.post_commit_segment_syncs);
    try std.testing.expectEqual(@as(u64, 1), after.post_commit_index_syncs - before.post_commit_index_syncs);
}

test "wal read-only lsm backend does not create missing root" {
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path_raw = try std.fmt.allocPrint(std.testing.allocator, ".zig-cache/tmp/{s}/wal-readonly-lsm-missing", .{tmp.sub_path});
    defer std.testing.allocator.free(path_raw);
    const path = try std.testing.allocator.dupeSentinel(u8, path_raw, 0);
    defer std.testing.allocator.free(path);

    var io_impl = std.Io.Threaded.init(std.testing.allocator, .{});
    defer io_impl.deinit();
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), path_raw, .{}));
    var wal = try WAL.open(path, .{
        .backend = .lsm,
        .read_only = true,
    });
    defer wal.close();
    try std.testing.expectEqual(@as(u64, 0), wal.lastLsn());
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io_impl.io(), path_raw, .{}));
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
    }
}

fn cleanupWalDir(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "WAL borrowed entry visit retires read snapshots and rejects corrupt bytes" {
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "borrowed-entry");
    defer cleanupWalDir(path);
    var wal = try WAL.open(path, .{ .backend = .lsm_memory, .commit_backend = .sync });
    defer wal.close();
    const lsn = try wal.append("borrowed");
    const Visit = struct {
        calls: usize = 0,
        reject: bool = false,
        fn run(self: *@This(), entry: WalEntry) !void {
            try std.testing.expectEqual(@as(u64, 1), entry.lsn);
            try std.testing.expectEqualStrings("borrowed", entry.data);
            self.calls += 1;
            if (self.reject) return error.InjectedWalVisitRejection;
        }
    };
    var visit: Visit = .{};
    try std.testing.expect(try wal.withEntryAt(lsn, &visit, Visit.run));
    try std.testing.expect(!try wal.withEntryAt(99, &visit, Visit.run));
    try std.testing.expectEqual(@as(usize, 1), visit.calls);
    visit.reject = true;
    try std.testing.expectError(error.InjectedWalVisitRejection, wal.withEntryAt(lsn, &visit, Visit.run));
    try std.testing.expectEqual(@as(usize, 2), visit.calls);
    // Callback rejection releases its transaction before another write.
    {
        var txn = try wal.beginWriteTxn();
        errdefer txn.abort();
        const key = std.mem.toBytes(std.mem.nativeToBig(u64, lsn));
        try txn.put(&key, "bad");
        try txn.commit();
    }
    try std.testing.expectError(error.CorruptWal, wal.withEntryAt(lsn, &visit, Visit.run));
    try std.testing.expectEqual(@as(usize, 2), visit.calls);
}

test "WAL borrowed controlled entry retires snapshots after every checksum and visitor rejection" {
    const read = @import("../common/read_admission.zig");
    const State = struct {
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.checks == self.reject_at) return self.cause;
        }
    };
    const a = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "controlled-borrow");
    defer cleanupWalDir(path);
    var wal = try WAL.open(path, .{ .backend = .lsm_memory, .commit_backend = .sync });
    defer wal.close();
    const bytes = try a.alloc(u8, 2 * 64 * 1024 + 17);
    defer a.free(bytes);
    @memset(bytes, 91);
    _ = try wal.append(bytes);
    var state: State = .{};
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check } };
    const control = read.Port.init(&payload);
    const Visit = struct {
        expected: []const u8,
        calls: usize = 0,
        fn run(self: *@This(), entry: WalEntry) !void {
            try std.testing.expectEqualSlices(u8, self.expected, entry.data);
            self.calls += 1;
        }
    };
    var visit: Visit = .{ .expected = bytes };
    try std.testing.expect(try wal.withEntryAtWithAdmission(1, control, &visit, Visit.run));
    const checks = state.checks;
    try std.testing.expectEqual(@as(usize, 0), wal.store_owner.lsm.backend.active_readers);
    try std.testing.expect(checks >= 8);
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            state = .{ .reject_at = reject_at, .cause = cause };
            try std.testing.expectError(cause, wal.withEntryAtWithAdmission(1, control, &visit, Visit.run));
            try std.testing.expectEqual(@as(usize, 0), wal.store_owner.lsm.backend.active_readers);
            // A write after each failure observes leaked read ownership.
            _ = try wal.append("after rejection");
        }
    }
    state = .{};
    try std.testing.expect(!try wal.withEntryAtWithAdmission(std.math.maxInt(u64), control, &visit, Visit.run));
    try std.testing.expect(try wal.withEntryAtWithAdmission(1, control, &visit, Visit.run));
}

test "WAL recovery position rejects tail ABA prefix deletion foreign owners and reopen" {
    const a = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "recovery-position-identity");
    defer cleanupWalDir(path);
    var old: RecoveryPosition = undefined;
    {
        var wal = try WAL.open(path, .{ .commit_backend = .sync });
        defer wal.close();
        _ = try wal.append("first");
        _ = try wal.append("old tail");
        old = (try wal.tryRecoveryPosition(null)).?;
        try wal.truncateAfter(1);
        _ = try wal.append("replacement");
        try std.testing.expectEqual(old.next_lsn, wal.next_lsn);
        try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try wal.appendIfRecoveryPosition(old, "must not append", null));
        const before_prefix = (try wal.tryRecoveryPosition(null)).?;
        try wal.truncate(1);
        try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try wal.appendIfRecoveryPosition(before_prefix, "must not append", null));
        const after_prefix = (try wal.tryRecoveryPosition(null)).?;
        try wal.truncate(0);
        try wal.truncateAfter(wal.lastLsn());
        try std.testing.expectEqualDeep(after_prefix, (try wal.tryRecoveryPosition(null)).?);
        var other_buf: [256]u8 = undefined;
        const other_path = walTmpPathWithSuffix(&other_buf, "recovery-position-other");
        defer cleanupWalDir(other_path);
        var other = try WAL.open(other_path, .{ .backend = .lsm_memory, .commit_backend = .sync });
        defer other.close();
        _ = try other.append("first");
        _ = try other.append("replacement");
        try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try other.appendIfRecoveryPosition(after_prefix, "foreign", null));
    }
    var reopened = try WAL.open(path, .{ .commit_backend = .sync });
    defer reopened.close();
    const current = (try reopened.tryRecoveryPosition(null)).?;
    try std.testing.expectEqual(old.next_lsn, current.next_lsn);
    try std.testing.expectEqual(old.revision, current.revision);
    try std.testing.expect(old.owner != current.owner);
    try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try reopened.appendIfRecoveryPosition(old, "old lifetime", null));
    const entry = (try reopened.readAt(a, 2)).?;
    defer a.free(entry.data);
    try std.testing.expectEqualStrings("replacement", entry.data);
}

test "WAL recovery position busy and cancellation preserve ownership before the commit boundary" {
    const read = @import("../common/read_admission.zig");
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "recovery-position-control");
    defer cleanupWalDir(path);
    var wal = try WAL.open(path, .{ .backend = .lsm_memory, .commit_backend = .sync });
    defer wal.close();
    const first = (try wal.tryRecoveryPosition(null)).?;
    const before = wal.stats;
    try std.testing.expect(wal.mutex.tryLock());
    const busy_position = wal.tryRecoveryPosition(null);
    const busy_append = wal.appendIfRecoveryPosition(first, "busy", null);
    wal.mutex.unlock(wal.sync_io);
    try std.testing.expectEqual(@as(?RecoveryPosition, null), try busy_position);
    try std.testing.expectEqual(@as(RecoveryAppendResult, .busy), try busy_append);
    try std.testing.expectEqualDeep(before, wal.stats);
    wal.coordinator_active = true;
    const active_position = wal.tryRecoveryPosition(null);
    const active_append = wal.appendIfRecoveryPosition(first, "active", null);
    wal.coordinator_active = false;
    try std.testing.expectEqual(@as(?RecoveryPosition, null), try active_position);
    try std.testing.expectEqual(@as(RecoveryAppendResult, .busy), try active_append);
    const State = struct {
        wal: *WAL,
        expected_next: u64,
        checks: usize = 0,
        reject_at: usize = 0,
        cause: anyerror = error.Canceled,
        after_commit: bool = false,
        fn check(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.checks += 1;
            if (self.wal.next_lsn != self.expected_next) {
                self.after_commit = true;
                return error.Canceled;
            }
            if (self.checks == self.reject_at) return self.cause;
        }
        fn wait(_: *anyopaque) !void {
            return error.UnexpectedWriterWait;
        }
    };
    var state: State = .{ .wal = &wal, .expected_next = first.next_lsn };
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.check, .check_lifetime = State.check, .wait_for_progress = State.wait } };
    const control = read.Port.init(&payload);
    try std.testing.expectEqual(@as(u64, 1), (try wal.appendIfRecoveryPosition(first, "committed", control)).appended);
    try std.testing.expect(!state.after_commit);
    const checks = state.checks;
    try std.testing.expect(checks >= 4);
    const next = (try wal.tryRecoveryPosition(null)).?;
    const commits = wal.stats.physical_commits;
    for ([_]anyerror{ error.Canceled, error.DeadlineExceeded, error.SchedulerClosed, error.CatalogGenerationChanged }) |cause| {
        for (1..checks + 1) |reject_at| {
            state = .{ .wal = &wal, .expected_next = next.next_lsn, .reject_at = reject_at, .cause = cause };
            try std.testing.expectError(cause, wal.appendIfRecoveryPosition(next, "rejected", control));
            try std.testing.expectEqual(next.next_lsn, wal.next_lsn);
            try std.testing.expectEqual(commits, wal.stats.physical_commits);
            try std.testing.expectEqual(reject_at, state.checks);
            try std.testing.expectEqualDeep(next, (try wal.tryRecoveryPosition(null)).?);
        }
    }
}

test "WAL recovery position competing writers append once and completion failure keeps committed cursor" {
    if (builtin.single_threaded or builtin.os.tag == .freestanding) return error.SkipZigTest;
    const a = std.testing.allocator;
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "recovery-position-compete");
    defer cleanupWalDir(path);
    var wal = try WAL.open(path, .{ .backend = .lsm_memory, .commit_backend = .sync });
    defer wal.close();
    const position = (try wal.tryRecoveryPosition(null)).?;
    const Call = struct {
        wal: *WAL,
        position: RecoveryPosition,
        start: *std.atomic.Value(bool),
        result: ?RecoveryAppendResult = null,
        failure: ?anyerror = null,
        fn run(self: *@This()) void {
            while (!self.start.load(.acquire)) std.Thread.yield() catch {};
            self.result = self.wal.appendIfRecoveryPosition(self.position, "one record", null) catch |err| {
                self.failure = err;
                return;
            };
        }
    };
    var start: std.atomic.Value(bool) = .init(false);
    var left: Call = .{ .wal = &wal, .position = position, .start = &start };
    var right: Call = .{ .wal = &wal, .position = position, .start = &start };
    {
        const first = try std.Thread.spawn(.{}, Call.run, .{&left});
        errdefer {
            start.store(true, .release);
            first.join();
        }
        const second = try std.Thread.spawn(.{}, Call.run, .{&right});
        start.store(true, .release);
        first.join();
        second.join();
    }
    try std.testing.expect(left.failure == null and right.failure == null);
    var successes: usize = 0;
    for ([_]RecoveryAppendResult{ left.result.?, right.result.? }) |result| switch (result) {
        .appended => |lsn| {
            successes += 1;
            try std.testing.expectEqual(@as(u64, 1), lsn);
        },
        .busy, .changed => {},
    };
    try std.testing.expectEqual(@as(usize, 1), successes);
    try std.testing.expectEqual(@as(u64, 1), wal.lastLsn());
    const Fail = struct {
        fn complete(_: ?*anyopaque, _: u64) !void {
            return error.InjectedCompletionFailure;
        }
    };
    const prior_scheduler = wal.commit_scheduler;
    const next = (try wal.tryRecoveryPosition(null)).?;
    wal.commit_scheduler = .{ .wait_ns_fn = Fail.complete };
    const failed = wal.appendIfRecoveryPosition(next, "committed before failure", null);
    wal.commit_scheduler = prior_scheduler;
    try std.testing.expectError(error.InjectedCompletionFailure, failed);
    try std.testing.expectEqual(@as(u64, 3), wal.next_lsn);
    try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try wal.appendIfRecoveryPosition(next, "duplicate", null));
    const entry = (try wal.readAt(a, 2)).?;
    defer a.free(entry.data);
    try std.testing.expectEqualStrings("committed before failure", entry.data);
    var boot_buf: [256]u8 = undefined;
    const boot_path = walTmpPathWithSuffix(&boot_buf, "recovery-position-bootstrap");
    defer cleanupWalDir(boot_path);
    var boot = try WAL.open(boot_path, .{ .backend = .lsm_memory, .commit_backend = .sync });
    defer boot.close();
    boot.commit_scheduler = .{ .wait_ns_fn = Fail.complete };
    const failed_boot = boot.appendAt(7, "bootstrap committed");
    boot.commit_scheduler = prior_scheduler;
    try std.testing.expectError(error.InjectedCompletionFailure, failed_boot);
    try std.testing.expectEqual(@as(u64, 8), boot.next_lsn);
    try std.testing.expectEqual(@as(u64, 8), try boot.append("next"));
}

test "WAL recovery position invalidation exhausts fail closed and prefix writes hold serialization" {
    var counter: std.atomic.Value(u64) = .init(std.math.maxInt(u64));
    try std.testing.expectEqual(std.math.maxInt(u64), try allocateRecoveryOwner(&counter));
    try std.testing.expectError(error.Overflow, allocateRecoveryOwner(&counter));
    try std.testing.expectError(error.Overflow, allocateRecoveryOwner(&counter));
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "recovery-position-exhaust");
    defer cleanupWalDir(path);
    var wal = try WAL.open(path, .{ .backend = .lsm_memory, .commit_backend = .sync });
    defer wal.close();
    _ = try wal.append("retained");
    const Observer = struct {
        var wal_ptr: *WAL = undefined;
        var original: @FieldType(backend_erased.NamespaceStore.VTable, "begin_write") = undefined;
        var observed: bool = false;
        fn begin(alloc: Allocator, ptr: *anyopaque) !backend_erased.NamespaceWriteTxn {
            observed = true;
            if (wal_ptr.mutex.tryLock()) {
                wal_ptr.mutex.unlock(wal_ptr.sync_io);
                return error.UnserializedPrefixMutation;
            }
            return original(alloc, ptr);
        }
    };
    Observer.wal_ptr = &wal;
    Observer.observed = false;
    Observer.original = wal.store.vtable.begin_write;
    const prior_vtable = wal.store.vtable;
    var vtable = prior_vtable.*;
    vtable.begin_write = Observer.begin;
    wal.store.vtable = &vtable;
    defer wal.store.vtable = prior_vtable;
    wal.destructive_revision.store(std.math.maxInt(u64), .release);
    const before = (try wal.tryRecoveryPosition(null)).?;
    try wal.truncate(1);
    try std.testing.expect(Observer.observed);
    wal.store.vtable = prior_vtable;
    try std.testing.expectError(error.Overflow, wal.tryRecoveryPosition(null));
    try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try wal.appendIfRecoveryPosition(before, "stale", null));
    try std.testing.expectEqual(@as(u64, 2), try wal.append("legacy append remains valid"));
}

test "WAL recovery position quarantines ambiguous commit errors and reopens from durable state" {
    const Proxy = struct {
        inner: backend_erased.NamespaceWriteTxn,
        alive: bool = true,
        apply: bool,
        pub fn abort(self: *@This()) void {
            if (self.alive) self.inner.abort();
            retired += 1;
        }
        pub fn commit(self: *@This()) !void {
            if (self.apply) {
                try self.inner.commit();
                self.alive = false;
            }
            return error.InjectedCommitBoundaryFailure;
        }
        pub fn get(self: *@This(), ns: backend_types.Namespace, key: []const u8) ![]const u8 {
            return self.inner.get(ns, key);
        }
        pub fn put(self: *@This(), ns: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            return self.inner.put(ns, key, value);
        }
        pub fn delete(self: *@This(), ns: backend_types.Namespace, key: []const u8) !void {
            return self.inner.delete(ns, key);
        }
        pub fn openCursor(self: *@This(), ns: backend_types.Namespace) !backend_erased.Cursor {
            return self.inner.openCursor(ns);
        }
        var retired: usize = 0;
        var apply_before_error: bool = false;
        var original: @FieldType(backend_erased.NamespaceStore.VTable, "begin_write") = undefined;
        fn map(ns: backend_types.Namespace) !backend_types.Namespace {
            return ns;
        }
        fn begin(alloc: Allocator, ptr: *anyopaque) !backend_erased.NamespaceWriteTxn {
            var inner = try original(alloc, ptr);
            errdefer inner.abort();
            return backend_erased.namespaceWriteTxnFrom(alloc, @This(){ .inner = inner, .apply = apply_before_error }, backend_types.Namespace, map);
        }
    };
    for ([_]bool{ false, true }) |apply_before_error| {
        var buf: [256]u8 = undefined;
        const path = walTmpPathWithSuffix(&buf, "recovery-position-commit-failure");
        defer cleanupWalDir(path);
        var old: RecoveryPosition = undefined;
        {
            var wal = try WAL.open(path, .{ .commit_backend = .sync });
            defer wal.close();
            old = (try wal.tryRecoveryPosition(null)).?;
            const prior = wal.store.vtable;
            var vtable = prior.*;
            vtable.begin_write = Proxy.begin;
            Proxy.original = prior.begin_write;
            Proxy.retired = 0;
            Proxy.apply_before_error = apply_before_error;
            wal.store.vtable = &vtable;
            const attempt = wal.appendIfRecoveryPosition(old, "uncertain", null);
            wal.store.vtable = prior;
            try std.testing.expectError(error.InjectedCommitBoundaryFailure, attempt);
            try std.testing.expectEqual(@as(usize, 1), Proxy.retired);
            try std.testing.expect(wal.recovery_poisoned.load(.acquire));
            try std.testing.expectError(error.CorruptWal, wal.tryRecoveryPosition(null));
            try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try wal.appendIfRecoveryPosition(old, "overwrite", null));
            try std.testing.expectError(error.CorruptWal, wal.append("overwrite"));
            try std.testing.expectError(error.CorruptWal, wal.appendAt(1, "overwrite"));
            try std.testing.expectError(error.CorruptWal, wal.appendIdempotent("key", "digest", "overwrite"));
            try std.testing.expectError(error.CorruptWal, wal.truncate(1));
            try std.testing.expectError(error.CorruptWal, wal.truncateAfter(0));
        }
        var reopened = try WAL.open(path, .{ .commit_backend = .sync });
        defer reopened.close();
        const fresh = (try reopened.tryRecoveryPosition(null)).?;
        try std.testing.expectEqual(@as(u64, if (apply_before_error) 2 else 1), fresh.next_lsn);
        try std.testing.expectEqual(@as(RecoveryAppendResult, .changed), try reopened.appendIfRecoveryPosition(old, "old owner", null));
        try std.testing.expectEqual(fresh.next_lsn, (try reopened.appendIfRecoveryPosition(fresh, "after reopen", null)).appended);
    }
}

test "WAL borrowed namespace read admission refuses legacy and contended snapshot access" {
    const read = @import("../common/read_admission.zig");
    const Check = struct {
        fn lifetime(_: *anyopaque) !void {}
        fn yield(_: *anyopaque) !void {
            return error.TestUnexpectedResult;
        }
    };
    var unused: u8 = 0;
    const payload: read.Payload = .{ .ptr = &unused, .vtable = &.{ .checkpoint = Check.yield, .check_lifetime = Check.lifetime, .wait_for_progress = Check.yield } };
    const control = read.Port.init(&payload);
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "namespace-read-admission");
    var wal = try WAL.open(path, .{ .backend = .lsm_memory });
    defer wal.close();
    _ = try wal.append("payload");
    const Visit = struct {
        fn run(_: *u8, entry: WalEntry) !void {
            try std.testing.expectEqualStrings("payload", entry.data);
        }
    };
    var legacy = wal.store.vtable.*;
    legacy.begin_read_with_admission = null;
    const original = wal.store.vtable;
    wal.store.vtable = &legacy;
    const refused = wal.withEntryAtWithAdmission(1, control, &unused, Visit.run);
    wal.store.vtable = original;
    try std.testing.expectError(error.ExecutionWaitUnsupported, refused);
    const backend = wal.store_owner.lsm.backend;
    try std.testing.expect(backend.mu.tryLock());
    const contended = wal.withEntryAtWithAdmission(1, control, &unused, Visit.run);
    backend.mu.unlock();
    try std.testing.expectError(error.Backpressured, contended);
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    try std.testing.expect(try wal.withEntryAtWithAdmission(1, control, &unused, Visit.run));
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
}

test "WAL borrowed namespace read admission never yields while capturing backend sources" {
    const read = @import("../common/read_admission.zig");
    var buf: [256]u8 = undefined;
    const path = walTmpPathWithSuffix(&buf, "namespace-capture-lifetime");
    var wal = try WAL.open(path, .{ .backend = .lsm_memory });
    defer wal.close();
    _ = try wal.append("value");
    const State = struct {
        backend: *lsm_backend.Backend,
        checkpoints: usize = 0,
        lifetimes: usize = 0,
        fn checkpoint(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            if (!self.backend.mu.tryLock()) return error.CpuHandoffWhileBackendLocked;
            self.backend.mu.unlock();
            self.checkpoints += 1;
        }
        fn lifetime(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.lifetimes += 1;
        }
    };
    var state: State = .{ .backend = wal.store_owner.lsm.backend };
    const payload: read.Payload = .{ .ptr = &state, .vtable = &.{ .checkpoint = State.checkpoint, .check_lifetime = State.lifetime } };
    const control = read.Port.init(&payload);
    {
        var txn = try wal.store.beginReadWithAdmission(control);
        defer txn.abort();
        const key = std.mem.toBytes(std.mem.nativeToBig(u64, @as(u64, 1)));
        _ = try txn.getWithAdmission(default_namespace, &key, control);
    }
    {
        var bound = try state.backend.runtimeStore(std.testing.allocator, default_namespace);
        defer bound.deinit();
        var txn = try bound.beginReadWithAdmission(.retain, control);
        defer txn.abort();
    }
    try std.testing.expect(state.checkpoints > 0 and state.lifetimes > 0);
    try std.testing.expectEqual(@as(usize, 0), state.backend.active_readers);
}

test "WAL allocator owner uses caller storage root and retires append and truncation allocations" {
    var tracking = std.testing.FailingAllocator.init(std.testing.allocator, .{});
    const allocator = tracking.allocator();
    {
        var wal = try WAL.openWithAllocator(allocator, "/unused-memory-wal", .{ .backend = .lsm_memory, .no_sync = true });
        defer wal.close();
        try std.testing.expect(tracking.allocated_bytes > 0);
        try std.testing.expectEqual(allocator.ptr, wal.allocator.ptr);
        tracking.fail_index = tracking.alloc_index;
        try std.testing.expectError(error.OutOfMemory, wal.appendIdempotent("id", "digest", "first"));
        try std.testing.expectEqual(@as(u64, 0), wal.lastLsn());
        tracking.fail_index = std.math.maxInt(usize);
        const bytes = try std.testing.allocator.alloc(u8, 8193);
        defer std.testing.allocator.free(bytes);
        @memset(bytes, 42);
        const first = try wal.appendIdempotent("id", "digest", bytes);
        try std.testing.expectEqual(@as(u64, 1), first.lsn);
        try std.testing.expect(!(try wal.appendIdempotent("id", "digest", bytes)).appended);
        _ = try wal.append("second");
        try wal.truncate(1);
        try wal.truncateAfter(1);
        try std.testing.expectEqual(@as(u64, 2), try wal.append("replacement"));
    }
    try std.testing.expectEqual(tracking.allocated_bytes, tracking.freed_bytes);
}

test "WAL allocator owner rejects large envelope allocation before invoking the store" {
    const TxnProbe = struct {
        calls: usize = 0,
        fn appendPut(_: *@This(), _: []const u8, _: []const u8) anyerror!void {
            return error.Unsupported;
        }
        fn put(self: *@This(), _: []const u8, _: []const u8) !void {
            self.calls += 1;
        }
    };
    var txn: TxnProbe = .{};
    var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 0 });
    var bytes: [8193]u8 = @splat(31);
    try std.testing.expectError(error.OutOfMemory, putEncodedEntry(failing.allocator(), &txn, 1, &bytes));
    try std.testing.expectEqual(@as(usize, 0), txn.calls);
    try putEncodedEntry(std.testing.allocator, &txn, 1, &bytes);
    try std.testing.expectEqual(@as(usize, 1), txn.calls);
}

test "WAL allocator owner failed memory storage opens retire their caller allocations" {
    const Fixture = struct {
        fn run(allocator: Allocator) !void {
            var wal = try WAL.openWithAllocator(allocator, "/unused-memory-wal", .{ .backend = .lsm_memory, .no_sync = true });
            defer wal.close();
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

const OwnedWalTestBudget = struct {
    pub fn checkpoint(_: *@This()) !void {}
    pub fn waitForProgress(_: *@This()) !void {
        return error.TestUnexpectedWalOwnedWait;
    }
};

fn drainOwnedWalForTest(operation: *WAL.OwnedRead, control: WAL.ReadPort) !void {
    var steps: usize = 0;
    while (try operation.step(4093, control) == .pending) {
        steps += 1;
        try std.testing.expect(steps < 4096);
    }
}

test "WAL owned read registry bounds pending snapshots reuses slots and closes detached handles" {
    const a = std.testing.allocator;
    var memory = lsm_storage.MemoryStorage.init(a);
    defer memory.deinit();
    var wal = try WAL.openWithAllocator(a, "/owned-wal-registry", .{ .no_sync = true, .storage = memory.storage(), .lsm_options = .{ .wal_enabled = false, .flush_threshold = std.math.maxInt(usize) } });
    var wal_live = true;
    defer if (wal_live) wal.close();
    _ = try wal.append("record");
    var budget: OwnedWalTestBudget = .{};
    var adapter: @import("../common/read_admission.zig").Adapter(*OwnedWalTestBudget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const control = WAL.ReadPort.init(&payload);
    const Failures = struct {
        fn run(allocator: Allocator, parent: *WAL, port: WAL.ReadPort) !void {
            const operation = try parent.beginOwnedRead(allocator, 1, port);
            operation.destroy();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Failures.run, .{ &wal, control });
    try std.testing.expectEqual(@as(usize, 0), wal.owned_read_starting);
    var reads: [WAL.owned_read_capacity]?*WAL.OwnedRead = @splat(null);
    defer for (&reads) |*read| if (read.*) |operation| operation.destroy();
    for (&reads) |*read| read.* = try wal.beginOwnedRead(a, 1, control);
    try std.testing.expectError(error.Backpressured, wal.beginOwnedRead(a, 1, control));
    reads[0].?.destroy();
    reads[0] = try wal.beginOwnedRead(a, 1, control);
    try drainOwnedWalForTest(reads[0].?, control);
    var copied: [6]u8 = undefined;
    const chunk = try reads[0].?.readChunk(0, &copied, control);
    try std.testing.expect(chunk.done and chunk.len == copied.len);
    wal.close();
    wal_live = false;
    wal = undefined;
    try std.testing.expectEqualStrings("record", &copied);
    for (reads) |read| {
        try std.testing.expectError(error.TransactionClosed, read.?.step(1, control));
        try std.testing.expectError(error.TransactionClosed, read.?.readChunk(0, &copied, control));
    }
}

test "WAL owned read checksum quanta preserve framing corruption and caller chunk ownership" {
    const a = std.testing.allocator;
    var memory = lsm_storage.MemoryStorage.init(a);
    defer memory.deinit();
    var wal = try WAL.openWithAllocator(a, "/owned-wal-crc", .{ .no_sync = true, .storage = memory.storage(), .lsm_options = .{ .wal_enabled = false, .flush_threshold = std.math.maxInt(usize) } });
    defer wal.close();
    const source = try a.alloc(u8, 128 * 1024 + 19);
    defer a.free(source);
    for (source, 0..) |*byte, index| byte.* = @truncate(index);
    const lsn = try wal.append(source);
    var budget: OwnedWalTestBudget = .{};
    var adapter: @import("../common/read_admission.zig").Adapter(*OwnedWalTestBudget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const control = WAL.ReadPort.init(&payload);
    const operation = try wal.beginOwnedRead(a, lsn, control);
    defer operation.destroy();
    var crc_steps: usize = 0;
    while (true) {
        const before = operation.checksum_offset;
        const state = try operation.step(4093, control);
        try std.testing.expect(operation.checksum_offset - before <= 4093);
        if (operation.checksum_offset != before) crc_steps += 1;
        if (state == .ready) break;
    }
    try std.testing.expect(crc_steps > 16);
    var output: [997]u8 = undefined;
    var offset: usize = 0;
    while (true) {
        const chunk = try operation.readChunk(offset, &output, control);
        try std.testing.expectEqualSlices(u8, source[offset..][0..chunk.len], output[0..chunk.len]);
        offset += chunk.len;
        if (chunk.done) break;
    }
    try std.testing.expectEqual(source.len, offset);
    try wal.injectCorruptEntryForTest(lsn);
    const corrupt = try wal.beginOwnedRead(a, lsn, control);
    defer corrupt.destroy();
    try std.testing.expectError(error.CorruptWal, drainOwnedWalForTest(corrupt, control));
    try std.testing.expect(corrupt.lookup == null and corrupt.value == null);
    try std.testing.expectError(error.CorruptWal, corrupt.step(1, control));
    const missing = try wal.beginOwnedRead(a, lsn + 99, control);
    defer missing.destroy();
    try std.testing.expectError(error.NotFound, drainOwnedWalForTest(missing, control));
}

test "WAL owned read close joins an in-flight step and concurrent caller destruction before backend teardown" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Fixture = struct {
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        retired: std.atomic.Value(usize) = .init(0),
        var current: *@This() = undefined;
        fn begin(allocator: Allocator, _: *anyopaque, _: backend_types.Namespace, _: []const u8, _: WAL.ReadPort) !backend_erased.OwnedValueRead {
            return .{ .allocator = allocator, .ptr = current, .vtable = &.{ .deinit = deinit, .step = step, .take_output = take } };
        }
        fn step(raw: *anyopaque, _: usize, _: WAL.ReadPort) !backend_erased.OwnedValueRead.Status {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.entered.set(std.testing.io);
            self.release.waitUncancelable(std.testing.io);
            return .pending;
        }
        fn take(_: *anyopaque) !backend_erased.OwnedValue {
            return error.TestUnexpectedWalOwnedTake;
        }
        fn deinit(_: Allocator, raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            _ = self.retired.fetchAdd(1, .acq_rel);
        }
    };
    var memory = lsm_storage.MemoryStorage.init(a);
    defer memory.deinit();
    var wal = try WAL.openWithAllocator(a, "/owned-wal-race", .{ .no_sync = true, .storage = memory.storage(), .lsm_options = .{ .wal_enabled = false } });
    var wal_live = true;
    defer if (wal_live) wal.close();
    var fixture: Fixture = .{};
    Fixture.current = &fixture;
    var table = wal.store.vtable.*;
    table.begin_owned_value_read = Fixture.begin;
    wal.store.vtable = &table;
    var budget: OwnedWalTestBudget = .{};
    var adapter: @import("../common/read_admission.zig").Adapter(*OwnedWalTestBudget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const control = WAL.ReadPort.init(&payload);
    const operation = try wal.beginOwnedRead(a, 1, control);
    var operation_live = true;
    defer if (operation_live) operation.destroy();
    const Step = struct {
        operation: *WAL.OwnedRead,
        control: WAL.ReadPort,
        failure: ?anyerror = null,
        done: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            _ = self.operation.step(1, self.control) catch |err| {
                self.failure = err;
                self.done.set(std.testing.io);
                return;
            };
            self.done.set(std.testing.io);
        }
    };
    const Close = struct {
        wal: *WAL,
        done: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            self.wal.close();
            self.done.set(std.testing.io);
        }
    };
    const Destroy = struct {
        operation: *WAL.OwnedRead,
        done: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            self.operation.destroy();
            self.done.set(std.testing.io);
        }
    };
    var step: Step = .{ .operation = operation, .control = control };
    const step_thread = try std.Thread.spawn(.{}, Step.run, .{&step});
    var step_joined = false;
    defer if (!step_joined) {
        fixture.release.set(io);
        step_thread.join();
    };
    fixture.entered.waitUncancelable(io);
    var close: Close = .{ .wal = &wal };
    const close_thread = try std.Thread.spawn(.{}, Close.run, .{&close});
    wal_live = false;
    var close_joined = false;
    defer if (!close_joined) {
        fixture.release.set(io);
        close_thread.join();
    };
    const deadline = std.Io.Clock.awake.now(io).addDuration(.fromSeconds(5));
    while (true) {
        platform.sync.lockYielding(&wal.owned_read_mutex);
        const claimed = operation.slot.?.claimed;
        wal.owned_read_mutex.unlock();
        if (claimed) break;
        if (std.Io.Clock.awake.now(io).nanoseconds >= deadline.nanoseconds) @panic("owned read close did not claim active operation");
        platform.time.yieldBriefly();
    }
    var destroy: Destroy = .{ .operation = operation };
    const destroy_thread = try std.Thread.spawn(.{}, Destroy.run, .{&destroy});
    operation_live = false;
    var destroy_joined = false;
    defer if (!destroy_joined) {
        fixture.release.set(io);
        destroy_thread.join();
    };
    fixture.release.set(io);
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    step.done.waitTimeout(io, timeout) catch @panic("owned read step failed to join");
    close.done.waitTimeout(io, timeout) catch @panic("WAL close failed to join owned read");
    destroy.done.waitTimeout(io, timeout) catch @panic("owned read destruction failed to join close");
    step_thread.join();
    step_joined = true;
    close_thread.join();
    close_joined = true;
    destroy_thread.join();
    destroy_joined = true;
    try std.testing.expect(step.failure == null);
    try std.testing.expectEqual(@as(usize, 1), fixture.retired.load(.acquire));
}

test "WAL owned read retiring callbacks retain capacity until resource retirement returns" {
    const a = std.testing.allocator;
    const io = std.testing.io;
    const Fixture = struct {
        entered: std.Io.Event = .unset,
        release: std.Io.Event = .unset,
        var current: *@This() = undefined;
        fn begin(allocator: Allocator, _: *anyopaque, _: backend_types.Namespace, _: []const u8, _: WAL.ReadPort) !backend_erased.OwnedValueRead {
            return .{ .allocator = allocator, .ptr = current, .vtable = &.{ .deinit = deinit, .step = step, .take_output = take } };
        }
        fn step(_: *anyopaque, _: usize, _: WAL.ReadPort) !backend_erased.OwnedValueRead.Status {
            return .pending;
        }
        fn take(_: *anyopaque) !backend_erased.OwnedValue {
            return error.TestUnexpectedWalOwnedTake;
        }
        fn deinit(_: Allocator, raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.entered.set(std.testing.io);
            self.release.waitUncancelable(std.testing.io);
        }
    };
    var memory = lsm_storage.MemoryStorage.init(a);
    defer memory.deinit();
    var wal = try WAL.openWithAllocator(a, "/owned-wal-retiring-capacity", .{ .no_sync = true, .storage = memory.storage(), .lsm_options = .{ .wal_enabled = false } });
    defer wal.close();
    var fixture: Fixture = .{};
    Fixture.current = &fixture;
    var table = wal.store.vtable.*;
    table.begin_owned_value_read = Fixture.begin;
    wal.store.vtable = &table;
    var budget: OwnedWalTestBudget = .{};
    var adapter: @import("../common/read_admission.zig").Adapter(*OwnedWalTestBudget) = .{ .budget = &budget };
    const payload = adapter.payload();
    const control = WAL.ReadPort.init(&payload);
    var operations: [WAL.owned_read_capacity]?*WAL.OwnedRead = @splat(null);
    defer for (operations) |operation| if (operation) |owned| owned.destroy();
    defer fixture.release.set(io);
    for (&operations) |*operation| operation.* = try wal.beginOwnedRead(a, 1, control);
    const Destroy = struct {
        operation: *WAL.OwnedRead,
        done: std.Io.Event = .unset,
        fn run(self: *@This()) void {
            self.operation.destroy();
            self.done.set(std.testing.io);
        }
    };
    var destroy: Destroy = .{ .operation = operations[0].? };
    const thread = try std.Thread.spawn(.{}, Destroy.run, .{&destroy});
    operations[0] = null;
    var joined = false;
    defer if (!joined) {
        fixture.release.set(io);
        thread.join();
    };
    const timeout: std.Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(5), .clock = .awake } };
    fixture.entered.waitTimeout(io, timeout) catch @panic("owned retirement callback was not entered");
    const saturated = wal.beginOwnedRead(a, 1, control);
    fixture.release.set(io);
    destroy.done.waitTimeout(io, timeout) catch @panic("owned retirement failed to join");
    thread.join();
    joined = true;
    if (saturated) |unexpected| {
        unexpected.destroy();
        return error.TestRetirementEscapedCapacity;
    } else |err| try std.testing.expectEqual(error.Backpressured, err);
    operations[0] = try wal.beginOwnedRead(a, 1, control);
}
