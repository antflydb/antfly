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

//! Idiomatic Zig wrapper over LMDB (Lightning Memory-Mapped Database).
//!
//! Provides RAII-style resource management, Zig error unions, and typed
//! key/value access over the LMDB C API. LMDB itself is compiled from
//! source via Zig's C compiler — no external dependencies.
//!
//! Usage:
//!   var env = try Environment.open("/path/to/db", .{});
//!   defer env.close();
//!
//!   // Write
//!   var txn = try env.begin(.{});
//!   try txn.put(dbi, "key", "value", .{});
//!   try txn.commit();
//!
//!   // Read
//!   var txn = try env.begin(.{ .read_only = true });
//!   defer txn.abort();
//!   const val = try txn.get(dbi, "key");

const builtin = @import("builtin");
const std = @import("std");
const platform = @import("antfly_platform");
const zig_lmdb = @import("lmdb_engine");
const lmdb_sim_test = @import("lmdb_sim_test.zig");
const c_backend = @import("lmdb_c_backend.zig");
const c = @import("lmdb_c_api.zig").Bindings;
const zig_backend = @import("lmdb_zig_backend.zig");

const use_zig_backend = zig_lmdb.is_zig_backend;
const storage_sim_soak = zig_lmdb.storage_sim_soak;
// Background derived/WAL workers can open read txns on the same wrapper
// environment while another thread is publishing. The Zig backend keeps
// mutable environment state around `refresh()`, so the wrapper serializes
// those environment entry points and tolerates short-lived writer lock
// contention instead of surfacing spurious root-suite flakes.
const writer_lock_retry_count: usize = 2000;
const writer_lock_retry_sleep_ns: u64 = 100_000;

fn heapAllocator() std.mem.Allocator {
    return platform.allocator.processAllocator(std.heap.smp_allocator);
}

fn lockAtomic(mutex: *std.atomic.Mutex) void {
    while (!mutex.tryLock()) std.Thread.yield() catch {};
}

fn backoffWriterLockRetry() void {
    std.Thread.yield() catch {};
    if (@hasDecl(std.Thread, "sleep")) {
        std.Thread.sleep(writer_lock_retry_sleep_ns);
    }
}

fn lockWrapperEnvironment(env: *Environment) void {
    lockAtomic(&env.mutex);
}

// ============================================================================
// Error handling
// ============================================================================

pub const Error = error{
    /// key/data pair already exists
    KeyExists,
    /// key/data pair not found
    NotFound,
    /// Requested page not found - database may be corrupted
    PageNotFound,
    /// Database is corrupted
    Corrupted,
    /// Environment had fatal error
    Panic,
    /// Database version mismatch
    VersionMismatch,
    /// File is not a valid LMDB file
    Invalid,
    /// Environment mapsize reached
    MapFull,
    /// Environment maxdbs reached
    DbsFull,
    /// Environment maxreaders reached
    ReadersFull,
    /// Too many TLS keys in use
    TlsFull,
    /// Another write transaction already owns the writer lock
    WriterLocked,
    /// Txn has too many dirty pages
    TxnFull,
    /// Cursor stack too deep
    CursorFull,
    /// Page has not enough space
    PageFull,
    /// Database contents grew beyond mapsize
    MapResized,
    /// Operation not supported or incompatible
    Incompatible,
    /// Invalid reuse of reader locktable slot
    BadReaderSlot,
    /// Transaction must abort, has child or is bad
    BadTxn,
    /// Unsupported key/value size
    BadValSize,
    /// Bad DBI
    BadDbi,
    /// Unexpected system error
    LmdbUnexpected,
};

pub fn check(rc: i32) Error!void {
    if (rc == 0) return;
    return switch (rc) {
        c.MDB_KEYEXIST => Error.KeyExists,
        c.MDB_NOTFOUND => Error.NotFound,
        c.MDB_PAGE_NOTFOUND => Error.PageNotFound,
        c.MDB_CORRUPTED => Error.Corrupted,
        c.MDB_PANIC => Error.Panic,
        c.MDB_VERSION_MISMATCH => Error.VersionMismatch,
        c.MDB_INVALID => Error.Invalid,
        c.MDB_MAP_FULL => Error.MapFull,
        c.MDB_DBS_FULL => Error.DbsFull,
        c.MDB_READERS_FULL => Error.ReadersFull,
        c.MDB_TLS_FULL => Error.TlsFull,
        c.MDB_TXN_FULL => Error.TxnFull,
        c.MDB_CURSOR_FULL => Error.CursorFull,
        c.MDB_PAGE_FULL => Error.PageFull,
        c.MDB_MAP_RESIZED => Error.MapResized,
        c.MDB_INCOMPATIBLE => Error.Incompatible,
        c.MDB_BAD_RSLOT => Error.BadReaderSlot,
        c.MDB_BAD_TXN => Error.BadTxn,
        c.MDB_BAD_VALSIZE => Error.BadValSize,
        c.MDB_BAD_DBI => Error.BadDbi,
        else => Error.LmdbUnexpected,
    };
}

pub fn mapZigError(err: anyerror) Error {
    return switch (err) {
        error.NotFound => Error.NotFound,
        error.Corrupted,
        error.TruncatedPage,
        error.TruncatedMetaPage,
        error.TruncatedNode,
        error.InvalidNodeIndex,
        error.InvalidNodeOffset,
        error.NoValidMetaPages,
        error.PageOutOfBounds,
        error.UnsupportedPageType,
        error.UnsupportedNodeFlags,
        error.InvalidPageSize,
        error.InvalidMagic,
        error.EmptyDataFile,
        => Error.Corrupted,
        error.InvalidVersion => Error.VersionMismatch,
        error.Incompatible => Error.Incompatible,
        error.WriterLocked => Error.WriterLocked,
        error.InvalidDbi => Error.BadDbi,
        error.ChildTransactionActive => Error.BadTxn,
        error.TransactionClosed,
        error.WriteTransactionsUnsupported,
        error.CreateUnsupported,
        => Error.BadTxn,
        error.KeyExists => Error.KeyExists,
        error.MapFull => Error.MapFull,
        error.CursorStackOverflow => Error.CursorFull,
        else => Error.LmdbUnexpected,
    };
}

fn syncZigEnvironment(zig_env: *zig_lmdb.env.Environment, force: bool) Error!void {
    if (zig_env.opts.write_map) {
        if (!force and zig_env.opts.no_sync) return;
        zig_env.syncMapped(zig_env.opts.map_async and !force) catch |err| return mapZigError(err);
        return;
    }

    if (!force and zig_env.opts.no_sync) return;

    if (force or !zig_env.opts.no_meta_sync) {
        while (true) switch (std.posix.errno(std.posix.system.fsync(zig_env.fd))) {
            .SUCCESS => return,
            .INTR => continue,
            else => return Error.LmdbUnexpected,
        };
    }

    std.posix.fdatasync(zig_env.fd) catch return Error.LmdbUnexpected;
}

// ============================================================================
// Environment
// ============================================================================

pub const EnvironmentOptions = struct {
    /// Maximum number of named databases (0 = only default unnamed db)
    max_dbs: u32 = 0,
    /// Maximum number of concurrent readers
    max_readers: u32 = 126,
    /// Map size in bytes (default 10MB, grow as needed)
    map_size: usize = 10 * 1024 * 1024,
    /// Use MDB_FIXEDMAP: map at a fixed virtual address
    fixed_map: bool = false,
    /// Use MDB_NOSUBDIR: path is a file, not a directory
    no_subdir: bool = false,
    /// Use MDB_RDONLY: open in read-only mode
    read_only: bool = false,
    /// Use MDB_NOSYNC: don't fsync after commit (faster, less safe)
    no_sync: bool = false,
    /// Use MDB_NOMETASYNC: defer metadata flushes
    no_meta_sync: bool = false,
    /// Use MDB_NOTLS: don't use thread-local storage for read txns
    no_tls: bool = false,
    /// Use MDB_WRITEMAP: writable memory map
    write_map: bool = false,
    /// Use MDB_MAPASYNC: asynchronous msync for writemap mode
    map_async: bool = false,
    /// Use MDB_NOLOCK: don't do any locking
    no_lock: bool = false,
    /// Use MDB_NORDAHEAD: disable OS read-ahead
    no_read_ahead: bool = false,
    /// Use MDB_NOMEMINIT: don't zero malloc'd pages before writing
    no_mem_init: bool = false,
    /// Zig-only artificial sync delay for stress benchmarking
    artificial_sync_delay_ns: u64 = 0,
    /// Zig-only mode: defer page mutation work until commit preparation
    defer_page_mutation: bool = false,
    /// Zig-only commit publication backend
    commit_backend: zig_lmdb.env.CommitBackend = .sync,
    /// File mode for created files
    mode: u32 = 0o664,
};

pub const CommitStats = zig_lmdb.env.CommitStats;
pub const CommitBackend = zig_lmdb.env.CommitBackend;
pub const CommitPublishPhase = zig_lmdb.commit_support.CommitPublishPhase;

pub const Environment = struct {
    env: ?*c.MDB_env = null,
    zig_env: ?zig_lmdb.env.Environment = null,
    path_z: [:0]u8,
    opts: EnvironmentOptions,
    // The wrapper serializes Zig-backend environment transitions such as
    // `refresh()`, read-txn creation, and close so callers can safely share one
    // `Environment` across background readers and writers.
    mutex: std.atomic.Mutex = .unlocked,

    pub fn open(path: [*:0]const u8, opts: EnvironmentOptions) Error!Environment {
        if (use_zig_backend and opts.map_async and !opts.write_map) return Error.Incompatible;

        const alloc = heapAllocator();
        const path_owned = alloc.dupeZ(u8, std.mem.span(path)) catch return Error.LmdbUnexpected;
        errdefer alloc.free(path_owned);

        var zig_env: ?zig_lmdb.env.Environment = null;
        if (use_zig_backend) {
            zig_env = zig_backend.openEnvironment(path_owned, opts) catch |err| switch (err) {
                error.FileNotFound => blk: {
                    if (opts.read_only) return mapZigError(err);
                    try bootstrapZigDataFile(path, opts);
                    break :blk zig_backend.openEnvironment(path_owned, opts) catch |retry_err| return mapZigError(retry_err);
                },
                else => return mapZigError(err),
            };
        }

        var env: ?*c.MDB_env = null;
        if (!use_zig_backend) {
            env = try c_backend.openEnvironment(path, opts, check);
        }

        return .{
            .env = env,
            .zig_env = zig_env,
            .path_z = path_owned,
            .opts = opts,
        };
    }

    pub fn close(self: *Environment) void {
        lockWrapperEnvironment(self);
        if (self.zig_env) |*zig_env| zig_env.close();
        if (self.env) |env| c_backend.closeEnvironment(env);
        heapAllocator().free(self.path_z);
        self.mutex.unlock();
        self.* = undefined;
    }

    pub fn sync(self: *Environment, force: bool) Error!void {
        lockWrapperEnvironment(self);
        defer self.mutex.unlock();
        if (self.zig_env) |*zig_env| {
            try syncZigEnvironment(zig_env, force);
            return;
        }
        try c_backend.syncEnvironment(self.env orelse return Error.LmdbUnexpected, force, check);
    }

    pub fn setMapSize(self: *Environment, size: usize) Error!void {
        lockWrapperEnvironment(self);
        defer self.mutex.unlock();
        if (self.env) |env| {
            try c_backend.setMapSize(env, size, check);
        }
        if (self.zig_env) |*zig_env| {
            zig_env.ensureMappedSize(size) catch |err| return mapZigError(err);
        }
    }

    pub fn splitRightToFile(self: *Environment, split_key: []const u8, dest_file_path: []const u8) Error!bool {
        if (!use_zig_backend) return false;
        lockWrapperEnvironment(self);
        defer self.mutex.unlock();
        const zig_env = &(self.zig_env orelse return Error.LmdbUnexpected);
        zig_backend.writeRightSplit(zig_env, split_key, dest_file_path) catch |err| return mapZigError(err);
        return true;
    }

    pub fn splitRightNamedDbToFile(self: *Environment, db_name: ?[]const u8, split_key: []const u8, dest_file_path: []const u8) Error!bool {
        if (!use_zig_backend) return false;
        lockWrapperEnvironment(self);
        defer self.mutex.unlock();
        const zig_env = &(self.zig_env orelse return Error.LmdbUnexpected);
        zig_backend.writeRightSplitNamedDb(zig_env, db_name, split_key, dest_file_path) catch |err| return mapZigError(err);
        return true;
    }

    pub fn splitLeftToFile(self: *Environment, split_key: []const u8, dest_file_path: []const u8) Error!bool {
        if (!use_zig_backend) return false;
        lockWrapperEnvironment(self);
        defer self.mutex.unlock();
        const zig_env = &(self.zig_env orelse return Error.LmdbUnexpected);
        zig_backend.writeLeftSplit(zig_env, split_key, dest_file_path) catch |err| return mapZigError(err);
        return true;
    }

    pub fn splitLeftNamedDbToFile(self: *Environment, db_name: ?[]const u8, split_key: []const u8, dest_file_path: []const u8) Error!bool {
        if (!use_zig_backend) return false;
        lockWrapperEnvironment(self);
        defer self.mutex.unlock();
        const zig_env = &(self.zig_env orelse return Error.LmdbUnexpected);
        zig_backend.writeLeftSplitNamedDb(zig_env, db_name, split_key, dest_file_path) catch |err| return mapZigError(err);
        return true;
    }

    pub fn begin(self: *Environment, opts: TransactionOptions) Error!Transaction {
        return Transaction.begin(self, opts);
    }

    pub fn beginBatch(self: *Environment) Error!Batch {
        return Batch.begin(self);
    }

    pub fn commitStatsSnapshot(self: *Environment) ?CommitStats {
        lockWrapperEnvironment(self);
        defer self.mutex.unlock();
        if (self.zig_env) |*zig_env| return zig_env.commitStatsSnapshot();
        return null;
    }
};

pub fn bootstrapZigDataFile(path: [*:0]const u8, opts: EnvironmentOptions) Error!void {
    zig_backend.bootstrapDataFile(path, opts) catch return Error.LmdbUnexpected;
}

// ============================================================================
// Database handle
// ============================================================================

pub const Dbi = union(enum) {
    c: c.MDB_dbi,
    zig: zig_lmdb.txn.Dbi,
};

pub const DbOptions = struct {
    /// Create the named database if it doesn't exist
    create: bool = false,
    /// Keys compare in reverse lexicographic order
    reverse_key: bool = false,
    /// Keys are compared as native byte order unsigned integers
    integer_key: bool = false,
    /// Duplicate keys with sorted values
    dup_sort: bool = false,
    /// Duplicate values all have a fixed size
    dup_fixed: bool = false,
    /// Duplicate values compare as native byte order unsigned integers
    integer_dup: bool = false,
    /// Duplicate values compare as reverse strings
    reverse_dup: bool = false,
};

// ============================================================================
// Transaction
// ============================================================================

pub const TransactionOptions = struct {
    read_only: bool = false,
    defer_page_mutation: bool = false,
};

pub const Transaction = struct {
    backend: union(enum) {
        c: *c.MDB_txn,
        zig: *ZigTxn,
    },

    const ZigTxn = struct {
        wrapper_env: *Environment,
        env: *zig_lmdb.env.Environment,
        txn: zig_lmdb.txn.Transaction,
    };

    fn begin(env: *Environment, opts: TransactionOptions) Error!Transaction {
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            const txn = beginOnce(env, opts) catch |err| switch (err) {
                Error.WriterLocked => {
                    if (opts.read_only or attempt >= writer_lock_retry_count) return err;
                    backoffWriterLockRetry();
                    continue;
                },
                else => return err,
            };
            return txn;
        }
    }

    fn beginOnce(env: *Environment, opts: TransactionOptions) Error!Transaction {
        if (use_zig_backend) {
            lockWrapperEnvironment(env);
            defer env.mutex.unlock();
            const zig_env = &(env.zig_env orelse return Error.LmdbUnexpected);
            zig_env.refresh() catch |err| return mapZigError(err);
            const zig_txn = zig_backend.beginTransaction(zig_env, opts) catch |err| {
                return mapZigError(err);
            };
            const zig_state = heapAllocator().create(ZigTxn) catch return Error.LmdbUnexpected;
            errdefer heapAllocator().destroy(zig_state);
            zig_state.* = .{
                .wrapper_env = env,
                .env = zig_env,
                .txn = zig_txn,
            };

            return .{
                .backend = .{
                    .zig = zig_state,
                },
            };
        }

        return .{ .backend = .{ .c = try c_backend.beginTransaction(env.env orelse return Error.LmdbUnexpected, opts, check) } };
    }

    pub fn commit(self: *Transaction) Error!void {
        switch (self.backend) {
            .c => |txn| try c_backend.commitTransaction(txn, check),
            .zig => |zig_txn| {
                const wrapper_env = zig_txn.wrapper_env;
                lockWrapperEnvironment(wrapper_env);
                defer wrapper_env.mutex.unlock();
                zig_txn.txn.commit() catch |err| return mapZigError(err);
                heapAllocator().destroy(zig_txn);
            },
        }
        self.* = undefined;
    }

    pub fn abort(self: *Transaction) void {
        switch (self.backend) {
            .c => |txn| c_backend.abortTransaction(txn),
            .zig => |zig_txn| {
                zig_txn.txn.abort();
                heapAllocator().destroy(zig_txn);
            },
        }
        self.* = undefined;
    }

    pub fn publishCommitPhaseForTest(self: *Transaction, phase: CommitPublishPhase) Error!void {
        switch (self.backend) {
            .c => return Error.Incompatible,
            .zig => |zig_txn| {
                lockWrapperEnvironment(zig_txn.wrapper_env);
                defer zig_txn.wrapper_env.mutex.unlock();
                zig_lmdb.txn.publishCommitPhaseForTest(&zig_txn.txn, phase) catch |err| return mapZigError(err);
            },
        }
    }

    pub fn beginChild(self: *Transaction) Error!Transaction {
        switch (self.backend) {
            .c => |txn| {
                return .{ .backend = .{ .c = try c_backend.beginChildTransaction(txn, check) } };
            },
            .zig => |zig_txn| {
                const child_txn = zig_backend.beginChildTransaction(&zig_txn.txn) catch |err| return mapZigError(err);
                const child_state = heapAllocator().create(ZigTxn) catch return Error.LmdbUnexpected;
                errdefer heapAllocator().destroy(child_state);
                child_state.* = .{
                    .wrapper_env = zig_txn.wrapper_env,
                    .env = zig_txn.env,
                    .txn = child_txn,
                };
                child_state.txn.rebindEnv(child_state.env);
                return .{ .backend = .{ .zig = child_state } };
            },
        }
    }

    pub fn openDb(self: *Transaction, name: ?[*:0]const u8, opts: DbOptions) Error!Dbi {
        switch (self.backend) {
            .c => |txn| {
                return .{ .c = try c_backend.openDb(txn, name, opts, check) };
            },
            .zig => |zig_txn| {
                const zig_name = if (name) |n| std.mem.span(n) else null;
                const dbi = zig_txn.txn.openDb(zig_name, .{
                    .create = opts.create,
                    .reverse_key = opts.reverse_key,
                    .integer_key = opts.integer_key,
                    .dup_sort = opts.dup_sort,
                    .dup_fixed = opts.dup_fixed,
                    .integer_dup = opts.integer_dup,
                    .reverse_dup = opts.reverse_dup,
                }) catch |err| return mapZigError(err);
                return .{ .zig = dbi };
            },
        }
    }

    pub fn get(self: *Transaction, dbi: Dbi, key: []const u8) Error![]const u8 {
        switch (self.backend) {
            .c => |txn| {
                const c_dbi = switch (dbi) {
                    .c => |c_value| c_value,
                    .zig => return Error.BadDbi,
                };
                return try c_backend.get(txn, c_dbi, key, check);
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                return zig_txn.txn.get(zig_dbi, key) catch |err| return mapZigError(err);
            },
        }
    }

    pub fn put(self: *Transaction, dbi: Dbi, key: []const u8, value: []const u8, opts: PutOptions) Error!void {
        switch (self.backend) {
            .c => |txn| {
                const c_dbi = switch (dbi) {
                    .c => |c_value| c_value,
                    .zig => return Error.BadDbi,
                };
                try c_backend.put(txn, c_dbi, key, value, opts, check);
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                zig_txn.txn.put(zig_dbi, key, value, .{
                    .no_overwrite = opts.no_overwrite,
                    .no_dup_data = opts.no_dup_data,
                    .append = opts.append,
                    .append_dup = opts.append_dup,
                }) catch |err| return mapZigError(err);
            },
        }
    }

    pub fn reserve(self: *Transaction, dbi: Dbi, key: []const u8, size: usize, opts: ReserveOptions) Error![]u8 {
        switch (self.backend) {
            .c => |txn| {
                const c_dbi = switch (dbi) {
                    .c => |c_value| c_value,
                    .zig => return Error.BadDbi,
                };
                return try c_backend.reserve(txn, c_dbi, key, size, opts, check);
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                return zig_txn.txn.reserve(zig_dbi, key, size, .{
                    .no_overwrite = opts.no_overwrite,
                    .append = opts.append,
                }) catch |err| return mapZigError(err);
            },
        }
    }

    pub fn delete(self: *Transaction, dbi: Dbi, key: []const u8) Error!void {
        switch (self.backend) {
            .c => |txn| {
                const c_dbi = switch (dbi) {
                    .c => |c_value| c_value,
                    .zig => return Error.BadDbi,
                };
                try c_backend.delete(txn, c_dbi, key, check);
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                zig_txn.txn.delete(zig_dbi, key) catch |err| return mapZigError(err);
            },
        }
    }

    pub fn deleteValue(self: *Transaction, dbi: Dbi, key: []const u8, value: []const u8) Error!void {
        switch (self.backend) {
            .c => |txn| {
                const c_dbi = switch (dbi) {
                    .c => |c_value| c_value,
                    .zig => return Error.BadDbi,
                };
                try c_backend.deleteValue(txn, c_dbi, key, value, check);
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                zig_txn.txn.deleteValue(zig_dbi, key, value) catch |err| return mapZigError(err);
            },
        }
    }

    pub fn cursor(self: *Transaction, dbi: Dbi) Error!Cursor {
        return Cursor.open(self, dbi);
    }

    pub fn rangeScanner(self: *Transaction, dbi: Dbi, start_key: []const u8) Error!RangeBatchIterator {
        return RangeBatchIterator.open(self, dbi, start_key);
    }

    pub fn rangeViewScanner(self: *Transaction, dbi: Dbi, start_key: []const u8) Error!RangeViewIterator {
        return RangeViewIterator.open(self, dbi, start_key);
    }
};

pub const Batch = struct {
    txn: Transaction,

    fn begin(env: *Environment) Error!Batch {
        return .{
            .txn = try env.begin(.{ .defer_page_mutation = true }),
        };
    }

    pub fn asTransaction(self: *Batch) *Transaction {
        return &self.txn;
    }

    pub fn commit(self: *Batch) Error!void {
        try self.txn.commit();
        self.* = undefined;
    }

    pub fn abort(self: *Batch) void {
        self.txn.abort();
        self.* = undefined;
    }

    pub fn openDb(self: *Batch, name: ?[*:0]const u8, opts: DbOptions) Error!Dbi {
        return self.txn.openDb(name, opts);
    }

    pub fn get(self: *Batch, dbi: Dbi, key: []const u8) Error![]const u8 {
        return self.txn.get(dbi, key);
    }

    pub fn put(self: *Batch, dbi: Dbi, key: []const u8, value: []const u8, opts: PutOptions) Error!void {
        try self.txn.put(dbi, key, value, opts);
    }

    pub fn reserve(self: *Batch, dbi: Dbi, key: []const u8, size: usize, opts: ReserveOptions) Error![]u8 {
        return self.txn.reserve(dbi, key, size, opts);
    }

    pub fn delete(self: *Batch, dbi: Dbi, key: []const u8) Error!void {
        try self.txn.delete(dbi, key);
    }

    pub fn deleteValue(self: *Batch, dbi: Dbi, key: []const u8, value: []const u8) Error!void {
        try self.txn.deleteValue(dbi, key, value);
    }

    pub fn cursor(self: *Batch, dbi: Dbi) Error!Cursor {
        return self.txn.cursor(dbi);
    }
};

pub const PutOptions = struct {
    /// Don't overwrite existing key
    no_overwrite: bool = false,
    /// Don't add duplicate data (for MDB_DUPSORT)
    no_dup_data: bool = false,
    /// Append key to end (keys must be in order)
    append: bool = false,
    /// Append duplicate data to the end of a duplicate set
    append_dup: bool = false,
};

pub const ReserveOptions = struct {
    /// Don't overwrite existing key
    no_overwrite: bool = false,
    /// Append key to end (keys must be in order)
    append: bool = false,
};

// ============================================================================
// Cursor
// ============================================================================

pub const CursorOp = enum(u32) {
    first = c.MDB_FIRST,
    first_dup = c.MDB_FIRST_DUP,
    last = c.MDB_LAST,
    last_dup = c.MDB_LAST_DUP,
    next = c.MDB_NEXT,
    next_dup = c.MDB_NEXT_DUP,
    next_nodup = c.MDB_NEXT_NODUP,
    prev = c.MDB_PREV,
    prev_dup = c.MDB_PREV_DUP,
    prev_nodup = c.MDB_PREV_NODUP,
    get_both = c.MDB_GET_BOTH,
    get_both_range = c.MDB_GET_BOTH_RANGE,
    set = c.MDB_SET,
    set_key = c.MDB_SET_KEY,
    set_range = c.MDB_SET_RANGE,
    get_current = c.MDB_GET_CURRENT,
};

pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub const Cursor = struct {
    backend: union(enum) {
        c: *c.MDB_cursor,
        zig: zig_lmdb.cursor.Cursor,
    },

    fn open(txn: *Transaction, dbi: Dbi) Error!Cursor {
        switch (txn.backend) {
            .c => |c_txn| {
                const c_dbi = switch (dbi) {
                    .c => |c_value| c_value,
                    .zig => return Error.BadDbi,
                };
                return .{ .backend = .{ .c = try c_backend.openCursor(c_txn, c_dbi, check) } };
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                return .{ .backend = .{ .zig = zig_lmdb.cursor.Cursor.init(&zig_txn.txn, zig_dbi) } };
            },
        }
    }

    pub fn close(self: *Cursor) void {
        switch (self.backend) {
            .c => |cursor| c_backend.closeCursor(cursor),
            .zig => {},
        }
        self.* = undefined;
    }

    pub fn getEntry(self: *Cursor, op: CursorOp) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGet(cursor, @intFromEnum(op), check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                return switch (op) {
                    .first => blk: {
                        const entry = cursor.first() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .first_dup => blk: {
                        const entry = cursor.firstDup() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .next => blk: {
                        const entry = cursor.next() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .prev => blk: {
                        const entry = cursor.prev() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .next_dup => blk: {
                        const entry = cursor.nextDup() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .prev_dup => blk: {
                        const entry = cursor.prevDup() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .next_nodup => blk: {
                        const entry = cursor.nextNoDup() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .prev_nodup => blk: {
                        const entry = cursor.prevNoDup() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .last => blk: {
                        const entry = cursor.last() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .last_dup => blk: {
                        const entry = cursor.lastDup() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    .get_current => blk: {
                        const entry = cursor.getCurrent() catch |err| return mapZigError(err);
                        break :blk .{ .key = entry.key, .value = entry.value };
                    },
                    else => Error.Incompatible,
                };
            },
        }
    }

    pub fn set(self: *Cursor, key: []const u8) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGetWithKey(cursor, key, @intFromEnum(CursorOp.set_key), check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                const entry = cursor.set(key) catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        }
    }

    pub fn first(self: *Cursor) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGet(cursor, c.MDB_FIRST, check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                const entry = cursor.first() catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        }
    }

    pub fn next(self: *Cursor) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGet(cursor, c.MDB_NEXT, check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                const entry = cursor.next() catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        }
    }

    pub fn seek(self: *Cursor, key: []const u8) Error!Entry {
        return self.seekRange(key);
    }

    pub fn seekRange(self: *Cursor, key: []const u8) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGetWithKey(cursor, key, c.MDB_SET_RANGE, check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                const entry = cursor.setRange(key) catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        }
    }

    pub fn seekExact(self: *Cursor, key: []const u8) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGetWithKey(cursor, key, c.MDB_SET_KEY, check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                const entry = cursor.set(key) catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        }
    }

    pub fn getBoth(self: *Cursor, key: []const u8, value: []const u8) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGetWithKeyValue(cursor, key, value, c.MDB_GET_BOTH, check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                const entry = cursor.getBoth(key, value) catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        }
    }

    pub fn getBothRange(self: *Cursor, key: []const u8, value: []const u8) Error!Entry {
        switch (self.backend) {
            .c => |cursor| {
                const entry = try c_backend.cursorGetWithKeyValue(cursor, key, value, c.MDB_GET_BOTH_RANGE, check);
                return .{ .key = entry.key, .value = entry.value };
            },
            .zig => |*cursor| {
                const entry = cursor.getBothRange(key, value) catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        }
    }

    pub fn putEntry(self: *Cursor, key: []const u8, value: []const u8, opts: PutOptions) Error!void {
        switch (self.backend) {
            .c => |cursor| try c_backend.cursorPut(cursor, key, value, opts, check),
            .zig => |*cursor| cursor.put(key, value, .{
                .no_overwrite = opts.no_overwrite,
                .no_dup_data = opts.no_dup_data,
                .append = opts.append,
                .append_dup = opts.append_dup,
            }) catch |err| return mapZigError(err),
        }
    }

    pub fn reserveEntry(self: *Cursor, key: []const u8, size: usize, opts: ReserveOptions) Error![]u8 {
        switch (self.backend) {
            .c => |cursor| return try c_backend.cursorReserve(cursor, key, size, opts, check),
            .zig => |*cursor| {
                return cursor.reserve(key, size, .{
                    .no_overwrite = opts.no_overwrite,
                    .append = opts.append,
                }) catch |err| return mapZigError(err);
            },
        }
    }

    pub fn deleteEntry(self: *Cursor) Error!void {
        switch (self.backend) {
            .c => |cursor| try c_backend.deleteCurrent(cursor, check),
            .zig => |*cursor| cursor.deleteCurrent() catch |err| return mapZigError(err),
        }
    }
};

pub const RangeBatchIterator = struct {
    backend: union(enum) {
        c: struct {
            cursor: Cursor,
            pending: ?Entry,
        },
        zig: zig_lmdb.cursor.PlainScanner,
    },

    fn open(txn: *Transaction, dbi: Dbi, start_key: []const u8) Error!RangeBatchIterator {
        switch (txn.backend) {
            .c => {
                var cursor = try Cursor.open(txn, dbi);
                errdefer cursor.close();
                const first = cursor.seekRange(start_key) catch |err| switch (err) {
                    Error.NotFound => return .{ .backend = .{ .c = .{ .cursor = cursor, .pending = null } } },
                    else => return err,
                };
                return .{ .backend = .{ .c = .{ .cursor = cursor, .pending = first } } };
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                var scanner = zig_lmdb.cursor.PlainScanner.init(&zig_txn.txn, zig_dbi) catch |err| return mapZigError(err);
                scanner.seekRange(start_key) catch |err| switch (err) {
                    error.NotFound => return .{ .backend = .{ .zig = scanner } },
                    else => return mapZigError(err),
                };
                return .{ .backend = .{ .zig = scanner } };
            },
        }
    }

    pub fn close(self: *RangeBatchIterator) void {
        switch (self.backend) {
            .c => |*state| state.cursor.close(),
            .zig => {},
        }
        self.* = undefined;
    }

    pub fn nextBatch(self: *RangeBatchIterator, out: []Entry) Error!usize {
        if (out.len == 0) return 0;
        switch (self.backend) {
            .c => |*state| {
                var written: usize = 0;
                if (state.pending) |pending| {
                    out[written] = pending;
                    written += 1;
                    state.pending = null;
                }
                while (written < out.len) {
                    out[written] = state.cursor.next() catch |err| switch (err) {
                        Error.NotFound => break,
                        else => return err,
                    };
                    written += 1;
                }
                if (written == 0) return Error.NotFound;
                return written;
            },
            .zig => |*scanner| {
                const zig_out: []zig_lmdb.cursor.Entry = @as([*]zig_lmdb.cursor.Entry, @ptrCast(out.ptr))[0..out.len];
                return scanner.nextBatch(zig_out) catch |err| switch (err) {
                    error.NotFound => Error.NotFound,
                    else => mapZigError(err),
                };
            },
        }
    }
};

pub const RangeViewBatch = union(enum) {
    c: []const Entry,
    zig: zig_lmdb.cursor.PlainLeafBatch,

    pub fn len(self: RangeViewBatch) usize {
        return switch (self) {
            .c => |entries| entries.len,
            .zig => |batch| batch.len(),
        };
    }

    pub fn entryAt(self: RangeViewBatch, index: usize) Error!Entry {
        return switch (self) {
            .c => |entries| if (index < entries.len) entries[index] else Error.NotFound,
            .zig => |batch| {
                const entry = batch.entryAt(index) catch |err| return mapZigError(err);
                return .{ .key = entry.key, .value = entry.value };
            },
        };
    }
};

pub const RangeViewIterator = struct {
    backend: union(enum) {
        c: struct {
            cursor: Cursor,
            pending: ?Entry,
            buffer: [256]Entry = undefined,
        },
        zig: zig_lmdb.cursor.PlainScanner,
    },

    fn open(txn: *Transaction, dbi: Dbi, start_key: []const u8) Error!RangeViewIterator {
        switch (txn.backend) {
            .c => {
                var cursor = try Cursor.open(txn, dbi);
                errdefer cursor.close();
                const first = cursor.seekRange(start_key) catch |err| switch (err) {
                    Error.NotFound => {
                        return .{ .backend = .{ .c = .{ .cursor = cursor, .pending = null } } };
                    },
                    else => return err,
                };
                return .{ .backend = .{ .c = .{ .cursor = cursor, .pending = first } } };
            },
            .zig => |zig_txn| {
                const zig_dbi = switch (dbi) {
                    .zig => |zig_value| zig_value,
                    .c => return Error.BadDbi,
                };
                var scanner = zig_lmdb.cursor.PlainScanner.init(&zig_txn.txn, zig_dbi) catch |err| return mapZigError(err);
                scanner.seekRange(start_key) catch |err| switch (err) {
                    error.NotFound => return .{ .backend = .{ .zig = scanner } },
                    else => return mapZigError(err),
                };
                return .{ .backend = .{ .zig = scanner } };
            },
        }
    }

    pub fn close(self: *RangeViewIterator) void {
        switch (self.backend) {
            .c => |*state| state.cursor.close(),
            .zig => {},
        }
        self.* = undefined;
    }

    pub fn nextViewBatch(self: *RangeViewIterator) Error!RangeViewBatch {
        switch (self.backend) {
            .c => |*state| {
                var written: usize = 0;
                if (state.pending) |pending| {
                    state.buffer[written] = pending;
                    written += 1;
                    state.pending = null;
                }
                while (written < state.buffer.len) {
                    state.buffer[written] = state.cursor.next() catch |err| switch (err) {
                        Error.NotFound => break,
                        else => return err,
                    };
                    written += 1;
                }
                if (written == 0) return Error.NotFound;
                return .{ .c = state.buffer[0..written] };
            },
            .zig => |*scanner| {
                const batch = scanner.nextLeafBatch() catch |err| return mapZigError(err);
                return .{ .zig = batch };
            },
        }
    }
};

// ============================================================================
// Iterator helper
// ============================================================================

/// Iterate all entries in a database within a transaction.
pub fn iterate(txn: *Transaction, dbi: Dbi) Error!CursorIterator {
    var cur = try txn.cursor(dbi);
    const first = cur.first() catch |err| switch (err) {
        Error.NotFound => return .{ .cursor = cur, .done = true },
        else => return err,
    };
    return .{ .cursor = cur, .done = false, .current = first };
}

pub const CursorIterator = struct {
    cursor: Cursor,
    done: bool,
    current: Entry = .{ .key = &.{}, .value = &.{} },

    pub fn next(self: *CursorIterator) ?Entry {
        if (self.done) return null;
        const entry = self.current;
        self.current = self.cursor.next() catch {
            self.done = true;
            return entry;
        };
        return entry;
    }

    pub fn deinit(self: *CursorIterator) void {
        self.cursor.close();
    }
};

// ============================================================================
// Tests
// ============================================================================

test "open, put, get, delete" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    // Write
    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "hello", "world", .{});
        try txn.put(dbi, "foo", "bar", .{});
        try txn.commit();
    }

    // Read
    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        const val = try txn.get(dbi, "hello");
        try std.testing.expectEqualStrings("world", val);

        const val2 = try txn.get(dbi, "foo");
        try std.testing.expectEqualStrings("bar", val2);
    }

    // Delete
    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{});
        try txn.delete(dbi, "hello");
        try txn.commit();
    }

    // Verify delete
    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        const err = txn.get(dbi, "hello");
        try std.testing.expectError(Error.NotFound, err);
    }
}

test "batch persists staged writes" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    {
        var batch = try env.beginBatch();
        errdefer batch.abort();

        const dbi = try batch.openDb(null, .{ .create = true });
        try batch.put(dbi, "alpha", "1", .{});
        try batch.put(dbi, "beta", "2", .{});
        try batch.put(dbi, "gamma", "3", .{});
        try batch.delete(dbi, "beta");
        try batch.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(dbi, "alpha"));
        try std.testing.expectEqualStrings("3", try txn.get(dbi, "gamma"));
        try std.testing.expectError(Error.NotFound, txn.get(dbi, "beta"));
    }
}

test "cursor iteration" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    // Write sorted data
    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "a", "1", .{});
        try txn.put(dbi, "b", "2", .{});
        try txn.put(dbi, "c", "3", .{});
        try txn.put(dbi, "d", "4", .{});
        try txn.commit();
    }

    // Iterate
    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        var iter = try iterate(&txn, dbi);
        defer iter.deinit();

        const expected_keys = [_][]const u8{ "a", "b", "c", "d" };
        const expected_vals = [_][]const u8{ "1", "2", "3", "4" };

        var i: usize = 0;
        while (iter.next()) |entry| {
            try std.testing.expectEqualStrings(expected_keys[i], entry.key);
            try std.testing.expectEqualStrings(expected_vals[i], entry.value);
            i += 1;
        }
        try std.testing.expectEqual(@as(usize, 4), i);
    }
}

test "cursor seekExact finds only exact keys" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();
        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "alpha", "1", .{});
        try txn.put(dbi, "alphabet", "2", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();
        const dbi = try txn.openDb(null, .{});
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try std.testing.expectEqualStrings("1", (try cur.seekExact("alpha")).value);
        try std.testing.expectError(Error.NotFound, cur.seekExact("alp"));
        try std.testing.expectEqualStrings("1", (try cur.seekRange("alp")).value);
    }
}

test "zig backend rejects unsupported env durability flags" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    try std.testing.expectError(Error.Incompatible, Environment.open(tmp_path, .{ .map_async = true }));
}

test "zig backend accepts no_read_ahead" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .no_read_ahead = true });
    defer env.close();
}

test "zig backend accepts no_sync no_meta_sync and no_tls" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    {
        var env = try Environment.open(tmp_path, .{
            .no_sync = true,
            .no_meta_sync = true,
            .no_tls = true,
        });
        defer env.close();
    }

    {
        var env = try Environment.open(tmp_path, .{
            .no_sync = true,
        });
        defer env.close();

        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "alpha", "1", .{});
        try txn.commit();
    }
}

test "zig backend sync forces durability for no_sync envs" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    {
        var env = try Environment.open(tmp_path, .{
            .no_sync = true,
            .no_meta_sync = true,
        });
        defer env.close();

        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "alpha", "1", .{});
        try txn.commit();
        try env.sync(true);
    }

    {
        var env = try Environment.open(tmp_path, .{});
        defer env.close();

        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();
        const dbi = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("1", try txn.get(dbi, "alpha"));
    }
}

test "zig backend commit stats track publication phases" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{});
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "alpha", "1", .{});
        try txn.commit();
    }

    const stats = env.commitStatsSnapshot().?;
    try std.testing.expectEqual(@as(u64, 1), stats.publish_calls);
    try std.testing.expectEqual(@as(u64, 1), stats.full_publish_calls);
    try std.testing.expect(stats.page_images_written > 0);
    try std.testing.expect(stats.bytes_written > 0);
    try std.testing.expect(stats.total_page_write_ns > 0);
    try std.testing.expect(stats.total_publish_ns > 0);
}

test "zig backend accepts worker-thread commit backend" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{
        .commit_backend = .worker_thread,
    });
    defer env.close();

    var txn = try env.begin(.{});
    errdefer txn.abort();

    const dbi = try txn.openDb(null, .{ .create = true });
    try txn.put(dbi, "alpha", "1", .{});
    try txn.commit();

    const stats = env.commitStatsSnapshot().?;
    try std.testing.expectEqual(@as(u64, 1), stats.publish_calls);
    try std.testing.expect(stats.total_publish_ns > 0);
}

test "zig backend accepts async-io commit backend" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{
        .commit_backend = .async_io,
    });
    defer env.close();

    var txn = try env.begin(.{});
    errdefer txn.abort();

    const dbi = try txn.openDb(null, .{ .create = true });
    try txn.put(dbi, "alpha", "1", .{});
    try txn.commit();

    const stats = env.commitStatsSnapshot().?;
    try std.testing.expectEqual(@as(u64, 1), stats.publish_calls);
    try std.testing.expect(stats.total_publish_ns > 0);
}

test "zig backend accepts adaptive commit backend" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{
        .commit_backend = .adaptive,
    });
    defer env.close();

    var txn = try env.begin(.{});
    errdefer txn.abort();

    const dbi = try txn.openDb(null, .{ .create = true });
    try txn.put(dbi, "alpha", "1", .{});
    try txn.commit();

    const stats = env.commitStatsSnapshot().?;
    try std.testing.expectEqual(@as(u64, 1), stats.publish_calls);
    try std.testing.expect(stats.total_publish_ns > 0);
}

test "zig backend async-io survives repeated reopen cycles" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var cycle: usize = 0;
    while (cycle < 8) : (cycle += 1) {
        var env = try Environment.open(tmp_path, .{
            .commit_backend = .async_io,
            .max_dbs = 2,
        });
        defer env.close();

        var txn = try env.begin(.{});
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        var key_buf: [32]u8 = undefined;
        var value_buf: [32]u8 = undefined;
        const key = try std.fmt.bufPrint(&key_buf, "k-{d}", .{cycle});
        const value = try std.fmt.bufPrint(&value_buf, "v-{d}", .{cycle});
        try txn.put(main, key, value, .{});
        try txn.commit();
    }

    var env = try Environment.open(tmp_path, .{
        .commit_backend = .async_io,
        .max_dbs = 2,
    });
    defer env.close();

    var txn = try env.begin(.{ .read_only = true });
    defer txn.abort();

    const main = try txn.openDb(null, .{});
    var key_buf: [32]u8 = undefined;
    var value_buf: [32]u8 = undefined;
    var cycle_check: usize = 0;
    while (cycle_check < 8) : (cycle_check += 1) {
        const key = try std.fmt.bufPrint(&key_buf, "k-{d}", .{cycle_check});
        const expected = try std.fmt.bufPrint(&value_buf, "v-{d}", .{cycle_check});
        try std.testing.expectEqualStrings(expected, try txn.get(main, key));
    }
}

test "zig backend accepts no_lock and skips single-writer exclusion" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{
        .max_dbs = 1,
        .no_lock = true,
    });
    defer env.close();

    var first = try env.begin(.{});
    defer first.abort();

    var second = try env.begin(.{});
    defer second.abort();

    const dbi = try first.openDb(null, .{ .create = true });
    try first.put(dbi, "alpha", "1", .{});
}

test "zig backend accepts no_mem_init" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{
        .max_dbs = 1,
        .no_mem_init = true,
    });
    defer env.close();

    var txn = try env.begin(.{});
    errdefer txn.abort();

    const dbi = try txn.openDb(null, .{ .create = true });
    try txn.put(dbi, "alpha", "1", .{});
    try txn.commit();
}

test "zig backend accepts write_map and map_async" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    {
        var env = try Environment.open(tmp_path, .{
            .max_dbs = 1,
            .write_map = true,
        });
        defer env.close();

        var txn = try env.begin(.{});
        errdefer txn.abort();
        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "alpha", "1", .{});
        try txn.commit();
    }

    {
        var env = try Environment.open(tmp_path, .{
            .max_dbs = 1,
            .write_map = true,
            .map_async = true,
        });
        defer env.close();

        var txn = try env.begin(.{});
        errdefer txn.abort();
        const dbi = try txn.openDb(null, .{});
        try txn.put(dbi, "beta", "2", .{});
        try txn.commit();
    }
}

test "zig backend accepts fixed_map" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{
        .max_dbs = 1,
        .fixed_map = true,
    });
    defer env.close();

    var txn = try env.begin(.{});
    errdefer txn.abort();
    const dbi = try txn.openDb(null, .{ .create = true });
    try txn.put(dbi, "alpha", "1", .{});
    try txn.commit();
}

test "cursor seek (set_range)" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{});
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        try txn.put(dbi, "aaa", "1", .{});
        try txn.put(dbi, "bbb", "2", .{});
        try txn.put(dbi, "ccc", "3", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        var cur = try txn.cursor(dbi);
        defer cur.close();

        // Seek to "b" should land on "bbb"
        const entry = try cur.seek("b");
        try std.testing.expectEqualStrings("bbb", entry.key);
        try std.testing.expectEqualStrings("2", entry.value);
    }
}

test "reserve stores caller-filled value" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        const reserved = try txn.reserve(dbi, "blob", 5, .{});
        @memcpy(reserved, "hello");
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("hello", try txn.get(dbi, "blob"));
    }
}

test "cursor reserve stores caller-filled value" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });
        var cursor = try txn.cursor(dbi);
        const reserved = try cursor.reserveEntry("blob", 5, .{});
        @memcpy(reserved, "world");
        cursor.close();
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings("world", try txn.get(dbi, "blob"));
    }
}

test "named databases" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 4 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const docs = try txn.openDb("docs", .{ .create = true });
        const meta = try txn.openDb("meta", .{ .create = true });

        try txn.put(docs, "doc1", "content1", .{});
        try txn.put(meta, "schema", "v1", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const docs = try txn.openDb("docs", .{});
        const meta = try txn.openDb("meta", .{});

        try std.testing.expectEqualStrings("content1", try txn.get(docs, "doc1"));
        try std.testing.expectEqualStrings("v1", try txn.get(meta, "schema"));

        // Cross-db isolation
        try std.testing.expectError(Error.NotFound, txn.get(docs, "schema"));
        try std.testing.expectError(Error.NotFound, txn.get(meta, "doc1"));
    }
}

test "named database cursor iteration" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 4 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const docs = try txn.openDb("docs", .{ .create = true });
        try txn.put(docs, "a", "1", .{});
        try txn.put(docs, "b", "2", .{});
        try txn.put(docs, "c", "3", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const docs = try txn.openDb("docs", .{});
        var iter = try iterate(&txn, docs);
        defer iter.deinit();

        const expected_keys = [_][]const u8{ "a", "b", "c" };
        const expected_vals = [_][]const u8{ "1", "2", "3" };

        var i: usize = 0;
        while (iter.next()) |entry| {
            try std.testing.expectEqualStrings(expected_keys[i], entry.key);
            try std.testing.expectEqualStrings(expected_vals[i], entry.value);
            i += 1;
        }
        try std.testing.expectEqual(@as(usize, 3), i);
    }
}

test "large write spans multiple leaf pages" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb(null, .{ .create = true });

        var key_buf: [16]u8 = undefined;
        var value_buf: [96]u8 = undefined;
        @memset(&value_buf, 'x');
        for (0..256) |i| {
            const key = try std.fmt.bufPrint(&key_buf, "key-{d:0>4}", .{i});
            try txn.put(dbi, key, &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb(null, .{});
        try std.testing.expectEqualStrings(
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            try txn.get(dbi, "key-0000"),
        );
        try std.testing.expectEqualStrings(
            "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx",
            try txn.get(dbi, "key-0255"),
        );

        var iter = try iterate(&txn, dbi);
        defer iter.deinit();

        var count: usize = 0;
        while (iter.next()) |_| {
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 256), count);
    }
}

test "overflow values round-trip" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 4 });
    defer env.close();

    var big_value: [9000]u8 = undefined;
    @memset(&big_value, 'z');

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const main = try txn.openDb(null, .{ .create = true });
        const docs = try txn.openDb("docs", .{ .create = true });
        try txn.put(main, "blob", &big_value, .{});
        try txn.put(docs, "blob", &big_value, .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const main = try txn.openDb(null, .{});
        const docs = try txn.openDb("docs", .{});

        const main_value = try txn.get(main, "blob");
        const docs_value = try txn.get(docs, "blob");
        try std.testing.expectEqual(@as(usize, big_value.len), main_value.len);
        try std.testing.expectEqual(@as(usize, big_value.len), docs_value.len);
        try std.testing.expectEqualSlices(u8, &big_value, main_value);
        try std.testing.expectEqualSlices(u8, &big_value, docs_value);

        var cur = try txn.cursor(docs);
        defer cur.close();
        const entry = try cur.seek("blob");
        try std.testing.expectEqualStrings("blob", entry.key);
        try std.testing.expectEqualSlices(u8, &big_value, entry.value);
    }
}

test "zig backend supports integer_key ordering" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("ints", .{ .create = true, .integer_key = true });
        const two = std.mem.asBytes(&@as(u64, 2));
        const ten = std.mem.asBytes(&@as(u64, 10));
        const thirty = std.mem.asBytes(&@as(u64, 30));
        try txn.put(dbi, ten, "ten", .{});
        try txn.put(dbi, two, "two", .{});
        try txn.put(dbi, thirty, "thirty", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("ints", .{ .integer_key = true });
        const two = std.mem.asBytes(&@as(u64, 2));
        const ten = std.mem.asBytes(&@as(u64, 10));
        const thirty = std.mem.asBytes(&@as(u64, 30));
        try std.testing.expectEqualStrings("two", try txn.get(dbi, two));
        try std.testing.expectEqualStrings("ten", try txn.get(dbi, ten));
        try std.testing.expectEqualStrings("thirty", try txn.get(dbi, thirty));

        var cur = try txn.cursor(dbi);
        defer cur.close();

        const first = try cur.getEntry(.first);
        try std.testing.expectEqualSlices(u8, two, first.key);
        try std.testing.expectEqualStrings("two", first.value);

        const second = try cur.getEntry(.next);
        try std.testing.expectEqualSlices(u8, ten, second.key);
        try std.testing.expectEqualStrings("ten", second.value);

        const third = try cur.getEntry(.next);
        try std.testing.expectEqualSlices(u8, thirty, third.key);
        try std.testing.expectEqualStrings("thirty", third.value);
    }
}

test "zig backend supports reverse_key ordering" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("rev", .{ .create = true, .reverse_key = true });
        try txn.put(dbi, "ab", "ab", .{});
        try txn.put(dbi, "ba", "ba", .{});
        try txn.put(dbi, "ac", "ac", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("rev", .{ .reverse_key = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try std.testing.expectEqualStrings("ba", (try cur.getEntry(.first)).key);
        try std.testing.expectEqualStrings("ab", (try cur.getEntry(.next)).key);
        try std.testing.expectEqualStrings("ac", (try cur.getEntry(.next)).key);
    }
}

test "zig backend write cursor supports put and delete" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("docs", .{ .create = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try cur.putEntry("b", "2", .{});
        try std.testing.expectEqualStrings("b", (try cur.getEntry(.get_current)).key);
        try cur.putEntry("a", "1", .{});
        try std.testing.expectEqualStrings("a", (try cur.getEntry(.get_current)).key);
        try cur.putEntry("c", "3", .{});
        try std.testing.expectEqualStrings("c", (try cur.getEntry(.get_current)).key);

        const middle = try cur.seek("b");
        try std.testing.expectEqualStrings("b", middle.key);
        try cur.deleteEntry();
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("docs", .{});
        try std.testing.expectEqualStrings("1", try txn.get(dbi, "a"));
        try std.testing.expectEqualStrings("3", try txn.get(dbi, "c"));
        try std.testing.expectError(Error.NotFound, txn.get(dbi, "b"));
    }
}

test "zig backend write cursor deletes only the current dupsort value" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("dups", .{ .create = true, .dup_sort = true });
        try txn.put(dbi, "k", "a", .{});
        try txn.put(dbi, "k", "b", .{});
        try txn.put(dbi, "k", "c", .{});
        try txn.put(dbi, "z", "tail", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("dups", .{ .dup_sort = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try std.testing.expectEqualStrings("a", (try cur.getEntry(.first)).value);
        try std.testing.expectEqualStrings("b", (try cur.getEntry(.next_dup)).value);
        try cur.deleteEntry();
        const current = try cur.getEntry(.get_current);
        try std.testing.expectEqualStrings("k", current.key);
        try std.testing.expectEqualStrings("c", current.value);
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("dups", .{ .dup_sort = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try std.testing.expectEqualStrings("a", (try cur.getEntry(.first)).value);
        try std.testing.expectEqualStrings("c", (try cur.getEntry(.next_dup)).value);
        try std.testing.expectError(Error.NotFound, cur.getEntry(.next_dup));
        try std.testing.expectEqualStrings("tail", (try cur.getEntry(.next_nodup)).value);
    }
}

test "zig backend supports dup_sort duplicate-key iteration" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("dups", .{ .create = true, .dup_sort = true });
        try txn.put(dbi, "k", "b", .{});
        try txn.put(dbi, "k", "a", .{});
        try txn.put(dbi, "k", "c", .{});
        try txn.put(dbi, "z", "tail", .{});
        try std.testing.expectError(Error.KeyExists, txn.put(dbi, "k", "b", .{ .no_dup_data = true }));
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("dups", .{ .dup_sort = true });
        try std.testing.expectEqualStrings("a", try txn.get(dbi, "k"));

        var cur = try txn.cursor(dbi);
        defer cur.close();

        const first = try cur.getEntry(.first);
        try std.testing.expectEqualStrings("k", first.key);
        try std.testing.expectEqualStrings("a", first.value);

        const first_dup = try cur.getEntry(.first_dup);
        try std.testing.expectEqualStrings("k", first_dup.key);
        try std.testing.expectEqualStrings("a", first_dup.value);

        const second = try cur.getEntry(.next_dup);
        try std.testing.expectEqualStrings("k", second.key);
        try std.testing.expectEqualStrings("b", second.value);

        const third = try cur.getEntry(.next_dup);
        try std.testing.expectEqualStrings("k", third.key);
        try std.testing.expectEqualStrings("c", third.value);

        const last_dup = try cur.getEntry(.last_dup);
        try std.testing.expectEqualStrings("k", last_dup.key);
        try std.testing.expectEqualStrings("c", last_dup.value);

        const prev_dup = try cur.getEntry(.prev_dup);
        try std.testing.expectEqualStrings("k", prev_dup.key);
        try std.testing.expectEqualStrings("b", prev_dup.value);

        const both = try cur.getBoth("k", "b");
        try std.testing.expectEqualStrings("k", both.key);
        try std.testing.expectEqualStrings("b", both.value);

        const both_range = try cur.getBothRange("k", "bb");
        try std.testing.expectEqualStrings("k", both_range.key);
        try std.testing.expectEqualStrings("c", both_range.value);

        const fourth = try cur.getEntry(.next_nodup);
        try std.testing.expectEqualStrings("z", fourth.key);
        try std.testing.expectEqualStrings("tail", fourth.value);

        const prev_nodup = try cur.getEntry(.prev_nodup);
        try std.testing.expectEqualStrings("k", prev_nodup.key);
        try std.testing.expectEqualStrings("c", prev_nodup.value);

        const last = try cur.getEntry(.last);
        try std.testing.expectEqualStrings("z", last.key);
        try std.testing.expectEqualStrings("tail", last.value);

        const prev = try cur.getEntry(.prev);
        try std.testing.expectEqualStrings("k", prev.key);
        try std.testing.expectEqualStrings("c", prev.value);
    }

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("dups", .{ .dup_sort = true });
        try txn.delete(dbi, "k");
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("dups", .{ .dup_sort = true });
        try std.testing.expectError(Error.NotFound, txn.get(dbi, "k"));
        try std.testing.expectEqualStrings("tail", try txn.get(dbi, "z"));
    }
}

test "deleteValue removes one duplicate value" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("dups", .{ .create = true, .dup_sort = true });
        try txn.put(dbi, "k", "a", .{});
        try txn.put(dbi, "k", "b", .{});
        try txn.put(dbi, "k", "c", .{});
        try txn.deleteValue(dbi, "k", "b");
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("dups", .{ .dup_sort = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try std.testing.expectEqualStrings("a", (try cur.getEntry(.first)).value);
        try std.testing.expectEqualStrings("c", (try cur.getEntry(.next_dup)).value);
        try std.testing.expectError(Error.NotFound, cur.getEntry(.next_dup));
    }
}

test "cursor putEntry honors no_dup_data" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("dups", .{ .create = true, .dup_sort = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try cur.putEntry("k", "a", .{});
        try std.testing.expectError(Error.KeyExists, cur.putEntry("k", "a", .{ .no_dup_data = true }));
        try txn.commit();
    }
}

test "append_dup appends duplicate values at the end of a duplicate set" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("dups", .{ .create = true, .dup_sort = true });
        try txn.put(dbi, "k", "a", .{});
        try txn.put(dbi, "k", "b", .{ .append_dup = true });
        try std.testing.expectError(Error.KeyExists, txn.put(dbi, "k", "aa", .{ .append_dup = true }));
        try txn.put(dbi, "z", "tail", .{ .append_dup = true });
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("dups", .{ .dup_sort = true });
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try std.testing.expectEqualStrings("a", (try cur.getEntry(.first)).value);
        try std.testing.expectEqualStrings("b", (try cur.getEntry(.next_dup)).value);
        try std.testing.expectEqualStrings("tail", (try cur.getEntry(.next_nodup)).value);
    }
}

test "zig backend supports dup_fixed duplicate values" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("fixed", .{ .create = true, .dup_sort = true, .dup_fixed = true });
        try txn.put(dbi, "k", "02", .{});
        try txn.put(dbi, "k", "01", .{});
        try txn.put(dbi, "k", "03", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("fixed", .{});
        var cur = try txn.cursor(dbi);
        defer cur.close();

        try std.testing.expectEqualStrings("01", (try cur.getEntry(.first)).value);
        try std.testing.expectEqualStrings("02", (try cur.getEntry(.next)).value);
        try std.testing.expectEqualStrings("03", (try cur.getEntry(.next)).value);
    }
}

test "zig backend supports integer_dup and reverse_dup ordering" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 3 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const ints = try txn.openDb("ints", .{ .create = true, .dup_sort = true, .dup_fixed = true, .integer_dup = true });
        const revs = try txn.openDb("revs", .{ .create = true, .dup_sort = true, .reverse_dup = true });

        var two: [8]u8 = undefined;
        var seven: [8]u8 = undefined;
        var ten: [8]u8 = undefined;
        std.mem.writeInt(u64, &two, 2, .little);
        std.mem.writeInt(u64, &seven, 7, .little);
        std.mem.writeInt(u64, &ten, 10, .little);

        try txn.put(ints, "k", &ten, .{});
        try txn.put(ints, "k", &two, .{});
        try txn.put(ints, "k", &seven, .{});

        try txn.put(revs, "k", "za", .{});
        try txn.put(revs, "k", "ab", .{});
        try txn.put(revs, "k", "yb", .{});
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const ints = try txn.openDb("ints", .{});
        const revs = try txn.openDb("revs", .{});

        var cur = try txn.cursor(ints);
        defer cur.close();
        try std.testing.expectEqualSlices(u8, &[_]u8{ 2, 0, 0, 0, 0, 0, 0, 0 }, (try cur.getEntry(.first)).value);
        try std.testing.expectEqualSlices(u8, &[_]u8{ 7, 0, 0, 0, 0, 0, 0, 0 }, (try cur.getEntry(.next)).value);
        try std.testing.expectEqualSlices(u8, &[_]u8{ 10, 0, 0, 0, 0, 0, 0, 0 }, (try cur.getEntry(.next)).value);

        var rev_cur = try txn.cursor(revs);
        defer rev_cur.close();
        try std.testing.expectEqualStrings("za", (try rev_cur.getEntry(.first)).value);
        try std.testing.expectEqualStrings("ab", (try rev_cur.getEntry(.next)).value);
        try std.testing.expectEqualStrings("yb", (try rev_cur.getEntry(.next)).value);
    }
}

test "zig backend promotes large duplicate sets into duplicate subdbs" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 2 });
    defer env.close();

    {
        var txn = try env.begin(.{});
        errdefer txn.abort();

        const dbi = try txn.openDb("promoted", .{ .create = true, .dup_sort = true });
        var value_buf: [96]u8 = undefined;
        for (0..64) |i| {
            @memset(&value_buf, @as(u8, @intCast('A' + (i % 26))));
            try txn.put(dbi, "k", &value_buf, .{});
        }
        try txn.commit();
    }

    {
        var txn = try env.begin(.{ .read_only = true });
        defer txn.abort();

        const dbi = try txn.openDb("promoted", .{});
        var iter = try iterate(&txn, dbi);
        defer iter.deinit();

        var count: usize = 0;
        while (iter.next()) |entry| {
            try std.testing.expectEqualStrings("k", entry.key);
            try std.testing.expectEqual(@as(usize, 96), entry.value.len);
            count += 1;
        }
        try std.testing.expectEqual(@as(usize, 64), count);
    }
}

test "zig backend rejects a second active writer on the same environment" {
    if (!use_zig_backend) return;

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env1 = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env1.close();
    var txn1 = try env1.begin(.{});
    defer txn1.abort();

    var env2 = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env2.close();
    try std.testing.expectError(Error.WriterLocked, env2.begin(.{}));
}

test "zig backend wrapper shares one environment across concurrent reader and writer threads" {
    if (!use_zig_backend) return;

    const Worker = struct {
        env: *Environment,
        iterations: usize,
        err: ?anyerror = null,

        fn runWriter(self: *@This()) void {
            var value_buf: [32]u8 = undefined;
            for (0..self.iterations) |i| {
                var txn = self.env.begin(.{}) catch |err| {
                    self.err = err;
                    return;
                };
                errdefer txn.abort();

                const main = txn.openDb(null, .{ .create = true }) catch |err| {
                    self.err = err;
                    return;
                };
                const value = std.fmt.bufPrint(&value_buf, "v-{d}", .{i}) catch {
                    self.err = error.LmdbUnexpected;
                    return;
                };
                txn.put(main, "k", value, .{}) catch |err| {
                    self.err = err;
                    return;
                };
                txn.commit() catch |err| {
                    self.err = err;
                    return;
                };
                std.Thread.yield() catch {};
            }
        }

        fn runReader(self: *@This()) void {
            for (0..self.iterations) |_| {
                var txn = self.env.begin(.{ .read_only = true }) catch |err| {
                    self.err = err;
                    return;
                };
                defer txn.abort();

                const main = txn.openDb(null, .{}) catch |err| {
                    self.err = err;
                    return;
                };
                _ = txn.get(main, "k") catch |err| switch (err) {
                    Error.NotFound => {},
                    else => {
                        self.err = err;
                        return;
                    },
                };
                _ = self.env.commitStatsSnapshot();
                std.Thread.yield() catch {};
            }
        }
    };

    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    var writer = Worker{ .env = &env, .iterations = 128 };
    var reader = Worker{ .env = &env, .iterations = 256 };

    const writer_thread = try std.Thread.spawn(.{}, Worker.runWriter, .{&writer});
    const reader_thread = try std.Thread.spawn(.{}, Worker.runReader, .{&reader});
    writer_thread.join();
    reader_thread.join();

    if (writer.err) |err| return err;
    if (reader.err) |err| return err;

    var txn = try env.begin(.{ .read_only = true });
    defer txn.abort();
    const main = try txn.openDb(null, .{});
    const value = try txn.get(main, "k");
    try std.testing.expect(value.len > 0);
}

test "nested child transaction commit merges into parent state" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    var parent = try env.begin(.{});
    errdefer parent.abort();

    const main = try parent.openDb(null, .{ .create = true });
    try parent.put(main, "alpha", "1", .{});

    var child = try parent.beginChild();
    errdefer child.abort();
    try child.put(main, "beta", "2", .{});

    try std.testing.expectError(Error.BadTxn, parent.get(main, "alpha"));
    try child.commit();

    try std.testing.expectEqualStrings("1", try parent.get(main, "alpha"));
    try std.testing.expectEqualStrings("2", try parent.get(main, "beta"));
    try parent.commit();

    var reopened_env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer reopened_env.close();
    var reopened_txn = try reopened_env.begin(.{ .read_only = true });
    defer reopened_txn.abort();

    const reopened_main = try reopened_txn.openDb(null, .{});
    try std.testing.expectEqualStrings("1", try reopened_txn.get(reopened_main, "alpha"));
    try std.testing.expectEqualStrings("2", try reopened_txn.get(reopened_main, "beta"));
}

test "nested child transaction abort discards child state" {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer env.close();

    var parent = try env.begin(.{});
    errdefer parent.abort();

    const main = try parent.openDb(null, .{ .create = true });
    try parent.put(main, "alpha", "1", .{});

    var child = try parent.beginChild();
    try child.put(main, "beta", "2", .{});
    child.abort();

    try std.testing.expectEqualStrings("1", try parent.get(main, "alpha"));
    try std.testing.expectError(Error.NotFound, parent.get(main, "beta"));

    try parent.put(main, "gamma", "3", .{});
    try parent.commit();

    var reopened_env = try Environment.open(tmp_path, .{ .max_dbs = 1 });
    defer reopened_env.close();
    var reopened_txn = try reopened_env.begin(.{ .read_only = true });
    defer reopened_txn.abort();

    const reopened_main = try reopened_txn.openDb(null, .{});
    try std.testing.expectEqualStrings("1", try reopened_txn.get(reopened_main, "alpha"));
    try std.testing.expectEqualStrings("3", try reopened_txn.get(reopened_main, "gamma"));
    try std.testing.expectError(Error.NotFound, reopened_txn.get(reopened_main, "beta"));
}

test "zig backend soak: write_map and map_async survive mixed reopen cycles" {
    if (!use_zig_backend) return;
    try runZigMixedModeSoak(.{
        .write_map = true,
        .map_async = true,
    });
}

test "zig backend soak: fixed_map and write_map survive mixed reopen cycles" {
    if (!use_zig_backend) return;
    try runZigMixedModeSoak(.{
        .fixed_map = true,
        .write_map = true,
    });
}

test "mixed feature script survives reopen cycles on current backend" {
    try runBackendSharedSoak(.{});
}

// ============================================================================
// Test helpers
// ============================================================================

fn dupCount(txn: *Transaction, dbi: Dbi, key: []const u8) Error!usize {
    var cur = try txn.cursor(dbi);
    defer cur.close();

    _ = cur.seekExact(key) catch |err| switch (err) {
        Error.NotFound => return 0,
        else => return err,
    };

    var count: usize = 1;
    while (true) {
        _ = cur.getEntry(.next_dup) catch |err| switch (err) {
            Error.NotFound => break,
            else => return err,
        };
        count += 1;
    }
    return count;
}

fn countAll(txn: *Transaction, dbi: Dbi) Error!usize {
    var iter = try iterate(txn, dbi);
    defer iter.deinit();

    var count: usize = 0;
    while (iter.next()) |_| count += 1;
    return count;
}

fn runBackendSharedSoak(base_opts: EnvironmentOptions) !void {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var expected_nested: ?usize = null;

    var iter: usize = 0;
    while (iter < 12) : (iter += 1) {
        var env_opts = base_opts;
        env_opts.max_dbs = 4;

        {
            var env = try Environment.open(tmp_path, env_opts);
            defer env.close();

            var txn = try env.begin(.{});
            errdefer txn.abort();

            const main = try txn.openDb(null, .{ .create = true });
            const docs = try txn.openDb("docs", .{ .create = true });

            var head_buf: [32]u8 = undefined;
            const head_value = try std.fmt.bufPrint(&head_buf, "head-{d:0>2}", .{iter});
            var doc_buf: [32]u8 = undefined;
            const doc_value = try std.fmt.bufPrint(&doc_buf, "doc-{d:0>2}", .{iter});

            try txn.put(main, "head", head_value, .{});
            try txn.put(docs, head_value, doc_value, .{});

            if (iter % 2 == 0) {
                var child = try txn.beginChild();
                var child_finished = false;
                defer if (!child_finished) child.abort();

                var nested_buf: [32]u8 = undefined;
                const nested_value = try std.fmt.bufPrint(&nested_buf, "nested-{d:0>2}", .{iter});
                try child.put(main, "nested", nested_value, .{});

                if (iter % 4 == 0) {
                    try child.commit();
                    child_finished = true;
                    expected_nested = iter;
                }
            }

            try txn.commit();
        }

        {
            var env = try Environment.open(tmp_path, env_opts);
            defer env.close();

            var txn = try env.begin(.{ .read_only = true });
            defer txn.abort();

            const main = try txn.openDb(null, .{});
            const docs = try txn.openDb("docs", .{});

            var head_buf: [32]u8 = undefined;
            const expected_head = try std.fmt.bufPrint(&head_buf, "head-{d:0>2}", .{iter});
            try std.testing.expectEqualStrings(expected_head, try txn.get(main, "head"));

            if (expected_nested) |nested_iter| {
                var nested_buf: [32]u8 = undefined;
                const expected_value = try std.fmt.bufPrint(&nested_buf, "nested-{d:0>2}", .{nested_iter});
                try std.testing.expectEqualStrings(expected_value, try txn.get(main, "nested"));
            } else {
                try std.testing.expectError(Error.NotFound, txn.get(main, "nested"));
            }

            try std.testing.expectEqual(iter + 1, try countAll(&txn, docs));
        }
    }
}

fn runZigMixedModeSoak(base_opts: EnvironmentOptions) !void {
    var tmp_buf: [256]u8 = undefined;
    const tmp_path = tmpPath(&tmp_buf);
    defer cleanupTmp(tmp_path);

    var expected_nested: ?usize = null;
    var expected_dup_count: usize = 0;
    var expected_promoted_count: usize = 0;

    var iter: usize = 0;
    while (iter < 12) : (iter += 1) {
        var env_opts = base_opts;
        env_opts.max_dbs = 5;

        {
            var env = try Environment.open(tmp_path, env_opts);
            defer env.close();

            var txn = env.begin(.{}) catch |err| switch (err) {
                Error.Incompatible => if (env_opts.fixed_map) return else return err,
                else => return err,
            };
            errdefer txn.abort();

            const main = try txn.openDb(null, .{ .create = true });
            const docs = try txn.openDb("docs", .{ .create = true });
            const dups = try txn.openDb("dups", .{ .create = true, .dup_sort = true });
            const promoted = try txn.openDb("promoted", .{ .create = true, .dup_sort = true });

            var head_buf: [32]u8 = undefined;
            const head_value = try std.fmt.bufPrint(&head_buf, "head-{d:0>2}", .{iter});
            var doc_buf: [32]u8 = undefined;
            const doc_value = try std.fmt.bufPrint(&doc_buf, "doc-{d:0>2}", .{iter});
            var parent_dup_buf: [32]u8 = undefined;
            const parent_dup = try std.fmt.bufPrint(&parent_dup_buf, "p-{d:0>2}", .{iter});

            try txn.put(main, "head", head_value, .{});
            try txn.put(docs, head_value, doc_value, .{});
            try txn.put(dups, "dup", parent_dup, .{});
            expected_dup_count += 1;

            var bulk_value: [96]u8 = undefined;
            @memset(&bulk_value, @as(u8, 'A' + @as(u8, @intCast(iter % 26))));
            for (0..8) |bulk_index| {
                bulk_value[0] = @as(u8, 'A' + @as(u8, @intCast(iter % 26)));
                bulk_value[1] = @as(u8, '0' + @as(u8, @intCast(bulk_index)));
                try txn.put(promoted, "bulk", &bulk_value, .{});
                expected_promoted_count += 1;
            }

            if (iter > 0 and iter % 5 == 0) {
                var cur = try txn.cursor(dups);
                defer cur.close();
                _ = try cur.seekExact("dup");
                try cur.deleteEntry();
                expected_dup_count -= 1;
            }

            if (iter > 0 and iter % 3 == 0) {
                var cur = try txn.cursor(promoted);
                defer cur.close();
                _ = try cur.seekExact("bulk");
                try cur.deleteEntry();
                expected_promoted_count -= 1;
            }

            if (iter % 2 == 0) {
                var child = try txn.beginChild();
                var child_finished = false;
                defer if (!child_finished) child.abort();

                var nested_buf: [32]u8 = undefined;
                const nested_value = try std.fmt.bufPrint(&nested_buf, "nested-{d:0>2}", .{iter});

                try child.put(main, "nested", nested_value, .{});

                if (iter % 4 == 0) {
                    try child.commit();
                    child_finished = true;
                    expected_nested = iter;
                }
            }

            try txn.commit();
        }

        {
            const reopen_opts = env_opts;
            var env = Environment.open(tmp_path, reopen_opts) catch |err| switch (err) {
                Error.Incompatible => if (reopen_opts.fixed_map) return else return err,
                else => return err,
            };
            defer env.close();

            var txn = env.begin(.{ .read_only = true }) catch |err| switch (err) {
                Error.Incompatible => if (reopen_opts.fixed_map) return else return err,
                else => return err,
            };
            defer txn.abort();

            const main = try txn.openDb(null, .{});
            const docs = try txn.openDb("docs", .{});
            const dups = try txn.openDb("dups", .{ .dup_sort = true });
            const promoted = try txn.openDb("promoted", .{ .dup_sort = true });

            var head_buf: [32]u8 = undefined;
            const expected_head = try std.fmt.bufPrint(&head_buf, "head-{d:0>2}", .{iter});
            try std.testing.expectEqualStrings(expected_head, try txn.get(main, "head"));

            if (expected_nested) |nested_iter| {
                var nested_buf: [32]u8 = undefined;
                const expected_value = try std.fmt.bufPrint(&nested_buf, "nested-{d:0>2}", .{nested_iter});
                try std.testing.expectEqualStrings(expected_value, try txn.get(main, "nested"));
            } else {
                try std.testing.expectError(Error.NotFound, txn.get(main, "nested"));
            }

            try std.testing.expectEqual(iter + 1, try countAll(&txn, docs));
            try std.testing.expectEqual(expected_dup_count, try dupCount(&txn, dups, "dup"));
            try std.testing.expectEqual(expected_promoted_count, try dupCount(&txn, promoted, "bulk"));

            var parent_dup_buf: [32]u8 = undefined;
            const expected_parent_dup = try std.fmt.bufPrint(&parent_dup_buf, "p-{d:0>2}", .{iter});
            {
                var cur = try txn.cursor(dups);
                defer cur.close();
                try std.testing.expectEqualStrings(expected_parent_dup, (try cur.getBoth("dup", expected_parent_dup)).value);
            }
        }
    }
}

fn tmpPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-lmdb-test-";
    var tspec: std.posix.timespec = undefined;
    switch (std.posix.errno(std.posix.system.clock_gettime(.MONOTONIC, &tspec))) {
        .SUCCESS => {},
        else => unreachable,
    }
    const ts = @as(u64, @intCast(tspec.sec)) * std.time.ns_per_s + @as(u64, @intCast(tspec.nsec));
    const pid: u32 = @intCast(std.posix.system.getpid());
    const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}\x00", .{ base, pid, ts }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().createDirPath(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch {};
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

const SimTests = lmdb_sim_test.namespace(@This());

test "LMDB replay fixtures stay green" {
    try SimTests.runReplayFixtures(std.testing.allocator);
}

test "differential workload keeps c and zig backends aligned across reopen cycles" {
    try SimTests.runDifferentialDefault(std.testing.allocator);
}

test "differential workload covers write_map durability modes" {
    try SimTests.runDifferentialWriteMapModes(std.testing.allocator);
}

test "differential workload covers sync policy modes" {
    try SimTests.runDifferentialSyncPolicyModes(std.testing.allocator);
}

test "differential workload covers zig commit backends" {
    try SimTests.runDifferentialCommitBackends(std.testing.allocator);
}

test "zig crash publish phases reopen to committed or previous state" {
    try SimTests.runCrashDefault(std.testing.allocator);
}

test "zig crash publish phases cover write_map modes" {
    try SimTests.runCrashWriteMapModes(std.testing.allocator);
}

test "zig crash publish phases cover sync policy modes" {
    try SimTests.runCrashSyncPolicyModes(std.testing.allocator);
}

test "zig crash publish phases cover zig commit backends" {
    try SimTests.runCrashCommitBackends(std.testing.allocator);
}

test "LMDB sim soak stays green" {
    if (!storage_sim_soak) return;
    try SimTests.runSoak(std.testing.allocator);
}
