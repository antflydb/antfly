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

//! LMDB-backed document key-value store with centralized binary key encoding.
//!
//! Public document IDs are raw byte strings. Internal primary, TTL, artifact,
//! chunk, and graph records are encoded through storage/internal_keys.zig so
//! user-controlled IDs never share a delimiter namespace with derived records.

const std = @import("std");
const builtin = @import("builtin");
const platform = @import("antfly_platform");
const Allocator = std.mem.Allocator;
const AtomicU64 = platform.atomic.Value(u64);
const fs_paths = @import("../common/fs_paths.zig");
const backend_adapter = @import("backend_adapter.zig");
const backend_erased = @import("backend_erased.zig");
const backend_types = @import("backend_types.zig");
const change_journal_mod = @import("db/derived/change_journal.zig");
const internal_keys = @import("internal_keys.zig");
const lsm_backend = @import("lsm_backend.zig");
const mem_backend = @import("mem_backend.zig");
const platform_time = @import("../platform/time.zig");
const supports_lmdb = builtin.os.tag != .freestanding;
const backend_lmdb_adapter = if (supports_lmdb) @import("backend_lmdb_adapter.zig") else struct {
    pub const Cursor = struct {
        pub fn init(_: anytype) @This() {
            return .{};
        }
    };
};
const writer_locked_retry_count: usize = 1000;
const writer_locked_retry_sleep_ns: u64 = 100_000;

fn backoffWriterLockRetry(io: ?std.Io) void {
    if (io) |active_io| {
        active_io.sleep(std.Io.Duration.fromNanoseconds(@intCast(writer_locked_retry_sleep_ns)), .awake) catch {};
        return;
    }
    if (comptime builtin.os.tag == .freestanding) return;
    std.Thread.yield() catch {};
    if (@hasDecl(std.Thread, "sleep")) {
        std.Thread.sleep(writer_locked_retry_sleep_ns);
    }
}

const replay_hints = [_]change_journal_mod.TargetHint{
    .enrichment,
    .full_text,
    .dense_vector,
    .sparse_vector,
    .graph,
    .algebraic,
};

fn replayHintOrdinal(hint: change_journal_mod.TargetHint) u8 {
    return @intCast(@intFromEnum(hint));
}

fn encodeReplayNextSequence(sequence: u64) [8]u8 {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, sequence, .little);
    return raw;
}

fn encodeReplaySequence(sequence: u64) [8]u8 {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, sequence, .little);
    return raw;
}

fn decodeReplaySequence(raw: []const u8) ?u64 {
    if (raw.len != 8) return null;
    return std.mem.readInt(u64, raw[0..8], .little);
}

fn isEmbeddingReplayArtifactKey(key: []const u8) bool {
    return internal_keys.isEmbeddingArtifactKey(key) or internal_keys.isDerivedEmbeddingArtifactKey(key);
}

fn appendReplayArtifactsForHint(
    alloc: Allocator,
    out: *std.ArrayListUnmanaged([]const u8),
    artifact_keys: []const []const u8,
    hint: change_journal_mod.TargetHint,
) !void {
    for (artifact_keys) |key| {
        const keep = switch (hint) {
            .dense_vector, .sparse_vector => isEmbeddingReplayArtifactKey(key),
            .graph => internal_keys.isGraphEdgeArtifactKey(key) or
                internal_keys.isAssetArtifactKey(key) or
                internal_keys.isResolutionArtifactKey(key),
            .resolution => internal_keys.isAssetArtifactKey(key),
            .promotion => internal_keys.isResolutionArtifactKey(key),
            .enrichment, .full_text, .algebraic => false,
        };
        if (keep) try out.append(alloc, key);
    }
}

fn encodeReplayPayloadForHint(
    alloc: Allocator,
    record: change_journal_mod.Record,
    hint: change_journal_mod.TargetHint,
) ![]u8 {
    var target_hints = [_]change_journal_mod.TargetHint{hint};
    var artifact_keys = std.ArrayListUnmanaged([]const u8).empty;
    defer artifact_keys.deinit(alloc);
    try appendReplayArtifactsForHint(alloc, &artifact_keys, record.changed_artifact_keys, hint);

    var filtered = change_journal_mod.Record{
        .version = record.version,
        .sequence = record.sequence,
        .target_hints = target_hints[0..],
    };
    switch (hint) {
        .enrichment => {
            filtered.changed_doc_keys = record.changed_doc_keys;
        },
        .full_text => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
        },
        .algebraic => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
        },
        .dense_vector, .sparse_vector => {
            filtered.changed_doc_keys = record.changed_doc_keys;
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.overwritten_doc_keys = record.overwritten_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .graph => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .resolution => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
        .promotion => {
            filtered.deleted_doc_keys = record.deleted_doc_keys;
            filtered.changed_artifact_keys = artifact_keys.items;
        },
    }
    return try change_journal_mod.encodeRecord(alloc, filtered);
}

fn writeOriginalReplayHintEntries(txn: anytype, sequence: u64, mask: u8, payload: []const u8) !void {
    const latest_raw = encodeReplaySequence(sequence);
    for (replay_hints) |hint| {
        if ((mask & change_journal_mod.singleHintMask(hint)) == 0) continue;
        const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), sequence);
        try txn.put(key[0..], payload);
        const latest_key = internal_keys.replayLatestSequenceKey(replayHintOrdinal(hint));
        try txn.put(latest_key[0..], latest_raw[0..]);
    }
}

fn writeReplayEntries(alloc: Allocator, txn: anytype, sequence: u64, payload: []const u8) !void {
    try txn.put(internal_keys.replay_meta_init_key[0..], "");
    const next_raw = encodeReplayNextSequence(sequence + 1);
    try txn.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
    const latest_raw = encodeReplaySequence(sequence);

    const all_key = internal_keys.replayEntryKey(internal_keys.replay_all_kind, sequence);
    try txn.put(all_key[0..], payload);
    const all_latest_key = internal_keys.replayLatestSequenceKey(internal_keys.replay_all_kind);
    try txn.put(all_latest_key[0..], latest_raw[0..]);

    const mask = change_journal_mod.encodedRecordHintMask(payload) catch return;
    if (mask == 0) return;

    var decoded = change_journal_mod.decodeRecord(alloc, payload) catch {
        try writeOriginalReplayHintEntries(txn, sequence, mask, payload);
        return;
    };
    defer decoded.deinit();

    for (replay_hints) |hint| {
        if ((mask & change_journal_mod.singleHintMask(hint)) == 0) continue;
        const lane_payload = try encodeReplayPayloadForHint(alloc, decoded.record, hint);
        defer alloc.free(lane_payload);
        const key = internal_keys.replayEntryKey(replayHintOrdinal(hint), sequence);
        try txn.put(key[0..], lane_payload);
        const latest_key = internal_keys.replayLatestSequenceKey(replayHintOrdinal(hint));
        try txn.put(latest_key[0..], latest_raw[0..]);
    }
}
const lmdb = if (supports_lmdb) @import("lmdb.zig") else struct {
    pub const Error = error{
        NotFound,
        Incompatible,
    };
    pub const Environment = struct {};
    pub const Dbi = struct {};
    pub const Transaction = struct {};
    pub const Batch = struct {};
    pub const Cursor = struct {};
    pub const RangeViewIterator = struct {};
    pub const CommitStats = struct {};
};

const LmdbEnvironment = lmdb.Environment;
const LmdbDbi = lmdb.Dbi;
const LmdbTransaction = lmdb.Transaction;
const LmdbBatch = lmdb.Batch;
const LmdbCursor = lmdb.Cursor;
const LmdbRangeViewIterator = lmdb.RangeViewIterator;
const LmdbCommitStats = lmdb.CommitStats;
const lmdb_user_db_name = "docs";
const LmdbUserDbKind = enum {
    named,
    main,
};
const ResolvedLmdbUserDb = struct {
    dbi: LmdbDbi,
    kind: LmdbUserDbKind,
};

fn lmdbUserDbName(kind: LmdbUserDbKind) ?[]const u8 {
    return switch (kind) {
        .named => lmdb_user_db_name,
        .main => null,
    };
}

fn openLmdbUserDbTxn(alloc: Allocator, txn: *LmdbTransaction, create: bool) !LmdbDbi {
    const name_z = try alloc.dupeZ(u8, lmdb_user_db_name);
    defer alloc.free(name_z);
    return try txn.openDb(name_z, .{ .create = create });
}

fn openLmdbUserDbBatch(alloc: Allocator, batch: *LmdbBatch, create: bool) !LmdbDbi {
    const name_z = try alloc.dupeZ(u8, lmdb_user_db_name);
    defer alloc.free(name_z);
    return try batch.openDb(name_z, .{ .create = create });
}

fn openExistingLmdbUserDbTxn(alloc: Allocator, txn: *LmdbTransaction) !ResolvedLmdbUserDb {
    const named = openLmdbUserDbTxn(alloc, txn, false) catch |err| switch (err) {
        lmdb.Error.NotFound => null,
        else => return err,
    };
    if (named) |dbi| return .{ .dbi = dbi, .kind = .named };

    const main = try txn.openDb(null, .{});
    var cursor = try txn.cursor(main);
    defer cursor.close();
    _ = cursor.first() catch |err| switch (err) {
        lmdb.Error.NotFound => return lmdb.Error.NotFound,
        else => return err,
    };
    return .{ .dbi = main, .kind = .main };
}

fn openConfiguredLmdbUserDbTxn(
    alloc: Allocator,
    txn: *LmdbTransaction,
    kind: LmdbUserDbKind,
    create_named: bool,
) !LmdbDbi {
    return switch (kind) {
        .named => try openLmdbUserDbTxn(alloc, txn, create_named),
        .main => try txn.openDb(null, .{}),
    };
}

fn openConfiguredLmdbUserDbBatch(
    alloc: Allocator,
    batch: *LmdbBatch,
    kind: LmdbUserDbKind,
    create_named: bool,
) !LmdbDbi {
    return switch (kind) {
        .named => try openLmdbUserDbBatch(alloc, batch, create_named),
        .main => try batch.openDb(null, .{}),
    };
}

// ============================================================================
// KV types
// ============================================================================

pub const KVPair = struct {
    key: []const u8,
    value: []const u8,
};

pub const OwnedKVPair = struct {
    key: []u8,
    value: []u8,
};

// ============================================================================
// ByteRange — shard ownership range
// ============================================================================

pub const ByteRange = struct {
    start: []const u8, // inclusive, empty = -inf
    end: []const u8, // exclusive, empty = +inf

    /// Check if key is within [start, end).
    pub fn contains(self: ByteRange, key: []const u8) bool {
        // start <= key
        if (self.start.len > 0) {
            if (std.mem.order(u8, key, self.start) == .lt) return false;
        }
        // key < end
        if (self.end.len > 0) {
            if (std.mem.order(u8, key, self.end) != .lt) return false;
        }
        return true;
    }
};

// ============================================================================
// DocStore
// ============================================================================

pub const DocStoreOptions = struct {
    map_size: usize = 256 * 1024 * 1024,
    no_sync: bool = false,
    no_meta_sync: bool = false,
};

pub const DocStore = struct {
    alloc: Allocator,
    kind: Kind,
    runtime_store: backend_erased.Store,
    owns_runtime_store: bool,
    env: LmdbEnvironment,
    dbi: LmdbDbi,
    lmdb_user_db_kind: LmdbUserDbKind,
    replay_index_state: std.atomic.Value(u8),
    next_replay_sequence_cached: AtomicU64,

    const replay_index_unknown: u8 = 0;
    const replay_index_missing: u8 = 1;
    const replay_index_available: u8 = 2;

    const Kind = enum {
        lmdb,
        runtime,
    };

    pub const BackendStore = backend_adapter.Store(DocStore, Txn, Txn, Batch, .{
        .capabilities = backendCapabilities,
        .begin_read = beginReadTxn,
        .begin_write = beginWriteTxn,
        .begin_batch = beginWriteBatch,
    });

    pub const Txn = struct {
        alloc: Allocator,
        raw: ?LmdbTransaction = null,
        dbi: LmdbDbi = undefined,
        read: ?backend_erased.ReadTxn = null,
        probe: ?backend_erased.ProbeTxn = null,
        current_scan: ?backend_erased.CurrentScanTxn = null,
        write: ?backend_erased.WriteTxn = null,

        pub const CursorAdapter = backend_erased.Cursor;
        pub const ReadAdapter = backend_adapter.ReadTxn(Txn, CursorAdapter, .{
            .abort = Txn.abort,
            .get = Txn.get,
            .open_cursor = Txn.openCursorAdapter,
        });
        pub const WriteAdapter = backend_adapter.WriteTxn(Txn, CursorAdapter, .{
            .abort = Txn.abort,
            .commit = Txn.commit,
            .get = Txn.get,
            .put = Txn.put,
            .delete = Txn.delete,
            .open_cursor = Txn.openCursorAdapter,
        });

        pub fn abort(self: *Txn) void {
            if (supports_lmdb) {
                if (self.raw) |*raw| {
                    raw.abort();
                    self.* = undefined;
                    return;
                }
            }
            if (self.write) |*write| {
                write.abort();
            } else if (self.current_scan) |*current_scan| {
                current_scan.abort();
            } else if (self.probe) |*probe| {
                probe.abort();
            } else if (self.read) |*read| {
                read.abort();
            }
            self.* = undefined;
        }

        pub fn commit(self: *Txn) !void {
            if (supports_lmdb) {
                if (self.raw) |*raw| {
                    try raw.commit();
                    self.* = undefined;
                    return;
                }
            }
            if (self.write) |*write| {
                try write.commit();
            } else {
                return error.ReadOnly;
            }
            self.* = undefined;
        }

        pub fn get(self: *Txn, key: []const u8) ![]const u8 {
            if (supports_lmdb) {
                if (self.raw) |*raw| return try raw.get(self.dbi, key);
            }
            if (self.write) |*write| return try write.get(key);
            if (self.probe) |*probe| return try probe.get(key);
            if (self.current_scan != null) return error.Unsupported;
            return try self.read.?.get(key);
        }

        pub fn getManySorted(self: *Txn, keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidArgument;
            if (supports_lmdb) {
                if (self.raw) |*raw| {
                    for (keys, 0..) |key, i| {
                        values[i] = raw.get(self.dbi, key) catch |err| switch (err) {
                            lmdb.Error.NotFound => null,
                            else => return err,
                        };
                    }
                    return;
                }
            }
            if (self.write) |*write| {
                for (keys, 0..) |key, i| {
                    values[i] = write.get(key) catch |err| switch (err) {
                        error.NotFound => null,
                        else => return err,
                    };
                }
                return;
            }
            if (self.probe) |*probe| {
                return try probe.getManySorted(keys, values);
            }
            if (self.current_scan != null) return error.Unsupported;
            return try self.read.?.getManySorted(keys, values);
        }

        pub fn put(self: *Txn, key: []const u8, value: []const u8) !void {
            if (supports_lmdb) {
                if (self.raw) |*raw| {
                    try raw.put(self.dbi, key, value, .{});
                    return;
                }
            }
            try self.write.?.put(key, value);
        }

        pub fn delete(self: *Txn, key: []const u8) !void {
            if (supports_lmdb) {
                if (self.raw) |*raw| {
                    try raw.delete(self.dbi, key);
                    return;
                }
            }
            try self.write.?.delete(key);
        }

        pub fn cursor(self: *Txn) !LmdbCursor {
            if (!supports_lmdb) return error.Unsupported;
            var raw = self.raw orelse return error.Unsupported;
            return try raw.cursor(self.dbi);
        }

        pub fn openCursor(self: *Txn) !CursorAdapter {
            return try self.openCursorAdapter();
        }

        pub fn rangeViewScanner(self: *Txn, start_key: []const u8) !LmdbRangeViewIterator {
            if (!supports_lmdb) return error.Unsupported;
            var raw = self.raw orelse return error.Unsupported;
            return try raw.rangeViewScanner(self.dbi, start_key);
        }

        fn openCursorAdapter(self: *Txn) !CursorAdapter {
            if (supports_lmdb and self.raw != null) {
                return try backend_erased.cursorFrom(self.alloc, backend_lmdb_adapter.Cursor.init(try self.cursor()));
            }
            if (self.write) |*write| return try write.openCursor();
            if (self.current_scan) |*current_scan| return try current_scan.openCursor();
            if (self.probe != null) return error.Unsupported;
            return try self.read.?.openCursor();
        }

        pub fn readAdapter(self: *Txn) ReadAdapter {
            return ReadAdapter.init(self);
        }

        pub fn writeAdapter(self: *Txn) WriteAdapter {
            return WriteAdapter.init(self);
        }
    };

    pub const Batch = struct {
        alloc: Allocator,
        raw: ?LmdbBatch = null,
        dbi: LmdbDbi = undefined,
        runtime: ?backend_erased.Batch = null,

        pub const BatchTxn = struct {
            alloc: Allocator,
            raw: ?*LmdbTransaction = null,
            dbi: LmdbDbi = undefined,
            runtime: ?*backend_erased.Batch = null,

            pub fn get(self: @This(), key: []const u8) ![]const u8 {
                if (supports_lmdb) {
                    if (self.raw) |raw| return try raw.get(self.dbi, key);
                }
                return try self.runtime.?.get(key);
            }

            pub fn getManySorted(self: @This(), keys: []const []const u8, values: []?[]const u8) !void {
                if (keys.len != values.len) return error.InvalidArgument;
                if (supports_lmdb) {
                    if (self.raw) |raw| {
                        for (keys, 0..) |key, i| {
                            values[i] = raw.get(self.dbi, key) catch |err| switch (err) {
                                lmdb.Error.NotFound => null,
                                else => return err,
                            };
                        }
                        return;
                    }
                }
                return try self.runtime.?.getManySorted(keys, values);
            }

            pub fn put(self: @This(), key: []const u8, value: []const u8) !void {
                if (supports_lmdb) {
                    if (self.raw) |raw| {
                        try raw.put(self.dbi, key, value, .{});
                        return;
                    }
                }
                try self.runtime.?.put(key, value);
            }

            pub fn appendPut(self: @This(), key: []const u8, value: []const u8) !void {
                if (supports_lmdb) {
                    if (self.raw != null) return error.Unsupported;
                }
                try self.runtime.?.appendPut(key, value);
            }

            pub fn delete(self: @This(), key: []const u8) !void {
                if (supports_lmdb) {
                    if (self.raw) |raw| {
                        try raw.delete(self.dbi, key);
                        return;
                    }
                }
                try self.runtime.?.delete(key);
            }

            pub fn openCursor(self: @This()) !backend_erased.Cursor {
                if (supports_lmdb) {
                    if (self.raw) |raw| {
                        return try backend_erased.cursorFrom(self.alloc, backend_lmdb_adapter.Cursor.init(try raw.cursor(self.dbi)));
                    }
                }
                return try self.runtime.?.openCursor();
            }

            pub fn setReplayOpaque(self: @This(), sequence: u64, payload: []const u8) !void {
                try writeReplayEntries(self.alloc, self, sequence, payload);
            }
        };

        pub const Adapter = backend_adapter.Batch(Batch, .{
            .abort = abort,
            .commit = commit,
            .get = batchGet,
            .put = batchPut,
            .delete = batchDelete,
        });

        pub fn abort(self: *Batch) void {
            if (supports_lmdb) {
                if (self.raw) |*raw| {
                    raw.abort();
                    self.* = undefined;
                    return;
                }
            }
            if (self.runtime) |*runtime| {
                runtime.abort();
            }
            self.* = undefined;
        }

        pub fn commit(self: *Batch) !void {
            if (supports_lmdb) {
                if (self.raw) |*raw| {
                    try raw.commit();
                    self.* = undefined;
                    return;
                }
            }
            if (self.runtime) |*runtime| {
                try runtime.commit();
            }
            self.* = undefined;
        }

        pub fn asTxn(self: *Batch) BatchTxn {
            return .{
                .alloc = self.alloc,
                .raw = if (supports_lmdb) if (self.raw) |*raw| raw.asTransaction() else null else null,
                .dbi = self.dbi,
                .runtime = if (self.runtime) |*runtime| runtime else null,
            };
        }

        pub fn get(self: *Batch, key: []const u8) ![]const u8 {
            return try self.asTxn().get(key);
        }

        pub fn put(self: *Batch, key: []const u8, value: []const u8) !void {
            try self.asTxn().put(key, value);
        }

        pub fn delete(self: *Batch, key: []const u8) !void {
            try self.asTxn().delete(key);
        }

        pub fn setReplayOpaque(self: *Batch, sequence: u64, payload: []const u8) !void {
            try self.asTxn().setReplayOpaque(sequence, payload);
        }

        fn batchGet(self: *Batch, key: []const u8) ![]const u8 {
            return try self.get(key);
        }

        fn batchPut(self: *Batch, key: []const u8, value: []const u8) !void {
            try self.put(key, value);
        }

        fn batchDelete(self: *Batch, key: []const u8) !void {
            try self.delete(key);
        }

        pub fn adapter(self: *Batch) Adapter {
            return Adapter.init(self);
        }
    };

    pub fn open(alloc: Allocator, path: [*:0]const u8, opts: DocStoreOptions) !DocStore {
        if (!supports_lmdb) return error.UnsupportedPlatform;
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        try fs_paths.createDirPathPortable(io_impl.io(), std.mem.span(path));

        var env = try lmdb.Environment.open(path, .{
            .map_size = opts.map_size,
            .no_sync = opts.no_sync,
            .no_meta_sync = opts.no_meta_sync,
            .no_tls = true,
        });
        errdefer env.close();

        var txn = try env.begin(.{});
        errdefer txn.abort();
        const resolved = openExistingLmdbUserDbTxn(alloc, &txn) catch |err| switch (err) {
            lmdb.Error.NotFound => blk: {
                const dbi = try openLmdbUserDbTxn(alloc, &txn, true);
                break :blk ResolvedLmdbUserDb{ .dbi = dbi, .kind = .named };
            },
            else => return err,
        };
        try txn.commit();

        return .{
            .alloc = alloc,
            .kind = .lmdb,
            .runtime_store = undefined,
            .owns_runtime_store = false,
            .env = env,
            .dbi = resolved.dbi,
            .lmdb_user_db_kind = resolved.kind,
            .replay_index_state = .init(replay_index_unknown),
            .next_replay_sequence_cached = .init(0),
        };
    }

    pub fn openRuntime(alloc: Allocator, store: anytype) !DocStore {
        const runtime_store = try initRuntimeStore(alloc, store);
        return .{
            .alloc = alloc,
            .kind = .runtime,
            .runtime_store = runtime_store.store,
            .owns_runtime_store = runtime_store.owned,
            .env = undefined,
            .dbi = undefined,
            .lmdb_user_db_kind = undefined,
            .replay_index_state = .init(replay_index_unknown),
            .next_replay_sequence_cached = .init(0),
        };
    }

    pub fn close(self: *DocStore) void {
        switch (self.kind) {
            .lmdb => if (supports_lmdb) self.env.close(),
            .runtime => if (self.owns_runtime_store) self.runtime_store.deinit(),
        }
        self.* = undefined;
    }

    fn backendCapabilities(self: *DocStore) backend_types.Capabilities {
        return switch (self.kind) {
            .lmdb => .{
                .ordered_ranges = true,
                .reverse_ranges = true,
                .cursors = true,
                .native_namespaces = false,
                .write_batches = .atomic,
                .single_writer = true,
                .read_snapshots = .snapshot,
            },
            .runtime => self.runtime_store.capabilities(),
        };
    }

    pub fn backendStore(self: *DocStore) BackendStore {
        return BackendStore.init(self);
    }

    pub fn commitStatsSnapshot(self: *DocStore) ?LmdbCommitStats {
        return switch (self.kind) {
            .lmdb => if (supports_lmdb) self.env.commitStatsSnapshot() else null,
            .runtime => null,
        };
    }

    pub fn sync(self: *DocStore, force: bool) !void {
        switch (self.kind) {
            .lmdb => if (supports_lmdb) try self.env.sync(force),
            .runtime => try self.runtime_store.sync(force),
        }
    }

    pub fn syncReplayState(self: *DocStore) !void {
        switch (self.kind) {
            .lmdb => if (supports_lmdb) try self.env.sync(false),
            .runtime => try self.runtime_store.syncReplayState(),
        }
    }

    pub fn splitRightToDir(self: *DocStore, split_key: []const u8, dest_dir: []const u8) !bool {
        if (!supports_lmdb) return error.UnsupportedPlatform;
        if (self.kind != .lmdb) return error.Unsupported;
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const data_path = try std.fmt.bufPrint(&path_buf, "{s}/data.mdb", .{dest_dir});
        return self.env.splitRightNamedDbToFile(lmdbUserDbName(self.lmdb_user_db_kind), split_key, data_path) catch |err| switch (err) {
            lmdb.Error.Incompatible => error.Incompatible,
            else => return err,
        };
    }

    pub fn rewriteLeftInPlace(self: *DocStore, split_key: []const u8) !bool {
        if (!supports_lmdb) return error.UnsupportedPlatform;
        if (self.kind != .lmdb) return error.Unsupported;
        var tmp_path_buf: [std.fs.max_path_bytes]u8 = undefined;
        var data_path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const base_path: []const u8 = self.env.path_z;
        const tmp_path = try std.fmt.bufPrint(&tmp_path_buf, "{s}/data.mdb.left.tmp", .{base_path});
        const data_path = try std.fmt.bufPrint(&data_path_buf, "{s}/data.mdb", .{base_path});

        const rewritten = self.env.splitLeftNamedDbToFile(lmdbUserDbName(self.lmdb_user_db_kind), split_key, tmp_path) catch |err| switch (err) {
            lmdb.Error.Incompatible => return error.Incompatible,
            else => return err,
        };
        if (!rewritten) return false;

        const opts = self.env.opts;
        const reopen_path = try self.alloc.dupeZ(u8, base_path);
        defer self.alloc.free(reopen_path);
        self.env.close();
        var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
        defer io_impl.deinit();
        if (std.fs.path.isAbsolute(tmp_path)) {
            std.Io.Dir.renameAbsolute(tmp_path, data_path, io_impl.io()) catch return error.Unexpected;
        } else {
            std.Io.Dir.rename(std.Io.Dir.cwd(), tmp_path, std.Io.Dir.cwd(), data_path, io_impl.io()) catch return error.Unexpected;
        }

        self.env = try lmdb.Environment.open(reopen_path.ptr, opts);
        var txn = try self.env.begin(.{});
        errdefer txn.abort();
        const resolved = try openExistingLmdbUserDbTxn(self.alloc, &txn);
        self.dbi = resolved.dbi;
        self.lmdb_user_db_kind = resolved.kind;
        try txn.commit();
        return true;
    }

    pub fn beginReadTxn(self: *DocStore) !Txn {
        return switch (self.kind) {
            .lmdb => if (supports_lmdb) blk: {
                var txn = try self.env.begin(.{ .read_only = true });
                errdefer txn.abort();
                const dbi = try openConfiguredLmdbUserDbTxn(self.alloc, &txn, self.lmdb_user_db_kind, false);
                break :blk .{
                    .alloc = self.alloc,
                    .raw = txn,
                    .dbi = dbi,
                };
            } else error.UnsupportedPlatform,
            .runtime => .{
                .alloc = self.alloc,
                .read = try self.runtime_store.beginRead(),
            },
        };
    }

    /// Open a current-tip probe transaction for hot single-writer point reads.
    ///
    /// Runtime backends use a dedicated point-read transaction here so callers
    /// can read the live mutable view without forcing a cloned read snapshot or
    /// requiring write access. LMDB keeps the normal read-only transaction
    /// because its snapshot semantics are cheap.
    pub fn beginProbeTxn(self: *DocStore) !Txn {
        return switch (self.kind) {
            .lmdb => try self.beginReadTxn(),
            .runtime => .{
                .alloc = self.alloc,
                .probe = try self.runtime_store.beginProbe(),
            },
        };
    }

    /// Open a current-tip replay scan transaction for ordered replay walks.
    ///
    /// Runtime backends use a dedicated live scan contract so replay can follow
    /// append-only lanes without widening the point-probe API. LMDB keeps the
    /// normal read-only transaction because its snapshot semantics are cheap.
    pub fn beginCurrentScanTxn(self: *DocStore) !Txn {
        return switch (self.kind) {
            .lmdb => try self.beginReadTxn(),
            .runtime => .{
                .alloc = self.alloc,
                .current_scan = try self.runtime_store.beginCurrentScan(),
            },
        };
    }

    pub fn beginWriteTxn(self: *DocStore) !Txn {
        return switch (self.kind) {
            .lmdb => if (supports_lmdb) blk: {
                var txn = try self.env.begin(.{});
                errdefer txn.abort();
                const dbi = try openConfiguredLmdbUserDbTxn(self.alloc, &txn, self.lmdb_user_db_kind, true);
                break :blk .{
                    .alloc = self.alloc,
                    .raw = txn,
                    .dbi = dbi,
                };
            } else error.UnsupportedPlatform,
            .runtime => .{
                .alloc = self.alloc,
                .write = try self.runtime_store.beginWrite(),
            },
        };
    }

    pub fn beginWriteBatch(self: *DocStore) !Batch {
        return try self.beginWriteBatchWithOptions(.{});
    }

    pub fn beginWriteBatchWithOptions(self: *DocStore, options: backend_types.BatchOptions) !Batch {
        return switch (self.kind) {
            .lmdb => if (supports_lmdb) blk: {
                var batch = try self.env.beginBatch();
                errdefer batch.abort();
                const dbi = try openConfiguredLmdbUserDbBatch(self.alloc, &batch, self.lmdb_user_db_kind, true);
                break :blk .{
                    .alloc = self.alloc,
                    .raw = batch,
                    .dbi = dbi,
                };
            } else error.UnsupportedPlatform,
            .runtime => .{
                .alloc = self.alloc,
                .runtime = try self.runtime_store.beginBatchWithOptions(options),
            },
        };
    }

    pub fn beginBulkIngestSession(self: *DocStore) !void {
        return switch (self.kind) {
            .lmdb => {},
            .runtime => try self.runtime_store.beginBulkIngestSession(),
        };
    }

    pub fn finishBulkIngestSessionWithOptions(self: *DocStore, options: backend_types.BulkIngestFinishOptions) !void {
        return switch (self.kind) {
            .lmdb => {},
            .runtime => try self.runtime_store.finishBulkIngestSessionWithOptions(options),
        };
    }

    pub fn flushBufferedWritesWithOptions(self: *DocStore, options: backend_types.BulkIngestFinishOptions) !void {
        return switch (self.kind) {
            .lmdb => if (supports_lmdb) try self.env.sync(false),
            .runtime => try self.runtime_store.flushBufferedWritesWithOptions(options),
        };
    }

    pub fn abortBulkIngestSession(self: *DocStore) void {
        switch (self.kind) {
            .lmdb => {},
            .runtime => self.runtime_store.abortBulkIngestSession(),
        }
    }

    pub fn put(self: *DocStore, key: []const u8, value: []const u8) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        try txn.put(key, value);
        try txn.commit();
    }

    /// Get a value by key. Caller owns the returned slice.
    pub fn get(self: *DocStore, alloc: Allocator, key: []const u8) ![]u8 {
        var txn = try self.beginReadTxn();
        defer txn.abort();
        const val = if (supports_lmdb)
            txn.get(key) catch |err| switch (err) {
                lmdb.Error.NotFound => return error.NotFound,
                else => return err,
            }
        else
            txn.get(key) catch |err| switch (err) {
                error.NotFound => return error.NotFound,
                else => return err,
            };
        return try alloc.dupe(u8, val);
    }

    pub fn delete(self: *DocStore, key: []const u8) !void {
        var txn = try self.beginWriteTxn();
        errdefer txn.abort();
        try txn.delete(key);
        try txn.commit();
    }

    /// Atomic batch: apply all writes and deletes in a single transaction.
    pub fn putBatch(self: *DocStore, writes: []const KVPair, deletes: []const []const u8) !void {
        try self.putBatchWithReplay(null, writes, deletes, null);
    }

    pub const ReplayAppend = struct {
        sequence: u64,
        payload: []const u8,
    };

    fn putBatchWithReplayOnceWithOptions(
        self: *DocStore,
        writes: []const KVPair,
        deletes: []const []const u8,
        replay: ?ReplayAppend,
        options: backend_types.BatchOptions,
    ) !void {
        var batch = try self.beginWriteBatchWithOptions(options);
        errdefer batch.abort();
        var txn = batch.asTxn();
        for (deletes) |key| {
            txn.delete(key) catch |err| switch (err) {
                error.NotFound => {}, // ignore missing keys
                else => return err,
            };
        }
        var used_bulk_append = false;
        if (deletes.len == 0 and options.mode == .bulk_ingest) {
            used_bulk_append = true;
            for (writes) |kv| {
                txn.appendPut(kv.key, kv.value) catch |err| switch (err) {
                    error.Unsupported => {
                        used_bulk_append = false;
                        break;
                    },
                    else => return err,
                };
            }
        }
        if (!used_bulk_append) {
            for (writes) |kv| {
                try txn.put(kv.key, kv.value);
            }
        }
        if (replay) |entry| {
            try batch.setReplayOpaque(entry.sequence, entry.payload);
        }
        try batch.commit();
    }

    fn putBatchWithReplayOnce(self: *DocStore, writes: []const KVPair, deletes: []const []const u8, replay: ?ReplayAppend) !void {
        try self.putBatchWithReplayOnceWithOptions(writes, deletes, replay, .{});
    }

    pub fn putBatchWithReplayWithOptions(
        self: *DocStore,
        io: ?std.Io,
        writes: []const KVPair,
        deletes: []const []const u8,
        replay: ?ReplayAppend,
        options: backend_types.BatchOptions,
    ) !void {
        var attempt: usize = 0;
        while (true) : (attempt += 1) {
            self.putBatchWithReplayOnceWithOptions(writes, deletes, replay, options) catch |err| switch (err) {
                error.WriterLocked => {
                    if (attempt >= writer_locked_retry_count) return err;
                    backoffWriterLockRetry(io);
                    continue;
                },
                else => return err,
            };
            if (replay) |entry| {
                self.markReplayIndexAvailable();
                self.observeCommittedReplaySequence(entry.sequence);
            }
            return;
        }
    }

    pub fn putBatchWithReplay(self: *DocStore, io: ?std.Io, writes: []const KVPair, deletes: []const []const u8, replay: ?ReplayAppend) !void {
        try self.putBatchWithReplayWithOptions(io, writes, deletes, replay, .{});
    }

    pub fn lastReplaySequence(self: *DocStore, fallback_last: u64) u64 {
        const next = self.nextReplaySequence(fallback_last + 1);
        return if (next <= 1) 0 else next - 1;
    }

    pub fn latestReplaySequenceForHint(self: *DocStore, hint: change_journal_mod.TargetHint, fallback_last: u64) !u64 {
        return try self.latestReplaySequenceForOrdinal(replayHintOrdinal(hint), fallback_last);
    }

    pub fn latestReplaySequenceForOrdinal(self: *DocStore, kind_ordinal: u8, fallback_last: u64) !u64 {
        if (!(try self.hasReplayEntries())) return fallback_last;

        var txn = try self.beginProbeTxn();
        defer txn.abort();

        const key = internal_keys.replayLatestSequenceKey(kind_ordinal);
        const raw = txn.get(key[0..]) catch |err| switch (err) {
            error.NotFound => return fallback_last,
            else => return err,
        };
        const latest = decodeReplaySequence(raw) orelse return error.CorruptReplayMetadata;
        return if (latest > fallback_last) latest else fallback_last;
    }

    pub fn nextReplaySequence(self: *DocStore, fallback_next: u64) u64 {
        return self.ensureReplayNextSequenceCached(fallback_next);
    }

    pub fn reserveNextReplaySequence(self: *DocStore, fallback_next: u64) u64 {
        while (true) {
            if (self.next_replay_sequence_cached.load(.acquire) != 0) {
                return self.next_replay_sequence_cached.fetchAdd(1, .acq_rel);
            }
            _ = self.ensureReplayNextSequenceCached(fallback_next);
        }
    }

    pub fn appendReplayOpaque(self: *DocStore, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var batch = try self.beginWriteBatch();
        errdefer batch.abort();
        try batch.setReplayOpaque(sequence, payload);
        try batch.commit();
        self.markReplayIndexAvailable();
        self.observeCommittedReplaySequence(sequence);
    }

    pub fn iterateReplayFrom(self: *DocStore, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
        return try self.iterateReplayEntriesFromOrdinal(alloc, from_sequence, internal_keys.replay_all_kind);
    }

    pub fn hasReplayEntries(self: *DocStore) !bool {
        switch (self.replay_index_state.load(.monotonic)) {
            replay_index_available => return true,
            replay_index_missing => return false,
            else => {},
        }

        var txn = try self.beginProbeTxn();
        defer txn.abort();
        _ = txn.get(internal_keys.replay_meta_init_key[0..]) catch |err| switch (err) {
            error.NotFound => {
                self.replay_index_state.store(replay_index_missing, .monotonic);
                return false;
            },
            else => return err,
        };
        self.markReplayIndexAvailable();
        return true;
    }

    pub fn ensureReplayIndexInitialized(self: *DocStore) !void {
        if (try self.hasReplayEntries()) return;

        var batch = try self.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(internal_keys.replay_meta_init_key[0..], "");
        const next_raw = encodeReplayNextSequence(1);
        try batch.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
        try batch.commit();
        self.markReplayIndexAvailable();
        _ = self.next_replay_sequence_cached.cmpxchgStrong(0, 1, .acq_rel, .acquire);
    }

    pub fn ensureReplayNextSequenceAtLeast(self: *DocStore, next_sequence: u64) !void {
        const desired_next = @max(next_sequence, @as(u64, 1));
        if (self.nextReplaySequence(1) >= desired_next) return;

        var next_raw: [8]u8 = undefined;
        std.mem.writeInt(u64, &next_raw, desired_next, .little);

        var batch = try self.beginWriteBatch();
        errdefer batch.abort();
        try batch.put(internal_keys.replay_meta_init_key[0..], "");
        try batch.put(internal_keys.replay_meta_next_sequence_key[0..], next_raw[0..]);
        try batch.commit();

        self.markReplayIndexAvailable();
        while (true) {
            const current = self.next_replay_sequence_cached.load(.acquire);
            if (current >= desired_next) return;
            if (self.next_replay_sequence_cached.cmpxchgWeak(current, desired_next, .acq_rel, .acquire) == null) return;
        }
    }

    fn markReplayIndexAvailable(self: *DocStore) void {
        self.replay_index_state.store(replay_index_available, .monotonic);
    }

    fn loadReplayNextSequenceFromStore(self: *DocStore, fallback_next: u64) u64 {
        var txn = self.beginProbeTxn() catch return fallback_next;
        defer txn.abort();
        const raw = txn.get(internal_keys.replay_meta_next_sequence_key[0..]) catch return fallback_next;
        if (raw.len != 8) return fallback_next;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    fn ensureReplayNextSequenceCached(self: *DocStore, fallback_next: u64) u64 {
        const cached = self.next_replay_sequence_cached.load(.acquire);
        if (cached != 0) return cached;

        const loaded = self.loadReplayNextSequenceFromStore(fallback_next);
        if (self.next_replay_sequence_cached.cmpxchgStrong(0, loaded, .acq_rel, .acquire)) |existing| {
            return existing;
        }
        return loaded;
    }

    fn observeCommittedReplaySequence(self: *DocStore, sequence: u64) void {
        const desired_next = sequence + 1;
        while (true) {
            const current = self.next_replay_sequence_cached.load(.acquire);
            if (current >= desired_next) return;
            if (self.next_replay_sequence_cached.cmpxchgWeak(current, desired_next, .acq_rel, .acquire) == null) return;
        }
    }

    fn iterateReplayEntriesFromOrdinal(
        self: *DocStore,
        alloc: Allocator,
        from_sequence: u64,
        kind_ordinal: u8,
    ) ![]backend_types.ReplayEntry {
        if (!(try self.hasReplayEntries())) return error.ReplayIndexUnavailable;

        var entries = std.ArrayListUnmanaged(backend_types.ReplayEntry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        }

        const Context = struct {
            alloc: Allocator,
            entries: *std.ArrayListUnmanaged(backend_types.ReplayEntry),

            fn handle(self_ctx: *@This(), sequence: u64, payload: []const u8) !void {
                try self_ctx.entries.append(self_ctx.alloc, .{
                    .sequence = sequence,
                    .payload = try self_ctx.alloc.dupe(u8, payload),
                });
            }
        };

        var ctx = Context{
            .alloc = alloc,
            .entries = &entries,
        };
        try self.forEachReplayEntryFromOrdinal(from_sequence, kind_ordinal, &ctx, Context.handle);
        return try entries.toOwnedSlice(alloc);
    }

    fn forEachReplayEntryFromOrdinal(
        self: *DocStore,
        from_sequence: u64,
        kind_ordinal: u8,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        if (!(try self.hasReplayEntries())) return error.ReplayIndexUnavailable;

        var txn = try self.beginCurrentScanTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        const lower = internal_keys.replayRangeLower(kind_ordinal, from_sequence);
        const upper = internal_keys.replayRangeUpper(kind_ordinal);
        cur.setUpperBound(upper[0..]);

        var entry = try cur.seekAtOrAfter(lower[0..]);
        while (entry) |kv| : (entry = try cur.next()) {
            if (std.mem.order(u8, kv.key, upper[0..]) != .lt) break;
            const sequence = internal_keys.parseReplayEntrySequence(kv.key, kind_ordinal) orelse break;
            try callback(ctx, sequence, kv.value);
        }
    }

    pub fn iterateReplayEntriesFromHint(
        self: *DocStore,
        alloc: Allocator,
        from_sequence: u64,
        hint: change_journal_mod.TargetHint,
    ) ![]backend_types.ReplayEntry {
        return try self.iterateReplayEntriesFromOrdinal(alloc, from_sequence, replayHintOrdinal(hint));
    }

    pub fn forEachReplayEntryFromHint(
        self: *DocStore,
        from_sequence: u64,
        hint: change_journal_mod.TargetHint,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        return try self.forEachReplayEntryFromOrdinal(from_sequence, replayHintOrdinal(hint), ctx, callback);
    }

    pub fn forEachReplayFrom(
        self: *DocStore,
        from_sequence: u64,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        return try self.forEachReplayEntryFromOrdinal(from_sequence, internal_keys.replay_all_kind, ctx, callback);
    }

    pub fn forEachReplayFromMatchingHint(
        self: *DocStore,
        from_sequence: u64,
        hint: change_journal_mod.TargetHint,
        ctx: anytype,
        comptime callback: fn (@TypeOf(ctx), u64, []const u8) anyerror!void,
    ) !void {
        return try self.forEachReplayEntryFromHint(from_sequence, hint, ctx, callback);
    }

    pub fn truncateReplayUpTo(self: *DocStore, alloc: Allocator, up_to_sequence: u64) !void {
        try self.truncateReplayEntries(alloc, up_to_sequence);
    }

    fn truncateReplayEntries(self: *DocStore, alloc: Allocator, up_to_sequence: u64) !void {
        if (up_to_sequence == 0) return;
        if (!(try self.hasReplayEntries())) return;

        var deletes = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(key);
            deletes.deinit(alloc);
        }

        {
            var txn = try self.beginCurrentScanTxn();
            defer txn.abort();

            var cur = try txn.openCursor();
            defer cur.close();

            for (replay_hints) |hint| {
                const hint_ordinal = replayHintOrdinal(hint);
                const lower = internal_keys.replayRangeLower(hint_ordinal, 0);
                const upper = internal_keys.replayRangeUpper(hint_ordinal);

                var entry = try cur.seekAtOrAfter(lower[0..]);
                while (entry) |kv| : (entry = try cur.next()) {
                    if (std.mem.order(u8, kv.key, upper[0..]) != .lt) break;
                    const sequence = internal_keys.parseReplayEntrySequence(kv.key, hint_ordinal) orelse break;
                    if (sequence > up_to_sequence) break;
                    try deletes.append(alloc, try alloc.dupe(u8, kv.key));
                }
            }

            const all_lower = internal_keys.replayRangeLower(internal_keys.replay_all_kind, 0);
            const all_upper = internal_keys.replayRangeUpper(internal_keys.replay_all_kind);
            var entry = try cur.seekAtOrAfter(all_lower[0..]);
            while (entry) |kv| : (entry = try cur.next()) {
                if (std.mem.order(u8, kv.key, all_upper[0..]) != .lt) break;
                const sequence = internal_keys.parseReplayEntrySequence(kv.key, internal_keys.replay_all_kind) orelse break;
                if (sequence > up_to_sequence) break;
                try deletes.append(alloc, try alloc.dupe(u8, kv.key));
            }
        }

        if (deletes.items.len == 0) return;

        var batch = try self.beginWriteBatch();
        errdefer batch.abort();
        for (deletes.items) |key| {
            batch.delete(key) catch |err| switch (err) {
                error.NotFound => {},
                else => return err,
            };
        }
        try batch.commit();
    }

    /// Scan all keys with the given prefix. Caller owns returned slices.
    pub fn scanPrefix(self: *DocStore, alloc: Allocator, prefix: []const u8) ![]OwnedKVPair {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();

        var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
        errdefer {
            for (results.items) |item| {
                alloc.free(item.key);
                alloc.free(item.value);
            }
            results.deinit(alloc);
        }

        // Seek to first key >= prefix
        const first = (try cur.seekAtOrAfter(prefix)) orelse return try alloc.dupe(OwnedKVPair, results.items);

        if (std.mem.startsWith(u8, first.key, prefix)) {
            try results.append(alloc, .{
                .key = try alloc.dupe(u8, first.key),
                .value = try alloc.dupe(u8, first.value),
            });
        } else {
            return try alloc.dupe(OwnedKVPair, results.items);
        }

        var entry = try cur.next();
        while (entry) |kv| : (entry = try cur.next()) {
            if (!std.mem.startsWith(u8, kv.key, prefix)) break;
            try results.append(alloc, .{
                .key = try alloc.dupe(u8, kv.key),
                .value = try alloc.dupe(u8, kv.value),
            });
        }

        const owned = try alloc.dupe(OwnedKVPair, results.items);
        results.deinit(alloc);
        return owned;
    }

    /// Scan keys in [lower, upper). Caller owns returned slices.
    pub fn scanRange(self: *DocStore, alloc: Allocator, lower: []const u8, upper: []const u8) ![]OwnedKVPair {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();
        cur.setUpperBound(if (upper.len > 0) upper else null);

        var results = std.ArrayListUnmanaged(OwnedKVPair).empty;
        errdefer {
            for (results.items) |item| {
                alloc.free(item.key);
                alloc.free(item.value);
            }
            results.deinit(alloc);
        }

        // Seek to first key >= lower, or first key in the DB when lower is empty.
        const first = if (lower.len == 0)
            (try cur.first()) orelse return try alloc.dupe(OwnedKVPair, results.items)
        else
            (try cur.seekAtOrAfter(lower)) orelse return try alloc.dupe(OwnedKVPair, results.items);

        if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) {
            return try alloc.dupe(OwnedKVPair, results.items);
        }

        try results.append(alloc, .{
            .key = try alloc.dupe(u8, first.key),
            .value = try alloc.dupe(u8, first.value),
        });

        var entry = try cur.next();
        while (entry) |kv| : (entry = try cur.next()) {
            if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
            try results.append(alloc, .{
                .key = try alloc.dupe(u8, kv.key),
                .value = try alloc.dupe(u8, kv.value),
            });
        }

        const owned = try alloc.dupe(OwnedKVPair, results.items);
        results.deinit(alloc);
        return owned;
    }

    pub fn findMedianKey(self: *DocStore, alloc: Allocator, lower: []const u8, upper: []const u8, options: ScanOptions) ![]u8 {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        const visible_count = try countVisibleRange(&txn, lower, upper, options);
        if (visible_count == 0) return error.NotFound;

        return try copyVisibleKeyAtIndex(&txn, alloc, lower, upper, options, visible_count / 2);
    }

    // ====================================================================
    // Streaming scan — constant memory, callback-based
    // ====================================================================

    pub const ScanOptions = struct {
        /// Return true to skip this key (callback not invoked).
        skip_fn: ?*const fn (key: []const u8) bool = null,
    };

    pub const ScanAction = enum { @"continue", stop };

    pub const ScanWithContextCallback = *const fn (
        ctx: ?*anyopaque,
        key: []const u8,
        value: []const u8,
    ) anyerror!ScanAction;

    /// Streaming scan over [lower, upper). Constant memory — callback sees
    /// LMDB mmap'd memory directly (valid only for duration of call).
    /// If upper is empty, scans to end of database.
    pub fn scan(
        self: *DocStore,
        lower: []const u8,
        upper: []const u8,
        options: ScanOptions,
        callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,
    ) !void {
        const Adapter = struct {
            callback: *const fn (key: []const u8, value: []const u8) anyerror!ScanAction,

            fn run(ctx: ?*anyopaque, key: []const u8, value: []const u8) anyerror!ScanAction {
                const adapter: *@This() = @ptrCast(@alignCast(ctx orelse return error.InvalidArgument));
                return try adapter.callback(key, value);
            }
        };
        var adapter = Adapter{ .callback = callback };
        return try self.scanWithContext(lower, upper, options, &adapter, Adapter.run);
    }

    pub fn scanWithContext(
        self: *DocStore,
        lower: []const u8,
        upper: []const u8,
        options: ScanOptions,
        ctx: ?*anyopaque,
        callback: ScanWithContextCallback,
    ) !void {
        var txn = try self.beginReadTxn();
        defer txn.abort();

        var cur = try txn.openCursor();
        defer cur.close();
        cur.setUpperBound(if (upper.len > 0) upper else null);

        // Seek to first key >= lower (use .first when lower is empty)
        const first = if (lower.len == 0)
            (try cur.first()) orelse return
        else
            (try cur.seekAtOrAfter(lower)) orelse return;

        // Check upper bound
        if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) return;

        // Process first entry
        if (options.skip_fn == null or !options.skip_fn.?(first.key)) {
            const action = try callback(ctx, first.key, first.value);
            if (action == .stop) return;
        }

        // Iterate remaining
        var entry = try cur.next();
        while (entry) |kv| : (entry = try cur.next()) {
            if (upper.len > 0 and std.mem.order(u8, kv.key, upper) != .lt) break;
            if (options.skip_fn) |skip| {
                if (skip(kv.key)) continue;
            }
            const action = try callback(ctx, kv.key, kv.value);
            if (action == .stop) return;
        }
    }

    /// Free results from scanPrefix or scanRange.
    pub fn freeResults(alloc: Allocator, results: []OwnedKVPair) void {
        for (results) |item| {
            alloc.free(item.key);
            alloc.free(item.value);
        }
        alloc.free(results);
    }
};

const RuntimeStoreHandle = struct {
    store: backend_erased.Store,
    owned: bool,
};

fn initRuntimeStore(alloc: Allocator, store: anytype) !RuntimeStoreHandle {
    const T = @TypeOf(store);
    if (T == backend_erased.Store) return .{ .store = store, .owned = true };
    if (T == *backend_erased.Store) return .{ .store = store.*, .owned = false };

    switch (@typeInfo(T)) {
        .pointer => |ptr| {
            if (@typeInfo(ptr.child) == .@"struct" and @hasDecl(ptr.child, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        .@"struct" => {
            if (@hasDecl(T, "backendStore")) {
                return .{
                    .store = try backend_erased.storeFrom(alloc, store.backendStore()),
                    .owned = true,
                };
            }
        },
        else => {},
    }

    return .{
        .store = try backend_erased.storeFrom(alloc, store),
        .owned = true,
    };
}

fn countVisibleRange(
    txn: *DocStore.Txn,
    lower: []const u8,
    upper: []const u8,
    options: DocStore.ScanOptions,
) !usize {
    var cur = try txn.openCursor();
    defer cur.close();

    const first = if (lower.len == 0)
        (try cur.first()) orelse return 0
    else
        (try cur.seekAtOrAfter(lower)) orelse return 0;

    if (upper.len > 0 and std.mem.order(u8, first.key, upper) != .lt) return 0;

    var count: usize = 0;
    var entry = first;
    while (true) {
        if (options.skip_fn) |skip| {
            if (!skip(entry.key)) count += 1;
        } else {
            count += 1;
        }

        entry = (try cur.next()) orelse break;
        if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break;
    }

    return count;
}

fn copyVisibleKeyAtIndex(
    txn: *DocStore.Txn,
    alloc: Allocator,
    lower: []const u8,
    upper: []const u8,
    options: DocStore.ScanOptions,
    target_index: usize,
) ![]u8 {
    var cur = try txn.openCursor();
    defer cur.close();

    var entry = if (lower.len == 0)
        (try cur.first()) orelse return error.NotFound
    else
        (try cur.seekAtOrAfter(lower)) orelse return error.NotFound;

    if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) return error.NotFound;

    var count: usize = 0;
    while (true) {
        if (options.skip_fn) |skip| {
            if (!skip(entry.key)) {
                if (count == target_index) return try alloc.dupe(u8, entry.key);
                count += 1;
            }
        } else {
            if (count == target_index) return try alloc.dupe(u8, entry.key);
            count += 1;
        }

        entry = (try cur.next()) orelse break;
        if (upper.len > 0 and std.mem.order(u8, entry.key, upper) != .lt) break;
    }

    return error.NotFound;
}

// ============================================================================
// KeyEncoder — static key construction helpers
// ============================================================================

pub const KeyEncoder = struct {
    /// Build edge key: <source>:i:<indexName>:out:<edgeType>:<target>:o
    pub fn makeEdgeKey(buf: []u8, source: []const u8, index_name: []const u8, edge_type: []const u8, target: []const u8) []const u8 {
        const result = std.fmt.bufPrint(buf, "{s}:i:{s}:out:{s}:{s}:o", .{ source, index_name, edge_type, target }) catch unreachable;
        return result;
    }

    /// Build reverse edge key: <target>:i:<indexName>:in:<edgeType>:<source>:i
    pub fn makeReverseEdgeKey(buf: []u8, target: []const u8, index_name: []const u8, edge_type: []const u8, source: []const u8) []const u8 {
        const result = std.fmt.bufPrint(buf, "{s}:i:{s}:in:{s}:{s}:i", .{ target, index_name, edge_type, source }) catch unreachable;
        return result;
    }

    /// Build edge prefix for scanning: <key>:i:<indexName>:out:<edgeType>:
    /// If edge_type is empty, prefix is: <key>:i:<indexName>:out:
    pub fn makeEdgePrefix(buf: []u8, key: []const u8, index_name: []const u8, edge_type: []const u8) []const u8 {
        if (edge_type.len > 0) {
            return std.fmt.bufPrint(buf, "{s}:i:{s}:out:{s}:", .{ key, index_name, edge_type }) catch unreachable;
        }
        return std.fmt.bufPrint(buf, "{s}:i:{s}:out:", .{ key, index_name }) catch unreachable;
    }

    /// Build reverse edge prefix: <key>:i:<indexName>:in:<edgeType>:
    pub fn makeReverseEdgePrefix(buf: []u8, key: []const u8, index_name: []const u8, edge_type: []const u8) []const u8 {
        if (edge_type.len > 0) {
            return std.fmt.bufPrint(buf, "{s}:i:{s}:in:{s}:", .{ key, index_name, edge_type }) catch unreachable;
        }
        return std.fmt.bufPrint(buf, "{s}:i:{s}:in:", .{ key, index_name }) catch unreachable;
    }

    /// Build embedding key: <doc>:i:<indexName>:e
    pub fn makeEmbeddingKey(buf: []u8, doc_key: []const u8, index_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:i:{s}:e", .{ doc_key, index_name }) catch unreachable;
    }

    /// Build summary key: <doc>:i:<indexName>:s
    pub fn makeSummaryKey(buf: []u8, doc_key: []const u8, index_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:i:{s}:s", .{ doc_key, index_name }) catch unreachable;
    }

    /// Build chunk key: <doc>:i:<indexName>:<chunkID>:c
    pub fn makeChunkKey(buf: []u8, doc_key: []const u8, index_name: []const u8, chunk_id: u32) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:i:{s}:{d}:c", .{ doc_key, index_name, chunk_id }) catch unreachable;
    }

    /// Build enrichment prefix: <doc>:e:<type>:<name>:
    pub fn makeEnrichmentPrefix(buf: []u8, doc_key: []const u8, enrichment_type: []const u8, enrichment_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:{s}:{s}:", .{ doc_key, enrichment_type, enrichment_name }) catch unreachable;
    }

    /// Build root enrichment prefix: <doc>:e:
    pub fn makeEnrichmentRootPrefix(buf: []u8, doc_key: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:", .{doc_key}) catch unreachable;
    }

    /// Build enrichment type prefix: <doc>:e:<type>:
    pub fn makeEnrichmentTypePrefix(buf: []u8, doc_key: []const u8, enrichment_type: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:{s}:", .{ doc_key, enrichment_type }) catch unreachable;
    }

    /// Build enrichment chunk key: <doc>:e:chunk:<name>:<chunkID>
    pub fn makeEnrichmentChunkKey(buf: []u8, doc_key: []const u8, enrichment_name: []const u8, chunk_id: u32) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:chunk:{s}:{d}", .{ doc_key, enrichment_name, chunk_id }) catch unreachable;
    }

    /// Build enrichment embedding key: <base>:e:embedding:<name>
    pub fn makeEnrichmentEmbeddingKey(buf: []u8, base_key: []const u8, enrichment_name: []const u8) []const u8 {
        return std.fmt.bufPrint(buf, "{s}:e:embedding:{s}", .{ base_key, enrichment_name }) catch unreachable;
    }

    /// Range start sentinel: <key>:\x00
    pub fn keyRangeStart(buf: []u8, key: []const u8) []const u8 {
        @memcpy(buf[0..key.len], key);
        buf[key.len] = ':';
        buf[key.len + 1] = 0x00;
        return buf[0 .. key.len + 2];
    }

    /// Range end sentinel: <key>:\xFF
    pub fn keyRangeEnd(buf: []u8, key: []const u8) []const u8 {
        @memcpy(buf[0..key.len], key);
        buf[key.len] = ':';
        buf[key.len + 1] = 0xFF;
        return buf[0 .. key.len + 2];
    }

    /// Check if a key is an edge key (contains ":i:" and ends with ":o").
    pub fn isEdgeKey(key: []const u8) bool {
        if (key.len < 6) return false; // minimum: "x:i:y:o"
        if (key[key.len - 2] != ':' or key[key.len - 1] != 'o') return false;
        return std.mem.indexOf(u8, key, ":i:") != null;
    }

    /// Parsed edge key components.
    pub const ParsedEdgeKey = struct {
        source: []const u8,
        index_name: []const u8,
        edge_type: []const u8,
        target: []const u8,
    };

    /// Parse an outgoing edge key: <source>:i:<indexName>:out:<edgeType>:<target>:o
    /// Also handles reverse edge keys: <target>:i:<indexName>:in:<edgeType>:<source>:i
    pub fn parseEdgeKey(key: []const u8) ?ParsedEdgeKey {
        // Find ":i:" marker
        const idx_marker = std.mem.indexOf(u8, key, ":i:") orelse return null;
        const before = key[0..idx_marker];
        const after_marker = key[idx_marker + 3 ..]; // <indexName>:out/in:<edgeType>:<target>:o/i

        // Try outgoing: <indexName>:out:<edgeType>:<target>:o
        if (std.mem.indexOf(u8, after_marker, ":out:")) |out_pos| {
            const index_name = after_marker[0..out_pos];
            const after_out = after_marker[out_pos + 5 ..]; // <edgeType>:<target>:o
            // Must end with ":o"
            if (after_out.len < 2) return null;
            if (after_out[after_out.len - 2] != ':' or after_out[after_out.len - 1] != 'o') return null;
            const rest = after_out[0 .. after_out.len - 2]; // <edgeType>:<target>
            const type_end = std.mem.indexOf(u8, rest, ":") orelse return null;
            return .{
                .source = before,
                .index_name = index_name,
                .edge_type = rest[0..type_end],
                .target = rest[type_end + 1 ..],
            };
        }

        // Try incoming: <indexName>:in:<edgeType>:<source>:i
        if (std.mem.indexOf(u8, after_marker, ":in:")) |in_pos| {
            const index_name = after_marker[0..in_pos];
            const after_in = after_marker[in_pos + 4 ..]; // <edgeType>:<source>:i
            // Must end with ":i"
            if (after_in.len < 2) return null;
            if (after_in[after_in.len - 2] != ':' or after_in[after_in.len - 1] != 'i') return null;
            const rest = after_in[0 .. after_in.len - 2]; // <edgeType>:<source>
            const type_end = std.mem.indexOf(u8, rest, ":") orelse return null;
            return .{
                .source = rest[type_end + 1 ..],
                .index_name = index_name,
                .edge_type = rest[0..type_end],
                .target = before,
            };
        }

        return null;
    }
};

// ============================================================================
// Tests
// ============================================================================

var tmp_path_nonce: u64 = 0;

fn tmpPath(buf: []u8) [*:0]const u8 {
    const base = "/tmp/antfly-docstore-test-";
    const ts = platform_time.monotonicNs();
    const nonce = @atomicRmw(u64, &tmp_path_nonce, .Add, 1, .monotonic);
    const slice = std.fmt.bufPrint(buf, "{s}{d}-{d}\x00", .{ base, ts, nonce }) catch unreachable;
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    fs_paths.createDirPathPortable(io_impl.io(), std.mem.span(@as([*:0]const u8, @ptrCast(slice.ptr)))) catch unreachable;
    return @ptrCast(slice.ptr);
}

fn cleanupTmp(path: [*:0]const u8) void {
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    std.Io.Dir.cwd().deleteTree(io_impl.io(), std.mem.span(path)) catch {};
}

test "docstore put/get/delete" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("doc1", "hello");
    try store.put("doc2", "world");

    const val1 = try store.get(std.testing.allocator, "doc1");
    defer std.testing.allocator.free(val1);
    try std.testing.expectEqualStrings("hello", val1);

    const val2 = try store.get(std.testing.allocator, "doc2");
    defer std.testing.allocator.free(val2);
    try std.testing.expectEqualStrings("world", val2);

    try store.delete("doc1");
    try std.testing.expectError(lmdb.Error.NotFound, store.get(std.testing.allocator, "doc1"));
}

test "docstore putBatch atomic" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    // Pre-populate
    try store.put("key_a", "old_a");
    try store.put("key_b", "old_b");

    // Batch: write two new, delete one old
    const writes = [_]KVPair{
        .{ .key = "key_c", .value = "val_c" },
        .{ .key = "key_d", .value = "val_d" },
    };
    const deletes = [_][]const u8{"key_a"};
    try store.putBatch(&writes, &deletes);

    // key_a deleted
    try std.testing.expectError(lmdb.Error.NotFound, store.get(std.testing.allocator, "key_a"));

    // key_b unchanged
    const b = try store.get(std.testing.allocator, "key_b");
    defer std.testing.allocator.free(b);
    try std.testing.expectEqualStrings("old_b", b);

    // New keys exist
    const c_val = try store.get(std.testing.allocator, "key_c");
    defer std.testing.allocator.free(c_val);
    try std.testing.expectEqualStrings("val_c", c_val);
}

test "docstore scanPrefix" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("user:1", "alice");
    try store.put("user:2", "bob");
    try store.put("user:3", "carol");
    try store.put("item:1", "widget");
    try store.put("item:2", "gadget");

    const users = try store.scanPrefix(std.testing.allocator, "user:");
    defer DocStore.freeResults(std.testing.allocator, users);

    try std.testing.expectEqual(@as(usize, 3), users.len);
    try std.testing.expectEqualStrings("user:1", users[0].key);
    try std.testing.expectEqualStrings("alice", users[0].value);
    try std.testing.expectEqualStrings("user:3", users[2].key);
}

test "docstore scanRange" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");
    try store.put("e", "5");

    // Range [b, d) should return b, c
    const results = try store.scanRange(std.testing.allocator, "b", "d");
    defer DocStore.freeResults(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("b", results[0].key);
    try std.testing.expectEqualStrings("c", results[1].key);
}

test "docstore scanRange unbounded upper" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");

    // Range [b, +inf) = empty upper bound
    const results = try store.scanRange(std.testing.allocator, "b", "");
    defer DocStore.freeResults(std.testing.allocator, results);

    try std.testing.expectEqual(@as(usize, 2), results.len);
    try std.testing.expectEqualStrings("b", results[0].key);
    try std.testing.expectEqualStrings("c", results[1].key);
}

fn skipInternalMedianKey(key: []const u8) bool {
    return std.mem.startsWith(u8, key, "\x00\x00__metadata__:") or
        std.mem.startsWith(u8, key, "splitstate:") or
        std.mem.startsWith(u8, key, "splitdelta:");
}

test "docstore findMedianKey ignores internal keys" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "va");
    try store.put("b", "vb");
    try store.put("c", "vc");
    try store.put("d", "vd");
    try store.put("e", "ve");
    try store.put("\x00\x00__metadata__:schema", "meta");
    try store.put("splitstate:current", "state");
    try store.put("splitdelta:0001", "delta");

    const key = try store.findMedianKey(std.testing.allocator, "", "", .{ .skip_fn = &skipInternalMedianKey });
    defer std.testing.allocator.free(key);
    try std.testing.expectEqualStrings("c", key);
}

test "docstore findMedianKey respects range bounds" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "va");
    try store.put("b", "vb");
    try store.put("c", "vc");
    try store.put("d", "vd");
    try store.put("e", "ve");

    const key = try store.findMedianKey(std.testing.allocator, "b", "e", .{});
    defer std.testing.allocator.free(key);
    try std.testing.expectEqualStrings("c", key);
}

test "ByteRange.contains" {
    // [b, d)
    const range = ByteRange{ .start = "b", .end = "d" };
    try std.testing.expect(!range.contains("a"));
    try std.testing.expect(range.contains("b"));
    try std.testing.expect(range.contains("c"));
    try std.testing.expect(!range.contains("d"));
    try std.testing.expect(!range.contains("e"));

    // Unbounded: ["", "")
    const all = ByteRange{ .start = "", .end = "" };
    try std.testing.expect(all.contains("anything"));

    // Half-bounded: [c, "")
    const from_c = ByteRange{ .start = "c", .end = "" };
    try std.testing.expect(!from_c.contains("b"));
    try std.testing.expect(from_c.contains("c"));
    try std.testing.expect(from_c.contains("z"));
}

test "KeyEncoder edge key round-trip" {
    var buf: [512]u8 = undefined;
    const key = KeyEncoder.makeEdgeKey(&buf, "doc1", "graph_idx", "cites", "paper2");
    try std.testing.expectEqualStrings("doc1:i:graph_idx:out:cites:paper2:o", key);
    try std.testing.expect(KeyEncoder.isEdgeKey(key));

    const parsed = KeyEncoder.parseEdgeKey(key).?;
    try std.testing.expectEqualStrings("doc1", parsed.source);
    try std.testing.expectEqualStrings("graph_idx", parsed.index_name);
    try std.testing.expectEqualStrings("cites", parsed.edge_type);
    try std.testing.expectEqualStrings("paper2", parsed.target);
}

test "KeyEncoder reverse edge key round-trip" {
    var buf: [512]u8 = undefined;
    const key = KeyEncoder.makeReverseEdgeKey(&buf, "paper2", "graph_idx", "cites", "doc1");
    try std.testing.expectEqualStrings("paper2:i:graph_idx:in:cites:doc1:i", key);

    const parsed = KeyEncoder.parseEdgeKey(key).?;
    try std.testing.expectEqualStrings("doc1", parsed.source);
    try std.testing.expectEqualStrings("graph_idx", parsed.index_name);
    try std.testing.expectEqualStrings("cites", parsed.edge_type);
    try std.testing.expectEqualStrings("paper2", parsed.target);
}

test "KeyEncoder enrichment keys" {
    var buf: [512]u8 = undefined;

    const emb = KeyEncoder.makeEmbeddingKey(&buf, "doc1", "emb_idx");
    try std.testing.expectEqualStrings("doc1:i:emb_idx:e", emb);

    const sum = KeyEncoder.makeSummaryKey(&buf, "doc1", "sum_idx");
    try std.testing.expectEqualStrings("doc1:i:sum_idx:s", sum);

    const chunk = KeyEncoder.makeChunkKey(&buf, "doc1", "chunk_idx", 42);
    try std.testing.expectEqualStrings("doc1:i:chunk_idx:42:c", chunk);

    const e_prefix = KeyEncoder.makeEnrichmentPrefix(&buf, "doc1", "chunk", "body_chunks_v1");
    try std.testing.expectEqualStrings("doc1:e:chunk:body_chunks_v1:", e_prefix);

    const e_root_prefix = KeyEncoder.makeEnrichmentRootPrefix(&buf, "doc1");
    try std.testing.expectEqualStrings("doc1:e:", e_root_prefix);

    const e_type_prefix = KeyEncoder.makeEnrichmentTypePrefix(&buf, "doc1", "chunk");
    try std.testing.expectEqualStrings("doc1:e:chunk:", e_type_prefix);

    const e_chunk = KeyEncoder.makeEnrichmentChunkKey(&buf, "doc1", "body_chunks_v1", 42);
    try std.testing.expectEqualStrings("doc1:e:chunk:body_chunks_v1:42", e_chunk);

    const e_emb = KeyEncoder.makeEnrichmentEmbeddingKey(&buf, "doc1", "body_dense_v1");
    try std.testing.expectEqualStrings("doc1:e:embedding:body_dense_v1", e_emb);

    const chunk_emb = KeyEncoder.makeEnrichmentEmbeddingKey(&buf, "doc1:e:chunk:body_chunks_v1:42", "body_dense_v1");
    try std.testing.expectEqualStrings("doc1:e:chunk:body_chunks_v1:42:e:embedding:body_dense_v1", chunk_emb);
}

test "KeyEncoder range sentinels" {
    var buf: [512]u8 = undefined;

    const start = KeyEncoder.keyRangeStart(&buf, "doc1");
    try std.testing.expectEqual(@as(usize, 6), start.len);
    try std.testing.expectEqualStrings("doc1:", start[0..5]);
    try std.testing.expectEqual(@as(u8, 0x00), start[5]);

    var buf2: [512]u8 = undefined;
    const end_key = KeyEncoder.keyRangeEnd(&buf2, "doc1");
    try std.testing.expectEqual(@as(usize, 6), end_key.len);
    try std.testing.expectEqualStrings("doc1:", end_key[0..5]);
    try std.testing.expectEqual(@as(u8, 0xFF), end_key[5]);
}

test "docstore streaming scan visits all keys" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");
    try store.put("e", "5");

    const Context = struct {
        var count: usize = 0;
        var last_key: [1]u8 = undefined;
        fn cb(key: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
            count += 1;
            last_key[0] = key[0];
            return .@"continue";
        }
    };
    Context.count = 0;

    // Scan [b, d) — should visit b, c
    try store.scan("b", "d", .{}, &Context.cb);
    try std.testing.expectEqual(@as(usize, 2), Context.count);
    try std.testing.expectEqual(@as(u8, 'c'), Context.last_key[0]);
}

test "docstore streaming scan skip_fn" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("aa", "1");
    try store.put("ab", "2");
    try store.put("ba", "3");
    try store.put("bb", "4");

    const Context = struct {
        var count: usize = 0;
        fn skipB(key: []const u8) bool {
            return key[0] == 'b';
        }
        fn cb(_: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
            count += 1;
            return .@"continue";
        }
    };
    Context.count = 0;

    // Scan all, skip keys starting with 'b'
    try store.scan("a", "", .{ .skip_fn = &Context.skipB }, &Context.cb);
    try std.testing.expectEqual(@as(usize, 2), Context.count); // only aa, ab
}

test "docstore streaming scan stop early" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("a", "1");
    try store.put("b", "2");
    try store.put("c", "3");
    try store.put("d", "4");

    const Context = struct {
        var count: usize = 0;
        fn cb(_: []const u8, _: []const u8) anyerror!DocStore.ScanAction {
            count += 1;
            if (count >= 2) return .stop;
            return .@"continue";
        }
    };
    Context.count = 0;

    try store.scan("a", "", .{}, &Context.cb);
    try std.testing.expectEqual(@as(usize, 2), Context.count);
}

test "docstore reopen preserves data" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    // Write data
    {
        var store = try DocStore.open(std.testing.allocator, path, .{});
        defer store.close();
        try store.put("persist_key", "persist_val");
    }

    // Reopen and verify
    {
        var store = try DocStore.open(std.testing.allocator, path, .{});
        defer store.close();
        const val = try store.get(std.testing.allocator, "persist_key");
        defer std.testing.allocator.free(val);
        try std.testing.expectEqualStrings("persist_val", val);
    }
}

test "docstore exposes lmdb commit stats when available" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.put("stats_key", "stats_val");

    if (store.commitStatsSnapshot()) |stats| {
        try std.testing.expect(stats.publish_calls >= 1);
        try std.testing.expect(stats.full_publish_calls >= 1);
        try std.testing.expect(stats.page_images_written > 0);
        try std.testing.expect(stats.bytes_written > 0);
        try std.testing.expect(stats.total_publish_ns > 0);
    }
}

test "docstore does not expose commit stats for runtime-backed stores" {
    var backend = mem_backend.Backend.init(std.testing.allocator, .{});
    defer backend.close();

    const runtime_store = try backend.runtimeStore(std.testing.allocator, .{});
    var store = try DocStore.openRuntime(std.testing.allocator, runtime_store);
    defer store.close();

    try store.put("stats_key", "stats_val");
    try std.testing.expect(store.commitStatsSnapshot() == null);
}

test "docstore runtime lsm exposes large replaying graph artifact prefix batch immediately after commit" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var backend = try lsm_backend.Backend.open(alloc, path, .{
        .flush_threshold_bytes = 32 * 1024 * 1024,
    });
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    var writes = std.ArrayListUnmanaged(KVPair).empty;
    defer writes.deinit(alloc);
    try writes.ensureTotalCapacity(alloc, 1500);

    var i: usize = 0;
    while (i < 1500) : (i += 1) {
        const target = try std.fmt.allocPrint(alloc, "doc:{d:0>4}", .{i});
        defer alloc.free(target);
        const key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:0000", "gr_v1", "links", target);
        errdefer alloc.free(key);
        const value = try std.fmt.allocPrint(alloc, "{{\"target\":\"{s}\"}}", .{target});
        errdefer alloc.free(value);
        try writes.append(alloc, .{ .key = key, .value = value });
    }
    defer {
        for (writes.items) |kv| {
            alloc.free(@constCast(kv.key));
            alloc.free(@constCast(kv.value));
        }
    }

    try store.putBatchWithReplay(null, writes.items, &.{}, .{
        .sequence = 1,
        .payload = "replay:graph",
    });

    const prefix = try internal_keys.graphArtifactIndexPrefixAlloc(alloc, "doc:0000", "gr_v1");
    defer alloc.free(prefix);
    const results = try store.scanPrefix(alloc, prefix);
    defer DocStore.freeResults(alloc, results);
    try std.testing.expectEqual(@as(usize, 1500), results.len);
}

test "docstore rewriteLeftInPlace keeps metadata and drops right range" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try DocStore.open(alloc, path_z, .{});
    defer store.close();

    try store.put("\x00\x00__metadata__:range", "meta");
    try store.put("doc:a", "a");
    try store.put("doc:m", "m");
    try store.put("doc:z", "z");

    if (!(try store.rewriteLeftInPlace("doc:m"))) return;

    const meta = try store.get(alloc, "\x00\x00__metadata__:range");
    defer alloc.free(meta);
    try std.testing.expectEqualStrings("meta", meta);

    const left_doc = try store.get(alloc, "doc:a");
    defer alloc.free(left_doc);
    try std.testing.expectEqualStrings("a", left_doc);
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "doc:m"));
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "doc:z"));
}

test "docstore splitRightToDir opens child image from split main db" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var src_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const src_path = try std.fmt.bufPrint(&src_path_buf, ".zig-cache/tmp/{s}/src", .{tmp.sub_path});
    const src_path_z = try alloc.dupeZ(u8, src_path);
    defer alloc.free(src_path_z);

    var child_dir_buf: [std.fs.max_path_bytes]u8 = undefined;
    const child_dir = try std.fmt.bufPrint(&child_dir_buf, ".zig-cache/tmp/{s}/child", .{tmp.sub_path});
    var io_impl = std.Io.Threaded.init(std.heap.page_allocator, .{});
    defer io_impl.deinit();
    try fs_paths.createDirPathPortable(io_impl.io(), child_dir);

    var store = try DocStore.open(alloc, src_path_z, .{});
    defer store.close();

    try store.put("\x00\x00__metadata__:range", "meta");
    try store.put("doc:a", "a");
    try store.put("doc:b", "b");
    try store.put("doc:m", "m");
    try store.put("doc:z", "z");

    if (!(try store.splitRightToDir("doc:m", child_dir))) return;

    const child_path_z = try alloc.dupeZ(u8, child_dir);
    defer alloc.free(child_path_z);
    var child = try DocStore.open(alloc, child_path_z, .{});
    defer child.close();

    const right_doc = try child.get(alloc, "doc:z");
    defer alloc.free(right_doc);
    try std.testing.expectEqualStrings("z", right_doc);

    const split_doc = try child.get(alloc, "doc:m");
    defer alloc.free(split_doc);
    try std.testing.expectEqualStrings("m", split_doc);

    try std.testing.expectError(lmdb.Error.NotFound, child.get(alloc, "doc:a"));
    try std.testing.expectError(lmdb.Error.NotFound, child.get(alloc, "doc:b"));
}

test "docstore rewriteLeftInPlace keeps all left docs across reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});
    const path_z = try alloc.dupeZ(u8, path);
    defer alloc.free(path_z);

    var store = try DocStore.open(alloc, path_z, .{});
    defer store.close();

    try store.put("doc:a", "a");
    try store.put("doc:b", "b");
    try store.put("doc:c", "c");
    try store.put("doc:m", "m");
    try store.put("doc:z", "z");

    if (!(try store.rewriteLeftInPlace("doc:m"))) return;

    inline for ([_][]const u8{ "doc:a", "doc:b", "doc:c" }) |key| {
        const value = try store.get(alloc, key);
        defer alloc.free(value);
        try std.testing.expectEqualStrings(key[4..], value);
    }
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "doc:m"));
    try std.testing.expectError(lmdb.Error.NotFound, store.get(alloc, "doc:z"));
}

test "docstore backend adapters expose txn cursor and batch operations" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    {
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put("doc:a", "1");
        var cur = try write.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("doc:a", (try cur.start(.{})).?.key);
        try write.commit();
    }

    {
        var txn = try store.beginReadTxn();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("1", try read.get("doc:a"));
    }

    {
        var batch = try store.beginWriteBatch();
        errdefer batch.abort();
        var batch_adapter = batch.adapter();
        try batch_adapter.put("doc:b", "2");
        try std.testing.expectEqualStrings("2", try batch_adapter.get("doc:b"));
        try batch_adapter.commit();
    }

    const results = try store.scanPrefix(std.testing.allocator, "doc:");
    defer DocStore.freeResults(std.testing.allocator, results);
    try std.testing.expectEqual(@as(usize, 2), results.len);
}

test "docstore backend store opens concrete txn and batch handles" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    var backend = store.backendStore();
    try std.testing.expect(backend.capabilities().cursors);
    try std.testing.expectEqual(backend_types.WriteBatchMode.atomic, backend.capabilities().write_batches);

    {
        var txn = try backend.beginWrite();
        errdefer txn.abort();
        var write = txn.writeAdapter();
        try write.put("doc:x", "9");
        try write.commit();
    }

    {
        var txn = try backend.beginRead();
        defer txn.abort();
        var read = txn.readAdapter();
        try std.testing.expectEqualStrings("9", try read.get("doc:x"));
    }

    {
        var batch = try backend.beginBatch();
        errdefer batch.abort();
        var batch_adapter = batch.adapter();
        try batch_adapter.put("doc:y", "10");
        try batch_adapter.commit();
    }
}

test "docstore backend runtime erases store handles" {
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    var runtime = try backend_erased.storeFrom(std.testing.allocator, store.backendStore());
    defer runtime.deinit();
    try std.testing.expect(runtime.capabilities().cursors);

    {
        var txn = try runtime.beginWrite();
        try txn.put("doc:r", "11");
        try txn.commit();
    }

    {
        var txn = try runtime.beginRead();
        defer txn.abort();
        try std.testing.expectEqualStrings("11", try txn.get("doc:r"));
        var cur = try txn.openCursor();
        defer cur.close();
        try std.testing.expectEqualStrings("doc:r", (try cur.seekAtOrAfter("doc:r")).?.key);
    }
}

test "docstore lmdb replay rows use replay keyspace" {
    if (!supports_lmdb) return error.UnsupportedPlatform;

    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(std.testing.allocator, path, .{});
    defer store.close();

    try store.putBatchWithReplay(null, &.{
        .{ .key = "doc:a", .value = "A" },
    }, &.{}, .{
        .sequence = 1,
        .payload = "replay:1",
    });

    try std.testing.expectEqual(@as(u64, 1), store.lastReplaySequence(0));
    try std.testing.expectEqual(@as(u64, 2), store.nextReplaySequence(1));

    const entries = try store.iterateReplayFrom(std.testing.allocator, 1);
    defer {
        for (entries) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("replay:1", entries[0].payload);

    try store.truncateReplayUpTo(std.testing.allocator, 1);
    const remaining = try store.iterateReplayFrom(std.testing.allocator, 1);
    defer {
        for (remaining) |*entry| entry.deinit(std.testing.allocator);
        std.testing.allocator.free(remaining);
    }
    try std.testing.expectEqual(@as(usize, 0), remaining.len);
}

test "docstore indexes replay rows by hint and truncates them" {
    if (!supports_lmdb) return error.UnsupportedPlatform;

    const alloc = std.testing.allocator;
    var path_buf: [256]u8 = undefined;
    const path = tmpPath(&path_buf);
    defer cleanupTmp(path);

    var store = try DocStore.open(alloc, path, .{});
    defer store.close();

    const embedding_artifact_key = try internal_keys.embeddingArtifactKeyForDocumentAlloc(alloc, "doc:a", "dv_v1");
    defer alloc.free(embedding_artifact_key);
    const graph_artifact_key = try internal_keys.graphEdgeArtifactKeyAlloc(alloc, "doc:a", "graph_v1", "links", "doc:b");
    defer alloc.free(graph_artifact_key);
    const graph_asset_artifact_key = try internal_keys.artifactNamedPrefixAlloc(alloc, "doc:a", "asset", "relations_v1");
    defer alloc.free(graph_asset_artifact_key);
    const record = change_journal_mod.Record{
        .sequence = 7,
        .changed_doc_keys = &.{"doc:a"},
        .deleted_doc_keys = &.{"doc:deleted"},
        .overwritten_doc_keys = &.{"doc:old"},
        .changed_artifact_keys = &.{ embedding_artifact_key, graph_artifact_key, graph_asset_artifact_key },
        .target_hints = &.{ .dense_vector, .full_text, .graph },
    };
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);

    try store.putBatchWithReplay(null, &.{
        .{ .key = "doc:a", .value = "A" },
    }, &.{}, .{
        .sequence = 7,
        .payload = payload,
    });

    try std.testing.expect(try store.hasReplayEntries());
    try std.testing.expectEqual(DocStore.replay_index_available, store.replay_index_state.load(.monotonic));

    const all_entries = try store.iterateReplayFrom(alloc, 7);
    defer {
        for (all_entries) |*entry| entry.deinit(alloc);
        alloc.free(all_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), all_entries.len);
    try std.testing.expectEqualSlices(u8, payload, all_entries[0].payload);

    const dense_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .dense_vector);
    defer {
        for (dense_entries) |*entry| entry.deinit(alloc);
        alloc.free(dense_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), dense_entries.len);
    try std.testing.expectEqual(@as(u64, 7), dense_entries[0].sequence);
    var dense_record = try change_journal_mod.decodeRecord(alloc, dense_entries[0].payload);
    defer dense_record.deinit();
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.dense_vector, dense_record.record.target_hints[0]);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.deleted_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), dense_record.record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings(embedding_artifact_key, dense_record.record.changed_artifact_keys[0]);

    const full_text_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .full_text);
    defer {
        for (full_text_entries) |*entry| entry.deinit(alloc);
        alloc.free(full_text_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), full_text_entries.len);
    var full_text_record = try change_journal_mod.decodeRecord(alloc, full_text_entries[0].payload);
    defer full_text_record.deinit();
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.full_text, full_text_record.record.target_hints[0]);
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.deleted_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), full_text_record.record.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 0), full_text_record.record.changed_artifact_keys.len);

    const graph_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .graph);
    defer {
        for (graph_entries) |*entry| entry.deinit(alloc);
        alloc.free(graph_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), graph_entries.len);
    var graph_record = try change_journal_mod.decodeRecord(alloc, graph_entries[0].payload);
    defer graph_record.deinit();
    try std.testing.expectEqual(@as(usize, 1), graph_record.record.target_hints.len);
    try std.testing.expectEqual(change_journal_mod.TargetHint.graph, graph_record.record.target_hints[0]);
    try std.testing.expectEqual(@as(usize, 0), graph_record.record.changed_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 1), graph_record.record.deleted_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 0), graph_record.record.overwritten_doc_keys.len);
    try std.testing.expectEqual(@as(usize, 2), graph_record.record.changed_artifact_keys.len);
    try std.testing.expectEqualStrings(graph_artifact_key, graph_record.record.changed_artifact_keys[0]);
    try std.testing.expectEqualStrings(graph_asset_artifact_key, graph_record.record.changed_artifact_keys[1]);

    const sparse_entries = try store.iterateReplayEntriesFromHint(alloc, 7, .sparse_vector);
    defer {
        for (sparse_entries) |*entry| entry.deinit(alloc);
        alloc.free(sparse_entries);
    }
    try std.testing.expectEqual(@as(usize, 0), sparse_entries.len);

    try store.truncateReplayUpTo(alloc, 7);
    try std.testing.expect(try store.hasReplayEntries());
    try std.testing.expectEqual(DocStore.replay_index_available, store.replay_index_state.load(.monotonic));
    const after = try store.iterateReplayEntriesFromHint(alloc, 7, .dense_vector);
    defer {
        for (after) |*entry| entry.deinit(alloc);
        alloc.free(after);
    }
    try std.testing.expectEqual(@as(usize, 0), after.len);
}

test "docstore runtime lsm hint replay iteration does not clone mutable snapshots" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    var backend = try lsm_backend.Backend.open(alloc, path, .{
        .flush_threshold_bytes = 64 * 1024 * 1024,
    });
    defer backend.close();

    const runtime_store = try backend.runtimeStore(alloc, .{});
    var store = try DocStore.openRuntime(alloc, runtime_store);
    defer store.close();

    const record = change_journal_mod.Record{
        .sequence = 1,
        .changed_doc_keys = &.{"doc:a"},
        .target_hints = &.{.full_text},
    };
    const payload = try change_journal_mod.encodeRecord(alloc, record);
    defer alloc.free(payload);

    try store.putBatchWithReplay(null, &.{.{ .key = "doc:a", .value = "{}" }}, &.{}, .{
        .sequence = 1,
        .payload = payload,
    });

    const Context = struct {
        seen: usize = 0,
        fn handle(self: *@This(), sequence: u64, entry_payload: []const u8) !void {
            try std.testing.expectEqual(@as(u64, 1), sequence);
            try std.testing.expect(entry_payload.len > 0);
            self.seen += 1;
        }
    };
    var ctx = Context{};
    try store.forEachReplayEntryFromHint(1, .full_text, &ctx, Context.handle);
    try std.testing.expectEqual(@as(usize, 1), ctx.seen);
    try std.testing.expectEqual(@as(u64, 0), backend.snapshotMaintenanceStats().mutable_snapshot_clone_calls);
}

test "docstore runtime lsm persists replay rows across namespace reopen" {
    const alloc = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}", .{tmp.sub_path});

    {
        var backend = try lsm_backend.Backend.open(alloc, path, .{
            .flush_threshold = 1,
        });
        defer backend.close();

        const runtime_store = try backend.runtimeStore(alloc, .{ .name = "docs" });
        var store = try DocStore.openRuntime(alloc, runtime_store);
        defer store.close();

        try store.putBatchWithReplay(null, &.{.{ .key = "doc:a", .value = "{}" }}, &.{}, .{
            .sequence = 1,
            .payload = "replay:1",
        });
        try store.sync(true);

        const live_entries = try store.iterateReplayFrom(alloc, 1);
        defer {
            for (live_entries) |*entry| entry.deinit(alloc);
            alloc.free(live_entries);
        }
        try std.testing.expectEqual(@as(usize, 1), live_entries.len);
    }

    var reopened_backend = try lsm_backend.Backend.open(alloc, path, .{});
    defer reopened_backend.close();

    const reopened_runtime_store = try reopened_backend.runtimeStore(alloc, .{ .name = "docs" });
    var reopened = try DocStore.openRuntime(alloc, reopened_runtime_store);
    defer reopened.close();

    const entries = try reopened.iterateReplayFrom(alloc, 1);
    defer {
        for (entries) |*entry| entry.deinit(alloc);
        alloc.free(entries);
    }
    try std.testing.expectEqual(@as(usize, 1), entries.len);
    try std.testing.expectEqualStrings("replay:1", entries[0].payload);
}
