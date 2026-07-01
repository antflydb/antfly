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

//! Runtime document-store adapter over native `.aflite` document pages.

const std = @import("std");
const antfly_platform = @import("antfly_platform");
const platform_sync = antfly_platform.sync;
const platform_time = antfly_platform.time;
const backend_adapter = @import("../backend_adapter.zig");
const backend_erased = @import("../backend_erased.zig");
const backend_types = @import("../backend_types.zig");
const change_journal_mod = @import("../db/derived/change_journal.zig");
const internal_keys = @import("../internal_keys.zig");
const native = @import("native.zig");

const Allocator = std.mem.Allocator;

pub const OpenOptions = struct {
    read_only: bool = false,
    no_sync: bool = false,
};

pub const CreateOptions = struct {
    exclusive: bool = false,
    no_sync: bool = false,
};

pub const Store = struct {
    allocator: Allocator,
    file: native.NativeFile,
    read_only: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    writer_active: bool = false,

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !Store {
        return try openWithOptions(allocator, path, .{ .read_only = read_only });
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, opts: OpenOptions) !Store {
        const file = try native.NativeFile.openWithOptions(allocator, path, .{
            .read_only = opts.read_only,
            .no_sync = opts.no_sync,
        });
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = opts.read_only,
        };
    }

    pub fn create(allocator: Allocator, path: []const u8, exclusive: bool) !Store {
        return try createWithOptions(allocator, path, .{ .exclusive = exclusive });
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, opts: CreateOptions) !Store {
        const file = try native.NativeFile.createWithOptions(allocator, path, .{
            .exclusive = opts.exclusive,
            .no_sync = opts.no_sync,
        });
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = false,
        };
    }

    pub fn close(self: *Store) void {
        self.file.close();
        self.* = undefined;
    }

    pub fn backendStore(self: *Store) NativeBackendStore {
        return NativeBackendStore.init(self);
    }

    pub fn runtimeStore(self: *Store, allocator: Allocator) !backend_erased.Store {
        return try backend_erased.storeFrom(allocator, RuntimeStore{ .store = self });
    }

    pub fn vacuum(self: *Store) !native.VacuumReport {
        try self.reserveWriterSlot();
        defer self.releaseWriterSlot();

        lockStore(self);
        defer self.mutex.unlock();
        return try self.file.vacuum();
    }

    pub fn reserveWriterSlot(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        lockStore(self);
        defer self.mutex.unlock();
        if (self.writer_active) return error.FileBusy;
        self.writer_active = true;
    }

    pub fn reserveWriterSlotYielding(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        while (true) {
            lockStore(self);
            if (!self.writer_active) {
                self.writer_active = true;
                self.mutex.unlock();
                return;
            }
            self.mutex.unlock();
            platform_time.yieldBriefly();
        }
    }

    pub fn releaseWriterSlot(self: *Store) void {
        lockStore(self);
        defer self.mutex.unlock();
        self.writer_active = false;
    }

    const NativeBackendStore = backend_adapter.Store(Store, Txn, Txn, Txn, .{
        .capabilities = capabilities,
        .begin_read = beginRead,
        .begin_write = beginWrite,
        .begin_batch = beginBatch,
        .begin_batch_with_options = beginBatchWithOptions,
    });

    pub fn capabilities(_: *Store) backend_types.Capabilities {
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

    pub fn beginRead(self: *Store) !Txn {
        return try Txn.openRead(self);
    }

    pub fn beginWrite(self: *Store) !Txn {
        if (self.read_only) return error.ReadOnly;
        return try Txn.openWrite(self);
    }

    pub fn beginWriteYielding(self: *Store) !Txn {
        if (self.read_only) return error.ReadOnly;
        return try Txn.openWriteYielding(self);
    }

    pub fn beginBatch(self: *Store) !Txn {
        return try self.beginWrite();
    }

    pub fn beginBatchYielding(self: *Store) !Txn {
        return try self.beginWriteYielding();
    }

    pub fn beginBatchWithOptions(self: *Store, options: backend_types.BatchOptions) !Txn {
        _ = options;
        return try self.beginBatch();
    }

    pub fn beginBatchWithOptionsYielding(self: *Store, options: backend_types.BatchOptions) !Txn {
        _ = options;
        return try self.beginBatchYielding();
    }

    pub fn lastReplaySequence(self: *Store, fallback_last: u64) u64 {
        const next = self.nextReplaySequence(fallback_last + 1);
        return if (next <= 1) 0 else next - 1;
    }

    pub fn nextReplaySequence(self: *Store, fallback_next: u64) u64 {
        var read = self.beginRead() catch return fallback_next;
        defer read.abort();
        const raw = read.get(internal_keys.replay_meta_next_sequence_key[0..]) catch return fallback_next;
        if (raw.len != 8) return fallback_next;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    pub fn appendReplayOpaque(self: *Store, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var txn = try self.beginWrite();
        errdefer txn.abort();
        try txn.setReplayOpaque(sequence, payload);
        try txn.commit();
    }

    pub fn appendReplayOpaqueYielding(self: *Store, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var txn = try self.beginWriteYielding();
        errdefer txn.abort();
        try txn.setReplayOpaque(sequence, payload);
        try txn.commit();
    }

    pub fn iterateReplayFrom(self: *Store, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
        var entries = std.ArrayListUnmanaged(backend_types.ReplayEntry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        }

        const Context = struct {
            allocator: Allocator,
            entries: *std.ArrayListUnmanaged(backend_types.ReplayEntry),

            fn handle(ctx: *@This(), sequence: u64, payload: []const u8) !void {
                try ctx.entries.append(ctx.allocator, .{
                    .sequence = sequence,
                    .payload = try ctx.allocator.dupe(u8, payload),
                });
            }
        };
        const Adapter = struct {
            fn handle(ptr: *anyopaque, sequence: u64, payload: []const u8) !void {
                const ctx: *Context = @ptrCast(@alignCast(ptr));
                try Context.handle(ctx, sequence, payload);
            }
        };

        var ctx = Context{
            .allocator = alloc,
            .entries = &entries,
        };
        _ = try self.forEachReplayLaneFrom(internal_keys.replay_all_kind, from_sequence, 0, &ctx, Adapter.handle);
        return try entries.toOwnedSlice(alloc);
    }

    pub fn forEachReplayLaneFrom(
        self: *Store,
        kind_ordinal: u8,
        from_sequence: u64,
        max_entries: usize,
        callback_ctx: *anyopaque,
        callback: backend_erased.Store.ReplayCallback,
    ) !backend_types.ReplayLaneIterationStats {
        var read = try self.beginRead();
        defer read.abort();
        _ = read.get(internal_keys.replay_meta_init_key[0..]) catch return error.ReplayIndexUnavailable;

        var cursor = try read.openCursor();
        defer cursor.close();

        const lower = internal_keys.replayRangeLower(kind_ordinal, from_sequence);
        const upper = internal_keys.replayRangeUpper(kind_ordinal);
        cursor.setUpperBound(upper[0..]);

        var stats = backend_types.ReplayLaneIterationStats{ .scan_batches = 1 };
        var entry = cursor.seekAtOrAfter(lower[0..]) catch return stats;
        while (true) {
            if (std.mem.order(u8, entry.key, upper[0..]) != .lt) break;
            const sequence = internal_keys.parseReplayEntrySequence(entry.key, kind_ordinal) orelse break;
            try callback(callback_ctx, sequence, entry.value);
            stats.scanned_entries += 1;
            stats.matched_entries += 1;
            stats.last_sequence = sequence;
            if (max_entries != 0 and stats.matched_entries >= max_entries) break;
            entry = cursor.next() catch break;
        }
        return stats;
    }

    pub fn truncateReplayUpTo(self: *Store, alloc: Allocator, up_to_sequence: u64) !void {
        if (up_to_sequence == 0) return;

        var deletes = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(key);
            deletes.deinit(alloc);
        }

        {
            var read = try self.beginRead();
            defer read.abort();
            _ = read.get(internal_keys.replay_meta_init_key[0..]) catch return;

            try collectReplayDeletes(alloc, &read, internal_keys.replay_all_kind, up_to_sequence, &deletes);
            for (replay_hints) |hint| {
                try collectReplayDeletes(alloc, &read, replayHintOrdinal(hint), up_to_sequence, &deletes);
            }
        }

        if (deletes.items.len == 0) return;
        var write = try self.beginWrite();
        errdefer write.abort();
        for (deletes.items) |key| try write.delete(key);
        try write.commit();
    }

    pub fn truncateReplayUpToYielding(self: *Store, alloc: Allocator, up_to_sequence: u64) !void {
        if (up_to_sequence == 0) return;

        var deletes = std.ArrayListUnmanaged([]u8).empty;
        defer {
            for (deletes.items) |key| alloc.free(key);
            deletes.deinit(alloc);
        }

        {
            var read = try self.beginRead();
            defer read.abort();
            _ = read.get(internal_keys.replay_meta_init_key[0..]) catch return;

            try collectReplayDeletes(alloc, &read, internal_keys.replay_all_kind, up_to_sequence, &deletes);
            for (replay_hints) |hint| {
                try collectReplayDeletes(alloc, &read, replayHintOrdinal(hint), up_to_sequence, &deletes);
            }
        }

        if (deletes.items.len == 0) return;
        var write = try self.beginWriteYielding();
        errdefer write.abort();
        for (deletes.items) |key| try write.delete(key);
        try write.commit();
    }
};

const RuntimeStore = struct {
    store: *Store,

    pub fn capabilities(self: *RuntimeStore) backend_types.Capabilities {
        return Store.capabilities(self.store);
    }

    pub fn beginRead(self: *RuntimeStore) !Txn {
        return try self.store.beginRead();
    }

    pub fn beginWrite(self: *RuntimeStore) !Txn {
        return try self.store.beginWriteYielding();
    }

    pub fn beginBatch(self: *RuntimeStore) !Txn {
        return try self.store.beginBatchYielding();
    }

    pub fn beginBatchWithOptions(self: *RuntimeStore, options: backend_types.BatchOptions) !Txn {
        return try self.store.beginBatchWithOptionsYielding(options);
    }

    pub fn lastReplaySequence(self: *RuntimeStore, fallback_last: u64) u64 {
        return self.store.lastReplaySequence(fallback_last);
    }

    pub fn nextReplaySequence(self: *RuntimeStore, fallback_next: u64) u64 {
        return self.store.nextReplaySequence(fallback_next);
    }

    pub fn appendReplayOpaque(self: *RuntimeStore, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        return try self.store.appendReplayOpaqueYielding(alloc, sequence, payload);
    }

    pub fn iterateReplayFrom(self: *RuntimeStore, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
        return try self.store.iterateReplayFrom(alloc, from_sequence);
    }

    pub fn forEachReplayLaneFrom(
        self: *RuntimeStore,
        kind_ordinal: u8,
        from_sequence: u64,
        max_entries: usize,
        callback_ctx: *anyopaque,
        callback: backend_erased.Store.ReplayCallback,
    ) !backend_types.ReplayLaneIterationStats {
        return try self.store.forEachReplayLaneFrom(kind_ordinal, from_sequence, max_entries, callback_ctx, callback);
    }

    pub fn truncateReplayUpTo(self: *RuntimeStore, alloc: Allocator, up_to_sequence: u64) !void {
        return try self.store.truncateReplayUpToYielding(alloc, up_to_sequence);
    }
};

const PendingMutation = struct {
    key: []u8,
    value: ?[]u8 = null,
};

pub const Txn = struct {
    allocator: Allocator,
    store: ?*Store = null,
    docs: []native.OwnedDocument = &.{},
    pending: std.ArrayListUnmanaged(PendingMutation) = .empty,
    read_only: bool = true,
    writer_reserved: bool = false,

    pub fn openRead(store: *Store) !Txn {
        lockStore(store);
        defer store.mutex.unlock();
        return .{
            .allocator = store.allocator,
            .docs = try store.file.snapshotDocumentsAlloc(store.allocator),
            .read_only = true,
        };
    }

    pub fn openWrite(store: *Store) !Txn {
        try store.reserveWriterSlot();
        errdefer store.releaseWriterSlot();

        lockStore(store);
        defer store.mutex.unlock();

        return .{
            .allocator = store.allocator,
            .store = store,
            .docs = try store.file.snapshotDocumentsAlloc(store.allocator),
            .read_only = false,
            .writer_reserved = true,
        };
    }

    pub fn openWriteYielding(store: *Store) !Txn {
        try store.reserveWriterSlotYielding();
        errdefer store.releaseWriterSlot();

        lockStore(store);
        defer store.mutex.unlock();

        return .{
            .allocator = store.allocator,
            .store = store,
            .docs = try store.file.snapshotDocumentsAlloc(store.allocator),
            .read_only = false,
            .writer_reserved = true,
        };
    }

    pub fn abort(self: *Txn) void {
        self.freePending();
        native.NativeFile.freeSnapshotDocuments(self.allocator, self.docs);
        self.releaseWriterSlot();
        self.* = undefined;
    }

    pub fn commit(self: *Txn) !void {
        const store = self.store orelse return error.ReadOnly;
        const allocator = self.allocator;
        var mutations = try allocator.alloc(native.DocumentMutation, self.pending.items.len);
        defer allocator.free(mutations);
        for (self.pending.items, 0..) |pending, i| {
            mutations[i] = .{
                .key = pending.key,
                .value = pending.value orelse "",
                .is_delete = pending.value == null,
            };
        }

        lockStore(store);
        defer store.mutex.unlock();
        errdefer {
            if (self.writer_reserved) {
                store.writer_active = false;
                self.writer_reserved = false;
            }
        }
        try store.file.putDocumentBatch(mutations);
        if (self.writer_reserved) {
            store.writer_active = false;
            self.writer_reserved = false;
        }

        self.freePending();
        native.NativeFile.freeSnapshotDocuments(self.allocator, self.docs);
        self.* = undefined;
    }

    pub fn get(self: *Txn, key: []const u8) ![]const u8 {
        if (self.pending.items.len > 0) {
            var i = self.pending.items.len;
            while (i > 0) {
                i -= 1;
                const pending = self.pending.items[i];
                if (!std.mem.eql(u8, pending.key, key)) continue;
                return pending.value orelse error.NotFound;
            }
        }
        const idx = lowerBoundDocs(self.docs, key);
        if (idx >= self.docs.len or !std.mem.eql(u8, self.docs[idx].key, key)) return error.NotFound;
        return self.docs[idx].value;
    }

    pub fn put(self: *Txn, key: []const u8, value: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);
        try self.pending.append(self.allocator, .{ .key = owned_key, .value = owned_value });
    }

    pub fn delete(self: *Txn, key: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = try self.allocator.dupe(u8, key);
        errdefer self.allocator.free(owned_key);
        try self.pending.append(self.allocator, .{ .key = owned_key });
    }

    pub fn setReplayOpaque(self: *Txn, sequence: u64, payload: []const u8) !void {
        try writeReplayEntries(self.allocator, self, sequence, payload);
    }

    pub fn openCursor(self: *Txn) !Cursor {
        return .{ .txn = self };
    }

    fn freePending(self: *Txn) void {
        for (self.pending.items) |pending| {
            self.allocator.free(pending.key);
            if (pending.value) |value| self.allocator.free(value);
        }
        self.pending.deinit(self.allocator);
        self.pending = .empty;
    }

    fn releaseWriterSlot(self: *Txn) void {
        if (!self.writer_reserved) return;
        const store = self.store orelse return;
        store.releaseWriterSlot();
        self.writer_reserved = false;
    }
};

pub const Cursor = struct {
    txn: *Txn,
    current: ?usize = null,
    upper_bound: ?[]const u8 = null,

    pub fn close(_: *Cursor) void {}

    pub fn first(self: *Cursor) !backend_adapter.Entry {
        if (self.txn.docs.len == 0) return error.NotFound;
        self.current = 0;
        return self.entryAt(0);
    }

    pub fn last(self: *Cursor) !backend_adapter.Entry {
        if (self.txn.docs.len == 0) return error.NotFound;
        const idx = self.txn.docs.len - 1;
        self.current = idx;
        return self.entryAt(idx);
    }

    pub fn next(self: *Cursor) !backend_adapter.Entry {
        const current = self.current orelse return error.NotFound;
        const next_idx = current + 1;
        if (next_idx >= self.txn.docs.len) return error.NotFound;
        self.current = next_idx;
        return self.entryAt(next_idx);
    }

    pub fn prev(self: *Cursor) !backend_adapter.Entry {
        const current = self.current orelse return error.NotFound;
        if (current == 0) return error.NotFound;
        const prev_idx = current - 1;
        self.current = prev_idx;
        return self.entryAt(prev_idx);
    }

    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        const idx = lowerBoundDocs(self.txn.docs, key);
        if (idx >= self.txn.docs.len) return error.NotFound;
        self.current = idx;
        return self.entryAt(idx);
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        const idx = lowerBoundDocs(self.txn.docs, key);
        if (idx < self.txn.docs.len and std.mem.eql(u8, self.txn.docs[idx].key, key)) {
            self.current = idx;
            return self.entryAt(idx);
        }
        if (idx == 0) return error.NotFound;
        self.current = idx - 1;
        return self.entryAt(idx - 1);
    }

    pub fn setUpperBound(self: *Cursor, upper: ?[]const u8) void {
        self.upper_bound = upper;
    }

    fn entryAt(self: *Cursor, idx: usize) !backend_adapter.Entry {
        const doc = self.txn.docs[idx];
        if (self.upper_bound) |upper| {
            if (std.mem.order(u8, doc.key, upper) != .lt) return error.NotFound;
        }
        return .{ .key = doc.key, .value = doc.value };
    }
};

fn lowerBoundDocs(docs: []const native.OwnedDocument, key: []const u8) usize {
    var low: usize = 0;
    var high: usize = docs.len;
    while (low < high) {
        const mid = low + (high - low) / 2;
        if (std.mem.order(u8, docs[mid].key, key) == .lt) {
            low = mid + 1;
        } else {
            high = mid;
        }
    }
    return low;
}

const replay_hints = [_]change_journal_mod.TargetHint{
    .enrichment,
    .full_text,
    .dense_vector,
    .sparse_vector,
    .graph,
    .algebraic,
    .resolution,
    .promotion,
};

fn replayHintOrdinal(hint: change_journal_mod.TargetHint) u8 {
    return @intCast(@intFromEnum(hint));
}

fn encodeReplaySequence(sequence: u64) [8]u8 {
    var raw: [8]u8 = undefined;
    std.mem.writeInt(u64, &raw, sequence, .little);
    return raw;
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
        .full_text, .algebraic => {
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
        .graph, .resolution, .promotion => {
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
    const next_raw = encodeReplaySequence(sequence + 1);
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

fn collectReplayDeletes(
    alloc: Allocator,
    read: *Txn,
    kind_ordinal: u8,
    up_to_sequence: u64,
    deletes: *std.ArrayListUnmanaged([]u8),
) !void {
    var cursor = try read.openCursor();
    defer cursor.close();

    const lower = internal_keys.replayRangeLower(kind_ordinal, 0);
    const upper = internal_keys.replayRangeUpper(kind_ordinal);
    cursor.setUpperBound(upper[0..]);

    var entry = cursor.seekAtOrAfter(lower[0..]) catch return;
    while (true) {
        if (std.mem.order(u8, entry.key, upper[0..]) != .lt) break;
        const sequence = internal_keys.parseReplayEntrySequence(entry.key, kind_ordinal) orelse break;
        if (sequence >= up_to_sequence) break;
        try deletes.append(alloc, try alloc.dupe(u8, entry.key));
        entry = cursor.next() catch break;
    }
}

fn lockStore(store: *Store) void {
    platform_sync.lockYielding(&store.mutex);
}

fn testPath(allocator: Allocator, tmp: std.testing.TmpDir, name: []const u8) ![]u8 {
    return try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "lite native docstore runtime persists atomic batch" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore.aflite");
    defer allocator.free(path);

    {
        var store = try Store.create(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        var batch = try runtime.beginBatch();
        try batch.put("doc:b", "second");
        try batch.put("doc:a", "first");
        try batch.put("doc:b", "newer second");
        try batch.put("doc:c", "deleted");
        try batch.delete("doc:c");
        try batch.commit();
    }

    var reopened = try Store.open(allocator, path, true);
    defer reopened.close();

    var runtime = try reopened.runtimeStore(allocator);
    defer runtime.deinit();

    var read = try runtime.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("first", try read.get("doc:a"));
    try std.testing.expectEqualStrings("newer second", try read.get("doc:b"));
    try std.testing.expectError(error.NotFound, read.get("doc:c"));
}

test "lite native docstore runtime scans ordered snapshot" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-scan.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    var runtime = try store.runtimeStore(allocator);
    defer runtime.deinit();

    {
        var batch = try runtime.beginBatch();
        try batch.put("doc:b", "second");
        try batch.put("doc:a", "first");
        try batch.put("doc:c", "third");
        try batch.commit();
    }

    var read = try runtime.beginRead();
    defer read.abort();

    var cursor = try read.openCursor();
    defer cursor.close();
    const first = (try cursor.first()).?;
    try std.testing.expectEqualStrings("doc:a", first.key);
    const next = (try cursor.next()).?;
    try std.testing.expectEqualStrings("doc:b", next.key);
    const seek = (try cursor.seekAtOrAfter("doc:bb")).?;
    try std.testing.expectEqualStrings("doc:c", seek.key);
}

test "lite native docstore persists replay lanes across reopen and truncation" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-replay.aflite");
    defer allocator.free(path);

    const changed_doc_keys = [_][]const u8{"doc:a"};
    const deleted_doc_keys = [_][]const u8{"doc:gone"};
    const hints = [_]change_journal_mod.TargetHint{ .full_text, .dense_vector };
    const payload = try change_journal_mod.encodeRecord(allocator, .{
        .sequence = 1,
        .changed_doc_keys = changed_doc_keys[0..],
        .deleted_doc_keys = deleted_doc_keys[0..],
        .target_hints = hints[0..],
    });
    defer allocator.free(payload);

    {
        var store = try Store.create(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        try runtime.appendReplayOpaque(allocator, 1, payload);
        try std.testing.expectEqual(@as(u64, 1), runtime.lastReplaySequence(0));
        try std.testing.expectEqual(@as(u64, 2), runtime.nextReplaySequence(0));
    }

    {
        var store = try Store.open(allocator, path, true);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        const entries = try runtime.iterateReplayFrom(allocator, 1);
        defer {
            for (entries) |*entry| entry.deinit(allocator);
            allocator.free(entries);
        }
        try std.testing.expectEqual(@as(usize, 1), entries.len);
        try std.testing.expectEqual(@as(u64, 1), entries[0].sequence);
        try std.testing.expectEqualSlices(u8, payload, entries[0].payload);

        const LaneContext = struct {
            allocator: Allocator,
            expected_hint: change_journal_mod.TargetHint,
            count: usize = 0,

            fn handle(ctx: *@This(), sequence: u64, lane_payload: []const u8) !void {
                try std.testing.expectEqual(@as(u64, 1), sequence);
                var decoded = try change_journal_mod.decodeRecord(ctx.allocator, lane_payload);
                defer decoded.deinit();
                try std.testing.expectEqual(@as(usize, 1), decoded.record.target_hints.len);
                try std.testing.expectEqual(ctx.expected_hint, decoded.record.target_hints[0]);
                ctx.count += 1;
            }
        };

        var full_text_ctx = LaneContext{ .allocator = allocator, .expected_hint = .full_text };
        const full_text_stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.full_text), 1, 0, &full_text_ctx, LaneContext.handle);
        try std.testing.expectEqual(@as(usize, 1), full_text_ctx.count);
        try std.testing.expectEqual(@as(u64, 1), full_text_stats.last_sequence);

        var dense_ctx = LaneContext{ .allocator = allocator, .expected_hint = .dense_vector };
        const dense_stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.dense_vector), 1, 1, &dense_ctx, LaneContext.handle);
        try std.testing.expectEqual(@as(usize, 1), dense_ctx.count);
        try std.testing.expectEqual(@as(u64, 1), dense_stats.last_sequence);
    }

    {
        var store = try Store.open(allocator, path, false);
        defer store.close();

        var runtime = try store.runtimeStore(allocator);
        defer runtime.deinit();

        try runtime.truncateReplayUpTo(allocator, 2);

        const entries = try runtime.iterateReplayFrom(allocator, 1);
        defer {
            for (entries) |*entry| entry.deinit(allocator);
            allocator.free(entries);
        }
        try std.testing.expectEqual(@as(usize, 0), entries.len);

        const EmptyContext = struct {
            fn handle(_: *@This(), _: u64, _: []const u8) !void {
                return error.UnexpectedReplayRecord;
            }
        };
        var empty_ctx = EmptyContext{};
        const stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.full_text), 1, 0, &empty_ctx, EmptyContext.handle);
        try std.testing.expectEqual(@as(u64, 0), stats.matched_entries);
    }
}

test "lite native docstore co-writes original hint lane when record decode fails" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-replay-decode-failure.aflite");
    defer allocator.free(path);

    var payload: [15]u8 = undefined;
    @memcpy(payload[0..4], "CJ2\x00");
    std.mem.writeInt(u16, payload[4..6], 1, .little);
    std.mem.writeInt(u64, payload[6..14], 11, .little);
    payload[14] = change_journal_mod.singleHintMask(.full_text);
    try std.testing.expectEqual(payload[14], try change_journal_mod.encodedRecordHintMask(payload[0..]));
    if (change_journal_mod.decodeRecord(allocator, payload[0..])) |decoded| {
        var owned = decoded;
        owned.deinit();
        return error.ExpectedDecodeFailure;
    } else |_| {}

    var store = try Store.create(allocator, path, true);
    defer store.close();

    var runtime = try store.runtimeStore(allocator);
    defer runtime.deinit();

    try runtime.appendReplayOpaque(allocator, 11, payload[0..]);
    try std.testing.expectEqual(@as(u64, 11), runtime.lastReplaySequence(0));
    try std.testing.expectEqual(@as(u64, 12), runtime.nextReplaySequence(0));

    const all_entries = try runtime.iterateReplayFrom(allocator, 11);
    defer {
        for (all_entries) |*entry| entry.deinit(allocator);
        allocator.free(all_entries);
    }
    try std.testing.expectEqual(@as(usize, 1), all_entries.len);
    try std.testing.expectEqual(@as(u64, 11), all_entries[0].sequence);
    try std.testing.expectEqualSlices(u8, payload[0..], all_entries[0].payload);

    const LaneContext = struct {
        expected_payload: []const u8,
        count: usize = 0,

        fn handle(ctx: *@This(), sequence: u64, lane_payload: []const u8) !void {
            try std.testing.expectEqual(@as(u64, 11), sequence);
            try std.testing.expectEqualSlices(u8, ctx.expected_payload, lane_payload);
            ctx.count += 1;
        }
    };
    var full_text_ctx = LaneContext{ .expected_payload = payload[0..] };
    const full_text_stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.full_text), 11, 0, &full_text_ctx, LaneContext.handle);
    try std.testing.expectEqual(@as(usize, 1), full_text_ctx.count);
    try std.testing.expectEqual(@as(u64, 11), full_text_stats.last_sequence);

    const EmptyContext = struct {
        fn handle(_: *@This(), _: u64, _: []const u8) !void {
            return error.UnexpectedReplayRecord;
        }
    };
    var dense_ctx = EmptyContext{};
    const dense_stats = try runtime.forEachReplayLaneFrom(replayHintOrdinal(.dense_vector), 11, 0, &dense_ctx, EmptyContext.handle);
    try std.testing.expectEqual(@as(u64, 0), dense_stats.matched_entries);
}

test "lite native docstore reserves one writer until abort or commit" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-single-writer.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    var writer = try store.beginWrite();
    try writer.put("doc:a", "first");
    try std.testing.expectError(error.FileBusy, store.beginWrite());
    try std.testing.expectError(error.FileBusy, store.vacuum());

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectError(error.NotFound, read.get("doc:a"));

    writer.abort();

    var committed = try store.beginWrite();
    try committed.put("doc:a", "committed");
    try committed.commit();

    var next_writer = try store.beginWrite();
    defer next_writer.abort();
    try std.testing.expectEqualStrings("committed", try next_writer.get("doc:a"));
}
