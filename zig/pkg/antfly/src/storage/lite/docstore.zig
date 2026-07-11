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
const backend_adapter = @import("../backend_adapter.zig");
const backend_erased = @import("../backend_erased.zig");
const backend_types = @import("../backend_types.zig");
const change_journal_mod = @import("../db/derived/change_journal.zig");
const internal_keys = @import("../internal_keys.zig");
const native = @import("native.zig");
const resource_manager_mod = @import("../resource_manager.zig");

const Allocator = std.mem.Allocator;
const max_eager_applied_snapshot_docs: usize = 4096;

pub const OpenOptions = struct {
    read_only: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
};

pub const CreateOptions = struct {
    exclusive: bool = false,
    no_sync: bool = false,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,
};

const DocumentAllocation = struct {
    allocator: Allocator,
    refs: std.atomic.Value(usize) = .init(1),
    key: []u8,
    value: []u8,

    fn release(self: *DocumentAllocation) void {
        if (self.refs.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.allocator;
        allocator.free(self.key);
        allocator.free(self.value);
        allocator.destroy(self);
    }
};

const SharedDocument = struct {
    allocation: *DocumentAllocation,
    key: []const u8,
    value: []const u8,

    fn adopt(allocator: Allocator, doc: native.OwnedDocument) !SharedDocument {
        const allocation = try allocator.create(DocumentAllocation);
        allocation.* = .{ .allocator = allocator, .key = doc.key, .value = doc.value };
        return .{ .allocation = allocation, .key = allocation.key, .value = allocation.value };
    }

    fn create(allocator: Allocator, key: []const u8, value: []const u8) !SharedDocument {
        const owned_key = try allocator.dupe(u8, key);
        errdefer allocator.free(owned_key);
        const owned_value = try allocator.dupe(u8, value);
        errdefer allocator.free(owned_value);
        return try adopt(allocator, .{ .key = owned_key, .value = owned_value });
    }

    fn retain(self: SharedDocument) SharedDocument {
        _ = self.allocation.refs.fetchAdd(1, .monotonic);
        return self;
    }

    fn release(self: SharedDocument) void {
        self.allocation.release();
    }
};

/// Immutable, refcounted materialized view of the document store at one
/// checkpoint. The store caches the latest snapshot so transactions can share
/// it instead of re-walking the document page chain per transaction; open
/// transactions keep their snapshot alive after newer ones are published,
/// preserving the existing pinned-snapshot semantics.
const SharedSnapshot = struct {
    allocator: Allocator,
    refs: std.atomic.Value(usize),
    commit_sequence: u64,
    document_root_page: u64,
    docs: []SharedDocument,
    resource_reservation: ?resource_manager_mod.Reservation = null,

    fn create(
        allocator: Allocator,
        docs: []SharedDocument,
        commit_sequence: u64,
        document_root_page: u64,
        initial_refs: usize,
        resource_reservation: ?resource_manager_mod.Reservation,
    ) !*SharedSnapshot {
        const snap = try allocator.create(SharedSnapshot);
        snap.* = .{
            .allocator = allocator,
            .refs = .init(initial_refs),
            .commit_sequence = commit_sequence,
            .document_root_page = document_root_page,
            .docs = docs,
            .resource_reservation = resource_reservation,
        };
        return snap;
    }

    fn retain(self: *SharedSnapshot) void {
        _ = self.refs.fetchAdd(1, .monotonic);
    }

    fn release(self: *SharedSnapshot) void {
        if (self.refs.fetchSub(1, .acq_rel) == 1) {
            const allocator = self.allocator;
            if (self.resource_reservation) |*reservation| reservation.release();
            for (self.docs) |doc| doc.release();
            allocator.free(self.docs);
            allocator.destroy(self);
        }
    }
};

pub const Store = struct {
    allocator: Allocator,
    file: native.NativeFile,
    read_only: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    writer_mutex: std.Io.Mutex = .init,
    writer_ready: std.Io.Condition = .init,
    writer_active: bool = false,
    writer_ticketed: bool = false,
    next_writer_ticket: u64 = 0,
    serving_writer_ticket: u64 = 0,
    snapshot_caches: std.StringHashMapUnmanaged(*SharedSnapshot) = .empty,
    known_document_root_page: u64 = 0,
    resource_manager: ?*resource_manager_mod.ResourceManager = null,

    pub fn open(allocator: Allocator, path: []const u8, read_only: bool) !Store {
        return try openWithOptions(allocator, path, .{ .read_only = read_only });
    }

    pub fn openWithOptions(allocator: Allocator, path: []const u8, opts: OpenOptions) !Store {
        const file = try native.NativeFile.openWithOptions(allocator, path, .{
            .read_only = opts.read_only,
            .no_sync = opts.no_sync,
            .resource_manager = opts.resource_manager,
        });
        const checkpoint = file.activeCheckpoint();
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = opts.read_only,
            .resource_manager = opts.resource_manager,
            .known_document_root_page = checkpoint.document_root_page,
        };
    }

    pub fn create(allocator: Allocator, path: []const u8, exclusive: bool) !Store {
        return try createWithOptions(allocator, path, .{ .exclusive = exclusive });
    }

    pub fn createWithOptions(allocator: Allocator, path: []const u8, opts: CreateOptions) !Store {
        const file = try native.NativeFile.createWithOptions(allocator, path, .{
            .exclusive = opts.exclusive,
            .no_sync = opts.no_sync,
            .resource_manager = opts.resource_manager,
        });
        const checkpoint = file.activeCheckpoint();
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = false,
            .resource_manager = opts.resource_manager,
            .known_document_root_page = checkpoint.document_root_page,
        };
    }

    pub fn close(self: *Store) void {
        clearCachedSnapshotsLocked(self);
        self.snapshot_caches.deinit(self.allocator);
        self.file.close();
        self.* = undefined;
    }

    pub fn backendStore(self: *Store) NativeBackendStore {
        return NativeBackendStore.init(self);
    }

    pub fn runtimeStore(self: *Store, allocator: Allocator) !backend_erased.Store {
        return try backend_erased.storeFrom(allocator, RuntimeStore{ .store = self });
    }

    /// Returns a DB runtime store isolated under a caller-owned key prefix, or
    /// the embedded root when `prefix` is empty. Non-empty prefixes must remain
    /// alive for the erased store's lifetime and end in a zero byte so range
    /// scans have an unambiguous namespace boundary.
    pub fn runtimeStoreWithPrefix(self: *Store, allocator: Allocator, prefix: []const u8) !backend_erased.Store {
        if (prefix.len > 0 and prefix[prefix.len - 1] != 0) return error.InvalidArgument;
        return try backend_erased.storeFrom(allocator, RuntimeStore{ .store = self, .prefix = prefix });
    }

    pub fn vacuum(self: *Store) !native.VacuumReport {
        try self.reserveWriterSlot();
        defer self.releaseWriterSlot();

        lockStore(self);
        defer self.mutex.unlock();
        const report = try self.file.vacuum();
        clearCachedSnapshotsLocked(self);
        self.known_document_root_page = self.file.activeCheckpoint().document_root_page;
        return report;
    }

    pub fn reserveWriterSlot(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        const io = self.file.io_impl.io();
        self.writer_mutex.lockUncancelable(io);
        defer self.writer_mutex.unlock(io);
        if (self.writer_active or self.next_writer_ticket != self.serving_writer_ticket) return error.FileBusy;
        self.writer_active = true;
        self.writer_ticketed = false;
    }

    pub fn reserveWriterSlotYielding(self: *Store) !void {
        if (self.read_only) return error.ReadOnly;
        const io = self.file.io_impl.io();
        self.writer_mutex.lockUncancelable(io);
        defer self.writer_mutex.unlock(io);
        const ticket = self.next_writer_ticket;
        self.next_writer_ticket +%= 1;
        while (self.writer_active or ticket != self.serving_writer_ticket) {
            self.writer_ready.waitUncancelable(io, &self.writer_mutex);
        }
        self.writer_active = true;
        self.writer_ticketed = true;
    }

    pub fn releaseWriterSlot(self: *Store) void {
        const io = self.file.io_impl.io();
        self.writer_mutex.lockUncancelable(io);
        defer self.writer_mutex.unlock(io);
        std.debug.assert(self.writer_active);
        self.writer_active = false;
        if (self.writer_ticketed) self.serving_writer_ticket +%= 1;
        self.writer_ticketed = false;
        self.writer_ready.broadcast(io);
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
    prefix: []const u8 = "",

    pub fn capabilities(self: *RuntimeStore) backend_types.Capabilities {
        return Store.capabilities(self.store);
    }

    pub fn beginRead(self: *RuntimeStore) !Txn {
        return try Txn.openReadWithPrefix(self.store, self.prefix);
    }

    pub fn beginWrite(self: *RuntimeStore) !Txn {
        return try Txn.openWriteYieldingWithPrefix(self.store, self.prefix);
    }

    pub fn beginBatch(self: *RuntimeStore) !Txn {
        return try Txn.openWriteYieldingWithPrefix(self.store, self.prefix);
    }

    pub fn beginBatchWithOptions(self: *RuntimeStore, options: backend_types.BatchOptions) !Txn {
        _ = options;
        return try Txn.openWriteYieldingWithPrefix(self.store, self.prefix);
    }

    pub fn lastReplaySequence(self: *RuntimeStore, fallback_last: u64) u64 {
        const next = self.nextReplaySequence(fallback_last + 1);
        return if (next <= 1) 0 else next - 1;
    }

    pub fn nextReplaySequence(self: *RuntimeStore, fallback_next: u64) u64 {
        var read = self.beginRead() catch return fallback_next;
        defer read.abort();
        const raw = read.get(internal_keys.replay_meta_next_sequence_key[0..]) catch return fallback_next;
        if (raw.len != 8) return fallback_next;
        return std.mem.readInt(u64, raw[0..8], .little);
    }

    pub fn appendReplayOpaque(self: *RuntimeStore, alloc: Allocator, sequence: u64, payload: []const u8) !void {
        _ = alloc;
        var txn = try self.beginWrite();
        errdefer txn.abort();
        try txn.setReplayOpaque(sequence, payload);
        try txn.commit();
    }

    pub fn iterateReplayFrom(self: *RuntimeStore, alloc: Allocator, from_sequence: u64) ![]backend_types.ReplayEntry {
        var entries = std.ArrayListUnmanaged(backend_types.ReplayEntry).empty;
        errdefer {
            for (entries.items) |*entry| entry.deinit(alloc);
            entries.deinit(alloc);
        }
        const Context = struct {
            allocator: Allocator,
            entries: *std.ArrayListUnmanaged(backend_types.ReplayEntry),
            fn handle(ptr: *anyopaque, sequence: u64, payload: []const u8) !void {
                const ctx: *@This() = @ptrCast(@alignCast(ptr));
                try ctx.entries.append(ctx.allocator, .{
                    .sequence = sequence,
                    .payload = try ctx.allocator.dupe(u8, payload),
                });
            }
        };
        var ctx = Context{ .allocator = alloc, .entries = &entries };
        _ = try self.forEachReplayLaneFrom(internal_keys.replay_all_kind, from_sequence, 0, &ctx, Context.handle);
        return try entries.toOwnedSlice(alloc);
    }

    pub fn forEachReplayLaneFrom(
        self: *RuntimeStore,
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

    pub fn truncateReplayUpTo(self: *RuntimeStore, alloc: Allocator, up_to_sequence: u64) !void {
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
};

const PendingMutation = struct {
    key: []u8,
    value: ?[]u8 = null,
};

fn clearCachedSnapshotsLocked(store: *Store) void {
    var it = store.snapshot_caches.iterator();
    while (it.next()) |entry| {
        entry.value_ptr.*.release();
        store.allocator.free(entry.key_ptr.*);
    }
    store.snapshot_caches.clearRetainingCapacity();
}

fn removeCachedSnapshotLocked(store: *Store, prefix: []const u8) void {
    const removed = store.snapshot_caches.fetchRemove(prefix) orelse return;
    removed.value.release();
    store.allocator.free(removed.key);
}

fn installCachedSnapshotLocked(store: *Store, prefix: []const u8, snapshot: *SharedSnapshot) !void {
    removeCachedSnapshotLocked(store, prefix);
    const owned_prefix = try store.allocator.dupe(u8, prefix);
    errdefer store.allocator.free(owned_prefix);
    try store.snapshot_caches.put(store.allocator, owned_prefix, snapshot);
}

fn snapshotByteCost(docs: []const SharedDocument) u64 {
    var total: u64 = @sizeOf(SharedSnapshot) + @as(u64, @intCast(docs.len)) * @as(u64, @sizeOf(SharedDocument));
    for (docs) |doc| {
        total +|= @intCast(doc.key.len);
        total +|= @intCast(doc.value.len);
    }
    return total;
}

fn createCachedSnapshotLocked(
    store: *Store,
    docs: []SharedDocument,
    commit_sequence: u64,
    document_root_page: u64,
    initial_refs: usize,
) !?*SharedSnapshot {
    var reservation: ?resource_manager_mod.Reservation = null;
    if (store.resource_manager) |manager| {
        reservation = manager.reserve(.lite_docstore_snapshot_cache, snapshotByteCost(docs)) catch return null;
    }
    errdefer if (reservation) |*reserved| reserved.release();
    return try SharedSnapshot.create(store.allocator, docs, commit_sequence, document_root_page, initial_refs, reservation);
}

fn sharedDocumentsFromOwned(allocator: Allocator, owned: []native.OwnedDocument) ![]SharedDocument {
    const docs = try allocator.alloc(SharedDocument, owned.len);
    var initialized: usize = 0;
    errdefer {
        for (docs[0..initialized]) |doc| doc.release();
        allocator.free(docs);
    }
    for (owned, 0..) |doc, i| {
        docs[i] = try SharedDocument.adopt(allocator, doc);
        initialized += 1;
    }
    // Ownership of every key/value moved into DocumentAllocation objects.
    allocator.free(owned);
    return docs;
}

/// Returns a retained snapshot for the store's active checkpoint, reusing the
/// cached one when it is still current. Must be called with `store.mutex`
/// held.
fn acquireSnapshotLocked(store: *Store, prefix: []const u8) !*SharedSnapshot {
    if (retainCachedSnapshotLocked(store, prefix)) |snap| return snap;

    const checkpoint = store.file.activeCheckpoint();
    var owned_docs = try store.file.snapshotDocumentsWithPrefixAlloc(store.allocator, prefix);
    errdefer if (owned_docs.len > 0) native.NativeFile.freeSnapshotDocuments(store.allocator, owned_docs);
    const docs = try sharedDocumentsFromOwned(store.allocator, owned_docs);
    owned_docs = &.{};
    var docs_owned = true;
    errdefer if (docs_owned) {
        for (docs) |doc| doc.release();
        store.allocator.free(docs);
    };
    // Two refs: one owned by the store cache, one handed to the caller. A
    // failed reservation is a hard bound, not permission to retain the same
    // materialized table outside resource accounting.
    const snap = (try createCachedSnapshotLocked(store, docs, checkpoint.commit_sequence, checkpoint.document_root_page, 2)) orelse
        return error.LiteSnapshotBudgetExceeded;
    docs_owned = false;
    installCachedSnapshotLocked(store, prefix, snap) catch |err| {
        snap.release();
        snap.release();
        return err;
    };
    return snap;
}

fn retainCachedSnapshotLocked(store: *Store, prefix: []const u8) ?*SharedSnapshot {
    const checkpoint = store.file.activeCheckpoint();
    if (checkpoint.document_root_page != store.known_document_root_page) {
        // A document commit bypassed this Store. Its namespace is unknown, so
        // invalidate once rather than risk serving a stale logical view.
        clearCachedSnapshotsLocked(store);
        store.known_document_root_page = checkpoint.document_root_page;
    }
    if (store.snapshot_caches.get(prefix)) |snap| {
        snap.commit_sequence = checkpoint.commit_sequence;
        snap.document_root_page = checkpoint.document_root_page;
        snap.retain();
        return snap;
    }
    return null;
}

/// After a successful `putDocumentBatch`, publish the committing
/// transaction's mutations applied to its base snapshot as the store's cached
/// snapshot for the new checkpoint. The document chain cannot have moved
/// between the base snapshot and this commit because the committer held the
/// writer slot throughout. Failure just drops the cache; the next transaction
/// rebuilds from disk. Must be called with `store.mutex` held.
fn publishAppliedSnapshotLocked(store: *Store, prefix: []const u8, base: []const SharedDocument, pending: []const PendingMutation) void {
    const checkpoint = store.file.activeCheckpoint();
    store.known_document_root_page = checkpoint.document_root_page;
    // Valid non-empty prefixes end in NUL and are therefore mutually
    // disjoint. A scoped write only invalidates its exact cache plus the
    // optional unscoped view; an unscoped write can affect every namespace.
    if (prefix.len == 0) clearCachedSnapshotsLocked(store) else removeCachedSnapshotLocked(store, "");
    const applied = buildAppliedSnapshotDocs(store.allocator, base, pending) catch {
        removeCachedSnapshotLocked(store, prefix);
        return;
    };
    removeCachedSnapshotLocked(store, prefix);
    const maybe_snap = createCachedSnapshotLocked(
        store,
        applied,
        checkpoint.commit_sequence,
        checkpoint.document_root_page,
        1,
    ) catch {
        for (applied) |doc| doc.release();
        store.allocator.free(applied);
        return;
    };
    if (maybe_snap) |snap| {
        installCachedSnapshotLocked(store, prefix, snap) catch {
            snap.release();
        };
    } else {
        for (applied) |doc| doc.release();
        store.allocator.free(applied);
    }
}

fn invalidateCommittedSnapshotLocked(store: *Store, prefix: []const u8) void {
    store.known_document_root_page = store.file.activeCheckpoint().document_root_page;
    if (prefix.len == 0) {
        clearCachedSnapshotsLocked(store);
    } else {
        removeCachedSnapshotLocked(store, "");
        removeCachedSnapshotLocked(store, prefix);
    }
}

/// Builds the sorted post-commit document set from a base snapshot plus the
/// commit's pending mutations. Later mutations win over earlier ones for the
/// same key and deletes drop the key, matching both `Txn.get` overlay
/// semantics and what a fresh chain walk would materialize after
/// `putDocumentBatch` appended the same mutations in order.
fn buildAppliedSnapshotDocs(
    allocator: Allocator,
    base: []const SharedDocument,
    pending: []const PendingMutation,
) ![]SharedDocument {
    var updates = std.StringHashMapUnmanaged(?[]const u8).empty;
    defer updates.deinit(allocator);
    for (pending) |mutation| {
        try updates.put(allocator, mutation.key, mutation.value);
    }

    const Update = struct { key: []const u8, value: ?[]const u8 };
    const sorted_updates = try allocator.alloc(Update, updates.count());
    defer allocator.free(sorted_updates);
    var update_it = updates.iterator();
    var update_count: usize = 0;
    while (update_it.next()) |entry| : (update_count += 1) {
        sorted_updates[update_count] = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
    }
    std.mem.sort(Update, sorted_updates, {}, struct {
        fn lessThan(_: void, lhs: Update, rhs: Update) bool {
            return std.mem.order(u8, lhs.key, rhs.key) == .lt;
        }
    }.lessThan);

    var docs = std.ArrayListUnmanaged(SharedDocument).empty;
    errdefer {
        for (docs.items) |doc| doc.release();
        docs.deinit(allocator);
    }
    try docs.ensureTotalCapacity(allocator, base.len + updates.count());

    var base_index: usize = 0;
    var update_index: usize = 0;
    while (base_index < base.len or update_index < sorted_updates.len) {
        if (base_index == base.len) {
            const update = sorted_updates[update_index];
            if (update.value) |value| docs.appendAssumeCapacity(try SharedDocument.create(allocator, update.key, value));
            update_index += 1;
            continue;
        }
        if (update_index == sorted_updates.len) {
            docs.appendAssumeCapacity(base[base_index].retain());
            base_index += 1;
            continue;
        }
        const order = std.mem.order(u8, base[base_index].key, sorted_updates[update_index].key);
        switch (order) {
            .lt => {
                docs.appendAssumeCapacity(base[base_index].retain());
                base_index += 1;
            },
            .eq => {
                const update = sorted_updates[update_index];
                if (update.value) |value| docs.appendAssumeCapacity(try SharedDocument.create(allocator, update.key, value));
                base_index += 1;
                update_index += 1;
            },
            .gt => {
                const update = sorted_updates[update_index];
                if (update.value) |value| docs.appendAssumeCapacity(try SharedDocument.create(allocator, update.key, value));
                update_index += 1;
            },
        }
    }
    return try docs.toOwnedSlice(allocator);
}

pub const Txn = struct {
    allocator: Allocator,
    store: ?*Store = null,
    snapshot: ?*SharedSnapshot = null,
    docs: []SharedDocument = &.{},
    pending: std.ArrayListUnmanaged(PendingMutation) = .empty,
    read_only: bool = true,
    writer_reserved: bool = false,
    prefix: []const u8 = "",

    pub fn openRead(store: *Store) !Txn {
        return try openReadWithPrefix(store, "");
    }

    pub fn openReadWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        lockStore(store);
        defer store.mutex.unlock();
        const snapshot = try acquireSnapshotLocked(store, prefix);
        return .{
            .allocator = store.allocator,
            .snapshot = snapshot,
            .docs = snapshot.docs,
            .read_only = true,
            .prefix = prefix,
        };
    }

    pub fn openWrite(store: *Store) !Txn {
        return try openWriteWithPrefix(store, "");
    }

    pub fn openWriteWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        try store.reserveWriterSlot();
        errdefer store.releaseWriterSlot();

        lockStore(store);
        defer store.mutex.unlock();

        const snapshot = retainCachedSnapshotLocked(store, prefix);
        return .{
            .allocator = store.allocator,
            .store = store,
            .snapshot = snapshot,
            .docs = if (snapshot) |snap| snap.docs else &.{},
            .read_only = false,
            .writer_reserved = true,
            .prefix = prefix,
        };
    }

    pub fn openWriteYielding(store: *Store) !Txn {
        return try openWriteYieldingWithPrefix(store, "");
    }

    pub fn openWriteYieldingWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        try store.reserveWriterSlotYielding();
        errdefer store.releaseWriterSlot();

        lockStore(store);
        defer store.mutex.unlock();

        const snapshot = retainCachedSnapshotLocked(store, prefix);
        return .{
            .allocator = store.allocator,
            .store = store,
            .snapshot = snapshot,
            .docs = if (snapshot) |snap| snap.docs else &.{},
            .read_only = false,
            .writer_reserved = true,
            .prefix = prefix,
        };
    }

    pub fn abort(self: *Txn) void {
        self.freePending();
        if (self.snapshot) |snap| snap.release();
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
                store.releaseWriterSlot();
                self.writer_reserved = false;
            }
        }
        try store.file.putDocumentBatch(mutations);
        // An empty batch publishes no new checkpoint, so the existing cached
        // snapshot is still current.
        if (self.pending.items.len > 0 and self.snapshot != null and self.docs.len <= max_eager_applied_snapshot_docs) {
            publishAppliedSnapshotLocked(store, self.prefix, self.docs, self.pending.items);
        } else if (self.pending.items.len > 0) {
            invalidateCommittedSnapshotLocked(store, self.prefix);
        }
        if (self.writer_reserved) {
            store.releaseWriterSlot();
            self.writer_reserved = false;
        }

        self.freePending();
        if (self.snapshot) |snap| snap.release();
        self.* = undefined;
    }

    pub fn get(self: *Txn, key: []const u8) ![]const u8 {
        const lookup_key = try self.prefixedKey(key);
        defer if (self.prefix.len > 0) self.allocator.free(lookup_key);
        if (self.pending.items.len > 0) {
            var i = self.pending.items.len;
            while (i > 0) {
                i -= 1;
                const pending = self.pending.items[i];
                if (!std.mem.eql(u8, pending.key, lookup_key)) continue;
                return pending.value orelse error.NotFound;
            }
        }
        try self.ensureSnapshot();
        const idx = lowerBoundDocs(self.docs, lookup_key);
        if (idx >= self.docs.len or !std.mem.eql(u8, self.docs[idx].key, lookup_key)) return error.NotFound;
        return self.docs[idx].value;
    }

    pub fn put(self: *Txn, key: []const u8, value: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = if (self.prefix.len == 0)
            try self.allocator.dupe(u8, key)
        else
            try std.mem.concat(self.allocator, u8, &.{ self.prefix, key });
        errdefer self.allocator.free(owned_key);
        const owned_value = try self.allocator.dupe(u8, value);
        errdefer self.allocator.free(owned_value);
        try self.pending.append(self.allocator, .{ .key = owned_key, .value = owned_value });
    }

    pub fn delete(self: *Txn, key: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = if (self.prefix.len == 0)
            try self.allocator.dupe(u8, key)
        else
            try std.mem.concat(self.allocator, u8, &.{ self.prefix, key });
        errdefer self.allocator.free(owned_key);
        try self.pending.append(self.allocator, .{ .key = owned_key });
    }

    pub fn setReplayOpaque(self: *Txn, sequence: u64, payload: []const u8) !void {
        try writeReplayEntries(self.allocator, self, sequence, payload);
    }

    pub fn openCursor(self: *Txn) !Cursor {
        try self.ensureSnapshot();
        return .{ .txn = self };
    }

    fn ensureSnapshot(self: *Txn) !void {
        if (self.snapshot != null) return;
        const store = self.store orelse return error.InvalidTransactionState;
        lockStore(store);
        defer store.mutex.unlock();
        const snapshot = try acquireSnapshotLocked(store, self.prefix);
        self.snapshot = snapshot;
        self.docs = snapshot.docs;
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

    fn prefixedKey(self: *Txn, key: []const u8) ![]const u8 {
        if (self.prefix.len == 0) return key;
        return try std.mem.concat(self.allocator, u8, &.{ self.prefix, key });
    }
};

fn validatePrefix(prefix: []const u8) !void {
    if (prefix.len > 0 and prefix[prefix.len - 1] != 0) return error.InvalidArgument;
}

pub const Cursor = struct {
    txn: *Txn,
    current: ?usize = null,
    upper_bound: ?[]const u8 = null,

    pub fn close(_: *Cursor) void {}

    pub fn first(self: *Cursor) !backend_adapter.Entry {
        const idx = if (self.txn.prefix.len == 0) 0 else lowerBoundDocs(self.txn.docs, self.txn.prefix);
        if (idx >= self.txn.docs.len) return error.NotFound;
        self.current = idx;
        return self.entryAt(idx);
    }

    pub fn last(self: *Cursor) !backend_adapter.Entry {
        if (self.txn.docs.len == 0) return error.NotFound;
        const idx = if (self.txn.prefix.len == 0)
            self.txn.docs.len - 1
        else blk: {
            const upper = try self.namespaceUpperBound();
            defer self.txn.allocator.free(upper);
            const end = lowerBoundDocs(self.txn.docs, upper);
            if (end == 0) return error.NotFound;
            break :blk end - 1;
        };
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
        const lookup_key = try self.txn.prefixedKey(key);
        defer if (self.txn.prefix.len > 0) self.txn.allocator.free(lookup_key);
        const idx = lowerBoundDocs(self.txn.docs, lookup_key);
        if (idx >= self.txn.docs.len) return error.NotFound;
        self.current = idx;
        return self.entryAt(idx);
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        const lookup_key = try self.txn.prefixedKey(key);
        defer if (self.txn.prefix.len > 0) self.txn.allocator.free(lookup_key);
        const idx = lowerBoundDocs(self.txn.docs, lookup_key);
        if (idx < self.txn.docs.len and std.mem.eql(u8, self.txn.docs[idx].key, lookup_key)) {
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
        if (!std.mem.startsWith(u8, doc.key, self.txn.prefix)) return error.NotFound;
        const key = doc.key[self.txn.prefix.len..];
        if (self.upper_bound) |upper| {
            if (std.mem.order(u8, key, upper) != .lt) return error.NotFound;
        }
        return .{ .key = key, .value = doc.value };
    }

    fn namespaceUpperBound(self: *Cursor) ![]u8 {
        const upper = try self.txn.allocator.dupe(u8, self.txn.prefix);
        std.debug.assert(upper.len > 0 and upper[upper.len - 1] == 0);
        upper[upper.len - 1] = 1;
        return upper;
    }
};

fn lowerBoundDocs(docs: []const SharedDocument, key: []const u8) usize {
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

test "lite native docstore prefixed runtimes isolate keys cursors and replay" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-prefixed.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };
    var runtime_a = try store.runtimeStoreWithPrefix(allocator, &prefix_a);
    defer runtime_a.deinit();
    var runtime_b = try store.runtimeStoreWithPrefix(allocator, &prefix_b);
    defer runtime_b.deinit();

    var write_a = try runtime_a.beginBatch();
    try write_a.put("doc:same", "a");
    try write_a.put("doc:only-a", "a-only");
    try write_a.commit();
    var write_b = try runtime_b.beginBatch();
    try write_b.put("doc:same", "b");
    try write_b.commit();

    var read_a = try runtime_a.beginRead();
    defer read_a.abort();
    try std.testing.expectEqualStrings("a", try read_a.get("doc:same"));
    var cursor_a = try read_a.openCursor();
    defer cursor_a.close();
    try std.testing.expectEqualStrings("doc:only-a", (try cursor_a.first()).?.key);
    try std.testing.expectEqualStrings("doc:same", (try cursor_a.next()).?.key);
    try std.testing.expect((try cursor_a.next()) == null);

    var read_b = try runtime_b.beginRead();
    defer read_b.abort();
    try std.testing.expectEqualStrings("b", try read_b.get("doc:same"));
    try std.testing.expectError(error.NotFound, read_b.get("doc:only-a"));

    try runtime_a.appendReplayOpaque(allocator, 7, "a-replay");
    try runtime_b.appendReplayOpaque(allocator, 2, "b-replay");
    try std.testing.expectEqual(@as(u64, 8), runtime_a.nextReplaySequence(0));
    try std.testing.expectEqual(@as(u64, 3), runtime_b.nextReplaySequence(0));
}

test "lite native docstore keeps disjoint namespace snapshots bounded and hot" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-namespace-cache.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    const prefix_a = [_]u8{ 't', 'a', 0 };
    const prefix_b = [_]u8{ 't', 'b', 0 };

    var write_a = try Txn.openWriteYieldingWithPrefix(&store, &prefix_a);
    try write_a.put("doc:a", "a1");
    try write_a.commit();
    var write_b = try Txn.openWriteYieldingWithPrefix(&store, &prefix_b);
    try write_b.put("doc:b", "b1");
    try write_b.commit();

    var read_b = try Txn.openReadWithPrefix(&store, &prefix_b);
    const cached_b = read_b.snapshot.?;
    try std.testing.expectEqual(@as(usize, 1), cached_b.docs.len);
    read_b.abort();

    write_a = try Txn.openWriteYieldingWithPrefix(&store, &prefix_a);
    try write_a.put("doc:a", "a2");
    try write_a.commit();

    read_b = try Txn.openReadWithPrefix(&store, &prefix_b);
    defer read_b.abort();
    try std.testing.expect(read_b.snapshot.? == cached_b);
    try std.testing.expectEqual(@as(usize, 1), read_b.docs.len);
    try std.testing.expectEqualStrings("b1", try read_b.get("doc:b"));
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

fn expectCachedSnapshotMatchesDiskRebuild(store: *Store) !void {
    const allocator = std.testing.allocator;
    // Writes deliberately avoid materializing a table on a cold cache. Open a
    // reader to exercise and validate the cached snapshot path explicitly.
    var read = try store.beginRead();
    defer read.abort();
    const cached = store.snapshot_caches.get("") orelse return error.TestUnexpectedResult;

    const checkpoint = store.file.activeCheckpoint();
    try std.testing.expectEqual(checkpoint.commit_sequence, cached.commit_sequence);
    try std.testing.expectEqual(checkpoint.document_root_page, cached.document_root_page);

    const rebuilt = try store.file.snapshotDocumentsAlloc(allocator);
    defer native.NativeFile.freeSnapshotDocuments(allocator, rebuilt);

    try std.testing.expectEqual(rebuilt.len, cached.docs.len);
    for (rebuilt, cached.docs) |expected, actual| {
        try std.testing.expectEqualStrings(expected.key, actual.key);
        try std.testing.expectEqualStrings(expected.value, actual.value);
    }
}

test "lite native docstore applied snapshot matches disk rebuild across mixed commits" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-applied-snapshot.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    {
        var batch = try store.beginWrite();
        try batch.put("doc:b", "b1");
        try batch.put("doc:a", "a1");
        try batch.put("doc:c", "c1");
        try batch.put("doc:b", "b2-last-wins");
        try batch.commit();
    }
    try expectCachedSnapshotMatchesDiskRebuild(&store);

    {
        var batch = try store.beginWrite();
        try batch.delete("doc:c");
        try batch.delete("doc:never-existed");
        try batch.put("doc:d", "d1");
        try batch.put("doc:a", "a2");
        try batch.commit();
    }
    try expectCachedSnapshotMatchesDiskRebuild(&store);

    {
        // Put-then-delete and delete-then-put of the same key in one batch.
        var batch = try store.beginWrite();
        try batch.put("doc:e", "e1");
        try batch.delete("doc:e");
        try batch.delete("doc:d");
        try batch.put("doc:d", "d2-resurrected");
        try batch.commit();
    }
    try expectCachedSnapshotMatchesDiskRebuild(&store);

    // A large value that spills to external value pages.
    {
        const big = try allocator.alloc(u8, 3 * native.default_page_size);
        defer allocator.free(big);
        @memset(big, 'x');
        var batch = try store.beginWrite();
        try batch.put("doc:big", big);
        try batch.commit();
    }
    try expectCachedSnapshotMatchesDiskRebuild(&store);

    // Empty commit publishes nothing and keeps the cache current.
    {
        var batch = try store.beginWrite();
        try batch.commit();
    }
    try expectCachedSnapshotMatchesDiskRebuild(&store);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("a2", try read.get("doc:a"));
    try std.testing.expectEqualStrings("b2-last-wins", try read.get("doc:b"));
    try std.testing.expectError(error.NotFound, read.get("doc:c"));
    try std.testing.expectEqualStrings("d2-resurrected", try read.get("doc:d"));
    try std.testing.expectError(error.NotFound, read.get("doc:e"));
}

test "lite native docstore snapshots share unchanged document payloads across commits" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-shared-payloads.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    {
        var batch = try store.beginWrite();
        errdefer batch.abort();
        try batch.put("doc:a", "a-value-that-must-not-be-copied");
        try batch.put("doc:b", "b-v1");
        try batch.commit();
    }
    {
        var read = try store.beginRead();
        read.abort();
    }
    const before = store.snapshot_caches.get("").?;
    const a_index = lowerBoundDocs(before.docs, "doc:a");
    const a_allocation = before.docs[a_index].allocation;
    {
        var batch = try store.beginWrite();
        errdefer batch.abort();
        try batch.put("doc:b", "b-v2");
        try batch.commit();
    }
    const after = store.snapshot_caches.get("").?;
    const after_a_index = lowerBoundDocs(after.docs, "doc:a");
    try std.testing.expect(after.docs[after_a_index].allocation == a_allocation);
    try std.testing.expectEqualStrings("a-value-that-must-not-be-copied", after.docs[after_a_index].value);
}

test "lite native docstore read transactions pin their snapshot across commits" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-pinned-snapshot.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    {
        var batch = try store.beginWrite();
        try batch.put("doc:pin", "v1");
        try batch.commit();
    }

    var pinned = try store.beginRead();
    defer pinned.abort();
    try std.testing.expectEqualStrings("v1", try pinned.get("doc:pin"));

    {
        var batch = try store.beginWrite();
        try batch.put("doc:pin", "v2");
        try batch.put("doc:new", "n1");
        try batch.commit();
    }

    // The pinned reader still sees its snapshot; a fresh reader sees the
    // committed state.
    try std.testing.expectEqualStrings("v1", try pinned.get("doc:pin"));
    try std.testing.expectError(error.NotFound, pinned.get("doc:new"));

    var fresh = try store.beginRead();
    defer fresh.abort();
    try std.testing.expectEqualStrings("v2", try fresh.get("doc:pin"));
    try std.testing.expectEqualStrings("n1", try fresh.get("doc:new"));
}

test "lite native docstore snapshot cache survives out-of-band catalog commits and vacuum" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-cache-oob.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();

    {
        var batch = try store.beginWrite();
        try batch.put("doc:oob", "v1");
        try batch.commit();
    }

    // A catalog commit bumps the checkpoint without touching documents; the
    // next read must key-miss, rebuild, and still see identical content.
    try store.file.putCatalogRecord("catalog:key", "catalog-value");
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("v1", try read.get("doc:oob"));
    }
    try expectCachedSnapshotMatchesDiskRebuild(&store);

    // Update churn then vacuum: the file is rewritten in place and the cache
    // key changes with the vacuum checkpoint.
    var round: usize = 0;
    while (round < 10) : (round += 1) {
        var value_buf: [32]u8 = undefined;
        const value = try std.fmt.bufPrint(&value_buf, "churn-{d}", .{round});
        var batch = try store.beginWrite();
        try batch.put("doc:oob", value);
        try batch.commit();
    }
    _ = try store.vacuum();
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("churn-9", try read.get("doc:oob"));
    }
    try expectCachedSnapshotMatchesDiskRebuild(&store);
}

test "lite native docstore snapshot cache reports usage to resource manager" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-snapshot-resource.aflite");
    defer allocator.free(path);

    var manager = resource_manager_mod.ResourceManager.init(.{});
    var store = try Store.createWithOptions(allocator, path, .{
        .no_sync = true,
        .resource_manager = &manager,
    });

    {
        var batch = try store.beginWrite();
        try batch.put("doc:resource", "tracked");
        try batch.commit();
    }
    {
        var read = try store.beginRead();
        read.abort();
    }
    try std.testing.expect(manager.sliceStats(.lite_docstore_snapshot_cache).used_bytes > 0);

    _ = try store.vacuum();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_docstore_snapshot_cache).used_bytes);

    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("tracked", try read.get("doc:resource"));
        try std.testing.expect(manager.sliceStats(.lite_docstore_snapshot_cache).used_bytes > 0);
    }

    store.close();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lite_docstore_snapshot_cache).used_bytes);
}

test "lite native docstore cold writes stay lazy and large cached tables invalidate in constant time" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-lazy-writes.aflite");
    defer allocator.free(path);

    var store = try Store.createWithOptions(allocator, path, .{ .no_sync = true });
    defer store.close();

    var seed = try store.beginWrite();
    try std.testing.expect(seed.snapshot == null);
    var key_buffer: [32]u8 = undefined;
    var i: usize = 0;
    while (i < max_eager_applied_snapshot_docs + 1) : (i += 1) {
        const key = try std.fmt.bufPrint(&key_buffer, "doc:{d:0>5}", .{i});
        try seed.put(key, "v1");
    }
    try seed.commit();
    try std.testing.expectEqual(@as(usize, 0), store.snapshot_caches.count());

    // A reader explicitly pays for and caches the table snapshot.
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("v1", try read.get("doc:00000"));
    }
    try std.testing.expectEqual(@as(usize, 1), store.snapshot_caches.count());

    // Retaining the cache is O(1); publishing a small write against a large
    // table drops it instead of rebuilding every live document.
    var update = try store.beginWrite();
    try std.testing.expect(update.snapshot != null);
    try std.testing.expectEqual(max_eager_applied_snapshot_docs + 1, update.docs.len);
    try update.put("doc:00000", "v2");
    try update.commit();
    try std.testing.expectEqual(@as(usize, 0), store.snapshot_caches.count());

    var verify = try store.beginRead();
    defer verify.abort();
    try std.testing.expectEqualStrings("v2", try verify.get("doc:00000"));
}

test "lite native docstore snapshot budget rejects materialization instead of bypassing governor" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-snapshot-budget.aflite");
    defer allocator.free(path);

    var budgets = resource_manager_mod.Options.defaultBudgets();
    budgets[@intFromEnum(resource_manager_mod.Slice.lite_docstore_snapshot_cache)] = .{
        .soft_limit_bytes = 1,
        .hard_limit_bytes = 1,
    };
    var manager = resource_manager_mod.ResourceManager.init(.{ .budgets = budgets });
    var store = try Store.createWithOptions(allocator, path, .{
        .no_sync = true,
        .resource_manager = &manager,
    });
    defer store.close();

    {
        var batch = try store.beginWrite();
        try batch.put("doc:budget", "uncached");
        try batch.commit();
    }
    try std.testing.expectEqual(@as(usize, 0), store.snapshot_caches.count());

    try std.testing.expectError(error.LiteSnapshotBudgetExceeded, store.beginRead());
    try std.testing.expectEqual(@as(usize, 0), store.snapshot_caches.count());

    const stats = manager.sliceStats(.lite_docstore_snapshot_cache);
    try std.testing.expectEqual(@as(u64, 0), stats.used_bytes);
    try std.testing.expect(stats.hard_limit_rejections > 0);
}
