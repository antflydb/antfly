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
const bounded_cursor_test_documents: usize = 512;

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

pub const Store = struct {
    allocator: Allocator,
    file: native.NativeFile,
    read_only: bool = false,
    mutex: std.atomic.Mutex = .unlocked,
    writer_mutex: std.Io.Mutex = .init,
    writer_ready: std.Io.Condition = .init,
    generation_lock: std.Io.RwLock = .init,
    writer_active: bool = false,
    writer_ticketed: bool = false,
    next_writer_ticket: u64 = 0,
    serving_writer_ticket: u64 = 0,
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
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = opts.read_only,
            .resource_manager = opts.resource_manager,
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
        return .{
            .allocator = allocator,
            .file = file,
            .read_only = false,
            .resource_manager = opts.resource_manager,
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

    /// Returns a DB runtime store isolated under a caller-owned key prefix, or
    /// the embedded root when `prefix` is empty. Non-empty prefixes must remain
    /// alive for the erased store's lifetime and end in a zero byte so range
    /// scans have an unambiguous namespace boundary.
    pub fn runtimeStoreWithPrefix(self: *Store, allocator: Allocator, prefix: []const u8) !backend_erased.Store {
        if (prefix.len > 0 and prefix[prefix.len - 1] != 0) return error.InvalidArgument;
        return try backend_erased.storeFrom(allocator, RuntimeStore{ .store = self, .prefix = prefix });
    }

    pub fn vacuum(self: *Store) !native.VacuumReport {
        return try self.vacuumWithCancel(null);
    }

    pub fn vacuumWithCancel(self: *Store, cancel: ?*const @import("../maintenance.zig").CancelToken) !native.VacuumReport {
        try self.reserveWriterSlot();
        defer self.releaseWriterSlot();

        const io = self.file.io_impl.io();
        self.generation_lock.lockUncancelable(io);
        defer self.generation_lock.unlock(io);

        lockStore(self);
        defer self.mutex.unlock();
        return try self.file.vacuumWithCancel(cancel);
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

pub const Txn = struct {
    allocator: Allocator,
    store: ?*Store = null,
    pending: std.ArrayListUnmanaged(PendingMutation) = .empty,
    pending_generation: u64 = 0,
    read_only: bool = true,
    writer_reserved: bool = false,
    prefix: []const u8 = "",
    owned_reads: std.ArrayListUnmanaged([]u8) = .empty,
    generation_read_locked: bool = false,
    checkpoint: native.CheckpointSlot = .{},

    pub fn openRead(store: *Store) !Txn {
        return try openReadWithPrefix(store, "");
    }

    pub fn openReadWithPrefix(store: *Store, prefix: []const u8) !Txn {
        try validatePrefix(prefix);
        const io = store.file.io_impl.io();
        store.generation_lock.lockSharedUncancelable(io);
        errdefer store.generation_lock.unlockShared(io);
        lockStore(store);
        defer store.mutex.unlock();
        const checkpoint = store.file.activeCheckpoint();
        return .{
            .allocator = store.allocator,
            .store = store,
            .read_only = true,
            .prefix = prefix,
            .generation_read_locked = true,
            .checkpoint = checkpoint,
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

        const checkpoint = store.file.activeCheckpoint();
        return .{
            .allocator = store.allocator,
            .store = store,
            .read_only = false,
            .writer_reserved = true,
            .prefix = prefix,
            .checkpoint = checkpoint,
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

        const checkpoint = store.file.activeCheckpoint();
        return .{
            .allocator = store.allocator,
            .store = store,
            .read_only = false,
            .writer_reserved = true,
            .prefix = prefix,
            .checkpoint = checkpoint,
        };
    }

    pub fn abort(self: *Txn) void {
        self.freePending();
        self.freeOwnedReads();
        self.releaseGenerationReadLock();
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
        if (self.writer_reserved) {
            store.releaseWriterSlot();
            self.writer_reserved = false;
        }

        self.freePending();
        self.freeOwnedReads();
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
        const store = self.store orelse return error.InvalidTransactionState;
        const value = try store.file.getDocumentAtCheckpointAlloc(self.allocator, self.checkpoint, lookup_key);
        const owned = value orelse return error.NotFound;
        errdefer self.allocator.free(owned);
        try self.owned_reads.append(self.allocator, owned);
        return owned;
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
        self.pending_generation +%= 1;
    }

    pub fn delete(self: *Txn, key: []const u8) !void {
        if (self.read_only) return error.ReadOnly;
        const owned_key = if (self.prefix.len == 0)
            try self.allocator.dupe(u8, key)
        else
            try std.mem.concat(self.allocator, u8, &.{ self.prefix, key });
        errdefer self.allocator.free(owned_key);
        try self.pending.append(self.allocator, .{ .key = owned_key });
        self.pending_generation +%= 1;
    }

    pub fn setReplayOpaque(self: *Txn, sequence: u64, payload: []const u8) !void {
        try writeReplayEntries(self.allocator, self, sequence, payload);
    }

    pub fn openCursor(self: *Txn) !Cursor {
        const store = self.store orelse return error.InvalidTransactionState;
        return .{
            .txn = self,
            .index_cursor = native.DocumentIndexCursor.init(&store.file, self.checkpoint),
        };
    }

    fn freePending(self: *Txn) void {
        for (self.pending.items) |pending| {
            self.allocator.free(pending.key);
            if (pending.value) |value| self.allocator.free(value);
        }
        self.pending.deinit(self.allocator);
        self.pending = .empty;
    }

    fn freeOwnedReads(self: *Txn) void {
        for (self.owned_reads.items) |value| self.allocator.free(value);
        self.owned_reads.deinit(self.allocator);
        self.owned_reads = .empty;
    }

    fn releaseGenerationReadLock(self: *Txn) void {
        if (!self.generation_read_locked) return;
        const store = self.store orelse return;
        store.generation_lock.unlockShared(store.file.io_impl.io());
        self.generation_read_locked = false;
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
    const OverlayEntry = struct {
        key: []const u8,
        value: ?[]const u8,
    };
    const Direction = enum { forward, backward };

    txn: *Txn,
    index_cursor: native.DocumentIndexCursor,
    current_key: ?[]u8 = null,
    upper_bound: ?[]const u8 = null,
    owned_value: ?[]u8 = null,
    overlay: []OverlayEntry = &.{},
    overlay_generation: u64 = std.math.maxInt(u64),
    disk_candidate: ?native.DocumentIndexEntry = null,
    last_direction: ?Direction = null,

    pub fn close(self: *Cursor) void {
        self.index_cursor.deinit();
        if (self.disk_candidate) |*candidate| candidate.deinit(self.txn.allocator);
        if (self.current_key) |key| self.txn.allocator.free(key);
        if (self.owned_value) |value| self.txn.allocator.free(value);
        if (self.overlay.len > 0) self.txn.allocator.free(self.overlay);
        self.current_key = null;
        self.owned_value = null;
        self.disk_candidate = null;
        self.overlay = &.{};
    }

    pub fn first(self: *Cursor) !backend_adapter.Entry {
        try self.ensureOverlay();
        self.clearBufferedDisk();
        const disk = if (self.txn.prefix.len == 0)
            try self.index_cursor.first()
        else
            try self.index_cursor.seekAtOrAfter(self.txn.prefix, false);
        return try self.resolveMerged(disk, self.overlayAtOrAfter(self.txn.prefix, false), .forward);
    }

    pub fn last(self: *Cursor) !backend_adapter.Entry {
        try self.ensureOverlay();
        self.clearBufferedDisk();
        if (self.upper_bound) |upper_bound| {
            const upper = try self.txn.prefixedKey(upper_bound);
            defer if (self.txn.prefix.len > 0) self.txn.allocator.free(upper);
            return try self.resolveMerged(
                try self.index_cursor.seekAtOrBefore(upper, true),
                self.overlayAtOrBefore(upper, true),
                .backward,
            );
        }
        if (self.txn.prefix.len == 0) {
            return try self.resolveMerged(
                try self.index_cursor.last(),
                if (self.overlay.len == 0) null else self.overlay.len - 1,
                .backward,
            );
        } else {
            const upper = try self.namespaceUpperBound();
            defer self.txn.allocator.free(upper);
            return try self.resolveMerged(
                try self.index_cursor.seekAtOrBefore(upper, true),
                self.overlayAtOrBefore(upper, true),
                .backward,
            );
        }
    }

    pub fn next(self: *Cursor) !backend_adapter.Entry {
        try self.ensureOverlay();
        const current = self.current_key orelse return error.NotFound;
        const disk = if (self.last_direction == .forward)
            self.takeBufferedDisk() orelse try self.index_cursor.next()
        else blk: {
            self.clearBufferedDisk();
            break :blk try self.index_cursor.seekAtOrAfter(current, true);
        };
        return try self.resolveMerged(disk, self.overlayAtOrAfter(current, true), .forward);
    }

    pub fn prev(self: *Cursor) !backend_adapter.Entry {
        try self.ensureOverlay();
        const current = self.current_key orelse return error.NotFound;
        const disk = if (self.last_direction == .backward)
            self.takeBufferedDisk() orelse try self.index_cursor.prev()
        else blk: {
            self.clearBufferedDisk();
            break :blk try self.index_cursor.seekAtOrBefore(current, true);
        };
        return try self.resolveMerged(disk, self.overlayAtOrBefore(current, true), .backward);
    }

    pub fn seekAtOrAfter(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        try self.ensureOverlay();
        const lookup_key = try self.txn.prefixedKey(key);
        defer if (self.txn.prefix.len > 0) self.txn.allocator.free(lookup_key);
        self.clearBufferedDisk();
        return try self.resolveMerged(
            try self.index_cursor.seekAtOrAfter(lookup_key, false),
            self.overlayAtOrAfter(lookup_key, false),
            .forward,
        );
    }

    pub fn seekAtOrBefore(self: *Cursor, key: []const u8) !backend_adapter.Entry {
        try self.ensureOverlay();
        const lookup_key = try self.txn.prefixedKey(key);
        defer if (self.txn.prefix.len > 0) self.txn.allocator.free(lookup_key);
        self.clearBufferedDisk();
        if (self.upper_bound) |upper_bound| {
            const upper = try self.txn.prefixedKey(upper_bound);
            defer if (self.txn.prefix.len > 0) self.txn.allocator.free(upper);
            if (std.mem.order(u8, lookup_key, upper) != .lt) {
                return try self.resolveMerged(
                    try self.index_cursor.seekAtOrBefore(upper, true),
                    self.overlayAtOrBefore(upper, true),
                    .backward,
                );
            }
        }
        return try self.resolveMerged(
            try self.index_cursor.seekAtOrBefore(lookup_key, false),
            self.overlayAtOrBefore(lookup_key, false),
            .backward,
        );
    }

    pub fn setUpperBound(self: *Cursor, upper: ?[]const u8) void {
        self.clearBufferedDisk();
        self.last_direction = null;
        self.upper_bound = upper;
    }

    fn resolveMerged(
        self: *Cursor,
        initial_disk: ?native.DocumentIndexEntry,
        initial_overlay_index: ?usize,
        direction: Direction,
    ) !backend_adapter.Entry {
        const store = self.txn.store orelse return error.InvalidTransactionState;
        var disk = initial_disk;
        errdefer if (disk) |*candidate| candidate.deinit(self.txn.allocator);
        var overlay_index = initial_overlay_index;

        while (true) {
            if (disk) |candidate| {
                if (!self.keyInRange(candidate.key)) {
                    var out_of_range = candidate;
                    out_of_range.deinit(self.txn.allocator);
                    disk = null;
                }
            }
            const overlay_candidate = if (overlay_index) |index| blk: {
                const candidate = self.overlay[index];
                if (!self.keyInRange(candidate.key)) {
                    overlay_index = null;
                    break :blk null;
                }
                break :blk candidate;
            } else null;

            const choose_overlay = if (overlay_candidate) |pending| blk: {
                const indexed = disk orelse break :blk true;
                const order = std.mem.order(u8, pending.key, indexed.key);
                break :blk switch (direction) {
                    .forward => order != .gt,
                    .backward => order != .lt,
                };
            } else false;

            if (choose_overlay) {
                const pending = overlay_candidate.?;
                if (disk) |indexed| {
                    if (std.mem.eql(u8, pending.key, indexed.key)) {
                        var consumed = indexed;
                        consumed.deinit(self.txn.allocator);
                        disk = null;
                        disk = try self.advanceDisk(direction);
                    }
                }
                overlay_index = self.advanceOverlayIndex(overlay_index.?, direction);
                const value = pending.value orelse continue;
                const buffered_disk = disk;
                disk = null;
                return try self.installPending(pending.key, value, buffered_disk, direction);
            }

            if (disk) |indexed| {
                var consumed = indexed;
                disk = null;
                const value = store.file.documentValueAtIndexEntryAlloc(self.txn.allocator, self.txn.checkpoint, consumed) catch |err| {
                    consumed.deinit(self.txn.allocator);
                    return err;
                };
                if (value) |owned_value| return self.installDisk(consumed, owned_value, direction);
                consumed.deinit(self.txn.allocator);
                disk = try self.advanceDisk(direction);
                continue;
            }
            return error.NotFound;
        }
    }

    fn ensureOverlay(self: *Cursor) !void {
        if (self.overlay_generation == self.txn.pending_generation) return;
        const alloc = self.txn.allocator;
        const indices = try alloc.alloc(usize, self.txn.pending.items.len);
        defer alloc.free(indices);
        for (indices, 0..) |*index, i| index.* = i;
        std.mem.sort(usize, indices, self.txn, struct {
            fn lessThan(txn: *Txn, lhs: usize, rhs: usize) bool {
                const order = std.mem.order(u8, txn.pending.items[lhs].key, txn.pending.items[rhs].key);
                return order == .lt or (order == .eq and lhs < rhs);
            }
        }.lessThan);

        var rebuilt = std.ArrayListUnmanaged(OverlayEntry).empty;
        defer rebuilt.deinit(alloc);
        var i: usize = 0;
        while (i < indices.len) {
            var latest = indices[i];
            i += 1;
            while (i < indices.len and std.mem.eql(u8, self.txn.pending.items[latest].key, self.txn.pending.items[indices[i]].key)) : (i += 1) {
                latest = indices[i];
            }
            const mutation = self.txn.pending.items[latest];
            try rebuilt.append(alloc, .{ .key = mutation.key, .value = mutation.value });
        }
        const owned = try rebuilt.toOwnedSlice(alloc);
        if (self.overlay.len > 0) alloc.free(self.overlay);
        self.overlay = owned;
        self.overlay_generation = self.txn.pending_generation;
    }

    fn overlayAtOrAfter(self: *const Cursor, key: []const u8, strict: bool) ?usize {
        var left: usize = 0;
        var right = self.overlay.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const order = std.mem.order(u8, self.overlay[mid].key, key);
            if (order == .lt or (strict and order == .eq)) left = mid + 1 else right = mid;
        }
        return if (left < self.overlay.len) left else null;
    }

    fn overlayAtOrBefore(self: *const Cursor, key: []const u8, strict: bool) ?usize {
        var left: usize = 0;
        var right = self.overlay.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            const order = std.mem.order(u8, self.overlay[mid].key, key);
            if (order == .lt or (!strict and order == .eq)) left = mid + 1 else right = mid;
        }
        return if (left == 0) null else left - 1;
    }

    fn advanceOverlayIndex(self: *const Cursor, index: usize, direction: Direction) ?usize {
        return switch (direction) {
            .forward => if (index + 1 < self.overlay.len) index + 1 else null,
            .backward => if (index > 0) index - 1 else null,
        };
    }

    fn advanceDisk(self: *Cursor, direction: Direction) !?native.DocumentIndexEntry {
        return switch (direction) {
            .forward => try self.index_cursor.next(),
            .backward => try self.index_cursor.prev(),
        };
    }

    fn keyInRange(self: *const Cursor, full_key: []const u8) bool {
        if (!std.mem.startsWith(u8, full_key, self.txn.prefix)) return false;
        if (self.upper_bound) |upper| {
            if (std.mem.order(u8, full_key[self.txn.prefix.len..], upper) != .lt) return false;
        }
        return true;
    }

    fn installPending(
        self: *Cursor,
        full_key: []const u8,
        value: []const u8,
        buffered_disk: ?native.DocumentIndexEntry,
        direction: Direction,
    ) !backend_adapter.Entry {
        var disk = buffered_disk;
        errdefer if (disk) |*candidate| candidate.deinit(self.txn.allocator);
        const owned_key = try self.txn.allocator.dupe(u8, full_key);
        errdefer self.txn.allocator.free(owned_key);
        const owned_value = try self.txn.allocator.dupe(u8, value);
        errdefer self.txn.allocator.free(owned_value);
        self.clearCurrent();
        self.current_key = owned_key;
        self.owned_value = owned_value;
        self.disk_candidate = disk;
        disk = null;
        self.last_direction = direction;
        return .{ .key = owned_key[self.txn.prefix.len..], .value = owned_value };
    }

    fn installDisk(self: *Cursor, indexed: native.DocumentIndexEntry, value: []u8, direction: Direction) backend_adapter.Entry {
        self.clearCurrent();
        self.current_key = indexed.key;
        self.owned_value = value;
        self.last_direction = direction;
        return .{ .key = indexed.key[self.txn.prefix.len..], .value = value };
    }

    fn clearCurrent(self: *Cursor) void {
        if (self.current_key) |key| self.txn.allocator.free(key);
        if (self.owned_value) |value| self.txn.allocator.free(value);
        self.current_key = null;
        self.owned_value = null;
    }

    fn clearBufferedDisk(self: *Cursor) void {
        if (self.disk_candidate) |*candidate| candidate.deinit(self.txn.allocator);
        self.disk_candidate = null;
    }

    fn takeBufferedDisk(self: *Cursor) ?native.DocumentIndexEntry {
        const candidate = self.disk_candidate;
        self.disk_candidate = null;
        return candidate;
    }

    fn namespaceUpperBound(self: *Cursor) ![]u8 {
        const upper = try self.txn.allocator.dupe(u8, self.txn.prefix);
        std.debug.assert(upper.len > 0 and upper[upper.len - 1] == 0);
        upper[upper.len - 1] = 1;
        return upper;
    }
};

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

test "lite native docstore keeps disjoint namespace cursors isolated without snapshots" {
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
    var cursor_b = try read_b.openCursor();
    try std.testing.expectEqualStrings("doc:b", (try cursor_b.first()).key);
    cursor_b.close();
    read_b.abort();

    write_a = try Txn.openWriteYieldingWithPrefix(&store, &prefix_a);
    try write_a.put("doc:a", "a2");
    try write_a.commit();

    read_b = try Txn.openReadWithPrefix(&store, &prefix_b);
    defer read_b.abort();
    cursor_b = try read_b.openCursor();
    defer cursor_b.close();
    try std.testing.expectEqualStrings("doc:b", (try cursor_b.first()).key);
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

test "lite native write cursors merge pending writes and deletes in order" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try testPath(allocator, tmp, "native-docstore-write-cursor.aflite");
    defer allocator.free(path);

    var store = try Store.create(allocator, path, true);
    defer store.close();
    var seed = try Txn.openWrite(&store);
    try seed.put("doc:a", "disk-a");
    try seed.put("doc:c", "disk-c");
    try seed.put("doc:e", "disk-e");
    try seed.commit();

    var write = try Txn.openWrite(&store);
    defer write.abort();
    try write.put("doc:b", "pending-b");
    try write.put("doc:c", "pending-c-old");
    try write.put("doc:c", "pending-c");
    try write.delete("doc:e");
    try write.put("doc:d", "pending-d-old");
    try write.delete("doc:d");
    try write.put("doc:d", "pending-d");

    var cursor = try write.openCursor();
    defer cursor.close();
    var entry = try cursor.first();
    try std.testing.expectEqualStrings("doc:a", entry.key);
    try std.testing.expectEqualStrings("disk-a", entry.value);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:b", entry.key);
    try std.testing.expectEqualStrings("pending-b", entry.value);
    entry = try cursor.prev();
    try std.testing.expectEqualStrings("doc:a", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:b", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:c", entry.key);
    try std.testing.expectEqualStrings("pending-c", entry.value);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:d", entry.key);
    try std.testing.expectEqualStrings("pending-d", entry.value);
    try std.testing.expectError(error.NotFound, cursor.next());

    entry = try cursor.last();
    try std.testing.expectEqualStrings("doc:d", entry.key);
    entry = try cursor.prev();
    try std.testing.expectEqualStrings("doc:c", entry.key);
    entry = try cursor.seekAtOrAfter("doc:bb");
    try std.testing.expectEqualStrings("doc:c", entry.key);
    entry = try cursor.seekAtOrBefore("doc:bb");
    try std.testing.expectEqualStrings("doc:b", entry.key);
    cursor.setUpperBound("doc:d");
    entry = try cursor.seekAtOrAfter("doc:c");
    try std.testing.expectEqualStrings("doc:c", entry.key);
    try std.testing.expectError(error.NotFound, cursor.next());
    entry = try cursor.last();
    try std.testing.expectEqualStrings("doc:c", entry.key);
    entry = try cursor.seekAtOrBefore("doc:z");
    try std.testing.expectEqualStrings("doc:c", entry.key);
    cursor.setUpperBound(null);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:d", entry.key);

    // A cursor opened before later mutations refreshes only its small sorted
    // overlay; it does not rematerialize the durable namespace.
    try write.put("doc:aa", "pending-aa");
    try write.delete("doc:c");
    entry = try cursor.seekAtOrAfter("doc:a");
    try std.testing.expectEqualStrings("doc:a", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:aa", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:b", entry.key);
    entry = try cursor.next();
    try std.testing.expectEqualStrings("doc:d", entry.key);
    try std.testing.expectError(error.NotFound, cursor.next());
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

fn expectIndexCursorMatchesDiskRebuild(store: *Store) !void {
    const allocator = std.testing.allocator;
    var read = try store.beginRead();
    defer read.abort();
    var cursor = try read.openCursor();
    defer cursor.close();

    const rebuilt = try store.file.snapshotDocumentsAlloc(allocator);
    defer native.NativeFile.freeSnapshotDocuments(allocator, rebuilt);
    if (rebuilt.len == 0) {
        try std.testing.expectError(error.NotFound, cursor.first());
    } else for (rebuilt, 0..) |expected, i| {
        const actual = if (i == 0) try cursor.first() else try cursor.next();
        try std.testing.expectEqualStrings(expected.key, actual.key);
        try std.testing.expectEqualStrings(expected.value, actual.value);
    }
    if (rebuilt.len > 0) try std.testing.expectError(error.NotFound, cursor.next());
}

test "lite native docstore disk index matches rebuild across mixed commits" {
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
    try expectIndexCursorMatchesDiskRebuild(&store);

    {
        var batch = try store.beginWrite();
        try batch.delete("doc:c");
        try batch.delete("doc:never-existed");
        try batch.put("doc:d", "d1");
        try batch.put("doc:a", "a2");
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    {
        // Put-then-delete and delete-then-put of the same key in one batch.
        var batch = try store.beginWrite();
        try batch.put("doc:e", "e1");
        try batch.delete("doc:e");
        try batch.delete("doc:d");
        try batch.put("doc:d", "d2-resurrected");
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    // A large value that spills to external value pages.
    {
        const big = try allocator.alloc(u8, 3 * native.default_page_size);
        defer allocator.free(big);
        @memset(big, 'x');
        var batch = try store.beginWrite();
        try batch.put("doc:big", big);
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    // Empty commit publishes nothing and keeps the cache current.
    {
        var batch = try store.beginWrite();
        try batch.commit();
    }
    try expectIndexCursorMatchesDiskRebuild(&store);

    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("a2", try read.get("doc:a"));
    try std.testing.expectEqualStrings("b2-last-wins", try read.get("doc:b"));
    try std.testing.expectError(error.NotFound, read.get("doc:c"));
    try std.testing.expectEqualStrings("d2-resurrected", try read.get("doc:d"));
    try std.testing.expectError(error.NotFound, read.get("doc:e"));
}

test "lite native docstore disk cursor loads values lazily" {
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
        var cursor = try read.openCursor();
        cursor.close();
        read.abort();
    }
    {
        var batch = try store.beginWrite();
        errdefer batch.abort();
        try batch.put("doc:b", "b-v2");
        try batch.commit();
    }
    var read = try store.beginRead();
    defer read.abort();
    try std.testing.expectEqualStrings("a-value-that-must-not-be-copied", try read.get("doc:a"));
    var cursor = try read.openCursor();
    defer cursor.close();
    try std.testing.expectEqualStrings("doc:a", (try cursor.first()).key);
    try std.testing.expectEqualStrings("a-value-that-must-not-be-copied", (try cursor.first()).value);
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

test "lite native docstore disk index survives out-of-band catalog commits and vacuum" {
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
    try expectIndexCursorMatchesDiskRebuild(&store);

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
    try expectIndexCursorMatchesDiskRebuild(&store);
}

test "lite native docstore cold writes and large disk-index cursors stay bounded" {
    const allocator = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();

    const path = try testPath(allocator, tmp, "native-docstore-lazy-writes.aflite");
    defer allocator.free(path);

    var store = try Store.createWithOptions(allocator, path, .{ .no_sync = true });
    defer store.close();

    var seed = try store.beginWrite();
    var key_buffer: [32]u8 = undefined;
    var i: usize = 0;
    while (i < bounded_cursor_test_documents) : (i += 1) {
        const key = try std.fmt.bufPrint(&key_buffer, "doc:{d:0>5}", .{i});
        try seed.put(key, "v1");
    }
    try seed.commit();

    // Point reads and ordered cursors both use the disk-resident index.
    {
        var read = try store.beginRead();
        defer read.abort();
        try std.testing.expectEqualStrings("v1", try read.get("doc:00000"));
        var cursor = try read.openCursor();
        defer cursor.close();
        var count: usize = 1;
        try std.testing.expectEqualStrings("doc:00000", (try cursor.first()).key);
        while (true) {
            _ = cursor.next() catch |err| switch (err) {
                error.NotFound => break,
                else => return err,
            };
            count += 1;
        }
        try std.testing.expectEqual(bounded_cursor_test_documents, count);
        try std.testing.expectEqualStrings("doc:00511", (try cursor.last()).key);
        var reverse_count: usize = 1;
        while (true) {
            _ = cursor.prev() catch |err| switch (err) {
                error.NotFound => break,
                else => return err,
            };
            reverse_count += 1;
        }
        try std.testing.expectEqual(bounded_cursor_test_documents, reverse_count);
        try std.testing.expectEqualStrings("doc:00256", (try cursor.seekAtOrAfter("doc:00256")).key);
    }

    // Publishing a small write does not rebuild or retain a table-sized cache.
    var update = try store.beginWrite();
    try update.put("doc:00000", "v2");
    try update.commit();

    var verify = try store.beginRead();
    defer verify.abort();
    try std.testing.expectEqualStrings("v2", try verify.get("doc:00000"));
}
