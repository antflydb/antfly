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

const std = @import("std");
const read_admission = @import("../../common/read_admission.zig");
const platform_sync = @import("antfly_platform").sync;
const builtin = @import("builtin");
const bloom = @import("bloom");
const Allocator = std.mem.Allocator;
const backend_adapter = @import("../backend_adapter.zig");
const backend_erased = @import("../backend_erased.zig");
const backend_types = @import("../backend_types.zig");
const internal_keys = @import("../internal_keys.zig");
const lsm_table_file = @import("../lsm/table_file.zig");
const cache_mod = @import("cache.zig");
const repository_mod = @import("repository.zig");
const run_store = @import("run_store.zig");
const state_mod = @import("state.zig");
const storage_io = @import("storage_io.zig");
const platform_time = @import("antfly_platform").time;

const Run = repository_mod.Run;
const State = state_mod.State;
const ActiveMemTable = state_mod.ActiveMemTable;
pub var test_private_read_versions: bool = false;
pub var test_current_point_unlocked_hook: ?*const fn (*anyopaque) anyerror!void = null;
pub var test_current_point_rank_walk: bool = false;
/// Benchmark control for the former synchronous batch's unconditional copy.
pub var test_duplicate_owned_point_results: bool = false;
const namespaceOf = state_mod.namespaceOf;
const compareNamespace = state_mod.compareNamespace;
const compareEntryTo = state_mod.compareEntryTo;

/// CPU work slices share the I/O authority used for cooperative continuation.
/// Storage timestamps separately govern retention and durable file ages.
pub fn workNowNs(backend: anytype) u64 {
    return workIoNowNs(if (comptime @hasDecl(@TypeOf(backend.*), "manifestCoordinationIo")) backend.manifestCoordinationIo() else null);
}

fn workIoNowNs(io: ?std.Io) u64 {
    if (io) |owner| {
        const now = std.Io.Clock.awake.now(owner).toNanoseconds();
        return @intCast(@max(0, @min(now, std.math.maxInt(u64))));
    }
    return @import("antfly_platform").time.monotonicNs();
}

const OwnedBytes = struct {
    allocator: Allocator,
    bytes: []u8,

    fn release(self: *@This()) void {
        self.allocator.free(self.bytes);
        self.* = undefined;
    }
};

const VisibleBytes = union(enum) {
    none,
    owned: OwnedBytes,
    borrowed,
    local: *SharedBytes,

    fn release(self: *@This()) void {
        switch (self.*) {
            .none, .borrowed => {},
            .owned => |*owned| owned.release(),
            .local => |payload| payload.release(),
        }
        self.* = .none;
    }

    fn setOwned(self: *@This(), allocator: Allocator, bytes: []u8) void {
        self.release();
        self.* = .{ .owned = .{ .allocator = allocator, .bytes = bytes } };
    }
};

const SharedBytes = @import("shared_bytes.zig").SharedBytes;
const LocalReader = @import("local_reader.zig").Pool;
const BatchScratch = @import("write_batch_scratch.zig");
const RunSourceLease = @import("source_lease.zig").Lease;

/// Transaction result pins share the same lifetime contract for both caches.
const BlockPin = union(enum) {
    cached: cache_mod.Handle,
    local: *SharedBytes,
    fn release(self: *@This()) void {
        switch (self.*) {
            .cached => |*handle| handle.release(),
            .local => |payload| payload.release(),
        }
        self.* = undefined;
    }
    fn releaseForAdmittedRead(self: *@This(), deferred: bool) void {
        switch (self.*) {
            .cached => |*handle| if (deferred) handle.releaseToCache() else handle.release(),
            .local => |payload| payload.release(),
        }
        self.* = undefined;
    }
};

/// One owner-wide bound for cursor and point-result borrowing.
fn retainLocalResultPin(backend: anytype, payload: *SharedBytes, held: *std.ArrayListUnmanaged(BlockPin)) !bool {
    if (!payload.result_pins_allowed) return false;
    var bytes: usize = 0;
    var count: usize = 0;
    for (held.items) |pin| if (pin == .local) {
        if (pin.local == payload) return true;
        bytes +|= pin.local.bytes.len;
        count += 1;
    };
    if (count >= 64 or payload.bytes.len > (1024 * 1024) -| bytes) return false;
    const owned = payload.retain();
    errdefer owned.release();
    try held.append(backend.allocator, .{ .local = owned });
    return true;
}

const ResultBlockRetention = enum { unknown, pinned, copy };

const SourceBlockLease = union(enum) {
    none,
    owned: OwnedBytes,
    cached: cache_mod.Handle,
    local: *SharedBytes,

    fn bytes(self: *const @This()) ?[]const u8 {
        return switch (self.*) {
            .none => null,
            .owned => |owned| owned.bytes,
            .cached => |*handle| handle.runTableBlock(),
            .local => |payload| payload.bytes,
        };
    }

    fn retainPin(self: *const @This()) ?BlockPin {
        return switch (self.*) {
            .cached => |*handle| .{ .cached = handle.retain() },
            .local => |payload| .{ .local = payload.retain() },
            else => null,
        };
    }

    fn release(self: *@This()) void {
        switch (self.*) {
            .none => {},
            .owned => |*owned| owned.release(),
            .cached => |*handle| handle.release(),
            .local => |payload| payload.release(),
        }
        self.* = .none;
    }

    fn releaseForAdmittedRead(self: *@This(), deferred: bool) void {
        switch (self.*) {
            .none => {},
            .owned => |*owned| owned.release(),
            .cached => |*handle| if (deferred) handle.releaseToCache() else handle.release(),
            .local => |payload| payload.release(),
        }
        self.* = .none;
    }
};

fn hashBulkEntryKey(namespace: backend_types.Namespace, key: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(0);
    if (namespace.name) |name| hasher.update(name);
    hasher.update(&.{0});
    hasher.update(key);
    return hasher.final();
}

/// Point reads must not walk the growing unordered bulk arena. Borrow keys
/// from its stable entry arena, retain the latest duplicate, and compare full
/// namespace/key bytes so hash collisions never affect read-your-writes.
const BulkAppendIndex = struct {
    const Key = struct { namespace: backend_types.Namespace, key: []const u8 };
    const Context = struct {
        pub fn hash(_: @This(), key: Key) u64 {
            return hashBulkEntryKey(key.namespace, key.key);
        }
        pub fn eql(_: @This(), left: Key, right: Key) bool {
            return compareNamespace(left.namespace, right.namespace) == .eq and std.mem.eql(u8, left.key, right.key);
        }
    };
    entries: std.HashMapUnmanaged(Key, usize, Context, 80) = .{},
    fn deinit(self: *@This(), alloc: Allocator) void {
        self.entries.deinit(alloc);
        self.* = .{};
    }
    fn clear(self: *@This()) void {
        self.entries.clearRetainingCapacity();
    }
    fn append(self: *@This(), alloc: Allocator, state: *State, namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
        // Reserve both containers before publishing either change. Failed
        // allocation leaves the prior overlay and its index consistent.
        try self.entries.ensureUnusedCapacity(alloc, 1);
        try state.entries.ensureUnusedCapacity(alloc, 1);
        const entry_allocator = try state.ensureArenaAllocator(alloc);
        const entry = try state_mod.initArenaEntry(entry_allocator, namespace, key, value, false);
        const index = state.entryCount();
        state.entries.appendAssumeCapacity(entry);
        self.entries.putAssumeCapacity(.{ .namespace = namespaceOf(entry), .key = entry.key }, index);
    }
    fn get(self: *const @This(), state: *const State, namespace: backend_types.Namespace, key: []const u8) ?state_mod.OwnedEntry {
        const index = self.entries.get(.{ .namespace = namespace, .key = key }) orelse return null;
        return state.entryAt(index);
    }
};

/// Lazily index only transaction keys for prefix admission. Once initialized,
/// mutations update this tree instead of cloning unordered key/value overlays.
const WriterPrefixIndex = struct {
    state: ?ActiveMemTable = null,

    fn deinit(self: *@This(), alloc: Allocator) void {
        if (self.state) |*state| state.deinit(alloc);
        self.state = null;
    }

    fn record(self: *@This(), alloc: Allocator, namespace: backend_types.Namespace, key: []const u8, tombstone: bool) void {
        if (self.state) |*state| state.upsert(alloc, namespace, key, "", tombstone) catch {
            // This is an optional cache: a successful authoritative mutation
            // must stay successful. The next probe rebuilds after cache OOM.
            self.deinit(alloc);
        };
    }

    fn ensure(self: *@This(), alloc: Allocator, mutable: *const ActiveMemTable, bulk: *const State) !*const ActiveMemTable {
        if (self.state == null) {
            var state: ActiveMemTable = .{};
            errdefer state.deinit(alloc);
            for (0..mutable.entryCount()) |i| {
                const entry = mutable.entryAt(i);
                try state.upsert(alloc, namespaceOf(entry), entry.key, "", entry.tombstone);
            }
            for (0..bulk.entryCount()) |i| {
                const entry = bulk.entryAt(i);
                try state.upsert(alloc, namespaceOf(entry), entry.key, "", entry.tombstone);
            }
            self.state = state;
        }
        return &self.state.?;
    }
};

/// Seek a pinned committed generation and merge only matching pending keys.
/// No write-cursor snapshot or document payload copy is needed for admission.
fn writerHasPrefix(comptime BackendType: type, writer: anytype, namespace: backend_types.Namespace, prefix: []const u8) !bool {
    const overlay = try writer.prefix_index.ensure(writer.allocator, &writer.mutable, &writer.bulk_appends);
    var index = overlay.lowerBound(namespace, prefix);
    while (index < overlay.entryCount()) : (index += 1) {
        const entry = overlay.entryAt(index);
        if (compareNamespace(namespaceOf(entry), namespace) != .eq or !std.mem.startsWith(u8, entry.key, prefix)) break;
        if (!entry.tombstone) return true;
    }
    var snapshot = capture: {
        const locked = lockBackend(BackendType, writer.backend);
        defer unlockBackend(BackendType, writer.backend, locked);
        var base = try writer.backend.mutable.snapshot(writer.allocator);
        errdefer base.deinit(writer.allocator);
        const immutable = if (@hasDecl(BackendType, "snapshotImmutableMemtables")) try writer.backend.snapshotImmutableMemtables() else &.{};
        errdefer releaseImmutableMemtableSnapshotList(BackendType, writer.backend, immutable);
        var view = try RunReadView.pin(writer.backend, writer.metadata_allocator);
        errdefer view.release(writer.backend);
        try view.prepareCursor(writer.backend);
        break :capture .{ .base = base, .immutable = immutable, .view = view };
    };
    defer {
        const locked = lockBackend(BackendType, writer.backend);
        defer unlockBackend(BackendType, writer.backend, locked);
        snapshot.view.release(writer.backend);
        releaseImmutableMemtableSnapshotList(BackendType, writer.backend, snapshot.immutable);
        snapshot.base.deinit(writer.allocator);
    }
    const upper = try writer.metadata_allocator.dupe(u8, prefix);
    defer writer.metadata_allocator.free(upper);
    var upper_len = upper.len;
    while (upper_len > 0 and upper[upper_len - 1] == 255) upper_len -= 1;
    if (upper_len > 0) upper[upper_len - 1] += 1;
    var cursor = try MergeCursor(BackendType, State).initView(writer.metadata_allocator, writer.backend, &snapshot.base, snapshot.immutable, snapshot.view, namespace, false);
    if (upper_len > 0) cursor.setUpperBound(upper[0..upper_len]);
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(prefix);
    while (row) |entry| {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        if (overlay.findIndex(namespace, entry.key) == null) return true;
        row = try cursor.next();
    }
    return false;
}

fn bulkStateHasDuplicateKeys(allocator: Allocator, state: *const State) !bool {
    var index: std.AutoHashMapUnmanaged(u64, std.ArrayListUnmanaged(usize)) = .{};
    defer {
        var values = index.valueIterator();
        while (values.next()) |bucket| bucket.deinit(allocator);
        index.deinit(allocator);
    }

    var cursor: State.EntryCursor = .{};
    for (0..state.entryCount()) |idx| {
        const entry = cursor.at(state, idx);
        const namespace = namespaceOf(entry);
        const hash = hashBulkEntryKey(namespace, entry.key);
        const gop = try index.getOrPut(allocator, hash);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.ArrayListUnmanaged(usize).empty;
        } else {
            for (gop.value_ptr.items) |existing_idx| {
                const existing = state.entryAt(existing_idx);
                if (compareEntryTo(existing, namespace, entry.key) == .eq) return true;
            }
        }
        try gop.value_ptr.append(allocator, idx);
    }
    return false;
}

fn releaseHeldBlocks(held_blocks: *std.ArrayListUnmanaged(BlockPin), allocator: Allocator) void {
    for (held_blocks.items) |*handle| handle.release();
    held_blocks.deinit(allocator);
}

// Result allocations and their packing cursor share one lifetime. Clearing or
// transferring ownership invalidates the cursor; appending unrelated buffers
// does not. Existing result slices never move when metadata or packing grows.
const PointResultValues = struct {
    items: [][]u8 = &.{},
    capacity: usize = 0,
    copies: AsyncPointResultCopies = .{},
    const empty: @This() = .{};
    const List = std.ArrayListUnmanaged([]u8);

    fn list(self: *const @This()) List {
        return .{ .items = self.items, .capacity = self.capacity, .pointer_stability = .{} };
    }
    fn update(self: *@This(), entries: List) void {
        self.items = entries.items;
        self.capacity = entries.capacity;
    }
    fn ensureTotalCapacity(self: *@This(), allocator: Allocator, count: usize) !void {
        var entries = self.list();
        defer self.update(entries);
        try entries.ensureTotalCapacity(allocator, count);
    }
    fn ensureUnusedCapacity(self: *@This(), allocator: Allocator, count: usize) !void {
        try self.ensureTotalCapacity(allocator, self.items.len + count);
    }
    fn appendAssumeCapacity(self: *@This(), value: []u8) void {
        var entries = self.list();
        entries.appendAssumeCapacity(value);
        self.update(entries);
    }
    fn append(self: *@This(), allocator: Allocator, value: []u8) !void {
        try self.ensureUnusedCapacity(allocator, 1);
        self.appendAssumeCapacity(value);
    }
    fn appendSlice(self: *@This(), allocator: Allocator, values: []const []u8) !void {
        var entries = self.list();
        defer self.update(entries);
        try entries.appendSlice(allocator, values);
    }
    fn pop(self: *@This()) ?[]u8 {
        // A caller can remove the active slab, so forget the packing cursor.
        self.copies = .{};
        var entries = self.list();
        defer self.update(entries);
        return entries.pop();
    }
    fn clearRetainingCapacity(self: *@This()) void {
        self.items.len = 0;
        self.copies = .{};
    }
    fn deinit(self: *@This(), allocator: Allocator) void {
        var entries = self.list();
        entries.deinit(allocator);
        self.* = .empty;
    }
};

fn releaseHeldValues(held_values: *PointResultValues, allocator: Allocator) void {
    for (held_values.items) |value| allocator.free(value);
    held_values.deinit(allocator);
    held_values.* = .empty;
}

pub fn recordCursorValueBorrow(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordCursorValueBorrow")) backend.recordCursorValueBorrow();
}

pub fn recordCursorValueCopy(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordCursorValueCopy")) backend.recordCursorValueCopy();
}

fn disableCursorScanValueStats(cursor: anytype) ?bool {
    const CursorType = @TypeOf(cursor.*);
    if (comptime !@hasField(CursorType, "record_scan_value_stats")) return null;
    const previous = cursor.record_scan_value_stats;
    cursor.record_scan_value_stats = false;
    return previous;
}

fn restoreCursorScanValueStats(cursor: anytype, previous: ?bool) void {
    const value = previous orelse return;
    const CursorType = @TypeOf(cursor.*);
    if (comptime @hasField(CursorType, "record_scan_value_stats")) {
        cursor.record_scan_value_stats = value;
    }
}

pub fn recordPointValueBorrow(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordPointValueBorrow")) backend.recordPointValueBorrow();
}

pub fn recordPointValueCopy(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordPointValueCopy")) backend.recordPointValueCopy();
}

pub fn recordPointRunPrecheck(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordPointRunPrecheck")) backend.recordPointRunPrecheck();
}

pub fn recordPointRunPrecheckSurvivor(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordPointRunPrecheckSurvivor")) backend.recordPointRunPrecheckSurvivor();
}

fn canBorrowReaderRetainedState(backend: anytype) bool {
    const BackendType = @TypeOf(backend.*);
    if (@hasDecl(BackendType, "hasVersionReaderPins")) return backend.hasVersionReaderPins();
    return @hasField(BackendType, "active_readers") and backend.active_readers > 0;
}

pub fn retainActiveMutableValueReader(backend: anytype) bool {
    if (@hasDecl(@TypeOf(backend.*), "retainActiveMutableValueReader")) {
        backend.retainActiveMutableValueReader();
        return true;
    }
    return false;
}

pub fn releaseActiveMutableValueReader(backend: anytype, retained: bool) void {
    if (!retained) return;
    if (@hasDecl(@TypeOf(backend.*), "releaseActiveMutableValueReader")) backend.releaseActiveMutableValueReader();
}

pub fn canBorrowActiveMutableValues(backend: anytype) bool {
    if (@hasDecl(@TypeOf(backend.*), "canBorrowActiveMutableValues")) return backend.canBorrowActiveMutableValues();
    return false;
}

fn shouldRetainActiveMutableValueReader(backend: anytype) bool {
    if (@hasDecl(@TypeOf(backend.*), "bulkIngestActive") and backend.bulkIngestActive()) return false;
    return true;
}

pub fn prepareMutableForWrite(backend: anytype) !void {
    if (@hasDecl(@TypeOf(backend.*), "prepareMutableForWrite")) try backend.prepareMutableForWrite();
}

fn publishMutableWithWal(backend: anytype, allocator: Allocator, incoming: *ActiveMemTable) !void {
    if (incoming.entryCount() == 0) return;
    if (comptime @TypeOf(backend.mutable) == ActiveMemTable) {
        var candidate = if (@hasDecl(@TypeOf(backend.*), "prepareAndAppendWalForMutable"))
            try backend.prepareAndAppendWalForMutable(incoming)
        else blk: {
            var prepared = try backend.mutable.preparePublication(allocator, incoming);
            errdefer prepared.deinit(allocator);
            try backend.appendWalForMutable(incoming);
            break :blk prepared;
        };
        defer candidate.deinit(allocator);
        if (@hasDecl(@TypeOf(backend.*), "invalidateMutableReadSnapshot")) backend.invalidateMutableReadSnapshot();
        backend.mutable.publishPrepared(&candidate);
        incoming.deinit(allocator);
        incoming.* = .{ .ordered_enabled = false };
    } else {
        try backend.appendWalForMutable(incoming);
        if (@hasDecl(@TypeOf(backend.*), "invalidateMutableReadSnapshot")) backend.invalidateMutableReadSnapshot();
        try state_mod.applyMutableMoveToMutable(&backend.mutable, allocator, incoming);
    }
}

pub fn enforceMutableWriteAdmission(backend: anytype, incoming: *const ActiveMemTable) !void {
    if (@hasDecl(@TypeOf(backend.*), "enforceMutableWriteAdmission")) {
        try backend.enforceMutableWriteAdmission(incoming);
    }
}

pub fn enforceSortedWriteAdmission(backend: anytype, incoming: *const State) !void {
    if (@hasDecl(@TypeOf(backend.*), "enforceSortedWriteAdmission")) {
        try backend.enforceSortedWriteAdmission(incoming);
    }
}

pub fn notePotentialMaintenanceDebtLocked(backend: anytype) void {
    const BackendType = @TypeOf(backend.*);
    if (@hasDecl(BackendType, "noteWriteMutationLocked")) {
        backend.noteWriteMutationLocked();
    } else if (@hasDecl(BackendType, "notePotentialMaintenanceDebtLocked")) {
        backend.notePotentialMaintenanceDebtLocked();
    } else if (@hasDecl(BackendType, "notePotentialMaintenanceDebt")) {
        backend.notePotentialMaintenanceDebt();
    }
}

pub fn finishCommittedWalAppend(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "finishCommittedWalAppend")) {
        backend.finishCommittedWalAppend();
    }
}

pub fn recordCursorBlockReadahead(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordCursorBlockReadahead")) backend.recordCursorBlockReadahead();
}

pub fn recordCursorTableIndexHit(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordCursorTableIndexHit")) backend.recordCursorTableIndexHit();
}

pub fn recordCursorTableIndexMiss(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordCursorTableIndexMiss")) backend.recordCursorTableIndexMiss();
}

pub fn recordPrefixBloomNegative(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordPrefixBloomNegative")) {
        backend.recordPrefixBloomNegative();
    } else if (@hasDecl(@TypeOf(backend.*), "recordBloomNegative")) {
        backend.recordBloomNegative();
    }
}

pub fn recordBlockPrefixBloomNegative(backend: anytype) void {
    if (@hasDecl(@TypeOf(backend.*), "recordBlockPrefixBloomNegative")) {
        backend.recordBlockPrefixBloomNegative();
    } else if (@hasDecl(@TypeOf(backend.*), "recordBloomNegative")) {
        backend.recordBloomNegative();
    }
}

fn compareTableEntryTo(entry: lsm_table_file.Entry, namespace: backend_types.Namespace, key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(.{ .name = entry.namespace_name }, namespace);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, entry.key, key);
}

fn blockBeforeScanLower(block: lsm_table_file.TableIndex.BlockMeta, namespace: backend_types.Namespace, lower: []const u8) bool {
    return switch (compareNamespace(.{ .name = block.largest_namespace_name }, namespace)) {
        .lt => true,
        .gt => false,
        .eq => std.mem.order(u8, block.largest_key, lower) == .lt,
    };
}

fn invalidateSnapshot(snapshot: *?State, allocator: Allocator) void {
    if (snapshot.*) |*state| {
        state.deinit(allocator);
        snapshot.* = null;
    }
}

fn runtimeScratchAllocator(fallback: Allocator) Allocator {
    // Preserve an explicitly supplied reclaimable root instead of escaping
    // its process ceiling through libc. Unwrap result arenas so scratch frees
    // remain independent, and never introduce reclaim callbacks under locks.
    if (@import("../resource_manager.zig").BudgetedAllocator.fromAllocator(fallback)) |root| {
        return root.threadSafeNoReclaimAllocator();
    }
    if (@import("runtime_memory_abi").Allocator.isStdAdapter(fallback)) return fallback;

    if (comptime builtin.os.tag == .freestanding) return fallback;
    if (comptime builtin.link_libc) return std.heap.c_allocator;
    if (comptime builtin.single_threaded) return fallback;
    return std.heap.smp_allocator;
}

// Decoder/key storage must be reclaimable independently of result arenas.
// Share admission and denial translation across sync and async point readers.
const PointReadScratch = struct {
    backing: Allocator,
    budget: ?@import("../resource_manager.zig").BudgetedAllocator,

    fn init(backend: anytype) @This() {
        const backing = runtimeScratchAllocator(backend.allocator);
        var budget = if (backend.options.resource_manager) |manager| @import("../resource_manager.zig").BudgetedAllocator.init(manager, .lsm_read_working_set, backing, 1) else null;
        if (budget) |*admitted| admitted.credit_quantum = 1;
        return .{ .backing = backing, .budget = budget };
    }

    fn allocator(self: *@This()) Allocator {
        return if (self.budget) |*admitted| admitted.allocator() else self.backing;
    }

    fn resetDenial(self: *@This()) void {
        if (self.budget) |*admitted| admitted.budget_denied = false;
    }

    fn failure(self: *@This(), err: anyerror) anyerror {
        if (err == error.OutOfMemory) if (self.budget) |*admitted| if (admitted.denied()) return error.ResourceBudgetExceeded;
        return err;
    }

    fn deinit(self: *@This()) void {
        if (self.budget) |*admitted| admitted.deinit();
    }
};

pub fn localBlockCacheEnabled(backend: anytype) bool {
    if (@hasDecl(@TypeOf(backend.*), "localBlockCacheEnabled")) {
        return backend.localBlockCacheEnabled();
    }
    return true;
}

fn elapsedNs(start_ns: u64) u64 {
    const end_ns = platform_time.monotonicNs();
    return if (end_ns >= start_ns) end_ns - start_ns else 0;
}

pub fn lockBackend(comptime BackendType: type, backend: *BackendType) bool {
    // Continuations release and reacquire this mutex even on one-thread hosts.
    // Skipping acquisition breaks reclamation and publication ownership.
    if (@hasField(BackendType, "mu")) {
        if (backend.mu.tryLock()) return true;
        const started_ns = if (@hasDecl(BackendType, "recordBackendLockWait"))
            platform_time.monotonicNs()
        else
            0;
        platform_sync.lockYielding(&backend.mu);
        if (@hasDecl(BackendType, "recordBackendLockWait")) {
            backend.recordBackendLockWait(platform_time.monotonicNs() -| started_ns);
        }
        return true;
    }
    return false;
}

pub fn lockBackendWithAdmission(comptime BackendType: type, backend: *BackendType, control: read_admission.Port) !bool {
    try control.checkpoint();
    if (@hasField(BackendType, "mu")) {
        const started_ns = if (@hasDecl(BackendType, "recordBackendLockWait")) platform_time.monotonicNs() else 0;
        try @import("../../common/bounded_mutex.zig").lockAtomic(&backend.mu, control);
        if (@hasDecl(BackendType, "recordBackendLockWait")) backend.recordBackendLockWait(platform_time.monotonicNs() -| started_ns);
        return true;
    }
    return false;
}

pub fn unlockBackend(comptime BackendType: type, backend: *BackendType, locked: bool) void {
    if (locked) {
        if (@hasDecl(BackendType, "unlockWithReclamation")) backend.unlockWithReclamation() else backend.mu.unlock();
    }
}

const RunGroup = struct {
    smallest_namespace_name: ?[]const u8,
    smallest_key: []const u8,
    largest_namespace_name: ?[]const u8,
    largest_key: []const u8,
    run_indices: []usize,

    pub fn deinit(self: *RunGroup, allocator: Allocator) void {
        allocator.free(self.run_indices);
        self.* = undefined;
    }
};

const RunLevel = struct {
    level: u32,
    start_index: usize,
    len: usize,
};

const BorrowedReadHint = struct {
    run_index: usize,
    namespace_name: ?[]const u8,
    key: []const u8,
    entry_index: usize,
};

pub fn BoundStore(comptime BackendType: type) type {
    const LocalReadTxn = BoundReadTxn(BackendType);
    const LocalProbeTxn = BoundProbeTxn(BackendType);
    const LocalCurrentScanTxn = BoundCurrentScanTxn(BackendType);
    const LocalWriteTxn = BoundWriteTxn(BackendType);
    return struct {
        backend: *BackendType,
        namespace: backend_types.Namespace,

        pub fn publicationGate(self: *@This()) ?*std.atomic.Mutex {
            if (@hasField(BackendType, "publication_backlog_mutex")) return &self.backend.publication_backlog_mutex;
            return null;
        }

        pub fn capabilities(_: *@This()) backend_types.Capabilities {
            return .{
                .semantic_cursor_bounds = true,
                .ordered_ranges = true,
                .reverse_ranges = true,
                .cursors = true,
                .ordered_append_puts = true,
                .unordered_bulk_append_puts = true,
                .native_namespaces = false,
                .write_batches = .atomic,
                .single_writer = true,
                .read_snapshots = .snapshot,
            };
        }

        pub fn beginOwnedKeyCursor(self: *@This(), allocator: Allocator, control: read_admission.Port) !OwnedNamespaceKeyCursor(BackendType) {
            return self.backend.beginOwnedKeyCursor(allocator, self.namespace, control);
        }

        pub fn beginOwnedValueRead(self: *@This(), allocator: Allocator, key: []const u8, control: read_admission.Port) !OwnedNamespaceValueRead(BackendType) {
            return self.backend.beginOwnedValueRead(allocator, self.namespace, key, control);
        }

        pub fn beginRead(self: *@This()) !LocalReadTxn {
            return try LocalReadTxn.open(self.backend, self.namespace);
        }

        pub fn beginReadWithAdmission(self: *@This(), admission: backend_types.Namespace.BlockCacheAdmission, control: read_admission.Port) !LocalReadTxn {
            var namespace = self.namespace;
            namespace.block_cache_admission = admission;
            return try LocalReadTxn.openWithAdmission(self.backend, namespace, control);
        }

        pub fn beginReadWithBlockCacheAdmission(
            self: *@This(),
            admission: backend_types.Namespace.BlockCacheAdmission,
        ) !LocalReadTxn {
            var namespace = self.namespace;
            namespace.block_cache_admission = admission;
            return try LocalReadTxn.open(self.backend, namespace);
        }

        pub fn beginProbeWithAdmission(self: *@This(), admission: backend_types.Namespace.BlockCacheAdmission, control: read_admission.Port) !LocalProbeTxn {
            var namespace = self.namespace;
            namespace.block_cache_admission = admission;
            return LocalProbeTxn.openWithAdmission(self.backend, namespace, control);
        }

        pub fn beginCurrentScanWithAdmission(self: *@This(), control: read_admission.Port) !LocalCurrentScanTxn {
            return LocalCurrentScanTxn.openWithAdmission(self.backend, self.namespace, control);
        }

        pub fn beginProbe(self: *@This()) !LocalProbeTxn {
            return try LocalProbeTxn.open(self.backend, self.namespace);
        }

        pub fn beginProbeWithBlockCacheAdmission(
            self: *@This(),
            admission: backend_types.Namespace.BlockCacheAdmission,
        ) !LocalProbeTxn {
            var namespace = self.namespace;
            namespace.block_cache_admission = admission;
            return try LocalProbeTxn.open(self.backend, namespace);
        }

        pub fn beginCurrentScan(self: *@This()) !LocalCurrentScanTxn {
            return try LocalCurrentScanTxn.open(self.backend, self.namespace);
        }

        pub fn beginReplayLaneScan(self: *@This(), lane_ordinal: u8, from_sequence: u64) !LocalCurrentScanTxn {
            var replay_namespace = self.namespace;
            replay_namespace.block_cache_admission = .transient;
            if (@hasDecl(BackendType, "cloneReplayLaneMutableRange")) {
                const lower = internal_keys.replayRangeLower(lane_ordinal, from_sequence);
                const upper = internal_keys.replayRangeUpper(lane_ordinal);
                return try LocalCurrentScanTxn.openReplayLane(
                    self.backend,
                    replay_namespace,
                    lower[0..],
                    upper[0..],
                );
            }
            return try LocalCurrentScanTxn.open(self.backend, replay_namespace);
        }

        pub fn forEachReplayLaneFrom(
            self: *@This(),
            lane_ordinal: u8,
            from_sequence: u64,
            max_entries: usize,
            ctx: *anyopaque,
            callback: backend_erased.Store.ReplayCallback,
        ) !backend_types.ReplayLaneIterationStats {
            // Replay is a sequential, one-shot scan. It must neither retain
            // the scanned data blocks in the shared cache nor clone an entire
            // bulk-ingest mutable generation merely to establish visibility.
            // A replay boundary freezes the current generation instead; the
            // upstream replay window owns the byte/dimension bound.
            var namespace = self.namespace;
            namespace.block_cache_admission = .transient;
            var scan = try LocalCurrentScanTxn.openReplay(self.backend, namespace);
            defer scan.abort();

            var cursor = try scan.openCursor();
            defer cursor.close();

            const lower = internal_keys.replayRangeLower(lane_ordinal, from_sequence);
            const upper = internal_keys.replayRangeUpper(lane_ordinal);
            cursor.setUpperBound(upper[0..]);

            var stats = backend_types.ReplayLaneIterationStats{ .scan_batches = 1 };
            var entry = try cursor.seekAtOrAfter(lower[0..]);
            while (entry) |kv| {
                if (std.mem.order(u8, kv.key, upper[0..]) != .lt) break;
                const sequence = internal_keys.parseReplayEntrySequence(kv.key, lane_ordinal) orelse break;
                try callback(ctx, sequence, kv.value);
                stats.scanned_entries += 1;
                stats.matched_entries += 1;
                stats.last_sequence = sequence;
                if (max_entries != 0 and stats.matched_entries >= max_entries) break;
                entry = try cursor.next();
            }
            return stats;
        }

        pub fn beginWrite(self: *@This()) !LocalWriteTxn {
            return try LocalWriteTxn.open(self.backend, self.namespace);
        }

        pub fn beginBatch(self: *@This()) !LocalWriteTxn {
            return try LocalWriteTxn.open(self.backend, self.namespace);
        }

        pub fn beginBatchWithOptions(self: *@This(), options: backend_types.BatchOptions) !LocalWriteTxn {
            var namespace = self.namespace;
            namespace.block_cache_admission = options.block_cache_admission;
            return try LocalWriteTxn.openWithOptions(self.backend, namespace, options);
        }

        pub fn sync(self: *@This(), force: bool) !void {
            if (@hasDecl(BackendType, "sync")) {
                try self.backend.sync(force);
            }
        }

        pub fn syncReplayState(self: *@This()) !void {
            if (@hasDecl(BackendType, "syncReplayState")) {
                try self.backend.syncReplayState();
            } else {
                try self.sync(false);
            }
        }

        pub fn beginBulkIngestSession(self: *@This()) !void {
            if (@hasDecl(BackendType, "beginBulkIngestSession")) {
                try self.backend.beginBulkIngestSession();
            }
        }

        pub fn finishBulkIngestSessionWithOptions(self: *@This(), options: backend_types.BulkIngestFinishOptions) !void {
            if (@hasDecl(BackendType, "finishBulkIngestSessionWithOptions")) {
                try self.backend.finishBulkIngestSessionWithOptions(options);
            } else if (@hasDecl(BackendType, "finishBulkIngestSession")) {
                try self.backend.finishBulkIngestSession();
            }
        }

        pub fn abortBulkIngestSession(self: *@This()) void {
            if (@hasDecl(BackendType, "abortBulkIngestSession")) {
                self.backend.abortBulkIngestSession();
            }
        }
    };
}

fn forEachReplayLaneFromRangeSnapshot(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    lane_ordinal: u8,
    from_sequence: u64,
    max_entries: usize,
    ctx: *anyopaque,
    callback: backend_erased.Store.ReplayCallback,
) !backend_types.ReplayLaneIterationStats {
    const LocalCursor = MergeCursor(BackendType, State);
    const lower = internal_keys.replayRangeLower(lane_ordinal, from_sequence);
    const upper = internal_keys.replayRangeUpper(lane_ordinal);

    var mutable_range: State = .{};
    var layout: CurrentReadLayout(BackendType) = blk: {
        const locked = lockBackend(BackendType, backend);
        defer unlockBackend(BackendType, backend, locked);
        mutable_range = try backend.cloneReplayLaneMutableRange(namespace, lower[0..], upper[0..]);
        errdefer mutable_range.deinit(backend.allocator);
        break :blk try CurrentReadLayout(BackendType).capture(backend, backend.allocator);
    };
    defer mutable_range.deinit(backend.allocator);
    defer layout.deinitAfterUnlockedRead();
    try layout.prepare();

    var cursor = try LocalCursor.init(
        layout.metadata_allocator,
        backend,
        &mutable_range,
        layout.immutable_memtables,
        layout.runs,
        layout.l0_groups,
        layout.levels,
        namespace,
        false,
    );
    defer cursor.close();
    cursor.boundPersistedRunBlockResidency();
    cursor.setUpperBound(upper[0..]);

    var stats = backend_types.ReplayLaneIterationStats{ .scan_batches = 1 };
    var entry = try cursor.seekAtOrAfter(lower[0..]);
    while (entry) |kv| {
        if (std.mem.order(u8, kv.key, upper[0..]) != .lt) break;
        const sequence = internal_keys.parseReplayEntrySequence(kv.key, lane_ordinal) orelse break;
        try callback(ctx, sequence, kv.value);
        stats.scanned_entries += 1;
        stats.matched_entries += 1;
        stats.last_sequence = sequence;
        if (max_entries != 0 and stats.matched_entries >= max_entries) break;
        entry = try cursor.next();
    }
    return stats;
}

pub fn BoundCursor(comptime StateType: type) type {
    return struct {
        state: *const StateType,
        namespace: backend_types.Namespace,
        current: ?usize = null,
        upper_bound: ?[]const u8 = null,

        pub fn close(_: *@This()) void {}

        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            self.upper_bound = upper;
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            const idx = self.firstIndex() orelse return null;
            self.current = idx;
            return self.entryIfBeforeUpper(idx);
        }

        pub fn last(self: *@This()) !?backend_adapter.Entry {
            const idx = self.lastIndex() orelse return null;
            self.current = idx;
            return self.state.entryAt(idx).entry();
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            const current = self.current orelse return null;
            var idx = current + 1;
            while (idx < self.state.entryCount()) : (idx += 1) {
                if (compareNamespace(namespaceOf(self.state.entryAt(idx)), self.namespace) == .eq) {
                    self.current = idx;
                    return self.entryIfBeforeUpper(idx);
                }
            }
            return null;
        }

        pub fn prev(self: *@This()) !?backend_adapter.Entry {
            const current = self.current orelse return null;
            if (current == 0) return null;
            var idx = current - 1;
            while (true) {
                if (compareNamespace(namespaceOf(self.state.entryAt(idx)), self.namespace) == .eq) {
                    self.current = idx;
                    return self.state.entryAt(idx).entry();
                }
                if (idx == 0) break;
                idx -= 1;
            }
            return null;
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            const idx = self.state.lowerBound(self.namespace, key);
            if (idx >= self.state.entryCount()) return null;
            if (compareNamespace(namespaceOf(self.state.entryAt(idx)), self.namespace) != .eq) return null;
            self.current = idx;
            return self.entryIfBeforeUpper(idx);
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            const idx = self.state.lowerBound(self.namespace, key);
            if (idx < self.state.entryCount() and compareEntryTo(self.state.entryAt(idx), self.namespace, key) == .eq) {
                self.current = idx;
                return self.state.entryAt(idx).entry();
            }
            if (idx == 0) return null;
            var probe = idx - 1;
            while (true) {
                if (compareNamespace(namespaceOf(self.state.entryAt(probe)), self.namespace) == .eq) {
                    self.current = probe;
                    return self.state.entryAt(probe).entry();
                }
                if (probe == 0) break;
                probe -= 1;
            }
            return null;
        }

        fn firstIndex(self: *const @This()) ?usize {
            const idx = self.state.lowerBound(self.namespace, "");
            if (idx >= self.state.entryCount()) return null;
            if (compareNamespace(namespaceOf(self.state.entryAt(idx)), self.namespace) != .eq) return null;
            return idx;
        }

        fn lastIndex(self: *const @This()) ?usize {
            if (self.state.entryCount() == 0) return null;
            var idx = self.state.entryCount();
            while (idx > 0) {
                idx -= 1;
                if (compareNamespace(namespaceOf(self.state.entryAt(idx)), self.namespace) == .eq) return idx;
            }
            return null;
        }

        fn entryIfBeforeUpper(self: *const @This(), idx: usize) ?backend_adapter.Entry {
            const entry = self.state.entryAt(idx).entry();
            if (!self.keyBeforeUpper(entry.key)) return null;
            return entry;
        }

        fn keyBeforeUpper(self: *const @This(), key: []const u8) bool {
            const upper = self.upper_bound orelse return true;
            return std.mem.order(u8, key, upper) == .lt;
        }
    };
}

pub fn MergeCursor(comptime BackendType: type, comptime MutableType: type) type {
    return struct {
        const Self = @This();
        const Directory = @import("run_directory.zig").Directory;
        const RunSpan = struct {
            start: usize = 0,
            end: usize = 0,
            current: usize = 0,
            // Cache hints are cursor-local. Never mutate a published payload.
            descriptor: ?Run = null,
        };
        const RunSequence = struct {
            directory: ?*const Directory = null,
            runs: []Run = &.{},
            levels: []const RunLevel = &.{},

            fn count(self: @This()) usize {
                return if (self.directory) |directory| directory.count() else self.runs.len;
            }

            fn at(self: @This(), rank: usize) Run {
                return if (self.directory) |directory| directory.at(rank).run.* else self.runs[rank];
            }

            fn end(self: @This(), start: usize) usize {
                const run = self.at(start);
                if (run.level == 0) return start + 1;
                if (self.directory) |directory| return start + directory.levelStats(run.level).count;
                return spanEnd(self.runs, self.levels, start);
            }

            fn contains(self: @This(), start: usize, finish: usize, namespace: backend_types.Namespace) bool {
                return compareNamespace(.{ .name = self.at(start).smallest_namespace_name }, namespace) != .gt and
                    compareNamespace(.{ .name = self.at(finish - 1).largest_namespace_name }, namespace) != .lt;
            }
        };
        const SourceEntry = struct {
            namespace_name: ?[]const u8,
            key: []const u8,
            value: []const u8,
            tombstone: bool,
        };
        const cursor_storage_alignment = @max(
            @max(@max(@alignOf(?usize), @alignOf(?SourceEntry)), @alignOf(?[]u8)),
            @max(
                @alignOf(SourceBlockLease),
                @max(
                    @alignOf(?*const lsm_table_file.TableIndex),
                    @max(@alignOf(?cache_mod.Handle), @alignOf(usize)),
                ),
            ),
        );
        const default_max_retained_mutable_source_entry_scratch: usize = 1 * 1024 * 1024;
        const min_retained_mutable_source_entry_scratch: usize = 4096;

        admission: read_admission.Scope = .{},
        // Ownership mode survives operation exit; no borrowed port survives.
        defer_pin_returns: bool = false,
        allocator: Allocator,
        backend: *BackendType,
        mutable: *const MutableType,
        immutable_memtables: []const *const State = &.{},
        runs: []Run,
        l0_groups: []const RunGroup,
        levels: []const RunLevel,
        sequence: RunSequence = .{},
        namespace: backend_types.Namespace,
        positions: []?usize,
        source_entries: []?SourceEntry,
        source_key_copies: []?[]u8 = &.{},
        source_blocks: []SourceBlockLease,
        source_run_leases: []?*RunSourceLease = &.{},
        source_result_retention: []ResultBlockRetention = &.{},
        source_block_indices: []?usize,
        source_table_indices: []?*const lsm_table_file.TableIndex,
        source_table_index_handles: []?cache_mod.Handle,
        advance_sources: []usize,
        source_heap: []usize,
        source_heap_positions: []?usize,
        run_spans: []RunSpan = &.{},
        source_heap_len: usize = 0,
        cursor_storage: []align(cursor_storage_alignment) u8 = &.{},
        cursor_reservation: ?@import("../resource_manager.zig").Reservation = null,
        visible_entry_bytes: VisibleBytes = .none,
        mutable_source_entry_bytes: ?[]u8 = null,
        mutable_entry_cursor: State.EntryCursor = .{},
        current_key: ?[]const u8 = null,
        test_full_forward_seek: if (builtin.is_test) bool else void = if (builtin.is_test) false else {},
        test_seek_sources: if (builtin.is_test) usize else void = if (builtin.is_test) 0 else {},
        current_visible_source: ?usize = null,
        upper_bound: ?[]const u8 = null,
        backend_locked: bool = false,
        record_scan_value_stats: bool = true,
        keys_only: bool = false,
        bounded_run_block_residency: bool = false,
        resident_run_source: ?usize = null,

        fn cursorStorageSize(source_count: usize) usize {
            var offset: usize = 0;
            cursorStorageAdvance(?usize, &offset, source_count);
            cursorStorageAdvance(?SourceEntry, &offset, source_count);
            cursorStorageAdvance(?[]u8, &offset, source_count);
            cursorStorageAdvance(SourceBlockLease, &offset, source_count);
            cursorStorageAdvance(?*RunSourceLease, &offset, source_count);
            cursorStorageAdvance(ResultBlockRetention, &offset, source_count);
            cursorStorageAdvance(?usize, &offset, source_count);
            cursorStorageAdvance(?*const lsm_table_file.TableIndex, &offset, source_count);
            cursorStorageAdvance(?cache_mod.Handle, &offset, source_count);
            cursorStorageAdvance(usize, &offset, source_count);
            cursorStorageAdvance(usize, &offset, source_count);
            cursorStorageAdvance(?usize, &offset, source_count);
            cursorStorageAdvance(RunSpan, &offset, source_count);
            return offset;
        }

        /// L0 inputs may overlap. Each lower level is a sorted, disjoint run
        /// sequence and needs only one active SST, not one source per file.
        fn spanEnd(runs: []const Run, levels: []const RunLevel, start: usize) usize {
            if (runs[start].level == 0) return start + 1;
            for (levels) |level| if (level.start_index == start) return start + level.len;
            var end = start + 1;
            while (end < runs.len and runs[end].level == runs[start].level) : (end += 1) {}
            return end;
        }

        fn spanContainsNamespace(runs: []const Run, start: usize, end: usize, namespace: backend_types.Namespace) bool {
            return compareNamespace(.{ .name = runs[start].smallest_namespace_name }, namespace) != .gt and
                compareNamespace(.{ .name = runs[end - 1].largest_namespace_name }, namespace) != .lt;
        }

        fn cursorStorageAdvance(comptime T: type, offset: *usize, count: usize) void {
            offset.* = std.mem.alignForward(usize, offset.*, @alignOf(T));
            offset.* += @sizeOf(T) * count;
        }

        fn allocCursorStorage(allocator: Allocator, source_count: usize) ![]align(cursor_storage_alignment) u8 {
            const size = cursorStorageSize(source_count);
            if (size == 0) return &.{};
            return try allocator.alignedAlloc(u8, std.mem.Alignment.fromByteUnits(cursor_storage_alignment), size);
        }

        fn cursorStorageSlice(
            comptime T: type,
            storage: []align(cursor_storage_alignment) u8,
            offset: *usize,
            count: usize,
        ) []T {
            offset.* = std.mem.alignForward(usize, offset.*, @alignOf(T));
            const len = @sizeOf(T) * count;
            const bytes: []align(@alignOf(T)) u8 = @alignCast(storage[offset.*..][0..len]);
            offset.* += len;
            return std.mem.bytesAsSlice(T, bytes);
        }

        pub fn init(
            allocator: Allocator,
            backend: *BackendType,
            mutable: *const MutableType,
            immutable_memtables: []const *const State,
            runs: []Run,
            l0_groups: []const RunGroup,
            levels: []const RunLevel,
            namespace: backend_types.Namespace,
            backend_locked: bool,
        ) !Self {
            return initSequence(allocator, backend, mutable, immutable_memtables, .{ .runs = runs, .levels = levels }, l0_groups, namespace, backend_locked);
        }

        /// The caller owns the directory pin for the cursor's lifetime. Cold
        /// setup visits L0 sources and level boundaries, not every lower SST.
        pub fn initDirectory(
            allocator: Allocator,
            backend: *BackendType,
            mutable: *const MutableType,
            immutable_memtables: []const *const State,
            directory: *const Directory,
            namespace: backend_types.Namespace,
            backend_locked: bool,
        ) !Self {
            return initSequence(allocator, backend, mutable, immutable_memtables, .{ .directory = directory }, &.{}, namespace, backend_locked);
        }

        fn initView(allocator: Allocator, backend: *BackendType, mutable: *const MutableType, immutable_memtables: []const *const State, view: RunReadView, namespace: backend_types.Namespace, backend_locked: bool) !Self {
            if (view.directory()) |directory| return initDirectory(allocator, backend, mutable, immutable_memtables, directory, namespace, backend_locked);
            return init(allocator, backend, mutable, immutable_memtables, view.runs, view.l0_groups, view.levels, namespace, backend_locked);
        }

        fn initSequence(
            allocator: Allocator,
            backend: *BackendType,
            mutable: *const MutableType,
            immutable_memtables: []const *const State,
            sequence: RunSequence,
            l0_groups: []const RunGroup,
            namespace: backend_types.Namespace,
            backend_locked: bool,
        ) !Self {
            return initSequenceControlled(allocator, backend, mutable, immutable_memtables, sequence, l0_groups, namespace, backend_locked, null);
        }

        fn initSequenceControlled(
            allocator: Allocator,
            backend: *BackendType,
            mutable: *const MutableType,
            immutable_memtables: []const *const State,
            sequence: RunSequence,
            l0_groups: []const RunGroup,
            namespace: backend_types.Namespace,
            backend_locked: bool,
            control: ?read_admission.Port,
        ) !Self {
            if (control) |budget| try budget.checkpoint();
            var source_count = 1 + immutable_memtables.len;
            var run_start: usize = 0;
            while (run_start < sequence.count()) {
                if (control) |budget| try budget.checkpoint();
                const end = sequence.end(run_start);
                if (sequence.contains(run_start, end, namespace)) source_count += 1;
                run_start = end;
            }
            var reservation: ?@import("../resource_manager.zig").Reservation = null;
            errdefer if (reservation) |*lease| lease.release();
            if (comptime @hasField(BackendType, "options")) {
                if (comptime @hasField(@TypeOf(backend.options), "resource_manager")) {
                    if (backend.options.resource_manager) |manager| {
                        reservation = try manager.reserve(.lsm_in_memory_state, cursorStorageSize(source_count));
                    }
                }
            }
            if (control) |budget| try budget.checkpoint();
            const storage = try allocCursorStorage(allocator, source_count);
            errdefer allocator.free(storage);

            var offset: usize = 0;
            const positions = cursorStorageSlice(?usize, storage, &offset, source_count);
            @memset(positions, null);
            const source_entries = cursorStorageSlice(?SourceEntry, storage, &offset, source_count);
            @memset(source_entries, null);
            const source_key_copies = cursorStorageSlice(?[]u8, storage, &offset, source_count);
            @memset(source_key_copies, null);
            const source_blocks = cursorStorageSlice(SourceBlockLease, storage, &offset, source_count);
            @memset(source_blocks, .none);
            const source_run_leases = cursorStorageSlice(?*RunSourceLease, storage, &offset, source_count);
            @memset(source_run_leases, null);
            const source_result_retention = cursorStorageSlice(ResultBlockRetention, storage, &offset, source_count);
            @memset(source_result_retention, .unknown);
            const source_block_indices = cursorStorageSlice(?usize, storage, &offset, source_count);
            @memset(source_block_indices, null);
            const source_table_indices = cursorStorageSlice(?*const lsm_table_file.TableIndex, storage, &offset, source_count);
            @memset(source_table_indices, null);
            const source_table_index_handles = cursorStorageSlice(?cache_mod.Handle, storage, &offset, source_count);
            @memset(source_table_index_handles, null);
            const advance_sources = cursorStorageSlice(usize, storage, &offset, source_count);
            const source_heap = cursorStorageSlice(usize, storage, &offset, source_count);
            const source_heap_positions = cursorStorageSlice(?usize, storage, &offset, source_count);
            @memset(source_heap_positions, null);
            const run_spans = cursorStorageSlice(RunSpan, storage, &offset, source_count);
            @memset(run_spans, .{});
            run_start = 0;
            var source_index = 1 + immutable_memtables.len;
            while (run_start < sequence.count()) {
                if (control) |budget| try budget.checkpoint();
                const end = sequence.end(run_start);
                if (sequence.contains(run_start, end, namespace)) {
                    run_spans[source_index] = .{ .start = run_start, .end = end, .current = run_start };
                    source_index += 1;
                }
                run_start = end;
            }

            if (control) |budget| try budget.checkpoint();
            return .{
                .allocator = allocator,
                .backend = backend,
                .mutable = mutable,
                .immutable_memtables = immutable_memtables,
                .runs = sequence.runs,
                .l0_groups = l0_groups,
                .levels = sequence.levels,
                .sequence = sequence,
                .namespace = namespace,
                .positions = positions,
                .source_entries = source_entries,
                .source_key_copies = source_key_copies,
                .source_blocks = source_blocks,
                .source_run_leases = source_run_leases,
                .source_result_retention = source_result_retention,
                .source_block_indices = source_block_indices,
                .source_table_indices = source_table_indices,
                .source_table_index_handles = source_table_index_handles,
                .advance_sources = advance_sources,
                .source_heap = source_heap,
                .source_heap_positions = source_heap_positions,
                .run_spans = run_spans,
                .cursor_storage = storage,
                .cursor_reservation = reservation,
                .backend_locked = backend_locked,
            };
        }

        fn returnCachePin(self: *@This(), handle: *cache_mod.Handle) void {
            if (self.defer_pin_returns) handle.releaseToCache() else handle.release();
        }

        pub fn setKeysOnly(self: *@This()) void {
            self.keys_only = true;
            self.record_scan_value_stats = false;
            self.boundPersistedRunBlockResidency();
        }

        pub fn close(self: *@This()) void {
            defer if (self.cursor_reservation) |*lease| lease.release();
            for (0..self.source_blocks.len) |source_index| self.clearSourceBlock(source_index);
            for (0..self.source_run_leases.len) |source_index| self.clearSourceRunLease(source_index);
            if (comptime @hasDecl(BackendType, "trimRunSources")) self.backend.trimRunSources();
            for (0..self.source_key_copies.len) |source_index| self.clearSourceKeyCopy(source_index);
            for (self.source_table_index_handles) |*maybe_handle| {
                if (maybe_handle.*) |*handle| self.returnCachePin(handle);
                maybe_handle.* = null;
            }
            self.clearVisibleEntryBytes();
            self.clearMutableSourceEntryBytes();
            if (self.cursor_storage.len > 0) {
                self.allocator.free(self.cursor_storage);
            } else {
                self.allocator.free(self.source_block_indices);
                self.allocator.free(self.source_blocks);
                self.allocator.free(self.source_run_leases);
                self.allocator.free(self.source_result_retention);
                self.allocator.free(self.source_entries);
                self.allocator.free(self.source_key_copies);
                self.allocator.free(self.positions);
                self.allocator.free(self.source_table_indices);
                self.allocator.free(self.source_table_index_handles);
                self.allocator.free(self.advance_sources);
                self.allocator.free(self.source_heap);
                self.allocator.free(self.source_heap_positions);
            }
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            try self.initForwardPositions("", true);
            const entry = try self.selectVisibleForward();
            self.current_key = if (entry) |e| e.key else null;
            return entry;
        }

        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            self.upper_bound = upper;
        }

        /// Bounds decoded persisted-run data residency to the run currently
        /// winning the merge. Inactive runs retain only their current key for
        /// heap ordering. This is intended for synchronous streaming consumers
        /// such as replay, which do not retain returned values across `next`.
        pub fn boundPersistedRunBlockResidency(self: *@This()) void {
            std.debug.assert(self.current_key == null);
            std.debug.assert(self.source_heap_len == 0);
            self.bounded_run_block_residency = true;
        }

        pub fn last(self: *@This()) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            const entry = try self.findLast();
            if (entry) |e| {
                try self.initForwardPositions(e.key, true);
                self.current_key = e.key;
            } else {
                self.current_key = null;
            }
            return entry;
        }

        pub fn nextWithAdmission(self: *@This(), control: read_admission.Port) !?backend_adapter.Entry {
            const guard = try self.admission.enter(control);
            self.defer_pin_returns = true;
            defer guard.deinit();
            const result = try self.next();
            try self.admission.checkpoint();
            return result;
        }
        pub fn seekAtOrAfterWithAdmission(self: *@This(), key: []const u8, control: read_admission.Port) !?backend_adapter.Entry {
            const guard = try self.admission.enter(control);
            self.defer_pin_returns = true;
            defer guard.deinit();
            const result = try self.seekAtOrAfter(key);
            try self.admission.checkpoint();
            return result;
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            if (self.current_key == null) return null;
            try self.advanceForwardSources();
            const entry = try self.selectVisibleForward();
            self.current_key = if (entry) |e| e.key else null;
            return entry;
        }

        pub fn prev(self: *@This()) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            const key = self.current_key orelse return null;
            const entry = try self.findAtOrBefore(key, false);
            if (entry) |e| {
                try self.initForwardPositions(e.key, true);
                self.current_key = e.key;
            } else {
                self.current_key = null;
            }
            return entry;
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            const restart = if (comptime builtin.is_test) self.test_full_forward_seek else false;
            if (!restart and self.current_key != null) {
                const current = self.current_key.?;
                if (std.mem.order(u8, key, current) != .lt) {
                    try self.skipForwardTo(key);
                } else {
                    try self.initForwardPositions(key, true);
                }
            } else {
                try self.initForwardPositions(key, true);
            }
            const entry = try self.selectVisibleForward();
            self.current_key = if (entry) |e| e.key else null;
            return entry;
        }

        /// Monotone seeks only move sources that precede the requested key.
        /// The heap already proves every other source is at or beyond it;
        /// those sources retain their block, position, and heap membership.
        /// Stabilize the target because it may alias a source's borrowed key.
        fn skipForwardTo(self: *@This(), target: []const u8) !void {
            try self.admission.checkpoint();
            if (self.source_heap_len == 0) return;
            if (std.mem.order(u8, self.source_entries[self.source_heap[0]].?.key, target) != .lt) return;
            const stable_target = try self.allocator.dupe(u8, target);
            defer self.allocator.free(stable_target);
            while (self.source_heap_len != 0) {
                try self.admission.checkpoint();
                const source = self.source_heap[0];
                if (std.mem.order(u8, self.source_entries[source].?.key, stable_target) != .lt) break;
                try self.setSourceAtOrAfter(source, stable_target, true);
                self.updateForwardHeapSource(source);
            }
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            const entry = try self.findAtOrBefore(key, true);
            if (entry) |e| {
                try self.initForwardPositions(e.key, true);
                self.current_key = e.key;
            } else {
                self.current_key = null;
            }
            return entry;
        }

        fn initForwardPositions(self: *@This(), target: []const u8, inclusive: bool) !void {
            try self.admission.checkpoint();
            // A public seek may reuse the previous entry's borrowed key. A
            // source switch can release its backing block before other spans
            // have sought, so stabilize nonempty seek keys across all sources.
            const stable_target = try self.allocator.dupe(u8, target);
            defer self.allocator.free(stable_target);
            for (0..self.positions.len) |source_index| {
                try self.admission.checkpoint();
                try self.setSourceAtOrAfter(source_index, stable_target, inclusive);
            }
            self.rebuildForwardHeap();
        }

        fn selectVisibleForward(self: *@This()) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            while (true) {
                try self.admission.checkpoint();
                const winner_source = self.bestVisibleForwardSource() orelse {
                    self.current_visible_source = null;
                    return null;
                };
                if (!self.keyBeforeUpper(self.source_entries[winner_source].?.key)) {
                    self.current_visible_source = null;
                    return null;
                }
                try self.makeRunSourceResident(winner_source);
                const entry = self.source_entries[winner_source].?;
                if (!entry.tombstone) {
                    self.current_visible_source = winner_source;
                    self.recordVisibleValueStat(winner_source);
                    return .{ .key = entry.key, .value = if (self.keys_only) "" else entry.value };
                }
                self.current_visible_source = null;
                try self.advanceForwardSourcesAtKey(entry.key);
            }
        }

        fn recordVisibleValueStat(self: *@This(), source_index: usize) void {
            if (!self.record_scan_value_stats) return;
            if (source_index == 0 and comptime MutableType == ActiveMemTable) {
                recordCursorValueCopy(self.backend);
            } else {
                recordCursorValueBorrow(self.backend);
            }
        }

        pub fn retainCurrentValueForTxn(self: *@This(), held_blocks: *std.ArrayListUnmanaged(BlockPin)) !bool {
            try self.admission.checkpoint();
            const source_index = self.current_visible_source orelse return false;
            if (self.source_result_retention.len != 0) switch (self.source_result_retention[source_index]) {
                .pinned => return true,
                .copy => return false,
                .unknown => {},
            };
            // Local cache payloads are not backed by the external cache's pin
            // budget. Bound retained amplification per result owner, then let
            // the caller copy values. The count cap also bounds this scan.
            if (self.source_blocks[source_index] == .local) {
                const pinned = try retainLocalResultPin(self.backend, self.source_blocks[source_index].local, held_blocks);
                if (self.source_result_retention.len != 0) self.source_result_retention[source_index] = if (pinned) .pinned else .copy;
                return pinned;
            }
            if (self.source_blocks[source_index].retainPin()) |retained_block| {
                var retained = retained_block;
                errdefer retained.releaseForAdmittedRead(self.defer_pin_returns);
                try held_blocks.append(self.backend.allocator, retained);
                if (self.source_result_retention.len != 0) self.source_result_retention[source_index] = .pinned;
                return true;
            }

            if (source_index == 0) {
                if (comptime MutableType == ActiveMemTable) return false;
                return true;
            }
            if (self.immutableForSource(source_index) != null) return true;

            const run = try self.runForSource(source_index);
            if (run.state != null) return true;
            return run.path == null;
        }

        fn bestVisibleForwardSource(self: *@This()) ?usize {
            if (self.source_heap_len == 0) return null;
            return self.source_heap[0];
        }

        fn rebuildForwardHeap(self: *@This()) void {
            @memset(self.source_heap_positions, null);
            self.source_heap_len = 0;
            for (0..self.positions.len) |source_index| {
                if (!self.sourceIsForwardActive(source_index)) continue;
                self.heapInsert(source_index);
            }
        }

        fn updateForwardHeapSource(self: *@This(), source_index: usize) void {
            if (!self.sourceIsForwardActive(source_index)) {
                self.heapRemove(source_index);
                return;
            }
            if (self.source_heap_positions[source_index]) |heap_index| {
                self.heapFix(heap_index);
            } else {
                self.heapInsert(source_index);
            }
        }

        fn sourceIsForwardActive(self: *const @This(), source_index: usize) bool {
            return self.positions[source_index] != null and self.source_entries[source_index] != null;
        }

        fn heapInsert(self: *@This(), source_index: usize) void {
            const heap_index = self.source_heap_len;
            self.source_heap_len += 1;
            self.source_heap[heap_index] = source_index;
            self.source_heap_positions[source_index] = heap_index;
            self.heapSiftUp(heap_index);
        }

        fn heapRemove(self: *@This(), source_index: usize) void {
            const heap_index = self.source_heap_positions[source_index] orelse return;
            self.source_heap_positions[source_index] = null;
            self.source_heap_len -= 1;
            if (heap_index == self.source_heap_len) return;
            const moved_source = self.source_heap[self.source_heap_len];
            self.source_heap[heap_index] = moved_source;
            self.source_heap_positions[moved_source] = heap_index;
            self.heapFix(heap_index);
        }

        fn heapFix(self: *@This(), heap_index: usize) void {
            if (heap_index > 0) {
                const parent = (heap_index - 1) / 2;
                if (self.sourceBeats(self.source_heap[heap_index], self.source_heap[parent])) {
                    self.heapSiftUp(heap_index);
                    return;
                }
            }
            self.heapSiftDown(heap_index);
        }

        fn heapSiftUp(self: *@This(), start_index: usize) void {
            var child = start_index;
            while (child > 0) {
                const parent = (child - 1) / 2;
                if (!self.sourceBeats(self.source_heap[child], self.source_heap[parent])) break;
                self.heapSwap(child, parent);
                child = parent;
            }
        }

        fn heapSiftDown(self: *@This(), start_index: usize) void {
            var parent = start_index;
            while (true) {
                const left = parent * 2 + 1;
                if (left >= self.source_heap_len) break;
                const right = left + 1;
                var best = left;
                if (right < self.source_heap_len and self.sourceBeats(self.source_heap[right], self.source_heap[left])) {
                    best = right;
                }
                if (!self.sourceBeats(self.source_heap[best], self.source_heap[parent])) break;
                self.heapSwap(parent, best);
                parent = best;
            }
        }

        fn heapSwap(self: *@This(), lhs: usize, rhs: usize) void {
            const lhs_source = self.source_heap[lhs];
            const rhs_source = self.source_heap[rhs];
            self.source_heap[lhs] = rhs_source;
            self.source_heap[rhs] = lhs_source;
            self.source_heap_positions[lhs_source] = rhs;
            self.source_heap_positions[rhs_source] = lhs;
        }

        fn sourceBeats(self: *const @This(), lhs_source: usize, rhs_source: usize) bool {
            const lhs = self.source_entries[lhs_source].?;
            const rhs = self.source_entries[rhs_source].?;
            return switch (std.mem.order(u8, lhs.key, rhs.key)) {
                .lt => true,
                .eq => lhs_source < rhs_source,
                .gt => false,
            };
        }

        fn advanceForwardSources(self: *@This()) !void {
            try self.admission.checkpoint();
            const key = self.current_key orelse return;
            try self.advanceForwardSourcesAtKey(key);
        }

        fn advanceForwardSourcesAtKey(self: *@This(), key: []const u8) !void {
            try self.admission.checkpoint();
            const match_count = self.collectForwardHeapSourcesAtKey(key);
            for (self.advance_sources[0..match_count]) |source_index| {
                try self.admission.checkpoint();
                const idx = self.positions[source_index] orelse continue;
                try self.advanceSource(source_index, idx);
                self.updateForwardHeapSource(source_index);
            }
        }

        fn collectForwardHeapSourcesAtKey(self: *@This(), key: []const u8) usize {
            if (self.source_heap_len == 0) return 0;
            var match_count: usize = 0;
            var stack_start = self.advance_sources.len;
            stack_start -= 1;
            self.advance_sources[stack_start] = 0;

            while (stack_start < self.advance_sources.len) {
                const heap_index = self.advance_sources[stack_start];
                stack_start += 1;
                if (heap_index >= self.source_heap_len) continue;

                const source_index = self.source_heap[heap_index];
                const entry = self.source_entries[source_index] orelse continue;
                const order = std.mem.order(u8, entry.key, key);
                if (order == .eq) {
                    self.advance_sources[match_count] = source_index;
                    match_count += 1;
                } else if (order == .gt) {
                    continue;
                }

                const left = heap_index * 2 + 1;
                const right = left + 1;
                if (right < self.source_heap_len) {
                    stack_start -= 1;
                    std.debug.assert(stack_start >= match_count);
                    self.advance_sources[stack_start] = right;
                }
                if (left < self.source_heap_len) {
                    stack_start -= 1;
                    std.debug.assert(stack_start >= match_count);
                    self.advance_sources[stack_start] = left;
                }
            }
            return match_count;
        }

        fn runSourceOffset(self: *const @This()) usize {
            return 1 + self.immutable_memtables.len;
        }

        fn immutableForSource(self: *const @This(), source_index: usize) ?*const State {
            if (source_index == 0 or source_index >= self.runSourceOffset()) return null;
            return self.immutable_memtables[source_index - 1];
        }

        fn runForSource(self: *@This(), source_index: usize) !*Run {
            try self.admission.checkpoint();
            const offset = self.runSourceOffset();
            if (source_index < offset) return error.RunStateUnavailable;
            const span = &self.run_spans[source_index];
            if (span.current >= self.sequence.count()) return error.RunStateUnavailable;
            if (span.descriptor == null) {
                span.descriptor = self.sequence.at(span.current);
                if (self.sequence.directory != null) span.descriptor.?.shared_read_version = true;
            }
            return &span.descriptor.?;
        }

        fn selectSpanRun(self: *@This(), source_index: usize, run_index: usize) void {
            const span = &self.run_spans[source_index];
            if (span.current == run_index) return;
            self.clearSourceBlock(source_index);
            self.clearSourceRunLease(source_index);
            if (self.source_table_index_handles[source_index]) |*handle| self.returnCachePin(handle);
            self.source_table_index_handles[source_index] = null;
            self.source_table_indices[source_index] = null;
            self.source_entries[source_index] = null;
            self.positions[source_index] = null;
            span.current = run_index;
            span.descriptor = null;
        }

        fn runStartsPastUpper(self: *const @This(), run: Run) bool {
            const ns = compareNamespace(.{ .name = run.smallest_namespace_name }, self.namespace);
            return ns == .gt or (ns == .eq and !self.keyBeforeUpper(run.smallest_key));
        }

        fn seekRunSpan(self: *@This(), source_index: usize, target: []const u8, inclusive: bool) !void {
            try self.admission.checkpoint();
            const span = self.run_spans[source_index];
            var lo = span.start;
            var hi = span.end;
            if (self.sequence.directory != null and self.sequence.at(span.start).level != 0) {
                lo = self.sequence.directory.?.levelBoundRank(self.sequence.at(span.start).level, self.namespace.name, target, false, inclusive);
            } else while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const run = self.sequence.at(mid);
                const order = compareRunBound(run.largest_namespace_name, run.largest_key, self.namespace.name, target);
                if (order == .lt or (!inclusive and order == .eq)) lo = mid + 1 else hi = mid;
            }
            self.positions[source_index] = null;
            self.source_entries[source_index] = null;
            while (lo < span.end) : (lo += 1) {
                try self.admission.checkpoint();
                if (self.runStartsPastUpper(self.sequence.at(lo))) break;
                self.selectSpanRun(source_index, lo);
                try self.setSingleRunAtOrAfter(source_index, target, inclusive);
                if (self.positions[source_index] != null) return;
            }
            self.clearSourceBlock(source_index);
        }

        fn tableIndexForRunSource(self: *@This(), source_index: usize, run: *Run) !*const lsm_table_file.TableIndex {
            try self.admission.checkpoint();
            if (self.source_table_indices[source_index]) |index| {
                recordCursorTableIndexHit(self.backend);
                return index;
            }
            const index = if (self.backend.options.cache != null) blk: {
                var handle = try loadRunTableIndexHandleWithAdmission(self.backend, run, self.admission.control);
                errdefer self.returnCachePin(&handle);
                const retained_index = handle.runTableIndex();
                self.source_table_index_handles[source_index] = handle;
                break :blk retained_index;
            } else try indexForRunNoCacheMaybeLockedWithAdmission(self.backend, run, self.backend_locked, self.admission.control);
            self.source_table_indices[source_index] = index;
            recordCursorTableIndexMiss(self.backend);
            return index;
        }

        fn sourceEntryAt(self: *@This(), source_index: usize, idx: usize) !SourceEntry {
            try self.admission.checkpoint();
            if (source_index == 0) {
                if (comptime MutableType == ActiveMemTable) return try self.copyMutableSourceEntryAt(idx);
                const entry = self.mutable_entry_cursor.at(self.mutable, idx);
                return .{ .namespace_name = namespaceOf(entry).name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
            }
            if (self.immutableForSource(source_index)) |state| {
                const entry = state.entryAt(idx);
                return .{ .namespace_name = namespaceOf(entry).name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
            }

            const run = try self.runForSource(source_index);
            if (run.state) |*state| {
                const entry = state.entryAt(idx);
                return .{ .namespace_name = namespaceOf(entry).name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
            }

            if (run.path != null) {
                const index = try self.tableIndexForRunSource(source_index, run);
                return try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
            }

            const table = try tableForRunMaybeLockedWithAdmission(self.backend, run, self.backend_locked, self.admission.control);
            const entry = try table.entryAt(idx);
            return .{ .namespace_name = entry.namespace_name, .key = entry.key, .value = entry.value, .tombstone = entry.tombstone };
        }

        fn sourceLowerBound(self: *@This(), source_index: usize, target: []const u8, inclusive: bool) !?usize {
            try self.admission.checkpoint();
            if (!self.keyBeforeUpper(target)) return null;
            if (source_index == 0) return nextStateIndex(self.mutable, self.namespace, target, inclusive);
            if (self.immutableForSource(source_index)) |state| return nextStateIndex(state, self.namespace, target, inclusive);

            const run = try self.runForSource(source_index);
            if (!runMayContainAtOrAfter(run.*, self.namespace, target)) return null;
            if (run.state) |*state| return nextStateIndex(state, self.namespace, target, inclusive);

            if (run.path != null) {
                const index = try self.tableIndexForRunSource(source_index, run);
                return try self.sourceLowerBoundFromLocalIndex(source_index, run, index, target, inclusive);
            }

            const table = try tableForRunMaybeLockedWithAdmission(self.backend, run, self.backend_locked, self.admission.control);
            var idx = try table.lowerBound(self.namespace.name, target);
            while (idx < table.entryCount()) : (idx += 1) {
                try self.admission.checkpoint();
                const entry = try table.entryAt(idx);
                if (compareNamespace(.{ .name = entry.namespace_name }, self.namespace) != .eq) return null;
                if (!inclusive and std.mem.eql(u8, entry.key, target)) continue;
                return idx;
            }
            return null;
        }

        fn setSourceAtOrAfter(self: *@This(), source_index: usize, target: []const u8, inclusive: bool) !void {
            try self.admission.checkpoint();
            if (comptime builtin.is_test) self.test_seek_sources += 1;
            if (source_index == 0 and comptime MutableType == ActiveMemTable) {
                try self.setMutableSourceAtOrAfter(target, inclusive);
                return;
            }
            if (source_index < self.runSourceOffset()) return try self.setSingleRunAtOrAfter(source_index, target, inclusive);
            try self.seekRunSpan(source_index, target, inclusive);
        }

        fn setSingleRunAtOrAfter(self: *@This(), source_index: usize, target: []const u8, inclusive: bool) !void {
            try self.admission.checkpoint();
            if (source_index == 0 or self.immutableForSource(source_index) != null) {
                self.positions[source_index] = try self.sourceLowerBound(source_index, target, inclusive);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            const run = try self.runForSource(source_index);
            if (self.bounded_run_block_residency and run.state == null and run.path != null) {
                self.resetRunSourceResidency(source_index);
            }
            if (!runMayContainAtOrAfter(run.*, self.namespace, target)) {
                self.clearSourceBlock(source_index);
                self.positions[source_index] = null;
                self.source_entries[source_index] = null;
                return;
            }
            if (run.state != null) {
                self.positions[source_index] = try self.sourceLowerBound(source_index, target, inclusive);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            if (run.path != null) {
                self.positions[source_index] = try self.sourceLowerBound(source_index, target, inclusive);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                if (self.positions[source_index] == null) {
                    self.clearSourceBlock(source_index);
                } else if (self.bounded_run_block_residency) {
                    try self.spillRunSource(source_index);
                }
                return;
            }

            const table = try tableForRunMaybeLockedWithAdmission(self.backend, run, self.backend_locked, self.admission.control);
            if (try table.lowerBoundPosition(self.namespace.name, target, inclusive)) |positioned| {
                self.positions[source_index] = positioned.index;
                self.source_entries[source_index] = .{
                    .namespace_name = positioned.entry.namespace_name,
                    .key = positioned.entry.key,
                    .value = positioned.entry.value,
                    .tombstone = positioned.entry.tombstone,
                };
            } else {
                self.positions[source_index] = null;
                self.source_entries[source_index] = null;
            }
        }

        fn advanceSource(self: *@This(), source_index: usize, current: usize) !void {
            try self.admission.checkpoint();
            try self.advanceSingleSource(source_index, current);
            if (source_index < self.runSourceOffset() or self.positions[source_index] != null) return;
            const span = self.run_spans[source_index];
            var next_run = span.current + 1;
            while (next_run < span.end) : (next_run += 1) {
                try self.admission.checkpoint();
                if (self.runStartsPastUpper(self.sequence.at(next_run))) break;
                self.selectSpanRun(source_index, next_run);
                try self.setSingleRunAtOrAfter(source_index, "", true);
                if (self.positions[source_index] != null) return;
            }
        }

        fn advanceSingleSource(self: *@This(), source_index: usize, current: usize) !void {
            try self.admission.checkpoint();
            if (source_index == 0 and comptime MutableType == ActiveMemTable) {
                try self.advanceMutableSource();
                return;
            }
            if (source_index == 0 and comptime MutableType == State) {
                const idx = current + 1;
                if (idx < self.mutable.entryCount()) {
                    const entry = try self.sourceEntryAt(0, idx);
                    if (compareNamespace(.{ .name = entry.namespace_name }, self.namespace) == .eq) {
                        self.positions[0] = idx;
                        self.source_entries[0] = entry;
                        return;
                    }
                }
                self.positions[0] = null;
                self.source_entries[0] = null;
                return;
            }
            if (source_index == 0 or self.immutableForSource(source_index) != null) {
                self.positions[source_index] = try self.nextSourceIndex(source_index, current);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            const run = try self.runForSource(source_index);
            if (run.state != null) {
                self.positions[source_index] = try self.nextSourceIndex(source_index, current);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                return;
            }

            if (run.path != null) {
                self.positions[source_index] = try self.nextSourceIndex(source_index, current);
                self.source_entries[source_index] = if (self.positions[source_index]) |idx|
                    try self.sourceEntryAt(source_index, idx)
                else
                    null;
                if (self.positions[source_index] == null) {
                    self.clearSourceBlock(source_index);
                    self.clearSourceKeyCopy(source_index);
                    if (self.resident_run_source == source_index) self.resident_run_source = null;
                } else if (self.bounded_run_block_residency and self.resident_run_source != source_index) {
                    try self.spillRunSource(source_index);
                }
                return;
            }

            const table = try tableForRunMaybeLockedWithAdmission(self.backend, run, self.backend_locked, self.admission.control);
            if (try table.nextPositionInNamespace(self.namespace.name, current)) |positioned| {
                self.positions[source_index] = positioned.index;
                self.source_entries[source_index] = .{
                    .namespace_name = positioned.entry.namespace_name,
                    .key = positioned.entry.key,
                    .value = positioned.entry.value,
                    .tombstone = positioned.entry.tombstone,
                };
            } else {
                self.positions[source_index] = null;
                self.source_entries[source_index] = null;
            }
        }

        fn nextSourceIndex(self: *@This(), source_index: usize, current: usize) !?usize {
            try self.admission.checkpoint();
            if (source_index == 0) return nextIndexFrom(self.mutable, self.namespace, current);
            if (self.immutableForSource(source_index)) |state| return nextIndexFrom(state, self.namespace, current);

            const run = try self.runForSource(source_index);
            if (run.state) |*state| return nextIndexFrom(state, self.namespace, current);

            if (run.path != null) {
                return try self.nextSourceIndexFromLocalIndex(source_index, run, current);
            }

            const table = try tableForRunMaybeLockedWithAdmission(self.backend, run, self.backend_locked, self.admission.control);
            var idx = current + 1;
            while (idx < table.entryCount()) : (idx += 1) {
                try self.admission.checkpoint();
                const entry = try table.entryAt(idx);
                const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                if (order == .eq) return idx;
                if (order == .gt) return null;
            }
            return null;
        }

        fn findAtOrBefore(self: *@This(), target: []const u8, inclusive: bool) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            var probe = target;
            var owned_probe: ?[]u8 = null;
            defer if (owned_probe) |bytes| self.allocator.free(bytes);
            var include_probe = inclusive;
            if (self.upper_bound) |upper| if (std.mem.order(u8, probe, upper) != .lt) {
                probe = upper;
                include_probe = false;
            };
            while (true) {
                try self.admission.checkpoint();
                const maybe_candidate = blk: {
                    const stable_probe = try self.allocator.dupe(u8, probe);
                    defer self.allocator.free(stable_probe);
                    break :blk try self.prevCandidateKey(stable_probe, include_probe);
                };
                const candidate = maybe_candidate orelse return null;
                const stable_candidate = try self.allocator.dupe(u8, candidate);
                const visible = self.visibleEntryAtKey(stable_candidate) catch |err| {
                    self.allocator.free(stable_candidate);
                    return err;
                };
                if (visible) |entry| {
                    self.allocator.free(stable_candidate);
                    return entry;
                }
                if (owned_probe) |bytes| self.allocator.free(bytes);
                owned_probe = stable_candidate;
                probe = stable_candidate;
                include_probe = false;
            }
        }

        fn findLast(self: *@This()) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            if (self.upper_bound) |upper| return try self.findAtOrBefore(upper, false);
            var best: ?[]const u8 = try self.mutableLastKeyStable();
            for (self.immutable_memtables) |state| {
                try self.admission.checkpoint();
                const concrete = mutableLastKey(state, self.namespace) orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            for (self.runSourceOffset()..self.positions.len) |source_index| {
                try self.admission.checkpoint();
                const concrete = (try self.spanPrevKey(source_index, null, true)) orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            const key = best orelse return null;
            const stable_key = try self.allocator.dupe(u8, key);
            defer self.allocator.free(stable_key);
            return try self.findAtOrBefore(stable_key, true);
        }

        fn prevCandidateKey(self: *@This(), target: []const u8, inclusive: bool) !?[]const u8 {
            try self.admission.checkpoint();
            var best: ?[]const u8 = try self.mutablePrevStateKeyStable(target, inclusive);
            for (self.immutable_memtables) |state| {
                try self.admission.checkpoint();
                const concrete = prevStateKey(state, self.namespace, target, inclusive) orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            for (self.runSourceOffset()..self.positions.len) |source_index| {
                try self.admission.checkpoint();
                const concrete = (try self.spanPrevKey(source_index, target, inclusive)) orelse continue;
                if (best == null or std.mem.order(u8, concrete, best.?) == .gt) best = concrete;
            }
            return best;
        }

        fn spanPrevKey(self: *@This(), source_index: usize, target: ?[]const u8, inclusive: bool) !?[]const u8 {
            try self.admission.checkpoint();
            const span = self.run_spans[source_index];
            var lo = span.start;
            var hi = span.end;
            if (self.sequence.directory != null and self.sequence.at(span.start).level != 0) {
                lo = self.sequence.directory.?.levelBoundRank(self.sequence.at(span.start).level, self.namespace.name, target, true, inclusive);
            } else while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const run = self.sequence.at(mid);
                const order = if (target) |key|
                    compareRunBound(run.smallest_namespace_name, run.smallest_key, self.namespace.name, key)
                else
                    compareNamespace(.{ .name = run.smallest_namespace_name }, self.namespace);
                if (order == .lt or (order == .eq and (target == null or inclusive))) lo = mid + 1 else hi = mid;
            }
            while (lo > span.start) {
                try self.admission.checkpoint();
                lo -= 1;
                if (compareNamespace(.{ .name = self.sequence.at(lo).largest_namespace_name }, self.namespace) == .lt) break;
                self.selectSpanRun(source_index, lo);
                const run = try self.runForSource(source_index);
                const candidate = if (run.state) |*state|
                    if (target) |key| prevStateKey(state, self.namespace, key, inclusive) else mutableLastKey(state, self.namespace)
                else if (run.path != null)
                    if (target) |key| try self.sourcePrevKeyFromLocalIndex(source_index, run, key, inclusive) else try self.sourceLastKeyFromLocalIndex(source_index, run)
                else
                    null;
                if (candidate) |key| return key;
            }
            return null;
        }

        fn visibleEntryAtKey(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            try self.admission.checkpoint();
            self.clearVisibleEntryBytes();
            if (comptime MutableType == ActiveMemTable) {
                switch (try self.visibleMutableEntryAtKey(key)) {
                    .absent => {},
                    .tombstone => return null,
                    .value => |entry| return entry,
                }
            } else if (self.mutable.findIndex(self.namespace, key)) |idx| {
                const entry = self.mutable.entryAt(idx);
                if (entry.tombstone) return null;
                return .{ .key = entry.key, .value = if (self.keys_only) "" else entry.value };
            }
            for (self.immutable_memtables) |state| {
                try self.admission.checkpoint();
                if (state.findIndex(self.namespace, key)) |idx| {
                    const entry = state.entryAt(idx);
                    if (entry.tombstone) return null;
                    return .{ .key = entry.key, .value = if (self.keys_only) "" else entry.value };
                }
            }
            for (self.runSourceOffset()..self.positions.len) |source_index| {
                try self.admission.checkpoint();
                try self.seekRunSpan(source_index, key, true);
                const entry = self.source_entries[source_index] orelse continue;
                if (!std.mem.eql(u8, entry.key, key)) continue;
                if (entry.tombstone) return null;
                return .{ .key = entry.key, .value = if (self.keys_only) "" else entry.value };
            }
            return null;
        }

        fn clearVisibleEntryBytes(self: *@This()) void {
            self.visible_entry_bytes.release();
        }

        fn clearMutableSourceEntryBytes(self: *@This()) void {
            if (self.mutable_source_entry_bytes) |bytes| self.allocator.free(bytes);
            self.mutable_source_entry_bytes = null;
        }

        fn mutableSourceEntryScratch(self: *@This(), needed: usize) ![]u8 {
            try self.admission.checkpoint();
            const retained_cap = self.maxRetainedMutableSourceEntryScratch();
            var current_capacity: usize = 0;
            if (self.mutable_source_entry_bytes) |bytes| {
                if (bytes.len >= needed) {
                    if (bytes.len <= retained_cap or needed > retained_cap) {
                        return bytes[0..needed];
                    }
                }
                current_capacity = if (bytes.len <= retained_cap) bytes.len else 0;
                self.allocator.free(bytes);
                self.mutable_source_entry_bytes = null;
            }
            const bytes = try self.allocator.alloc(u8, self.mutableSourceEntryScratchCapacity(current_capacity, needed));
            self.mutable_source_entry_bytes = bytes;
            return bytes[0..needed];
        }

        fn mutableSourceEntryScratchCapacity(self: *@This(), current_capacity: usize, needed: usize) usize {
            const retained_cap = self.maxRetainedMutableSourceEntryScratch();
            if (needed > retained_cap) return needed;
            var capacity = @max(current_capacity, min_retained_mutable_source_entry_scratch);
            while (capacity < needed) {
                const next_capacity = capacity * 2;
                if (next_capacity >= retained_cap) return retained_cap;
                capacity = next_capacity;
            }
            return capacity;
        }

        fn maxRetainedMutableSourceEntryScratch(self: *const @This()) usize {
            if (@hasField(BackendType, "options") and @hasField(@TypeOf(self.backend.options), "cursor_scratch_retained_cap_bytes")) {
                return @max(self.backend.options.cursor_scratch_retained_cap_bytes, min_retained_mutable_source_entry_scratch);
            }
            return default_max_retained_mutable_source_entry_scratch;
        }

        fn mutableSourceLock(self: *@This()) bool {
            if (comptime MutableType == ActiveMemTable) {
                if (!self.backend_locked) return lockBackend(BackendType, self.backend);
            }
            return false;
        }

        fn mutableSourceUnlock(self: *@This(), locked: bool) void {
            if (comptime MutableType == ActiveMemTable) {
                unlockBackend(BackendType, self.backend, locked);
            }
        }

        fn copyMutableSourceEntryAt(self: *@This(), idx: usize) !SourceEntry {
            try self.admission.checkpoint();
            const entry = self.mutable.entryAt(idx);
            const namespace_name = namespaceOf(entry).name;
            const namespace_len = if (namespace_name) |name| name.len else 0;
            const bytes = try self.mutableSourceEntryScratch(namespace_len + entry.key.len + (if (self.keys_only) @as(usize, 0) else entry.value.len));
            var offset: usize = 0;
            const copied_namespace = if (namespace_name) |name| blk: {
                @memcpy(bytes[offset..][0..name.len], name);
                const copied = bytes[offset..][0..name.len];
                offset += name.len;
                break :blk copied;
            } else null;
            @memcpy(bytes[offset..][0..entry.key.len], entry.key);
            const copied_key = bytes[offset..][0..entry.key.len];
            offset += entry.key.len;
            const value_len = if (self.keys_only) @as(usize, 0) else entry.value.len;
            @memcpy(bytes[offset..][0..value_len], entry.value[0..value_len]);
            const copied_value = bytes[offset..][0..value_len];

            return .{
                .namespace_name = copied_namespace,
                .key = copied_key,
                .value = copied_value,
                .tombstone = entry.tombstone,
            };
        }

        fn setMutableSourceAtOrAfter(self: *@This(), target: []const u8, inclusive: bool) !void {
            try self.admission.checkpoint();
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const idx = nextStateIndex(self.mutable, self.namespace, target, inclusive) orelse {
                self.positions[0] = null;
                self.source_entries[0] = null;
                self.clearMutableSourceEntryBytes();
                return;
            };
            self.positions[0] = idx;
            self.source_entries[0] = try self.copyMutableSourceEntryAt(idx);
        }

        fn advanceMutableSource(self: *@This()) !void {
            try self.admission.checkpoint();
            const current = self.source_entries[0] orelse {
                self.positions[0] = null;
                self.clearMutableSourceEntryBytes();
                return;
            };
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const idx = nextStateIndex(self.mutable, self.namespace, current.key, false) orelse {
                self.positions[0] = null;
                self.source_entries[0] = null;
                self.clearMutableSourceEntryBytes();
                return;
            };
            self.positions[0] = idx;
            self.source_entries[0] = try self.copyMutableSourceEntryAt(idx);
        }

        fn copyKeyToVisibleBytes(self: *@This(), key: []const u8) ![]const u8 {
            try self.admission.checkpoint();
            const bytes = try self.backend.allocator.dupe(u8, key);
            errdefer self.backend.allocator.free(bytes);
            self.visible_entry_bytes.setOwned(self.backend.allocator, bytes);
            return bytes;
        }

        fn mutableLastKeyStable(self: *@This()) !?[]const u8 {
            try self.admission.checkpoint();
            if (comptime MutableType != ActiveMemTable) return mutableLastKey(self.mutable, self.namespace);
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const key = mutableLastKey(self.mutable, self.namespace) orelse return null;
            return try self.copyKeyToVisibleBytes(key);
        }

        fn mutablePrevStateKeyStable(self: *@This(), target: []const u8, inclusive: bool) !?[]const u8 {
            try self.admission.checkpoint();
            if (comptime MutableType != ActiveMemTable) return prevStateKey(self.mutable, self.namespace, target, inclusive);
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const key = prevStateKey(self.mutable, self.namespace, target, inclusive) orelse return null;
            return try self.copyKeyToVisibleBytes(key);
        }

        fn visibleMutableEntryAtKey(self: *@This(), key: []const u8) !VisibleLookup {
            try self.admission.checkpoint();
            const locked = self.mutableSourceLock();
            defer self.mutableSourceUnlock(locked);
            const idx = self.mutable.findIndex(self.namespace, key) orelse return .absent;
            const entry = self.mutable.entryAt(idx);
            if (entry.tombstone) return .tombstone;
            const value_len = if (self.keys_only) @as(usize, 0) else entry.value.len;
            const bytes = try self.backend.allocator.alloc(u8, entry.key.len + value_len);
            errdefer self.backend.allocator.free(bytes);
            @memcpy(bytes[0..entry.key.len], entry.key);
            @memcpy(bytes[entry.key.len..][0..value_len], entry.value[0..value_len]);
            self.visible_entry_bytes.setOwned(self.backend.allocator, bytes);
            return .{ .value = .{
                .key = bytes[0..entry.key.len],
                .value = bytes[entry.key.len..][0..value_len],
            } };
        }

        fn clearSourceRunLease(self: *@This(), source_index: usize) void {
            if (self.source_run_leases.len == 0) return;
            if (self.source_run_leases[source_index]) |lease| lease.release();
            self.source_run_leases[source_index] = null;
        }

        fn clearSourceBlock(self: *@This(), source_index: usize) void {
            self.source_blocks[source_index].releaseForAdmittedRead(self.defer_pin_returns);
            if (self.source_result_retention.len != 0) self.source_result_retention[source_index] = .unknown;
            self.source_block_indices[source_index] = null;
        }

        fn clearSourceKeyCopy(self: *@This(), source_index: usize) void {
            if (self.source_key_copies[source_index]) |key| self.allocator.free(key);
            self.source_key_copies[source_index] = null;
        }

        fn resetRunSourceResidency(self: *@This(), source_index: usize) void {
            self.clearSourceBlock(source_index);
            self.clearSourceKeyCopy(source_index);
            if (self.resident_run_source == source_index) self.resident_run_source = null;
        }

        fn isPersistedRunSource(self: *const @This(), source_index: usize) bool {
            const offset = self.runSourceOffset();
            if (source_index < offset) return false;
            const run_index = source_index - offset;
            if (run_index >= self.runs.len) return false;
            const run = self.runs[run_index];
            return run.state == null and run.path != null;
        }

        fn spillRunSource(self: *@This(), source_index: usize) !void {
            try self.admission.checkpoint();
            if (!self.bounded_run_block_residency or !self.isPersistedRunSource(source_index)) return;
            const entry = self.source_entries[source_index] orelse {
                self.resetRunSourceResidency(source_index);
                return;
            };
            if (self.source_key_copies[source_index] != null) return;

            const key = try self.allocator.dupe(u8, entry.key);
            self.source_key_copies[source_index] = key;
            self.source_entries[source_index] = .{
                .namespace_name = self.namespace.name,
                .key = key,
                .value = &.{},
                .tombstone = entry.tombstone,
            };
            self.clearSourceBlock(source_index);
            if (self.resident_run_source == source_index) self.resident_run_source = null;
        }

        fn makeRunSourceResident(self: *@This(), source_index: usize) !void {
            try self.admission.checkpoint();
            if (!self.bounded_run_block_residency) return;
            if (self.resident_run_source == source_index) return;

            if (self.resident_run_source) |resident_source| {
                try self.spillRunSource(resident_source);
            }
            if (!self.isPersistedRunSource(source_index)) return;

            const copied_key = self.source_key_copies[source_index] orelse {
                self.resident_run_source = source_index;
                return;
            };
            const position = self.positions[source_index] orelse return error.RunStateUnavailable;
            const entry = try self.sourceEntryAt(source_index, position);
            self.source_entries[source_index] = entry;
            self.allocator.free(copied_key);
            self.source_key_copies[source_index] = null;
            self.resident_run_source = source_index;
        }

        fn sourceEntryAtFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            index: *const lsm_table_file.TableIndex,
            entry_index: usize,
        ) !SourceEntry {
            const block_index = index.findBlockIndexForEntry(entry_index) orelse return error.InvalidTableFile;
            const window = index.blockWindow(block_index);
            const bytes = try self.ensureSourceBlockLoaded(source_index, run, index, window, block_index);
            const relative_offset: usize = @intCast(index.entryStartInBlock(entry_index, block_index) - window.relative_offset);
            const entry = try parseEntryAtWithStats(self.backend, bytes, relative_offset);
            return .{
                .namespace_name = entry.namespace_name,
                .key = entry.key,
                .value = entry.value,
                .tombstone = entry.tombstone,
            };
        }

        fn ensureSourceBlockLoaded(
            self: *@This(),
            source_index: usize,
            run: *Run,
            index: *const lsm_table_file.TableIndex,
            window: lsm_table_file.EntryDataWindow,
            block_index: usize,
        ) ![]const u8 {
            if (self.source_block_indices[source_index]) |loaded_index| {
                if (loaded_index == window.relative_offset) {
                    self.backend.recordCursorBlockReuse();
                    return self.source_blocks[source_index].bytes().?;
                }
            }
            self.clearSourceBlock(source_index);
            self.backend.recordCursorBlockLoad();
            const bytes = if (self.backend.options.cache != null) blk: {
                var handle = try loadRunTableBlockHandleWithAdmission(self.backend, run, index, window, self.namespace.retainDataBlocks(), self.admission.control);
                errdefer self.returnCachePin(&handle);
                const block = handle.runTableBlock();
                self.source_blocks[source_index] = .{ .cached = handle };
                break :blk block;
            } else if (localBlockCacheEnabled(self.backend)) blk: {
                const payload = try loadLocalBlockLeaseWithAdmission(self.backend, run, index, window, self.backend_locked, self.namespace.retainDataBlocks(), self.admission.control);
                self.source_blocks[source_index] = .{ .local = payload };
                break :blk payload.bytes;
            } else blk: {
                const owned = if (self.admission.control) |control|
                    try loadDecodedLocalBlockWithAdmission(self.backend, self.backend.allocator, run.path orelse return error.RunStateUnavailable, @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(), window, control)
                else
                    try loadOwnedBlockForWindowAllocMaybeLocked(
                        self.backend,
                        self.backend.allocator,
                        run,
                        index,
                        window,
                        self.backend_locked,
                    );
                self.source_blocks[source_index] = .{ .owned = .{ .allocator = self.backend.allocator, .bytes = owned } };
                break :blk owned;
            };
            // Retain only sources already opened by real I/O. A fully cached
            // scan needs no extra descriptor or catalog publication.
            if (comptime @hasDecl(BackendType, "retainCachedRunSource")) {
                if (self.source_run_leases.len != 0 and self.source_run_leases[source_index] == null) {
                    self.source_run_leases[source_index] = if (comptime @hasDecl(BackendType, "retainCachedRunSourceWithAdmission"))
                        try self.backend.retainCachedRunSourceWithAdmission(run.path.?, self.admission.control)
                    else
                        self.backend.retainCachedRunSource(run.path.?);
                }
            }
            self.source_block_indices[source_index] = window.relative_offset;
            try self.prefetchNextSourceBlock(source_index, run, index, block_index);
            return bytes;
        }

        fn prefetchNextSourceBlock(
            self: *@This(),
            source_index: usize,
            run: *Run,
            index: *const lsm_table_file.TableIndex,
            block_index: usize,
        ) !void {
            if (self.backend.options.cache == null) return;

            const next_block_index = block_index + 1;
            if (next_block_index >= index.blockCount()) return;
            const next_block = index.blocks[next_block_index];
            if (self.blockStartsAtOrPastUpper(next_block)) return;

            const next_window = index.blockWindow(next_block_index);
            if (self.source_block_indices[source_index]) |loaded_index| {
                if (loaded_index == next_window.relative_offset) return;
            }
            var handle = try loadRunTableBlockHandleWithAdmission(self.backend, run, index, next_window, self.namespace.retainDataBlocks(), self.admission.control);
            defer self.returnCachePin(&handle);
            recordCursorBlockReadahead(self.backend);
        }

        fn sourceLastKeyFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
        ) !?[]const u8 {
            const index = try self.tableIndexForRunSource(source_index, run);
            var idx = index.entryCount();
            while (idx > 0) {
                idx -= 1;
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
                const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                if (order == .eq) return entry.key;
                if (order == .lt) return null;
            }
            return null;
        }

        fn sourcePrevKeyFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            target: []const u8,
            inclusive: bool,
        ) !?[]const u8 {
            const index = try self.tableIndexForRunSource(source_index, run);
            var lo: usize = 0;
            var hi: usize = index.entryCount();
            while (lo < hi) {
                const mid = lo + (hi - lo) / 2;
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, mid);
                const ord = compareTableEntryTo(.{
                    .namespace_name = entry.namespace_name,
                    .key = entry.key,
                    .value = entry.value,
                    .tombstone = entry.tombstone,
                }, self.namespace, target);
                if (ord == .lt or (inclusive and ord == .eq)) {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }

            var idx = lo;
            while (idx > 0) {
                idx -= 1;
                const entry = try self.sourceEntryAtFromLocalIndex(source_index, run, index, idx);
                const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                if (order == .eq) return entry.key;
                if (order == .lt) return null;
            }
            return null;
        }

        fn sourceLowerBoundFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            index: *const lsm_table_file.TableIndex,
            target: []const u8,
            inclusive: bool,
        ) !?usize {
            const scan_prefix = self.boundedScanPrefix(index, target);
            if (scan_prefix) |prefix| {
                if (!index.maybeContainsPrefix(self.namespace.name, prefix)) {
                    recordPrefixBloomNegative(self.backend);
                    return null;
                }
            }
            try requireTableBlocks(index);
            var block_index = index.findBlockIndex(self.namespace.name, target) orelse return null;
            while (block_index < index.blockCount()) : (block_index += 1) {
                const block = index.blocks[block_index];
                if (blockBeforeScanLower(block, self.namespace, target)) continue;
                if (self.blockStartsAtOrPastUpper(block)) return null;
                if (scan_prefix) |prefix| {
                    if (!block.maybeContainsPrefix(self.namespace.name, prefix)) {
                        recordBlockPrefixBloomNegative(self.backend);
                        continue;
                    }
                }
                const window = index.blockWindow(block_index);
                const bytes = try self.ensureSourceBlockLoaded(source_index, run, index, window, block_index);
                if (try lsm_table_file.lowerBoundPositionInBlock(
                    index,
                    bytes,
                    block_index,
                    self.namespace.name,
                    target,
                    inclusive,
                )) |positioned| {
                    return positioned.index;
                }

                if (compareNamespace(.{ .name = block.largest_namespace_name }, self.namespace) == .gt) {
                    return null;
                }
            }
            return null;
        }

        fn nextSourceIndexFromLocalIndex(
            self: *@This(),
            source_index: usize,
            run: *Run,
            current: usize,
        ) !?usize {
            const index = try self.tableIndexForRunSource(source_index, run);
            if (current + 1 >= index.entryCount()) return null;
            const scan_prefix = if (self.source_entries[source_index]) |entry|
                self.boundedScanPrefix(index, entry.key)
            else
                null;

            try requireTableBlocks(index);
            var block_index = index.findBlockIndexForEntry(current) orelse return error.RunStateUnavailable;
            var idx = current + 1;
            while (block_index < index.blockCount()) : (block_index += 1) {
                const block = index.blocks[block_index];
                if (idx < block.first_entry_index) idx = block.first_entry_index;
                if (idx > block.lastEntryIndex()) continue;
                if (blockBeforeScanLower(block, self.namespace, "")) continue;
                if (self.blockStartsAtOrPastUpper(block)) return null;
                if (scan_prefix) |prefix| {
                    if (!block.maybeContainsPrefix(self.namespace.name, prefix)) {
                        recordBlockPrefixBloomNegative(self.backend);
                        idx = block.lastEntryIndex() + 1;
                        continue;
                    }
                }

                const window = index.blockWindow(block_index);
                const bytes = try self.ensureSourceBlockLoaded(source_index, run, index, window, block_index);
                var probe = idx;
                while (probe <= block.lastEntryIndex()) : (probe += 1) {
                    const relative_offset: usize = @intCast(index.entryStartInBlock(probe, block_index) - window.relative_offset);
                    const entry = try parseEntryAtWithStats(self.backend, bytes, relative_offset);
                    const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                    if (order == .eq) {
                        if (!self.keyBeforeUpper(entry.key)) return null;
                        return probe;
                    }
                    if (order == .gt) return null;
                }
                idx = block.lastEntryIndex() + 1;
            }
            return null;
        }

        fn keyBeforeUpper(self: *const @This(), key: []const u8) bool {
            const upper = self.upper_bound orelse return true;
            return std.mem.order(u8, key, upper) == .lt;
        }

        fn boundedScanPrefix(self: *const @This(), index: *const lsm_table_file.TableIndex, key: []const u8) ?[]const u8 {
            const upper = self.upper_bound orelse return null;
            const prefix = lsm_table_file.extractKeyPrefix(index.prefix_extractor, key) orelse return null;
            if (!lsm_table_file.upperBoundWithinPrefix(prefix, upper)) return null;
            return prefix;
        }

        fn blockStartsAtOrPastUpper(self: *const @This(), block: lsm_table_file.TableIndex.BlockMeta) bool {
            const smallest_key = block.smallest_key orelse return false;
            return switch (compareNamespace(.{ .name = block.smallest_namespace_name }, self.namespace)) {
                .lt => false,
                .gt => true,
                .eq => blk: {
                    const upper = self.upper_bound orelse break :blk false;
                    break :blk std.mem.order(u8, smallest_key, upper) != .lt;
                },
            };
        }
    };
}

const BatchCursorReadResult = struct {
    hits: usize = 0,
    misses: usize = 0,

    fn add(self: *@This(), other: @This()) void {
        self.hits += other.hits;
        self.misses += other.misses;
    }
};

const max_current_batch_read_keys_per_backend_lock: usize = 128;

const MultiGetPlan = enum {
    point,
    sorted_by_run,
    cursor,
};

const MultiGetContext = enum {
    snapshot,
    stable_probe,
    current_live,
};

fn chooseMultiGetPlan(keys: []const []const u8, context: MultiGetContext) MultiGetPlan {
    const min_sorted_by_run_keys: usize = 32;
    if (keys.len < 2) return .point;
    if (!keysAreStrictlySorted(keys)) return .point;

    // Probe batches are exact key lookups, not range scans. Sorted key order is
    // not enough evidence that the table layout is scan-friendly, especially
    // for public source hydration and HBC artifact payload reads.
    if (context != .snapshot) return .point;

    if (isCursorFriendlyExactBatch(keys)) return .cursor;
    if (keys.len >= min_sorted_by_run_keys and context == .snapshot) return .sorted_by_run;
    return .point;
}

fn recordMultiGetPlan(backend: anytype, plan: MultiGetPlan) void {
    if (@hasDecl(@TypeOf(backend.*), "recordGetManySortedPlan")) {
        backend.recordGetManySortedPlan(switch (plan) {
            .point => .point,
            .sorted_by_run => .sorted_by_run,
            .cursor => .cursor,
        });
    }
}

fn isCursorFriendlyExactBatch(keys: []const []const u8) bool {
    const max_cursor_exact_batch_keys: usize = 64;
    if (keys.len < 2 or keys.len > max_cursor_exact_batch_keys) return false;

    for (keys[1..], 1..) |key, i| {
        const prev = keys[i - 1];
        if (std.mem.order(u8, prev, key) != .lt) return false;
        if (prev.len != key.len) return false;
        var common_prefix: usize = 0;
        while (common_prefix < prev.len and prev[common_prefix] == key[common_prefix]) : (common_prefix += 1) {}
        if (common_prefix + 2 < prev.len) return false;
    }
    return true;
}

fn advanceSortedBatchCursorToKey(cursor: anytype, current: ?backend_adapter.Entry, target: []const u8) !?backend_adapter.Entry {
    const max_linear_skips: usize = 64;
    var entry = current orelse return null;
    var skipped: usize = 0;
    while (std.mem.order(u8, entry.key, target) == .lt) {
        if (skipped >= max_linear_skips) return try cursor.seekAtOrAfter(target);
        entry = (try cursor.next()) orelse return null;
        skipped += 1;
    }
    return entry;
}

fn readManySortedFromCursor(
    backend: anytype,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    cursor: anytype,
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    @memset(values, null);
    if (keys.len == 0) return .{};

    // Pins belong to this result owner, even when a caller reuses its cursor.
    if (comptime @hasField(@TypeOf(cursor.*), "source_result_retention")) @memset(cursor.source_result_retention, .unknown);

    var result: BatchCursorReadResult = .{};
    backend.recordPointGets(keys.len);
    const previous_scan_value_stats = disableCursorScanValueStats(cursor);
    defer restoreCursorScanValueStats(cursor, previous_scan_value_stats);
    var current = try cursor.seekAtOrAfter(keys[0]);
    for (keys, 0..) |key, i| {
        current = try advanceSortedBatchCursorToKey(cursor, current, key);
        const entry = current orelse {
            result.misses += keys.len - i;
            break;
        };
        if (!std.mem.eql(u8, entry.key, key)) {
            result.misses += 1;
            continue;
        }
        if (held_blocks) |blocks| {
            if (try cursor.retainCurrentValueForTxn(blocks)) {
                values[i] = entry.value;
                recordCursorValueBorrow(backend);
                result.hits += 1;
                continue;
            }
        }
        const owned = try allocator.dupe(u8, entry.value);
        errdefer allocator.free(owned);
        try held_values.append(allocator, owned);
        values[i] = owned;
        recordCursorValueCopy(backend);
        result.hits += 1;
    }
    return result;
}

/// Result lifetime is independent of the physical read plan. Snapshot readers
/// keep sources and cache handles pinned; current-tip writers release their
/// view at the end of the call and must own every returned value instead.
const PointResultLifetime = enum {
    snapshot_pinned,
    transaction_owned,

    fn forBlockPins(blocks: ?*std.ArrayListUnmanaged(BlockPin)) PointResultLifetime {
        return if (blocks != null) .snapshot_pinned else .transaction_owned;
    }

    /// Reuse allocations produced by this lookup, including interior slices
    /// of decoded blocks. Never scan the transaction's entire read history.
    fn retain(
        self: PointResultLifetime,
        backend: anytype,
        allocator: Allocator,
        held: *PointResultValues,
        first_owned: usize,
        value: []const u8,
    ) ![]const u8 {
        return self.retainWithAdmission(backend, allocator, held, first_owned, value, null);
    }

    fn retainWithAdmission(self: PointResultLifetime, backend: anytype, allocator: Allocator, held: *PointResultValues, first_owned: usize, value: []const u8, control: ?read_admission.Port) ![]const u8 {
        if (control) |budget| try budget.checkpoint();
        if (self == .snapshot_pinned) return value;
        const address = @intFromPtr(value.ptr);
        if (!(builtin.is_test and test_duplicate_owned_point_results)) {
            const buffer = held.copies.buffer;
            const base = @intFromPtr(buffer.ptr);
            if (buffer.len != 0 and address >= base and address - base <= held.copies.used and value.len <= held.copies.used - (address - base)) return value;
        }
        const candidates = if (builtin.is_test and test_duplicate_owned_point_results) held.items[held.items.len..] else held.items[first_owned..];
        for (candidates) |owned| {
            if (control) |budget| try budget.checkpoint();
            const base = @intFromPtr(owned.ptr);
            if (address >= base and address - base <= owned.len and value.len <= owned.len - (address - base)) return value;
        }
        if (!(builtin.is_test and test_duplicate_owned_point_results)) return copyPointValueWithAdmission(backend, allocator, held, value, control);
        const owned = try copyPointPromotionRows(allocator, value, control);
        errdefer allocator.free(owned);
        try held.append(allocator, owned);
        recordPointValueCopy(backend);
        return owned;
    }
};

fn readManySortedPointFromSnapshot(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
    control: ?read_admission.Port,
) !BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    // Legacy/non-directory readers still charge transient index metadata.
    const resources = @import("../resource_manager.zig");
    var budget: ?resources.BudgetedAllocator = if (backend.options.resource_manager) |manager| resources.BudgetedAllocator.init(manager, .lsm_in_memory_state, runtimeScratchAllocator(allocator), 1) else null;
    defer if (budget) |*admitted| admitted.deinit();
    if (budget) |*admitted| admitted.credit_quantum = 4096;
    const scratch = if (budget) |*admitted| admitted.allocator() else runtimeScratchAllocator(allocator);
    return readManySortedPointFromSnapshotWithScratch(backend, mutable, immutable_memtables, runs, l0_groups, levels, allocator, held_blocks, held_values, namespace, keys, values, backend_locked, scratch, control) catch |err| {
        if (budget) |*admitted| if (admitted.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
}

fn readManySortedPointFromSnapshotWithScratch(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
    scratch: Allocator,
    control: ?read_admission.Port,
) !BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    @memset(values, null);
    if (try readManySortedPointFromSnapshotAsyncWithAdmission(
        backend,
        mutable,
        immutable_memtables,
        runs,
        l0_groups,
        levels,
        allocator,
        held_values,
        namespace,
        keys,
        values,
        backend_locked,
        PointResultLifetime.forBlockPins(held_blocks),
        held_blocks,
        null,
        control,
    )) |result| return result;

    var local_held_blocks = std.ArrayListUnmanaged(BlockPin).empty;
    defer if (held_blocks == null) releaseHeldBlocks(&local_held_blocks, backend.allocator);
    const block_handles = held_blocks orelse &local_held_blocks;
    var batch_indexes = RunBatchIndexHandles{ .allocator = scratch, .control = control };
    defer batch_indexes.deinit();

    var result: BatchCursorReadResult = .{};
    var last_l0_group_index: ?usize = null;
    var read_hint: ?BorrowedReadHint = null;
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        if (control) |budget| try budget.checkpoint();
        const first_owned = held_values.items.len;
        const value = getFromSnapshotRuns(
            backend,
            mutable,
            immutable_memtables,
            runs,
            l0_groups,
            levels,
            &last_l0_group_index,
            &read_hint,
            block_handles,
            held_values,
            allocator,
            namespace,
            key,
            backend_locked,
            &batch_indexes,
        ) catch |err| switch (err) {
            error.NotFound => {
                result.misses += 1;
                continue;
            },
            else => return err,
        };
        values[i] = try PointResultLifetime.forBlockPins(held_blocks).retainWithAdmission(backend, allocator, held_values, first_owned, value, control);
        result.hits += 1;
    }
    try batch_indexes.transferBlocks(backend.allocator, block_handles);
    return result;
}

const RunBatchIndexState = struct {
    control: ?read_admission.Port = null,
    run_index: usize,
    handle: cache_mod.Handle,
    block_index: ?usize = null,
    block_handle: ?cache_mod.Handle = null,
    block_has_values: bool = false,
    // The first lookup in a prefix-compressed block materializes only that
    // entry. A second lookup in the same block promotes it to the decoded
    // block cache so dense adjacent batches retain their amortized path.
    direct_prefix_block_index: ?usize = null,

    pub fn deinit(self: *@This()) void {
        self.handle.release();
        if (self.block_handle) |*handle| handle.release();
        self.* = undefined;
    }

    fn transferBlock(self: *@This(), allocator: Allocator, held_blocks: *std.ArrayListUnmanaged(BlockPin)) !void {
        if (self.block_handle) |handle| {
            self.block_handle = null;
            self.block_index = null;
            if (self.block_has_values) {
                var transfer = handle;
                errdefer transfer.release();
                try held_blocks.append(allocator, .{ .cached = transfer });
            } else {
                var discard = handle;
                discard.release();
            }
        }
        self.block_has_values = false;
    }
};

const RunBatchIndexHandles = struct {
    control: ?read_admission.Port = null,
    allocator: Allocator,
    items: std.ArrayListUnmanaged(RunBatchIndexState) = .empty,
    by_run: std.AutoHashMapUnmanaged(usize, usize) = .empty,

    pub fn deinit(self: *@This()) void {
        for (self.items.items) |*item| item.deinit();
        self.items.deinit(self.allocator);
        self.by_run.deinit(self.allocator);
        self.* = undefined;
    }

    fn tableIndex(self: *@This(), backend: anytype, run: *Run, run_index: usize) !*const lsm_table_file.TableIndex {
        return (try self.state(backend, run, run_index)).handle.runTableIndex();
    }

    fn state(self: *@This(), backend: anytype, run: *Run, run_index: usize) !*RunBatchIndexState {
        if (self.control) |budget| try budget.checkpoint();
        if (self.by_run.get(run_index)) |i| return &self.items.items[i];
        try self.items.ensureUnusedCapacity(self.allocator, 1);
        try self.by_run.ensureUnusedCapacity(self.allocator, 1);
        var handle = try loadRunTableIndexHandleWithAdmission(backend, run, self.control);
        errdefer handle.release();
        const i = self.items.items.len;
        self.items.appendAssumeCapacity(.{ .run_index = run_index, .handle = handle, .control = self.control });
        self.by_run.putAssumeCapacityNoClobber(run_index, i);
        return &self.items.items[i];
    }

    fn transferBlocks(self: *@This(), allocator: Allocator, held_blocks: *std.ArrayListUnmanaged(BlockPin)) !void {
        for (self.items.items) |*item| try item.transferBlock(allocator, held_blocks);
    }
};

fn readManySortedByRunFromSnapshot(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
    control: ?read_admission.Port,
) !BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    // Legacy/non-directory readers still charge transient index metadata.
    const resources = @import("../resource_manager.zig");
    var budget: ?resources.BudgetedAllocator = if (backend.options.resource_manager) |manager| resources.BudgetedAllocator.init(manager, .lsm_in_memory_state, runtimeScratchAllocator(allocator), 1) else null;
    defer if (budget) |*admitted| admitted.deinit();
    if (budget) |*admitted| admitted.credit_quantum = 4096;
    const scratch = if (budget) |*admitted| admitted.allocator() else runtimeScratchAllocator(allocator);
    return readManySortedByRunFromSnapshotWithScratch(backend, mutable, immutable_memtables, runs, l0_groups, levels, allocator, held_blocks, held_values, namespace, keys, values, backend_locked, scratch, control) catch |err| {
        if (budget) |*admitted| if (admitted.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
}

fn readManySortedByRunFromSnapshotWithScratch(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
    scratch: Allocator,
    control: ?read_admission.Port,
) !BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    @memset(values, null);
    var local_held_blocks = std.ArrayListUnmanaged(BlockPin).empty;
    defer if (held_blocks == null) releaseHeldBlocks(&local_held_blocks, backend.allocator);
    const block_handles = held_blocks orelse &local_held_blocks;

    var batch_indexes = RunBatchIndexHandles{ .allocator = scratch, .control = control };
    defer batch_indexes.deinit();

    var result: BatchCursorReadResult = .{};
    var last_l0_group_index: ?usize = null;
    var read_hint: ?BorrowedReadHint = null;
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        if (control) |budget| try budget.checkpoint();
        const first_owned = held_values.items.len;
        const value = getFromSnapshotRuns(
            backend,
            mutable,
            immutable_memtables,
            runs,
            l0_groups,
            levels,
            &last_l0_group_index,
            &read_hint,
            block_handles,
            held_values,
            allocator,
            namespace,
            key,
            backend_locked,
            &batch_indexes,
        ) catch |err| switch (err) {
            error.NotFound => {
                result.misses += 1;
                continue;
            },
            else => return err,
        };
        values[i] = try PointResultLifetime.forBlockPins(held_blocks).retainWithAdmission(backend, allocator, held_values, first_owned, value, control);
        result.hits += 1;
    }
    try batch_indexes.transferBlocks(backend.allocator, block_handles);
    return result;
}

fn readManyCurrentPointLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    @memset(values, null);
    var result: BatchCursorReadResult = .{};
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        const value = getCurrentPointRetainedLocked(BackendType, backend, namespace, allocator, held_blocks, held_values, key) catch |err| switch (err) {
            error.NotFound => {
                result.misses += 1;
                continue;
            },
            else => return err,
        } orelse {
            result.misses += 1;
            continue;
        };
        values[i] = value;
        result.hits += 1;
    }
    return result;
}

fn getCurrentPointRetainedLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    key: []const u8,
) !?[]const u8 {
    if (backend.mutable.findIndex(namespace, key)) |idx| {
        const entry = backend.mutable.entryAt(idx);
        if (entry.tombstone) return error.NotFound;
        const owned = try copyPointValue(backend, allocator, held_values, entry.value);
        backend.recordMutableHit();
        return owned;
    }

    var immutable_index = backend.immutable_memtables.items.len;
    while (immutable_index > backend.immutable_head) {
        immutable_index -= 1;
        const immutable = backend.immutable_memtables.items[immutable_index];
        if (immutable.findIndex(namespace, key)) |idx| {
            const entry = immutable.entryAt(idx);
            if (entry.tombstone) return error.NotFound;
            const owned = try copyPointValue(backend, allocator, held_values, entry.value);
            backend.recordMutableHit();
            return owned;
        }
    }

    // Writer reads resolve their overlay/memtables first, then pin the exact
    // current SST directory at that same serialized boundary. Reuse indexed
    // candidate discovery and do storage I/O outside the writer mutex. A new
    // call pins a new tip; this is not a transaction-wide read snapshot.
    if (@hasDecl(BackendType, "createReadVersionFromDirectory") and !(builtin.is_test and test_current_point_rank_walk)) {
        const view = try RunReadView.pin(backend, runtimeScratchAllocator(allocator));
        defer view.release(backend);
        if (view.directory()) |directory| {
            unlockBackend(BackendType, backend, @hasField(BackendType, "mu"));
            defer if (@hasField(BackendType, "mu")) {
                _ = lockBackend(BackendType, backend);
            };
            if (builtin.is_test) if (test_current_point_unlocked_hook) |hook| try hook(backend);
            return getOwnedDirectoryPoint(backend, directory, held_values, allocator, namespace, key) catch |err| switch (err) {
                error.NotFound => null,
                else => return err,
            };
        }
        // Only flat oracle fixtures can reach the fallback with SSTs.
        if (run_store.count(backend) == 0) return null;
    }

    var run_index: usize = 0;
    while (run_index < run_store.count(backend) and run_store.at(backend, run_index).*.level == 0) : (run_index += 1) {
        if (try getFromRunPointRetainedLocked(backend, run_store.at(backend, run_index), run_index, held_blocks, held_values, allocator, namespace, key)) |value| return value;
    }

    while (run_index < run_store.count(backend)) {
        const level = run_store.at(backend, run_index).*.level;
        const level_start = run_index;
        while (run_index < run_store.count(backend) and run_store.at(backend, run_index).*.level == level) : (run_index += 1) {}
        var lo = level_start;
        var hi = run_index;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const run = run_store.at(backend, mid);
            if (compareRunBound(run.largest_namespace_name, run.largest_key, namespace.name, key) == .lt) lo = mid + 1 else hi = mid;
        }
        if (lo < run_index) if (try getFromRunPointRetainedLocked(backend, run_store.at(backend, lo), lo, held_blocks, held_values, allocator, namespace, key)) |value| return value;
    }

    return null;
}

fn getFromRunPointRetainedLocked(
    backend: anytype,
    run: *Run,
    run_index: usize,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?[]const u8 {
    if (!try runMayContainWithFilterMaybeLocked(backend, run, namespace, key, true)) return null;
    backend.recordRunProbe();

    if (run.path != null) {
        if (held_blocks) |blocks| {
            if (backend.options.cache != null) {
                var read_hint: ?BorrowedReadHint = null;
                const located = try getFromRunWithBlockCache(backend, run, run_index, &read_hint, blocks, held_values, value_allocator, namespace, key, true) orelse return null;
                if (located.entry.tombstone) return error.NotFound;
                recordPointValueBorrow(backend);
                if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
                return located.entry.value;
            }
        }
        const value = try getFromRunWithLocalIndex(backend, run, held_blocks, held_values, value_allocator, namespace, key, true) orelse return null;
        if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
        return value;
    }

    const state = if (run.state) |*present_state| present_state else return null;
    if (state.findIndex(namespace, key)) |idx| {
        const entry = state.entryAt(idx);
        if (entry.tombstone) return error.NotFound;
        const owned = try copyPointValue(backend, value_allocator, held_values, entry.value);
        if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
        return owned;
    }
    return null;
}

/// Return a transaction-owned value without retaining a whole SST epoch.
/// Decoded allocations already transferred to held_values are reused (also
/// when the value is a subslice of a wide decoded block). Cache/in-memory
/// borrows are copied before their temporary block and directory pins end.
fn getOwnedDirectoryPoint(
    backend: anytype,
    directory: *const @import("run_directory.zig").Directory,
    held_values: *PointResultValues,
    allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) ![]const u8 {
    var hint: ?BorrowedReadHint = null;
    const first_owned = held_values.items.len;
    const value = try getFromDirectoryPointWithLifetime(backend, directory, &.{}, &hint, null, held_values, allocator, namespace, key, .transaction_owned);
    return PointResultLifetime.transaction_owned.retain(backend, allocator, held_values, first_owned, value);
}

fn readManyCurrentSortedPointByRunLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    @memset(values, null);
    const metadata_allocator = runtimeScratchAllocator(allocator);
    const resolved = try metadata_allocator.alloc(bool, keys.len);
    defer metadata_allocator.free(resolved);
    @memset(resolved, false);

    var result: BatchCursorReadResult = .{};
    backend.recordPointGets(keys.len);
    for (keys, 0..) |key, i| {
        if (backend.mutable.findIndex(namespace, key)) |idx| {
            const entry = backend.mutable.entryAt(idx);
            resolved[i] = true;
            if (entry.tombstone) {
                result.misses += 1;
            } else {
                const owned = try copyPointValue(backend, allocator, held_values, entry.value);
                values[i] = owned;
                result.hits += 1;
                backend.recordMutableHit();
            }
        }
    }

    var immutable_index = backend.immutable_memtables.items.len;
    while (immutable_index > backend.immutable_head) {
        immutable_index -= 1;
        const immutable = backend.immutable_memtables.items[immutable_index];
        for (keys, 0..) |key, i| {
            if (resolved[i]) continue;
            if (immutable.findIndex(namespace, key)) |idx| {
                const entry = immutable.entryAt(idx);
                resolved[i] = true;
                if (entry.tombstone) {
                    result.misses += 1;
                } else {
                    const owned = try copyPointValue(backend, allocator, held_values, entry.value);
                    values[i] = owned;
                    result.hits += 1;
                    backend.recordMutableHit();
                }
            }
        }
    }

    var local_held_blocks = std.ArrayListUnmanaged(BlockPin).empty;
    defer if (held_blocks == null) releaseHeldBlocks(&local_held_blocks, backend.allocator);
    const block_handles = held_blocks orelse &local_held_blocks;
    var batch_indexes = RunBatchIndexHandles{ .allocator = metadata_allocator };
    defer batch_indexes.deinit();
    var read_hint: ?BorrowedReadHint = null;

    for (0..run_store.count(backend)) |run_index| {
        const run = run_store.at(backend, run_index);
        var key_index = lowerBoundRunStart(keys, namespace, run.*);
        var state: ?*const State = null;
        while (key_index < keys.len) : (key_index += 1) {
            if (compareRunBound(namespace.name, keys[key_index], run.largest_namespace_name, run.largest_key) == .gt) break;
            if (resolved[key_index]) continue;
            backend.recordRunProbe();

            if (run.path != null) {
                const value = if (backend.options.cache != null) blk: {
                    const located = try getFromRunWithBlockCacheBatch(backend, run, run_index, &read_hint, block_handles, held_values, allocator, namespace, keys[key_index], false, &batch_indexes) orelse break :blk null;
                    read_hint = .{
                        .run_index = run_index,
                        .namespace_name = namespace.name,
                        .key = located.entry.key,
                        .entry_index = located.entry_index,
                    };
                    resolved[key_index] = true;
                    if (located.entry.tombstone) {
                        result.misses += 1;
                        continue;
                    }
                    break :blk located.entry.value;
                } else getFromRunWithLocalIndex(backend, run, held_blocks, held_values, allocator, namespace, keys[key_index], true) catch |err| switch (err) {
                    error.NotFound => {
                        resolved[key_index] = true;
                        result.misses += 1;
                        continue;
                    },
                    else => return err,
                };

                const concrete = value orelse continue;
                resolved[key_index] = true;
                if (backend.options.cache != null) {
                    if (held_blocks != null) {
                        values[key_index] = concrete;
                        recordPointValueBorrow(backend);
                    } else {
                        const owned = try copyPointValue(backend, allocator, held_values, concrete);
                        values[key_index] = owned;
                    }
                } else {
                    values[key_index] = concrete;
                }
                result.hits += 1;
                if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
                continue;
            }

            const maybe_value = if (run.state) |*present_state| blk: {
                if (!runMayContain(run.*, namespace, keys[key_index])) break :blk null;
                if (try ensureRunBloomFilterForRead(backend, run)) |filter| {
                    if (!lsm_table_file.maybeContains(filter, namespace.name, keys[key_index])) {
                        backend.recordBloomNegative();
                        break :blk null;
                    }
                }
                state = present_state;
                break :blk state.?;
            } else null;

            if (maybe_value) |present_state| {
                if (present_state.findIndex(namespace, keys[key_index])) |idx| {
                    const entry = present_state.entryAt(idx);
                    resolved[key_index] = true;
                    if (entry.tombstone) {
                        result.misses += 1;
                    } else {
                        const owned = try copyPointValue(backend, allocator, held_values, entry.value);
                        values[key_index] = owned;
                        result.hits += 1;
                        if (run.level == 0) backend.recordL0Hit() else backend.recordLevelHit();
                    }
                }
                continue;
            }
        }
    }

    for (resolved) |was_resolved| {
        if (!was_resolved) result.misses += 1;
    }
    try batch_indexes.transferBlocks(backend.allocator, block_handles);
    return result;
}

fn keysAreSorted(keys: []const []const u8) bool {
    if (keys.len < 2) return true;
    for (keys[1..], 1..) |key, i| {
        if (std.mem.order(u8, keys[i - 1], key) == .gt) return false;
    }
    return true;
}

fn keysAreStrictlySorted(keys: []const []const u8) bool {
    if (keys.len < 2) return true;
    for (keys[1..], 1..) |key, i| {
        if (std.mem.order(u8, keys[i - 1], key) != .lt) return false;
    }
    return true;
}

fn lowerBoundRunStart(keys: []const []const u8, namespace: backend_types.Namespace, run: Run) usize {
    var lo: usize = 0;
    var hi: usize = keys.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(namespace.name, keys[mid], run.smallest_namespace_name, run.smallest_key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    return lo;
}

/// Immutable run membership and search topology, shared by all reads of one
/// published LSM version. Mutable cache-index hints in Run are only accessed
/// under the backend mutex; lazy Bloom ownership is disabled for these runs.
pub const ReadVersion = struct {
    pub const OwnedRetirement = struct {
        context: *anyopaque,
        retire: *const fn (*anyopaque) void,
    };
    owned_retirement: ?OwnedRetirement = null,
    // Backend-mutex protected. No builder owns a service mutex while yielding.
    preparing: bool = false,
    prepared: bool = true,
    references: std.atomic.Value(usize) = .init(1),
    retired_next: ?*ReadVersion = null,
    live_next: ?*ReadVersion = null,
    live_previous: ?*ReadVersion = null,
    registered: bool = false,
    directory: ?*@import("run_directory.zig").Directory = null,
    projection_bytes: u64 = 0,
    allocator: Allocator,
    runs: []Run = &.{},
    l0_groups: []RunGroup = &.{},
    levels: []RunLevel = &.{},

    pub fn create(backend: anytype) !*ReadVersion {
        if (comptime @hasDecl(@TypeOf(backend.*), "createReadVersionFromDirectory")) return try backend.createReadVersionFromDirectory();
        const allocator = runtimeScratchAllocator(backend.allocator);
        const version = try allocator.create(ReadVersion);
        errdefer allocator.destroy(version);
        const runs = try allocator.alloc(Run, run_store.count(backend));
        var count: usize = 0;
        errdefer {
            for (runs[0..count]) |*run| {
                backend.releaseRunSnapshotRef(run);
                run.deinit(allocator);
            }
            allocator.free(runs);
        }
        for (0..run_store.count(backend)) |i| {
            const run = run_store.at(backend, i).*;
            runs[i] = try repository_mod.cloneRunCompactionSnapshot(allocator, run);
            count += 1;
            runs[i].shared_read_version = true;
            try backend.retainRunSnapshotRef(&runs[i]);
        }
        const groups = try buildL0RunGroupsWithStats(backend, allocator, runs);
        errdefer deinitRunGroups(allocator, groups);
        const levels = try buildLowerLevels(allocator, runs);
        version.* = .{ .allocator = allocator, .runs = runs, .l0_groups = groups, .levels = levels };
        return version;
    }

    /// Ownership of directory transfers only on success. The immutable root
    /// owns metadata and file pins; the projection contains only borrowed data
    /// and per-version cache hints. All O(number-of-runs) work is off-lock.
    pub fn createFromDirectory(allocator: Allocator, directory: *@import("run_directory.zig").Directory) !*ReadVersion {
        return createFromDirectoryWithAdmission(allocator, directory, null);
    }

    pub fn createFromDirectoryWithAdmission(allocator: Allocator, directory: *@import("run_directory.zig").Directory, control: ?read_admission.Port) !*ReadVersion {
        if (control) |owner| try owner.checkpoint();
        const version = try allocator.create(ReadVersion);
        errdefer allocator.destroy(version);
        const runs = try directory.projectWithAdmission(allocator, control);
        errdefer allocator.free(runs);
        const groups = try buildL0RunGroupsControlled(allocator, runs, control);
        errdefer deinitRunGroups(allocator, groups);
        const levels = try buildLowerLevelsControlled(allocator, runs, control);
        errdefer allocator.free(levels);
        var projection_bytes: u64 = runs.len * @sizeOf(Run) + groups.len * @sizeOf(RunGroup) + levels.len * @sizeOf(RunLevel);
        for (groups) |group| {
            if (control) |owner| try owner.checkpoint();
            projection_bytes += group.run_indices.len * @sizeOf(usize);
        }
        if (control) |owner| try owner.checkpoint();
        version.* = .{ .allocator = allocator, .runs = runs, .l0_groups = groups, .levels = levels, .directory = directory, .projection_bytes = projection_bytes };
        return version;
    }

    /// Discard unpublished projection arrays. The pinned epoch still owns
    /// the directory; this temporary header must never destroy that root.
    pub fn discardProjection(self: *ReadVersion) void {
        std.debug.assert(self.owned_retirement == null);
        const allocator = self.allocator;
        allocator.free(self.runs);
        deinitRunGroups(allocator, self.l0_groups);
        allocator.free(self.levels);
        allocator.destroy(self);
    }

    /// Compare registered projection semantics against the existing builders.
    /// This allocator is diagnostic-only and outside measured fault/OOM sweeps.
    pub fn qualifyProjectionForTest(self: *const ReadVersion, allocator: Allocator) !void {
        if (!@import("builtin").is_test) @compileError("projection oracle is test-only");
        const expected_runs = try self.directory.?.projectWithAdmission(allocator, null);
        defer allocator.free(expected_runs);
        try std.testing.expectEqual(expected_runs.len, self.runs.len);
        for (expected_runs, self.runs) |expected, actual| try std.testing.expect(std.meta.eql(expected, actual));
        const expected_groups = try buildL0RunGroupsControlled(allocator, expected_runs, null);
        defer deinitRunGroups(allocator, expected_groups);
        try std.testing.expectEqual(expected_groups.len, self.l0_groups.len);
        for (expected_groups, self.l0_groups) |expected, actual| {
            inline for (.{ "smallest_namespace_name", "largest_namespace_name" }) |field| {
                const e = @field(expected, field);
                const a = @field(actual, field);
                try std.testing.expectEqual(e == null, a == null);
                if (e) |bytes| try std.testing.expectEqualStrings(bytes, a.?);
            }
            try std.testing.expectEqualStrings(expected.smallest_key, actual.smallest_key);
            try std.testing.expectEqualStrings(expected.largest_key, actual.largest_key);
            try std.testing.expectEqualSlices(usize, expected.run_indices, actual.run_indices);
        }
        const expected_levels = try buildLowerLevelsControlled(allocator, expected_runs, null);
        defer allocator.free(expected_levels);
        try std.testing.expectEqual(expected_levels.len, self.levels.len);
        for (expected_levels, self.levels) |expected, actual| try std.testing.expectEqualDeep(expected, actual);
    }

    pub fn buildMemoryBound(run_count: usize) u64 {
        // Flat descriptors, sort scratch, per-component groups/indices and
        // geometric ArrayList slack. All temporary metadata is admitted before
        // allocating the projection, not merely observed after it is built.
        return @sizeOf(ReadVersion) + @as(u64, @intCast(run_count)) *
            (@sizeOf(Run) + 4 * @sizeOf(RunGroup) + 8 * @sizeOf(usize) + 2 * @sizeOf(RunLevel));
    }

    /// Caller holds the backend mutex when using backend-owned retirement.
    /// The last reference queues reclamation; it does not free metadata here.
    /// An existing owned reference pins the epoch through acquisition. Closing
    /// its creator does not invalidate that reference or its descendants.
    pub fn retainOwnedWithAdmission(self: *ReadVersion, original: read_admission.Port) !*ReadVersion {
        try original.checkpoint();
        if (self.owned_retirement == null or self.references.load(.acquire) == 0) return error.InvalidArgument;
        const previous = self.references.fetchAdd(1, .monotonic);
        std.debug.assert(previous != 0);
        return self;
    }
    /// Standalone owned epochs need no Backend receiver after controller exit.
    /// Backend-tracked epochs must release through release(backend) to unlink.
    pub fn releaseOwned(self: *ReadVersion) void {
        std.debug.assert(!self.registered);
        const owner = self.owned_retirement.?;
        const previous = self.references.fetchSub(1, .acq_rel);
        std.debug.assert(previous != 0);
        if (previous == 1) owner.retire(owner.context);
    }
    /// Called only after the last reference and any Backend list handoff.
    /// No epoch access may follow a successful return receipt.
    pub fn retireOwned(self: *ReadVersion) bool {
        const owner = self.owned_retirement orelse return false;
        std.debug.assert(self.references.load(.acquire) == 0 and !self.registered);
        owner.retire(owner.context);
        return true;
    }

    pub fn release(self: *ReadVersion, backend: anytype) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        if (comptime @hasDecl(@TypeOf(backend.*), "retireReadVersion")) return backend.retireReadVersion(self);
        self.destroy(backend);
    }

    pub fn destroy(self: *ReadVersion, backend: anytype) void {
        if (self.retireOwned()) return;
        self.destroyContents(backend);
        if (self.directory) |directory| self.allocator.destroy(directory);
        self.allocator.destroy(self);
    }

    /// Directory-backed projections borrow run metadata. Retire the directory
    /// separately; reclaiming these buffers must not release individual runs.
    pub fn destroyProjectionHeader(self: *ReadVersion) void {
        std.debug.assert(self.owned_retirement == null);
        std.debug.assert(self.directory != null);
        self.allocator.free(self.runs);
        deinitRunGroups(self.allocator, self.l0_groups);
        self.allocator.free(self.levels);
        self.allocator.destroy(self);
    }

    pub fn accountedMemoryBytes(self: *const ReadVersion, pass: u64) u64 {
        if (self.owned_retirement != null) return 0; // The registered fixed grant owns these headers.
        return @sizeOf(ReadVersion) + self.projection_bytes + (if (self.directory) |directory| directory.accountedMemoryBytes(pass) else 0);
    }

    /// Reclamation keeps these small headers immutable until the backend
    /// reacquires its lock, allowing concurrent memory-accounting passes.
    pub fn destroyContents(self: *ReadVersion, backend: anytype) void {
        std.debug.assert(self.owned_retirement == null);
        const allocator = self.allocator;
        if (self.directory) |directory| {
            directory.destroyContents(allocator);
        } else for (self.runs) |*run| {
            if (@hasDecl(@TypeOf(backend.*), "releaseRunSnapshotRef")) backend.releaseRunSnapshotRef(run);
            run.deinit(allocator);
        }
        allocator.free(self.runs);
        deinitRunGroups(allocator, self.l0_groups);
        allocator.free(self.levels);
    }
};

const RunReadView = struct {
    allocator: Allocator,
    runs: []Run,
    l0_groups: []RunGroup,
    levels: []RunLevel,
    version: ?*ReadVersion = null,

    fn directory(self: RunReadView) ?*const @import("run_directory.zig").Directory {
        return if (self.version) |version| version.directory else null;
    }

    fn prepareCursor(self: *RunReadView, backend: anytype) !void {
        if (self.directory() == null) try self.prepare(backend);
    }

    fn prepareCursorWithAdmission(self: *RunReadView, backend: anytype, control: read_admission.Port) !void {
        if (self.directory() == null) try self.prepareWithAdmission(backend, control);
    }

    /// Caller holds the backend mutex. Publishing a new run set invalidates
    /// the backend's reference; readers continue owning the previous version.
    fn pin(backend: anytype, allocator: Allocator) !RunReadView {
        if (run_store.count(backend) == 0) return .{ .allocator = allocator, .runs = &.{}, .l0_groups = &.{}, .levels = &.{} };
        if (comptime @hasField(@TypeOf(backend.*), "read_version")) if (!(builtin.is_test and test_private_read_versions)) {
            if (backend.read_version == null) {
                backend.read_version = try ReadVersion.create(backend);
                if (comptime !@hasDecl(@TypeOf(backend.*), "createReadVersionFromDirectory")) backend.read_version_builds +|= 1;
            }
            const version = backend.read_version.?;
            _ = version.references.fetchAdd(1, .monotonic);
            backend.read_version_pins +|= 1;
            return .{ .allocator = version.allocator, .runs = version.runs, .l0_groups = version.l0_groups, .levels = version.levels, .version = version };
        };
        const BackendType = @TypeOf(backend.*);
        const runs = try borrowRunSnapshotList(BackendType, backend, allocator, &backend.runs);
        errdefer freeRunSnapshotList(BackendType, backend, allocator, runs);
        const groups = try buildL0RunGroupsWithStats(backend, allocator, runs);
        errdefer deinitRunGroups(allocator, groups);
        return .{ .allocator = allocator, .runs = runs, .l0_groups = groups, .levels = try buildLowerLevels(allocator, runs) };
    }

    /// Pin every memtable source before preparing: preparation can release the
    /// backend mutex, but always builds the exact epoch already owned here.
    fn prepare(self: *RunReadView, backend: anytype) !void {
        if (comptime @hasDecl(@TypeOf(backend.*), "prepareReadVersion")) if (self.version) |version| {
            try backend.prepareReadVersion(version);
            self.runs = version.runs;
            self.l0_groups = version.l0_groups;
            self.levels = version.levels;
        };
    }

    fn prepareWithAdmission(self: *RunReadView, backend: anytype, control: read_admission.Port) !void {
        try control.checkLifetime();
        if (self.version) |version| {
            if (comptime !@hasDecl(@TypeOf(backend.*), "prepareReadVersionWithAdmission")) return error.ExecutionWaitUnsupported;
            try backend.prepareReadVersionWithAdmission(version, control);
            self.runs = version.runs;
            self.l0_groups = version.l0_groups;
            self.levels = version.levels;
        }
        try control.checkLifetime();
    }

    fn release(self: RunReadView, backend: anytype) void {
        if (self.version) |version| return version.release(backend);
        freeRunSnapshotList(@TypeOf(backend.*), backend, self.allocator, self.runs);
        deinitRunGroups(self.allocator, self.l0_groups);
        self.allocator.free(self.levels);
    }
};

fn CurrentReadLayout(comptime BackendType: type) type {
    return struct {
        backend: *BackendType,
        metadata_allocator: Allocator,
        mutable_snapshot: ?MutableReadSnapshot,
        immutable_memtables: []const *const State = &.{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},
        read_view: RunReadView,
        owns_version_reader: bool = false,

        /// Pin the published topology and exact immutable generations under
        /// the backend lock. SST I/O runs after releasing that lock.
        fn capture(backend: *BackendType, allocator: Allocator) !@This() {
            return captureSources(backend, allocator, false, false);
        }

        fn capturePoint(backend: *BackendType, allocator: Allocator) !@This() {
            return captureSources(backend, allocator, false, true);
        }

        fn captureSources(backend: *BackendType, allocator: Allocator, pin_mutable: bool, point_only: bool) !@This() {
            const metadata_allocator = runtimeScratchAllocator(allocator);
            var read_view = try RunReadView.pin(backend, metadata_allocator);
            errdefer read_view.release(backend);
            // Point probes already resolved mutable keys under this lock.
            // Current-tip cursors must also pin that source before preparation.
            const mutable_snapshot = if (pin_mutable) try snapshotReadMutable(BackendType, backend, .current_scan) else null;
            errdefer if (mutable_snapshot) |snapshot| snapshot.release(backend);
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer releaseImmutableMemtableSnapshotList(BackendType, backend, immutable_memtables);
            if (!point_only or read_view.version == null or read_view.version.?.directory == null) try read_view.prepareCursor(backend);
            return .{
                .backend = backend,
                .metadata_allocator = metadata_allocator,
                .mutable_snapshot = mutable_snapshot,
                .immutable_memtables = immutable_memtables,
                .runs = read_view.runs,
                .l0_groups = read_view.l0_groups,
                .levels = read_view.levels,
                .read_view = read_view,
            };
        }

        fn init(backend: *BackendType, allocator: Allocator) !@This() {
            // A write transaction is only a lifecycle pin, not a version
            // reader. Keep its captured memtables alive while batch I/O is
            // unlocked, including across intervening writes and reclamation.
            try retainReadReader(BackendType, backend, .current_scan);
            errdefer releaseReadReader(BackendType, backend, .current_scan);
            var layout = try @This().captureSources(backend, allocator, true, false);
            layout.owns_version_reader = true;
            return layout;
        }

        pub fn deinit(self: *@This()) void {
            if (self.mutable_snapshot) |snapshot| snapshot.release(self.backend);
            self.read_view.release(self.backend);
            releaseImmutableMemtableSnapshotList(BackendType, self.backend, self.immutable_memtables);
            if (self.owns_version_reader) releaseReadReader(BackendType, self.backend, .current_scan);
            self.* = undefined;
        }

        /// Retire the epoch under the backend lock; unlockBackend reclaims
        /// its expensive metadata outside the lock after the pin handoff.
        fn deinitAfterUnlockedRead(self: *@This()) void {
            const backend = self.backend;
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            self.deinit();
        }
    };
}

fn readManySortedCurrentWithLayoutLocked(
    comptime BackendType: type,
    backend: *BackendType,
    layout: *const CurrentReadLayout(BackendType),
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    const LocalCursor = MergeCursor(BackendType, State);

    if (layout.read_view.directory()) |directory| {
        unlockBackend(BackendType, backend, @hasField(BackendType, "mu"));
        defer if (@hasField(BackendType, "mu")) {
            _ = lockBackend(BackendType, backend);
        };
        if (builtin.is_test) if (test_current_point_unlocked_hook) |hook| try hook(backend);
        return readManySortedDirectoryBatch(backend, layout.mutable_snapshot.?.state, layout.immutable_memtables, directory, allocator, held_blocks, held_values, namespace, keys, values, false, false, null, allocator, null);
    }

    switch (chooseMultiGetPlan(keys, .stable_probe)) {
        .cursor => {},
        .sorted_by_run => return try readManySortedByRunFromSnapshot(
            backend,
            layout.mutable_snapshot.?.state,
            layout.immutable_memtables,
            layout.runs,
            layout.l0_groups,
            layout.levels,
            allocator,
            null,
            held_values,
            namespace,
            keys,
            values,
            true,
            null,
        ),
        .point => return try readManySortedPointFromSnapshot(
            backend,
            layout.mutable_snapshot.?.state,
            layout.immutable_memtables,
            layout.runs,
            layout.l0_groups,
            layout.levels,
            allocator,
            null,
            held_values,
            namespace,
            keys,
            values,
            true,
            null,
        ),
    }

    var cursor = try LocalCursor.init(layout.metadata_allocator, backend, layout.mutable_snapshot.?.state, layout.immutable_memtables, layout.runs, layout.l0_groups, layout.levels, namespace, true);
    defer cursor.close();

    return try readManySortedFromCursor(backend, allocator, held_blocks, held_values, &cursor, keys, values);
}

fn readManySortedCurrentLocked(
    comptime BackendType: type,
    backend: *BackendType,
    namespace: backend_types.Namespace,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    keys: []const []const u8,
    values: []?[]const u8,
) !BatchCursorReadResult {
    var layout = try CurrentReadLayout(BackendType).init(backend, allocator);
    defer layout.deinit();
    return try readManySortedCurrentWithLayoutLocked(BackendType, backend, &layout, namespace, allocator, held_blocks, held_values, keys, values);
}

pub fn BoundReadTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        mutable_snapshot: *const State,
        owns_mutable_snapshot: bool = false,
        owns_snapshot: bool = true,
        read_view: RunReadView,
        immutable_memtables: []const *const State = &.{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},
        last_l0_group_index: ?usize = null,
        read_hint: ?BorrowedReadHint = null,
        held_blocks: std.ArrayListUnmanaged(BlockPin) = .empty,
        held_values: PointResultValues = .empty,
        planning_scratch: BatchScratch.ProbeScratch = .{},

        pub const ReadScope = struct {
            parent: *BoundReadTxn(BackendType),
            allocator: Allocator,
            held_blocks: std.ArrayListUnmanaged(BlockPin) = .empty,
            held_values: PointResultValues = .empty,
            planning_scratch: BatchScratch.ProbeScratch = .{},
            read_hint: ?BorrowedReadHint = null,
            last_l0_group_index: ?usize = null,

            pub fn get(self: *@This(), key: []const u8) ![]const u8 {
                const p = self.parent;
                p.backend.recordPointGet();
                return getFromReadView(p.backend, p.mutable_snapshot, p.immutable_memtables, p.read_view, &self.last_l0_group_index, &self.read_hint, &self.held_blocks, &self.held_values, self.allocator, p.namespace, key);
            }

            pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
                if (keys.len != values.len) return error.InvalidBatch;
                @memset(values, null);
                const p = self.parent;
                p.backend.recordGetManySorted(keys.len);
                p.backend.recordGetManySortedLocality(keys);
                const result = try readManySortedFromReadView(p.backend, p.mutable_snapshot, p.immutable_memtables, p.read_view, self.allocator, &self.held_blocks, &self.held_values, p.namespace, keys, values, &self.planning_scratch, self.allocator, null);
                p.backend.recordGetManySortedResults(result.hits, result.misses);
            }

            pub fn getManySortedWithAdmission(self: *@This(), keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) !void {
                if (keys.len != values.len) return error.InvalidBatch;
                @memset(values, null);
                errdefer @memset(values, null);
                try control.checkpoint();
                const p = self.parent;
                p.backend.recordGetManySorted(keys.len);
                p.backend.recordGetManySortedLocality(keys);
                const result = try readManySortedFromReadView(p.backend, p.mutable_snapshot, p.immutable_memtables, p.read_view, self.allocator, &self.held_blocks, &self.held_values, p.namespace, keys, values, &self.planning_scratch, self.allocator, control);
                try control.checkpoint();
                p.backend.recordGetManySortedResults(result.hits, result.misses);
            }

            /// Release payload ownership between streaming batches while
            /// reusing bounded pin/value pointer arrays. Larger batches never
            /// permanently inflate the next ordinary batch's scratch.
            pub fn reset(self: *@This()) void {
                for (self.held_blocks.items) |*handle| handle.release();
                for (self.held_values.items) |value| self.allocator.free(value);
                if (self.held_blocks.capacity * @sizeOf(BlockPin) > 64 * 1024) {
                    self.held_blocks.deinit(self.parent.backend.allocator);
                    self.held_blocks = .empty;
                } else self.held_blocks.clearRetainingCapacity();
                if (self.held_values.capacity * @sizeOf([]u8) > 64 * 1024) {
                    self.held_values.deinit(self.allocator);
                    self.held_values = .empty;
                } else self.held_values.clearRetainingCapacity();
                self.read_hint = null;
                self.last_l0_group_index = null;
            }

            pub fn close(self: *@This()) void {
                self.planning_scratch.deinit(self.allocator);
                releaseHeldBlocks(&self.held_blocks, self.parent.backend.allocator);
                releaseHeldValues(&self.held_values, self.allocator);
                self.* = undefined;
            }
        };

        pub fn openReadScope(self: *@This(), allocator: Allocator) !ReadScope {
            return .{ .parent = self, .allocator = allocator };
        }

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            return try openControlled(backend, namespace, null);
        }

        pub fn openWithAdmission(backend: *BackendType, namespace: backend_types.Namespace, control: read_admission.Port) !@This() {
            return try openControlled(backend, namespace, control);
        }

        fn openControlled(backend: *BackendType, namespace: backend_types.Namespace, control: ?read_admission.Port) !@This() {
            var captured = try openCaptured(backend, namespace, control);
            errdefer captured.abort();
            // The backend mutex has been released. Rejection discards the
            // complete capture before publishing it to the caller.
            if (control) |owner| try owner.checkpoint();
            return captured;
        }

        fn openCaptured(backend: *BackendType, namespace: backend_types.Namespace, original: ?read_admission.Port) !@This() {
            const locked = if (original) |budget| try lockBackendWithAdmission(BackendType, backend, budget) else lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            var retained: read_admission.RetainedDependency = .{};
            const control = retained.bind(original);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            try retainReadReader(BackendType, backend, .bound_read_txn);
            errdefer releaseReadReader(BackendType, backend, .bound_read_txn);
            if (control) |budget| try budget.checkLifetime();
            if (@hasDecl(BackendType, "prepareReadSnapshot")) try backend.prepareReadSnapshot();
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer releaseImmutableMemtableSnapshotList(BackendType, backend, immutable_memtables);
            if (control) |budget| try budget.checkLifetime();
            const mutable_snapshot = try snapshotReadMutable(BackendType, backend, .bound_read_txn);
            errdefer {
                if (mutable_snapshot.owned) {
                    var owned = @constCast(mutable_snapshot.state);
                    owned.deinit(backend.allocator);
                    backend.allocator.destroy(owned);
                } else {
                    releaseMutableReadSnapshot(BackendType, backend, mutable_snapshot.state, false);
                }
            }
            if (control) |budget| try budget.checkLifetime();
            var read_view = try RunReadView.pin(backend, metadata_allocator);
            errdefer read_view.release(backend);
            if (control) |owner| try read_view.prepareCursorWithAdmission(backend, owner) else try read_view.prepareCursor(backend);
            if (control) |budget| try budget.checkLifetime();
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .namespace = namespace,
                .mutable_snapshot = mutable_snapshot.state,
                .owns_mutable_snapshot = mutable_snapshot.owned,
                .immutable_memtables = immutable_memtables,
                .read_view = read_view,
                .runs = read_view.runs,
                .l0_groups = read_view.l0_groups,
                .levels = read_view.levels,
            };
        }

        /// The erased read handle retains the parent snapshot until all forks
        /// and their cursors close. Only immutable metadata is shared here.
        pub fn forkBorrowedRead(self: *@This()) !@This() {
            // Do not copy mutable read scratch even transiently: the source
            // handle may be serving a read while another worker forks it.
            return .{
                .allocator = self.allocator,
                .metadata_allocator = self.metadata_allocator,
                .backend = self.backend,
                .namespace = self.namespace,
                .mutable_snapshot = self.mutable_snapshot,
                .owns_mutable_snapshot = self.owns_mutable_snapshot,
                .owns_snapshot = false,
                .read_view = self.read_view,
                .immutable_memtables = self.immutable_memtables,
                .runs = self.runs,
                .l0_groups = self.l0_groups,
                .levels = self.levels,
            };
        }

        pub fn abort(self: *@This()) void {
            const backend = self.backend;
            self.planning_scratch.deinit(self.metadata_allocator);
            if (!self.owns_snapshot) {
                releaseHeldBlocks(&self.held_blocks, backend.allocator);
                releaseHeldValues(&self.held_values, self.allocator);
                self.* = undefined;
                return;
            }
            if (self.owns_mutable_snapshot) {
                var owned = @constCast(self.mutable_snapshot);
                owned.deinit(self.allocator);
                self.allocator.destroy(owned);
            }
            releaseHeldBlocks(&self.held_blocks, backend.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            self.read_view.release(backend);
            releaseMutableReadSnapshot(BackendType, backend, self.mutable_snapshot, self.owns_mutable_snapshot);
            releaseImmutableMemtableSnapshotList(BackendType, backend, self.immutable_memtables);
            releaseReadReader(BackendType, backend, .bound_read_txn);
            self.* = undefined;
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.backend.recordPointGet();
            return try getFromReadView(self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, &self.last_l0_group_index, &self.read_hint, &self.held_blocks, &self.held_values, self.allocator, self.namespace, key);
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            @memset(values, null);
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            const result = try readManySortedFromReadView(self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, self.allocator, &self.held_blocks, &self.held_values, self.namespace, keys, values, &self.planning_scratch, self.metadata_allocator, null);
            self.backend.recordGetManySortedResults(result.hits, result.misses);
        }

        pub fn getManySortedWithAdmission(self: *@This(), keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            @memset(values, null);
            errdefer @memset(values, null);
            try control.checkpoint();

            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            const result = try readManySortedFromReadView(self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, self.allocator, &self.held_blocks, &self.held_values, self.namespace, keys, values, &self.planning_scratch, self.metadata_allocator, control);
            try control.checkpoint();
            self.backend.recordGetManySortedResults(result.hits, result.misses);
        }

        pub fn openCursorWithAdmission(self: *@This(), control: read_admission.Port) !LocalCursor {
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            const sequence: LocalCursor.RunSequence = if (self.read_view.directory()) |directory| .{ .directory = directory } else .{ .runs = self.read_view.runs, .levels = self.read_view.levels };
            return try LocalCursor.initSequenceControlled(cursor_alloc, self.backend, self.mutable_snapshot, self.immutable_memtables, sequence, if (sequence.directory != null) &.{} else self.read_view.l0_groups, self.namespace, false, control);
        }

        pub fn openCursor(self: *@This()) !LocalCursor {
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            return try LocalCursor.initView(cursor_alloc, self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, self.namespace, false);
        }
    };
}

const MutableReadSnapshot = struct {
    state: *const State,
    owned: bool,

    fn release(self: @This(), backend: anytype) void {
        if (self.owned) {
            const state = @constCast(self.state);
            state.deinit(backend.allocator);
            backend.allocator.destroy(state);
        } else releaseMutableReadSnapshot(@TypeOf(backend.*), backend, self.state, false);
    }
};

fn retainReadReader(comptime BackendType: type, backend: *BackendType, kind: anytype) !void {
    if (@hasField(BackendType, "closing") and backend.closing.load(.acquire)) return error.BackendClosing;
    if (@hasDecl(BackendType, "retainReaderKind")) {
        backend.retainReaderKind(kind);
    } else {
        backend.retainReader();
    }
}

/// Releases both the snapshot pointer array and the exact immutable-generation
/// pins represented by it. Callers must hold the backend lock when the backend
/// implements generation pinning.
fn releaseImmutableMemtableSnapshotList(
    comptime BackendType: type,
    backend: *BackendType,
    snapshot: []const *const State,
) void {
    if (@hasDecl(BackendType, "releaseImmutableMemtableSnapshot")) {
        backend.releaseImmutableMemtableSnapshot(snapshot);
    } else if (snapshot.len > 0) {
        backend.allocator.free(snapshot);
    }
}

pub fn releaseImmutableMemtablePins(
    comptime BackendType: type,
    backend: *BackendType,
    snapshot: []const *const State,
) void {
    if (@hasDecl(BackendType, "releaseImmutableMemtablePins")) {
        backend.releaseImmutableMemtablePins(snapshot);
    }
}

/// Release the exact shared mutable generation returned by
/// `snapshotMutableStateWithReason`. Fallback backends return owned snapshots
/// and do not implement this hook.
pub fn releaseMutableReadSnapshot(
    comptime BackendType: type,
    backend: *BackendType,
    snapshot: *const State,
    owned: bool,
) void {
    if (owned) return;
    if (@hasDecl(BackendType, "releaseMutableReadSnapshot")) {
        backend.releaseMutableReadSnapshot(snapshot);
    } else if (@hasDecl(BackendType, "releaseMutableStateSnapshot")) {
        backend.releaseMutableStateSnapshot(snapshot);
    }
}

fn releaseReadReader(comptime BackendType: type, backend: *BackendType, kind: anytype) void {
    if (@hasDecl(BackendType, "finalizeReadReaderReleaseKind")) {
        backend.finalizeReadReaderReleaseKind(kind);
    } else if (@hasDecl(BackendType, "finalizeReadReaderRelease")) {
        backend.finalizeReadReaderRelease();
    } else if (@hasDecl(BackendType, "releaseReaderKind")) {
        backend.releaseReaderKind(kind);
    } else {
        backend.releaseReader();
    }
}

fn releaseWriteReader(comptime BackendType: type, backend: *BackendType, kind: anytype) void {
    if (@hasDecl(BackendType, "releaseReaderKind")) {
        backend.releaseReaderKind(kind);
    } else {
        backend.releaseReader();
    }
}

fn finalizeWriteReader(comptime BackendType: type, backend: *BackendType, kind: anytype) !void {
    if (@hasDecl(BackendType, "finalizeWriteReaderReleaseKind")) {
        try backend.finalizeWriteReaderReleaseKind(kind);
    } else if (@hasDecl(BackendType, "finalizeWriteReaderRelease")) {
        try backend.finalizeWriteReaderRelease();
    } else {
        releaseWriteReader(BackendType, backend, kind);
    }
}

fn snapshotReadMutable(comptime BackendType: type, backend: *BackendType, reason: anytype) !MutableReadSnapshot {
    if (@hasDecl(BackendType, "snapshotMutableStateWithReason")) {
        return .{ .state = try backend.snapshotMutableStateWithReason(reason), .owned = false };
    }
    if (@hasDecl(BackendType, "snapshotMutableState")) {
        return .{ .state = try backend.snapshotMutableState(), .owned = false };
    }
    const snapshot = try backend.allocator.create(State);
    errdefer backend.allocator.destroy(snapshot);
    snapshot.* = try backend.mutable.clone(backend.allocator);
    return .{ .state = snapshot, .owned = true };
}

pub fn BoundProbeTxn(comptime BackendType: type) type {
    return struct {
        // Mutable hits and the immutable/run layout are captured under one
        // backend lock. Disk reads use that pinned layout after releasing it.
        pub const get_many_sorted_is_atomic = true;
        allocator: Allocator,
        metadata_allocator: Allocator,
        batch_scratch: BatchScratch.ProbeScratch = .{},
        borrowed_batch_scratch: ?*BatchScratch.ProbeScratch = null,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        stable_point_view: bool = false,
        stable_point_view_loaded: bool = false,
        read_view: ?RunReadView = null,
        // Values from immutable generations/in-memory runs borrow their
        // captured layout. Disk values are already owned by held_values or
        // pinned by held_blocks and need no second whole-value allocation.
        held_layouts: std.ArrayListUnmanaged(CurrentReadLayout(BackendType)) = .empty,
        leased_values: PointResultValues = .empty,
        leased_entries: std.ArrayListUnmanaged(state_mod.OwnedEntry) = .empty,
        empty_state: State = .{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},
        last_l0_group_index: ?usize = null,
        read_hint: ?BorrowedReadHint = null,
        held_blocks: std.ArrayListUnmanaged(BlockPin) = .empty,
        held_values: PointResultValues = .empty,

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            return openControlled(backend, namespace, null);
        }

        pub fn openWithAdmission(backend: *BackendType, namespace: backend_types.Namespace, control: read_admission.Port) !@This() {
            return openControlled(backend, namespace, control);
        }

        fn openControlled(backend: *BackendType, namespace: backend_types.Namespace, control: ?read_admission.Port) !@This() {
            var captured = try openCaptured(backend, namespace, control);
            errdefer captured.abort();
            // The backend mutex has been released. Rejection discards the
            // complete capture before publishing it to the caller.
            if (control) |owner| try owner.checkpoint();
            return captured;
        }

        fn openCaptured(backend: *BackendType, namespace: backend_types.Namespace, control: ?read_admission.Port) !@This() {
            const locked = if (control) |owner| try lockBackendWithAdmission(BackendType, backend, owner) else lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            try retainReadReader(BackendType, backend, .probe_txn);
            errdefer releaseReadReader(BackendType, backend, .probe_txn);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            const stable_point_view = backend.mutable.entryCount() == 0 and backend.immutable_memtables.items.len == backend.immutable_head;
            const read_view = if (stable_point_view) try RunReadView.pin(backend, metadata_allocator) else null;
            errdefer if (read_view) |view| view.release(backend);
            if (control) |owner| try owner.checkLifetime();
            return .{
                .allocator = runtimeScratchAllocator(backend.allocator),
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .namespace = namespace,
                .stable_point_view = stable_point_view,
                .read_view = read_view,
            };
        }

        pub fn abort(self: *@This()) void {
            const backend = self.backend;
            self.batch_scratch.deinit(self.metadata_allocator);
            for (self.held_layouts.items) |*layout| layout.deinitAfterUnlockedRead();
            self.held_layouts.deinit(self.metadata_allocator);
            releaseHeldValues(&self.leased_values, backend.allocator);
            for (self.leased_entries.items) |*entry| entry.deinit(self.allocator);
            self.leased_entries.deinit(self.metadata_allocator);
            releaseHeldBlocks(&self.held_blocks, backend.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            if (self.read_view) |view| view.release(backend);
            releaseReadReader(BackendType, backend, .probe_txn);
            self.* = undefined;
        }

        fn ownValue(self: *@This(), value: []const u8) ![]const u8 {
            return copyPointValue(self.backend, self.allocator, &self.held_values, value);
        }

        fn ensureStablePointViewLoaded(self: *@This(), control: ?read_admission.Port) !void {
            if (!self.stable_point_view or self.stable_point_view_loaded) return;
            const locked = if (control) |owner| try lockBackendWithAdmission(BackendType, self.backend, owner) else lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            if (control) |owner| try self.read_view.?.prepareWithAdmission(self.backend, owner) else try self.read_view.?.prepare(self.backend);
            const read_view = self.read_view.?;
            self.runs = read_view.runs;
            self.l0_groups = read_view.l0_groups;
            self.levels = read_view.levels;
            self.stable_point_view_loaded = true;
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            return self.getWithLease(key, false);
        }

        /// Presence owns no value bytes. Mutable decisions happen under the
        /// writer lock; immutable and SST decisions use a pinned read layout.
        pub fn contains(self: *@This(), key: []const u8) !bool {
            self.backend.recordPointGet();
            if (self.stable_point_view) {
                if (self.read_view.?.directory() == null) try self.ensureStablePointViewLoaded(null);
                return try containsFromReadView(self.backend, &.{}, self.read_view.?, self.namespace, key);
            }
            var layout = blk: {
                const locked = lockBackend(BackendType, self.backend);
                defer unlockBackend(BackendType, self.backend, locked);
                if (self.backend.mutable.findIndex(self.namespace, key)) |idx| {
                    self.backend.recordMutableHit();
                    return !self.backend.mutable.entryAt(idx).tombstone;
                }
                break :blk try CurrentReadLayout(BackendType).capturePoint(self.backend, self.allocator);
            };
            defer layout.deinitAfterUnlockedRead();
            return try containsFromReadView(self.backend, layout.immutable_memtables, layout.read_view, self.namespace, key);
        }

        /// Opt-in for short projection scopes. Ordinary probes deliberately
        /// copy values so long-lived maintenance probes do not pin generations.
        pub fn getLeased(self: *@This(), key: []const u8) ![]const u8 {
            return self.getWithLease(key, true);
        }

        fn getWithLease(self: *@This(), key: []const u8, lease: bool) ![]const u8 {
            // Backend-owned decoded buffers can be transferred directly to
            // this short lease without changing allocator ownership.
            const value_allocator = if (lease) self.backend.allocator else self.allocator;
            const held_values = if (lease) &self.leased_values else &self.held_values;
            const lifetime: PointResultLifetime = if (lease) .snapshot_pinned else .transaction_owned;
            const first_owned = held_values.items.len;
            // Legacy run-array hints can borrow keys from temporary blocks.
            // Ordinary probes release those blocks at the end of this call.
            if (!lease) self.read_hint = null;
            defer if (!lease) {
                self.read_hint = null;
            };
            const blocks: ?*std.ArrayListUnmanaged(BlockPin) = if (lease) &self.held_blocks else null;
            if (self.stable_point_view) {
                if (self.read_view.?.version) |version| if (version.directory) |directory| {
                    self.backend.recordPointGet();
                    const value = try getFromDirectoryPointWithLifetime(self.backend, directory, &.{}, &self.read_hint, blocks, held_values, value_allocator, self.namespace, key, lifetime);
                    if (!lease) return try lifetime.retain(self.backend, value_allocator, held_values, first_owned, value);
                    recordPointValueBorrow(self.backend);
                    return value;
                };
                try self.ensureStablePointViewLoaded(null);
                self.backend.recordPointGet();
                if (!lease) switch (try getFromStableCachedPointView(self.backend, self.metadata_allocator, self.runs, self.l0_groups, self.levels, &self.last_l0_group_index, self.namespace, key)) {
                    .hit => |value| return try self.ownValue(value),
                    .miss => return error.NotFound,
                    .unavailable => {},
                };
                const value = try getFromSnapshotRuns(
                    self.backend,
                    &self.empty_state,
                    &.{},
                    self.runs,
                    self.l0_groups,
                    self.levels,
                    &self.last_l0_group_index,
                    &self.read_hint,
                    blocks,
                    held_values,
                    value_allocator,
                    self.namespace,
                    key,
                    false,
                    null,
                );
                if (!lease) return try lifetime.retain(self.backend, value_allocator, held_values, first_owned, value);
                recordPointValueBorrow(self.backend);
                return value;
            }
            self.backend.recordPointGet();

            // Resolve the only mutable structure while holding the backend
            // lock. If it does not decide this key, pin the immutable/run
            // generation at the same linearization point and do all table IO
            // after releasing the writer lock.
            var layout: CurrentReadLayout(BackendType) = blk: {
                const locked = lockBackend(BackendType, self.backend);
                defer unlockBackend(BackendType, self.backend, locked);
                if (self.backend.mutable.findIndex(self.namespace, key)) |idx| {
                    const entry = self.backend.mutable.entryAt(idx);
                    if (entry.tombstone) return error.NotFound;
                    self.backend.recordMutableHit();
                    if (lease and entry.shared != null) {
                        try self.leased_entries.ensureUnusedCapacity(self.metadata_allocator, 1);
                        self.leased_entries.appendAssumeCapacity(try state_mod.cloneEntry(self.allocator, entry));
                        recordPointValueBorrow(self.backend);
                        return entry.value;
                    }
                    return try self.ownValue(entry.value);
                }
                break :blk try CurrentReadLayout(BackendType).capturePoint(self.backend, self.allocator);
            };
            var retain_layout = false;
            defer if (!retain_layout) layout.deinitAfterUnlockedRead();

            const value = if (layout.read_view.version != null and layout.read_view.version.?.directory != null)
                try getFromDirectoryPointWithLifetime(self.backend, layout.read_view.version.?.directory.?, layout.immutable_memtables, &self.read_hint, blocks, held_values, value_allocator, self.namespace, key, lifetime)
            else
                try getFromSnapshotRuns(
                    self.backend,
                    &self.empty_state,
                    layout.immutable_memtables,
                    layout.runs,
                    layout.l0_groups,
                    layout.levels,
                    &self.last_l0_group_index,
                    &self.read_hint,
                    blocks,
                    held_values,
                    value_allocator,
                    self.namespace,
                    key,
                    false,
                    null,
                );
            if (!lease) {
                return try lifetime.retain(self.backend, value_allocator, held_values, first_owned, value);
            }
            const needs_layout = layout.immutable_memtables.len != 0 or blk: {
                if (layout.read_view.version) |version| if (version.directory) |directory| break :blk directory.memory_run_count != 0;
                for (layout.runs) |run| if (run.path == null) break :blk true;
                break :blk false;
            };
            if (needs_layout) {
                try self.held_layouts.append(self.metadata_allocator, layout);
                retain_layout = true;
            }
            recordPointValueBorrow(self.backend);
            return value;
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            return self.getManySortedWithStats(keys, values, true, null);
        }

        pub fn getManySortedWithAdmission(self: *@This(), keys: []const []const u8, values: []?[]const u8, control: read_admission.Port) !void {
            return self.getManySortedWithStats(keys, values, true, control);
        }

        fn getManySortedWithStats(self: *@This(), keys: []const []const u8, values: []?[]const u8, record_batch: bool, control: ?read_admission.Port) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            @memset(values, null);
            errdefer @memset(values, null);
            if (control) |owner| try owner.checkpoint();
            if (record_batch) {
                self.backend.recordGetManySorted(keys.len);
                self.backend.recordGetManySortedLocality(keys);
            }

            var result: BatchCursorReadResult = .{};
            if (self.stable_point_view) {
                var offset: usize = 0;
                while (offset < keys.len) {
                    if (control) |owner| try owner.checkpoint();
                    const end = @min(offset + max_current_batch_read_keys_per_backend_lock, keys.len);
                    const plan = chooseMultiGetPlan(keys[offset..end], .stable_probe);
                    recordMultiGetPlan(self.backend, plan);
                    if (self.read_view.?.directory() == null) try self.ensureStablePointViewLoaded(control);
                    const chunk_result = if (self.read_view.?.directory()) |directory|
                        try readManySortedDirectoryBatch(self.backend, &self.empty_state, &.{}, directory, self.allocator, &self.held_blocks, &self.held_values, self.namespace, keys[offset..end], values[offset..end], plan == .sorted_by_run, false, self.borrowed_batch_scratch orelse &self.batch_scratch, self.metadata_allocator, control)
                    else switch (plan) {
                        .sorted_by_run => try readManySortedByRunFromSnapshot(
                            self.backend,
                            &self.empty_state,
                            &.{},
                            self.runs,
                            self.l0_groups,
                            self.levels,
                            self.allocator,
                            &self.held_blocks,
                            &self.held_values,
                            self.namespace,
                            keys[offset..end],
                            values[offset..end],
                            false,
                            control,
                        ),
                        .cursor, .point => try readManySortedPointFromSnapshot(
                            self.backend,
                            &self.empty_state,
                            &.{},
                            self.runs,
                            self.l0_groups,
                            self.levels,
                            self.allocator,
                            &self.held_blocks,
                            &self.held_values,
                            self.namespace,
                            keys[offset..end],
                            values[offset..end],
                            false,
                            control,
                        ),
                    };
                    result.add(chunk_result);
                    offset = end;
                }
                // A stable probe has no mutable or live-immutable sources.
                // Run-backed results are already retained by held_blocks (or
                // owned in held_values when the block cache cannot lend a
                // view), and the captured run generation remains pinned until
                // abort. Copying every hit again here only duplicates large
                // point-read payloads such as dense-vector artifacts.
            } else {
                var oversized: BatchScratch.ProbeScratch = .{};
                defer oversized.deinit(self.metadata_allocator);
                const scratch = if (keys.len <= BatchScratch.Scratch.max_retained_keys)
                    self.borrowed_batch_scratch orelse &self.batch_scratch
                else
                    &oversized;
                const resolved = try scratch.prepareResolved(self.metadata_allocator, keys.len);

                var unresolved_count: usize = keys.len;
                var maybe_layout: ?CurrentReadLayout(BackendType) = null;
                {
                    if (control) |owner| try owner.checkpoint();
                    const locked = if (control) |owner| try lockBackendWithAdmission(BackendType, self.backend, owner) else lockBackend(BackendType, self.backend);
                    defer unlockBackend(BackendType, self.backend, locked);
                    for (keys, 0..) |key, i| {
                        if (control) |owner| try owner.checkpoint();
                        const idx = self.backend.mutable.findIndex(self.namespace, key) orelse continue;
                        const entry = self.backend.mutable.entryAt(idx);
                        resolved[i] = true;
                        unresolved_count -= 1;
                        if (entry.tombstone) {
                            result.misses += 1;
                            continue;
                        }
                        values[i] = try copyPointValueWithAdmission(self.backend, self.allocator, &self.held_values, entry.value, control);
                        self.backend.recordMutableHit();
                        result.hits += 1;
                    }
                    self.backend.recordPointGets(keys.len - unresolved_count);
                    if (unresolved_count > 0) {
                        maybe_layout = try CurrentReadLayout(BackendType).capture(self.backend, self.allocator);
                    }
                }

                if (maybe_layout) |*layout| {
                    defer layout.deinitAfterUnlockedRead();
                    // Writer batches use this probe path. Inject publication
                    // or cancellation only after the exact tip is pinned and
                    // the backend lock is released, as for single-key reads.
                    if (builtin.is_test) if (test_current_point_unlocked_hook) |hook| try hook(self.backend);

                    try scratch.pending.prepare(self.metadata_allocator, unresolved_count);
                    const unresolved_keys = scratch.pending.keys.items;
                    const unresolved_values = scratch.pending.values.items;
                    const unresolved_indexes = scratch.pending.indexes.items;
                    var unresolved_index: usize = 0;
                    for (keys, 0..) |key, i| {
                        if (control) |owner| try owner.checkpoint();
                        if (resolved[i]) continue;
                        unresolved_keys[unresolved_index] = key;
                        unresolved_indexes[unresolved_index] = i;
                        unresolved_index += 1;
                    }

                    var source_namespace = self.namespace;
                    source_namespace.own_source_point_results = true;
                    const plan = chooseMultiGetPlan(unresolved_keys, .stable_probe);
                    recordMultiGetPlan(self.backend, plan);
                    const unresolved_result = if (layout.read_view.directory()) |directory|
                        try readManySortedDirectoryBatch(self.backend, &self.empty_state, layout.immutable_memtables, directory, self.allocator, &self.held_blocks, &self.held_values, source_namespace, unresolved_keys, unresolved_values, plan == .sorted_by_run, false, scratch, self.metadata_allocator, control)
                    else switch (plan) {
                        .sorted_by_run => try readManySortedByRunFromSnapshot(
                            self.backend,
                            &self.empty_state,
                            layout.immutable_memtables,
                            layout.runs,
                            layout.l0_groups,
                            layout.levels,
                            self.allocator,
                            &self.held_blocks,
                            &self.held_values,
                            source_namespace,
                            unresolved_keys,
                            unresolved_values,
                            false,
                            control,
                        ),
                        .cursor, .point => try readManySortedPointFromSnapshot(
                            self.backend,
                            &self.empty_state,
                            layout.immutable_memtables,
                            layout.runs,
                            layout.l0_groups,
                            layout.levels,
                            self.allocator,
                            &self.held_blocks,
                            &self.held_values,
                            source_namespace,
                            unresolved_keys,
                            unresolved_values,
                            false,
                            control,
                        ),
                    };
                    for (unresolved_values, unresolved_indexes) |value, index| values[index] = value;
                    result.add(unresolved_result);
                }
            }
            if (control) |owner| try owner.checkpoint();
            if (record_batch) self.backend.recordGetManySortedResults(result.hits, result.misses);
        }

        /// Apply a per-read block-cache policy without changing namespace
        /// identity. Probe transactions are request-local, so temporarily
        /// changing this hint cannot affect concurrent readers or writes.
        pub fn getManySortedWithBlockCacheAdmission(
            self: *@This(),
            keys: []const []const u8,
            values: []?[]const u8,
            admission: backend_types.Namespace.BlockCacheAdmission,
        ) !void {
            const previous = self.namespace.block_cache_admission;
            self.namespace.block_cache_admission = admission;
            defer self.namespace.block_cache_admission = previous;
            return try self.getManySorted(keys, values);
        }
    };
}

pub fn BoundCurrentScanTxn(comptime BackendType: type) type {
    const ActiveCursor = MergeCursor(BackendType, ActiveMemTable);
    const SnapshotCursor = MergeCursor(BackendType, State);
    const LocalCursor = union(enum) {
        active: ActiveCursor,
        snapshot: SnapshotCursor,

        pub fn close(self: *@This()) void {
            switch (self.*) {
                .active => |*cursor| cursor.close(),
                .snapshot => |*cursor| cursor.close(),
            }
            self.* = undefined;
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.first(),
                .snapshot => |*cursor| try cursor.first(),
            };
        }

        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            switch (self.*) {
                .active => |*cursor| cursor.setUpperBound(upper),
                .snapshot => |*cursor| cursor.setUpperBound(upper),
            }
        }

        pub fn last(self: *@This()) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.last(),
                .snapshot => |*cursor| try cursor.last(),
            };
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.next(),
                .snapshot => |*cursor| try cursor.next(),
            };
        }

        pub fn nextWithAdmission(self: *@This(), control: read_admission.Port) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.nextWithAdmission(control),
                .snapshot => |*cursor| try cursor.nextWithAdmission(control),
            };
        }

        pub fn seekAtOrAfterWithAdmission(self: *@This(), key: []const u8, control: read_admission.Port) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.seekAtOrAfterWithAdmission(key, control),
                .snapshot => |*cursor| try cursor.seekAtOrAfterWithAdmission(key, control),
            };
        }

        pub fn prev(self: *@This()) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.prev(),
                .snapshot => |*cursor| try cursor.prev(),
            };
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.seekAtOrAfter(key),
                .snapshot => |*cursor| try cursor.seekAtOrAfter(key),
            };
        }

        pub fn seekAtOrBefore(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            return switch (self.*) {
                .active => |*cursor| try cursor.seekAtOrBefore(key),
                .snapshot => |*cursor| try cursor.seekAtOrBefore(key),
            };
        }
    };
    const MutableSnapshot = union(enum) {
        none,
        borrowed: *const State,
        owned: *State,

        fn ptr(self: *@This()) ?*const State {
            return switch (self.*) {
                .none => null,
                .borrowed => |state| state,
                .owned => |state| state,
            };
        }

        fn ownedPtr(self: *@This()) ?*State {
            return switch (self.*) {
                .owned => |state| state,
                else => null,
            };
        }

        fn borrowedPtr(self: *@This()) ?*const State {
            return switch (self.*) {
                .borrowed => |state| state,
                else => null,
            };
        }

        pub fn deinitOwned(self: *@This(), backend: *BackendType) void {
            switch (self.*) {
                .owned => |state| {
                    if (@hasDecl(BackendType, "retireOwnedMutableSnapshot")) {
                        backend.retireOwnedMutableSnapshot(state);
                    } else {
                        state.deinit(backend.allocator);
                        backend.allocator.destroy(state);
                    }
                },
                else => {},
            }
            self.* = .none;
        }
    };

    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        mutable_snapshot: MutableSnapshot = .none,
        mutable_snapshot_is_bulk_current_scan_clone: bool = false,
        read_view: RunReadView,
        immutable_memtables: []const *const State = &.{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},

        const Purpose = enum { general, replay };

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            return try openWithPurpose(backend, namespace, .general, null);
        }

        pub fn openWithAdmission(backend: *BackendType, namespace: backend_types.Namespace, control: read_admission.Port) !@This() {
            return openWithPurpose(backend, namespace, .general, control);
        }

        pub fn openReplay(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            return try openWithPurpose(backend, namespace, .replay, null);
        }

        fn openWithPurpose(backend: *BackendType, namespace: backend_types.Namespace, purpose: Purpose, control: ?read_admission.Port) !@This() {
            var captured = try openCaptured(backend, namespace, purpose, control);
            errdefer captured.abort();
            if (control) |owner| try owner.checkpoint();
            return captured;
        }

        fn openCaptured(backend: *BackendType, namespace: backend_types.Namespace, purpose: Purpose, control: ?read_admission.Port) !@This() {
            const locked = if (control) |owner| try lockBackendWithAdmission(BackendType, backend, owner) else lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            try retainReadReader(BackendType, backend, .current_scan);
            errdefer releaseReadReader(BackendType, backend, .current_scan);
            var mutable_snapshot: MutableSnapshot = .none;
            var mutable_snapshot_is_bulk_current_scan_clone = false;
            var bulk_current_scan_clone_denied = false;
            if (purpose == .general and @hasDecl(BackendType, "cloneCurrentScanMutableStateForBulkIngest") and
                (!@hasDecl(BackendType, "bulkIngestActive") or backend.bulkIngestActive()))
            {
                // Allocate the retirement header before acquiring ownership.
                // An OOM here must not strand a snapshot/accounting lease.
                const owned = backend.allocator.create(State) catch null;
                var adopted = false;
                defer if (!adopted) {
                    if (owned) |header| backend.allocator.destroy(header);
                };
                if (owned) |header| {
                    if (try backend.cloneCurrentScanMutableStateForBulkIngest()) |snapshot| {
                        header.* = snapshot;
                        mutable_snapshot = .{ .owned = header };
                        adopted = true;
                        mutable_snapshot_is_bulk_current_scan_clone = true;
                    }
                }
                // A denied optional clone, including its retirement header,
                // falls back to the existing rotation/admission path.
                if (!adopted and @hasDecl(BackendType, "bulkIngestActive") and backend.bulkIngestActive()) {
                    bulk_current_scan_clone_denied = true;
                }
            } else if (purpose == .replay and @hasDecl(BackendType, "bulkIngestActive") and backend.bulkIngestActive()) {
                bulk_current_scan_clone_denied = true;
            }
            errdefer {
                if (mutable_snapshot_is_bulk_current_scan_clone and @hasDecl(BackendType, "releaseCurrentScanMutableStateForBulkIngest")) {
                    if (mutable_snapshot.ownedPtr()) |snapshot| backend.releaseCurrentScanMutableStateForBulkIngest(snapshot);
                }
                if (mutable_snapshot.borrowedPtr()) |snapshot| {
                    releaseMutableReadSnapshot(BackendType, backend, snapshot, false);
                }
                mutable_snapshot.deinitOwned(backend);
            }
            if (mutable_snapshot.ptr() == null) {
                if (bulk_current_scan_clone_denied and @hasDecl(BackendType, "prepareCurrentScanSnapshot")) {
                    try backend.prepareCurrentScanSnapshot();
                } else if (@hasDecl(BackendType, "prepareReadSnapshot")) {
                    try backend.prepareReadSnapshot();
                }
                const read_snapshot = try snapshotReadMutable(BackendType, backend, .current_scan);
                mutable_snapshot = if (read_snapshot.owned)
                    .{ .owned = @constCast(read_snapshot.state) }
                else
                    .{ .borrowed = read_snapshot.state };
            }
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer releaseImmutableMemtableSnapshotList(BackendType, backend, immutable_memtables);
            var read_view = try RunReadView.pin(backend, metadata_allocator);
            errdefer read_view.release(backend);
            if (control) |owner| try read_view.prepareCursorWithAdmission(backend, owner) else try read_view.prepareCursor(backend);
            if (control) |owner| try owner.checkLifetime();
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .namespace = namespace,
                .mutable_snapshot = mutable_snapshot,
                .mutable_snapshot_is_bulk_current_scan_clone = mutable_snapshot_is_bulk_current_scan_clone,
                .immutable_memtables = immutable_memtables,
                .read_view = read_view,
                .runs = read_view.runs,
                .l0_groups = read_view.l0_groups,
                .levels = read_view.levels,
            };
        }

        /// Capture a replay lane once and keep its merge generation pinned
        /// while the derived worker consumes multiple bounded windows. Only
        /// the append-only mutable lane range is copied; documents and other
        /// replay lanes remain outside this snapshot.
        pub fn openReplayLane(
            backend: *BackendType,
            namespace: backend_types.Namespace,
            lower: []const u8,
            upper: []const u8,
        ) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            var read_view = try RunReadView.pin(backend, metadata_allocator);
            errdefer read_view.release(backend);
            try read_view.prepareCursor(backend);
            try retainReadReader(BackendType, backend, .current_scan);
            errdefer releaseReadReader(BackendType, backend, .current_scan);

            const owned = blk: {
                const state = try backend.allocator.create(State);
                errdefer backend.allocator.destroy(state);
                state.* = try backend.cloneReplayLaneMutableRange(namespace, lower, upper);
                break :blk state;
            };
            var mutable_snapshot: MutableSnapshot = .{ .owned = owned };
            errdefer mutable_snapshot.deinitOwned(backend);

            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer releaseImmutableMemtableSnapshotList(BackendType, backend, immutable_memtables);
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .namespace = namespace,
                .mutable_snapshot = .{ .owned = owned },
                .immutable_memtables = immutable_memtables,
                .read_view = read_view,
                .runs = read_view.runs,
                .l0_groups = read_view.l0_groups,
                .levels = read_view.levels,
            };
        }

        pub fn abort(self: *@This()) void {
            const backend = self.backend;
            {
                const locked = lockBackend(BackendType, backend);
                defer unlockBackend(BackendType, backend, locked);
                self.read_view.release(backend);
                releaseImmutableMemtableSnapshotList(BackendType, backend, self.immutable_memtables);
                if (self.mutable_snapshot_is_bulk_current_scan_clone and @hasDecl(BackendType, "releaseCurrentScanMutableStateForBulkIngest")) {
                    if (self.mutable_snapshot.ownedPtr()) |snapshot| backend.releaseCurrentScanMutableStateForBulkIngest(snapshot);
                }
                if (self.mutable_snapshot.borrowedPtr()) |snapshot| {
                    releaseMutableReadSnapshot(BackendType, backend, snapshot, false);
                }
                self.mutable_snapshot.deinitOwned(backend);
                releaseReadReader(BackendType, backend, .current_scan);
            }
            self.* = undefined;
        }

        /// Admission is borrowed only while constructing this cursor. Every
        /// admitted step supplies its own borrow; the cursor never retains it.
        pub fn openCursorWithAdmission(self: *@This(), control: read_admission.Port) !LocalCursor {
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            const sequence: SnapshotCursor.RunSequence = if (self.read_view.directory()) |directory| .{ .directory = directory } else .{ .runs = self.read_view.runs, .levels = self.read_view.levels };
            const groups = if (sequence.directory != null) &.{} else self.read_view.l0_groups;
            if (self.mutable_snapshot.ptr()) |snapshot| {
                return .{ .snapshot = try SnapshotCursor.initSequenceControlled(cursor_alloc, self.backend, snapshot, self.immutable_memtables, sequence, groups, self.namespace, false, control) };
            }
            const active_sequence: ActiveCursor.RunSequence = if (sequence.directory) |directory| .{ .directory = directory } else .{ .runs = self.read_view.runs, .levels = self.read_view.levels };
            return .{ .active = try ActiveCursor.initSequenceControlled(cursor_alloc, self.backend, &self.backend.mutable, self.immutable_memtables, active_sequence, groups, self.namespace, false, control) };
        }

        pub fn openCursor(self: *@This()) !LocalCursor {
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            if (self.mutable_snapshot.ptr()) |snapshot| {
                return .{ .snapshot = try SnapshotCursor.initView(cursor_alloc, self.backend, snapshot, self.immutable_memtables, self.read_view, self.namespace, false) };
            }
            return .{ .active = try ActiveCursor.initView(cursor_alloc, self.backend, &self.backend.mutable, self.immutable_memtables, self.read_view, self.namespace, false) };
        }
    };
}

pub fn BoundProbeCursor(comptime BackendType: type) type {
    return struct {
        allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        current_key: ?[]u8 = null,
        visible_entry_bytes: ?[]u8 = null,
        upper_bound: ?[]const u8 = null,

        pub fn close(self: *@This()) void {
            self.clearCurrentKey();
            self.clearVisibleEntryBytes();
            self.* = undefined;
        }

        pub fn first(self: *@This()) !?backend_adapter.Entry {
            return try self.seekAtOrAfter("");
        }

        pub fn setUpperBound(self: *@This(), upper: ?[]const u8) void {
            self.upper_bound = upper;
        }

        pub fn last(_: *@This()) !?backend_adapter.Entry {
            return error.Unsupported;
        }

        pub fn next(self: *@This()) !?backend_adapter.Entry {
            const key = self.current_key orelse return null;
            return try self.findAtOrAfter(key, false);
        }

        pub fn prev(_: *@This()) !?backend_adapter.Entry {
            return error.Unsupported;
        }

        pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?backend_adapter.Entry {
            return try self.findAtOrAfter(key, true);
        }

        pub fn seekAtOrBefore(_: *@This(), _: []const u8) !?backend_adapter.Entry {
            return error.Unsupported;
        }

        fn findAtOrAfter(self: *@This(), key: []const u8, inclusive: bool) !?backend_adapter.Entry {
            const stable_key = try self.allocator.dupe(u8, key);
            defer self.allocator.free(stable_key);

            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);

            const metadata_allocator = runtimeScratchAllocator(self.allocator);
            var view = try RunReadView.pin(self.backend, metadata_allocator);
            defer view.release(self.backend);
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try self.backend.snapshotImmutableMemtables()
            else
                &.{};
            defer releaseImmutableMemtableSnapshotList(BackendType, self.backend, immutable_memtables);

            try view.prepareCursor(self.backend);
            var cursor = try MergeCursor(BackendType, ActiveMemTable).initView(metadata_allocator, self.backend, &self.backend.mutable, immutable_memtables, view, self.namespace, true);
            defer cursor.close();
            cursor.upper_bound = self.upper_bound;
            const entry = if (inclusive)
                try cursor.seekAtOrAfter(stable_key)
            else blk: {
                try cursor.initForwardPositions(stable_key, false);
                break :blk try cursor.selectVisibleForward();
            };
            const visible = entry orelse {
                self.clearCurrentKey();
                self.clearVisibleEntryBytes();
                return null;
            };
            return try self.replaceVisibleEntry(visible);
        }

        fn replaceVisibleEntry(self: *@This(), entry: backend_adapter.Entry) !backend_adapter.Entry {
            const key_len = entry.key.len;
            const value_len = entry.value.len;
            const bytes = try self.allocator.alloc(u8, key_len + value_len);
            errdefer self.allocator.free(bytes);
            @memcpy(bytes[0..key_len], entry.key);
            @memcpy(bytes[key_len..][0..value_len], entry.value);
            const key = bytes[0..key_len];
            const value = bytes[key_len..][0..value_len];

            self.clearCurrentKey();
            self.clearVisibleEntryBytes();
            self.current_key = try self.allocator.dupe(u8, key);
            self.visible_entry_bytes = bytes;
            return .{ .key = key, .value = value };
        }

        fn clearCurrentKey(self: *@This()) void {
            if (self.current_key) |key| self.allocator.free(key);
            self.current_key = null;
        }

        fn clearVisibleEntryBytes(self: *@This()) void {
            if (self.visible_entry_bytes) |bytes| self.allocator.free(bytes);
            self.visible_entry_bytes = null;
        }
    };
}

pub fn BoundWriteTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        namespace: backend_types.Namespace,
        mutable: ActiveMemTable,
        bulk_appends: State = .{},
        bulk_index: BulkAppendIndex = .{},
        prefix_index: WriterPrefixIndex = .{},
        cursor_overlay: ?State = null,
        cursor_base_mutable: ?MutableReadSnapshot = null,
        cursor_immutable_memtables: []const *const State = &.{},
        cursor_read_view: ?RunReadView = null,
        cursor_runs: []Run = &.{},
        cursor_l0_groups: []RunGroup = &.{},
        cursor_levels: []RunLevel = &.{},
        held_values: PointResultValues = .empty,
        batch_options: backend_types.BatchOptions = .{},
        cursor_reader_retained: bool = false,
        closed: bool = false,

        pub fn open(backend: *BackendType, namespace: backend_types.Namespace) !@This() {
            return try openWithOptions(backend, namespace, .{});
        }

        pub fn openWithOptions(backend: *BackendType, namespace: backend_types.Namespace, options: backend_types.BatchOptions) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            try retainReadReader(BackendType, backend, .write_txn);
            errdefer releaseWriteReader(BackendType, backend, .write_txn);
            backend.beginBatchMode(options);
            errdefer backend.finishBatchMode(options);
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = runtimeScratchAllocator(backend.allocator),
                .backend = backend,
                .namespace = namespace,
                .mutable = .{ .ordered_enabled = false },
                .batch_options = options,
            };
        }

        pub fn abort(self: *@This()) void {
            if (self.closed) return;
            const backend = self.backend;
            self.bulk_index.deinit(self.allocator);
            self.prefix_index.deinit(self.allocator);
            self.mutable.deinit(self.allocator);
            self.bulk_appends.deinit(self.allocator);
            self.invalidateCursorSnapshot();
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.finishBatchMode(self.batch_options);
            if (self.cursor_reader_retained) releaseWriteReader(BackendType, backend, .current_scan);
            releaseWriteReader(BackendType, backend, .write_txn);
            self.* = undefined;
        }

        pub fn commit(self: *@This()) !void {
            if (self.closed) return error.TransactionClosed;
            defer if (self.closed) {
                self.bulk_index.deinit(self.allocator);
                self.prefix_index.deinit(self.allocator);
            };
            const wire_credit = if (comptime @hasDecl(BackendType, "prepareManifestCredit")) try self.backend.prepareManifestCredit(&self.mutable, &self.bulk_appends) else 0;
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            var release_on_error = true;
            errdefer if (release_on_error) {
                self.mutable.deinit(self.allocator);
                self.mutable = .{};
                self.bulk_appends.deinit(self.allocator);
                self.bulk_appends = .{};
                self.invalidateCursorSnapshotLocked();
                releaseHeldValues(&self.held_values, self.allocator);
                self.backend.finishBatchMode(self.batch_options);
                if (self.cursor_reader_retained) releaseWriteReader(BackendType, self.backend, .current_scan);
                releaseWriteReader(BackendType, self.backend, .write_txn);
                self.closed = true;
            };
            const admission = if (comptime @hasDecl(BackendType, "admitPreparedCommit")) try self.backend.admitPreparedCommit(&self.mutable, &self.bulk_appends, wire_credit) else if (comptime @hasDecl(BackendType, "admitCommit")) try self.backend.admitCommit(&self.mutable, &self.bulk_appends) else {};
            defer if (comptime @hasDecl(BackendType, "admitCommit")) {
                if (comptime @hasDecl(BackendType, "admitPreparedCommit")) {
                    if ((self.mutable.entryCount() == 0 and self.bulk_appends.entryCount() == 0) or
                        (if (@hasField(BackendType, "manifest_recovery_required")) self.backend.manifest_recovery_required else false)) admission.retainDebt();
                }
                admission.release();
            };
            const direct_ingested_bulk_appends = try self.tryCommitDirectBulkAppends();
            var committed_write = direct_ingested_bulk_appends;
            const direct_ingested_bulk_state = try self.tryCommitDirectBulkIngest();
            if (!direct_ingested_bulk_state) {
                const mutated = self.mutable.entryCount() > 0;
                committed_write = committed_write or mutated;
                if (mutated) {
                    try enforceMutableWriteAdmission(self.backend, &self.mutable);
                    try prepareMutableForWrite(self.backend);
                }
                if (@hasDecl(BackendType, "appendWalForMutable")) {
                    try publishMutableWithWal(self.backend, self.allocator, &self.mutable);
                } else if (@hasDecl(BackendType, "appendWalForState")) {
                    var sorted = try self.mutable.toStateMove(self.allocator);
                    defer sorted.deinit(self.allocator);
                    try self.backend.appendWalForState(&sorted);
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyStateMoveToMutable(&self.backend.mutable, self.allocator, &sorted);
                } else {
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyMutableMoveToMutable(&self.backend.mutable, self.allocator, &self.mutable);
                }
                if (mutated or direct_ingested_bulk_appends) notePotentialMaintenanceDebtLocked(self.backend);
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
                if (!self.batch_options.defer_commit_flush) {
                    try self.backend.maybeFlushMutable();
                }
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            } else {
                committed_write = true;
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            }
            if (committed_write) finishCommittedWalAppend(self.backend);
            self.backend.finishBatchMode(self.batch_options);
            try self.backend.finalizeExitedBatchMode(self.batch_options);
            release_on_error = false;
            self.closed = true;
            self.invalidateCursorSnapshotLocked();
            releaseHeldValues(&self.held_values, self.allocator);
            var finalize_err: ?anyerror = null;
            if (self.cursor_reader_retained) {
                releaseWriteReader(BackendType, self.backend, .current_scan);
                self.cursor_reader_retained = false;
            }
            finalizeWriteReader(BackendType, self.backend, .write_txn) catch |err| {
                finalize_err = err;
            };
            if (finalize_err) |err| return err;
        }

        fn drainBulkAppendsToMutable(self: *@This()) !void {
            if (self.bulk_appends.entryCount() == 0) return;
            try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
            self.bulk_index.clear();
        }

        fn tryCommitDirectBulkAppends(self: *@This()) !bool {
            const entries = self.bulk_appends.entryCount();
            if (entries == 0) return false;
            if (self.batch_options.mode != .bulk_ingest) {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackNonBulk")) self.backend.recordBulkAppendFallbackNonBulk(entries);
                try self.drainBulkAppendsToMutable();
                return false;
            }
            if (!@hasDecl(BackendType, "ingestSortedState") or !@hasDecl(BackendType, "shouldDirectIngestBulkState")) {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackUnsupported")) self.backend.recordBulkAppendFallbackUnsupported(entries);
                try self.drainBulkAppendsToMutable();
                return false;
            }
            const can_queue_pending_immutable = @hasDecl(BackendType, "canQueueDirectBulkStateWithPendingImmutable") and
                self.backend.canQueueDirectBulkStateWithPendingImmutable();
            if ((self.backend.mutable.entryCount() != 0 or
                (self.backend.activeImmutableMemtableCount() != 0 and !can_queue_pending_immutable)) and
                @hasDecl(BackendType, "drainMutableBeforeBulkAppendDirectIngest"))
            {
                if (!try self.backend.drainMutableBeforeBulkAppendDirectIngest()) {
                    if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                    if (@hasDecl(BackendType, "recordBulkAppendFallbackBackendPending")) self.backend.recordBulkAppendFallbackBackendPending(entries);
                    try self.drainBulkAppendsToMutable();
                    return false;
                }
            }
            if (self.backend.mutable.entryCount() != 0 or
                (self.backend.activeImmutableMemtableCount() != 0 and !can_queue_pending_immutable))
            {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackBackendPending")) self.backend.recordBulkAppendFallbackBackendPending(entries);
                try self.drainBulkAppendsToMutable();
                return false;
            }
            if (self.mutable.entryCount() > 0) {
                try self.drainBulkAppendsToMutable();
                return false;
            }
            if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);

            const duplicate_check_start_ns = platform_time.monotonicNs();
            if (self.bulk_index.entries.count() != entries) {
                if (@hasDecl(BackendType, "recordBulkAppendFallbackDuplicateKeys")) self.backend.recordBulkAppendFallbackDuplicateKeys(entries, elapsedNs(duplicate_check_start_ns));
                try self.drainBulkAppendsToMutable();
                return false;
            }

            const sort_start_ns = platform_time.monotonicNs();
            state_mod.sortStateEntries(&self.bulk_appends);
            const sort_ns = elapsedNs(sort_start_ns);
            std.debug.assert(bulkStateEntriesAreUnique(&self.bulk_appends));
            if (!self.backend.shouldDirectIngestBulkState(&self.bulk_appends)) {
                if (@hasDecl(BackendType, "recordBulkAppendFallbackBelowThreshold")) self.backend.recordBulkAppendFallbackBelowThreshold(entries, sort_ns);
                try self.drainBulkAppendsToMutable();
                return false;
            }

            try enforceSortedWriteAdmission(self.backend, &self.bulk_appends);
            if (@hasDecl(BackendType, "appendWalForState")) try self.backend.appendWalForState(&self.bulk_appends);
            errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
            const queued = if (@hasDecl(BackendType, "enqueueOwnedSortedStateForFlush"))
                try self.backend.enqueueOwnedSortedStateForFlush(&self.bulk_appends)
            else
                false;
            if (!queued and @hasDecl(BackendType, "ingestOwnedSortedState")) {
                try self.backend.ingestOwnedSortedState(&self.bulk_appends);
            } else if (!queued) {
                try self.backend.ingestSortedState(&self.bulk_appends);
            }
            if (@hasDecl(BackendType, "recordBulkAppendSuccess")) self.backend.recordBulkAppendSuccess(entries, sort_ns);
            self.bulk_appends.deinit(self.allocator);
            self.bulk_appends = .{};
            return true;
        }

        fn bulkStateEntriesAreUnique(state: *const State) bool {
            if (state.entryCount() <= 1) return true;
            var cursor: State.EntryCursor = .{};
            var previous = cursor.at(state, 0);
            for (1..state.entryCount()) |i| {
                const entry = cursor.at(state, i);
                if (compareEntryTo(previous, state_mod.namespaceOf(entry), entry.key) == .eq) return false;
                previous = entry;
            }
            return true;
        }

        fn tryCommitDirectBulkIngest(self: *@This()) !bool {
            if (self.batch_options.mode != .bulk_ingest) return false;
            const entries = self.mutable.entryCount();
            if (entries == 0) return false;
            if (@hasDecl(BackendType, "recordDirectBulkIngestAttempt")) self.backend.recordDirectBulkIngestAttempt(entries);
            if (!@hasDecl(BackendType, "ingestSortedState") or !@hasDecl(BackendType, "shouldDirectIngestBulkState")) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackUnsupported")) self.backend.recordDirectBulkIngestFallbackUnsupported();
                return false;
            }
            if ((self.backend.mutable.entryCount() != 0 or self.backend.activeImmutableMemtableCount() != 0) and
                @hasDecl(BackendType, "shouldDrainMutableBeforeDirectBulkIngest") and
                self.backend.shouldDrainMutableBeforeDirectBulkIngest(&self.mutable) and
                @hasDecl(BackendType, "directIngestCombinedMutable"))
            {
                try enforceMutableWriteAdmission(self.backend, &self.mutable);
                if (@hasDecl(BackendType, "appendWalForMutable")) try self.backend.appendWalForMutable(&self.mutable);
                errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
                const sort_start_ns = platform_time.monotonicNs();
                if (!try self.backend.directIngestCombinedMutable(&self.mutable)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBackendMutable")) self.backend.recordDirectBulkIngestFallbackBackendMutable();
                    return false;
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, elapsedNs(sort_start_ns));
                notePotentialMaintenanceDebtLocked(self.backend);
                return true;
            }
            if (self.backend.mutable.entryCount() != 0) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBackendMutable")) self.backend.recordDirectBulkIngestFallbackBackendMutable();
                return false;
            }
            if (@hasDecl(BackendType, "shouldDirectIngestBulkMutable")) {
                if (!self.backend.shouldDirectIngestBulkMutable(&self.mutable)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    return false;
                }
                try enforceMutableWriteAdmission(self.backend, &self.mutable);
                if (@hasDecl(BackendType, "appendWalForMutable")) try self.backend.appendWalForMutable(&self.mutable);
                errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.toStateMove(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                const queued = if (@hasDecl(BackendType, "enqueueOwnedSortedStateForFlush"))
                    try self.backend.enqueueOwnedSortedStateForFlush(&sorted)
                else
                    false;
                if (!queued and @hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else if (!queued) {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            } else {
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.clone(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                if (!self.backend.shouldDirectIngestBulkState(&sorted)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    sorted.deinit(self.allocator);
                    return false;
                }
                try enforceSortedWriteAdmission(self.backend, &sorted);
                if (@hasDecl(BackendType, "appendWalForState")) try self.backend.appendWalForState(&sorted);
                errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
                const queued = if (@hasDecl(BackendType, "enqueueOwnedSortedStateForFlush"))
                    try self.backend.enqueueOwnedSortedStateForFlush(&sorted)
                else
                    false;
                if (!queued and @hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else if (!queued) {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            }
            self.mutable.deinit(self.allocator);
            self.mutable = .{};
            notePotentialMaintenanceDebtLocked(self.backend);
            return true;
        }

        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            if (self.closed) return error.TransactionClosed;
            if (self.bulk_index.get(&self.bulk_appends, self.namespace, key)) |entry| {
                if (entry.tombstone) return error.NotFound;
                return entry.value;
            }
            if (self.mutable.findIndex(self.namespace, key)) |idx| {
                const entry = self.mutable.entryAt(idx);
                if (entry.tombstone) return error.NotFound;
                return entry.value;
            }
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            if (comptime @hasField(BackendType, "runs") and @hasField(BackendType, "immutable_memtables")) {
                self.backend.recordPointGets(1);
                return try getCurrentPointRetainedLocked(BackendType, self.backend, self.namespace, self.allocator, null, &self.held_values, key) orelse error.NotFound;
            }
            return self.backend.getMergedWithOverlay(&self.backend.mutable, &self.mutable, self.namespace, key);
        }

        pub fn contains(self: *@This(), key: []const u8) !bool {
            if (self.closed) return error.TransactionClosed;
            self.backend.recordPointGet();
            var bulk = self.bulk_appends.entries.items.len;
            while (bulk != 0) {
                bulk -= 1;
                const entry = self.bulk_appends.entries.items[bulk];
                if (compareEntryTo(entry, self.namespace, key) == .eq) return !entry.tombstone;
            }
            if (self.mutable.findIndex(self.namespace, key)) |i| return !self.mutable.entryAt(i).tombstone;
            var layout = blk: {
                const locked = lockBackend(BackendType, self.backend);
                defer unlockBackend(BackendType, self.backend, locked);
                if (self.backend.mutable.findIndex(self.namespace, key)) |i| return !self.backend.mutable.entryAt(i).tombstone;
                break :blk try CurrentReadLayout(BackendType).capturePoint(self.backend, self.allocator);
            };
            defer layout.deinitAfterUnlockedRead();
            return containsFromReadView(self.backend, layout.immutable_memtables, layout.read_view, self.namespace, key);
        }

        pub fn containsManySorted(self: *@This(), keys: []const []const u8, present: []bool) !void {
            if (self.closed) return error.TransactionClosed;
            if (keys.len != present.len or !keysAreSorted(keys)) return error.InvalidBatch;
            @memset(present, false);
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            var offset: usize = 0;
            while (offset < keys.len) {
                const end = @min(keys.len, offset + 256);
                // Pin the exact epoch under the lock, then perform directory
                // and SST reads unlocked: their index-cache access takes the
                // backend lock itself. All borrowed sources outlive this page.
                var layout = blk: {
                    const locked = lockBackend(BackendType, self.backend);
                    defer unlockBackend(BackendType, self.backend, locked);
                    break :blk try CurrentReadLayout(BackendType).init(self.backend, self.allocator);
                };
                defer layout.deinitAfterUnlockedRead();
                for (keys[offset..end], present[offset..end]) |key, *exists| {
                    var bulk = self.bulk_appends.entries.items.len;
                    while (bulk != 0) {
                        bulk -= 1;
                        const entry = self.bulk_appends.entries.items[bulk];
                        if (compareEntryTo(entry, self.namespace, key) == .eq) {
                            exists.* = !entry.tombstone;
                            break;
                        }
                    } else {
                        if (self.mutable.findIndex(self.namespace, key)) |i| {
                            exists.* = !self.mutable.entryAt(i).tombstone;
                            continue;
                        }
                        exists.* = if (layout.mutable_snapshot.?.state.findIndex(self.namespace, key)) |i|
                            !layout.mutable_snapshot.?.state.entryAt(i).tombstone
                        else
                            try containsFromReadView(self.backend, layout.immutable_memtables, layout.read_view, self.namespace, key);
                    }
                }
                offset = end;
            }
        }

        pub fn getManySorted(self: *@This(), keys: []const []const u8, values: []?[]const u8) !void {
            if (self.closed) return error.TransactionClosed;
            if (keys.len != values.len) return error.InvalidBatch;
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            @memset(values, null);

            const miss_keys = try self.metadata_allocator.alloc([]const u8, keys.len);
            defer self.metadata_allocator.free(miss_keys);
            const miss_indexes = try self.metadata_allocator.alloc(usize, keys.len);
            defer self.metadata_allocator.free(miss_indexes);

            var hits: usize = 0;
            var misses: usize = 0;
            var miss_count: usize = 0;
            var overlay_point_gets: usize = 0;
            for (keys, 0..) |key, i| {
                if (self.bulk_index.get(&self.bulk_appends, self.namespace, key)) |entry| {
                    overlay_point_gets += 1;
                    if (entry.tombstone) {
                        misses += 1;
                    } else {
                        values[i] = entry.value;
                        hits += 1;
                    }
                } else {
                    if (self.mutable.findIndex(self.namespace, key)) |idx| {
                        overlay_point_gets += 1;
                        const entry = self.mutable.entryAt(idx);
                        if (entry.tombstone) {
                            misses += 1;
                        } else {
                            values[i] = entry.value;
                            hits += 1;
                        }
                        continue;
                    }
                    miss_keys[miss_count] = key;
                    miss_indexes[miss_count] = i;
                    miss_count += 1;
                    continue;
                }
                continue;
            }
            self.backend.recordPointGets(overlay_point_gets);

            if (miss_count > 0) {
                const miss_values = try self.metadata_allocator.alloc(?[]const u8, miss_count);
                defer self.metadata_allocator.free(miss_values);
                // Select mutable values and pin immutable generations under
                // the writer lock, then read/decode blocks outside it. Holding
                // the lock here disabled bounded parallel point reads and
                // serialized ingestion behind ANN split payload lookups.
                var probe = try BoundProbeTxn(BackendType).open(self.backend, self.namespace);
                defer probe.abort();
                // Write reads select the current mutable and immutable view
                // together, even if the backend was empty when probe opened.
                probe.stable_point_view = false;
                try probe.getManySortedWithStats(miss_keys[0..miss_count], miss_values, false, null);
                for (miss_values) |*value| {
                    const present = value.* orelse {
                        misses += 1;
                        continue;
                    };
                    // The write transaction owns returned values beyond the
                    // probe's pinned-block lifetime, including across writes.
                    const owned = try self.allocator.dupe(u8, present);
                    errdefer self.allocator.free(owned);
                    try self.held_values.append(self.allocator, owned);
                    value.* = owned;
                    hits += 1;
                }
                for (miss_values, 0..) |maybe_value, miss_index| {
                    values[miss_indexes[miss_index]] = maybe_value;
                }
            }

            self.backend.recordGetManySortedResults(hits, misses);
        }

        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.closed) return error.TransactionClosed;
            try self.drainBulkAppendsToMutable();
            try self.mutable.upsert(self.allocator, self.namespace, key, value, false);
            self.invalidateCursorSnapshot();
            self.prefix_index.record(self.allocator, self.namespace, key, false);
        }

        pub fn appendPut(self: *@This(), key: []const u8, value: []const u8) !void {
            if (self.closed) return error.TransactionClosed;
            if (self.batch_options.mode == .bulk_ingest and self.mutable.entryCount() == 0) {
                try self.bulk_index.append(self.allocator, &self.bulk_appends, self.namespace, key, value);
                self.invalidateCursorSnapshot();
                self.prefix_index.record(self.allocator, self.namespace, key, false);
                return;
            }
            try self.mutable.appendUpsert(self.allocator, self.namespace, key, value, false);
            self.invalidateCursorSnapshot();
            self.prefix_index.record(self.allocator, self.namespace, key, false);
        }

        pub fn delete(self: *@This(), key: []const u8) !void {
            if (self.closed) return error.TransactionClosed;
            try self.drainBulkAppendsToMutable();
            try self.mutable.upsert(self.allocator, self.namespace, key, "", true);
            self.invalidateCursorSnapshot();
            self.prefix_index.record(self.allocator, self.namespace, key, true);
        }

        pub fn hasPrefix(self: *@This(), prefix: []const u8) !bool {
            if (self.closed) return error.TransactionClosed;
            return writerHasPrefix(BackendType, self, self.namespace, prefix);
        }

        pub fn openCursor(self: *@This()) !LocalCursor {
            if (self.closed) return error.TransactionClosed;
            try self.ensureCursorSnapshot();
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            return try LocalCursor.initView(cursor_alloc, self.backend, &self.cursor_overlay.?, self.cursor_immutable_memtables, self.cursor_read_view.?, self.namespace, false);
        }

        fn ensureCursorSnapshot(self: *@This()) !void {
            if (self.cursor_overlay != null) return;
            try self.drainBulkAppendsToMutable();
            try self.mutable.enableOrdered(self.allocator);
            var overlay = try self.mutable.snapshot(self.allocator);
            errdefer overlay.deinit(self.allocator);
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);

            var retained_now = false;
            if (!self.cursor_reader_retained) {
                try retainReadReader(BackendType, self.backend, .current_scan);
                self.cursor_reader_retained = true;
                retained_now = true;
            }
            errdefer if (retained_now) {
                releaseWriteReader(BackendType, self.backend, .current_scan);
                self.cursor_reader_retained = false;
            };

            const base_mutable = try snapshotReadMutable(BackendType, self.backend, .current_scan);
            errdefer base_mutable.release(self.backend);

            const backend_immutable = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try self.backend.snapshotImmutableMemtables()
            else
                &.{};
            var backend_immutable_pins_transferred = false;
            defer if (backend_immutable.len > 0) {
                if (backend_immutable_pins_transferred)
                    self.allocator.free(backend_immutable)
                else
                    releaseImmutableMemtableSnapshotList(BackendType, self.backend, backend_immutable);
            };

            const immutable = try self.allocator.alloc(*const State, 1 + backend_immutable.len);
            errdefer self.allocator.free(immutable);
            for (backend_immutable, 0..) |state, i| immutable[i + 1] = state;

            var read_view = try RunReadView.pin(self.backend, self.metadata_allocator);
            errdefer read_view.release(self.backend);
            try read_view.prepareCursor(self.backend);

            self.cursor_overlay = overlay;
            self.cursor_base_mutable = base_mutable;
            immutable[0] = base_mutable.state;
            backend_immutable_pins_transferred = true;
            self.cursor_immutable_memtables = immutable;
            self.cursor_read_view = read_view;
            self.cursor_runs = read_view.runs;
            self.cursor_l0_groups = read_view.l0_groups;
            self.cursor_levels = read_view.levels;
        }

        fn invalidateCursorSnapshot(self: *@This()) void {
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            self.invalidateCursorSnapshotLocked();
        }

        fn invalidateCursorSnapshotLocked(self: *@This()) void {
            if (self.cursor_overlay) |*state| {
                state.deinit(self.allocator);
                self.cursor_overlay = null;
            }
            if (self.cursor_base_mutable) |snapshot| {
                snapshot.release(self.backend);
                self.cursor_base_mutable = null;
            }
            if (self.cursor_immutable_memtables.len > 0) {
                releaseImmutableMemtablePins(BackendType, self.backend, self.cursor_immutable_memtables[1..]);
                self.allocator.free(self.cursor_immutable_memtables);
                self.cursor_immutable_memtables = &.{};
            }
            if (self.cursor_read_view) |view| view.release(self.backend);
            self.cursor_read_view = null;
            self.cursor_l0_groups = &.{};
            self.cursor_levels = &.{};
            self.cursor_runs = &.{};
        }
    };
}

/// Current-tip point reads across dynamic namespaces. Values are copied into
/// transaction-owned storage, so callers may issue several namespace probes
/// without pinning or cloning the mutable LSM generation.
pub fn NamespaceProbeTxn(comptime BackendType: type) type {
    const LocalProbeTxn = BoundProbeTxn(BackendType);
    return struct {
        allocator: Allocator,
        backend: *BackendType,
        held_values: PointResultValues = .empty,

        pub fn open(backend: *BackendType) @This() {
            return .{
                .allocator = runtimeScratchAllocator(backend.allocator),
                .backend = backend,
            };
        }

        pub fn abort(self: *@This()) void {
            releaseHeldValues(&self.held_values, self.allocator);
            self.* = undefined;
        }

        fn ownValue(self: *@This(), value: []const u8) ![]const u8 {
            const owned = try self.allocator.dupe(u8, value);
            errdefer self.allocator.free(owned);
            try self.held_values.append(self.allocator, owned);
            return owned;
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            var probe = try LocalProbeTxn.open(self.backend, namespace);
            defer probe.abort();
            return try self.ownValue(try probe.get(key));
        }

        pub fn getManySorted(
            self: *@This(),
            namespace: backend_types.Namespace,
            keys: []const []const u8,
            values: []?[]const u8,
        ) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            var probe = try LocalProbeTxn.open(self.backend, namespace);
            defer probe.abort();
            try probe.getManySorted(keys, values);
            for (values) |*value| {
                const present = value.* orelse continue;
                value.* = try self.ownValue(present);
            }
        }
    };
}

pub fn NamespaceReadTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        mutable_snapshot: *const State,
        owns_mutable_snapshot: bool = false,
        read_view: RunReadView,
        snapshot: ?State = null,
        immutable_memtables: []const *const State = &.{},
        runs: []Run = &.{},
        l0_groups: []RunGroup = &.{},
        levels: []RunLevel = &.{},
        last_l0_group_index: ?usize = null,
        read_hint: ?BorrowedReadHint = null,
        held_blocks: std.ArrayListUnmanaged(BlockPin) = .empty,
        held_values: PointResultValues = .empty,
        planning_scratch: BatchScratch.ProbeScratch = .{},

        pub fn open(backend: *BackendType) !@This() {
            return try openControlled(backend, null);
        }

        pub fn openWithAdmission(backend: *BackendType, control: read_admission.Port) !@This() {
            return try openControlled(backend, control);
        }

        fn openControlled(backend: *BackendType, control: ?read_admission.Port) !@This() {
            var captured = try openCaptured(backend, control);
            errdefer captured.abort();
            if (control) |owner| try owner.checkpoint();
            return captured;
        }

        fn openCaptured(backend: *BackendType, original: ?read_admission.Port) !@This() {
            const locked = if (original) |owner| try lockBackendWithAdmission(BackendType, backend, owner) else lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            var retained: read_admission.RetainedDependency = .{};
            const control = retained.bind(original);
            const metadata_allocator = runtimeScratchAllocator(backend.allocator);
            try retainReadReader(BackendType, backend, .namespace_read_txn);
            errdefer releaseReadReader(BackendType, backend, .namespace_read_txn);
            if (control) |owner| try owner.checkLifetime();
            if (@hasDecl(BackendType, "prepareReadSnapshot")) try backend.prepareReadSnapshot();
            const immutable_memtables = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try backend.snapshotImmutableMemtables()
            else
                &.{};
            errdefer releaseImmutableMemtableSnapshotList(BackendType, backend, immutable_memtables);
            if (control) |owner| try owner.checkLifetime();
            const mutable_snapshot = try snapshotReadMutable(BackendType, backend, .namespace_read_txn);
            errdefer {
                if (mutable_snapshot.owned) {
                    var owned = @constCast(mutable_snapshot.state);
                    owned.deinit(backend.allocator);
                    backend.allocator.destroy(owned);
                } else {
                    releaseMutableReadSnapshot(BackendType, backend, mutable_snapshot.state, false);
                }
            }
            if (control) |owner| try owner.checkLifetime();
            var read_view = try RunReadView.pin(backend, metadata_allocator);
            errdefer read_view.release(backend);
            if (control) |owner| try read_view.prepareCursorWithAdmission(backend, owner) else try read_view.prepareCursor(backend);
            if (control) |owner| try owner.checkLifetime();
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = metadata_allocator,
                .backend = backend,
                .mutable_snapshot = mutable_snapshot.state,
                .owns_mutable_snapshot = mutable_snapshot.owned,
                .immutable_memtables = immutable_memtables,
                .read_view = read_view,
                .runs = read_view.runs,
                .l0_groups = read_view.l0_groups,
                .levels = read_view.levels,
            };
        }

        pub fn abort(self: *@This()) void {
            self.planning_scratch.deinit(self.metadata_allocator);
            const backend = self.backend;
            if (self.owns_mutable_snapshot) {
                var owned = @constCast(self.mutable_snapshot);
                owned.deinit(self.allocator);
                self.allocator.destroy(owned);
            }
            if (self.snapshot) |*snapshot| snapshot.deinit(self.allocator);
            releaseHeldBlocks(&self.held_blocks, self.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            self.read_view.release(backend);
            releaseMutableReadSnapshot(BackendType, backend, self.mutable_snapshot, self.owns_mutable_snapshot);
            releaseImmutableMemtableSnapshotList(BackendType, backend, self.immutable_memtables);
            releaseReadReader(BackendType, backend, .namespace_read_txn);
            self.* = undefined;
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            self.backend.recordPointGet();
            return try getFromReadView(self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, &self.last_l0_group_index, &self.read_hint, &self.held_blocks, &self.held_values, self.allocator, namespace, key);
        }

        pub fn getWithAdmission(self: *@This(), namespace: backend_types.Namespace, key: []const u8, control: read_admission.Port) ![]const u8 {
            try control.checkpoint();
            self.backend.recordPointGet();
            const keys = [_][]const u8{key};
            var values: [1]?[]const u8 = .{null};
            _ = try readManySortedFromReadView(self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, self.allocator, &self.held_blocks, &self.held_values, namespace, &keys, &values, &self.planning_scratch, self.metadata_allocator, control);
            try control.checkpoint();
            return values[0] orelse error.NotFound;
        }

        pub fn getManySorted(self: *@This(), namespace: backend_types.Namespace, keys: []const []const u8, values: []?[]const u8) !void {
            if (keys.len != values.len) return error.InvalidBatch;
            @memset(values, null);
            self.backend.recordGetManySorted(keys.len);
            self.backend.recordGetManySortedLocality(keys);
            const result = try readManySortedFromReadView(self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, self.allocator, &self.held_blocks, &self.held_values, namespace, keys, values, &self.planning_scratch, self.metadata_allocator, null);
            self.backend.recordGetManySortedResults(result.hits, result.misses);
        }

        pub fn openCursor(self: *@This(), namespace: backend_types.Namespace) !LocalCursor {
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            return try LocalCursor.initView(cursor_alloc, self.backend, self.mutable_snapshot, self.immutable_memtables, self.read_view, namespace, false);
        }
    };
}

/// Detached data ownership. Value may be an interior slice of a verified table
/// block; retain its original allocation until deinit, even after backend close.
pub const OwnedNamespaceValue = struct {
    allocator: Allocator,
    allocation: []u8,
    offset: usize = 0,
    len: usize,
    pub fn bytes(self: @This()) []const u8 {
        return self.allocation[self.offset..][0..self.len];
    }
    pub fn deinit(self: *@This()) void {
        self.allocator.free(self.allocation);
        self.* = undefined;
    }
};

/// A caller-funded immutable-run index continuation. Footer and metadata use
/// one bound source, never a whole-file fallback or a reopened generation.
/// Source open, allocator internals and snapshot acquisition remain separate
/// resource/CPU work. Physical reads and metadata CRC use at most quantum;
/// footer validation consumes its fixed 48-byte record.
pub fn OwnedRunIndexRead(comptime BackendType: type) type {
    const Source = BackendType.RunRangeReader;
    return struct {
        pub const Status = enum { pending, ready };
        const Phase = enum { footer, allocate, metadata, decode, ready, taken, failed, closed };
        backend: *BackendType,
        allocator: Allocator,
        path: ?[]u8,
        source: ?Source,
        file_size: usize,
        footer_bytes: [lsm_table_file.footer_len]u8 = undefined,
        footer: ?lsm_table_file.Footer = null,
        position: usize = 0,
        metadata: ?[]u8 = null,
        decoder: ?lsm_table_file.IndexDecodeCursor = null,
        phase: Phase = .footer,
        failure: ?anyerror = null,
        load_ns: u64 = 0,
        decode_ns: u64 = 0,

        pub fn init(backend: *BackendType, allocator: Allocator, path: []const u8, file_size: u64, control: ?read_admission.Port) !@This() {
            if (control) |port| try port.checkpoint();
            if (file_size > repository_mod.maxRunFileReadBytes()) return error.FileTooBig;
            if (file_size < lsm_table_file.header_len + lsm_table_file.footer_len) return error.InvalidTableFile;
            const owned_path = try allocator.dupe(u8, path);
            errdefer allocator.free(owned_path);
            var source = try backend.openRunRangeReaderWithAdmission(owned_path, 0, @intCast(file_size), control);
            errdefer source.deinit();
            if (control) |port| try port.checkpoint();
            return .{ .backend = backend, .allocator = allocator, .path = owned_path, .source = source, .file_size = @intCast(file_size) };
        }
        pub fn step(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (quantum == 0 or quantum > 65536) return error.InvalidArgument;
            if (self.failure) |err| return err;
            if (self.phase == .closed or self.phase == .taken) return error.TransactionClosed;
            const decoding = self.phase == .decode;
            const started = self.backend.readStatsNowNs();
            defer {
                const elapsed = self.backend.readStatsElapsedNs(started);
                if (decoding) self.decode_ns +|= elapsed else self.load_ns +|= elapsed;
            }
            return self.advance(quantum, control) catch |err| {
                const cause = if (control) |port| blk: {
                    port.checkpoint() catch |original| break :blk original;
                    break :blk err;
                } else err;
                self.deinit();
                self.failure = cause;
                self.phase = .failed;
                return cause;
            };
        }
        fn advance(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (control) |port| try port.checkpoint();
            switch (self.phase) {
                .ready => return .ready,
                .footer => {
                    const take = @min(quantum, self.footer_bytes.len - self.position);
                    try self.source.?.readInto(self.file_size - self.footer_bytes.len + self.position, self.footer_bytes[self.position..][0..take], control);
                    self.position += take;
                    if (self.position == self.footer_bytes.len) {
                        if (!lsm_table_file.hasFooterMagic(&self.footer_bytes)) return error.UnsupportedVersion;
                        const footer = try lsm_table_file.decodeFooterBytes(&self.footer_bytes);
                        const end = self.file_size - lsm_table_file.footer_len;
                        if (footer.metadata_offset > end or footer.metadata_len != end - footer.metadata_offset) return error.InvalidTableFile;
                        self.footer = footer;
                        self.position = 0;
                        self.phase = .allocate;
                    }
                },
                .allocate => {
                    self.metadata = try self.allocator.alloc(u8, self.footer.?.metadata_len);
                    self.phase = .metadata;
                },
                .metadata => {
                    const bytes = self.metadata.?;
                    const take = @min(quantum, bytes.len - self.position);
                    try self.source.?.readInto(self.footer.?.metadata_offset + self.position, bytes[self.position..][0..take], control);
                    self.position += take;
                    if (self.position == bytes.len) {
                        self.decoder = lsm_table_file.IndexDecodeCursor.init(self.allocator, self.footer.?, bytes);
                        self.metadata = null;
                        self.retireSource();
                        self.phase = .decode;
                    }
                },
                .decode => if (try self.decoder.?.step(quantum, control) == .ready) {
                    self.phase = .ready;
                },
                else => unreachable,
            }
            if (control) |port| try port.checkpoint();
            return if (self.phase == .ready) .ready else .pending;
        }
        pub fn takeOutput(self: *@This()) !lsm_table_file.RetainedIndex {
            if (self.failure) |err| return err;
            if (self.phase != .ready) return error.InvalidArgument;
            const index = try self.decoder.?.takeOutput();
            self.decoder.?.deinit();
            self.decoder = null;
            self.backend.recordTableIndexLoad(self.load_ns +| self.decode_ns);
            self.backend.recordTableIndexDecode(self.decode_ns);
            self.phase = .taken;
            return index;
        }
        fn retireSource(self: *@This()) void {
            if (self.source) |*source| source.deinit();
            self.source = null;
            if (self.path) |path| self.allocator.free(path);
            self.path = null;
        }
        pub fn deinit(self: *@This()) void {
            if (self.decoder) |*decoder| decoder.deinit();
            self.decoder = null;
            if (self.metadata) |bytes| self.allocator.free(bytes);
            self.metadata = null;
            self.retireSource();
            self.phase = .closed;
        }
    };
}

/// Owns the namespace snapshot as well as lookup/decode progress. No control,
/// stack key or self-relative cursor escapes a step. Pending owners must join
/// retirement before backend close; backend reader pins prevent early teardown.
/// Snapshot capture and allocation remain separate metadata work,
/// not yet byte-bounded physical continuations for every provider/codec.
pub fn OwnedNamespaceValueRead(comptime BackendType: type) type {
    const Txn = NamespaceReadTxn(BackendType);
    const Block = OwnedRunBlockRead(BackendType);
    const Directory = @import("run_directory.zig").Directory;
    return struct {
        pub const Status = enum { pending, ready };
        const Phase = enum { mutable, immutable, runs, run_state, index, block_init, block_decode, block_search, copy_allocate, copy, ready, taken, failed, closed };
        backend: *BackendType,
        allocator: Allocator,
        txn: ?Txn,
        namespace: backend_types.Namespace,
        key: []u8,
        directory_cursor: ?Directory.ReadPointCursor = null,
        immutable_index: usize = 0,
        run_index: usize = 0,
        current_run: ?Run = null,
        index: ?lsm_table_file.RetainedIndex = null,
        index_read: ?OwnedRunIndexRead(BackendType) = null,
        block_index: usize = 0,
        block: ?Block = null,
        decoded: ?[]u8 = null,
        copy_source: ?[]const u8 = null,
        copy_offset: usize = 0,
        result: ?OwnedNamespaceValue = null,
        phase: Phase = .mutable,
        failure: ?anyerror = null,

        pub fn init(backend: *BackendType, allocator: Allocator, namespace: backend_types.Namespace, key: []const u8, control: ?read_admission.Port) !@This() {
            if (control) |port| try port.checkpoint();
            const owned_key = try allocator.dupe(u8, key);
            errdefer allocator.free(owned_key);
            var owned_namespace = namespace;
            owned_namespace.name = if (namespace.name) |name| try allocator.dupe(u8, name) else null;
            errdefer if (owned_namespace.name) |name| allocator.free(name);
            var txn = if (control) |port| try Txn.openWithAdmission(backend, port) else try Txn.open(backend);
            errdefer txn.abort();
            if (control) |port| try port.checkpoint();
            backend.recordPointGet();
            return .{ .backend = backend, .allocator = allocator, .txn = txn, .namespace = owned_namespace, .key = owned_key, .directory_cursor = if (txn.read_view.directory()) |directory| directory.readPoint(owned_namespace.name, owned_key) else null };
        }

        pub fn step(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (quantum == 0 or quantum > Block.max_step_bytes) return error.InvalidArgument;
            switch (self.phase) {
                .failed => return self.failure.?,
                .closed, .taken => return error.InvalidArgument,
                else => {},
            }
            return self.advance(quantum, control) catch |err| {
                const cause = if (control) |port| blk: {
                    port.checkpoint() catch |original| break :blk original;
                    break :blk err;
                } else err;
                self.discardResult();
                self.retireWorking();
                self.phase = .failed;
                self.failure = cause;
                return cause;
            };
        }

        fn selectEntry(self: *@This(), entry: anytype) !void {
            if (entry.tombstone) return error.NotFound;
            self.copy_source = entry.value;
            self.phase = .copy_allocate;
        }

        fn advance(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (control) |port| try port.checkpoint();
            switch (self.phase) {
                .ready => return .ready,
                .mutable => {
                    const state = self.txn.?.mutable_snapshot;
                    if (state.findIndex(self.namespace, self.key)) |index| {
                        self.backend.recordMutableHit();
                        try self.selectEntry(state.entryAt(index));
                    } else self.phase = .immutable;
                },
                .immutable => {
                    const states = self.txn.?.immutable_memtables;
                    if (self.immutable_index < states.len) {
                        const state = states[self.immutable_index];
                        self.immutable_index += 1;
                        if (state.findIndex(self.namespace, self.key)) |index| {
                            self.backend.recordMutableHit();
                            try self.selectEntry(state.entryAt(index));
                        }
                    } else self.phase = .runs;
                },
                .runs => {
                    const run = if (self.directory_cursor) |*cursor| blk: {
                        var credits: usize = 64;
                        if (cursor.nextWithBudget(&credits)) |handle| break :blk handle.run.*;
                        if (cursor.done()) return error.NotFound;
                        return .pending;
                    } else blk: {
                        const runs = self.txn.?.read_view.runs;
                        if (self.run_index == runs.len) return error.NotFound;
                        const candidate = runs[self.run_index];
                        self.run_index += 1;
                        break :blk candidate;
                    };
                    if (runMayContain(run, self.namespace, self.key)) {
                        self.current_run = run;
                        self.current_run.?.shared_read_version = true;
                        self.backend.recordRunProbe();
                        self.phase = if (run.state != null) .run_state else .index;
                    }
                },
                .run_state => {
                    const state = &self.current_run.?.state.?;
                    if (state.findIndex(self.namespace, self.key)) |index| try self.selectEntry(state.entryAt(index)) else {
                        self.current_run = null;
                        self.phase = .runs;
                    }
                },
                .index => {
                    const path = self.current_run.?.path orelse return error.RunStateUnavailable;
                    if (self.index_read == null) {
                        self.index_read = try OwnedRunIndexRead(BackendType).init(self.backend, self.allocator, path, self.current_run.?.size_bytes, control);
                        return .pending;
                    }
                    if (try self.index_read.?.step(quantum, control) != .ready) return .pending;
                    self.index = try self.index_read.?.takeOutput();
                    self.index_read.?.deinit();
                    self.index_read = null;
                    try requireTableBlocks(self.index.?.view());
                    if (self.index.?.view().findBlockIndex(self.namespace.name, self.key)) |block_index| {
                        const block = self.index.?.view().blocks[block_index];
                        if (block.mayContainKeyByBounds(self.namespace.name, self.key) and block.maybeContains(self.namespace.name, self.key)) {
                            self.block_index = block_index;
                            self.phase = .block_init;
                        } else self.nextRun();
                    } else self.nextRun();
                },
                .block_init => {
                    const window = self.index.?.view().blockWindow(self.block_index);
                    const offset = std.math.add(u64, self.index.?.view().entry_data_start, window.physicalRelativeOffset()) catch return error.InvalidTableFile;
                    self.block = try Block.init(self.backend, self.allocator, self.current_run.?.path.?, offset, window, control);
                    self.phase = .block_decode;
                },
                .block_decode => if (try self.block.?.step(quantum, control) == .ready) {
                    self.decoded = try self.block.?.takeOutput();
                    self.block.?.deinit();
                    self.block = null;
                    self.phase = .block_search;
                },
                .block_search => {
                    if (try lsm_table_file.findExactEntryInBlock(self.index.?.view(), self.decoded.?, self.block_index, self.namespace.name, self.key)) |positioned| {
                        const entry = positioned.entry;
                        if (entry.tombstone) return error.NotFound;
                        const offset = @intFromPtr(entry.value.ptr) - @intFromPtr(self.decoded.?.ptr);
                        self.result = .{ .allocator = self.allocator, .allocation = self.decoded.?, .offset = offset, .len = entry.value.len };
                        self.decoded = null;
                        if (self.current_run.?.level == 0) self.backend.recordL0Hit() else self.backend.recordLevelHit();
                        if (control) |port| try port.checkpoint();
                        self.retireWorking();
                        self.phase = .ready;
                        return .ready;
                    }
                    self.nextRun();
                },
                .copy_allocate => {
                    self.result = .{ .allocator = self.allocator, .allocation = try self.allocator.alloc(u8, self.copy_source.?.len), .len = self.copy_source.?.len };
                    self.phase = .copy;
                },
                .copy => {
                    const source = self.copy_source.?;
                    const take = @min(quantum, source.len - self.copy_offset);
                    @memcpy(self.result.?.allocation[self.copy_offset..][0..take], source[self.copy_offset..][0..take]);
                    self.copy_offset += take;
                    if (self.copy_offset == source.len) {
                        if (control) |port| try port.checkpoint();
                        self.retireWorking();
                        self.phase = .ready;
                        return .ready;
                    }
                },
                else => unreachable,
            }
            if (control) |port| try port.checkpoint();
            return .pending;
        }

        fn nextRun(self: *@This()) void {
            if (self.decoded) |bytes| self.allocator.free(bytes);
            self.decoded = null;
            if (self.index_read) |*read| read.deinit();
            self.index_read = null;
            if (self.index) |*index| index.deinit();
            self.index = null;
            self.current_run = null;
            self.phase = .runs;
        }

        pub fn takeOutput(self: *@This()) !OwnedNamespaceValue {
            if (self.phase != .ready) return error.InvalidArgument;
            const result = self.result.?;
            self.result = null;
            self.phase = .taken;
            return result;
        }

        fn discardResult(self: *@This()) void {
            if (self.result) |*result| result.deinit();
            self.result = null;
        }

        fn retireWorking(self: *@This()) void {
            if (self.block) |*block| block.deinit();
            self.block = null;
            self.nextRun();
            self.copy_source = null;
            self.directory_cursor = null;
            self.allocator.free(self.key);
            self.key = &.{};
            if (self.namespace.name) |name| self.allocator.free(name);
            self.namespace.name = null;
            // Last backend-dependent action: close can proceed as soon as this
            // snapshot reader pin retires. Do not touch backend afterward.
            if (self.txn) |*txn| txn.abort();
            self.txn = null;
        }

        pub fn deinit(self: *@This()) void {
            self.discardResult();
            self.retireWorking();
            self.phase = .closed;
        }
    };
}

/// Reusable snapshot key cursor. Each step decodes at most one block quantum,
/// probes one row, advances one source, or copies one key quantum. Index loads,
/// snapshot capture and allocations remain separate metadata work. Key views
/// borrow this owner until seek/handoff/deinit; no control borrow is retained.
pub fn OwnedNamespaceKeyCursor(comptime BackendType: type) type {
    const Txn = NamespaceReadTxn(BackendType);
    const Block = OwnedRunBlockRead(BackendType);
    const Directory = @import("run_directory.zig").Directory;
    return struct {
        pub const Status = enum { pending, ready };
        const Phase = enum { idle, mutable, immutable, runs, run_state, index, block_init, block_decode, search, candidate, copy_allocate, copy, finish, ready, transferred, failed, closed };
        backend: *BackendType,
        allocator: Allocator,
        txn: ?Txn,
        namespace: backend_types.Namespace,
        lower: []u8 = &.{},
        upper: ?[]u8 = null,
        exclusive: bool = false,
        directory_cursor: ?Directory.Cursor = null,
        immutable_index: usize = 0,
        run_index: usize = 0,
        current_run: ?Run = null,
        index: ?lsm_table_file.RetainedIndex = null,
        index_read: ?OwnedRunIndexRead(BackendType) = null,
        block_index: usize = 0,
        block: ?Block = null,
        decoded: ?[]u8 = null,
        lo: usize = 0,
        hi: usize = 0,
        best: ?OwnedNamespaceValue = null,
        best_tombstone: bool = false,
        candidate_tombstone: bool = false,
        candidate: ?OwnedNamespaceValue = null,
        copy_source: ?[]const u8 = null,
        copy_offset: usize = 0,
        next_phase: Phase = .runs,
        phase: Phase = .idle,
        failure: ?anyerror = null,

        pub fn init(backend: *BackendType, allocator: Allocator, namespace: backend_types.Namespace, control: ?read_admission.Port) !@This() {
            if (control) |port| try port.checkpoint();
            var owned_namespace = namespace;
            owned_namespace.name = if (namespace.name) |name| try allocator.dupe(u8, name) else null;
            errdefer if (owned_namespace.name) |name| allocator.free(name);
            var txn = if (control) |port| try Txn.openWithAdmission(backend, port) else try Txn.open(backend);
            errdefer txn.abort();
            if (control) |port| try port.checkpoint();
            return .{ .backend = backend, .allocator = allocator, .txn = txn, .namespace = owned_namespace };
        }
        fn checkOpen(self: *const @This()) !void {
            if (self.failure) |err| return err;
            if (self.phase == .closed or self.phase == .transferred) return error.TransactionClosed;
        }
        pub fn seek(self: *@This(), lower: []const u8, upper: ?[]const u8, exclusive: bool, control: ?read_admission.Port) !void {
            try self.checkOpen();
            if (control) |port| try port.checkpoint();
            const owned_lower = try self.allocator.dupe(u8, lower);
            errdefer self.allocator.free(owned_lower);
            const owned_upper = if (upper) |limit| try self.allocator.dupe(u8, limit) else null;
            errdefer if (owned_upper) |limit| self.allocator.free(limit);
            if (control) |port| try port.checkpoint();
            self.clearQuery();
            self.lower = owned_lower;
            self.upper = owned_upper;
            self.exclusive = exclusive;
            self.restartSources();
            // Callers may pass key() or previous bounds back into seek.
            // clearQuery retired those borrows; compare the owned copies.
            if (self.upper) |limit| {
                if (std.mem.order(u8, self.lower, limit) != .lt) self.phase = .ready;
            }
        }
        fn restartSources(self: *@This()) void {
            self.immutable_index = 0;
            self.run_index = 0;
            self.directory_cursor = if (self.txn.?.read_view.directory()) |directory| directory.readCursor() else null;
            self.phase = .mutable;
        }
        pub fn step(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (quantum == 0 or quantum > Block.max_step_bytes) return error.InvalidArgument;
            try self.checkOpen();
            if (self.phase == .idle) return error.InvalidArgument;
            return self.advance(quantum, control) catch |err| {
                const cause = if (control) |port| blk: {
                    port.checkpoint() catch |original| break :blk original;
                    break :blk err;
                } else err;
                self.deinit();
                self.failure = cause;
                self.phase = .failed;
                return cause;
            };
        }
        fn consider(self: *@This(), entry: anytype, next_phase: Phase) void {
            self.next_phase = next_phase;
            if (self.upper) |upper| if (std.mem.order(u8, entry.key, upper) != .lt) {
                self.endSource();
                return;
            };
            // Sources are visited newest first; equal keys retain the newer
            // value or tombstone already selected.
            if (self.best) |best| if (std.mem.order(u8, entry.key, best.bytes()) != .lt) {
                self.endSource();
                return;
            };
            self.copy_source = entry.key;
            self.candidate_tombstone = entry.tombstone;
            self.copy_offset = 0;
            self.phase = .copy_allocate;
        }
        fn stateCandidate(self: *@This(), state: *const State, next_phase: Phase) void {
            if (nextStateIndex(state, self.namespace, self.lower, !self.exclusive)) |row|
                self.consider(state.entryAt(row), next_phase)
            else {
                self.next_phase = next_phase;
                self.endSource();
            }
        }
        fn blockEntry(self: *@This(), row: usize) !lsm_table_file.Entry {
            const index = self.index.?.view();
            const start = index.entryStartInBlock(row, self.block_index);
            const base = index.blocks[self.block_index].relative_offset;
            if (start < base) return error.InvalidTableFile;
            return lsm_table_file.parseEntryAt(self.decoded.?, start - base);
        }
        fn advance(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (control) |port| try port.checkpoint();
            switch (self.phase) {
                .ready => return .ready,
                .mutable => self.stateCandidate(self.txn.?.mutable_snapshot, .immutable),
                .immutable => {
                    const states = self.txn.?.immutable_memtables;
                    if (self.immutable_index == states.len) self.phase = .runs else {
                        const state = states[self.immutable_index];
                        self.immutable_index += 1;
                        self.stateCandidate(state, .immutable);
                    }
                },
                .runs => {
                    const run = if (self.directory_cursor) |*cursor| blk: {
                        const handle = cursor.next() orelse {
                            self.phase = .finish;
                            break :blk null;
                        };
                        break :blk @as(?Run, handle.run.*);
                    } else blk: {
                        const runs = self.txn.?.read_view.runs;
                        if (self.run_index == runs.len) {
                            self.phase = .finish;
                            break :blk null;
                        }
                        const next = runs[self.run_index];
                        self.run_index += 1;
                        break :blk @as(?Run, next);
                    };
                    if (run) |candidate| if (runMayContainAtOrAfter(candidate, self.namespace, self.lower) and
                        compareNamespace(self.namespace, .{ .name = candidate.smallest_namespace_name }) != .lt and
                        (self.upper == null or compareRunBound(candidate.smallest_namespace_name, candidate.smallest_key, self.namespace.name, self.upper.?) == .lt))
                    {
                        self.current_run = candidate;
                        self.current_run.?.shared_read_version = true;
                        self.phase = if (candidate.state != null) .run_state else .index;
                    };
                },
                .run_state => self.stateCandidate(&self.current_run.?.state.?, .runs),
                .index => {
                    if (self.index_read == null) {
                        self.index_read = try OwnedRunIndexRead(BackendType).init(self.backend, self.allocator, self.current_run.?.path orelse return error.RunStateUnavailable, self.current_run.?.size_bytes, control);
                        return .pending;
                    }
                    if (try self.index_read.?.step(quantum, control) != .ready) return .pending;
                    self.index = try self.index_read.?.takeOutput();
                    self.index_read.?.deinit();
                    self.index_read = null;
                    try requireTableBlocks(self.index.?.view());
                    if (self.index.?.view().findBlockLowerBound(self.namespace.name, self.lower)) |block_index| {
                        self.block_index = block_index;
                        self.phase = .block_init;
                    } else self.endRun();
                },
                .block_init => {
                    if (self.block_index == self.index.?.view().blockCount()) self.endRun() else {
                        const meta = self.index.?.view().blocks[self.block_index];
                        const past_upper = if (self.upper) |upper| if (meta.smallest_key) |smallest| compareRunBound(meta.smallest_namespace_name, smallest, self.namespace.name, upper) != .lt else false else false;
                        const lower_order = compareRunBound(meta.largest_namespace_name, meta.largest_key, self.namespace.name, self.lower);
                        if (past_upper or compareNamespace(self.namespace, .{ .name = meta.smallest_namespace_name }) == .lt) {
                            self.endRun();
                        } else if (lower_order == .lt or (lower_order == .eq and self.exclusive)) {
                            self.block_index += 1;
                        } else {
                            const window = self.index.?.view().blockWindow(self.block_index);
                            const offset = std.math.add(u64, self.index.?.view().entry_data_start, window.physicalRelativeOffset()) catch return error.InvalidTableFile;
                            self.block = try Block.init(self.backend, self.allocator, self.current_run.?.path.?, offset, window, control);
                            self.phase = .block_decode;
                        }
                    }
                },
                .block_decode => if (try self.block.?.step(quantum, control) == .ready) {
                    self.decoded = try self.block.?.takeOutput();
                    self.block.?.deinit();
                    self.block = null;
                    const meta = self.index.?.view().blocks[self.block_index];
                    self.lo = meta.first_entry_index;
                    self.hi = self.lo + meta.entry_count;
                    self.phase = .search;
                },
                .search => {
                    if (self.lo < self.hi) {
                        const mid = self.lo + (self.hi - self.lo) / 2;
                        const entry = try self.blockEntry(mid);
                        const order = compareRunBound(entry.namespace_name, entry.key, self.namespace.name, self.lower);
                        if (order == .lt or (order == .eq and self.exclusive)) self.lo = mid + 1 else self.hi = mid;
                    } else self.phase = .candidate;
                },
                .candidate => {
                    const meta = self.index.?.view().blocks[self.block_index];
                    if (self.lo == meta.first_entry_index + meta.entry_count) {
                        self.clearBlock();
                        self.block_index += 1;
                        self.phase = .block_init;
                    } else {
                        const entry = try self.blockEntry(self.lo);
                        const order = compareNamespace(.{ .name = entry.namespace_name }, self.namespace);
                        if (order == .eq) self.consider(entry, .runs) else if (order == .gt) self.endRun() else return error.InvalidTableFile;
                    }
                },
                .copy_allocate => {
                    self.candidate = .{ .allocator = self.allocator, .allocation = try self.allocator.alloc(u8, self.copy_source.?.len), .len = self.copy_source.?.len };
                    self.phase = .copy;
                },
                .copy => {
                    const take = @min(quantum, self.copy_source.?.len - self.copy_offset);
                    @memcpy(self.candidate.?.allocation[self.copy_offset..][0..take], self.copy_source.?[self.copy_offset..][0..take]);
                    self.copy_offset += take;
                    if (self.copy_offset == self.copy_source.?.len) {
                        if (self.best) |*best| best.deinit();
                        self.best = self.candidate;
                        self.candidate = null;
                        self.best_tombstone = self.candidate_tombstone;
                        self.endSource();
                    }
                },
                .finish => {
                    if (self.best_tombstone and self.best != null) {
                        // A newest tombstone hides every older copy. Seek the
                        // next key in the same pinned snapshot, never live tip.
                        self.allocator.free(self.lower);
                        self.lower = self.best.?.allocation;
                        self.best = null;
                        self.best_tombstone = false;
                        self.exclusive = true;
                        self.restartSources();
                    } else self.phase = .ready;
                },
                else => unreachable,
            }
            if (control) |port| try port.checkpoint();
            return if (self.phase == .ready) .ready else .pending;
        }
        pub fn key(self: *const @This()) !?[]const u8 {
            try self.checkOpen();
            if (self.phase != .ready) return error.InvalidArgument;
            return if (self.best) |best| best.bytes() else null;
        }
        /// Transfer the very same snapshot into point-value capture. No key or
        /// namespace is borrowed, no allocator bridge is replaced, no new read
        /// observes an intervening mutation. This cursor becomes inert.
        pub fn intoValueRead(self: *@This(), control: ?read_admission.Port) !OwnedNamespaceValueRead(BackendType) {
            try self.checkOpen();
            if (self.phase != .ready) return error.InvalidArgument;
            if (control) |port| try port.checkpoint();
            const best = self.best orelse return error.NotFound;
            const txn = self.txn.?;
            const result: OwnedNamespaceValueRead(BackendType) = .{
                .backend = self.backend,
                .allocator = self.allocator,
                .txn = txn,
                .namespace = self.namespace,
                .key = best.allocation,
                .directory_cursor = if (txn.read_view.directory()) |directory| directory.readPoint(self.namespace.name, best.bytes()) else null,
            };
            self.txn = null;
            self.namespace.name = null;
            self.best = null;
            self.clearQuery();
            self.phase = .transferred;
            return result;
        }
        fn clearBlock(self: *@This()) void {
            if (self.block) |*block| block.deinit();
            self.block = null;
            if (self.decoded) |bytes| self.allocator.free(bytes);
            self.decoded = null;
        }
        fn endRun(self: *@This()) void {
            self.clearBlock();
            if (self.index_read) |*read| read.deinit();
            self.index_read = null;
            if (self.index) |*index| index.deinit();
            self.index = null;
            self.current_run = null;
            self.phase = .runs;
        }
        fn endSource(self: *@This()) void {
            self.copy_source = null;
            if (self.next_phase == .runs) self.endRun() else self.phase = self.next_phase;
        }
        fn clearQuery(self: *@This()) void {
            self.endRun();
            if (self.candidate) |*candidate| candidate.deinit();
            self.candidate = null;
            if (self.best) |*best| best.deinit();
            self.best = null;
            self.best_tombstone = false;
            self.copy_source = null;
            self.allocator.free(self.lower);
            self.lower = &.{};
            if (self.upper) |upper| self.allocator.free(upper);
            self.upper = null;
            self.directory_cursor = null;
        }
        pub fn deinit(self: *@This()) void {
            self.clearQuery();
            if (self.namespace.name) |name| self.allocator.free(name);
            self.namespace.name = null;
            if (self.txn) |*txn| txn.abort(); // Final backend-dependent action.
            self.txn = null;
            self.phase = .closed;
        }
    };
}

fn borrowRunSnapshotList(comptime BackendType: type, backend: *BackendType, allocator: Allocator, source: anytype) ![]Run {
    const runs = try allocator.alloc(Run, run_store.len(source));
    var initialized: usize = 0;
    errdefer {
        for (runs[0..initialized]) |*run| {
            if (run.releaseMemory()) continue;
            if (@hasDecl(BackendType, "releaseRunSnapshotRef")) {
                backend.releaseRunSnapshotRef(run);
            }
            run.deinit(allocator);
        }
        allocator.free(runs);
    }

    for (0..run_store.len(source)) |i| {
        const run = run_store.get(source, i);
        // Private/oracle views own metadata too. A file pin alone cannot
        // preserve strings or in-memory state retired by a writer edit.
        runs[i] = try repository_mod.cloneRunCompactionSnapshot(allocator, run);
        runs[i].shared_read_version = true;
        initialized = i + 1;
        if (runs[i].shared_memory != null) continue;
        if (@hasDecl(BackendType, "retainRunSnapshotRef")) {
            try backend.retainRunSnapshotRef(&runs[i]);
        }
    }
    return runs;
}

fn freeRunSnapshotList(comptime BackendType: type, backend: *BackendType, allocator: Allocator, runs: []Run) void {
    for (runs) |*run| {
        if (run.releaseMemory()) continue;
        if (@hasDecl(BackendType, "releaseRunSnapshotRef")) {
            backend.releaseRunSnapshotRef(run);
        }
        run.deinit(allocator);
    }
    allocator.free(runs);
}

fn deinitRunGroups(allocator: Allocator, groups: []RunGroup) void {
    for (groups) |*group| group.deinit(allocator);
    allocator.free(groups);
}

/// A point read needs only overlapping SSTs, not the epoch's global scan
/// projection. The caller pins the immutable directory and all memtables.
fn getFromDirectoryPoint(
    backend: anytype,
    directory: *const @import("run_directory.zig").Directory,
    immutable_memtables: []const *const State,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) ![]const u8 {
    return getFromDirectoryPointWithLifetime(backend, directory, immutable_memtables, read_hint, held_blocks, held_values, value_allocator, namespace, key, .snapshot_pinned);
}

fn getFromDirectoryPointWithLifetime(
    backend: anytype,
    directory: *const @import("run_directory.zig").Directory,
    immutable_memtables: []const *const State,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    lifetime: PointResultLifetime,
) ![]const u8 {
    read_hint.* = null;
    defer read_hint.* = null; // Candidate positions are local to this lookup.
    for (immutable_memtables) |state| if (state.findIndex(namespace, key)) |index| {
        const entry = state.entryAt(index);
        if (entry.tombstone) return error.NotFound;
        backend.recordMutableHit();
        return entry.value;
    };
    const resources = @import("../resource_manager.zig");
    var admitted: ?resources.BudgetedAllocator = null;
    if (comptime @hasField(@TypeOf(backend.options), "resource_manager")) if (backend.options.resource_manager) |manager| {
        admitted = resources.BudgetedAllocator.init(manager, .lsm_in_memory_state, value_allocator, 1);
    };
    defer if (admitted) |*budget| budget.deinit();
    const scratch = if (admitted) |*budget| budget.allocator() else value_allocator;
    return getFromDirectoryPointCandidatesWithLifetime(backend, directory, read_hint, held_blocks, held_values, scratch, value_allocator, namespace, key, lifetime) catch |err| {
        if (admitted) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
}

fn getFromDirectoryPointCandidates(
    backend: anytype,
    directory: *const @import("run_directory.zig").Directory,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    scratch: Allocator,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) ![]const u8 {
    return getFromDirectoryPointCandidatesWithLifetime(backend, directory, read_hint, held_blocks, held_values, scratch, value_allocator, namespace, key, .snapshot_pinned);
}

fn getFromDirectoryPointCandidatesWithLifetime(
    backend: anytype,
    directory: *const @import("run_directory.zig").Directory,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    scratch: Allocator,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    lifetime: PointResultLifetime,
) ![]const u8 {
    if (directory.supportsAsyncPoints()) {
        if (try tryReadDirectoryPointAsync(backend, directory, held_blocks, held_values, value_allocator, namespace, key, lifetime)) |result| switch (result) {
            .hit => |value| return value,
            .miss, .tombstone => return error.NotFound,
        };
        var cursor = directory.readPoint(namespace.name, key);
        while (cursor.next()) |handle| {
            var run = [_]Run{handle.run.*};
            run[0].shared_read_version = true;
            // Array positions are local to each candidate. A hint from the
            // preceding run cannot identify the same run by index zero.
            read_hint.* = null;
            if (try getFromRunIndices(backend, &run, &.{0}, read_hint, held_blocks, held_values, value_allocator, namespace, key, false, null)) |value| {
                if (run[0].level == 0) backend.recordL0Hit() else backend.recordLevelHit();
                return value;
            }
        }
        return error.NotFound;
    }
    return getFromDirectoryPointCandidatesPlanned(backend, directory, read_hint, held_blocks, held_values, scratch, value_allocator, namespace, key);
}

fn getFromDirectoryPointCandidatesPlanned(
    backend: anytype,
    directory: *const @import("run_directory.zig").Directory,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    scratch: Allocator,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) ![]const u8 {
    const Directory = @import("run_directory.zig").Directory;
    var inline_handles: [16]Directory.Handle = undefined;
    var overflow: std.ArrayListUnmanaged(Directory.Handle) = .empty;
    defer overflow.deinit(scratch);
    var count: usize = 0;
    var cursor = directory.overlaps(namespace.name, key, namespace.name, key);
    while (!cursor.done()) {
        var budget: usize = 16384;
        while (cursor.next(&budget)) |handle| {
            if (count < inline_handles.len) inline_handles[count] = handle else {
                if (count == inline_handles.len) try overflow.appendSlice(scratch, &inline_handles);
                try overflow.append(scratch, handle);
            }
            count += 1;
        }
    }
    if (count == 0) return error.NotFound;
    const handles = if (count <= inline_handles.len) inline_handles[0..count] else overflow.items;
    std.mem.sort(Directory.Handle, handles, {}, Directory.readLess);
    var inline_runs: [16]Run = undefined;
    var inline_indices: [16]usize = undefined;
    const runs = if (count <= inline_runs.len) inline_runs[0..count] else try scratch.alloc(Run, count);
    defer if (count > inline_runs.len) scratch.free(runs);
    const indices = if (count <= inline_indices.len) inline_indices[0..count] else try scratch.alloc(usize, count);
    defer if (count > inline_indices.len) scratch.free(indices);
    for (handles, runs, indices, 0..) |handle, *run, *index, i| {
        run.* = handle.run.*;
        run.shared_read_version = true;
        index.* = i;
    }
    var start: usize = 0;
    while (start < count) {
        var end = start + 1;
        while (end < count and runs[end].level == runs[start].level) end += 1;
        if (try getFromRunIndices(backend, runs, indices[start..end], read_hint, held_blocks, held_values, value_allocator, namespace, key, false, null)) |value| {
            if (runs[start].level == 0) backend.recordL0Hit() else backend.recordLevelHit();
            return value;
        }
        start = end;
    }
    return error.NotFound;
}

/// A nullable decision distinguishes an absent key from a visible tombstone.
/// Never copy a selected row: callers need only its tombstone bit.
fn presenceInRun(backend: anytype, run: *Run, namespace: backend_types.Namespace, key: []const u8) !?bool {
    if (!runMayContain(run.*, namespace, key)) return null;
    backend.recordRunProbe();
    if (run.state) |*state| {
        const idx = state.findIndex(namespace, key) orelse return null;
        return !state.entryAt(idx).tombstone;
    }
    if (!try runMayContainWithFilterMaybeLocked(backend, run, namespace, key, false)) return null;
    if (run.path == null) {
        const table = try tableForRunMaybeLocked(backend, run, false);
        const idx = try table.findIndex(namespace.name, key) orelse return null;
        return !(try table.entryAt(idx)).tombstone;
    }
    var index_handle: ?cache_mod.Handle = if (backend.options.cache != null) try loadRunTableIndexHandle(backend, run) else null;
    defer if (index_handle) |*handle| handle.release();
    const index = if (index_handle) |handle| handle.runTableIndex() else try indexForRunNoCache(backend, run);
    try requireTableBlocks(index);
    const block_index = index.findBlockIndex(namespace.name, key) orelse return null;
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
    if (!block.maybeContains(namespace.name, key)) {
        backend.recordBloomNegative();
        return null;
    }
    const window = index.blockWindow(block_index);
    if (backend.options.cache) |cache| {
        const offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
        if (cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, window.physicalLen())) |retained| {
            var handle = retained;
            defer handle.release();
            backend.recordSharedBlockCacheHit();
            const found = try lsm_table_file.findExactEntryInBlock(index, handle.runTableBlock(), block_index, namespace.name, key) orelse return null;
            return !found.entry.tombstone;
        }
    }
    if (localBlockCacheEnabled(backend)) {
        if (retainLocalCachedBlock(backend, run, index, window, false)) |lease| {
            defer lease.release();
            const found = try lsm_table_file.findExactEntryInBlock(index, lease.bytes, block_index, namespace.name, key) orelse return null;
            return !found.entry.tombstone;
        }
    }
    if (window.compression == .prefix or window.compression == .prefix_snappy) {
        var workspace: ?LocalReader.Workspace = if (comptime @hasField(@TypeOf(backend.*), "local_reader")) try localWorkspace(backend, window, true) else null;
        defer if (workspace) |*work| work.release();
        const scratch = if (workspace) |*work| work.allocator() else backend.allocator;
        const offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
        const payload = try loadRunTableBlockWithStats(backend, scratch, run.path.?, offset, window.physicalLen());
        defer scratch.free(payload);
        try lsm_table_file.validateBlockPayload(payload, window.checksum);
        const decoded = if (window.compression == .prefix_snappy) blk: {
            try lsm_table_file.validatePrefixDecodedSize(payload, window.len);
            break :blk try @import("../../encoding/snappy.zig").decode(scratch, payload);
        } else payload;
        defer if (window.compression == .prefix_snappy) scratch.free(decoded);
        var reader = try lsm_table_file.PrefixPointReader.init(decoded, block.first_entry_index, window.len);
        defer reader.deinit(scratch);
        if (reader.entryCount() != block.entry_count) return error.InvalidTableFile;
        const found = try reader.find(scratch, namespace.name, key) orelse return null;
        return !found.entry.tombstone;
    }
    if (backend.options.cache != null) {
        var handle = try loadRunTableBlockHandle(backend, run, index, window, namespace.retainDataBlocks());
        defer handle.release();
        const found = try lsm_table_file.findExactEntryInBlock(index, handle.runTableBlock(), block_index, namespace.name, key) orelse return null;
        return !found.entry.tombstone;
    }
    const bytes = try loadOwnedBlockForWindow(backend, run, index, window);
    defer backend.allocator.free(bytes);
    const found = try lsm_table_file.findExactEntryInBlock(index, bytes, block_index, namespace.name, key) orelse return null;
    return !found.entry.tombstone;
}

fn containsFromReadView(backend: anytype, immutable_memtables: []const *const State, view: RunReadView, namespace: backend_types.Namespace, key: []const u8) !bool {
    for (immutable_memtables) |state| if (state.findIndex(namespace, key)) |idx| {
        backend.recordMutableHit();
        return !state.entryAt(idx).tombstone;
    };
    if (view.directory()) |directory| {
        var cursor = directory.readPoint(namespace.name, key);
        while (cursor.next()) |handle| {
            var run = handle.run.*;
            run.shared_read_version = true;
            if (try presenceInRun(backend, &run, namespace, key)) |present| return present;
        }
        return false;
    }
    if (findRunGroupIndex(view.l0_groups, namespace, key)) |group| {
        for (view.l0_groups[group].run_indices) |idx| {
            if (try presenceInRun(backend, &view.runs[idx], namespace, key)) |present| return present;
        }
    }
    for (view.levels) |level| {
        const idx = findRunIndexInLevel(view.runs, level, namespace, key) orelse continue;
        if (try presenceInRun(backend, &view.runs[idx], namespace, key)) |present| return present;
    }
    return false;
}

fn getFromReadView(
    backend: anytype,
    mutable: *const State,
    immutable_memtables: []const *const State,
    view: RunReadView,
    last_l0_group_index: *?usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: *std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) ![]const u8 {
    if (view.directory()) |directory| {
        if (mutable.findIndex(namespace, key)) |idx| {
            const entry = mutable.entryAt(idx);
            if (entry.tombstone) return error.NotFound;
            backend.recordMutableHit();
            return entry.value;
        }
        return getFromDirectoryPoint(backend, directory, immutable_memtables, read_hint, held_blocks, held_values, allocator, namespace, key);
    }
    return getFromSnapshotRuns(backend, mutable, immutable_memtables, view.runs, view.l0_groups, view.levels, last_l0_group_index, read_hint, held_blocks, held_values, allocator, namespace, key, false, null);
}

fn readManySortedFromReadView(
    backend: anytype,
    mutable: *const State,
    immutable_memtables: []const *const State,
    view: RunReadView,
    allocator: Allocator,
    held_blocks: *std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    reuse: *BatchScratch.ProbeScratch,
    metadata_allocator: Allocator,
    control: ?read_admission.Port,
) !BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    const plan = chooseMultiGetPlan(keys, .snapshot);
    recordMultiGetPlan(backend, plan);
    if (view.directory() == null) switch (plan) {
        .point => return readManySortedPointFromSnapshot(backend, mutable, immutable_memtables, view.runs, view.l0_groups, view.levels, allocator, held_blocks, held_values, namespace, keys, values, false, control),
        .sorted_by_run => return readManySortedByRunFromSnapshot(backend, mutable, immutable_memtables, view.runs, view.l0_groups, view.levels, allocator, held_blocks, held_values, namespace, keys, values, false, control),
        .cursor => {},
    };
    if (plan != .cursor) return readManySortedDirectoryBatch(backend, mutable, immutable_memtables, view.directory().?, allocator, held_blocks, held_values, namespace, keys, values, plan == .sorted_by_run, false, reuse, metadata_allocator, control);
    const CursorType = MergeCursor(@TypeOf(backend.*), State);
    const sequence: CursorType.RunSequence = if (view.directory()) |directory| .{ .directory = directory } else .{ .runs = view.runs, .levels = view.levels };
    var cursor = if (control) |budget|
        try CursorType.initSequenceControlled(runtimeScratchAllocator(allocator), backend, mutable, immutable_memtables, sequence, if (sequence.directory != null) &.{} else view.l0_groups, namespace, false, budget)
    else
        try CursorType.initView(runtimeScratchAllocator(allocator), backend, mutable, immutable_memtables, view, namespace, false);
    defer cursor.close();
    const guard = if (control) |budget| try cursor.admission.enter(budget) else null;
    defer if (guard) |scope| scope.deinit();
    return readManySortedFromCursor(backend, allocator, held_blocks, held_values, &cursor, keys, values);
}

/// Project only the union of point candidates. In particular a sparse batch
/// does not project all the SSTs between its first and last key. Existing
/// per-run batch index/block reuse remains intact.
fn readManySortedDirectoryBatch(
    backend: anytype,
    mutable: *const State,
    immutable_memtables: []const *const State,
    directory: *const @import("run_directory.zig").Directory,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    sorted_by_run: bool,
    backend_locked: bool,
    reuse: ?*BatchScratch.ProbeScratch,
    metadata_allocator: Allocator,
    control: ?read_admission.Port,
) !BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    if (!sorted_by_run) {
        @memset(values, null);
        if (try readManySortedPointFromSnapshotAsyncWithAdmission(backend, mutable, immutable_memtables, &.{}, &.{}, &.{}, allocator, held_values, namespace, keys, values, backend_locked, PointResultLifetime.forBlockPins(held_blocks), held_blocks, directory, control)) |result| return result;
    }
    if (reuse) |owner| {
        const planner = try owner.planning(metadata_allocator, runtimeScratchAllocator(allocator), backend.options.resource_manager);
        defer planner.finish();
        return readManySortedDirectoryCandidates(backend, mutable, immutable_memtables, directory, planner.arena.allocator(), allocator, held_blocks, held_values, namespace, keys, values, sorted_by_run, backend_locked, control) catch |err| {
            if (planner.denied()) return error.ResourceBudgetExceeded;
            return err;
        };
    }
    const resources = @import("../resource_manager.zig");
    var admitted: ?resources.BudgetedAllocator = null;
    if (comptime @hasField(@TypeOf(backend.options), "resource_manager")) if (backend.options.resource_manager) |manager| {
        admitted = resources.BudgetedAllocator.init(manager, .lsm_in_memory_state, runtimeScratchAllocator(allocator), 1);
    };
    defer if (admitted) |*budget| budget.deinit();
    const scratch = if (admitted) |*budget| budget.allocator() else runtimeScratchAllocator(allocator);
    return readManySortedDirectoryCandidates(backend, mutable, immutable_memtables, directory, scratch, allocator, held_blocks, held_values, namespace, keys, values, sorted_by_run, backend_locked, control) catch |err| {
        if (admitted) |*budget| if (budget.denied()) return error.ResourceBudgetExceeded;
        return err;
    };
}

fn readManySortedDirectoryCandidates(
    backend: anytype,
    mutable: *const State,
    immutable_memtables: []const *const State,
    directory: *const @import("run_directory.zig").Directory,
    scratch: Allocator,
    allocator: Allocator,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    sorted_by_run: bool,
    backend_locked: bool,
    control: ?read_admission.Port,
) !BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    const Directory = @import("run_directory.zig").Directory;
    var selected = std.ArrayListUnmanaged(Directory.Handle).empty;
    defer selected.deinit(scratch);
    // Classify each key once. Query the union using unresolved keys only,
    // rather than revisiting every memtable hit for every overlapping run.
    var unresolved = std.ArrayListUnmanaged([]const u8).empty;
    defer unresolved.deinit(scratch);
    const planning_keys = try directoryUnresolvedKeys(scratch, mutable, immutable_memtables, namespace, keys, &unresolved, control);
    var cursor = directory.sortedPoints(namespace.name, planning_keys);
    while (!cursor.done()) {
        if (control) |authority| try authority.checkpoint();
        var budget: usize = if (control != null) 64 else 16384;
        while (cursor.next(&budget)) |handle| {
            if (control) |authority| try authority.checkpoint();
            try selected.append(scratch, handle);
        }
    }
    const handles = selected.items;
    std.mem.sort(Directory.Handle, handles, {}, Directory.readLess);
    const runs = try scratch.alloc(Run, handles.len);
    defer scratch.free(runs);
    for (runs, handles) |*run, handle| {
        if (control) |authority| try authority.checkpoint();
        run.* = handle.run.*;
        run.shared_read_version = true;
    }
    const groups = try buildL0RunGroups(scratch, runs);
    defer deinitRunGroups(scratch, groups);
    const levels = try buildLowerLevels(scratch, runs);
    defer scratch.free(levels);
    if (sorted_by_run) return readManySortedByRunFromSnapshotWithScratch(backend, mutable, immutable_memtables, runs, groups, levels, allocator, held_blocks, held_values, namespace, keys, values, backend_locked, scratch, control);
    return readManySortedPointFromSnapshotWithScratch(backend, mutable, immutable_memtables, runs, groups, levels, allocator, held_blocks, held_values, namespace, keys, values, backend_locked, scratch, control);
}

fn directoryUnresolvedKeys(
    allocator: Allocator,
    mutable: anytype,
    immutable_memtables: []const *const State,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    unresolved: *std.ArrayListUnmanaged([]const u8),
    control: ?read_admission.Port,
) ![]const []const u8 {
    if (control) |budget| try budget.checkpoint();
    if (mutable.entryCount() == 0 and immutable_memtables.len == 0) return keys;
    var first_resolved: ?usize = null;
    for (keys, 0..) |key, i| {
        if (control) |budget| try budget.checkpoint();
        const resolved = found: {
            if (mutable.findIndex(namespace, key) != null) break :found true;
            for (immutable_memtables) |state| if (state.findIndex(namespace, key) != null) break :found true;
            break :found false;
        };
        if (resolved) {
            if (first_resolved == null) {
                first_resolved = i;
                try unresolved.appendSlice(allocator, keys[0..i]);
            }
        } else if (first_resolved != null) try unresolved.append(allocator, key);
    }
    // Cold batches with no memtable matches borrow the original key vector.
    return if (first_resolved != null) unresolved.items else keys;
}

fn retainSourcePointValueWithAdmission(backend: anytype, allocator: Allocator, held: *PointResultValues, namespace: backend_types.Namespace, value: []const u8, control: ?read_admission.Port) ![]const u8 {
    if (!namespace.own_source_point_results) return value;
    return PointResultLifetime.transaction_owned.retainWithAdmission(backend, allocator, held, held.items.len, value, control);
}

fn getFromSnapshotRuns(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    last_l0_group_index: *?usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) ![]const u8 {
    const control = if (batch_run_indexes) |indexes| indexes.control else null;
    if (control) |budget| try budget.checkpoint();
    if (mutable.findIndex(namespace, key)) |idx| {
        const entry = mutable.entryAt(idx);
        if (entry.tombstone) return error.NotFound;
        read_hint.* = null;
        backend.recordMutableHit();
        return try retainSourcePointValueWithAdmission(backend, value_allocator, held_values, namespace, entry.value, control);
    }
    for (immutable_memtables) |state| {
        if (state.findIndex(namespace, key)) |idx| {
            const entry = state.entryAt(idx);
            if (entry.tombstone) return error.NotFound;
            read_hint.* = null;
            backend.recordMutableHit();
            return try retainSourcePointValueWithAdmission(backend, value_allocator, held_values, namespace, entry.value, control);
        }
    }
    var candidate_group_index = if (last_l0_group_index.*) |hinted_index|
        if (hinted_index < l0_groups.len and groupMayContain(l0_groups[hinted_index], namespace, key)) hinted_index else null
    else
        null;
    if (candidate_group_index == null) {
        candidate_group_index = findRunGroupIndex(l0_groups, namespace, key);
    }
    last_l0_group_index.* = candidate_group_index;
    if (candidate_group_index) |group_index| {
        if (try getFromRunIndices(backend, runs, l0_groups[group_index].run_indices, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes)) |value| {
            backend.recordL0Hit();
            return value;
        }
    }
    for (levels) |level| {
        const run_index = findRunIndexInLevel(runs, level, namespace, key) orelse continue;
        const one = [_]usize{run_index};
        if (try getFromRunIndices(backend, runs, &one, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes)) |value| {
            backend.recordLevelHit();
            return value;
        }
    }
    read_hint.* = null;
    return error.NotFound;
}

fn buildL0RunGroups(allocator: Allocator, runs: []const Run) ![]RunGroup {
    return buildL0RunGroupsControlled(allocator, runs, null);
}

fn buildL0RunGroupsControlled(allocator: Allocator, runs: []const Run, control: ?read_admission.Port) ![]RunGroup {
    if (control) |owner| try owner.checkpoint();
    var l0_count: usize = 0;
    while (l0_count < runs.len and runs[l0_count].level == 0) : (l0_count += 1) {
        if (control) |owner| try owner.checkpoint();
    }
    return try buildRunGroupsControlled(allocator, runs[0..l0_count], control);
}

fn buildL0RunGroupsWithStats(backend: anytype, allocator: Allocator, runs: []const Run) ![]RunGroup {
    const BackendType = @TypeOf(backend.*);
    const start_ns = if (@hasDecl(BackendType, "readStatsNowNs")) backend.readStatsNowNs() else 0;
    var l0_count: usize = 0;
    while (l0_count < runs.len and runs[l0_count].level == 0) : (l0_count += 1) {}
    const groups = try buildRunGroups(allocator, runs[0..l0_count]);
    if (@hasDecl(BackendType, "recordRunGroupBuild")) {
        const elapsed_ns = if (@hasDecl(BackendType, "readStatsElapsedNs")) backend.readStatsElapsedNs(start_ns) else 0;
        backend.recordRunGroupBuild(runs.len, l0_count, elapsed_ns);
    }
    return groups;
}

fn buildLowerLevels(allocator: Allocator, runs: []const Run) ![]RunLevel {
    return buildLowerLevelsControlled(allocator, runs, null);
}

fn buildLowerLevelsControlled(allocator: Allocator, runs: []const Run, control: ?read_admission.Port) ![]RunLevel {
    if (control) |owner| try owner.checkpoint();
    var start: usize = 0;
    while (start < runs.len and runs[start].level == 0) : (start += 1) {
        if (control) |owner| try owner.checkpoint();
    }
    if (start >= runs.len) return try allocator.alloc(RunLevel, 0);

    var levels = std.ArrayListUnmanaged(RunLevel).empty;
    errdefer levels.deinit(allocator);

    var i = start;
    while (i < runs.len) {
        if (control) |owner| try owner.checkpoint();
        const level = runs[i].level;
        const level_start = i;
        while (i < runs.len and runs[i].level == level) : (i += 1) {
            if (control) |owner| try owner.checkpoint();
        }
        try levels.append(allocator, .{
            .level = level,
            .start_index = level_start,
            .len = i - level_start,
        });
    }
    if (control) |owner| try owner.checkpoint();
    return try levels.toOwnedSlice(allocator);
}

fn buildRunGroups(allocator: Allocator, runs: []const Run) ![]RunGroup {
    return buildRunGroupsControlled(allocator, runs, null);
}

fn buildRunGroupsControlled(allocator: Allocator, runs: []const Run, control: ?read_admission.Port) ![]RunGroup {
    if (control) |owner| try owner.checkpoint();
    const IndexedRun = struct {
        run_index: usize,
        smallest_namespace_name: ?[]const u8,
        smallest_key: []const u8,
        largest_namespace_name: ?[]const u8,
        largest_key: []const u8,
    };

    if (runs.len == 0) return try allocator.alloc(RunGroup, 0);

    var indexed = try allocator.alloc(IndexedRun, runs.len);
    defer allocator.free(indexed);
    for (runs, 0..) |run, i| {
        if (control) |owner| try owner.checkpoint();
        indexed[i] = .{
            .run_index = i,
            .smallest_namespace_name = run.smallest_namespace_name,
            .smallest_key = run.smallest_key,
            .largest_namespace_name = run.largest_namespace_name,
            .largest_key = run.largest_key,
        };
    }
    try read_admission.sort(IndexedRun, indexed, {}, struct {
        fn lessThan(_: void, lhs: IndexedRun, rhs: IndexedRun) bool {
            return compareRunBound(lhs.smallest_namespace_name, lhs.smallest_key, rhs.smallest_namespace_name, rhs.smallest_key) == .lt;
        }
    }.lessThan, control);

    var groups = std.ArrayListUnmanaged(RunGroup).empty;
    errdefer {
        for (groups.items) |*group| group.deinit(allocator);
        groups.deinit(allocator);
    }

    var current = std.ArrayListUnmanaged(usize).empty;
    defer current.deinit(allocator);

    var group_smallest_namespace_name = indexed[0].smallest_namespace_name;
    var group_smallest_key = indexed[0].smallest_key;
    var group_largest_namespace_name = indexed[0].largest_namespace_name;
    var group_largest_key = indexed[0].largest_key;
    try current.append(allocator, indexed[0].run_index);

    for (indexed[1..]) |run| {
        if (control) |owner| try owner.checkpoint();
        if (!rangesOverlap(
            run.smallest_namespace_name,
            run.smallest_key,
            run.largest_namespace_name,
            run.largest_key,
            group_smallest_namespace_name,
            group_smallest_key,
            group_largest_namespace_name,
            group_largest_key,
        )) {
            try read_admission.sort(usize, current.items, {}, std.sort.asc(usize), control);
            try groups.ensureUnusedCapacity(allocator, 1);
            groups.appendAssumeCapacity(.{
                .smallest_namespace_name = group_smallest_namespace_name,
                .smallest_key = group_smallest_key,
                .largest_namespace_name = group_largest_namespace_name,
                .largest_key = group_largest_key,
                .run_indices = try current.toOwnedSlice(allocator),
            });
            current = .empty;
            group_smallest_namespace_name = run.smallest_namespace_name;
            group_smallest_key = run.smallest_key;
            group_largest_namespace_name = run.largest_namespace_name;
            group_largest_key = run.largest_key;
        } else {
            if (compareRunBound(run.smallest_namespace_name, run.smallest_key, group_smallest_namespace_name, group_smallest_key) == .lt) {
                group_smallest_namespace_name = run.smallest_namespace_name;
                group_smallest_key = run.smallest_key;
            }
            if (compareRunBound(run.largest_namespace_name, run.largest_key, group_largest_namespace_name, group_largest_key) == .gt) {
                group_largest_namespace_name = run.largest_namespace_name;
                group_largest_key = run.largest_key;
            }
        }
        try current.append(allocator, run.run_index);
    }

    try read_admission.sort(usize, current.items, {}, std.sort.asc(usize), control);
    try groups.ensureUnusedCapacity(allocator, 1);
    groups.appendAssumeCapacity(.{
        .smallest_namespace_name = group_smallest_namespace_name,
        .smallest_key = group_smallest_key,
        .largest_namespace_name = group_largest_namespace_name,
        .largest_key = group_largest_key,
        .run_indices = try current.toOwnedSlice(allocator),
    });

    if (control) |owner| try owner.checkpoint();
    return try groups.toOwnedSlice(allocator);
}

fn findRunGroupIndex(groups: []const RunGroup, namespace: backend_types.Namespace, key: []const u8) ?usize {
    var lo: usize = 0;
    var hi: usize = groups.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(groups[mid].largest_namespace_name, groups[mid].largest_key, namespace.name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= groups.len) return null;
    if (!groupMayContain(groups[lo], namespace, key)) return null;
    return lo;
}

fn findRunGroup(groups: []const RunGroup, namespace: backend_types.Namespace, key: []const u8) ?RunGroup {
    const idx = findRunGroupIndex(groups, namespace, key) orelse return null;
    return groups[idx];
}

fn groupMayContain(group: RunGroup, namespace: backend_types.Namespace, key: []const u8) bool {
    return compareRunBound(namespace.name, key, group.smallest_namespace_name, group.smallest_key) != .lt and
        compareRunBound(namespace.name, key, group.largest_namespace_name, group.largest_key) != .gt;
}

fn findRunIndexInSortedLevel(runs: []const Run, namespace: backend_types.Namespace, key: []const u8) ?usize {
    var lo: usize = 0;
    var hi: usize = runs.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(runs[mid].largest_namespace_name, runs[mid].largest_key, namespace.name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= runs.len) return null;
    if (!runMayContain(runs[lo], namespace, key)) return null;
    return lo;
}

fn findRunIndexInLevel(runs: []const Run, level: RunLevel, namespace: backend_types.Namespace, key: []const u8) ?usize {
    if (level.len == 0) return null;
    const slice = runs[level.start_index .. level.start_index + level.len];
    var lo: usize = 0;
    var hi: usize = slice.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        if (compareRunBound(slice[mid].largest_namespace_name, slice[mid].largest_key, namespace.name, key) == .lt) {
            lo = mid + 1;
        } else {
            hi = mid;
        }
    }
    if (lo >= slice.len) return null;
    const run_index = level.start_index + lo;
    if (!runMayContain(runs[run_index], namespace, key)) return null;
    return run_index;
}

const PointRunCandidate = struct {
    run_index: usize,
    directory_run: ?*const Run = null,
};

const max_stack_point_run_candidates = 16;
const max_point_async_stack_reads = 16;

const PointAsyncBatchLease = struct {
    limit: usize,
    managed: bool = false,
};

fn acquirePointAsyncBatchLease(backend: anytype) PointAsyncBatchLease {
    if (@hasDecl(@TypeOf(backend.*), "acquirePointAsyncBatchLimit")) {
        const per_batch = @min(backend.options.max_concurrent_point_block_reads, max_point_async_stack_reads);
        return .{
            .limit = backend.acquirePointAsyncBatchLimit(max_point_async_stack_reads),
            .managed = per_batch >= 2,
        };
    }
    return .{
        .limit = @min(backend.options.max_concurrent_point_block_reads, max_point_async_stack_reads),
    };
}

fn releasePointAsyncBatchLease(backend: anytype, lease: PointAsyncBatchLease) void {
    if (!lease.managed) return;
    if (@hasDecl(@TypeOf(backend.*), "releasePointAsyncBatchLimit")) {
        backend.releasePointAsyncBatchLimit();
    }
}

fn runIndicesUsePathBackedPointPrecheck(backend: anytype, runs: []Run, run_indices: []const usize) bool {
    for (run_indices) |run_index| {
        const run = &runs[run_index];
        if (run.state != null or run.path == null) return false;
        if (run.cached_state_index) |index| {
            if (@hasDecl(@TypeOf(backend.*), "cachedRunStateIndexMatches") and
                backend.cachedRunStateIndexMatches(index, run.path.?, run.id)) return false;
        }
    }
    return true;
}

fn pathRunSurvivesPointPrecheck(
    backend: anytype,
    run: *Run,
    run_index: usize,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) !bool {
    const control = if (batch_run_indexes) |indexes| indexes.control else null;
    if (control) |budget| try budget.checkpoint();
    if (!runMayContain(run.*, namespace, key)) return false;

    const filter = try ensureRunBloomFilterForReadMaybeLockedWithAdmission(backend, run, backend_locked, control);
    if (filter) |present_filter| {
        const present = lsm_table_file.maybeContains(present_filter, namespace.name, key);
        if (!present) {
            backend.recordBloomNegative();
            return false;
        }
    }

    const present = if (backend.options.cache != null) blk: {
        if (batch_run_indexes) |indexes| {
            const state = try indexes.state(backend, run, run_index);
            break :blk lsm_table_file.maybeContains(state.handle.runTableIndex().borrowFilter(), namespace.name, key);
        }
        var handle = try loadRunTableIndexHandleWithAdmission(backend, run, control);
        defer handle.release();
        break :blk lsm_table_file.maybeContains(handle.runTableIndex().borrowFilter(), namespace.name, key);
    } else blk: {
        const index = try indexForRunNoCacheMaybeLockedWithAdmission(backend, run, backend_locked, control);
        break :blk lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
    };
    if (!present) backend.recordBloomNegative();
    return present;
}

fn readPointRunCandidate(
    backend: anytype,
    runs: []Run,
    candidate: PointRunCandidate,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) !?[]const u8 {
    const control = if (batch_run_indexes) |indexes| indexes.control else null;
    if (control) |budget| try budget.checkpoint();
    const run = &runs[candidate.run_index];
    if (backend.options.cache != null) {
        const located = if (batch_run_indexes) |indexes|
            try getFromRunWithBlockCacheBatch(backend, run, candidate.run_index, read_hint, held_blocks, held_values, value_allocator, namespace, key, true, indexes) orelse return null
        else
            try getFromRunWithBlockCache(backend, run, candidate.run_index, read_hint, held_blocks, held_values, value_allocator, namespace, key, true) orelse return null;
        if (located.entry.tombstone) return error.NotFound;
        read_hint.* = .{
            .run_index = candidate.run_index,
            .namespace_name = namespace.name,
            .key = located.entry.key,
            .entry_index = located.entry_index,
        };
        return located.entry.value;
    }
    if (try getFromRunWithLocalIndexWithAdmission(backend, run, held_blocks, held_values, value_allocator, namespace, key, backend_locked, control)) |value| {
        read_hint.* = null;
        return value;
    }
    return null;
}

fn getFromPathRunIndicesPrechecked(
    backend: anytype,
    runs: []Run,
    run_indices: []const usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) !?[]const u8 {
    const control = if (batch_run_indexes) |indexes| indexes.control else null;
    if (control) |budget| try budget.checkpoint();
    var stack_candidates: [max_stack_point_run_candidates]PointRunCandidate = undefined;
    var stack_candidate_len: usize = 0;
    var overflow_candidates = std.ArrayListUnmanaged(PointRunCandidate).empty;
    defer overflow_candidates.deinit(value_allocator);

    for (run_indices) |run_index| {
        if (control) |budget| try budget.checkpoint();
        backend.recordRunProbe();
        recordPointRunPrecheck(backend);
        if (!try pathRunSurvivesPointPrecheck(backend, &runs[run_index], run_index, namespace, key, backend_locked, batch_run_indexes)) continue;
        if (stack_candidate_len < stack_candidates.len) {
            stack_candidates[stack_candidate_len] = .{ .run_index = run_index };
            stack_candidate_len += 1;
        } else {
            try overflow_candidates.append(value_allocator, .{ .run_index = run_index });
        }
        recordPointRunPrecheckSurvivor(backend);
    }

    const candidate_count = stack_candidate_len + overflow_candidates.items.len;
    if (candidate_count > 1 and batch_run_indexes == null and backend.options.cache != null and backend.options.max_concurrent_point_block_reads > 1) {
        if (overflow_candidates.items.len == 0) {
            if (try tryReadPointRunCandidatesAsync(
                backend,
                runs,
                stack_candidates[0..stack_candidate_len],
                if (held_blocks != null) read_hint else null,
                held_values,
                value_allocator,
                namespace,
                key,
            )) |result| switch (result) {
                .hit => |value| return value,
                .miss => return null,
                .tombstone => return error.NotFound,
            };
        } else {
            var all_candidates = std.ArrayListUnmanaged(PointRunCandidate).empty;
            defer all_candidates.deinit(value_allocator);
            try all_candidates.appendSlice(value_allocator, stack_candidates[0..stack_candidate_len]);
            try all_candidates.appendSlice(value_allocator, overflow_candidates.items);
            if (try tryReadPointRunCandidatesAsync(
                backend,
                runs,
                all_candidates.items,
                if (held_blocks != null) read_hint else null,
                held_values,
                value_allocator,
                namespace,
                key,
            )) |result| switch (result) {
                .hit => |value| return value,
                .miss => return null,
                .tombstone => return error.NotFound,
            };
        }
    }

    for (stack_candidates[0..stack_candidate_len]) |candidate| {
        if (try readPointRunCandidateWithStats(backend, runs, candidate, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes)) |value| return value;
    }

    for (overflow_candidates.items) |candidate| {
        if (try readPointRunCandidateWithStats(backend, runs, candidate, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes)) |value| return value;
    }
    return null;
}

fn readPointRunCandidateWithStats(
    backend: anytype,
    runs: []Run,
    candidate: PointRunCandidate,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) !?[]const u8 {
    backend.recordPointRunSurvivorRead();
    const maybe_value = readPointRunCandidate(backend, runs, candidate, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes) catch |err| switch (err) {
        error.NotFound => {
            backend.recordPointRunSurvivorTombstone();
            return error.NotFound;
        },
        else => return err,
    };
    if (maybe_value) |value| {
        backend.recordPointRunSurvivorHit();
        return value;
    }
    backend.recordPointRunSurvivorMiss();
    return null;
}

const AsyncPointLookupResult = union(enum) {
    hit: []const u8,
    miss,
    tombstone,
};

// Snapshot results may borrow retained cache storage within an owner-wide cap.
// Metadata is fixed-size; transient handles and exceptional owners use copies.
const AsyncPointResultPins = struct {
    const max_pins = 64;
    const max_bytes = 1024 * 1024;
    held: ?*std.ArrayListUnmanaged(BlockPin),
    identities: [max_pins]usize = @splat(0),
    count: usize = 0,
    bytes: usize = 0,

    fn init(held: ?*std.ArrayListUnmanaged(BlockPin)) @This() {
        var out: @This() = .{ .held = held };
        if (held) |pins| {
            if (pins.items.len > max_pins) {
                out.held = null;
                return out;
            }
            for (pins.items) |pin| {
                switch (pin) {
                    .cached => |handle| {
                        out.identities[out.count] = @intFromPtr(handle.entry);
                        out.bytes +|= handle.entry.byte_cost;
                    },
                    .local => |payload| out.bytes +|= payload.bytes.len,
                }
                out.count += 1;
            }
        }
        return out;
    }

    fn contains(self: *const @This(), handle: *const cache_mod.Handle) bool {
        const identity = @intFromPtr(handle.entry);
        for (self.identities[0..self.count]) |present| if (present == identity) return true;
        return false;
    }

    fn retain(self: *@This(), backend: anytype, handle: *const cache_mod.Handle) bool {
        const held = self.held orelse return false;
        if (!handle.isRetained()) return false;
        const identity = @intFromPtr(handle.entry);
        if (self.contains(handle)) return true;
        if (self.count == max_pins or handle.entry.byte_cost > max_bytes -| self.bytes) return false;
        // Pin metadata is optional: failed growth falls back to an owned value.
        held.ensureUnusedCapacity(backend.allocator, 1) catch return false;
        held.appendAssumeCapacity(.{ .cached = handle.retain() });
        self.identities[self.count] = identity;
        self.count += 1;
        self.bytes += handle.entry.byte_cost;
        return true;
    }
};

// Final result storage, not temporary scratch. Small values share an owned
// buffer; oversized values keep their exact-size allocation. Every buffer is
// registered with the caller before use and survives pool eviction/errors.
fn copyPointValue(backend: anytype, allocator: Allocator, held: *PointResultValues, value: []const u8) ![]const u8 {
    return copyPointValueWithAdmission(backend, allocator, held, value, null);
}

fn copyPointValueWithAdmission(backend: anytype, allocator: Allocator, held: *PointResultValues, value: []const u8, control: ?read_admission.Port) ![]const u8 {
    const copied = try held.copies.copyWithAdmission(allocator, held, value, control);
    recordPointValueCopy(backend);
    return copied;
}

const AsyncPointResultCopies = struct {
    const max_buffer_bytes = 16 * 1024;
    const max_packed_value_bytes = 1024;
    buffer: []u8 = &.{},
    used: usize = 0,
    next_capacity: usize = 256,

    fn copy(self: *@This(), allocator: Allocator, held: *PointResultValues, value: []const u8) ![]const u8 {
        return self.copyWithAdmission(allocator, held, value, null);
    }

    fn copyWithAdmission(self: *@This(), allocator: Allocator, held: *PointResultValues, value: []const u8, control: ?read_admission.Port) ![]const u8 {
        if (control) |budget| try budget.checkpoint();
        if (value.len == 0) return "";
        // Keep the small buffer across larger results. A mixed-size batch
        // cannot strand a nearly empty slab for every large value.
        if (value.len > max_packed_value_bytes) {
            try held.ensureUnusedCapacity(allocator, 1);
            const owned = try copyPointPromotionRows(allocator, value, control);
            held.appendAssumeCapacity(owned);
            return owned;
        }
        if (value.len > self.buffer.len - self.used) {
            try held.ensureUnusedCapacity(allocator, 1);
            const capacity = @max(value.len, self.next_capacity);
            // Spare capacity is optional. A bounded allocator that can fit the
            // actual result must not fail because of speculative slab growth.
            const owned = allocator.alloc(u8, capacity) catch |err| fallback: {
                if (capacity == value.len) return err;
                break :fallback try allocator.alloc(u8, value.len);
            };
            held.appendAssumeCapacity(owned);
            self.buffer = owned;
            self.used = 0;
            self.next_capacity = @min(max_buffer_bytes, owned.len *| 2);
        }
        const result = self.buffer[self.used..][0..value.len];
        try copyPointResultBytes(result, value, control);
        if (control) |budget| try budget.checkpoint();
        self.used += value.len;
        return result;
    }
};

const AsyncPointBlockRead = struct {
    const Status = enum {
        known_miss,
        ready_handle,
        future,
        shared,
    };

    candidate: PointRunCandidate,
    path: []const u8,
    run_id: u64,
    generation: u64,
    index_handle: ?cache_mod.Handle,
    block_index: usize,
    absolute_offset: u64,
    physical_len: u32,
    logical_len: u32,
    compression: lsm_table_file.BlockCompression,
    checksum: u32,
    status: Status,
    retain_block: bool = true,
    physical_handle: ?cache_mod.Handle = null,
    decoded_handle: ?cache_mod.Handle = null,
    future: ?storage_io.RangeReadFuture = null,
    shared_block: ?*BatchAsyncBlock = null,
    // Borrowed only within the synchronous batch; physical futures never retain it.
    control: ?read_admission.Port = null,
    result_pins: ?*AsyncPointResultPins = null,
    result_retention: ResultBlockRetention = .unknown,
    selected_result_bytes: usize = 0,
    result_copies: ?*AsyncPointResultCopies = null,

    fn release(self: *AsyncPointBlockRead) void {
        if (self.shared_block) |block| {
            std.debug.assert(block.users > 0);
            block.users -= 1;
            self.shared_block = null;
        }
        if (self.future) |*future| {
            future.cancel();
            self.future = null;
        }
        if (self.physical_handle) |*handle| {
            handle.release();
            self.physical_handle = null;
        }
        if (self.decoded_handle) |*handle| {
            handle.release();
            self.decoded_handle = null;
        }
        if (self.index_handle) |*handle| {
            handle.release();
            self.index_handle = null;
        }
    }

    fn cancel(self: *AsyncPointBlockRead, backend: anytype) void {
        if (self.future != null) backend.recordPointRunAsyncCancel();
        self.release();
    }

    fn index(self: *const AsyncPointBlockRead) !*const lsm_table_file.TableIndex {
        if (self.index_handle) |*handle| return handle.runTableIndex();
        if (self.shared_block) |owner| return owner.read.index();
        return error.RunStateUnavailable;
    }
};

// A batch owns at most one physical read per resident block. Slots retain
// users, not futures, so a follower cannot cancel another key's read. Idle
// entries are reusable; decoded retention has both per-block and total caps.
const BatchAsyncBlock = struct {
    occupied: bool = false,
    users: usize = 0,
    read: AsyncPointBlockRead = undefined,
    decoded: ?[]u8 = null,
    decode_disabled: bool = false,
    checksum_validated: bool = false,
    raw_reader: lsm_table_file.IndexedPointReader = .{},
    prefix_reader: ?lsm_table_file.PrefixPointReader = null,
    prefix_key_bytes: usize = 0,
    prefix_allocator: ?Allocator = null,
};

const BatchAsyncBlocks = struct {
    const max_decoded_block_bytes = 64 * 1024;
    const max_decoded_bytes = 256 * 1024;
    entries: [max_point_async_stack_reads]BatchAsyncBlock = @splat(.{}),
    allocator: Allocator,
    scratch_allocator: ?Allocator = null,
    workspace: ?LocalReader.Workspace = null,
    workspace_config: ?struct {
        pool: *LocalReader,
        backing: Allocator,
        manager: ?*@import("../resource_manager.zig").ResourceManager,
        io: ?std.Io,
        limit: usize,
    } = null,
    limit: usize = max_point_async_stack_reads,
    control: ?read_admission.Port = null,
    decoded_bytes: usize = 0,
    prefix_key_bytes: usize = 0,

    fn scratchAllocator(self: *@This()) Allocator {
        return self.scratch_allocator orelse self.allocator;
    }

    // Acquire lazily: warm decoded reads do not touch the workspace gate.
    // Physical scratch is bounded and arbitrary-order frees are reusable.
    // Idle storage and spare credit are reclaimed before mandatory fallback.
    fn reusableAllocator(self: *@This()) Allocator {
        return self.reusableAllocatorWithAdmission(null) catch unreachable;
    }

    fn reusableAllocatorWithAdmission(self: *@This(), control: ?read_admission.Port) !Allocator {
        if (self.workspace_config) |config| {
            if (self.workspace == null) self.workspace = try config.pool.acquireRecycledWithAdmission(config.backing, config.manager, config.io, 3 * max_decoded_bytes, config.limit, control);
            return self.workspace.?.allocator();
        }
        return self.allocator;
    }

    fn reclaimIdleScratch(self: *@This()) void {
        // Completed owners retain optional decoded/key state for reuse. Drop
        // that state before mandatory admission, preserving physical/index pins.
        for (self.entries[0..self.limit]) |*entry| if (entry.occupied and entry.users == 0) {
            self.clearPrefixReader(entry);
            if (entry.decoded) |bytes| {
                self.decoded_bytes -= bytes.len;
                self.reusableAllocator().free(bytes);
                entry.decoded = null;
                entry.raw_reader = .{};
            }
        };
        if (self.workspace) |*workspace| workspace.reclaimIdle();
    }

    fn prefixAllocator(self: *@This(), entry: *BatchAsyncBlock) Allocator {
        return entry.prefix_allocator orelse self.scratchAllocator();
    }

    fn clearPrefixReader(self: *@This(), entry: *BatchAsyncBlock) void {
        if (entry.prefix_reader) |*reader| reader.deinit(self.prefixAllocator(entry));
        entry.prefix_reader = null;
        entry.prefix_allocator = null;
        self.prefix_key_bytes -= entry.prefix_key_bytes;
        entry.prefix_key_bytes = 0;
    }

    fn prefixReader(self: *@This(), entry: *BatchAsyncBlock, payload: []const u8, first_entry_index: usize, entry_count: usize) !*lsm_table_file.PrefixPointReader {
        if (entry.prefix_reader == null) {
            entry.prefix_allocator = if (entry.read.logical_len <= max_decoded_block_bytes and self.workspace_config != null) try self.reusableAllocatorWithAdmission(self.control) else self.scratchAllocator();
            const reader = try lsm_table_file.PrefixPointReader.init(payload, first_entry_index, entry.read.logical_len);
            if (reader.entryCount() != entry_count) return error.InvalidTableFile;
            entry.prefix_reader = reader;
        }
        return &entry.prefix_reader.?;
    }

    fn finishPrefixRead(self: *@This(), entry: *BatchAsyncBlock) void {
        const bytes = if (entry.prefix_reader) |*reader| reader.retainedBytes() else 0;
        self.prefix_key_bytes = self.prefix_key_bytes - entry.prefix_key_bytes + bytes;
        entry.prefix_key_bytes = bytes;
        // Required large keys can use temporary admitted scratch. They never
        // raise retained capacity; idle readers are discarded before an active
        // reader is discarded to enforce the aggregate cap.
        if (self.prefix_key_bytes > max_decoded_bytes) {
            for (self.entries[0..self.limit]) |*idle| if (idle != entry and idle.users == 0) self.clearPrefixReader(idle);
        }
        if (bytes > max_decoded_block_bytes or self.prefix_key_bytes > max_decoded_bytes) self.clearPrefixReader(entry);
    }

    fn clear(self: *@This(), entry: *BatchAsyncBlock) void {
        std.debug.assert(entry.users == 0);
        if (!entry.occupied) return;
        self.clearPrefixReader(entry);
        entry.read.release();
        if (entry.decoded) |bytes| {
            self.decoded_bytes -= bytes.len;
            self.reusableAllocator().free(bytes);
        }
        entry.* = .{};
    }

    fn deinit(self: *@This()) void {
        for (self.entries[0..self.limit]) |*entry| self.clear(entry);
        std.debug.assert(self.decoded_bytes == 0);
        std.debug.assert(self.prefix_key_bytes == 0);
        if (self.workspace) |*work| work.release();
        self.workspace = null;
    }

    fn find(self: *@This(), path: []const u8, run_id: u64, generation: u64, offset: u64, len: u32) ?*BatchAsyncBlock {
        for (self.entries[0..self.limit]) |*entry| {
            if (!entry.occupied) continue;
            const read = &entry.read;
            if (read.run_id == run_id and read.generation == generation and read.absolute_offset == offset and read.physical_len == len and std.mem.eql(u8, read.path, path)) return entry;
        }
        return null;
    }

    fn indexHandle(self: *@This(), path: []const u8, run_id: u64, generation: u64) ?*const cache_mod.Handle {
        for (self.entries[0..self.limit]) |*entry| {
            if (!entry.occupied) continue;
            const read = &entry.read;
            if (read.run_id == run_id and read.generation == generation and std.mem.eql(u8, read.path, path))
                if (read.index_handle) |*handle| return handle;
        }
        return null;
    }

    fn prepareInsert(self: *@This()) void {
        for (self.entries[0..self.limit]) |entry| if (!entry.occupied) return;
        for (self.entries[0..self.limit]) |*entry| if (entry.users == 0) {
            // Unpin the outgoing physical block before allocating its
            // replacement, including when resource admission is tight.
            self.clear(entry);
            return;
        };
        unreachable;
    }

    fn insert(self: *@This(), read: AsyncPointBlockRead) *BatchAsyncBlock {
        // Initial fill has fewer than 16 users; replacement happens only
        // after releasing a slot. Thus an idle entry always exists.
        for (self.entries[0..self.limit]) |*entry| if (!entry.occupied) {
            entry.* = .{ .occupied = true, .read = read };
            return entry;
        };
        for (self.entries[0..self.limit]) |*entry| if (entry.users == 0) {
            self.clear(entry);
            entry.* = .{ .occupied = true, .read = read };
            return entry;
        };
        unreachable;
    }

    fn decodedPayload(self: *@This(), entry: *BatchAsyncBlock, payload: []const u8) !?[]const u8 {
        if (entry.decoded) |bytes| return bytes;
        if (!entry.checksum_validated) {
            try lsm_table_file.validateBlockPayloadWithAdmission(payload, entry.read.checksum, self.control);
            entry.checksum_validated = true;
        }
        if (entry.read.compression == .none) {
            if (payload.len != entry.read.logical_len) return error.InvalidTableFile;
            return payload;
        }
        // Prefix records already have a restart index. Never reconstruct every
        // row merely to serve a few points. Share only Snappy decompression.
        if (entry.read.compression == .prefix) return payload;
        if (entry.decode_disabled) return null;
        const snappy = @import("../../encoding/snappy.zig");
        if (entry.read.compression == .prefix_snappy)
            try lsm_table_file.validatePrefixDecodedSize(payload, entry.read.logical_len);
        const decoded_len = try snappy.decodedLen(payload);
        if (entry.read.compression == .snappy and decoded_len != entry.read.logical_len) return error.InvalidTableFile;
        if (decoded_len > max_decoded_block_bytes) return null;
        if (self.decoded_bytes + decoded_len > max_decoded_bytes) {
            for (self.entries[0..self.limit]) |*idle| if (idle != entry and idle.users == 0) self.clear(idle);
        }
        if (self.decoded_bytes + decoded_len > max_decoded_bytes) return null;
        const bytes = snappy.decodeWithAdmission(try self.reusableAllocatorWithAdmission(self.control), payload, self.control) catch |err| switch (err) {
            error.OutOfMemory => {
                // Optional cache admission must not fail an otherwise readable
                // point or repeatedly ask the allocator for the same block.
                entry.decode_disabled = true;
                return null;
            },
            else => return err,
        };
        entry.decoded = bytes;
        self.decoded_bytes += bytes.len;
        return bytes;
    }
};

fn cleanupAsyncPointReads(reads: []AsyncPointBlockRead) void {
    for (reads) |*read| read.release();
}

fn cancelAsyncPointReads(backend: anytype, reads: []AsyncPointBlockRead) void {
    for (reads) |*read| read.cancel(backend);
}

fn prepareAsyncPointBlockRead(
    backend: anytype,
    runs: []Run,
    candidate: PointRunCandidate,
    namespace: backend_types.Namespace,
    key: []const u8,
    shared: ?*BatchAsyncBlocks,
) !?AsyncPointBlockRead {
    return prepareAsyncPointBlockReadWithAdmission(backend, runs, candidate, namespace, key, shared, null);
}

fn prepareAsyncPointBlockReadWithAdmission(
    backend: anytype,
    runs: []Run,
    candidate: PointRunCandidate,
    namespace: backend_types.Namespace,
    key: []const u8,
    shared: ?*BatchAsyncBlocks,
    control: ?read_admission.Port,
) !?AsyncPointBlockRead {
    if (control) |budget| try budget.checkpoint();
    var read = (try prepareAsyncPointBlockReadControlled(backend, runs, candidate, namespace, key, shared, control)) orelse return null;
    errdefer read.release();
    read.control = control;
    if (control) |budget| try budget.checkpoint();
    return read;
}

fn prepareAsyncPointBlockReadControlled(
    backend: anytype,
    runs: []Run,
    candidate: PointRunCandidate,
    namespace: backend_types.Namespace,
    key: []const u8,
    shared: ?*BatchAsyncBlocks,
    control: ?read_admission.Port,
) !?AsyncPointBlockRead {
    const cache = backend.options.cache orelse return null;
    var directory_run: Run = undefined;
    const run = if (candidate.directory_run) |source| blk: {
        directory_run = source.*;
        directory_run.shared_read_version = true;
        break :blk &directory_run;
    } else &runs[candidate.run_index];
    const path = run.path orelse return null;
    const resident = if (shared) |pool| pool.indexHandle(path, run.id, backend.root_generation) else null;
    var index_handle: ?cache_mod.Handle = if (resident == null) try loadRunTableIndexHandleWithAdmission(backend, run, control) else null;
    errdefer if (index_handle) |*handle| handle.release();
    const index = if (resident) |handle| handle.runTableIndex() else index_handle.?.runTableIndex();
    const block_index = index.findBlockIndex(namespace.name, key) orelse {
        if (index_handle) |*handle| handle.release();
        return null;
    };
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key) or !block.maybeContains(namespace.name, key)) {
        return .{
            .candidate = candidate,
            .path = path,
            .run_id = run.id,
            .generation = backend.root_generation,
            .index_handle = index_handle,
            .block_index = block_index,
            .absolute_offset = 0,
            .physical_len = 0,
            .logical_len = 0,
            .compression = .none,
            .checksum = 0,
            .status = .known_miss,
        };
    }

    const window = index.blockWindow(block_index);
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const physical_len = window.physicalLen();
    if (shared) |pool| if (pool.find(path, run.id, backend.root_generation, absolute_offset, physical_len)) |block_owner| {
        block_owner.users += 1;
        if (index_handle) |*handle| handle.release();
        return .{
            .candidate = candidate,
            .path = path,
            .run_id = run.id,
            .generation = backend.root_generation,
            .index_handle = null,
            .block_index = block_index,
            .absolute_offset = absolute_offset,
            .physical_len = physical_len,
            .logical_len = window.len,
            .compression = window.compression,
            .checksum = window.checksum,
            .status = .shared,
            .shared_block = block_owner,
        };
    };
    // Retain before evicting the resident block that lent this index.
    if (index_handle == null) index_handle = resident.?.retain();
    if (shared) |pool| pool.prepareInsert();
    if (try cache.retainRunTableBlockWithAdmission(path, run.id, backend.root_generation, absolute_offset, physical_len, control)) |handle| {
        backend.recordSharedBlockCacheHit();
        return .{
            .candidate = candidate,
            .path = path,
            .run_id = run.id,
            .generation = backend.root_generation,
            .index_handle = index_handle,
            .block_index = block_index,
            .absolute_offset = absolute_offset,
            .physical_len = physical_len,
            .logical_len = window.len,
            .compression = window.compression,
            .checksum = window.checksum,
            .status = .ready_handle,
            .decoded_handle = handle,
        };
    }
    if (try cache.retainRunTablePhysicalBlockWithAdmission(path, run.id, backend.root_generation, absolute_offset, physical_len, control)) |handle| {
        backend.recordSharedBlockCacheHit();
        return .{
            .candidate = candidate,
            .path = path,
            .run_id = run.id,
            .generation = backend.root_generation,
            .index_handle = index_handle,
            .block_index = block_index,
            .absolute_offset = absolute_offset,
            .physical_len = physical_len,
            .logical_len = window.len,
            .compression = window.compression,
            .checksum = window.checksum,
            .status = .ready_handle,
            .physical_handle = handle,
        };
    }

    backend.recordSharedBlockCacheMiss();
    var future = if (control) |budget|
        try backend.storage.?.beginReadFileRangeAllocWithAdmission(backend.options.read_runtime, cache.valueAllocator(), path, absolute_offset, physical_len, budget)
    else
        try backend.storage.?.beginReadFileRangeAllocWithRuntime(backend.options.read_runtime, cache.valueAllocator(), path, absolute_offset, physical_len);
    errdefer future.cancel();
    return .{
        .candidate = candidate,
        .path = path,
        .run_id = run.id,
        .generation = backend.root_generation,
        .index_handle = index_handle,
        .block_index = block_index,
        .absolute_offset = absolute_offset,
        .physical_len = physical_len,
        .logical_len = window.len,
        .compression = window.compression,
        .checksum = window.checksum,
        .status = .future,
        .retain_block = namespace.retainDataBlocks(),
        .future = future,
    };
}

fn payloadForAsyncPointRead(
    backend: anytype,
    read: *AsyncPointBlockRead,
) ![]const u8 {
    if (read.shared_block) |block| return payloadForAsyncPointRead(backend, &block.read);
    if (read.physical_handle) |*handle| return handle.runTablePhysicalBlock();
    const cache = backend.options.cache orelse return error.RunStateUnavailable;
    var future = read.future orelse return error.RunStateUnavailable;
    read.future = null;
    const start_ns = backend.readStatsNowNs();
    const bytes = (if (read.control) |control| future.waitWithAdmission(cache.valueAllocator(), control) else future.wait()) catch |err| {
        backend.recordPointRunAsyncWait(backend.readStatsElapsedNs(start_ns));
        return err;
    };
    const elapsed_ns = backend.readStatsElapsedNs(start_ns);
    backend.recordPointRunAsyncWait(elapsed_ns);
    backend.recordTableBlockLoad(bytes.len, elapsed_ns);
    lsm_table_file.validateBlockPayloadWithAdmission(bytes, read.checksum, read.control) catch |err| {
        cache.valueAllocator().free(bytes);
        return err;
    };
    read.physical_handle = if (read.retain_block)
        try cache.putRunTablePhysicalBlockWithAdmission(read.path, read.run_id, read.generation, read.absolute_offset, read.physical_len, bytes, read.control)
    else
        try cache.putTransientRunTablePhysicalBlockWithAdmission(read.path, read.run_id, read.generation, read.absolute_offset, read.physical_len, bytes, read.control);
    return read.physical_handle.?.runTablePhysicalBlock();
}

// Cache storage is optional. Mandatory decoding and key reconstruction use
// distinct admitted scratch, and allocation-budget errors remain attributable
// to the operation that failed instead of a sticky optional-cache denial.
fn consumeAsyncPointRead(
    backend: anytype,
    read: *AsyncPointBlockRead,
    read_hint: ?*?BorrowedReadHint,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    shared: ?*BatchAsyncBlocks,
) !?AsyncPointLookupResult {
    if (shared) |pool| return consumeAsyncPointReadWithScratch(backend, read, read_hint, held_values, value_allocator, namespace, key, shared, pool.scratchAllocator());
    // Misses and uncompressed pinned data need no temporary allocation.
    if (read.status == .known_miss or read.compression == .none or read.decoded_handle != null) return consumeAsyncPointReadWithScratch(backend, read, read_hint, held_values, value_allocator, namespace, key, null, value_allocator);
    var scratch = PointReadScratch.init(backend);
    defer scratch.deinit();
    return consumeAsyncPointReadWithScratch(backend, read, read_hint, held_values, value_allocator, namespace, key, null, scratch.allocator()) catch |err| return scratch.failure(err);
}

fn retainAsyncPointEntry(
    backend: anytype,
    read: *AsyncPointBlockRead,
    read_hint: ?*?BorrowedReadHint,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    positioned: lsm_table_file.BorrowedDecoded.PositionedEntry,
) !AsyncPointLookupResult {
    if (positioned.entry.tombstone) {
        backend.recordPointRunSurvivorTombstone();
        return .tombstone;
    }
    if (read_hint == null) if (read.result_pins) |pins| {
        const owner = if (read.shared_block) |shared| &shared.read else read;
        if (owner.result_retention == .unknown) {
            const handle: ?cache_mod.Handle = owner.decoded_handle orelse (if (read.compression == .none or read.compression == .prefix) owner.physical_handle else null);
            owner.selected_result_bytes +|= positioned.entry.value.len;
            if (handle) |h| {
                // Under pressure, tiny selections copy instead of prolonging a
                // whole block's lifetime. Reconsider as selection becomes dense.
                if (!h.isRetained()) {
                    owner.result_retention = .copy;
                } else if (pins.contains(&h) or !h.cache.?.resultPinsUnderPressure() or owner.selected_result_bytes >= h.entry.byte_cost / 8)
                    owner.result_retention = if (pins.retain(backend, &h)) .pinned else .copy;
            } else owner.result_retention = .copy;
        }
        if (owner.result_retention == .pinned) {
            backend.recordPointRunSurvivorHit();
            return .{ .hit = positioned.entry.value };
        }
    };
    // A single-point caller may keep a key hint. Batch slots never consume
    // hints and retain only values, independent of cache/decoder lifetimes.
    const value = if (read_hint) |hint| blk: {
        const owned = try copyTableEntryWithAdmission(value_allocator, positioned.entry, read.control);
        errdefer value_allocator.free(owned.bytes);
        try held_values.append(value_allocator, owned.bytes);
        hint.* = .{ .run_index = read.candidate.run_index, .namespace_name = namespace.name, .key = owned.entry.key, .entry_index = positioned.index };
        break :blk owned.entry.value;
    } else if (read.result_copies) |copies| try copies.copyWithAdmission(value_allocator, held_values, positioned.entry.value, read.control) else blk: {
        const owned = try copyPointPromotionRows(value_allocator, positioned.entry.value, read.control);
        errdefer value_allocator.free(owned);
        try held_values.append(value_allocator, owned);
        break :blk owned;
    };
    recordPointValueCopy(backend);
    backend.recordPointRunSurvivorHit();
    return .{ .hit = value };
}

// Share the physical owner's heat across point and batch readers. The result
// is owned before optional promotion; follower slots never acquire a second
// promotion lease or cancel the owner's physical handle.
fn retainAsyncDecodedPointEntry(backend: anytype, read: *AsyncPointBlockRead, read_hint: ?*?BorrowedReadHint, held_values: *PointResultValues, value_allocator: Allocator, namespace: backend_types.Namespace, positioned: lsm_table_file.BorrowedDecoded.PositionedEntry, decoded: DecodedPointBlock) !AsyncPointLookupResult {
    const result = try retainAsyncPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, positioned);
    if (read.control) |budget| try budget.checkpoint();
    const owner = if (read.shared_block) |shared_owner| &shared_owner.read else read;
    if (namespace.retainDataBlocks()) if (owner.physical_handle) |*handle| if (try handle.claimPointBlockPromotionWithAdmission(read.logical_len, read.control)) {
        defer handle.finishPointBlockPromotion();
        const index = try read.index();
        try promoteCachedPointBlockWithAdmission(backend, read.path, read.run_id, read.generation, index.blockWindow(read.block_index), read.absolute_offset, handle.runTablePhysicalBlock(), decoded, read.control);
    };
    return result;
}

// Large values are normally emitted as singleton raw Snappy blocks. Probe a
// bounded header first, then decode into final result storage. The read charge
// covers construction and is released when the buffer becomes an owned result,
// just as for a copied value; no scratch allocation is stranded in its arena.
fn retainLargeSingletonSnappy(
    backend: anytype,
    read: *AsyncPointBlockRead,
    index: *const lsm_table_file.TableIndex,
    payload: []const u8,
    read_hint: ?*?BorrowedReadHint,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?AsyncPointLookupResult {
    if (read.compression != .snappy or read.logical_len <= BatchAsyncBlocks.max_decoded_block_bytes or index.blocks[read.block_index].entry_count != 1) return null;
    const snappy = @import("../../encoding/snappy.zig");
    if (try snappy.decodedLen(payload) != read.logical_len) return error.InvalidTableFile;
    const name = namespace.name orelse "";
    var header: [4096]u8 = undefined;
    if (key.len > header.len - 13 or name.len > header.len - 13 - key.len) return null;
    const prefix_len = 13 + name.len + key.len;
    if (prefix_len > read.logical_len) return null;
    const view: @import("../../segment_source.zig").View = .{ .source = .{ .contiguous = payload }, .offset = 0, .length = payload.len };
    _ = try snappy.decodePrefixFromView(view, header[0..prefix_len]);
    // Other shapes and misses use the ordinary decoder, which validates the
    // complete stream before returning either a value or a miss.
    if (header[0] != 0 or std.mem.readInt(u32, header[1..5], .little) != name.len or std.mem.readInt(u32, header[5..9], .little) != key.len) return null;
    const value_len: usize = std.mem.readInt(u32, header[9..13], .little);
    if (value_len != read.logical_len - prefix_len or value_len < read.logical_len / 2) return null;
    if (!std.mem.eql(u8, header[13..][0..name.len], name) or !std.mem.eql(u8, header[13 + name.len ..][0..key.len], key)) return null;
    const resources = @import("../resource_manager.zig");
    var reservation: ?resources.Reservation = if (backend.options.resource_manager) |manager| try manager.reserveWithoutReclaim(.lsm_read_working_set, read.logical_len) else null;
    defer if (reservation) |*charge| charge.release();
    // Grow metadata before allocating arena-owned output so error cleanup of
    // the large buffer remains LIFO even for an arena result allocator.
    try held_values.ensureUnusedCapacity(value_allocator, 1);
    const owned = try value_allocator.alloc(u8, read.logical_len);
    errdefer value_allocator.free(owned);
    try snappy.decodeIntoWithAdmission(payload, owned, read.control);
    const found = (try lsm_table_file.findExactEntryInBlock(index, owned, read.block_index, namespace.name, key)) orelse return error.InvalidTableFile;
    held_values.appendAssumeCapacity(owned);
    if (read_hint) |hint| hint.* = .{ .run_index = read.candidate.run_index, .namespace_name = namespace.name, .key = found.entry.key, .entry_index = found.index };
    backend.recordPointRunSurvivorHit();
    return .{ .hit = found.entry.value };
}

fn consumeAsyncPointReadWithScratch(
    backend: anytype,
    read: *AsyncPointBlockRead,
    read_hint: ?*?BorrowedReadHint,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    shared: ?*BatchAsyncBlocks,
    scratch: Allocator,
) !?AsyncPointLookupResult {
    if (read.control) |budget| try budget.checkpoint();
    backend.recordPointRunSurvivorRead();
    if (read.status == .known_miss) {
        backend.recordPointRunSurvivorMiss();
        return null;
    }
    const index = try read.index();
    const block = index.blocks[read.block_index];
    const backing_read = if (read.shared_block) |shared_owner| &shared_owner.read else read;
    if (backing_read.decoded_handle) |handle| {
        const found = (if (read.shared_block) |owner| try owner.raw_reader.find(index, handle.runTableBlock(), read.block_index, namespace.name, key) else try lsm_table_file.findExactEntryInBlock(index, handle.runTableBlock(), read.block_index, namespace.name, key)) orelse {
            backend.recordPointRunSurvivorMiss();
            return null;
        };
        return try retainAsyncPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, found);
    }
    const payload = try payloadForAsyncPointRead(backend, read);
    if (shared) |pool| if (read.shared_block) |owner| if (try pool.decodedPayload(owner, payload)) |decoded| {
        const positioned = if (read.compression == .prefix or read.compression == .prefix_snappy) {
            var reader = try pool.prefixReader(owner, decoded, block.first_entry_index, block.entry_count);
            // The result must be copied before a large key's scratch is freed.
            defer pool.finishPrefixRead(owner);
            if (pool.workspace) |workspace| {
                const prefix_allocator = pool.prefixAllocator(owner);
                const recycled = workspace.allocator();
                if (prefix_allocator.ptr != recycled.ptr or prefix_allocator.vtable != recycled.vtable) pool.reclaimIdleScratch();
            }
            const found = (reader.findWithAdmission(pool.prefixAllocator(owner), namespace.name, key, read.control) catch |err| fallback: {
                if (err != error.OutOfMemory or pool.workspace == null or owner.prefix_allocator.?.ptr != pool.workspace.?.allocator().ptr) return err;
                pool.clearPrefixReader(owner);
                reader = try pool.prefixReader(owner, decoded, block.first_entry_index, block.entry_count);
                owner.prefix_allocator = scratch;
                pool.reclaimIdleScratch();
                break :fallback try reader.findWithAdmission(scratch, namespace.name, key, read.control);
            }) orelse {
                backend.recordPointRunSurvivorMiss();
                return null;
            };
            return try retainAsyncDecodedPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, found, .{ .prefix_records = decoded });
        } else try owner.raw_reader.find(index, decoded, read.block_index, namespace.name, key);
        const found = positioned orelse {
            backend.recordPointRunSurvivorMiss();
            return null;
        };
        if (read.compression == .snappy) return try retainAsyncDecodedPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, found, .{ .raw_rows = decoded });
        return try retainAsyncPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, found);
    };

    if (shared) |pool| pool.reclaimIdleScratch();
    // The fallback decoder is mandatory scratch, never optional cache memory.
    // None borrows its pinned payload. Snappy scratch is freed before returning.
    const validated = if (read.shared_block) |owner| owner.checksum_validated else false;
    if (!validated) try lsm_table_file.validateBlockPayloadWithAdmission(payload, read.checksum, read.control);
    if (try retainLargeSingletonSnappy(backend, read, index, payload, read_hint, held_values, value_allocator, namespace, key)) |result| return result;
    const snappy = @import("../../encoding/snappy.zig");
    const decoded = switch (read.compression) {
        .none, .prefix => payload,
        .snappy => blk: {
            if (try snappy.decodedLen(payload) != read.logical_len) return error.InvalidTableFile;
            break :blk try snappy.decodeWithAdmission(scratch, payload, read.control);
        },
        .prefix_snappy => blk: {
            try lsm_table_file.validatePrefixDecodedSize(payload, read.logical_len);
            break :blk try snappy.decodeWithAdmission(scratch, payload, read.control);
        },
    };
    defer if (read.compression == .snappy or read.compression == .prefix_snappy) scratch.free(decoded);
    const positioned = switch (read.compression) {
        .prefix, .prefix_snappy => {
            var reader = try lsm_table_file.PrefixPointReader.init(decoded, block.first_entry_index, read.logical_len);
            defer reader.deinit(scratch);
            if (reader.entryCount() != block.entry_count) return error.InvalidTableFile;
            const found = try reader.findWithAdmission(scratch, namespace.name, key, read.control) orelse {
                backend.recordPointRunSurvivorMiss();
                return null;
            };
            return try retainAsyncDecodedPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, found, .{ .prefix_records = decoded });
        },
        .none, .snappy => blk: {
            if (decoded.len != read.logical_len) return error.InvalidTableFile;
            break :blk try lsm_table_file.findExactEntryInBlock(index, decoded, read.block_index, namespace.name, key);
        },
    };
    const found = positioned orelse {
        backend.recordPointRunSurvivorMiss();
        return null;
    };
    if (read.compression == .snappy) return try retainAsyncDecodedPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, found, .{ .raw_rows = decoded });
    return try retainAsyncPointEntry(backend, read, read_hint, held_values, value_allocator, namespace, found);
}

// Warm deciding candidates stop traversal immediately. A cold candidate starts
// a bounded read-order window so overlapping disk reads retain concurrency.
// Directory metadata remains borrowed from the caller's immutable read view.
fn tryReadDirectoryPointAsync(
    backend: anytype,
    directory: *const @import("run_directory.zig").Directory,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    lifetime: PointResultLifetime,
) !?AsyncPointLookupResult {
    if (backend.storage == null or backend.options.cache == null or backend.options.max_concurrent_point_block_reads < 2) return null;
    // One-run reads use the established block loader, which can adopt large
    // decoded buffers directly into short point leases. Async windows are
    // useful only when another overlapping run exists.
    var cursor = directory.readPoint(namespace.name, key);
    const first = cursor.next() orelse return null;
    const second = cursor.next() orelse return null;
    const leading = [_]@import("run_directory.zig").Directory.Handle{ first, second };
    var leading_index: usize = 0;
    const lease = acquirePointAsyncBatchLease(backend);
    defer releasePointAsyncBatchLease(backend, lease);
    if (lease.limit < 2) return null;
    var reads: [max_point_async_stack_reads]AsyncPointBlockRead = undefined;
    var pins = AsyncPointResultPins.init(if (lifetime == .snapshot_pinned) held_blocks else null);
    while (true) {
        var count: usize = 0;
        var consumed: usize = 0;
        var issued: usize = 0;
        errdefer cleanupAsyncPointReads(reads[consumed..count]);
        while (count < lease.limit) {
            const handle = if (leading_index < leading.len) blk: {
                const handle = leading[leading_index];
                leading_index += 1;
                break :blk handle;
            } else cursor.next() orelse break;
            backend.recordRunProbe();
            recordPointRunPrecheck(backend);
            var read = try prepareAsyncPointBlockRead(backend, &.{}, .{ .run_index = 0, .directory_run = handle.run }, namespace, key, null) orelse {
                cleanupAsyncPointReads(reads[0..count]);
                return null;
            };
            if (read.status == .known_miss) {
                read.release();
                continue;
            }
            recordPointRunPrecheckSurvivor(backend);
            read.result_pins = &pins;
            read.result_copies = &held_values.copies;
            reads[count] = read;
            count += 1;
            if (read.status == .future) issued += 1;
            // Avoid inspecting older runs for a warm first candidate. Once a
            // cold read is issued, prepare its followers before waiting.
            if (count == 1 and read.status != .future) break;
        }
        if (count == 0) return .miss;
        backend.recordPointRunAsyncBatch(issued);
        while (consumed < count) : (consumed += 1) {
            const result = try consumeAsyncPointRead(backend, &reads[consumed], null, held_values, allocator, namespace, key, null);
            if (result) |decision| switch (decision) {
                .miss => {},
                .hit, .tombstone => {
                    cancelAsyncPointReads(backend, reads[consumed + 1 .. count]);
                    switch (decision) {
                        .hit => if (reads[consumed].candidate.directory_run.?.level == 0) backend.recordL0Hit() else backend.recordLevelHit(),
                        else => {},
                    }
                    reads[consumed].release();
                    return decision;
                },
            };
            reads[consumed].release();
        }
    }
}

fn tryReadPointRunCandidatesAsync(
    backend: anytype,
    runs: []Run,
    candidates: []const PointRunCandidate,
    read_hint: ?*?BorrowedReadHint,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?AsyncPointLookupResult {
    if (backend.storage == null) return null;
    var stack_reads: [max_point_async_stack_reads]AsyncPointBlockRead = undefined;
    const async_lease = acquirePointAsyncBatchLease(backend);
    defer releasePointAsyncBatchLease(backend, async_lease);
    const configured_limit = async_lease.limit;
    if (configured_limit < 2) return null;
    const batch_limit = @max(@as(usize, 1), configured_limit);
    var offset: usize = 0;
    while (offset < candidates.len) {
        const end = @min(candidates.len, offset + batch_limit);
        var read_count: usize = 0;
        var issued_count: usize = 0;
        var consumed: usize = 0;
        errdefer cleanupAsyncPointReads(stack_reads[consumed..read_count]);
        for (candidates[offset..end]) |candidate| {
            const prepared = try prepareAsyncPointBlockRead(backend, runs, candidate, namespace, key, null) orelse {
                cleanupAsyncPointReads(stack_reads[0..read_count]);
                return null;
            };
            if (prepared.status == .future) issued_count += 1;
            stack_reads[read_count] = prepared;
            stack_reads[read_count].result_copies = &held_values.copies;
            read_count += 1;
        }
        backend.recordPointRunAsyncBatch(issued_count);
        while (consumed < read_count) : (consumed += 1) {
            if (try consumeAsyncPointRead(backend, &stack_reads[consumed], read_hint, held_values, value_allocator, namespace, key, null)) |result| {
                cancelAsyncPointReads(backend, stack_reads[consumed + 1 .. read_count]);
                stack_reads[consumed].release();
                return result;
            }
            stack_reads[consumed].release();
        }
        offset = end;
    }
    return .miss;
}

// Candidate traversal is local to an in-flight slot, not materialized for
// every key/run pair. Memory is bounded by max_point_async_stack_reads.
const BatchPointCandidates = struct {
    directory_cursor: ?@import("run_directory.zig").Directory.ReadPointCursor = null,
    l0_indices: []const usize = &.{},
    next_l0: usize = 0,
    next_level: usize = 0,

    fn init(groups: []const RunGroup, namespace: backend_types.Namespace, key: []const u8) @This() {
        return .{ .l0_indices = if (findRunGroupIndex(groups, namespace, key)) |i| groups[i].run_indices else &.{} };
    }

    fn next(self: *@This(), runs: []const Run, levels: []const RunLevel, namespace: backend_types.Namespace, key: []const u8) ?PointRunCandidate {
        if (self.directory_cursor) |*cursor| {
            const handle = cursor.next() orelse return null;
            return .{ .run_index = 0, .directory_run = handle.run };
        }
        while (self.next_l0 < self.l0_indices.len) {
            const i = self.l0_indices[self.next_l0];
            self.next_l0 += 1;
            if (runMayContain(runs[i], namespace, key)) return .{ .run_index = i };
        }
        while (self.next_level < levels.len) {
            const level = levels[self.next_level];
            self.next_level += 1;
            if (findRunIndexInLevel(runs, level, namespace, key)) |i| return .{ .run_index = i };
        }
        return null;
    }
};

const BatchAsyncPointSlot = struct {
    active: bool = false,
    key_index: usize = 0,
    candidates: BatchPointCandidates = .{},
    read: AsyncPointBlockRead = undefined,
};

fn startBatchAsyncPointSlot(
    backend: anytype,
    runs: []Run,
    levels: []const RunLevel,
    keys: []const []const u8,
    namespace: backend_types.Namespace,
    slot: *BatchAsyncPointSlot,
    issued_count: *usize,
    shared: *BatchAsyncBlocks,
) !bool {
    const candidate = slot.candidates.next(runs, levels, namespace, keys[slot.key_index]) orelse return false;
    backend.recordRunProbe();
    recordPointRunPrecheck(backend);
    var prepared = (try prepareAsyncPointBlockReadWithAdmission(backend, runs, candidate, namespace, keys[slot.key_index], shared, shared.control)) orelse return error.RunStateUnavailable;
    if (prepared.status != .known_miss) recordPointRunPrecheckSurvivor(backend);
    if (prepared.status == .future) issued_count.* += 1;
    if (prepared.status != .known_miss and prepared.shared_block == null) {
        const owner = shared.insert(prepared);
        owner.users = 1;
        prepared.index_handle = null;
        prepared.physical_handle = null;
        prepared.decoded_handle = null;
        prepared.future = null;
        prepared.shared_block = owner;
        prepared.status = .shared;
    }
    slot.read = prepared;
    slot.active = true;
    return true;
}

fn fillBatchAsyncPointSlot(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    lifetime: PointResultLifetime,
    next_key: *usize,
    result: *BatchCursorReadResult,
    slot: *BatchAsyncPointSlot,
    issued_count: *usize,
    shared: *BatchAsyncBlocks,
    directory: ?*const @import("run_directory.zig").Directory,
    copies: *AsyncPointResultCopies,
) !bool {
    while (next_key.* < keys.len) {
        if (shared.control) |budget| try budget.checkpoint();
        const i = next_key.*;
        next_key.* += 1;
        const key = keys[i];
        const entry = found: {
            if (mutable.findIndex(namespace, key)) |index| break :found mutable.entryAt(index);
            for (immutable_memtables) |state| if (state.findIndex(namespace, key)) |index| break :found state.entryAt(index);
            break :found null;
        };
        if (entry) |present| {
            if (present.tombstone) {
                result.misses += 1;
            } else {
                values[i] = if (namespace.own_source_point_results or lifetime == .transaction_owned) owned: {
                    const value = try copies.copyWithAdmission(allocator, held_values, present.value, shared.control);
                    recordPointValueCopy(backend);
                    break :owned value;
                } else present.value;
                backend.recordMutableHit();
                result.hits += 1;
            }
            continue;
        }
        slot.key_index = i;
        slot.candidates = if (directory) |source| .{ .directory_cursor = source.readPoint(namespace.name, key) } else BatchPointCandidates.init(groups, namespace, key);
        if (try startBatchAsyncPointSlot(backend, runs, levels, keys, namespace, slot, issued_count, shared)) return true;
        result.misses += 1;
    }
    return false;
}

/// Overlap independent sparse point reads in a fixed-size pipeline. One
/// candidate per key is in flight, preserving tombstone/value precedence.
fn readManySortedPointFromSnapshotAsync(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
    result_lifetime: PointResultLifetime,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
) !?BatchCursorReadResult {
    return readManySortedPointFromSourceAsync(backend, mutable, immutable_memtables, runs, l0_groups, levels, allocator, held_values, namespace, keys, values, backend_locked, result_lifetime, held_blocks, null);
}

fn readManySortedPointFromSourceAsync(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
    result_lifetime: PointResultLifetime,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    directory: ?*const @import("run_directory.zig").Directory,
) !?BatchCursorReadResult {
    return readManySortedPointFromSnapshotAsyncWithAdmission(backend, mutable, immutable_memtables, runs, l0_groups, levels, allocator, held_values, namespace, keys, values, backend_locked, result_lifetime, held_blocks, directory, null);
}

fn readManySortedPointFromSnapshotAsyncWithAdmission(
    backend: anytype,
    mutable: anytype,
    immutable_memtables: []const *const State,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    allocator: Allocator,
    held_values: *PointResultValues,
    namespace: backend_types.Namespace,
    keys: []const []const u8,
    values: []?[]const u8,
    backend_locked: bool,
    result_lifetime: PointResultLifetime,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    directory: ?*const @import("run_directory.zig").Directory,
    control: ?read_admission.Port,
) !?BatchCursorReadResult {
    if (control) |budget| try budget.checkpoint();
    if (backend_locked or keys.len < 2 or backend.storage == null or backend.options.cache == null) return null;
    const async_lease = acquirePointAsyncBatchLease(backend);
    defer releasePointAsyncBatchLease(backend, async_lease);
    const configured_limit = async_lease.limit;
    if (configured_limit < 2) return null;
    if (directory) |source| {
        if (!source.supportsAsyncPoints()) return null;
    } else for (runs) |run| if (run.path == null or run.state != null) return null;
    backend.recordPointGets(keys.len);

    const resources = @import("../resource_manager.zig");
    var decode_budget: ?resources.BudgetedAllocator = if (backend.options.resource_manager) |manager| resources.BudgetedAllocator.init(manager, .lsm_in_memory_state, runtimeScratchAllocator(allocator), 1) else null;
    defer if (decode_budget) |*budget| budget.deinit();
    var scratch = PointReadScratch.init(backend);
    defer scratch.deinit();
    var shared: BatchAsyncBlocks = .{ .allocator = if (decode_budget) |*budget| budget.allocator() else runtimeScratchAllocator(allocator), .scratch_allocator = scratch.allocator(), .limit = configured_limit, .control = control };
    if (comptime @hasField(@TypeOf(backend.*), "point_reader")) shared.workspace_config = .{
        .pool = &backend.point_reader,
        .backing = backend.allocator,
        .manager = backend.options.resource_manager,
        .io = backend.manifestCoordinationIo(),
        .limit = backend.options.local_decode_working_bytes,
    };
    defer shared.deinit();
    var slots: [max_point_async_stack_reads]BatchAsyncPointSlot = undefined;
    for (slots[0..configured_limit]) |*slot| slot.* = .{};
    defer for (slots[0..configured_limit]) |*slot| if (slot.active) slot.read.release();
    var result_pins = AsyncPointResultPins.init(if (result_lifetime == .snapshot_pinned) held_blocks else null);
    const result_copies = &held_values.copies;
    var result: BatchCursorReadResult = .{};
    var issued_count: usize = 0;
    var next_key: usize = 0;
    var active_slots: usize = 0;
    for (slots[0..configured_limit]) |*slot| {
        if (!try fillBatchAsyncPointSlot(backend, mutable, immutable_memtables, runs, l0_groups, levels, allocator, held_values, namespace, keys, values, result_lifetime, &next_key, &result, slot, &issued_count, &shared, directory, result_copies)) break;
        active_slots += 1;
    }
    var cursor: usize = 0;
    while (active_slots > 0) {
        const slot = &slots[cursor];
        cursor = (cursor + 1) % configured_limit;
        if (!slot.active) continue;
        if (control) |budget| try budget.checkpoint();
        const key_index = slot.key_index;
        const deciding_level = if (slot.read.candidate.directory_run) |run| run.level else runs[slot.read.candidate.run_index].level;
        scratch.resetDenial();
        slot.read.result_copies = result_copies;
        slot.read.result_pins = if (result_pins.held != null) &result_pins else null;
        const lookup = consumeAsyncPointRead(backend, &slot.read, null, held_values, allocator, namespace, keys[key_index], &shared) catch |err| return scratch.failure(err);
        slot.read.release();
        slot.active = false;
        var key_decided = false;
        if (lookup) |decision| switch (decision) {
            .hit => |value| {
                values[key_index] = value;
                result.hits += 1;
                if (deciding_level == 0) backend.recordL0Hit() else backend.recordLevelHit();
                key_decided = true;
            },
            .tombstone => {
                result.misses += 1;
                key_decided = true;
            },
            .miss => {},
        };
        if (!key_decided and try startBatchAsyncPointSlot(backend, runs, levels, keys, namespace, slot, &issued_count, &shared)) continue;
        if (!key_decided) result.misses += 1;
        if (!try fillBatchAsyncPointSlot(backend, mutable, immutable_memtables, runs, l0_groups, levels, allocator, held_values, namespace, keys, values, result_lifetime, &next_key, &result, slot, &issued_count, &shared, directory, result_copies)) active_slots -= 1;
    }
    backend.recordPointRunAsyncBatch(issued_count);
    if (control) |budget| try budget.checkpoint();
    return result;
}

fn getFromRunIndices(
    backend: anytype,
    runs: []Run,
    run_indices: []const usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    batch_run_indexes: ?*RunBatchIndexHandles,
) !?[]const u8 {
    const control = if (batch_run_indexes) |indexes| indexes.control else null;
    if (control) |budget| try budget.checkpoint();
    if (runIndicesUsePathBackedPointPrecheck(backend, runs, run_indices)) {
        return try getFromPathRunIndicesPrechecked(backend, runs, run_indices, read_hint, held_blocks, held_values, value_allocator, namespace, key, backend_locked, batch_run_indexes);
    }

    for (run_indices) |run_index| {
        if (control) |budget| try budget.checkpoint();
        backend.recordRunProbe();
        const run = &runs[run_index];
        if (run.state) |*state| {
            if (state.findIndex(namespace, key)) |idx| {
                const entry = state.entryAt(idx);
                if (entry.tombstone) return error.NotFound;
                read_hint.* = null;
                return try retainSourcePointValueWithAdmission(backend, value_allocator, held_values, namespace, entry.value, control);
            }
            continue;
        }

        if (run.path != null) {
            if (run.cached_state_index) |index| {
                if (backend.cachedRunStateIndexMatches(index, run.path.?, run.id)) {
                    const state = backend.getCachedRunStateByIndex(index);
                    if (state.findIndex(namespace, key)) |idx| {
                        const entry = state.entryAt(idx);
                        if (entry.tombstone) return error.NotFound;
                        read_hint.* = null;
                        return try retainSourcePointValueWithAdmission(backend, value_allocator, held_values, namespace, entry.value, control);
                    }
                    continue;
                }
            }
            const run_filter_checked = run.bloom_filter != null or run.path != null;
            if (run_filter_checked and !try runMayContainWithFilterMaybeLockedWithAdmission(backend, run, namespace, key, backend_locked, control)) continue;
            if (backend.options.cache != null) {
                const located = if (batch_run_indexes) |indexes|
                    try getFromRunWithBlockCacheBatch(backend, run, run_index, read_hint, held_blocks, held_values, value_allocator, namespace, key, run_filter_checked, indexes) orelse continue
                else
                    try getFromRunWithBlockCache(backend, run, run_index, read_hint, held_blocks, held_values, value_allocator, namespace, key, run_filter_checked) orelse continue;
                if (located.entry.tombstone) return error.NotFound;
                read_hint.* = .{
                    .run_index = run_index,
                    .namespace_name = namespace.name,
                    .key = located.entry.key,
                    .entry_index = located.entry_index,
                };
                return located.entry.value;
            }
            if (try getFromRunWithLocalIndexWithAdmission(backend, run, held_blocks, held_values, value_allocator, namespace, key, backend_locked, control)) |value| {
                read_hint.* = null;
                return value;
            }
            continue;
        }

        if (!try runMayContainWithFilterMaybeLockedWithAdmission(backend, run, namespace, key, backend_locked, control)) continue;

        const table = try tableForRunMaybeLockedWithAdmission(backend, run, backend_locked, control);
        var read_hint_attempted = false;
        const positioned = if (read_hint.*) |hint|
            if (hint.run_index == run_index and
                compareNamespace(namespace, .{ .name = hint.namespace_name }) == .eq and
                std.mem.order(u8, key, hint.key) != .lt)
            blk: {
                read_hint_attempted = true;
                backend.recordReadHintAttempt();
                break :blk try table.seekAtOrAfterFromIndex(namespace.name, key, hint.entry_index);
            } else null
        else
            null;
        const located = if (positioned) |cached|
            if (compareNamespace(.{ .name = cached.entry.namespace_name }, namespace) == .eq and std.mem.eql(u8, cached.entry.key, key)) blk2: {
                backend.recordReadHintHit();
                break :blk2 .{ cached.index, cached.entry };
            } else blk2: {
                if (read_hint_attempted) backend.recordReadHintMiss();
                const idx = try table.findIndex(namespace.name, key) orelse break :blk2 null;
                break :blk2 .{ idx, try table.entryAt(idx) };
            }
        else blk2: {
            if (read_hint_attempted) backend.recordReadHintMiss();
            const idx = try table.findIndex(namespace.name, key) orelse break :blk2 null;
            break :blk2 .{ idx, try table.entryAt(idx) };
        };
        const entry_index, const entry = located orelse continue;
        if (entry.tombstone) return error.NotFound;
        read_hint.* = .{
            .run_index = run_index,
            .namespace_name = namespace.name,
            .key = entry.key,
            .entry_index = entry_index,
        };
        return try retainSourcePointValueWithAdmission(backend, value_allocator, held_values, namespace, entry.value, control);
    }
    return null;
}

const BlockLocatedEntry = struct {
    entry_index: usize,
    entry: lsm_table_file.Entry,
    handle: ?cache_mod.Handle,
};

const LocatedTableEntry = struct {
    entry_index: usize,
    entry: lsm_table_file.Entry,
};

const CachedPointLookupResult = union(enum) {
    hit: []const u8,
    miss,
    unavailable,
};

fn getFromStableCachedPointView(
    backend: anytype,
    allocator: Allocator,
    runs: []Run,
    l0_groups: []const RunGroup,
    levels: []const RunLevel,
    last_l0_group_index: *?usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) !CachedPointLookupResult {
    var candidate_group_index = if (last_l0_group_index.*) |hinted_index|
        if (hinted_index < l0_groups.len and groupMayContain(l0_groups[hinted_index], namespace, key)) hinted_index else null
    else
        null;
    if (candidate_group_index == null) {
        candidate_group_index = findRunGroupIndex(l0_groups, namespace, key);
    }
    last_l0_group_index.* = candidate_group_index;
    if (candidate_group_index) |group_index| {
        switch (try getFromCachedRunStates(backend, allocator, runs, l0_groups[group_index].run_indices, namespace, key)) {
            .hit => |value| return .{ .hit = value },
            .unavailable => return .unavailable,
            .miss => {},
        }
    }

    for (levels) |level| {
        const run_index = findRunIndexInLevel(runs, level, namespace, key) orelse continue;
        const one = [_]usize{run_index};
        switch (try getFromCachedRunStates(backend, allocator, runs, &one, namespace, key)) {
            .hit => |value| return .{ .hit = value },
            .unavailable => return .unavailable,
            .miss => {},
        }
    }
    return .miss;
}

fn getFromCachedRunStates(
    backend: anytype,
    allocator: Allocator,
    runs: []Run,
    run_indices: []const usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) !CachedPointLookupResult {
    _ = allocator;
    for (run_indices) |run_index| {
        const run = &runs[run_index];
        if (!runMayContain(run.*, namespace, key)) continue;
        if (try ensureRunBloomFilterForRead(backend, run)) |filter| {
            if (!lsm_table_file.maybeContains(filter, namespace.name, key)) continue;
        }
        const state = if (run.state) |*present|
            present
        else blk: {
            const path = run.path orelse return .unavailable;
            const index = run.cached_state_index orelse return .unavailable;
            if (!backend.cachedRunStateIndexMatches(index, path, run.id)) return .unavailable;
            break :blk backend.getCachedRunStateByIndex(index);
        };
        if (state.findIndex(namespace, key)) |idx| {
            const entry = state.entryAt(idx);
            if (entry.tombstone) return error.NotFound;
            return .{ .hit = entry.value };
        }
    }
    return .miss;
}

fn parseEntryAtWithStats(backend: anytype, bytes: []const u8, relative_offset: usize) !lsm_table_file.Entry {
    const start_ns = backend.readStatsNowNs();
    const parsed = lsm_table_file.parseEntryAt(bytes, relative_offset);
    backend.recordTableEntryParse(backend.readStatsElapsedNs(start_ns));
    return try parsed;
}

fn requireTableBlocks(index: *const lsm_table_file.TableIndex) !void {
    if (index.entryCount() > 0 and index.blockCount() == 0) return error.InvalidTableFile;
}

fn decodeRunTableIndexWithStats(backend: anytype, allocator: Allocator, bytes: []const u8, control: ?read_admission.Port) !lsm_table_file.TableIndex {
    const start_ns = backend.readStatsNowNs();
    const decoded = lsm_table_file.decodeIndexAllocWithAdmission(allocator, bytes, control);
    backend.recordTableIndexDecode(backend.readStatsElapsedNs(start_ns));
    return try decoded;
}

fn loadRunTableIndexWithStats(backend: anytype, allocator: Allocator, path: []const u8, control: ?read_admission.Port) !lsm_table_file.TableIndex {
    const start_ns = backend.readStatsNowNs();
    const loaded = repository_mod.loadRunTableIndexAllocWithStorageAndAdmission(backend.storage.?, allocator, path, control);
    backend.recordTableIndexLoad(backend.readStatsElapsedNs(start_ns));
    return try loaded;
}

fn loadRunTableBlockWithStats(backend: anytype, allocator: Allocator, path: []const u8, absolute_offset: u64, len: usize) ![]u8 {
    return loadRunTableBlockWithStatsAndAdmission(backend, allocator, path, absolute_offset, len, null);
}

fn loadRunTableBlockWithStatsAndAdmission(backend: anytype, allocator: Allocator, path: []const u8, absolute_offset: u64, len: usize, control: ?read_admission.Port) ![]u8 {
    const start_ns = backend.readStatsNowNs();
    const loaded = if (@hasDecl(@TypeOf(backend.*), "readRunRangeAllocWithAdmission")) backend.readRunRangeAllocWithAdmission(allocator, path, absolute_offset, len, control) else blk: {
        if (control) |budget| try budget.checkpoint();
        const bytes = try (if (@hasDecl(@TypeOf(backend.*), "readRunRangeAlloc")) backend.readRunRangeAlloc(allocator, path, absolute_offset, len) else backend.storage.?.readFileRangeAlloc(allocator, path, absolute_offset, len));
        errdefer allocator.free(bytes);
        if (control) |budget| try budget.checkpoint();
        break :blk bytes;
    };
    const elapsed_ns = backend.readStatsElapsedNs(start_ns);
    if (loaded) |bytes| backend.recordTableBlockLoad(bytes.len, elapsed_ns) else |_| backend.recordTableBlockLoad(len, elapsed_ns);
    return try loaded;
}

fn loadRunTableDecodedBlockWithStats(
    backend: anytype,
    allocator: Allocator,
    path: []const u8,
    absolute_offset: u64,
    physical_len: usize,
    compression: lsm_table_file.BlockCompression,
    logical_len: usize,
    checksum: u32,
) ![]u8 {
    const payload = try loadRunTableBlockWithStats(backend, allocator, path, absolute_offset, physical_len);
    defer allocator.free(payload);
    return try lsm_table_file.decodeBlockPayloadAlloc(allocator, compression, payload, logical_len, checksum);
}

fn getFromRunWithBlockCache(
    backend: anytype,
    run: *Run,
    run_index: usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
) !?LocatedTableEntry {
    if (!runMayContain(run.*, namespace, key)) return null;

    var index_handle = try loadRunTableIndexHandle(backend, run);
    defer index_handle.release();
    return try getFromRunWithBlockCacheIndex(backend, run, run_index, index_handle.runTableIndex(), read_hint, held_blocks, held_values, value_allocator, namespace, key, run_filter_checked);
}

fn getFromRunWithBlockCacheBatch(
    backend: anytype,
    run: *Run,
    run_index: usize,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
    batch_run_indexes: *RunBatchIndexHandles,
) !?LocatedTableEntry {
    if (!runMayContain(run.*, namespace, key)) return null;

    const state = try batch_run_indexes.state(backend, run, run_index);
    return try getFromRunWithBlockCacheBatchState(backend, run, run_index, state, read_hint, held_blocks, held_values, value_allocator, namespace, key, run_filter_checked);
}

fn getFromRunWithBlockCacheIndex(
    backend: anytype,
    run: *Run,
    run_index: usize,
    index: *const lsm_table_file.TableIndex,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
) !?LocatedTableEntry {
    if (!run_filter_checked) {
        const present = lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        if (!present) {
            backend.recordBloomNegative();
            return null;
        }
    }

    _ = run_index;
    _ = read_hint;
    const located = try findExactEntryInCachedBlocksWithLifetime(backend, run, index, held_values, value_allocator, namespace, key, PointResultLifetime.forBlockPins(held_blocks));
    var pinned = located orelse return null;
    errdefer if (pinned.handle) |*handle| handle.release();
    if (pinned.handle) |handle| {
        if (held_blocks) |pins| {
            try pins.append(backend.allocator, .{ .cached = handle });
        } else {
            pinned.entry.value = if (pinned.entry.tombstone) "" else try copyPointValue(backend, value_allocator, held_values, pinned.entry.value);
            pinned.entry.key = key;
            pinned.entry.namespace_name = namespace.name;
            var owned_handle = handle;
            owned_handle.release();
            pinned.handle = null;
        }
    }
    return .{
        .entry_index = pinned.entry_index,
        .entry = pinned.entry,
    };
}

fn getFromRunWithBlockCacheBatchState(
    backend: anytype,
    run: *Run,
    run_index: usize,
    state: *RunBatchIndexState,
    read_hint: *?BorrowedReadHint,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    run_filter_checked: bool,
) !?LocatedTableEntry {
    if (held_blocks == null) return getFromRunWithBlockCacheIndex(backend, run, run_index, state.handle.runTableIndex(), read_hint, null, held_values, value_allocator, namespace, key, run_filter_checked);
    const index = state.handle.runTableIndex();
    if (!run_filter_checked) {
        const present = lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        if (!present) {
            backend.recordBloomNegative();
            return null;
        }
    }

    const located = try findExactEntryInBatchBlocks(backend, run, index, state, held_blocks.?, held_values, value_allocator, namespace, key);
    if (located) |entry| {
        if (!entry.entry.tombstone) state.block_has_values = true;
    }
    return located;
}

fn checkedCacheHandle(handle: cache_mod.Handle, control: ?read_admission.Port) !cache_mod.Handle {
    var owned = handle;
    errdefer owned.releaseForAdmissionFailure();
    if (control) |budget| try budget.checkpoint();
    return owned;
}

fn loadRunTableIndexHandle(backend: anytype, run: *Run) !cache_mod.Handle {
    return loadRunTableIndexHandleWithAdmission(backend, run, null);
}

fn loadRunTableIndexHandleWithAdmission(backend: anytype, run: *Run, control: ?read_admission.Port) !cache_mod.Handle {
    const cache = backend.options.cache orelse return error.RunStateUnavailable;
    const path = run.path orelse return error.RunStateUnavailable;
    const generation = backend.root_generation;
    while (true) {
        if (try cache.retainRunTableIndexWithAdmission(path, run.id, generation, control)) |retained| return checkedCacheHandle(retained, control);
        var load_claim = try cache.beginLoadClaim(path, run.id, generation, .run_table_index, 0, 0, control);
        defer load_claim.transferToCache();
        if (control) |budget| try budget.checkpoint();
        if (try cache.retainRunTableIndexWithAdmission(path, run.id, generation, control)) |retained| return checkedCacheHandle(retained, control);
        if (try cache.retainRunTableRawWithAdmission(path, run.id, generation, control)) |retained_raw| {
            var raw_handle = retained_raw;
            defer raw_handle.releaseWithAdmission(control);
            const start_ns = backend.readStatsNowNs();
            const result = cache.decodeRunTableIndexWithAdmission(path, raw_handle.runTableRaw(), control);
            backend.recordTableIndexDecode(backend.readStatsElapsedNs(start_ns));
            var decoded = try result;
            defer decoded.deinit();
            return checkedCacheHandle(try decoded.publish(run.id, generation, control), control);
        }
        const start_ns = backend.readStatsNowNs();
        const result = cache.loadRunTableIndexWithAdmission(backend.storage.?, path, control);
        backend.recordTableIndexLoad(backend.readStatsElapsedNs(start_ns));
        var loaded = try result;
        defer loaded.deinit();
        return checkedCacheHandle(try loaded.publish(run.id, generation, control), control);
    }
}

fn loadRunTableBlockHandle(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    retain_block: bool,
) !cache_mod.Handle {
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    return try loadRunTableBlockHandleAtOffset(
        backend,
        run,
        absolute_offset,
        window.physicalLen(),
        window.compression,
        window.len,
        window.checksum,
        retain_block,
    );
}

fn loadRunTableBlockHandleWithAdmission(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, retain_block: bool, control: ?read_admission.Port) !cache_mod.Handle {
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    return loadRunTableBlockHandleAtOffsetControlled(backend, run, absolute_offset, window.physicalLen(), window.compression, window.len, window.checksum, retain_block, control);
}

fn loadRunTableBlockHandleAtOffset(
    backend: anytype,
    run: *Run,
    absolute_offset: u64,
    physical_len: u32,
    compression: lsm_table_file.BlockCompression,
    logical_len: u32,
    checksum: u32,
    retain_block: bool,
) !cache_mod.Handle {
    return loadRunTableBlockHandleAtOffsetControlled(backend, run, absolute_offset, physical_len, compression, logical_len, checksum, retain_block, null);
}

fn loadRunTableBlockHandleAtOffsetControlled(
    backend: anytype,
    run: *Run,
    absolute_offset: u64,
    physical_len: u32,
    compression: lsm_table_file.BlockCompression,
    logical_len: u32,
    checksum: u32,
    retain_block: bool,
    control: ?read_admission.Port,
) !cache_mod.Handle {
    const cache = backend.options.cache orelse return error.RunStateUnavailable;
    const path = run.path orelse return error.RunStateUnavailable;
    const generation = backend.root_generation;
    while (true) {
        if (control) |budget| try budget.checkpoint();
        if (try cache.retainRunTableBlockWithAdmission(path, run.id, generation, absolute_offset, physical_len, control)) |retained| {
            backend.recordSharedBlockCacheHit();
            return checkedCacheHandle(retained, control);
        }
        var load_claim = try cache.beginLoadClaim(path, run.id, generation, .run_table_block, absolute_offset, physical_len, control);
        defer load_claim.transferToCache();
        if (control) |budget| try budget.checkpoint();
        if (try cache.retainRunTableBlockWithAdmission(path, run.id, generation, absolute_offset, physical_len, control)) |retained| {
            backend.recordSharedBlockCacheHit();
            return checkedCacheHandle(retained, control);
        }
        backend.recordSharedBlockCacheMiss();
        const Reader = struct {
            backend: @TypeOf(backend),
            control: ?read_admission.Port,
            pub fn readFileRangeAlloc(self: @This(), allocator: Allocator, block_path: []const u8, offset: u64, len: usize) ![]u8 {
                return loadRunTableBlockWithStatsAndAdmission(self.backend, allocator, block_path, offset, len, self.control);
            }
        };
        var decoded = try cache.loadRunTableBlockWithAdmission(Reader{ .backend = backend, .control = control }, path, absolute_offset, physical_len, compression, logical_len, checksum, control);
        defer decoded.deinit();
        return checkedCacheHandle(try decoded.publish(run.id, generation, retain_block, control), control);
    }
}

fn loadRunTablePhysicalBlockHandleAtOffset(
    backend: anytype,
    run: *Run,
    absolute_offset: u64,
    physical_len: u32,
    checksum: u32,
    retain_block: bool,
) !cache_mod.Handle {
    const cache = backend.options.cache orelse return error.RunStateUnavailable;
    const path = run.path orelse return error.RunStateUnavailable;
    const generation = backend.root_generation;
    while (true) {
        if (cache.retainRunTablePhysicalBlock(path, run.id, generation, absolute_offset, physical_len)) |retained| {
            backend.recordSharedBlockCacheHit();
            return retained;
        }
        var load_claim = try cache.beginLoadClaim(path, run.id, generation, .run_table_physical_block, absolute_offset, physical_len, null);
        defer load_claim.transferToCache();
        if (cache.retainRunTablePhysicalBlock(path, run.id, generation, absolute_offset, physical_len)) |retained| {
            backend.recordSharedBlockCacheHit();
            return retained;
        }
        backend.recordSharedBlockCacheMiss();
        const block = try loadRunTableBlockWithStats(backend, cache.valueAllocator(), path, absolute_offset, physical_len);
        lsm_table_file.validateBlockPayload(block, checksum) catch |err| {
            cache.valueAllocator().free(block);
            return err;
        };
        return if (retain_block)
            try cache.putRunTablePhysicalBlock(path, run.id, generation, absolute_offset, physical_len, block)
        else
            try cache.putTransientRunTablePhysicalBlock(path, run.id, generation, absolute_offset, physical_len, block);
    }
}

fn findExactEntryInCachedCompressedPrefixBlock(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    block_index: usize,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    lifetime: PointResultLifetime,
) !?LocatedTableEntry {
    const block = index.blocks[block_index];
    const window = index.blockWindow(block_index);
    switch (window.compression) {
        .prefix, .prefix_snappy => {},
        .none, .snappy => return null,
    }
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    var handle = try loadRunTablePhysicalBlockHandleAtOffset(
        backend,
        run,
        absolute_offset,
        window.physicalLen(),
        window.checksum,
        namespace.retainDataBlocks(),
    );
    defer handle.release();
    var scratch = PointReadScratch.init(backend);
    defer scratch.deinit();
    const found = (findExactEntryInCachedPrefixWithScratch(backend, handle.runTablePhysicalBlock(), block, window, held_values, value_allocator, scratch.allocator(), namespace, key, lifetime) catch |err| return scratch.failure(err)) orelse return null;
    if (namespace.retainDataBlocks() and handle.claimPointBlockPromotion(window.len)) {
        defer handle.finishPointBlockPromotion();
        try promoteCachedPointBlock(backend, run.path.?, run.id, backend.root_generation, window, absolute_offset, handle.runTablePhysicalBlock(), null);
    }
    return found;
}

/// Promotion is optional: admit construction independently of caller results,
/// then hand its credit to shared cache retention without a second host charge. The physical owner elects one
/// attempt; the normal decoded-load gate also coalesces with scan/batch loads.
const DecodedPointBlock = union(enum) { prefix_records: []const u8, raw_rows: []const u8 };

fn promoteCachedPointBlock(backend: anytype, path: []const u8, run_id: u64, generation: u64, window: lsm_table_file.EntryDataWindow, offset: u64, payload: []const u8, prepared: ?DecodedPointBlock) !void {
    return promoteCachedPointBlockWithAdmission(backend, path, run_id, generation, window, offset, payload, prepared, null);
}

fn promoteCachedPointBlockWithAdmission(backend: anytype, path: []const u8, run_id: u64, generation: u64, window: lsm_table_file.EntryDataWindow, offset: u64, payload: []const u8, prepared: ?DecodedPointBlock, control: ?read_admission.Port) !void {
    const cache = backend.options.cache orelse return;
    if (control) |budget| {
        try budget.checkpoint();
        // A managed read cannot publish promotion into an uncharged cache.
        // This optional optimization leaves its already-owned result intact.
        if (backend.options.resource_manager != null and cache.resource_manager == null) return;
    }
    var claim = (cache.tryBeginLoadClaim(path, run_id, generation, .run_table_block, offset, window.physicalLen(), control) catch |err| {
        if (control == null) return;
        switch (err) {
            error.OutOfMemory, error.ResourceBudgetExceeded, error.ResourceTemporarilyUnavailable => return,
            else => return err,
        }
    }) orelse return;
    defer claim.transferToCache();
    const winner_handle = if (control) |budget| try cache.retainRunTableBlockWithAdmission(path, run_id, generation, offset, window.physicalLen(), budget) else cache.retainRunTableBlock(path, run_id, generation, offset, window.physicalLen());
    if (winner_handle) |retained| {
        var winner = retained;
        winner.releaseWithAdmission(control);
        return;
    }
    const resources = @import("../resource_manager.zig");
    // Unprepared prefix-Snappy decoding owns a temporary prefix buffer as well
    // as the final rows. Admit both before allocation and keep their credit in
    // the claim until buffers retire or cache retention adopts it.
    const scratch_bytes = if (prepared == null and window.compression == .prefix_snappy) try @import("../../encoding/snappy.zig").decodedLen(payload) else 0;
    const construction_bytes = std.math.add(u64, window.len, scratch_bytes) catch return error.ResourceBudgetExceeded;
    // Decoded rows belong to the shared cache. Charge its manager even when
    // the requesting backend has a different working-set accounting owner.
    const promotion_manager = cache.resource_manager orelse backend.options.resource_manager;
    const output_credit: ?*resources.Reservation = if (promotion_manager) |manager|
        claim.reserveConstructionCredit(manager, construction_bytes, control) catch |err| switch (err) {
            error.ResourceBudgetExceeded => return,
            else => return err,
        }
    else
        null;
    const scratch = runtimeScratchAllocator(backend.allocator);
    const decoded = (if (prepared) |ready| blk: {
        try lsm_table_file.validateBlockPayloadWithAdmission(payload, window.checksum, control);
        break :blk switch (ready) {
            .prefix_records => |records| lsm_table_file.expandValidatedPrefixRecordsAllocWithAdmission(cache.valueAllocator(), records, window.len, control),
            .raw_rows => |rows| copy: {
                if (rows.len != window.len) return error.InvalidTableFile;
                break :copy copyPointPromotionRows(cache.valueAllocator(), rows, control);
            },
        };
    } else lsm_table_file.decodeBlockPayloadWithScratchAllocWithAdmission(cache.valueAllocator(), scratch, window.compression, payload, window.len, window.checksum, control)) catch |err| switch (err) {
        error.OutOfMemory => return,
        else => return err,
    };
    // put consumes decoded on every exit, including allocation failure.
    var promoted = (if (control) |budget| cache.putRunTableBlockWithCreditAdmission(path, run_id, generation, offset, window.physicalLen(), decoded, output_credit, budget) else cache.putRunTableBlockWithCredit(path, run_id, generation, offset, window.physicalLen(), decoded, output_credit)) catch |err| {
        if (control == null) return;
        switch (err) {
            error.OutOfMemory, error.ResourceBudgetExceeded => return,
            else => return err,
        }
    };
    promoted.releaseWithAdmission(control);
}

fn copyPointPromotionRows(allocator: Allocator, rows: []const u8, control: ?read_admission.Port) ![]u8 {
    if (control) |budget| try budget.checkpoint();
    const owned = try allocator.alloc(u8, rows.len);
    errdefer allocator.free(owned);
    try copyPointResultBytes(owned, rows, control);
    if (control) |budget| try budget.checkpoint();
    return owned;
}

fn copyPointResultBytes(dst: []u8, src: []const u8, control: ?read_admission.Port) !void {
    std.debug.assert(dst.len == src.len);
    if (control == null) {
        @memcpy(dst, src);
        return;
    }
    var cursor: usize = 0;
    while (cursor < src.len) {
        try control.?.checkpoint();
        const end = cursor + @min(src.len - cursor, 64 * 1024);
        @memcpy(dst[cursor..end], src[cursor..end]);
        cursor = end;
    }
}

fn findExactEntryInCachedPrefixWithScratch(
    backend: anytype,
    payload: []const u8,
    block: lsm_table_file.TableIndex.BlockMeta,
    window: lsm_table_file.EntryDataWindow,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    scratch: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    lifetime: PointResultLifetime,
) !?LocatedTableEntry {
    return findExactEntryInCachedPrefixWithScratchWithAdmission(backend, payload, block, window, held_values, value_allocator, scratch, namespace, key, lifetime, null);
}

fn findExactEntryInCachedPrefixWithScratchWithAdmission(
    backend: anytype,
    payload: []const u8,
    block: lsm_table_file.TableIndex.BlockMeta,
    window: lsm_table_file.EntryDataWindow,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    scratch: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    lifetime: PointResultLifetime,
    control: ?read_admission.Port,
) !?LocatedTableEntry {
    try lsm_table_file.validateBlockPayloadWithAdmission(payload, window.checksum, control);
    const snappy = @import("../../encoding/snappy.zig");
    const decoded = if (window.compression == .prefix_snappy) blk: {
        try lsm_table_file.validatePrefixDecodedSize(payload, window.len);
        break :blk try snappy.decodeWithAdmission(scratch, payload, control);
    } else payload;
    defer if (window.compression == .prefix_snappy) scratch.free(decoded);
    var reader = try lsm_table_file.PrefixPointReader.init(decoded, block.first_entry_index, window.len);
    defer reader.deinit(scratch);
    if (reader.entryCount() != block.entry_count) return error.InvalidTableFile;
    const found = try reader.findWithAdmission(scratch, namespace.name, key, control) orelse return null;
    if (control) |budget| try budget.checkpoint();
    if (lifetime == .transaction_owned) {
        var entry = found.entry;
        entry.value = if (entry.tombstone) "" else try copyPointValueWithAdmission(backend, value_allocator, held_values, entry.value, control);
        entry.key = key;
        entry.namespace_name = namespace.name;
        return .{ .entry_index = found.index, .entry = entry };
    }
    const owned = try copyTableEntryWithAdmission(value_allocator, found.entry, control);
    errdefer value_allocator.free(owned.bytes);
    try held_values.append(value_allocator, owned.bytes);
    recordPointValueCopy(backend);
    return .{ .entry_index = found.index, .entry = owned.entry };
}

// A retained handle is consumed on every exit. Only a successful lookup
// transfers ownership to the caller; normal misses release it like errors.
fn findExactEntryInCachedBlockHandle(
    retained: cache_mod.Handle,
    index: *const lsm_table_file.TableIndex,
    block_index: usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?BlockLocatedEntry {
    var handle = retained;
    var transferred = false;
    defer if (!transferred) handle.release();
    const positioned = try lsm_table_file.findExactEntryInBlock(index, handle.runTableBlock(), block_index, namespace.name, key) orelse return null;
    transferred = true;
    return .{ .entry_index = positioned.index, .entry = positioned.entry, .handle = handle };
}

fn findExactEntryInCachedBlocks(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?BlockLocatedEntry {
    return findExactEntryInCachedBlocksWithLifetime(backend, run, index, held_values, value_allocator, namespace, key, .snapshot_pinned);
}

fn findExactEntryInCachedBlocksWithLifetime(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    lifetime: PointResultLifetime,
) !?BlockLocatedEntry {
    try requireTableBlocks(index);
    const block_index = index.findBlockIndex(namespace.name, key) orelse return null;
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
    if (!block.maybeContains(namespace.name, key)) {
        backend.recordBloomNegative();
        return null;
    }
    const window = index.blockWindow(block_index);
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const cache = backend.options.cache orelse return error.RunStateUnavailable;
    if (cache.retainRunTableBlock(run.path orelse return error.RunStateUnavailable, run.id, backend.root_generation, absolute_offset, window.physicalLen())) |handle| {
        backend.recordSharedBlockCacheHit();
        return findExactEntryInCachedBlockHandle(handle, index, block_index, namespace, key);
    }
    // A checked prefix lookup distinguishes a hit from a definitive miss.
    // Never decode the entire block a second time just to confirm that miss.
    if (window.compression == .prefix or window.compression == .prefix_snappy) {
        const entry = try findExactEntryInCachedCompressedPrefixBlock(backend, run, index, block_index, held_values, value_allocator, namespace, key, lifetime) orelse return null;
        return .{ .entry_index = entry.entry_index, .entry = entry.entry, .handle = null };
    }
    const handle = try loadRunTableBlockHandle(backend, run, index, window, namespace.retainDataBlocks());
    return findExactEntryInCachedBlockHandle(handle, index, block_index, namespace, key);
}

fn loadBatchBlock(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    state: *RunBatchIndexState,
    held_blocks: *std.ArrayListUnmanaged(BlockPin),
    block_index: usize,
    retain_block: bool,
) ![]const u8 {
    if (state.block_index == null or state.block_index.? != block_index) {
        try state.transferBlock(backend.allocator, held_blocks);
        const window = index.blockWindow(block_index);
        state.block_handle = try loadRunTableBlockHandleWithAdmission(backend, run, index, window, retain_block, state.control);
        state.block_index = block_index;
        state.block_has_values = false;
    }
    return state.block_handle.?.runTableBlock();
}

fn findExactEntryInBatchBlocks(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    state: *RunBatchIndexState,
    held_blocks: *std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?LocatedTableEntry {
    if (state.control) |budget| try budget.checkpoint();
    try requireTableBlocks(index);
    const block_index = index.findBlockIndex(namespace.name, key) orelse return null;
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
    if (!block.maybeContains(namespace.name, key)) {
        backend.recordBloomNegative();
        return null;
    }
    // A single point lookup can search a prefix-compressed physical block
    // without retaining its decoded form. Repeating that work for every key
    // in a batch is much more expensive than decoding the block once. Keep
    // the decoded block in this run's batch state and reuse it until sorted
    // iteration advances to another block.
    _ = held_values;
    _ = value_allocator;
    const block_bytes = try loadBatchBlock(backend, run, index, state, held_blocks, block_index, namespace.retainDataBlocks());
    const positioned = try lsm_table_file.findExactEntryInBlock(
        index,
        block_bytes,
        block_index,
        namespace.name,
        key,
    ) orelse return null;
    return .{
        .entry_index = positioned.index,
        .entry = positioned.entry,
    };
}

const VisibleLookup = union(enum) { absent, tombstone, value: backend_adapter.Entry };

fn visibleEntryFromRunIndices(
    backend: anytype,
    runs: []Run,
    run_indices: []const usize,
    namespace: backend_types.Namespace,
    key: []const u8,
    visible_entry_bytes: *VisibleBytes,
    backend_locked: bool,
) !VisibleLookup {
    for (run_indices) |run_index| {
        const run = &runs[run_index];
        if (!try runMayContainWithFilterMaybeLocked(backend, run, namespace, key, backend_locked)) continue;
        if (run.state) |*state| {
            if (state.findIndex(namespace, key)) |idx| {
                const entry = state.entryAt(idx);
                if (entry.tombstone) return .tombstone;
                return .{ .value = entry.entry() };
            }
            continue;
        }

        if (run.path != null) {
            const loaded = try loadVisibleEntryFromPathRunMaybeLocked(backend, run, namespace, key, backend_locked) orelse continue;
            if (loaded.entry.tombstone) {
                loaded.deinit(backend.allocator);
                return .tombstone;
            }
            if (loaded.local) |lease| {
                visible_entry_bytes.release();
                visible_entry_bytes.* = .{ .local = lease };
            } else visible_entry_bytes.setOwned(backend.allocator, loaded.bytes);
            return .{ .value = .{
                .key = loaded.entry.key,
                .value = loaded.entry.value,
            } };
        }
    }
    return .absent;
}

fn rangesOverlap(
    lhs_smallest_namespace_name: ?[]const u8,
    lhs_smallest_key: []const u8,
    lhs_largest_namespace_name: ?[]const u8,
    lhs_largest_key: []const u8,
    rhs_smallest_namespace_name: ?[]const u8,
    rhs_smallest_key: []const u8,
    rhs_largest_namespace_name: ?[]const u8,
    rhs_largest_key: []const u8,
) bool {
    return compareRunBound(lhs_smallest_namespace_name, lhs_smallest_key, rhs_largest_namespace_name, rhs_largest_key) != .gt and
        compareRunBound(lhs_largest_namespace_name, lhs_largest_key, rhs_smallest_namespace_name, rhs_smallest_key) != .lt;
}

fn stateForRun(backend: anytype, run: *Run) !*const State {
    if (run.state) |*state| return state;
    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    const path = run.path orelse return error.RunStateUnavailable;
    if (run.cached_state_index) |index| {
        if (!(index < backend.run_state_cache.items.len and
            backend.run_state_cache.items[index].run_id == run.id and
            std.mem.eql(u8, backend.run_state_cache.items[index].path, path)))
        {
            run.cached_state_index = null;
        }
    }
    if (run.cached_state_index == null) {
        run.cached_state_index = try backend.getCachedRunStateIndex(path, run.id);
    }
    return backend.getCachedRunStateByIndex(run.cached_state_index.?);
}

fn tableForRunLocked(backend: anytype, run: *Run) !*const lsm_table_file.BorrowedDecoded {
    return tableForRunLockedWithAdmission(backend, run, null);
}

fn tableForRunLockedWithAdmission(backend: anytype, run: *Run, control: ?read_admission.Port) !*const lsm_table_file.BorrowedDecoded {
    const path = run.path orelse return error.RunStateUnavailable;
    if (run.cached_table_index) |index| {
        if (!(index < backend.run_table_cache.items.len and
            backend.run_table_cache.items[index].run_id == run.id and
            std.mem.eql(u8, backend.run_table_cache.items[index].path, path)))
        {
            run.cached_table_index = null;
        }
    }
    if (run.cached_table_index == null) {
        run.cached_table_index = try backend.getCachedRunTableIndexWithAdmission(path, run.id, control);
    }
    return backend.getCachedRunTableByIndex(run.cached_table_index.?);
}

fn tableForRun(backend: anytype, run: *Run) !*const lsm_table_file.BorrowedDecoded {
    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    return try tableForRunLocked(backend, run);
}

fn tableForRunMaybeLocked(backend: anytype, run: *Run, backend_locked: bool) !*const lsm_table_file.BorrowedDecoded {
    if (backend_locked) return try tableForRunLocked(backend, run);
    return try tableForRun(backend, run);
}

fn tableForRunMaybeLockedWithAdmission(backend: anytype, run: *Run, backend_locked: bool, control: ?read_admission.Port) !*const lsm_table_file.BorrowedDecoded {
    const budget = control orelse return tableForRunMaybeLocked(backend, run, backend_locked);
    try budget.checkpoint();
    const locked = if (!backend_locked) try lockBackendWithAdmission(@TypeOf(backend.*), backend, budget) else false;
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    const result = try tableForRunLockedWithAdmission(backend, run, budget);
    try budget.checkpoint();
    return result;
}

fn indexForRunNoCacheLocked(backend: anytype, run: *Run) !*const lsm_table_file.TableIndex {
    return indexForRunNoCacheLockedWithAdmission(backend, run, null);
}

fn indexForRunNoCacheLockedWithAdmission(backend: anytype, run: *Run, control: ?read_admission.Port) !*const lsm_table_file.TableIndex {
    const path = run.path orelse return error.RunStateUnavailable;
    if (run.cached_index_index) |index| {
        if (!(index < backend.run_index_cache.items.len and
            backend.run_index_cache.items[index].run_id == run.id and
            std.mem.eql(u8, backend.run_index_cache.items[index].path, path)))
        {
            run.cached_index_index = null;
        }
    }
    if (run.cached_index_index == null) {
        run.cached_index_index = try backend.getCachedRunIndexIndexWithAdmission(path, run.id, control);
    }
    return backend.getCachedRunIndexByIndex(run.cached_index_index.?);
}

fn indexForRunNoCache(backend: anytype, run: *Run) !*const lsm_table_file.TableIndex {
    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    return try indexForRunNoCacheLocked(backend, run);
}

fn indexForRunNoCacheMaybeLocked(backend: anytype, run: *Run, backend_locked: bool) !*const lsm_table_file.TableIndex {
    if (backend_locked) return try indexForRunNoCacheLocked(backend, run);
    return try indexForRunNoCache(backend, run);
}

fn indexForRunNoCacheMaybeLockedWithAdmission(backend: anytype, run: *Run, backend_locked: bool, control: ?read_admission.Port) !*const lsm_table_file.TableIndex {
    const budget = control orelse return indexForRunNoCacheMaybeLocked(backend, run, backend_locked);
    try budget.checkpoint();
    const locked = if (!backend_locked) try lockBackendWithAdmission(@TypeOf(backend.*), backend, budget) else false;
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    const result = try indexForRunNoCacheLockedWithAdmission(backend, run, budget);
    try budget.checkpoint();
    return result;
}

const OwnedTableEntry = struct {
    entry: lsm_table_file.Entry,
    bytes: []u8 = &.{},
    local: ?*SharedBytes = null,
    // The point caller already owns this value; bytes/local carry no owner.
    final_point_value: bool = false,
    fn deinit(self: @This(), allocator: Allocator) void {
        if (self.local) |lease| lease.release() else allocator.free(self.bytes);
    }
};

// Point-result ownership needs the selected row, never a duplicate of the
// entire decoded block. Large rows can still transfer this compact buffer.
fn copyTableEntry(allocator: Allocator, entry: lsm_table_file.Entry) !OwnedTableEntry {
    return copyTableEntryWithAdmission(allocator, entry, null);
}

fn copyTableEntryWithAdmission(allocator: Allocator, entry: lsm_table_file.Entry, control: ?read_admission.Port) !OwnedTableEntry {
    if (control) |budget| try budget.checkpoint();
    const namespace_len = if (entry.namespace_name) |name| name.len else 0;
    const prefix_len = std.math.add(usize, namespace_len, entry.key.len) catch return error.OutOfMemory;
    const total_len = std.math.add(usize, prefix_len, entry.value.len) catch return error.OutOfMemory;
    const bytes = try allocator.alloc(u8, total_len);
    errdefer allocator.free(bytes);
    var copied = entry;
    if (entry.namespace_name) |name| {
        try copyPointResultBytes(bytes[0..namespace_len], name, control);
        copied.namespace_name = bytes[0..namespace_len];
    }
    copied.key = bytes[namespace_len..][0..entry.key.len];
    try copyPointResultBytes(@constCast(copied.key), entry.key, control);
    copied.value = bytes[prefix_len..];
    try copyPointResultBytes(@constCast(copied.value), entry.value, control);
    if (control) |budget| try budget.checkpoint();
    return .{ .entry = copied, .bytes = bytes };
}

fn findExactEntryInLocalLease(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, block_index: usize, namespace: backend_types.Namespace, key: []const u8, locked: bool) !?OwnedTableEntry {
    return findExactEntryInLocalLeaseWithAdmission(backend, run, index, window, block_index, namespace, key, locked, null);
}

fn findExactEntryInLocalLeaseWithAdmission(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    block_index: usize,
    namespace: backend_types.Namespace,
    key: []const u8,
    locked: bool,
    control: ?read_admission.Port,
) !?OwnedTableEntry {
    const lease = try loadLocalBlockLeaseWithAdmission(backend, run, index, window, locked, namespace.retainDataBlocks(), control);
    return findExactEntryInBlockLease(lease, index, block_index, namespace, key);
}

/// Consumes the retained block reference, including on a miss or error.
fn findExactEntryInBlockLease(lease: *SharedBytes, index: *const lsm_table_file.TableIndex, block_index: usize, namespace: backend_types.Namespace, key: []const u8) !?OwnedTableEntry {
    errdefer lease.release();
    const positioned = try lsm_table_file.findExactEntryInBlock(index, lease.bytes, block_index, namespace.name, key) orelse {
        lease.release();
        return null;
    };
    return .{ .entry = positioned.entry, .local = lease };
}

fn findExactEntryWithLocalIndex(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?OwnedTableEntry {
    const index = try indexForRunNoCache(backend, run);
    try requireTableBlocks(index);
    return try findExactEntryWithLocalIndexBlockMeta(backend, run, index, namespace, key);
}

fn localBlockCacheEligible(backend: anytype, bytes: usize) bool {
    if (comptime @hasDecl(@TypeOf(backend.*), "localBlockCacheEligible")) return backend.localBlockCacheEligible(bytes);
    return localBlockCacheEnabled(backend);
}

/// One elected attempt in backend-owned fixed metadata. The backend and its
/// read pin outlive this token. Finish only after physical decoder work joins.
/// Completion stores no borrowed path, scope, allocator or callback.
pub const LocalBlockPromotion = struct {
    pub const State = enum(u8) { idle, active, admitted, denied };
    completion: ?*std.atomic.Value(@This().State),

    pub fn finish(self: *@This(), admitted: bool) void {
        const completion = self.completion orelse return;
        self.completion = null;
        // Final access: a cache pass can immediately reconcile/reuse the slot.
        completion.store(if (admitted) .admitted else .denied, .release);
    }
};

fn beginLocalBlockPromotionWithAdmission(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    locked: bool,
    control: ?read_admission.Port,
) !?LocalBlockPromotion {
    if (comptime !@hasDecl(@TypeOf(backend.*), "beginLocalBlockPromotionClaim")) return null;
    if (window.compression != .prefix and window.compression != .prefix_snappy) return null;
    const path = run.path orelse return null;
    const held = if (locked) false else if (control) |budget| try lockBackendWithAdmission(@TypeOf(backend.*), backend, budget) else lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, held);
    return backend.beginLocalBlockPromotionClaim(path, run.id, @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(), window.physicalLen(), window.len);
}

fn retainLocalCachedBlock(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, backend_locked: bool) ?*SharedBytes {
    return retainLocalCachedBlockWithAdmission(backend, run, index, window, backend_locked, null) catch unreachable;
}

fn retainLocalCachedBlockWithAdmission(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, backend_locked: bool, control: ?read_admission.Port) !?*SharedBytes {
    if (control) |budget| try budget.checkpoint();
    const path = run.path orelse return null;
    const offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const locked = if (backend_locked) false else if (control) |budget| try lockBackendWithAdmission(@TypeOf(backend.*), backend, budget) else lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    if (backend.retainCachedRunBlock(path, run.id, offset, window.physicalLen())) |lease| {
        backend.recordLocalBlockCacheHit();
        return lease;
    }
    return null;
}

fn localDecodeWorkingBytes(window: lsm_table_file.EntryDataWindow) !usize {
    return localDecodeWorkingBytesFor(window, false);
}

fn localDecodeWorkingBytesFor(window: lsm_table_file.EntryDataWindow, point: bool) !usize {
    // Uncompressed input becomes the output allocation directly. Snappy only
    // needs its encoded input in scratch. Full prefix decoding reconstructs
    // keys in the output; point decoding borrows restart keys and reconstructs one key in place.
    // Keep eightfold logical headroom for that growing arena-backed key.
    // Prefix+Snappy reserves the output plus fourfold arena headroom for the
    // validated intermediate prefix payload (at most twice the logical size).
    const logical_factor: usize = switch (window.compression) {
        .none, .snappy => 1,
        .prefix => if (point) 9 else 1,
        .prefix_snappy => if (point) 17 else 9,
    };
    const physical_factor: usize = if (window.compression == .none) 0 else 4;
    const logical = std.math.mul(usize, window.len, logical_factor) catch return error.InvalidTableFile;
    const physical = std.math.mul(usize, window.physicalLen(), physical_factor) catch return error.InvalidTableFile;
    return std.math.add(usize, std.math.add(usize, logical, physical) catch return error.InvalidTableFile, LocalReader.retained_bytes_per_workspace + @sizeOf(SharedBytes)) catch return error.InvalidTableFile;
}

fn localWorkspace(backend: anytype, window: lsm_table_file.EntryDataWindow, point: bool) !LocalReader.Workspace {
    return localWorkspaceWithAdmission(backend, window, point, null);
}

fn localWorkspaceWithAdmission(backend: anytype, window: lsm_table_file.EntryDataWindow, point: bool, control: ?read_admission.Port) !LocalReader.Workspace {
    return backend.local_reader.acquireWithAdmission(backend.allocator, backend.options.resource_manager, backend.manifestCoordinationIo(), try localDecodeWorkingBytesFor(window, point), backend.options.local_decode_working_bytes, window.len + @sizeOf(SharedBytes), control);
}

fn loadDecodedLocalBlock(backend: anytype, allocator: Allocator, path: []const u8, offset: u64, window: lsm_table_file.EntryDataWindow) ![]u8 {
    return loadDecodedLocalBlockWithAdmission(backend, allocator, path, offset, window, null);
}

/// The source binding and input window belong to the enclosing read operation.
/// Per-step adapters borrow controls; no control is retained by this owner.
fn BufferedRunBlock(comptime Source: type) type {
    return struct {
        source: Source,
        bytes: []u8,
        length: usize,
        start: usize = 0,
        valid: usize = 0,
        physical_bytes: usize = 0,
        decode_pass: bool = false,
        decode_crc: @import("antfly_hash").Crc32 = .init(),
        verified_bytes: usize = 0,

        fn beginDecode(self: *@This()) void {
            self.valid = 0;
            self.decode_pass = true;
        }

        fn window(self: *@This(), offset: usize, max_len: usize, control: ?read_admission.Port) ![]const u8 {
            if (offset > self.length) return error.InvalidArgument;
            if (max_len == 0 or offset == self.length) return &.{};
            if (self.bytes.len == 0) return error.InvalidArgument;
            if (self.valid == 0 or offset < self.start or offset - self.start >= self.valid) {
                const start = offset / self.bytes.len * self.bytes.len;
                const take = @min(self.bytes.len, self.length - start);
                // A failed provider may partially overwrite bytes. Never leave
                // stale cache metadata pointing at that partial refill.
                self.valid = 0;
                self.physical_bytes += take;
                try self.source.readInto(@intCast(start), self.bytes[0..take], control);
                self.start = start;
                self.valid = take;
                if (self.decode_pass) {
                    if (start != self.verified_bytes) return error.CorruptInput;
                    self.decode_crc.update(self.bytes[0..take]);
                    self.verified_bytes += take;
                }
            }
            const local = offset - self.start;
            return self.bytes[local..][0..@min(max_len, self.valid - local)];
        }

        const Borrow = struct {
            owner: *BufferedRunBlock(Source),
            length: usize,
            control: ?read_admission.Port,
            pub fn window(self: @This(), offset: usize, max_len: usize) ![]const u8 {
                return self.owner.window(offset, max_len, self.control);
            }
            pub fn readInto(self: @This(), offset: u64, out: []u8) !void {
                var copied: usize = 0;
                const start = std.math.cast(usize, offset) orelse return error.InvalidArgument;
                if (start > self.length or out.len > self.length - start) return error.InvalidArgument;
                while (copied < out.len) {
                    const bytes = try self.window(start + copied, out.len - copied);
                    @memcpy(out[copied..][0..bytes.len], bytes);
                    copied += bytes.len;
                }
            }
        };
    };
}

/// Movable ownership of a funded raw, Snappy or prefix block materialization. Pending
/// callers pin the immutable run version and backend until retirement. The
/// input path, source use, workspace and partial output belong to this owner;
/// prefix variants also own their funded encoded/intermediate buffers.
/// Controls and self-relative adapters exist only within each step. Output's
/// allocator must outlive its eventual transfer or destruction.
pub fn OwnedRunBlockRead(comptime BackendType: type) type {
    const Source = BackendType.RunRangeReader;
    const Reader = BufferedRunBlock(Source);
    return struct {
        pub const max_step_bytes: usize = 64 * 1024;
        pub const Status = enum { pending, ready };
        const Phase = enum { allocate_output, raw, compressed, prefix_input_allocate, prefix_input, prefix_prepare, prefix_snappy_allocate, prefix_snappy, prefix_expand, ready, taken, failed, closed };
        backend: *BackendType,
        allocator: Allocator,
        work: ?LocalReader.Workspace,
        reader: ?Reader,
        window: lsm_table_file.EntryDataWindow,
        cursor: lsm_table_file.BlockDecodeCursor,
        output: ?[]u8 = null,
        prefix_input: ?[]u8 = null,
        prefix_intermediate: ?[]u8 = null,
        prefix_offset: usize = 0,
        prefix_crc: @import("antfly_hash").Crc32 = .init(),
        prefix_decoder: @import("../../encoding/snappy.zig").DecodeCursor = .init(0, 0),
        prefix_expander: lsm_table_file.PrefixDecodeCursor = .init(0, 0),
        raw_offset: usize = 0,
        raw_crc: @import("antfly_hash").Crc32 = .init(),
        phase: Phase,
        failure: ?anyerror = null,
        started_ns: u64,

        pub fn init(backend: *BackendType, allocator: Allocator, path: []const u8, offset: u64, window: lsm_table_file.EntryDataWindow, control: ?read_admission.Port) !@This() {
            if (control) |port| try port.checkpoint();
            const prefix = window.compression == .prefix or window.compression == .prefix_snappy;
            const input_len: usize = window.physicalLen();
            if (window.compression == .none and input_len != window.len) return error.InvalidTableFile;
            const window_len: usize = if (window.compression == .none or prefix) 0 else @min(input_len, max_step_bytes);
            const result_bytes = std.math.add(usize, window.len, @sizeOf(SharedBytes)) catch return error.InvalidTableFile;
            const prefix_ceiling = if (window.compression == .prefix_snappy)
                std.math.add(usize, std.math.mul(usize, window.len, 2) catch return error.InvalidTableFile, 32) catch return error.InvalidTableFile
            else
                0;
            const encoded_bytes = if (prefix) input_len else 0;
            const encoded_work = std.math.add(usize, encoded_bytes, prefix_ceiling) catch return error.InvalidTableFile;
            const arena_bytes = try LocalReader.streamingScratchBound(path.len, window_len);
            const retained = std.math.add(usize, encoded_work, arena_bytes) catch return error.InvalidTableFile;
            const working = std.math.add(usize, result_bytes, retained) catch return error.InvalidTableFile;
            var work = try backend.local_reader.acquireWithAdmission(backend.allocator, backend.options.resource_manager, backend.manifestCoordinationIo(), working, backend.options.local_decode_working_bytes, result_bytes, control);
            errdefer work.release();
            const scratch = work.allocator();
            const owned_path = try scratch.dupe(u8, path);
            var source = try backend.openRunRangeReaderWithAdmission(owned_path, offset, input_len, control);
            errdefer source.deinit();
            const bytes = try scratch.alloc(u8, window_len);
            if (control) |port| try port.checkpoint();
            return .{
                .backend = backend,
                .allocator = allocator,
                .work = work,
                .reader = .{ .source = source, .bytes = bytes, .length = input_len },
                .window = window,
                .cursor = try lsm_table_file.BlockDecodeCursor.init(if (prefix) .none else window.compression, if (prefix) window.len else input_len, window.len, window.checksum),
                .phase = if (prefix) .prefix_input_allocate else if (window.compression == .none) .allocate_output else .compressed,
                .started_ns = backend.readStatsNowNs(),
            };
        }

        /// Returns after one finite decode/checksum/copy step or one funded
        /// output allocation. Allocator work is separate from byte quanta.
        /// Source refills remain at most 64 KiB each (up to two per step).
        pub fn step(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (quantum == 0 or quantum > max_step_bytes) return error.InvalidArgument;
            switch (self.phase) {
                .failed => return self.failure.?,
                .taken, .closed => return error.InvalidArgument,
                else => {},
            }
            return self.advance(quantum, control) catch |err| {
                // Original authority takes precedence over a simultaneous
                // provider/codec failure, before unconditional retirement.
                const cause = if (control) |port| blk: {
                    port.checkpoint() catch |original| break :blk original;
                    break :blk err;
                } else err;
                self.discardOutput();
                self.retireWorking();
                self.phase = .failed;
                self.failure = cause;
                return cause;
            };
        }

        fn advance(self: *@This(), quantum: usize, control: ?read_admission.Port) !Status {
            if (control) |port| try port.checkpoint();
            switch (self.phase) {
                .ready => return .ready,
                .allocate_output => {
                    self.output = try self.allocator.alloc(u8, self.window.len);
                    if (control) |port| try port.checkpoint();
                    self.phase = if (self.window.compression == .prefix or self.window.compression == .prefix_snappy) .prefix_expand else if (self.window.compression == .none) .raw else .compressed;
                    return .pending;
                },
                .prefix_input_allocate => {
                    // Prefix variants currently retain a whole encoded buffer,
                    // admitted up front. Physical reads and expansion resume;
                    // there is no synchronous full-block get or decode fallback.
                    self.prefix_input = try self.allocator.alloc(u8, self.window.physicalLen());
                    self.phase = .prefix_input;
                    if (control) |port| try port.checkpoint();
                    return .pending;
                },
                .prefix_input => {
                    const bytes = self.prefix_input.?;
                    const take = @min(quantum, bytes.len - self.prefix_offset);
                    const reader = &self.reader.?;
                    if (take != 0) {
                        try reader.source.readInto(@intCast(self.prefix_offset), bytes[self.prefix_offset..][0..take], control);
                        reader.physical_bytes += take;
                        self.prefix_crc.update(bytes[self.prefix_offset..][0..take]);
                        self.prefix_offset += take;
                    }
                    if (control) |port| try port.checkpoint();
                    if (self.prefix_offset == bytes.len) {
                        if (self.prefix_crc.final() != self.window.checksum) return error.TableBlockChecksumMismatch;
                        self.phase = .prefix_prepare;
                    }
                    return .pending;
                },
                .prefix_prepare => {
                    if (self.window.compression == .prefix_snappy) {
                        try lsm_table_file.validatePrefixDecodedSize(self.prefix_input.?, self.window.len);
                        const len = try @import("../../encoding/snappy.zig").decodedLen(self.prefix_input.?);
                        self.prefix_decoder = .init(self.prefix_input.?.len, len);
                        self.phase = .prefix_snappy_allocate;
                    } else {
                        self.prefix_expander = .init(self.prefix_input.?.len, self.window.len);
                        self.phase = .prefix_expand;
                    }
                    if (control) |port| try port.checkpoint();
                    return .pending;
                },
                .prefix_snappy_allocate => {
                    self.prefix_intermediate = try self.allocator.alloc(u8, self.prefix_decoder.output_len);
                    self.phase = .prefix_snappy;
                    if (control) |port| try port.checkpoint();
                    return .pending;
                },
                .prefix_snappy => {
                    const progress = try self.prefix_decoder.step(self.prefix_input.?, self.prefix_intermediate.?, quantum);
                    if (control) |port| try port.checkpoint();
                    if (progress.done) {
                        self.prefix_expander = .init(self.prefix_intermediate.?.len, self.window.len);
                        self.phase = .prefix_expand;
                    }
                    return .pending;
                },
                .prefix_expand => {
                    const input = self.prefix_intermediate orelse self.prefix_input.?;
                    const progress = try self.prefix_expander.step(input, self.output, quantum);
                    if (control) |port| try port.checkpoint();
                    switch (progress.status) {
                        .pending => return .pending,
                        .needs_output => {
                            self.phase = .allocate_output;
                            return .pending;
                        },
                        .done => {},
                    }
                },
                .raw => {
                    const out = self.output.?;
                    const take = @min(out.len - self.raw_offset, quantum);
                    const reader = &self.reader.?;
                    reader.physical_bytes += take;
                    if (take != 0) try reader.source.readInto(@intCast(self.raw_offset), out[self.raw_offset..][0..take], control);
                    self.raw_crc.update(out[self.raw_offset..][0..take]);
                    self.raw_offset += take;
                    if (control) |port| try port.checkpoint();
                    if (self.raw_offset != out.len) return .pending;
                    if (self.raw_crc.final() != self.window.checksum) return error.TableBlockChecksumMismatch;
                },
                .compressed => {
                    const reader = &self.reader.?;
                    const borrow: Reader.Borrow = .{ .owner = reader, .length = reader.length, .control = control };
                    const progress = try self.cursor.stepFromReader(borrow, self.output, quantum);
                    if (control) |port| try port.checkpoint();
                    if (!reader.decode_pass and self.cursor.phase == .header) reader.beginDecode();
                    switch (progress.status) {
                        .pending => return .pending,
                        .needs_output => {
                            std.debug.assert(self.output == null);
                            self.phase = .allocate_output;
                            return .pending;
                        },
                        .done => if (reader.verified_bytes != reader.length or reader.decode_crc.final() != self.window.checksum)
                            return error.TableBlockChecksumMismatch,
                    }
                },
                else => unreachable,
            }
            self.retireWorking();
            self.phase = .ready;
            return .ready;
        }

        /// Transfer bytes exactly once, only after complete verification. This
        /// moves data ownership; publication still needs the caller's authority.
        pub fn takeOutput(self: *@This()) ![]u8 {
            if (self.phase != .ready) return error.InvalidArgument;
            const bytes = self.output.?;
            self.output = null;
            self.phase = .taken;
            return bytes;
        }

        fn discardOutput(self: *@This()) void {
            if (self.output) |bytes| self.allocator.free(bytes);
            self.output = null;
        }

        fn retireWorking(self: *@This()) void {
            if (self.prefix_input) |bytes| self.allocator.free(bytes);
            self.prefix_input = null;
            if (self.prefix_intermediate) |bytes| self.allocator.free(bytes);
            self.prefix_intermediate = null;
            if (self.reader) |*reader| {
                self.backend.recordTableBlockLoad(reader.physical_bytes, self.backend.readStatsElapsedNs(self.started_ns));
                reader.source.deinit();
                self.reader = null;
            }
            if (self.work) |*work| {
                work.release();
                self.work = null;
            }
        }

        /// Pending, ready and failed states all retire without a port or a new
        /// execution grant. Ready/failure already released backend dependencies.
        pub fn deinit(self: *@This()) void {
            self.discardOutput();
            self.retireWorking();
            self.phase = .closed;
        }
    };
}

fn loadStreamingLocalBlock(backend: anytype, allocator: Allocator, path: []const u8, offset: u64, window: lsm_table_file.EntryDataWindow, control: ?read_admission.Port) ![]u8 {
    var operation = try OwnedRunBlockRead(@TypeOf(backend.*)).init(backend, allocator, path, offset, window, control);
    defer operation.deinit();
    while (try operation.step(64 * 1024, control) == .pending) {}
    return operation.takeOutput();
}

fn loadDecodedLocalBlockWithAdmission(backend: anytype, allocator: Allocator, path: []const u8, offset: u64, window: lsm_table_file.EntryDataWindow, control: ?read_admission.Port) ![]u8 {
    if (comptime @hasField(@TypeOf(backend.*), "local_reader")) {
        if (comptime @hasDecl(@TypeOf(backend.*), "openRunRangeReaderWithAdmission")) {
            if ((window.compression == .none or window.compression == .snappy) and backend.supportsRunRangeReader(control))
                return loadStreamingLocalBlock(backend, allocator, path, offset, window, control);
        }
        var work = try localWorkspaceWithAdmission(backend, window, false, control);
        defer work.release();
        if (window.compression == .none) {
            if (window.physicalLen() != window.len) return error.InvalidTableFile;
            const payload = try loadRunTableBlockWithStatsAndAdmission(backend, allocator, path, offset, window.physicalLen(), control);
            errdefer allocator.free(payload);
            if (payload.len != window.len) return error.InvalidTableFile;
            try lsm_table_file.validateBlockPayloadWithAdmission(payload, window.checksum, control);
            return payload;
        }
        const scratch = work.allocator();
        const payload = try loadRunTableBlockWithStatsAndAdmission(backend, scratch, path, offset, window.physicalLen(), control);
        return lsm_table_file.decodeBlockPayloadWithScratchAllocWithAdmission(allocator, scratch, window.compression, payload, window.len, window.checksum, control);
    }
    const payload = try loadRunTableBlockWithStatsAndAdmission(backend, allocator, path, offset, window.physicalLen(), control);
    defer allocator.free(payload);
    return lsm_table_file.decodeBlockPayloadAllocWithAdmission(allocator, window.compression, payload, window.len, window.checksum, control);
}

fn loadLocalBlockCandidate(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, backend_locked: bool, admit: bool, control: ?read_admission.Port) !*SharedBytes {
    if (control) |budget| try budget.checkpoint();
    const path = run.path orelse return error.RunStateUnavailable;
    const offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const len = window.physicalLen();
    if (try retainLocalCachedBlockWithAdmission(backend, run, index, window, backend_locked, control)) |lease| return lease;
    backend.recordLocalBlockCacheMiss();
    // Release decoder admission before taking the writer mutex to publish.
    // A writer may wait for a workspace, but never for a cache publisher.
    const resources = @import("../resource_manager.zig");
    var construction: ?resources.ConstructionCredit = null;
    // Declared before decoded allocations: failure destroys physical bytes
    // before accounting retirement, independently of a rejected read scope.
    defer if (construction) |*credit| credit.retire();
    var read_credit: ?resources.Reservation = if (backend.options.resource_manager) |manager| blk: {
        const bytes = @as(usize, window.len) + @sizeOf(SharedBytes);
        if (control) |budget| {
            construction = try manager.reserveConstructionWithAdmission(.lsm_read_working_set, bytes, budget);
            break :blk null;
        }
        break :blk try manager.reserveWithoutReclaim(.lsm_read_working_set, bytes);
    } else null;
    defer if (read_credit) |*credit| credit.release();
    const lease = blk: {
        const bytes = try loadDecodedLocalBlockWithAdmission(backend, backend.allocator, path, offset, window, control);
        errdefer backend.allocator.free(bytes);
        break :blk try SharedBytes.create(backend.allocator, bytes);
    };
    lease.result_pins_allowed = backend.options.resource_manager == null;
    errdefer lease.release();
    if (construction) |*credit| try lease.registerRetirementWithAdmission(credit, control.?);
    const locked = if (backend_locked) false else if (control) |budget| try lockBackendWithAdmission(@TypeOf(backend.*), backend, budget) else lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    if (backend.retainCachedRunBlock(path, run.id, offset, len)) |winner| {
        lease.release();
        return winner;
    }
    // Legacy credit transfers after the final scope check. Controlled credit
    // already belongs to the registered physical owner; rejection or a losing
    // duplicate only publishes readiness for independent admitted cleanup.
    if (construction == null) lease.reservation = read_credit;
    read_credit = null;
    if (admit) {
        if (control) |budget| _ = try backend.cacheRunBlockLeaseWithAdmission(path, run.id, offset, len, lease, budget) else _ = backend.cacheRunBlockLease(path, run.id, offset, len, lease);
    }
    return lease;
}

fn localDecodeKey(run: *const Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, admit: bool) !LocalReader.Key {
    return .{
        .path = run.path orelse return error.RunStateUnavailable,
        .generation = run.read_identity,
        .run_id = run.id,
        .offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(),
        .physical_len = window.physicalLen(),
        .logical_len = window.len,
        .compression = window.compression,
        .checksum = window.checksum,
        .admit = admit,
    };
}

/// Retire the peer reference before any retry. Never restart flight election:
/// a failed producer allows one independent physical attempt, with the same scope.
fn waitLocalBlockOrDecode(backend: anytype, flight: *LocalReader.Flight, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, admit: bool, control: ?read_admission.Port) !*SharedBytes {
    const result = backend.local_reader.waitWithAdmission(flight, control);
    backend.local_reader.releaseFlight(flight);
    return result catch |cause| {
        if (cause != error.LocalDecodeProducerFailed) return cause;
        if (control) |budget| try budget.checkpoint();
        return loadLocalBlockCandidate(backend, run, index, window, false, admit, control);
    };
}

fn joinLocalBlockDecode(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow) !?*SharedBytes {
    return joinLocalBlockDecodeWithAdmission(backend, run, index, window, null);
}

fn joinLocalBlockDecodeWithAdmission(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    control: ?read_admission.Port,
) !?*SharedBytes {
    if (comptime @hasField(@TypeOf(backend.*), "local_reader")) {
        const flight = try backend.local_reader.joinWithAdmission(try localDecodeKey(run, index, window, true), control) orelse return null;
        return waitLocalBlockOrDecode(backend, flight, run, index, window, true, control);
    }
    return null;
}

fn loadLocalBlockLease(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, backend_locked: bool, admit: bool) !*SharedBytes {
    return loadLocalBlockLeaseWithAdmission(backend, run, index, window, backend_locked, admit, null);
}

fn loadLocalBlockLeaseWithAdmission(backend: anytype, run: *Run, index: *const lsm_table_file.TableIndex, window: lsm_table_file.EntryDataWindow, backend_locked: bool, admit: bool, control: ?read_admission.Port) !*SharedBytes {
    if (try retainLocalCachedBlockWithAdmission(backend, run, index, window, backend_locked, control)) |lease| return lease;
    if (comptime @hasField(@TypeOf(backend.*), "local_reader")) {
        // Joining a publisher while holding the writer mutex would deadlock.
        // Locked callers decode independently through the same bounded pool.
        if (!backend_locked) {
            const ticket = try backend.local_reader.beginWithAdmission(backend.allocator, backend.manifestCoordinationIo(), try localDecodeKey(run, index, window, admit), control);
            if (ticket) |owned| {
                if (!owned.leader) return waitLocalBlockOrDecode(backend, owned.flight, run, index, window, admit, control);
                defer backend.local_reader.releaseFlight(owned.flight);
                const payload = loadLocalBlockCandidate(backend, run, index, window, false, admit, control);
                backend.local_reader.finish(owned.flight, payload);
                return payload;
            }
        }
    }
    return loadLocalBlockCandidate(backend, run, index, window, backend_locked, admit, control);
}

fn loadOwnedBlockForWindowAlloc(
    backend: anytype,
    allocator: Allocator,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
) ![]u8 {
    const path = run.path orelse return error.RunStateUnavailable;
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const physical_len = window.physicalLen();
    if (backend.options.cache != null) {
        var block_handle = try loadRunTableBlockHandleAtOffset(
            backend,
            run,
            absolute_offset,
            physical_len,
            window.compression,
            window.len,
            window.checksum,
            true,
        );
        defer block_handle.release();
        return try allocator.dupe(u8, block_handle.runTableBlock());
    }

    {
        const locked = lockBackend(@TypeOf(backend.*), backend);
        defer unlockBackend(@TypeOf(backend.*), backend, locked);
        if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend)) {
            if (backend.getCachedRunBlock(path, run.id, absolute_offset, physical_len)) |cached_bytes| {
                backend.recordLocalBlockCacheHit();
                return try allocator.dupe(u8, cached_bytes);
            }
        }
    }
    if (localBlockCacheEnabled(backend)) {
        backend.recordLocalBlockCacheMiss();
    }

    const bytes = try loadDecodedLocalBlock(backend, allocator, path, absolute_offset, window);
    errdefer allocator.free(bytes);
    {
        const locked = lockBackend(@TypeOf(backend.*), backend);
        defer unlockBackend(@TypeOf(backend.*), backend, locked);
        if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend) and localBlockCacheEligible(backend, bytes.len)) {
            _ = try backend.putCachedRunBlock(path, run.id, absolute_offset, physical_len, try backend.allocator.dupe(u8, bytes));
        }
    }
    return bytes;
}

fn loadOwnedBlockForWindowAllocMaybeLocked(
    backend: anytype,
    allocator: Allocator,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    backend_locked: bool,
) ![]u8 {
    if (!backend_locked) return try loadOwnedBlockForWindowAlloc(backend, allocator, run, index, window);

    const path = run.path orelse return error.RunStateUnavailable;
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const physical_len = window.physicalLen();
    if (backend.options.cache != null) {
        var block_handle = try loadRunTableBlockHandleAtOffset(
            backend,
            run,
            absolute_offset,
            physical_len,
            window.compression,
            window.len,
            window.checksum,
            true,
        );
        defer block_handle.release();
        return try allocator.dupe(u8, block_handle.runTableBlock());
    }

    if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend)) {
        if (backend.getCachedRunBlock(path, run.id, absolute_offset, physical_len)) |cached_bytes| {
            backend.recordLocalBlockCacheHit();
            return try allocator.dupe(u8, cached_bytes);
        }
    }
    if (localBlockCacheEnabled(backend)) {
        backend.recordLocalBlockCacheMiss();
    }

    const bytes = try loadDecodedLocalBlock(backend, allocator, path, absolute_offset, window);
    errdefer allocator.free(bytes);
    if (@hasField(@TypeOf(backend.*), "run_block_cache") and localBlockCacheEnabled(backend) and localBlockCacheEligible(backend, bytes.len)) {
        _ = try backend.putCachedRunBlock(path, run.id, absolute_offset, physical_len, try backend.allocator.dupe(u8, bytes));
    }
    return bytes;
}

fn loadOwnedBlockForWindow(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
) ![]u8 {
    return try loadOwnedBlockForWindowAlloc(backend, backend.allocator, run, index, window);
}

fn loadOwnedBlockForWindowMaybeLocked(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    window: lsm_table_file.EntryDataWindow,
    backend_locked: bool,
) ![]u8 {
    return try loadOwnedBlockForWindowAllocMaybeLocked(backend, backend.allocator, run, index, window, backend_locked);
}

const PointValueOutput = struct { allocator: Allocator, held: *PointResultValues, control: ?read_admission.Port = null };

fn findExactEntryInCompressedPrefixBlock(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    block_index: usize,
    namespace: backend_types.Namespace,
    key: []const u8,
    output: ?PointValueOutput,
) !?OwnedTableEntry {
    const control = if (output) |target| target.control else null;
    if (control) |budget| try budget.checkpoint();
    const block = index.blocks[block_index];
    const window = index.blockWindow(block_index);
    switch (window.compression) {
        .prefix, .prefix_snappy => {},
        .none, .snappy => return null,
    }
    const path = run.path orelse return error.RunStateUnavailable;
    const absolute_offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    var workspace: ?LocalReader.Workspace = if (comptime @hasField(@TypeOf(backend.*), "local_reader")) try localWorkspaceWithAdmission(backend, window, true, control) else null;
    defer if (workspace) |*work| work.release();
    const scratch = if (workspace) |*work| work.allocator() else backend.allocator;
    const payload = try loadRunTableBlockWithStatsAndAdmission(backend, scratch, path, absolute_offset, window.physicalLen(), control);
    defer scratch.free(payload);
    if (output) |target| {
        const found = try findExactEntryInCachedPrefixWithScratchWithAdmission(backend, payload, block, window, target.held, target.allocator, scratch, namespace, key, .transaction_owned, control) orelse return null;
        return .{ .entry = found.entry, .final_point_value = true };
    }
    const positioned = try lsm_table_file.findExactEntryInCompressedBlockPayloadWithScratchAlloc(
        backend.allocator,
        scratch,
        window.compression,
        payload,
        window.checksum,
        block.first_entry_index,
        namespace.name,
        key,
        window.len,
    ) orelse return null;
    return .{
        .entry = positioned.entry,
        .bytes = positioned.bytes,
    };
}

fn findExactEntryWithLocalIndexBlockMeta(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?OwnedTableEntry {
    return findExactEntryWithLocalIndexBlockMetaWithOutput(backend, run, index, namespace, key, null);
}

fn findExactEntryWithLocalIndexBlockMetaWithOutput(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    namespace: backend_types.Namespace,
    key: []const u8,
    output: ?PointValueOutput,
) !?OwnedTableEntry {
    const control = if (output) |target| target.control else null;
    if (control) |budget| try budget.checkpoint();
    const block_index = index.findBlockIndex(namespace.name, key) orelse return null;
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
    if (!block.maybeContains(namespace.name, key)) {
        backend.recordBloomNegative();
        return null;
    }
    const window = index.blockWindow(block_index);
    // Borrow a warm decoded block before allocating compressed lookup scratch.
    // A miss preserves the compact direct-prefix lookup for cold point reads.
    if (localBlockCacheEnabled(backend)) {
        if (try retainLocalCachedBlockWithAdmission(backend, run, index, window, false, control)) |lease| {
            return findExactEntryInBlockLease(lease, index, block_index, namespace, key);
        }
        if (namespace.retainDataBlocks()) if (try joinLocalBlockDecodeWithAdmission(backend, run, index, window, control)) |lease| {
            return findExactEntryInBlockLease(lease, index, block_index, namespace, key);
        };
        if (namespace.retainDataBlocks()) {
            if (try beginLocalBlockPromotionWithAdmission(backend, run, index, window, false, control)) |claim| {
                var promotion = claim;
                var admitted = false;
                defer promotion.finish(admitted);
                const lease = try loadLocalBlockLeaseWithAdmission(backend, run, index, window, false, namespace.retainDataBlocks(), control);
                admitted = lease.cache_admitted;
                return findExactEntryInBlockLease(lease, index, block_index, namespace, key);
            }
        }
    }
    if (window.compression == .prefix or window.compression == .prefix_snappy)
        return try findExactEntryInCompressedPrefixBlock(backend, run, index, block_index, namespace, key, output);
    if (localBlockCacheEnabled(backend)) return try findExactEntryInLocalLeaseWithAdmission(backend, run, index, window, block_index, namespace, key, false, control);
    const bytes = if (control != null) try loadDecodedLocalBlockWithAdmission(backend, backend.allocator, run.path orelse return error.RunStateUnavailable, @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(), window, control) else try loadOwnedBlockForWindow(
        backend,
        run,
        index,
        window,
    );
    errdefer backend.allocator.free(bytes);
    const positioned = try lsm_table_file.findExactEntryInBlock(
        index,
        bytes,
        block_index,
        namespace.name,
        key,
    ) orelse {
        backend.allocator.free(bytes);
        return null;
    };
    return .{
        .entry = positioned.entry,
        .bytes = bytes,
    };
}

fn findExactEntryWithLocalIndexBlockMetaMaybeLocked(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?OwnedTableEntry {
    return findExactEntryWithLocalIndexBlockMetaMaybeLockedWithOutput(backend, run, index, namespace, key, backend_locked, null);
}

fn findExactEntryWithLocalIndexBlockMetaMaybeLockedWithOutput(
    backend: anytype,
    run: *Run,
    index: *const lsm_table_file.TableIndex,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    output: ?PointValueOutput,
) !?OwnedTableEntry {
    const control = if (output) |target| target.control else null;
    if (control) |budget| try budget.checkpoint();
    if (!backend_locked) return try findExactEntryWithLocalIndexBlockMetaWithOutput(backend, run, index, namespace, key, output);

    const block_index = index.findBlockIndex(namespace.name, key) orelse return null;
    const block = index.blocks[block_index];
    if (!block.mayContainKeyByBounds(namespace.name, key)) return null;
    if (!block.maybeContains(namespace.name, key)) {
        backend.recordBloomNegative();
        return null;
    }
    const window = index.blockWindow(block_index);
    // Borrow a warm decoded block before allocating compressed lookup scratch.
    // A miss preserves the compact direct-prefix lookup for cold point reads.
    if (localBlockCacheEnabled(backend)) {
        if (try retainLocalCachedBlockWithAdmission(backend, run, index, window, true, control)) |lease| {
            return findExactEntryInBlockLease(lease, index, block_index, namespace, key);
        }
        if (namespace.retainDataBlocks()) {
            if (try beginLocalBlockPromotionWithAdmission(backend, run, index, window, true, control)) |claim| {
                var promotion = claim;
                var admitted = false;
                defer promotion.finish(admitted);
                const lease = try loadLocalBlockLeaseWithAdmission(backend, run, index, window, true, namespace.retainDataBlocks(), control);
                admitted = lease.cache_admitted;
                return findExactEntryInBlockLease(lease, index, block_index, namespace, key);
            }
        }
    }
    if (window.compression == .prefix or window.compression == .prefix_snappy)
        return try findExactEntryInCompressedPrefixBlock(backend, run, index, block_index, namespace, key, output);
    if (localBlockCacheEnabled(backend)) return try findExactEntryInLocalLeaseWithAdmission(backend, run, index, window, block_index, namespace, key, true, control);
    const bytes = if (control != null) try loadDecodedLocalBlockWithAdmission(backend, backend.allocator, run.path orelse return error.RunStateUnavailable, @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset(), window, control) else try loadOwnedBlockForWindowMaybeLocked(
        backend,
        run,
        index,
        window,
        true,
    );
    errdefer backend.allocator.free(bytes);
    const positioned = try lsm_table_file.findExactEntryInBlock(
        index,
        bytes,
        block_index,
        namespace.name,
        key,
    ) orelse {
        backend.allocator.free(bytes);
        return null;
    };
    return .{
        .entry = positioned.entry,
        .bytes = bytes,
    };
}

fn loadVisibleEntryFromPathRun(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
) !?OwnedTableEntry {
    return try findExactEntryWithLocalIndex(backend, run, namespace, key);
}

fn findExactEntryWithLocalIndexMaybeLocked(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?OwnedTableEntry {
    if (!backend_locked) return try findExactEntryWithLocalIndex(backend, run, namespace, key);

    const index = try indexForRunNoCacheMaybeLocked(backend, run, true);
    try requireTableBlocks(index);
    return try findExactEntryWithLocalIndexBlockMetaMaybeLocked(backend, run, index, namespace, key, true);
}

fn loadVisibleEntryFromPathRunMaybeLocked(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?OwnedTableEntry {
    return try findExactEntryWithLocalIndexMaybeLocked(backend, run, namespace, key, backend_locked);
}

fn getFromRunWithLocalIndex(
    backend: anytype,
    run: *Run,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !?[]const u8 {
    return getFromRunWithLocalIndexWithAdmission(backend, run, held_blocks, held_values, value_allocator, namespace, key, backend_locked, null);
}

fn getFromRunWithLocalIndexWithAdmission(
    backend: anytype,
    run: *Run,
    held_blocks: ?*std.ArrayListUnmanaged(BlockPin),
    held_values: *PointResultValues,
    value_allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    control: ?read_admission.Port,
) !?[]const u8 {
    if (control) |budget| try budget.checkpoint();
    const index = try indexForRunNoCacheMaybeLockedWithAdmission(backend, run, backend_locked, control);
    try requireTableBlocks(index);
    const loaded = try findExactEntryWithLocalIndexBlockMetaMaybeLockedWithOutput(backend, run, index, namespace, key, backend_locked, .{ .allocator = value_allocator, .held = held_values, .control = control }) orelse return null;
    var transferred = false;
    defer if (!transferred) loaded.deinit(backend.allocator);
    if (control) |budget| try budget.checkpoint();
    if (loaded.entry.tombstone) return error.NotFound;
    if (loaded.final_point_value) return loaded.entry.value;
    if (namespace.borrow_local_point_results) if (held_blocks) |pins| if (loaded.local) |payload| {
        if (try retainLocalResultPin(backend, payload, pins)) return loaded.entry.value;
    };

    // Wide values dominate their block. Transfer the decoded allocation when
    // its owner matches rather than copying the row out and immediately
    // freeing it. Small metadata gets keep their compact value-only buffer;
    // retained amplification is at most 2x for this transfer path.
    if (loaded.local == null and loaded.entry.value.len >= 4096 and loaded.entry.value.len >= loaded.bytes.len / 2 and
        value_allocator.ptr == backend.allocator.ptr and value_allocator.vtable == backend.allocator.vtable)
    {
        try held_values.append(value_allocator, loaded.bytes);
        transferred = true;
        return loaded.entry.value;
    }

    return try copyPointValueWithAdmission(backend, value_allocator, held_values, loaded.entry.value, control);
}

fn runMayContainWithFilter(backend: anytype, run: *Run, namespace: backend_types.Namespace, key: []const u8) !bool {
    if (!runMayContain(run.*, namespace, key)) return false;
    const filter = try ensureRunBloomFilterForRead(backend, run);
    if (filter) |present_filter| {
        const present = lsm_table_file.maybeContains(present_filter, namespace.name, key);
        if (!present) backend.recordBloomNegative();
        return present;
    }
    if (run.path != null) {
        const present = if (backend.options.cache != null) blk: {
            var handle = try loadRunTableIndexHandle(backend, run);
            defer handle.release();
            break :blk lsm_table_file.maybeContains(handle.runTableIndex().borrowFilter(), namespace.name, key);
        } else blk: {
            const index = try indexForRunNoCache(backend, run);
            break :blk lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        };
        if (!present) backend.recordBloomNegative();
        return present;
    }
    return true;
}

fn runMayContainWithFilterMaybeLocked(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
) !bool {
    return runMayContainWithFilterMaybeLockedWithAdmission(backend, run, namespace, key, backend_locked, null);
}

fn runMayContainWithFilterMaybeLockedWithAdmission(
    backend: anytype,
    run: *Run,
    namespace: backend_types.Namespace,
    key: []const u8,
    backend_locked: bool,
    control: ?read_admission.Port,
) !bool {
    if (control) |budget| try budget.checkpoint();

    if (!runMayContain(run.*, namespace, key)) return false;
    const filter = try ensureRunBloomFilterForReadMaybeLockedWithAdmission(backend, run, backend_locked, control);
    if (filter) |present_filter| {
        const present = lsm_table_file.maybeContains(present_filter, namespace.name, key);
        if (!present) backend.recordBloomNegative();
        return present;
    }
    if (run.path != null) {
        const present = if (backend.options.cache != null) blk: {
            var handle = try loadRunTableIndexHandleWithAdmission(backend, run, control);
            defer handle.release();
            break :blk lsm_table_file.maybeContains(handle.runTableIndex().borrowFilter(), namespace.name, key);
        } else blk: {
            const index = try indexForRunNoCacheMaybeLockedWithAdmission(backend, run, backend_locked, control);
            break :blk lsm_table_file.maybeContains(index.borrowFilter(), namespace.name, key);
        };
        if (!present) backend.recordBloomNegative();
        return present;
    }
    return true;
}

fn ensureRunBloomFilterForRead(backend: anytype, run: *Run) !?bloom.OwnedFilter {
    if (run.shared_read_version) return null;
    if (run.bloom_filter) |filter| return filter;
    // Cache-backed reads retain the table-index handle that owns the Bloom
    // filter. There is no per-run filter to materialize, so taking the backend
    // mutex here only to return null adds one contended lock acquisition per
    // key/run probe during batch reads.
    if (backend.options.cache != null or run.path == null) return null;

    const locked = lockBackend(@TypeOf(backend.*), backend);
    defer unlockBackend(@TypeOf(backend.*), backend, locked);
    return try ensureRunBloomFilterForReadLocked(backend, run, locked);
}

fn ensureRunBloomFilterForReadMaybeLocked(backend: anytype, run: *Run, backend_locked: bool) !?bloom.OwnedFilter {
    return ensureRunBloomFilterForReadMaybeLockedWithAdmission(backend, run, backend_locked, null);
}

fn ensureRunBloomFilterForReadMaybeLockedWithAdmission(
    backend: anytype,
    run: *Run,
    backend_locked: bool,
    control: ?read_admission.Port,
) !?bloom.OwnedFilter {
    const budget = control orelse {
        if (!backend_locked) return ensureRunBloomFilterForRead(backend, run);
        return ensureRunBloomFilterForReadLocked(backend, run, true);
    };
    try budget.checkpoint();
    if (run.shared_read_version) return null;
    if (run.bloom_filter) |filter| return filter;
    if (backend.options.cache != null or run.path == null) return null;
    const held = if (backend_locked) false else try lockBackendWithAdmission(@TypeOf(backend.*), backend, budget);
    defer unlockBackend(@TypeOf(backend.*), backend, held);
    return ensureRunBloomFilterForReadLockedWithAdmission(backend, run, true, budget);
}

fn ensureRunBloomFilterForReadLocked(backend: anytype, run: *Run, backend_locked: bool) !?bloom.OwnedFilter {
    return ensureRunBloomFilterForReadLockedWithAdmission(backend, run, backend_locked, null);
}

fn ensureRunBloomFilterForReadLockedWithAdmission(
    backend: anytype,
    run: *Run,
    backend_locked: bool,
    control: ?read_admission.Port,
) !?bloom.OwnedFilter {
    if (control) |budget| try budget.checkpoint();
    if (run.shared_read_version) return null;
    if (run.bloom_filter) |filter| return filter;

    if (@hasField(@TypeOf(backend.*), "runs")) {
        for (0..run_store.count(backend)) |rank| {
            if (control) |budget| try budget.checkpoint();
            const source_run = run_store.at(backend, rank);
            if (source_run.id != run.id) continue;
            if (!sameRunPath(source_run.path, run.path)) continue;

            const filter = try materializeRunBloomFilterForReadWithAdmission(backend, source_run, backend_locked, control);
            if (source_run != run) {
                run.bloom_filter = filter;
                run.owns_bloom_filter = false;
            }
            return filter;
        }
    }

    return try materializeRunBloomFilterForReadWithAdmission(backend, run, backend_locked, control);
}

fn materializeRunBloomFilterForRead(backend: anytype, run: *Run, backend_locked: bool) !?bloom.OwnedFilter {
    return materializeRunBloomFilterForReadWithAdmission(backend, run, backend_locked, null);
}

fn materializeRunBloomFilterForReadWithAdmission(
    backend: anytype,
    run: *Run,
    backend_locked: bool,
    control: ?read_admission.Port,
) !?bloom.OwnedFilter {
    if (run.bloom_filter) |filter| return filter;
    if (run.path == null) return null;

    // A shared-cache index already owns this filter. Cloning it into Run made
    // the duplicate live for the backend lifetime, outside cache eviction and
    // ResourceManager accounting. Cache-backed point-read paths retain an
    // index handle while probing the borrowed filter; cached full-state paths
    // may safely proceed without the optional Bloom precheck.
    if (backend.options.cache != null) return null;

    const index = try indexForRunNoCacheMaybeLockedWithAdmission(backend, run, backend_locked, control);
    var filter = try index.borrowFilter().clone(backend.allocator);
    errdefer filter.deinit(backend.allocator);
    if (control) |budget| try budget.checkpoint();
    run.bloom_filter = filter;
    run.owns_bloom_filter = true;
    return run.bloom_filter.?;
}

fn sameRunPath(lhs: ?[]const u8, rhs: ?[]const u8) bool {
    if (lhs == null and rhs == null) return true;
    if (lhs == null or rhs == null) return false;
    return std.mem.eql(u8, lhs.?, rhs.?);
}

fn runMayContain(run: Run, namespace: backend_types.Namespace, key: []const u8) bool {
    return compareRunBound(namespace.name, key, run.smallest_namespace_name, run.smallest_key) != .lt and
        compareRunBound(namespace.name, key, run.largest_namespace_name, run.largest_key) != .gt;
}

fn runMayContainAtOrAfter(run: Run, namespace: backend_types.Namespace, key: []const u8) bool {
    if (compareNamespace(namespace, .{ .name = run.smallest_namespace_name }) == .lt) {
        return compareNamespace(namespace, .{ .name = run.largest_namespace_name }) != .gt;
    }
    return compareRunBound(namespace.name, key, run.largest_namespace_name, run.largest_key) != .gt;
}

fn compareRunBound(lhs_namespace_name: ?[]const u8, lhs_key: []const u8, rhs_namespace_name: ?[]const u8, rhs_key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(.{ .name = lhs_namespace_name }, .{ .name = rhs_namespace_name });
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, lhs_key, rhs_key);
}

fn nextStateKey(state: *const State, namespace: backend_types.Namespace, target: []const u8, inclusive: bool) ?[]const u8 {
    var idx = state.lowerBound(namespace, target);
    while (idx < state.entryCount()) : (idx += 1) {
        const entry = state.entryAt(idx);
        if (compareNamespace(namespaceOf(entry), namespace) != .eq) return null;
        if (!inclusive and std.mem.eql(u8, entry.key, target)) continue;
        return entry.key;
    }
    return null;
}

fn nextStateIndex(state: anytype, namespace: backend_types.Namespace, target: []const u8, inclusive: bool) ?usize {
    const StateType = @TypeOf(state.*);
    if (if (comptime StateType == ActiveMemTable) !state.ordered_enabled else false) {
        var best: ?usize = null;
        for (state.entries.items, 0..) |entry, idx| {
            if (compareNamespace(namespaceOf(entry), namespace) != .eq) continue;
            switch (std.mem.order(u8, entry.key, target)) {
                .lt => continue,
                .eq => if (!inclusive) continue,
                .gt => {},
            }
            if (best == null or std.mem.order(u8, entry.key, state.entryAt(best.?).key) == .lt) {
                best = idx;
            }
        }
        return best;
    }

    var idx = state.lowerBound(namespace, target);
    while (idx < state.entryCount()) : (idx += 1) {
        const entry = state.entryAt(idx);
        if (compareNamespace(namespaceOf(entry), namespace) != .eq) return null;
        if (!inclusive and std.mem.eql(u8, entry.key, target)) continue;
        return idx;
    }
    return null;
}

fn nextIndexFrom(state: anytype, namespace: backend_types.Namespace, current: usize) ?usize {
    const StateType = @TypeOf(state.*);
    if (if (comptime StateType == ActiveMemTable) !state.ordered_enabled else false) {
        if (current >= state.entryCount()) return null;
        const current_entry = state.entryAt(current);
        var best: ?usize = null;
        for (state.entries.items, 0..) |entry, idx| {
            if (idx == current) continue;
            if (compareNamespace(namespaceOf(entry), namespace) != .eq) continue;
            if (std.mem.order(u8, entry.key, current_entry.key) != .gt) continue;
            if (best == null or std.mem.order(u8, entry.key, state.entryAt(best.?).key) == .lt) {
                best = idx;
            }
        }
        return best;
    }

    var idx = current + 1;
    while (idx < state.entryCount()) : (idx += 1) {
        if (compareNamespace(namespaceOf(state.entryAt(idx)), namespace) == .eq) return idx;
        if (compareNamespace(namespaceOf(state.entryAt(idx)), namespace) == .gt) return null;
    }
    return null;
}

fn prevStateKey(state: anytype, namespace: backend_types.Namespace, target: []const u8, inclusive: bool) ?[]const u8 {
    const StateType = @TypeOf(state.*);
    if (if (comptime StateType == ActiveMemTable) !state.ordered_enabled else false) {
        var best: ?[]const u8 = null;
        for (state.entries.items) |entry| {
            if (compareNamespace(namespaceOf(entry), namespace) != .eq) continue;
            switch (std.mem.order(u8, entry.key, target)) {
                .gt => continue,
                .eq => if (!inclusive) continue,
                .lt => {},
            }
            if (best == null or std.mem.order(u8, entry.key, best.?) == .gt) {
                best = entry.key;
            }
        }
        return best;
    }

    const idx = state.lowerBound(namespace, target);
    var probe: usize = if (idx < state.entryCount() and inclusive and compareEntryTo(state.entryAt(idx), namespace, target) == .eq)
        idx
    else if (idx > 0)
        idx - 1
    else
        return null;

    while (true) {
        const entry = state.entryAt(probe);
        if (compareNamespace(namespaceOf(entry), namespace) == .eq) {
            if (inclusive or !std.mem.eql(u8, entry.key, target)) return entry.key;
        } else if (compareNamespace(namespaceOf(entry), namespace) == .lt) {
            return null;
        }
        if (probe == 0) break;
        probe -= 1;
    }
    return null;
}

fn mutableLastKey(state: anytype, namespace: backend_types.Namespace) ?[]const u8 {
    if (state.entryCount() == 0) return null;
    var idx = state.entryCount();
    while (idx > 0) {
        idx -= 1;
        if (compareNamespace(namespaceOf(state.entryAt(idx)), namespace) == .eq) {
            return state.entryAt(idx).key;
        }
    }
    return null;
}

pub fn NamespaceWriteTxn(comptime BackendType: type) type {
    const LocalCursor = MergeCursor(BackendType, State);
    return struct {
        allocator: Allocator,
        metadata_allocator: Allocator,
        backend: *BackendType,
        mutable: ActiveMemTable,
        bulk_appends: State = .{},
        bulk_index: BulkAppendIndex = .{},
        prefix_index: WriterPrefixIndex = .{},
        cursor_overlay: ?State = null,
        cursor_base_mutable: ?MutableReadSnapshot = null,
        cursor_read_view: ?RunReadView = null,
        cursor_immutable_memtables: []const *const State = &.{},
        cursor_runs: []Run = &.{},
        cursor_l0_groups: []RunGroup = &.{},
        cursor_levels: []RunLevel = &.{},
        held_blocks: std.ArrayListUnmanaged(BlockPin) = .empty,
        held_values: PointResultValues = .empty,
        batch_scratch: BatchScratch.Scratch = .{},
        probe_scratch: BatchScratch.ProbeScratch = .{},
        batch_options: backend_types.BatchOptions = .{},
        cursor_reader_retained: bool = false,
        closed: bool = false,

        pub fn open(backend: *BackendType) !@This() {
            return try openWithOptions(backend, .{});
        }

        pub fn openWithOptions(backend: *BackendType, options: backend_types.BatchOptions) !@This() {
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            try retainReadReader(BackendType, backend, .write_txn);
            errdefer releaseWriteReader(BackendType, backend, .write_txn);
            backend.beginBatchMode(options);
            errdefer backend.finishBatchMode(options);
            return .{
                .allocator = backend.allocator,
                .metadata_allocator = runtimeScratchAllocator(backend.allocator),
                .backend = backend,
                .mutable = .{ .ordered_enabled = false },
                .batch_options = options,
            };
        }

        pub fn abort(self: *@This()) void {
            if (self.closed) return;
            const backend = self.backend;
            self.bulk_index.deinit(self.allocator);
            self.prefix_index.deinit(self.allocator);
            self.mutable.deinit(self.allocator);
            self.bulk_appends.deinit(self.allocator);
            self.invalidateCursorSnapshot();
            self.batch_scratch.deinit(self.metadata_allocator);
            self.probe_scratch.deinit(self.metadata_allocator);
            releaseHeldBlocks(&self.held_blocks, self.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            const locked = lockBackend(BackendType, backend);
            defer unlockBackend(BackendType, backend, locked);
            backend.finishBatchMode(self.batch_options);
            if (self.cursor_reader_retained) releaseWriteReader(BackendType, backend, .current_scan);
            releaseWriteReader(BackendType, backend, .write_txn);
            self.* = undefined;
        }

        pub fn commit(self: *@This()) !void {
            if (self.closed) return error.TransactionClosed;
            defer if (self.closed) {
                self.bulk_index.deinit(self.allocator);
                self.prefix_index.deinit(self.allocator);
                self.batch_scratch.deinit(self.metadata_allocator);
                self.probe_scratch.deinit(self.metadata_allocator);
            };
            const wire_credit = if (comptime @hasDecl(BackendType, "prepareManifestCredit")) try self.backend.prepareManifestCredit(&self.mutable, &self.bulk_appends) else 0;
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            var release_on_error = true;
            errdefer if (release_on_error) {
                self.mutable.deinit(self.allocator);
                self.mutable = .{};
                self.bulk_appends.deinit(self.allocator);
                self.bulk_appends = .{};
                self.invalidateCursorSnapshotLocked();
                releaseHeldBlocks(&self.held_blocks, self.allocator);
                releaseHeldValues(&self.held_values, self.allocator);
                self.backend.finishBatchMode(self.batch_options);
                if (self.cursor_reader_retained) releaseWriteReader(BackendType, self.backend, .current_scan);
                releaseWriteReader(BackendType, self.backend, .write_txn);
                self.closed = true;
            };
            const admission = if (comptime @hasDecl(BackendType, "admitPreparedCommit")) try self.backend.admitPreparedCommit(&self.mutable, &self.bulk_appends, wire_credit) else if (comptime @hasDecl(BackendType, "admitCommit")) try self.backend.admitCommit(&self.mutable, &self.bulk_appends) else {};
            defer if (comptime @hasDecl(BackendType, "admitCommit")) {
                if (comptime @hasDecl(BackendType, "admitPreparedCommit")) {
                    if ((self.mutable.entryCount() == 0 and self.bulk_appends.entryCount() == 0) or
                        (if (@hasField(BackendType, "manifest_recovery_required")) self.backend.manifest_recovery_required else false)) admission.retainDebt();
                }
                admission.release();
            };
            const direct_ingested_bulk_appends = try self.tryCommitDirectBulkAppends();
            var committed_write = direct_ingested_bulk_appends;
            const direct_ingested_bulk_state = try self.tryCommitDirectBulkIngest();
            if (!direct_ingested_bulk_state) {
                const mutated = self.mutable.entryCount() > 0;
                committed_write = committed_write or mutated;
                if (mutated) {
                    try enforceMutableWriteAdmission(self.backend, &self.mutable);
                    try prepareMutableForWrite(self.backend);
                }
                if (@hasDecl(BackendType, "appendWalForMutable")) {
                    try publishMutableWithWal(self.backend, self.allocator, &self.mutable);
                } else if (@hasDecl(BackendType, "appendWalForState")) {
                    var sorted = try self.mutable.toStateMove(self.allocator);
                    defer sorted.deinit(self.allocator);
                    try self.backend.appendWalForState(&sorted);
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyStateMoveToMutable(&self.backend.mutable, self.allocator, &sorted);
                } else {
                    if (@hasDecl(BackendType, "invalidateMutableReadSnapshot")) self.backend.invalidateMutableReadSnapshot();
                    try state_mod.applyMutableMoveToMutable(&self.backend.mutable, self.allocator, &self.mutable);
                }
                if (mutated or direct_ingested_bulk_appends) notePotentialMaintenanceDebtLocked(self.backend);
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
                if (!self.batch_options.defer_commit_flush) {
                    try self.backend.maybeFlushMutable();
                }
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            } else {
                committed_write = true;
                if (@hasDecl(BackendType, "syncTrackedInMemoryStateUsageCurrentLocked")) self.backend.syncTrackedInMemoryStateUsageCurrentLocked();
            }
            if (committed_write) finishCommittedWalAppend(self.backend);
            self.backend.finishBatchMode(self.batch_options);
            try self.backend.finalizeExitedBatchMode(self.batch_options);
            release_on_error = false;
            self.closed = true;
            self.invalidateCursorSnapshotLocked();
            releaseHeldBlocks(&self.held_blocks, self.allocator);
            releaseHeldValues(&self.held_values, self.allocator);
            var finalize_err: ?anyerror = null;
            if (self.cursor_reader_retained) {
                releaseWriteReader(BackendType, self.backend, .current_scan);
                self.cursor_reader_retained = false;
            }
            finalizeWriteReader(BackendType, self.backend, .write_txn) catch |err| {
                finalize_err = err;
            };
            if (finalize_err) |err| return err;
        }

        fn drainBulkAppendsToMutable(self: *@This()) !void {
            if (self.bulk_appends.entryCount() == 0) return;
            try state_mod.applyStateMoveToMutable(&self.mutable, self.allocator, &self.bulk_appends);
            self.bulk_index.clear();
        }

        fn tryCommitDirectBulkAppends(self: *@This()) !bool {
            const entries = self.bulk_appends.entryCount();
            if (entries == 0) return false;
            if (self.batch_options.mode != .bulk_ingest) {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackNonBulk")) self.backend.recordBulkAppendFallbackNonBulk(entries);
                try self.drainBulkAppendsToMutable();
                return false;
            }
            if (!@hasDecl(BackendType, "ingestSortedState") or !@hasDecl(BackendType, "shouldDirectIngestBulkState")) {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackUnsupported")) self.backend.recordBulkAppendFallbackUnsupported(entries);
                try self.drainBulkAppendsToMutable();
                return false;
            }
            const can_queue_pending_immutable = @hasDecl(BackendType, "canQueueDirectBulkStateWithPendingImmutable") and
                self.backend.canQueueDirectBulkStateWithPendingImmutable();
            if ((self.backend.mutable.entryCount() != 0 or
                (self.backend.activeImmutableMemtableCount() != 0 and !can_queue_pending_immutable)) and
                @hasDecl(BackendType, "drainMutableBeforeBulkAppendDirectIngest"))
            {
                if (!try self.backend.drainMutableBeforeBulkAppendDirectIngest()) {
                    if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                    if (@hasDecl(BackendType, "recordBulkAppendFallbackBackendPending")) self.backend.recordBulkAppendFallbackBackendPending(entries);
                    try self.drainBulkAppendsToMutable();
                    return false;
                }
            }
            if (self.backend.mutable.entryCount() != 0 or
                (self.backend.activeImmutableMemtableCount() != 0 and !can_queue_pending_immutable))
            {
                if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);
                if (@hasDecl(BackendType, "recordBulkAppendFallbackBackendPending")) self.backend.recordBulkAppendFallbackBackendPending(entries);
                try self.drainBulkAppendsToMutable();
                return false;
            }
            if (self.mutable.entryCount() > 0) {
                try self.drainBulkAppendsToMutable();
                return false;
            }
            if (@hasDecl(BackendType, "recordBulkAppendAttempt")) self.backend.recordBulkAppendAttempt(entries);

            const duplicate_check_start_ns = platform_time.monotonicNs();
            if (self.bulk_index.entries.count() != entries) {
                if (@hasDecl(BackendType, "recordBulkAppendFallbackDuplicateKeys")) self.backend.recordBulkAppendFallbackDuplicateKeys(entries, elapsedNs(duplicate_check_start_ns));
                try self.drainBulkAppendsToMutable();
                return false;
            }

            const sort_start_ns = platform_time.monotonicNs();
            state_mod.sortStateEntries(&self.bulk_appends);
            const sort_ns = elapsedNs(sort_start_ns);
            std.debug.assert(bulkStateEntriesAreUnique(&self.bulk_appends));
            if (!self.backend.shouldDirectIngestBulkState(&self.bulk_appends)) {
                if (@hasDecl(BackendType, "recordBulkAppendFallbackBelowThreshold")) self.backend.recordBulkAppendFallbackBelowThreshold(entries, sort_ns);
                try self.drainBulkAppendsToMutable();
                return false;
            }

            try enforceSortedWriteAdmission(self.backend, &self.bulk_appends);
            if (@hasDecl(BackendType, "appendWalForState")) try self.backend.appendWalForState(&self.bulk_appends);
            errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
            const queued = if (@hasDecl(BackendType, "enqueueOwnedSortedStateForFlush"))
                try self.backend.enqueueOwnedSortedStateForFlush(&self.bulk_appends)
            else
                false;
            if (!queued and @hasDecl(BackendType, "ingestOwnedSortedState")) {
                try self.backend.ingestOwnedSortedState(&self.bulk_appends);
            } else if (!queued) {
                try self.backend.ingestSortedState(&self.bulk_appends);
            }
            if (@hasDecl(BackendType, "recordBulkAppendSuccess")) self.backend.recordBulkAppendSuccess(entries, sort_ns);
            self.bulk_appends.deinit(self.allocator);
            self.bulk_appends = .{};
            return true;
        }

        fn bulkStateEntriesAreUnique(state: *const State) bool {
            if (state.entryCount() <= 1) return true;
            var cursor: State.EntryCursor = .{};
            var previous = cursor.at(state, 0);
            for (1..state.entryCount()) |i| {
                const entry = cursor.at(state, i);
                if (compareEntryTo(previous, state_mod.namespaceOf(entry), entry.key) == .eq) return false;
                previous = entry;
            }
            return true;
        }

        fn tryCommitDirectBulkIngest(self: *@This()) !bool {
            if (self.batch_options.mode != .bulk_ingest) return false;
            const entries = self.mutable.entryCount();
            if (entries == 0) return false;
            if (@hasDecl(BackendType, "recordDirectBulkIngestAttempt")) self.backend.recordDirectBulkIngestAttempt(entries);
            if (!@hasDecl(BackendType, "ingestSortedState") or !@hasDecl(BackendType, "shouldDirectIngestBulkState")) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackUnsupported")) self.backend.recordDirectBulkIngestFallbackUnsupported();
                return false;
            }
            if ((self.backend.mutable.entryCount() != 0 or self.backend.activeImmutableMemtableCount() != 0) and
                @hasDecl(BackendType, "shouldDrainMutableBeforeDirectBulkIngest") and
                self.backend.shouldDrainMutableBeforeDirectBulkIngest(&self.mutable) and
                @hasDecl(BackendType, "directIngestCombinedMutable"))
            {
                try enforceMutableWriteAdmission(self.backend, &self.mutable);
                if (@hasDecl(BackendType, "appendWalForMutable")) try self.backend.appendWalForMutable(&self.mutable);
                errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
                const sort_start_ns = platform_time.monotonicNs();
                if (!try self.backend.directIngestCombinedMutable(&self.mutable)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBackendMutable")) self.backend.recordDirectBulkIngestFallbackBackendMutable();
                    return false;
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, elapsedNs(sort_start_ns));
                notePotentialMaintenanceDebtLocked(self.backend);
                return true;
            }
            if (self.backend.mutable.entryCount() != 0) {
                if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBackendMutable")) self.backend.recordDirectBulkIngestFallbackBackendMutable();
                return false;
            }
            if (@hasDecl(BackendType, "shouldDirectIngestBulkMutable")) {
                if (!self.backend.shouldDirectIngestBulkMutable(&self.mutable)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    return false;
                }
                try enforceMutableWriteAdmission(self.backend, &self.mutable);
                if (@hasDecl(BackendType, "appendWalForMutable")) try self.backend.appendWalForMutable(&self.mutable);
                errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.toStateMove(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                const queued = if (@hasDecl(BackendType, "enqueueOwnedSortedStateForFlush"))
                    try self.backend.enqueueOwnedSortedStateForFlush(&sorted)
                else
                    false;
                if (!queued and @hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else if (!queued) {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            } else {
                const sort_start_ns = platform_time.monotonicNs();
                var sorted = try self.mutable.clone(self.allocator);
                errdefer sorted.deinit(self.allocator);
                const sort_ns = elapsedNs(sort_start_ns);
                if (!self.backend.shouldDirectIngestBulkState(&sorted)) {
                    if (@hasDecl(BackendType, "recordDirectBulkIngestFallbackBelowThreshold")) self.backend.recordDirectBulkIngestFallbackBelowThreshold();
                    sorted.deinit(self.allocator);
                    return false;
                }
                try enforceSortedWriteAdmission(self.backend, &sorted);
                if (@hasDecl(BackendType, "appendWalForState")) try self.backend.appendWalForState(&sorted);
                errdefer if (@hasDecl(BackendType, "fenceFailedBulkWal")) self.backend.fenceFailedBulkWal();
                const queued = if (@hasDecl(BackendType, "enqueueOwnedSortedStateForFlush"))
                    try self.backend.enqueueOwnedSortedStateForFlush(&sorted)
                else
                    false;
                if (!queued and @hasDecl(BackendType, "ingestOwnedSortedState")) {
                    try self.backend.ingestOwnedSortedState(&sorted);
                } else if (!queued) {
                    try self.backend.ingestSortedState(&sorted);
                }
                if (@hasDecl(BackendType, "recordDirectBulkIngestSuccess")) self.backend.recordDirectBulkIngestSuccess(entries, sort_ns);
                sorted.deinit(self.allocator);
            }
            self.mutable.deinit(self.allocator);
            self.mutable = .{};
            notePotentialMaintenanceDebtLocked(self.backend);
            return true;
        }

        pub fn get(self: *@This(), namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
            if (self.closed) return error.TransactionClosed;
            if (self.bulk_index.get(&self.bulk_appends, namespace, key)) |entry| {
                if (entry.tombstone) return error.NotFound;
                return entry.value;
            }
            if (self.mutable.findIndex(namespace, key)) |idx| {
                const entry = self.mutable.entryAt(idx);
                if (entry.tombstone) return error.NotFound;
                return entry.value;
            }
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            if (comptime @hasField(BackendType, "runs") and @hasField(BackendType, "immutable_memtables")) {
                self.backend.recordPointGets(1);
                return try getCurrentPointRetainedLocked(BackendType, self.backend, namespace, self.allocator, null, &self.held_values, key) orelse error.NotFound;
            }
            return self.backend.getMergedWithOverlay(&self.backend.mutable, &self.mutable, namespace, key);
        }

        /// Resolve a sorted batch against the transaction overlay first, then
        /// probe the current durable view once for all misses. Keeping this on
        /// the namespace write transaction is important for HBC mutations:
        /// split maintenance often needs dozens of sibling range records, and
        /// scalar `get` calls otherwise reread and decompress the same table
        /// block for every child.
        pub fn getManySorted(
            self: *@This(),
            namespace: backend_types.Namespace,
            keys: []const []const u8,
            values: []?[]const u8,
        ) !void {
            if (self.closed) return error.TransactionClosed;
            if (keys.len != values.len) return error.InvalidBatch;
            @memset(values, null);

            const Scratch = @import("write_batch_scratch.zig").Scratch;
            var oversized: Scratch = .{};
            defer oversized.deinit(self.metadata_allocator);
            const scratch = if (keys.len <= Scratch.max_retained_keys) &self.batch_scratch else &oversized;
            try scratch.prepareKeys(self.metadata_allocator, keys.len);
            const miss_keys = scratch.keys.items;
            const miss_indexes = scratch.indexes.items;

            var miss_count: usize = 0;
            var overlay_point_gets: usize = 0;
            for (keys, 0..) |key, i| {
                if (self.bulk_index.get(&self.bulk_appends, namespace, key)) |entry| {
                    overlay_point_gets += 1;
                    if (!entry.tombstone) values[i] = entry.value;
                } else if (self.mutable.findIndex(namespace, key)) |idx| {
                    overlay_point_gets += 1;
                    const entry = self.mutable.entryAt(idx);
                    if (!entry.tombstone) values[i] = entry.value;
                } else {
                    miss_keys[miss_count] = key;
                    miss_indexes[miss_count] = i;
                    miss_count += 1;
                }
            }
            self.backend.recordPointGets(overlay_point_gets);
            if (miss_count == 0) return;

            try scratch.prepareValues(self.metadata_allocator, miss_count);
            const miss_values = scratch.values.items[0..miss_count];
            var probe = try BoundProbeTxn(BackendType).open(self.backend, namespace);
            probe.metadata_allocator = self.metadata_allocator;
            probe.borrowed_batch_scratch = &self.probe_scratch;
            // Preserve the transaction-wide unique pin budget across probes.
            // All owned probe results use the writer's allocator, so their
            // allocations can transfer directly without a namespace copy.
            probe.allocator = self.allocator;
            probe.namespace.borrow_local_point_results = true;
            probe.held_blocks = self.held_blocks;
            self.held_blocks = .empty;
            defer {
                self.held_blocks = probe.held_blocks;
                probe.held_blocks = .empty;
                probe.abort();
            }
            try probe.getManySorted(miss_keys[0..miss_count], miss_values);
            try self.held_values.appendSlice(self.allocator, probe.held_values.items);
            probe.held_values.deinit(self.allocator);
            probe.held_values = .empty;
            for (miss_values, 0..) |value, miss_index| values[miss_indexes[miss_index]] = value;
        }

        pub fn put(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            if (self.closed) return error.TransactionClosed;
            try self.drainBulkAppendsToMutable();
            try self.mutable.upsert(self.allocator, namespace, key, value, false);
            self.invalidateCursorSnapshot();
            self.prefix_index.record(self.allocator, namespace, key, false);
        }

        pub fn appendPut(self: *@This(), namespace: backend_types.Namespace, key: []const u8, value: []const u8) !void {
            if (self.closed) return error.TransactionClosed;
            if (self.batch_options.mode == .bulk_ingest and self.mutable.entryCount() == 0) {
                try self.bulk_index.append(self.allocator, &self.bulk_appends, namespace, key, value);
                self.invalidateCursorSnapshot();
                self.prefix_index.record(self.allocator, namespace, key, false);
                return;
            }
            try self.mutable.appendUpsert(self.allocator, namespace, key, value, false);
            self.invalidateCursorSnapshot();
            self.prefix_index.record(self.allocator, namespace, key, false);
        }

        pub fn delete(self: *@This(), namespace: backend_types.Namespace, key: []const u8) !void {
            if (self.closed) return error.TransactionClosed;
            try self.drainBulkAppendsToMutable();
            try self.mutable.upsert(self.allocator, namespace, key, "", true);
            self.invalidateCursorSnapshot();
            self.prefix_index.record(self.allocator, namespace, key, true);
        }

        pub fn hasPrefix(self: *@This(), namespace: backend_types.Namespace, prefix: []const u8) !bool {
            if (self.closed) return error.TransactionClosed;
            return writerHasPrefix(BackendType, self, namespace, prefix);
        }

        pub fn openCursor(self: *@This(), namespace: backend_types.Namespace) !LocalCursor {
            if (self.closed) return error.TransactionClosed;
            try self.ensureCursorSnapshot();
            const cursor_alloc = runtimeScratchAllocator(self.allocator);
            return try LocalCursor.initView(cursor_alloc, self.backend, &self.cursor_overlay.?, self.cursor_immutable_memtables, self.cursor_read_view.?, namespace, false);
        }

        fn ensureCursorSnapshot(self: *@This()) !void {
            if (self.cursor_overlay != null) return;
            try self.drainBulkAppendsToMutable();
            try self.mutable.enableOrdered(self.allocator);
            var overlay = try self.mutable.snapshot(self.allocator);
            errdefer overlay.deinit(self.allocator);
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);

            var retained_now = false;
            if (!self.cursor_reader_retained) {
                try retainReadReader(BackendType, self.backend, .current_scan);
                self.cursor_reader_retained = true;
                retained_now = true;
            }
            errdefer if (retained_now) {
                releaseWriteReader(BackendType, self.backend, .current_scan);
                self.cursor_reader_retained = false;
            };

            const base_mutable = try snapshotReadMutable(BackendType, self.backend, .current_scan);
            errdefer base_mutable.release(self.backend);

            const backend_immutable = if (@hasDecl(BackendType, "snapshotImmutableMemtables"))
                try self.backend.snapshotImmutableMemtables()
            else
                &.{};
            var backend_immutable_pins_transferred = false;
            defer if (backend_immutable.len > 0) {
                if (backend_immutable_pins_transferred)
                    self.allocator.free(backend_immutable)
                else
                    releaseImmutableMemtableSnapshotList(BackendType, self.backend, backend_immutable);
            };

            const immutable = try self.allocator.alloc(*const State, 1 + backend_immutable.len);
            errdefer self.allocator.free(immutable);
            for (backend_immutable, 0..) |state, i| immutable[i + 1] = state;

            var read_view = try RunReadView.pin(self.backend, self.metadata_allocator);
            errdefer read_view.release(self.backend);
            try read_view.prepareCursor(self.backend);

            self.cursor_overlay = overlay;
            self.cursor_base_mutable = base_mutable;
            immutable[0] = base_mutable.state;
            backend_immutable_pins_transferred = true;
            self.cursor_immutable_memtables = immutable;
            self.cursor_read_view = read_view;
            self.cursor_runs = read_view.runs;
            self.cursor_l0_groups = read_view.l0_groups;
            self.cursor_levels = read_view.levels;
        }

        fn invalidateCursorSnapshot(self: *@This()) void {
            const locked = lockBackend(BackendType, self.backend);
            defer unlockBackend(BackendType, self.backend, locked);
            self.invalidateCursorSnapshotLocked();
        }

        fn invalidateCursorSnapshotLocked(self: *@This()) void {
            if (self.cursor_overlay) |*state| {
                state.deinit(self.allocator);
                self.cursor_overlay = null;
            }
            if (self.cursor_base_mutable) |snapshot| {
                snapshot.release(self.backend);
                self.cursor_base_mutable = null;
            }
            if (self.cursor_immutable_memtables.len > 0) {
                releaseImmutableMemtablePins(BackendType, self.backend, self.cursor_immutable_memtables[1..]);
                self.allocator.free(self.cursor_immutable_memtables);
                self.cursor_immutable_memtables = &.{};
            }
            if (self.cursor_read_view) |view| view.release(self.backend);
            self.cursor_read_view = null;
            self.cursor_l0_groups = &.{};
            self.cursor_levels = &.{};
            self.cursor_runs = &.{};
        }
    };
}

test "lsm namespace write txn keeps merged mutable state when flush fails after ownership transfer" {
    const TestBackend = struct {
        allocator: Allocator,
        mu: std.atomic.Mutex = .unlocked,
        mutable: State = .{},
        retained_readers: usize = 0,
        active_batches: usize = 0,

        pub fn retainReader(self: *@This()) void {
            self.retained_readers += 1;
        }

        pub fn releaseReader(self: *@This()) void {
            std.debug.assert(self.retained_readers > 0);
            self.retained_readers -= 1;
        }

        fn beginBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            self.active_batches += 1;
        }

        fn finishBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            std.debug.assert(self.active_batches > 0);
            self.active_batches -= 1;
        }

        fn maybeFlushMutable(_: *@This()) !void {
            return error.InjectedFlushFailure;
        }

        fn finalizeExitedBatchMode(_: *@This(), _: backend_types.BatchOptions) !void {}

        pub fn finalizeWriteReaderRelease(_: *@This()) !void {}

        fn getMergedWithOverlay(
            _: *@This(),
            backend_mutable: *const State,
            overlay: *const State,
            namespace: backend_types.Namespace,
            key: []const u8,
        ) ![]const u8 {
            if (overlay.get(namespace, key)) |value| return value else |_| {}
            return backend_mutable.get(namespace, key);
        }
    };

    var backend = TestBackend{
        .allocator = std.testing.allocator,
    };
    defer {
        backend.mutable.deinit(std.testing.allocator);
        std.testing.expectEqual(@as(usize, 0), backend.retained_readers) catch unreachable;
        std.testing.expectEqual(@as(usize, 0), backend.active_batches) catch unreachable;
    }

    var txn = try NamespaceWriteTxn(TestBackend).open(&backend);
    errdefer txn.abort();
    try txn.put(.{ .name = "docs" }, "doc:a", "A");
    try std.testing.expectError(error.InjectedFlushFailure, txn.commit());

    try std.testing.expectEqual(@as(usize, 1), backend.mutable.entryCount());
    try std.testing.expectEqualStrings("docs", backend.mutable.entryAt(0).namespace_name.?);
    try std.testing.expectEqualStrings("doc:a", backend.mutable.entryAt(0).key);
    try std.testing.expectEqualStrings("A", backend.mutable.entryAt(0).value);
    try std.testing.expect(txn.closed);
}

test "lsm namespace write txn releases local mutable state when wal append fails" {
    const TestBackend = struct {
        allocator: Allocator,
        mu: std.atomic.Mutex = .unlocked,
        mutable: State = .{},
        retained_readers: usize = 0,
        active_batches: usize = 0,

        pub fn retainReader(self: *@This()) void {
            self.retained_readers += 1;
        }

        pub fn releaseReader(self: *@This()) void {
            std.debug.assert(self.retained_readers > 0);
            self.retained_readers -= 1;
        }

        fn beginBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            self.active_batches += 1;
        }

        fn finishBatchMode(self: *@This(), _: backend_types.BatchOptions) void {
            std.debug.assert(self.active_batches > 0);
            self.active_batches -= 1;
        }

        pub fn appendWalForState(_: *@This(), _: *const State) !void {
            return error.InjectedWalFailure;
        }

        fn maybeFlushMutable(_: *@This()) !void {}

        fn finalizeExitedBatchMode(_: *@This(), _: backend_types.BatchOptions) !void {}

        pub fn finalizeWriteReaderRelease(_: *@This()) !void {}
    };

    var backend = TestBackend{
        .allocator = std.testing.allocator,
    };
    defer {
        backend.mutable.deinit(std.testing.allocator);
        std.testing.expectEqual(@as(usize, 0), backend.retained_readers) catch unreachable;
        std.testing.expectEqual(@as(usize, 0), backend.active_batches) catch unreachable;
    }

    var txn = try NamespaceWriteTxn(TestBackend).open(&backend);
    try txn.put(.{ .name = "docs" }, "doc:a", "A");
    try std.testing.expectError(error.InjectedWalFailure, txn.commit());

    try std.testing.expect(txn.closed);
    try std.testing.expectEqual(@as(usize, 0), txn.mutable.entryCount());
    try std.testing.expectEqual(@as(usize, 0), backend.mutable.entryCount());
}

test "lsm merge cursor frees loaded blocks with backend allocator" {
    const TestBackend = struct {
        allocator: Allocator,
    };

    const Cursor = MergeCursor(TestBackend, State);
    const cursor_alloc = std.heap.c_allocator;
    const backend_alloc = std.heap.page_allocator;

    var backend = TestBackend{ .allocator = backend_alloc };
    const positions = try cursor_alloc.alloc(?usize, 1);
    defer cursor_alloc.free(positions);
    positions[0] = null;

    const source_entries = try cursor_alloc.alloc(?Cursor.SourceEntry, 1);
    defer cursor_alloc.free(source_entries);
    source_entries[0] = null;

    const source_blocks = try cursor_alloc.alloc(SourceBlockLease, 1);
    defer cursor_alloc.free(source_blocks);
    source_blocks[0] = .{ .owned = .{ .allocator = backend_alloc, .bytes = try backend_alloc.alloc(u8, 4096) } };

    const source_block_indices = try cursor_alloc.alloc(?usize, 1);
    defer cursor_alloc.free(source_block_indices);
    source_block_indices[0] = 0;
    const source_table_indices = try cursor_alloc.alloc(?*const lsm_table_file.TableIndex, 1);
    defer cursor_alloc.free(source_table_indices);
    source_table_indices[0] = null;
    const advance_sources = try cursor_alloc.alloc(usize, 1);
    defer cursor_alloc.free(advance_sources);
    const source_heap = try cursor_alloc.alloc(usize, 1);
    defer cursor_alloc.free(source_heap);
    const source_heap_positions = try cursor_alloc.alloc(?usize, 1);
    defer cursor_alloc.free(source_heap_positions);
    source_heap_positions[0] = null;

    var cursor = Cursor{
        .allocator = cursor_alloc,
        .backend = &backend,
        .mutable = undefined,
        .immutable_memtables = &.{},
        .runs = &.{},
        .l0_groups = &.{},
        .levels = &.{},
        .namespace = .{ .name = "docs" },
        .positions = positions,
        .source_entries = source_entries,
        .source_blocks = source_blocks,
        .source_block_indices = source_block_indices,
        .source_table_indices = source_table_indices,
        .source_table_index_handles = &.{},
        .advance_sources = advance_sources,
        .source_heap = source_heap,
        .source_heap_positions = source_heap_positions,
    };

    cursor.clearSourceBlock(0);
    try std.testing.expectEqual(SourceBlockLease.none, cursor.source_blocks[0]);
    try std.testing.expectEqual(@as(?usize, null), cursor.source_block_indices[0]);
}

test "lsm bounded merge cursor spills inactive persisted block" {
    const TestBackend = struct {
        allocator: Allocator,
    };
    const Cursor = MergeCursor(TestBackend, State);

    var backend = TestBackend{ .allocator = std.testing.allocator };
    var mutable: State = .{};
    defer mutable.deinit(std.testing.allocator);
    var path = [_]u8{'r'};
    var empty = [_]u8{};
    var runs = [_]Run{.{
        .id = 1,
        .level = 0,
        .size_bytes = 4096,
        .path = path[0..],
        .smallest_namespace_name = @constCast("docs"),
        .smallest_key = empty[0..],
        .largest_namespace_name = @constCast("docs"),
        .largest_key = empty[0..],
        .entry_count = 1,
        .bloom_filter = null,
        .owns_metadata = false,
        .owns_path = false,
        .owns_bloom_filter = false,
        .state = null,
    }};
    var cursor = try Cursor.init(
        std.testing.allocator,
        &backend,
        &mutable,
        &.{},
        runs[0..],
        &.{},
        &.{},
        .{ .name = "docs" },
        false,
    );
    defer cursor.close();
    cursor.boundPersistedRunBlockResidency();

    const run_source = cursor.runSourceOffset();
    cursor.positions[run_source] = 0;
    cursor.source_entries[run_source] = .{
        .namespace_name = "docs",
        .key = "replay:1",
        .value = "payload",
        .tombstone = false,
    };
    cursor.source_blocks[run_source] = .{ .owned = .{
        .allocator = std.testing.allocator,
        .bytes = try std.testing.allocator.alloc(u8, 4096),
    } };
    cursor.source_block_indices[run_source] = 0;
    cursor.resident_run_source = run_source;

    try cursor.spillRunSource(run_source);
    try std.testing.expectEqual(@as(?usize, null), cursor.resident_run_source);
    try std.testing.expectEqual(SourceBlockLease.none, cursor.source_blocks[run_source]);
    try std.testing.expectEqual(@as(?usize, null), cursor.source_block_indices[run_source]);
    try std.testing.expectEqualStrings("replay:1", cursor.source_key_copies[run_source].?);
    try std.testing.expectEqual(@as(usize, 0), cursor.source_entries[run_source].?.value.len);
}

test "graph metric batch presence uses directory backed runs without a flat projection" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();
    // No block cache: indexForRunNoCache must acquire its own backend lock.
    // The existence probe must not hold that lock across directory/SST I/O.
    var backend = try Backend.open(allocator, "/graph-directory-presence", .{ .storage = storage.storage(), .flush_threshold = 1 });
    defer backend.close();
    var store = try backend.runtimeStore(allocator, .{ .name = "graph" });
    defer store.deinit();
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("persisted", "node");
        try write.commit();
    }
    while (try backend.runMaintenanceStep()) {}
    try std.testing.expect(run_store.count(&backend) != 0);
    var batch = try store.beginBatch();
    defer batch.abort();
    var present: [2]bool = undefined;
    try batch.containsManySorted(&.{ "missing", "persisted" }, &present);
    try std.testing.expectEqualSlices(bool, &.{ false, true }, &present);
    const version = backend.read_version orelse return error.MissingReadVersion;
    try std.testing.expect(version.directory != null);
    try std.testing.expectEqual(@as(usize, 0), version.runs.len);
    try batch.delete("persisted");
    try batch.put("missing", "new node");
    try batch.containsManySorted(&.{ "missing", "persisted" }, &present);
    try std.testing.expectEqualSlices(bool, &.{ true, false }, &present);
}

test "lsm async batch reads tree backed mutable and immutable snapshots" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();
    var backend = try Backend.open(allocator, "/async-tree-snapshot", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    var active: ActiveMemTable = .{};
    defer active.deinit(allocator);
    try active.upsert(allocator, .{}, "a", "old", false);
    try active.upsert(allocator, .{}, "b", "", true);
    var snapshot = try active.snapshot(allocator);
    defer snapshot.deinit(allocator);
    try std.testing.expect(snapshot.ordered_root != null);
    try std.testing.expectEqual(@as(usize, 0), snapshot.entries.items.len);
    try active.upsert(allocator, .{}, "a", "new", false);
    const keys = [_][]const u8{ "a", "b", "missing" };
    const empty: State = .{};
    for (0..3) |mode| {
        var values: [3]?[]const u8 = undefined;
        var held: PointResultValues = .empty;
        defer {
            for (held.items) |value| allocator.free(value);
            held.deinit(allocator);
        }
        @memset(&values, null);
        const result = if (mode == 0)
            try readManySortedPointFromSnapshotAsync(&backend, &active, &.{}, &.{}, &.{}, &.{}, allocator, &held, .{}, &keys, &values, false, .snapshot_pinned, null)
        else
            try readManySortedPointFromSnapshotAsync(&backend, if (mode == 1) &snapshot else &empty, if (mode == 1) &.{} else &.{&snapshot}, &.{}, &.{}, &.{}, allocator, &held, .{}, &keys, &values, false, .snapshot_pinned, null);
        try std.testing.expect(result != null);
        try std.testing.expectEqual(@as(usize, 1), result.?.hits);
        try std.testing.expectEqual(@as(usize, 2), result.?.misses);
        try std.testing.expectEqualStrings(if (mode == 0) "new" else "old", values[0].?);
        try std.testing.expect(values[1] == null and values[2] == null);
    }
    try std.testing.expect(!try bulkStateHasDuplicateKeys(allocator, &snapshot));
}

test "lsm async batch result lifetimes preserve borrowing and unwind owned allocation failures" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const Fixture = struct {
        fn check(allocator: Allocator, backend: *Backend, snapshot: *const State, immutable: bool, lifetime: PointResultLifetime) !void {
            var held: PointResultValues = .empty;
            defer releaseHeldValues(&held, allocator);
            const keys = [_][]const u8{ "a", "a", "b", "c", "missing" };
            var values: [keys.len]?[]const u8 = @splat(null);
            const empty: State = .{};
            const before = backend.snapshotReadStats().point_value_copies;
            const result = (try readManySortedPointFromSnapshotAsync(backend, if (immutable) &empty else snapshot, if (immutable) &.{snapshot} else &.{}, &.{}, &.{}, &.{}, allocator, &held, .{}, &keys, &values, false, lifetime, null)).?;
            try std.testing.expectEqual(@as(usize, 3), result.hits);
            try std.testing.expectEqual(@as(usize, 2), result.misses);
            try std.testing.expectEqualStrings("old", values[0].?);
            try std.testing.expectEqualStrings("old", values[1].?);
            try std.testing.expect(values[2] == null and values[4] == null);
            try std.testing.expectEqualStrings("", values[3].?);
            const original = snapshot.entryAt(snapshot.findIndex(.{}, "a").?).value;
            if (lifetime == .snapshot_pinned) {
                try std.testing.expect(values[0].?.ptr == original.ptr);
                try std.testing.expectEqual(@as(usize, 0), held.items.len);
                try std.testing.expectEqual(before, backend.snapshotReadStats().point_value_copies);
            } else {
                try std.testing.expect(values[0].?.ptr != original.ptr);
                try std.testing.expectEqual(@as(usize, 1), held.items.len);
                try std.testing.expect(@intFromPtr(values[0].?.ptr) >= @intFromPtr(held.items[0].ptr));
                try std.testing.expect(@intFromPtr(values[1].?.ptr) >= @intFromPtr(held.items[0].ptr));
                try std.testing.expect(@intFromPtr(values[1].?.ptr) + values[1].?.len <= @intFromPtr(held.items[0].ptr) + held.items[0].len);
                try std.testing.expectEqual(before + 3, backend.snapshotReadStats().point_value_copies);
            }
        }
    };
    const allocator = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(allocator);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();
    var backend = try Backend.open(allocator, "/async-result-lifetimes", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    var active: ActiveMemTable = .{};
    defer active.deinit(allocator);
    try active.upsert(allocator, .{}, "a", "old", false);
    try active.upsert(allocator, .{}, "b", "", true);
    try active.upsert(allocator, .{}, "c", "", false);
    var snapshot = try active.snapshot(allocator);
    defer snapshot.deinit(allocator);
    for ([_]bool{ false, true }) |immutable| {
        for ([_]PointResultLifetime{ .snapshot_pinned, .transaction_owned }) |lifetime|
            try std.testing.checkAllAllocationFailures(allocator, Fixture.check, .{ &backend, &snapshot, immutable, lifetime });
    }
}

test "lsm point result lifetime adopts decoded interior slices without another allocation" {
    const Fixture = struct {
        fn check(allocator: Allocator) !void {
            var backend = @import("../lsm_backend.zig").Backend.init(allocator, .{});
            defer backend.close();
            var held: PointResultValues = .empty;
            defer releaseHeldValues(&held, allocator);
            const decoded = try allocator.alloc(u8, 8192);
            errdefer if (held.items.len == 0) allocator.free(decoded);
            try held.append(allocator, decoded);
            @memset(decoded, 'x');
            const value = decoded[32..8000];
            const adopted = try PointResultLifetime.transaction_owned.retain(&backend, allocator, &held, 0, value);
            try std.testing.expect(adopted.ptr == value.ptr);
            try std.testing.expectEqual(@as(usize, 1), held.items.len);
            try std.testing.expectEqual(@as(u64, 0), backend.snapshotReadStats().point_value_copies);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.check, .{});
}

test "lsm async point read cleanup preserves independently retained index pin" {
    const allocator = std.testing.allocator;
    const path = "async-point-index";
    const entries = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "doc:a", .value = "alpha" },
    };

    const encoded = try lsm_table_file.encodeAlloc(allocator, &entries);
    defer allocator.free(encoded);

    var index = try lsm_table_file.decodeIndexAlloc(allocator, encoded);
    var index_owned = true;
    errdefer if (index_owned) index.deinit(allocator);

    var cache = cache_mod.Cache.init(allocator, 1024 * 1024);
    defer cache.deinit();

    var read_handle = try cache.putRunTableIndex(path, 1, 1, index);
    index_owned = false;
    var independent_handle = read_handle.retain();

    var read = AsyncPointBlockRead{
        .candidate = .{ .run_index = 0 },
        .path = path,
        .run_id = 1,
        .generation = 1,
        .index_handle = read_handle,
        .block_index = 0,
        .absolute_offset = 0,
        .physical_len = 0,
        .logical_len = 0,
        .compression = .none,
        .checksum = 0,
        .status = .known_miss,
    };

    cache.invalidatePrefix(path);
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());

    read.release();
    read.release();
    try std.testing.expectEqual(@as(usize, 1), cache.entryCount());

    independent_handle.release();
    try std.testing.expectEqual(@as(usize, 0), cache.entryCount());
}

test "lsm merge cursor caps retained mutable source scratch" {
    const TestBackend = struct {
        allocator: Allocator,
        mutable: ActiveMemTable = .{},
    };

    const Cursor = MergeCursor(TestBackend, ActiveMemTable);

    var backend = TestBackend{ .allocator = std.testing.allocator };
    defer backend.mutable.deinit(std.testing.allocator);

    var cursor = try Cursor.init(
        std.testing.allocator,
        &backend,
        &backend.mutable,
        &.{},
        &.{},
        &.{},
        &.{},
        .{ .name = "docs" },
        true,
    );
    defer cursor.close();

    const oversized_needed = Cursor.default_max_retained_mutable_source_entry_scratch + 128;
    _ = try cursor.mutableSourceEntryScratch(oversized_needed);
    try std.testing.expectEqual(oversized_needed, cursor.mutable_source_entry_bytes.?.len);

    _ = try cursor.mutableSourceEntryScratch(32);
    try std.testing.expectEqual(Cursor.min_retained_mutable_source_entry_scratch, cursor.mutable_source_entry_bytes.?.len);

    _ = try cursor.mutableSourceEntryScratch(Cursor.min_retained_mutable_source_entry_scratch + 1);
    try std.testing.expectEqual(Cursor.min_retained_mutable_source_entry_scratch * 2, cursor.mutable_source_entry_bytes.?.len);
}

test "bulk append index preserves namespaces duplicates and allocation failure atomicity" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var state: State = .{};
            defer state.deinit(alloc);
            var index: BulkAppendIndex = .{};
            defer index.deinit(alloc);
            try index.append(alloc, &state, .{}, "key", "first");
            try index.append(alloc, &state, .{ .name = "other" }, "key", "other");
            index.append(alloc, &state, .{}, "key", "last") catch |err| {
                try std.testing.expectEqualStrings("first", index.get(&state, .{}, "key").?.value);
                try std.testing.expectEqualStrings("other", index.get(&state, .{ .name = "other" }, "key").?.value);
                return err;
            };
            try std.testing.expectEqualStrings("last", index.get(&state, .{}, "key").?.value);
            try std.testing.expectEqualStrings("other", index.get(&state, .{ .name = "other" }, "key").?.value);
            try std.testing.expect(index.get(&state, .{}, "missing") == null);
            index.clear();
            try std.testing.expect(index.get(&state, .{}, "key") == null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "bulk append index prefix cache releases partial allocations and invalidates failed updates" {
    const Fixture = struct {
        fn run(alloc: Allocator) !void {
            var mutable: ActiveMemTable = .{ .ordered_enabled = false };
            defer mutable.deinit(alloc);
            var bulk: State = .{};
            defer bulk.deinit(alloc);
            var prefix: WriterPrefixIndex = .{};
            defer prefix.deinit(alloc);
            try mutable.upsert(alloc, .{}, "a", "old", false);
            _ = try prefix.ensure(alloc, &mutable, &bulk);
            try mutable.upsert(alloc, .{}, "b", "new", false);
            prefix.record(alloc, .{}, "b", false);
            try std.testing.expectEqualStrings("new", try mutable.get(.{}, "b"));
            const index = try prefix.ensure(alloc, &mutable, &bulk);
            try std.testing.expect(index.findIndex(.{}, "a") != null);
            try std.testing.expect(index.findIndex(.{}, "b") != null);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "lsm local point result copies selected row instead of decoded block" {
    const a = std.testing.allocator;
    const block = try a.alloc(u8, 32 * 1024);
    defer a.free(block);
    @memset(block, 'x');
    @memcpy(block[0..4], "docs");
    @memcpy(block[4..7], "key");
    @memcpy(block[7..12], "value");
    var budget = @import("../lite/test_allocator.zig").BudgetAllocator{ .backing = a, .limit = 12 };
    const copied = try copyTableEntry(budget.allocator(), .{ .namespace_name = block[0..4], .key = block[4..7], .value = block[7..12] });
    defer budget.allocator().free(copied.bytes);
    try std.testing.expectEqual(@as(usize, 12), budget.live);
    try std.testing.expectEqual(@as(usize, 1), budget.alloc_calls);
    @memset(block, 0);
    try std.testing.expectEqualStrings("docs", copied.entry.namespace_name.?);
    try std.testing.expectEqualStrings("key", copied.entry.key);
    try std.testing.expectEqualStrings("value", copied.entry.value);
}

test "lsm local result block retention bounds bytes and pin metadata" {
    const a = std.testing.allocator;
    var backend = @import("../lsm_backend.zig").Backend.init(a, .{});
    defer backend.close();
    const large = try SharedBytes.create(a, try a.alloc(u8, 2 * 1024 * 1024));
    defer large.release();
    const small = try SharedBytes.create(a, try a.alloc(u8, 1024));
    defer small.release();
    var blocks = [_]SourceBlockLease{.{ .local = large }};
    var cursor: MergeCursor(@TypeOf(backend), State) = undefined;
    cursor.backend = &backend;
    cursor.current_visible_source = 0;
    cursor.source_result_retention = &.{};
    cursor.source_blocks = &blocks;
    var held: std.ArrayListUnmanaged(BlockPin) = .empty;
    defer releaseHeldBlocks(&held, a);
    try std.testing.expect(!try cursor.retainCurrentValueForTxn(&held));
    try std.testing.expectEqual(@as(usize, 0), held.items.len);
    blocks[0] = .{ .local = small };
    try held.ensureTotalCapacity(a, 64);
    for (0..64) |_| {
        const distinct = try SharedBytes.create(a, try a.alloc(u8, 1024));
        held.appendAssumeCapacity(.{ .local = distinct });
    }
    try std.testing.expect(!try cursor.retainCurrentValueForTxn(&held));
    try std.testing.expectEqual(@as(usize, 64), held.items.len);
}

test "lsm local compressed absence reads once without promotion" {
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var backend = try B.open(a, "/review-negative", .{ .storage = storage.storage(), .flush_threshold = 1 });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "docs" });
    defer runtime.deinit();
    var write = try runtime.beginWrite();
    try write.put("document:long-shared-prefix-for-compression:00", "value");
    try write.put("document:long-shared-prefix-for-compression:02", "other");
    try write.commit();
    const run = backend.runs.at(0);
    const index = try indexForRunNoCache(&backend, run);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix, index.blockWindow(0).compression);
    // Model a Bloom false positive without depending on hash collision luck.
    if (index.blocks[0].filter) |filter| @memset(filter.bytes, 255);
    const loads = backend.read_stats.table_block_loads.load(.monotonic);
    const absent = try findExactEntryWithLocalIndexBlockMeta(&backend, run, index, .{ .name = "docs" }, "document:long-shared-prefix-for-compression:01");
    defer if (absent) |entry| entry.deinit(a);
    const extra = backend.read_stats.table_block_loads.load(.monotonic) - loads;
    std.debug.print("lite cold compressed absence: loads={d}, decoded cache blocks={d}\n", .{ extra, backend.run_block_cache.items.len });
    try std.testing.expect(absent == null);
    try std.testing.expectEqual(@as(u64, 1), extra);
    try std.testing.expectEqual(@as(usize, 0), backend.run_block_cache.items.len);
}

test "lsm cold optimization uncompressed allocation measurement" {
    const B = @import("../lsm_backend.zig").Backend;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    const payload = try a.alloc(u8, 512 * 1024);
    defer a.free(payload);
    @memset(payload, 37);
    try storage.storage().writeFileAbsolute("/raw-block", payload);
    var budget = Budget{ .backing = a };
    var backend = try B.open(budget.allocator(), "/cold-copy-measurement", .{ .storage = storage.storage() });
    defer backend.close();
    const window: lsm_table_file.EntryDataWindow = .{ .relative_offset = 0, .len = @intCast(payload.len), .checksum = std.hash.Crc32.hash(payload) };
    const live = budget.live;
    const calls = budget.alloc_calls;
    budget.peak = live;
    const decoded = try loadDecodedLocalBlock(&backend, budget.allocator(), "/raw-block", 0, window);
    defer budget.allocator().free(decoded);
    try std.testing.expectEqualSlices(u8, payload, decoded);
    std.debug.print("lite cold uncompressed 512KiB: allocations={d}, peak extra={d}, admission={d}\n", .{ budget.alloc_calls - calls, budget.peak - live, try localDecodeWorkingBytes(window) });
    try std.testing.expectEqual(@as(usize, 1), budget.alloc_calls - calls);
    try std.testing.expectEqual(payload.len, budget.peak - live);
    try std.testing.expect((try localDecodeWorkingBytes(window)) < 1024 * 1024);
    const held = budget.live;
    var corrupt = window;
    corrupt.checksum ^= 1;
    try std.testing.expectError(error.TableBlockChecksumMismatch, loadDecodedLocalBlock(&backend, budget.allocator(), "/raw-block", 0, corrupt));
    try std.testing.expectEqual(held, budget.live);
    corrupt = window;
    corrupt.physical_len = window.len - 1;
    try std.testing.expectError(error.InvalidTableFile, loadDecodedLocalBlock(&backend, budget.allocator(), "/raw-block", 0, corrupt));
    budget.limit = budget.live;
    try std.testing.expectError(error.OutOfMemory, loadDecodedLocalBlock(&backend, budget.allocator(), "/raw-block", 0, window));
    try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
    budget.limit = std.math.maxInt(usize);
}

test "lsm cold optimization mutable probe metadata measurement" {
    const B = @import("../lsm_backend.zig").Backend;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var backend = try B.open(a, "/probe-scratch-measurement", .{ .storage = storage.storage(), .flush_threshold = 1 });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "docs" });
    defer runtime.deinit();
    var disk = try runtime.beginWrite();
    try disk.put("document:00", "disk");
    try disk.commit();
    backend.options.flush_threshold = std.math.maxInt(usize);
    var mutable = try runtime.beginWrite();
    try mutable.put("document:01", "mutable");
    try mutable.commit();
    var metadata = Budget{ .backing = a };
    var probe = try BoundProbeTxn(B).open(&backend, .{ .name = "docs" });
    probe.metadata_allocator = metadata.allocator();
    defer probe.abort();
    const keys = [_][]const u8{ "document:00", "document:01" };
    var values: [2]?[]const u8 = undefined;
    try probe.getManySorted(&keys, &values);
    const calls = metadata.alloc_calls;
    for (0..100) |_| {
        try probe.getManySorted(&keys, &values);
        try std.testing.expectEqualStrings("disk", values[0].?);
        try std.testing.expectEqualStrings("mutable", values[1].?);
    }
    std.debug.print("lite mixed mutable probe: 100 batches, metadata allocations={d}\n", .{metadata.alloc_calls - calls});
    try std.testing.expectEqual(@as(usize, 0), metadata.alloc_calls - calls);
    const old_value = values[1].?;
    var later = try runtime.beginWrite();
    try later.put("document:01", "replacement");
    try later.commit();
    try std.testing.expectEqualStrings("mutable", old_value);
    try probe.getManySorted(&keys, &values);
    try std.testing.expectEqualStrings("replacement", values[1].?);
    const retained = metadata.live;
    const large_count = BatchScratch.Scratch.max_retained_keys + 1;
    var missing_keys: [large_count][]const u8 = undefined;
    var key_storage: [large_count][32]u8 = undefined;
    var missing_values: [large_count]?[]const u8 = undefined;
    for (&missing_keys, &key_storage, 0..) |*key, *buffer, i| key.* = try std.fmt.bufPrint(buffer, "missing:{d:0>4}", .{i});
    try probe.getManySorted(&missing_keys, &missing_values);
    for (missing_values) |value| try std.testing.expect(value == null);
    try std.testing.expectEqual(retained, metadata.live);
    metadata.limit = metadata.live;
    try std.testing.expectError(error.OutOfMemory, probe.getManySorted(&missing_keys, &missing_values));
    try std.testing.expectEqual(retained, metadata.live);
    // Failure and an oversized batch must leave the small workspace reusable.
    try probe.getManySorted(&keys, &values);
    try std.testing.expectEqualStrings("replacement", values[1].?);
    metadata.limit = std.math.maxInt(usize);
}

test "lsm cold optimization admits four large none and snappy blocks within default gate" {
    for ([_]lsm_table_file.BlockCompression{ .none, .snappy }) |codec| {
        const window: lsm_table_file.EntryDataWindow = .{
            .relative_offset = 0,
            .len = 512 * 1024,
            .physical_len = if (codec == .none) 512 * 1024 else 64 * 1024,
            .compression = codec,
        };
        const bytes = try localDecodeWorkingBytes(window);
        try std.testing.expect(bytes * LocalReader.workspace_count <= 8 * 1024 * 1024);
        var pool: LocalReader = .{};
        defer pool.deinit();
        var work: [LocalReader.workspace_count]LocalReader.Workspace = undefined;
        var count: usize = 0;
        defer for (work[0..count]) |*slot| slot.release();
        for (&work) |*slot| {
            slot.* = pool.acquire(std.testing.allocator, null, std.testing.io, bytes, 8 * 1024 * 1024, window.len + @sizeOf(SharedBytes));
            count += 1;
        }
        try std.testing.expectEqual(LocalReader.workspace_count, pool.active);
        try std.testing.expect(pool.active_bytes <= 8 * 1024 * 1024);
    }
}

test "lsm cold optimization snappy bounds decode compressible and incompressible large blocks" {
    const B = @import("../lsm_backend.zig").Backend;
    const snappy = @import("../../encoding/snappy.zig");
    const a = std.testing.allocator;
    for ([_]bool{ false, true }) |random| {
        var storage = storage_io.MemoryStorage.init(a);
        defer storage.deinit();
        var backend = try B.open(a, "/snappy-cold-bounds", .{ .storage = storage.storage() });
        defer backend.close();
        const raw = try a.alloc(u8, 512 * 1024);
        defer a.free(raw);
        if (random) {
            var rng = std.Random.DefaultPrng.init(42);
            rng.random().bytes(raw);
        } else @memset(raw, 37);
        const encoded = try snappy.encode(a, raw);
        defer a.free(encoded);
        try storage.storage().writeFileAbsolute("/snappy-block", encoded);
        const window: lsm_table_file.EntryDataWindow = .{
            .relative_offset = 0,
            .len = @intCast(raw.len),
            .physical_len = @intCast(encoded.len),
            .compression = .snappy,
            .checksum = std.hash.Crc32.hash(encoded),
        };
        const decoded = try loadDecodedLocalBlock(&backend, a, "/snappy-block", 0, window);
        defer a.free(decoded);
        try std.testing.expectEqualSlices(u8, raw, decoded);
        try std.testing.expect((try localDecodeWorkingBytes(window)) < 3 * 1024 * 1024);
        var corrupt = window;
        corrupt.len -= 1;
        try std.testing.expectError(error.InvalidTableFile, loadDecodedLocalBlock(&backend, a, "/snappy-block", 0, corrupt));
        try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
    }
}

test "lsm fresh review mixed 257 key metadata allocations" {
    const B = @import("../lsm_backend.zig").Backend;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var backend = try B.open(a, "/fresh-review-257", .{ .storage = storage.storage(), .flush_threshold = 1 });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "docs" });
    defer runtime.deinit();
    var keys: [257][]const u8 = undefined;
    var key_storage: [257][32]u8 = undefined;
    var values: [257]?[]const u8 = undefined;
    for (&keys, &key_storage, 0..) |*key, *buffer, i| key.* = try std.fmt.bufPrint(buffer, "document:{d:0>4}", .{i});
    var disk = try runtime.beginWrite();
    try disk.put(keys[0], "disk");
    try disk.commit();
    backend.options.flush_threshold = std.math.maxInt(usize);
    var mutable = try runtime.beginWrite();
    for (keys[1..]) |key| try mutable.put(key, "mutable");
    try mutable.commit();
    var metadata = Budget{ .backing = a };
    var probe = try BoundProbeTxn(B).open(&backend, .{ .name = "docs" });
    probe.metadata_allocator = metadata.allocator();
    defer probe.abort();
    try probe.getManySorted(&keys, &values);
    const calls = metadata.alloc_calls;
    for (0..100) |_| {
        try probe.getManySorted(&keys, &values);
        try std.testing.expectEqualStrings("disk", values[0].?);
        for (values[1..]) |value| try std.testing.expectEqualStrings("mutable", value.?);
    }
    std.debug.print("fresh review: 100 warm mixed 257-key batches, metadata allocations={d}\n", .{metadata.alloc_calls - calls});
    try std.testing.expectEqual(@as(usize, 0), metadata.alloc_calls - calls);
}

test "lsm cold optimization full prefix admission is separate from point scratch" {
    const window: lsm_table_file.EntryDataWindow = .{
        .relative_offset = 0,
        .len = 512 * 1024,
        .physical_len = 64 * 1024,
        .compression = .prefix,
    };
    const full = try localDecodeWorkingBytes(window);
    const point = try localDecodeWorkingBytesFor(window, true);
    try std.testing.expectEqual(8 * @as(usize, window.len), point - full);
    var snappy_window = window;
    snappy_window.compression = .prefix_snappy;
    const snappy_full = try localDecodeWorkingBytes(snappy_window);
    const snappy_point = try localDecodeWorkingBytesFor(snappy_window, true);
    try std.testing.expectEqual(8 * @as(usize, window.len), snappy_point - snappy_full);
    std.debug.print("prefix admission: logical=524288 physical=65536 full={d} point={d} prefix_snappy_full={d}\n", .{ full, point, snappy_full });
}

test "lsm cold optimization mixed batch owns immutable values before releasing its tip" {
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var backend = try B.open(a, "/batch-source-lifetime", .{
        .storage = storage.storage(),
        .flush_threshold = 1000,
        .flush_threshold_bytes = 256,
    });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "docs" });
    defer runtime.deinit();
    const original: [300]u8 = @splat('x');
    var write = try runtime.beginWrite();
    try write.put("a", &original);
    try write.commit();
    try std.testing.expectEqual(@as(usize, 1), backend.activeImmutableMemtableCount());
    var probe = try BoundProbeTxn(B).open(&backend, .{ .name = "docs" });
    defer probe.abort();
    var values: [2]?[]const u8 = undefined;
    try probe.getManySorted(&.{ "a", "missing" }, &values);
    const saved = values[0].?;
    try std.testing.expect(values[1] == null);
    try std.testing.expectEqual(@as(usize, 0), probe.held_layouts.items.len);
    try std.testing.expectEqual(@as(usize, 1), probe.held_values.items.len);
    try std.testing.expect(try backend.runMaintenanceStep());
    try std.testing.expectEqual(@as(usize, 0), backend.activeImmutableMemtableCount());
    try std.testing.expectEqual(@as(usize, 0), backend.retired_immutable_memtables.items.len);
    try std.testing.expectEqualStrings(&original, saved);
    backend.options.flush_threshold_bytes = std.math.maxInt(usize);
    var overwrite = try runtime.beginWrite();
    try overwrite.put("a", "new");
    try overwrite.commit();
    try probe.getManySorted(&.{ "a", "missing" }, &values);
    try std.testing.expectEqualStrings("new", values[0].?);
    try std.testing.expectEqualStrings(&original, saved);
}

test "lsm remaining wins writer cursors pin base and promoted overlay roots" {
    const B = @import("../lsm_backend.zig").Backend;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var count = Budget{ .backing = std.testing.allocator };
    const a = count.allocator();
    var storage = storage_io.MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    var backend = try B.open(a, "/writer-root-pins", .{ .storage = storage.storage(), .flush_threshold = 100000 });
    defer backend.close();
    {
        var write = try NamespaceWriteTxn(B).open(&backend);
        errdefer write.abort();
        for (0..8192) |i| {
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, i, .big);
            try write.put(.{ .name = "docs" }, &key, "value");
        }
        try write.commit();
    }
    var write = try BoundWriteTxn(B).open(&backend, .{ .name = "docs" });
    defer write.abort();
    try write.put("overlay", "value");
    try write.ensureCursorSnapshot();
    try std.testing.expect(write.mutable.ordered_enabled);
    try std.testing.expect(write.cursor_overlay.?.ordered_root == write.mutable.ordered.root);
    try std.testing.expect(write.cursor_base_mutable.?.state.ordered_root == backend.mutable.ordered.root);
    var copied: State = .{};
    defer copied.deinit(a);
    const old_live = count.live;
    count.peak = old_live;
    try state_mod.applyState(&copied, a, &backend.mutable);
    const copy_extra = count.peak - old_live;
    copied.deinit(a);
    write.invalidateCursorSnapshot();
    const warm_live = count.live;
    count.peak = warm_live;
    const calls = count.alloc_calls;
    for (0..100) |_| {
        try write.ensureCursorSnapshot();
        write.invalidateCursorSnapshot();
    }
    const pin_extra = count.peak - warm_live;
    try std.testing.expect(pin_extra < 4096);
    try std.testing.expect(copy_extra > 8192 * @sizeOf(state_mod.OwnedEntry));
    std.debug.print("writer cursor base: entries=8192 clone_peak={d} root_pin_peak={d} warm_allocations_per_capture={d}\n", .{ copy_extra, pin_extra, (count.alloc_calls - calls) / 100 });
    // Namespace writers use the same design, including drained bulk overlays.
    var ns_write = try NamespaceWriteTxn(B).openWithOptions(&backend, .{ .mode = .bulk_ingest });
    defer ns_write.abort();
    try ns_write.appendPut(.{ .name = "graph" }, "edge", "v");
    try ns_write.ensureCursorSnapshot();
    try std.testing.expect(ns_write.mutable.ordered_enabled);
    try std.testing.expect(ns_write.cursor_overlay.?.ordered_root == ns_write.mutable.ordered.root);
    try std.testing.expectEqualStrings("v", try ns_write.cursor_overlay.?.get(.{ .name = "graph" }, "edge"));
    var cursor = try ns_write.openCursor(.{ .name = "graph" });
    defer cursor.close();
    try std.testing.expectEqualStrings("v", (try cursor.seekAtOrAfter("edge")).?.value);
}

test "lsm remaining wins async candidate cursor preserves precedence without pair storage" {
    const a = std.testing.allocator;
    var runs: [1024]Run = undefined;
    var indices: [1024]usize = undefined;
    for (&runs, &indices, 0..) |*run, *index, i| {
        run.* = .{ .id = i + 1, .level = 0, .size_bytes = 0, .entry_count = 1, .smallest_key = @constCast("a"), .largest_key = @constCast("z"), .smallest_namespace_name = null, .largest_namespace_name = null, .bloom_filter = null, .path = null, .state = null };
        index.* = i;
    }
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    var candidates = BatchPointCandidates.init(groups, .{}, "m");
    for (0..1024) |i| try std.testing.expectEqual(i, candidates.next(&runs, &.{}, .{}, "m").?.run_index);
    try std.testing.expect(candidates.next(&runs, &.{}, .{}, "m") == null);
    std.debug.print("async candidate planning: overlapping_runs=1024 heap_allocations=0 per_slot_bytes={d}\n", .{@sizeOf(BatchPointCandidates)});
}

test "lsm remaining wins run handle lookup scales and unwinds allocation failures" {
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/run-handle-map", .{ .storage = storage.storage(), .cache = &cache, .flush_threshold = 1 });
    defer backend.close();
    {
        var write = try NamespaceWriteTxn(B).open(&backend);
        errdefer write.abort();
        try write.put(.{}, "a", "value");
        try write.commit();
    }
    while (try backend.runMaintenanceStep()) {}
    const source_run = run_store.at(&backend, 0);
    // One cached physical index can stand in for many independently indexed
    // handles. The ownership path is real; only the batch-local IDs differ.
    var warm = try loadRunTableIndexHandle(&backend, source_run);
    defer warm.release();
    const Fixture = struct {
        fn run(alloc: Allocator, b: *B, r: *Run) !void {
            var indexes = RunBatchIndexHandles{ .allocator = alloc };
            defer indexes.deinit();
            for (0..8) |i| _ = try indexes.state(b, r, i);
            for (0..8) |i| try std.testing.expectEqual(i, (try indexes.state(b, r, i)).run_index);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{ &backend, source_run });
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var backing = Budget{ .backing = a };
    var indexes = RunBatchIndexHandles{ .allocator = backing.allocator() };
    defer indexes.deinit();
    for (0..1024) |i| _ = try indexes.state(&backend, source_run, i);
    const calls = backing.alloc_calls;
    const start = platform_time.monotonicNs();
    for (0..32) |_| for (0..1024) |i| try std.testing.expectEqual(i, (try indexes.state(&backend, source_run, i)).run_index);
    const indexed_ns = platform_time.monotonicNs() - start;
    try std.testing.expectEqual(calls, backing.alloc_calls);
    var comparisons: usize = 0;
    const old_start = platform_time.monotonicNs();
    for (0..32) |_| for (0..1024) |i| {
        for (indexes.items.items) |item| {
            comparisons += 1;
            if (item.run_index == i) break;
        }
    };
    const linear_ns = platform_time.monotonicNs() - old_start;
    try std.testing.expectEqual(@as(usize, 32 * 1024 * 1025 / 2), comparisons);
    std.debug.print("run handle lookup: runs=1024 lookups=32768 linear_comparisons={d} linear_ns={d} indexed_ns={d} warm_allocations=0\n", .{ comparisons, linear_ns, indexed_ns });
}

test "lsm shared async batch reads one block for many slots and owns results" {
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-same-block", .{ .storage = storage.storage(), .cache = &cache, .flush_threshold = 1, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "docs" });
    defer runtime.deinit();
    var key_bytes: [256][64]u8 = undefined;
    var keys: [256][]const u8 = undefined;
    var write = try runtime.beginWrite();
    for (&keys, 0..) |*key, i| {
        key.* = try std.fmt.bufPrint(&key_bytes[i], "document:long-shared-prefix-for-compression:{d:0>4}", .{i});
        try write.put(key.*, key.*);
    }
    try write.commit();
    var runs = [_]Run{backend.runs.at(0).*};
    var handle = try loadRunTableIndexHandle(&backend, &runs[0]);
    try std.testing.expectEqual(@as(usize, 1), handle.runTableIndex().blocks.len);
    handle.release();
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, &runs);
    defer a.free(levels);
    const empty: State = .{};
    var values: [256]?[]const u8 = @splat(null);
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    for ([_]usize{ 16, 64, 256 }) |count| {
        cache.invalidatePrefix("/review-same-block");
        const before = backend.snapshotReadStats().table_block_loads;
        const result = (try readManySortedPointFromSnapshotAsync(&backend, &empty, &.{}, &runs, groups, levels, a, &held, .{ .name = "docs" }, keys[0..count], values[0..count], false, .snapshot_pinned, null)).?;
        const loads = backend.snapshotReadStats().table_block_loads - before;
        try std.testing.expectEqual(count, result.hits);
        cache.invalidatePrefix("/review-same-block");
        for (values[0..count], keys[0..count]) |value, key| try std.testing.expectEqualStrings(key, value.?);
        std.debug.print("shared async batch: keys={d} physical_blocks=1 physical_loads={d}\n", .{ count, loads });
        try std.testing.expectEqual(@as(u64, 1), loads);
    }
    const mixed_keys = [_][]const u8{ keys[255], keys[0], keys[128], keys[128], keys[5] };
    var mixed_values: [5]?[]const u8 = @splat(null);
    const mixed = (try readManySortedPointFromSnapshotAsync(&backend, &empty, &.{}, &runs, groups, levels, a, &held, .{ .name = "docs" }, &mixed_keys, &mixed_values, false, .snapshot_pinned, null)).?;
    try std.testing.expectEqual(@as(usize, 5), mixed.hits);
    for (mixed_values, mixed_keys) |value, key| try std.testing.expectEqualStrings(key, value.?);
    const resources = @import("../resource_manager.zig");
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = 1 };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    backend.options.resource_manager = &manager;
    defer backend.options.resource_manager = null;
    try std.testing.expectError(error.ResourceBudgetExceeded, readManySortedPointFromSnapshotAsync(&backend, &empty, &.{}, &runs, groups, levels, a, &held, .{ .name = "docs" }, keys[0..2], values[0..2], false, .snapshot_pinned, null));
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_in_memory_state).used_bytes);
    backend.options.resource_manager = null;
    const retry = (try readManySortedPointFromSnapshotAsync(&backend, &empty, &.{}, &runs, groups, levels, a, &held, .{ .name = "docs" }, keys[0..2], values[0..2], false, .snapshot_pinned, null)).?;
    try std.testing.expectEqual(@as(usize, 2), retry.hits);
}

test "lsm shared async batch cleanup unwinds every allocation failure" {
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var backend = try B.open(a, "/async-shared-oom", .{ .storage = storage.storage(), .flush_threshold = 1, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    var runtime = try backend.runtimeStore(a, .{ .name = "docs" });
    defer runtime.deinit();
    var write = try runtime.beginWrite();
    try write.put("doc:aaaaaaaaa:01", "one");
    try write.put("doc:aaaaaaaaa:02", "two");
    try write.put("doc:aaaaaaaaa:03", "three");
    try write.commit();
    var runs = [_]Run{backend.runs.at(0).*};
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, &runs);
    defer a.free(levels);
    const Fixture = struct {
        fn run(alloc: Allocator, b: *B, source_runs: []Run, source_groups: []const RunGroup, source_levels: []const RunLevel) !void {
            var cache = try cache_mod.Cache.initFallible(alloc, 1024 * 1024);
            defer cache.deinit();
            b.options.cache = &cache;
            defer b.options.cache = null;
            const keys = [_][]const u8{ "doc:aaaaaaaaa:01", "doc:aaaaaaaaa:02", "doc:aaaaaaaaa:03" };
            var values: [3]?[]const u8 = @splat(null);
            var held: PointResultValues = .empty;
            defer releaseHeldValues(&held, alloc);
            const empty: State = .{};
            const result = (try readManySortedPointFromSnapshotAsync(b, &empty, &.{}, source_runs, source_groups, source_levels, alloc, &held, .{ .name = "docs" }, &keys, &values, false, .snapshot_pinned, null)).?;
            try std.testing.expectEqual(@as(usize, 3), result.hits);
            try std.testing.expectEqualStrings("one", values[0].?);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{ &backend, &runs, groups, levels });
}

test "lsm shared directory classification visits keys once and includes mutable tombstones" {
    const Fixture = struct {
        calls: usize = 0,
        fn entryCount(_: *@This()) usize {
            return 128;
        }
        fn findIndex(self: *@This(), _: backend_types.Namespace, key: []const u8) ?usize {
            self.calls += 1;
            return if (std.mem.eql(u8, key, "missing")) null else 0;
        }
    };
    var mutable: Fixture = .{};
    var unresolved: std.ArrayListUnmanaged([]const u8) = .empty;
    defer unresolved.deinit(std.testing.allocator);
    const keys = [_][]const u8{ "hit", "missing", "tombstone" };
    const planned = try directoryUnresolvedKeys(std.testing.allocator, &mutable, &.{}, .{}, &keys, &unresolved, null);
    try std.testing.expectEqual(@as(usize, 3), mutable.calls);
    try std.testing.expectEqual(@as(usize, 1), planned.len);
    try std.testing.expectEqualStrings("missing", planned[0]);
}

test "lsm shared async decode retention is bounded and allocation failures unwind" {
    const a = std.testing.allocator;
    const raw: [64 * 1024]u8 = @splat('x');
    const payload = try @import("../../encoding/snappy.zig").encode(a, &raw);
    defer a.free(payload);
    const Fixture = struct {
        fn run(alloc: Allocator, compressed: []const u8) !void {
            var shared: BatchAsyncBlocks = .{ .allocator = alloc };
            defer {
                for (&shared.entries) |*entry| entry.users = 0;
                shared.deinit();
            }
            const read: AsyncPointBlockRead = .{
                .candidate = .{ .run_index = 0 },
                .path = "/fake",
                .run_id = 1,
                .generation = 1,
                .index_handle = null,
                .block_index = 0,
                .absolute_offset = 0,
                .physical_len = @intCast(compressed.len),
                .logical_len = 64 * 1024,
                .compression = .snappy,
                .checksum = @import("antfly_hash").Crc32.hash(compressed),
                .status = .ready_handle,
            };
            for (0..4) |_| {
                const entry = shared.insert(read);
                entry.users = 2;
                const first = (try shared.decodedPayload(entry, compressed)) orelse return error.OutOfMemory;
                try std.testing.expectEqualSlices(u8, &raw, first);
                const second = (try shared.decodedPayload(entry, compressed)).?;
                try std.testing.expect(first.ptr == second.ptr);
            }
            try std.testing.expectEqual(@as(usize, 256 * 1024), shared.decoded_bytes);
            const fifth = shared.insert(read);
            fifth.users = 2;
            try std.testing.expect(try shared.decodedPayload(fifth, compressed) == null);
            shared.entries[0].users = 0;
            _ = (try shared.decodedPayload(fifth, compressed)) orelse return error.OutOfMemory;
            try std.testing.expectEqual(@as(usize, 256 * 1024), shared.decoded_bytes);
            var oversized = read;
            oversized.logical_len += 1;
            const big = shared.insert(oversized);
            big.users = 2;
            try std.testing.expectError(error.InvalidTableFile, shared.decodedPayload(big, compressed));
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{payload});
}

test "lsm shared async block registry evicts before replacing at the configured limit" {
    var shared: BatchAsyncBlocks = .{ .allocator = std.testing.allocator, .limit = 2 };
    defer shared.deinit();
    const read: AsyncPointBlockRead = .{
        .candidate = .{ .run_index = 0 },
        .path = "/fake",
        .run_id = 1,
        .generation = 1,
        .index_handle = null,
        .block_index = 0,
        .absolute_offset = 0,
        .physical_len = 1,
        .logical_len = 1,
        .compression = .none,
        .checksum = 0,
        .status = .ready_handle,
    };
    const first = shared.insert(read);
    const second = shared.insert(read);
    second.users = 1;
    first.decoded = try std.testing.allocator.dupe(u8, "x");
    shared.decoded_bytes = 1;
    shared.prepareInsert();
    try std.testing.expect(!first.occupied);
    try std.testing.expectEqual(@as(usize, 0), shared.decoded_bytes);
    try std.testing.expect(second.occupied);
    const replacement = shared.insert(read);
    try std.testing.expect(replacement == first);
    second.users = 0;
}

test "lsm shared optional prefix decode OOM falls back and disables retries" {
    const a = std.testing.allocator;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-decode-budget", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    const encoded = try lsm_table_file.encodeAlloc(a, &entries);
    defer a.free(encoded);
    const index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    const window = index.blockWindow(0);
    try std.testing.expectEqual(@as(usize, 1), index.blocks.len);
    const physical = encoded[index.entry_data_start + window.physicalRelativeOffset() ..][0..window.physicalLen()];
    const prefix = if (window.compression == .prefix_snappy) try @import("../../encoding/snappy.zig").decode(a, physical) else try a.dupe(u8, physical);
    defer a.free(prefix);
    const compressed = try @import("../../encoding/snappy.zig").encode(a, prefix);
    defer a.free(compressed);
    try std.testing.expect(window.compression == .prefix or window.compression == .prefix_snappy);
    var decode_budget = Budget{ .backing = a, .limit = 512 };
    var pool: BatchAsyncBlocks = .{ .allocator = decode_budget.allocator(), .scratch_allocator = a };
    defer {
        for (&pool.entries) |*entry| entry.users = 0;
        pool.deinit();
    }
    const owner = pool.insert(.{
        .candidate = .{ .run_index = 0 },
        .path = "/block",
        .run_id = 1,
        .generation = 1,
        .index_handle = try cache.putRunTableIndex("/block", 1, 1, index),
        .block_index = 0,
        .absolute_offset = 0,
        .physical_len = @intCast(compressed.len),
        .logical_len = window.len,
        .compression = .prefix_snappy,
        .checksum = @import("antfly_hash").Crc32.hash(compressed),
        .status = .ready_handle,
        .physical_handle = try cache.putTransientRunTablePhysicalBlock("/block", 1, 1, 0, @intCast(compressed.len), try a.dupe(u8, compressed)),
    });
    owner.users = 2;
    var read = owner.read;
    read.index_handle = owner.read.index_handle.?.retain();
    read.physical_handle = null;
    read.shared_block = owner;
    read.status = .shared;
    defer read.release();
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    var hint: ?BorrowedReadHint = null;
    const result = (try consumeAsyncPointRead(&backend, &read, &hint, &held, a, .{}, entries[0].key, &pool)).?;
    try std.testing.expectEqualStrings("v", result.hit);
    try std.testing.expect(owner.decode_disabled);
    try std.testing.expectEqual(@as(usize, 0), pool.decoded_bytes);
    try std.testing.expect(try pool.decodedPayload(owner, compressed) == null);
    var corrupt = BatchAsyncBlock{ .read = owner.read, .users = 2 };
    corrupt.read.checksum ^= 1;
    try std.testing.expectError(error.TableBlockChecksumMismatch, pool.decodedPayload(&corrupt, compressed));
    try std.testing.expect(!corrupt.checksum_validated);
    // Plain prefix records need no expanded cache allocation, even when
    // admission is zero and only two distant keys share a large block.
    var plain = BatchAsyncBlock{ .read = owner.read, .users = 2 };
    plain.read.compression = .prefix;
    plain.read.checksum = @import("antfly_hash").Crc32.hash(prefix);
    const before_calls = decode_budget.alloc_calls;
    try std.testing.expect((try pool.decodedPayload(&plain, prefix)).?.ptr == prefix.ptr);
    try std.testing.expectEqual(before_calls, decode_budget.alloc_calls);
    try std.testing.expectEqual(@as(usize, 0), pool.decoded_bytes);
    // An admitted decode stores prefix records once, even for sparse points.
    decode_budget.limit = std.math.maxInt(usize);
    owner.decode_disabled = false;
    const decoded = (try pool.decodedPayload(owner, compressed)).?;
    try std.testing.expectEqualSlices(u8, prefix, decoded);
    try std.testing.expectEqual(prefix.len, pool.decoded_bytes);
    std.debug.print("shared sparse prefix: raw_bytes={d} retained_prefix_bytes={d} plain_expansion_allocations=0\n", .{ window.len, prefix.len });
    const calls = decode_budget.alloc_calls;
    try std.testing.expect((try pool.decodedPayload(owner, compressed)).?.ptr == decoded.ptr);
    try std.testing.expectEqual(calls, decode_budget.alloc_calls);
    const last = (try consumeAsyncPointRead(&backend, &read, &hint, &held, a, .{}, entries[entries.len - 1].key, &pool)).?;
    try std.testing.expectEqualStrings("v", last.hit);
    const fallback = (try consumeAsyncPointRead(&backend, &read, &hint, &held, a, .{}, entries[0].key, null)).?;
    try std.testing.expectEqualStrings("v", fallback.hit);
}

test "lsm shared single user snappy retains selected rows only" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-decode-budget", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .none });
    defer a.free(encoded);
    const index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    const window = index.blockWindow(0);
    try std.testing.expectEqual(@as(usize, 1), index.blocks.len);
    const physical = encoded[index.entry_data_start + window.physicalRelativeOffset() ..][0..window.physicalLen()];
    const prefix = try @import("../../encoding/snappy.zig").encode(a, physical);
    defer a.free(prefix);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.none, window.compression);
    var pool: BatchAsyncBlocks = .{ .allocator = a };
    defer {
        for (&pool.entries) |*entry| entry.users = 0;
        pool.deinit();
    }
    const owner = pool.insert(.{
        .candidate = .{ .run_index = 0 },
        .path = "/block",
        .run_id = 1,
        .generation = 1,
        .index_handle = try cache.putRunTableIndex("/block", 1, 1, index),
        .block_index = 0,
        .absolute_offset = 0,
        .physical_len = @intCast(prefix.len),
        .logical_len = window.len,
        .compression = .snappy,
        .checksum = @import("antfly_hash").Crc32.hash(prefix),
        .status = .ready_handle,
        .physical_handle = try cache.putTransientRunTablePhysicalBlock("/block", 1, 1, 0, @intCast(prefix.len), try a.dupe(u8, prefix)),
    });
    owner.users = 1;
    var read = owner.read;
    read.index_handle = owner.read.index_handle.?.retain();
    read.physical_handle = null;
    read.shared_block = owner;
    read.status = .shared;
    defer read.release();
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    for (0..64) |_| {
        const result = (try consumeAsyncPointRead(&backend, &read, null, &held, a, .{}, entries[0].key, &pool)).?;
        try std.testing.expectEqualStrings("v", result.hit);
    }
    var retained: usize = 0;
    for (held.items) |bytes| retained += bytes.len;
    try std.testing.expectEqual(@as(usize, 64), retained);
    try std.testing.expectEqual(@as(usize, window.len), pool.decoded_bytes);
    const direct = (try consumeAsyncPointRead(&backend, &read, null, &held, a, .{}, entries[0].key, null)).?;
    try std.testing.expectEqualStrings("v", direct.hit);
    try std.testing.expectEqual(@as(usize, 1), held.items[held.items.len - 1].len);
}

test "lsm shared mandatory snappy scratch respects resource and result budgets" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-decode-budget", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .none });
    defer a.free(encoded);
    const index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    const window = index.blockWindow(0);
    try std.testing.expectEqual(@as(usize, 1), index.blocks.len);
    const physical = encoded[index.entry_data_start + window.physicalRelativeOffset() ..][0..window.physicalLen()];
    const prefix = try @import("../../encoding/snappy.zig").encode(a, physical);
    defer a.free(prefix);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.none, window.compression);
    var pool: BatchAsyncBlocks = .{ .allocator = a };
    defer {
        for (&pool.entries) |*entry| entry.users = 0;
        pool.deinit();
    }
    const owner = pool.insert(.{
        .candidate = .{ .run_index = 0 },
        .path = "/block",
        .run_id = 1,
        .generation = 1,
        .index_handle = try cache.putRunTableIndex("/block", 1, 1, index),
        .block_index = 0,
        .absolute_offset = 0,
        .physical_len = @intCast(prefix.len),
        .logical_len = window.len,
        .compression = .snappy,
        .checksum = @import("antfly_hash").Crc32.hash(prefix),
        .status = .ready_handle,
        .physical_handle = try cache.putTransientRunTablePhysicalBlock("/block", 1, 1, 0, @intCast(prefix.len), try a.dupe(u8, prefix)),
    });
    owner.users = 1;
    var read = owner.read;
    read.index_handle = owner.read.index_handle.?.retain();
    read.physical_handle = null;
    read.shared_block = owner;
    read.status = .shared;
    defer read.release();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var bounded = Budget{ .backing = a, .limit = 0 };
    var held: PointResultValues = .empty;
    try held.ensureTotalCapacity(a, 8);
    defer releaseHeldValues(&held, a);
    try std.testing.expectError(error.OutOfMemory, consumeAsyncPointRead(&backend, &read, null, &held, bounded.allocator(), .{}, entries[0].key, null));
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(@as(usize, 0), held.items.len);
    // The supplied allocator controls results, while mandatory scratch has
    // independent reclaimable storage and resource admission.
    bounded.limit = 64;
    const small = (try consumeAsyncPointRead(&backend, &read, null, &held, bounded.allocator(), .{}, entries[0].key, null)).?;
    try std.testing.expectEqualStrings("v", small.hit);
    bounded.allocator().free(held.pop().?);
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    const resources = @import("../resource_manager.zig");
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = 1 };
    var denied = resources.ResourceManager.init(.{ .budgets = budgets });
    defer denied.deinit(a);
    backend.options.resource_manager = &denied;
    defer backend.options.resource_manager = null;
    try std.testing.expectError(error.ResourceBudgetExceeded, consumeAsyncPointRead(&backend, &read, null, &held, a, .{}, entries[0].key, null));
    read.checksum ^= 1;
    try std.testing.expectError(error.TableBlockChecksumMismatch, consumeAsyncPointRead(&backend, &read, null, &held, a, .{}, entries[0].key, null));
    read.checksum ^= 1;
    try std.testing.expectEqual(@as(u64, 0), denied.sliceStats(.lsm_read_working_set).used_bytes);
    budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = 32768 };
    var admitted = resources.ResourceManager.init(.{ .budgets = budgets });
    defer admitted.deinit(a);
    backend.options.resource_manager = &admitted;
    const result = (try consumeAsyncPointRead(&backend, &read, null, &held, a, .{}, entries[0].key, null)).?;
    try std.testing.expectEqualStrings("v", result.hit);
    try std.testing.expectEqual(@as(usize, 1), held.items[0].len);
    try std.testing.expectEqual(@as(u64, 0), admitted.sliceStats(.lsm_read_working_set).used_bytes);
}

test "lsm shared prefix scratch retention enforces per block and aggregate charged caps" {
    const a = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(a);
    var budget = resources.BudgetedAllocator.init(&manager, .lsm_read_working_set, a, 1);
    budget.credit_quantum = 1;
    defer budget.deinit();
    var pool: BatchAsyncBlocks = .{ .allocator = a, .scratch_allocator = budget.allocator() };
    defer {
        for (&pool.entries) |*entry| entry.users = 0;
        pool.deinit();
    }
    var entries: [16]lsm_table_file.Entry = @splat(.{ .key = "shared-prefix-key", .value = "v" });
    // Private payload preparation does not rely on index handles: this test
    // exercises only workspace capacity and its resource ownership.
    const encoded = try lsm_table_file.encodeAlloc(a, &entries);
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    const window = index.blockWindow(0);
    const physical = encoded[index.entry_data_start + window.physicalRelativeOffset() ..][0..window.physicalLen()];
    const prefix = if (window.compression == .prefix_snappy) try @import("../../encoding/snappy.zig").decode(a, physical) else try a.dupe(u8, physical);
    defer a.free(prefix);
    try std.testing.expect(window.compression == .prefix or window.compression == .prefix_snappy);
    const read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/fake", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = @intCast(prefix.len), .logical_len = window.len, .compression = .prefix, .checksum = @import("antfly_hash").Crc32.hash(prefix), .status = .ready_handle };
    for (0..4) |_| {
        const entry = pool.insert(read);
        entry.users = 1;
        const reader = try pool.prefixReader(entry, prefix, 0, 16);
        try reader.key_bytes.ensureTotalCapacityPrecise(budget.allocator(), BatchAsyncBlocks.max_decoded_block_bytes);
        pool.finishPrefixRead(entry);
    }
    try std.testing.expectEqual(@as(usize, 256 * 1024), pool.prefix_key_bytes);
    try std.testing.expectEqual(@as(u64, 256 * 1024), manager.sliceStats(.lsm_read_working_set).used_bytes);
    const fifth = pool.insert(read);
    fifth.users = 1;
    var reader = try pool.prefixReader(fifth, prefix, 0, 16);
    try reader.key_bytes.ensureTotalCapacityPrecise(budget.allocator(), BatchAsyncBlocks.max_decoded_block_bytes);
    pool.finishPrefixRead(fifth);
    try std.testing.expect(fifth.prefix_reader == null);
    try std.testing.expectEqual(@as(usize, 256 * 1024), pool.prefix_key_bytes);
    pool.entries[0].users = 0;
    reader = try pool.prefixReader(fifth, prefix, 0, 16);
    try reader.key_bytes.ensureTotalCapacityPrecise(budget.allocator(), BatchAsyncBlocks.max_decoded_block_bytes);
    pool.finishPrefixRead(fifth);
    try std.testing.expect(pool.entries[0].occupied);
    try std.testing.expect(pool.entries[0].prefix_reader == null);
    try std.testing.expect(fifth.prefix_reader != null);
    const large = pool.insert(read);
    large.users = 1;
    reader = try pool.prefixReader(large, prefix, 0, 16);
    try reader.key_bytes.ensureTotalCapacityPrecise(budget.allocator(), BatchAsyncBlocks.max_decoded_block_bytes + 1);
    pool.finishPrefixRead(large);
    try std.testing.expect(large.prefix_reader == null);
    try std.testing.expectEqual(@as(usize, 256 * 1024), pool.prefix_key_bytes);
    for (&pool.entries) |*entry| entry.users = 0;
    pool.deinit();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
}

test "lsm shared scratch is reclaimed with arena owned results" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-decode-budget", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .none });
    defer a.free(encoded);
    const index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    const window = index.blockWindow(0);
    try std.testing.expectEqual(@as(usize, 1), index.blocks.len);
    const physical = encoded[index.entry_data_start + window.physicalRelativeOffset() ..][0..window.physicalLen()];
    const prefix = try @import("../../encoding/snappy.zig").encode(a, physical);
    defer a.free(prefix);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.none, window.compression);
    var pool: BatchAsyncBlocks = .{ .allocator = a };
    defer {
        for (&pool.entries) |*entry| entry.users = 0;
        pool.deinit();
    }
    const owner = pool.insert(.{
        .candidate = .{ .run_index = 0 },
        .path = "/block",
        .run_id = 1,
        .generation = 1,
        .index_handle = try cache.putRunTableIndex("/block", 1, 1, index),
        .block_index = 0,
        .absolute_offset = 0,
        .physical_len = @intCast(prefix.len),
        .logical_len = window.len,
        .compression = .snappy,
        .checksum = @import("antfly_hash").Crc32.hash(prefix),
        .status = .ready_handle,
        .physical_handle = try cache.putTransientRunTablePhysicalBlock("/block", 1, 1, 0, @intCast(prefix.len), try a.dupe(u8, prefix)),
    });
    owner.users = 1;
    var read = owner.read;
    read.index_handle = owner.read.index_handle.?.retain();
    read.physical_handle = null;
    read.shared_block = owner;
    read.status = .shared;
    defer read.release();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var backing = Budget{ .backing = a };
    var arena = std.heap.ArenaAllocator.init(backing.allocator());
    defer arena.deinit();
    const va = arena.allocator();
    var held: PointResultValues = .empty;
    try held.ensureTotalCapacity(va, 64);
    const resources = @import("../resource_manager.zig");
    var manager = resources.ResourceManager.init(.{});
    defer manager.deinit(a);
    backend.options.resource_manager = &manager;
    defer backend.options.resource_manager = null;
    for (0..64) |_| {
        const result = (try consumeAsyncPointRead(&backend, &read, null, &held, va, .{}, entries[0].key, null)).?;
        try std.testing.expectEqualStrings("v", result.hit);
    }
    var retained: usize = 0;
    for (held.items) |bytes| retained += bytes.len;
    std.debug.print("\nLSM arena scratch decoded={d} results={d} backing_live={d} read_charge={d}\n", .{ window.len, retained, backing.live, manager.sliceStats(.lsm_read_working_set).used_bytes });
    try std.testing.expectEqual(@as(usize, 64), retained);
    try std.testing.expect(backing.live < 16 * 1024);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
}

test "lsm shared large singleton snappy transfers decoded storage" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-decode-budget", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    var entries: [1]lsm_table_file.Entry = undefined;
    var buffers: [1][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    const large = try a.alloc(u8, 1024 * 1024);
    defer a.free(large);
    @memset(large, 'v');
    entries[0].value = large;
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .none });
    defer a.free(encoded);
    const index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    const window = index.blockWindow(0);
    try std.testing.expectEqual(@as(usize, 1), index.blocks.len);
    const physical = encoded[index.entry_data_start + window.physicalRelativeOffset() ..][0..window.physicalLen()];
    const prefix = try @import("../../encoding/snappy.zig").encode(a, physical);
    defer a.free(prefix);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.none, window.compression);
    var pool: BatchAsyncBlocks = .{ .allocator = a };
    defer {
        for (&pool.entries) |*entry| entry.users = 0;
        pool.deinit();
    }
    const owner = pool.insert(.{
        .candidate = .{ .run_index = 0 },
        .path = "/block",
        .run_id = 1,
        .generation = 1,
        .index_handle = try cache.putRunTableIndex("/block", 1, 1, index),
        .block_index = 0,
        .absolute_offset = 0,
        .physical_len = @intCast(prefix.len),
        .logical_len = window.len,
        .compression = .snappy,
        .checksum = @import("antfly_hash").Crc32.hash(prefix),
        .status = .ready_handle,
        .physical_handle = try cache.putTransientRunTablePhysicalBlock("/block", 1, 1, 0, @intCast(prefix.len), try a.dupe(u8, prefix)),
    });
    owner.users = 1;
    var read = owner.read;
    read.index_handle = owner.read.index_handle.?.retain();
    read.physical_handle = null;
    read.shared_block = owner;
    read.status = .shared;
    defer read.release();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var bounded = Budget{ .backing = a, .limit = 1536 * 1024 };
    var held: PointResultValues = .empty;
    try held.ensureTotalCapacity(a, 8);
    defer held.deinit(a);

    const resources = @import("../resource_manager.zig");
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = window.len };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    backend.options.resource_manager = &manager;
    defer backend.options.resource_manager = null;
    const result = (try consumeAsyncPointRead(&backend, &read, null, &held, bounded.allocator(), .{}, entries[0].key, null)).?;
    try std.testing.expectEqualSlices(u8, large, result.hit);
    std.debug.print("\nLSM singleton Snappy decoded={d} result={d} peak={d} live={d}\n", .{ window.len, result.hit.len, bounded.peak, bounded.live });
    try std.testing.expectEqual(@as(usize, window.len), bounded.peak);
    for (held.items) |bytes| bounded.allocator().free(bytes);
    held.clearRetainingCapacity();
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
    try std.testing.expectError(error.CorruptInput, retainLargeSingletonSnappy(&backend, &read, read.index_handle.?.runTableIndex(), prefix[0 .. prefix.len - 1], null, &held, bounded.allocator(), .{}, entries[0].key));
    try std.testing.expectEqual(@as(usize, 0), bounded.live);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
    try std.testing.expect((try consumeAsyncPointRead(&backend, &read, null, &held, bounded.allocator(), .{}, "absent", null)) == null);
    try std.testing.expectEqual(@as(usize, 0), held.items.len);
    const Fixture = struct {
        fn run(alloc: Allocator, bck: *B, r: *AsyncPointBlockRead, k: []const u8) !void {
            var values: PointResultValues = .empty;
            defer releaseHeldValues(&values, alloc);
            const found = (try consumeAsyncPointRead(bck, r, null, &values, alloc, .{}, k, null)).?;
            try std.testing.expectEqual(@as(usize, 1024 * 1024), found.hit.len);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.run, .{ &backend, &read, entries[0].key });
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
}

test "lsm shared directory classification excludes immutable values and tombstones" {
    const a = std.testing.allocator;
    const empty: State = .{};
    var newest: State = .{};
    defer newest.deinit(a);
    var older: State = .{};
    defer older.deinit(a);
    try newest.appendUpsert(a, .{ .name = "docs" }, "deleted", "", true);
    try newest.appendUpsert(a, .{ .name = "docs" }, "new", "v", false);
    try older.appendUpsert(a, .{ .name = "docs" }, "deleted", "old", false);
    try older.appendUpsert(a, .{ .name = "docs" }, "old", "v", false);
    const keys = [_][]const u8{ "deleted", "missing", "new", "old" };
    var unresolved: std.ArrayListUnmanaged([]const u8) = .empty;
    defer unresolved.deinit(a);
    const planned = try directoryUnresolvedKeys(a, &empty, &.{ &newest, &older }, .{ .name = "docs" }, &keys, &unresolved, null);
    try std.testing.expectEqual(@as(usize, 1), planned.len);
    try std.testing.expectEqualStrings("missing", planned[0]);
    unresolved.clearRetainingCapacity();
    const other = try directoryUnresolvedKeys(a, &empty, &.{ &newest, &older }, .{ .name = "other" }, &keys, &unresolved, null);
    try std.testing.expectEqual(@intFromPtr(&keys), @intFromPtr(other.ptr));
}

test "lsm shared immutable classification eliminates overlapping directory selection" {
    const a = std.testing.allocator;
    const Directory = @import("run_directory.zig").Directory;
    const Fixture = struct {
        allocator: Allocator,
        pins: usize = 0,
        pub fn retainRunSnapshotRef(self: *@This(), run: *Run) !void {
            self.pins += 1;
            run.version_ref_pinned = true;
        }
        pub fn releaseRunSnapshotRef(self: *@This(), run: *Run) void {
            self.pins -= 1;
            run.version_ref_pinned = false;
        }
    };
    var fixture: Fixture = .{ .allocator = a };
    const directory = try Directory.create(a);
    defer directory.destroy(a);
    for (0..64) |i| try directory.put(&fixture, .{
        .id = i + 1,
        .level = 0,
        .size_bytes = 1024,
        .path = @constCast("/classification/run.sst"),
        .smallest_namespace_name = @constCast("docs"),
        .smallest_key = @constCast("deleted"),
        .largest_namespace_name = @constCast("docs"),
        .largest_key = @constCast("old"),
        .entry_count = 3,
        .bloom_filter = null,
        .state = null,
    });
    var immutable: State = .{};
    defer immutable.deinit(a);
    try immutable.appendUpsert(a, .{ .name = "docs" }, "deleted", "", true);
    try immutable.appendUpsert(a, .{ .name = "docs" }, "new", "v", false);
    try immutable.appendUpsert(a, .{ .name = "docs" }, "old", "v", false);
    const empty: State = .{};
    const keys = [_][]const u8{ "deleted", "new", "old" };
    var unresolved: std.ArrayListUnmanaged([]const u8) = .empty;
    defer unresolved.deinit(a);
    const planned = try directoryUnresolvedKeys(a, &empty, &.{&immutable}, .{ .name = "docs" }, &keys, &unresolved, null);
    var before = directory.sortedPoints("docs", &keys);
    var before_count: usize = 0;
    while (!before.done()) {
        var budget: usize = 16384;
        while (before.next(&budget) != null) before_count += 1;
    }
    var after = directory.sortedPoints("docs", planned);
    var after_count: usize = 0;
    while (!after.done()) {
        var budget: usize = 16384;
        while (after.next(&budget) != null) after_count += 1;
    }
    try std.testing.expectEqual(@as(usize, 64), before_count);
    try std.testing.expectEqual(@as(usize, 0), after_count);
    std.debug.print("\nLSM immutable classification keys={d} overlapping_runs={d} selected_before={d} selected_after={d}\n", .{ keys.len, directory.count(), before_count, after_count });
}

test "lsm shared sync prefix admission warm reuse and miss cleanup" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    const resources = @import("../resource_manager.zig");
    var denied_budgets = resources.Options.defaultBudgets();
    denied_budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = 1 };
    var denied = resources.ResourceManager.init(.{ .budgets = denied_budgets });
    defer denied.deinit(a);
    backend.options.resource_manager = &denied;
    defer backend.options.resource_manager = null;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var backing = Budget{ .backing = a };
    var arena = std.heap.ArenaAllocator.init(backing.allocator());
    defer arena.deinit();
    const va = arena.allocator();
    var held: PointResultValues = .empty;
    try held.ensureTotalCapacity(va, 64);
    try std.testing.expectError(error.ResourceBudgetExceeded, findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, .{}, entries[0].key));
    try std.testing.expectEqual(@as(usize, 0), held.items.len);
    try std.testing.expectEqual(@as(u64, 0), denied.sliceStats(.lsm_read_working_set).used_bytes);
    index.blocks[0].checksum ^= 1;
    try std.testing.expectError(error.TableBlockChecksumMismatch, findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, .{}, entries[0].key));
    index.blocks[0].checksum ^= 1;
    var admitted = resources.ResourceManager.init(.{});
    defer admitted.deinit(a);
    backend.options.resource_manager = &admitted;
    // Exercise the mandatory direct path independently of optional promotion.
    cache.max_bytes = 16 * 1024;
    for (0..64) |_| {
        const found = (try findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, .{}, entries[0].key)).?;
        try std.testing.expectEqualStrings("v", found.entry.value);
        try std.testing.expect(found.handle == null);
    }
    var retained: usize = 0;
    for (held.items) |bytes| retained += bytes.len;
    std.debug.print("\nLSM sync prefix result_rows={d} arena_live={d} read_charge={d}\n", .{ retained, backing.live, admitted.sliceStats(.lsm_read_working_set).used_bytes });
    try std.testing.expect(backing.live < 16 * 1024);
    try std.testing.expectEqual(@as(u64, 0), admitted.sliceStats(.lsm_read_working_set).used_bytes);
    index.blocks[0].entry_count += 1;
    try std.testing.expectError(error.InvalidTableFile, findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, .{}, entries[0].key));
    index.blocks[0].entry_count -= 1;
    const Fixture = struct {
        fn check(alloc: Allocator, b: *B, source_run: *Run, idx: *const lsm_table_file.TableIndex, key: []const u8) !void {
            var values: PointResultValues = .empty;
            defer releaseHeldValues(&values, alloc);
            const found = (try findExactEntryInCachedBlocks(b, source_run, idx, &values, alloc, .{}, key)).?;
            try std.testing.expectEqualStrings("v", found.entry.value);
            try std.testing.expect(found.handle == null);
        }
    };
    try std.testing.checkAllAllocationFailures(a, Fixture.check, .{ &backend, &run, &index, entries[0].key });
    try std.testing.expectEqual(@as(u64, 0), admitted.sliceStats(.lsm_read_working_set).used_bytes);
    cache.max_bytes = 1024 * 1024;
    var warm_decoded = try loadRunTableBlockHandle(&backend, &run, &index, index.blockWindow(0), true);
    defer warm_decoded.release();
    backend.options.resource_manager = &denied;
    const calls = backing.alloc_calls;
    const rows = held.items.len;
    const loads = backend.snapshotReadStats().table_block_loads;
    for (0..64) |i| {
        var found = (try findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, .{}, entries[i].key)).?;
        defer if (found.handle) |*handle| handle.release();
        try std.testing.expectEqualStrings("v", found.entry.value);
        try std.testing.expect(found.handle != null);
    }
    try std.testing.expectEqual(calls, backing.alloc_calls);
    try std.testing.expectEqual(rows, held.items.len);
    try std.testing.expectEqual(loads, backend.snapshotReadStats().table_block_loads);
    std.debug.print("LSM sync warm prefix 64 hits: result allocations={d} physical reads={d} owned_rows={d}\n", .{ backing.alloc_calls - calls, backend.snapshotReadStats().table_block_loads - loads, held.items.len - rows });
    if (index.blocks[0].filter) |*f| f.deinit(a);
    index.blocks[0].filter = null;
    const gap = try std.fmt.allocPrint(a, "{s}x", .{entries[0].key});
    defer a.free(gap);
    const refs = warm_decoded.entry.ref_count;
    try std.testing.expect((try findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, .{}, gap)) == null);
    try std.testing.expectEqual(refs, warm_decoded.entry.ref_count);
    std.debug.print("LSM sync miss cached refs: before={d} after={d}\n", .{ refs, warm_decoded.entry.ref_count });
    backend.options.resource_manager = &admitted;
    cache.invalidatePath("/sync-block");
    const miss_loads = backend.snapshotReadStats().table_block_loads;
    try std.testing.expect((try findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, .{}, gap)) == null);
    try std.testing.expectEqual(miss_loads + 1, backend.snapshotReadStats().table_block_loads);
    // Prefix misses never publish a decoded block or change the invalidated
    // decoded block's reference count. Owned cold results survive eviction.
    try std.testing.expectEqual(refs, warm_decoded.entry.ref_count);
    for (held.items) |bytes| try std.testing.expectEqual(@as(u8, 'v'), bytes[bytes.len - 1]);
    const corrupt = try a.dupe(u8, encoded);
    defer a.free(corrupt);
    corrupt[index.entry_data_start + index.blockWindow(0).physicalRelativeOffset()] ^= 1;
    try storage.storage().writeFileAbsolute("/sync-corrupt", corrupt);
    var bad_run = run;
    bad_run.id = 2;
    bad_run.path = @constCast("/sync-corrupt");
    try std.testing.expectError(error.TableBlockChecksumMismatch, findExactEntryInCachedBlocks(&backend, &bad_run, &index, &held, va, .{}, entries[0].key));
    try std.testing.expectEqual(@as(u64, 0), admitted.sliceStats(.lsm_read_working_set).used_bytes);
}

test "lsm shared hot prefix promotion is bounded optional and reuses pins" {
    for (0..6) |mode| {
        const a = std.testing.allocator;
        const B = @import("../lsm_backend.zig").Backend;
        var storage = storage_io.MemoryStorage.init(a);
        defer storage.deinit();
        const resources = @import("../resource_manager.zig");
        var budgets = resources.Options.defaultBudgets();
        if (mode == 1) budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = 8192 };
        if (mode == 2) budgets[@backingInt(resources.Slice.lsm_block_table_cache)] = .{ .hard_limit_bytes = 2048 };
        var manager = resources.ResourceManager.init(.{ .budgets = budgets });
        defer manager.deinit(a);
        const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
        var cache_budget = Budget{ .backing = a };
        var cache = cache_mod.Cache.init(cache_budget.allocator(), if (mode == 3) 16 * 1024 else 1024 * 1024);
        cache.attachResourceManager(&manager);
        defer cache.deinit();
        var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1, .resource_manager = &manager });
        defer backend.close();
        var entries: [192]lsm_table_file.Entry = undefined;
        var buffers: [192][80]u8 = undefined;
        for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
        var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
        defer filter.deinit(a);
        const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
        defer a.free(encoded);
        var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
        defer index.deinit(a);
        try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
        try storage.storage().writeFileAbsolute("/sync-block", encoded);
        var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
        var result_budget = Budget{ .backing = std.heap.c_allocator };
        const va = result_budget.allocator();
        var held: PointResultValues = .empty;
        defer releaseHeldValues(&held, va);
        try held.ensureTotalCapacity(va, 1);
        const namespace: backend_types.Namespace = .{ .block_cache_admission = if (mode == 4) .transient else .retain };
        const offset = @as(u64, @intCast(index.entry_data_start)) + index.blockWindow(0).physicalRelativeOffset();
        const count: usize = if (mode == 0) 10000 else 64;
        const before = result_budget.alloc_calls;
        const started = platform_time.monotonicNs();
        for (0..count) |i| {
            // Freeze cache allocation after seven direct hits. Required prefix
            // scratch/results use independent allocators and must still succeed.
            if (mode == 5 and i == 7) cache_budget.limit = cache_budget.live;
            var found = (try findExactEntryInCachedBlocks(&backend, &run, &index, &held, va, namespace, entries[i % entries.len].key)).?;
            defer if (found.handle) |*handle| handle.release();
            try std.testing.expectEqualStrings("v", found.entry.value);
            try std.testing.expectEqual(mode == 0 and i >= 8, found.handle != null);
            for (held.items) |bytes| va.free(bytes);
            held.clearRetainingCapacity();
            if (i == 6) {
                var premature = cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, index.blockWindow(0).physicalLen());
                defer if (premature) |*handle| handle.release();
                try std.testing.expect(premature == null);
            }
            try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
        }
        cache_budget.limit = std.math.maxInt(usize);
        const elapsed = platform_time.monotonicNs() - started;
        try std.testing.expectEqual(@as(usize, if (mode == 0) 8 else count), result_budget.alloc_calls - before);
        try std.testing.expectEqual(@as(u64, if (mode == 4) count else 1), backend.snapshotReadStats().table_block_loads);
        var promoted = cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, index.blockWindow(0).physicalLen());
        defer if (promoted) |*handle| handle.release();
        try std.testing.expectEqual(mode == 0, promoted != null);
        if (mode == 0) {
            cache.invalidatePath(run.path.?);
            const found = (try lsm_table_file.findExactEntryInBlock(&index, promoted.?.runTableBlock(), 0, null, entries[0].key)).?;
            try std.testing.expectEqualStrings("v", found.entry.value);
            try std.testing.expect(cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, index.blockWindow(0).physicalLen()) == null);
            std.debug.print("\nLSM hot prefix promoted {d} lookups ns={d} result_allocations={d}\n", .{ count, elapsed, result_budget.alloc_calls - before });
        }
    }
}

test "lsm shared optional promotion skips an active decoded load" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    for (0..7) |_| {
        _ = (try findExactEntryInCachedBlocks(&backend, &run, &index, &held, a, .{}, entries[0].key)).?;
    }
    const offset = @as(u64, @intCast(index.entry_data_start)) + index.blockWindow(0).physicalRelativeOffset();
    try cache.beginLoadWithBlock(run.path.?, run.id, backend.root_generation, .run_table_block, offset, index.blockWindow(0).physicalLen());
    var gate_open = true;
    defer if (gate_open) cache.finishLoadWithBlock(run.path.?, run.id, backend.root_generation, .run_table_block, offset, index.blockWindow(0).physicalLen());
    const Worker = struct {
        backend: *B,
        run: *Run,
        index: *const lsm_table_file.TableIndex,
        key: []const u8,
        done: std.atomic.Value(bool) = .init(false),
        failure: ?anyerror = null,
        fn work(self: *@This()) void {
            self.lookup() catch |err| {
                self.failure = err;
            };
            self.done.store(true, .release);
        }
        fn lookup(self: *@This()) !void {
            var values: PointResultValues = .empty;
            defer releaseHeldValues(&values, std.testing.allocator);
            var found = (try findExactEntryInCachedBlocks(self.backend, self.run, self.index, &values, std.testing.allocator, .{}, self.key)).?;
            defer if (found.handle) |*handle| handle.release();
            try std.testing.expectEqualStrings("v", found.entry.value);
        }
    };
    var worker = Worker{ .backend = &backend, .run = &run, .index = &index, .key = entries[0].key };
    const thread = try std.Thread.spawn(.{}, Worker.work, .{&worker});
    var joined = false;
    defer if (!joined) {
        if (gate_open) {
            cache.finishLoadWithBlock(run.path.?, run.id, backend.root_generation, .run_table_block, offset, index.blockWindow(0).physicalLen());
            gate_open = false;
        }
        thread.join();
    };
    const started = platform_time.monotonicNs();
    while (!worker.done.load(.acquire) and platform_time.monotonicNs() - started < 1000000000) platform_time.yieldBriefly();
    const waits = cache.snapshotStats().run_table_block.waits;
    const finished_before_release = worker.done.load(.acquire);
    cache.finishLoadWithBlock(run.path.?, run.id, backend.root_generation, .run_table_block, offset, index.blockWindow(0).physicalLen());
    gate_open = false;
    thread.join();
    joined = true;
    if (worker.failure) |err| return err;
    try std.testing.expectEqual(@as(u64, 0), waits);
    try std.testing.expect(finished_before_release);
    std.debug.print("\nLSM optional promotion: cache waits={d}, successful point returned before load-gate release={}\n", .{ waits, finished_before_release });
}

test "lsm shared async checksum failure releases fetched payload" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var cache_budget = Budget{ .backing = std.heap.c_allocator };
    var cache = cache_mod.Cache.init(cache_budget.allocator(), 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    var index_handle = try loadRunTableIndexHandle(&backend, &run);
    defer index_handle.release();
    const before = cache_budget.live;
    const window = index.blockWindow(0);
    const offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
    const payload = try cache.valueAllocator().dupe(u8, encoded[@intCast(offset)..][0..window.physicalLen()]);
    // Clean up the fixture on regression without masking the live-byte check.
    defer if (cache_budget.live > before) cache.valueAllocator().free(payload);
    payload[0] ^= 1;
    const Future = struct {
        bytes: []u8,
        waited: bool = false,
        canceled: bool = false,
        fn wait(ptr: *anyopaque) ![]u8 {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.waited = true;
            return self.bytes;
        }
        fn cancel(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.canceled = true;
        }
    };
    var future = Future{ .bytes = payload };
    var read: AsyncPointBlockRead = .{
        .candidate = .{ .run_index = 0 },
        .path = run.path.?,
        .run_id = run.id,
        .generation = backend.root_generation,
        .index_handle = index_handle.retain(),
        .block_index = 0,
        .absolute_offset = offset,
        .physical_len = window.physicalLen(),
        .logical_len = window.len,
        .compression = window.compression,
        .checksum = window.checksum,
        .status = .future,
        .future = .{ .ptr = &future, .vtable = &.{ .wait = Future.wait, .cancel = Future.cancel } },
    };
    try std.testing.expectError(error.TableBlockChecksumMismatch, payloadForAsyncPointRead(&backend, &read));
    read.release();
    try std.testing.expect(future.waited);
    try std.testing.expect(!future.canceled);
    try std.testing.expectEqual(before, cache_budget.live);
    std.debug.print("\nLSM async CRC failure: unowned payload after read cleanup={d} bytes\n", .{cache_budget.live - before});
}

test "lsm shared async warm decoded blocks skip scratch and preserve pool ownership" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    var warm = try loadRunTableBlockHandle(&backend, &run, &index, index.blockWindow(0), true);
    defer warm.release();
    var warm_index = try loadRunTableIndexHandle(&backend, &run);
    defer warm_index.release();
    const resources = @import("../resource_manager.zig");
    var budgets = resources.Options.defaultBudgets();
    budgets[@backingInt(resources.Slice.lsm_read_working_set)] = .{ .hard_limit_bytes = 1 };
    var manager = resources.ResourceManager.init(.{ .budgets = budgets });
    defer manager.deinit(a);
    backend.options.resource_manager = &manager;
    defer backend.options.resource_manager = null;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var scratch = Budget{ .backing = std.heap.c_allocator };
    var result = Budget{ .backing = std.heap.c_allocator };
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, result.allocator());
    try held.ensureTotalCapacity(result.allocator(), 1);
    var runs = [_]Run{run};
    const count = 10000;
    const loads = backend.snapshotReadStats().table_block_loads;
    const refs = warm.entry.ref_count;
    for (0..2) |mode| {
        const before = scratch.alloc_calls;
        const results_before = result.alloc_calls;
        const started = platform_time.monotonicNs();
        for (0..count) |i| {
            const key = entries[i % entries.len].key;
            var pool: BatchAsyncBlocks = .{ .allocator = scratch.allocator(), .scratch_allocator = scratch.allocator(), .limit = 2 };
            defer pool.deinit();
            var slot: BatchAsyncPointSlot = .{};
            slot.key_index = 0;
            slot.candidates = .{ .l0_indices = &.{0} };
            var issued: usize = 0;
            var read = if (mode == 0)
                (try prepareAsyncPointBlockRead(&backend, &runs, .{ .run_index = 0 }, .{}, key, null)).?
            else blk: {
                try std.testing.expect(try startBatchAsyncPointSlot(&backend, &runs, &.{}, &.{key}, .{}, &slot, &issued, &pool));
                try std.testing.expect(slot.read.decoded_handle == null);
                try std.testing.expect(slot.read.shared_block.?.read.decoded_handle != null);
                break :blk slot.read;
            };
            try std.testing.expectEqual(@as(usize, 0), issued);
            if (i == 0) {
                // Cancellation frees this pin without affecting the warm owner.
                var canceled = read;
                canceled.decoded_handle = if (read.decoded_handle) |handle| handle.retain() else null;
                canceled.index_handle = if (read.index_handle) |handle| handle.retain() else null;
                if (canceled.shared_block) |owner| owner.users += 1;
                canceled.cancel(&backend);
            }
            const lookup = (try consumeAsyncPointRead(&backend, &read, null, &held, result.allocator(), .{}, key, if (mode == 1) &pool else null)).?;
            read.release();
            try std.testing.expectEqualStrings("v", lookup.hit);
            for (held.items) |bytes| result.allocator().free(bytes);
            held.clearRetainingCapacity();
        }
        const elapsed = platform_time.monotonicNs() - started;
        try std.testing.expectEqual(@as(usize, 0), scratch.alloc_calls - before);
        try std.testing.expectEqual(@as(usize, count), result.alloc_calls - results_before);
        try std.testing.expectEqual(refs, warm.entry.ref_count);
        try std.testing.expectEqual(loads, backend.snapshotReadStats().table_block_loads);
        try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
        std.debug.print("\nLSM async warm decoded mode={d} lookups={d} ns={d} scratch_allocations={d} result_allocations={d}\n", .{ mode, count, elapsed, scratch.alloc_calls - before, result.alloc_calls - results_before });
    }
    // Owned async results survive release and cache invalidation.
    var read = (try prepareAsyncPointBlockRead(&backend, &runs, .{ .run_index = 0 }, .{}, entries[0].key, null)).?;
    var hint: ?BorrowedReadHint = null;
    const lookup = (try consumeAsyncPointRead(&backend, &read, &hint, &held, result.allocator(), .{}, entries[0].key, null)).?;
    read.release();
    cache.invalidatePath(run.path.?);
    try std.testing.expectEqualStrings("v", lookup.hit);
    try std.testing.expectEqualStrings(entries[0].key, hint.?.key);
}

test "lsm shared prefix promotion adopts construction credit under aggregate pressure" {
    const a = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 20 * 1024 } });
    defer manager.deinit(a);
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    cache.attachResourceManager(&manager);
    backend.options.resource_manager = &manager;
    defer backend.options.resource_manager = null;
    var sentinel = try cache.putRunTableBlock("sentinel", 99, 0, 0, 1024, try a.alloc(u8, 1024));
    sentinel.release();
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    for (0..8) |_| {
        var found = (try findExactEntryInCachedBlocks(&backend, &run, &index, &held, a, .{}, entries[0].key)).?;
        defer if (found.handle) |*handle| handle.release();
        try std.testing.expectEqualStrings("v", found.entry.value);
    }
    const offset = @as(u64, @intCast(index.entry_data_start)) + index.blockWindow(0).physicalRelativeOffset();
    var promoted = cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, index.blockWindow(0).physicalLen());
    defer if (promoted) |*handle| handle.release();
    var retained_sentinel = cache.retainRunTableBlock("sentinel", 99, 0, 0, 1024);
    defer if (retained_sentinel) |*handle| handle.release();
    try std.testing.expect(promoted != null);
    try std.testing.expect(retained_sentinel != null);
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
    try std.testing.expect(manager.snapshot().memory.peak_bytes <= 20 * 1024);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    std.debug.print("\nLSM promotion admission: aggregate_limit=20480 decoded_bytes={d} promoted={} sentinel_survived={} peak_bytes={d}\n", .{ index.blockWindow(0).len, promoted != null, retained_sentinel != null, manager.snapshot().memory.peak_bytes });
}

test "lsm shared cold async hits promote and eliminate repeated decoder scratch" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    var warm_index = try loadRunTableIndexHandle(&backend, &run);
    defer warm_index.release();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var scratch = Budget{ .backing = std.heap.c_allocator };
    var result = Budget{ .backing = std.heap.c_allocator };
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, result.allocator());
    try held.ensureTotalCapacity(result.allocator(), 1);
    var runs = [_]Run{run};
    const offset = @as(u64, @intCast(index.entry_data_start)) + index.blockWindow(0).physicalRelativeOffset();
    for (0..2) |mode| {
        const before = scratch.alloc_calls;
        const started = platform_time.monotonicNs();
        for (0..10000) |i| {
            const key = entries[i % entries.len].key;
            var read = (try prepareAsyncPointBlockRead(&backend, &runs, .{ .run_index = 0 }, .{}, key, null)).?;
            defer read.release();
            const lookup = (try consumeAsyncPointReadWithScratch(&backend, &read, null, &held, result.allocator(), .{}, key, null, scratch.allocator())).?;
            try std.testing.expectEqualStrings("v", lookup.hit);
            for (held.items) |bytes| result.allocator().free(bytes);
            held.clearRetainingCapacity();
        }
        const elapsed = platform_time.monotonicNs() - started;
        var decoded = cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, index.blockWindow(0).physicalLen());
        defer if (decoded) |*handle| handle.release();
        try std.testing.expect(decoded != null);
        try std.testing.expectEqual(@as(usize, if (mode == 0) 16 else 0), scratch.alloc_calls - before);
        std.debug.print("\nLSM cold async promotion {d} lookups=10000 ns={d} scratch_allocations={d} decoded_block_cached={}\n", .{ mode, elapsed, scratch.alloc_calls - before, decoded != null });
    }
}

test "lsm shared cold async batch promotes within a tight aggregate budget" {
    const a = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 24 * 1024 } });
    defer manager.deinit(a);
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    const run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    cache.attachResourceManager(&manager);
    backend.options.resource_manager = &manager;
    defer backend.options.resource_manager = null;
    var runs = [_]Run{run};
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, &runs);
    defer a.free(levels);
    const empty: State = .{};
    var keys: [16][]const u8 = undefined;
    for (&keys, 0..) |*key, i| key.* = entries[i].key;
    var values: [16]?[]const u8 = @splat(null);
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    const before = backend.snapshotReadStats().table_block_loads;
    for (0..2) |_| {
        const result = (try readManySortedPointFromSnapshotAsync(&backend, &empty, &.{}, &runs, groups, levels, a, &held, .{}, &keys, &values, false, .snapshot_pinned, null)).?;
        try std.testing.expectEqual(@as(usize, 16), result.hits);
        for (values) |value| try std.testing.expectEqualStrings("v", value.?);
        const window = index.blockWindow(0);
        const offset = @as(u64, @intCast(index.entry_data_start)) + window.physicalRelativeOffset();
        var decoded = cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, window.physicalLen());
        defer if (decoded) |*handle| handle.release();
        try std.testing.expect(decoded != null);
        var retained_scratch: u64 = 0;
        for (backend.point_reader.slots) |slot| if (slot.initialized) {
            try std.testing.expect(slot.cap.live <= LocalReader.retained_bytes_per_workspace + 128);
            retained_scratch += slot.cap.live;
        };
        try std.testing.expectEqual(retained_scratch, manager.sliceStats(.lsm_read_working_set).used_bytes);
        try std.testing.expectEqual(@as(usize, 0), backend.point_reader.active);
    }
    try std.testing.expectEqual(@as(u64, 1), backend.snapshotReadStats().table_block_loads - before);
    try std.testing.expect(manager.snapshot().memory.peak_bytes <= 24 * 1024);
    try std.testing.expectEqual(@as(u64, 0), manager.snapshot().memory.accounting_errors);
    backend.point_reader.deinit();
    try std.testing.expectEqual(@as(u64, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
}

test "lsm async reuse warm snapshot batches avoid result copies and per-key indexes" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "v" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    const run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    var warm_index = try loadRunTableIndexHandle(&backend, @constCast(&run));
    defer warm_index.release();
    var warmed = try loadRunTableBlockHandle(&backend, @constCast(&run), &index, index.blockWindow(0), true);
    warmed.release();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var results = Budget{ .backing = std.heap.c_allocator };
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, results.allocator());
    try held.ensureTotalCapacity(results.allocator(), 192);
    var pins: std.ArrayListUnmanaged(BlockPin) = .empty;
    defer releaseHeldBlocks(&pins, backend.allocator);
    try pins.ensureTotalCapacity(backend.allocator, 1);
    var runs = [_]Run{run};
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, &runs);
    defer a.free(levels);
    const empty: State = .{};
    var keys: [16][]const u8 = undefined;
    for (&keys, 0..) |*key, i| key.* = entries[i * 12].key;
    var values: [16]?[]const u8 = @splat(null);
    try std.testing.expectEqual(MultiGetPlan.point, chooseMultiGetPlan(&keys, .snapshot));
    const Directory = @import("run_directory.zig").Directory;
    const Fixture = struct {
        allocator: Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), run_ptr: *Run) !void {
            run_ptr.version_ref_pinned = true;
        }
        pub fn releaseRunSnapshotRef(_: *@This(), run_ptr: *Run) void {
            run_ptr.version_ref_pinned = false;
        }
    };
    var fixture: Fixture = .{ .allocator = a };
    const directory = try Directory.create(a);
    defer directory.destroy(a);
    for (0..64) |i| {
        var source = run;
        source.id = i + 1;
        try directory.put(&fixture, source);
    }
    var newest = run;
    newest.id = 64;
    var newest_index = try loadRunTableIndexHandle(&backend, &newest);
    newest_index.release();
    var newest_block = try loadRunTableBlockHandle(&backend, &newest, &index, index.blockWindow(0), true);
    newest_block.release();
    var planning = Budget{ .backing = std.heap.c_allocator };
    var reuse: BatchScratch.ProbeScratch = .{};
    defer reuse.deinit(a);
    for ([_]usize{ 1, 16, 2, 3, 4, 5 }) |mode| {
        const concurrency: usize = if (mode == 1) 1 else 16;
        backend.options.max_concurrent_point_block_reads = concurrency;
        cache.pressure_target_bytes.store(if (mode == 3) cache.max_bytes else 0, .monotonic);
        const before = results.alloc_calls;
        const planning_before = planning.alloc_calls;
        const indexes_before = cache.snapshotStats().run_table_index.hits;
        const started = platform_time.monotonicNs();
        for (0..600) |_| {
            const found = if (mode == 2) try readManySortedDirectoryBatch(&backend, &empty, &.{}, directory, results.allocator(), &pins, &held, .{}, &keys, &values, false, false, &reuse, planning.allocator()) else if (mode == 5) try readManySortedDirectoryCandidates(&backend, &empty, &.{}, directory, planning.allocator(), results.allocator(), &pins, &held, .{}, &keys, &values, false, false) else try readManySortedPointFromSnapshot(&backend, &empty, &.{}, &runs, groups, levels, results.allocator(), if (mode == 4) null else &pins, &held, .{}, &keys, &values, false);
            try std.testing.expectEqual(@as(usize, 16), found.hits);
            for (values) |value| try std.testing.expectEqualStrings("v", value.?);
            if (mode == 3) {
                try std.testing.expectEqual(@as(usize, 0), pins.items.len);
                try std.testing.expectEqual(@as(usize, 256), held.items[0].len);
            }
            for (held.items) |bytes| results.allocator().free(bytes);
            held.clearRetainingCapacity();
            for (pins.items) |*pin| pin.release();
            pins.clearRetainingCapacity();
        }
        try std.testing.expectEqual(@as(usize, if (mode == 3 or mode == 4) 600 else 0), results.alloc_calls - before);
        try std.testing.expect(reuse.planner == null);
        if (mode == 2) try std.testing.expectEqual(@as(usize, 0), planning.alloc_calls - planning_before);
        if (mode == 5) try std.testing.expect(planning.alloc_calls - planning_before > 0);
        try std.testing.expectEqual(@as(u64, 600), cache.snapshotStats().run_table_index.hits - indexes_before);
        std.debug.print("\nAsync reuse warm production batches: mode={d} batches=600 keys=16 ns={d} result_allocations={d} index_cache_hits={d} planning_allocations={d}\n", .{ mode, platform_time.monotonicNs() - started, results.alloc_calls - before, cache.snapshotStats().run_table_index.hits - indexes_before, planning.alloc_calls - planning_before });
    }
    cache.pressure_target_bytes.store(0, .monotonic);
    for ([_]bool{ true, false }) |planned| {
        const before = planning.alloc_calls;
        const started = platform_time.monotonicNs();
        for (0..600) |_| {
            for (keys) |key| {
                var hint: ?BorrowedReadHint = null;
                const value = if (planned)
                    try getFromDirectoryPointCandidatesPlanned(&backend, directory, &hint, &pins, &held, planning.allocator(), results.allocator(), .{}, key)
                else
                    try getFromDirectoryPointCandidates(&backend, directory, &hint, &pins, &held, planning.allocator(), results.allocator(), .{}, key);
                try std.testing.expectEqualStrings("v", value);
            }
            for (held.items) |bytes| results.allocator().free(bytes);
            held.clearRetainingCapacity();
            for (pins.items) |*pin| pin.release();
            pins.clearRetainingCapacity();
        }
        const allocations = planning.alloc_calls - before;
        if (planned) try std.testing.expect(allocations > 0) else try std.testing.expectEqual(@as(usize, 0), allocations);
        std.debug.print("Single directory reads: planned={} lookups=9600 ns={d} planning_allocations={d}\n", .{ planned, platform_time.monotonicNs() - started, allocations });
    }
    backend.options.max_concurrent_point_block_reads = 16;
    const found = try readManySortedPointFromSnapshot(&backend, &empty, &.{}, &runs, groups, levels, results.allocator(), &pins, &held, .{}, &keys, &values, false);
    try std.testing.expectEqual(@as(usize, 16), found.hits);
    try std.testing.expectEqual(@as(usize, 1), pins.items.len);
    // Reuse an already-owned pin under pressure without copying it again.
    cache.pressure_target_bytes.store(cache.max_bytes, .monotonic);
    const allocation_calls = results.alloc_calls;
    _ = try readManySortedPointFromSnapshot(&backend, &empty, &.{}, &runs, groups, levels, results.allocator(), &pins, &held, .{}, &keys, &values, false);
    try std.testing.expectEqual(allocation_calls, results.alloc_calls);
    try std.testing.expectEqual(@as(usize, 1), pins.items.len);
    cache.pressure_target_bytes.store(0, .monotonic);
    cache.invalidatePath(run.path.?);
    for (values) |value| try std.testing.expectEqualStrings("v", value.?);
}

test "lsm async reuse raw snappy promotes without decoding twice" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 1 });
    defer backend.close();
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| {
        buffers[i][0] = @intCast(i);
        entry.* = .{ .key = buffers[i][0..1], .value = "aaaaaaaaaaaaaaaa" };
    }
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.snappy, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    var run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    var warm_index = try loadRunTableIndexHandle(&backend, &run);
    defer warm_index.release();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var scratch = Budget{ .backing = std.heap.c_allocator };
    var result = Budget{ .backing = std.heap.c_allocator };
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, result.allocator());
    try held.ensureTotalCapacity(result.allocator(), 1);
    var runs = [_]Run{run};
    const offset = @as(u64, @intCast(index.entry_data_start)) + index.blockWindow(0).physicalRelativeOffset();
    for (0..2) |mode| {
        if (mode == 1) {
            var warmed = try loadRunTableBlockHandle(&backend, &run, &index, index.blockWindow(0), true);
            warmed.release();
        }
        const before = scratch.alloc_calls;
        const started = platform_time.monotonicNs();
        for (0..10000) |i| {
            const key = entries[i % entries.len].key;
            var read = (try prepareAsyncPointBlockRead(&backend, &runs, .{ .run_index = 0 }, .{}, key, null)).?;
            defer read.release();
            const lookup = (try consumeAsyncPointReadWithScratch(&backend, &read, null, &held, result.allocator(), .{}, key, null, scratch.allocator())).?;
            try std.testing.expectEqualStrings("aaaaaaaaaaaaaaaa", lookup.hit);
            for (held.items) |bytes| result.allocator().free(bytes);
            held.clearRetainingCapacity();
        }
        const elapsed = platform_time.monotonicNs() - started;
        var decoded = cache.retainRunTableBlock(run.path.?, run.id, backend.root_generation, offset, index.blockWindow(0).physicalLen());
        defer if (decoded) |*handle| handle.release();
        try std.testing.expect(decoded != null);
        try std.testing.expectEqual(@as(usize, if (mode == 0) 8 else 0), scratch.alloc_calls - before);
        std.debug.print("\nAsync reuse raw snappy {d} lookups=10000 ns={d} scratch_allocations={d} decoded_block_cached={}\n", .{ mode, elapsed, scratch.alloc_calls - before, decoded != null });
    }
}

test "lsm async reuse result pins bound owners and preserve fallback on metadata OOM" {
    const a = std.testing.allocator;
    var cache = cache_mod.Cache.init(a, 16 * 1024 * 1024);
    defer cache.deinit();
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    var backend = struct { allocator: Allocator }{ .allocator = failing.allocator() };
    var held: std.ArrayListUnmanaged(BlockPin) = .empty;
    defer releaseHeldBlocks(&held, backend.allocator);
    var pins = AsyncPointResultPins.init(&held);
    var first = try cache.putRunTableBlock("pins", 1, 0, 0, 1, try a.dupe(u8, "v"));
    defer first.release();
    const refs = first.entry.ref_count;
    try std.testing.expect(!pins.retain(&backend, &first));
    try std.testing.expectEqual(refs, first.entry.ref_count);
    failing.fail_index = std.math.maxInt(usize);
    for (0..64) |i| {
        var handle = try cache.putRunTableBlock("pins", i + 1, 0, 0, 1, try a.dupe(u8, "v"));
        defer handle.release();
        try std.testing.expect(pins.retain(&backend, &handle));
    }
    try std.testing.expectEqual(@as(usize, 64), held.items.len);
    // Repeated calls to the same transaction cannot reset the owner-wide bound.
    pins = AsyncPointResultPins.init(&held);
    try std.testing.expect(pins.retain(&backend, &first));
    var next = try cache.putRunTableBlock("pins", 65, 0, 0, 1, try a.dupe(u8, "v"));
    defer next.release();
    try std.testing.expect(!pins.retain(&backend, &next));
    var transient = try cache.putTransientRunTableBlock("transient-pins", 1, 0, 0, 1, try a.dupe(u8, "v"));
    defer transient.release();
    try std.testing.expect(!pins.retain(&backend, &transient));
    var empty: std.ArrayListUnmanaged(BlockPin) = .empty;
    defer releaseHeldBlocks(&empty, a);
    var oversized_pins = AsyncPointResultPins.init(&empty);
    var oversized = try cache.putRunTableBlock("oversized-pins", 1, 0, 0, 1024 * 1024, try a.alloc(u8, 1024 * 1024));
    defer oversized.release();
    try std.testing.expect(!oversized_pins.retain(&backend, &oversized));
}

test "lsm async reuse multi block index borrowing survives replacement and pin cap fallback" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 16 * 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/review-sync", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    const payload: [8192]u8 = @splat('x');
    var entries: [192]lsm_table_file.Entry = undefined;
    var buffers: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .key = try std.fmt.bufPrint(&buffers[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = &payload };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = .none });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.none, index.blockWindow(0).compression);
    try storage.storage().writeFileAbsolute("/sync-block", encoded);
    const run: Run = .{ .id = 1, .level = 0, .size_bytes = encoded.len, .path = @constCast("/sync-block"), .smallest_namespace_name = null, .largest_namespace_name = null, .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null };
    var warm_index = try loadRunTableIndexHandle(&backend, @constCast(&run));
    defer warm_index.release();
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var result_alloc = Budget{ .backing = std.heap.c_allocator };
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, result_alloc.allocator());
    var pins: std.ArrayListUnmanaged(BlockPin) = .empty;
    defer releaseHeldBlocks(&pins, backend.allocator);
    var runs = [_]Run{run};
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, &runs);
    defer a.free(levels);
    const empty: State = .{};
    var keys: [192][]const u8 = undefined;
    for (&keys, 0..) |*key, i| key.* = entries[i].key;
    var values: [192]?[]const u8 = @splat(null);
    const before = cache.snapshotStats().run_table_index.hits;
    const loads = backend.snapshotReadStats().table_block_loads;
    cache.pressure_target_bytes.store(cache.max_bytes, .monotonic);
    for (0..2) |_| {
        const found = (try readManySortedPointFromSnapshotAsync(&backend, &empty, &.{}, &runs, groups, levels, result_alloc.allocator(), &held, .{}, &keys, &values, false, .snapshot_pinned, &pins)).?;
        try std.testing.expectEqual(@as(usize, 192), found.hits);
        for (values) |value| try std.testing.expectEqualStrings(&payload, value.?);
        const budget = AsyncPointResultPins.init(&pins);
        try std.testing.expect(budget.bytes <= AsyncPointResultPins.max_bytes);
        try std.testing.expect(pins.items.len > 0 and pins.items.len <= AsyncPointResultPins.max_pins);
        try std.testing.expect(held.items.len > 0);
    }
    try std.testing.expectEqual(@as(u64, 2), cache.snapshotStats().run_table_index.hits - before);
    try std.testing.expectEqual(@as(u64, @intCast(index.blocks.len)), backend.snapshotReadStats().table_block_loads - loads);
    cache.invalidatePath(run.path.?);
    for (values) |value| try std.testing.expectEqualStrings(&payload, value.?);
}

test "lsm async reuse packed result buffers survive growth and allocation failure" {
    const Fixture = struct {
        fn run(a: Allocator) !void {
            var held: PointResultValues = .empty;
            defer releaseHeldValues(&held, a);
            var copies: AsyncPointResultCopies = .{};
            const first = try copies.copy(a, &held, "one");
            const second = try copies.copy(a, &held, "two");
            try std.testing.expectEqual(@as(usize, 1), held.items.len);
            const grows: [300]u8 = @splat('t');
            const third = try copies.copy(a, &held, &grows);
            const large: [20000]u8 = @splat('x');
            const fourth = try copies.copy(a, &held, &large);
            try std.testing.expectEqualStrings("one", first);
            try std.testing.expectEqualStrings("two", second);
            try std.testing.expectEqualStrings(&grows, third);
            try std.testing.expectEqualStrings(&large, fourth);
            try std.testing.expectEqual(@as(usize, 3), held.items.len);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Fixture.run, .{});
}

test "lsm async reuse mixed sizes keep large copies exact and reuse small buffer" {
    const a = std.testing.allocator;
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    var copies: AsyncPointResultCopies = .{};
    const small: [128]u8 = @splat('s');
    const large: [16384]u8 = @splat('l');
    const first = try copies.copy(a, &held, &small);
    for (0..32) |_| {
        const value = try copies.copy(a, &held, &large);
        try std.testing.expectEqualStrings(&large, value);
        try std.testing.expectEqual(large.len, held.items[held.items.len - 1].len);
    }
    const last = try copies.copy(a, &held, &small);
    try std.testing.expectEqual(@as(usize, 33), held.items.len);
    try std.testing.expect(last.ptr == first.ptr + small.len);
    try std.testing.expectEqualStrings(&small, first);
    try std.testing.expectEqualStrings(&small, last);
    try std.testing.expect(copies.buffer.len <= AsyncPointResultCopies.max_buffer_bytes);
}

test "lsm async reuse sparse batches bound owner slack and fit exact budgets" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/fresh-sparse-results", .{ .storage = storage.storage(), .cache = &cache, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    var active: ActiveMemTable = .{};
    defer active.deinit(a);
    const payload: [1024]u8 = @splat('v');
    try active.upsert(a, .{}, "a", &payload, false);
    var state = try active.snapshot(a);
    defer state.deinit(a);
    const keys = [_][]const u8{ "a", "missing00", "missing01", "missing02", "missing03", "missing04", "missing05", "missing06", "missing07", "missing08", "missing09", "missing10", "missing11", "missing12", "missing13", "missing14" };
    try std.testing.expectEqual(MultiGetPlan.point, chooseMultiGetPlan(&keys, .snapshot));
    for ([_]usize{ 2048, std.math.maxInt(usize) }) |limit| {
        var budget = Budget{ .backing = std.heap.c_allocator };
        const va = budget.allocator();
        var held: PointResultValues = .empty;
        defer releaseHeldValues(&held, va);
        try held.ensureTotalCapacity(va, 1000);
        const before = budget.live;
        budget.limit = before +| limit;
        var values: [keys.len]?[]const u8 = @splat(null);
        if (limit == 2048) {
            const found = (try readManySortedPointFromSnapshotAsync(&backend, &state, &.{}, &.{}, &.{}, &.{}, va, &held, .{}, &keys, &values, false, .transaction_owned, null)).?;
            try std.testing.expectEqual(@as(usize, 1), found.hits);
            try std.testing.expectEqual(@as(usize, 1024), budget.live - before);
        } else {
            var first: ?[]const u8 = null;
            for (0..1000) |_| {
                const found = (try readManySortedPointFromSnapshotAsync(&backend, &state, &.{}, &.{}, &.{}, &.{}, va, &held, .{}, &keys, &values, false, .transaction_owned, null)).?;
                try std.testing.expectEqual(@as(usize, 1), found.hits);
                try std.testing.expectEqual(@as(usize, 15), found.misses);
                if (first == null) first = values[0];
            }
            try std.testing.expectEqualStrings(&payload, first.?);
            try std.testing.expect(budget.live - before <= 1024000 + 16 * 1024);
            try std.testing.expect(held.items.len < 100);
            std.debug.print("Sparse copied batches: calls=1000 copied=1024000 retained={d} allocations={d}\n", .{ budget.live - before, held.items.len });
        }
    }
}

test "lsm async reuse copy growth falls back and clears owner cursor" {
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = std.testing.allocator };
    const a = budget.allocator();
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    try held.ensureTotalCapacity(a, 8);
    const metadata = budget.live;
    budget.limit = metadata + 3;
    const first = try held.copies.copy(a, &held, "one");
    try std.testing.expectEqual(@as(usize, 3), held.items[0].len);
    try std.testing.expectError(error.OutOfMemory, held.copies.copy(a, &held, "two"));
    try std.testing.expectEqualStrings("one", first);
    budget.limit += 3;
    const second = try held.copies.copy(a, &held, "two");
    try std.testing.expectEqualStrings("one", first);
    try std.testing.expectEqualStrings("two", second);
    for (held.items) |value| a.free(value);
    held.clearRetainingCapacity();
    try std.testing.expectEqual(@as(usize, 0), held.copies.buffer.len);
    const third = try held.copies.copy(a, &held, "three");
    try std.testing.expectEqualStrings("three", third);
    try std.testing.expectEqual(@as(usize, 5), held.items[0].len);
    budget.limit = std.math.maxInt(usize);
    var receiver: PointResultValues = .empty;
    defer releaseHeldValues(&receiver, a);
    try receiver.appendSlice(a, held.items);
    held.deinit(a); // Transfer allocations, release only source metadata.
    try std.testing.expectEqual(@as(usize, 0), held.copies.buffer.len);
    const fourth = try receiver.copies.copy(a, &receiver, "four");
    try std.testing.expectEqualStrings("three", third);
    try std.testing.expectEqualStrings("four", fourth);
}

test "lsm async reuse single directory reads preserve tombstones namespaces and fallback" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    const Directory = @import("run_directory.zig").Directory;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/single-directory", .{ .storage = storage.storage(), .cache = &cache });
    defer backend.close();
    const Fixture = struct {
        allocator: Allocator,
        pub fn retainRunSnapshotRef(_: *@This(), _: *Run) !void {}
        pub fn releaseRunSnapshotRef(_: *@This(), _: *Run) void {}
    };
    var fixture: Fixture = .{ .allocator = a };
    const directory = try Directory.create(a);
    defer directory.destroy(a);
    const older = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "a", .value = "old-a" },
        .{ .namespace_name = "docs", .key = "b", .value = "old-b" },
        .{ .namespace_name = "docs", .key = "c", .value = "old-c" },
        .{ .namespace_name = "docs", .key = "d", .value = "" },
    };
    const newer = [_]lsm_table_file.Entry{
        .{ .namespace_name = "docs", .key = "a", .value = "", .tombstone = true },
        .{ .namespace_name = "docs", .key = "b", .value = "new-b" },
        .{ .namespace_name = "docs", .key = "x", .value = "new-x" },
    };
    for ([_][]const lsm_table_file.Entry{ &older, &newer }, 0..) |entries, i| {
        const path = if (i == 0) "/older.sst" else "/newer.sst";
        const encoded = try lsm_table_file.encodeAlloc(a, entries);
        defer a.free(encoded);
        try storage.storage().writeFileAbsolute(path, encoded);
        try directory.put(&fixture, .{ .id = i + 1, .level = 0, .size_bytes = encoded.len, .path = @constCast(path), .smallest_namespace_name = @constCast("docs"), .largest_namespace_name = @constCast("docs"), .smallest_key = @constCast(entries[0].key), .largest_key = @constCast(entries[entries.len - 1].key), .entry_count = @intCast(entries.len), .bloom_filter = null, .state = null });
    }
    for (0..2) |backlog| {
        try std.testing.expectEqual(backlog == 0, directory.supportsAsyncPoints());
        for ([_]?[]const u8{ null, "docs", "other" }) |ns| {
            for ([_][]const u8{ "a", "b", "c", "d", "x", "missing" }) |key| {
                var pins: std.ArrayListUnmanaged(BlockPin) = .empty;
                defer releaseHeldBlocks(&pins, a);
                var values: PointResultValues = .empty;
                defer releaseHeldValues(&values, a);
                var hint: ?BorrowedReadHint = null;
                const expected: ?[]const u8 = getFromDirectoryPointCandidatesPlanned(&backend, directory, &hint, &pins, &values, a, a, .{ .name = ns }, key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                hint = null;
                const actual: ?[]const u8 = getFromDirectoryPointCandidates(&backend, directory, &hint, &pins, &values, a, a, .{ .name = ns }, key) catch |err| switch (err) {
                    error.NotFound => null,
                    else => return err,
                };
                if (expected) |value| try std.testing.expectEqualStrings(value, actual.?) else try std.testing.expect(actual == null);
                if (backlog == 0 and ns != null and std.mem.eql(u8, ns.?, "docs")) {
                    if (std.mem.eql(u8, key, "a")) try std.testing.expect(actual == null);
                    if (std.mem.eql(u8, key, "b")) try std.testing.expectEqualStrings("new-b", actual.?);
                    if (std.mem.eql(u8, key, "c")) try std.testing.expectEqualStrings("old-c", actual.?);
                    if (std.mem.eql(u8, key, "d")) try std.testing.expectEqualStrings("", actual.?);
                }
            }
        }
        if (backlog == 0) {
            for (3..66) |id| try directory.put(&fixture, .{ .id = id, .level = 0, .size_bytes = 1, .path = @constCast("/older.sst"), .smallest_namespace_name = @constCast("docs"), .largest_namespace_name = @constCast("docs"), .smallest_key = @constCast("a"), .largest_key = @constCast("d"), .entry_count = older.len, .bloom_filter = null, .state = null });
        }
    }
}

test "lsm async reuse ordinary probes own one copy without retaining blocks" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    for ([_]lsm_table_file.CompressionPolicy{ .none, .snappy_adaptive }) |compression| {
        var storage = storage_io.MemoryStorage.init(a);
        defer storage.deinit();
        var cache = cache_mod.Cache.init(a, 16 * 1024 * 1024);
        defer cache.deinit();
        var backend = try B.open(a, "/ordinary-probe-ownership", .{
            .storage = storage.storage(),
            .cache = &cache,
            .flush_threshold = 1,
            .compact_threshold_runs = 100,
            .l0_overlap_compact_threshold_runs = 100,
            .table_block_compression = compression,
            .max_concurrent_point_block_reads = 16,
        });
        defer backend.close();
        var keys: [192][80]u8 = undefined;
        for (&keys, 0..) |*key, i| {
            @memset(key, 'k');
            key[0] = @intCast(i);
        }
        const large: [8192]u8 = @splat('v');
        for (0..2) |_| {
            var write = try NamespaceWriteTxn(B).open(&backend);
            errdefer write.abort();
            for (&keys) |*key| try write.put(.{}, key, "v");
            try write.put(.{}, "large", &large);
            try write.commit();
            while (try backend.runMaintenanceStep()) {}
        }
        try std.testing.expectEqual(@as(usize, 2), run_store.count(&backend));
        for (0..6) |mode| {
            if (mode == 3) {
                backend.options.flush_threshold = 1000;
                var write = try NamespaceWriteTxn(B).open(&backend);
                errdefer write.abort();
                try write.put(.{}, "pending", "mutable");
                try write.commit();
            }
            if (mode % 3 != 1) for (0..run_store.count(&backend)) |i| {
                const run = run_store.at(&backend, i);
                var index = try loadRunTableIndexHandle(&backend, run);
                defer index.release();
                for (0..index.runTableIndex().blockCount()) |block_index| {
                    var block = try loadRunTableBlockHandle(&backend, run, index.runTableIndex(), index.runTableIndex().blockWindow(block_index), true);
                    block.release();
                }
            };
            cache.pressure_target_bytes.store(if (mode % 3 == 2) cache.max_bytes else 0, .monotonic);
            var probe = try BoundProbeTxn(B).open(&backend, .{});
            defer probe.abort();
            try std.testing.expectEqual(mode < 3, probe.stable_point_view);
            const before_copies = backend.snapshotReadStats().point_value_copies;
            const first = try probe.get(&keys[0]);
            const second = try probe.get(&keys[1]);
            const third = try probe.get(&keys[2]);
            const wide = try probe.get("large");
            try std.testing.expectEqual(@as(u64, 4), backend.snapshotReadStats().point_value_copies - before_copies);
            try std.testing.expectEqual(@as(usize, 0), probe.held_blocks.items.len);
            try std.testing.expectEqual(@as(usize, 2), probe.held_values.items.len);
            try std.testing.expectEqual(@as(usize, 256), probe.held_values.items[0].len);
            try std.testing.expectEqual(large.len, probe.held_values.items[1].len);
            for (0..run_store.count(&backend)) |i| cache.invalidatePath(run_store.at(&backend, i).path.?);
            try std.testing.expectEqual(@as(usize, 0), cache.currentBytes());
            try std.testing.expectEqualStrings("v", first);
            try std.testing.expectEqualStrings("v", second);
            try std.testing.expectEqualStrings("v", third);
            try std.testing.expectEqualStrings(&large, wide);
            std.debug.print("\nordinary probe ownership: mode={d} pins=0 owned_bytes=8448 payload_buffers=2 cache_bytes_after_invalidation=0\n", .{mode});
        }
        cache.pressure_target_bytes.store(0, .monotonic);
        // Short leases still opt into block borrowing, independently of ordinary
        // probes, and remain valid after cache invalidation.
        for (0..run_store.count(&backend)) |i| {
            const run = run_store.at(&backend, i);
            var index = try loadRunTableIndexHandle(&backend, run);
            defer index.release();
            var block = try loadRunTableBlockHandle(&backend, run, index.runTableIndex(), index.runTableIndex().blockWindow(0), true);
            block.release();
        }
        var lease = try BoundProbeTxn(B).open(&backend, .{});
        defer lease.abort();
        const borrowed = try lease.getLeased(&keys[0]);
        try std.testing.expect(lease.held_blocks.items.len != 0);
        try std.testing.expectEqual(@as(usize, 0), lease.leased_values.items.len);
        for (0..run_store.count(&backend)) |i| cache.invalidatePath(run_store.at(&backend, i).path.?);
        try std.testing.expectEqualStrings("v", borrowed);
    }
}

test "lsm async reuse ordinary fallback packs copies without pin metadata" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    for ([_]bool{ false, true }) |with_cache| {
        for ([_]usize{ 0, 1, 2 }) |run_count| {
            var storage = storage_io.MemoryStorage.init(a);
            defer storage.deinit();
            var cache = cache_mod.Cache.init(a, 16 * 1024 * 1024);
            defer cache.deinit();
            var budget = Budget{ .backing = a };
            var backend = try B.open(budget.allocator(), "/ordinary-allocation-review", .{
                .storage = storage.storage(),
                .cache = if (with_cache) &cache else null,
                .flush_threshold = 1,
                .compact_threshold_runs = 100,
                .l0_overlap_compact_threshold_runs = 100,
                .table_block_compression = .none,
                .max_concurrent_point_block_reads = 16,
            });
            defer backend.close();
            for (0..run_count) |_| {
                var write = try NamespaceWriteTxn(B).open(&backend);
                errdefer write.abort();
                try write.put(.{}, "key", "v");
                try write.commit();
                while (try backend.runMaintenanceStep()) {}
            }
            if (run_count == 0) {
                backend.options.flush_threshold = 1000;
                var write = try NamespaceWriteTxn(B).open(&backend);
                errdefer write.abort();
                try write.put(.{}, "key", "v");
                try write.commit();
            }
            try std.testing.expectEqual(run_count, run_store.count(&backend));
            if (with_cache) for (0..run_store.count(&backend)) |i| {
                const run = run_store.at(&backend, i);
                var index = try loadRunTableIndexHandle(&backend, run);
                defer index.release();
                var block = try loadRunTableBlockHandle(&backend, run, index.runTableIndex(), index.runTableIndex().blockWindow(0), true);
                block.release();
            };
            for ([_]bool{ true, false }) |stable| {
                if (run_count == 0 and stable) continue;
                if (!stable) {
                    backend.options.flush_threshold = 1000;
                    var write = try NamespaceWriteTxn(B).open(&backend);
                    errdefer write.abort();
                    try write.put(.{}, "pending", "mutable");
                    try write.commit();
                }
                var probe = try BoundProbeTxn(B).open(&backend, .{});
                defer probe.abort();
                try std.testing.expectEqual(stable, probe.stable_point_view);
                try probe.held_values.ensureTotalCapacity(probe.allocator, 2048);
                try std.testing.expectEqualStrings("v", try probe.get("key"));
                const before_calls = budget.alloc_calls;
                const before_copies = backend.snapshotReadStats().point_value_copies;
                for (0..1000) |_| try std.testing.expectEqualStrings("v", try probe.get("key"));
                try std.testing.expectEqual(@as(usize, 0), budget.alloc_calls - before_calls);
                try std.testing.expectEqual(@as(u64, 1000), backend.snapshotReadStats().point_value_copies - before_copies);
                try std.testing.expectEqual(@as(usize, 0), probe.held_blocks.items.len);
                try std.testing.expectEqual(@as(usize, 3), probe.held_values.items.len);
                std.debug.print("\nordinary fallback: cache={} runs={d} stable={} lookups=1000 allocator_calls={d} reported_copies={d} persistent_pins={d} payload_buffers={d}\n", .{ with_cache, run_count, stable, budget.alloc_calls - before_calls, backend.snapshotReadStats().point_value_copies - before_copies, probe.held_blocks.items.len, probe.held_values.items.len });
                budget.limit = budget.live + 16;
                const bounded_result = probe.get("key");
                budget.limit = std.math.maxInt(usize);
                try std.testing.expectEqualStrings("v", try bounded_result);
                try std.testing.expectEqual(@as(usize, 0), budget.alloc_calls - before_calls);
                try std.testing.expectEqual(@as(u64, 1001), backend.snapshotReadStats().point_value_copies - before_copies);
                if (run_count == 0) {
                    const before_batch = backend.snapshotReadStats().point_value_copies;
                    var values: [2]?[]const u8 = undefined;
                    try probe.getManySorted(&.{ "key", "pending" }, &values);
                    try std.testing.expectEqualStrings("v", values[0].?);
                    try std.testing.expectEqualStrings("mutable", values[1].?);
                    try std.testing.expectEqual(@as(u64, 2), backend.snapshotReadStats().point_value_copies - before_batch);
                }
            }
        }
    }
}

test "lsm point followup local prefix writes directly to packed result storage" {
    const a = std.testing.allocator;
    const B = @import("../lsm_backend.zig").Backend;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var budget = Budget{ .backing = a };
    var backend = try B.open(budget.allocator(), "/point-direct-prefix", .{ .storage = storage.storage(), .local_block_cache_enabled = false, .flush_threshold = 1 });
    defer backend.close();
    var keys: [128][80]u8 = undefined;
    var write = try NamespaceWriteTxn(B).open(&backend);
    errdefer write.abort();
    for (&keys, 0..) |*key, i| {
        @memset(key, 'k');
        key[79] = @intCast(i);
        try write.put(.{}, key, "v");
    }
    try write.commit();
    while (try backend.runMaintenanceStep()) {}
    const run = run_store.at(&backend, 0);
    const index = try indexForRunNoCache(&backend, run);
    const codec = index.blockWindow(0).compression;
    try std.testing.expect(codec == .prefix or codec == .prefix_snappy);
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    try held.ensureTotalCapacity(a, 32);
    const first = (try getFromRunWithLocalIndex(&backend, run, null, &held, a, .{}, &keys[0], false)).?;
    const before = budget.alloc_calls;
    const before_copies = backend.snapshotReadStats().point_value_copies;
    for (0..1000) |i| try std.testing.expectEqualStrings("v", (try getFromRunWithLocalIndex(&backend, run, null, &held, a, .{}, &keys[i % keys.len], i % 2 == 0)).?);
    try std.testing.expectEqual(@as(usize, 0), budget.alloc_calls - before);
    try std.testing.expectEqual(@as(u64, 1000), backend.snapshotReadStats().point_value_copies - before_copies);
    try std.testing.expectEqualStrings("v", first);
    try std.testing.expectEqual(@as(usize, 3), held.items.len);
    const before_legacy = budget.alloc_calls;
    for (0..1000) |i| {
        const loaded = (try findExactEntryWithLocalIndexMaybeLocked(&backend, run, .{}, &keys[i % keys.len], i % 2 == 0)).?;
        defer loaded.deinit(backend.allocator);
        try std.testing.expectEqualStrings("v", loaded.entry.value);
    }
    try std.testing.expectEqual(@as(usize, 1000), budget.alloc_calls - before_legacy);
    std.debug.print("\nlocal prefix point: lookups=1000 intermediate_backend_allocations=1000->0 payload_buffers=3\n", .{});
}

test "lsm point followup shared decoded scratch reuses bounded workspace" {
    const a = std.testing.allocator;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var budget = Budget{ .backing = a };
    var pool: LocalReader = .{};
    defer pool.deinit();
    const raw: [16 * 1024]u8 = @splat('x');
    const compressed = try @import("../../encoding/snappy.zig").encode(a, &raw);
    defer a.free(compressed);
    const read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/fixture", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = @intCast(compressed.len), .logical_len = raw.len, .compression = .snappy, .checksum = @import("antfly_hash").Crc32.hash(compressed), .status = .ready_handle };
    var warm_calls: usize = 0;
    for (0..101) |i| {
        var shared: BatchAsyncBlocks = .{ .allocator = a, .workspace_config = .{ .pool = &pool, .backing = budget.allocator(), .manager = null, .io = std.testing.io, .limit = 8 * 1024 * 1024 } };
        const block = shared.insert(read);
        const bytes = (try shared.decodedPayload(block, compressed)).?;
        try std.testing.expectEqualSlices(u8, &raw, bytes);
        shared.deinit();
        if (i == 0) warm_calls = budget.alloc_calls;
    }
    try std.testing.expectEqual(warm_calls, budget.alloc_calls);
    try std.testing.expectEqual(@as(usize, 0), pool.active);
    try std.testing.expect(budget.live <= LocalReader.retained_bytes_per_workspace + 128);
    const before_unpooled = budget.alloc_calls;
    for (0..100) |_| {
        var shared: BatchAsyncBlocks = .{ .allocator = budget.allocator() };
        const block = shared.insert(read);
        try std.testing.expectEqualSlices(u8, &raw, (try shared.decodedPayload(block, compressed)).?);
        shared.deinit();
    }
    try std.testing.expectEqual(@as(usize, 100), budget.alloc_calls - before_unpooled);
    std.debug.print("\nshared decoded scratch: batches=100 warm_backend_allocations=100->0 retained_bytes={d}\n", .{budget.live});
}

test "lsm point followup shared prefix keys reuse workspace and failure cleanup" {
    const a = std.testing.allocator;
    const Budget = @import("../lite/test_allocator.zig").BudgetAllocator;
    var keys: [64][80]u8 = undefined;
    var entries: [64]lsm_table_file.Entry = undefined;
    for (&entries, 0..) |*entry, i| {
        @memset(&keys[i], 'k');
        keys[i][79] = @intCast(i);
        entry.* = .{ .key = &keys[i], .value = "v" };
    }
    const encoded = try lsm_table_file.encodeAlloc(a, &entries);
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    const window = index.blockWindow(0);
    try std.testing.expect(window.compression == .prefix or window.compression == .prefix_snappy);
    const payload = encoded[index.entry_data_start + window.physicalRelativeOffset() ..][0..window.physicalLen()];
    const read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/prefix-fixture", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = window.physicalLen(), .logical_len = window.len, .compression = window.compression, .checksum = window.checksum, .status = .ready_handle };
    const Fixture = struct {
        fn run(backing: Allocator, compressed: []const u8, block_read: AsyncPointBlockRead, key: []const u8) !void {
            var pool: LocalReader = .{};
            defer pool.deinit();
            var shared: BatchAsyncBlocks = .{ .allocator = backing, .workspace_config = .{ .pool = &pool, .backing = backing, .manager = null, .io = std.testing.io, .limit = 8 * 1024 * 1024 } };
            defer shared.deinit();
            const block = shared.insert(block_read);
            const decoded = try shared.decodedPayload(block, compressed) orelse return error.OutOfMemory;
            const reader = try shared.prefixReader(block, decoded, 0, 64);
            defer shared.finishPrefixRead(block);
            const row = try reader.find(shared.prefixAllocator(block), null, key) orelse return error.NotFound;
            try std.testing.expectEqualStrings("v", row.entry.value);
        }
    };
    var no_resize = @import("../lite/test_allocator.zig").NoResizeAllocator{ .backing = a };
    try std.testing.checkAllAllocationFailures(no_resize.allocator(), Fixture.run, .{ payload, read, &keys[32] });
    var budget = Budget{ .backing = a };
    var pool: LocalReader = .{};
    defer pool.deinit();
    var warm_calls: usize = 0;
    for (0..101) |i| {
        var shared: BatchAsyncBlocks = .{ .allocator = a, .workspace_config = .{ .pool = &pool, .backing = budget.allocator(), .manager = null, .io = std.testing.io, .limit = 8 * 1024 * 1024 } };
        const block = shared.insert(read);
        const decoded = (try shared.decodedPayload(block, payload)).?;
        const reader = try shared.prefixReader(block, decoded, 0, entries.len);
        for (entries) |entry| {
            const row = (try reader.find(shared.prefixAllocator(block), null, entry.key)).?;
            try std.testing.expectEqualStrings("v", row.entry.value);
        }
        shared.finishPrefixRead(block);
        shared.deinit();
        if (i == 0) warm_calls = budget.alloc_calls;
    }
    try std.testing.expectEqual(warm_calls, budget.alloc_calls);
    try std.testing.expect(budget.live <= LocalReader.retained_bytes_per_workspace + 128);
    std.debug.print("\nshared prefix scratch: batches=100 keys_per_batch=64 warm_backend_allocations=0 retained_bytes={d}\n", .{budget.live});
}

test "lsm point followup recycled scratch reuses non-LIFO freed blocks" {
    const a = std.testing.allocator;
    const raw: [16 * 1024]u8 = @splat('x');
    const compressed = try @import("../../encoding/snappy.zig").encode(a, &raw);
    defer a.free(compressed);
    const read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/review-churn", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = @intCast(compressed.len), .logical_len = raw.len, .compression = .snappy, .checksum = @import("antfly_hash").Crc32.hash(compressed), .status = .ready_handle };
    for ([_]bool{ false, true }) |pooled| {
        var pool: LocalReader = .{};
        defer pool.deinit();
        var shared: BatchAsyncBlocks = .{ .allocator = a, .limit = 16 };
        if (pooled) shared.workspace_config = .{ .pool = &pool, .backing = a, .manager = null, .io = std.testing.io, .limit = 8 * 1024 * 1024 };
        defer {
            for (&shared.entries) |*entry| entry.users = 0;
            shared.deinit();
        }
        var fallbacks: usize = 0;
        var first_fallback: ?usize = null;
        var peak_live: usize = 0;
        for (0..512) |i| {
            if (i >= 16) shared.entries[i % 16].users = 0;
            shared.prepareInsert();
            var candidate = read;
            candidate.absolute_offset = i * 16384;
            const block = shared.insert(candidate);
            block.users = 1;
            if (try shared.decodedPayload(block, compressed)) |decoded| {
                try std.testing.expectEqualSlices(u8, &raw, decoded);
            } else {
                fallbacks += 1;
                if (first_fallback == null) first_fallback = i;
            }
            peak_live = @max(peak_live, shared.decoded_bytes);
        }
        std.debug.print("\nrecycled scratch churn: pooled={} blocks=512 live_limit=262144 peak_live={d} decode_fallbacks={d} first_fallback={any}\n", .{ pooled, peak_live, fallbacks, first_fallback });
        try std.testing.expect(peak_live <= BatchAsyncBlocks.max_decoded_bytes);
        try std.testing.expectEqual(@as(usize, 0), fallbacks);
    }
}

test "lsm point followup recycled scratch preserves mandatory budget" {
    const a = std.testing.allocator;
    const raw: [16 * 1024]u8 = @splat('x');
    const compressed = try @import("../../encoding/snappy.zig").encode(a, &raw);
    defer a.free(compressed);
    const read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/review-churn", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = @intCast(compressed.len), .logical_len = raw.len, .compression = .snappy, .checksum = @import("antfly_hash").Crc32.hash(compressed), .status = .ready_handle };
    for ([_]bool{ false, true }) |pooled| {
        const resources = @import("../resource_manager.zig");
        var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 400 * 1024 } });
        defer manager.deinit(a);
        var decode_budget = resources.BudgetedAllocator.init(&manager, .lsm_in_memory_state, a, 1);
        defer decode_budget.deinit();
        var mandatory = resources.BudgetedAllocator.init(&manager, .lsm_read_working_set, a, 1);
        defer mandatory.deinit();
        var mandatory_denials: usize = 0;
        var pool: LocalReader = .{};
        defer pool.deinit();
        var shared: BatchAsyncBlocks = .{ .allocator = decode_budget.allocator(), .limit = 16 };
        if (pooled) shared.workspace_config = .{ .pool = &pool, .backing = a, .manager = &manager, .io = std.testing.io, .limit = 8 * 1024 * 1024 };
        defer {
            for (&shared.entries) |*entry| entry.users = 0;
            shared.deinit();
        }
        var fallbacks: usize = 0;
        var first_fallback: ?usize = null;
        var peak_live: usize = 0;
        for (0..512) |i| {
            if (i >= 16) shared.entries[i % 16].users = 0;
            shared.prepareInsert();
            var candidate = read;
            candidate.absolute_offset = i * 16384;
            const block = shared.insert(candidate);
            block.users = 1;
            if (try shared.decodedPayload(block, compressed)) |decoded| {
                try std.testing.expectEqualSlices(u8, &raw, decoded);
            } else {
                mandatory.budget_denied = false;
                const bytes = @import("../../encoding/snappy.zig").decode(mandatory.allocator(), compressed) catch |err| denied: {
                    if (err != error.OutOfMemory or !mandatory.denied()) return err;
                    mandatory_denials += 1;
                    break :denied null;
                };
                if (bytes) |decoded| mandatory.allocator().free(decoded);
                _ = mandatory.releaseUnusedCredit();
                fallbacks += 1;
                if (first_fallback == null) first_fallback = i;
            }
            peak_live = @max(peak_live, shared.decoded_bytes);
        }
        std.debug.print("\nrecycled scratch churn: pooled={} blocks=512 live_limit=262144 peak_live={d} decode_fallbacks={d} first_fallback={any}\n", .{ pooled, peak_live, fallbacks, first_fallback });
        try std.testing.expectEqual(@as(usize, 0), mandatory_denials);
        std.debug.print("\nrecycled tight host budget: pooled={} hard_limit=409600 mandatory_decode_denials={d} pool_active_charge={d}\n", .{ pooled, mandatory_denials, manager.sliceStats(.lsm_read_working_set).used_bytes });
        try std.testing.expect(peak_live <= BatchAsyncBlocks.max_decoded_bytes);
        try std.testing.expectEqual(@as(usize, 0), fallbacks);
    }
}

test "lsm point followup oversized mandatory decode reclaims idle workspace credit" {
    const a = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    const snappy = @import("../../encoding/snappy.zig");
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 * 1024 } });
    defer manager.deinit(a);
    var mandatory = resources.BudgetedAllocator.init(&manager, .lsm_read_working_set, a, 1);
    defer mandatory.deinit();
    var pool: LocalReader = .{};
    defer pool.deinit();
    var shared: BatchAsyncBlocks = .{ .allocator = a, .workspace_config = .{ .pool = &pool, .backing = a, .manager = &manager, .io = std.testing.io, .limit = 8 * 1024 * 1024 } };
    defer shared.deinit();
    const small: [16 * 1024]u8 = @splat('x');
    const compressed = try snappy.encode(a, &small);
    defer a.free(compressed);
    var read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/oversized-fallback", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = @intCast(compressed.len), .logical_len = small.len, .compression = .snappy, .checksum = @import("antfly_hash").Crc32.hash(compressed), .status = .ready_handle };
    const block = shared.insert(read);
    try std.testing.expect(try shared.decodedPayload(block, compressed) != null);
    shared.clear(block);
    try std.testing.expect(manager.sliceStats(.lsm_read_working_set).used_bytes > 0);
    const large: [128 * 1024]u8 = @splat('y');
    const large_compressed = try snappy.encode(a, &large);
    defer a.free(large_compressed);
    read.logical_len = large.len;
    read.physical_len = @intCast(large_compressed.len);
    read.checksum = @import("antfly_hash").Crc32.hash(large_compressed);
    const large_block = shared.insert(read);
    try std.testing.expect(try shared.decodedPayload(large_block, large_compressed) == null);
    shared.reclaimIdleScratch();
    try std.testing.expectEqual(@as(usize, 0), manager.sliceStats(.lsm_read_working_set).used_bytes);
    const decoded = try snappy.decode(mandatory.allocator(), large_compressed);
    defer mandatory.allocator().free(decoded);
    try std.testing.expectEqualSlices(u8, &large, decoded);
}

test "lsm point followup inactive decoded scratch yields to mandatory admission" {
    const a = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    const snappy = @import("../../encoding/snappy.zig");
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 * 1024 } });
    defer manager.deinit(a);
    var mandatory = resources.BudgetedAllocator.init(&manager, .lsm_read_working_set, a, 1);
    defer mandatory.deinit();
    var pool: LocalReader = .{};
    defer pool.deinit();
    var shared: BatchAsyncBlocks = .{ .allocator = a, .workspace_config = .{ .pool = &pool, .backing = a, .manager = &manager, .io = std.testing.io, .limit = 8 * 1024 * 1024 } };
    defer shared.deinit();
    const small: [16 * 1024]u8 = @splat('x');
    const compressed = try snappy.encode(a, &small);
    defer a.free(compressed);
    var read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/inactive-decoded", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = @intCast(compressed.len), .logical_len = small.len, .compression = .snappy, .checksum = @import("antfly_hash").Crc32.hash(compressed), .status = .ready_handle };
    const old = shared.insert(read);
    try std.testing.expect(try shared.decodedPayload(old, compressed) != null);
    try std.testing.expectEqual(@as(usize, 0), old.users);
    // Completed owners remain occupied and cached until the batch ends or
    // the window fills. Reclamation must also release their optional buffers.
    const large: [128 * 1024]u8 = @splat('y');
    const large_compressed = try snappy.encode(a, &large);
    defer a.free(large_compressed);
    read.absolute_offset = 16384;
    read.logical_len = large.len;
    read.physical_len = @intCast(large_compressed.len);
    read.checksum = @import("antfly_hash").Crc32.hash(large_compressed);
    const block = shared.insert(read);
    try std.testing.expect(try shared.decodedPayload(block, large_compressed) == null);
    shared.reclaimIdleScratch();
    const charged = manager.sliceStats(.lsm_read_working_set).used_bytes;
    try std.testing.expectEqual(@as(usize, 0), charged);
    try std.testing.expect(old.occupied);
    try std.testing.expect(old.decoded == null);
    const decoded = try snappy.decode(mandatory.allocator(), large_compressed);
    defer mandatory.allocator().free(decoded);
    try std.testing.expectEqualSlices(u8, &large, decoded);
    std.debug.print("INACTIVE_SCRATCH_ADMISSION idle_owner_users=0 retained_charge={d} mandatory_bytes=131072 mandatory=success inactive_owner_preserved=true\n", .{charged});
}

test "lsm point followup mandatory reclamation preserves active decoded owners" {
    const a = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    const snappy = @import("../../encoding/snappy.zig");
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 128 * 1024 } });
    defer manager.deinit(a);
    var mandatory = resources.BudgetedAllocator.init(&manager, .lsm_read_working_set, a, 1);
    defer mandatory.deinit();
    var pool: LocalReader = .{};
    defer pool.deinit();
    var shared: BatchAsyncBlocks = .{ .allocator = a, .workspace_config = .{ .pool = &pool, .backing = a, .manager = &manager, .io = std.testing.io, .limit = 8 * 1024 * 1024 } };
    defer shared.deinit();
    const small: [16 * 1024]u8 = @splat('x');
    const compressed = try snappy.encode(a, &small);
    defer a.free(compressed);
    var read: AsyncPointBlockRead = .{ .candidate = .{ .run_index = 0 }, .path = "/inactive-decoded", .run_id = 1, .generation = 1, .index_handle = null, .block_index = 0, .absolute_offset = 0, .physical_len = @intCast(compressed.len), .logical_len = small.len, .compression = .snappy, .checksum = @import("antfly_hash").Crc32.hash(compressed), .status = .ready_handle };
    const old = shared.insert(read);
    try std.testing.expect(try shared.decodedPayload(old, compressed) != null);
    try std.testing.expectEqual(@as(usize, 0), old.users);
    // Completed owners remain occupied and cached until the batch ends or
    // the window fills. Reclamation must also release their optional buffers.
    read.absolute_offset = 32768;
    const active = shared.insert(read);
    active.users = 1;
    defer active.users = 0;
    const active_decoded = (try shared.decodedPayload(active, compressed)).?;
    const large: [96 * 1024]u8 = @splat('y');
    const large_compressed = try snappy.encode(a, &large);
    defer a.free(large_compressed);
    read.absolute_offset = 16384;
    read.logical_len = large.len;
    read.physical_len = @intCast(large_compressed.len);
    read.checksum = @import("antfly_hash").Crc32.hash(large_compressed);
    const block = shared.insert(read);
    try std.testing.expect(try shared.decodedPayload(block, large_compressed) == null);
    shared.reclaimIdleScratch();
    const charged = manager.sliceStats(.lsm_read_working_set).used_bytes;
    try std.testing.expect(charged > 0 and charged < 20 * 1024);
    try std.testing.expectEqualSlices(u8, &small, active_decoded);
    try std.testing.expect(active.decoded != null);
    try std.testing.expect(old.occupied);
    try std.testing.expect(old.decoded == null);
    const decoded = try snappy.decode(mandatory.allocator(), large_compressed);
    defer mandatory.allocator().free(decoded);
    try std.testing.expectEqualSlices(u8, &large, decoded);
    std.debug.print("INACTIVE_SCRATCH_ADMISSION idle_owner_users=0 retained_charge={d} mandatory_bytes=98304 mandatory=success inactive_owner_preserved=true\n", .{charged});
}

/// Exercises the real fixed-size shared pipeline without exporting an authority
/// borrow into a persistent read handle. Runtime callers still need forwarding.
pub fn qualifyControlledAsyncBatch(control: read_admission.Port) !void {
    if (!builtin.is_test) @compileError("test fixture only");
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/controlled-async-batch", .{ .storage = storage.storage(), .cache = &cache, .flush_threshold = 1, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "docs" });
    defer store.deinit();
    var key_bytes: [16][64]u8 = undefined;
    var keys: [16][]const u8 = undefined;
    var write = try store.beginWrite();
    for (&keys, 0..) |*key, i| {
        key.* = try std.fmt.bufPrint(&key_bytes[i], "document:long-shared-prefix-for-compression:{d:0>4}", .{i});
        try write.put(key.*, key.*);
    }
    try write.commit();
    var runs = [_]Run{backend.runs.at(0).*};
    var handle = try loadRunTableIndexHandle(&backend, &runs[0]);
    try std.testing.expectEqual(@as(usize, 1), handle.runTableIndex().blocks.len);
    handle.release();
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, &runs);
    defer a.free(levels);
    const empty: State = .{};
    var values: [16]?[]const u8 = @splat(null);
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    const before = backend.snapshotReadStats().table_block_loads;
    const result = (try readManySortedPointFromSnapshotAsyncWithAdmission(&backend, &empty, &.{}, &runs, groups, levels, a, &held, .{ .name = "docs" }, &keys, &values, false, .snapshot_pinned, null, null, control)).?;
    try std.testing.expectEqual(keys.len, result.hits);
    try std.testing.expectEqual(@as(usize, 0), result.misses);
    try std.testing.expectEqual(@as(u64, 1), backend.snapshotReadStats().table_block_loads - before);
    cache.invalidatePrefix("/controlled-async-batch");
    for (values, keys) |value, key| try std.testing.expectEqualStrings(key, value.?);
}

pub fn qualifyControlledNativeReadBatch(control: read_admission.Port, use_scope: bool) !void {
    if (!builtin.is_test) @compileError("test fixture only");
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/controlled-native-read-batch", .{ .storage = storage.storage(), .cache = &cache, .flush_threshold = 1, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "docs" });
    defer store.deinit();
    var key_bytes: [128][64]u8 = undefined;
    var keys: [128][]const u8 = undefined;
    var write = try store.beginWrite();
    for (&keys, 0..) |*key, i| {
        key.* = try std.fmt.bufPrint(&key_bytes[i], "document:long-shared-prefix-for-compression:{d:0>4}", .{i});
        try write.put(key.*, key.*);
    }
    try write.commit();
    var runs = [_]Run{backend.runs.at(0).*};
    var handle = try loadRunTableIndexHandle(&backend, &runs[0]);
    try std.testing.expectEqual(@as(usize, 1), handle.runTableIndex().blocks.len);
    handle.release();
    var handle_owner = try BoundReadTxn(B).open(&backend, .{ .name = "docs" });
    var read = backend_erased.readTxnFrom(a, handle_owner) catch |err| {
        handle_owner.abort();
        return err;
    };
    defer read.abort();
    var scope = try read.openReadScope(a);
    defer scope.close();
    var values: [128]?[]const u8 = @splat(null);
    const sparse = [_][]const u8{ keys[0], keys[keys.len - 1] };
    // Preserve all three existing planner choices: cursor, grouped synchronous
    // reads, and sparse asynchronous points, including reusable-scope results.
    for ([_][]const []const u8{ keys[0..16], &keys, &sparse }) |batch| {
        cache.invalidatePrefix("/controlled-native-read-batch");
        const result = if (use_scope) scope.getManySortedWithAdmission(batch, values[0..batch.len], control) else read.getManySortedWithAdmission(batch, values[0..batch.len], control);
        result catch |err| {
            for (values[0..batch.len]) |value| try std.testing.expect(value == null);
            return err;
        };
        cache.invalidatePrefix("/controlled-native-read-batch");
        for (values[0..batch.len], batch) |value, key| try std.testing.expectEqualStrings(key, value.?);
    }
}

pub fn qualifyControlledUncachedReadBatch(control: read_admission.Port, local_cache: bool, use_scope: bool) !void {
    return qualifyUncachedReadBatch(control, local_cache, use_scope, false);
}

pub fn qualifyControlledManagedUncachedReadBatch(control: read_admission.Port, local_cache: bool, use_scope: bool) !void {
    return qualifyUncachedReadBatch(control, local_cache, use_scope, true);
}

fn qualifyUncachedReadBatch(control: read_admission.Port, local_cache: bool, use_scope: bool, managed: bool) !void {
    if (!builtin.is_test) @compileError("test fixture only");
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    const path = try std.fmt.allocPrint(a, ".zig-cache/tmp/{s}/uncached-batch", .{tmp.sub_path});
    defer a.free(path);
    var storage = try storage_io.NativeStorage.init(a, .threaded);
    defer storage.deinit();
    var resources = @import("../resource_manager.zig").ResourceManager.init(.{ .identity_allocator = a });
    defer resources.deinit(a);
    var backend = try B.open(a, path, .{ .resource_manager = if (managed) &resources else null, .storage = storage.storage(), .flush_threshold = 1, .local_block_cache_enabled = local_cache });
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "docs" });
    defer store.deinit();
    var key_bytes: [128][64]u8 = undefined;
    var keys: [128][]const u8 = undefined;
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        for (&keys, 0..) |*key, i| {
            key.* = try std.fmt.bufPrint(&key_bytes[i], "document:long-shared-prefix-for-compression:{d:0>4}", .{i});
            try write.put(key.*, key.*);
        }
        try write.commit();
    }
    var handle_owner = try BoundReadTxn(B).open(&backend, .{ .name = "docs" });
    var read = backend_erased.readTxnFrom(a, handle_owner) catch |err| {
        handle_owner.abort();
        return err;
    };
    defer read.abort();
    var scope = try read.openReadScope(a);
    defer scope.close();
    var values: [128]?[]const u8 = @splat(null);
    const sparse = [_][]const u8{ keys[0], keys[keys.len - 1] };
    // Cursor, grouped and sparse plans, then warm repeat without changing the
    // snapshot. No shared Cache: local-index and local-block paths are real.
    for ([_][]const []const u8{ &sparse, &keys, keys[0..16], &sparse }) |batch| {
        const result = if (use_scope) scope.getManySortedWithAdmission(batch, values[0..batch.len], control) else read.getManySortedWithAdmission(batch, values[0..batch.len], control);
        result catch |err| {
            for (values[0..batch.len]) |value| try std.testing.expect(value == null);
            return err;
        };
        for (values[0..batch.len], batch) |value, key| try std.testing.expectEqualStrings(key, value.?);
    }
}

pub fn qualifyControlledLargeSnappyBatch(control: read_admission.Port) !void {
    if (!builtin.is_test) @compileError("test fixture only");
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/controlled-large-snappy", .{ .storage = storage.storage(), .cache = &cache, .flush_threshold = 1, .max_concurrent_point_block_reads = 16 });
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "docs" });
    defer store.deinit();
    const bytes: [128 * 1024]u8 = @splat('x');
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("huge", &bytes);
        try write.commit();
    }
    var runs = [_]Run{backend.runs.at(0).*};
    var handle = try loadRunTableIndexHandle(&backend, &runs[0]);
    try std.testing.expectEqual(@as(usize, 1), handle.runTableIndex().blocks.len);
    try std.testing.expectEqual(lsm_table_file.BlockCompression.snappy, handle.runTableIndex().blocks[0].compression);
    handle.release();
    const groups = try buildL0RunGroups(a, &runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, &runs);
    defer a.free(levels);
    const empty: State = .{};
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    var values: [2]?[]const u8 = @splat(null);
    const result = (try readManySortedPointFromSnapshotAsyncWithAdmission(&backend, &empty, &.{}, &runs, groups, levels, a, &held, .{ .name = "docs" }, &.{ "huge", "missing" }, &values, false, .transaction_owned, null, null, control)).?;
    try std.testing.expectEqual(@as(usize, 1), result.hits);
    try std.testing.expectEqual(@as(usize, 1), result.misses);
    try std.testing.expectEqualSlices(u8, &bytes, values[0].?);
    try std.testing.expect(values[1] == null);
}

pub fn qualifyControlledPointPromotion(control: read_admission.Port, fixture: usize) !void {
    if (!builtin.is_test) @compileError("test fixture only");
    const a = std.testing.allocator;
    const resources = @import("../resource_manager.zig");
    var parent = resources.ResourceManager.init(.{ .identity_allocator = a, .memory_budget = .{ .hard_limit_bytes = 2 * 1024 * 1024 } });
    defer parent.deinit(a);
    var child = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer child.deinit(a);
    try child.installMemoryParent(parent.memoryParentPort());
    var peer = resources.ResourceManager.init(.{ .identity_allocator = a });
    defer peer.deinit(a);
    try peer.installMemoryParent(parent.memoryParentPort());
    defer {
        resources.metadata_fixture.expectUsage(0, &parent, &.{ &parent, &child, &peer }) catch unreachable;
        resources.metadata_fixture.expectUsage(0, &child, &.{&child}) catch unreachable;
        std.debug.assert(parent.snapshot().memory.accounting_errors == 0);
        std.debug.assert(child.snapshot().memory.accounting_errors == 0);
    }
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    if (fixture != 4) cache.attachResourceManager(&child);
    const Backend = struct {
        allocator: Allocator,
        options: struct { cache: ?*cache_mod.Cache, resource_manager: ?*resources.ResourceManager },
    };
    var backend = Backend{ .allocator = a, .options = .{ .cache = &cache, .resource_manager = if (fixture == 5) &peer else &child } };
    var entries: [192]lsm_table_file.Entry = undefined;
    var keys: [192][80]u8 = undefined;
    for (&entries, 0..) |*entry, i| entry.* = .{ .namespace_name = "docs", .key = try std.fmt.bufPrint(&keys[i], "long-shared-document-key-prefix-for-compression:{d:0>4}", .{i}), .value = "a selected document's exact value" };
    var filter = try lsm_table_file.buildFilterAlloc(a, &entries, lsm_table_file.default_filter_config);
    defer filter.deinit(a);
    const encoded = try lsm_table_file.encodeWithFilterAllocOptions(a, &entries, filter, .{ .block_compression = if (fixture == 3) .none else .snappy_adaptive });
    defer a.free(encoded);
    var index = try lsm_table_file.decodeIndexAlloc(a, encoded);
    defer index.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), index.blocks.len);
    const window = index.blockWindow(0);
    if (fixture != 3) try std.testing.expectEqual(lsm_table_file.BlockCompression.prefix_snappy, window.compression);
    const offset = index.entry_data_start + window.physicalRelativeOffset();
    const payload = encoded[offset..][0..window.physicalLen()];
    const expected = try lsm_table_file.decodeBlockPayloadAlloc(a, window.compression, payload, window.len, window.checksum);
    defer a.free(expected);
    const prefix = if (fixture == 1) try @import("../../encoding/snappy.zig").decode(a, payload) else null;
    defer if (prefix) |owned| a.free(owned);
    const prepared: ?DecodedPointBlock = switch (fixture) {
        0 => null,
        1 => .{ .prefix_records = prefix.? },
        else => .{ .raw_rows = expected },
    };
    const path = "/controlled-point-promotion";
    try promoteCachedPointBlockWithAdmission(&backend, path, 1, 1, window, offset, payload, prepared, control);
    if (fixture == 4) {
        try std.testing.expect(cache.retainRunTableBlock(path, 1, 1, offset, window.physicalLen()) == null);
        try resources.metadata_fixture.expectUsage(@as(u64, 0), &parent, &.{ &parent, &child, &peer });
        return;
    }
    var retained = cache.retainRunTableBlock(path, 1, 1, offset, window.physicalLen()) orelse return error.TestUnexpectedResult;
    defer retained.release();
    try std.testing.expectEqualSlices(u8, expected, retained.runTableBlock());
    try cache.drainLoadCleanupWithAdmission(control);
    try std.testing.expectEqual(@as(u64, 0), child.sliceStats(.lsm_read_working_set).used_bytes);
    try resources.metadata_fixture.expectUsage(@as(u64, 0), &peer, &.{&peer});
    try std.testing.expectEqual(child.snapshot().memory.used_bytes, parent.snapshot().memory.used_bytes);
    try std.testing.expectEqual(child.snapshot().memory.peak_bytes, parent.snapshot().memory.peak_bytes);
    try std.testing.expect(parent.snapshot().memory.peak_bytes < @as(u64, window.len) * 2);
}

pub fn qualifyControlledPointResultCopies(control: read_admission.Port, fixture: usize) !void {
    if (!builtin.is_test) @compileError("test fixture only");
    const a = std.testing.allocator;
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    const seed = try held.copies.copy(a, &held, "seed");
    const previous_buffer = held.copies.buffer;
    const previous_used = held.copies.used;
    const previous_count = held.items.len;
    const large: [192 * 1024]u8 = @splat('v');
    const key: [128 * 1024]u8 = @splat('k');
    const name: [65 * 1024]u8 = @splat('n');
    if (fixture == 3) {
        const entry: lsm_table_file.Entry = .{ .namespace_name = &name, .key = &key, .value = &large };
        const owned = copyTableEntryWithAdmission(a, entry, control) catch |err| {
            const retry = try copyTableEntry(a, entry);
            defer a.free(retry.bytes);
            try std.testing.expectEqualSlices(u8, &name, retry.entry.namespace_name.?);
            try std.testing.expectEqualSlices(u8, &key, retry.entry.key);
            try std.testing.expectEqualSlices(u8, &large, retry.entry.value);
            try std.testing.expectEqualStrings("seed", seed);
            return err;
        };
        defer a.free(owned.bytes);
        try std.testing.expectEqualSlices(u8, &name, owned.entry.namespace_name.?);
        try std.testing.expectEqualSlices(u8, &key, owned.entry.key);
        try std.testing.expectEqualSlices(u8, &large, owned.entry.value);
    } else {
        const value = large[0..switch (fixture) {
            0 => @as(usize, 32),
            1 => 512,
            else => large.len,
        }];
        const copied = held.copies.copyWithAdmission(a, &held, value, control) catch |err| {
            try std.testing.expectEqualStrings("seed", seed);
            if (held.copies.buffer.ptr == previous_buffer.ptr) try std.testing.expectEqual(previous_used, held.copies.used) else try std.testing.expectEqual(@as(usize, 0), held.copies.used);
            if (fixture == 2) try std.testing.expectEqual(previous_count, held.items.len);
            const retry = try held.copies.copy(a, &held, value);
            try std.testing.expectEqualSlices(u8, value, retry);
            try std.testing.expectEqualStrings("seed", seed);
            return err;
        };
        try std.testing.expectEqualSlices(u8, value, copied);
    }
    try std.testing.expectEqualStrings("seed", seed);
}

pub fn qualifyControlledOwnedPointBatch(control: read_admission.Port, fixture: usize) !void {
    if (!builtin.is_test) @compileError("test fixture only");
    const B = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    const persisted = fixture % 2 == 1;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var cache = cache_mod.Cache.init(a, 1024 * 1024);
    defer cache.deinit();
    var backend = try B.open(a, "/controlled-owned-point-copy", .{ .storage = storage.storage(), .cache = &cache, .flush_threshold = if (persisted) 1 else 1024 * 1024, .table_block_compression = .none, .max_concurrent_point_block_reads = if (fixture >= 2) 1 else 16 });
    defer backend.close();
    var store = try backend.runtimeStore(a, .{ .name = "docs" });
    defer store.deinit();
    const large: [192 * 1024]u8 = @splat('v');
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("a:small", "seed");
        try write.put("b:large", &large);
        try write.commit();
    }
    const runs = try a.alloc(Run, backend.runs.count());
    defer a.free(runs);
    for (runs, 0..) |*run, i| run.* = backend.runs.at(i).*;
    if (persisted) try std.testing.expect(runs.len > 0) else try std.testing.expectEqual(@as(usize, 0), runs.len);
    const groups = try buildL0RunGroups(a, runs);
    defer deinitRunGroups(a, groups);
    const levels = try buildLowerLevels(a, runs);
    defer a.free(levels);
    var held: PointResultValues = .empty;
    defer releaseHeldValues(&held, a);
    var values: [4]?[]const u8 = @splat(null);
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const result = try readManySortedPointFromSnapshotWithScratch(&backend, &backend.mutable, &.{}, runs, groups, levels, a, null, &held, .{ .name = "docs", .own_source_point_results = true }, &.{ "a:small", "b:large", "b:large", "z:missing" }, &values, false, scratch.allocator(), control);
    try std.testing.expectEqual(@as(usize, 3), result.hits);
    try std.testing.expectEqual(@as(usize, 1), result.misses);
    cache.invalidatePrefix("/controlled-owned-point-copy");
    try std.testing.expectEqualStrings("seed", values[0].?);
    try std.testing.expectEqualSlices(u8, &large, values[1].?);
    try std.testing.expectEqualSlices(u8, &large, values[2].?);
    try std.testing.expect(values[3] == null);
}
test "lsm presence probes avoid value copies across source tiers (#1095)" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    for ([_]lsm_table_file.CompressionPolicy{ .none, .snappy_adaptive }) |compression| {
        for ([_]bool{ false, true }) |with_cache| {
            var storage = storage_io.MemoryStorage.init(a);
            defer storage.deinit();
            var cache = cache_mod.Cache.init(a, 16 * 1024 * 1024);
            defer cache.deinit();
            var backend = try Backend.open(a, "/presence-probe", .{
                .storage = storage.storage(),
                .cache = if (with_cache) &cache else null,
                .flush_threshold = 8 * 1024 * 1024,
                .compact_threshold_runs = 100,
                .l0_overlap_compact_threshold_runs = 100,
                .table_block_compression = compression,
            });
            defer backend.close();
            var store = try backend.runtimeStore(a, .{ .name = "docs" });
            defer store.deinit();
            const large = try a.alloc(u8, 1024 * 1024);
            defer a.free(large);
            @memset(large, 'v');
            {
                var write = try store.beginWrite();
                errdefer write.abort();
                try write.put("large", large);
                try write.put("small", "value");
                try write.put("empty", "");
                // Shared long keys force the prefix reader, while the wide
                // standalone row exercises raw/Snappy blocks.
                for (0..64) |i| {
                    var key_buf: [128]u8 = undefined;
                    const key = try std.fmt.bufPrint(&key_buf, "tenant:shared-prefix-for-presence-metadata-only-read:document:{d:0>3}", .{i});
                    try write.put(key, "value");
                }
                try write.commit();
            }
            for (0..3) |tier| {
                if (tier == 1) {
                    backend.options.flush_threshold = 1;
                    var write = try store.beginWrite();
                    errdefer write.abort();
                    try write.put("trigger", "freeze");
                    try write.commit();
                } else if (tier == 2) {
                    while (try backend.runMaintenanceStep()) {}
                    try std.testing.expect(run_store.count(&backend) != 0);
                }
                const before = backend.snapshotReadStats().point_value_copies;
                var probe = try store.beginProbe();
                defer probe.abort();
                for (0..2) |pass| {
                    if (tier == 2 and pass == 1 and with_cache) {
                        // Warm decoded blocks exercise the borrowed-cache
                        // path as well as the first cold compressed lookup.
                        for (0..run_store.count(&backend)) |i| {
                            var run = run_store.at(&backend, i).*;
                            var index_handle = try loadRunTableIndexHandle(&backend, &run);
                            defer index_handle.release();
                            const index = index_handle.runTableIndex();
                            for (0..index.blocks.len) |block_index| {
                                var block_handle = try loadRunTableBlockHandle(&backend, &run, index, index.blockWindow(block_index), true);
                                block_handle.release();
                            }
                        }
                    }
                    try std.testing.expect(try probe.contains("large"));
                    try std.testing.expect(try probe.contains("small"));
                    try std.testing.expect(try probe.contains("empty"));
                    try std.testing.expect(try probe.contains("tenant:shared-prefix-for-presence-metadata-only-read:document:017"));
                    try std.testing.expect(!try probe.contains("larg"));
                    try std.testing.expect(!try probe.contains("missing"));
                }
                try std.testing.expectEqual(before, backend.snapshotReadStats().point_value_copies);
            }
            // A newer tombstone decides presence even when an older SST has
            // the value, and a later resurrection must reverse that decision.
            for ([_]bool{ false, true }) |resurrect| {
                {
                    var write = try store.beginWrite();
                    errdefer write.abort();
                    if (resurrect) try write.put("large", large) else try write.delete("large");
                    try write.commit();
                }
                for (0..2) |phase| {
                    if (phase == 1) while (try backend.runMaintenanceStep()) {};
                    const before = backend.snapshotReadStats().point_value_copies;
                    var probe = try store.beginProbe();
                    defer probe.abort();
                    try std.testing.expectEqual(resurrect, try probe.contains("large"));
                    try std.testing.expectEqual(before, backend.snapshotReadStats().point_value_copies);
                }
            }
        }
    }
}

test "public graph metric borrowed wire LSM write presence and key cursor preserve snapshots" {
    const Backend = @import("../lsm_backend.zig").Backend;
    const a = std.testing.allocator;
    var storage = storage_io.MemoryStorage.init(a);
    defer storage.deinit();
    var backend = try Backend.open(a, "/authority-presence", .{ .storage = storage.storage(), .flush_threshold = 1 });
    defer backend.close();
    var store = try backend.runtimeStore(a, .{});
    defer store.deinit();
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("a", "old");
        try write.put("c", "old-c");
        try write.commit();
    }
    while (try backend.runMaintenanceStep()) {}
    var pinned = try store.beginRead();
    defer pinned.abort();
    {
        var write = try store.beginWrite();
        defer write.abort();
        try std.testing.expect(try write.contains("a"));
        try std.testing.expect(!try write.contains("b"));
        try write.delete("a");
        try std.testing.expect(!try write.contains("a"));
        try write.put("b", "pending");
        try std.testing.expect(try write.contains("b"));
    }
    {
        var write = try store.beginWrite();
        errdefer write.abort();
        try write.put("a", "new");
        try write.put("b", "new-b");
        try write.commit();
    }
    var cursor = try pinned.openCursor();
    defer cursor.close();
    cursor.setKeysOnly();
    const Check = struct {
        fn entry(actual: ?backend_adapter.Entry, key: []const u8) !void {
            try std.testing.expectEqualStrings(key, actual.?.key);
            try std.testing.expectEqual(@as(usize, 0), actual.?.value.len);
        }
    };
    try Check.entry(try cursor.first(), "a");
    try Check.entry(try cursor.next(), "c");
    try Check.entry(try cursor.last(), "c");
    try Check.entry(try cursor.prev(), "a");
    try Check.entry(try cursor.seekAtOrBefore("b"), "a");
    try std.testing.expectEqualStrings("old", try pinned.get("a"));
}

test "WAL allocator owner point scratch preserves budget and ABI roots outside result arenas" {
    const resources = @import("../resource_manager.zig");
    var manager = resources.ResourceManager.init(.{ .memory_budget = .{ .hard_limit_bytes = 1024 * 1024 }, .identity_allocator = std.testing.allocator });
    defer manager.deinit(std.testing.allocator);
    var budget = resources.BudgetedAllocator.initReclaiming(&manager, .lsm_in_memory_state, std.testing.allocator, 1);
    defer budget.deinit();
    budget.credit_quantum = 1;
    var arena = std.heap.ArenaAllocator.init(budget.threadSafeAllocator());
    defer arena.deinit();
    const scratch = runtimeScratchAllocator(arena.allocator());
    const bytes = try scratch.alloc(u8, 64);
    try std.testing.expectEqual(@as(u64, 64), budget.liveBytesThreadSafe());
    try std.testing.expect(arena.state.used_list == null and arena.state.free_list == null);
    scratch.free(bytes);
    try std.testing.expectEqual(@as(u64, 0), budget.liveBytesThreadSafe());
    try std.testing.expectError(error.OutOfMemory, scratch.alloc(u8, 2 * 1024 * 1024));
    var root = budget.threadSafeNoReclaimAllocator();
    const descriptor = @import("runtime_memory_abi").Allocator.fromStd(&root);
    const bridged = runtimeScratchAllocator(descriptor.asStd());
    try std.testing.expectError(error.OutOfMemory, bridged.alloc(u8, 2 * 1024 * 1024));
    try std.testing.expectEqual(@as(u64, 0), budget.liveBytesThreadSafe());
}

test "LSM streaming block keeps one source lease and bounded physical windows across verification passes" {
    const a = std.testing.allocator;
    const Fixture = struct {
        bytes: []const u8,
        opens: usize = 0,
        closes: usize = 0,
        uses: usize = 0,
        largest: usize = 0,
        total: usize = 0,
        reads: usize = 0,
        fail_at: ?usize = null,
        mutate_second_pass: bool = false,
        var current: *@This() = undefined;
        fn open(_: *anyopaque, _: Allocator, _: []const u8) !@import("../../segment_source.zig").Source {
            const self = current;
            self.opens += 1;
            return .{ .ranges = .{ .ptr = self, .length = self.bytes.len, .read_into = read, .close = close, .acquire_use = acquire, .release_use = release } };
        }
        fn direct(_: *anyopaque, _: []const u8, offset: u64, out: []u8) !void {
            return read(current, offset, out);
        }
        fn controlled(_: *anyopaque, _: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
            try control.checkpoint();
            try read(current, offset, out);
            try control.checkpoint();
        }
        fn acquire(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.uses += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.uses -= 1;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.uses == 0);
            self.closes += 1;
        }
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            self.largest = @max(self.largest, out.len);
            if (self.fail_at == self.reads) {
                @memset(out, 0xff);
                return error.TestWindowFault;
            }
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
            if (self.mutate_second_pass and self.total >= self.bytes.len and offset + out.len == self.bytes.len)
                out[out.len - 1] ^= 1;
            self.total += out.len;
        }
    };
    const plain = try a.alloc(u8, 256 * 1024 + 31);
    defer a.free(plain);
    var random = std.Random.DefaultPrng.init(92814);
    random.random().bytes(plain);
    const encoded = try @import("../../encoding/snappy.zig").encode(a, plain);
    defer a.free(encoded);
    for ([_]lsm_table_file.BlockCompression{ .none, .snappy }) |compression| {
        const bytes = if (compression == .none) plain else encoded;
        for ([_]bool{ false, true }) |leased| {
            for (0..5) |mode| {
                var memory = storage_io.MemoryStorage.init(a);
                defer memory.deinit();
                var fixture: Fixture = .{ .bytes = bytes, .fail_at = if (mode == 1) 2 else null, .mutate_second_pass = mode == 2 and compression == .snappy };
                var storage = memory.storage();
                var vtable = storage.vtable.*;
                vtable.open_leased_immutable_source = if (leased) Fixture.open else null;
                vtable.read_file_range_into = Fixture.direct;
                vtable.read_file_range_into_with_admission = Fixture.controlled;
                storage.vtable = &vtable;
                Fixture.current = &fixture;
                var backend = @import("../lsm_backend.zig").Backend.init(a, .{ .storage = storage, .local_decode_working_bytes = plain.len + 140 * 1024 });
                var backend_live = true;
                defer if (backend_live) backend.close();
                const Budget = struct {
                    checks: usize = 0,
                    pub fn waitForProgress(_: *@This()) !void {
                        return error.TestUnexpectedWindowWait;
                    }
                    pub fn checkpoint(self: *@This()) !void {
                        self.checks += 1;
                        if (self.checks >= 12) return error.TestOriginalWindowRejection;
                    }
                };
                var budget: Budget = .{};
                var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
                const authority = adapter.payload();
                var refused = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
                // The test calls the exact production loader on an immutable source;
                // no filesystem paths are opened by this initialized backend.
                const result = loadDecodedLocalBlockWithAdmission(&backend, if (mode == 4) refused.allocator() else a, "immutable-run", 0, .{ .relative_offset = 0, .len = @intCast(plain.len), .physical_len = @intCast(bytes.len), .compression = compression, .checksum = @import("antfly_hash").Crc32.hash(bytes) }, if (mode == 3) .init(&authority) else null);
                if (mode == 3) {
                    try std.testing.expectError(error.TestOriginalWindowRejection, result);
                } else if (mode == 4) {
                    try std.testing.expectError(error.OutOfMemory, result);
                } else if (mode == 1) {
                    try std.testing.expectError(error.TestWindowFault, result);
                } else if (mode == 2 and compression == .snappy) {
                    try std.testing.expectError(error.TableBlockChecksumMismatch, result);
                } else {
                    const output = try result;
                    defer a.free(output);
                    try std.testing.expectEqualSlices(u8, plain, output);
                    try std.testing.expectEqual(bytes.len * @as(usize, if (compression == .none) 1 else 2), fixture.total);
                    try std.testing.expectEqual(fixture.total, backend.snapshotReadStats().table_block_bytes);
                }
                try std.testing.expect(fixture.largest <= 64 * 1024);
                if (mode == 3) {
                    try std.testing.expect(fixture.opens <= @as(usize, if (leased) 1 else 0));
                } else try std.testing.expectEqual(@as(usize, if (leased) 1 else 0), fixture.opens);
                try std.testing.expectEqual(@as(usize, 0), fixture.uses);
                try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
                backend.close();
                backend_live = false;
                try std.testing.expectEqual(fixture.opens, fixture.closes);
            }
        }
    }
}

fn qualifyOwnedRunBlock(comptime fail: bool) !void {
    const a = std.testing.allocator;
    const scheduling = @import("../../common/runnable_scheduler.zig");
    const Backend = @import("../lsm_backend.zig").Backend;
    const Owner = OwnedRunBlockRead(Backend);
    try std.testing.expect(@sizeOf(Owner) < 2048);
    const Scope = struct {
        execution: scheduling.Execution = .{},
        valid: bool = true,
        reject: bool = false,
        pub fn checkLifetime(self: *@This()) !void {
            if (!self.valid) return error.TestExpiredStepBorrow;
            if (self.reject) return error.TestOwnedReadOriginalRejection;
            try self.execution.checkLifetime();
        }
        pub fn checkpoint(self: *@This()) !void {
            try self.checkLifetime();
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedOwnerWait;
        }
        fn begin(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkLifetime();
            try self.execution.park(.resource_blocked);
        }
        fn end(raw: *anyopaque) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            try self.checkLifetime();
            try self.execution.enter(.query);
        }
        pub fn executionBoundary(self: *@This()) ?@import("../../common/execution_wait.zig").Boundary {
            return .{ .ptr = self, .begin = begin, .end = end };
        }
    };
    const Fixture = struct {
        bytes: []const u8,
        total: usize = 0,
        uses: usize = 0,
        closes: usize = 0,
        reads: usize = 0,
        fail_after: ?usize = null,
        reject_original: bool = false,
        scope: ?*Scope = null,
        var current: *@This() = undefined;
        fn open(_: *anyopaque, _: Allocator, path: []const u8) !@import("../../segment_source.zig").Source {
            try std.testing.expectEqualStrings("immutable-owned-run", path);
            return .{ .ranges = .{ .ptr = current, .length = current.bytes.len, .read_into = read, .close = close, .acquire_use = acquire, .release_use = release } };
        }
        fn read(raw: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.reads += 1;
            try std.testing.expect(out.len <= 64 * 1024);
            if (self.fail_after) |limit| if (self.total >= limit) {
                @memset(out, 0xaa);
                if (self.reject_original) self.scope.?.reject = true;
                return error.TestOwnedReadFault;
            };
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
            self.total += out.len;
        }
        fn direct(_: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
            try std.testing.expectEqualStrings("immutable-owned-run", path);
            return read(current, offset, out);
        }
        fn controlled(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
            try control.checkpoint();
            try direct(ptr, path, offset, out);
            try control.checkpoint();
        }
        fn acquire(raw: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.uses += 1;
            return true;
        }
        fn release(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            self.uses -= 1;
        }
        fn close(raw: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(raw));
            std.debug.assert(self.uses == 0);
            self.closes += 1;
        }
    };
    const plain = try a.alloc(u8, 128 * 1024 + 53);
    defer a.free(plain);
    var random = std.Random.DefaultPrng.init(293061);
    random.random().bytes(plain);
    const encoded = try @import("../../encoding/snappy.zig").encode(a, plain);
    defer a.free(encoded);
    // One valid prefix record with a long value, used in both prefix variants.
    const prefix_raw = try a.alloc(u8, 16 + plain.len);
    defer a.free(prefix_raw);
    prefix_raw[0] = 0;
    std.mem.writeInt(u32, prefix_raw[1..5], 0, .little);
    std.mem.writeInt(u32, prefix_raw[5..9], 3, .little);
    std.mem.writeInt(u32, prefix_raw[9..13], @intCast(plain.len), .little);
    @memcpy(prefix_raw[13..16], "key");
    @memcpy(prefix_raw[16..], plain);
    const prefix = try a.alloc(u8, 24 + 20 + plain.len + 4);
    defer a.free(prefix);
    @memcpy(prefix[0..8], "ALSMPFX1");
    std.mem.writeInt(u32, prefix[8..12], 1, .little);
    std.mem.writeInt(u32, prefix[12..16], 16, .little);
    std.mem.writeInt(u32, prefix[16..20], 1, .little);
    std.mem.writeInt(u32, prefix[20..24], @intCast(20 + plain.len), .little);
    prefix[24] = 0;
    std.mem.writeInt(u32, prefix[25..29], 0, .little);
    std.mem.writeInt(u32, prefix[29..33], 0, .little);
    std.mem.writeInt(u32, prefix[33..37], 3, .little);
    std.mem.writeInt(u32, prefix[37..41], @intCast(plain.len), .little);
    @memcpy(prefix[41..44], "key");
    @memcpy(prefix[44..][0..plain.len], plain);
    std.mem.writeInt(u32, prefix[44 + plain.len ..][0..4], 0, .little);
    const prefix_snappy = try @import("../../encoding/snappy.zig").encode(a, prefix);
    defer a.free(prefix_snappy);
    for ([_]lsm_table_file.BlockCompression{ .none, .snappy, .prefix, .prefix_snappy }) |compression| {
        const bytes = switch (compression) {
            .none => plain,
            .snappy => encoded,
            .prefix => prefix,
            .prefix_snappy => prefix_snappy,
        };
        const expected = if (compression == .prefix or compression == .prefix_snappy) prefix_raw else plain;
        for ([_]bool{ false, true }) |leased| {
            for (0..@as(usize, if (fail) 3 else 1)) |mode| {
                var scheduler = try scheduling.Scheduler.init(.{ .slots = 2 });
                defer scheduler.close();
                var fixture: Fixture = .{ .bytes = bytes, .fail_after = if (!fail or mode == 2) null else if (compression == .snappy) bytes.len + 64 * 1024 else 4093, .reject_original = mode == 1 };
                Fixture.current = &fixture;
                var memory = storage_io.MemoryStorage.init(a);
                defer memory.deinit();
                var storage = memory.storage();
                var table = storage.vtable.*;
                table.open_leased_immutable_source = if (leased) Fixture.open else null;
                table.read_file_range_into = Fixture.direct;
                table.read_file_range_into_with_admission = Fixture.controlled;
                storage.vtable = &table;
                var backend = Backend.init(a, .{ .storage = storage, .local_decode_working_bytes = expected.len * 4 + 256 * 1024 });
                var backend_live = true;
                defer if (backend_live) backend.close();
                var path = "immutable-owned-run".*;
                var owners: [2]?Owner = .{ try Owner.init(&backend, a, &path, 0, .{ .relative_offset = 0, .len = @intCast(expected.len), .physical_len = @intCast(bytes.len), .compression = compression, .checksum = @import("antfly_hash").Crc32.hash(bytes) }, null), null };
                @memset(&path, 0xaa);
                var active: usize = 0;
                defer if (owners[active]) |*owner| owner.deinit();
                try std.testing.expectError(error.InvalidArgument, owners[active].?.takeOutput());
                try std.testing.expectError(error.InvalidArgument, owners[active].?.step(0, null));
                try std.testing.expectError(error.InvalidArgument, owners[active].?.step(64 * 1024 + 1, null));
                var steps: usize = 0;
                while (true) {
                    steps += 1;
                    try std.testing.expect(steps < 1024);
                    var scope: Scope = .{};
                    try scope.execution.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .query);
                    var adapter: read_admission.Adapter(*Scope) = .{ .budget = &scope };
                    const payload = adapter.payload();
                    fixture.scope = &scope;
                    const before = fixture.total;
                    const allocated_before = owners[active].?.output != null;
                    const input_before = owners[active].?.cursor.decoder.input_offset;
                    const raw_before = owners[active].?.raw_offset;
                    const result = owners[active].?.step(4093, .init(&payload));
                    if (!allocated_before and owners[active].?.output != null) {
                        // Allocation is a distinct schedulable step, with no
                        // physical input or codec progress folded into it.
                        try std.testing.expectEqual(before, fixture.total);
                        try std.testing.expectEqual(input_before, owners[active].?.cursor.decoder.input_offset);
                        try std.testing.expectEqual(raw_before, owners[active].?.raw_offset);
                    }
                    scope.execution.finish();
                    scope.valid = false;
                    fixture.scope = null;
                    try std.testing.expect(fixture.total - before <= 128 * 1024);
                    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().runnable_slots);
                    try std.testing.expectEqual(@as(usize, 0), scheduler.stats().operations);
                    var health: scheduling.Execution = .{};
                    try health.start(&scheduler, .{ .io = std.testing.io, .allow_wait = false }, .control);
                    health.finish();
                    // Move progress out of the stack slot used by this step.
                    // The old slot is destroyed without retiring the moved owner.
                    owners[1 - active] = owners[active];
                    owners[active] = null;
                    active = 1 - active;
                    if (fail and mode == 2 and owners[active].?.output != null and
                        (owners[active].?.raw_offset != 0 or owners[active].?.cursor.decoder.output_offset != 0 or owners[active].?.prefix_expander.output_offset != 0))
                    {
                        owners[active].?.deinit();
                        try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
                        try std.testing.expectEqual(@as(usize, 0), fixture.uses);
                        break;
                    }
                    if (result) |status| {
                        if (status == .pending) continue;
                        try std.testing.expect(!fail);
                        try std.testing.expectEqual(bytes.len * @as(usize, if (compression == .snappy) 2 else 1), fixture.total);
                        try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
                        try std.testing.expectEqual(@as(usize, 0), fixture.uses);
                        backend.close();
                        backend_live = false;
                        backend = undefined;
                        try std.testing.expectEqual(Owner.Status.ready, try owners[active].?.step(1, null));
                        const output = try owners[active].?.takeOutput();
                        defer a.free(output);
                        try std.testing.expectEqualSlices(u8, expected, output);
                        try std.testing.expectError(error.InvalidArgument, owners[active].?.takeOutput());
                        break;
                    } else |err| {
                        try std.testing.expect(fail and mode != 2);
                        const cause = if (mode == 1) error.TestOwnedReadOriginalRejection else error.TestOwnedReadFault;
                        try std.testing.expectEqual(cause, err);
                        try std.testing.expect(owners[active].?.output == null and owners[active].?.reader == null and owners[active].?.work == null);
                        try std.testing.expectEqual(@as(usize, 0), fixture.uses);
                        try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
                        backend.close();
                        backend_live = false;
                        backend = undefined;
                        try std.testing.expectError(cause, owners[active].?.step(1, null));
                        break;
                    }
                }
            }
        }
    }
}

test "LSM owned block read moves between finite worker steps with fresh borrows and detached verified output" {
    try qualifyOwnedRunBlock(false);
}

test "LSM owned block read retires partial state on failure rejection and abandonment without a live grant" {
    try qualifyOwnedRunBlock(true);
}

const OwnedValueTestStorage = struct {
    fn read(raw: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
        const memory: *storage_io.MemoryStorage = @ptrCast(@alignCast(raw));
        const platform = @import("antfly_platform");
        platform.sync.lockYielding(&memory.mutex);
        defer memory.mutex.unlock();
        const bytes = memory.files.get(path) orelse return error.FileNotFound;
        if (offset > bytes.len or out.len > bytes.len - offset) return error.EndOfStream;
        @memcpy(out, bytes[@intCast(offset)..][0..out.len]);
    }
    fn controlled(raw: *anyopaque, path: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
        try control.checkpoint();
        try read(raw, path, offset, out);
        try control.checkpoint();
    }
};

fn drainOwnedValueForTest(owner: anytype) !OwnedNamespaceValue {
    const Budget = struct {
        valid: bool = true,
        pub fn checkpoint(self: *@This()) !void {
            if (!self.valid) return error.TestExpiredNamespaceBorrow;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedNamespaceWait;
        }
    };
    var steps: usize = 0;
    while (true) {
        steps += 1;
        try std.testing.expect(steps < 4096);
        var budget: Budget = .{};
        var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
        const payload = adapter.payload();
        const state = try owner.step(4093, .init(&payload));
        budget.valid = false;
        if (state == .ready) return owner.takeOutput();
    }
}

test "LSM owned namespace read preserves captured versions namespace isolation and detached values across source families" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    const plain = try a.alloc(u8, 128 * 1024 + 53);
    defer a.free(plain);
    @memset(plain, 'v');
    for ([_]bool{ false, true }) |private| {
        test_private_read_versions = private;
        defer test_private_read_versions = false;
        for (0..5) |mode| {
            var memory = storage_io.MemoryStorage.init(a);
            defer memory.deinit();
            var storage = memory.storage();
            var table = storage.vtable.*;
            table.read_file_range_into = OwnedValueTestStorage.read;
            table.read_file_range_into_with_admission = OwnedValueTestStorage.controlled;
            storage.vtable = &table;
            var backend = if (mode == 3) Backend.init(a, .{ .wal_enabled = false, .flush_threshold = 1, .compact_threshold_runs = 10000 }) else try Backend.open(a, "/owned-value", .{ .storage = storage, .wal_enabled = false, .flush_threshold = if (mode == 0 or mode == 4) std.math.maxInt(usize) else 1, .compact_threshold_runs = 10000, .table_block_compression = if (mode == 1) .none else .snappy_adaptive, .read_snapshot_rotate_mutable_bytes = if (mode == 4) 1 else 0 });
            var backend_live = true;
            defer if (backend_live) backend.close();
            {
                var write = try backend.beginWrite();
                errdefer write.abort();
                try write.put(.{ .name = "docs" }, "key", plain);
                try write.put(.{ .name = "other" }, "key", "isolated");
                try write.commit();
            }
            var name = "docs".*;
            var key = "key".*;
            var owner = try backend.beginOwnedValueRead(a, .{ .name = &name }, &key, null);
            defer owner.deinit();
            if (mode == 3) {
                try std.testing.expect(backend.runs.count() > 0);
                try std.testing.expect(backend.runs.at(0).state != null);
            }
            if (mode == 4) {
                try std.testing.expect(owner.txn.?.immutable_memtables.len > 0);
                try std.testing.expectEqual(@as(usize, 0), owner.txn.?.mutable_snapshot.entryCount());
            }
            @memset(&name, 0xaa);
            @memset(&key, 0xaa);
            {
                var write = try backend.beginWrite();
                errdefer write.abort();
                try write.put(.{ .name = "docs" }, "key", "newer");
                try write.commit();
            }
            // Move the operation, including its snapshot and directory cursor.
            var moved = owner;
            owner = undefined;
            defer moved.deinit();
            // Prevent the original deferred cleanup from touching moved state.
            owner = .{ .backend = undefined, .allocator = a, .txn = null, .namespace = .{}, .key = &.{}, .phase = .closed };
            var value = try drainOwnedValueForTest(&moved);
            defer value.deinit();
            try std.testing.expectEqualSlices(u8, plain, value.bytes());
            try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
            try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
            var other = try backend.beginOwnedValueRead(a, .{ .name = "other" }, "key", null);
            defer other.deinit();
            var isolated = try drainOwnedValueForTest(&other);
            defer isolated.deinit();
            try std.testing.expectEqualStrings("isolated", isolated.bytes());
            backend.close();
            backend_live = false;
            backend = undefined;
            try std.testing.expectEqualSlices(u8, plain, value.bytes());
            try std.testing.expectError(error.InvalidArgument, moved.takeOutput());
        }
    }
}

test "LSM owned namespace read honors newest tombstones and retires snapshot and partial result on rejection" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    var memory = storage_io.MemoryStorage.init(a);
    defer memory.deinit();
    var storage = memory.storage();
    var table = storage.vtable.*;
    table.read_file_range_into = OwnedValueTestStorage.read;
    table.read_file_range_into_with_admission = OwnedValueTestStorage.controlled;
    storage.vtable = &table;
    var backend = try Backend.open(a, "/owned-value-retire", .{ .storage = storage, .wal_enabled = false, .flush_threshold = 1, .compact_threshold_runs = 10000 });
    defer backend.close();
    const value: [128 * 1024]u8 = @splat('x');
    {
        var write = try backend.beginWrite();
        errdefer write.abort();
        try write.put(.{}, "removed", "old");
        try write.put(.{}, "large", &value);
        try write.commit();
    }
    {
        var write = try backend.beginWrite();
        errdefer write.abort();
        try write.delete(.{}, "removed");
        try write.commit();
    }
    for ([_][]const u8{ "removed", "missing" }) |key| {
        var owner = try backend.beginOwnedValueRead(a, .{}, key, null);
        defer owner.deinit();
        try std.testing.expectError(error.NotFound, drainOwnedValueForTest(&owner));
        try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
        try std.testing.expectError(error.NotFound, owner.step(1, null));
    }
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            return error.TestOwnedNamespaceRejection;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedNamespaceWait;
        }
    };
    var owner = try backend.beginOwnedValueRead(a, .{}, "large", null);
    defer owner.deinit();
    var steps: usize = 0;
    while (owner.block == null or owner.block.?.output == null) {
        steps += 1;
        try std.testing.expect(steps < 256);
        try std.testing.expectEqual(@TypeOf(owner).Status.pending, try owner.step(4093, null));
    }
    var budget: Budget = .{};
    var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    try std.testing.expectError(error.TestOwnedNamespaceRejection, owner.step(4093, .init(&payload)));
    try std.testing.expect(owner.txn == null and owner.block == null and owner.result == null);
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
}

test "LSM owned namespace read pending cleanup joins backend close without a current execution grant" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    for ([_]bool{ false, true }) |flushed| {
        var memory = storage_io.MemoryStorage.init(a);
        defer memory.deinit();
        var storage = memory.storage();
        var table = storage.vtable.*;
        table.read_file_range_into = OwnedValueTestStorage.read;
        table.read_file_range_into_with_admission = OwnedValueTestStorage.controlled;
        storage.vtable = &table;
        var backend = try Backend.open(a, "/owned-close", .{ .storage = storage, .wal_enabled = false, .flush_threshold = if (flushed) 1 else std.math.maxInt(usize), .compact_threshold_runs = 10000 });
        var backend_live = true;
        defer if (backend_live) backend.close();
        const value: [128 * 1024]u8 = @splat('z');
        {
            var write = try backend.beginWrite();
            errdefer write.abort();
            try write.put(.{}, "large", &value);
            try write.commit();
        }
        var owner = try backend.beginOwnedValueRead(a, .{}, "large", null);
        defer owner.deinit();
        while (if (flushed) owner.block == null or owner.block.?.output == null else owner.copy_offset == 0)
            try std.testing.expectEqual(@TypeOf(owner).Status.pending, try owner.step(4093, null));
        if (flushed) {
            try std.testing.expectEqual(@TypeOf(owner).Status.pending, try owner.step(4093, null));
            try std.testing.expectEqual(@as(usize, 1), backend.local_reader.active);
        }
        const Closing = struct {
            backend: *Backend,
            started: std.Io.Event = .unset,
            done: std.atomic.Value(bool) = .init(false),
            fn run(self: *@This()) void {
                self.started.set(std.testing.io);
                self.backend.close();
                self.done.store(true, .release);
            }
        };
        var closing: Closing = .{ .backend = &backend };
        const thread = try std.Thread.spawn(.{}, Closing.run, .{&closing});
        backend_live = false;
        var joined = false;
        defer if (!joined) {
            owner.deinit();
            thread.join();
        };
        closing.started.waitUncancelable(std.testing.io);
        const deadline = std.Io.Clock.awake.now(std.testing.io).addDuration(.fromSeconds(5));
        while (!backend.closing.load(.acquire)) {
            if (std.Io.Clock.awake.now(std.testing.io).nanoseconds >= deadline.nanoseconds) return error.TestBackendCloseDidNotStart;
            platform_time.yieldBriefly();
        }
        try std.testing.expect(!closing.done.load(.acquire));
        try std.testing.expectError(error.BackendClosing, backend.beginOwnedValueRead(a, .{}, "another", null));
        owner.deinit();
        thread.join();
        joined = true;
        try std.testing.expect(closing.done.load(.acquire));
    }
}

fn drainOwnedKeyForTest(cursor: anytype, quantum: usize) !?[]const u8 {
    var steps: usize = 0;
    while (try cursor.step(quantum, null) != .ready) : (steps += 1) try std.testing.expect(steps < 100000);
    return cursor.key();
}

test "LSM owned namespace key cursor preserves snapshot tombstones bounds moves and same-snapshot value handoff" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    const value = try a.alloc(u8, 128 * 1024 + 17);
    defer a.free(value);
    @memset(value, 'v');
    for ([_]bool{ false, true }) |private| {
        test_private_read_versions = private;
        defer test_private_read_versions = false;
        for (0..4) |mode| {
            var memory = storage_io.MemoryStorage.init(a);
            defer memory.deinit();
            var storage = memory.storage();
            var table = storage.vtable.*;
            table.read_file_range_into = OwnedValueTestStorage.read;
            table.read_file_range_into_with_admission = OwnedValueTestStorage.controlled;
            storage.vtable = &table;
            var backend = if (mode == 3) Backend.init(a, .{ .wal_enabled = false, .flush_threshold = 1, .compact_threshold_runs = 10000 }) else try Backend.open(a, "/owned-key", .{ .storage = storage, .wal_enabled = false, .flush_threshold = if (mode == 0) std.math.maxInt(usize) else 1, .compact_threshold_runs = 10000, .table_block_compression = if (mode == 1) .none else .snappy_adaptive });
            defer backend.close();
            {
                var write = try backend.beginWrite();
                errdefer write.abort();
                try write.put(.{ .name = "docs" }, "a", "removed");
                try write.put(.{ .name = "docs" }, "b", value);
                try write.put(.{ .name = "docs" }, "c", "tail");
                try write.put(.{ .name = "other" }, "a", "isolated");
                try write.commit();
            }
            {
                var write = try backend.beginWrite();
                errdefer write.abort();
                try write.delete(.{ .name = "docs" }, "a");
                try write.put(.{ .name = "docs" }, "c", "new tail");
                try write.commit();
            }
            var cursor = try backend.beginOwnedKeyCursor(a, .{ .name = "docs" }, null);
            defer cursor.deinit();
            var lower = "a".*;
            var upper = "d".*;
            try cursor.seek(&lower, &upper, false, null);
            @memset(&lower, 0xdd);
            @memset(&upper, 0xdd);
            // Later writes cannot change any query or the value handoff.
            {
                var write = try backend.beginWrite();
                errdefer write.abort();
                try write.put(.{ .name = "docs" }, "a", "revived after snapshot");
                try write.put(.{ .name = "docs" }, "b", "replaced after snapshot");
                try write.commit();
            }
            try std.testing.expectEqualStrings("b", (try drainOwnedKeyForTest(&cursor, 4093)).?);
            const borrowed_lower = (try cursor.key()).?;
            try cursor.seek(borrowed_lower, "d", true, null);
            try std.testing.expectEqualStrings("c", (try drainOwnedKeyForTest(&cursor, 4093)).?);
            const borrowed_both = (try cursor.key()).?;
            try cursor.seek(borrowed_both, borrowed_both, false, null);
            try std.testing.expect((try drainOwnedKeyForTest(&cursor, 1)) == null);
            try cursor.seek("b", "c", false, null);
            try std.testing.expectEqualStrings("b", (try drainOwnedKeyForTest(&cursor, 4093)).?);
            var moved = cursor;
            cursor = .{ .backend = undefined, .allocator = a, .txn = null, .namespace = .{}, .phase = .closed };
            defer moved.deinit();
            var point = try moved.intoValueRead(null);
            defer point.deinit();
            try std.testing.expectError(error.TransactionClosed, moved.step(1, null));
            try std.testing.expectEqual(@as(usize, 1), backend.active_readers);
            var result = try drainOwnedValueForTest(&point);
            defer result.deinit();
            try std.testing.expectEqualSlices(u8, value, result.bytes());
            try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
            try std.testing.expectEqual(@as(usize, 0), backend.local_reader.active);
        }
    }
}

test "LSM owned namespace key cursor rejects original cancellation and unwinds caller allocation refusal" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    var backend = Backend.init(a, .{ .wal_enabled = false });
    defer backend.close();
    {
        var write = try backend.beginWrite();
        errdefer write.abort();
        try write.put(.{}, "key", "value");
        try write.commit();
    }
    const Probe = struct {
        fn run(root: Allocator, source: *Backend) !void {
            var cursor = try source.beginOwnedKeyCursor(root, .{}, null);
            defer cursor.deinit();
            try cursor.seek("", "z", false, null);
            try std.testing.expectEqualStrings("key", (try drainOwnedKeyForTest(&cursor, 1)).?);
            var point = try cursor.intoValueRead(null);
            defer point.deinit();
            var result = try drainOwnedValueForTest(&point);
            defer result.deinit();
            try std.testing.expectEqualStrings("value", result.bytes());
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{&backend});
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            return error.TestOwnedKeyOriginalRejection;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedKeyWait;
        }
    };
    var budget: Budget = .{};
    var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    var cursor = try backend.beginOwnedKeyCursor(a, .{}, null);
    defer cursor.deinit();
    try cursor.seek("", "z", false, null);
    _ = try cursor.step(1, null);
    try std.testing.expectError(error.TestOwnedKeyOriginalRejection, cursor.step(1, .init(&payload)));
    try std.testing.expectEqual(@as(usize, 0), backend.active_readers);
    try std.testing.expectError(error.TestOwnedKeyOriginalRejection, cursor.step(1, null));
}

test "LSM owned namespace cold index reads only bounded metadata windows and detaches before backend close" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    const Fixture = struct {
        var current: *@This() = undefined;
        limit: usize = 0,
        calls: usize = 0,
        total: usize = 0,
        fn read(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8) !void {
            try std.testing.expect(out.len <= current.limit);
            current.calls += 1;
            current.total += out.len;
            try OwnedValueTestStorage.read(ptr, path, offset, out);
        }
        fn controlled(ptr: *anyopaque, path: []const u8, offset: u64, out: []u8, control: read_admission.Port) !void {
            try control.checkpoint();
            try read(ptr, path, offset, out);
            try control.checkpoint();
        }
    };
    var fixture: Fixture = .{};
    Fixture.current = &fixture;
    var memory = storage_io.MemoryStorage.init(a);
    defer memory.deinit();
    var storage = memory.storage();
    var vtable = storage.vtable.*;
    vtable.read_file_range_into = Fixture.read;
    vtable.read_file_range_into_with_admission = Fixture.controlled;
    storage.vtable = &vtable;
    const keys = try a.alloc(u8, 32 * 4096);
    defer a.free(keys);
    const value: [lsm_table_file.default_block_size]u8 = @splat('v');
    var entries: [32]lsm_table_file.Entry = undefined;
    for (&entries, 0..) |*entry, i| {
        const key = keys[i * 4096 ..][0..4096];
        @memset(key, 'k');
        key[key.len - 1] = @intCast(i);
        entry.* = .{ .namespace_name = "docs", .key = key, .value = &value };
    }
    const encoded = try lsm_table_file.encodeAlloc(a, &entries);
    defer a.free(encoded);
    try storage.writeFileAbsolute("owned-index-file", encoded);
    const footer = try lsm_table_file.decodeFooter(encoded);
    try std.testing.expect(footer.metadata_len > 65536);
    var backend = Backend.init(a, .{ .storage = storage });
    var backend_live = true;
    defer if (backend_live) backend.close();
    const Owner = OwnedRunIndexRead(Backend);
    var path = "owned-index-file".*;
    var owner = try Owner.init(&backend, a, &path, encoded.len, null);
    defer owner.deinit();
    @memset(&path, 0xdd);
    fixture.limit = 7;
    _ = try owner.step(7, null);
    var moved = owner;
    owner = .{ .backend = &backend, .allocator = a, .path = null, .source = null, .file_size = encoded.len, .phase = .closed };
    defer moved.deinit();
    var polls: usize = 0;
    while (true) : (polls += 1) {
        try std.testing.expect(polls < 100000);
        fixture.limit = if (polls % 2 == 0) 127 else 4093;
        fixture.calls = 0;
        const state = try moved.step(fixture.limit, null);
        try std.testing.expect(fixture.calls <= 1);
        if (state == .ready) break;
    }
    try std.testing.expectEqual(lsm_table_file.footer_len + footer.metadata_len, fixture.total);
    var index = try moved.takeOutput();
    defer index.deinit();
    try std.testing.expect(moved.source == null and moved.path == null);
    backend.close();
    backend_live = false;
    try std.testing.expectEqual(@as(usize, entries.len), index.view().entryCount());
    try std.testing.expectEqualSlices(u8, entries[0].key, index.view().blocks[0].smallest_key.?);
}

test "LSM owned namespace cold index unwinds allocator refusal and original cancellation during partial physical read" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    var memory = storage_io.MemoryStorage.init(a);
    defer memory.deinit();
    var storage = memory.storage();
    var vtable = storage.vtable.*;
    vtable.read_file_range_into = OwnedValueTestStorage.read;
    vtable.read_file_range_into_with_admission = OwnedValueTestStorage.controlled;
    storage.vtable = &vtable;
    const entries = [_]lsm_table_file.Entry{.{ .key = "key", .value = "value" }};
    const encoded = try lsm_table_file.encodeAlloc(a, &entries);
    defer a.free(encoded);
    try storage.writeFileAbsolute("owned-index-file", encoded);
    var backend = Backend.init(a, .{ .storage = storage });
    defer backend.close();
    const Probe = struct {
        fn run(root: Allocator, source: *Backend, size: usize) !void {
            var owner = try OwnedRunIndexRead(Backend).init(source, root, "owned-index-file", size, null);
            defer owner.deinit();
            var polls: usize = 0;
            while (try owner.step(7, null) != .ready) : (polls += 1) try std.testing.expect(polls < 10000);
            var index = try owner.takeOutput();
            defer index.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Probe.run, .{ &backend, encoded.len });
    const Budget = struct {
        pub fn checkpoint(_: *@This()) !void {
            return error.TestColdIndexOriginal;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedColdIndexWait;
        }
    };
    var budget: Budget = .{};
    var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
    const payload = adapter.payload();
    var owner = try OwnedRunIndexRead(Backend).init(&backend, a, "owned-index-file", encoded.len, null);
    defer owner.deinit();
    _ = try owner.step(7, null);
    try std.testing.expectError(error.TestColdIndexOriginal, owner.step(7, .init(&payload)));
    try std.testing.expect(owner.source == null and owner.path == null and owner.metadata == null);
    try std.testing.expectError(error.TestColdIndexOriginal, owner.step(7, null));
}

test "LSM owned namespace cold index pins one leased generation and preserves mid-read original refusal" {
    const a = std.testing.allocator;
    const Backend = @import("../lsm_backend.zig").Backend;
    const Budget = struct {
        reject: bool = false,
        pub fn checkpoint(self: *@This()) !void {
            if (self.reject) return error.TestIndexLeasedOriginal;
        }
        pub fn waitForProgress(_: *@This()) !void {
            return error.TestUnexpectedIndexLeasedWait;
        }
    };
    const Generation = struct {
        var current: *@This() = undefined;
        bytes: []const u8,
        opens: usize = 0,
        closes: usize = 0,
        uses: usize = 0,
        fail: bool = false,
        original: ?*Budget = null,
        fn open(_: *anyopaque, _: Allocator, _: []const u8) !@import("../../segment_source.zig").Source {
            current.opens += 1;
            return .{ .ranges = .{ .ptr = current, .length = current.bytes.len, .read_into = read, .close = close, .acquire_use = acquire, .release_use = release } };
        }
        fn read(ptr: *anyopaque, offset: u64, out: []u8) !void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            try std.testing.expect(out.len <= 7);
            if (self.fail) {
                if (self.original) |budget| budget.reject = true;
                @memset(out, 0xaa);
                return error.TestIndexLeasedProviderFault;
            }
            if (offset > self.bytes.len or out.len > self.bytes.len - offset) return error.EndOfStream;
            @memcpy(out, self.bytes[@intCast(offset)..][0..out.len]);
        }
        fn acquire(ptr: *anyopaque) bool {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            self.uses += 1;
            return true;
        }
        fn release(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.uses > 0);
            self.uses -= 1;
        }
        fn close(ptr: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ptr));
            std.debug.assert(self.uses == 0);
            self.closes += 1;
        }
    };
    const first = [_]lsm_table_file.Entry{.{ .key = "first-key", .value = "first-value" }};
    const second = [_]lsm_table_file.Entry{.{ .key = "other-key", .value = "other-value" }};
    const bytes1 = try lsm_table_file.encodeAlloc(a, &first);
    defer a.free(bytes1);
    const bytes2 = try lsm_table_file.encodeAlloc(a, &second);
    defer a.free(bytes2);
    try std.testing.expectEqual(bytes1.len, bytes2.len);
    for (0..3) |mode| {
        var memory = storage_io.MemoryStorage.init(a);
        defer memory.deinit();
        var storage = memory.storage();
        var vtable = storage.vtable.*;
        vtable.open_leased_immutable_source = Generation.open;
        storage.vtable = &vtable;
        var old: Generation = .{ .bytes = bytes1 };
        var replacement: Generation = .{ .bytes = bytes2 };
        Generation.current = &old;
        var backend = Backend.init(a, .{ .storage = storage });
        var backend_live = true;
        defer if (backend_live) backend.close();
        var owner = try OwnedRunIndexRead(Backend).init(&backend, a, "leased-index", bytes1.len, null);
        defer owner.deinit();
        var polls: usize = 0;
        while (owner.phase == .footer) : (polls += 1) {
            try std.testing.expect(polls < 100);
            _ = try owner.step(7, null);
        }
        Generation.current = &replacement;
        if (mode != 0) {
            // The footer has been validated; fail during its metadata read.
            _ = try owner.step(7, null);
            var budget: Budget = .{};
            var adapter: read_admission.Adapter(*Budget) = .{ .budget = &budget };
            const payload = adapter.payload();
            old.fail = true;
            old.original = if (mode == 2) &budget else null;
            if (mode == 2) try std.testing.expectError(error.TestIndexLeasedOriginal, owner.step(7, .init(&payload))) else try std.testing.expectError(error.TestIndexLeasedProviderFault, owner.step(7, null));
            try std.testing.expect(owner.source == null and owner.metadata == null and owner.decoder == null);
        } else {
            while (try owner.step(7, null) != .ready) : (polls += 1) try std.testing.expect(polls < 10000);
            var index = try owner.takeOutput();
            defer index.deinit();
            try std.testing.expectEqualStrings("first-key", index.view().blocks[0].largest_key);
        }
        try std.testing.expectEqual(@as(usize, 1), old.opens);
        try std.testing.expectEqual(@as(usize, 0), old.uses);
        try std.testing.expectEqual(@as(usize, 0), replacement.opens);
        backend.close();
        backend_live = false;
        try std.testing.expectEqual(@as(usize, 1), old.closes);
        try std.testing.expectEqual(@as(usize, 0), replacement.closes);
    }
}
