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
const Allocator = std.mem.Allocator;
const backend_adapter = @import("../backend_adapter.zig");
const backend_types = @import("../backend_types.zig");

const CollisionBucket = std.ArrayListUnmanaged(usize);

/// Most key hashes are unique. Keep that path to one compact hash-table value
/// and allocate collision storage only for the exceptional case. The previous
/// hash -> ArrayList representation allocated a backing buffer for every key,
/// even though virtually every list contained exactly one entry.
const EntryIndex = struct {
    primary: std.AutoHashMapUnmanaged(u64, usize) = .empty,
    collisions: std.AutoHashMapUnmanaged(u64, CollisionBucket) = .empty,
    collision_capacity_bytes: u64 = 0,

    fn deinit(self: *EntryIndex, allocator: Allocator) void {
        var values = self.collisions.valueIterator();
        while (values.next()) |bucket| bucket.deinit(allocator);
        self.collisions.deinit(allocator);
        self.primary.deinit(allocator);
        self.* = .{};
    }

    fn count(self: *const EntryIndex) usize {
        return self.primary.count();
    }

    fn find(
        self: *const EntryIndex,
        entries: []const OwnedEntry,
        key_hash: u64,
        namespace: backend_types.Namespace,
        key: []const u8,
    ) ?usize {
        const primary_idx = self.primary.get(key_hash) orelse return null;
        if (entryAtIndexMatches(entries, primary_idx, namespace, key)) return primary_idx;
        const bucket = self.collisions.get(key_hash) orelse return null;
        for (bucket.items) |idx| {
            if (entryAtIndexMatches(entries, idx, namespace, key)) return idx;
        }
        return null;
    }

    fn insert(self: *EntryIndex, allocator: Allocator, key_hash: u64, idx: usize) !void {
        const primary = try self.primary.getOrPut(allocator, key_hash);
        if (!primary.found_existing) {
            primary.value_ptr.* = idx;
            return;
        }

        const collision = try self.collisions.getOrPut(allocator, key_hash);
        if (!collision.found_existing) collision.value_ptr.* = .empty;
        const old_capacity = collision.value_ptr.capacity;
        collision.value_ptr.append(allocator, idx) catch |err| {
            if (!collision.found_existing) {
                collision.value_ptr.deinit(allocator);
                _ = self.collisions.remove(key_hash);
            }
            return err;
        };
        self.collision_capacity_bytes +|= (collision.value_ptr.capacity - old_capacity) * @sizeOf(usize);
    }

    fn estimatedMemoryBytes(self: *const EntryIndex) u64 {
        var total = hashMapAllocationBytes(u64, usize, self.primary.capacity());
        total +|= hashMapAllocationBytes(u64, CollisionBucket, self.collisions.capacity());
        total +|= self.collision_capacity_bytes;
        return total;
    }
};

fn hashMapAllocationBytes(comptime Key: type, comptime Value: type, capacity: usize) u64 {
    if (capacity == 0) return 0;
    // std.HashMap uses one allocation containing a two-pointer/u32 header,
    // one metadata byte per slot, then aligned key and value arrays.
    const header_bytes = std.mem.alignForward(usize, 2 * @sizeOf(usize) + @sizeOf(u32), @alignOf(usize));
    var total = header_bytes + capacity;
    total = std.mem.alignForward(usize, total, @alignOf(Key));
    total += capacity * @sizeOf(Key);
    total = std.mem.alignForward(usize, total, @alignOf(Value));
    total += capacity * @sizeOf(Value);
    total = std.mem.alignForward(usize, total, @max(@alignOf(usize), @alignOf(Key), @alignOf(Value)));
    return @intCast(total);
}

pub const OwnedEntry = struct {
    /// Immutable, independently reclaimable bytes shared with read epochs.
    /// Only the last owning writer may reuse a same-sized value in place.
    shared: ?*SharedEntry = null,
    namespace_name: ?[]u8,
    namespace_from_arena: bool = false,
    key: []u8,
    key_from_arena: bool = false,
    value: []u8,
    value_from_arena: bool = false,
    tombstone: bool,

    pub fn deinit(self: *OwnedEntry, allocator: Allocator) void {
        if (self.shared) |owner| {
            owner.release();
            self.* = undefined;
            return;
        }
        if (self.namespace_name) |name| {
            if (!self.namespace_from_arena) allocator.free(name);
        }
        if (!self.key_from_arena) allocator.free(self.key);
        if (!self.value_from_arena) allocator.free(self.value);
        self.* = undefined;
    }

    pub fn entry(self: *const OwnedEntry) backend_adapter.Entry {
        std.debug.assert(!self.tombstone);
        return .{
            .key = self.key,
            .value = self.value,
        };
    }

    pub fn sharedOverheadBytes(self: OwnedEntry) u64 {
        return if (self.shared != null) @sizeOf(SharedEntry) else 0;
    }

    pub fn retainShared(self: OwnedEntry) OwnedEntry {
        _ = self.shared.?.references.fetchAdd(1, .monotonic);
        return self;
    }

    pub fn retainedBytes(self: OwnedEntry) u64 {
        return logicalEntryBytes(self) - @sizeOf(OwnedEntry) + self.sharedOverheadBytes();
    }
};

const OrderedIndex = @import("ordered_index.zig").Index(OwnedEntry, struct {
    fn compare(a: OwnedEntry, b: OwnedEntry) std.math.Order {
        return compareEntryTo(a, namespaceOf(b), b.key);
    }
}.compare);

const SharedEntry = struct {
    references: std.atomic.Value(usize) = .init(1),
    allocator: Allocator,
    allocation_len: usize,

    fn release(self: *@This()) void {
        if (self.references.fetchSub(1, .acq_rel) != 1) return;
        const allocator = self.allocator;
        const bytes: [*]align(@alignOf(SharedEntry)) u8 = @ptrCast(self);
        allocator.free(bytes[0..self.allocation_len]);
    }
};

fn initSharedEntry(allocator: Allocator, namespace: backend_types.Namespace, key: []const u8, value: []const u8, tombstone: bool) !OwnedEntry {
    const namespace_len = if (namespace.name) |name| name.len else 0;
    const allocation = try allocator.alignedAlloc(u8, .of(SharedEntry), @sizeOf(SharedEntry) + namespace_len + key.len + value.len);
    const owner: *SharedEntry = @ptrCast(allocation.ptr);
    owner.* = .{ .allocator = allocator, .allocation_len = allocation.len };
    const bytes = allocation[@sizeOf(SharedEntry)..];
    if (namespace.name) |name| @memcpy(bytes[0..namespace_len], name);
    @memcpy(bytes[namespace_len..][0..key.len], key);
    @memcpy(bytes[namespace_len + key.len ..], value);
    return .{
        .shared = owner,
        .namespace_name = if (namespace.name != null) bytes[0..namespace_len] else null,
        .key = bytes[namespace_len..][0..key.len],
        .value = bytes[namespace_len + key.len ..],
        .tombstone = tombstone,
    };
}

pub const State = struct {
    entries: std.ArrayListUnmanaged(OwnedEntry) = .empty,
    arena_owner: ?*std.heap.ArenaAllocator = null,
    frozen_memory_bytes: ?u64 = null,
    ordered_root: ?*OrderedIndex.Node = null,

    pub const EntryCursor = struct {
        ordered: OrderedIndex.Cursor = .{},
        pub fn at(self: *@This(), state: *const State, index: usize) OwnedEntry {
            return if (state.ordered_root) |root| self.ordered.at(root, index) else state.entries.items[index];
        }
    };

    pub fn entryCount(self: *const State) usize {
        return if (self.ordered_root) |root| root.count else self.entries.items.len;
    }

    pub fn entryAt(self: *const State, index: usize) OwnedEntry {
        return if (self.ordered_root) |root| root.at(index) else self.entries.items[index];
    }

    /// Materialization is confined to callers that mutate a snapshot or feed
    /// flat storage encoders. Ordinary snapshot reads retain the tree root.
    pub fn ensureFlat(self: *State, allocator: Allocator) !void {
        if (self.ordered_root == null) return;
        const out = try self.clone(allocator);
        self.deinit(allocator);
        self.* = out;
    }

    /// Published states are immutable. Compute the retained-byte charge once,
    /// before publication, instead of walking their entries on every commit.
    pub fn freezeMemoryAccounting(self: *State) void {
        self.frozen_memory_bytes = self.computeMemoryBytes();
    }

    pub fn estimatedMemoryBytes(self: *const State) u64 {
        return self.frozen_memory_bytes orelse self.computeMemoryBytes();
    }

    pub fn estimatedLogicalBytes(self: *const State) u64 {
        if (self.ordered_root) |root| return root.bytes - root.count * (@sizeOf(OrderedIndex.Node) + @sizeOf(SharedEntry)) + root.count * @sizeOf(OwnedEntry);
        var bytes: u64 = 0;
        for (self.entries.items) |entry| bytes +|= logicalEntryBytes(entry);
        return bytes;
    }

    fn computeMemoryBytes(self: *const State) u64 {
        if (self.ordered_root) |root| return root.bytes;
        var bytes: u64 = @as(u64, @intCast(self.entries.capacity)) * @sizeOf(OwnedEntry);
        if (self.arena_owner) |arena| return bytes +| arena.queryCapacity();
        for (self.entries.items) |entry| bytes +|= logicalEntryBytes(entry) - @sizeOf(OwnedEntry) + entry.sharedOverheadBytes();
        return bytes;
    }

    pub fn deinit(self: *State, allocator: Allocator) void {
        if (self.ordered_root) |root| root.release(allocator);
        for (self.entries.items) |*entry| entry.deinit(allocator);
        self.entries.deinit(allocator);
        if (self.arena_owner) |arena| {
            arena.deinit();
            allocator.destroy(arena);
        }
        self.* = .{};
    }

    pub fn clone(self: *const State, allocator: Allocator) !State {
        var out: State = .{};
        errdefer out.deinit(allocator);
        try out.entries.ensureTotalCapacity(allocator, self.entryCount());
        var cursor: EntryCursor = .{};
        for (0..self.entryCount()) |i| {
            const entry = cursor.at(self, i);
            out.entries.appendAssumeCapacity(try cloneEntry(allocator, entry));
        }
        return out;
    }

    pub fn cloneArena(self: *const State, allocator: Allocator) !State {
        var out: State = .{};
        errdefer out.deinit(allocator);
        try out.entries.ensureTotalCapacity(allocator, self.entryCount());
        const arena_allocator = try out.ensureArenaAllocator(allocator);
        var cursor: EntryCursor = .{};
        for (0..self.entryCount()) |i| {
            const entry = cursor.at(self, i);
            out.entries.appendAssumeCapacity(try initArenaEntry(arena_allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone));
        }
        return out;
    }

    pub fn ensureArenaAllocator(self: *State, allocator: Allocator) !Allocator {
        if (self.arena_owner == null) {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            errdefer allocator.destroy(arena);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            self.arena_owner = arena;
        }
        return self.arena_owner.?.allocator();
    }

    pub fn get(self: *const State, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        const idx = self.findIndex(namespace, key) orelse return error.NotFound;
        const entry = self.entryAt(idx);
        if (entry.tombstone) return error.NotFound;
        return entry.value;
    }

    pub fn upsert(
        self: *State,
        allocator: Allocator,
        namespace: backend_types.Namespace,
        key: []const u8,
        value: []const u8,
        tombstone: bool,
    ) !void {
        self.frozen_memory_bytes = null;
        try self.ensureFlat(allocator);
        if (self.findIndex(namespace, key)) |idx| {
            try replaceEntryValueCopy(&self.entries.items[idx], allocator, value, tombstone, false);
            return;
        }

        const idx = self.lowerBound(namespace, key);
        try self.entries.ensureUnusedCapacity(allocator, 1);
        self.entries.insertAssumeCapacity(idx, try initEntry(allocator, namespace, key, value, tombstone));
    }

    pub fn appendUpsert(
        self: *State,
        allocator: Allocator,
        namespace: backend_types.Namespace,
        key: []const u8,
        value: []const u8,
        tombstone: bool,
    ) !void {
        self.frozen_memory_bytes = null;
        try self.ensureFlat(allocator);
        if (self.entries.items.len == 0) {
            try self.entries.ensureUnusedCapacity(allocator, 1);
            self.entries.appendAssumeCapacity(try initEntry(allocator, namespace, key, value, tombstone));
            return;
        }

        const last_idx = self.entries.items.len - 1;
        const last = self.entries.items[last_idx];
        switch (compareEntryTo(last, namespace, key)) {
            .lt => {
                try self.entries.ensureUnusedCapacity(allocator, 1);
                self.entries.appendAssumeCapacity(try initEntry(allocator, namespace, key, value, tombstone));
            },
            .eq => {
                try replaceEntryValueCopy(&self.entries.items[last_idx], allocator, value, tombstone, false);
            },
            .gt => try self.upsert(allocator, namespace, key, value, tombstone),
        }
    }

    pub fn upsertMove(self: *State, allocator: Allocator, entry: OwnedEntry) !void {
        self.frozen_memory_bytes = null;
        try self.ensureFlat(allocator);
        const namespace = namespaceOf(entry);
        const idx = self.lowerBound(namespace, entry.key);
        if (idx < self.entries.items.len and compareEntryTo(self.entries.items[idx], namespace, entry.key) == .eq) {
            replaceEntryValueMove(&self.entries.items[idx], allocator, entry);
            return;
        }
        try self.entries.insert(allocator, idx, entry);
    }

    pub fn lowerBound(self: *const State, namespace: backend_types.Namespace, key: []const u8) usize {
        if (self.ordered_root) |root| return root.lowerBound(.{ .namespace_name = if (namespace.name) |name| @constCast(name) else null, .key = @constCast(key), .value = &.{}, .tombstone = false });
        var lo: usize = 0;
        var hi: usize = self.entries.items.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            const ord = compareEntryTo(self.entries.items[mid], namespace, key);
            if (ord == .lt) {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        return lo;
    }

    pub fn findIndex(self: *const State, namespace: backend_types.Namespace, key: []const u8) ?usize {
        const idx = self.lowerBound(namespace, key);
        if (idx >= self.entryCount()) return null;
        if (compareEntryTo(self.entryAt(idx), namespace, key) != .eq) return null;
        return idx;
    }

    pub fn splitAtKey(self: *const State, allocator: Allocator, split_key: []const u8) !SplitStates {
        var left: State = .{};
        errdefer left.deinit(allocator);
        var right: State = .{};
        errdefer right.deinit(allocator);

        try left.entries.ensureTotalCapacity(allocator, self.entryCount());
        try right.entries.ensureTotalCapacity(allocator, self.entryCount());

        for (0..self.entryCount()) |i| {
            const entry = self.entryAt(i);
            if (std.mem.order(u8, entry.key, split_key) == .lt) {
                left.entries.appendAssumeCapacity(try cloneEntry(allocator, entry));
            } else {
                right.entries.appendAssumeCapacity(try cloneEntry(allocator, entry));
            }
        }

        return .{
            .left = left,
            .right = right,
        };
    }
};

pub const ActiveMemTable = struct {
    entries: std.ArrayListUnmanaged(OwnedEntry) = .empty,
    index: EntryIndex = .{},
    arena_owner: ?*std.heap.ArenaAllocator = null,
    logical_bytes: u64 = 0,
    ordered: OrderedIndex = .{},
    ordered_enabled: bool = true,

    pub fn entryCount(self: *const ActiveMemTable) usize {
        return self.entries.items.len;
    }
    pub fn entryAt(self: *const ActiveMemTable, index: usize) OwnedEntry {
        return self.entries.items[index];
    }

    pub fn deinit(self: *ActiveMemTable, allocator: Allocator) void {
        self.ordered.deinit(allocator);
        for (self.entries.items) |*entry| entry.deinit(allocator);
        self.entries.deinit(allocator);
        self.index.deinit(allocator);
        if (self.arena_owner) |arena| {
            arena.deinit();
            allocator.destroy(arena);
        }
        self.* = .{};
    }

    pub fn clone(self: *const ActiveMemTable, allocator: Allocator) !State {
        var out: State = .{};
        errdefer out.deinit(allocator);
        try out.entries.ensureTotalCapacity(allocator, self.entries.items.len);
        for (self.entries.items) |entry| {
            out.entries.appendAssumeCapacity(try cloneEntry(allocator, entry));
        }
        sortStateEntries(&out);
        return out;
    }

    pub fn cloneArena(self: *const ActiveMemTable, allocator: Allocator) !State {
        var out: State = .{};
        errdefer out.deinit(allocator);
        try out.entries.ensureTotalCapacity(allocator, self.entries.items.len);
        const arena_allocator = try out.ensureArenaAllocator(allocator);
        for (self.entries.items) |entry| {
            out.entries.appendAssumeCapacity(try initArenaEntry(arena_allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone));
        }
        sortStateEntries(&out);
        return out;
    }

    /// Snapshot just the ordered index; shared entry bytes are immutable for
    /// this epoch. Overwrites replace only the affected entry, and retired
    /// values are released with the last reader instead of a whole arena.
    pub fn snapshot(self: *const ActiveMemTable, allocator: Allocator) !State {
        if (!self.ordered_enabled) return self.clone(allocator);
        return .{ .ordered_root = if (self.ordered.root) |root| root.retain() else null };
    }

    pub fn toStateMove(self: *ActiveMemTable, allocator: Allocator) !State {
        self.ordered.deinit(allocator);
        var out = State{
            .entries = self.entries,
            .arena_owner = self.arena_owner,
        };
        self.entries = .empty;
        self.arena_owner = null;
        self.logical_bytes = 0;
        self.clearIndex(allocator);
        sortStateEntries(&out);
        out.freezeMemoryAccounting();
        return out;
    }

    fn clearIndex(self: *ActiveMemTable, allocator: Allocator) void {
        self.index.deinit(allocator);
    }

    pub fn resetAfterEntriesMoved(self: *ActiveMemTable, allocator: Allocator) void {
        self.ordered.deinit(allocator);
        self.entries.items.len = 0;
        self.entries.deinit(allocator);
        self.clearIndex(allocator);
        if (self.arena_owner) |arena| {
            arena.deinit();
            allocator.destroy(arena);
        }
        self.* = .{};
    }

    pub fn ensureArenaAllocator(self: *ActiveMemTable, allocator: Allocator) !Allocator {
        if (self.arena_owner == null) {
            const arena = try allocator.create(std.heap.ArenaAllocator);
            errdefer allocator.destroy(arena);
            arena.* = std.heap.ArenaAllocator.init(allocator);
            self.arena_owner = arena;
        }
        return self.arena_owner.?.allocator();
    }

    pub fn get(self: *const ActiveMemTable, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        const idx = self.findIndex(namespace, key) orelse return error.NotFound;
        const entry = self.entries.items[idx];
        if (entry.tombstone) return error.NotFound;
        return entry.value;
    }

    pub fn findIndex(self: *const ActiveMemTable, namespace: backend_types.Namespace, key: []const u8) ?usize {
        return self.index.find(self.entries.items, hashEntryKey(namespace, key), namespace, key);
    }

    pub fn estimatedIndexMemoryBytes(self: *const ActiveMemTable) u64 {
        return self.index.estimatedMemoryBytes() +| self.ordered.memoryBytes();
    }

    pub fn estimatedLogicalBytes(self: *const ActiveMemTable) u64 {
        return self.logical_bytes;
    }

    pub fn estimatedMemoryBytes(self: *const ActiveMemTable) u64 {
        // Every active entry crosses upsertSharedMove/upsert, so its packed
        // allocation has exactly one SharedEntry header. logical_bytes is
        // maintained on every insert/overwrite; capacity is charged separately.
        const count: u64 = @intCast(self.entries.items.len);
        const capacity: u64 = @intCast(self.entries.capacity);
        return self.logical_bytes -| count * @sizeOf(OwnedEntry) +|
            count * @sizeOf(SharedEntry) +| capacity * @sizeOf(OwnedEntry) +|
            self.index.estimatedMemoryBytes() +| self.ordered.memoryBytes() +| (if (self.arena_owner) |arena| arena.queryCapacity() else 0);
    }

    pub fn upsert(
        self: *ActiveMemTable,
        allocator: Allocator,
        namespace: backend_types.Namespace,
        key: []const u8,
        value: []const u8,
        tombstone: bool,
    ) !void {
        if (self.ordered_enabled) try self.ordered.prepare(allocator);
        const key_hash = hashEntryKey(namespace, key);
        if (self.index.find(self.entries.items, key_hash, namespace, key)) |idx| {
            const old_value_len: u64 = @intCast(self.entries.items[idx].value.len);
            try replaceEntryValueCopy(&self.entries.items[idx], allocator, value, tombstone, false);
            if (self.ordered_enabled) self.ordered.putPrepared(allocator, self.entries.items[idx]);
            self.logical_bytes = self.logical_bytes -| old_value_len +| @as(u64, @intCast(value.len));
            return;
        }

        var owned = try initSharedEntry(allocator, namespace, key, value, tombstone);
        errdefer owned.deinit(allocator);
        try self.entries.ensureUnusedCapacity(allocator, 1);
        const idx = self.entries.items.len;
        try self.index.insert(allocator, key_hash, idx);
        self.entries.appendAssumeCapacity(owned);
        if (self.ordered_enabled) self.ordered.putPrepared(allocator, owned);
        self.logical_bytes +|= logicalEntryBytes(self.entries.items[idx]);
    }

    pub fn upsertMove(self: *ActiveMemTable, allocator: Allocator, entry: OwnedEntry) !void {
        if (entry.shared != null) return self.upsertSharedMove(allocator, entry);
        // Normalize replay/ingest ownership once at the mutable boundary.
        // Ordinary write batches already move shared allocations directly.
        var shared = try initSharedEntry(allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone);
        errdefer shared.deinit(allocator);
        try self.upsertSharedMove(allocator, shared);
        var moved = entry;
        moved.deinit(allocator);
    }

    fn upsertSharedMove(self: *ActiveMemTable, allocator: Allocator, entry: OwnedEntry) !void {
        if (self.ordered_enabled) try self.ordered.prepare(allocator);
        const namespace = namespaceOf(entry);
        const key_hash = hashEntryKey(namespace, entry.key);
        if (self.index.find(self.entries.items, key_hash, namespace, entry.key)) |idx| {
            const old_value_len: u64 = @intCast(self.entries.items[idx].value.len);
            const new_value_len: u64 = @intCast(entry.value.len);
            replaceEntryValueMove(&self.entries.items[idx], allocator, entry);
            if (self.ordered_enabled) self.ordered.putPrepared(allocator, entry);
            self.logical_bytes = self.logical_bytes -| old_value_len +| new_value_len;
            return;
        }

        try self.entries.ensureUnusedCapacity(allocator, 1);
        const idx = self.entries.items.len;
        try self.index.insert(allocator, key_hash, idx);
        self.entries.appendAssumeCapacity(entry);
        if (self.ordered_enabled) self.ordered.putPrepared(allocator, entry);
        self.logical_bytes +|= logicalEntryBytes(self.entries.items[idx]);
    }

    pub fn appendUpsert(
        self: *ActiveMemTable,
        allocator: Allocator,
        namespace: backend_types.Namespace,
        key: []const u8,
        value: []const u8,
        tombstone: bool,
    ) !void {
        try self.upsert(allocator, namespace, key, value, tombstone);
    }
};

fn logicalEntryBytes(entry: OwnedEntry) u64 {
    var total: u64 = @sizeOf(OwnedEntry);
    if (entry.namespace_name) |name| total +|= name.len;
    total +|= entry.key.len;
    total +|= entry.value.len;
    return total;
}

pub const SplitStates = struct {
    left: State,
    right: State,

    pub fn deinit(self: *SplitStates, allocator: Allocator) void {
        self.left.deinit(allocator);
        self.right.deinit(allocator);
        self.* = undefined;
    }
};

pub fn cloneEntry(allocator: Allocator, entry: OwnedEntry) !OwnedEntry {
    if (entry.shared) |owner| {
        _ = owner.references.fetchAdd(1, .monotonic);
        return entry;
    }
    return initEntry(allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone);
}

pub fn initEntry(
    allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    value: []const u8,
    tombstone: bool,
) !OwnedEntry {
    const name = if (namespace.name) |value_name| try allocator.dupe(u8, value_name) else null;
    errdefer if (name) |value_name| allocator.free(value_name);
    const owned_key = try allocator.dupe(u8, key);
    errdefer allocator.free(owned_key);
    return .{
        .namespace_name = name,
        .namespace_from_arena = false,
        .key = owned_key,
        .key_from_arena = false,
        .value = try allocator.dupe(u8, value),
        .value_from_arena = false,
        .tombstone = tombstone,
    };
}

pub fn initArenaEntry(
    allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    value: []const u8,
    tombstone: bool,
) !OwnedEntry {
    return .{
        .namespace_name = if (namespace.name) |name| try allocator.dupe(u8, name) else null,
        .namespace_from_arena = true,
        .key = try allocator.dupe(u8, key),
        .key_from_arena = true,
        .value = try allocator.dupe(u8, value),
        .value_from_arena = true,
        .tombstone = tombstone,
    };
}

pub fn namespaceOf(entry: OwnedEntry) backend_types.Namespace {
    return .{ .name = entry.namespace_name };
}

pub fn compareNamespace(a: backend_types.Namespace, b: backend_types.Namespace) std.math.Order {
    if (a.name == null and b.name == null) return .eq;
    if (a.name == null) return .lt;
    if (b.name == null) return .gt;
    return std.mem.order(u8, a.name.?, b.name.?);
}

pub fn compareEntryTo(entry: OwnedEntry, namespace: backend_types.Namespace, key: []const u8) std.math.Order {
    const namespace_order = compareNamespace(namespaceOf(entry), namespace);
    if (namespace_order != .eq) return namespace_order;
    return std.mem.order(u8, entry.key, key);
}

pub fn applyState(target: *State, allocator: Allocator, source: anytype) !void {
    for (0..source.entryCount()) |i| {
        const entry = source.entryAt(i);
        try target.upsert(allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone);
    }
}

pub fn sortStateEntries(state: *State) void {
    std.sort.heap(OwnedEntry, state.entries.items, {}, struct {
        fn lessThan(_: void, a: OwnedEntry, b: OwnedEntry) bool {
            const namespace_order = compareNamespace(namespaceOf(a), namespaceOf(b));
            if (namespace_order != .eq) return namespace_order == .lt;
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.lessThan);
}

fn hashEntryKey(namespace: backend_types.Namespace, key: []const u8) u64 {
    var hasher = std.hash.Wyhash.init(0);
    if (namespace.name) |name| hasher.update(name);
    hasher.update(&.{0});
    hasher.update(key);
    return hasher.final();
}

fn entryAtIndexMatches(
    entries: []const OwnedEntry,
    idx: usize,
    namespace: backend_types.Namespace,
    key: []const u8,
) bool {
    return idx < entries.len and compareEntryTo(entries[idx], namespace, key) == .eq;
}

pub fn mergeStates(
    allocator: Allocator,
    older: *const State,
    newer: *const State,
) !State {
    var merged: State = .{};
    errdefer merged.deinit(allocator);

    try merged.entries.ensureTotalCapacity(allocator, older.entryCount() + newer.entryCount());

    var older_idx: usize = 0;
    var newer_idx: usize = 0;
    while (older_idx < older.entryCount() and newer_idx < newer.entryCount()) {
        const older_entry = older.entryAt(older_idx);
        const newer_entry = newer.entryAt(newer_idx);
        switch (compareEntryTo(older_entry, namespaceOf(newer_entry), newer_entry.key)) {
            .lt => {
                merged.entries.appendAssumeCapacity(try cloneEntry(allocator, older_entry));
                older_idx += 1;
            },
            .gt => {
                merged.entries.appendAssumeCapacity(try cloneEntry(allocator, newer_entry));
                newer_idx += 1;
            },
            .eq => {
                merged.entries.appendAssumeCapacity(try cloneEntry(allocator, newer_entry));
                older_idx += 1;
                newer_idx += 1;
            },
        }
    }

    while (older_idx < older.entryCount()) : (older_idx += 1) {
        merged.entries.appendAssumeCapacity(try cloneEntry(allocator, older.entryAt(older_idx)));
    }
    while (newer_idx < newer.entryCount()) : (newer_idx += 1) {
        merged.entries.appendAssumeCapacity(try cloneEntry(allocator, newer.entryAt(newer_idx)));
    }

    return merged;
}

pub fn mergeStatesMove(
    allocator: Allocator,
    older: *State,
    newer: *State,
) !State {
    try older.ensureFlat(allocator);
    try newer.ensureFlat(allocator);
    if (older.arena_owner != null or newer.arena_owner != null) {
        const merged = try mergeStates(allocator, older, newer);
        older.deinit(allocator);
        newer.deinit(allocator);
        return merged;
    }

    var merged: State = .{};

    try merged.entries.ensureTotalCapacity(allocator, older.entries.items.len + newer.entries.items.len);

    var older_idx: usize = 0;
    var newer_idx: usize = 0;
    while (older_idx < older.entries.items.len and newer_idx < newer.entries.items.len) {
        const older_entry = older.entries.items[older_idx];
        const newer_entry = newer.entries.items[newer_idx];
        switch (compareEntryTo(older_entry, namespaceOf(newer_entry), newer_entry.key)) {
            .lt => {
                merged.entries.appendAssumeCapacity(older_entry);
                older_idx += 1;
            },
            .gt => {
                merged.entries.appendAssumeCapacity(newer_entry);
                newer_idx += 1;
            },
            .eq => {
                older.entries.items[older_idx].deinit(allocator);
                merged.entries.appendAssumeCapacity(newer_entry);
                older_idx += 1;
                newer_idx += 1;
            },
        }
    }

    while (older_idx < older.entries.items.len) : (older_idx += 1) {
        merged.entries.appendAssumeCapacity(older.entries.items[older_idx]);
    }
    while (newer_idx < newer.entries.items.len) : (newer_idx += 1) {
        merged.entries.appendAssumeCapacity(newer.entries.items[newer_idx]);
    }

    older.entries.items.len = 0;
    older.entries.deinit(allocator);
    if (older.arena_owner) |arena| {
        arena.deinit();
        allocator.destroy(arena);
    }
    older.* = .{};
    newer.entries.items.len = 0;
    newer.entries.deinit(allocator);
    if (newer.arena_owner) |arena| {
        arena.deinit();
        allocator.destroy(arena);
    }
    newer.* = .{};

    return merged;
}

pub fn applyStateMove(target: *State, allocator: Allocator, source: *State) !void {
    target.frozen_memory_bytes = null;
    try target.ensureFlat(allocator);
    try source.ensureFlat(allocator);
    if (source.entries.items.len == 0) return;
    if (source.arena_owner != null) {
        for (source.entries.items) |entry| {
            try target.upsert(allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone);
        }
        source.deinit(allocator);
        return;
    }

    try target.entries.ensureTotalCapacity(allocator, target.entries.items.len + source.entries.items.len);

    for (source.entries.items) |entry| {
        if (target.entries.items.len == 0) {
            target.entries.appendAssumeCapacity(entry);
            continue;
        }

        const last_idx = target.entries.items.len - 1;
        switch (compareEntryTo(target.entries.items[last_idx], namespaceOf(entry), entry.key)) {
            .lt => {
                target.entries.appendAssumeCapacity(entry);
            },
            .eq => {
                replaceEntryValueMove(&target.entries.items[last_idx], allocator, entry);
            },
            .gt => {
                const idx = target.lowerBound(namespaceOf(entry), entry.key);
                if (idx < target.entries.items.len and compareEntryTo(target.entries.items[idx], namespaceOf(entry), entry.key) == .eq) {
                    replaceEntryValueMove(&target.entries.items[idx], allocator, entry);
                } else {
                    target.entries.insertAssumeCapacity(idx, entry);
                }
            },
        }
    }

    source.entries.items.len = 0;
    source.entries.deinit(allocator);
    if (source.arena_owner) |arena| {
        arena.deinit();
        allocator.destroy(arena);
    }
    source.* = .{};
}

pub fn applyStateMoveToMutable(target: anytype, allocator: Allocator, source: *State) !void {
    return try applyMutableMoveToMutable(target, allocator, source);
}

pub fn applyMutableMoveToMutable(target: anytype, allocator: Allocator, source: anytype) !void {
    const Target = @TypeOf(target.*);
    const Source = @TypeOf(source.*);
    if (Source == State and Target == State) {
        return try applyStateMove(target, allocator, source);
    }
    if (Source == State and Target == ActiveMemTable) {
        return try applyStateMoveToActive(target, allocator, source);
    }
    if (Source == ActiveMemTable) {
        return try applyActiveMoveToMutable(target, allocator, source);
    }
    @compileError("unsupported LSM mutable state type");
}

fn applyStateMoveToActive(target: *ActiveMemTable, allocator: Allocator, source: *State) !void {
    try source.ensureFlat(allocator);
    if (source.arena_owner != null) {
        for (source.entries.items) |entry| {
            try target.upsert(allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone);
        }
        source.deinit(allocator);
        return;
    }

    var transferred: usize = 0;
    errdefer {
        std.mem.copyForwards(OwnedEntry, source.entries.items, source.entries.items[transferred..]);
        source.entries.items.len -= transferred;
    }
    for (source.entries.items) |entry| {
        try target.upsertMove(allocator, entry);
        transferred += 1;
    }
    source.entries.items.len = 0;
    source.entries.deinit(allocator);
    if (source.arena_owner) |arena| {
        arena.deinit();
        allocator.destroy(arena);
    }
    source.* = .{};
}

fn applyActiveMoveToMutable(target: anytype, allocator: Allocator, source: *ActiveMemTable) !void {
    if (source.arena_owner != null) {
        for (source.entries.items) |entry| {
            try target.upsert(allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone);
        }
        source.deinit(allocator);
        return;
    }

    var transferred: usize = 0;
    errdefer {
        // Failed commit preparation must still leave one owner per entry.
        // The source is discarded by its caller; its index is no longer used.
        std.mem.copyForwards(OwnedEntry, source.entries.items, source.entries.items[transferred..]);
        source.entries.items.len -= transferred;
    }
    for (source.entries.items) |entry| {
        try target.upsertMove(allocator, entry);
        transferred += 1;
    }
    source.resetAfterEntriesMoved(allocator);
}

fn replaceEntryValueMove(target: *OwnedEntry, allocator: Allocator, source: OwnedEntry) void {
    if (target.shared != null or source.shared != null) {
        target.deinit(allocator);
        target.* = source;
        return;
    }
    if (!target.value_from_arena) allocator.free(target.value);
    target.value = source.value;
    target.value_from_arena = source.value_from_arena;
    target.tombstone = source.tombstone;
    if (source.namespace_name) |name| {
        if (!source.namespace_from_arena) allocator.free(name);
    }
    if (!source.key_from_arena) allocator.free(source.key);
}

fn replaceEntryValueCopy(target: *OwnedEntry, allocator: Allocator, value: []const u8, tombstone: bool, replacement_from_arena: bool) !void {
    if (target.shared) |owner| {
        if (owner.references.load(.acquire) == 1 and target.value.len == value.len) {
            @memcpy(target.value, value);
            target.tombstone = tombstone;
            return;
        }
        const replacement = try initSharedEntry(owner.allocator, namespaceOf(target.*), target.key, value, tombstone);
        target.deinit(allocator);
        target.* = replacement;
        return;
    }
    if (target.value.len == value.len) {
        @memcpy(target.value, value);
        target.tombstone = tombstone;
        return;
    }
    const replacement = try allocator.dupe(u8, value);
    if (!target.value_from_arena) allocator.free(target.value);
    target.value = replacement;
    target.value_from_arena = replacement_from_arena;
    target.tombstone = tombstone;
}

pub fn stripTombstones(state: *State, allocator: Allocator) !void {
    state.frozen_memory_bytes = null;
    try state.ensureFlat(allocator);
    var filtered = std.ArrayListUnmanaged(OwnedEntry).empty;
    errdefer {
        for (filtered.items) |*entry| entry.deinit(allocator);
        filtered.deinit(allocator);
    }

    try filtered.ensureTotalCapacity(allocator, state.entries.items.len);
    for (state.entries.items) |entry| {
        if (entry.tombstone) continue;
        filtered.appendAssumeCapacity(try cloneEntry(allocator, entry));
    }
    state.deinit(allocator);
    state.entries = filtered;
}

test "ordered mutable snapshot setup benchmark" {
    const time = @import("antfly_platform").time;
    const alloc = std.testing.allocator;
    var mutable: ActiveMemTable = .{};
    defer mutable.deinit(alloc);
    for (0..8192) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, i, .big);
        try mutable.upsert(alloc, .{}, &key, "12345678", false);
    }
    var samples: [2][5]u64 = undefined;
    for (0..5) |sample| for (0..2) |turn| {
        const mode = (sample + turn) % 2;
        const started = time.monotonicNs();
        for (0..32) |_| {
            var snapshot = if (mode == 0) try mutable.clone(alloc) else try mutable.snapshot(alloc);
            defer snapshot.deinit(alloc);
            try std.testing.expectEqual(@as(usize, 8192), snapshot.entryCount());
            try std.testing.expectEqualStrings("12345678", try snapshot.get(.{}, &.{ 0, 0, 0, 0, 0, 0, 0, 1 }));
        }
        samples[mode][sample] = time.monotonicNs() - started;
    };
    for (&samples) |*values| std.mem.sort(u64, values, {}, std.sort.asc(u64));
    std.debug.print("\nLSM 8192 narrow keys / 32 snapshots: descriptor-copy/root-pin median ns={d}/{d}, copied descriptor bytes={d}/0\n", .{ samples[0][2], samples[1][2], 8192 * 32 * @sizeOf(OwnedEntry) });
}

test "ordered mutable epochs preserve rank scans through rotations and random overwrites" {
    const alloc = std.testing.allocator;
    var mutable: ActiveMemTable = .{};
    defer mutable.deinit(alloc);
    var reference: State = .{};
    defer reference.deinit(alloc);
    var snapshots: std.ArrayListUnmanaged(State) = .empty;
    defer {
        for (snapshots.items) |*snapshot| snapshot.deinit(alloc);
        snapshots.deinit(alloc);
    }
    // Odd multiplication permutes the 512 keys, exercising both rotations.
    for (0..512) |i| {
        var key: [2]u8 = undefined;
        std.mem.writeInt(u16, &key, @intCast((i * 317) % 512), .big);
        try mutable.upsert(alloc, .{}, &key, &key, false);
        try reference.upsert(alloc, .{}, &key, &key, false);
        if (i % 32 == 0) try snapshots.append(alloc, try mutable.snapshot(alloc));
    }
    try std.testing.expect(mutable.ordered.root.?.height <= 18);
    var full = try mutable.snapshot(alloc);
    defer full.deinit(alloc);
    var cursor: State.EntryCursor = .{};
    for (0..512) |i| {
        const entry = cursor.at(&full, i);
        try std.testing.expectEqualSlices(u8, reference.entryAt(i).key, entry.key);
        try std.testing.expectEqual(i, full.lowerBound(.{}, entry.key));
        try mutable.upsert(alloc, .{}, entry.key, "updated", i % 3 == 0);
    }
    // Every older root keeps its exact count and values after all overwrites.
    for (snapshots.items, 0..) |*snapshot, epoch| {
        try std.testing.expectEqual(epoch * 32 + 1, snapshot.entryCount());
        var old_cursor: State.EntryCursor = .{};
        for (0..snapshot.entryCount()) |i| {
            const entry = old_cursor.at(snapshot, i);
            try std.testing.expectEqualSlices(u8, entry.key, entry.value);
            try std.testing.expectEqual(i, snapshot.lowerBound(.{}, entry.key));
        }
    }
    mutable.deinit(alloc);
    try full.ensureFlat(alloc);
    try std.testing.expectEqual(@as(usize, 512), full.entries.items.len);
}

test "ordered mutable shared rotations are allocation failure safe" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(alloc: Allocator) !void {
            var mutable: ActiveMemTable = .{};
            defer mutable.deinit(alloc);
            for (0..20) |i| {
                const key = [1]u8{@intCast(i)};
                try mutable.upsert(alloc, .{}, &key, "old", false);
            }
            var pinned = try mutable.snapshot(alloc);
            defer pinned.deinit(alloc);
            for (20..40) |i| {
                const key = [1]u8{@intCast(i)};
                mutable.upsert(alloc, .{}, &key, "new", false) catch |err| {
                    try std.testing.expectEqual(@as(usize, 20), pinned.entryCount());
                    try std.testing.expectEqualStrings("old", try pinned.get(.{}, &.{0}));
                    return err;
                };
            }
            try std.testing.expectEqual(@as(usize, 20), pinned.entryCount());
        }
    }.run, .{});
}

test "mutable snapshot shares immutable bytes and copy-on-writes only overwritten entries" {
    const alloc = std.testing.allocator;
    var mutable: ActiveMemTable = .{};
    defer mutable.deinit(alloc);
    try mutable.upsert(alloc, .{}, "a", "original", false);
    try mutable.upsert(alloc, .{}, "b", "untouched", false);
    var snapshot = try mutable.snapshot(alloc);
    defer snapshot.deinit(alloc);
    try std.testing.expectEqual((try mutable.get(.{}, "a")).ptr, (try snapshot.get(.{}, "a")).ptr);
    try mutable.upsert(alloc, .{}, "a", "replaced", false);
    try mutable.upsert(alloc, .{}, "b", "", true);
    try std.testing.expectEqualStrings("original", try snapshot.get(.{}, "a"));
    try std.testing.expectEqualStrings("untouched", try snapshot.get(.{}, "b"));
    try std.testing.expectEqualStrings("replaced", try mutable.get(.{}, "a"));
    try std.testing.expectError(error.NotFound, mutable.get(.{}, "b"));
    mutable.deinit(alloc);
    try std.testing.expectEqualStrings("original", try snapshot.get(.{}, "a"));
}

test "mutable shared entry snapshot ownership survives allocation failures" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(alloc: Allocator) !void {
            var mutable: ActiveMemTable = .{};
            defer mutable.deinit(alloc);
            try mutable.upsert(alloc, .{ .name = "docs" }, "a", "original", false);
            var snapshot = try mutable.snapshot(alloc);
            defer snapshot.deinit(alloc);
            try mutable.upsert(alloc, .{ .name = "docs" }, "a", "replacement", false);
            try std.testing.expectEqualStrings("original", try snapshot.get(.{ .name = "docs" }, "a"));
            var state: State = .{};
            defer state.deinit(alloc);
            try state.upsert(alloc, .{}, "b", "moved", false);
            try applyStateMoveToActive(&mutable, alloc, &state);
        }
    }.run, .{});
}

test "mergeStates prefers newer entries and preserves ordering" {
    var older: State = .{};
    defer older.deinit(std.testing.allocator);
    try older.appendUpsert(std.testing.allocator, .{}, "doc:a", "A1", false);
    try older.appendUpsert(std.testing.allocator, .{}, "doc:c", "C1", false);
    try older.appendUpsert(std.testing.allocator, .{ .name = "meta" }, "lsn", "1", false);

    var newer: State = .{};
    defer newer.deinit(std.testing.allocator);
    try newer.appendUpsert(std.testing.allocator, .{}, "doc:b", "B1", false);
    try newer.appendUpsert(std.testing.allocator, .{}, "doc:c", "C2", false);
    try newer.appendUpsert(std.testing.allocator, .{ .name = "meta" }, "lsn", "", true);

    var merged = try mergeStates(std.testing.allocator, &older, &newer);
    defer merged.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 4), merged.entries.items.len);
    try std.testing.expectEqualStrings("doc:a", merged.entries.items[0].key);
    try std.testing.expectEqualStrings("doc:b", merged.entries.items[1].key);
    try std.testing.expectEqualStrings("doc:c", merged.entries.items[2].key);
    try std.testing.expectEqualStrings("C2", merged.entries.items[2].value);
    try std.testing.expect(merged.entries.items[3].tombstone);
    try std.testing.expectEqualStrings("lsn", merged.entries.items[3].key);
}

test "mergeStatesMove prefers newer entries and consumes inputs" {
    var older: State = .{};
    try older.appendUpsert(std.testing.allocator, .{}, "doc:a", "A1", false);
    try older.appendUpsert(std.testing.allocator, .{}, "doc:c", "C1", false);

    var newer: State = .{};
    try newer.appendUpsert(std.testing.allocator, .{}, "doc:b", "B1", false);
    try newer.appendUpsert(std.testing.allocator, .{}, "doc:c", "C2", false);

    var merged = try mergeStatesMove(std.testing.allocator, &older, &newer);
    defer merged.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), older.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), newer.entries.items.len);
    try std.testing.expectEqual(@as(usize, 3), merged.entries.items.len);
    try std.testing.expectEqualStrings("doc:a", merged.entries.items[0].key);
    try std.testing.expectEqualStrings("doc:b", merged.entries.items[1].key);
    try std.testing.expectEqualStrings("doc:c", merged.entries.items[2].key);
    try std.testing.expectEqualStrings("C2", merged.entries.items[2].value);
}

test "applyStateMove updates active mutable in place and consumes source" {
    var target: State = .{};
    defer target.deinit(std.testing.allocator);
    try target.appendUpsert(std.testing.allocator, .{}, "doc:a", "A1", false);
    try target.appendUpsert(std.testing.allocator, .{}, "doc:c", "C1", false);

    var source: State = .{};
    try source.appendUpsert(std.testing.allocator, .{}, "doc:b", "B1", false);
    try source.appendUpsert(std.testing.allocator, .{}, "doc:c", "C2", false);
    try source.appendUpsert(std.testing.allocator, .{}, "doc:d", "D1", false);

    try applyStateMove(&target, std.testing.allocator, &source);

    try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
    try std.testing.expectEqual(@as(usize, 4), target.entries.items.len);
    try std.testing.expectEqualStrings("doc:a", target.entries.items[0].key);
    try std.testing.expectEqualStrings("doc:b", target.entries.items[1].key);
    try std.testing.expectEqualStrings("doc:c", target.entries.items[2].key);
    try std.testing.expectEqualStrings("C2", target.entries.items[2].value);
    try std.testing.expectEqualStrings("doc:d", target.entries.items[3].key);
}

test "applyStateMove copies arena backed source into active mutable" {
    var target: ActiveMemTable = .{};
    defer target.deinit(std.testing.allocator);

    var source: State = .{};
    const arena_allocator = try source.ensureArenaAllocator(std.testing.allocator);
    try source.entries.append(std.testing.allocator, try initArenaEntry(arena_allocator, .{}, "doc:a", "A1", false));

    try applyMutableMoveToMutable(&target, std.testing.allocator, &source);

    try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
    try std.testing.expectEqual(@as(usize, 1), target.entries.items.len);
    try std.testing.expect(target.arena_owner == null);
    try std.testing.expect(target.entries.items[0].shared != null);
    try std.testing.expectEqualStrings("A1", try target.get(.{}, "doc:a"));
}

test "applyMutableMoveToMutable transfers shared active entries without copying" {
    var target: ActiveMemTable = .{};
    defer target.deinit(std.testing.allocator);

    var source: ActiveMemTable = .{};
    try source.upsert(std.testing.allocator, .{}, "doc:a", "A1", false);
    try source.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:b", "B1", false);
    const first_bytes = (try source.get(.{}, "doc:a")).ptr;

    try applyMutableMoveToMutable(&target, std.testing.allocator, &source);

    try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
    try std.testing.expect(source.arena_owner == null);
    try std.testing.expect(target.arena_owner == null);
    try std.testing.expectEqual(@as(usize, 2), target.entries.items.len);
    for (target.entries.items) |entry| {
        try std.testing.expect(entry.shared != null);
    }
    try std.testing.expectEqualStrings("A1", try target.get(.{}, "doc:a"));
    try std.testing.expectEqual(first_bytes, (try target.get(.{}, "doc:a")).ptr);
    try std.testing.expectEqualStrings("B1", try target.get(.{ .name = "docs" }, "doc:b"));
}

test "mergeStatesMove clones arena backed inputs before consuming them" {
    var older: State = .{};
    const older_allocator = try older.ensureArenaAllocator(std.testing.allocator);
    try older.entries.append(std.testing.allocator, try initArenaEntry(older_allocator, .{}, "doc:a", "A1", false));

    var newer: State = .{};
    const newer_allocator = try newer.ensureArenaAllocator(std.testing.allocator);
    try newer.entries.append(std.testing.allocator, try initArenaEntry(newer_allocator, .{}, "doc:b", "B1", false));

    var merged = try mergeStatesMove(std.testing.allocator, &older, &newer);
    defer merged.deinit(std.testing.allocator);

    try std.testing.expectEqual(@as(usize, 0), older.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), newer.entries.items.len);
    try std.testing.expectEqual(@as(usize, 2), merged.entries.items.len);
    try std.testing.expect(!merged.entries.items[0].key_from_arena);
    try std.testing.expect(!merged.entries.items[1].value_from_arena);
    try std.testing.expectEqualStrings("doc:a", merged.entries.items[0].key);
    try std.testing.expectEqualStrings("doc:b", merged.entries.items[1].key);
}

test "ActiveMemTable overwrites by hash index and materializes sorted state" {
    var active: ActiveMemTable = .{};
    defer active.deinit(std.testing.allocator);

    try active.upsert(std.testing.allocator, .{}, "doc:c", "C1", false);
    try active.upsert(std.testing.allocator, .{}, "doc:a", "A1", false);
    try active.upsert(std.testing.allocator, .{ .name = "meta" }, "lsn", "1", false);
    try active.upsert(std.testing.allocator, .{}, "doc:c", "C2", false);

    try std.testing.expectEqual(@as(usize, 3), active.entries.items.len);
    try std.testing.expect(active.arena_owner == null);
    for (active.entries.items) |entry| {
        try std.testing.expect(entry.shared != null);
    }
    try std.testing.expectEqualStrings("C2", try active.get(.{}, "doc:c"));

    var flushed = try active.clone(std.testing.allocator);
    defer flushed.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 3), flushed.entries.items.len);
    try std.testing.expectEqualStrings("doc:a", flushed.entries.items[0].key);
    try std.testing.expectEqualStrings("doc:c", flushed.entries.items[1].key);
    try std.testing.expectEqualStrings("C2", flushed.entries.items[1].value);
    try std.testing.expectEqualStrings("lsn", flushed.entries.items[2].key);

    var moved = try active.toStateMove(std.testing.allocator);
    defer moved.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(usize, 0), active.entries.items.len);
    try std.testing.expectEqual(@as(usize, 0), active.index.count());
    try std.testing.expect(active.arena_owner == null);
    try std.testing.expect(moved.arena_owner == null);
    for (moved.entries.items) |entry| try std.testing.expect(entry.shared != null);
    try std.testing.expectEqual(@as(usize, 3), moved.entries.items.len);
    try std.testing.expectEqualStrings("doc:a", moved.entries.items[0].key);
    try std.testing.expectEqualStrings("doc:c", moved.entries.items[1].key);
}

test "EntryIndex stores unique hashes inline and preserves collision lookup" {
    var entries: std.ArrayListUnmanaged(OwnedEntry) = .empty;
    defer {
        for (entries.items) |*entry| entry.deinit(std.testing.allocator);
        entries.deinit(std.testing.allocator);
    }
    try entries.append(std.testing.allocator, try initEntry(std.testing.allocator, .{}, "alpha", "1", false));
    try entries.append(std.testing.allocator, try initEntry(std.testing.allocator, .{}, "beta", "2", false));

    var index: EntryIndex = .{};
    defer index.deinit(std.testing.allocator);
    const forced_hash: u64 = 42;
    try index.insert(std.testing.allocator, forced_hash, 0);
    try index.insert(std.testing.allocator, forced_hash, 1);

    try std.testing.expectEqual(@as(usize, 1), index.primary.count());
    try std.testing.expectEqual(@as(usize, 1), index.collisions.count());
    try std.testing.expectEqual(index.collisions.get(forced_hash).?.capacity * @sizeOf(usize), index.collision_capacity_bytes);
    try std.testing.expectEqual(
        hashMapAllocationBytes(u64, usize, index.primary.capacity()) + hashMapAllocationBytes(u64, CollisionBucket, index.collisions.capacity()) + index.collision_capacity_bytes,
        index.estimatedMemoryBytes(),
    );
    try std.testing.expectEqual(@as(?usize, 0), index.find(entries.items, forced_hash, .{}, "alpha"));
    try std.testing.expectEqual(@as(?usize, 1), index.find(entries.items, forced_hash, .{}, "beta"));
    try std.testing.expectEqual(@as(?usize, null), index.find(entries.items, forced_hash, .{}, "missing"));
}
