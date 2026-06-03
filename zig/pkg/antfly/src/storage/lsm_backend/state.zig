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

const EntryIndexBucket = std.ArrayListUnmanaged(usize);
const EntryIndex = std.AutoHashMapUnmanaged(u64, EntryIndexBucket);

pub const OwnedEntry = struct {
    namespace_name: ?[]u8,
    namespace_from_arena: bool = false,
    key: []u8,
    key_from_arena: bool = false,
    value: []u8,
    value_from_arena: bool = false,
    tombstone: bool,

    pub fn deinit(self: *OwnedEntry, allocator: Allocator) void {
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
};

pub const State = struct {
    entries: std.ArrayListUnmanaged(OwnedEntry) = .empty,
    arena_owner: ?*std.heap.ArenaAllocator = null,

    pub fn deinit(self: *State, allocator: Allocator) void {
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
        try out.entries.ensureTotalCapacity(allocator, self.entries.items.len);
        for (self.entries.items) |entry| {
            out.entries.appendAssumeCapacity(try cloneEntry(allocator, entry));
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
        const entry = self.entries.items[idx];
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
        if (self.findIndex(namespace, key)) |idx| {
            try replaceEntryValueCopy(&self.entries.items[idx], allocator, value, tombstone, false);
            return;
        }

        const idx = self.lowerBound(namespace, key);
        try self.entries.insert(allocator, idx, try initEntry(allocator, namespace, key, value, tombstone));
    }

    pub fn appendUpsert(
        self: *State,
        allocator: Allocator,
        namespace: backend_types.Namespace,
        key: []const u8,
        value: []const u8,
        tombstone: bool,
    ) !void {
        if (self.entries.items.len == 0) {
            try self.entries.append(allocator, try initEntry(allocator, namespace, key, value, tombstone));
            return;
        }

        const last_idx = self.entries.items.len - 1;
        const last = self.entries.items[last_idx];
        switch (compareEntryTo(last, namespace, key)) {
            .lt => {
                try self.entries.append(allocator, try initEntry(allocator, namespace, key, value, tombstone));
            },
            .eq => {
                try replaceEntryValueCopy(&self.entries.items[last_idx], allocator, value, tombstone, false);
            },
            .gt => try self.upsert(allocator, namespace, key, value, tombstone),
        }
    }

    pub fn upsertMove(self: *State, allocator: Allocator, entry: OwnedEntry) !void {
        const namespace = namespaceOf(entry);
        const idx = self.lowerBound(namespace, entry.key);
        if (idx < self.entries.items.len and compareEntryTo(self.entries.items[idx], namespace, entry.key) == .eq) {
            replaceEntryValueMove(&self.entries.items[idx], allocator, entry);
            return;
        }
        try self.entries.insert(allocator, idx, entry);
    }

    pub fn lowerBound(self: *const State, namespace: backend_types.Namespace, key: []const u8) usize {
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
        if (idx >= self.entries.items.len) return null;
        if (compareEntryTo(self.entries.items[idx], namespace, key) != .eq) return null;
        return idx;
    }

    pub fn splitAtKey(self: *const State, allocator: Allocator, split_key: []const u8) !SplitStates {
        var left: State = .{};
        errdefer left.deinit(allocator);
        var right: State = .{};
        errdefer right.deinit(allocator);

        try left.entries.ensureTotalCapacity(allocator, self.entries.items.len);
        try right.entries.ensureTotalCapacity(allocator, self.entries.items.len);

        for (self.entries.items) |entry| {
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
    index: EntryIndex = .empty,
    arena_owner: ?*std.heap.ArenaAllocator = null,

    pub fn deinit(self: *ActiveMemTable, allocator: Allocator) void {
        for (self.entries.items) |*entry| entry.deinit(allocator);
        self.entries.deinit(allocator);
        deinitEntryIndex(&self.index, allocator);
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

    pub fn toStateMove(self: *ActiveMemTable, allocator: Allocator) !State {
        var out = State{
            .entries = self.entries,
            .arena_owner = self.arena_owner,
        };
        self.entries = .empty;
        self.arena_owner = null;
        self.clearIndex(allocator);
        sortStateEntries(&out);
        return out;
    }

    fn clearIndex(self: *ActiveMemTable, allocator: Allocator) void {
        deinitEntryIndex(&self.index, allocator);
        self.index = .empty;
    }

    pub fn resetAfterEntriesMoved(self: *ActiveMemTable, allocator: Allocator) void {
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

    pub fn ensureRecoveryAllocator(self: *ActiveMemTable, allocator: Allocator) !Allocator {
        return try self.ensureArenaAllocator(allocator);
    }

    pub fn get(self: *const ActiveMemTable, namespace: backend_types.Namespace, key: []const u8) ![]const u8 {
        const idx = self.findIndex(namespace, key) orelse return error.NotFound;
        const entry = self.entries.items[idx];
        if (entry.tombstone) return error.NotFound;
        return entry.value;
    }

    pub fn findIndex(self: *const ActiveMemTable, namespace: backend_types.Namespace, key: []const u8) ?usize {
        const bucket = self.index.get(hashEntryKey(namespace, key)) orelse return null;
        return findIndexInEntries(self.entries.items, bucket, namespace, key);
    }

    pub fn upsert(
        self: *ActiveMemTable,
        allocator: Allocator,
        namespace: backend_types.Namespace,
        key: []const u8,
        value: []const u8,
        tombstone: bool,
    ) !void {
        const entry_allocator = try self.ensureArenaAllocator(allocator);
        const entry_from_arena = true;
        const key_hash = hashEntryKey(namespace, key);
        const gop = try self.index.getOrPut(allocator, key_hash);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        if (findIndexInBucket(self, gop.value_ptr.*, namespace, key)) |idx| {
            try replaceEntryValueCopy(&self.entries.items[idx], entry_allocator, value, tombstone, entry_from_arena);
            return;
        }

        var owned = try initEntry(entry_allocator, namespace, key, value, tombstone);
        owned.namespace_from_arena = entry_from_arena;
        owned.key_from_arena = entry_from_arena;
        owned.value_from_arena = entry_from_arena;
        errdefer owned.deinit(allocator);
        try self.entries.append(allocator, owned);
        owned = undefined;
        const idx = self.entries.items.len - 1;
        gop.value_ptr.append(allocator, idx) catch |err| {
            var removed = self.entries.pop().?;
            removed.deinit(allocator);
            if (!gop.found_existing and gop.value_ptr.items.len == 0) {
                gop.value_ptr.deinit(allocator);
                _ = self.index.remove(key_hash);
            }
            return err;
        };
    }

    pub fn upsertMove(self: *ActiveMemTable, allocator: Allocator, entry: OwnedEntry) !void {
        const namespace = namespaceOf(entry);
        const key_hash = hashEntryKey(namespace, entry.key);
        const gop = try self.index.getOrPut(allocator, key_hash);
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        if (findIndexInBucket(self, gop.value_ptr.*, namespace, entry.key)) |idx| {
            replaceEntryValueMove(&self.entries.items[idx], allocator, entry);
            return;
        }

        try self.entries.append(allocator, entry);
        const idx = self.entries.items.len - 1;
        gop.value_ptr.append(allocator, idx) catch |err| {
            var removed = self.entries.pop().?;
            removed.deinit(allocator);
            if (!gop.found_existing and gop.value_ptr.items.len == 0) {
                gop.value_ptr.deinit(allocator);
                _ = self.index.remove(key_hash);
            }
            return err;
        };
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
    return .{
        .namespace_name = if (entry.namespace_name) |name| try allocator.dupe(u8, name) else null,
        .namespace_from_arena = false,
        .key = try allocator.dupe(u8, entry.key),
        .key_from_arena = false,
        .value = try allocator.dupe(u8, entry.value),
        .value_from_arena = false,
        .tombstone = entry.tombstone,
    };
}

pub fn initEntry(
    allocator: Allocator,
    namespace: backend_types.Namespace,
    key: []const u8,
    value: []const u8,
    tombstone: bool,
) !OwnedEntry {
    return .{
        .namespace_name = if (namespace.name) |name| try allocator.dupe(u8, name) else null,
        .namespace_from_arena = false,
        .key = try allocator.dupe(u8, key),
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
    for (source.entries.items) |entry| {
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

fn deinitEntryIndex(index: *EntryIndex, allocator: Allocator) void {
    var values = index.valueIterator();
    while (values.next()) |bucket| bucket.deinit(allocator);
    index.deinit(allocator);
}

fn findIndexInEntries(
    entries: []const OwnedEntry,
    bucket: EntryIndexBucket,
    namespace: backend_types.Namespace,
    key: []const u8,
) ?usize {
    for (bucket.items) |idx| {
        if (idx >= entries.len) continue;
        if (compareEntryTo(entries[idx], namespace, key) == .eq) return idx;
    }
    return null;
}

fn findIndexInBucket(
    self: *const ActiveMemTable,
    bucket: EntryIndexBucket,
    namespace: backend_types.Namespace,
    key: []const u8,
) ?usize {
    return findIndexInEntries(self.entries.items, bucket, namespace, key);
}

pub fn mergeStates(
    allocator: Allocator,
    older: *const State,
    newer: *const State,
) !State {
    var merged: State = .{};
    errdefer merged.deinit(allocator);

    try merged.entries.ensureTotalCapacity(allocator, older.entries.items.len + newer.entries.items.len);

    var older_idx: usize = 0;
    var newer_idx: usize = 0;
    while (older_idx < older.entries.items.len and newer_idx < newer.entries.items.len) {
        const older_entry = older.entries.items[older_idx];
        const newer_entry = newer.entries.items[newer_idx];
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

    while (older_idx < older.entries.items.len) : (older_idx += 1) {
        merged.entries.appendAssumeCapacity(try cloneEntry(allocator, older.entries.items[older_idx]));
    }
    while (newer_idx < newer.entries.items.len) : (newer_idx += 1) {
        merged.entries.appendAssumeCapacity(try cloneEntry(allocator, newer.entries.items[newer_idx]));
    }

    return merged;
}

pub fn mergeStatesMove(
    allocator: Allocator,
    older: *State,
    newer: *State,
) !State {
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
    if (source.arena_owner != null) {
        for (source.entries.items) |entry| {
            try target.upsert(allocator, namespaceOf(entry), entry.key, entry.value, entry.tombstone);
        }
        source.deinit(allocator);
        return;
    }

    for (source.entries.items) |entry| {
        try target.upsertMove(allocator, entry);
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

    for (source.entries.items) |entry| {
        try target.upsertMove(allocator, entry);
    }
    source.resetAfterEntriesMoved(allocator);
}

fn replaceEntryValueMove(target: *OwnedEntry, allocator: Allocator, source: OwnedEntry) void {
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
    try std.testing.expect(target.arena_owner != null);
    try std.testing.expect(target.entries.items[0].key_from_arena);
    try std.testing.expect(target.entries.items[0].value_from_arena);
    try std.testing.expectEqualStrings("A1", try target.get(.{}, "doc:a"));
}

test "applyMutableMoveToMutable copies arena backed active source into target arena" {
    var target: ActiveMemTable = .{};
    defer target.deinit(std.testing.allocator);

    var source: ActiveMemTable = .{};
    try source.upsert(std.testing.allocator, .{}, "doc:a", "A1", false);
    try source.upsert(std.testing.allocator, .{ .name = "docs" }, "doc:b", "B1", false);

    try applyMutableMoveToMutable(&target, std.testing.allocator, &source);

    try std.testing.expectEqual(@as(usize, 0), source.entries.items.len);
    try std.testing.expect(source.arena_owner == null);
    try std.testing.expect(target.arena_owner != null);
    try std.testing.expectEqual(@as(usize, 2), target.entries.items.len);
    for (target.entries.items) |entry| {
        try std.testing.expect(entry.key_from_arena);
        try std.testing.expect(entry.value_from_arena);
    }
    try std.testing.expectEqualStrings("A1", try target.get(.{}, "doc:a"));
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
    try std.testing.expect(active.arena_owner != null);
    for (active.entries.items) |entry| {
        try std.testing.expect(entry.key_from_arena);
        try std.testing.expect(entry.value_from_arena);
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
    try std.testing.expect(moved.arena_owner != null);
    try std.testing.expectEqual(@as(usize, 3), moved.entries.items.len);
    try std.testing.expectEqualStrings("doc:a", moved.entries.items[0].key);
    try std.testing.expectEqualStrings("doc:c", moved.entries.items[1].key);
}
