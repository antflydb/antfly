// Copyright 2026 Antfly, Inc.
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
const types = @import("types.zig");

pub const Storage = struct {
    pub const InitialState = struct {
        hard_state: types.HardState = .{},
        conf_state: types.ConfState = .{},

        pub fn deinit(self: *InitialState, alloc: std.mem.Allocator) void {
            self.conf_state.deinit(alloc);
            self.* = undefined;
        }
    };

    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        initial_state: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror!InitialState,
        entries: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator, low: types.Index, high: types.Index, max_bytes: usize) anyerror![]types.Entry,
        term: *const fn (ptr: *anyopaque, index: types.Index) anyerror!types.Term,
        first_index: *const fn (ptr: *anyopaque) anyerror!types.Index,
        last_index: *const fn (ptr: *anyopaque) anyerror!types.Index,
        snapshot: *const fn (ptr: *anyopaque, alloc: std.mem.Allocator) anyerror!types.Snapshot,
    };

    pub fn initialState(self: Storage, alloc: std.mem.Allocator) !InitialState {
        return try self.vtable.initial_state(self.ptr, alloc);
    }

    pub fn entries(self: Storage, alloc: std.mem.Allocator, low: types.Index, high: types.Index, max_bytes: usize) ![]types.Entry {
        return try self.vtable.entries(self.ptr, alloc, low, high, max_bytes);
    }

    pub fn term(self: Storage, index: types.Index) !types.Term {
        return try self.vtable.term(self.ptr, index);
    }

    pub fn firstIndex(self: Storage) !types.Index {
        return try self.vtable.first_index(self.ptr);
    }

    pub fn lastIndex(self: Storage) !types.Index {
        return try self.vtable.last_index(self.ptr);
    }

    pub fn snapshot(self: Storage, alloc: std.mem.Allocator) !types.Snapshot {
        return try self.vtable.snapshot(self.ptr, alloc);
    }
};

pub const MemoryStorage = struct {
    pub const Diagnostics = struct {
        entries: usize = 0,
        entry_capacity: usize = 0,
        first_index: types.Index = 0,
        last_index: types.Index = 0,
        snapshot_index: types.Index = 0,
        snapshot_bytes: usize = 0,
        entry_payload_bytes: usize = 0,
        estimated_bytes: usize = 0,
    };

    alloc: std.mem.Allocator,
    hard_state: types.HardState = .{},
    conf_state: types.ConfState = .{},
    snapshot_state: types.Snapshot = .{},
    compacted_index: types.Index = 0,
    compacted_term: types.Term = 0,
    entries_state: std.ArrayListUnmanaged(types.Entry) = .empty,

    pub fn init(alloc: std.mem.Allocator) MemoryStorage {
        return .{ .alloc = alloc };
    }

    pub fn deinit(self: *MemoryStorage) void {
        for (self.entries_state.items) |*entry| entry.deinit(self.alloc);
        self.entries_state.deinit(self.alloc);
        self.conf_state.deinit(self.alloc);
        self.snapshot_state.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn storage(self: *MemoryStorage) Storage {
        return .{
            .ptr = self,
            .vtable = &.{
                .initial_state = initialStateImpl,
                .entries = entriesImpl,
                .term = termImpl,
                .first_index = firstIndexImpl,
                .last_index = lastIndexImpl,
                .snapshot = snapshotImpl,
            },
        };
    }

    pub fn diagnostics(self: *const MemoryStorage) Diagnostics {
        var out = Diagnostics{
            .entries = self.entries_state.items.len,
            .entry_capacity = self.entries_state.capacity,
            .first_index = self.compacted_index + 1,
            .last_index = if (self.entries_state.items.len > 0) self.entries_state.items[self.entries_state.items.len - 1].index else self.snapshot_state.metadata.index,
            .snapshot_index = self.snapshot_state.metadata.index,
            .snapshot_bytes = self.snapshot_state.data.len,
            .estimated_bytes = @sizeOf(types.Entry) * self.entries_state.capacity +
                self.snapshot_state.data.len +
                confStateEstimatedBytes(self.conf_state) +
                confStateEstimatedBytes(self.snapshot_state.metadata.conf_state),
        };
        for (self.entries_state.items) |entry| {
            out.entry_payload_bytes += entry.data.len;
            out.estimated_bytes += entry.data.len;
        }
        return out;
    }

    pub fn setHardState(self: *MemoryStorage, hard_state: types.HardState) void {
        self.hard_state = hard_state;
    }

    pub fn compactedIndex(self: *const MemoryStorage) types.Index {
        return self.compacted_index;
    }

    pub fn compactedTerm(self: *const MemoryStorage) types.Term {
        return self.compacted_term;
    }

    pub fn setConfState(self: *MemoryStorage, conf_state: types.ConfState) !void {
        var cloned = try conf_state.clone(self.alloc);
        errdefer cloned.deinit(self.alloc);

        self.conf_state.deinit(self.alloc);
        self.conf_state = cloned;
    }

    pub fn seedConfState(self: *MemoryStorage, conf_state: types.ConfState) !void {
        try self.setConfState(conf_state);

        var snapshot = types.Snapshot{
            .metadata = .{
                .index = 0,
                .term = 0,
                .conf_state = try conf_state.clone(self.alloc),
            },
            .data = &.{},
        };
        errdefer snapshot.deinit(self.alloc);

        self.snapshot_state.deinit(self.alloc);
        self.snapshot_state = snapshot;
    }

    pub fn append(self: *MemoryStorage, entries: []const types.Entry) !void {
        if (entries.len == 0) return;
        var first_new: usize = 0;
        while (first_new < entries.len and entries[first_new].index <= self.compacted_index) : (first_new += 1) {}
        if (first_new == entries.len) return;
        const retained_entries = entries[first_new..];
        const first_new_index = retained_entries[0].index;
        var truncate_at: ?usize = null;
        for (self.entries_state.items, 0..) |entry, i| {
            if (entry.index >= first_new_index) {
                truncate_at = i;
                break;
            }
        }
        if (truncate_at) |idx| {
            for (self.entries_state.items[idx..]) |*entry| entry.deinit(self.alloc);
            self.entries_state.shrinkRetainingCapacity(idx);
        }

        try self.entries_state.ensureUnusedCapacity(self.alloc, retained_entries.len);
        for (retained_entries) |entry| self.entries_state.appendAssumeCapacity(try entry.clone(self.alloc));
    }

    pub fn applySnapshot(self: *MemoryStorage, snapshot: types.Snapshot) !void {
        for (self.entries_state.items) |*entry| entry.deinit(self.alloc);
        self.entries_state.clearRetainingCapacity();
        try self.setConfState(snapshot.metadata.conf_state);
        self.snapshot_state.deinit(self.alloc);
        self.snapshot_state = try snapshot.cloneDetached(self.alloc);
        self.compacted_index = snapshot.metadata.index;
        self.compacted_term = snapshot.metadata.term;
    }

    /// Installs a locally-created state-machine snapshot while retaining log
    /// entries newer than compact_index for follower replication. The state
    /// snapshot may be newer than the log compaction boundary.
    pub fn compactToSnapshot(self: *MemoryStorage, snapshot: types.Snapshot, compact_index: types.Index) !void {
        if (snapshot.metadata.index < self.snapshot_state.metadata.index) return error.SnapshotOutOfDate;
        if (compact_index > snapshot.metadata.index or compact_index < self.compacted_index)
            return error.InvalidCompactionBoundary;
        const local_term = try termImpl(self, snapshot.metadata.index);
        if (local_term != snapshot.metadata.term) return error.SnapshotTermMismatch;
        const compact_term = if (compact_index == self.compacted_index)
            self.compacted_term
        else
            try termImpl(self, compact_index);

        var owned_snapshot = try snapshot.cloneDetached(self.alloc);
        errdefer owned_snapshot.deinit(self.alloc);
        try self.setConfState(snapshot.metadata.conf_state);

        var remove_count: usize = 0;
        while (remove_count < self.entries_state.items.len and
            self.entries_state.items[remove_count].index <= compact_index)
        {
            remove_count += 1;
        }
        for (self.entries_state.items[0..remove_count]) |*entry| entry.deinit(self.alloc);
        if (remove_count > 0) {
            std.mem.copyForwards(
                types.Entry,
                self.entries_state.items[0 .. self.entries_state.items.len - remove_count],
                self.entries_state.items[remove_count..],
            );
            self.entries_state.shrinkRetainingCapacity(self.entries_state.items.len - remove_count);
        }

        self.snapshot_state.deinit(self.alloc);
        self.snapshot_state = owned_snapshot;
        self.compacted_index = compact_index;
        self.compacted_term = compact_term;
    }

    pub fn restoreCompactionBoundary(self: *MemoryStorage, index: types.Index, term: types.Term) !void {
        if (index > self.snapshot_state.metadata.index) return error.InvalidCompactionBoundary;
        if (index == 0 and term != 0) return error.InvalidCompactionBoundary;
        self.compacted_index = index;
        self.compacted_term = term;
    }

    pub fn validate(self: *const MemoryStorage) !void {
        if (self.compacted_index > self.snapshot_state.metadata.index) return error.InvalidCompactionBoundary;
        if (self.compacted_index == 0 and self.compacted_term != 0) return error.InvalidCompactionBoundary;
        if (self.snapshot_state.metadata.index == self.compacted_index and
            self.snapshot_state.metadata.term != self.compacted_term)
        {
            return error.SnapshotTermMismatch;
        }
        if (self.entries_state.items.len == 0) {
            if (self.compacted_index != self.snapshot_state.metadata.index) return error.InvalidCompactionBoundary;
            return;
        }
        if (self.entries_state.items[0].index != self.compacted_index + 1) return error.InvalidCompactionBoundary;
        for (self.entries_state.items[1..], self.entries_state.items[0 .. self.entries_state.items.len - 1]) |entry, previous| {
            if (entry.index != previous.index + 1) return error.InvalidCompactionBoundary;
        }
        if (self.entries_state.items[self.entries_state.items.len - 1].index < self.snapshot_state.metadata.index)
            return error.InvalidCompactionBoundary;
        if (self.snapshot_state.metadata.index > self.compacted_index and
            try termImpl(@constCast(self), self.snapshot_state.metadata.index) != self.snapshot_state.metadata.term)
        {
            return error.SnapshotTermMismatch;
        }
    }

    pub fn compactTo(self: *MemoryStorage, index: types.Index, conf_state: types.ConfState) !void {
        const snap_term = try termImpl(self, index);
        try self.setConfState(conf_state);

        var new_snapshot = types.Snapshot{
            .metadata = .{
                .index = index,
                .term = snap_term,
                .conf_state = try conf_state.clone(self.alloc),
            },
            .data = &.{},
        };
        errdefer new_snapshot.deinit(self.alloc);

        var remove_count: usize = 0;
        while (remove_count < self.entries_state.items.len and self.entries_state.items[remove_count].index <= index) {
            remove_count += 1;
        }

        for (self.entries_state.items[0..remove_count]) |*entry| entry.deinit(self.alloc);
        if (remove_count > 0) {
            std.mem.copyForwards(types.Entry, self.entries_state.items[0 .. self.entries_state.items.len - remove_count], self.entries_state.items[remove_count..]);
            self.entries_state.shrinkRetainingCapacity(self.entries_state.items.len - remove_count);
        }

        self.snapshot_state.deinit(self.alloc);
        self.snapshot_state = new_snapshot;
        self.compacted_index = index;
        self.compacted_term = snap_term;
    }

    fn initialStateImpl(ptr: *anyopaque, alloc: std.mem.Allocator) !Storage.InitialState {
        const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
        return .{
            .hard_state = self.hard_state,
            .conf_state = try self.conf_state.clone(alloc),
        };
    }

    fn entriesImpl(ptr: *anyopaque, alloc: std.mem.Allocator, low: types.Index, high: types.Index, max_bytes: usize) ![]types.Entry {
        const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
        if (high < low) return error.InvalidRange;

        var matches = std.ArrayListUnmanaged(types.Entry).empty;
        defer matches.deinit(alloc);

        for (self.entries_state.items) |entry| {
            if (entry.index < low) continue;
            if (entry.index >= high) break;
            try matches.append(alloc, try entry.clone(alloc));
        }

        const owned = try matches.toOwnedSlice(alloc);
        if (max_bytes == 0) return owned;
        const limited_len = types.limitEntriesByBytes(owned, max_bytes).len;
        if (limited_len == owned.len) return owned;

        for (owned[limited_len..]) |*entry| entry.deinit(alloc);
        return owned[0..limited_len];
    }

    fn termImpl(ptr: *anyopaque, index: types.Index) !types.Term {
        const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
        if (index == self.compacted_index) return self.compacted_term;
        if (index == self.snapshot_state.metadata.index) return self.snapshot_state.metadata.term;
        for (self.entries_state.items) |entry| {
            if (entry.index == index) return entry.term;
        }
        return error.IndexNotFound;
    }

    fn firstIndexImpl(ptr: *anyopaque) !types.Index {
        const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
        return self.compacted_index + 1;
    }

    fn lastIndexImpl(ptr: *anyopaque) !types.Index {
        const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
        if (self.entries_state.items.len > 0) return self.entries_state.items[self.entries_state.items.len - 1].index;
        return self.snapshot_state.metadata.index;
    }

    fn snapshotImpl(ptr: *anyopaque, alloc: std.mem.Allocator) !types.Snapshot {
        const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
        return try self.snapshot_state.clone(alloc);
    }
};

fn confStateEstimatedBytes(conf_state: types.ConfState) usize {
    return @sizeOf(types.NodeId) * (conf_state.voters.len +
        conf_state.voters_outgoing.len +
        conf_state.learners.len +
        conf_state.learners_next.len);
}

test "memory storage appends and serves entries" {
    var storage = MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();

    try storage.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
    });

    const entries = try storage.storage().entries(std.testing.allocator, 1, 3, 0);
    defer types.freeEntries(std.testing.allocator, entries);

    try std.testing.expectEqual(@as(usize, 2), entries.len);
    try std.testing.expectEqual(@as(types.Index, 1), try storage.storage().firstIndex());
    try std.testing.expectEqual(@as(types.Index, 2), try storage.storage().lastIndex());
}

test "memory storage keeps a replication suffix behind a newer state snapshot" {
    var storage = MemoryStorage.init(std.testing.allocator);
    defer storage.deinit();
    try storage.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
        .{ .index = 3, .term = 2 },
        .{ .index = 4, .term = 2 },
        .{ .index = 5, .term = 2 },
    });

    try storage.compactToSnapshot(.{
        .metadata = .{ .index = 5, .term = 2 },
        .data = @constCast("state-at-five"),
    }, 3);

    try std.testing.expectEqual(@as(types.Index, 3), storage.compactedIndex());
    try std.testing.expectEqual(@as(types.Term, 2), storage.compactedTerm());
    try std.testing.expectEqual(@as(types.Index, 4), try storage.storage().firstIndex());
    try std.testing.expectEqual(@as(types.Index, 5), try storage.storage().lastIndex());
    try std.testing.expectEqual(@as(types.Term, 2), try storage.storage().term(3));
    const retained = try storage.storage().entries(std.testing.allocator, 4, 6, 0);
    defer types.freeEntries(std.testing.allocator, retained);
    try std.testing.expectEqual(@as(usize, 2), retained.len);

    var snapshot = try storage.storage().snapshot(std.testing.allocator);
    defer snapshot.deinit(std.testing.allocator);
    try std.testing.expectEqual(@as(types.Index, 5), snapshot.metadata.index);
    try std.testing.expectEqualStrings("state-at-five", snapshot.data);
}
