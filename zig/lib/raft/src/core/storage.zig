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
    alloc: std.mem.Allocator,
    hard_state: types.HardState = .{},
    conf_state: types.ConfState = .{},
    snapshot_state: types.Snapshot = .{},
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

    pub fn setHardState(self: *MemoryStorage, hard_state: types.HardState) void {
        self.hard_state = hard_state;
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
        const first_new_index = entries[0].index;
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

        try self.entries_state.ensureUnusedCapacity(self.alloc, entries.len);
        for (entries) |entry| self.entries_state.appendAssumeCapacity(try entry.clone(self.alloc));
    }

    pub fn applySnapshot(self: *MemoryStorage, snapshot: types.Snapshot) !void {
        for (self.entries_state.items) |*entry| entry.deinit(self.alloc);
        self.entries_state.clearRetainingCapacity();
        try self.setConfState(snapshot.metadata.conf_state);
        self.snapshot_state.deinit(self.alloc);
        self.snapshot_state = try snapshot.clone(self.alloc);
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
        if (index == self.snapshot_state.metadata.index) return self.snapshot_state.metadata.term;
        for (self.entries_state.items) |entry| {
            if (entry.index == index) return entry.term;
        }
        return error.IndexNotFound;
    }

    fn firstIndexImpl(ptr: *anyopaque) !types.Index {
        const self: *MemoryStorage = @ptrCast(@alignCast(ptr));
        if (self.entries_state.items.len > 0) return self.entries_state.items[0].index;
        return self.snapshot_state.metadata.index + 1;
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
