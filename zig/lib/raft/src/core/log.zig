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
const storage_mod = @import("storage.zig");

pub const RaftLog = struct {
    alloc: std.mem.Allocator,
    snapshot_index: types.Index,
    snapshot_term: types.Term,
    entries: std.ArrayListUnmanaged(types.Entry) = .empty,
    stable_index: types.Index,
    persisting_index: types.Index,
    committed: types.Index = 0,
    applying: types.Index = 0,
    applied: types.Index = 0,

    pub fn init(alloc: std.mem.Allocator, storage: storage_mod.Storage) !RaftLog {
        var snapshot = try storage.snapshot(alloc);
        defer snapshot.deinit(alloc);

        const first_index = try storage.firstIndex();
        const last_index = try storage.lastIndex();

        var loaded_entries = std.ArrayListUnmanaged(types.Entry).empty;
        errdefer {
            for (loaded_entries.items) |*entry| entry.deinit(alloc);
            loaded_entries.deinit(alloc);
        }

        if (last_index >= first_index) {
            const from_storage = try storage.entries(alloc, first_index, last_index + 1, 0);
            defer types.freeEntries(alloc, from_storage);
            try loaded_entries.ensureUnusedCapacity(alloc, from_storage.len);
            for (from_storage) |entry| loaded_entries.appendAssumeCapacity(try entry.clone(alloc));
        }

        return .{
            .alloc = alloc,
            .snapshot_index = snapshot.metadata.index,
            .snapshot_term = snapshot.metadata.term,
            .entries = loaded_entries,
            .stable_index = last_index,
            .persisting_index = last_index,
            .committed = snapshot.metadata.index,
            .applying = snapshot.metadata.index,
            .applied = snapshot.metadata.index,
        };
    }

    pub fn deinit(self: *RaftLog) void {
        for (self.entries.items) |*entry| entry.deinit(self.alloc);
        self.entries.deinit(self.alloc);
        self.* = undefined;
    }

    pub fn firstIndex(self: *const RaftLog) types.Index {
        if (self.entries.items.len > 0) return self.entries.items[0].index;
        return self.snapshot_index + 1;
    }

    pub fn lastIndex(self: *const RaftLog) types.Index {
        if (self.entries.items.len > 0) return self.entries.items[self.entries.items.len - 1].index;
        return self.snapshot_index;
    }

    pub fn term(self: *const RaftLog, index: types.Index) ?types.Term {
        if (index == self.snapshot_index) return self.snapshot_term;
        for (self.entries.items) |entry| {
            if (entry.index == index) return entry.term;
        }
        return null;
    }

    pub fn matchTerm(self: *const RaftLog, index: types.Index, expected_term: types.Term) bool {
        return self.term(index) == expected_term;
    }

    pub fn appendEntries(self: *RaftLog, entries: []const types.Entry) !types.Index {
        if (entries.len == 0) return self.lastIndex();

        const first_new_index = entries[0].index;
        var truncate_at: ?usize = null;
        for (self.entries.items, 0..) |entry, i| {
            if (entry.index >= first_new_index) {
                truncate_at = i;
                break;
            }
        }
        if (truncate_at) |idx| {
            for (self.entries.items[idx..]) |*entry| entry.deinit(self.alloc);
            self.entries.shrinkRetainingCapacity(idx);
        }

        try self.entries.ensureUnusedCapacity(self.alloc, entries.len);
        for (entries) |entry| self.entries.appendAssumeCapacity(try entry.clone(self.alloc));

        return self.lastIndex();
    }

    pub fn maybeAppend(self: *RaftLog, prev_index: types.Index, prev_term: types.Term, leader_commit: types.Index, entries: []const types.Entry) !?types.Index {
        if (!self.matchTerm(prev_index, prev_term)) return null;

        const last_new_index = try self.appendEntries(entries);
        self.commitTo(@min(leader_commit, last_new_index));
        return last_new_index;
    }

    pub fn commitTo(self: *RaftLog, index: types.Index) void {
        if (index > self.committed) self.committed = index;
    }

    pub fn appliedTo(self: *RaftLog, index: types.Index) void {
        std.debug.assert(index <= self.committed);
        if (index > self.applied) self.applied = index;
        if (self.applying < self.applied) self.applying = self.applied;
    }

    pub fn compactTo(self: *RaftLog, index: types.Index) !void {
        if (index <= self.snapshot_index) return;
        if (index > self.applied) return error.CompactBeyondApplied;

        const snap_term = self.term(index) orelse return error.IndexNotFound;
        var remove_count: usize = 0;
        while (remove_count < self.entries.items.len and self.entries.items[remove_count].index <= index) {
            remove_count += 1;
        }
        if (remove_count == 0) return;

        for (self.entries.items[0..remove_count]) |*entry| entry.deinit(self.alloc);
        std.mem.copyForwards(types.Entry, self.entries.items[0 .. self.entries.items.len - remove_count], self.entries.items[remove_count..]);
        self.entries.shrinkRetainingCapacity(self.entries.items.len - remove_count);

        self.snapshot_index = index;
        self.snapshot_term = snap_term;
        if (self.stable_index < index) self.stable_index = index;
        if (self.persisting_index < index) self.persisting_index = index;
        if (self.committed < index) self.committed = index;
        if (self.applying < index) self.applying = index;
        if (self.applied < index) self.applied = index;
    }

    pub fn unstableEntries(self: *const RaftLog) []const types.Entry {
        const start = @max(self.stable_index, self.persisting_index) + 1;
        for (self.entries.items, 0..) |entry, i| {
            if (entry.index >= start) return self.entries.items[i..];
        }
        return &.{};
    }

    pub fn hasNextUnstableEntries(self: *const RaftLog) bool {
        return self.unstableEntries().len > 0;
    }

    pub fn hasNextOrInProgressUnstableEntries(self: *const RaftLog) bool {
        return self.lastIndex() > self.stable_index;
    }

    pub fn nextCommittedEntries(self: *const RaftLog) []const types.Entry {
        return self.nextCommittedEntriesMaxAllow(0, true);
    }

    pub fn nextCommittedEntriesMax(self: *const RaftLog, max_bytes: usize) []const types.Entry {
        return self.nextCommittedEntriesMaxAllow(max_bytes, true);
    }

    pub fn nextCommittedEntriesMaxAllow(self: *const RaftLog, max_bytes: usize, allow_unstable: bool) []const types.Entry {
        const start = self.applying + 1;
        const end = if (allow_unstable) self.committed else @min(self.committed, self.stable_index);
        if (start > end) return &.{};

        var low: ?usize = null;
        var high: ?usize = null;
        for (self.entries.items, 0..) |entry, i| {
            if (entry.index == start and low == null) low = i;
            if (entry.index == end) {
                high = i + 1;
                break;
            }
        }
        if (low == null or high == null) return &.{};
        const entries = self.entries.items[low.?..high.?];
        if (max_bytes == 0) return entries;
        return types.limitEntriesByBytes(entries, max_bytes);
    }

    pub fn hasNextCommittedEntries(self: *const RaftLog) bool {
        return self.nextCommittedEntries().len > 0;
    }

    pub fn hasNextCommittedEntriesAllow(self: *const RaftLog, allow_unstable: bool) bool {
        return self.nextCommittedEntriesMaxAllow(0, allow_unstable).len > 0;
    }

    pub fn stableTo(self: *RaftLog, index: types.Index) void {
        if (index > self.stable_index) self.stable_index = index;
        if (self.persisting_index < self.stable_index) self.persisting_index = self.stable_index;
    }

    pub fn acceptPersisting(self: *RaftLog, index: types.Index) void {
        if (index > self.persisting_index) self.persisting_index = index;
    }

    pub fn acceptApplying(self: *RaftLog, index: types.Index) void {
        if (index > self.applying) self.applying = index;
    }

    pub fn entriesFrom(self: *const RaftLog, from: types.Index) []const types.Entry {
        for (self.entries.items, 0..) |entry, i| {
            if (entry.index >= from) return self.entries.items[i..];
        }
        return &.{};
    }

    pub fn entriesFromMax(self: *const RaftLog, from: types.Index, max_bytes: usize) []const types.Entry {
        const entries = self.entriesFrom(from);
        return types.limitEntriesByBytes(entries, max_bytes);
    }

    pub fn restore(self: *RaftLog, snapshot: types.Snapshot) void {
        for (self.entries.items) |*entry| entry.deinit(self.alloc);
        self.entries.clearRetainingCapacity();
        self.snapshot_index = snapshot.metadata.index;
        self.snapshot_term = snapshot.metadata.term;
        self.stable_index = snapshot.metadata.index;
        self.persisting_index = snapshot.metadata.index;
        self.committed = snapshot.metadata.index;
        self.applying = snapshot.metadata.index;
        self.applied = snapshot.metadata.index;
    }
};

test "raft log replaces conflicting suffix" {
    var mem = storage_mod.MemoryStorage.init(std.testing.allocator);
    defer mem.deinit();

    try mem.append(&.{
        .{ .index = 1, .term = 1 },
        .{ .index = 2, .term = 1 },
    });

    var log = try RaftLog.init(std.testing.allocator, mem.storage());
    defer log.deinit();

    _ = try log.appendEntries(&.{
        .{ .index = 2, .term = 2 },
        .{ .index = 3, .term = 2 },
    });

    try std.testing.expectEqual(@as(types.Term, 2), log.term(2).?);
    try std.testing.expectEqual(@as(types.Index, 3), log.lastIndex());
}
