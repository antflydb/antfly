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
const groups = @import("group_sources.zig");
const state = @import("group_source_state.zig");
const index = @import("group_source_index.zig");
const identity = @import("group_source_identity.zig");
const A = std.mem.Allocator;
const Txn = struct {
    const Self = @This();
    a: A,
    incarnation: u128 = 7,
    values: std.StringHashMapUnmanaged([]const u8) = .empty,
    pub fn groupIncarnation(self: *const Self) u128 {
        return self.incarnation;
    }
    const Row = struct { key: []const u8, value: []const u8 };
    const Cursor = struct {
        txn: *Self,
        last: []const u8 = "",
        pub fn close(_: *@This()) void {}
        pub fn seekAtOrAfter(self: *@This(), after: []const u8) !?Row {
            var chosen: ?Row = null;
            var it = self.txn.values.iterator();
            while (it.next()) |entry| {
                if (std.mem.order(u8, entry.key_ptr.*, after) == .lt) continue;
                if (chosen == null or std.mem.order(u8, entry.key_ptr.*, chosen.?.key) == .lt) chosen = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
            }
            if (chosen) |row| self.last = row.key;
            return chosen;
        }
        pub fn next(self: *@This()) !?Row {
            return self.seekAtOrAfter(try std.mem.concat(self.txn.a, u8, &.{ self.last, "\x00" }));
        }
    };
    pub fn openPhysicalCursorAdapter(self: *Self) !Cursor {
        return .{ .txn = self };
    }
    pub fn get(self: *Self, selected: []const u8) ![]const u8 {
        return self.values.get(selected) orelse error.NotFound;
    }
    pub fn put(self: *Self, selected: []const u8, raw: []const u8) !void {
        try self.values.put(self.a, try self.a.dupe(u8, selected), try self.a.dupe(u8, raw));
    }
    pub fn delete(self: *Self, selected: []const u8) !void {
        _ = self.values.remove(selected);
    }
};
test "group leases fence catalog ABA and root replacement even with the same worker identity" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 9, .configuration = groups.digest("definition") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const partition = index.Partition{ .group = group.key, .segment = "turn", .cell = 0 };
    const source = state.Owner{ .namespace = @splat(2), .incarnation = 8 };
    const event = index.Invalidation{ .definition = definition, .partition = partition, .document = "row", .source_revision = 1, .ordinal = 1 };
    _ = try state.invalidate(a, &txn, source, event);
    const worker = groups.digest("same-server-worker");
    const old = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, worker, 100, 30000);
    const prefix = try identity.documentMetadataKey(a, try group.anchor(a), "");
    var deleted: std.ArrayList([]const u8) = .empty;
    var it = txn.values.keyIterator();
    while (it.next()) |key| if (std.mem.startsWith(u8, key.*, prefix)) {
        try deleted.append(a, key.*);
    };
    for (deleted.items) |key| try txn.delete(key);
    // Re-add an identical catalog: all local group revisions start at one.
    _ = try state.invalidate(a, &txn, source, event);
    const fresh = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, worker, 101, 30000);
    try std.testing.expectEqual(old.revision, fresh.revision);
    try std.testing.expect(fresh.token > old.token);
    try std.testing.expectError(error.EnrichmentSourceChanged, state.verify(a, &txn, group, partition, old, 102));
    // Restored/transferred leases cannot delay or authorize the replacement root.
    txn.incarnation = 10;
    try std.testing.expectError(error.EnrichmentSourceChanged, state.verify(a, &txn, group, partition, fresh, 102));
    const replaced = try state.claim(a, &txn, group, (try state.nextPending(a, &txn, group)).?, worker, 102, 30000);
    try std.testing.expect(replaced.token > fresh.token and replaced.incarnation == 10);
}
