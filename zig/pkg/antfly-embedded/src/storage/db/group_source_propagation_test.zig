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
const propagation = @import("group_source_propagation.zig");
const A = std.mem.Allocator;
const Txn = struct {
    const Self = @This();
    a: A,
    values: std.StringHashMapUnmanaged([]const u8) = .empty,
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
test "original group event pages an entire causal horizon without transitive cascade or future starvation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const root = try group.anchor(a);
    for (0..301) |i| {
        const segment = if (i < 300) "one-later-segment" else "beyond-horizon";
        const partition = index.Partition{ .group = group.key, .segment = segment, .cell = @intCast(i + 1) };
        const unit = try std.fmt.allocPrint(a, "{s}/u/{x}", .{ try identity.partition(a, root, segment, partition.cell), &groups.digest(try std.fmt.allocPrint(a, "unit-{d}", .{i})) });
        const snapshot = state.Snapshot{ .revision = 1, .input_digest = groups.digest(unit), .units = &.{unit}, .sequence = 1, .ordinal = @intCast((i + 1) * 128) };
        try state.writeRecord(a, &txn, try state.snapshotKey(a, group, partition), snapshot);
        try state.writeRecord(a, &txn, try state.directoryKey(a, group, partition, snapshot.ordinal), state.DirectoryEntry{ .partition = partition, .snapshot = snapshot });
    }
    const event = index.Invalidation{ .definition = definition, .partition = .{ .group = group.key, .segment = "original", .cell = 0 }, .document = "source", .source_revision = 7, .since_sequence = 11, .through_sequence = 11, .ordinal = 0 };
    try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expect(!(try propagation.status(a, &txn, owner, event)).?.propagated);
    try std.testing.expectEqual(@as(u64, 1), (try state.currentHead(a, &txn, group)).pending);
    const enrolled = (try state.pendingFor(a, &txn, group, event.partition)).?;
    try state.writeRecord(a, &txn, try state.snapshotKey(a, group, event.partition), state.Snapshot{ .revision = enrolled.revision, .input_digest = groups.digest("original materialization"), .units = &.{}, .sequence = 0, .ordinal = 0 });
    try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expectEqual(@as(u64, 129), (try state.currentHead(a, &txn, group)).pending);
    // A new call is a restart: only durable cursor/segment state is retained.
    try propagation.advance(a, &txn, owner, event, 1);
    try propagation.advance(a, &txn, owner, event, 1);
    const complete = (try propagation.status(a, &txn, owner, event)).?;
    try std.testing.expect(complete.propagated and !complete.settled);
    try std.testing.expectEqual(@as(u64, 301), (try state.currentHead(a, &txn, group)).pending);
    const before = (try state.currentHead(a, &txn, group)).revision;
    try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expectEqual(before, (try state.currentHead(a, &txn, group)).revision);
    var future = event;
    future.document = "independent-future";
    future.partition = .{ .group = group.key, .segment = "independent-future", .cell = 400 };
    future.ordinal = 400 * 128;
    _ = try state.invalidate(a, &txn, owner, future);
    var completed: usize = 0;
    while (try state.nextPending(a, &txn, group)) |pending| {
        if (pending.revision > complete.frontier) break;
        const lease = try state.claim(a, &txn, group, pending, groups.digest("worker"), 100, 1000);
        try state.finish(a, &txn, group, pending.partition, lease, 101, groups.digest("ordinary publication"));
        completed += 1;
    }
    try std.testing.expectEqual(@as(usize, 301), completed);
    try std.testing.expectEqual(@as(u64, 1), (try state.currentHead(a, &txn, group)).pending);
    try std.testing.expect((try propagation.status(a, &txn, owner, event)).?.settled);
    var repair = event;
    repair.source_revision += 1;
    repair.repair = true;
    try propagation.advance(a, &txn, owner, repair, 1);
    try std.testing.expect((try propagation.status(a, &txn, owner, repair)).?.propagated);
    // One repair partition plus the independent future partition; the 300
    // earlier outputs keep their original context epochs and are not replayed.
    try std.testing.expectEqual(@as(u64, 2), (try state.currentHead(a, &txn, group)).pending);
}
