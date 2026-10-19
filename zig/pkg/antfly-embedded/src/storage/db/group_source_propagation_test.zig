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
    // Completion retirement is bounded independently of the propagation
    // pages and never waits for the independent future partition.
    try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expect(!(try propagation.status(a, &txn, owner, event)).?.settled);
    for (0..3) |_| try propagation.advance(a, &txn, owner, event, 1);
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

test "sibling context policies retain independent epochs across restart and unit publication" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const root = try group.anchor(a);
    var units: [3][]const u8 = undefined;
    var partitions: [3]index.Partition = undefined;
    for (&partitions, &units, 0..) |*partition, *unit, i| {
        const segment = try std.fmt.allocPrint(a, "segment-{d}", .{i});
        partition.* = .{ .group = group.key, .segment = segment, .cell = 0 };
        unit.* = try std.fmt.allocPrint(a, "{s}/u/{x}", .{ try identity.partition(a, root, segment, 0), &groups.digest(segment) });
        const snapshot = state.Snapshot{ .revision = 1, .input_digest = groups.digest(unit.*), .units = try a.dupe([]const u8, &.{unit.*}), .sequence = 10 + i, .ordinal = @intCast(i) };
        try state.writeRecord(a, &txn, try state.snapshotKey(a, group, partition.*), snapshot);
        try state.writeRecord(a, &txn, try state.directoryKey(a, group, partition.*, snapshot.ordinal), state.DirectoryEntry{ .partition = partition.*, .snapshot = snapshot });
    }
    const event = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "source", .source_revision = 7, .since_sequence = 11, .through_sequence = 11, .ordinal = 0 };
    // A prior correction already established a short-policy guard here.
    _ = try state.invalidateContextHorizons(a, &txn, group, partitions[2], 2, &.{1});
    const first_pending = (try state.pendingFor(a, &txn, group, partitions[2])).?;
    const first_lease = try state.claim(a, &txn, group, first_pending, groups.digest("worker"), 1, 1000);
    var first_snapshot = (try state.readSnapshot(a, &txn, group, partitions[2])).?;
    first_snapshot.revision = first_pending.revision;
    try state.stageSnapshot(a, &txn, group, partitions[2], first_lease, 2, first_snapshot);
    try state.finish(a, &txn, group, partitions[2], first_lease, 3, groups.digest("first publication"));
    const short_far_before = try txn.get(try identity.unitHorizonEpochKey(a, units[2], 1));
    try propagation.advanceHorizons(a, &txn, owner, event, &.{ 1, 2 });
    var original = (try state.readSnapshot(a, &txn, group, partitions[0])).?;
    original.revision = (try state.pendingFor(a, &txn, group, partitions[0])).?.revision;
    try state.writeRecord(a, &txn, try state.snapshotKey(a, group, partitions[0]), original);
    try propagation.advanceHorizons(a, &txn, owner, event, &.{ 1, 2 });
    const short_near = try identity.unitHorizonEpochKey(a, units[1], 1);
    const long_near = try identity.unitHorizonEpochKey(a, units[1], 2);
    const short_far = try identity.unitHorizonEpochKey(a, units[2], 1);
    const long_far = try identity.unitHorizonEpochKey(a, units[2], 2);
    // Enrolling later work must not wake its ordinary producers ahead of
    // the predecessor's replacement snapshot on the replay stream.
    try std.testing.expectError(error.NotFound, txn.get(short_near));
    try std.testing.expectError(error.NotFound, txn.get(long_near));
    try std.testing.expectError(error.NotFound, txn.get(long_far));
    try std.testing.expectEqualStrings(short_far_before, try txn.get(short_far));
    // A fresh invocation uses only durable propagation state.
    try propagation.advanceHorizons(a, &txn, owner, event, &.{ 1, 2 });
    try std.testing.expectError(error.NotFound, txn.get(long_far));
    while (try state.nextPending(a, &txn, group)) |pending| {
        const lease = try state.claim(a, &txn, group, pending, groups.digest("worker"), 100, 1000);
        var snapshot = (try state.readSnapshot(a, &txn, group, pending.partition)).?;
        snapshot.revision = lease.revision;
        try state.stageSnapshot(a, &txn, group, pending.partition, lease, 101, snapshot);
        try state.finish(a, &txn, group, pending.partition, lease, 102, groups.digest("published"));
    }
    // Publication must copy each policy's own revision, never the maximum
    // sibling epoch. Transfer validation accepts the extended private keys.
    try std.testing.expectEqualStrings(short_far_before, try txn.get(short_far));
    try std.testing.expectEqualStrings(try txn.get(short_near), try txn.get(long_near));
    _ = try txn.get(long_far);
    try @import("group_source_transfer.zig").validate(a, long_far, try txn.get(long_far));
    const partition_epoch_key = try state.contextEpochKey(a, group, partitions[2]);
    try @import("group_source_transfer.zig").reconstruct(a, &txn, definition.table_incarnation, partition_epoch_key, try txn.get(partition_epoch_key));
    try state.clearUnitContext(a, &txn, group, partitions[2], units[2]);
    try std.testing.expectError(error.NotFound, txn.get(short_far));
    try std.testing.expectError(error.NotFound, txn.get(long_far));
}

test "group visibility drives causal snapshots and excludes later unrelated work" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const partition = index.Partition{ .group = group.key, .segment = "original", .cell = 0 };
    const event = index.Invalidation{ .definition = definition, .partition = partition, .document = "source", .source_revision = 7, .since_sequence = 11, .through_sequence = 11, .ordinal = 0 };
    try propagation.advance(a, &txn, owner, event, 0);
    const pending = (try state.nextPending(a, &txn, group)).?;
    const root = try group.anchor(a);
    const unit = try std.fmt.allocPrint(a, "{s}/u/{x}", .{ try identity.partition(a, root, partition.segment, 0), &groups.digest("unit") });
    const lease = try state.claim(a, &txn, group, pending, groups.digest("worker"), 100, 1000);
    try state.stageSnapshot(a, &txn, group, partition, lease, 101, .{ .revision = lease.revision, .input_digest = groups.digest("snapshot"), .units = &.{unit}, .sequence = 20, .ordinal = 0 });
    const visibility = @import("group_source_visibility.zig");
    try std.testing.expectEqual(@as(u64, 20), try visibility.producerSequence(a, &txn, owner, event));
    var future = event;
    future.document = "independent-future";
    future.partition = .{ .group = group.key, .segment = "future", .cell = 1 };
    future.ordinal = 128;
    future.source_revision += 1;
    future.since_sequence = 100;
    future.through_sequence = 100;
    _ = try state.invalidate(a, &txn, owner, future);
    try state.finish(a, &txn, group, partition, lease, 102, groups.digest("published"));
    // Even in the source-acknowledgement gap, a later pending cause cannot
    // extend the completed source request's enrichment target.
    try std.testing.expectEqual(@as(u64, 0), try visibility.producerSequence(a, &txn, owner, event));
    try std.testing.expect((try state.nextPending(a, &txn, group)) != null);
}

const CorrectionFixture = struct {
    fn snapshot(a: A, group: state.Group, partition: index.Partition, ordinal: i64, revision: u64) !state.Snapshot {
        const root = try group.anchor(a);
        const unit = try std.fmt.allocPrint(a, "{s}/u/{x}", .{ try identity.partition(a, root, partition.segment, partition.cell), &groups.digest(partition.segment) });
        return .{ .revision = revision, .input_digest = groups.digest(unit), .units = try a.dupe([]const u8, &.{unit}), .sequence = revision, .ordinal = ordinal };
    }
    fn seed(a: A, txn: *Txn, group: state.Group, partitions: []const index.Partition, ordinals: []const i64) !void {
        try state.writeRecord(a, txn, try state.metadataKey(a, group, 1, ""), state.Head{ .revision = 1 });
        for (partitions, ordinals) |partition, ordinal| {
            const value = try snapshot(a, group, partition, ordinal, 1);
            try state.writeRecord(a, txn, try state.snapshotKey(a, group, partition), value);
            try state.writeRecord(a, txn, try state.directoryKey(a, group, partition, ordinal), state.DirectoryEntry{ .partition = partition, .snapshot = value });
        }
    }
    fn publish(a: A, txn: *Txn, group: state.Group, partition: index.Partition, ordinal: i64) !state.Lease {
        const pending = (try state.pendingFor(a, txn, group, partition)).?;
        const lease = try state.claim(a, txn, group, pending, groups.digest("worker"), 100, 1000);
        try state.stageSnapshot(a, txn, group, partition, lease, 101, try snapshot(a, group, partition, ordinal, lease.revision));
        return lease;
    }
};

test "correction tracks old and new neighborhoods for moves in either direction within one cell" {
    for ([_]bool{ false, true }) |backwards| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var txn = Txn{ .a = a };
        const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
        const group = state.Group{ .definition = definition, .key = "thread" };
        const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
        const partitions = [_]index.Partition{
            .{ .group = group.key, .segment = "a", .cell = 0 },
            .{ .group = group.key, .segment = "b", .cell = 0 },
            .{ .group = group.key, .segment = "c", .cell = 0 },
        };
        const before: i64 = if (backwards) 100 else 20;
        const after: i64 = if (backwards) 20 else 100;
        try CorrectionFixture.seed(a, &txn, group, &partitions, &.{ before, 50, 110 });
        const event = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "source", .source_revision = 7, .since_sequence = 11, .through_sequence = 11, .ordinal = before };
        try propagation.advance(a, &txn, owner, event, 1);
        // No old snapshot may choose the post-correction neighborhood.
        try std.testing.expect(!(try propagation.currentProgress(a, &txn, owner, event)).?.anchors_bound);
        _ = try CorrectionFixture.publish(a, &txn, group, partitions[0], after);
        try propagation.advance(a, &txn, owner, event, 1);
        // An owner can run between scans. Always enroll the earlier
        // successor first, including when the corrected source moved back.
        try std.testing.expect((try state.pendingFor(a, &txn, group, partitions[1])) != null);
        try std.testing.expect((try state.pendingFor(a, &txn, group, partitions[2])) == null);
        // Restart between the two scans: all state comes from durable metadata.
        try propagation.advance(a, &txn, owner, event, 1);
        try std.testing.expect((try state.pendingFor(a, &txn, group, partitions[1])) != null);
        try std.testing.expect((try state.pendingFor(a, &txn, group, partitions[2])) != null);
        try std.testing.expect((try propagation.status(a, &txn, owner, event)).?.propagated);
    }
}

test "correction completion excludes a distant source cause enrolled during propagation" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const partitions = [_]index.Partition{
        .{ .group = group.key, .segment = "a", .cell = 0 },
        .{ .group = group.key, .segment = "b", .cell = 1 },
    };
    try CorrectionFixture.seed(a, &txn, group, &partitions, &.{ 0, 128 });
    const event = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "source", .source_revision = 7, .since_sequence = 11, .through_sequence = 11, .ordinal = 0 };
    try propagation.advance(a, &txn, owner, event, 1);
    const source_lease = try CorrectionFixture.publish(a, &txn, group, partitions[0], 0);
    try state.finish(a, &txn, group, partitions[0], source_lease, 102, groups.digest("source accepted"));
    var future = event;
    future.partition = .{ .group = group.key, .segment = "future", .cell = 100 };
    future.document = "future";
    future.source_revision += 1;
    future.ordinal = 12800;
    future.since_sequence = 100;
    future.through_sequence = 100;
    _ = try state.invalidate(a, &txn, owner, future);
    try propagation.advance(a, &txn, owner, event, 1);
    const pending = (try state.pendingFor(a, &txn, group, partitions[1])).?;
    const lease = try state.claim(a, &txn, group, pending, groups.digest("worker"), 200, 1000);
    try state.finish(a, &txn, group, partitions[1], lease, 201, groups.digest("neighbor accepted"));
    try std.testing.expect((try propagation.status(a, &txn, owner, event)).?.retire_ready);
    try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expect((try propagation.status(a, &txn, owner, event)).?.settled);
    try std.testing.expect((try state.pendingFor(a, &txn, group, future.partition)) != null);
    try std.testing.expectEqual(@as(u64, 0), try @import("group_source_visibility.zig").producerSequence(a, &txn, owner, event));
}

test "correction deduplicates overlapping horizon policies without losing new short-policy neighbors" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const partitions = [_]index.Partition{
        .{ .group = group.key, .segment = "a", .cell = 0 },
        .{ .group = group.key, .segment = "b", .cell = 0 },
        .{ .group = group.key, .segment = "c", .cell = 0 },
        .{ .group = group.key, .segment = "d", .cell = 0 },
    };
    try CorrectionFixture.seed(a, &txn, group, &partitions, &.{ 20, 50, 110, 120 });
    const event = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "source", .source_revision = 7, .ordinal = 20 };
    try propagation.advanceHorizons(a, &txn, owner, event, &.{ 1, 2 });
    _ = try CorrectionFixture.publish(a, &txn, group, partitions[0], 100);
    try propagation.advanceHorizons(a, &txn, owner, event, &.{ 1, 2 });
    const epoch_key = try state.contextEpochKey(a, group, partitions[2]);
    const Epoch = struct {
        revision: u64,
        horizons: []const struct { horizon: u16, revision: u64 },
    };
    const before_raw = try txn.get(epoch_key);
    const before_epoch = try std.json.parseFromSliceLeaky(Epoch, a, before_raw[4 .. before_raw.len - 32], .{});
    try std.testing.expectEqual(@as(usize, 1), before_epoch.horizons.len);
    try std.testing.expectEqual(@as(u16, 2), before_epoch.horizons[0].horizon);
    const long_before = before_epoch.horizons[0].revision;
    try propagation.advanceHorizons(a, &txn, owner, event, &.{ 1, 2 });
    const after_raw = try txn.get(epoch_key);
    const after_epoch = try std.json.parseFromSliceLeaky(Epoch, a, after_raw[4 .. after_raw.len - 32], .{});
    try std.testing.expectEqual(@as(usize, 2), after_epoch.horizons.len);
    try std.testing.expectEqual(long_before, after_epoch.horizons[0].revision);
    try std.testing.expectEqual(@as(u16, 1), after_epoch.horizons[1].horizon);
    const head = (try state.currentHead(a, &txn, group)).revision;
    try propagation.advanceHorizons(a, &txn, owner, event, &.{ 1, 2 });
    try std.testing.expectEqual(head, (try state.currentHead(a, &txn, group)).revision);
    // Typed transfer carries per-cause metadata with its group anchor.
    const entries = try @import("group_source_causes.zig").page(a, &txn, group, try propagation.causeId(a, owner, event));
    try std.testing.expectEqual(@as(usize, 4), entries.len);
    for (entries) |entry| try @import("group_source_transfer.zig").validate(a, entry.key, try txn.get(entry.key));
}

test "correction coalescing retains older owned dependencies while excluding foreign pending work" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const partitions = [_]index.Partition{
        .{ .group = group.key, .segment = "a", .cell = 0 },
        .{ .group = group.key, .segment = "b", .cell = 1 },
    };
    try CorrectionFixture.seed(a, &txn, group, &partitions, &.{ 0, 128 });
    const event = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "source", .source_revision = 7, .ordinal = 0 };
    try propagation.advance(a, &txn, owner, event, 1);
    var lease = try CorrectionFixture.publish(a, &txn, group, partitions[0], 0);
    try state.finish(a, &txn, group, partitions[0], lease, 102, groups.digest("first source accepted"));
    try propagation.advance(a, &txn, owner, event, 1);
    // A newer delivery of the same source outbox can supersede its receipt,
    // but cannot erase the correction's still-uncompleted neighbor obligation.
    var newer = event;
    newer.source_revision += 1;
    newer.repair = true;
    try propagation.advance(a, &txn, owner, newer, 1);
    lease = try CorrectionFixture.publish(a, &txn, group, partitions[0], 0);
    try state.finish(a, &txn, group, partitions[0], lease, 102, groups.digest("new source accepted"));
    try propagation.advance(a, &txn, owner, newer, 1);
    try std.testing.expect(!(try propagation.status(a, &txn, owner, newer)).?.settled);
    var future = event;
    future.partition = .{ .group = group.key, .segment = "foreign", .cell = 100 };
    future.document = "foreign";
    future.ordinal = 12800;
    _ = try state.invalidate(a, &txn, owner, future);
    lease = try CorrectionFixture.publish(a, &txn, group, partitions[1], 128);
    try std.testing.expect((try @import("group_source_visibility.zig").producerSequence(a, &txn, owner, newer)) > 0);
    try state.finish(a, &txn, group, partitions[1], lease, 102, groups.digest("old neighbor accepted"));
    try propagation.advance(a, &txn, owner, newer, 1);
    try std.testing.expect((try propagation.status(a, &txn, owner, newer)).?.settled);
    try std.testing.expect((try state.pendingFor(a, &txn, group, future.partition)) != null);
}

test "coalesced move uses the published position rather than the oldest outbox ordinal" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const partitions = [_]index.Partition{
        .{ .group = group.key, .segment = "a", .cell = 0 },
        .{ .group = group.key, .segment = "b", .cell = 0 },
        .{ .group = group.key, .segment = "c", .cell = 0 },
        .{ .group = group.key, .segment = "d", .cell = 0 },
    };
    try CorrectionFixture.seed(a, &txn, group, &partitions, &.{ 40, 2, 41, 81 });
    // A previous move 1 -> 40 was accepted before outbox acknowledgement.
    // The next 40 -> 80 move retains ordinal=1 in the coalesced source outbox.
    const event = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "row", .source_revision = 8, .ordinal = 1 };
    try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expectEqual(@as(?i64, 40), (try propagation.currentProgress(a, &txn, owner, event)).?.ordinal);
    _ = try CorrectionFixture.publish(a, &txn, group, partitions[0], 80);
    for (0..4) |_| try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expect((try propagation.status(a, &txn, owner, event)).?.propagated);
    try std.testing.expect(try state.pendingFor(a, &txn, group, partitions[2]) != null);
    try std.testing.expect(try state.pendingFor(a, &txn, group, partitions[3]) != null);
    try std.testing.expect(try state.pendingFor(a, &txn, group, partitions[1]) == null);
}

test "superseding moves drain persisted neighborhoods before enrolling the newest correction" {
    for ([_]bool{ false, true }) |between_scans| {
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const a = arena.allocator();
        var txn = Txn{ .a = a };
        const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
        const group = state.Group{ .definition = definition, .key = "thread" };
        const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
        const partitions = [_]index.Partition{
            .{ .group = group.key, .segment = "a", .cell = 0 },
            .{ .group = group.key, .segment = "b", .cell = 0 },
            .{ .group = group.key, .segment = "c", .cell = 0 },
            .{ .group = group.key, .segment = "d", .cell = 0 },
        };
        try CorrectionFixture.seed(a, &txn, group, &partitions, &.{ 1, 2, 41, 81 });
        const first = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "row", .source_revision = 7, .ordinal = 1 };
        try propagation.advance(a, &txn, owner, first, 1);
        const first_lease = try CorrectionFixture.publish(a, &txn, group, partitions[0], 40);
        try state.finish(a, &txn, group, partitions[0], first_lease, 102, groups.digest("first accepted"));
        if (between_scans) try propagation.advance(a, &txn, owner, first, 1);
        var latest = first;
        latest.source_revision = 8;
        // Simulate another append superseding it again during the drain.
        while (!(try propagation.status(a, &txn, owner, first)).?.propagated) {
            latest.source_revision += 1;
            try propagation.advance(a, &txn, owner, latest, 1);
            try std.testing.expect(try propagation.currentProgress(a, &txn, owner, latest) == null);
            try std.testing.expect(try state.pendingFor(a, &txn, group, partitions[0]) == null);
        }
        // The original two neighborhoods survived without a growing anchor list.
        for (partitions[1..3]) |partition| {
            try std.testing.expect(try state.pendingFor(a, &txn, group, partition) != null);
            const lease = try CorrectionFixture.publish(a, &txn, group, partition, if (std.mem.eql(u8, partition.segment, "b")) 2 else 41);
            try state.finish(a, &txn, group, partition, lease, 102, groups.digest("neighbor accepted"));
        }
        try propagation.advance(a, &txn, owner, latest, 1);
        try std.testing.expectEqual(@as(?i64, 40), (try propagation.currentProgress(a, &txn, owner, latest)).?.ordinal);
        const latest_lease = try CorrectionFixture.publish(a, &txn, group, partitions[0], 80);
        try state.finish(a, &txn, group, partitions[0], latest_lease, 102, groups.digest("latest accepted"));
        for (0..4) |_| try propagation.advance(a, &txn, owner, latest, 1);
        try std.testing.expect((try propagation.status(a, &txn, owner, latest)).?.propagated);
        try std.testing.expect(try state.pendingFor(a, &txn, group, partitions[2]) != null);
        try std.testing.expect(try state.pendingFor(a, &txn, group, partitions[3]) != null);
    }
}

test "unmaterialized corrections coalesce without waiting for an obsolete source delivery" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 5, .configuration = groups.digest("turns") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 8 };
    const partitions = [_]index.Partition{
        .{ .group = group.key, .segment = "a", .cell = 0 },
        .{ .group = group.key, .segment = "b", .cell = 0 },
        .{ .group = group.key, .segment = "c", .cell = 0 },
    };
    try CorrectionFixture.seed(a, &txn, group, &partitions, &.{ 40, 2, 41 });
    var event = index.Invalidation{ .definition = definition, .partition = partitions[0], .document = "row", .source_revision = 7, .ordinal = 40 };
    try propagation.advance(a, &txn, owner, event, 1);
    const obsolete = (try state.pendingFor(a, &txn, group, partitions[0])).?.revision;
    event.source_revision += 1;
    event.ordinal = 1;
    // The newer, earlier delivery must enroll immediately; waiting for the
    // old snapshot here could fence its own fan-in on an undelivered event.
    try propagation.advance(a, &txn, owner, event, 1);
    const progress = (try propagation.currentProgress(a, &txn, owner, event)).?;
    try std.testing.expect(progress.enrollment > obsolete);
    try std.testing.expectEqual(@as(?i64, 40), progress.ordinal);
    _ = try CorrectionFixture.publish(a, &txn, group, partitions[0], 1);
    for (0..4) |_| try propagation.advance(a, &txn, owner, event, 1);
    try std.testing.expect((try propagation.status(a, &txn, owner, event)).?.propagated);
    for (partitions[1..]) |partition| try std.testing.expect(try state.pendingFor(a, &txn, group, partition) != null);
}
