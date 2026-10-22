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

//! Snapshot-bound newest-first prior-artifact scan. The read adapter owns
//! artifact acceptance checks; irrelevant artifacts are never fetched or checked.
const std = @import("std");
const groups = @import("group_sources.zig");
const state = @import("group_source_state.zig");
const prior = @import("group_prior_context.zig");
const A = std.mem.Allocator;

/// The caller supplies an arena for retained artifact JSON and directory data.
pub fn collect(a: A, read: anytype, group: state.Group, current: prior.Position, config: groups.PriorContext) ![]const prior.Item {
    try config.validate();
    const prefix = try state.directoryPrefix(a, group);
    const upper = try state.directoryOrdinalBound(a, group, current.ordinal, true);
    var cursor = try read.openPhysicalCursorAdapter();
    defer cursor.close();
    var row = try cursor.seekAtOrBefore(upper);
    var candidates = prior.Candidates{ .limit = config.max_items };
    var segments: std.AutoHashMapUnmanaged(groups.Digest, void) = .empty;
    var visits: usize = 0;
    var decoded_bytes: usize = 0;
    while (row) |entry| {
        if (!std.mem.startsWith(u8, entry.key, prefix)) break;
        if (visits == 1024) return error.GroupedPriorContextLimitExceeded;
        visits += 1;
        const directory = try state.directoryEntry(a, read, group, entry.key);
        const position = prior.Position{ .ordinal = directory.snapshot.ordinal, .segment = directory.partition.segment };
        if (candidates.canStop(position)) break;
        if (prior.Position.before(position, current) and !std.mem.eql(u8, position.segment, current.segment)) {
            const segment_hash = groups.digest(position.segment);
            if (!segments.contains(segment_hash)) {
                if (segments.count() == config.horizon_segments) break;
                try segments.put(a, segment_hash, {});
            }
            var unit_index = directory.snapshot.units.len;
            while (unit_index > 0) {
                unit_index -= 1;
                const unit = directory.snapshot.units[unit_index];
                const unit_hash = groups.digest(unit);
                // Earlier units cannot improve a full window. Equal ordinals
                // still compare unit identities across directory-cell ties.
                if (candidates.canSkipUnit(position, @intCast(unit_index), unit_hash)) continue;
                const artifact = (try read.getArtifact(unit, config.artifact)) orelse continue;
                decoded_bytes += artifact.len;
                if (decoded_bytes > 4 * 1024 * 1024) return error.GroupedPriorContextLimitExceeded;
                const value = try @import("antfly-json").parseValueLosslessLeaky(a, artifact);
                const collection = prior.artifactCollection(value);
                const items: []const std.json.Value = if (collection == .array) collection.array.items else (&collection)[0..1];
                const fingerprint = groups.digest(artifact);
                var item_index = items.len;
                var taken: usize = 0;
                while (item_index > 0 and taken < config.max_items) : (taken += 1) {
                    item_index -= 1;
                    try candidates.insert(a, .{ .group = group.key, .position = position, .unit = unit_hash, .unit_ordinal = @intCast(unit_index), .ordinal = @intCast(item_index), .fingerprint = fingerprint, .value = items[item_index] });
                }
            }
        }
        row = try cursor.prev();
    }
    return candidates.items.items;
}

const TestRead = struct {
    a: A,
    records: std.StringHashMapUnmanaged([]const u8) = .empty,
    artifacts: std.StringHashMapUnmanaged([]const u8) = .empty,
    artifact_reads: usize = 0,
    const Row = struct { key: []const u8 };
    const Cursor = struct {
        read: *TestRead,
        last: []const u8 = "",
        pub fn close(_: *@This()) void {}
        pub fn seekAtOrBefore(self: *@This(), upper: []const u8) !?Row {
            return self.seek(upper, true);
        }
        fn seek(self: *@This(), upper: []const u8, inclusive: bool) !?Row {
            var chosen: ?Row = null;
            var it = self.read.records.keyIterator();
            while (it.next()) |key| {
                const order = std.mem.order(u8, key.*, upper);
                if (order == .gt or (!inclusive and order == .eq)) continue;
                if (chosen == null or std.mem.order(u8, key.*, chosen.?.key) == .gt) chosen = .{ .key = key.* };
            }
            if (chosen) |row| self.last = row.key;
            return chosen;
        }
        pub fn prev(self: *@This()) !?Row {
            return self.seek(self.last, false);
        }
    };
    pub fn openPhysicalCursorAdapter(self: *TestRead) !Cursor {
        return .{ .read = self };
    }
    pub fn get(self: *TestRead, key: []const u8) ![]const u8 {
        return self.records.get(key) orelse error.NotFound;
    }
    pub fn put(self: *TestRead, key: []const u8, value: []const u8) !void {
        try self.records.put(self.a, try self.a.dupe(u8, key), try self.a.dupe(u8, value));
    }
    pub fn getArtifact(self: *TestRead, unit: []const u8, _: []const u8) !?[]const u8 {
        self.artifact_reads += 1;
        // An irrelevant output could be oversized, corrupt, or awaiting
        // acceptance. All must be skipped before consulting the adapter.
        if (std.mem.startsWith(u8, unit, "poison")) return error.ArtifactPublicationPending;
        return self.artifacts.get(unit);
    }
    fn directory(self: *TestRead, group: state.Group, ordinal: i64, cell: i64, units: []const []const u8) !void {
        const partition = @import("group_source_index.zig").Partition{ .group = group.key, .segment = "previous", .cell = cell };
        try state.writeRecord(self.a, self, try state.directoryKey(self.a, group, partition, ordinal), state.DirectoryEntry{
            .partition = partition,
            .snapshot = .{ .revision = 1, .input_digest = groups.digest("input"), .units = units, .sequence = 1, .ordinal = ordinal },
        });
    }
};

test "prior scan stops artifact reads once newest items fill the window" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var read = TestRead{ .a = a };
    const group = state.Group{ .definition = .{ .table_incarnation = 5, .configuration = groups.digest("turns") }, .key = "thread" };
    var units: [128][]const u8 = @splat("poison-large-old-output");
    units[127] = "newest";
    try read.directory(group, 128, 1, &units);
    try read.directory(group, 0, 0, &.{"poison-older-cell"});
    try read.artifacts.put(a, "newest", "{\"items\":[\"older\",\"latest\"]}");
    const result = try collect(a, &read, group, .{ .ordinal = 256, .segment = "current" }, .{ .artifact = "claims", .render = "{{this}}", .max_items = 1, .horizon_segments = 2 });
    try std.testing.expectEqual(@as(usize, 1), read.artifact_reads);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("latest", result[0].value.string);
}

test "prior scan keeps unit identity ties across directory cells" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var read = TestRead{ .a = a };
    const group = state.Group{ .definition = .{ .table_incarnation = 5, .configuration = groups.digest("turns") }, .key = "thread" };
    const partition = @import("group_source_index.zig").Partition{ .group = group.key, .segment = "previous", .cell = 0 };
    var second = partition;
    second.cell = 1;
    const zero_key = try state.directoryKey(a, group, partition, 0);
    const one_key = try state.directoryKey(a, group, second, 0);
    const first_cell: i64 = if (std.mem.order(u8, zero_key, one_key) == .gt) 0 else 1;
    const left = groups.digest("left");
    const right = groups.digest("right");
    const newest = if (std.mem.order(u8, &left, &right) == .gt) "left" else "right";
    const oldest = if (std.mem.eql(u8, newest, "left")) "right" else "left";
    try read.directory(group, 0, first_cell, &.{oldest});
    try read.directory(group, 0, 1 - first_cell, &.{newest});
    try read.artifacts.put(a, oldest, "\"oldest\"");
    try read.artifacts.put(a, newest, "\"newest\"");
    const result = try collect(a, &read, group, .{ .ordinal = 1, .segment = "current" }, .{ .artifact = "claims", .render = "{{this}}", .max_items = 1 });
    try std.testing.expectEqual(@as(usize, 2), read.artifact_reads);
    try std.testing.expectEqual(@as(usize, 1), result.len);
    try std.testing.expectEqualStrings("newest", result[0].value.string);
}

test "prior scan continues past missing and empty outputs and bounds needed bytes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var read = TestRead{ .a = a };
    const group = state.Group{ .definition = .{ .table_incarnation = 5, .configuration = groups.digest("turns") }, .key = "thread" };
    try read.directory(group, 0, 0, &.{ "available", "empty", "missing" });
    try read.artifacts.put(a, "empty", "[]");
    try read.artifacts.put(a, "available", "\"available\"");
    const config = groups.PriorContext{ .artifact = "claims", .render = "{{this}}", .max_items = 1 };
    const result = try collect(a, &read, group, .{ .ordinal = 1, .segment = "current" }, config);
    try std.testing.expectEqual(@as(usize, 3), read.artifact_reads);
    try std.testing.expectEqualStrings("available", result[0].value.string);
    try read.artifacts.put(a, "available", try a.alloc(u8, 4 * 1024 * 1024 + 1));
    try std.testing.expectError(error.GroupedPriorContextLimitExceeded, collect(a, &read, group, .{ .ordinal = 1, .segment = "current" }, config));
}

test "prior scan preserves exact generated numeric literals through rendering" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var read = TestRead{ .a = a };
    const group = state.Group{ .definition = .{ .table_incarnation = 5, .configuration = groups.digest("turns") }, .key = "thread" };
    try read.directory(group, 0, 0, &.{"numbers"});
    const raw = "{\"id\":9007199254740993.0,\"tiny\":1e-400}";
    try read.artifacts.put(a, "numbers", raw);
    const config = groups.PriorContext{ .artifact = "claims", .render = "{{this}}", .max_items = 1 };
    const current = prior.Position{ .ordinal = 1, .segment = "current" };
    const candidates = try collect(a, &read, group, current, config);
    const Render = struct {
        fn render(_: *anyopaque, alloc: A, _: []const u8, value: std.json.Value) ![]const u8 {
            return std.json.Stringify.valueAlloc(alloc, value, .{});
        }
    };
    var ptr: u8 = 0;
    const selected = try prior.select(a, config, group.key, current, candidates, .{ .ptr = &ptr, .render = Render.render });
    try std.testing.expectEqualStrings(raw, selected.text);
}
