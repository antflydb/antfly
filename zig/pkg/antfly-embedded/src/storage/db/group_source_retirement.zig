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

//! Bounded catalog retirement. The current immutable plan is rechecked by the
//! primary writer. A catalog/range change restarts the durable sweep, so a
//! concurrent re-add cannot be deleted by a stale cleanup command. Appends do
//! not trigger scans; only catalog activation or ownership changes do.
const std = @import("std");
const identity = @import("group_source_identity.zig");
const transfer = @import("group_source_transfer.zig");
const reads = @import("group_source_reads.zig");
const A = std.mem.Allocator;
pub const progress_key = "\x00\x00__group_source_v1:retirement";
pub const Progress = struct { generation: u64, incarnation: u64 = 0, cursor: []const u8 = "", done: bool = false };
pub const Result = struct { changed_artifacts: []const []const u8, deleted_sources: []const []const u8 };
pub fn load(a: A, txn: anytype, epoch: u64) !Progress {
    const raw = txn.get(progress_key) catch |err| if (err == error.NotFound) return .{ .generation = epoch } else return err;
    const stored = try std.json.parseFromSliceLeaky(Progress, a, raw, .{});
    return if (stored.generation == epoch) stored else .{ .generation = epoch, .incarnation = stored.incarnation };
}
fn active(plan: anytype, configuration: [32]u8) bool {
    for (plan.entries) |entry| if (std.mem.eql(u8, &entry.configuration, &configuration)) return true;
    return false;
}
pub fn step(comptime Keys: type, a: A, txn: anytype, plan: anytype, table: u128, epoch: u64, expected: []const u8, bounds: reads.Bounds) !Result {
    const progress = try load(a, txn, epoch);
    if (progress.done) return .{ .changed_artifacts = &.{}, .deleted_sources = &.{} };
    if (!std.mem.eql(u8, expected, progress.cursor)) return error.EnrichmentSourceChanged;
    const page = try preparePage(a, txn, epoch);
    return applyPage(Keys, a, txn, plan, table, epoch, page, bounds);
}
pub fn preparePage(a: A, txn: anytype, epoch: u64) !@import("group_source_page.zig").Page {
    const progress = try load(a, txn, epoch);
    return @import("group_source_page.zig").prepare(a, txn, epoch, progress.incarnation, .retire, progress.cursor, "");
}
pub fn applyPage(comptime Keys: type, a: A, txn: anytype, plan: anytype, table: u128, epoch: u64, page: @import("group_source_page.zig").Page, bounds: reads.Bounds) !Result {
    try page.validate();
    var progress = try load(a, txn, epoch);
    if (progress.done or page.incarnation != progress.incarnation or page.generation != epoch or page.phase != .retire or !std.mem.eql(u8, page.cursor, progress.cursor)) return .{ .changed_artifacts = &.{}, .deleted_sources = &.{} };
    var artifacts: std.ArrayList([]const u8) = .empty;
    var deleted: std.ArrayList([]const u8) = .empty;
    for (page.candidates) |candidate| {
        const selected = try reads.decode(a, candidate.key);
        if (selected.len >= 52 and std.mem.startsWith(u8, selected, "\x00\x00G") and (selected[3] == 1 or selected[3] == 6 or selected[3] == 7 or selected[3] == 8 or selected[3] == 9 or selected[3] == 10) and std.mem.readInt(u128, selected[4..20], .big) == table and !active(plan, selected[20..52].*)) {
            try txn.delete(selected);
        }
        const owner = (try Keys.decodeDocumentComponentAlloc(a, selected)) orelse {
            continue;
        };
        if (!bounds.contains(owner)) {
            continue;
        }
        const config: ?[32]u8 = if (identity.isAnchor(owner)) identity.configurationOf(owner) else if (transfer.isKey(selected)) blk: {
            const info = try transfer.info(a, selected);
            break :blk if (info.kind == identity.reverse_kind or info.kind == identity.outbox_kind) info.suffix[0..32].* else null;
        } else null;
        const root = identity.root(owner);
        const orphan = if (root != null and root.?.len < owner.len) !try @import("group_source_state.zig").unitIsCurrent(a, txn, owner) else false;
        if (config) |definition| if (!active(plan, definition) or orphan) {
            if (!active(plan, definition)) if (root) |group_root| try @import("group_source_queue.zig").removeGroup(a, txn, group_root);
            if (transfer.isKey(selected)) {
                const info = try transfer.info(a, selected);
                if (info.kind == identity.primary_kind) try deleted.append(a, owner);
                try transfer.stage(a, txn, table, selected, null);
            } else {
                try txn.delete(selected);
                try artifacts.append(a, selected);
            }
        };
    }
    progress.cursor = page.next;
    progress.done = page.done;
    try txn.put(progress_key, try std.json.Stringify.valueAlloc(a, progress, .{}));
    return .{ .changed_artifacts = try artifacts.toOwnedSlice(a), .deleted_sources = try deleted.toOwnedSlice(a) };
}

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
test "retirement resumes bounded catalog sweeps and preserves re-added live definitions" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var txn = Txn{ .a = a };
    const groups = @import("group_sources.zig");
    const old_config = groups.digest("old");
    const live_config = groups.digest("live");
    const old_root = try identity.anchor(a, 7, old_config, groups.digest("thread"));
    const live_root = try identity.anchor(a, 7, live_config, groups.digest("thread"));
    const old_key = try identity.unitContextEpochKey(a, old_root);
    const live_key = try identity.unitContextEpochKey(a, live_root);
    try txn.put(old_key, "1");
    try txn.put(live_key, "1");
    const state = @import("group_source_state.zig");
    const reads_mod = @import("group_source_reads.zig");
    const group = state.Group{ .definition = .{ .table_incarnation = 7, .configuration = live_config }, .key = "thread" };
    const partition = @import("group_source_index.zig").Partition{ .group = "thread", .segment = "turn", .cell = 0 };
    const unit_prefix = try identity.partition(a, live_root, "turn", 0);
    const current_unit = try std.fmt.allocPrint(a, "{s}/u/{x}", .{ unit_prefix, &groups.digest("current") });
    const orphan_unit = try std.fmt.allocPrint(a, "{s}/u/{x}", .{ unit_prefix, &groups.digest("orphan") });
    const current_key = try identity.unitContextEpochKey(a, current_unit);
    const orphan_key = try identity.unitContextEpochKey(a, orphan_unit);
    try txn.put(current_key, "1");
    try txn.put(orphan_key, "1");
    const snapshot_key = try state.snapshotKey(a, group, partition);
    try state.writeRecord(a, &txn, snapshot_key, state.Snapshot{ .revision = 1, .input_digest = groups.digest("snapshot"), .units = &.{current_unit}, .sequence = 1 });
    try state.writeRecord(a, &txn, try identity.unitMembershipKey(a, current_unit), state.UnitMembership{ .snapshot_key_base64 = try reads_mod.encode(a, snapshot_key) });
    const old_definition = @import("group_source_index.zig").Definition{ .table_incarnation = 7, .configuration = old_config };
    try std.testing.expectEqual(@as(u64, 1), try @import("group_source_index.zig").nextRevision(a, &txn, old_definition));

    for (0..300) |i| try txn.put(try std.fmt.allocPrint(a, "z{d:0>4}", .{i}), "untouched");
    const Keys = struct {
        pub fn decodeDocumentComponentAlloc(alloc: A, selected: []const u8) !?[]u8 {
            if (!transfer.isKey(selected)) return null;
            return @constCast((try transfer.info(alloc, selected)).document);
        }
    };
    const Entry = struct { configuration: [32]u8 };
    const Plan = struct { entries: []const Entry };
    const plan = Plan{ .entries = &.{.{ .configuration = live_config }} };
    var progress = try load(a, &txn, 1);
    _ = try step(Keys, a, &txn, plan, 7, 1, progress.cursor, .{});
    progress = try load(a, &txn, 1);
    try std.testing.expect(!progress.done);
    try std.testing.expectError(error.NotFound, txn.get(old_key));
    try std.testing.expectEqualStrings("1", try txn.get(live_key));
    try std.testing.expectEqualStrings("1", try txn.get(current_key));
    try std.testing.expectError(error.NotFound, txn.get(orphan_key));
    try std.testing.expectEqual(@as(u64, 2), try @import("group_source_index.zig").nextRevision(a, &txn, old_definition));

    // A restarted owner continues from the durable cursor, not a memory cache.
    while (!progress.done) {
        _ = try step(Keys, a, &txn, plan, 7, 1, progress.cursor, .{});
        progress = try load(a, &txn, 1);
    }
    try txn.put(old_key, "2");
    const readded = Plan{ .entries = &.{ .{ .configuration = live_config }, .{ .configuration = old_config } } };
    // A new catalog fences the stale expected cursor before touching any row.
    try std.testing.expectError(error.EnrichmentSourceChanged, step(Keys, a, &txn, readded, 7, 2, progress.cursor, .{}));
    _ = try step(Keys, a, &txn, readded, 7, 2, "", .{});
    try std.testing.expectEqualStrings("2", try txn.get(old_key));
}

pub fn invalidate(a: A, txn: anytype) !void {
    var progress = try load(a, txn, 0);
    progress.incarnation = try std.math.add(u64, progress.incarnation, 1);
    progress.cursor = "";
    progress.done = false;
    try txn.put(progress_key, try std.json.Stringify.valueAlloc(a, progress, .{}));
}
