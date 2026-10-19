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

//! Sequence-ordered source visibility debt. A barrier probes the earliest
//! outstanding source obligation instead of scanning every pending group.
const std = @import("std");
const index = @import("group_source_index.zig");
const groups = @import("group_sources.zig");
const queue = @import("group_source_queue.zig");
const A = std.mem.Allocator;
pub const namespace = "\x00\x00G\x09";
pub fn prefix(a: A, definition: index.Definition) ![]u8 {
    try definition.validate();
    var table: [16]u8 = undefined;
    std.mem.writeInt(u128, &table, definition.table_incarnation, .big);
    return std.mem.concat(a, u8, &.{ namespace, &table, &definition.configuration });
}
pub fn key(a: A, definition: index.Definition, sequence: u64, outbox: []const u8) ![]u8 {
    var order: [8]u8 = undefined;
    std.mem.writeInt(u64, &order, sequence, .big);
    return std.mem.concat(a, u8, &.{ try prefix(a, definition), &order, &groups.digest(outbox) });
}
pub fn stage(a: A, txn: anytype, outbox: []const u8, previous: ?index.Invalidation, event: index.Invalidation) !void {
    if (previous) |old| if (old.since_sequence != event.since_sequence) try remove(a, txn, outbox, old);
    const selected = try key(a, event.definition, event.since_sequence, outbox);
    try txn.put(selected, try queue.encode(a, selected, outbox));
}
pub fn remove(a: A, txn: anytype, outbox: []const u8, event: index.Invalidation) !void {
    try txn.delete(try key(a, event.definition, event.since_sequence, outbox));
}
pub fn settled(a: A, txn: anytype, definition: index.Definition, through: u64) !bool {
    const selected = try prefix(a, definition);
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    const first = (try cursor.seekAtOrAfter(selected)) orelse return true;
    if (!std.mem.startsWith(u8, first.key, selected)) return true;
    if (first.key.len != selected.len + 40) return error.GroupedSourceCorrupt;
    const sequence = std.mem.readInt(u64, first.key[selected.len..][0..8], .big);
    const outbox = try queue.decode(first.key, first.value);
    const raw = txn.get(outbox) catch |err| return if (err == error.NotFound) error.GroupedSourceCorrupt else err;
    const event = try index.readInvalidation(a, outbox, raw);
    if (!std.meta.eql(event.definition, definition) or event.since_sequence != sequence or !std.mem.eql(u8, first.key, try key(a, definition, sequence, outbox))) return error.GroupedSourceCorrupt;
    return sequence > through;
}

test "source group barrier retains coalesced old debt and excludes independent later source writes" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const Txn = struct {
        const Self = @This();
        a: A,
        values: std.StringHashMapUnmanaged([]const u8) = .empty,
        const Row = struct { key: []const u8, value: []const u8 };
        const Cursor = struct {
            txn: *Self,
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), after: []const u8) !?Row {
                var chosen: ?Row = null;
                var it = self.txn.values.iterator();
                while (it.next()) |entry| {
                    if (std.mem.order(u8, entry.key_ptr.*, after) == .lt) continue;
                    if (chosen == null or std.mem.order(u8, entry.key_ptr.*, chosen.?.key) == .lt) chosen = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
                }
                return chosen;
            }
        };
        pub fn openPhysicalCursorAdapter(self: *@This()) !Cursor {
            return .{ .txn = self };
        }
        pub fn get(self: *@This(), selected: []const u8) ![]const u8 {
            return self.values.get(selected) orelse error.NotFound;
        }
        pub fn put(self: *@This(), selected: []const u8, raw: []const u8) !void {
            try self.values.put(self.a, try self.a.dupe(u8, selected), try self.a.dupe(u8, raw));
        }
        pub fn delete(self: *@This(), selected: []const u8) !void {
            _ = self.values.remove(selected);
        }
    };
    var txn = Txn{ .a = a };
    const definition = index.Definition{ .table_incarnation = 3, .configuration = groups.digest("configuration") };
    const projection = index.Projection{ .partition = .{ .group = "thread", .segment = "turn", .cell = 0 }, .member = .{ .document = "source", .version = 7, .ordinal = 1, .segment = "turn", .text = "one" } };
    _ = try index.updateProjection(a, &txn, definition, "source", 7, 11, projection);
    var outbox: []const u8 = "";
    var it = txn.values.iterator();
    while (it.next()) |entry| if (@import("group_source_transfer.zig").isKey(entry.key_ptr.*)) {
        const owner = try @import("group_source_transfer.zig").info(a, entry.key_ptr.*);
        if (owner.kind == index.outbox_kind) outbox = entry.key_ptr.*;
    };
    const first = try index.readInvalidation(a, outbox, try txn.get(outbox));
    try stage(a, &txn, outbox, null, first);
    try std.testing.expect(try settled(a, &txn, definition, 10));
    try std.testing.expect(!try settled(a, &txn, definition, 11));
    var update = projection;
    update.member.text = "two";
    _ = try index.updateProjection(a, &txn, definition, "source", 8, 99, update);
    const second = try index.readInvalidation(a, outbox, try txn.get(outbox));
    try stage(a, &txn, outbox, first, second);
    try std.testing.expectEqual(@as(u64, 11), second.since_sequence);
    try std.testing.expect(!try settled(a, &txn, definition, 11));
    const future = index.Invalidation{ .definition = definition, .partition = projection.partition, .document = "future", .source_revision = 9, .since_sequence = 100, .through_sequence = 100, .ordinal = 2 };
    // Use the actual source writer to obtain checksummed, key-bound bytes.
    var later_projection = projection;
    later_projection.member.document = future.document;
    _ = try index.updateProjection(a, &txn, definition, future.document, future.source_revision, 100, later_projection);
    var later_key: []const u8 = "";
    it = txn.values.iterator();
    while (it.next()) |entry| if (@import("group_source_transfer.zig").isKey(entry.key_ptr.*)) {
        const owner = try @import("group_source_transfer.zig").info(a, entry.key_ptr.*);
        if (owner.kind == index.outbox_kind and std.mem.eql(u8, owner.document, future.document)) later_key = entry.key_ptr.*;
    };
    const later_event = try index.readInvalidation(a, later_key, try txn.get(later_key));
    try stage(a, &txn, later_key, null, later_event);
    const old = try txn.get(outbox);
    try std.testing.expect(try index.acknowledge(a, &txn, outbox, old));
    try remove(a, &txn, outbox, second);
    try std.testing.expect(try settled(a, &txn, definition, 99));
    try std.testing.expect(!try settled(a, &txn, definition, 100));
}
