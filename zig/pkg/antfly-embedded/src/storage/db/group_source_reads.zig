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

//! Snapshot-bounded private read ports. Wire cursors use base64 so physical
//! keys (including NUL and non-UTF8 bytes) never pass through JSON strings.
const std = @import("std");
const groups = @import("group_sources.zig");
const index = @import("group_source_index.zig");
const state = @import("group_source_state.zig");
const queue = @import("group_source_queue.zig");
const contract = @import("group_source_contract.zig");
const A = std.mem.Allocator;
pub const Bounds = struct {
    start: []const u8 = "",
    end: []const u8 = "",
    pub fn contains(self: @This(), document: []const u8) bool {
        return (self.start.len == 0 or std.mem.order(u8, document, self.start) != .lt) and (self.end.len == 0 or std.mem.order(u8, document, self.end) == .lt);
    }
};
pub const SourcePage = struct {
    owner: state.Owner,
    revision: u64,
    members: []const groups.Member,
    cursor: []const u8,
    done: bool,
    earlier_pending: bool = false,
};
pub const Pointer = struct { key_base64: []const u8, value_base64: []const u8 };
pub const PointerPage = struct { rows: []const Pointer, cursor: []const u8, done: bool };
pub fn encode(a: A, raw: []const u8) ![]const u8 {
    const encoder = std.base64.standard.Encoder;
    const buffer = try a.alloc(u8, encoder.calcSize(raw.len));
    return encoder.encode(buffer, raw);
}
pub fn decode(a: A, raw: []const u8) ![]const u8 {
    const decoder = std.base64.standard.Decoder;
    const buffer = try a.alloc(u8, decoder.calcSizeForSlice(raw) catch return error.InvalidGroupedSourceQuery);
    decoder.decode(buffer, raw) catch return error.InvalidGroupedSourceQuery;
    return buffer;
}
pub fn partition(a: A, txn: anytype, query: contract.Query, owner: state.Owner, bounds: Bounds) !SourcePage {
    try query.validate();
    const selected_partition = index.Partition{ .group = query.group, .segment = query.segment, .cell = query.cell };
    if (try @import("group_source_order.zig").hasEarlier(a, txn, query.definition, selected_partition, query.ordinal, bounds)) return .{ .owner = owner, .revision = try index.partitionRevision(a, txn, query.definition, selected_partition), .members = &.{}, .cursor = "", .done = true, .earlier_pending = true };
    const prefix = try index.partitionPrefix(a, query.definition, .{ .group = query.group, .segment = query.segment, .cell = query.cell });
    const start = if (query.cursor.len > 0) try decode(a, query.cursor) else prefix;
    if (!std.mem.startsWith(u8, start, prefix)) return error.InvalidGroupedSourceQuery;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(start);
    if (query.cursor.len > 0 and row != null and std.mem.eql(u8, row.?.key, start)) row = try cursor.next();
    var members: std.ArrayList(groups.Member) = .empty;
    var bytes: usize = 0;
    var visits: usize = 0;
    var last: []const u8 = "";
    while (row) |current| {
        if (!std.mem.startsWith(u8, current.key, prefix)) break;
        if (members.items.len == query.limit or visits == 128) break;
        visits += 1;
        if (bytes + current.value.len > query.max_bytes) {
            if (members.items.len == 0) return error.GroupedSourceMemberTooLarge;
            break;
        }
        const projection = try index.readProjection(a, query.definition, current.key, current.value);
        if (bounds.contains(projection.member.document) and try index.coveringIsCurrent(a, txn, query.definition, projection, current.value)) try members.append(a, projection.member);
        bytes += current.value.len;
        last = try a.dupe(u8, current.key);
        row = try cursor.next();
    }
    const done = row == null or !std.mem.startsWith(u8, row.?.key, prefix);
    return .{ .owner = owner, .revision = try index.partitionRevision(a, txn, query.definition, .{ .group = query.group, .segment = query.segment, .cell = query.cell }), .members = try members.toOwnedSlice(a), .cursor = if (done) "" else try encode(a, last), .done = done };
}
/// The pointer directory has no source text. Each caller rechecks the
/// document-owned authority record before acting on a reconstructible hint.
pub fn directory(a: A, txn: anytype, prefix: []const u8, after: []const u8, limit: usize, bounds: Bounds) !PointerPage {
    if (limit == 0 or limit > 128 or after.len > 32768) return error.InvalidGroupedSourceQuery;
    const start = if (after.len > 0) try decode(a, after) else prefix;
    if (!std.mem.startsWith(u8, start, prefix)) return error.InvalidGroupedSourceQuery;
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(start);
    if (after.len > 0 and row != null and std.mem.eql(u8, row.?.key, start)) row = try cursor.next();
    var rows: std.ArrayList(Pointer) = .empty;
    var last: []const u8 = "";
    var visits: usize = 0;
    while (row) |current| {
        if (!std.mem.startsWith(u8, current.key, prefix) or rows.items.len == limit or visits == 128) break;
        visits += 1;
        last = try a.dupe(u8, current.key);
        const payload = try queue.decode(current.key, current.value);
        if (std.mem.eql(u8, prefix, queue.source_prefix)) {
            const value = txn.get(payload) catch |err| if (err == error.NotFound) {
                row = try cursor.next();
                continue;
            } else return err;
            const event = try index.readInvalidation(a, payload, value);
            if (!bounds.contains(event.document)) {
                row = try cursor.next();
                continue;
            }
            try rows.append(a, .{ .key_base64 = try encode(a, payload), .value_base64 = try encode(a, value) });
        } else {
            const group = try std.json.parseFromSliceLeaky(state.Group, a, payload, .{});
            if (!bounds.contains(try group.anchor(a)) or !try state.hasPending(a, txn, group)) {
                row = try cursor.next();
                continue;
            }
            try rows.append(a, .{ .key_base64 = try encode(a, current.key), .value_base64 = try encode(a, payload) });
        }
        last = try a.dupe(u8, current.key);
        row = try cursor.next();
    }
    const done = row == null or !std.mem.startsWith(u8, row.?.key, prefix);
    return .{ .rows = try rows.toOwnedSlice(a), .cursor = if (done) "" else try encode(a, last), .done = done };
}

test "group read directories ignore stale hints and retain authoritative deliveries" {
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
            last: []const u8 = "",
            pub fn close(_: *@This()) void {}
            pub fn seekAtOrAfter(self: *@This(), start: []const u8) !?Row {
                var best: ?Row = null;
                var it = self.txn.values.iterator();
                while (it.next()) |entry| {
                    if (std.mem.order(u8, entry.key_ptr.*, start) == .lt) continue;
                    if (best == null or std.mem.order(u8, entry.key_ptr.*, best.?.key) == .lt) best = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
                }
                if (best) |row| self.last = row.key;
                return best;
            }
            pub fn next(self: *@This()) !?Row {
                return self.seekAtOrAfter(try std.mem.concat(self.txn.a, u8, &.{ self.last, "\x00" }));
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
    const definition = index.Definition{ .table_incarnation = 7, .configuration = groups.digest("configuration") };
    const group = state.Group{ .definition = definition, .key = "thread" };
    const anchor = try group.anchor(a);
    try queue.stageSource(a, &txn, "missing-outbox");
    try queue.stageGroup(a, &txn, anchor, group);
    try std.testing.expectEqual(@as(usize, 0), (try directory(a, &txn, queue.source_prefix, "", 128, .{})).rows.len);
    try std.testing.expectEqual(@as(usize, 0), (try directory(a, &txn, queue.group_prefix, "", 128, .{})).rows.len);
    try index.dirty(a, &txn, definition, .{ .group = "thread", .segment = "turn", .cell = 0 }, "row", 1, 1, 1);
    const ordered = @import("group_source_order.zig");
    const later = index.Partition{ .group = "thread", .segment = "later", .cell = 0 };
    try std.testing.expect(try ordered.hasEarlier(a, &txn, definition, later, 2, Bounds{}));
    try std.testing.expect(!try ordered.hasEarlier(a, &txn, definition, later, 0, Bounds{}));
    try std.testing.expect(!try ordered.hasEarlier(a, &txn, definition, later, 2, Bounds{ .start = "z" }));
    const blocked = try partition(a, &txn, .{ .action = .partition, .artifact = "turns", .definition = definition, .group = "thread", .segment = "later", .ordinal = 2 }, .{ .namespace = @splat(1), .incarnation = 1 }, .{});
    try std.testing.expect(blocked.earlier_pending and blocked.members.len == 0);
    const page = try directory(a, &txn, queue.source_prefix, "", 128, .{});
    try std.testing.expectEqual(@as(usize, 1), page.rows.len);
    const outbox = try decode(a, page.rows[0].key_base64);
    const event = try index.readInvalidation(a, outbox, try decode(a, page.rows[0].value_base64));
    const owner = state.Owner{ .namespace = @splat(1), .incarnation = 1 };
    const scheduled = try state.invalidate(a, &txn, owner, event);
    const original = try txn.get(outbox);
    try std.testing.expect(try index.setDelivery(a, &txn, outbox, original, true));
    const delivered_raw = try txn.get(outbox);
    const delivered = try index.readInvalidation(a, outbox, delivered_raw);
    try std.testing.expect(!try ordered.hasEarlier(a, &txn, definition, later, 2, Bounds{}));
    try std.testing.expectEqualSlices(u8, &(try index.eventDigest(a, event)), &(try index.eventDigest(a, delivered)));
    try std.testing.expectEqual(scheduled, try state.invalidate(a, &txn, owner, delivered));
    try std.testing.expect(try index.setDelivery(a, &txn, outbox, delivered_raw, false));
    try std.testing.expect(!std.mem.eql(u8, original, try txn.get(outbox))); // Delivery CAS cannot ABA.
    try std.testing.expect(!try index.acknowledge(a, &txn, outbox, original));
    try std.testing.expect(try ordered.hasEarlier(a, &txn, definition, later, 2, Bounds{}));
    try std.testing.expectEqual(@as(usize, 1), (try directory(a, &txn, queue.group_prefix, "", 128, .{})).rows.len);
    try txn.delete(outbox);
    try std.testing.expect(!try ordered.hasEarlier(a, &txn, definition, later, 2, Bounds{}));
    try txn.delete(try @import("group_source_identity.zig").documentMetadataKey(a, anchor, &.{1}));
    try std.testing.expectEqual(@as(usize, 0), (try directory(a, &txn, queue.source_prefix, "", 128, .{})).rows.len);
    try std.testing.expectEqual(@as(usize, 0), (try directory(a, &txn, queue.group_prefix, "", 128, .{})).rows.len);
}
