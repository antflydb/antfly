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

//! Reconstructible pending source positions. A later segment must not outrun
//! an earlier event still waiting for delivery/settlement on another owner.
const std = @import("std");
const index = @import("group_source_index.zig");
const groups = @import("group_sources.zig");
const queue = @import("group_source_queue.zig");
const A = std.mem.Allocator;
pub const prefix = "\x00\x00G\x0a";
fn groupPrefix(definition: index.Definition, group: []const u8) [84]u8 {
    var out: [84]u8 = undefined;
    @memcpy(out[0..4], prefix);
    std.mem.writeInt(u128, out[4..20], definition.table_incarnation, .big);
    @memcpy(out[20..52], &definition.configuration);
    @memcpy(out[52..84], &groups.digest(group));
    return out;
}
fn position(a: A, definition: index.Definition, partition: index.Partition, ordinal: i64) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(a, &groupPrefix(definition, partition.group));
    var order: [8]u8 = undefined;
    std.mem.writeInt(u64, &order, @as(u64, @bitCast(ordinal)) ^ (@as(u64, 1) << 63), .big);
    try out.appendSlice(a, &order);
    for (partition.segment) |byte| {
        try out.append(a, byte);
        if (byte == 0) try out.append(a, 255);
    }
    try out.appendSlice(a, &.{ 0, 0 });
    return out.toOwnedSlice(a);
}
fn key(a: A, event: index.Invalidation, outbox: []const u8) ![]u8 {
    return std.mem.concat(a, u8, &.{ try position(a, event.definition, event.partition, event.ordinal), &groups.digest(outbox) });
}
pub fn remove(a: A, txn: anytype, outbox: []const u8, event: index.Invalidation) !void {
    try txn.delete(try key(a, event, outbox));
}
pub fn stage(a: A, txn: anytype, outbox: []const u8, previous: ?index.Invalidation, event: index.Invalidation) !void {
    if (previous) |old| try remove(a, txn, outbox, old);
    if (event.delivered) {
        try remove(a, txn, outbox, event);
        return;
    }
    const selected = try key(a, event, outbox);
    try txn.put(selected, try queue.encode(a, selected, outbox));
}
pub fn hasEarlier(a: A, txn: anytype, definition: index.Definition, partition: index.Partition, ordinal: i64, bounds: anytype) !bool {
    const start = groupPrefix(definition, partition.group);
    const upper = try position(a, definition, partition, ordinal);
    var cursor = try txn.openPhysicalCursorAdapter();
    defer cursor.close();
    var row = try cursor.seekAtOrAfter(&start);
    var visits: usize = 0;
    while (row) |entry| {
        if (!std.mem.startsWith(u8, entry.key, &start) or std.mem.order(u8, entry.key, upper) != .lt) return false;
        if (visits == 128) return error.GroupedSourceBaselinePending;
        visits += 1;
        const outbox = try queue.decode(entry.key, entry.value);
        const raw = txn.get(outbox) catch |err| if (err == error.NotFound) {
            row = try cursor.next();
            continue;
        } else return err;
        const event = try index.readInvalidation(a, outbox, raw);
        if (!event.delivered and bounds.contains(event.document) and std.mem.eql(u8, entry.key, try key(a, event, outbox))) return true;
        row = try cursor.next();
    }
    return false;
}
