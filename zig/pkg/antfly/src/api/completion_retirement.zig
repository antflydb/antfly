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

//! Sequence checkpoints reject delayed registration after acknowledgement. Only
//! acknowledged markers are collected; live completion proofs never expire.
const std = @import("std");
pub const marker = "acknowledged-v2";
pub fn sequence(reference: [16]u8) u64 {
    return std.mem.readInt(u64, reference[8..16], .big);
}
pub fn advance(txn: anytype, alloc: std.mem.Allocator, root: []const u8, reference: [16]u8, proposed: u64) !u64 {
    const prefix = try std.fmt.allocPrint(alloc, "{s}{s}:", .{ root, &std.fmt.bytesToHex(reference[0..8].*, .lower) });
    defer alloc.free(prefix);
    const key = try std.fmt.allocPrint(alloc, "{s}floor", .{prefix});
    defer alloc.free(key);
    const old = txn.get(key) catch |err| switch (err) {
        error.NotFound => "0:0",
        else => return err,
    };
    var parts = std.mem.splitScalar(u8, old, ':');
    const previous = try std.fmt.parseInt(u64, parts.next().?, 10);
    const collected = try std.fmt.parseInt(u64, parts.next() orelse "0", 10);
    const floor = @max(previous, proposed);
    if (floor == 0 or (floor == previous and collected == floor)) return floor;
    const ack_prefix = try std.fmt.allocPrint(alloc, "{s}ack:", .{prefix});
    defer alloc.free(ack_prefix);
    const Entry = struct { key: []u8, target: []u8 };
    var expired: std.ArrayList(Entry) = .empty;
    defer {
        for (expired.items) |item| {
            alloc.free(item.key);
            alloc.free(item.target);
        }
        expired.deinit(alloc);
    }
    var cursor = try txn.openCursor();
    var open = true;
    defer if (open) cursor.close();
    var complete = true;
    var row = try cursor.seekAtOrAfter(ack_prefix);
    while (row) |entry| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, entry.key, ack_prefix)) break;
        const seq = try std.fmt.parseInt(u64, entry.key[ack_prefix.len..], 16);
        if (seq > floor) break;
        if (expired.items.len == 128) {
            complete = false;
            break;
        }
        const owned_key = try alloc.dupe(u8, entry.key);
        errdefer alloc.free(owned_key);
        const target = try alloc.dupe(u8, entry.value);
        errdefer alloc.free(target);
        try expired.append(alloc, .{ .key = owned_key, .target = target });
    }
    cursor.close();
    open = false;
    for (expired.items) |item| {
        try txn.delete(item.target);
        try txn.delete(item.key);
    }
    var value: [64]u8 = undefined;
    try txn.put(key, try std.fmt.bufPrint(&value, "{d}:{d}", .{ floor, if (complete) floor else collected }));
    return floor;
}
pub fn acknowledge(txn: anytype, alloc: std.mem.Allocator, root: []const u8, reference: [16]u8, target: []const u8) !void {
    const key = try std.fmt.allocPrint(alloc, "{s}{s}:ack:{x:0>16}", .{ root, &std.fmt.bytesToHex(reference[0..8].*, .lower), sequence(reference) });
    defer alloc.free(key);
    try txn.put(target, marker);
    try txn.put(key, target);
}
