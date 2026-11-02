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

//! Definition and dependency-source leases for a retiring view closure.
//! Query readers and COMMENT remain live; adding dependencies, changing a
//! definition/name and deleting a leased view require completion of its root.
const std = @import("std");
pub const Owner = @import("sequence_leases.zig").Owner;
const invalid = error.InvalidCatalogRecord;
pub const Key = struct { view: u64, table: u64, publication: [16]u8 };
pub fn prefix(buf: []u8, group: u64) ![]const u8 {
    if (group == 0) return invalid;
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:system_catalog:{d}:view-lease:", .{group});
}
pub fn viewPrefix(buf: []u8, group: u64, view: u64) ![]const u8 {
    if (view == 0) return invalid;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buf, "{s}{x:0>16}:", .{ try prefix(&base, group), view });
}
pub fn key(buf: []u8, group: u64, view: u64, owner: Owner) ![]const u8 {
    _ = try owner.value();
    var base: [160]u8 = undefined;
    return std.fmt.bufPrint(buf, "{s}{x:0>16}:{s}", .{ try viewPrefix(&base, group, view), owner.table, std.fmt.bytesToHex(owner.publication, .lower) });
}
pub fn parse(group: u64, raw: []const u8, value: []const u8) !Key {
    var base: [128]u8 = undefined;
    const start = try prefix(&base, group);
    if (!std.mem.startsWith(u8, raw, start) or raw.len != start.len + 66 or value.len != 64 or
        std.mem.allEqual(u8, value[0..32], 0) or std.mem.allEqual(u8, value[32..], 0)) return invalid;
    const tail = raw[start.len..];
    if (tail[16] != ':' or tail[33] != ':') return invalid;
    for (tail, 0..) |byte, i| {
        if (i == 16 or i == 33) continue;
        if (!(byte >= '0' and byte <= '9') and !(byte >= 'a' and byte <= 'f')) return invalid;
    }
    const view = std.fmt.parseInt(u64, tail[0..16], 16) catch return invalid;
    const table = std.fmt.parseInt(u64, tail[17..33], 16) catch return invalid;
    var id: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&id, tail[34..]) catch return invalid;
    if (view == 0 or table == 0 or std.mem.allEqual(u8, &id, 0)) return invalid;
    return .{ .view = view, .table = table, .publication = id };
}
pub fn acquire(txn: anytype, group: u64, view: u64, owner: Owner) !void {
    var buf: [256]u8 = undefined;
    const selected = try key(&buf, group, view, owner);
    if (txn.get(selected)) |_| return invalid else |err| if (err != error.NotFound) return err;
    try txn.put(selected, &try owner.value());
}
pub fn require(txn: anytype, group: u64, view: u64, owner: Owner) !void {
    var buf: [256]u8 = undefined;
    const value = txn.get(try key(&buf, group, view, owner)) catch |err| return if (err == error.NotFound) invalid else err;
    if (!std.mem.eql(u8, value, &try owner.value())) return invalid;
}
pub fn release(txn: anytype, group: u64, view: u64, owner: Owner) !void {
    try require(txn, group, view, owner);
    var buf: [256]u8 = undefined;
    try txn.delete(try key(&buf, group, view, owner));
}
pub fn requireUnleased(txn: anytype, group: u64, view: u64) !void {
    var buf: [192]u8 = undefined;
    const start = try viewPrefix(&buf, group, view);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(start)) orelse return;
    if (!std.mem.startsWith(u8, row.key, start)) return;
    _ = try parse(group, row.key, row.value);
    return error.CatalogPublicationProofPending;
}
