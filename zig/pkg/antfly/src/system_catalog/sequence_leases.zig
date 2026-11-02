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

//! Shared definition/lifetime leases. Values and allocations stay live; only
//! descriptor mutations and retirement conflict with these catalog records.
const std = @import("std");
const A = std.mem.Allocator;
pub const Owner = struct {
    table: u64,
    publication: [16]u8,
    incarnation: [32]u8,
    effects: [32]u8,

    pub fn value(self: Owner) ![64]u8 {
        if (self.table == 0 or std.mem.allEqual(u8, &self.publication, 0) or
            std.mem.allEqual(u8, &self.incarnation, 0) or std.mem.allEqual(u8, &self.effects, 0)) return error.InvalidSequenceState;
        var bytes: [64]u8 = undefined;
        @memcpy(bytes[0..32], &self.incarnation);
        @memcpy(bytes[32..], &self.effects);
        return bytes;
    }
};
pub const Key = struct { counter: u64, table: u64, publication: [16]u8 };
pub fn prefix(buf: []u8, group: u64) ![]const u8 {
    if (group == 0) return error.InvalidSequenceState;
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:system_catalog:{d}:sequence-lease:", .{group});
}
pub fn counterPrefix(buf: []u8, group: u64, counter: u64) ![]const u8 {
    if (counter == 0) return error.InvalidSequenceState;
    var base: [128]u8 = undefined;
    return std.fmt.bufPrint(buf, "{s}{x:0>16}:", .{ try prefix(&base, group), counter });
}
pub fn key(buf: []u8, group: u64, counter: u64, owner: Owner) ![]const u8 {
    _ = try owner.value();
    var base: [160]u8 = undefined;
    return std.fmt.bufPrint(buf, "{s}{x:0>16}:{s}", .{ try counterPrefix(&base, group, counter), owner.table, std.fmt.bytesToHex(owner.publication, .lower) });
}
pub fn parse(group: u64, raw: []const u8, value: []const u8) !Key {
    var base: [128]u8 = undefined;
    const start = try prefix(&base, group);
    if (!std.mem.startsWith(u8, raw, start) or raw.len != start.len + 66 or value.len != 64 or
        std.mem.allEqual(u8, value[0..32], 0) or std.mem.allEqual(u8, value[32..], 0)) return error.InvalidSequenceState;
    const tail = raw[start.len..];
    if (tail[16] != ':' or tail[33] != ':') return error.InvalidSequenceState;
    for (tail, 0..) |byte, i| {
        if (i == 16 or i == 33) continue;
        if (!(byte >= '0' and byte <= '9') and !(byte >= 'a' and byte <= 'f')) return error.InvalidSequenceState;
    }
    const counter = std.fmt.parseInt(u64, tail[0..16], 16) catch return error.InvalidSequenceState;
    const table = std.fmt.parseInt(u64, tail[17..33], 16) catch return error.InvalidSequenceState;
    var id: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&id, tail[34..]) catch return error.InvalidSequenceState;
    if (counter == 0 or table == 0 or std.mem.allEqual(u8, &id, 0)) return error.InvalidSequenceState;
    return .{ .counter = counter, .table = table, .publication = id };
}
pub fn acquire(txn: anytype, group: u64, counter: u64, owner: Owner) !void {
    var buf: [256]u8 = undefined;
    const storage_key = try key(&buf, group, counter, owner);
    if (txn.get(storage_key)) |_| return error.InvalidSequenceState else |err| if (err != error.NotFound) return err;
    try txn.put(storage_key, &try owner.value());
}
pub fn require(txn: anytype, group: u64, counter: u64, owner: Owner) !void {
    var buf: [256]u8 = undefined;
    const raw = txn.get(try key(&buf, group, counter, owner)) catch |err| return if (err == error.NotFound) error.InvalidSequenceState else err;
    if (!std.mem.eql(u8, raw, &try owner.value())) return error.InvalidSequenceState;
}
pub fn release(txn: anytype, group: u64, counter: u64, owner: Owner) !void {
    try require(txn, group, counter, owner);
    var buf: [256]u8 = undefined;
    try txn.delete(try key(&buf, group, counter, owner));
}
/// One prefix seek, independent of unrelated resources or number of readers.
pub fn requireUnleased(txn: anytype, group: u64, counter: u64) !void {
    var buf: [192]u8 = undefined;
    const start = try counterPrefix(&buf, group, counter);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(start)) orelse return;
    if (!std.mem.startsWith(u8, row.key, start)) return;
    _ = try parse(group, row.key, row.value);
    return error.CatalogPublicationProofPending;
}

test "sequence definition leases share counters retain cold owners and reject stale releases" {
    const local = @import("antfly_local_sources");
    const a: A = std.testing.allocator;
    var directory = try local.common_test_directory.TestDirectory.init("sequence-shared-definition-leases");
    defer directory.cleanup();
    const path = try a.dupeSentinel(u8, directory.path(), 0);
    defer a.free(path);
    const first: Owner = .{ .table = 7, .publication = @splat(1), .incarnation = @splat(2), .effects = @splat(3) };
    const second: Owner = .{ .table = 8, .publication = @splat(4), .incarnation = first.incarnation, .effects = @splat(5) };
    {
        var store = try local.storage_docstore.DocStore.open(a, path, .{});
        defer store.close();
        var txn = try store.beginWriteTxn();
        errdefer txn.abort();
        try acquire(&txn, 21, 40, first);
        try acquire(&txn, 21, 40, second);
        try std.testing.expectError(error.CatalogPublicationProofPending, requireUnleased(&txn, 21, 40));
        try requireUnleased(&txn, 21, 41);
        try txn.commit();
    }
    var store = try local.storage_docstore.DocStore.open(a, path, .{});
    defer store.close();
    var txn = try store.beginWriteTxn();
    defer txn.abort();
    try require(&txn, 21, 40, first);
    try require(&txn, 21, 40, second);
    var stale = first;
    stale.effects[0] ^= 1;
    try std.testing.expectError(error.InvalidSequenceState, release(&txn, 21, 40, stale));
    try release(&txn, 21, 40, first);
    try std.testing.expectError(error.CatalogPublicationProofPending, requireUnleased(&txn, 21, 40));
    try release(&txn, 21, 40, second);
    try requireUnleased(&txn, 21, 40);
}
