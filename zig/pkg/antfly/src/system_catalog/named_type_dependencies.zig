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

//! Durable type/table dependency edges, committed with table metadata.
//! Membership changes avoid rewriting unchanged edges under schema churn.
const std = @import("std");
const A = std.mem.Allocator;
const max_types = @import("antfly_local_sources").sql_catalog.named_layouts.max_types;

fn tableKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:type-table:{d}", .{ group, table });
}
fn prefixKey(buffer: []u8, group: u64, id: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:type-dependent:{d}:", .{ group, id });
}
fn edgeKey(buffer: []u8, group: u64, id: u64, table: u64) ![]const u8 {
    const prefix = try prefixKey(buffer, group, id);
    if (buffer.len - prefix.len < 8) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], table, .big);
    return buffer[0 .. prefix.len + 8];
}
fn validateIds(ids: []const u64) !void {
    if (ids.len > max_types) return error.InvalidCatalogRecord;
    var previous: u64 = 0;
    for (ids) |id| {
        if (id <= previous) return error.InvalidCatalogRecord;
        previous = id;
    }
}
pub fn loadAlloc(a: A, txn: anytype, group: u64, table: u64) ![]u64 {
    var buffer: [160]u8 = undefined;
    const bytes = txn.get(try tableKey(&buffer, group, table)) catch |err| switch (err) {
        error.NotFound => return a.alloc(u64, 0),
        else => return err,
    };
    if (bytes.len < 8 or !std.mem.eql(u8, bytes[0..4], "ATYP")) return error.InvalidCatalogRecord;
    const count = std.mem.readInt(u32, bytes[4..8], .little);
    if (count > max_types or bytes.len != 8 + @as(usize, count) * 8) return error.InvalidCatalogRecord;
    const ids = try a.alloc(u64, count);
    errdefer a.free(ids);
    for (ids, 0..) |*id, i| id.* = std.mem.readInt(u64, bytes[8 + i * 8 ..][0..8], .little);
    try validateIds(ids);
    return ids;
}
pub fn synchronize(a: A, txn: anytype, group: u64, table: u64, ids: []const u64) !void {
    if (table == 0) return error.InvalidCatalogRecord;
    try validateIds(ids);
    const previous = try loadAlloc(a, txn, group, table);
    defer a.free(previous);
    if (std.mem.eql(u64, previous, ids)) return;
    var old_index: usize = 0;
    var new_index: usize = 0;
    while (old_index < previous.len or new_index < ids.len) {
        var key_buffer: [192]u8 = undefined;
        if (new_index == ids.len or (old_index < previous.len and previous[old_index] < ids[new_index])) {
            try txn.delete(try edgeKey(&key_buffer, group, previous[old_index], table));
            old_index += 1;
        } else if (old_index == previous.len or ids[new_index] < previous[old_index]) {
            try txn.put(try edgeKey(&key_buffer, group, ids[new_index], table), &.{1});
            new_index += 1;
        } else {
            old_index += 1;
            new_index += 1;
        }
    }
    var buffer: [160]u8 = undefined;
    const key = try tableKey(&buffer, group, table);
    if (ids.len == 0) {
        try txn.delete(key);
        return;
    }
    const bytes = try a.alloc(u8, 8 + ids.len * 8);
    defer a.free(bytes);
    @memcpy(bytes[0..4], "ATYP");
    std.mem.writeInt(u32, bytes[4..8], @intCast(ids.len), .little);
    for (ids, 0..) |id, i| std.mem.writeInt(u64, bytes[8 + i * 8 ..][0..8], id, .little);
    try txn.put(key, bytes);
}
pub fn firstDependent(txn: anytype, group: u64, id: u64) !?u64 {
    var buffer: [192]u8 = undefined;
    const prefix = try prefixKey(&buffer, group, id);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(prefix)) orelse return null;
    if (!std.mem.startsWith(u8, row.key, prefix)) return null;
    if (row.key.len != prefix.len + 8 or !std.mem.eql(u8, row.value, &.{1})) return error.InvalidCatalogRecord;
    const table = std.mem.readInt(u64, row.key[prefix.len..][0..8], .big);
    if (table == 0) return error.InvalidCatalogRecord;
    return table;
}

/// Immutable activation preparation, not an authority or a paginated partial
/// membership proof. Read at most one excess edge before rejecting oversized
/// cohorts; ordinary point mutations keep their O(1) firstDependent path.
pub const max_dependents = 128;
pub fn dependentsAlloc(a: A, txn: anytype, group: u64, id: u64, limit: usize) ![]u64 {
    if (id == 0 or limit == 0 or limit > max_dependents) return error.InvalidCatalogMutation;
    var buffer: [192]u8 = undefined;
    const prefix = try prefixKey(&buffer, group, id);
    var cursor = try txn.openCursor();
    defer cursor.close();
    var result: std.ArrayList(u64) = .empty;
    defer result.deinit(a);
    var row = try cursor.seekAtOrAfter(prefix);
    var previous: u64 = 0;
    while (row) |item| : (row = try cursor.next()) {
        if (!std.mem.startsWith(u8, item.key, prefix)) break;
        if (item.key.len != prefix.len + 8 or !std.mem.eql(u8, item.value, &.{1})) return error.InvalidCatalogRecord;
        const table = std.mem.readInt(u64, item.key[prefix.len..][0..8], .big);
        if (table <= previous) return error.InvalidCatalogRecord;
        if (result.items.len == limit) return error.SqlProgramLimitExceeded;
        try result.append(a, table);
        previous = table;
    }
    return result.toOwnedSlice(a);
}

/// Discovery/exclusion only. The immutable staged rewrite and its exact
/// current type/dependency cut are the publication authority.
pub const RetirementLock = struct {
    id: [16]u8,
    intent_digest: [32]u8,
    admission_digest: [32]u8,
    generation: u32,
    pub const encoded_len = 88;
    pub fn encode(self: @This()) ![encoded_len]u8 {
        if (self.generation == 0 or std.mem.allEqual(u8, &self.id, 0) or std.mem.allEqual(u8, &self.intent_digest, 0) or std.mem.allEqual(u8, &self.admission_digest, 0)) return error.InvalidCatalogRecord;
        var bytes: [encoded_len]u8 = undefined;
        bytes[0..4].* = "ATR1".*;
        bytes[4..20].* = self.id;
        bytes[20..52].* = self.intent_digest;
        bytes[52..84].* = self.admission_digest;
        std.mem.writeInt(u32, bytes[84..88], self.generation, .little);
        return bytes;
    }
    pub fn decode(bytes: []const u8) !@This() {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "ATR1")) return error.InvalidCatalogRecord;
        const value: @This() = .{ .id = bytes[4..20].*, .intent_digest = bytes[20..52].*, .admission_digest = bytes[52..84].*, .generation = std.mem.readInt(u32, bytes[84..88], .little) };
        _ = try value.encode();
        return value;
    }
};
pub fn retirementKey(buffer: []u8, group: u64, type_id: u64) ![]const u8 {
    if (group == 0 or type_id == 0) return error.InvalidCatalogRecord;
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:type-retirement:{d}", .{ group, type_id });
}
pub fn retirementLock(txn: anytype, group: u64, type_id: u64) !?RetirementLock {
    var buffer: [192]u8 = undefined;
    const bytes = txn.get(try retirementKey(&buffer, group, type_id)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try RetirementLock.decode(bytes);
}
pub fn acquireRetirement(txn: anytype, group: u64, type_id: u64, lock: RetirementLock) !void {
    const bytes = try lock.encode();
    if (try retirementLock(txn, group, type_id) != null) return error.CatalogPublicationProofPending;
    var buffer: [192]u8 = undefined;
    try txn.put(try retirementKey(&buffer, group, type_id), &bytes);
}
pub fn releaseRetirement(txn: anytype, group: u64, type_id: u64, expected: RetirementLock) !void {
    const actual = (try retirementLock(txn, group, type_id)) orelse return error.CatalogGenerationChanged;
    if (!std.meta.eql(actual, expected)) return error.CatalogGenerationChanged;
    var buffer: [192]u8 = undefined;
    try txn.delete(try retirementKey(&buffer, group, type_id));
}

test "SQL routine catalog type retirement locks reject corrupt and substituted ownership" {
    const lock: RetirementLock = .{ .id = @splat(1), .intent_digest = @splat(2), .admission_digest = @splat(3), .generation = 7 };
    const bytes = try lock.encode();
    try std.testing.expectEqualDeep(lock, try RetirementLock.decode(&bytes));
    try std.testing.expectError(error.InvalidCatalogRecord, RetirementLock.decode(bytes[0 .. bytes.len - 1]));
    var bad = bytes;
    @memset(bad[84..88], 0);
    try std.testing.expectError(error.InvalidCatalogRecord, RetirementLock.decode(&bad));
}
