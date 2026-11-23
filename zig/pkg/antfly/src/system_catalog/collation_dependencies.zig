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

//! Durable collation/table dependency edges, committed with table metadata.
//! Membership changes avoid rewriting unchanged edges under schema churn.
const std = @import("std");
const A = std.mem.Allocator;
const max_types = 4096;

pub fn tableKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-table:{d}", .{ group, table });
}
pub fn tableFromKey(bytes: []const u8, group: u64) !?u64 {
    var buffer: [192]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-table:", .{group});
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    const id = std.fmt.parseInt(u64, bytes[prefix.len..], 10) catch return error.InvalidCatalogRecord;
    if (id == 0 or !std.mem.eql(u8, bytes, try tableKey(&buffer, group, id))) return error.InvalidCatalogRecord;
    return id;
}
fn prefixKey(buffer: []u8, group: u64, id: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-dependent:{d}:", .{ group, id });
}
pub fn edgeKey(buffer: []u8, group: u64, id: u64, table: u64) ![]const u8 {
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
    if (bytes.len < 8 or !std.mem.eql(u8, bytes[0..4], "ACOL")) return error.InvalidCatalogRecord;
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
    @memcpy(bytes[0..4], "ACOL");
    std.mem.writeInt(u32, bytes[4..8], @intCast(ids.len), .little);
    for (ids, 0..) |id, i| std.mem.writeInt(u64, bytes[8 + i * 8 ..][0..8], id, .little);
    try txn.put(key, bytes);
}
fn firstTableDependent(txn: anytype, group: u64, id: u64) !?u64 {
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

/// Hidden initial publication owns a separate immutable hold until normal
/// table dependencies are installed (or every private owner is canceled).
pub const Initial = struct {
    plan: [16]u8,
    digest: [32]u8,
    ids: []u64,
    pub fn deinit(self: Initial, a: A) void {
        a.free(self.ids);
    }
};
pub fn initialTableKey(buffer: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-initial-table:{d}", .{ group, table });
}
pub fn initialTableFromKey(bytes: []const u8, group: u64) !?u64 {
    var buffer: [192]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-initial-table:", .{group});
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    const id = std.fmt.parseInt(u64, bytes[prefix.len..], 10) catch return error.InvalidCatalogRecord;
    if (id == 0 or !std.mem.eql(u8, bytes, try initialTableKey(&buffer, group, id))) return error.InvalidCatalogRecord;
    return id;
}
fn initialPrefix(buffer: []u8, group: u64, id: u64) ![]const u8 {
    return std.fmt.bufPrint(buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-initial-dependent:{d}:", .{ group, id });
}
pub fn initialEdgeKey(buffer: []u8, group: u64, id: u64, table: u64) ![]const u8 {
    const prefix = try initialPrefix(buffer, group, id);
    if (buffer.len - prefix.len < 8) return error.NoSpaceLeft;
    std.mem.writeInt(u64, buffer[prefix.len..][0..8], table, .big);
    return buffer[0 .. prefix.len + 8];
}
pub fn initialEdgeFromKey(bytes: []const u8, group: u64) !?struct { collation: u64, table: u64 } {
    var buffer: [192]u8 = undefined;
    const prefix = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-initial-dependent:", .{group});
    if (!std.mem.startsWith(u8, bytes, prefix)) return null;
    const tail = bytes[prefix.len..];
    const colon = std.mem.indexOfScalar(u8, tail, ':') orelse return error.InvalidCatalogRecord;
    if (colon == 0 or tail.len != colon + 9) return error.InvalidCatalogRecord;
    const id = std.fmt.parseInt(u64, tail[0..colon], 10) catch return error.InvalidCatalogRecord;
    const table = std.mem.readInt(u64, tail[colon + 1 ..][0..8], .big);
    if (id == 0 or table == 0 or !std.mem.eql(u8, bytes, try initialEdgeKey(&buffer, group, id, table))) return error.InvalidCatalogRecord;
    return .{ .collation = id, .table = table };
}
pub fn initialProof(plan: [16]u8, digest: [32]u8) ![48]u8 {
    if (std.mem.allEqual(u8, &plan, 0) or std.mem.allEqual(u8, &digest, 0)) return error.InvalidCatalogRecord;
    var proof: [48]u8 = undefined;
    @memcpy(proof[0..16], &plan);
    @memcpy(proof[16..48], &digest);
    return proof;
}
pub fn readInitial(a: A, reader: anytype, group: u64, table: u64) !?Initial {
    var buffer: [192]u8 = undefined;
    const bytes = reader.get(try initialTableKey(&buffer, group, table)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    if (bytes.len < 56 or !std.mem.eql(u8, bytes[0..4], "ACIH")) return error.InvalidCatalogRecord;
    var plan: [16]u8 = undefined;
    @memcpy(&plan, bytes[4..20]);
    var digest: [32]u8 = undefined;
    @memcpy(&digest, bytes[20..52]);
    _ = try initialProof(plan, digest);
    const count = std.mem.readInt(u32, bytes[52..56], .little);
    if (count == 0 or count > max_types or bytes.len != 56 + @as(usize, count) * 8) return error.InvalidCatalogRecord;
    const ids = try a.alloc(u64, count);
    errdefer a.free(ids);
    for (ids, 0..) |*id, i| id.* = std.mem.readInt(u64, bytes[56 + i * 8 ..][0..8], .little);
    try validateIds(ids);
    return .{ .plan = plan, .digest = digest, .ids = ids };
}
pub fn reserveInitial(a: A, txn: anytype, group: u64, table: u64, plan: [16]u8, digest: [32]u8, ids: []const u64) !void {
    if (table == 0 or ids.len == 0) return error.InvalidCatalogRecord;
    try validateIds(ids);
    const proof = try initialProof(plan, digest);
    if (try readInitial(a, txn, group, table)) |existing| {
        defer existing.deinit(a);
        if (!std.mem.eql(u8, &existing.plan, &plan) or !std.mem.eql(u8, &existing.digest, &digest) or !std.mem.eql(u64, existing.ids, ids)) return error.InvalidCatalogRecord;
        return;
    }
    const bytes = try a.alloc(u8, 56 + ids.len * 8);
    defer a.free(bytes);
    @memcpy(bytes[0..4], "ACIH");
    @memcpy(bytes[4..52], &proof);
    std.mem.writeInt(u32, bytes[52..56], @intCast(ids.len), .little);
    var buffer: [192]u8 = undefined;
    for (ids) |id| {
        const existing = txn.get(try initialEdgeKey(&buffer, group, id, table)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        if (existing != null) return error.InvalidCatalogRecord;
    }
    for (ids, 0..) |id, i| {
        std.mem.writeInt(u64, bytes[56 + i * 8 ..][0..8], id, .little);
        try txn.put(try initialEdgeKey(&buffer, group, id, table), &proof);
    }
    try txn.put(try initialTableKey(&buffer, group, table), bytes);
}
pub fn releaseInitial(a: A, txn: anytype, group: u64, table: u64, plan: [16]u8, digest: [32]u8) !void {
    const existing = (try readInitial(a, txn, group, table)) orelse return error.InvalidCatalogRecord;
    defer existing.deinit(a);
    const proof = try initialProof(plan, digest);
    if (!std.mem.eql(u8, &existing.plan, &plan) or !std.mem.eql(u8, &existing.digest, &digest)) return error.InvalidCatalogRecord;
    var buffer: [192]u8 = undefined;
    for (existing.ids) |id| {
        const key = try initialEdgeKey(&buffer, group, id, table);
        if (!std.mem.eql(u8, try txn.get(key), &proof)) return error.InvalidCatalogRecord;
        try txn.delete(key);
    }
    try txn.delete(try initialTableKey(&buffer, group, table));
}
pub fn firstDependent(txn: anytype, group: u64, id: u64) !?u64 {
    const ordinary = try firstTableDependent(txn, group, id);
    var buffer: [192]u8 = undefined;
    const prefix = try initialPrefix(&buffer, group, id);
    var cursor = try txn.openCursor();
    defer cursor.close();
    const row = (try cursor.seekAtOrAfter(prefix)) orelse return ordinary;
    if (!std.mem.startsWith(u8, row.key, prefix)) return ordinary;
    const edge = (try initialEdgeFromKey(row.key, group)) orelse return error.InvalidCatalogRecord;
    if (row.value.len != 48) return error.InvalidCatalogRecord;
    return if (ordinary) |table| @min(table, edge.table) else edge.table;
}

/// Immutable activation preparation, not an authority or a paginated partial
/// membership proof. Read at most one excess edge before rejecting oversized
/// cohorts; ordinary point mutations keep their O(1) firstDependent path.
pub const max_dependents = 128;
pub fn dependentsAlloc(a: A, txn: anytype, group: u64, id: u64, limit: usize) ![]u64 {
    if (id == 0 or limit == 0 or limit > max_dependents) return error.InvalidCatalogMutation;
    var result: std.ArrayList(u64) = .empty;
    defer result.deinit(a);
    for ([_]bool{ false, true }) |initial| {
        var buffer: [192]u8 = undefined;
        const prefix = if (initial) try initialPrefix(&buffer, group, id) else try prefixKey(&buffer, group, id);
        var cursor = try txn.openCursor();
        defer cursor.close();
        var row = try cursor.seekAtOrAfter(prefix);
        var previous: u64 = 0;
        while (row) |item| : (row = try cursor.next()) {
            if (!std.mem.startsWith(u8, item.key, prefix)) break;
            if (item.key.len != prefix.len + 8 or (if (initial) item.value.len != 48 else !std.mem.eql(u8, item.value, &.{1}))) return error.InvalidCatalogRecord;
            const table = std.mem.readInt(u64, item.key[prefix.len..][0..8], .big);
            if (table <= previous) return error.InvalidCatalogRecord;
            previous = table;
            if (std.mem.indexOfScalar(u64, result.items, table) != null) continue;
            if (result.items.len == limit) return error.SqlProgramLimitExceeded;
            try result.append(a, table);
        }
    }
    std.mem.sort(u64, result.items, {}, std.sort.asc(u64));
    return result.toOwnedSlice(a);
}

/// Canonical reverse-edge decoding for independent snapshot cardinality checks.
pub fn edgeFromKey(bytes: []const u8, group: u64) !?struct { collation: u64, table: u64 } {
    var buffer: [192]u8 = undefined;
    const base = try std.fmt.bufPrint(&buffer, "\x00\x00__metadata__:system_catalog:{d}:collation-dependent:", .{group});
    if (!std.mem.startsWith(u8, bytes, base)) return null;
    const tail = bytes[base.len..];
    const colon = std.mem.indexOfScalar(u8, tail, ':') orelse return error.InvalidCatalogRecord;
    if (colon == 0 or tail.len != colon + 1 + 8) return error.InvalidCatalogRecord;
    const collation = std.fmt.parseInt(u64, tail[0..colon], 10) catch return error.InvalidCatalogRecord;
    const table = std.mem.readInt(u64, tail[colon + 1 ..][0..8], .big);
    if (collation == 0 or table == 0 or !std.mem.eql(u8, bytes, try edgeKey(&buffer, group, collation, table))) return error.InvalidCatalogRecord;
    return .{ .collation = collation, .table = table };
}

test "SQL ICU native dependency directory commits precise forward and reverse membership" {
    const Mock = struct {
        const Row = struct { key: []const u8, value: []const u8 };
        const Cursor = struct {
            a: A,
            rows: []Row,
            index: usize = 0,
            pub fn close(self: *@This()) void {
                self.a.free(self.rows);
            }
            pub fn seekAtOrAfter(self: *@This(), key: []const u8) !?Row {
                self.index = 0;
                while (self.index < self.rows.len and std.mem.order(u8, self.rows[self.index].key, key) == .lt) self.index += 1;
                return if (self.index < self.rows.len) self.rows[self.index] else null;
            }
            pub fn next(self: *@This()) !?Row {
                self.index += 1;
                return if (self.index < self.rows.len) self.rows[self.index] else null;
            }
        };
        a: A,
        rows: std.StringHashMapUnmanaged([]u8) = .empty,
        pub fn get(self: *@This(), key: []const u8) anyerror![]const u8 {
            return self.rows.get(key) orelse error.NotFound;
        }
        pub fn put(self: *@This(), key: []const u8, value: []const u8) !void {
            const owned_value = try self.a.dupe(u8, value);
            errdefer self.a.free(owned_value);
            const result = try self.rows.getOrPut(self.a, key);
            if (result.found_existing) self.a.free(result.value_ptr.*) else result.key_ptr.* = try self.a.dupe(u8, key);
            result.value_ptr.* = owned_value;
        }
        pub fn delete(self: *@This(), key: []const u8) !void {
            if (self.rows.fetchRemove(key)) |entry| {
                self.a.free(entry.key);
                self.a.free(entry.value);
            }
        }
        pub fn openCursor(self: *@This()) !Cursor {
            const rows = try self.a.alloc(Row, self.rows.count());
            var iterator = self.rows.iterator();
            var i: usize = 0;
            while (iterator.next()) |entry| : (i += 1) rows[i] = .{ .key = entry.key_ptr.*, .value = entry.value_ptr.* };
            std.mem.sort(Row, rows, {}, struct {
                fn less(_: void, l: Row, r: Row) bool {
                    return std.mem.order(u8, l.key, r.key) == .lt;
                }
            }.less);
            return .{ .a = self.a, .rows = rows };
        }
        fn deinit(self: *@This()) void {
            var iterator = self.rows.iterator();
            while (iterator.next()) |entry| {
                self.a.free(entry.key_ptr.*);
                self.a.free(entry.value_ptr.*);
            }
            self.rows.deinit(self.a);
        }
    };
    const a = std.testing.allocator;
    var txn: Mock = .{ .a = a };
    defer txn.deinit();
    try synchronize(a, &txn, 7, 100, &.{ 9, 12 });
    try synchronize(a, &txn, 7, 101, &.{9});
    try std.testing.expectEqual(@as(?u64, 100), try firstDependent(&txn, 7, 9));
    const ids = try dependentsAlloc(a, &txn, 7, 9, max_dependents);
    defer a.free(ids);
    try std.testing.expectEqualSlices(u64, &.{ 100, 101 }, ids);
    try std.testing.expectError(error.SqlProgramLimitExceeded, dependentsAlloc(a, &txn, 7, 9, 1));
    try synchronize(a, &txn, 7, 100, &.{12});
    try std.testing.expectEqual(@as(?u64, 101), try firstDependent(&txn, 7, 9));
    try synchronize(a, &txn, 7, 101, &.{});
    try std.testing.expectEqual(@as(?u64, null), try firstDependent(&txn, 7, 9));
    try reserveInitial(a, &txn, 7, 200, @splat(3), @splat(4), &.{9});
    try std.testing.expectEqual(@as(?u64, 200), try firstDependent(&txn, 7, 9));
    try reserveInitial(a, &txn, 7, 200, @splat(3), @splat(4), &.{9});
    try std.testing.expectError(error.InvalidCatalogRecord, releaseInitial(a, &txn, 7, 200, @splat(3), @splat(5)));
    try releaseInitial(a, &txn, 7, 200, @splat(3), @splat(4));
    try std.testing.expectEqual(@as(?u64, null), try firstDependent(&txn, 7, 9));
    var buffer: [192]u8 = undefined;
    const edge = (try edgeFromKey(try edgeKey(&buffer, 7, 12, 100), 7)).?;
    try std.testing.expectEqual(@as(u64, 12), edge.collation);
    try std.testing.expectEqual(@as(u64, 100), edge.table);
    try std.testing.expectError(error.InvalidCatalogRecord, edgeFromKey("\x00\x00__metadata__:system_catalog:7:collation-dependent:012:garbage!", 7));
}
