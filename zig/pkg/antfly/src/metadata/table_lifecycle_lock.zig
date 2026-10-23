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

//! Shared table-cut exclusion for coordinated schema lifecycle jobs.
//! The caller stages the lock with its durable job/name reservation in ONE
//! metadata transaction. A lock alone is not publication authority. New kinds
//! require the coordinated decoder barrier and checkpoint/job integration
//! before admission; supporting decoding does not enable a public operation.
const std = @import("std");
pub const Id = [16]u8;
pub const Kind = enum(u8) { generation = 1, initial_support = 2, index_build = 3, schema_validation = 4 };
pub const encoded_len = 17;
const all_prefix = "\x00\x00__metadata__:fk_generation_table_lock:";
pub fn allGroupsPrefix() []const u8 {
    return all_prefix;
}
pub fn groupFromKey(storage_key: []const u8) !?u64 {
    if (!std.mem.startsWith(u8, storage_key, all_prefix)) return null;
    var parts = std.mem.splitScalar(u8, storage_key[all_prefix.len..], ':');
    const group_bytes = parts.next() orelse return error.InvalidCatalogRecord;
    const table_bytes = parts.next() orelse return error.InvalidCatalogRecord;
    if (parts.next() != null) return error.InvalidCatalogRecord;
    const group = std.fmt.parseInt(u64, group_bytes, 10) catch return error.InvalidCatalogRecord;
    const table = std.fmt.parseInt(u64, table_bytes, 10) catch return error.InvalidCatalogRecord;
    var buf: [20]u8 = undefined;
    if (group == 0 or table == 0 or !std.mem.eql(u8, group_bytes, try std.fmt.bufPrint(&buf, "{d}", .{group})) or
        !std.mem.eql(u8, table_bytes, try std.fmt.bufPrint(&buf, "{d}", .{table}))) return error.InvalidCatalogRecord;
    return group;
}

pub fn tableFromKey(storage_key: []const u8) !?u64 {
    if (try groupFromKey(storage_key) == null) return null;
    const split = std.mem.lastIndexOfScalar(u8, storage_key, ':') orelse return error.InvalidCatalogRecord;
    return std.fmt.parseInt(u64, storage_key[split + 1 ..], 10) catch return error.InvalidCatalogRecord;
}

// Keep the original durable namespace: all schema/topology writers must see
// the SAME exclusion key, including databases predating index-build jobs.
pub fn key(buf: []u8, group_id: u64, table_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_generation_table_lock:{d}:{d}", .{ group_id, table_id });
}
pub fn prefix(buf: []u8, group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_generation_table_lock:{d}:", .{group_id});
}

pub fn value(kind: Kind, id: Id) [encoded_len]u8 {
    var bytes: [encoded_len]u8 = undefined;
    bytes[0] = @backingInt(kind);
    @memcpy(bytes[1..], &id);
    return bytes;
}

pub const Lock = struct {
    kind: Kind,
    id: Id,

    pub fn decode(bytes: []const u8) !Lock {
        if (bytes.len != encoded_len or std.mem.allEqual(u8, bytes[1..], 0)) return error.InvalidGenerationPublication;
        return .{ .kind = std.enums.fromInt(Kind, bytes[0]) orelse return error.InvalidGenerationPublication, .id = bytes[1..17].* };
    }

    fn encode(self: Lock) ![encoded_len]u8 {
        if (std.mem.allEqual(u8, &self.id, 0)) return error.InvalidGenerationPublication;
        return value(self.kind, self.id);
    }
};

pub fn read(txn: anytype, group_id: u64, table_id: u64) !?Lock {
    var buf: [160]u8 = undefined;
    const bytes = txn.get(try key(&buf, group_id, table_id)) catch |err| switch (err) {
        error.NotFound => return null,
        else => return err,
    };
    return try Lock.decode(bytes);
}

/// Deliberately not idempotent: retry/replay is decided by the durable job
/// revision, never by guessing that an equal lock means begin was completed.
pub fn acquire(txn: anytype, group_id: u64, table_id: u64, lock: Lock) !void {
    const bytes = try lock.encode();
    if (try read(txn, group_id, table_id) != null) return error.GenerationPublicationChanged;
    var buf: [160]u8 = undefined;
    try txn.put(try key(&buf, group_id, table_id), &bytes);
}

pub fn release(txn: anytype, group_id: u64, table_id: u64, lock: Lock) !void {
    const bytes = try lock.encode();
    var buf: [160]u8 = undefined;
    const storage_key = try key(&buf, group_id, table_id);
    const current = txn.get(storage_key) catch |err| switch (err) {
        error.NotFound => return error.GenerationPublicationChanged,
        else => return err,
    };
    if (!std.mem.eql(u8, current, &bytes)) return error.GenerationPublicationChanged;
    try txn.delete(storage_key);
}

test "table lifecycle lock preserves legacy wire and rejects malformed future identities" {
    const id: Id = @splat(7);
    for ([_]Kind{ .generation, .initial_support, .index_build, .schema_validation }) |kind| {
        const encoded = value(kind, id);
        try std.testing.expectEqual(@backingInt(kind), encoded[0]);
        try std.testing.expectEqualSlices(u8, &id, encoded[1..]);
        const decoded = try Lock.decode(&encoded);
        try std.testing.expectEqual(kind, decoded.kind);
        try std.testing.expectEqualSlices(u8, &id, &decoded.id);
    }
    var buf: [160]u8 = undefined;
    try std.testing.expectEqualStrings("\x00\x00__metadata__:fk_generation_table_lock:7:99", try key(&buf, 7, 99));
    var unknown = value(.index_build, id);
    unknown[0] = 255;
    try std.testing.expectError(error.InvalidGenerationPublication, Lock.decode(&unknown));
    try std.testing.expectError(error.InvalidGenerationPublication, Lock.decode(&value(.index_build, @splat(0))));
    try std.testing.expectError(error.InvalidGenerationPublication, Lock.decode("short"));
}

test "table lifecycle lock point CAS rejects cross kind release and preserves storage failures" {
    const Txn = struct {
        bytes: ?[encoded_len]u8 = null,
        reads: usize = 0,
        writes: usize = 0,
        failed: bool = false,
        fn get(self: *@This(), _: []const u8) ![]const u8 {
            self.reads += 1;
            if (self.failed) return error.StorageFailure;
            return if (self.bytes) |*bytes| bytes else error.NotFound;
        }
        fn put(self: *@This(), _: []const u8, bytes: []const u8) !void {
            self.writes += 1;
            self.bytes = bytes[0..encoded_len].*;
        }
        fn delete(self: *@This(), _: []const u8) !void {
            self.writes += 1;
            self.bytes = null;
        }
    };
    var txn: Txn = .{};
    const lock: Lock = .{ .kind = .index_build, .id = @splat(9) };
    try acquire(&txn, 1, 2, lock);
    try std.testing.expectEqual(@as(usize, 1), txn.reads);
    try std.testing.expectEqual(@as(usize, 1), txn.writes);
    try std.testing.expectError(error.GenerationPublicationChanged, acquire(&txn, 1, 2, lock));
    try std.testing.expectError(error.GenerationPublicationChanged, release(&txn, 1, 2, .{ .kind = .generation, .id = lock.id }));
    try std.testing.expectError(error.GenerationPublicationChanged, release(&txn, 1, 2, .{ .kind = .index_build, .id = @splat(8) }));
    txn.failed = true;
    try std.testing.expectError(error.StorageFailure, acquire(&txn, 1, 2, lock));
    try std.testing.expectError(error.StorageFailure, release(&txn, 1, 2, lock));
    try std.testing.expectEqual(@as(usize, 1), txn.writes);
    txn.failed = false;
    try release(&txn, 1, 2, lock);
    try std.testing.expect(txn.bytes == null);
    try std.testing.expectError(error.GenerationPublicationChanged, release(&txn, 1, 2, lock));
}
