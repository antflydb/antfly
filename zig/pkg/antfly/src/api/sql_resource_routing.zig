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

// HTTP SQL handles carry their immutable issuer, independently of transactions.
const std = @import("std");
const transactions = @import("transactions.zig");
pub const Kind = enum { prepared, connection };
pub fn encode(issuer: u64, entropy: [16]u8) [32]u8 {
    var bytes = entropy;
    std.mem.writeInt(u64, bytes[0..8], issuer, .big);
    return std.fmt.bytesToHex(bytes, .lower);
}
pub fn owner(id: []const u8) !u64 {
    if (id.len != 32) return error.InvalidArgument;
    var bytes: [16]u8 = undefined;
    _ = try std.fmt.hexToBytes(&bytes, id);
    return std.mem.readInt(u64, bytes[0..8], .big);
}
fn owns(txn: anytype, alloc: std.mem.Allocator, id: []const u8, kind: Kind, local_node_id: u64, bounded: bool) !bool {
    return switch (kind) {
        .prepared => ownsKey(txn, alloc, &(try @import("sql_prepared.zig").resourceKey(id)), id, local_node_id, @import("sql_prepared.zig").max_resource_bytes, bounded),
        .connection => ownsKey(txn, alloc, &(try @import("sql_connection_record.zig").key(id)), id, local_node_id, @import("sql_connection_record.zig").max_record_bytes, bounded),
    };
}

const id_field = "{\"id\":\"";
const owner_field = "\",\"owner_node_id\":";
const owner_offset = id_field.len + 32 + owner_field.len;

fn ownsKey(txn: anytype, alloc: std.mem.Allocator, key: []const u8, id: []const u8, node: u64, limit: usize, bounded: bool) !bool {
    if (bounded) {
        var scope = try txn.openReadScope(alloc);
        defer scope.close();
        const prefix = scope.probePrefix(key, owner_offset + 21) catch |err| switch (err) {
            error.NotFound => return false,
            error.EndOfStream => return error.Unsupported,
            else => return err,
        };
        if (prefix.len <= owner_offset or !std.mem.startsWith(u8, prefix, id_field) or !std.mem.eql(u8, prefix[id_field.len + 32 .. owner_offset], owner_field))
            return error.Unsupported;
        const end = std.mem.indexOfScalarPos(u8, prefix, owner_offset, ',') orelse return error.InvalidSqlConnection;
        const owner_node_id = std.fmt.parseUnsigned(u64, prefix[owner_offset..end], 10) catch return error.InvalidSqlConnection;
        return std.mem.eql(u8, prefix[id_field.len..][0..32], id) and owner_node_id == node;
    }
    const raw = txn.get(key) catch |err| switch (err) {
        error.NotFound => return false,
        else => return err,
    };
    if (raw.len > limit) return error.SqlProgramLimitExceeded;
    // Read only routing metadata. Authentication, expiry and resource validation
    // remain the responsibility of the normal load on the selected issuer.
    const Ownership = struct { id: [32]u8, owner_node_id: u64 };
    var parsed = try std.json.parseFromSlice(Ownership, alloc, raw, .{ .ignore_unknown_fields = true });
    defer parsed.deinit();
    return std.mem.eql(u8, &parsed.value.id, id) and parsed.value.owner_node_id == node;
}
// Legacy random handles may override their encoded issuer only when the
// persisted record is owned here. Shared-store visibility does not imply ownership.
pub fn ownsLocal(store: *transactions.DurableSessionStore, id: []const u8, kind: Kind, local_node_id: u64) !bool {
    return readOwnership(store, id, kind, local_node_id, false);
}

pub fn probeLocalOwner(store: *transactions.DurableSessionStore, id: []const u8, kind: Kind, local_node_id: u64) !bool {
    return readOwnership(store, id, kind, local_node_id, true);
}

fn readOwnership(store: *transactions.DurableSessionStore, id: []const u8, kind: Kind, local_node_id: u64, bounded: bool) !bool {
    return switch (store.backend) {
        .docstore => |backend| blk: {
            var txn = try backend.beginReadTxn();
            defer txn.abort();
            break :blk try owns(&txn, store.alloc, id, kind, local_node_id, bounded);
        },
        .runtime => |backend| blk: {
            var txn = try backend.beginRead();
            defer txn.abort();
            break :blk try owns(&txn, store.alloc, id, kind, local_node_id, bounded);
        },
    };
}
test "SQL pgwire native HTTP handles bind issuer and preserve entropy" {
    const id = encode(7, @splat(0xaa));
    try std.testing.expectEqual(@as(u64, 7), try owner(&id));
    try std.testing.expectEqualStrings("aaaaaaaaaaaaaaaa", id[16..]);
    try std.testing.expectError(error.InvalidArgument, owner("bad"));
}

test "HTTP connection record ownership probe reads only routing fields" {
    const a = std.testing.allocator;
    const id: [32]u8 = @splat('a');
    const Fixture = struct {
        raw: []const u8,
        fn openReadScope(self: *@This(), _: std.mem.Allocator) !*@This() {
            return self;
        }
        fn close(_: *@This()) void {}
        fn probePrefix(self: *@This(), _: []const u8, length: usize) ![]const u8 {
            return self.raw[0..length];
        }
        fn get(self: *@This(), _: []const u8) ![]const u8 {
            return self.raw;
        }
    };
    const record: @import("sql_connection_record.zig").Record = .{
        .id = id,
        .owner_node_id = 7,
        .principal = "alice",
        .expires_at_ms = 1000,
        .database = "main",
        .namespace = "public",
    };
    const modern = try std.json.Stringify.valueAlloc(a, record, .{});
    defer a.free(modern);
    var fixture: Fixture = .{ .raw = modern };
    try std.testing.expect(try ownsKey(&fixture, a, "key", &id, 7, 0, true));
    try std.testing.expect(!try ownsKey(&fixture, a, "key", &id, 8, 0, true));
    const legacy = try std.json.Stringify.valueAlloc(a, .{ .id = id, .principal = "alice", .owner_node_id = @as(u64, 7), .database = "main", .namespace = "public" }, .{});
    defer a.free(legacy);
    fixture.raw = legacy;
    try std.testing.expectError(error.Unsupported, ownsKey(&fixture, a, "key", &id, 7, legacy.len, true));
    try std.testing.expect(try ownsKey(&fixture, a, "key", &id, 7, legacy.len, false));
}
