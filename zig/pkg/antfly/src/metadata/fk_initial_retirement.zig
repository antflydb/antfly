// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//! Bounded, metadata-owned history of hidden initial-FK replica admission.
//! A canceled work item is only a candidate for local retirement. Physical
//! deletion requires a separately proven store-root identity and owner ACK.
const std = @import("std");
const publication = @import("fk_generation_publication.zig");

pub const max_replicas_per_group: u64 = 4096;
pub const encoded_len = 176;

pub const Replica = struct {
    plan_id: publication.Id,
    plan_digest: publication.Digest,
    child_table_id: u64,
    group_id: u64,
    range_id: u64,
    node_id: u64,
    store_id: u64,
    store_incarnation: u64,
    /// Fsynced physical store-root UUID, distinct from process incarnation.
    store_root_incarnation: u128 = 0,
    replica_id: u64,
    root_generation: u64,
    /// Metadata may set this only in the terminal canceled publication txn.
    canceled: bool = false,
    /// This may become true only after an exact store-root UUID is recorded
    /// and an incarnation-fenced physical retirement receipt is verified.
    acked: bool = false,

    pub fn validate(self: Replica) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0) or
            self.child_table_id == 0 or self.group_id == 0 or self.range_id == 0 or self.node_id == 0 or
            self.replica_id == 0 or self.root_generation == 0 or
            (self.store_id != 0 and self.store_root_incarnation == 0) or
            (self.acked and !self.canceled))
            return error.InvalidInitialFkRetirement;
    }

    pub fn encode(self: Replica) ![encoded_len]u8 {
        try self.validate();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "IFRW");
        bytes[4] = 1;
        bytes[5] = @intFromBool(self.canceled);
        bytes[6] = @intFromBool(self.acked);
        @memcpy(bytes[8..24], &self.plan_id);
        @memcpy(bytes[24..56], &self.plan_digest);
        inline for (.{ self.child_table_id, self.group_id, self.range_id, self.node_id, self.store_id, self.store_incarnation, self.replica_id, self.root_generation }, 0..) |value, index| {
            std.mem.writeInt(u64, bytes[56 + index * 8 ..][0..8], value, .little);
        }
        std.mem.writeInt(u128, bytes[120..136], self.store_root_incarnation, .little);
        var checksum: publication.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. encoded_len - 32], &checksum, .{});
        @memcpy(bytes[encoded_len - 32 ..], &checksum);
        return bytes;
    }

    pub fn decode(bytes: []const u8) !Replica {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "IFRW") or bytes[4] != 1 or
            bytes[5] > 1 or bytes[6] > 1 or bytes[7] != 0 or !std.mem.allEqual(u8, bytes[136..144], 0))
            return error.InvalidInitialFkRetirement;
        var checksum: publication.Digest = undefined;
        std.crypto.hash.Blake3.hash(bytes[0 .. encoded_len - 32], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[encoded_len - 32 ..])) return error.InvalidInitialFkRetirement;
        const result: Replica = .{
            .plan_id = bytes[8..24].*,
            .plan_digest = bytes[24..56].*,
            .child_table_id = std.mem.readInt(u64, bytes[56..64], .little),
            .group_id = std.mem.readInt(u64, bytes[64..72], .little),
            .range_id = std.mem.readInt(u64, bytes[72..80], .little),
            .node_id = std.mem.readInt(u64, bytes[80..88], .little),
            .store_id = std.mem.readInt(u64, bytes[88..96], .little),
            .store_incarnation = std.mem.readInt(u64, bytes[96..104], .little),
            .replica_id = std.mem.readInt(u64, bytes[104..112], .little),
            .root_generation = std.mem.readInt(u64, bytes[112..120], .little),
            .store_root_incarnation = std.mem.readInt(u128, bytes[120..136], .little),
            .canceled = bytes[5] == 1,
            .acked = bytes[6] == 1,
        };
        try result.validate();
        return result;
    }
};

pub fn groupPrefix(buf: []u8, metadata_group_id: u64, owner_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica:{d}:{x:0>16}:", .{ metadata_group_id, owner_group_id });
}

pub fn allGroupsPrefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica:{d}:", .{metadata_group_id});
}

pub fn groupKey(buf: []u8, metadata_group_id: u64, replica: Replica) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica:{d}:{x:0>16}:{x:0>16}:{x:0>16}:{x:0>16}:{x:0>16}:{x:0>32}", .{
        metadata_group_id, replica.group_id, replica.node_id, replica.store_id, replica.replica_id, replica.root_generation, replica.store_root_incarnation,
    });
}

pub fn storePrefix(buf: []u8, metadata_group_id: u64, store_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_retire:{d}:{x:0>16}:", .{ metadata_group_id, store_id });
}

pub fn allStoresPrefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_retire:{d}:", .{metadata_group_id});
}

pub fn storeKey(buf: []u8, metadata_group_id: u64, replica: Replica) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_retire:{d}:{x:0>16}:{x:0>16}:{x:0>16}:{x:0>16}:{x:0>16}:{x:0>32}", .{
        metadata_group_id, replica.store_id, replica.group_id, replica.node_id, replica.replica_id, replica.root_generation, replica.store_root_incarnation,
    });
}

pub fn livePrefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica_live:{d}:", .{metadata_group_id});
}

pub fn liveKey(buf: []u8, metadata_group_id: u64, owner_group_id: u64, node_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica_live:{d}:{x:0>16}:{x:0>16}", .{ metadata_group_id, owner_group_id, node_id });
}

pub fn allStoreLiveCountsPrefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica_live_count:{d}:", .{metadata_group_id});
}

pub fn storeLiveCountKey(buf: []u8, metadata_group_id: u64, store_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica_live_count:{d}:{x:0>16}", .{ metadata_group_id, store_id });
}

pub fn groupCountKey(buf: []u8, metadata_group_id: u64, owner_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica_count:{d}:{x:0>16}", .{ metadata_group_id, owner_group_id });
}

pub fn planCountKey(buf: []u8, metadata_group_id: u64, child_table_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica_count:{d}:plan:{x:0>16}", .{ metadata_group_id, child_table_id });
}

pub fn allCountsPrefix(buf: []u8, metadata_group_id: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "\x00\x00__metadata__:fk_initial_replica_count:{d}:", .{metadata_group_id});
}

test "initial FK retired replica work encodes exact incarnation and rejects corruption" {
    const value: Replica = .{
        .plan_id = @splat(1),
        .plan_digest = @splat(2),
        .child_table_id = 11,
        .group_id = 13,
        .range_id = 17,
        .node_id = 19,
        .store_id = 23,
        .store_incarnation = 29,
        .store_root_incarnation = 41,
        .replica_id = 31,
        .root_generation = 37,
    };
    const raw = try value.encode();
    try std.testing.expectEqualDeep(value, try Replica.decode(&raw));
    var canceled = value;
    canceled.canceled = true;
    const terminal = try canceled.encode();
    try std.testing.expectEqualDeep(canceled, try Replica.decode(&terminal));
    var corrupt = terminal;
    corrupt[96] ^= 1;
    try std.testing.expectError(error.InvalidInitialFkRetirement, Replica.decode(&corrupt));
    var key_a: [256]u8 = undefined;
    var key_b: [256]u8 = undefined;
    try std.testing.expect(!std.mem.eql(u8, try groupKey(&key_a, 1, value), try storeKey(&key_b, 1, value)));
}
