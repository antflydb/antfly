// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! The initialized hidden image owns this marker. It distinguishes a retry of
//! the same reserved image attempt from a later process replacing that image.
//! It contains no source data and grants no metadata decision or row access.
const std = @import("std");
const persistence = @import("../../common/relation_persistence.zig");
pub const key = "\x00\x00__metadata__:native_empty_image_attempt";
pub const Record = struct {
    scope: [32]u8,
    descriptor_fingerprint: [32]u8,
    number: u64,
    digest: [32]u8,
    physical_root_incarnation: u128,
    pub const encoded_len = 4 + 32 * 3 + 8 + 16 + 32;
    pub fn validate(self: Record) !void {
        if (self.number < 2 or self.physical_root_incarnation == 0 or std.mem.allEqual(u8, &self.scope, 0) or
            std.mem.allEqual(u8, &self.descriptor_fingerprint, 0) or std.mem.allEqual(u8, &self.digest, 0)) return error.InvalidSqlRelationPersistence;
    }
    pub fn encode(self: Record) ![encoded_len]u8 {
        try self.validate();
        var out: [encoded_len]u8 = undefined;
        @memcpy(out[0..4], "NEA1");
        @memcpy(out[4..36], &self.scope);
        @memcpy(out[36..68], &self.descriptor_fingerprint);
        std.mem.writeInt(u64, out[68..76], self.number, .little);
        @memcpy(out[76..108], &self.digest);
        std.mem.writeInt(u128, out[108..124], self.physical_root_incarnation, .little);
        std.crypto.hash.Blake3.hash(out[0..124], out[124..156], .{});
        return out;
    }
    pub fn decode(bytes: []const u8) !Record {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "NEA1")) return error.InvalidSqlRelationPersistence;
        var checksum: [32]u8 = undefined;
        std.crypto.hash.Blake3.hash(bytes[0..124], &checksum, .{});
        if (!std.mem.eql(u8, &checksum, bytes[124..156])) return error.InvalidSqlRelationPersistence;
        const out: Record = .{ .scope = bytes[4..36].*, .descriptor_fingerprint = bytes[36..68].*, .number = std.mem.readInt(u64, bytes[68..76], .little), .digest = bytes[76..108].*, .physical_root_incarnation = std.mem.readInt(u128, bytes[108..124], .little) };
        try out.validate();
        return out;
    }
    pub fn require(self: Record, descriptor: persistence.Descriptor, scope: [32]u8, number: u64, digest: [32]u8, root: u128) !void {
        try self.validate();
        if (self.number != number or self.physical_root_incarnation != root or !std.mem.eql(u8, &self.scope, &scope) or
            !std.mem.eql(u8, &self.digest, &digest) or !std.mem.eql(u8, &self.descriptor_fingerprint, &try descriptor.fingerprint())) return error.SqlRelationOwnerChanged;
    }
};
pub fn read(reader: anytype) !?Record {
    const bytes = reader.get(key) catch |err| return if (err == error.NotFound) null else err;
    return try Record.decode(bytes);
}
pub fn stage(txn: anytype, value: Record) !void {
    const encoded = try value.encode();
    if (try read(txn)) |before| {
        if (!std.meta.eql(before, value)) return error.SqlRelationOwnerChanged;
        return;
    }
    try txn.put(key, &encoded);
}

test "native empty attempt marker checks full width image identity and rejects stale or corrupt retries" {
    const descriptor: persistence.Descriptor = .{ .kind = .unlogged, .owner_node_id = 9, .owner_store_id = 11, .generation_instance = @splat(1), .data_generation = 3 };
    const value: Record = .{ .scope = @splat(1), .descriptor_fingerprint = try descriptor.fingerprint(), .number = 2, .digest = @splat(2), .physical_root_incarnation = std.math.maxInt(u128) };
    const encoded = try value.encode();
    const decoded = try Record.decode(&encoded);
    try std.testing.expectEqualDeep(value, decoded);
    try decoded.require(descriptor, value.scope, value.number, value.digest, value.physical_root_incarnation);
    try std.testing.expectError(error.SqlRelationOwnerChanged, decoded.require(descriptor, value.scope, 3, value.digest, value.physical_root_incarnation));
    var corrupt = encoded;
    corrupt[115] ^= 1;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, Record.decode(&corrupt));
}
