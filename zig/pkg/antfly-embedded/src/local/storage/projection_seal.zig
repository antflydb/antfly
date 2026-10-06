// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Physical index replay coverage, stored with the index's own catalog.
//! This is not producer acceptance or a primary-store completion certificate.
const std = @import("std");

pub const metadata_key = "physical_projection_seal";
pub const Seal = struct {
    root: u128,
    namespace: [24]u8,
    generation: u64,
    config_hash: u64,
    applied_sequence: u64,
    /// Issued only by complete snapshot construction plus fenced replay, never
    /// by ordinary incremental replay or a clean external sidecar.
    baseline: ?@import("db/artifact_source_gap.zig").Guard = null,
    pub const Encoded = [254]u8;

    pub fn sameIdentity(self: Seal, other: Seal) bool {
        return self.root == other.root and std.mem.eql(u8, &self.namespace, &other.namespace) and
            self.generation == other.generation and self.config_hash == other.config_hash;
    }

    pub fn encode(self: Seal) !Encoded {
        if (self.root == 0 or self.generation == 0 or self.applied_sequence == std.math.maxInt(u64)) return error.InvalidProjectionSeal;
        for (0..3) |part| if (std.mem.readInt(u64, self.namespace[part * 8 ..][0..8], .big) == 0) return error.InvalidProjectionSeal;
        if (self.baseline) |guard| {
            const boundary = guard.boundary orelse return error.InvalidProjectionSeal;
            if (!std.mem.eql(u8, &boundary.authority.namespace, &self.namespace) or self.applied_sequence < boundary.replay_sequence) return error.InvalidProjectionSeal;
        }
        var raw: Encoded = @splat(0);
        @memcpy(raw[0..4], "APS2");
        std.mem.writeInt(u128, raw[4..20], self.root, .little);
        @memcpy(raw[20..44], &self.namespace);
        std.mem.writeInt(u64, raw[44..52], self.generation, .little);
        std.mem.writeInt(u64, raw[52..60], self.config_hash, .little);
        std.mem.writeInt(u64, raw[60..68], self.applied_sequence, .little);
        raw[68] = @intFromBool(self.baseline != null);
        if (self.baseline) |guard| @memcpy(raw[69..222], &try guard.encode());
        std.crypto.hash.sha2.Sha256.hash(raw[0..222], raw[222..254], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !Seal {
        if (raw.len != @sizeOf(Encoded) or !std.mem.eql(u8, raw[0..4], "APS2") or raw[68] > 1 or (raw[68] == 0 and !std.mem.allEqual(u8, raw[69..222], 0))) return error.InvalidProjectionSeal;
        const result: Seal = .{ .root = std.mem.readInt(u128, raw[4..20], .little), .namespace = raw[20..44].*, .generation = std.mem.readInt(u64, raw[44..52], .little), .config_hash = std.mem.readInt(u64, raw[52..60], .little), .applied_sequence = std.mem.readInt(u64, raw[60..68], .little), .baseline = if (raw[68] == 1) @import("db/artifact_source_gap.zig").Guard.decode(raw[69..222]) catch return error.InvalidProjectionSeal else null };
        if (!std.mem.eql(u8, raw, &try result.encode())) return error.InvalidProjectionSeal;
        return result;
    }
};

test "ordered artifact inventory physical projection seal authenticates identity and replay cut" {
    const seal: Seal = .{ .root = 1, .namespace = @splat(1), .generation = 2, .config_hash = 3, .applied_sequence = 4 };
    const encoded = try seal.encode();
    try std.testing.expectEqualDeep(seal, try Seal.decode(&encoded));
    for (0..encoded.len) |i| {
        var corrupt = encoded;
        corrupt[i] ^= 1;
        try std.testing.expectError(error.InvalidProjectionSeal, Seal.decode(&corrupt));
        try std.testing.expectError(error.InvalidProjectionSeal, Seal.decode(encoded[0..i]));
    }
    var changed = seal;
    changed.applied_sequence += 1;
    try std.testing.expect(seal.sameIdentity(changed));
    changed.root += 1;
    try std.testing.expect(!seal.sameIdentity(changed));
    changed = seal;
    changed.generation = 0;
    try std.testing.expectError(error.InvalidProjectionSeal, changed.encode());
    changed = seal;
    changed.baseline = .{ .boundary = .{ .authority = .{ .namespace = seal.namespace, .epoch = 1, .catalog_digest = @splat(2) }, .replay_sequence = 3 }, .gap_epoch = 0 };
    try std.testing.expectEqualDeep(changed, try Seal.decode(&try changed.encode()));
    changed.baseline.?.boundary.?.replay_sequence = 5;
    try std.testing.expectError(error.InvalidProjectionSeal, changed.encode());
    changed.baseline.?.boundary = null;
    try std.testing.expectError(error.InvalidProjectionSeal, changed.encode());
}
