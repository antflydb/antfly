// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Fence source changes which snapshot + replay cannot reconstruct. Normal
//! journaled mutations do not invalidate a shadow; unjournaled/ambiguous writes
//! advance this scalar in the SAME transaction as their materialization.
const std = @import("std");
const activation = @import("artifact_activation_boundary.zig");
const publication = @import("artifact_publication.zig");
pub const key = "\x00\x00__artifact_publication__:source-gap";

fn encodeEpoch(value: u64) [44]u8 {
    var raw: [44]u8 = undefined;
    @memcpy(raw[0..4], "ASG1");
    std.mem.writeInt(u64, raw[4..12], value, .little);
    std.crypto.hash.sha2.Sha256.hash(raw[0..12], raw[12..44], .{});
    return raw;
}

pub fn load(txn: anytype) !u64 {
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return 0 else return err;
    if (raw.len != 44 or !std.mem.eql(u8, raw[0..4], "ASG1")) return error.ArtifactCatalogCorrupt;
    const value = std.mem.readInt(u64, raw[4..12], .little);
    if (value == 0 or !std.mem.eql(u8, raw, &encodeEpoch(value))) return error.ArtifactCatalogCorrupt;
    return value;
}

pub fn record(txn: anytype) !void {
    const next = std.math.add(u64, try load(txn), 1) catch return error.ResourceLimitExceeded;
    try txn.put(key, &encodeEpoch(next));
}

pub const Guard = struct {
    boundary: ?activation.Boundary,
    gap_epoch: u64,
    pub const Encoded = [153]u8;

    pub fn capture(txn: anytype) !Guard {
        const active = try publication.authority(txn);
        return .{ .boundary = if (active) |value| try activation.requireAuthority(txn, value) else null, .gap_epoch = try load(txn) };
    }

    pub fn requireCurrent(self: Guard, txn: anytype) !void {
        if (!std.meta.eql(self, try capture(txn))) return error.EnrichmentSourceChanged;
    }

    pub fn encode(self: Guard) !Encoded {
        var raw: Encoded = @splat(0);
        @memcpy(raw[0..4], "ASR1");
        raw[4] = @intFromBool(self.boundary != null);
        if (self.boundary) |value| @memcpy(raw[5..113], &try value.encode());
        std.mem.writeInt(u64, raw[113..121], self.gap_epoch, .little);
        // The outer repair checkpoint also checksums the frame. Retain an
        // independent digest so this proof remains portable between internal
        // candidate manifests without accepting an unchecked counter.
        std.crypto.hash.sha2.Sha256.hash(raw[0..121], raw[121..153], .{});
        return raw;
    }

    pub fn decode(raw: []const u8) !Guard {
        if (raw.len != @sizeOf(Encoded) or !std.mem.eql(u8, raw[0..4], "ASR1") or raw[4] > 1 or
            (raw[4] == 0 and !std.mem.allEqual(u8, raw[5..113], 0))) return error.ArtifactCatalogCorrupt;
        const guard: Guard = .{ .boundary = if (raw[4] == 1) try activation.Boundary.decode(raw[5..113]) else null, .gap_epoch = std.mem.readInt(u64, raw[113..121], .little) };
        if (!std.mem.eql(u8, raw, &try guard.encode())) return error.ArtifactCatalogCorrupt;
        return guard;
    }
};

test "ordered artifact inventory source gap guard binds baseline and unjournaled epoch" {
    const guard: Guard = .{ .boundary = .{ .authority = .{ .namespace = @splat(1), .epoch = 2, .catalog_digest = @splat(3) }, .replay_sequence = 4 }, .gap_epoch = 5 };
    const raw = try guard.encode();
    try std.testing.expectEqualDeep(guard, try Guard.decode(&raw));
    for (0..raw.len) |i| {
        var damaged = raw;
        damaged[i] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Guard.decode(&damaged));
        try std.testing.expectError(error.ArtifactCatalogCorrupt, Guard.decode(raw[0..i]));
    }
    const inactive: Guard = .{ .boundary = null, .gap_epoch = 0 };
    try std.testing.expectEqualDeep(inactive, try Guard.decode(&try inactive.encode()));
}
