// Copyright 2026 Antfly, Inc. SPDX-License-Identifier: Elastic-2.0
//! Local projection lifecycle fence. Physical rebuild/replacement revokes
//! previously checked completion prefixes BEFORE a sidecar transition is
//! exposed. Monotonic progress does not revoke evidence. This is not itself
//! proof that any index is durable, complete, or ready to serve.
const std = @import("std");
const publication = @import("artifact_publication.zig");
pub const key = "\x00\x00__artifact_publication__:projection-lifecycle";
const Encoded = [44]u8;

fn encode(epoch: u64) Encoded {
    var raw: Encoded = undefined;
    @memcpy(raw[0..4], "APE1");
    std.mem.writeInt(u64, raw[4..12], epoch, .little);
    std.crypto.hash.sha2.Sha256.hash(raw[0..12], raw[12..44], .{});
    return raw;
}

pub fn load(txn: anytype) !u64 {
    const raw = txn.get(key) catch |err| if (err == error.NotFound) return 0 else return err;
    if (raw.len != @sizeOf(Encoded) or !std.mem.eql(u8, raw[0..4], "APE1")) return error.ArtifactCatalogCorrupt;
    const epoch = std.mem.readInt(u64, raw[4..12], .little);
    if (epoch == 0 or !std.mem.eql(u8, raw, &encode(epoch))) return error.ArtifactCatalogCorrupt;
    return epoch;
}

pub fn requireCurrent(txn: anytype, expected: u64) !void {
    if (try load(txn) != expected) return error.EnrichmentSourceChanged;
}

/// The caller commits this transaction before replacing/resetting physical
/// projection authority. Failure may leave an extra revocation, never a stale
/// proof. Inactive owners need no metadata or compatibility migration.
pub fn revoke(txn: anytype) !void {
    if (try publication.authority(txn) == null) return;
    const next = std.math.add(u64, try load(txn), 1) catch return error.ResourceLimitExceeded;
    try txn.put(key, &encode(next));
}

test "ordered artifact inventory projection lifecycle epoch rejects corruption" {
    const Fake = struct {
        raw: Encoded,
        pub fn get(self: *@This(), _: []const u8) ![]const u8 {
            return &self.raw;
        }
    };
    const raw = encode(7);
    var fake: Fake = .{ .raw = raw };
    try std.testing.expectEqual(@as(u64, 7), try load(&fake));
    try requireCurrent(&fake, 7);
    try std.testing.expectError(error.EnrichmentSourceChanged, requireCurrent(&fake, 6));
    for (0..raw.len) |offset| {
        fake.raw = raw;
        fake.raw[offset] ^= 1;
        try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&fake));
    }
    fake.raw = encode(0);
    try std.testing.expectError(error.ArtifactCatalogCorrupt, load(&fake));
}
