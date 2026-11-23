// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Nominal pointer-free Source link shared by native P and participant votes.
//! No SQL principal, synthetic table or caller-selected slot grants authority.
const std = @import("std");
pub const Owner = @import("../system_catalog/logical_replication_origin.zig").Owner;
pub const Reference = struct {
    owner: Owner,
    /// Native-assigned immutable Origin birth, separate from transaction G.
    birth: u64,
    generation: u64,
    source_digest: [32]u8,
    event_digest: [32]u8,
    intent_digest: [32]u8,
    manifest_digest: [32]u8,
    txn_id: [16]u8,
    pub fn validate(self: Reference) !void {
        try self.owner.validate();
        if (self.birth == 0 or self.generation == 0 or std.mem.allEqual(u8, &self.txn_id, 0)) return error.InvalidCatalogMutation;
        inline for (.{ "source_digest", "event_digest", "intent_digest", "manifest_digest" }) |field| {
            if (std.mem.allEqual(u8, &@field(self, field), 0)) return error.InvalidCatalogMutation;
        }
    }
    pub fn eql(self: Reference, other: Reference) bool {
        return std.meta.eql(self, other);
    }
};

test "Source transaction reference separates native owner birth and event identity" {
    const original: Reference = .{ .owner = .{ .logical_subscription = .{ .database_id = 3, .subscription_id = 17 } }, .birth = 4, .generation = 1, .source_digest = @splat(1), .event_digest = @splat(2), .intent_digest = @splat(3), .manifest_digest = @splat(4), .txn_id = @splat(5) };
    try original.validate();
    var replacement = original;
    replacement.birth += 1;
    try std.testing.expect(!replacement.eql(original));
    replacement = original;
    replacement.owner = .{ .table_source = .{ .table_id = 17, .source_ordinal = 0 } };
    try replacement.validate();
    try std.testing.expect(!replacement.eql(original));
    replacement = original;
    replacement.birth = 0;
    try std.testing.expectError(error.InvalidCatalogMutation, replacement.validate());
    var buffer: [96]u8 = undefined;
    try std.testing.expectEqualStrings("subscription:3:17", try original.owner.keyPart(&buffer));
}
