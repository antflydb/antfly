// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Why a hidden native target is known empty. A schema-only physical snapshot
//! asserts no source data identity; only lost_source carries a genuine Pin and
//! metadata loss checkpoint. Neither origin is an owner transaction decision.
const std = @import("std");
const persistence = @import("../../common/relation_persistence.zig");
pub const Digest = [32]u8;
pub const Kind = enum(u8) { lost_source = 1, schema_only_snapshot = 2 };
pub const Origin = union(Kind) {
    lost_source: struct { checkpoint_digest: Digest, source: persistence.PhysicalPin },
    schema_only_snapshot: struct { manifest_digest: Digest, schema_digest: Digest, source_table_id: u64 },

    pub fn validate(self: Origin) !void {
        switch (self) {
            .lost_source => |loss| {
                try loss.source.validate();
                if (std.mem.allEqual(u8, &loss.checkpoint_digest, 0)) return error.InvalidRestoreStagingCommand;
            },
            .schema_only_snapshot => |snapshot| {
                if (snapshot.source_table_id == 0 or std.mem.allEqual(u8, &snapshot.manifest_digest, 0) or
                    std.mem.allEqual(u8, &snapshot.schema_digest, 0)) return error.InvalidRestoreStagingCommand;
            },
        }
    }
    pub fn kind(self: Origin) Kind {
        return std.meta.activeTag(self);
    }
    /// Scope hashes remain total for untrusted values; admission separately
    /// validates every fact before granting initialization or installation.
    pub fn digestUnchecked(self: Origin) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/native-empty-target-origin/v1\x00");
        hash.update(&.{@backingInt(self.kind())});
        var number: [16]u8 = undefined;
        switch (self) {
            .lost_source => |loss| {
                hash.update(&loss.checkpoint_digest);
                std.mem.writeInt(u128, &number, loss.source.physical_root_incarnation, .little);
                hash.update(&number);
                inline for (.{ loss.source.table_id, loss.source.shard_id, loss.source.range_id, loss.source.data_generation }) |value| {
                    std.mem.writeInt(u64, number[0..8], value, .little);
                    hash.update(number[0..8]);
                }
            },
            .schema_only_snapshot => |snapshot| {
                hash.update(&snapshot.manifest_digest);
                hash.update(&snapshot.schema_digest);
                std.mem.writeInt(u64, number[0..8], snapshot.source_table_id, .little);
                hash.update(number[0..8]);
            },
        }
        var result: Digest = undefined;
        hash.final(&result);
        return result;
    }
    pub fn digest(self: Origin) !Digest {
        try self.validate();
        return self.digestUnchecked();
    }
};

test "native empty origins distinguish genuine loss from schema-only snapshot without a synthetic source pin" {
    const loss: Origin = .{ .lost_source = .{ .checkpoint_digest = @splat(1), .source = .{ .physical_root_incarnation = std.math.maxInt(u128), .table_id = 2, .shard_id = 3, .range_id = 4, .data_generation = 5 } } };
    const schema: Origin = .{ .schema_only_snapshot = .{ .manifest_digest = @splat(1), .schema_digest = @splat(2), .source_table_id = 2 } };
    try std.testing.expect(!std.mem.eql(u8, &try loss.digest(), &try schema.digest()));
    var invalid = loss;
    invalid.lost_source.source.physical_root_incarnation = 0;
    try std.testing.expectError(error.InvalidSqlRelationPersistence, invalid.validate());
    var changed = schema;
    changed.schema_only_snapshot.schema_digest[0] ^= 1;
    try std.testing.expect(!std.mem.eql(u8, &try schema.digest(), &try changed.digest()));
}
