// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0

//! Durable initialized/adopted primary facts. Only the native owner may
//! produce the observation; enrollment, Boot and metadata cuts are rejoined
//! independently when the signed postimage is published.
const std = @import("std");
const persistence = @import("antfly_local_sources").common_topology_records.RelationPersistence;
const enrollment = @import("store_root_enrollment.zig");
const signing = @import("antfly_local_sources").storage_db_root_signing_identity;

/// Existing intents may follow the same physical image through a clean
/// process restart. Process/reporter freshness is rejoined separately; it is
/// not part of this immutable root authority.
pub fn nativeAuthorityDigest(identity: enrollment.Identity, pin: persistence.PhysicalPin) ![32]u8 {
    try identity.validate();
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/native-relation/control-root/v1\x00");
    hash.update(&try identity.digest());
    hash.update(&try pin.encode());
    var out: [32]u8 = undefined;
    hash.final(&out);
    return out;
}

pub fn nativeReportDigest(report: anytype) ![32]u8 {
    var hash = std.crypto.hash.Blake3.init(.{});
    hash.update("antfly/native-relation/group-observation/v1\x00");
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hash, &.{});
    try std.json.Stringify.value(report, .{}, &writer.writer);
    var out: [32]u8 = undefined;
    writer.hasher.final(&out);
    return out;
}

pub const CreationScope = struct {
    plan_id: [16]u8,
    plan_digest: [32]u8,

    pub fn validate(self: CreationScope) !void {
        if (std.mem.allEqual(u8, &self.plan_id, 0) or std.mem.allEqual(u8, &self.plan_digest, 0)) return error.InvalidSqlRelationPersistence;
    }
};

pub const SignedBinding = struct {
    version: u8 = 1,
    kind: enum { initialized, adopted },
    metadata_group_id: u64,
    boot_generation: u64,
    identity: enrollment.Identity,
    reporter_incarnation: u64,
    binding: persistence.NativeBinding,
    /// The existing hidden restore/population reservation owns this receipt
    /// before its globally allocated target table becomes publicly visible.
    creation_scope: ?CreationScope = null,
    previous_descriptor_fingerprint: ?[32]u8 = null,
    previous_binding_digest: ?[32]u8 = null,
    signature: [64]u8 = @splat(0),

    pub fn digest(self: SignedBinding) ![32]u8 {
        try self.identity.validate();
        try self.binding.validate();
        if (self.version != 1 or self.metadata_group_id == 0 or self.boot_generation == 0 or self.reporter_incarnation == 0 or
            self.identity.node_id != self.binding.descriptor.owner_node_id or self.identity.store_id != self.binding.descriptor.owner_store_id or
            ((self.kind == .adopted) != (self.previous_descriptor_fingerprint != null)) or
            ((self.kind == .adopted) != (self.previous_binding_digest != null)) or
            (self.kind == .adopted and self.binding.descriptor.kind != .unlogged)) return error.InvalidSqlRelationPersistence;
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/sql-relation-native-durable-binding/v1\x00");
        hash.update(&.{ self.version, @backingInt(self.kind) });
        hash.update(&try self.identity.digest());
        hash.update(&try self.binding.digest());
        hash.update(&.{@intFromBool(self.creation_scope != null)});
        if (self.creation_scope) |scope| {
            try scope.validate();
            hash.update(&scope.plan_id);
            hash.update(&scope.plan_digest);
        }
        var number: [8]u8 = undefined;
        for ([_]u64{ self.metadata_group_id, self.boot_generation, self.reporter_incarnation }) |value| {
            std.mem.writeInt(u64, &number, value, .little);
            hash.update(&number);
        }
        if (self.previous_binding_digest) |previous| {
            if (std.mem.allEqual(u8, &previous, 0)) return error.InvalidSqlRelationPersistence;
            hash.update(&previous);
        }
        if (self.previous_descriptor_fingerprint) |previous| {
            if (std.mem.allEqual(u8, &previous, 0)) return error.InvalidSqlRelationPersistence;
            hash.update(&previous);
        }
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }

    pub fn sign(value: SignedBinding, root: signing.State) !SignedBinding {
        if (value.identity.root_incarnation != root.root_incarnation or !std.mem.eql(u8, &value.identity.public_key, &root.public_key)) return error.SqlRelationOwnerChanged;
        var result = value;
        result.signature = try root.sign(&try value.digest());
        return result;
    }

    pub fn verify(self: SignedBinding) !void {
        signing.verify(self.identity.public_key, &try self.digest(), self.signature) catch return error.InvalidSqlRelationPersistence;
    }
};
