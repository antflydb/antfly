// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Elastic-2.0
//
// Licensed under the Elastic License 2.0 (ELv2); you may not use this file
// except in compliance with the Elastic License 2.0. You may obtain a copy of
// the Elastic License 2.0 at
//
//     https://www.antfly.io/licensing/ELv2-license
//
// Unless required by applicable law or agreed to in writing, software distributed
// under the Elastic License 2.0 is distributed on an "AS IS" BASIS, WITHOUT
// WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
// Elastic License 2.0 for the specific language governing permissions and
// limitations.

//! Physical evidence for native validation owners, not publication authority.
//! The caller obtains the expected binding/root scope independently from the
//! metadata owner; metadata rechecks that authority when accepting evidence.
const std = @import("std");
const local = @import("antfly_local_sources");
const native = local.storage_db_relational_schema_owner_control;
const job = local.storage_db_relational_nullability_job;
const roots = @import("store_root_attestation.zig");
const Digest = [32]u8;

pub const Proof = roots.ProofFor(
    "antfly/schema-validation-native-observation/v1",
    "antfly/schema-validation-native-accepted-root/v1",
);
pub const RootedObservation = struct {
    root_generation: u64,
    observation: native.Observation,
};

fn hashValue(comptime domain: []const u8, value: anytype) !Digest {
    var state = std.crypto.hash.Blake3.init(.{});
    state.update(domain);
    var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(state, &.{});
    try std.json.Stringify.value(value, .{}, &writer.writer);
    var result: Digest = undefined;
    writer.hasher.final(&result);
    return result;
}

pub fn bindingDigest(binding: job.Binding) !Digest {
    try binding.validate();
    return hashValue("antfly/schema-validation-owner-binding/v1", binding);
}

/// A permission read's expected physical scope, not a caller-set grant.
/// In particular the physical DB root is distinct from its enclosing signing
/// root and from a runtime cache generation; all three must agree independently.
pub const NativeScope = struct {
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    binding_digest: Digest,
    signing: roots.NativeView,
    root_generation: u64,
    physical_root_incarnation: u128,

    pub fn validate(self: @This()) !void {
        try self.signing.validate(self.metadata_incarnation);
        if (self.root_generation == 0 or self.physical_root_incarnation == 0 or
            std.mem.allEqual(u8, &self.binding_digest, 0)) return error.InvalidCatalogRecord;
    }
    pub fn requireBinding(self: @This(), binding: job.Binding) !void {
        try self.validate();
        if (binding.authority != .native or !std.mem.eql(u8, &self.binding_digest, &try bindingDigest(binding)))
            return error.SchemaValidationJobChanged;
    }
    pub fn require(self: @This(), observed: RootedObservation) !void {
        try self.requireBinding(observed.observation.binding);
        try observed.observation.validateFor(observed.observation.binding);
        if (observed.root_generation != self.root_generation or
            observed.observation.physical_root_incarnation != self.physical_root_incarnation)
            return error.SchemaValidationRuntimeRecoveryRequired;
    }
};

pub const SignedNativeObservation = struct {
    pub const max_wire_bytes = 48 * 1024;
    rooted: RootedObservation,
    proof: Proof,

    fn requireSignable(observed: RootedObservation) !void {
        try observed.observation.validateFor(observed.observation.binding);
        if (observed.observation.header == null) return error.SchemaValidationJobRequired;
        if (observed.observation.replication_pending) return error.ReplicationPublisherUnavailable;
    }
    /// Stable logical progress is separate from physical re-attestation. A
    /// later actual native clock cannot become a new logical phase receipt.
    pub fn logicalDigest(self: @This()) !Digest {
        try requireSignable(self.rooted);
        return hashValue("antfly/schema-validation-logical-state/v1", self.rooted.observation.header.?);
    }
    pub fn evidenceDigest(self: @This()) !Digest {
        try requireSignable(self.rooted);
        return hashValue("antfly/schema-validation-observed-state/v1", self.rooted.observation);
    }
    pub fn sign(observed: RootedObservation, expected: NativeScope, root: local.storage_db_root_signing_identity.State) !@This() {
        try expected.require(observed);
        try requireSignable(observed);
        var result: @This() = .{
            .rooted = observed,
            .proof = .{
                .identity = expected.signing.identity,
                .reporter_incarnation = expected.signing.reporter_incarnation,
                .authority = .{ .native = expected.signing.binding_digest },
                .root_generation = observed.root_generation,
                .signature = undefined,
            },
        };
        result.proof = try result.proof.sign(root, try result.evidenceDigest());
        return result;
    }
    pub fn verify(self: @This(), expected: NativeScope) !void {
        try expected.require(self.rooted);
        try requireSignable(self.rooted);
        if (!std.meta.eql(self.proof.identity, expected.signing.identity) or
            self.proof.reporter_incarnation != expected.signing.reporter_incarnation or
            self.proof.root_generation != self.rooted.root_generation or self.proof.authority != .native or
            !std.mem.eql(u8, &self.proof.authority.native, &expected.signing.binding_digest))
            return error.SchemaValidationJobChanged;
        try self.proof.verify(try self.evidenceDigest());
    }
    pub fn commitment(self: @This()) !Digest {
        return self.proof.commitment(try self.evidenceDigest());
    }
};

/// A pending observation is useful progress but never carries a signature.
pub const Result = union(enum) {
    pending: RootedObservation,
    signed: SignedNativeObservation,
};

const Fixture = struct {
    fn observed() RootedObservation {
        const binding: job.Binding = .{
            .id = @splat(1),
            .plan_digest = @splat(2),
            .owner = @splat(3),
            .fence = .{ .transition_id = 4, .attempt = 1, .admission_epoch = 1, .owner_group_id = 72, .peer_group_id = 72, .namespace = .{ .table_id = 7, .shard_id = 172, .range_id = 272 }, .catalog_digest = @splat(5), .role = .schema_validation },
            .authority = .native,
            .before_version = 1,
            .after_version = 2,
            .columns = 1,
            .before_schema = @splat(6),
            .after_schema = @splat(7),
            .before_public = @splat(8),
            .after_public = @splat(9),
            .after_catalog = @splat(10),
            .physical_catalog = @splat(11),
            .inputs = @splat(12),
            .projection_identity = @splat(13),
        };
        const cut = local.storage_db_receipt_position.Position{ .native = .{ .namespace = binding.fence.namespace, .sequence = 1 } };
        return .{ .root_generation = 6, .observation = .{
            .binding = binding,
            .physical_root_incarnation = 17,
            .sequence = 1,
            .replication_pending = false,
            .header = .{ .binding = binding, .phase = .fencing, .admitted = cut, .updated = cut, .cursor_digest = job.hash(""), .chain = @splat(15), .last_effect = @splat(16) },
        } };
    }
    fn root() !local.storage_db_root_signing_identity.State {
        const seed: [32]u8 = @splat(9);
        const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
        return .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    }
    fn scope(observation: RootedObservation) !NativeScope {
        const state = try root();
        const identity: @import("store_root_enrollment.zig").Identity = .{
            .metadata_incarnation = "0123456789abcdef0123456789abcdef".*,
            .node_id = 2,
            .store_id = 3,
            .root_incarnation = state.root_incarnation,
            .public_key = state.public_key,
        };
        const binding: @import("standalone_native_owner.zig").Binding = .{
            .metadata_incarnation = identity.metadata_incarnation,
            .node_id = identity.node_id,
            .store_id = identity.store_id,
            .root_incarnation = identity.root_incarnation,
        };
        return .{ .metadata_incarnation = identity.metadata_incarnation, .binding_digest = try bindingDigest(observation.observation.binding), .signing = .{ .identity = identity, .reporter_incarnation = 4, .binding_digest = try binding.digest() }, .root_generation = observation.root_generation, .physical_root_incarnation = observation.observation.physical_root_incarnation };
    }
};

test "schema validation attestation binds the real job root reporter and observation without becoming permission" {
    // Cryptographic contract fixture only: this does not qualify an actual
    // owner scan, metadata admission, enrollment or distributed publication.
    const observed = Fixture.observed();
    const expected = try Fixture.scope(observed);
    const root = try Fixture.root();
    const signed = try SignedNativeObservation.sign(observed, expected, root);
    try signed.verify(expected);
    var late = observed;
    late.observation.sequence += 1;
    const reattested = try SignedNativeObservation.sign(late, expected, root);
    try reattested.verify(expected);
    try std.testing.expectEqualSlices(u8, &try signed.logicalDigest(), &try reattested.logicalDigest());
    try std.testing.expect(!std.mem.eql(u8, &try signed.commitment(), &try reattested.commitment()));
    var forged = signed;
    forged.rooted.observation.sequence += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, forged.verify(expected));
    forged = signed;
    forged.rooted.observation.header.?.chain[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, forged.verify(expected));
    forged = signed;
    forged.proof.reporter_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, forged.verify(expected));
    forged = signed;
    forged.proof.authority = .{ .raft = 42 };
    try std.testing.expectError(error.SchemaValidationJobChanged, forged.verify(expected));
    var stale = expected;
    stale.root_generation = 0;
    try std.testing.expectError(error.InvalidCatalogRecord, signed.verify(stale));
    var unknown_root = observed;
    unknown_root.root_generation = 0;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, SignedNativeObservation.sign(unknown_root, expected, root));
    stale = expected;
    stale.root_generation += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, signed.verify(stale));
    stale = expected;
    stale.physical_root_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, signed.verify(stale));
    stale = expected;
    stale.binding_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, signed.verify(stale));
    stale = expected;
    const other = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(@splat(10));
    stale.signing.identity.public_key = other.public_key.toBytes();
    try std.testing.expectError(error.SchemaValidationJobChanged, signed.verify(stale));
    var missing = observed;
    missing.observation.header = null;
    try std.testing.expectError(error.SchemaValidationJobRequired, SignedNativeObservation.sign(missing, expected, root));
    missing = observed;
    missing.observation.replication_pending = true;
    try std.testing.expectError(error.ReplicationPublisherUnavailable, SignedNativeObservation.sign(missing, expected, root));
    const decoded = blk: {
        const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, signed, .{});
        defer std.testing.allocator.free(encoded);
        try std.testing.expect(encoded.len <= SignedNativeObservation.max_wire_bytes);
        var parsed = try std.json.parseFromSlice(SignedNativeObservation, std.testing.allocator, encoded, .{ .ignore_unknown_fields = false });
        defer parsed.deinit();
        break :blk parsed.value;
    };
    try decoded.verify(expected);
    try std.testing.expectEqualDeep(signed, decoded);
}

test "schema validation attestation cannot replay an index proof in either direction" {
    const observed = Fixture.observed();
    const expected = try Fixture.scope(observed);
    const root = try Fixture.root();
    const signed = try SignedNativeObservation.sign(observed, expected, root);
    const index = @import("relational_index_root_attestation.zig");
    const forged_index: index.Proof = .{
        .identity = signed.proof.identity,
        .reporter_incarnation = signed.proof.reporter_incarnation,
        .authority = signed.proof.authority,
        .root_generation = signed.proof.root_generation,
        .signature = signed.proof.signature,
    };
    try std.testing.expectError(error.InvalidGenerationPublication, forged_index.verify(try signed.evidenceDigest()));
    const index_signed = try forged_index.sign(root, try signed.evidenceDigest());
    var forged_schema = signed;
    forged_schema.proof.signature = index_signed.signature;
    try std.testing.expectError(error.InvalidGenerationPublication, forged_schema.verify(expected));
    try std.testing.expect(!std.mem.eql(u8, &try forged_index.commitment(try signed.evidenceDigest()), &try signed.commitment()));
}
