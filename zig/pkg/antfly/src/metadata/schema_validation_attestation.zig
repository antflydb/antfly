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
const job = local.storage_db_relational_schema_owner_contract;
const roots = @import("store_root_attestation.zig");
const Digest = [32]u8;

pub const Proof = roots.ProofFor(
    "antfly/schema-validation-native-observation/v1",
    "antfly/schema-validation-native-accepted-root/v1",
);
pub const RootedObservation = struct {
    root_generation: u64,
    observation: job.NativeObservation,
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

pub const PreparationProof = roots.ProofFor(
    "antfly/schema-validation-preparation/v1",
    "antfly/schema-validation-preparation-accepted-root/v1",
);

/// Bootstrap evidence has its own signature domain. It cannot be replayed as
/// ready/installed evidence and does not adopt a root from a later job ACK.
pub const SignedPreparation = struct {
    pub const max_wire_bytes = 16 * 1024;
    value: job.Preparation,
    proof: PreparationProof,

    pub fn evidenceDigest(self: @This()) !Digest {
        try self.value.validate();
        if (self.value.replication_pending) return error.ReplicationPublisherUnavailable;
        var intent = self.value;
        intent.binding.plan_digest = @splat(0);
        return hashValue("antfly/schema-validation-prepared-physical-cut/v1", intent);
    }
    pub fn sign(value: job.Preparation, identity: @import("store_root_enrollment.zig").Identity, reporter: u64, authority: roots.Authority, generation: u64, root: local.storage_db_root_signing_identity.State) !@This() {
        var result: @This() = .{ .value = value, .proof = .{ .identity = identity, .reporter_incarnation = reporter, .authority = authority, .root_generation = generation, .signature = undefined } };
        if ((value.binding.authority == .native) != (authority == .native)) return error.SchemaValidationJobChanged;
        result.proof = try result.proof.sign(root, try result.evidenceDigest());
        return result;
    }
    pub fn verifyFor(self: @This(), binding: job.Binding, physical_root: u128) !void {
        try self.value.requireFor(binding);
        if (physical_root != self.value.physical_root_incarnation or
            (binding.authority == .native) != (self.proof.authority == .native)) return error.SchemaValidationJobChanged;
        try self.proof.verify(try self.evidenceDigest());
    }
    pub fn commitment(self: @This()) !Digest {
        return self.proof.commitment(try self.evidenceDigest());
    }
};

pub fn logicalHeaderDigest(header: job.Header) !Digest {
    try header.validate();
    return hashValue("antfly/schema-validation-logical-state/v1", header);
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
        return logicalHeaderDigest(self.rooted.observation.header.?);
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

pub const RaftProof = roots.ProofFor(
    "antfly/schema-validation-raft-observation/v1",
    "antfly/schema-validation-raft-accepted-root/v1",
);
pub const RootedRaftObservation = struct {
    root_generation: u64,
    observation: job.RaftObservation,
    /// Captured and quorum-validated by the retained owner's runtime. This is
    /// a completed prefix, NOT the term/index of the last data mutation.
    completed: ?local.storage_read_consistency.ReadSafetyBarrier.FrozenProof = null,

    pub fn validateCompletion(self: @This()) !void {
        const completed = self.completed orelse return;
        const applied = self.observation.applied orelse return error.InvalidControlReceiptPosition;
        if (completed.incarnation == 0 or completed.term == 0 or completed.applied_index == 0 or
            applied.index > completed.applied_index or applied.term > completed.term)
            return error.InvalidControlReceiptPosition;
    }
};

/// Independently resolved enrollment, placement and report facts. This is not
/// a transport grant: the receiving metadata transaction must recheck them.
pub const RaftScope = struct {
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    binding_digest: Digest,
    identity: @import("store_root_enrollment.zig").Identity,
    reporter_incarnation: u64,
    replica_id: u64,
    group_id: u64,
    root_generation: u64,
    physical_root_incarnation: u128,
    /// A report's current election term is NOT the term of its applied log
    /// entry. A new leader may legitimately retain a committed older-term cut.
    reported_term: u64,
    reported_applied_index: u64,

    pub fn validate(self: @This()) !void {
        self.identity.validate() catch return error.InvalidCatalogRecord;
        if (self.reporter_incarnation == 0 or self.replica_id == 0 or self.group_id == 0 or
            self.root_generation == 0 or self.physical_root_incarnation == 0 or
            self.reported_term == 0 or self.reported_applied_index == 0 or
            std.mem.allEqual(u8, &self.binding_digest, 0) or
            !std.mem.eql(u8, &self.metadata_incarnation, &self.identity.metadata_incarnation))
            return error.InvalidCatalogRecord;
    }
    pub fn requireBinding(self: @This(), binding: job.Binding) !void {
        try self.validate();
        if (binding.authority != .raft or self.group_id != binding.fence.owner_group_id or
            !std.mem.eql(u8, &self.binding_digest, &try bindingDigest(binding)))
            return error.SchemaValidationJobChanged;
    }
    pub fn require(self: @This(), observed: RootedRaftObservation) !void {
        try self.requireBinding(observed.observation.binding);
        try observed.observation.validateFor(observed.observation.binding);
        if (observed.root_generation != self.root_generation or
            observed.observation.physical_root_incarnation != self.physical_root_incarnation)
            return error.SchemaValidationRuntimeRecoveryRequired;
        const applied = observed.observation.applied orelse return error.InvalidControlReceiptPosition;
        try observed.validateCompletion();
        const completed_index = if (observed.completed) |completed| completed.applied_index else applied.index;
        const completed_term = if (observed.completed) |completed| completed.term else applied.term;
        if (completed_index < self.reported_applied_index or completed_term > self.reported_term)
            return error.InvalidControlReceiptPosition;
    }
};

pub const SignedRaftObservation = struct {
    pub const max_wire_bytes = 48 * 1024;
    rooted: RootedRaftObservation,
    proof: RaftProof,

    fn requireSignable(observed: RootedRaftObservation) !void {
        try observed.observation.validateFor(observed.observation.binding);
        try observed.validateCompletion();
        if (observed.observation.header == null) return error.SchemaValidationJobRequired;
    }
    /// A newer actual watermark must not change the original phase receipt.
    pub fn logicalDigest(self: @This()) !Digest {
        try requireSignable(self.rooted);
        return logicalHeaderDigest(self.rooted.observation.header.?);
    }
    pub fn evidenceDigest(self: @This()) !Digest {
        try requireSignable(self.rooted);
        if (self.rooted.completed) |completed| return hashValue("antfly/schema-validation-raft-observed-state/v2", .{
            .observation = self.rooted.observation,
            .completed = completed,
        });
        return hashValue("antfly/schema-validation-raft-observed-state/v1", self.rooted.observation);
    }
    pub fn sign(observed: RootedRaftObservation, expected: RaftScope, root: local.storage_db_root_signing_identity.State) !@This() {
        try expected.require(observed);
        try requireSignable(observed);
        var result: @This() = .{
            .rooted = observed,
            .proof = .{
                .identity = expected.identity,
                .reporter_incarnation = expected.reporter_incarnation,
                .authority = .{ .raft = expected.replica_id },
                .root_generation = observed.root_generation,
                .signature = undefined,
            },
        };
        result.proof = try result.proof.sign(root, try result.evidenceDigest());
        return result;
    }
    pub fn verify(self: @This(), expected: RaftScope) !void {
        try expected.require(self.rooted);
        try requireSignable(self.rooted);
        if (!std.meta.eql(self.proof.identity, expected.identity) or
            self.proof.reporter_incarnation != expected.reporter_incarnation or
            self.proof.root_generation != self.rooted.root_generation or self.proof.authority != .raft or
            self.proof.authority.raft != expected.replica_id) return error.SchemaValidationJobChanged;
        try self.proof.verify(try self.evidenceDigest());
    }
    pub fn commitment(self: @This()) !Digest {
        return self.proof.commitment(try self.evidenceDigest());
    }
};

pub const SignedObservation = union(enum) {
    native: SignedNativeObservation,
    raft: SignedRaftObservation,
};

/// Frozen source-image witness for a separately durable parent authorization.
/// This is neither a progress ACK nor permission to install/adopt a root. The
/// metadata writer must authenticate its live source before retaining a grant.
pub const SnapshotParentProof = roots.ProofFor(
    "antfly/schema-validation-snapshot-parent/v1",
    "antfly/schema-validation-snapshot-parent-accepted-root/v1",
);
pub const SnapshotParent = struct {
    version: u8 = 1,
    binding: job.Binding,
    origin: local.storage_db_native_backup.Origin,
    /// SHA-256 footer of the complete immutable NRSP2 stream, not only its
    /// control-origin fields. Signing happens after source artifact completion.
    stream_digest: Digest,
    through_index: u64,
    completed: local.storage_read_consistency.ReadSafetyBarrier.FrozenProof,

    pub fn validate(self: @This()) !void {
        try self.binding.validate();
        if (std.mem.allEqual(u8, &self.stream_digest, 0)) return error.InvalidGenerationPublication;
        if (self.version != 1 or self.binding.authority != .raft or self.through_index == 0 or
            self.completed.incarnation == 0 or self.completed.term == 0 or
            self.completed.applied_index < self.through_index) return error.InvalidControlReceiptPosition;
        try self.origin.requireRaftTransfer(local.storage_db_online_source_contract.namespaceBytes(self.binding.fence.namespace), self.through_index);
        if (self.origin.applied) |cut| if (cut.term > self.completed.term) return error.InvalidControlReceiptPosition;
    }
};

pub const SignedSnapshotParent = struct {
    pub const max_wire_bytes = 24 * 1024;
    value: SnapshotParent,
    proof: SnapshotParentProof,

    pub fn evidenceDigest(self: @This()) !Digest {
        try self.value.validate();
        return hashValue("antfly/schema-validation-frozen-parent-image/v1", self.value);
    }
    /// The source adapter supplies its owned frozen snapshot, completed stream
    /// footer and retained runtime/root scope. No request claims are signed.
    pub fn sign(value: SnapshotParent, identity: @import("store_root_enrollment.zig").Identity, reporter: u64, replica: u64, generation: u64, root: local.storage_db_root_signing_identity.State) !@This() {
        var result: @This() = .{ .value = value, .proof = .{ .identity = identity, .reporter_incarnation = reporter, .authority = .{ .raft = replica }, .root_generation = generation, .signature = undefined } };
        result.proof = try result.proof.sign(root, try result.evidenceDigest());
        return result;
    }
    pub fn verifyFor(self: @This(), binding: job.Binding, physical_root: u128) !void {
        try self.value.validate();
        if (!std.meta.eql(self.value.binding, binding)) return error.SchemaValidationJobChanged;
        if (physical_root == 0 or
            self.value.origin.physical_root_incarnation != physical_root or self.proof.authority != .raft)
            return error.SchemaValidationRuntimeRecoveryRequired;
        try self.proof.verify(try self.evidenceDigest());
    }
    pub fn commitment(self: @This()) !Digest {
        return self.proof.commitment(try self.evidenceDigest());
    }
    pub fn requireInstallation(self: @This(), binding: job.Binding, installed: local.storage_db_native_backup.InstalledOrigin) !void {
        try installed.validate();
        try self.verifyFor(binding, installed.source.physical_root_incarnation);
        if (installed.group_id != binding.fence.owner_group_id or
            !std.mem.eql(u8, &installed.namespace, &local.storage_db_online_source_contract.namespaceBytes(binding.fence.namespace)) or
            installed.through_index != self.value.through_index or !std.meta.eql(installed.source, self.value.origin) or
            !std.mem.eql(u8, &installed.stream_digest, &self.value.stream_digest)) return error.SchemaValidationRuntimeRecoveryRequired;
    }
};

/// Verified physical evidence, not publication authority. The accepting
/// metadata job still compares its durable owner phase and decision identity.
pub const VerifiedObservation = struct {
    header: job.Header,
    logical_digest: Digest,
    root_commitment: Digest,
    physical_root_incarnation: u128,
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

test "schema validation attestation preparation discovers roots without creating phase receipts" {
    const original = Fixture.observed();
    const scope = try Fixture.scope(original);
    const value: job.Preparation = .{ .binding = original.observation.binding, .physical_root_incarnation = original.observation.physical_root_incarnation, .position = null };
    const prepared = try SignedPreparation.sign(value, scope.signing.identity, scope.signing.reporter_incarnation, .{ .native = scope.signing.binding_digest }, scope.root_generation, try Fixture.root());
    try prepared.verifyFor(value.binding, value.physical_root_incarnation);
    var final_binding = value.binding;
    final_binding.plan_digest = @splat(97);
    try prepared.verifyFor(final_binding, value.physical_root_incarnation);
    var altered = prepared;
    altered.value.binding.plan_digest = final_binding.plan_digest;
    try altered.verifyFor(final_binding, value.physical_root_incarnation);
    try std.testing.expectEqualSlices(u8, &try prepared.commitment(), &try altered.commitment());
    altered = prepared;
    altered.value.binding.after_schema[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, altered.verifyFor(final_binding, value.physical_root_incarnation));
    altered = prepared;
    altered.value.physical_root_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, altered.verifyFor(final_binding, value.physical_root_incarnation));
    altered = prepared;
    altered.value.position = .{ .native = .{ .namespace = value.binding.fence.namespace, .sequence = 1 } };
    try std.testing.expectError(error.InvalidGenerationPublication, altered.verifyFor(final_binding, value.physical_root_incarnation));
    altered = prepared;
    altered.value.replication_pending = true;
    try std.testing.expectError(error.ReplicationPublisherUnavailable, altered.verifyFor(final_binding, value.physical_root_incarnation));
    altered = prepared;
    altered.proof.root_generation += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, altered.verifyFor(final_binding, value.physical_root_incarnation));
    const phase_proof: Proof = .{ .identity = prepared.proof.identity, .reporter_incarnation = prepared.proof.reporter_incarnation, .authority = prepared.proof.authority, .root_generation = prepared.proof.root_generation, .signature = prepared.proof.signature };
    try std.testing.expectError(error.InvalidGenerationPublication, phase_proof.verify(try prepared.evidenceDigest()));
    var raft_value = value;
    raft_value.binding.authority = .raft;
    const raft = try SignedPreparation.sign(raft_value, scope.signing.identity, scope.signing.reporter_incarnation, .{ .raft = 42 }, scope.root_generation, try Fixture.root());
    try raft.verifyFor(raft_value.binding, raft_value.physical_root_incarnation);
    raft_value.position = .{ .raft = .{ .term = 2, .index = 8 } };
    const nonempty = try SignedPreparation.sign(raft_value, scope.signing.identity, scope.signing.reporter_incarnation, .{ .raft = 42 }, scope.root_generation, try Fixture.root());
    try nonempty.verifyFor(raft_value.binding, raft_value.physical_root_incarnation);
}

test "schema validation attestation Raft evidence binds actual cuts and rejects foreign roots and clock domains" {
    const native = Fixture.observed();
    var binding = native.observation.binding;
    binding.authority = .raft;
    const cut: local.storage_db_receipt_position.Position = .{ .raft = .{ .term = 2, .index = 10 } };
    var header = native.observation.header.?;
    header.binding = binding;
    header.admitted = cut;
    header.updated = cut;
    const observed: RootedRaftObservation = .{ .root_generation = native.root_generation, .observation = .{
        .binding = binding,
        .physical_root_incarnation = native.observation.physical_root_incarnation,
        .applied = .{ .term = 2, .index = 12 },
        .header = header,
    } };
    const native_scope = try Fixture.scope(native);
    const expected: RaftScope = .{
        .metadata_incarnation = native_scope.metadata_incarnation,
        .binding_digest = try bindingDigest(binding),
        .identity = native_scope.signing.identity,
        .reporter_incarnation = native_scope.signing.reporter_incarnation,
        .replica_id = 42,
        .group_id = binding.fence.owner_group_id,
        .root_generation = observed.root_generation,
        .physical_root_incarnation = observed.observation.physical_root_incarnation,
        .reported_term = 2,
        .reported_applied_index = 11,
    };
    const signed = try SignedRaftObservation.sign(observed, expected, try Fixture.root());
    try signed.verify(expected);
    var later = observed;
    later.observation.applied.?.index += 1;
    const reattested = try SignedRaftObservation.sign(later, expected, try Fixture.root());
    try reattested.verify(expected);
    try std.testing.expectEqualSlices(u8, &try signed.logicalDigest(), &try reattested.logicalDigest());
    try std.testing.expect(!std.mem.eql(u8, &try signed.commitment(), &try reattested.commitment()));
    var forged = signed;
    forged.rooted.observation.applied.?.index += 1;
    try std.testing.expectError(error.InvalidGenerationPublication, forged.verify(expected));
    forged = signed;
    forged.rooted.observation.header.?.chain[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, forged.verify(expected));
    forged = signed;
    forged.proof.authority = .{ .native = @splat(1) };
    try std.testing.expectError(error.SchemaValidationJobChanged, forged.verify(expected));
    inline for (.{ "replica_id", "group_id", "reporter_incarnation", "root_generation", "physical_root_incarnation" }) |field| {
        var stale = expected;
        @field(stale, field) += 1;
        try std.testing.expectError(if (comptime std.mem.eql(u8, field, "root_generation") or std.mem.eql(u8, field, "physical_root_incarnation")) error.SchemaValidationRuntimeRecoveryRequired else error.SchemaValidationJobChanged, signed.verify(stale));
    }
    var stale = expected;
    stale.reported_applied_index = 13;
    try std.testing.expectError(error.InvalidControlReceiptPosition, signed.verify(stale));
    stale = expected;
    stale.reported_term = 1;
    try std.testing.expectError(error.InvalidControlReceiptPosition, signed.verify(stale));
    stale = expected;
    stale.reported_term = 3;
    // Election is not a physical mutation or a rewrite of the committed cut.
    try signed.verify(stale);
    // A no-op completed at 14 must not be represented as a data mutation at
    // (3,14): the actual immutable storage marker remains (2,12).
    var completed = observed;
    completed.completed = .{ .incarnation = 91, .term = 3, .applied_index = 14 };
    var completion_scope = expected;
    completion_scope.reported_term = 3;
    completion_scope.reported_applied_index = 14;
    const completed_signed = try SignedRaftObservation.sign(completed, completion_scope, try Fixture.root());
    try completed_signed.verify(completion_scope);
    try std.testing.expectEqualDeep(observed.observation.applied, completed_signed.rooted.observation.applied);
    try std.testing.expectEqualSlices(u8, &try signed.logicalDigest(), &try completed_signed.logicalDigest());
    try std.testing.expect(!std.mem.eql(u8, &try signed.commitment(), &try completed_signed.commitment()));
    try std.testing.expectError(error.InvalidControlReceiptPosition, signed.verify(completion_scope));
    var changed_completion = completed_signed;
    // All completion fields, including the runtime incarnation, are signed.
    inline for (.{ "incarnation", "term", "applied_index" }) |field| {
        changed_completion = completed_signed;
        @field(changed_completion.rooted.completed.?, field) -= 1;
        try std.testing.expectError(error.InvalidGenerationPublication, changed_completion.proof.verify(try changed_completion.evidenceDigest()));
    }
    changed_completion = completed_signed;
    changed_completion.rooted.completed = null;
    try std.testing.expectError(error.InvalidGenerationPublication, changed_completion.proof.verify(try changed_completion.evidenceDigest()));
    inline for (.{ "incarnation", "term", "applied_index" }) |field| {
        var invalid = completed;
        @field(invalid.completed.?, field) = 0;
        try std.testing.expectError(error.InvalidControlReceiptPosition, SignedRaftObservation.sign(invalid, completion_scope, try Fixture.root()));
    }
    var invalid = completed;
    invalid.completed.?.applied_index = 11;
    try std.testing.expectError(error.InvalidControlReceiptPosition, invalid.validateCompletion());
    invalid = completed;
    invalid.completed.?.term = 1;
    try std.testing.expectError(error.InvalidControlReceiptPosition, invalid.validateCompletion());
    var stale_completion = completion_scope;
    stale_completion.reported_term = 2;
    try std.testing.expectError(error.InvalidControlReceiptPosition, completed_signed.verify(stale_completion));
    stale_completion = completion_scope;
    stale_completion.reported_applied_index = 15;
    try std.testing.expectError(error.InvalidControlReceiptPosition, completed_signed.verify(stale_completion));
    stale = expected;
    stale.binding_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, signed.verify(stale));
    stale = expected;
    stale.metadata_incarnation[0] ^= 1;
    try std.testing.expectError(error.InvalidCatalogRecord, signed.verify(stale));
    var missing = observed;
    missing.observation.header = null;
    try std.testing.expectError(error.SchemaValidationJobRequired, SignedRaftObservation.sign(missing, expected, try Fixture.root()));
    missing = observed;
    missing.observation.applied = null;
    try std.testing.expectError(error.InvalidControlReceiptPosition, SignedRaftObservation.sign(missing, expected, try Fixture.root()));
    missing = observed;
    missing.observation.applied.?.index = 9;
    try std.testing.expectError(error.InvalidControlReceiptPosition, missing.observation.validateFor(binding));
    missing = observed;
    missing.observation.header.?.updated = native.observation.header.?.updated;
    try std.testing.expectError(error.InvalidControlReceiptPosition, missing.observation.validateFor(binding));
    // Equal field layouts never make another protocol's signature portable.
    const cross: Proof = .{ .identity = signed.proof.identity, .reporter_incarnation = signed.proof.reporter_incarnation, .authority = signed.proof.authority, .root_generation = signed.proof.root_generation, .signature = signed.proof.signature };
    try std.testing.expectError(error.InvalidGenerationPublication, cross.verify(try signed.evidenceDigest()));
    const encoded = try std.json.Stringify.valueAlloc(std.testing.allocator, signed, .{});
    defer std.testing.allocator.free(encoded);
    try std.testing.expect(encoded.len <= SignedRaftObservation.max_wire_bytes);
    var decoded = try std.json.parseFromSlice(SignedRaftObservation, std.testing.allocator, encoded, .{});
    defer decoded.deinit();
    try decoded.value.verify(expected);
    try std.testing.expectEqualDeep(signed, decoded.value);
}

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

test "schema validation attestation pending audit progress never grants full image coverage" {
    const wire = local.storage_db_relational_schema_root_contract;
    var header = Fixture.observed().observation.header.?;
    header.phase = .ready;
    header.frozen = .{ .native = .{ .namespace = header.binding.fence.namespace, .sequence = 2 } };
    header.updated = .{ .native = .{ .namespace = header.binding.fence.namespace, .sequence = 3 } };
    header.revision = 3;
    header.records = 2;
    header.rows_scanned = 2;
    header.cursor_digest = job.hash("complete");
    const root = try Fixture.root();
    var rows: wire.RowSummary = .{ .header = header, .snapshot_cut = header.updated, .coverage = .full, .records = 1, .rows = 1, .cursor_digest = job.hash("prefix") };
    try (wire.AuditResult{ .pending = rows }).requireFor(header.binding, root.root_incarnation, root.public_key);
    try std.testing.expectError(error.InvalidSchemaValidationJob, rows.validate());
    rows.records = 3;
    try std.testing.expectError(error.InvalidSchemaValidationJob, (wire.AuditResult{ .pending = rows }).requireFor(header.binding, root.root_incarnation, root.public_key));
    rows.records = 1;
    var foreign = header.binding;
    foreign.plan_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, (wire.AuditResult{ .pending = rows }).requireFor(foreign, root.root_incarnation, root.public_key));
    try std.testing.expectError(error.InvalidRelationalRowsBudget, (wire.AuditBudget{ .records = 65_537 }).validate());
    try std.testing.expectError(error.InvalidRelationalRowsBudget, (wire.AuditBudget{ .time_ns = 0 }).validate());
}

test "schema validation attestation destination ancestry requires canonical phase predecessors" {
    const lineage = @import("schema_validation_parent.zig").requireHeaderLineage;
    const decisions = @import("schema_validation_parent.zig").requireHeaderDecisions;
    const Head = @import("schema_validation_publication.zig").Head;
    var ready = Fixture.observed().observation.header.?;
    ready.binding.authority = .raft;
    ready.phase = .ready;
    ready.admitted = .{ .raft = .{ .term = 1, .index = 1 } };
    ready.frozen = .{ .raft = .{ .term = 1, .index = 2 } };
    ready.updated = .{ .raft = .{ .term = 1, .index = 3 } };
    ready.revision = 3;
    const validated: Head = .{ .metadata_group_id = 13, .metadata_incarnation = "0123456789abcdef0123456789abcdef".*, .id = ready.binding.id, .plan_digest = ready.binding.plan_digest, .table_id = ready.binding.fence.namespace.table_id, .owner_count = 1, .phase = .validated, .revision = 3, .ready = 1, .ready_chain = @splat(31) };
    const admitted = try validated.decideInstall(3);
    var installed = ready;
    installed.phase = .installed;
    installed.updated = .{ .raft = .{ .term = 1, .index = 4 } };
    installed.revision = 4;
    installed.last_effect = @splat(51);
    installed.installation = .{ .decision = try admitted.installDecision(), .ready_digest = try ready.digest(), .ready_position = ready.updated, .ready_last_effect = ready.last_effect, .physical_catalog = @splat(53), .activation = @splat(54), .position = installed.updated };
    try decisions(admitted, installed);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, decisions(validated, installed));
    var wrong_install = admitted;
    wrong_install.revision += 1;
    wrong_install.install_revision += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, decisions(wrong_install, installed));
    var published = admitted;
    published.phase = .releasing;
    published.revision = 6;
    published.publication_revision = 6;
    published.installed = 1;
    published.installed_chain = @splat(32);
    var released = installed;
    released.phase = .released;
    released.updated = .{ .raft = .{ .term = 1, .index = 5 } };
    released.revision = 5;
    released.last_effect = @splat(55);
    released.release = .{ .publication = try published.publicationDecision(), .installed_digest = try installed.digest(), .installed_last_effect = installed.last_effect };
    try decisions(published, released);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, decisions(admitted, released));
    var wrong_publication = published;
    wrong_publication.installed_chain[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, decisions(wrong_publication, released));
    try lineage(ready, ready);
    try lineage(ready, installed);
    try lineage(ready, released);
    try lineage(installed, released);
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, lineage(released, ready));
    var unrelated = ready;
    unrelated.last_effect[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, lineage(unrelated, released));
    released.installation.?.ready_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidSchemaValidationJob, lineage(ready, released));
}

test "schema validation attestation restored destinations join audited images with exact authorized parents" {
    // Cryptographic/wire qualification only, not a fabricated native audit or
    // a root-adoption writer. Physical capture remains owned by storage.
    const parent = @import("schema_validation_parent.zig");
    const barriers = @import("schema_validation_publication.zig");
    const wire = local.storage_db_relational_schema_root_contract;
    const t = std.testing;
    const root = try Fixture.root();
    const native = Fixture.observed();
    const original_scope = try Fixture.scope(native);
    var header = native.observation.header.?;
    header.binding.authority = .raft;
    header.phase = .canceled;
    header.admitted = null;
    header.updated = .{ .raft = .{ .term = 2, .index = 5 } };
    const header_bytes = try header.encode(t.allocator);
    defer t.allocator.free(header_bytes);
    const source: local.storage_db_native_backup.Origin = .{
        .physical_root_incarnation = 17,
        .namespace = header.binding.fence.namespace,
        .authority = .{ .kind = .raft, .namespace = local.storage_db_online_source_contract.namespaceBytes(header.binding.fence.namespace) },
        .applied = .{ .term = 2, .index = 5 },
        .schema_header = job.hash(header_bytes),
        .transaction_directory = null,
        .schema_outcomes = null,
    };
    const head: barriers.Head = .{ .metadata_group_id = 13, .metadata_incarnation = original_scope.metadata_incarnation, .id = header.binding.id, .plan_digest = header.binding.plan_digest, .table_id = header.binding.fence.namespace.table_id, .owner_count = 1 };
    const owner: barriers.Owner = .{ .ordinal = 0, .binding = header.binding, .physical_root_incarnation = 17 };
    const retained: parent.Request = .{
        .lookup = .fromHead(head),
        .ordinal = 0,
        .revision = head.revision,
        .owner_digest = try parent.ownerDigest(owner, head),
        .source_header = header,
        .signed = try SignedSnapshotParent.sign(.{ .binding = header.binding, .origin = source, .through_index = 9, .stream_digest = @splat(21), .completed = .{ .incarnation = 77, .term = 3, .applied_index = 9 } }, original_scope.signing.identity, 4, 42, 6, root),
    };
    const installed: local.storage_db_native_backup.InstalledOrigin = .{ .group_id = header.binding.fence.owner_group_id, .through_index = 9, .destination_root_incarnation = 23, .namespace = local.storage_db_online_source_contract.namespaceBytes(header.binding.fence.namespace), .source = source, .stream_digest = @splat(21) };
    var destination_origin = source;
    destination_origin.physical_root_incarnation = 23;
    const Sign = struct {
        fn evidence(facts: wire.Facts, state: local.storage_db_root_signing_identity.State) !wire.Evidence {
            var result: wire.Evidence = .{ .value = facts, .signature = undefined };
            result.signature = try state.sign(&try result.signatureDigest());
            return result;
        }
        fn decode(a: std.mem.Allocator, bytes: []const u8, key: [32]u8) !void {
            _ = try wire.Evidence.decode(a, bytes, key);
        }
    };
    const destination = try Sign.evidence(.{
        .signing_root = root.root_incarnation,
        .physical_root = 23,
        .checkpoint_digest = @splat(91),
        .rows = .{ .header = header, .snapshot_cut = header.updated, .coverage = .none, .records = 0, .rows = 0, .cursor_digest = job.hash("") },
        .qualification = .{ .origin = destination_origin, .directory_head = null, .directory = .{}, .transactions = .{}, .history_head = null, .history = .{}, .installation = installed, .source_history_head = null },
    }, root);
    const scope: RaftScope = .{ .metadata_incarnation = head.metadata_incarnation, .binding_digest = try bindingDigest(header.binding), .identity = original_scope.signing.identity, .reporter_incarnation = 4, .replica_id = 42, .group_id = header.binding.fence.owner_group_id, .root_generation = 6, .physical_root_incarnation = 23, .reported_term = 3, .reported_applied_index = 9 };
    try retained.requireDestination(head, owner, destination, scope);
    var later = head;
    later.revision += 1;
    try retained.requireDestination(later, owner, destination, scope);
    var stale = scope;
    stale.reported_applied_index = 8;
    try t.expectError(error.IndexOwnerReceiptChanged, retained.requireDestination(head, owner, destination, stale));
    stale = scope;
    stale.reported_term = 2;
    try t.expectError(error.IndexOwnerReceiptChanged, retained.requireDestination(head, owner, destination, stale));
    stale = scope;
    stale.physical_root_incarnation += 1;
    try t.expectError(error.SchemaValidationRuntimeRecoveryRequired, retained.requireDestination(head, owner, destination, stale));
    var corrupted = destination;
    corrupted.signature[0] ^= 1;
    try t.expectError(error.InvalidSchemaValidationJob, retained.requireDestination(head, owner, corrupted, scope));
    var facts = destination.value;
    facts.qualification.installation.?.stream_digest[0] ^= 1;
    try t.expectError(error.SchemaValidationRuntimeRecoveryRequired, retained.requireDestination(head, owner, try Sign.evidence(facts, root), scope));
    facts = destination.value;
    facts.qualification.installation = null;
    try t.expectError(error.SchemaValidationRuntimeRecoveryRequired, retained.requireDestination(head, owner, try Sign.evidence(facts, root), scope));
    facts = destination.value;
    facts.rows.header.last_effect[0] ^= 1;
    {
        const changed = try facts.rows.header.encode(t.allocator);
        defer t.allocator.free(changed);
        facts.qualification.origin.schema_header = job.hash(changed);
    }
    try t.expectError(error.SchemaValidationRuntimeRecoveryRequired, retained.requireDestination(head, owner, try Sign.evidence(facts, root), scope));
    const bytes = try destination.encode(t.allocator);
    defer t.allocator.free(bytes);
    try t.expectEqualDeep(destination, try wire.Evidence.decode(t.allocator, bytes, root.public_key));
    try t.checkAllAllocationFailures(t.allocator, Sign.decode, .{ bytes, root.public_key });
    bytes[bytes.len - 1] ^= 1;
    var failing = t.FailingAllocator.init(t.allocator, .{ .fail_index = 0 });
    try t.expectError(error.InvalidSchemaValidationJob, wire.Evidence.decode(failing.allocator(), bytes, root.public_key));
    try t.expectEqual(@as(usize, 0), failing.alloc_index);
}

test "schema validation attestation snapshot parents bind frozen source without becoming progress ACKs" {
    const root = try Fixture.root();
    const native = Fixture.observed();
    const scope = try Fixture.scope(native);
    var binding = native.observation.binding;
    binding.authority = .raft;
    const image: SnapshotParent = .{
        .binding = binding,
        .origin = .{ .physical_root_incarnation = 17, .namespace = binding.fence.namespace, .authority = .{ .kind = .raft, .namespace = local.storage_db_online_source_contract.namespaceBytes(binding.fence.namespace) }, .applied = .{ .term = 2, .index = 5 }, .schema_header = null, .transaction_directory = null, .schema_outcomes = null },
        .through_index = 9,
        .stream_digest = @splat(21),
        .completed = .{ .incarnation = 77, .term = 3, .applied_index = 9 },
    };
    const signed = try SignedSnapshotParent.sign(image, scope.signing.identity, scope.signing.reporter_incarnation, 42, scope.root_generation, root);
    try signed.verifyFor(binding, 17);
    var later_completion = image;
    later_completion.completed.applied_index = 12;
    const later_signed = try SignedSnapshotParent.sign(later_completion, scope.signing.identity, scope.signing.reporter_incarnation, 42, scope.root_generation, root);
    try later_signed.verifyFor(binding, 17);
    try std.testing.expectEqual(@as(u64, 9), later_signed.value.through_index);
    try std.testing.expectEqual(@as(u64, 12), later_signed.value.completed.applied_index);
    var behind = image;
    behind.completed.applied_index = 8;
    try std.testing.expectError(error.InvalidControlReceiptPosition, behind.validate());
    try std.testing.expectEqual(@as(u64, 5), signed.value.origin.applied.?.index);
    try std.testing.expectEqual(@as(u64, 9), signed.value.through_index);
    const installed: local.storage_db_native_backup.InstalledOrigin = .{ .group_id = binding.fence.owner_group_id, .through_index = image.through_index, .destination_root_incarnation = 23, .namespace = local.storage_db_online_source_contract.namespaceBytes(binding.fence.namespace), .source = image.origin, .stream_digest = image.stream_digest };
    try signed.requireInstallation(binding, installed);
    var wrong_installation = installed;
    wrong_installation.stream_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, signed.requireInstallation(binding, wrong_installation));
    wrong_installation = installed;
    wrong_installation.through_index += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, signed.requireInstallation(binding, wrong_installation));
    wrong_installation = installed;
    wrong_installation.group_id += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, signed.requireInstallation(binding, wrong_installation));
    wrong_installation = installed;
    wrong_installation.source.physical_root_incarnation += 1;
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, signed.requireInstallation(binding, wrong_installation));
    var wrong_stream = signed;
    wrong_stream.value.stream_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidGenerationPublication, wrong_stream.verifyFor(binding, 17));
    var other_binding = binding;
    other_binding.plan_digest[0] ^= 1;
    try std.testing.expectError(error.SchemaValidationJobChanged, signed.verifyFor(other_binding, 17));
    try std.testing.expectError(error.SchemaValidationRuntimeRecoveryRequired, signed.verifyFor(binding, 18));
    inline for (.{ "incarnation", "term", "applied_index" }) |field| {
        var changed = signed;
        @field(changed.value.completed, field) += 1;
        if (changed.verifyFor(binding, 17)) |_| return error.TestUnexpectedResult else |_| {}
    }
    inline for (.{ "physical_root_incarnation", "schema_header", "transaction_directory", "schema_outcomes" }) |field| {
        var changed = signed;
        if (comptime std.mem.eql(u8, field, "physical_root_incarnation")) {
            changed.value.origin.physical_root_incarnation += 1;
        } else @field(changed.value.origin, field) = @splat(33);
        if (changed.verifyFor(binding, 17)) |_| return error.TestUnexpectedResult else |_| {}
    }
    var empty = image;
    empty.origin.applied = null;
    const signed_empty = try SignedSnapshotParent.sign(empty, scope.signing.identity, scope.signing.reporter_incarnation, 42, scope.root_generation, root);
    try signed_empty.verifyFor(binding, 17);
    try std.testing.expect(signed_empty.value.origin.applied == null);
    var invalid = image;
    invalid.completed.incarnation = 0;
    try std.testing.expectError(error.InvalidControlReceiptPosition, invalid.validate());
    invalid = image;
    invalid.origin.applied.?.term = 4;
    try std.testing.expectError(error.InvalidControlReceiptPosition, invalid.validate());
    invalid = image;
    invalid.origin.applied.?.index = 10;
    try std.testing.expectError(error.InvalidNativeBackupOrigin, invalid.validate());
    invalid = image;
    invalid.stream_digest = @splat(0);
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.validate());
    const ProgressProof = roots.ProofFor("antfly/schema-validation-raft-observation/v1", "antfly/schema-validation-raft-accepted-root/v1");
    const wrong_domain: ProgressProof = .{ .identity = signed.proof.identity, .reporter_incarnation = signed.proof.reporter_incarnation, .authority = signed.proof.authority, .root_generation = signed.proof.root_generation, .signature = signed.proof.signature };
    try std.testing.expectError(error.InvalidGenerationPublication, wrong_domain.verify(try signed.evidenceDigest()));
    const wrong_signed = try wrong_domain.sign(root, try signed.evidenceDigest());
    var wrong_parent = signed;
    wrong_parent.proof.signature = wrong_signed.signature;
    try std.testing.expectError(error.InvalidGenerationPublication, wrong_parent.verifyFor(binding, 17));
    try std.testing.expect(!std.mem.eql(u8, &try wrong_signed.commitment(try signed.evidenceDigest()), &try signed.commitment()));
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
