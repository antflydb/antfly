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

//! Zero-progress physical-root recovery. This is neither a row audit nor a
//! schema-phase ACK. Native preparation rejects active jobs/topology fences;
//! the producer additionally certifies the actual shared Raft apply prefix.
//! Metadata may consume this only while this owner has no accepted progress.
const std = @import("std");
const local = @import("antfly_local_sources");
const attestation = @import("schema_validation_attestation.zig");
const roots = @import("store_root_attestation.zig");
const contract = local.storage_db_relational_schema_owner_contract;
const Digest = [32]u8;

pub const Proof = roots.ProofFor(
    "antfly/schema-validation-bootstrap-root/v1",
    "antfly/schema-validation-bootstrap-accepted-root/v1",
);

pub const CertifiedPreparation = struct {
    preparation: attestation.SignedPreparation,
    completed: local.storage_read_consistency.ReadSafetyBarrier.FrozenProof,
    proof: Proof,

    pub fn evidenceDigest(self: @This()) !Digest {
        try self.preparation.value.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/schema-validation-bootstrap-physical-cut/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hash, &.{});
        try std.json.Stringify.value(.{ self.preparation, self.completed }, .{}, &writer.writer);
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }

    fn require(self: @This(), scope: attestation.RaftScope) !void {
        const prepared = self.preparation;
        try scope.requireBinding(prepared.value.binding);
        try prepared.verifyFor(prepared.value.binding, scope.physical_root_incarnation);
        if (!std.meta.eql(prepared.proof.identity, scope.identity) or
            prepared.proof.reporter_incarnation != scope.reporter_incarnation or
            prepared.proof.root_generation != scope.root_generation or
            prepared.proof.authority != .raft or prepared.proof.authority.raft != scope.replica_id or
            self.completed.incarnation == 0 or self.completed.term == 0 or self.completed.applied_index == 0 or
            self.completed.term > scope.reported_term or self.completed.applied_index < scope.reported_applied_index)
            return error.InvalidControlReceiptPosition;
        if (prepared.value.position) |position| {
            if (position != .raft or position.raft.index > self.completed.applied_index or position.raft.term > self.completed.term)
                return error.InvalidControlReceiptPosition;
        }
    }

    pub fn sign(preparation: attestation.SignedPreparation, completed: local.storage_read_consistency.ReadSafetyBarrier.FrozenProof, scope: attestation.RaftScope, key: local.storage_db_root_signing_identity.State) !@This() {
        var result: @This() = .{
            .preparation = preparation,
            .completed = completed,
            .proof = .{ .identity = scope.identity, .reporter_incarnation = scope.reporter_incarnation, .authority = .{ .raft = scope.replica_id }, .root_generation = scope.root_generation, .signature = undefined },
        };
        try result.require(scope);
        result.proof = try result.proof.sign(key, try result.evidenceDigest());
        return result;
    }

    /// Cryptographic consistency only. Current enrollment, placement, source
    /// report, reader requirements and exact predecessor CAS belong to metadata.
    pub fn verify(self: @This(), scope: attestation.RaftScope) !void {
        try self.require(scope);
        if (!std.meta.eql(self.proof.identity, scope.identity) or
            self.proof.reporter_incarnation != scope.reporter_incarnation or self.proof.authority != .raft or
            self.proof.authority.raft != scope.replica_id or self.proof.root_generation != scope.root_generation)
            return error.SchemaValidationJobChanged;
        try self.proof.verify(try self.evidenceDigest());
    }

    pub fn attestedScope(self: @This(), binding: contract.Binding, metadata_incarnation: [32]u8) !attestation.RaftScope {
        try self.preparation.value.requireFor(binding);
        const scope: attestation.RaftScope = .{
            .metadata_incarnation = metadata_incarnation,
            .binding_digest = try attestation.bindingDigest(binding),
            .identity = self.proof.identity,
            .reporter_incarnation = self.proof.reporter_incarnation,
            .replica_id = if (self.proof.authority == .raft) self.proof.authority.raft else return error.InvalidControlReceiptPosition,
            .group_id = binding.fence.owner_group_id,
            .root_generation = self.proof.root_generation,
            .physical_root_incarnation = self.preparation.value.physical_root_incarnation,
            .reported_term = self.completed.term,
            .reported_applied_index = self.completed.applied_index,
        };
        try self.verify(scope);
        return scope;
    }
};
