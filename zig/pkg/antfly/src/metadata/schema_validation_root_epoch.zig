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

//! Immutable accepted root epochs. Decoding/cryptographic consistency is not
//! live authority: only the metadata writer may accept a new epoch after its
//! independent enrollment/report checks and exact owner/head predecessor CAS.
const std = @import("std");
const local = @import("antfly_local_sources");
const barriers = @import("schema_validation_publication.zig");
const parent = @import("schema_validation_parent.zig");
const evidence = @import("schema_validation_attestation.zig");
const contract = local.storage_db_relational_schema_owner_contract;
const bootstrap = @import("schema_validation_bootstrap.zig");
const Digest = [32]u8;
pub const Lineage = union(enum) {
    retained_parent: Digest,
    /// The serving replica has independently audited its native state and
    /// quorum-certified the SHARED native/raw Raft apply prefix. This also
    /// covers promoted followers that never installed a snapshot artifact.
    raft_replay: void,
    /// Current standalone-primary binding and the actual durable native
    /// namespace clock. Native sequences never stand in for Raft positions.
    native_recovery: void,
};
pub const Cut = local.storage_db_receipt_position.Position;
pub const Request = struct {
    lookup: @import("schema_validation_read.zig").Lookup,
    ordinal: u16,
    revision: u64,
    owner_digest: Digest,
    expected_epoch: u64,
    lineage: Lineage,
    proposal: union(enum) {
        audited: struct {
            destination: local.storage_db_relational_schema_root_contract.Evidence,
            observation: evidence.SignedRaftObservation,
        },
        bootstrap: bootstrap.CertifiedPreparation,
        audited_native: struct {
            destination: local.storage_db_relational_schema_root_contract.Evidence,
            observation: evidence.SignedNativeObservation,
        },
        native_bootstrap: evidence.SignedPreparation,
    },

    pub fn binding(self: @This()) contract.Binding {
        return switch (self.proposal) {
            .audited => |value| value.destination.value.rows.header.binding,
            .bootstrap => |value| value.preparation.value.binding,
            .audited_native => |value| value.destination.value.rows.header.binding,
            .native_bootstrap => |value| value.value.binding,
        };
    }
    pub fn physicalRoot(self: @This()) u128 {
        return switch (self.proposal) {
            .audited => |value| value.destination.value.physical_root,
            .bootstrap => |value| value.preparation.value.physical_root_incarnation,
            .audited_native => |value| value.destination.value.physical_root,
            .native_bootstrap => |value| value.value.physical_root_incarnation,
        };
    }
    pub fn completed(self: @This()) ?local.storage_read_consistency.ReadSafetyBarrier.FrozenProof {
        return switch (self.proposal) {
            .audited => |value| value.observation.rooted.completed,
            .bootstrap => |value| value.completed,
            .audited_native, .native_bootstrap => null,
        };
    }
    pub fn sourceCut(self: @This()) !?Cut {
        switch (self.proposal) {
            .audited => |value| {
                if (value.destination.value.rows.snapshot_cut != .raft) return error.InvalidControlReceiptPosition;
                return value.destination.value.rows.snapshot_cut;
            },
            .bootstrap => |value| {
                if (value.preparation.value.position) |position| {
                    if (position != .raft) return error.InvalidControlReceiptPosition;
                    return position;
                }
                return null;
            },
            .audited_native => |value| {
                if (value.destination.value.rows.snapshot_cut != .native) return error.InvalidControlReceiptPosition;
                return value.destination.value.rows.snapshot_cut;
            },
            .native_bootstrap => |value| {
                if (value.value.position) |position| if (position != .native) return error.InvalidControlReceiptPosition;
                return value.value.position;
            },
        }
    }

    pub fn validateShape(self: @This()) !void {
        try (@import("schema_validation_read.zig").OwnerReadRequest{ .lookup = self.lookup, .ordinal = self.ordinal }).validate();
        if (self.revision == 0 or self.revision == std.math.maxInt(u64) or self.expected_epoch == std.math.maxInt(u64) or
            std.mem.allEqual(u8, &self.owner_digest, 0)) return error.InvalidGenerationPublication;
        if (self.lineage == .retained_parent and std.mem.allEqual(u8, &self.lineage.retained_parent, 0)) return error.InvalidGenerationPublication;
        if (self.lineage == .raft_replay and self.completed() == null) return error.InvalidControlReceiptPosition;
        const incarnation = switch (self.proposal) {
            .audited => |value| blk: {
                try value.destination.value.validate();
                break :blk value.observation.proof.identity.metadata_incarnation;
            },
            .bootstrap => |value| blk: {
                if (self.lineage != .raft_replay) return error.InvalidGenerationPublication;
                try value.preparation.value.validate();
                break :blk value.proof.identity.metadata_incarnation;
            },
            .audited_native => |value| blk: {
                if (self.lineage != .native_recovery) return error.InvalidGenerationPublication;
                try value.destination.value.validate();
                break :blk value.observation.proof.identity.metadata_incarnation;
            },
            .native_bootstrap => |value| blk: {
                if (self.lineage != .native_recovery) return error.InvalidGenerationPublication;
                try value.value.validate();
                break :blk value.proof.identity.metadata_incarnation;
            },
        };
        const bound = self.binding();
        if (!std.mem.eql(u8, &bound.id, &self.lookup.id) or !std.mem.eql(u8, &bound.plan_digest, &self.lookup.plan_digest) or
            bound.fence.namespace.table_id != self.lookup.table or
            ((bound.authority == .native) != (self.lineage == .native_recovery)) or
            !std.mem.eql(u8, &incarnation, &self.lookup.metadata_incarnation)) return error.SchemaValidationJobChanged;
    }
    /// Reuse immutable physical evidence while unrelated owners advance the
    /// head. This changes only the next exact CAS intent, never signatures,
    /// epoch lineage or an uncertain physical operation.
    pub fn rebaseForReplay(self: @This(), head: barriers.Head, owner: barriers.Owner) !@This() {
        if (self.lineage == .retained_parent or self.expected_epoch != owner.root_epoch or
            head.phase == .succeeded or head.phase == .canceled) return error.GenerationPublicationChanged;
        var result = self;
        result.revision = head.revision;
        result.owner_digest = try parent.ownerDigest(owner, head);
        try result.requireDestination(head, owner, null);
        return result;
    }

    pub fn digest(self: @This()) !Digest {
        try self.validateShape();
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/schema-validation-root-adoption/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(state, &.{});
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }
    /// Attested lower bounds, NOT metadata report authority. The accepting
    /// context independently verifies this observation in its current writer.
    pub fn attestedScope(self: @This()) !evidence.RaftScope {
        if (self.proposal == .bootstrap) return self.proposal.bootstrap.attestedScope(self.binding(), self.lookup.metadata_incarnation);
        if (self.proposal != .audited) return error.InvalidControlReceiptPosition;
        const audited = self.proposal.audited;
        const observed = audited.observation.rooted.observation;
        const cut = observed.applied orelse return error.InvalidControlReceiptPosition;
        const proof = audited.observation.proof;
        if (proof.authority != .raft) return error.InvalidGenerationPublication;
        const scope: evidence.RaftScope = .{
            .metadata_incarnation = self.lookup.metadata_incarnation,
            .binding_digest = try evidence.bindingDigest(observed.binding),
            .identity = proof.identity,
            .reporter_incarnation = proof.reporter_incarnation,
            .replica_id = proof.authority.raft,
            .group_id = observed.binding.fence.owner_group_id,
            .root_generation = proof.root_generation,
            .physical_root_incarnation = observed.physical_root_incarnation,
            .reported_term = @max(cut.term, if (audited.observation.rooted.completed) |prefix| prefix.term else 0),
            .reported_applied_index = @max(cut.index, if (audited.observation.rooted.completed) |prefix| prefix.applied_index else 0),
        };
        try audited.observation.verify(scope);
        return scope;
    }
    pub fn nativeAttestedScope(self: @This()) !evidence.NativeScope {
        const Signer = struct { view: @import("store_root_attestation.zig").NativeView, generation: u64 };
        const signer: Signer = switch (self.proposal) {
            inline .audited_native, .native_bootstrap => |value, tag| blk: {
                const proof = if (comptime tag == .audited_native) value.observation.proof else value.proof;
                if (proof.authority != .native) return error.InvalidControlReceiptPosition;
                break :blk .{ .view = .{ .identity = proof.identity, .reporter_incarnation = proof.reporter_incarnation, .binding_digest = proof.authority.native }, .generation = proof.root_generation };
            },
            else => return error.InvalidControlReceiptPosition,
        };
        _ = try self.sourceCut();
        const scope: evidence.NativeScope = .{
            .metadata_incarnation = self.lookup.metadata_incarnation,
            .binding_digest = try evidence.bindingDigest(self.binding()),
            .signing = signer.view,
            .root_generation = signer.generation,
            .physical_root_incarnation = self.physicalRoot(),
        };
        try scope.requireBinding(self.binding());
        switch (self.proposal) {
            .audited_native => |value| try value.observation.verify(scope),
            .native_bootstrap => |value| try value.verifyFor(self.binding(), self.physicalRoot()),
            else => unreachable,
        }
        return scope;
    }

    fn requireZeroProgress(self: @This(), head: barriers.Head, owner: barriers.Owner) !void {
        if ((head.install_revision != 0 and head.install_revision <= self.revision) or
            (head.publication_revision != 0 and head.publication_revision <= self.revision)) return error.GenerationPublicationChanged;
        for (std.meta.tags(barriers.Stage)) |stage| if (owner.has(stage)) return error.SchemaValidationRuntimeRecoveryRequired;
    }
    pub fn requireDestination(self: @This(), head: barriers.Head, owner: barriers.Owner, retained: ?parent.Request) !void {
        try self.validateShape();
        try self.lookup.require(head);
        if (self.ordinal != owner.ordinal or self.expected_epoch != owner.root_epoch or
            !std.mem.eql(u8, &self.owner_digest, &try parent.ownerDigest(owner, head))) return error.GenerationPublicationChanged;
        if (self.lineage == .native_recovery) {
            const scope = try self.nativeAttestedScope();
            try scope.requireBinding(owner.binding);
            if (retained != null or self.physicalRoot() == owner.physical_root_incarnation or
                !std.meta.eql(self.binding(), owner.binding)) return error.SchemaValidationRuntimeRecoveryRequired;
            if (self.proposal == .native_bootstrap) {
                try self.requireZeroProgress(head, owner);
                return;
            }
            const audited = self.proposal.audited_native;
            const observed = audited.observation.rooted.observation;
            if (observed.header == null or !std.meta.eql(observed.header.?, audited.destination.value.rows.header) or
                audited.destination.value.rows.snapshot_cut != .native or
                audited.destination.value.rows.snapshot_cut.native.sequence > observed.sequence or
                audited.destination.value.signing_root != scope.signing.identity.root_incarnation or
                audited.destination.value.physical_root != observed.physical_root_incarnation) return error.SchemaValidationRuntimeRecoveryRequired;
            try audited.destination.verify(scope.signing.identity.public_key);
            try parent.requireHeaderDecisions(head, audited.destination.value.rows.header);
            try requireReplayPhase(head, owner, audited.destination.value.rows.header);
            return;
        }
        const scope = try self.attestedScope();
        if (self.proposal == .bootstrap) {
            if (retained != null or self.lineage != .raft_replay or self.physicalRoot() == owner.physical_root_incarnation or
                !std.meta.eql(self.binding(), owner.binding)) return error.SchemaValidationRuntimeRecoveryRequired;
            // Validate historical epochs against the decisions that existed
            // at adoption, not a later installation/publication of this job.
            try self.requireZeroProgress(head, owner);
            try scope.requireBinding(owner.binding);
            return;
        }
        const audited = self.proposal.audited;
        const observed = audited.observation.rooted.observation;
        if (!std.meta.eql(observed.binding, owner.binding) or observed.header == null or
            !std.mem.eql(u8, &try observed.header.?.digest(), &try audited.destination.value.rows.header.digest()) or
            audited.destination.value.rows.snapshot_cut != .raft or
            audited.destination.value.rows.snapshot_cut.raft.index > observed.applied.?.index or
            audited.destination.value.rows.snapshot_cut.raft.term > observed.applied.?.term) return error.SchemaValidationRuntimeRecoveryRequired;
        switch (self.lineage) {
            .retained_parent => |grant| {
                const source = retained orelse return error.GenerationPublicationChanged;
                if (!std.mem.eql(u8, &grant, &try source.id()) or source.ordinal != self.ordinal or
                    !std.meta.eql(source.lookup, self.lookup)) return error.GenerationPublicationChanged;
                try source.requireDestination(head, owner, audited.destination, scope);
            },
            .raft_replay => {
                if (retained != null or audited.destination.value.physical_root == owner.physical_root_incarnation or
                    audited.destination.value.physical_root != scope.physical_root_incarnation or
                    audited.destination.value.signing_root != scope.identity.root_incarnation) return error.SchemaValidationRuntimeRecoveryRequired;
                try scope.requireBinding(owner.binding);
                try audited.destination.verify(scope.identity.public_key);
                try parent.requireHeaderDecisions(head, audited.destination.value.rows.header);
                try requireReplayPhase(head, owner, audited.destination.value.rows.header);
            },
            .native_recovery => unreachable,
        }
    }
};

pub fn requireReplayPhase(head: barriers.Head, owner: barriers.Owner, header: contract.Header) !void {
    const canceling = head.phase == .canceling or head.phase == .canceled;
    if ((header.phase == .canceling or header.phase == .canceled) and !canceling) return error.SchemaValidationRuntimeRecoveryRequired;
    const digest = try header.digest();
    for ([_]barriers.Stage{ .released, .canceled, .installed, .failed, .ready }) |stage| {
        if (!owner.has(stage)) continue;
        const accepted = owner.headers[@backingInt(stage)];
        if (std.mem.eql(u8, &digest, &accepted)) return;
        if (header.release) |release| if (std.mem.eql(u8, &release.installed_digest, &accepted)) return;
        if (header.installation) |installed| if (std.mem.eql(u8, &installed.ready_digest, &accepted)) return;
        // Cancellation cannot publish a candidate. Its metadata decision and
        // certified replay coverage of ALL prior ACK cuts are mandatory; this
        // is not a phase-order shortcut for installation/publication.
        if ((stage == .failed or stage == .ready) and (header.phase == .canceling or header.phase == .canceled) and
            canceling and head.install_revision == 0) return;
        return error.SchemaValidationRuntimeRecoveryRequired;
    }
    if (header.phase == .installed or header.phase == .released) return error.SchemaValidationRuntimeRecoveryRequired;
}

pub fn includeCut(previous: ?Cut, value: Cut) !Cut {
    try value.validate();
    const before = previous orelse return value;
    try before.validate();
    if (std.meta.eql(value, before) or before.strictlyAfter(value)) return before;
    if (value.strictlyAfter(before)) return value;
    return error.InvalidControlReceiptPosition;
}

pub const Epoch = struct {
    request: Request,
    source_owner: barriers.Owner,
    /// Recomputed from immutable original preparation and archived signed ACKs
    /// by the accepting writer/reverse audit, never accepted from the caller.
    required_cut: ?Cut = null,

    pub fn number(self: @This()) u64 {
        return self.request.expected_epoch + 1;
    }
    pub fn digest(self: @This()) !Digest {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/schema-validation-accepted-root-epoch/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(state, &.{});
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }
    pub fn validate(self: @This(), head: barriers.Head, retained: ?parent.Request) !void {
        if (self.request.revision >= head.revision) return error.InvalidGenerationPublication;
        try self.source_owner.validate(head);
        for (self.source_owner.accepted_revisions) |revision| if (revision > self.request.revision) return error.InvalidGenerationPublication;
        try self.request.requireDestination(head, self.source_owner, retained);
        if (self.request.lineage != .retained_parent and self.required_cut == null) {
            for (std.meta.tags(barriers.Stage)) |stage| if (self.source_owner.has(stage)) return error.InvalidGenerationPublication;
        }
        if (self.request.lineage == .retained_parent) {
            if (self.required_cut != null) return error.InvalidGenerationPublication;
        } else if (self.required_cut) |required| {
            const audited = (try self.request.sourceCut()) orelse return error.InvalidControlReceiptPosition;
            try required.requireNamespace(self.source_owner.binding.fence.namespace);
            if (!std.meta.eql(audited, required) and !audited.strictlyAfter(required)) return error.InvalidControlReceiptPosition;
            if (self.request.lineage == .raft_replay) {
                if (required != .raft) return error.InvalidControlReceiptPosition;
                const completed_prefix = self.request.completed() orelse return error.InvalidControlReceiptPosition;
                if (completed_prefix.applied_index < required.raft.index or completed_prefix.term < required.raft.term) return error.InvalidControlReceiptPosition;
            } else if (required != .native) return error.InvalidControlReceiptPosition;
        }
    }
    pub fn successor(self: @This(), head: barriers.Head) !barriers.Owner {
        var result = self.source_owner;
        result.root_origin = result.initialRoot();
        result.root_epoch = self.number();
        result.root_digest = try self.digest();
        result.physical_root_incarnation = self.request.physicalRoot();
        try result.validate(head);
        return result;
    }
};
pub const EpochRecord = barriers.Record(Epoch, "SVRE", 64 * 1024);
pub const HistoricalProof = struct { epoch: u64, first_saved_epoch: u64, signed: evidence.SignedObservation };
pub const HistoricalProofRecord = barriers.Record(HistoricalProof, "SVRH", 48 * 1024);
