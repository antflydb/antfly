// Copyright 2026 Antfly, Inc.
// SPDX-License-Identifier: Apache-2.0
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! Retained pre-transfer source authorization. This is neither an owner ACK
//! nor root-adoption permission: consumption must compare the exact owner/head
//! predecessor and independently qualify the installed destination image.
const std = @import("std");
const local = @import("antfly_local_sources");
const read = @import("schema_validation_read.zig");
const barriers = @import("schema_validation_publication.zig");
const evidence = @import("schema_validation_attestation.zig");
const contract = local.storage_db_relational_schema_owner_contract;
pub const Digest = [32]u8;

pub const Request = struct {
    lookup: read.Lookup,
    ordinal: u16,
    root_epoch: u64 = 0,
    revision: u64,
    owner_digest: Digest,
    signed: evidence.SignedSnapshotParent,
    source_header: ?contract.Header = null,

    pub fn validate(self: @This()) !void {
        try self.validateShape();
        try self.signed.verifyFor(self.signed.value.binding, self.signed.value.origin.physical_root_incarnation);
    }

    /// Decoder bounds and image consistency, not signature/enrollment authority.
    /// The accepting writer and historical reader verify the signature afresh.
    pub fn validateShape(self: @This()) !void {
        try (read.OwnerReadRequest{ .lookup = self.lookup, .ordinal = self.ordinal }).validate();
        if (self.revision == 0 or std.mem.allEqual(u8, &self.owner_digest, 0) or
            !std.mem.eql(u8, &self.signed.proof.identity.metadata_incarnation, &self.lookup.metadata_incarnation)) return error.InvalidGenerationPublication;
        try self.signed.value.validate();
        const namespace = self.signed.value.origin.namespace orelse return error.SchemaValidationRuntimeRecoveryRequired;
        if (!namespace.eql(self.signed.value.binding.fence.namespace)) return error.SchemaValidationRuntimeRecoveryRequired;
        if (self.signed.value.binding.fence.namespace.table_id != self.lookup.table or
            !std.mem.eql(u8, &self.signed.value.binding.id, &self.lookup.id) or
            !std.mem.eql(u8, &self.signed.value.binding.plan_digest, &self.lookup.plan_digest)) return error.GenerationPublicationChanged;
        if (self.source_header) |header| {
            if (!std.meta.eql(header.binding, self.signed.value.binding)) return error.SchemaValidationJobChanged;
            var buffer: [contract.max_header + 44]u8 = undefined;
            var fixed = std.heap.FixedBufferAllocator.init(&buffer);
            const encoded = try header.encode(fixed.allocator());
            const committed = self.signed.value.origin.schema_header orelse return error.SchemaValidationJobChanged;
            if (!std.mem.eql(u8, &committed, &contract.hash(encoded))) return error.SchemaValidationJobChanged;
            if (header.updated != .raft or header.updated.raft.index > self.signed.value.through_index or
                header.updated.raft.term > self.signed.value.completed.term) return error.InvalidControlReceiptPosition;
        } else if (self.signed.value.origin.schema_header != null) return error.SchemaValidationJobChanged;
    }

    pub fn id(self: @This()) !Digest {
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/schema-validation-parent-authorization/v1");
        var writer = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(state, &.{});
        try std.json.Stringify.value(self, .{}, &writer.writer);
        var result: Digest = undefined;
        writer.hasher.final(&result);
        return result;
    }

    pub fn image(self: @This()) !read.ImageIdentity {
        return .{ .namespace = local.storage_db_online_source_contract.namespaceBytes(self.signed.value.binding.fence.namespace), .source_root = self.signed.value.origin.physical_root_incarnation, .through_index = self.signed.value.through_index, .stream_digest = self.signed.value.stream_digest, .origin_digest = try self.signed.value.origin.digest() };
    }

    pub fn requireScope(self: @This(), head: barriers.Head, owner: barriers.Owner) !void {
        try self.validate();
        try self.lookup.require(head);
        try owner.validate(head);
        if (self.ordinal != owner.ordinal or self.revision > head.revision) return error.GenerationPublicationChanged;
        if (self.root_epoch != owner.root_epoch) return error.SchemaValidationRuntimeRecoveryRequired;
        if (!std.meta.eql(self.signed.value.binding, owner.binding)) return error.SchemaValidationJobChanged;
        if (self.signed.value.origin.physical_root_incarnation != owner.physical_root_incarnation)
            return error.SchemaValidationRuntimeRecoveryRequired;
    }

    /// New acceptance compares the requested head revision in the writer.
    /// Adoption must supply its own current head CAS; unrelated peer-owner
    /// progress must not force another source transfer. Neither helper grants
    /// adoption or execution without that separately qualified transaction.
    pub fn requireCurrent(self: @This(), head: barriers.Head, owner: barriers.Owner) !void {
        try self.requireScope(head, owner);
        if (self.revision != head.revision) return error.GenerationPublicationChanged;
        if (!std.mem.eql(u8, &self.owner_digest, &try ownerDigest(owner, head))) return error.GenerationPublicationChanged;
        try self.requireImagePhase(head, owner);
    }

    /// Only after loading a durable authorization. A fresh adoption transaction
    /// must ALSO compare its current owner/head and qualify the destination.
    /// Re-attesting the same logical header does not invalidate source bytes.
    pub fn requirePredecessor(self: @This(), head: barriers.Head, owner: barriers.Owner) !void {
        try self.requireScope(head, owner);
        try self.requireImagePhase(head, owner);
    }

    /// Destination qualification for a RETAINED parent authorization. The
    /// accepting writer must separately load that authorization, compare its
    /// current owner/head CAS, and persist immutable root-epoch history.
    /// `scope` comes from live enrollment/placement/report reads in that SAME
    /// writer; neither the source signature nor this envelope grants it.
    pub fn requireDestination(self: @This(), head: barriers.Head, owner: barriers.Owner, destination: local.storage_db_relational_schema_root_contract.Evidence, scope: evidence.RaftScope) !void {
        try self.requirePredecessor(head, owner);
        try scope.requireBinding(owner.binding);
        if (!std.mem.eql(u8, &scope.metadata_incarnation, &self.lookup.metadata_incarnation) or
            destination.value.signing_root != scope.identity.root_incarnation or
            destination.value.physical_root != scope.physical_root_incarnation or
            destination.value.physical_root == owner.physical_root_incarnation)
            return error.SchemaValidationRuntimeRecoveryRequired;
        try destination.verify(scope.identity.public_key);
        const facts = destination.value;
        if (!std.meta.eql(facts.rows.header.binding, owner.binding) or facts.rows.snapshot_cut != .raft)
            return error.SchemaValidationJobChanged;
        const installed = facts.qualification.installation orelse return error.SchemaValidationRuntimeRecoveryRequired;
        try self.signed.requireInstallation(owner.binding, installed);
        const cut = facts.rows.snapshot_cut.raft;
        if (scope.reported_applied_index < @max(cut.index, self.signed.value.through_index) or
            scope.reported_term < @max(cut.term, self.signed.value.completed.term)) return error.IndexOwnerReceiptChanged;
        const source = self.source_header orelse return error.SchemaValidationJobRequired;
        try requireHeaderLineage(source, facts.rows.header);
        // Imported replay can be ahead of the source header. Its canonical
        // ancestry is insufficient unless its decisions are THIS metadata
        // job's durable decisions, including a not-yet-acknowledged install.
        try requireHeaderDecisions(head, facts.rows.header);
    }

    fn requireImagePhase(self: @This(), head: barriers.Head, owner: barriers.Owner) !void {
        if (self.source_header) |header| try requireHeaderDecisions(head, header);
        for ([_]barriers.Stage{ .released, .canceled, .installed, .failed, .ready }) |stage| {
            if (!owner.has(stage)) continue;
            const header = self.source_header orelse return error.SchemaValidationRuntimeRecoveryRequired;
            const expected = owner.headers[@backingInt(stage)];
            if (std.mem.eql(u8, &try header.digest(), &expected)) return;
            // Header.validate reconstructs these exact predecessors from the
            // same counters, positions and effects. A lost install/release ACK
            // must not make an otherwise qualified source image unusable.
            const ancestor: ?Digest = switch (stage) {
                .ready => if (header.installation) |value| value.ready_digest else null,
                .installed => if (header.release) |value| value.installed_digest else null,
                else => null,
            };
            if (ancestor) |digest| if (std.mem.eql(u8, &digest, &expected)) return;
            return error.SchemaValidationRuntimeRecoveryRequired;
        }
        if (self.source_header) |header| if (header.phase == .installed or header.phase == .released)
            return error.SchemaValidationRuntimeRecoveryRequired;
    }
};

/// A canonical local history must still name metadata's exact decisions.
/// In particular forward ancestry cannot create an installation/publication
/// decision which this durable metadata head has never made.
pub fn requireHeaderDecisions(head: barriers.Head, header: contract.Header) !void {
    try head.validate();
    try header.validate();
    if (!std.mem.eql(u8, &head.id, &header.binding.id) or
        !std.mem.eql(u8, &head.plan_digest, &header.binding.plan_digest) or
        head.table_id != header.binding.fence.namespace.table_id) return error.SchemaValidationJobChanged;
    if (header.installation) |installed| {
        if (head.install_revision == 0 or !std.mem.eql(u8, &installed.decision, &try head.installDecision()))
            return error.SchemaValidationRuntimeRecoveryRequired;
    }
    if (header.release) |released| {
        if (head.publication_revision == 0 or !std.mem.eql(u8, &released.publication, &try head.publicationDecision()))
            return error.SchemaValidationRuntimeRecoveryRequired;
    }
}

/// Only exact canonical predecessors are admissible. Phase ordering, larger
/// row counts and a later Raft index do not prove inclusion of the source.
/// Cancellation/failure and pre-begin ancestry require their retained history
/// proofs; they must not be guessed from an ordinal comparison.
pub fn requireHeaderLineage(source: contract.Header, destination: contract.Header) !void {
    try source.validate();
    try destination.validate();
    if (!std.meta.eql(source.binding, destination.binding)) return error.SchemaValidationJobChanged;
    const digest = try source.digest();
    if (std.mem.eql(u8, &digest, &try destination.digest())) return;
    if (destination.release) |released| if (std.mem.eql(u8, &digest, &released.installed_digest)) return;
    if (destination.installation) |installed| if (std.mem.eql(u8, &digest, &installed.ready_digest)) return;
    return error.SchemaValidationRuntimeRecoveryRequired;
}

pub fn ownerDigest(owner: barriers.Owner, head: barriers.Head) !Digest {
    const encoded = try owner.encode(head);
    return contract.hash(encoded.slice());
}

pub const Record = barriers.Record(Request, "SVPA", 48 * 1024);
