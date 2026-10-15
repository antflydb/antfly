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

//! Immutable regular UNIQUE-build plan and owned metadata admission postimage.
//! No public operation is enabled here. Admission must independently verify
//! current catalog/name authority, the complete live owner cut, capabilities
//! and conflicting lifecycle jobs before staging these records in ONE txn.
const std = @import("std");
const local = @import("antfly_local_sources");
const records = local.common_topology_records;
const names = local.system_catalog_relation_names;
const domain = local.system_catalog_domain;
const manager = @import("table_manager.zig");
const progress = local.storage_db_relational_index_publication_progress;
const candidate_mod = @import("relational_index_candidate.zig");
const shape = @import("schema_publication_shape.zig");
const lifecycle = @import("table_lifecycle_lock.zig");
const Allocator = std.mem.Allocator;
const native_control = local.storage_db_relational_index_owner_control;
const readers = local.common_schema_readers;
const root_enrollment = @import("store_root_enrollment.zig");

fn readerRequirements(before: records.TableRecord, after: records.TableRecord) !?readers.ReaderRequirements {
    var joined: readers.ReaderFloor = .{};
    if (before.schema_reader_requirements) |requirements| joined = try joined.include(requirements);
    if (after.schema_reader_requirements) |requirements| joined = try joined.include(requirements);
    return if (joined.active()) .{ .semantic_version = joined.semantic_version, .encoding_version = joined.encoding_version } else null;
}
pub const Id = progress.Id;
pub const Digest = progress.Digest;
/// A decision selects only the already admitted immutable successor. It cannot
/// introduce a different schema, owner cohort or validation receipt.
pub const DecidePublish = struct {
    table_id: u64,
    id: Id,
    expected_revision: u64,

    pub fn validate(self: @This()) !void {
        if (self.table_id == 0 or std.mem.allEqual(u8, &self.id, 0) or self.expected_revision == 0 or
            self.expected_revision == std.math.maxInt(u64)) return error.InvalidGenerationPublication;
    }
};
pub const RootProof = @import("relational_index_root_attestation.zig").Proof;

/// Exact observation of an admitted job's irreversible decision. A metadata
/// outbox obligation withholds completion even if the local head advanced.
pub const DecisionStatus = struct {
    pub const max_wire_bytes = 2048;
    head: progress.Head,
    replication_pending: bool = false,

    pub fn isDecided(self: @This(), request: DecidePublish) !bool {
        try request.validate();
        try self.head.validate();
        if (self.head.table_id != request.table_id or !std.mem.eql(u8, &self.head.plan_id, &request.id) or
            self.head.revision < request.expected_revision) return error.GenerationPublicationChanged;
        const decided = self.head.phase == .installing or self.head.phase == .releasing or self.head.phase == .published;
        if (decided and self.head.revision <= request.expected_revision) return error.GenerationPublicationChanged;
        return decided and !self.replication_pending;
    }
};

test "relational index publication decision requires an exact nonzero job and usable revision" {
    const valid: DecidePublish = .{ .table_id = 1, .id = @splat(2), .expected_revision = 3 };
    try valid.validate();
    var invalid = valid;
    invalid.table_id = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.validate());
    invalid = valid;
    invalid.id = @splat(0);
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.validate());
    invalid = valid;
    invalid.expected_revision = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.validate());
    invalid.expected_revision = std.math.maxInt(u64);
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.validate());
}

/// Compact physical witness. The outcome is carried once in the enclosing
/// progress reply; the signature binds every field through its ready digest.
/// This is not a logical validation ACK or an installation/publication grant.
pub const SignedAudit = struct {
    outcome_digest: Digest,
    proof: RootProof,
    pub fn verifiedDigest(self: @This(), outcome: native_control.AuditOutcome) !Digest {
        const actual = try outcome.readyDigest();
        if (self.proof.authority != .native or !std.mem.eql(u8, &actual, &self.outcome_digest) or
            self.proof.identity.root_incarnation != outcome.audit.root_incarnation or
            !std.mem.eql(u8, &self.proof.identity.metadata_incarnation, &outcome.expected.metadata_incarnation)) return error.InvalidIndexOwnerReceipt;
        try (NativeSigningView{ .identity = self.proof.identity, .reporter_incarnation = self.proof.reporter_incarnation, .binding_digest = self.proof.authority.native }).validate(outcome.expected.metadata_incarnation);
        self.proof.verify(actual) catch return error.InvalidIndexOwnerReceipt;
        return actual;
    }
    pub fn commitment(self: @This(), outcome: native_control.AuditOutcome) !Digest {
        return self.proof.commitment(try self.verifiedDigest(outcome));
    }
};

test {
    _ = @import("relational_index_work.zig");
    _ = @import("relational_index_worker.zig");
}

/// Provenance for one actual canonical source batch. This is not a source ACK,
/// complete-stream seal, or current placement/enrollment authority.
pub const SignedSourceBatch = struct {
    binding: local.storage_db_relational_index_owner_job.Binding,
    header: local.storage_db_relational_index_owner_job.private_claims.SourceStream.Header,
    proof: RootProof,

    pub fn verifyFor(self: SignedSourceBatch, read: *const native_control.SourceRead) !void {
        _ = try self.verifiedDigest(read);
    }

    /// Retain the authenticated digest across current-authority reads so an
    /// accepting transaction need not hash the complete body a second time.
    pub fn verifiedDigest(self: SignedSourceBatch, read: *const native_control.SourceRead) !Digest {
        const batch = if (read.batch) |*value| value else return error.InvalidPrivateIndexClaimReceipt;
        if (self.proof.authority != .native or !std.mem.eql(u8, &try self.binding.encode(), &try read.binding.encode()) or
            !std.mem.eql(u8, &self.header.bytes, &batch.header.bytes)) return error.InvalidPrivateIndexClaimReceipt;
        (NativeSigningView{ .identity = self.proof.identity, .reporter_incarnation = self.proof.reporter_incarnation, .binding_digest = self.proof.authority.native }).validate(self.proof.identity.metadata_incarnation) catch return error.InvalidPrivateIndexClaimReceipt;
        const logical_digest = try read.digest();
        self.proof.verify(logical_digest) catch return error.InvalidPrivateIndexClaimReceipt;
        return logical_digest;
    }

    pub fn commitment(self: SignedSourceBatch, read: *const native_control.SourceRead) !Digest {
        return self.proof.commitment(try self.verifiedDigest(read));
    }
};

/// Exact durable target delivery, not a source ACK or publication receipt.
/// Verification must additionally read current enrollment, placement, root and
/// observed cut in the transaction that will accept the delivery.
pub const SignedClaimDelivery = struct {
    outcome: native_control.ClaimOutcome,
    proof: RootProof,

    pub fn verifyFor(self: SignedClaimDelivery, command: native_control.ClaimCommand) !void {
        try self.outcome.validateFor(command);
        if (self.proof.authority != .native) return error.InvalidPrivateIndexClaimReceipt;
        (NativeSigningView{ .identity = self.proof.identity, .reporter_incarnation = self.proof.reporter_incarnation, .binding_digest = self.proof.authority.native }).validate(self.proof.identity.metadata_incarnation) catch
            return error.InvalidPrivateIndexClaimReceipt;
        self.proof.verify(try self.outcome.deliveryDigest()) catch return error.InvalidPrivateIndexClaimReceipt;
    }

    pub fn verifyPartition(self: SignedClaimDelivery, plan: *const local.storage_db_relational_index_owner_job.private_claims.delivery.Plan, cohort: anytype, group_index: usize) !void {
        try plan.requireTarget(cohort, group_index, self.outcome.binding);
        try self.verifyFor(.{ .binding = self.outcome.binding, .claims = try plan.groupClaims(group_index) });
    }

    pub fn commitment(self: SignedClaimDelivery) !Digest {
        return self.proof.commitment(try self.outcome.deliveryDigest());
    }

    /// Use only within a source-root-signed complete delivery statement, which
    /// authenticates the relation between this exact set and actual source rows.
    pub fn verifyRecorded(self: SignedClaimDelivery, logical_rows: usize) !void {
        try self.validateRecordedShape(logical_rows);
        self.proof.verify(try self.outcome.deliveryDigest()) catch return error.InvalidPrivateIndexClaimReceipt;
    }

    pub fn validateRecordedShape(self: SignedClaimDelivery, logical_rows: usize) !void {
        if (logical_rows == 0 or logical_rows > local.storage_db_relational_index_owner_job.private_claims.max_claims or
            self.outcome.claims_added > logical_rows or self.outcome.duplicate != (self.outcome.claims_added == 0) or self.proof.authority != .native)
            return error.InvalidPrivateIndexClaimReceipt;
        (NativeSigningView{ .identity = self.proof.identity, .reporter_incarnation = self.proof.reporter_incarnation, .binding_digest = self.proof.authority.native }).validate(self.proof.identity.metadata_incarnation) catch return error.InvalidPrivateIndexClaimReceipt;
        _ = try self.outcome.deliveryDigest();
    }
};

/// Signature verification alone is not authority. The metadata apply adapter
/// must also check the enrolled current root/reporter and exact live placement
/// in the SAME transaction as its progress CAS. Never accept a service ACK or
/// caller-supplied verification boolean in place of those reads.
pub const SignedReceipt = struct {
    pub const encoded_len = 512;
    receipt: progress.Receipt,
    proof: RootProof,

    pub fn commitment(self: SignedReceipt) !Digest {
        return self.proof.commitment(try self.receipt.digest());
    }

    fn validateShape(self: SignedReceipt) !void {
        self.receipt.owner.validate() catch return error.InvalidIndexOwnerReceipt;
        self.receipt.position.requireNamespace(self.receipt.owner.namespace) catch return error.InvalidIndexOwnerReceipt;
        self.proof.identity.validate() catch return error.InvalidIndexOwnerReceipt;
        if ((self.receipt.owner.authority == .native) != (self.receipt.position == .native) or
            self.proof.reporter_incarnation == 0 or self.proof.root_generation == 0 or
            std.mem.allEqual(u8, &self.receipt.candidate_digest, 0) or std.mem.allEqual(u8, &self.receipt.reservation_digest, 0) or
            std.mem.allEqual(u8, &self.receipt.evidence_digest, 0)) return error.InvalidIndexOwnerReceipt;
        if (self.receipt.owner.authority != switch (self.proof.authority) {
            .raft => progress.Authority.raft,
            .native => progress.Authority.native,
        }) return error.InvalidIndexOwnerReceipt;
        switch (self.proof.authority) {
            .raft => |replica| if (replica == 0) return error.InvalidIndexOwnerReceipt,
            .native => |binding| if (std.mem.allEqual(u8, &binding, 0)) return error.InvalidIndexOwnerReceipt,
        }
    }

    pub fn verifySignature(self: SignedReceipt) !void {
        try self.validateShape();
        const logical_digest = self.receipt.digest() catch return error.InvalidIndexOwnerReceipt;
        self.proof.verify(logical_digest) catch return error.InvalidIndexOwnerReceipt;
    }

    /// Canonical physical representation, not an ownership grant. Admission
    /// verifies the signature/current root before staging this fixed record.
    pub fn encode(self: SignedReceipt) ![encoded_len]u8 {
        try self.validateShape();
        var bytes: [encoded_len]u8 = @splat(0);
        @memcpy(bytes[0..4], "AIPR");
        bytes[4] = 1;
        bytes[5] = @backingInt(self.receipt.action);
        bytes[6] = @backingInt(self.receipt.owner.authority);
        @memcpy(bytes[8..24], &self.receipt.owner.plan_id);
        @memcpy(bytes[24..56], &self.receipt.owner.plan_digest);
        std.mem.writeInt(u16, bytes[56..58], self.receipt.owner.ordinal, .little);
        for ([_]u64{ self.receipt.owner.group_id, self.receipt.owner.namespace.table_id, self.receipt.owner.namespace.shard_id, self.receipt.owner.namespace.range_id }, 0..) |value, i|
            std.mem.writeInt(u64, bytes[64 + i * 8 ..][0..8], value, .little);
        @memcpy(bytes[96..128], &self.receipt.owner.fence_digest);
        @memcpy(bytes[128..161], &try self.receipt.position.encode());
        @memcpy(bytes[168..200], &self.receipt.candidate_digest);
        @memcpy(bytes[200..232], &self.receipt.reservation_digest);
        @memcpy(bytes[232..264], &self.receipt.evidence_digest);
        @memcpy(bytes[264..296], &self.proof.identity.metadata_incarnation);
        std.mem.writeInt(u64, bytes[296..304], self.proof.identity.node_id, .little);
        std.mem.writeInt(u64, bytes[304..312], self.proof.identity.store_id, .little);
        std.mem.writeInt(u128, bytes[312..328], self.proof.identity.root_incarnation, .little);
        @memcpy(bytes[328..360], &self.proof.identity.public_key);
        std.mem.writeInt(u64, bytes[360..368], self.proof.reporter_incarnation, .little);
        bytes[368] = @backingInt(self.receipt.owner.authority);
        switch (self.proof.authority) {
            .raft => |replica| std.mem.writeInt(u64, bytes[376..384], replica, .little),
            .native => |binding| @memcpy(bytes[376..408], &binding),
        }
        std.mem.writeInt(u64, bytes[408..416], self.proof.root_generation, .little);
        @memcpy(bytes[416..480], &self.proof.signature);
        @memcpy(bytes[480..512], &digest(bytes[0..480]));
        return bytes;
    }

    pub fn decode(bytes: []const u8) !SignedReceipt {
        if (bytes.len != encoded_len or !std.mem.eql(u8, bytes[0..4], "AIPR") or bytes[4] != 1 or bytes[7] != 0 or
            !std.mem.allEqual(u8, bytes[58..64], 0) or !std.mem.allEqual(u8, bytes[161..168], 0) or
            !std.mem.allEqual(u8, bytes[369..376], 0) or !std.mem.eql(u8, &digest(bytes[0..480]), bytes[480..512])) return error.InvalidCatalogRecord;
        if (bytes[368] == @backingInt(progress.Authority.raft) and !std.mem.allEqual(u8, bytes[384..408], 0)) return error.InvalidCatalogRecord;
        const value: SignedReceipt = .{
            .receipt = .{
                .owner = .{
                    .plan_id = bytes[8..24].*,
                    .plan_digest = bytes[24..56].*,
                    .ordinal = std.mem.readInt(u16, bytes[56..58], .little),
                    .group_id = std.mem.readInt(u64, bytes[64..72], .little),
                    .namespace = .{ .table_id = std.mem.readInt(u64, bytes[72..80], .little), .shard_id = std.mem.readInt(u64, bytes[80..88], .little), .range_id = std.mem.readInt(u64, bytes[88..96], .little) },
                    .authority = std.enums.fromInt(progress.Authority, bytes[6]) orelse return error.InvalidCatalogRecord,
                    .fence_digest = bytes[96..128].*,
                },
                .action = std.enums.fromInt(progress.Action, bytes[5]) orelse return error.InvalidCatalogRecord,
                .position = local.storage_db_receipt_position.Position.decode(bytes[128..161]) catch return error.InvalidCatalogRecord,
                .candidate_digest = bytes[168..200].*,
                .reservation_digest = bytes[200..232].*,
                .evidence_digest = bytes[232..264].*,
            },
            .proof = .{
                .identity = .{ .metadata_incarnation = bytes[264..296].*, .node_id = std.mem.readInt(u64, bytes[296..304], .little), .store_id = std.mem.readInt(u64, bytes[304..312], .little), .root_incarnation = std.mem.readInt(u128, bytes[312..328], .little), .public_key = bytes[328..360].* },
                .reporter_incarnation = std.mem.readInt(u64, bytes[360..368], .little),
                .authority = switch (bytes[368]) {
                    1 => .{ .raft = std.mem.readInt(u64, bytes[376..384], .little) },
                    2 => .{ .native = bytes[376..408].* },
                    else => return error.InvalidCatalogRecord,
                },
                .root_generation = std.mem.readInt(u64, bytes[408..416], .little),
                .signature = bytes[416..480].*,
            },
        };
        value.verifySignature() catch return error.InvalidCatalogRecord;
        return value;
    }
};
pub const max_bytes = 8 * 1024 * 1024;
pub const max_command_bytes = max_bytes + 16 * 1024;
pub const max_ack_bytes = 32 * 1024;

pub const ValidationWitness = struct {
    outcome: native_control.AuditOutcome,
    signed: SignedAudit,

    /// Consistency and signatures only. Accepting apply must independently
    /// join frozen provenance and current root authority in its transaction.
    pub fn verifyFor(self: @This(), receipt: SignedReceipt) !void {
        if (receipt.receipt.action != .validated or receipt.receipt.position != .native) return error.InvalidIndexOwnerReceipt;
        try receipt.verifySignature();
        return self.verifyPhysicalAndScope(receipt);
    }
    fn verifyPhysicalAndScope(self: @This(), receipt: SignedReceipt) !void {
        const command = try self.outcome.logicalCommand();
        if (!std.meta.eql(try command.receipt(receipt.receipt.position.native), receipt.receipt)) return error.InvalidIndexOwnerReceipt;
        try (AuditEvidence{ .outcome = self.outcome, .signed = self.signed }).verifyScope(receipt);
    }
};

pub const AuditEvidence = struct {
    outcome: native_control.AuditOutcome,
    signed: SignedAudit,

    fn verifyScope(self: @This(), receipt: SignedReceipt) !void {
        _ = try self.signed.verifiedDigest(self.outcome);
        if (!std.meta.eql(self.signed.proof.identity, receipt.proof.identity) or
            !std.meta.eql(self.signed.proof.authority, receipt.proof.authority) or
            self.signed.proof.reporter_incarnation != receipt.proof.reporter_incarnation or
            self.signed.proof.root_generation != receipt.proof.root_generation) return error.InvalidIndexOwnerReceipt;
    }
};

/// The installed logical cut is distinct from the original generation audit.
/// Metadata must additionally join this command to its retained validation and
/// durable publication decision, under the successor catalog's ownership.
pub const InstallationWitness = struct {
    outcome: native_control.AuditOutcome,
    command: local.storage_db_relational_index_validation.InstallCommand,
    signed: SignedAudit,

    pub fn verifyFor(self: @This(), receipt: SignedReceipt) !void {
        if (receipt.receipt.action != .installed or receipt.receipt.position != .native) return error.InvalidIndexOwnerReceipt;
        try receipt.verifySignature();
        try self.verifyPhysicalAndScope(receipt);
    }
    fn verifyPhysicalAndScope(self: @This(), receipt: SignedReceipt) !void {
        try self.command.validate();
        if (!std.meta.eql(self.command.validation.command, try self.outcome.logicalCommand()) or
            !std.meta.eql(try self.command.receipt(receipt.receipt.position.native), receipt.receipt)) return error.InvalidIndexOwnerReceipt;
        try (AuditEvidence{ .outcome = self.outcome, .signed = self.signed }).verifyScope(receipt);
    }
};

fn requireWitnesses(signed: SignedReceipt, validation: ?ValidationWitness, installation: ?InstallationWitness) !void {
    const is_native = signed.receipt.owner.authority == .native;
    if ((is_native and signed.receipt.action == .validated) != (validation != null) or
        (is_native and signed.receipt.action == .installed) != (installation != null)) return error.InvalidIndexOwnerReceipt;
}

/// One retained stage record, not a second independently committed journal.
/// Native validation/installation retain mandatory bounded physical witnesses;
/// neither bare receipts nor older private witness formats are accepted.
pub const StoredProof = struct {
    const magic = "AIVP\x02\x00\x00\x00";
    bytes: [max_ack_bytes]u8,
    len: usize,
    const Witnesses = struct { validation: ?ValidationWitness, installation: ?InstallationWitness };
    pub const Decoded = struct { signed: SignedReceipt, validation: ?ValidationWitness, installation: ?InstallationWitness = null };

    pub fn slice(self: *const @This()) []const u8 {
        return self.bytes[0..self.len];
    }
    pub fn encode(signed: SignedReceipt, validation: ?ValidationWitness, installation: ?InstallationWitness) !@This() {
        try requireWitnesses(signed, validation, installation);
        if (validation) |witness| try witness.verifyFor(signed) else if (installation) |witness| try witness.verifyFor(signed) else try signed.verifySignature();
        return encodeCanonical(signed, validation, installation);
    }
    fn encodeCanonical(signed: SignedReceipt, validation: ?ValidationWitness, installation: ?InstallationWitness) !StoredProof {
        var result: @This() = .{ .bytes = undefined, .len = 0 };
        const fixed = try signed.encode();
        if (validation != null or installation != null) {
            @memcpy(result.bytes[0..8], magic);
            @memcpy(result.bytes[8..][0..SignedReceipt.encoded_len], &fixed);
            var writer = std.Io.Writer.fixed(result.bytes[8 + SignedReceipt.encoded_len ..]);
            std.json.Stringify.value(Witnesses{ .validation = validation, .installation = installation }, .{}, &writer) catch return error.InvalidIndexOwnerReceipt;
            result.len = 8 + SignedReceipt.encoded_len + writer.end;
        } else {
            @memcpy(result.bytes[0..SignedReceipt.encoded_len], &fixed);
            result.len = fixed.len;
        }
        return result;
    }
    pub fn decode(alloc: Allocator, raw: []const u8) !Decoded {
        if (raw.len == SignedReceipt.encoded_len) {
            const signed = try SignedReceipt.decode(raw);
            requireWitnesses(signed, null, null) catch return error.InvalidCatalogRecord;
            return .{ .signed = signed, .validation = null };
        }
        if (raw.len <= 8 + SignedReceipt.encoded_len or raw.len > max_ack_bytes or !std.mem.eql(u8, raw[0..8], magic)) return error.InvalidCatalogRecord;
        const signed = try SignedReceipt.decode(raw[8..][0..SignedReceipt.encoded_len]);
        var parsed = std.json.parseFromSlice(Witnesses, alloc, raw[8 + SignedReceipt.encoded_len ..], .{ .ignore_unknown_fields = false, .max_value_len = 128 }) catch |err| switch (err) {
            error.OutOfMemory => return err,
            else => return error.InvalidCatalogRecord,
        };
        defer parsed.deinit();
        // The fixed receipt decoder already verified its logical signature.
        const witnesses = parsed.value;
        requireWitnesses(signed, witnesses.validation, witnesses.installation) catch return error.InvalidCatalogRecord;
        if (witnesses.validation) |witness| witness.verifyPhysicalAndScope(signed) catch return error.InvalidCatalogRecord;
        if (witnesses.installation) |witness| witness.verifyPhysicalAndScope(signed) catch return error.InvalidCatalogRecord;
        const canonical = try encodeCanonical(signed, witnesses.validation, witnesses.installation);
        if (!std.mem.eql(u8, raw, canonical.slice())) return error.InvalidCatalogRecord;
        return .{ .signed = signed, .validation = witnesses.validation, .installation = witnesses.installation };
    }
};
pub const max_ack_status_bytes = 8192;

/// Only use for reads or definitely-not-proposed mutations. The transport
/// outcome header takes precedence over any apparently retryable error body.
pub fn ownerAckHttpFailure(status: u16, body: []const u8) ?anyerror {
    const failures: []const anyerror = switch (status) {
        400 => &.{ error.InvalidIndexOwnerReceipt, error.InvalidGenerationPublication },
        409 => &.{ error.IndexOwnerReceiptChanged, error.GenerationPublicationChanged, error.GenerationPublicationNotFound, error.MetadataReplicationPending },
        503 => &.{error.MetadataReplicationPending},
        500 => &.{error.InvalidCatalogRecord},
        else => return null,
    };
    for (failures) |err| if (std.mem.eql(u8, body, @errorName(err))) return err;
    return null;
}
/// A planless, bounded owner acknowledgement. The admitted head and ordinal
/// directory, not request fields, determine the records that may change.
pub const Ack = struct {
    table_id: u64,
    expected_revision: u64,
    signed: SignedReceipt,
    validation: ?ValidationWitness = null,
    installation: ?InstallationWitness = null,

    pub fn validate(self: Ack) !void {
        if (self.table_id == 0 or self.signed.receipt.owner.namespace.table_id != self.table_id or
            self.expected_revision == 0 or self.expected_revision == std.math.maxInt(u64)) return error.InvalidGenerationPublication;
        try (progress.OwnerState{ .owner = self.signed.receipt.owner }).validate();
        try requireWitnesses(self.signed, self.validation, self.installation);
        if (self.validation) |witness| try witness.verifyFor(self.signed) else if (self.installation) |witness| try witness.verifyFor(self.signed) else try self.signed.verifySignature();
    }

    pub fn auditEvidence(self: @This()) ?AuditEvidence {
        if (self.validation) |witness| return .{ .outcome = witness.outcome, .signed = witness.signed };
        if (self.installation) |witness| return .{ .outcome = witness.outcome, .signed = witness.signed };
        return null;
    }
};

/// Fixed-size exact receipt observation. Later owners may advance the head;
/// neither a matching revision nor a phase alone proves this receipt committed.
pub const AckStatus = struct {
    head: progress.Head,
    owner: progress.OwnerState,
    validation_digest: ?Digest = null,
    installation_digest: ?Digest = null,
    replication_pending: bool = false,

    pub fn isAcknowledged(self: AckStatus, request: Ack) !bool {
        try request.validate();
        try self.head.validate();
        try self.owner.validate();
        for (self.owner.receipts, self.owner.attestations) |logical, physical| {
            if (!std.mem.allEqual(u8, &logical, 0) and std.mem.allEqual(u8, &physical, 0)) return error.InvalidCatalogRecord;
        }
        for (self.head.counts, 0..) |count, i| {
            const present = self.owner.flags & (@as(u8, 1) << @as(u3, @intCast(i))) != 0;
            if ((count == 0 and present) or (count == self.head.owner_count and !present)) return error.InvalidCatalogRecord;
        }
        const receipt = request.signed.receipt;
        if (self.head.table_id != request.table_id or self.head.revision < request.expected_revision or
            !std.mem.eql(u8, &self.head.plan_id, &receipt.owner.plan_id) or
            !std.mem.eql(u8, &self.head.plan_digest, &receipt.owner.plan_digest) or
            !std.mem.eql(u8, &self.head.candidate_digest, &receipt.candidate_digest) or
            !std.mem.eql(u8, &self.head.reservation_digest, &receipt.reservation_digest) or
            receipt.owner.ordinal >= self.head.owner_count or !std.meta.eql(self.owner.owner, receipt.owner)) return error.GenerationPublicationChanged;
        const stored = self.owner.receipts[@backingInt(receipt.action)];
        if (std.mem.allEqual(u8, &stored, 0)) return false;
        if (!std.mem.eql(u8, &stored, &try receipt.digest())) return error.GenerationPublicationChanged;
        const root = self.owner.attestations[@backingInt(receipt.action)];
        if (std.mem.allEqual(u8, &root, 0)) return error.InvalidCatalogRecord;
        if (request.validation) |witness| {
            const retained = self.validation_digest orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &retained, &witness.signed.outcome_digest)) return false;
        }
        if (request.installation) |witness| {
            const retained = self.installation_digest orelse return error.InvalidCatalogRecord;
            if (!std.mem.eql(u8, &retained, &witness.signed.outcome_digest)) return false;
        }
        return !self.replication_pending and std.mem.eql(u8, &root, &try request.signed.commitment());
    }
};

pub const OwnerOperation = enum { fence, reserve, build, validate, install, release, cancel, cancel_release };
pub const CancellationControlEffect = enum { fenced, reserved };
pub const OwnerRequest = struct {
    table_id: u64,
    group_id: u64,
    id: Id,
    operation: OwnerOperation,
    projection: enum { permission, admission } = .permission,
    /// At most the destinations touched by one bounded source batch. The
    /// metadata service derives their routes; callers never supply descriptors.
    claim_groups: ?ClaimGroups = null,

    pub fn validate(self: OwnerRequest) !void {
        if (self.table_id == 0 or self.group_id == 0 or std.mem.allEqual(u8, &self.id, 0) or
            (self.projection == .admission and self.operation != .fence and self.operation != .cancel))
            return error.InvalidGenerationPublication;
        if (self.claim_groups) |groups| {
            if (self.operation != .build or self.projection != .permission) return error.InvalidGenerationPublication;
            try groups.validate();
        }
    }
};

pub const ClaimGroups = struct {
    items: []const u64,
    pub fn validate(self: ClaimGroups) !void {
        if (self.items.len > local.storage_db_relational_index_owner_job.private_claims.max_claims) return error.InvalidGenerationPublication;
        var previous: u64 = 0;
        for (self.items) |group| {
            if (group <= previous) return error.InvalidGenerationPublication;
            previous = group;
        }
    }
    pub fn jsonStringify(self: ClaimGroups, jw: anytype) !void {
        try jw.write(self.items);
    }
    pub fn jsonParse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) !ClaimGroups {
        if (try source.next() != .array_begin) return error.UnexpectedToken;
        var items: std.ArrayList(u64) = .empty;
        var previous: u64 = 0;
        while (try source.peekNextTokenType() != .array_end) {
            if (items.items.len >= local.storage_db_relational_index_owner_job.private_claims.max_claims) return error.LengthMismatch;
            const group = try std.json.innerParse(u64, alloc, source, options);
            if (group <= previous) return error.UnexpectedToken;
            previous = group;
            try items.append(alloc, group);
        }
        _ = try source.next();
        return .{ .items = try items.toOwnedSlice(alloc) };
    }
};

pub const ClaimRoutes = struct {
    items: []const local.storage_db_relational_index_owner_job.private_claims.delivery.Target,
    pub fn jsonStringify(self: ClaimRoutes, jw: anytype) !void {
        try jw.write(self.items);
    }
    pub fn jsonParse(alloc: Allocator, source: anytype, options: std.json.ParseOptions) !ClaimRoutes {
        if (try source.next() != .array_begin) return error.UnexpectedToken;
        const TargetRoute = local.storage_db_relational_index_owner_job.private_claims.delivery.Target;
        var items: std.ArrayList(TargetRoute) = .empty;
        var limited = options;
        limited.max_value_len = @min(options.max_value_len orelse std.math.maxInt(usize), 33);
        while (try source.peekNextTokenType() != .array_end) {
            if (items.items.len >= local.storage_db_relational_index_owner_job.private_claims.max_claims) return error.LengthMismatch;
            const target = try std.json.innerParse(TargetRoute, alloc, source, limited);
            if (target.start.len > 33 or target.end.len > 33) return error.LengthMismatch;
            try items.append(alloc, target);
        }
        _ = try source.next();
        return .{ .items = try items.toOwnedSlice(alloc) };
    }
};
/// Fixed-size authority observed in the permission transaction. This is not
/// enrollment or a lease: metadata independently revalidates every ACK.
pub const NativeSigningView = struct {
    identity: @import("store_root_enrollment.zig").Identity,
    reporter_incarnation: u64,
    binding_digest: [32]u8,

    pub fn validate(self: NativeSigningView, metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation) !void {
        self.identity.validate() catch return error.InvalidCatalogRecord;
        if (self.reporter_incarnation == 0 or !std.mem.eql(u8, &metadata_incarnation, &self.identity.metadata_incarnation))
            return error.InvalidCatalogRecord;
        const binding: @import("standalone_native_owner.zig").Binding = .{
            .metadata_incarnation = self.identity.metadata_incarnation,
            .node_id = self.identity.node_id,
            .store_id = self.identity.store_id,
            .root_incarnation = self.identity.root_incarnation,
        };
        if (!std.mem.eql(u8, &try binding.digest(), &self.binding_digest)) return error.InvalidCatalogRecord;
    }
};

/// Root-independent logical install input derived only from metadata's retained
/// accepted validation proof. Not a physical audit, transport authentication or
/// permission to adopt a replacement root. ACK revisions do not change its ID.
pub const NativeInstallGrant = struct {
    validation: local.storage_db_relational_index_validation.Seal,
    decision_digest: Digest,

    pub fn decisionDigest(group: u64, incarnation: local.metadata_incarnation.MetadataClusterIncarnation, head: progress.Head) !Digest {
        try head.validate();
        if (group == 0 or !local.metadata_incarnation.isValid(incarnation) or
            (head.phase != .installing and head.phase != .releasing and head.phase != .published)) return error.InvalidCatalogRecord;
        var state = std.crypto.hash.Blake3.init(.{});
        state.update("antfly/native-index-install-decision/v1");
        state.update(&incarnation);
        state.update(&head.plan_id);
        state.update(&head.plan_digest);
        state.update(&head.candidate_digest);
        state.update(&head.reservation_digest);
        var numbers: [24]u8 = undefined;
        std.mem.writeInt(u64, numbers[0..8], group, .little);
        std.mem.writeInt(u64, numbers[8..16], head.table_id, .little);
        std.mem.writeInt(u64, numbers[16..24], head.completion_revision, .little);
        state.update(&numbers);
        var result: Digest = undefined;
        state.final(&result);
        return result;
    }

    pub fn validateFor(self: @This(), group: u64, incarnation: local.metadata_incarnation.MetadataClusterIncarnation, head: progress.Head, owner: progress.OwnerState) !void {
        try owner.validate();
        const receipt = try self.validation.command.receipt(self.validation.position);
        const expected = self.validation.command.expected;
        if (!std.mem.eql(u8, &self.decision_digest, &try decisionDigest(group, incarnation, head)) or
            owner.owner.namespace.table_id != head.table_id or owner.owner.ordinal >= head.owner_count or
            !std.mem.eql(u8, &owner.owner.plan_id, &head.plan_id) or !std.mem.eql(u8, &owner.owner.plan_digest, &head.plan_digest) or
            head.completed_sources != head.owner_count or head.completion_revision == 0 or !owner.source_completed or
            std.mem.allEqual(u8, &owner.receipts[@backingInt(progress.Action.validated)], 0) or !std.meta.eql(receipt.owner, owner.owner) or
            !std.mem.eql(u8, &receipt.candidate_digest, &head.candidate_digest) or
            !std.mem.eql(u8, &receipt.reservation_digest, &head.reservation_digest) or
            !std.mem.eql(u8, &try receipt.digest(), &owner.receipts[@backingInt(progress.Action.validated)]) or
            !std.mem.eql(u8, &expected.metadata_incarnation, &incarnation) or
            expected.completion_revision != head.completion_revision) return error.InvalidCatalogRecord;
    }
};

pub const OwnerDecision = struct {
    pub const Attestation = struct {
        table_name: []const u8,
        identity: native_control.Identity,
        signing: NativeSigningView,
    };
    metadata_group_id: u64,
    metadata_incarnation: local.metadata_incarnation.MetadataClusterIncarnation,
    head: progress.Head,
    owner: progress.OwnerState,
    disposition: enum { execute, acknowledged },
    dispatch: ?Dispatch = null,
    inputs: ?Inputs = null,
    native_signing: ?NativeSigningView = null,
    claim_routes: ?ClaimRoutes = null,
    target_expectation: ?local.storage_db_relational_index_owner_job.private_claims.TargetExpectation = null,
    native_install: ?NativeInstallGrant = null,
    /// Read-only recovery of an already accepted latest stage. This is never
    /// execution permission, schema input, or an index allocation request.
    attestation: ?Attestation = null,

    pub fn requireRequest(self: OwnerDecision, request: OwnerRequest) !void {
        try request.validate();
        if (self.metadata_group_id == 0 or !local.metadata_incarnation.isValid(self.metadata_incarnation)) return error.InvalidGenerationPublication;
        if (self.head.table_id != request.table_id or self.owner.owner.group_id != request.group_id or
            !std.mem.eql(u8, &self.head.plan_id, &request.id)) return error.GenerationPublicationChanged;
        if (try ownerPermission(self.head, self.owner, request.operation) != self.disposition) return error.InvalidCatalogRecord;
        if (request.operation == .validate and self.disposition == .execute and self.owner.owner.authority == .native) {
            const expected = self.target_expectation orelse return error.InvalidCatalogRecord;
            try expected.validate();
            if (self.head.completed_sources != self.head.owner_count or !self.owner.source_completed or
                expected.completion_revision != self.head.completion_revision or
                !std.mem.eql(u8, &expected.metadata_incarnation, &self.metadata_incarnation)) return error.InvalidCatalogRecord;
        } else if (self.target_expectation != null) return error.InvalidCatalogRecord;
        if (request.operation == .install and self.disposition == .execute and self.owner.owner.authority == .native) {
            const grant = self.native_install orelse return error.InvalidCatalogRecord;
            try grant.validateFor(self.metadata_group_id, self.metadata_incarnation, self.head, self.owner);
        } else if (self.native_install != null) return error.InvalidCatalogRecord;
        if (request.claim_groups) |groups| {
            if (self.disposition == .execute) {
                const routes = self.claim_routes orelse return error.InvalidCatalogRecord;
                if (routes.items.len != groups.items.len) return error.InvalidCatalogRecord;
                for (routes.items, groups.items) |target, group| {
                    try target.owner.validate();
                    const peer = target.owner;
                    if (peer.group_id != group or peer.ordinal >= self.head.owner_count or peer.namespace.table_id != request.table_id or
                        !std.mem.eql(u8, &peer.plan_id, &self.head.plan_id) or !std.mem.eql(u8, &peer.plan_digest, &self.head.plan_digest) or
                        target.start.len > 33 or target.end.len > 33 or (target.end.len != 0 and std.mem.order(u8, target.start, target.end) != .lt)) return error.InvalidCatalogRecord;
                }
            } else if (self.claim_routes != null) return error.InvalidCatalogRecord;
        } else if (self.claim_routes != null) return error.InvalidCatalogRecord;
        if (self.native_signing) |view| {
            if (self.disposition != .execute or self.owner.owner.authority != .native) return error.InvalidCatalogRecord;
            try view.validate(self.metadata_incarnation);
        }
        if (self.attestation) |grant| {
            if (self.disposition != .acknowledged or self.owner.owner.authority != .native or
                self.dispatch != null or self.inputs != null or self.native_signing != null or grant.table_name.len == 0)
                return error.InvalidCatalogRecord;
            const action: progress.Action = switch (request.operation) {
                .fence => .fenced,
                .reserve => .reserved,
                else => return error.InvalidCatalogRecord,
            };
            if ((try self.owner.latestReceiptAction()) != action or std.mem.allEqual(u8, &self.owner.attestations[@backingInt(action)], 0)) return error.InvalidCatalogRecord;
            try grant.identity.validate();
            if (!std.mem.eql(u8, &grant.identity.id, &self.head.plan_id) or
                !std.mem.eql(u8, &grant.identity.plan_digest, &self.head.plan_digest) or
                !std.mem.eql(u8, &grant.identity.owner_digest, &self.owner.owner.fence_digest) or
                grant.identity.ordinal != self.owner.owner.ordinal or grant.identity.owner_group_id != request.group_id or
                !grant.identity.namespace.eql(self.owner.owner.namespace)) return error.InvalidCatalogRecord;
            try grant.signing.validate(self.metadata_incarnation);
        }
    }

    /// Bind a fresh permission response to the receiver's independently parsed
    /// request before acquiring storage. A response for another table, owner or
    /// attempt is not interchangeable, even if it is otherwise well formed.
    /// This is neither transport authentication nor a cancellation lease.
    pub fn nativeExecutionIdentity(self: OwnerDecision, request: OwnerRequest) !native_control.Identity {
        if (self.disposition != .execute) return error.InvalidGenerationPublication;
        try self.requireRequest(request);
        return (self.dispatch orelse return error.InvalidGenerationPublication).nativeIdentity(self.owner.owner);
    }

    /// Use only a binding read from the physical owner's typed inspection, not
    /// one supplied by the RPC caller. Hot controls require no manifest/schema
    /// projection and never choose a physical index allocation. Storage still
    /// CASes this binding against its durable job in the effect transaction.
    pub fn nativeControl(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding) !native_control.Command {
        const effect: local.storage_db_relational_index_owner_job.NativeEffect = switch (request.operation) {
            .fence => .fenced,
            .reserve => .reserved,
            else => return error.InvalidGenerationPublication,
        };
        return self.boundNativeControl(request, observed, effect);
    }

    pub fn nativeCancellationControl(self: OwnerDecision, observed: local.storage_db_relational_index_owner_job.Binding, effect: CancellationControlEffect) !native_control.Command {
        return self.boundNativeControl(.{
            .table_id = self.head.table_id,
            .group_id = self.owner.owner.group_id,
            .id = self.head.plan_id,
            .operation = .cancel,
        }, observed, switch (effect) {
            .fenced => .fenced,
            .reserved => .reserved,
        });
    }

    fn boundNativeControl(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding, effect: local.storage_db_relational_index_owner_job.NativeEffect) !native_control.Command {
        try self.requireNativeBinding(request, observed);
        var command: native_control.Command = .{ .effect = effect, .binding = observed };
        // Command identity is the admitted preimage even on a reserved retry;
        // do not rewrite or regress the phase of the durable observed job.
        command.binding.phase = .fencing;
        try command.validate();
        return command;
    }

    /// Claims are produced only during build, against the actual reserved job.
    /// The caller supplies values, never an allocation, fence or physical cut.
    pub fn nativeClaims(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding, claims: @FieldType(native_control.ClaimCommand, "claims")) !native_control.ClaimCommand {
        if (request.operation != .build) return error.InvalidGenerationPublication;
        try self.requireNativeBinding(request, observed);
        const command: native_control.ClaimCommand = .{ .binding = observed, .claims = claims };
        try command.validate();
        return command;
    }

    /// Build dispatch binds actual reserved state to freshly observed metadata
    /// permission. No physical allocation or schema manifest enters the body.
    pub fn nativeBuild(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding, budget: local.storage_db_relational_index_jobs.Budget) !native_control.BuildRequest {
        if (request.operation != .build) return error.InvalidGenerationPublication;
        try self.requireNativeBinding(request, observed);
        return native_control.BuildRequest.init(try self.nativeExecutionIdentity(request), observed, budget);
    }

    /// Frozen metadata input only; a receiver must inspect the physical binding
    /// independently, then core must scan/seal the actual generation and claims.
    pub fn nativeTargetExpectation(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding) !local.storage_db_relational_index_owner_job.private_claims.TargetExpectation {
        if (request.operation != .validate) return error.InvalidGenerationPublication;
        try self.requireNativeBinding(request, observed);
        const expected = self.target_expectation orelse return error.InvalidCatalogRecord;
        try expected.requireBinding(observed);
        return expected;
    }
    pub fn nativeInstallationIntent(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding) !local.storage_db_relational_index_validation.InstallIntent {
        if (request.operation != .install) return error.InvalidGenerationPublication;
        try self.requireNativeBinding(request, observed);
        if (observed.phase != .reserved and observed.phase != .installed) return error.PrivateIndexOwnerJobChanged;
        var reserved = observed;
        reserved.phase = .reserved;
        const grant = self.native_install orelse return error.InvalidCatalogRecord;
        if (!std.meta.eql(reserved, grant.validation.command.binding)) return error.PrivateIndexOwnerJobChanged;
        const intent: local.storage_db_relational_index_validation.InstallIntent = .{ .validation = grant.validation, .metadata_decision = grant.decision_digest };
        try intent.validate();
        return intent;
    }
    pub fn nativeInventoryValidation(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding, budget: local.storage_db_relational_index_jobs.Budget) !native_control.BuildRequest {
        const expected = try self.nativeTargetExpectation(request, observed);
        var result = try native_control.BuildRequest.init(try self.nativeExecutionIdentity(request), observed, budget);
        result.target_expectation = expected;
        try result.validate();
        return result;
    }

    fn requireNativeBinding(self: OwnerDecision, request: OwnerRequest, observed: local.storage_db_relational_index_owner_job.Binding) !void {
        _ = try self.nativeExecutionIdentity(request);
        try self.requireRetainedNativeBinding(observed);
    }

    /// Immutable identity only, not live execution permission. Checkpoints use
    /// this for retained acceptance after cancellation or publication as well.
    pub fn requireRetainedNativeBinding(self: OwnerDecision, observed: local.storage_db_relational_index_owner_job.Binding) !void {
        const identity = self.owner.owner;
        const dispatch = self.dispatch orelse return error.InvalidGenerationPublication;
        _ = try observed.encode();
        if (observed.authority != .native or !std.mem.eql(u8, &observed.id, &identity.plan_id) or
            !std.mem.eql(u8, &observed.plan_digest, &identity.plan_digest) or
            !std.mem.eql(u8, &observed.owner_digest, &identity.fence_digest) or observed.ordinal != identity.ordinal or
            !observed.fence.eql(try privateIndexFence(dispatch.id, dispatch.owner)) or
            !std.mem.eql(u8, &observed.integrity_candidate, &self.head.candidate_digest) or
            !std.mem.eql(u8, &observed.integrity_reserved, &self.head.reservation_digest) or
            observed.physical_reserved.schema_version != dispatch.table_guard.owner.schema_version or
            observed.physical_candidate.schema_version != dispatch.successor_schema_version) return error.PrivateIndexOwnerJobChanged;
    }

    /// Cancel a never-visited owner without loading schema-sized inputs. This
    /// is permission to persist intent, not proof of cleanup or a canceled ACK.
    pub fn nativeCancellationIntent(self: OwnerDecision) !native_control.Admission {
        const identity = try self.nativeExecutionIdentity(.{
            .table_id = self.head.table_id,
            .group_id = self.owner.owner.group_id,
            .id = self.head.plan_id,
            .operation = .cancel,
        });
        const dispatch = self.dispatch orelse return error.InvalidGenerationPublication;
        const intent: native_control.Admission = .{
            .id = identity.id,
            .plan_digest = identity.plan_digest,
            .owner_digest = identity.owner_digest,
            .ordinal = identity.ordinal,
            .fence = try privateIndexFence(dispatch.id, dispatch.owner),
            .candidate_digest = self.head.candidate_digest,
            .reservation_digest = self.head.reservation_digest,
        };
        try intent.validate();
        return intent;
    }

    /// Convert an independently authenticated admission projection, not a
    /// caller-chosen physical binding. A fresh decision remains a permission
    /// observation, not a lease against later metadata cancellation.
    pub fn nativeAdmission(self: OwnerDecision) !native_control.Admission {
        return self.nativeAdmissionImpl(false);
    }

    pub fn nativeCancellationAdmission(self: OwnerDecision) !native_control.Admission {
        return self.nativeAdmissionImpl(true);
    }

    fn nativeAdmissionImpl(self: OwnerDecision, comptime canceling: bool) !native_control.Admission {
        const identity = try self.nativeExecutionIdentity(.{
            .table_id = self.head.table_id,
            .group_id = self.owner.owner.group_id,
            .id = self.head.plan_id,
            .operation = if (canceling) .cancel else .fence,
            .projection = .admission,
        });
        const dispatch = self.dispatch orelse return error.InvalidGenerationPublication;
        const inputs = self.inputs orelse return error.InvalidGenerationPublication;
        try inputs.requireHead(self.head);
        if (inputs.namespace_id != dispatch.namespace_id or
            !std.mem.eql(u8, inputs.before.name, dispatch.table_name) or
            !std.mem.eql(u8, &digest(inputs.before.schema_json), &dispatch.table_guard.owner.schema_digest) or
            !std.mem.eql(u8, &digest(inputs.after.schema_json), &dispatch.successor_schema_digest)) return error.InvalidCatalogRecord;
        return .{
            .id = identity.id,
            .plan_digest = identity.plan_digest,
            .owner_digest = identity.owner_digest,
            .ordinal = identity.ordinal,
            .fence = try privateIndexFence(dispatch.id, dispatch.owner),
            .candidate_digest = self.head.candidate_digest,
            .reservation_digest = self.head.reservation_digest,
        };
    }
};

/// Pure phase permission, not remote authentication or a lease. Data apply
/// rechecks its local job/fence/cursor, and metadata ACK rechecks the live head.
pub fn ownerPermission(head: progress.Head, owner: progress.OwnerState, operation: OwnerOperation) !@FieldType(OwnerDecision, "disposition") {
    try head.validate();
    try owner.validate();
    if (owner.owner.namespace.table_id != head.table_id or owner.owner.ordinal >= head.owner_count or
        !std.mem.eql(u8, &owner.owner.plan_id, &head.plan_id) or !std.mem.eql(u8, &owner.owner.plan_digest, &head.plan_digest))
        return error.InvalidCatalogRecord;
    for (head.counts, 0..) |count, index| {
        const present = owner.flags & (@as(u8, 1) << @as(u3, @intCast(index))) != 0;
        if ((count == 0 and present) or (count == head.owner_count and !present)) return error.InvalidCatalogRecord;
    }
    const action: progress.Action = switch (operation) {
        .fence => .fenced,
        .reserve => .reserved,
        .build, .validate => .validated,
        .install => .installed,
        .release => .released,
        .cancel => .canceled,
        .cancel_release => .cancel_released,
    };
    if (owner.flags & (@as(u8, 1) << @as(u3, @intCast(@backingInt(action)))) != 0) return .acknowledged;
    const allowed = switch (operation) {
        .fence => head.phase == .fencing,
        .reserve => head.phase == .reserving,
        .build, .validate => head.phase == .building,
        .install => head.phase == .installing,
        .release => head.phase == .releasing,
        .cancel => head.phase == .canceling,
        .cancel_release => head.phase == .cancel_releasing,
    };
    if (!allowed) return error.GenerationPublicationChanged;
    return .execute;
}

/// The namespace's pending-name source disappears at the publication decision,
/// while the index lifecycle lock remains through install/release. ACK and
/// dispatch must agree on this distinction; absence is not blanket authority.
pub fn requireActivePointer(txn: anytype, group: u64, head: progress.Head) !void {
    var buf: [160]u8 = undefined;
    const active = try optionalGet(txn, try activeKey(&buf, group, head.table_id));
    const pending = switch (head.phase) {
        .installing, .releasing, .published, .canceled => false,
        else => true,
    };
    if (pending) {
        if (active == null or !std.mem.eql(u8, active.?, &head.plan_id)) return error.GenerationPublicationChanged;
    } else if (active != null) return error.GenerationPublicationChanged;
}
const prefix = "\x00\x00__metadata__:relational_index_publication:v1:";
pub fn allGroupsPrefix() []const u8 {
    return prefix;
}
pub fn groupFromKey(key: []const u8) !?u64 {
    if (!std.mem.startsWith(u8, key, prefix)) return null;
    var parts = std.mem.splitScalar(u8, key[prefix.len..], ':');
    const kind = parts.next() orelse return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, kind, "plan") and !std.mem.eql(u8, kind, "head") and
        !std.mem.eql(u8, kind, "active") and !std.mem.eql(u8, kind, "owner") and !std.mem.eql(u8, kind, "group") and
        !std.mem.eql(u8, kind, "inputs") and !std.mem.eql(u8, kind, "dispatch") and !std.mem.eql(u8, kind, "proof")) return error.InvalidCatalogRecord;
    const number = parts.next() orelse return error.InvalidCatalogRecord;
    const group = std.fmt.parseInt(u64, number, 10) catch return error.InvalidCatalogRecord;
    var buf: [20]u8 = undefined;
    if (group == 0 or !std.mem.eql(u8, number, try std.fmt.bufPrint(&buf, "{d}", .{group}))) return error.InvalidCatalogRecord;
    _ = try tableSuffix(parts.next() orelse return error.InvalidCatalogRecord);
    if (std.mem.eql(u8, kind, "owner") or std.mem.eql(u8, kind, "dispatch") or std.mem.eql(u8, kind, "proof")) {
        const ordinal = parts.next() orelse return error.InvalidCatalogRecord;
        if (ordinal.len != 4) return error.InvalidCatalogRecord;
        const value = std.fmt.parseInt(u16, ordinal, 16) catch return error.InvalidCatalogRecord;
        var ordinal_buf: [4]u8 = undefined;
        if (value >= progress.max_owners or !std.mem.eql(u8, ordinal, try std.fmt.bufPrint(&ordinal_buf, "{x:0>4}", .{value}))) return error.InvalidCatalogRecord;
        if (std.mem.eql(u8, kind, "proof")) {
            const action = parts.next() orelse return error.InvalidCatalogRecord;
            if (action.len != 1 or action[0] < '0' or action[0] > '6') return error.InvalidCatalogRecord;
        }
    } else if (std.mem.eql(u8, kind, "group")) {
        _ = try tableSuffix(parts.next() orelse return error.InvalidCatalogRecord);
    }
    if (parts.next() != null) return error.InvalidCatalogRecord;
    return group;
}

pub fn planPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}plan:{d}:", .{ prefix, group });
}
pub fn activePrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}active:{d}:", .{ prefix, group });
}
pub fn headPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}head:{d}:", .{ prefix, group });
}
pub fn ownersPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}owner:{d}:", .{ prefix, group });
}
pub fn ownerGroupsPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}group:{d}:", .{ prefix, group });
}
pub fn inputsPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}inputs:{d}:", .{ prefix, group });
}
pub fn dispatchPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}dispatch:{d}:", .{ prefix, group });
}
pub fn proofsPrefix(buf: []u8, group: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}proof:{d}:", .{ prefix, group });
}
pub fn proofKey(buf: []u8, group: u64, table: u64, ordinal: u16, action: progress.Action) ![]const u8 {
    if (ordinal >= progress.max_owners or table == 0) return error.InvalidIndexPublicationCandidate;
    return std.fmt.bufPrint(buf, "{s}proof:{d}:{x:0>16}:{x:0>4}:{d}", .{ prefix, group, table, ordinal, @backingInt(action) });
}

/// Retained historical proof, not a fresh ownership grant. All returned fields
/// are fixed-size values; no parsed allocation or borrowed JSON escapes.
pub fn storedReceipt(alloc: Allocator, bytes: []const u8, head: progress.Head, owner: progress.OwnerState, action: progress.Action) !SignedReceipt {
    return (try storedProof(alloc, bytes, head, owner, action)).signed;
}
pub fn storedProof(alloc: Allocator, bytes: []const u8, head: progress.Head, owner: progress.OwnerState, action: progress.Action) !StoredProof.Decoded {
    const decoded = try StoredProof.decode(alloc, bytes);
    const signed = decoded.signed;
    if (signed.receipt.action != action or !std.meta.eql(signed.receipt.owner, owner.owner) or
        !std.mem.eql(u8, &signed.receipt.candidate_digest, &head.candidate_digest) or
        !std.mem.eql(u8, &signed.receipt.reservation_digest, &head.reservation_digest) or
        !std.mem.eql(u8, &try signed.receipt.digest(), &owner.receipts[@backingInt(action)]) or
        !std.mem.eql(u8, &try signed.commitment(), &owner.attestations[@backingInt(action)])) return error.InvalidCatalogRecord;
    return decoded;
}
pub fn inputsKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}inputs:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn dispatchKey(buf: []u8, group: u64, table: u64, ordinal: u16) ![]const u8 {
    if (ordinal >= progress.max_owners) return error.InvalidIndexPublicationCandidate;
    return std.fmt.bufPrint(buf, "{s}dispatch:{d}:{x:0>16}:{x:0>4}", .{ prefix, group, table, ordinal });
}
pub fn ownerGroupKey(buf: []u8, group: u64, table: u64, owner_group: u64) ![]const u8 {
    if (table == 0 or owner_group == 0) return error.InvalidCatalogRecord;
    return std.fmt.bufPrint(buf, "{s}group:{d}:{x:0>16}:{x:0>16}", .{ prefix, group, table, owner_group });
}

/// Fixed-size point directory, shared by owner dispatch and reverse cohort
/// verification. It is not a namespace source or independent job authority.
pub fn ownerOrdinal(reader: anytype, group: u64, table: u64, owner_group: u64) !u16 {
    var buf: [160]u8 = undefined;
    const bytes = try reader.get(try ownerGroupKey(&buf, group, table, owner_group));
    if (bytes.len != 2) return error.InvalidCatalogRecord;
    const ordinal = std.mem.readInt(u16, bytes[0..2], .big);
    if (ordinal >= progress.max_owners) return error.InvalidCatalogRecord;
    return ordinal;
}
pub fn planKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}plan:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn activeKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}active:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn headKey(buf: []u8, group: u64, table: u64) ![]const u8 {
    return std.fmt.bufPrint(buf, "{s}head:{d}:{x:0>16}", .{ prefix, group, table });
}
pub fn ownerKey(buf: []u8, group: u64, table: u64, ordinal: u16) ![]const u8 {
    if (ordinal >= progress.max_owners) return error.InvalidIndexPublicationCandidate;
    return std.fmt.bufPrint(buf, "{s}owner:{d}:{x:0>16}:{x:0>4}", .{ prefix, group, table, ordinal });
}

pub const SourceKey = union(enum) { plan: u64, active: u64 };
pub fn sourceKey(key: []const u8, group: u64) !?SourceKey {
    var buf: [160]u8 = undefined;
    const plans = try planPrefix(&buf, group);
    if (std.mem.startsWith(u8, key, plans)) return .{ .plan = try tableSuffix(key[plans.len..]) };
    const active = try activePrefix(&buf, group);
    if (std.mem.startsWith(u8, key, active)) return .{ .active = try tableSuffix(key[active.len..]) };
    return null;
}
fn tableSuffix(bytes: []const u8) !u64 {
    if (bytes.len != 16) return error.InvalidCatalogRecord;
    for (bytes) |c| if (!(c >= '0' and c <= '9') and !(c >= 'a' and c <= 'f')) return error.InvalidCatalogRecord;
    const id = std.fmt.parseInt(u64, bytes, 16) catch return error.InvalidCatalogRecord;
    if (id == 0) return error.InvalidCatalogRecord;
    return id;
}

/// A fresh begin CASes the exact terminal predecessor head (or true absence).
/// Random job IDs alone cannot prevent replay after multiple canceled attempts
/// against an unchanged public schema. Old owner rows are replaced/trimmed in
/// the same transaction, keeping retained storage bounded after DDL churn.
pub const Begin = struct {
    expected_head: ?[progress.Head.encoded_len]u8 = null,
    plan: Plan,
};

/// Namespace readers need definitions, not owner receipt arrays. Allocate the
/// projected strings independently: a subsequent transactional point read may
/// invalidate a borrowed store value.
pub const Projection = struct {
    format_version: u16 = 1,
    id: Id,
    namespace_id: u64,
    before: Table,
    after: Table,
    const Table = struct { table_id: u64, name: []const u8, schema_json: []const u8 };

    pub fn decode(a: Allocator, bytes: []const u8, table_id: u64) !std.json.Parsed(Projection) {
        if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidCatalogRecord;
        var parsed = try std.json.parseFromSlice(Projection, a, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
        errdefer parsed.deinit();
        const p = parsed.value;
        if (p.format_version != 1 or std.mem.allEqual(u8, &p.id, 0) or p.namespace_id == 0 or table_id == 0 or
            p.before.table_id != table_id or p.after.table_id != table_id or
            !std.mem.eql(u8, p.before.name, p.after.name) or p.before.schema_json.len == 0 or p.after.schema_json.len == 0)
            return error.InvalidCatalogRecord;
        return parsed;
    }

    pub fn sourceDigest(self: Projection) Digest {
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly.relation-index-source.v1");
        hash.update(&self.id);
        var number: [8]u8 = undefined;
        for ([_]u64{ self.namespace_id, self.before.table_id }) |n| {
            std.mem.writeInt(u64, &number, n, .big);
            hash.update(&number);
        }
        for ([_][]const u8{ self.before.name, self.before.schema_json, self.after.schema_json }) |bytes| {
            std.mem.writeInt(u64, &number, bytes.len, .big);
            hash.update(&number);
            hash.update(bytes);
        }
        var out: Digest = undefined;
        hash.final(&out);
        return out;
    }
};

fn digest(bytes: []const u8) Digest {
    var out: Digest = undefined;
    std.crypto.hash.Blake3.hash(bytes, &out, .{});
    return out;
}

/// Counting writer rejects oversized/escaped plans before the first frame
/// allocation. It also bounds work for callers constructing plans in memory,
/// not just for a network decoder which already checked the incoming frame.
const FrameBudget = struct {
    remaining: usize = max_bytes,
    writer: std.Io.Writer = .{ .vtable = &.{ .drain = drain }, .buffer = &.{} },

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *FrameBudget = @alignCast(@fieldParentPtr("writer", w));
        var count = std.math.mul(usize, data[data.len - 1].len, splat) catch return error.WriteFailed;
        for (data[0 .. data.len - 1]) |bytes| count = std.math.add(usize, count, bytes.len) catch return error.WriteFailed;
        if (count > self.remaining) return error.WriteFailed;
        self.remaining -= count;
        return count;
    }
};

fn encodeBounded(alloc: Allocator, value: anytype) ![]const u8 {
    var budget: FrameBudget = .{};
    std.json.Stringify.value(value, .{}, &budget.writer) catch return error.IndexPublicationTooLarge;
    const bytes = try alloc.alloc(u8, max_bytes - budget.remaining);
    errdefer alloc.free(bytes);
    var writer: std.Io.Writer = .fixed(bytes);
    std.json.Stringify.value(value, .{}, &writer) catch return error.InvalidIndexPublicationCandidate;
    if (writer.end != bytes.len) return error.InvalidIndexPublicationCandidate;
    return bytes;
}

pub const Owner = struct {
    range: records.RangeRecord,
    namespace: local.storage_db_doc_identity.Namespace,
    admission_epoch: u64,
    catalog_digest: Digest,
    receipt_authority: progress.Authority,
};

/// The decision needs table declarations, not a second owned copy of the
/// potentially multi-megabyte base64 predecessor catalog. Project from the
/// existing immutable inputs without reading/decoding the owner cohort.
pub const DecisionInputs = struct {
    format_version: u16 = 1,
    id: Id,
    plan_digest: Digest,
    namespace_id: u64,
    index_name: []const u8,
    table_guard: domain.RelationMutationGuard,
    before: records.TableRecord,
    after: records.TableRecord,
};

fn requireInputsHead(input: anytype, head: progress.Head) !void {
    try head.validate();
    if (!std.meta.eql(try readerRequirements(input.before, input.after), head.reader_requirements)) return error.InvalidCatalogRecord;
    if (input.format_version != 1 or !std.mem.eql(u8, &input.id, &head.plan_id) or
        !std.mem.eql(u8, &input.plan_digest, &head.plan_digest) or input.namespace_id == 0 or
        input.before.table_id != head.table_id or input.after.table_id != head.table_id or
        input.index_name.len == 0) return error.InvalidCatalogRecord;
}

/// Shared, immutable worker input. Decode once at admission, not once per page
/// and not once per owner inside the durable metadata representation.
pub const Inputs = struct {
    format_version: u16 = 1,
    id: Id,
    plan_digest: Digest,
    namespace_id: u64,
    index_name: []const u8,
    table_guard: domain.RelationMutationGuard,
    before: records.TableRecord,
    after: records.TableRecord,
    catalog_before_b64: []const u8,

    /// Pack immutable bytes once for owner admission. The physical owner
    /// compiles the candidate against its actual catalog and verifies the head
    /// fingerprints; doing that compilation here as well would duplicate work.
    pub fn prepareNativeManifest(self: Inputs, a: Allocator, head: progress.Head) !native_control.Manifest {
        try self.requireHead(head);
        try self.table_guard.validate();
        var budget: FrameBudget = .{};
        std.json.Stringify.value(self, .{}, &budget.writer) catch return error.IndexPublicationTooLarge;
        const length = std.base64.standard.Decoder.calcSizeForSlice(self.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
        if (length == 0 or length > local.storage_db_relational_integrity_catalog.max_catalog_bytes) return error.InvalidIndexPublicationCandidate;
        const predecessor = try a.alloc(u8, length);
        defer a.free(predecessor);
        std.base64.standard.Decoder.decode(predecessor, self.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
        return native_control.Manifest.init(a, .{
            .before_json = self.before.schema_json,
            .after_json = self.after.schema_json,
            .predecessor = predecessor,
            .name = self.index_name,
        });
    }

    /// Admission rebuilds the typed logical candidate and verifies BOTH admitted
    /// fingerprints. The projected plan ID/checksum is not a substitute for
    /// schema, index-pair and catalog validation. No owner-cohort decode occurs.
    pub fn prepareCandidate(self: Inputs, a: Allocator, head: progress.Head) !candidate_mod.Candidate {
        try self.requireHead(head);
        var budget: FrameBudget = .{};
        std.json.Stringify.value(self, .{}, &budget.writer) catch return error.IndexPublicationTooLarge;
        var candidate = try prepareInputCandidate(a, self);
        errdefer candidate.deinit();
        if (!std.mem.eql(u8, &digest(candidate.next.value), &head.candidate_digest) or
            !std.mem.eql(u8, &digest(candidate.reservation.value), &head.reservation_digest)) return error.InvalidIndexPublicationCandidate;
        return candidate;
    }

    fn requireHead(self: Inputs, head: progress.Head) !void {
        try requireInputsHead(self, head);
    }
};

/// The hot owner projection contains no schema/catalog JSON or owner cohort.
/// The table guard and successor identity support indexed live-catalog checks;
/// the complete owner descriptor is independently bound by fence_digest.
pub const Dispatch = struct {
    format_version: u16 = 1,
    id: Id,
    plan_digest: Digest,
    ordinal: u16,
    namespace_id: u64,
    table_guard: domain.RelationMutationGuard,
    table_name: []const u8,
    successor_schema_version: u32,
    successor_schema_digest: Digest,
    owner: Owner,

    pub fn nativeIdentity(self: Dispatch, expected: progress.Owner) !native_control.Identity {
        try self.requireOwner(expected);
        if (self.owner.receipt_authority != .native) return error.InvalidGenerationPublication;
        const identity: native_control.Identity = .{
            .id = self.id,
            .plan_digest = self.plan_digest,
            .owner_digest = expected.fence_digest,
            .ordinal = self.ordinal,
            .namespace = self.owner.namespace,
            .owner_group_id = self.owner.range.group_id,
        };
        try identity.validate();
        return identity;
    }

    fn requireOwner(d: Dispatch, expected: progress.Owner) !void {
        try (progress.OwnerState{ .owner = expected }).validate();
        if (d.format_version != 1 or !std.mem.eql(u8, &d.id, &expected.plan_id) or
            !std.mem.eql(u8, &d.plan_digest, &expected.plan_digest) or d.ordinal != expected.ordinal or
            d.namespace_id == 0 or d.table_name.len == 0 or d.successor_schema_version == 0 or
            d.owner.range.group_id != expected.group_id or !d.owner.namespace.eql(expected.namespace) or
            d.owner.range.table_id != expected.namespace.table_id or
            manager.rangeDocIdentityShardId(d.owner.range) != expected.namespace.shard_id or
            manager.rangeDocIdentityRangeId(d.owner.range) != expected.namespace.range_id or
            d.owner.receipt_authority != expected.authority or
            !std.mem.eql(u8, &try ownerFenceDigest(d.owner), &expected.fence_digest)) return error.InvalidCatalogRecord;
        try d.table_guard.validate();
        if (d.table_guard.owner.kind != .table or d.table_guard.owner.table_id != expected.namespace.table_id or
            d.table_guard.constraint_name_admission != null or
            d.successor_schema_version != (std.math.add(u32, d.table_guard.owner.schema_version, 1) catch return error.InvalidCatalogRecord) or
            !std.mem.eql(u8, d.table_guard.target.name, d.table_guard.logical_table)) return error.InvalidCatalogRecord;
        _ = privateIndexFence(d.id, d.owner) catch return error.InvalidCatalogRecord;
    }
};

/// Preserve both halves of the full publication ID, matching the existing
/// generation-publication fence convention. Never synthesize Raft positions
/// from these control identities or the separately admitted source epoch.
fn privateIndexFence(id: Id, owner: Owner) !local.storage_db_relational_integrity_topology_contract.Fence {
    const fence: local.storage_db_relational_integrity_topology_contract.Fence = .{
        .role = .index_build,
        .transition_id = std.mem.readInt(u64, id[0..8], .little),
        .attempt = std.mem.readInt(u64, id[8..16], .little),
        .admission_epoch = owner.admission_epoch,
        .peer_group_id = owner.range.group_id,
        .owner_group_id = owner.range.group_id,
        .namespace = owner.namespace,
        .catalog_digest = owner.catalog_digest,
    };
    _ = try fence.encode();
    return fence;
}

/// These reads own every decoded string before another transactional get can
/// invalidate borrowed bytes. A projection is not authority by itself: callers
/// must independently recheck the live phase, lifecycle lock and catalog cut.
pub fn readDecisionInputs(a: Allocator, txn: anytype, group: u64, head: progress.Head) !std.json.Parsed(DecisionInputs) {
    try head.validate();
    var buf: [160]u8 = undefined;
    const bytes = try txn.get(try inputsKey(&buf, group, head.table_id));
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(DecisionInputs, a, bytes, .{ .ignore_unknown_fields = true, .allocate = .alloc_always });
    errdefer parsed.deinit();
    try requireInputsHead(parsed.value, head);
    try parsed.value.table_guard.validate();
    return parsed;
}

pub fn readInputs(a: Allocator, txn: anytype, group: u64, head: progress.Head) !std.json.Parsed(Inputs) {
    try head.validate();
    var buf: [160]u8 = undefined;
    const bytes = try txn.get(try inputsKey(&buf, group, head.table_id));
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(Inputs, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.requireHead(head);
    try parsed.value.table_guard.validate();
    return parsed;
}

pub fn readDispatch(a: Allocator, txn: anytype, group: u64, expected: progress.Owner) !std.json.Parsed(Dispatch) {
    try (progress.OwnerState{ .owner = expected }).validate();
    var buf: [160]u8 = undefined;
    const bytes = try txn.get(try dispatchKey(&buf, group, expected.namespace.table_id, expected.ordinal));
    if (bytes.len == 0 or bytes.len > max_bytes) return error.InvalidCatalogRecord;
    var parsed = try std.json.parseFromSlice(Dispatch, a, bytes, .{ .allocate = .alloc_always });
    errdefer parsed.deinit();
    try parsed.value.requireOwner(expected);
    return parsed;
}

/// Call only for an owner from a bounded, validated frame. Hash directly into
/// the writer, without allocating another serialized copy per owner.
pub fn ownerFenceDigest(owner: Owner) !Digest {
    var hasher = std.crypto.hash.Blake3.init(.{});
    hasher.update("antfly:index-build-owner:v1");
    var hashing = std.Io.Writer.Hashing(std.crypto.hash.Blake3).initHasher(hasher, &.{});
    try std.json.Stringify.value(owner, .{}, &hashing.writer);
    var out: Digest = undefined;
    hashing.hasher.final(&out);
    return out;
}

/// Shared validation for original admission and its independently fetched
/// worker input. Candidate allocations cannot retain this temporary arena.
fn prepareInputCandidate(alloc: Allocator, input: anytype) !candidate_mod.Candidate {
    if (input.format_version != 1 or input.index_name.len == 0 or input.index_name.len > names.max_name_bytes)
        return error.InvalidIndexPublicationCandidate;
    var scratch = std.heap.ArenaAllocator.init(alloc);
    defer scratch.deinit();
    const s = scratch.allocator();
    try input.table_guard.validate();
    if (input.table_guard.owner.kind != .table or input.table_guard.constraint_name_admission != null or
        input.table_guard.owner.table_id != input.before.table_id or
        !std.mem.eql(u8, input.table_guard.target.name, input.table_guard.logical_table) or
        input.table_guard.owner.schema_version != try @import("../api/tables.zig").schemaVersion(input.before.schema_json) or
        !std.mem.eql(u8, &input.table_guard.owner.schema_digest, &digest(input.before.schema_json)))
        return error.InvalidIndexPublicationCandidate;
    try shape.requireTableImages(s, input.before, input.after);
    const length = std.base64.standard.Decoder.calcSizeForSlice(input.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
    if (length == 0 or length > local.storage_db_relational_integrity_catalog.max_catalog_bytes) return error.InvalidIndexPublicationCandidate;
    const predecessor = try s.alloc(u8, length);
    std.base64.standard.Decoder.decode(predecessor, input.catalog_before_b64) catch return error.InvalidIndexPublicationCandidate;
    var candidate = try candidate_mod.Candidate.prepare(alloc, input.before.table_id, input.before.schema_json, input.after.schema_json, predecessor, input.index_name);
    errdefer candidate.deinit();
    if (!try @import("../api/tables.zig").foreignKeyPublicationIndexesValid(s, input.before.name, input.before.indexes_json, input.after.indexes_json, input.after.schema_json, candidate.next.catalog.schema_version))
        return error.InvalidIndexPublicationCandidate;
    return candidate;
}

pub const Plan = struct {
    format_version: u16 = 1,
    id: Id,
    namespace_id: u64,
    index_name: []const u8,
    table_guard: domain.RelationMutationGuard,
    before: records.TableRecord,
    after: records.TableRecord,
    catalog_before_b64: []const u8,
    /// Keyspace order, not arrival order. Full range descriptors bind restore,
    /// placement and physical identity fields, not only routing boundaries.
    owners: []const Owner,

    pub fn prepare(self: Plan, alloc: Allocator) !Prepared {
        if (self.format_version != 1 or std.mem.allEqual(u8, &self.id, 0) or
            self.namespace_id == 0 or self.index_name.len == 0 or self.index_name.len > names.max_name_bytes or
            self.owners.len == 0 or self.owners.len > progress.max_owners)
            return error.InvalidIndexPublicationCandidate;
        var arena = std.heap.ArenaAllocator.init(alloc);
        errdefer arena.deinit();
        const a = arena.allocator();
        const frame = try encodeBounded(a, self);
        var scratch = std.heap.ArenaAllocator.init(alloc);
        defer scratch.deinit();
        const s = scratch.allocator();
        var candidate = try prepareInputCandidate(alloc, self);
        errdefer candidate.deinit();
        const ranges = try s.alloc(records.RangeRecord, self.owners.len);
        // Membership checks are linear even for the maximum owner cohort.
        var groups: std.AutoHashMapUnmanaged(u64, void) = .empty;
        defer groups.deinit(s);
        const before_digest = digest(candidate.reservation.expected.?);
        for (self.owners, ranges) |owner, *range| {
            range.* = owner.range;
            _ = privateIndexFence(self.id, owner) catch return error.InvalidIndexPublicationCandidate;
            if (owner.range.table_id != self.before.table_id or owner.range.group_id == 0 or owner.admission_epoch == 0 or
                owner.namespace.table_id != self.before.table_id or
                owner.namespace.shard_id != manager.rangeDocIdentityShardId(owner.range) or
                owner.namespace.range_id != manager.rangeDocIdentityRangeId(owner.range) or
                !std.mem.eql(u8, &owner.catalog_digest, &before_digest)) return error.InvalidIndexPublicationCandidate;
            if ((try groups.getOrPut(s, owner.range.group_id)).found_existing) return error.InvalidIndexPublicationCandidate;
        }
        manager.validateCompleteKeyspaceRanges(ranges) catch return error.InvalidIndexPublicationCandidate;
        const plan_digest = digest(frame);
        const owners = try a.alloc(progress.OwnerState, self.owners.len);
        for (self.owners, owners, 0..) |owner, *state, ordinal| {
            // The head's plan digest binds namespace, exact before/after images
            // and all owners. This owner digest also binds its full local cut.
            state.* = .{ .owner = .{
                .plan_id = self.id,
                .plan_digest = plan_digest,
                .ordinal = @intCast(ordinal),
                .group_id = owner.range.group_id,
                .namespace = owner.namespace,
                .authority = owner.receipt_authority,
                .fence_digest = try ownerFenceDigest(owner),
            } };
            try state.validate();
        }
        const head: progress.Head = .{
            .reader_requirements = try readerRequirements(self.before, self.after),
            .plan_id = self.id,
            .plan_digest = plan_digest,
            .candidate_digest = digest(candidate.next.value),
            .reservation_digest = digest(candidate.reservation.value),
            .table_id = self.before.table_id,
            .owner_count = @intCast(self.owners.len),
        };
        try head.validate();
        const inputs = try encodeBounded(a, Inputs{
            .id = self.id,
            .plan_digest = plan_digest,
            .namespace_id = self.namespace_id,
            .index_name = self.index_name,
            .table_guard = self.table_guard,
            .before = self.before,
            .after = self.after,
            .catalog_before_b64 = self.catalog_before_b64,
        });
        const dispatch = try a.alloc([]const u8, self.owners.len);
        const successor_version = try @import("../api/tables.zig").schemaVersion(self.after.schema_json);
        const successor_digest = digest(self.after.schema_json);
        for (self.owners, dispatch, 0..) |owner, *bytes, ordinal| bytes.* = try encodeBounded(a, Dispatch{
            .id = self.id,
            .plan_digest = plan_digest,
            .ordinal = @intCast(ordinal),
            .namespace_id = self.namespace_id,
            .table_guard = self.table_guard,
            .table_name = self.before.name,
            .successor_schema_version = successor_version,
            .successor_schema_digest = successor_digest,
            .owner = owner,
        });
        const reservation: names.Entry.Reservation = .{ .successor = .{
            .table_id = self.before.table_id,
            .schema_version = candidate.next.catalog.schema_version,
            .schema_digest = digest(self.after.schema_json),
            .publication_id = self.id,
            .kind = .index,
            .phase = .reserved,
        } };
        try reservation.validate();
        return .{
            .arena = arena,
            .frame = frame,
            .inputs = inputs,
            .dispatch = dispatch,
            .candidate = candidate,
            .head = head,
            .owners = owners,
            .name_key = .{ .namespace_id = self.namespace_id, .name = try a.dupe(u8, self.index_name) },
            .name_reservation = reservation,
        };
    }
};

/// All returned slices are owned. Progress application does not retain or
/// reserialize the immutable frame; it uses head + one owner record per ACK.
pub const Prepared = struct {
    arena: std.heap.ArenaAllocator,
    frame: []const u8,
    inputs: []const u8,
    dispatch: []const []const u8,
    candidate: candidate_mod.Candidate,
    head: progress.Head,
    owners: []const progress.OwnerState,
    name_key: names.Key,
    name_reservation: names.Entry.Reservation,

    /// The caller must first revalidate the live table/name guard, complete
    /// range cut and protocol admission in this SAME transaction. Any error
    /// during staging requires abort; this helper never independently commits.
    pub fn stageBegin(self: *const Prepared, txn: anytype, group: u64, expected_head: ?[progress.Head.encoded_len]u8) !void {
        var buf: [160]u8 = undefined;
        const prior_bytes = txn.get(try headKey(&buf, group, self.head.table_id)) catch |err| switch (err) {
            error.NotFound => null,
            else => return err,
        };
        var old_count: u16 = 0;
        var old_id: Id = @splat(0);
        if (expected_head) |expected| {
            if (prior_bytes == null or !std.mem.eql(u8, prior_bytes.?, &expected)) return error.GenerationPublicationChanged;
            const prior = try progress.Head.decode(&expected);
            if (prior.table_id != self.head.table_id or (prior.phase != .published and prior.phase != .canceled) or
                std.mem.eql(u8, &prior.plan_id, &self.head.plan_id)) return error.GenerationPublicationChanged;
            old_count = prior.owner_count;
            old_id = prior.plan_id;
        } else {
            if (prior_bytes != null) return error.GenerationPublicationChanged;
            if (try optionalGet(txn, try planKey(&buf, group, self.head.table_id)) != null) return error.InvalidCatalogRecord;
            if (try optionalGet(txn, try inputsKey(&buf, group, self.head.table_id)) != null) return error.InvalidCatalogRecord;
            const owner_prefix = try std.fmt.bufPrint(&buf, "{s}owner:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(owner_prefix)) return error.InvalidCatalogRecord;
            const group_prefix = try std.fmt.bufPrint(&buf, "{s}group:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(group_prefix)) return error.InvalidCatalogRecord;
            const dispatch_prefix = try std.fmt.bufPrint(&buf, "{s}dispatch:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(dispatch_prefix)) return error.InvalidCatalogRecord;
            const proofs_prefix = try std.fmt.bufPrint(&buf, "{s}proof:{d}:{x:0>16}:", .{ prefix, group, self.head.table_id });
            if (try txn.hasPrefix(proofs_prefix)) return error.InvalidCatalogRecord;
        }
        if (try optionalGet(txn, try activeKey(&buf, group, self.head.table_id)) != null) return error.GenerationPublicationChanged;
        try lifecycle.acquire(txn, group, self.head.table_id, .{ .kind = .index_build, .id = self.head.plan_id });
        // Remove the predecessor's group directory before overwriting ordinal
        // rows. Its cohort may have changed completely during terminal DDL.
        for (0..old_count) |i| {
            const old = try progress.OwnerState.decode(try txn.get(try ownerKey(&buf, group, self.head.table_id, @intCast(i))));
            if (old.owner.ordinal != i or !std.mem.eql(u8, &old.owner.plan_id, &old_id)) return error.InvalidCatalogRecord;
            try deleteProofs(txn, group, self.head.table_id, old);
            try txn.delete(try ownerGroupKey(&buf, group, self.head.table_id, old.owner.group_id));
        }
        try txn.put(try planKey(&buf, group, self.head.table_id), self.frame);
        try txn.put(try inputsKey(&buf, group, self.head.table_id), self.inputs);
        try txn.put(try headKey(&buf, group, self.head.table_id), &try self.head.encode());
        for (self.owners, 0..) |owner, i| {
            try txn.put(try ownerKey(&buf, group, self.head.table_id, @intCast(i)), &try owner.encode());
            try txn.put(try dispatchKey(&buf, group, self.head.table_id, @intCast(i)), self.dispatch[i]);
            var ordinal: [2]u8 = undefined;
            std.mem.writeInt(u16, &ordinal, @intCast(i), .big);
            try txn.put(try ownerGroupKey(&buf, group, self.head.table_id, owner.owner.group_id), &ordinal);
        }
        for (self.owners.len..@max(self.owners.len, old_count)) |i| {
            try txn.delete(try ownerKey(&buf, group, self.head.table_id, @intCast(i)));
            try txn.delete(try dispatchKey(&buf, group, self.head.table_id, @intCast(i)));
        }
        try txn.put(try activeKey(&buf, group, self.head.table_id), &self.head.plan_id);
    }

    pub fn deinit(self: *Prepared) void {
        self.candidate.deinit();
        self.arena.deinit();
        self.* = undefined;
    }
};

fn optionalGet(txn: anytype, key: []const u8) !?[]const u8 {
    return txn.get(key) catch |err| switch (err) {
        error.NotFound => null,
        else => return err,
    };
}

/// DROP removes the one retained terminal attempt, not an unbounded history.
/// Active jobs must finish their owner cleanup before ordinary table deletion.
fn deleteProofs(txn: anytype, group: u64, table: u64, owner: progress.OwnerState) !void {
    try @import("relational_index_delivery_journal.zig").deleteOwner(txn, group, table, owner);
    var buf: [160]u8 = undefined;
    for (owner.attestations, 0..) |commitment, action| {
        if (!std.mem.allEqual(u8, &commitment, 0)) try txn.delete(try proofKey(&buf, group, table, owner.owner.ordinal, @fromBackingInt(@intCast(action))));
    }
}

pub fn deleteTerminal(txn: anytype, group: u64, table: u64) !bool {
    var buf: [160]u8 = undefined;
    const bytes = (try optionalGet(txn, try headKey(&buf, group, table))) orelse return false;
    const head = try progress.Head.decode(bytes);
    if (head.table_id != table or (head.phase != .published and head.phase != .canceled) or
        try optionalGet(txn, try activeKey(&buf, group, table)) != null) return error.GenerationPublicationChanged;
    for (0..head.owner_count) |i| {
        const owner = try progress.OwnerState.decode(try txn.get(try ownerKey(&buf, group, table, @intCast(i))));
        if (owner.owner.ordinal != i or !std.mem.eql(u8, &owner.owner.plan_id, &head.plan_id)) return error.InvalidCatalogRecord;
        try deleteProofs(txn, group, table, owner);
        try txn.delete(try ownerGroupKey(&buf, group, table, owner.owner.group_id));
        try txn.delete(try ownerKey(&buf, group, table, @intCast(i)));
        try txn.delete(try dispatchKey(&buf, group, table, @intCast(i)));
    }
    try txn.delete(try headKey(&buf, group, table));
    try txn.delete(try planKey(&buf, group, table));
    try txn.delete(try inputsKey(&buf, group, table));
    return true;
}

/// A checkpoint verifies the complete immutable cohort once. Normal owner ACKs
/// use only their fixed-size head/owner CAS and never invoke this full verifier.
pub fn verifyJob(a: Allocator, txn: anytype, group: u64, table: u64) !progress.Head {
    var verified = try verifyOwned(a, txn, group, table);
    defer verified.deinit();
    return verified.head;
}

pub const Verified = struct {
    plan: std.json.Parsed(Plan),
    prepared: Prepared,
    head: progress.Head,
    receipt_count: usize,
    delivery_counts: [2]usize,

    pub fn deinit(self: *Verified) void {
        self.prepared.deinit();
        self.plan.deinit();
        self.* = undefined;
    }
};

/// Keep the independently decoded plan alive for the metadata adapter's
/// current table/range/namespace checks, without parsing the frame a second time.
pub fn verifyOwned(a: Allocator, txn: anytype, group: u64, table: u64) !Verified {
    var buf: [160]u8 = undefined;
    const frame = try txn.get(try planKey(&buf, group, table));
    if (frame.len == 0 or frame.len > max_bytes) return error.InvalidCatalogRecord;
    const frame_digest = digest(frame);
    var parsed = std.json.parseFromSlice(Plan, a, frame, .{ .allocate = .alloc_always }) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCatalogRecord,
    };
    errdefer parsed.deinit();
    var prepared = parsed.value.prepare(a) catch |err| switch (err) {
        error.OutOfMemory => return err,
        else => return error.InvalidCatalogRecord,
    };
    errdefer prepared.deinit();
    const head = progress.Head.decode(try txn.get(try headKey(&buf, group, table))) catch return error.InvalidCatalogRecord;
    if (table != prepared.head.table_id or head.table_id != table or
        !std.mem.eql(u8, &head.plan_id, &prepared.head.plan_id) or
        !std.mem.eql(u8, &head.plan_digest, &frame_digest) or !std.mem.eql(u8, &head.plan_digest, &prepared.head.plan_digest) or
        !std.mem.eql(u8, &head.candidate_digest, &prepared.head.candidate_digest) or
        !std.mem.eql(u8, &head.reservation_digest, &prepared.head.reservation_digest) or head.owner_count != prepared.head.owner_count or
        !std.meta.eql(head.reader_requirements, prepared.head.reader_requirements))
        return error.InvalidCatalogRecord;
    if (!std.mem.eql(u8, try txn.get(try inputsKey(&buf, group, table)), prepared.inputs)) return error.InvalidCatalogRecord;
    var counts: [7]u16 = @splat(0);
    var receipt_count: usize = 0;
    var delivery_counts: [2]usize = @splat(0);
    var delivery_rows: [2]u64 = @splat(0);
    var completed_sources: u16 = 0;
    var cached_enrollment: ?root_enrollment.Identity = null;
    for (prepared.owners, 0..) |expected, i| {
        const owner = progress.OwnerState.decode(try txn.get(try ownerKey(&buf, group, table, @intCast(i)))) catch return error.InvalidCatalogRecord;
        if (!std.meta.eql(owner.owner, expected.owner)) return error.InvalidCatalogRecord;
        if (try ownerOrdinal(txn, group, table, owner.owner.group_id) != i) return error.InvalidCatalogRecord;
        if (!std.mem.eql(u8, try txn.get(try dispatchKey(&buf, group, table, @intCast(i))), prepared.dispatch[i])) return error.InvalidCatalogRecord;
        const accepted = try @import("relational_index_delivery_journal.zig").verifyOwner(a, txn, group, head, owner, std.fmt.bytesToHex(parsed.value.table_guard.incarnation, .lower));
        delivery_counts[0] += accepted.sources;
        completed_sources += accepted.completed;
        delivery_counts[1] += accepted.targets;
        delivery_rows[0] = std.math.add(u64, delivery_rows[0], accepted.source_rows) catch return error.InvalidCatalogRecord;
        delivery_rows[1] = std.math.add(u64, delivery_rows[1], accepted.target_rows) catch return error.InvalidCatalogRecord;
        for (owner.receipts, 0..) |logical, action_index| {
            const action: progress.Action = @fromBackingInt(@intCast(action_index));
            if (std.mem.allEqual(u8, &logical, 0)) {
                if (!std.mem.allEqual(u8, &owner.attestations[action_index], 0)) return error.InvalidCatalogRecord;
                continue;
            }
            const proof_bytes = try optionalGet(txn, try proofKey(&buf, group, table, @intCast(i), action));
            const retained = try storedProof(a, proof_bytes orelse return error.InvalidCatalogRecord, head, owner, action);
            const signed = retained.signed;
            if (retained.validation) |witness| {
                const frozen = @import("relational_index_delivery_journal.zig").frozenTargetForRecovery(a, txn, group, head, owner, signed.proof.identity.metadata_incarnation) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return error.InvalidCatalogRecord,
                };
                if (!std.mem.eql(u8, &try frozen.expected.digest(), &try witness.outcome.expected.digest()) or
                    frozen.source_members != witness.outcome.audit.source_members.? or
                    witness.outcome.audit.canonical_position.?.sequence <= frozen.source_position.sequence) return error.InvalidCatalogRecord;
            }
            if (!std.mem.eql(u8, &signed.proof.identity.metadata_incarnation, &std.fmt.bytesToHex(parsed.value.table_guard.incarnation, .lower))) return error.InvalidCatalogRecord;
            // Usually all stages share one grant. Cache only a fixed-size
            // immutable identity in this pinned view, never a borrowed value.
            if (cached_enrollment == null or !std.meta.eql(cached_enrollment.?, signed.proof.identity)) {
                const enrolled_bytes = try optionalGet(txn, try root_enrollment.key(&buf, group, signed.proof.identity.store_id, signed.proof.identity.root_incarnation)) orelse return error.InvalidCatalogRecord;
                if (enrolled_bytes.len > 1024) return error.InvalidCatalogRecord;
                var enrolled = std.json.parseFromSlice(root_enrollment.Identity, a, enrolled_bytes, .{}) catch |err| switch (err) {
                    error.OutOfMemory => return err,
                    else => return error.InvalidCatalogRecord,
                };
                defer enrolled.deinit();
                if (!std.meta.eql(enrolled.value, signed.proof.identity)) return error.InvalidCatalogRecord;
                cached_enrollment = enrolled.value;
            }
            if (try owner.latestReceiptAction() == action and !std.meta.eql(signed.receipt.position, owner.position.?)) return error.InvalidCatalogRecord;
            receipt_count += 1;
        }
        for (&counts, 0..) |*count, flag| if (owner.flags & (@as(u8, 1) << @as(u3, @intCast(flag))) != 0) {
            count.* += 1;
        };
    }
    if (!std.meta.eql(counts, head.counts)) return error.InvalidCatalogRecord;
    if (delivery_rows[0] != delivery_rows[1]) return error.InvalidCatalogRecord;
    if (completed_sources != head.completed_sources) return error.InvalidCatalogRecord;
    const pending = switch (head.phase) {
        .installing, .releasing, .published, .canceled => false,
        else => true,
    };
    const active = try optionalGet(txn, try activeKey(&buf, group, table));
    if (pending) {
        if (active == null or !std.mem.eql(u8, active.?, &head.plan_id)) return error.InvalidCatalogRecord;
    } else if (active != null) return error.InvalidCatalogRecord;
    const lock = try lifecycle.read(txn, group, table);
    if (head.phase != .published and head.phase != .canceled) {
        if (lock == null or lock.?.kind != .index_build or !std.mem.eql(u8, &lock.?.id, &head.plan_id)) return error.InvalidCatalogRecord;
    } else if (lock) |held| {
        // A terminal attempt may coexist with a newer, unrelated lifecycle job.
        if (held.kind == .index_build) return error.InvalidCatalogRecord;
    }
    return .{ .plan = parsed, .prepared = prepared, .head = head, .receipt_count = receipt_count, .delivery_counts = delivery_counts };
}

test "relational index publication durable keys are canonical group scoped and exclude owner progress from namespace sources" {
    var buf: [160]u8 = undefined;
    const Kind = enum { plan, active, head, owner, group, inputs, dispatch, proof };
    inline for (std.meta.tags(Kind)) |kind| {
        const key = switch (kind) {
            .plan => try planKey(&buf, 13, 101),
            .active => try activeKey(&buf, 13, 101),
            .head => try headKey(&buf, 13, 101),
            .owner => try ownerKey(&buf, 13, 101, 0),
            .group => try ownerGroupKey(&buf, 13, 101, 301),
            .inputs => try inputsKey(&buf, 13, 101),
            .dispatch => try dispatchKey(&buf, 13, 101, 0),
            .proof => try proofKey(&buf, 13, 101, 0, .validated),
        };
        try std.testing.expectEqual(@as(?u64, 13), try groupFromKey(key));
        try std.testing.expectEqual(null, try sourceKey(key, 14));
        try std.testing.expectEqual(kind == .plan or kind == .active, try sourceKey(key, 13) != null);
    }
    for ([_][]const u8{
        "plan:013:0000000000000065",        "plan:13:00000000000000Af",          "plan:13:0000000000000000",
        "head:0:0000000000000065",          "owner:13:0000000000000065:1000",    "owner:13:0000000000000065:000A",
        "active:13:0000000000000065:extra", "unknown:13:0000000000000065",       "group:13:0000000000000065:0000000000000000",
        "group:13:0000000000000065:000A",   "dispatch:13:0000000000000065:1000", "inputs:13:0000000000000065:extra",
    }) |suffix| {
        const key = try std.fmt.bufPrint(&buf, "{s}{s}", .{ prefix, suffix });
        try std.testing.expectError(error.InvalidCatalogRecord, groupFromKey(key));
    }
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, ownerKey(&buf, 13, 101, progress.max_owners));
}

test "relational index publication frame preflight bounds escaping before allocation" {
    const a = std.testing.allocator;
    const huge = try a.alloc(u8, max_bytes / 6 + 1);
    defer a.free(huge);
    @memset(huge, 0);
    try std.testing.expectError(error.IndexPublicationTooLarge, encodeBounded(std.testing.failing_allocator, .{ .text = huge }));
    const small = try encodeBounded(a, .{ .value = "quoted \" value" });
    defer a.free(small);
    try std.testing.expectEqualStrings("{\"value\":\"quoted \\\" value\"}", small);
}

fn testPlan(a: Allocator) !Plan {
    const before_json =
        \\{"version":1,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}}}
    ;
    const after_json =
        \\{"version":2,"storage_mode":"relational","default_type":"row","checks":[{"name":"positive","column":"id","op":"gt","value":0}],"document_schemas":{"row":{"schema":{"type":"object","properties":{"id":{"type":"integer"},"email":{"type":"string"}},"additionalProperties":false}}},"relational_indexes":[{"name":"email_idx","keys":[{"column":"email"}]}],"unique_constraints":[{"name":"email_idx","origin":"index","columns":["email"]}]}
    ;
    const before: records.TableRecord = .{ .table_id = 101, .name = "physical:101", .schema_json = before_json };
    const after = try @import("../api/tables.zig").prepareForeignKeyPublicationRecord(a, &before, after_json);
    var schema = try local.schema_mod.parseValidatedTableSchema(a, before_json);
    defer schema.deinit(a);
    var compiled = try @import("fk_generation_publication.zig").compileCatalog(a, schema, 101, null);
    defer compiled.deinit();
    const b64 = try a.alloc(u8, std.base64.standard.Encoder.calcSize(compiled.value.len));
    _ = std.base64.standard.Encoder.encode(b64, compiled.value);
    const owners = try a.alloc(Owner, 1);
    owners[0] = .{
        .range = .{ .table_id = 101, .group_id = 301, .range_id = 301, .doc_identity_shard_id = 301, .doc_identity_range_id = 301, .start_key = "" },
        .namespace = .{ .table_id = 101, .shard_id = 301, .range_id = 301 },
        .admission_epoch = 7,
        .receipt_authority = .raft,
        .catalog_digest = digest(compiled.value),
    };
    return .{
        .id = @splat(1),
        .namespace_id = domain.default_namespace_id,
        .index_name = "email_idx",
        .table_guard = .{ .target = .{ .name = "items" }, .logical_table = "items", .owner = .{ .table_id = 101, .schema_version = 1, .schema_digest = digest(before_json), .kind = .table }, .incarnation = @splat(1) },
        .before = before,
        .after = after,
        .catalog_before_b64 = b64,
        .owners = owners,
    };
}

pub const testing = if (@import("builtin").is_test) struct {
    pub const plan = testPlan;

    /// Historical signature fixture only; live ownership admission is tested
    /// through the real metadata transaction/enrollment adapters separately.
    pub fn signedHistoricalReceipt(receipt: progress.Receipt, incarnation: [16]u8) !SignedReceipt {
        const seed: [32]u8 = @splat(9);
        const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
        const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
        return .{ .receipt = receipt, .proof = try (RootProof{
            .identity = .{ .metadata_incarnation = std.fmt.bytesToHex(incarnation, .lower), .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
            .reporter_incarnation = 4,
            .authority = if (receipt.owner.authority == .native) .{ .native = @splat(7) } else .{ .raft = 5 },
            .root_generation = 6,
            .signature = undefined,
        }).sign(root, try receipt.digest()) };
    }

    pub fn completeAckStatus(request: Ack) !AckStatus {
        const pending = pendingAckStatus(request);
        const update = try progress.acknowledge(pending.head, pending.owner, request.expected_revision, request.signed.receipt);
        var owner = update.owner;
        owner.attestations[@backingInt(request.signed.receipt.action)] = try request.signed.commitment();
        return .{ .head = update.head, .owner = owner };
    }

    pub fn fencedAck() !Ack {
        const seed: [32]u8 = @splat(9);
        const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
        const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
        const namespace: local.storage_db_doc_identity.Namespace = .{ .table_id = 101, .shard_id = 301, .range_id = 401 };
        const receipt: progress.Receipt = .{
            .owner = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .ordinal = 0, .group_id = 301, .namespace = namespace, .authority = .native, .fence_digest = @splat(3) },
            .action = .fenced,
            .position = .{ .native = .{ .namespace = namespace, .sequence = 5 } },
            .candidate_digest = @splat(4),
            .reservation_digest = @splat(5),
            .evidence_digest = @splat(6),
        };
        return .{ .table_id = 101, .expected_revision = 1, .signed = .{ .receipt = receipt, .proof = try (RootProof{
            .identity = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
            .reporter_incarnation = 4,
            .authority = .{ .native = @splat(7) },
            .root_generation = 6,
            .signature = undefined,
        }).sign(root, try receipt.digest()) } };
    }

    pub fn pendingAckStatus(request: Ack) AckStatus {
        const receipt = request.signed.receipt;
        return .{
            .head = .{ .plan_id = receipt.owner.plan_id, .plan_digest = receipt.owner.plan_digest, .candidate_digest = receipt.candidate_digest, .reservation_digest = receipt.reservation_digest, .table_id = request.table_id, .owner_count = 1 },
            .owner = .{ .owner = receipt.owner },
        };
    }
} else struct {};

test "relational index publication ACK status binds exact committed digest not revisions or phases" {
    const request = try testing.fencedAck();
    const pending = testing.pendingAckStatus(request);
    try std.testing.expect(!try pending.isAcknowledged(request));
    const complete = try testing.completeAckStatus(request);
    try std.testing.expect(try complete.isAcknowledged(request));
    const root: local.storage_db_root_signing_identity.State = .{ .seed = @splat(9), .root_incarnation = request.signed.proof.identity.root_incarnation, .public_key = request.signed.proof.identity.public_key };
    var other_root = request;
    other_root.signed.proof.root_generation += 1;
    other_root.signed.proof = try other_root.signed.proof.sign(root, try other_root.signed.receipt.digest());
    try other_root.validate();
    try std.testing.expect(!try complete.isAcknowledged(other_root));
    var missing_root = complete;
    missing_root.owner.attestations[0] = @splat(0);
    try std.testing.expectError(error.InvalidCatalogRecord, missing_root.isAcknowledged(request));
    var bad = complete;
    bad.owner.receipts[0][0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, bad.isAcknowledged(request));
    bad = complete;
    bad.head.plan_id[0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, bad.isAcknowledged(request));
    var future = request;
    future.expected_revision = complete.head.revision + 1;
    try std.testing.expectError(error.GenerationPublicationChanged, complete.isAcknowledged(future));
    future.expected_revision = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, future.validate());
    try std.testing.expectEqual(error.IndexOwnerReceiptChanged, ownerAckHttpFailure(409, "IndexOwnerReceiptChanged").?);
    try std.testing.expect(ownerAckHttpFailure(503, "IndexOwnerReceiptChanged") == null);
    try std.testing.expect(ownerAckHttpFailure(409, "unrecognized") == null);
}

test "relational index publication signatures bind owner authority position and root generation" {
    const seed: [32]u8 = @splat(9);
    const pair = try std.crypto.sign.Ed25519.KeyPair.generateDeterministic(seed);
    const root: local.storage_db_root_signing_identity.State = .{ .seed = seed, .root_incarnation = 7, .public_key = pair.public_key.toBytes() };
    inline for (.{ progress.Authority.raft, progress.Authority.native }) |authority| {
        const namespace: local.storage_db_doc_identity.Namespace = .{ .table_id = 101, .shard_id = 301, .range_id = 401 };
        const receipt: progress.Receipt = .{
            .owner = .{ .plan_id = @splat(1), .plan_digest = @splat(2), .ordinal = 0, .group_id = 301, .namespace = namespace, .authority = authority, .fence_digest = @splat(3) },
            .action = .fenced,
            .position = if (authority == .native) .{ .native = .{ .namespace = namespace, .sequence = 5 } } else .{ .raft = .{ .term = 2, .index = 5 } },
            .candidate_digest = @splat(4),
            .reservation_digest = @splat(5),
            .evidence_digest = @splat(6),
        };
        const signed: SignedReceipt = .{ .receipt = receipt, .proof = try (RootProof{
            .identity = .{ .metadata_incarnation = @splat('1'), .node_id = 2, .store_id = 3, .root_incarnation = 7, .public_key = root.public_key },
            .reporter_incarnation = 4,
            .authority = if (authority == .native) .{ .native = @splat(7) } else .{ .raft = 5 },
            .root_generation = 6,
            .signature = undefined,
        }).sign(root, try receipt.digest()) };
        try signed.verifySignature();
        const encoded = try signed.encode();
        try std.testing.expectEqual(@as(usize, 512), encoded.len);
        try std.testing.expectEqualDeep(signed, try SignedReceipt.decode(&encoded));
        for (0..encoded.len) |offset| {
            var damaged = encoded;
            damaged[offset] ^= 1;
            try std.testing.expectError(error.InvalidCatalogRecord, SignedReceipt.decode(&damaged));
        }
        for ([_]usize{ 7, 58, 161, 369, 416 }) |offset| {
            var damaged = encoded;
            damaged[offset] ^= 1;
            @memcpy(damaged[480..512], &digest(damaged[0..480]));
            try std.testing.expectError(error.InvalidCatalogRecord, SignedReceipt.decode(&damaged));
        }
        if (authority == .raft) {
            var damaged = encoded;
            damaged[384] = 1;
            @memcpy(damaged[480..512], &digest(damaged[0..480]));
            try std.testing.expectError(error.InvalidCatalogRecord, SignedReceipt.decode(&damaged));
        }
        var altered = signed;
        altered.receipt.owner.namespace.shard_id += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        if (authority == .native) altered.receipt.position.native.sequence += 1 else altered.receipt.position.raft.index += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.receipt.candidate_digest[0] ^= 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.receipt.action = .reserved;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.proof.root_generation += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.proof.reporter_incarnation += 1;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.proof.authority = if (authority == .native) .{ .raft = 5 } else .{ .native = @splat(7) };
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        // A valid root cannot certify one logical authority using the other's
        // physical ownership contract, even with a freshly valid signature.
        altered.proof = try altered.proof.sign(root, try receipt.digest());
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
        altered = signed;
        altered.proof.authority = if (authority == .native) .{ .native = @splat(0) } else .{ .raft = 0 };
        try std.testing.expectError(error.InvalidGenerationPublication, altered.proof.sign(root, try receipt.digest()));
        const foreign = try (@import("initial_child_root_attestation.zig").Attestation{
            .identity = signed.proof.identity,
            .reporter_incarnation = signed.proof.reporter_incarnation,
            .replica_id = 5,
            .root_generation = signed.proof.root_generation,
            .signature = undefined,
        }).sign(root, try receipt.digest());
        altered = signed;
        altered.proof.signature = foreign.signature;
        try std.testing.expectError(error.InvalidIndexOwnerReceipt, altered.verifySignature());
    }
}

test "relational index publication native dispatch binds exact admitted fence and packs raw manifest without recompilation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var plan = try testPlan(arena.allocator());
    var native_owner = [_]Owner{plan.owners[0]};
    native_owner[0].receipt_authority = .native;
    plan.owners = &native_owner;
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    var dispatch = try std.json.parseFromSlice(Dispatch, a, prepared.dispatch[0], .{});
    defer dispatch.deinit();
    var inputs = try std.json.parseFromSlice(Inputs, a, prepared.inputs, .{});
    defer inputs.deinit();
    const decision: OwnerDecision = .{
        .metadata_group_id = 13,
        .metadata_incarnation = @splat('1'),
        .head = prepared.head,
        .owner = prepared.owners[0],
        .disposition = .execute,
        .dispatch = dispatch.value,
        .inputs = inputs.value,
    };
    const admission = try decision.nativeAdmission();
    const request: OwnerRequest = .{
        .table_id = plan.before.table_id,
        .group_id = native_owner[0].range.group_id,
        .id = plan.id,
        .operation = .fence,
    };
    // The hot permission projection needs no schema-sized inputs. Admission
    // still requires them, but control/status dispatch must bind the request.
    var permission = decision;
    permission.inputs = null;
    const execution_identity = try permission.nativeExecutionIdentity(request);
    try std.testing.expectEqual(admission.id, execution_identity.id);
    try std.testing.expectEqual(admission.owner_digest, execution_identity.owner_digest);
    // This synthetic binding tests only projection conversion. It is not
    // committed storage evidence and is never signed or sent as an ACK.
    const binding: local.storage_db_relational_index_owner_job.Binding = .{
        .id = admission.id,
        .plan_digest = admission.plan_digest,
        .owner_digest = admission.owner_digest,
        .ordinal = admission.ordinal,
        .authority = .native,
        .fence = admission.fence,
        .integrity_before = admission.fence.catalog_digest,
        .integrity_reserved = admission.reservation_digest,
        .integrity_candidate = admission.candidate_digest,
        .schema_before = @splat(3),
        .physical_before = null,
        .physical_reserved = .{ .revision = 2, .schema_version = 1, .blob_digest = @splat(4) },
        .physical_candidate = .{ .revision = 2, .schema_version = 2, .blob_digest = @splat(5) },
        .index = .{ .generation = 2, .slot = 0 },
        .inputs_digest = @splat(6),
    };
    const fence_control = try permission.nativeControl(request, binding);
    try std.testing.expectEqual(.fenced, fence_control.effect);
    try std.testing.expectEqualSlices(u8, &try binding.encode(), &try fence_control.binding.encode());
    try std.testing.expect(fence_control.inputs_b64 == null);
    const fenced = try progress.acknowledge(permission.head, permission.owner, permission.head.revision, .{
        .owner = permission.owner.owner,
        .action = .fenced,
        .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = 1 } },
        .candidate_digest = permission.head.candidate_digest,
        .reservation_digest = permission.head.reservation_digest,
        .evidence_digest = @splat(9),
    });
    var reserving = permission;
    reserving.head = fenced.head;
    reserving.owner = fenced.owner;
    var reserve_request = request;
    reserve_request.operation = .reserve;
    var already_reserved = binding;
    already_reserved.phase = .reserved;
    const reserve_control = try reserving.nativeControl(reserve_request, already_reserved);
    try std.testing.expectEqual(.reserved, reserve_control.effect);
    try std.testing.expectEqual(.fencing, reserve_control.binding.phase);
    try std.testing.expectEqual(.reserved, already_reserved.phase);
    try std.testing.expectEqualSlices(u8, &try binding.encode(), &try reserve_control.binding.encode());
    const reserved = try progress.acknowledge(reserving.head, reserving.owner, reserving.head.revision, .{
        .owner = reserving.owner.owner,
        .action = .reserved,
        .position = .{ .native = .{ .namespace = binding.fence.namespace, .sequence = 2 } },
        .candidate_digest = reserving.head.candidate_digest,
        .reservation_digest = reserving.head.reservation_digest,
        .evidence_digest = @splat(10),
    });
    var building = permission;
    building.head = reserved.head;
    building.owner = reserved.owner;
    var build_request = request;
    build_request.operation = .build;
    const build = try building.nativeBuild(build_request, already_reserved, .{ .records = 7 });
    try build.requireBinding(already_reserved);
    try std.testing.expectEqual(@as(usize, 7), build.budget.records);
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, building.nativeBuild(build_request, binding, .{}));
    try std.testing.expectError(error.GenerationPublicationChanged, permission.nativeBuild(build_request, already_reserved, .{}));
    try std.testing.expectError(error.InvalidGenerationPublication, building.nativeBuild(reserve_request, already_reserved, .{}));
    var replaced = already_reserved;
    replaced.inputs_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, build.requireBinding(replaced));
    try std.testing.expectError(error.InvalidRelationalIndexBudget, building.nativeBuild(build_request, already_reserved, .{ .records = 0 }));
    try std.testing.expectError(error.GenerationPublicationChanged, permission.nativeControl(reserve_request, binding));
    var changed_binding = binding;
    changed_binding.owner_digest[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, permission.nativeControl(request, changed_binding));
    changed_binding = binding;
    changed_binding.fence.admission_epoch += 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, permission.nativeControl(request, changed_binding));
    changed_binding = binding;
    changed_binding.integrity_candidate[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, permission.nativeControl(request, changed_binding));
    changed_binding = binding;
    changed_binding.integrity_reserved[0] ^= 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, permission.nativeControl(request, changed_binding));
    changed_binding = binding;
    changed_binding.authority = .raft;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, permission.nativeControl(request, changed_binding));
    changed_binding = binding;
    changed_binding.physical_reserved.schema_version += 1;
    changed_binding.physical_candidate.schema_version += 1;
    try std.testing.expectError(error.PrivateIndexOwnerJobChanged, permission.nativeControl(request, changed_binding));
    var unsupported = request;
    unsupported.operation = .cancel;
    try std.testing.expectError(error.InvalidGenerationPublication, permission.nativeControl(unsupported, binding));
    try std.testing.expectError(error.GenerationPublicationChanged, permission.nativeCancellationIntent());
    var cancel_permission = permission;
    cancel_permission.head = try permission.head.cancel(permission.head.revision, .requested);
    const intent = try cancel_permission.nativeCancellationIntent();
    try std.testing.expect(intent.eql(admission));
    try std.testing.expect(cancel_permission.inputs == null);
    cancel_permission.disposition = .acknowledged;
    try std.testing.expectError(error.InvalidGenerationPublication, cancel_permission.nativeCancellationIntent());
    var wrong_request = request;
    wrong_request.table_id += 1;
    try std.testing.expectError(error.GenerationPublicationChanged, permission.nativeExecutionIdentity(wrong_request));
    wrong_request = request;
    wrong_request.group_id += 1;
    try std.testing.expectError(error.GenerationPublicationChanged, permission.nativeExecutionIdentity(wrong_request));
    wrong_request = request;
    wrong_request.id[0] ^= 1;
    try std.testing.expectError(error.GenerationPublicationChanged, permission.nativeExecutionIdentity(wrong_request));
    wrong_request = request;
    wrong_request.operation = .reserve;
    try std.testing.expectError(error.GenerationPublicationChanged, permission.nativeExecutionIdentity(wrong_request));
    permission.disposition = .acknowledged;
    try std.testing.expectError(error.InvalidGenerationPublication, permission.nativeExecutionIdentity(request));
    permission = decision;
    permission.metadata_group_id = 0;
    try std.testing.expectError(error.InvalidGenerationPublication, permission.nativeExecutionIdentity(request));
    permission = decision;
    permission.metadata_incarnation = @splat(0);
    try std.testing.expectError(error.InvalidGenerationPublication, permission.nativeExecutionIdentity(request));
    try std.testing.expectEqual(plan.id, admission.id);
    try std.testing.expectEqual(prepared.head.plan_digest, admission.plan_digest);
    try std.testing.expectEqual(prepared.owners[0].owner.fence_digest, admission.owner_digest);
    try std.testing.expectEqual(prepared.head.candidate_digest, admission.candidate_digest);
    try std.testing.expectEqual(prepared.head.reservation_digest, admission.reservation_digest);
    try std.testing.expectEqual(@as(u64, 7), admission.fence.admission_epoch);
    try std.testing.expectEqual(std.mem.readInt(u64, plan.id[0..8], .little), admission.fence.transition_id);
    try std.testing.expectEqual(std.mem.readInt(u64, plan.id[8..16], .little), admission.fence.attempt);
    try std.testing.expectEqual(@as(u64, 301), admission.fence.owner_group_id);
    try std.testing.expectEqual(admission.fence.owner_group_id, admission.fence.peer_group_id);
    try std.testing.expectEqual(.index_build, admission.fence.role);
    try std.testing.expectEqual(native_owner[0].namespace, admission.fence.namespace);
    try std.testing.expectEqual(native_owner[0].catalog_digest, admission.fence.catalog_digest);
    var manifest = try inputs.value.prepareNativeManifest(a, prepared.head);
    defer manifest.deinit();
    try std.testing.expectEqualSlices(u8, prepared.candidate.inputs.bytes, manifest.bytes);
    const Check = struct {
        fn run(allocator: Allocator, input: Inputs, head: progress.Head) !void {
            var owned_manifest = try input.prepareNativeManifest(allocator, head);
            defer owned_manifest.deinit();
        }
    };
    try std.testing.checkAllAllocationFailures(a, Check.run, .{ inputs.value, prepared.head });
    var bad = decision;
    bad.inputs = null;
    try std.testing.expectError(error.InvalidGenerationPublication, bad.nativeAdmission());
    bad = decision;
    bad.disposition = .acknowledged;
    try std.testing.expectError(error.InvalidGenerationPublication, bad.nativeAdmission());
    bad = decision;
    bad.head = try prepared.head.cancel(prepared.head.revision, .requested);
    try std.testing.expectError(error.GenerationPublicationChanged, bad.nativeAdmission());
    bad = decision;
    bad.dispatch.?.owner.admission_epoch += 1;
    try std.testing.expectError(error.InvalidCatalogRecord, bad.nativeAdmission());
    bad = decision;
    bad.dispatch.?.owner.namespace.range_id += 1;
    try std.testing.expectError(error.InvalidCatalogRecord, bad.nativeAdmission());
    bad = decision;
    bad.inputs.?.after.schema_json = "{}";
    try std.testing.expectError(error.InvalidCatalogRecord, bad.nativeAdmission());
    bad = decision;
    bad.inputs.?.before.schema_json = "{}";
    try std.testing.expectError(error.InvalidCatalogRecord, bad.nativeAdmission());
    bad = decision;
    bad.dispatch.?.successor_schema_version += 1;
    try std.testing.expectError(error.InvalidCatalogRecord, bad.nativeAdmission());
    var malformed = inputs.value;
    malformed.catalog_before_b64 = "not base64!";
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, malformed.prepareNativeManifest(a, prepared.head));
    malformed = inputs.value;
    malformed.plan_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidCatalogRecord, malformed.prepareNativeManifest(a, prepared.head));
    // IDs must be representable without replacing either half by a fake
    // transition or attempt. Reject them at metadata admission, not data apply.
    var invalid_plan = plan;
    @memset(invalid_plan.id[0..8], 0);
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid_plan.prepare(a));
    invalid_plan = plan;
    @memset(invalid_plan.id[8..16], 0);
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid_plan.prepare(a));
}

test "relational index publication prepares owned immutable head owner and reserved name postimages" {
    const a = std.testing.allocator;
    var prepared = blk: {
        var scratch = std.heap.ArenaAllocator.init(a);
        defer scratch.deinit();
        const plan = try testPlan(scratch.allocator());
        break :blk try plan.prepare(a);
    };
    defer prepared.deinit();
    try std.testing.expectEqual(progress.Phase.fencing, prepared.head.phase);
    try std.testing.expectEqual(@as(u16, 1), prepared.head.owner_count);
    try std.testing.expectEqualSlices(u8, &digest(prepared.frame), &prepared.head.plan_digest);
    try std.testing.expectEqualStrings("email_idx", prepared.name_key.name);
    const pending = try (names.Entry{}).reserve(prepared.name_reservation);
    try std.testing.expect(pending.active == null);
    try std.testing.expectEqual(names.Phase.reserved, pending.pending.?.phase);
    try std.testing.expectEqual(@as(u32, 1), prepared.candidate.reservation.catalog.schema_version);
    try std.testing.expect(prepared.candidate.reservation.catalog.find(.unique, "email_idx") == null);
    // Decode the immutable frame after preparation; progress is not embedded
    // in the plan, and every typed owner/schema identity survives persistence.
    var decoded = try std.json.parseFromSlice(Plan, a, prepared.frame, .{});
    defer decoded.deinit();
    try std.testing.expectEqualSlices(u8, &try ownerFenceDigest(decoded.value.owners[0]), &prepared.owners[0].owner.fence_digest);
    var replay = try decoded.value.prepare(a);
    defer replay.deinit();
    try std.testing.expectEqualSlices(u8, prepared.frame, replay.frame);
    try std.testing.expectEqualSlices(u8, &prepared.head.plan_digest, &replay.head.plan_digest);
}

test "relational index publication worker projections use three point reads own borrowed bytes and clean every allocation failure" {
    const Reader = struct {
        prepared: *const Prepared,
        scratch: []u8,
        calls: usize = 0,
        pub fn get(self: *@This(), key: []const u8) ![]const u8 {
            self.calls += 1;
            var buf: [160]u8 = undefined;
            const source = if (std.mem.eql(u8, key, try inputsKey(&buf, 13, self.prepared.head.table_id)))
                self.prepared.inputs
            else if (std.mem.eql(u8, key, try dispatchKey(&buf, 13, self.prepared.head.table_id, 0)))
                self.prepared.dispatch[0]
            else
                return error.UnexpectedRead;
            @memset(self.scratch, 0);
            @memcpy(self.scratch[0..source.len], source);
            return self.scratch[0..source.len];
        }
        fn exercise(a: Allocator, p: *const Prepared) !void {
            const scratch = try a.alloc(u8, @max(p.inputs.len, p.dispatch[0].len));
            defer a.free(scratch);
            var reader: @This() = .{ .prepared = p, .scratch = scratch };
            var inputs = try readInputs(a, &reader, 13, p.head);
            defer inputs.deinit();
            var dispatch = try readDispatch(a, &reader, 13, p.owners[0].owner);
            defer dispatch.deinit();
            var decision = try readDecisionInputs(a, &reader, 13, p.head);
            defer decision.deinit();
            @memset(scratch, 0);
            try std.testing.expectEqual(@as(usize, 3), reader.calls);
            try std.testing.expectEqualDeep(inputs.value.before, decision.value.before);
            try std.testing.expectEqualDeep(inputs.value.after, decision.value.after);
            try std.testing.expectEqualSlices(u8, &dispatch.value.table_guard.owner.schema_digest, &digest(inputs.value.before.schema_json));
            var candidate = try inputs.value.prepareCandidate(a, p.head);
            defer candidate.deinit();
            try std.testing.expectEqualSlices(u8, p.candidate.next.value, candidate.next.value);
            try std.testing.expectEqualStrings(inputs.value.before.name, dispatch.value.table_name);
            try std.testing.expectEqualSlices(u8, &digest(inputs.value.after.schema_json), &dispatch.value.successor_schema_digest);
        }
    };
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try testPlan(arena.allocator());
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    try Reader.exercise(a, &prepared);
    try @import("antfly_platform").allocator.checkAllAllocationFailures(a, Reader.exercise, .{&prepared});
}

test "relational index publication decision projection skips large predecessor payloads within bounded memory" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try testPlan(arena.allocator());
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    var parsed = try std.json.parseFromSlice(Inputs, a, prepared.inputs, .{});
    defer parsed.deinit();
    // Synthetic opaque input tests decoder memory only, not an admitted or
    // independently compiled catalog. Decisions do not reinterpret this field.
    const omitted_payload = try arena.allocator().alloc(u8, 1024 * 1024);
    @memset(omitted_payload, 'a');
    parsed.value.catalog_before_b64 = omitted_payload;
    const bytes = try std.json.Stringify.valueAlloc(a, parsed.value, .{});
    defer a.free(bytes);
    const Reader = struct {
        bytes: []const u8,
        pub fn get(self: @This(), _: []const u8) ![]const u8 {
            return self.bytes;
        }
    };
    var reader: Reader = .{ .bytes = bytes };
    var memory: [64 * 1024]u8 = undefined;
    var limited = std.heap.FixedBufferAllocator.init(&memory);
    {
        var projected = try readDecisionInputs(limited.allocator(), &reader, 13, prepared.head);
        defer projected.deinit();
        try std.testing.expectEqualDeep(parsed.value.after, projected.value.after);
    }
    limited.reset();
    try std.testing.expectError(error.OutOfMemory, readInputs(limited.allocator(), &reader, 13, prepared.head));
}

test "relational index publication dispatch omits schema sized inputs and immutable projections replay exactly" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var plan = try testPlan(arena.allocator());
    const description = try arena.allocator().alloc(u8, 64 * 1024);
    @memset(description, 'x');
    plan.before.description = description;
    plan.after.description = description;
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    try std.testing.expect(prepared.inputs.len > 128 * 1024);
    try std.testing.expect(prepared.dispatch[0].len < 4096);
    try std.testing.expect(std.mem.indexOf(u8, prepared.dispatch[0], description[0..32]) == null);
    var decoded = try std.json.parseFromSlice(Plan, a, prepared.frame, .{});
    defer decoded.deinit();
    var replay = try decoded.value.prepare(a);
    defer replay.deinit();
    try std.testing.expectEqualSlices(u8, prepared.inputs, replay.inputs);
    try std.testing.expectEqualSlices(u8, prepared.dispatch[0], replay.dispatch[0]);
}

test "relational index publication reader pair joins active and prospective physical axes" {
    var before: records.TableRecord = .{ .table_id = 7, .name = "rows" };
    var after = before;
    try std.testing.expect(try readerRequirements(before, after) == null);
    before.schema_reader_requirements = .{ .semantic_version = 30, .encoding_version = 17 };
    after.schema_reader_requirements = .{ .semantic_version = 14, .encoding_version = 31 };
    try std.testing.expectEqualDeep(@as(?readers.ReaderRequirements, .{ .semantic_version = 30, .encoding_version = 31 }), try readerRequirements(before, after));
    after.schema_reader_requirements.?.semantic_version = 0;
    try std.testing.expectError(error.InvalidSchemaReaderRequirements, readerRequirements(before, after));
}

test "relational index publication projected candidate rejects changed admitted fingerprints" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try testPlan(arena.allocator());
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    var inputs = try std.json.parseFromSlice(Inputs, a, prepared.inputs, .{});
    defer inputs.deinit();
    var candidate = try inputs.value.prepareCandidate(a, prepared.head);
    defer candidate.deinit();
    var changed = prepared.head;
    changed.candidate_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, inputs.value.prepareCandidate(a, changed));
    changed = prepared.head;
    changed.reservation_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, inputs.value.prepareCandidate(a, changed));
    changed = prepared.head;
    changed.plan_id[0] ^= 1;
    try std.testing.expectError(error.InvalidCatalogRecord, inputs.value.prepareCandidate(a, changed));
    changed = prepared.head;
    changed.reader_requirements = .{ .semantic_version = 30, .encoding_version = 17 };
    try std.testing.expectError(error.InvalidCatalogRecord, inputs.value.prepareCandidate(a, changed));
    try std.testing.expectError(error.InvalidCatalogRecord, inputs.value.prepareNativeManifest(a, changed));
}

test "relational index publication permission separates phase execution acknowledged retries and cancellation" {
    const a = std.testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const plan = try testPlan(arena.allocator());
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    var head = prepared.head;
    var owner = prepared.owners[0];
    try std.testing.expectEqual(.execute, try ownerPermission(head, owner, .fence));
    try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owner, .reserve));
    const canceled = try head.cancel(head.revision, .requested);
    try std.testing.expectEqual(.execute, try ownerPermission(canceled, owner, .cancel));
    try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(canceled, owner, .fence));
    for ([_]progress.Action{ .fenced, .reserved, .validated }, 0..) |action, i| {
        const update = try progress.acknowledge(head, owner, head.revision, .{
            .owner = owner.owner,
            .action = action,
            .position = .{ .raft = .{ .term = 1, .index = i + 1 } },
            .candidate_digest = head.candidate_digest,
            .reservation_digest = head.reservation_digest,
            .evidence_digest = @splat(9),
        });
        head = update.head;
        owner = update.owner;
        try std.testing.expectEqual(.acknowledged, try ownerPermission(head, owner, .fence));
    }
    try std.testing.expectEqual(.acknowledged, try ownerPermission(head, owner, .build));
    try std.testing.expectError(error.GenerationPublicationChanged, ownerPermission(head, owner, .install));
    head = try head.decidePublish(head.revision);
    try std.testing.expectEqual(.execute, try ownerPermission(head, owner, .install));
    const Reader = struct {
        value: ?[]const u8,
        pub fn get(self: *@This(), _: []const u8) anyerror![]const u8 {
            return self.value orelse error.NotFound;
        }
    };
    var reader: Reader = .{ .value = null };
    try requireActivePointer(&reader, 13, head);
    reader.value = &head.plan_id;
    try std.testing.expectError(error.GenerationPublicationChanged, requireActivePointer(&reader, 13, head));
    try requireActivePointer(&reader, 13, prepared.head);
    reader.value = null;
    try std.testing.expectError(error.GenerationPublicationChanged, requireActivePointer(&reader, 13, prepared.head));
}

test "relational index publication rejects stale guards partial cuts and substituted owner identities" {
    const a = std.testing.allocator;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    const plan = try testPlan(scratch.allocator());
    var invalid = plan;
    invalid.after.description = "unrelated mutation";
    try std.testing.expectError(error.InvalidGenerationPublication, invalid.prepare(a));
    invalid = plan;
    invalid.table_guard.owner.schema_digest[0] ^= 1;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    var owner = plan.owners[0];
    owner.catalog_digest[0] ^= 1;
    invalid = plan;
    invalid.owners = &.{owner};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    owner = plan.owners[0];
    owner.range.end_key = "m";
    invalid.owners = &.{owner};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    owner = plan.owners[0];
    owner.namespace.range_id += 1;
    invalid.owners = &.{owner};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
    invalid = plan;
    invalid.owners = &.{};
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, invalid.prepare(a));
}

test "relational index publication binds complete disjoint multiowner cuts and rejects duplicate groups" {
    const a = std.testing.allocator;
    var scratch = std.heap.ArenaAllocator.init(a);
    defer scratch.deinit();
    var plan = try testPlan(scratch.allocator());
    var owners = [_]Owner{ plan.owners[0], plan.owners[0] };
    owners[0].range.end_key = "m";
    owners[1].range.start_key = "m";
    owners[1].range.group_id = 302;
    owners[1].range.range_id = 302;
    owners[1].range.doc_identity_shard_id = 302;
    owners[1].range.doc_identity_range_id = 302;
    owners[1].namespace = .{ .table_id = 101, .shard_id = 302, .range_id = 302 };
    plan.owners = &owners;
    var prepared = try plan.prepare(a);
    defer prepared.deinit();
    try std.testing.expectEqual(@as(u16, 2), prepared.head.owner_count);
    try std.testing.expectEqual(@as(u16, 1), prepared.owners[1].owner.ordinal);
    owners[1].range.group_id = owners[0].range.group_id;
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, plan.prepare(a));
    owners[1].range.group_id = 302;
    owners[1].range.start_key = "n";
    try std.testing.expectError(error.InvalidIndexPublicationCandidate, plan.prepare(a));
}
