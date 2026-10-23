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

//! Private schema-validation owner wire contract. Fixed scope and control
//! projections are independent of metadata storage, immutable plan decoding
//! and publication writers. Decoding these types grants no authority.
const std = @import("std");
const local = @import("antfly_local_sources");
const contract = local.storage_db_relational_schema_owner_contract;
const barriers = @import("schema_validation_publication.zig");

pub const Lookup = struct {
    group: u64,
    table: u64,
    id: [16]u8,
    metadata_incarnation: [32]u8,
    plan_digest: [32]u8,

    pub fn fromHead(head: barriers.Head) Lookup {
        return .{ .group = head.metadata_group_id, .table = head.table_id, .id = head.id, .metadata_incarnation = head.metadata_incarnation, .plan_digest = head.plan_digest };
    }
    pub fn require(self: Lookup, head: barriers.Head) !void {
        try head.validate();
        if (!std.meta.eql(self, fromHead(head))) return error.GenerationPublicationChanged;
    }
};

/// Read-only discovery before opening a cold native owner. Neither a returned
/// lookup nor absence authorizes physical work or establishes a durable result.
pub const DiscoveryRequest = struct {
    group: u64,
    table: u64,
    metadata_incarnation: [32]u8,

    pub fn validate(self: @This()) !void {
        if (self.group == 0 or self.table == 0 or !local.metadata_incarnation.isValid(self.metadata_incarnation)) return error.InvalidGenerationPublication;
    }
};

pub const OwnerControlRead = struct {
    head: barriers.Head,
    owner: barriers.Owner,
    action: contract.Action,
    disposition: barriers.OwnerPermission,

    pub fn requireFor(self: @This(), request: OwnerReadRequest) !void {
        try request.validate();
        if (request.projection != .control or self.action != request.action or self.owner.ordinal != request.ordinal) return error.InvalidGenerationPublication;
        try request.lookup.require(self.head);
        if (self.disposition != try barriers.ownerPermission(self.head, self.owner, self.action)) return error.InvalidGenerationPublication;
    }
};

/// Identifies already materialized immutable bytes without requiring the source
/// process or its volatile parent-grant ID. This is lookup intent, not authority.
pub const ImageIdentity = struct {
    namespace: [24]u8,
    source_root: u128,
    through_index: u64,
    stream_digest: [32]u8,
    origin_digest: [32]u8,

    /// Describe receiver-owned, verified installation provenance. This is an
    /// image lookup key, never proof of parent authorization or root adoption.
    pub fn fromInstallation(installed: local.storage_db_native_backup_origin.Installation) !@This() {
        try installed.validate();
        const value: @This() = .{
            .namespace = installed.namespace,
            .source_root = installed.source.physical_root_incarnation,
            .through_index = installed.through_index,
            .stream_digest = installed.stream_digest,
            .origin_digest = try installed.source.digest(),
        };
        try value.validate();
        return value;
    }

    pub fn validate(self: @This()) !void {
        if (std.mem.allEqual(u8, &self.namespace, 0) or self.source_root == 0 or self.through_index == 0 or
            std.mem.allEqual(u8, &self.stream_digest, 0) or std.mem.allEqual(u8, &self.origin_digest, 0)) return error.InvalidGenerationPublication;
    }
    pub fn digest(self: @This()) ![32]u8 {
        try self.validate();
        var hash = std.crypto.hash.Blake3.init(.{});
        hash.update("antfly/schema-validation-parent-image/v1");
        hash.update(&self.namespace);
        var fields: [24]u8 = undefined;
        std.mem.writeInt(u128, fields[0..16], self.source_root, .little);
        std.mem.writeInt(u64, fields[16..24], self.through_index, .little);
        hash.update(&fields);
        hash.update(&self.stream_digest);
        hash.update(&self.origin_digest);
        var result: [32]u8 = undefined;
        hash.final(&result);
        return result;
    }
};

test "system catalog schema validation installed image lookup binds canonical provenance" {
    const native_origin = local.storage_db_native_backup_origin;
    const namespace = local.storage_db_doc_identity_namespace.Namespace{ .table_id = 3, .shard_id = 4, .range_id = 5 };
    const origin: native_origin.Origin = .{
        .physical_root_incarnation = 6,
        .namespace = namespace,
        .authority = .{ .kind = .raft, .namespace = local.storage_db_online_source_contract.namespaceBytes(namespace) },
        .applied = .{ .term = 2, .index = 7 },
        .schema_header = null,
        .transaction_directory = null,
        .schema_outcomes = null,
    };
    var installed: native_origin.Installation = .{ .group_id = 4, .through_index = 9, .destination_root_incarnation = 10, .namespace = local.storage_db_online_source_contract.namespaceBytes(namespace), .source = origin, .stream_digest = @splat(11) };
    const image = try ImageIdentity.fromInstallation(installed);
    try std.testing.expectEqualDeep(installed.namespace, image.namespace);
    try std.testing.expectEqual(@as(u128, 6), image.source_root);
    try std.testing.expectEqual(@as(u64, 9), image.through_index);
    try std.testing.expectEqualDeep(installed.stream_digest, image.stream_digest);
    try std.testing.expectEqualDeep(try origin.digest(), image.origin_digest);
    // A receiver move does not relabel source bytes. The signed root audit
    // and metadata writer separately qualify that destination's authority.
    installed.destination_root_incarnation = 12;
    try std.testing.expectEqualDeep(image, try ImageIdentity.fromInstallation(installed));
    installed.source.physical_root_incarnation = 13;
    const other = try ImageIdentity.fromInstallation(installed);
    try std.testing.expect(!std.meta.eql(image, other));
    try std.testing.expect(!std.mem.eql(u8, &image.origin_digest, &other.origin_digest));
    installed.stream_digest = @splat(0);
    try std.testing.expectError(error.InvalidNativeBackupOrigin, ImageIdentity.fromInstallation(installed));
}

/// A single metadata snapshot joins the retained source image to the CURRENT
/// owner/head predecessor. This is historical recovery material, not ordinary
/// owner execution permission: the metadata adoption writer independently
/// rechecks enrollment, placement, reader capabilities and the destination.
pub const RestorationControl = struct {
    head: barriers.Head,
    owner: barriers.Owner,
    parent: @import("schema_validation_parent.zig").Request,

    pub fn requireFor(self: @This(), lookup: Lookup, image: ImageIdentity) !void {
        try lookup.require(self.head);
        try self.owner.validate(self.head);
        try self.parent.validate();
        if (!std.meta.eql(self.parent.lookup, lookup) or self.parent.ordinal != self.owner.ordinal or
            !std.meta.eql(self.parent.signed.value.binding, self.owner.binding) or
            !std.meta.eql(try self.parent.image(), image) or self.parent.revision > self.head.revision or
            self.parent.root_epoch > self.owner.root_epoch) return error.GenerationPublicationChanged;
    }

    /// A historical parent may survive later source progress or another root
    /// epoch. It must not obstruct a separately certified complete Raft replay.
    /// Only the predecessor/phase check selects the proof family: malformed
    /// provenance, invalid signatures and failed metadata reads still fail.
    pub fn prepareAdoptionOrReplay(self: @This(), destination: local.storage_db_relational_schema_root_contract.Evidence, observation: @import("schema_validation_attestation.zig").SignedRaftObservation) !?@import("schema_validation_root_epoch.zig").Request {
        try self.requireFor(self.parent.lookup, try self.parent.image());
        self.parent.requirePredecessor(self.head, self.owner) catch |err| switch (err) {
            error.GenerationPublicationChanged, error.SchemaValidationRuntimeRecoveryRequired => return (ReplayControl{ .head = self.head, .owner = self.owner }).prepareAdoption(destination, observation),
            else => return err,
        };
        return self.prepareAdoption(destination, observation);
    }

    /// Null means metadata already names this exact physical destination. It
    /// does NOT permit an owner action: normal control authorization must still
    /// rejoin the current enrollment/report. A lost adoption reply can therefore
    /// recover through the existing immutable epoch/CAS, without minting a
    /// second physical operation or depending on the departed source process.
    pub fn prepareAdoption(self: @This(), destination: local.storage_db_relational_schema_root_contract.Evidence, observation: @import("schema_validation_attestation.zig").SignedRaftObservation) !?@import("schema_validation_root_epoch.zig").Request {
        try self.requireFor(self.parent.lookup, try self.parent.image());
        const request: @import("schema_validation_root_epoch.zig").Request = .{
            .lookup = .fromHead(self.head),
            .ordinal = self.owner.ordinal,
            .revision = self.head.revision,
            .owner_digest = try @import("schema_validation_parent.zig").ownerDigest(self.owner, self.head),
            .expected_epoch = self.owner.root_epoch,
            .lineage = .{ .retained_parent = try self.parent.id() },
            .proposal = .{ .audited = .{ .destination = destination, .observation = observation } },
        };
        try request.validateShape();
        const scope = try request.attestedScope();
        try scope.requireBinding(self.owner.binding);
        try destination.verify(scope.identity.public_key);
        const observed = observation.rooted.observation;
        if (destination.value.physical_root != observed.physical_root_incarnation or
            !std.meta.eql(observed.binding, self.owner.binding) or observed.header == null or
            !std.meta.eql(destination.value.rows.header, observed.header.?)) return error.SchemaValidationRuntimeRecoveryRequired;
        if (self.owner.physical_root_incarnation == destination.value.physical_root) return null;
        if (self.head.phase == .succeeded or self.head.phase == .canceled) return error.GenerationPublicationChanged;
        try request.requireDestination(self.head, self.owner, self.parent);
        return request;
    }
};

/// Historical discovery by the actual native namespace, including after the
/// previous reporter/source disappeared. This projection grants no execution
/// permission; adoption still verifies native audit, certified replay and CAS.
pub const OwnerAddress = struct {
    group_id: u64,
    namespace: [24]u8,

    pub fn fromBinding(binding: contract.Binding) @This() {
        return .{ .group_id = binding.fence.owner_group_id, .namespace = local.storage_db_online_source_contract.namespaceBytes(binding.fence.namespace) };
    }
    pub fn digest(self: @This()) [32]u8 {
        var bytes: [32]u8 = undefined;
        std.mem.writeInt(u64, bytes[0..8], self.group_id, .little);
        bytes[8..32].* = self.namespace;
        return contract.hash(&bytes);
    }
};

pub const ReplayControl = struct {
    head: barriers.Head,
    owner: barriers.Owner,

    pub fn requireFor(self: @This(), lookup: Lookup, address: OwnerAddress) !void {
        try lookup.require(self.head);
        try self.owner.validate(self.head);
        if (!std.meta.eql(address, OwnerAddress.fromBinding(self.owner.binding))) return error.SchemaValidationJobChanged;
    }

    pub fn prepareAdoption(self: @This(), destination: local.storage_db_relational_schema_root_contract.Evidence, observation: @import("schema_validation_attestation.zig").SignedRaftObservation) !?@import("schema_validation_root_epoch.zig").Request {
        try self.requireFor(.fromHead(self.head), .fromBinding(destination.value.rows.header.binding));
        const request: @import("schema_validation_root_epoch.zig").Request = .{
            .lookup = .fromHead(self.head),
            .ordinal = self.owner.ordinal,
            .revision = self.head.revision,
            .owner_digest = try @import("schema_validation_parent.zig").ownerDigest(self.owner, self.head),
            .expected_epoch = self.owner.root_epoch,
            .lineage = .raft_replay,
            .proposal = .{ .audited = .{ .destination = destination, .observation = observation } },
        };
        try request.validateShape();
        const scope = try request.attestedScope();
        try scope.requireBinding(self.owner.binding);
        try destination.verify(scope.identity.public_key);
        if (destination.value.physical_root != observation.rooted.observation.physical_root_incarnation or
            !std.meta.eql(observation.rooted.observation.binding, self.owner.binding) or
            !std.meta.eql(destination.value.rows.header, observation.rooted.observation.header orelse return error.SchemaValidationJobRequired)) return error.SchemaValidationRuntimeRecoveryRequired;
        if (self.owner.physical_root_incarnation == destination.value.physical_root) return null;
        if (self.head.phase == .succeeded or self.head.phase == .canceled) return error.GenerationPublicationChanged;
        try request.requireDestination(self.head, self.owner, null);
        return request;
    }
};

pub fn prepareBootstrapAdoption(control: ReplayControl, certified: @import("schema_validation_bootstrap.zig").CertifiedPreparation) !?@import("schema_validation_root_epoch.zig").Request {
    const lookup = Lookup.fromHead(control.head);
    try control.requireFor(lookup, .fromBinding(certified.preparation.value.binding));
    const request: @import("schema_validation_root_epoch.zig").Request = .{
        .lookup = lookup,
        .ordinal = control.owner.ordinal,
        .revision = control.head.revision,
        .owner_digest = try @import("schema_validation_parent.zig").ownerDigest(control.owner, control.head),
        .expected_epoch = control.owner.root_epoch,
        .lineage = .raft_replay,
        .proposal = .{ .bootstrap = certified },
    };
    try request.validateShape();
    _ = try request.attestedScope();
    if (control.owner.physical_root_incarnation == request.physicalRoot()) return null;
    if (control.head.phase == .succeeded or control.head.phase == .canceled) return error.GenerationPublicationChanged;
    try request.requireDestination(control.head, control.owner, null);
    return request;
}

pub fn prepareNativeAdoption(control: ReplayControl, proposal: @FieldType(@import("schema_validation_root_epoch.zig").Request, "proposal")) !?@import("schema_validation_root_epoch.zig").Request {
    const request: @import("schema_validation_root_epoch.zig").Request = .{
        .lookup = .fromHead(control.head),
        .ordinal = control.owner.ordinal,
        .revision = control.head.revision,
        .owner_digest = try @import("schema_validation_parent.zig").ownerDigest(control.owner, control.head),
        .expected_epoch = control.owner.root_epoch,
        .lineage = .native_recovery,
        .proposal = proposal,
    };
    try request.validateShape();
    try control.requireFor(request.lookup, .fromBinding(request.binding()));
    _ = try request.nativeAttestedScope();
    if (control.owner.physical_root_incarnation == request.physicalRoot()) return null;
    if (control.head.phase == .succeeded or control.head.phase == .canceled) return error.GenerationPublicationChanged;
    try request.requireDestination(control.head, control.owner, null);
    return request;
}

/// Fresh signed facts rejoin a restarted reporter without rewriting retained
/// preparation or ACKs. Enrollment, placement and reporting are independently
/// checked in the metadata snapshot; a witness is not authority by itself.
pub const OwnerWitness = union(enum) {
    preparation: @import("schema_validation_attestation.zig").SignedPreparation,
    observation: @import("schema_validation_attestation.zig").SignedObservation,
};

/// Authority comes from authenticated ingress and independent snapshot checks,
/// never a requested action or unverified fields in the body.
pub const OwnerReadRequest = struct {
    lookup: Lookup,
    ordinal: u16,
    action: contract.Action = .begin,
    projection: enum { control, inputs, parent, recovery_inputs, replay, replay_inputs } = .control,
    source_image: ?ImageIdentity = null,
    source_owner: ?OwnerAddress = null,
    witness: ?OwnerWitness = null,

    pub fn validate(self: @This()) !void {
        if (self.lookup.group == 0 or self.lookup.table == 0 or self.ordinal >= barriers.max_owners or
            std.mem.allEqual(u8, &self.lookup.id, 0) or std.mem.allEqual(u8, &self.lookup.plan_digest, 0) or
            !local.metadata_incarnation.isValid(self.lookup.metadata_incarnation) or
            (self.projection != .control and self.action != .begin) or
            ((self.projection == .parent or self.projection == .recovery_inputs) != (self.source_image != null)) or
            ((self.projection == .replay or self.projection == .replay_inputs) != (self.source_owner != null)) or
            ((self.projection == .parent or self.projection == .replay) and self.ordinal != 0) or
            (self.witness != null and self.projection != .control and self.projection != .inputs)) return error.InvalidGenerationPublication;
        if (self.source_image) |image| try image.validate();
        if (self.source_owner) |address| {
            if (address.group_id == 0 or std.mem.readInt(u64, address.namespace[0..8], .big) != self.lookup.table or
                std.mem.readInt(u64, address.namespace[8..16], .big) == 0 or
                std.mem.readInt(u64, address.namespace[16..24], .big) == 0) return error.InvalidGenerationPublication;
        }
    }

    pub fn contentType(self: @This()) []const u8 {
        return if (self.projection == .inputs or self.projection == .recovery_inputs or self.projection == .replay_inputs) "application/octet-stream" else "application/json";
    }
    pub fn responseLimit(self: @This()) usize {
        return switch (self.projection) {
            .inputs, .recovery_inputs, .replay_inputs => contract.max_input_bytes,
            // The bounded parent, owner and head are returned together.
            .parent => 96 * 1024,
            .control, .replay => 32 * 1024,
        };
    }
};

/// Read-only canonical negative replies. Unknown names/status mismatches never
/// authorize a replacement root, a new plan, or a physical-operation replay.
pub fn ownerReadHttpFailure(status: u16, body: []const u8) ?anyerror {
    inline for (.{
        error.InvalidGenerationPublication,            error.GenerationPublicationChanged,         error.GenerationPublicationNotFound,
        error.InvalidSchemaValidationJob,              error.SchemaValidationJobChanged,           error.SchemaValidationPublicationRequired,
        error.SchemaValidationNotReady,                error.SchemaValidationCanceled,             error.SchemaValidationCancellationRequired,
        error.InvalidIndexOwnerReceipt,                error.InvalidStoreRootEnrollment,           error.SchemaValidationJobRequired,
        error.SchemaValidationRuntimeRecoveryRequired, error.TableTopologyProtocolUpgradeRequired, error.CatalogOccupancyNotReady,
        error.MetadataReplicationPending,              error.MetadataIncarnationMismatch,          error.StoreRootEnrollmentChanged,
        error.IndexOwnerReceiptChanged,                error.Forbidden,                            error.CatalogPublicationProofPending,
        error.NotLeader,                               error.DeadlineExceeded,                     error.Cancelled,
    }) |err| if (status == local.system_catalog_domain.httpStatus(err) and std.mem.eql(u8, body, @errorName(err))) return err;
    return null;
}
