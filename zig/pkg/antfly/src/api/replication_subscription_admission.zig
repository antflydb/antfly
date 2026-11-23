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

const std = @import("std");
const local = @import("antfly_local_sources");
const registry = @import("../system_catalog/prepared_transactions.zig");
const payload = @import("../system_catalog/replication/payload.zig");
const artifacts = @import("../serverless/artifacts/store.zig");
const A = std.mem.Allocator;
const Permit = local.api_replication_source_apply.Permit;
const Command = @FieldType(@FieldType(registry.Mutation, "operation"), "replication_admit");

pub const Member = struct {
    participant: registry.Participant,
    intent: local.storage_db_types.TransactionIntentRequest,
    schema_digest: [32]u8,
    policy_epoch: local.api_replication_source_apply.PolicyEpoch = .{},
    image: ?local.api_replication_source_apply.Image = null,
    previous_marker_digest: ?[32]u8 = null,
};

pub const Attempt = struct {
    metadata_group: u64,
    incarnation: [32]u8,
    owner: @FieldType(registry.ReplicationReference, "owner"),
    birth: u64,
    generation: u64,
    origin: [32]u8,
    txn_id: [16]u8,
    recipe_digest: [32]u8,
    upload_token: u64,
    begin_timestamp: u64,
    admitted_at_ns: u64,
    expires_at_ns: u64,
    event: ?registry.ReplicationEvent = null,
    wal_access: ?[]const registry.ReplicationWalAccess = null,
    initialization: @FieldType(Command, "initialization") = null,
};

pub fn seal(a: A, store: ?*artifacts.ArtifactStore, attempt: Attempt, members: []const Member, cancellation: local.api_operation.CancellationToken) !Command {
    try cancellation.check();
    if (attempt.owner != .logical_subscription or members.len > registry.max_participants or attempt.admitted_at_ns == 0 or
        attempt.admitted_at_ns >= attempt.expires_at_ns or std.mem.allEqual(u8, &attempt.recipe_digest, 0)) return error.InvalidCatalogMutation;
    const frozen = try a.dupe(Member, members);
    defer a.free(frozen);
    // Subscription PREPARE requires a data codec that already supports
    // durable revisions. Freeze the scheme before hashing so the participant
    // leader's normal admission cannot change the retained request digest.
    for (frozen) |*member| {
        const probe = member.intent.asBatch(attempt.txn_id, member.participant.route.topology_epoch);
        member.intent.document_version_scheme = probe.admittedDocumentVersionScheme(true);
    }
    const participants = try a.alloc(registry.Participant, members.len);
    const permits = try a.alloc(Permit, members.len);
    const bodies = try a.alloc([32]u8, members.len);
    defer a.free(bodies);
    for (frozen, participants, permits, bodies, 0..) |member, *part, *permit, *body, index| {
        try cancellation.check();
        part.* = member.participant;
        permit.* = try describeMember(a, attempt, member, @intCast(index));
        body.* = permit.pregrant_body_digest;
    }
    const source: registry.ReplicationReference = .{
        .owner = attempt.owner,
        .birth = attempt.birth,
        .generation = attempt.generation,
        .source_digest = attempt.origin,
        .event_digest = try (attempt.event orelse return error.InvalidCatalogMutation).eventDigest(),
        .intent_digest = registry.replicationIntentDigest(bodies),
        .manifest_digest = try registry.manifestDigest(a, participants, attempt.incarnation),
        .txn_id = attempt.txn_id,
    };
    try source.validate();
    const retained = if (frozen.len == 0) null else try retainMembers(a, store orelse return error.ArtifactUnavailable, attempt, source, frozen, cancellation);
    const identity: registry.Identity = .{
        .kind = .replication,
        .replication_source = source,
        .gid = "",
        .database_id = source.owner.logical_subscription.database_id,
        .principal = .bootstrap,
        .txn_id = source.txn_id,
        .operation_id = source.txn_id,
        .metadata_incarnation = attempt.incarnation,
        .begin_timestamp = attempt.begin_timestamp,
        .expires_at_unix_ns = attempt.expires_at_ns,
        .plan_digest = if (retained) |intents| try intents.digest() else try registry.planDigest("[]"),
        .manifest_digest = source.manifest_digest,
    };
    const owner = try identity.owner(a);
    for (permits) |*permit| {
        permit.semantic = .{
            .metadata_group = attempt.metadata_group,
            .metadata_incarnation = attempt.incarnation,
            .owner = source.owner,
            .birth = source.birth,
            .source_digest = source.source_digest,
            .event_digest = source.event_digest,
            .transaction_id = source.txn_id,
        };
        permit.reference = source;
        permit.decision_owner = owner;
        try permit.validate();
    }
    return .{
        .input = .{ .identity = identity, .participants = participants, .admitted_at_ns = attempt.admitted_at_ns },
        .event = attempt.event orelse return error.InvalidCatalogMutation,
        .initialization = attempt.initialization,
        .recipe_digest = attempt.recipe_digest,
        .intents = retained,
        .permits = permits,
        .wal_access = attempt.wal_access,
    };
}

pub const SnapshotSigner = struct {
    identity: @import("../metadata/store_root_enrollment.zig").Identity,
    reporter: u64,
    root: local.storage_db_root_signing_identity.State,
};

pub const SnapshotPageInput = struct {
    routing: *const @import("table_catalog.zig").AuthoritativeTableRoutingSnapshot,
    destination: registry.ReplicationDestination,
    expected_page: [32]u8,
    relation: local.system_catalog_logical_replication_origin.Relation,
    page: @import("../system_catalog/replication/snapshot_codec.zig").CapturePage,
    proof: @import("../system_catalog/replication/snapshot_chain.zig").Page,
};

pub const PagePlan = @import("replication_subscription_rows.zig").PagePlan;

pub fn planSnapshotPage(a: A, attempt: Attempt, input: SnapshotPageInput) !PagePlan {
    const cut = input.routing.snapshot.value;
    const incarnation = cut.metadata_incarnation orelse return error.CatalogGenerationChanged;
    if (input.routing.table_id != input.destination.table_id or cut.metadata_group_id != attempt.metadata_group or
        !std.mem.eql(u8, &incarnation, &attempt.incarnation))
        return error.CatalogGenerationChanged;
    return @import("replication_subscription_rows.zig").planPage(a, input.routing, input.destination.storage_name, input.destination.schema_version, attempt.origin, (attempt.event orelse return error.InvalidCatalogMutation).source_point, input.expected_page, input.relation, input.page, input.proof);
}

pub fn sealPlannedSnapshotPage(a: A, store: *artifacts.ArtifactStore, attempt: Attempt, page: PagePlan, destination: registry.ReplicationDestination, targets: []const ?registry.ReplicationTarget, cancellation: local.api_operation.CancellationToken) !Command {
    if (page.groups.len != targets.len or page.groups.len == 0 or page.event.kind != .snapshot_page)
        return error.InvalidReplicationSnapshotCursor;
    var selected = attempt;
    selected.event = page.event;
    const members = try a.alloc(Member, page.groups.len);
    defer a.free(members);
    for (members, page.groups, targets) |*member, group, target|
        member.* = plannedMember(group, destination.schema_digest, destination.policy_epoch, target);
    return seal(a, store, selected, members, cancellation);
}

pub fn sealSnapshotPage(a: A, store: *artifacts.ArtifactStore, attempt: Attempt, input: SnapshotPageInput, targets: []const ?registry.ReplicationTarget, signer: SnapshotSigner, cancellation: local.api_operation.CancellationToken) !Command {
    const plan = try planSnapshotPage(a, attempt, input);
    var command = try sealPlannedSnapshotPage(a, store, attempt, plan, input.destination, targets, cancellation);
    const attestation = @import("../metadata/replication_initialization_attestation.zig");
    const expected = try attestation.expectedPage(attempt.metadata_group, attempt.incarnation, command.input.identity.replication_source.?, command.input.identity.plan_digest);
    try cancellation.check();
    command.page_proof = try attestation.Proof.sign(expected, signer.identity, signer.reporter, signer.root);
    return command;
}

fn retainMembers(a: A, store: *artifacts.ArtifactStore, attempt: Attempt, source: registry.ReplicationReference, members: []const Member, cancellation: local.api_operation.CancellationToken) !registry.ReplicationIntents {
    const scope = try payload.uploadScope(source, attempt.metadata_group, attempt.incarnation, attempt.upload_token, source.txn_id);
    const refs = try a.alloc(payload.Retained, members.len);
    for (members, refs) |member, *ref| {
        try cancellation.check();
        const bytes = try payload.encodeIntent(a, .{
            .transaction = source.txn_id,
            .topology_epoch = member.participant.route.topology_epoch,
            .intent = member.intent,
        });
        defer a.free(bytes);
        if (bytes.len > local.storage_db_table_catalog.default_transaction_admission_bytes) return error.InvalidReplicationPayloadAdmission;
        ref.* = try payload.retain(a, store, scope, bytes, cancellation);
    }
    return .{ .upload_token = attempt.upload_token, .members = refs };
}

fn describeMember(a: A, attempt: Attempt, member: Member, ordinal: u16) !Permit {
    const part = member.participant;
    const route = part.route;
    const ns = route.route.identity_namespace;
    if (route.metadata_group_id != attempt.metadata_group or route.route.execution != .raft or part.sql_locks != null or
        part.schema_version != member.intent.schema_version) return error.InvalidCatalogMutation;
    try (payload.FrozenIntent{ .transaction = attempt.txn_id, .topology_epoch = route.topology_epoch, .intent = member.intent }).validate();
    if (std.mem.allEqual(u8, &member.schema_digest, 0))
        return error.InvalidCatalogMutation;
    const namespace: @FieldType(Permit, "namespace") = .{ .table_id = ns.table_id, .shard_id = ns.shard_id, .range_id = ns.range_id };
    const custody_digest = try registry.replicationTargetDigest(attempt.metadata_group, attempt.incarnation, attempt.owner, attempt.birth, namespace);
    if (member.image) |image| {
        try image.validate();
        if (!std.mem.eql(u8, &image.target_custody_digest, &custody_digest)) return error.InvalidCatalogMutation;
    } else if (member.previous_marker_digest != null) return error.InvalidCatalogMutation;
    const witnesses = try local.storage_db_logical_replication_request.digests(a, .{
        .namespace = namespace,
        .owner_group_id = route.route.group_id,
        .transaction_id = attempt.txn_id,
        .member = ordinal,
        .topology_epoch = route.topology_epoch,
    }, member.intent);
    if (part.publication_capture) |publication| {
        if (!std.mem.eql(u8, &publication.pregrant_body_digest, &witnesses.body) or
            !std.mem.eql(u8, &publication.request_digest, &witnesses.request)) return error.InvalidCatalogMutation;
    }
    return .{
        .semantic = undefined,
        .reference = undefined,
        .decision_owner = undefined,
        .member = ordinal,
        .owner_group_id = route.route.group_id,
        .topology_epoch = route.topology_epoch,
        .namespace = namespace,
        .schema_version = member.intent.schema_version orelse return error.InvalidCatalogMutation,
        .schema_digest = member.schema_digest,
        .target_custody_digest = custody_digest,
        .expected_image = member.image,
        .previous_marker_digest = member.previous_marker_digest,
        .sql_rights = try local.api_replication_source_apply.intentRights(member.intent),
        .policy_epoch = member.policy_epoch,
        .pregrant_body_digest = witnesses.body,
        .request_digest = witnesses.request,
    };
}

pub const SnapshotSigningDriver = struct {
    ptr: *anyopaque,
    sign: *const fn (*anyopaque, A, @import("../metadata/replication_initialization_attestation.zig").Expected, local.api_operation.RequestContext) anyerror!@import("../metadata/replication_initialization_attestation.zig").Proof,
};

const row_planner = @import("replication_subscription_rows.zig");

fn plannedMember(group: row_planner.Group, schema_digest: [32]u8, policy_epoch: local.api_replication_source_apply.PolicyEpoch, target: ?registry.ReplicationTarget) Member {
    return .{
        .participant = group.participant,
        .intent = group.intent,
        .schema_digest = schema_digest,
        .policy_epoch = policy_epoch,
        .image = if (target) |custody| custody.image else null,
        .previous_marker_digest = if (target) |custody| custody.marker else null,
    };
}

/// Bind all owners of one source commit to the same decision and immutable
/// retained intent roster. An empty final effect still advances via the lane.
pub fn sealWal(a: A, store: *artifacts.ArtifactStore, attempt: Attempt, planned: []const row_planner.WalMember, targets: []const ?registry.ReplicationTarget, cancellation: local.api_operation.CancellationToken) !Command {
    const event = attempt.event orelse return error.InvalidReplicationTransaction;
    if (event.kind != .transaction or attempt.initialization != null or planned.len != targets.len or planned.len > registry.max_participants)
        return error.InvalidReplicationTransaction;
    const members = try a.alloc(Member, planned.len);
    defer a.free(members);
    for (members, planned, targets) |*member, entry, target|
        member.* = plannedMember(entry.group, entry.schema_digest, entry.policy_epoch, target);
    return seal(a, store, attempt, members, cancellation);
}
